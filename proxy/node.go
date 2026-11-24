package proxy

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"

	"github.com/singular-group/csi-direct-iscsi/directiscsi"
)

// resizeRequired is upstream's volume context key for a volume created larger
// than its source.
const resizeRequired = "cinder.csi.openstack.org/resizeRequired"

// Node stages, publishes a raw block and expands direct volumes; everything
// else, Nova volumes included, is upstream's.
type Node struct {
	csi.UnimplementedNodeServer
	Up    csi.NodeClient
	Mount *mount.SafeFormatAndMount
}

func (s *Node) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if !directiscsi.IsDirect(req.GetPublishContext()) {
		return s.Up.NodeStageVolume(ctx, req)
	}
	volumeID, target, capability := req.GetVolumeId(), req.GetStagingTargetPath(), req.GetVolumeCapability()
	if volumeID == "" || target == "" || capability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume needs a volume ID, a staging target path and a volume capability")
	}
	devicePath, err := directiscsi.NodeStage(ctx, volumeID, req.GetPublishContext())
	if err != nil {
		return nil, err
	}
	if capability.GetBlock() != nil {
		return &csi.NodeStageVolumeResponse{}, nil
	}
	notMnt, err := s.notMountPoint(target)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		fsType, options := "ext4", []string(nil)
		if m := capability.GetMount(); m != nil {
			if m.FsType != "" {
				fsType = m.FsType
			}
			options = append(options, m.GetMountFlags()...)
		}
		// Upstream's: xfs refuses a second volume with the same filesystem UUID (a clone).
		if fsType == "xfs" {
			options = append(options, "nouuid")
		}
		if err := s.Mount.FormatAndMount(devicePath, target, fsType, options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if strings.EqualFold(req.GetVolumeContext()[resizeRequired], "true") {
		r := mount.NewResizeFs(s.Mount.Exec)
		need, err := r.NeedResize(devicePath, target)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not determine if volume %q needs a resize: %v", volumeID, err)
		}
		if need {
			if _, err := r.Resize(devicePath, target); err != nil {
				return nil, status.Errorf(codes.Internal, "could not resize volume %q: %v", volumeID, err)
			}
		}
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// notMountPoint creates a missing target, as upstream does, and reports whether it is unmounted.
func (s *Node) notMountPoint(path string) (bool, error) {
	notMnt, err := s.Mount.IsLikelyNotMountPoint(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o750); err != nil {
			return false, err
		}
		return true, nil
	}
	return notMnt, err
}

// NodeUnstageVolume: upstream unmounts the staging path, then the direct volume's
// reservation, map and paths go.
func (s *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	resp, err := s.Up.NodeUnstageVolume(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := directiscsi.NodeUnstage(ctx, req.GetVolumeId()); err != nil {
		return nil, err
	}
	return resp, nil
}

// NodePublishVolume: upstream finds a raw block's device by its virtio serial,
// which a direct volume's multipath map lacks; a filesystem is a bind mount of
// the staging path either way.
func (s *Node) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeCapability().GetBlock() == nil {
		return s.Up.NodePublishVolume(ctx, req)
	}
	source, err := directiscsi.BlockDevicePath(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "direct iSCSI: device of volume %s: %v", req.GetVolumeId(), err)
	}
	if source == "" {
		return s.Up.NodePublishVolume(ctx, req)
	}
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Target Path must be provided")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "could not create dir %q: %v", filepath.Dir(target), err)
	}
	f, err := os.OpenFile(target, os.O_CREATE, 0o640)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not create file %q: %v", target, err)
	}
	f.Close()
	options := []string{"bind", "rw"}
	if req.GetReadonly() {
		options[1] = "ro"
	}
	if err := s.Mount.Mount(source, target, "", options); err != nil {
		os.Remove(target)
		return nil, status.Errorf(codes.Internal, "could not mount %q at %q: %v", source, target, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeExpandVolume: directiscsi grows a direct volume's paths and map; its
// filesystem is resized here, since upstream's rescan expects a virtio disk.
func (s *Node) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	direct, err := directiscsi.NodeExpand(ctx, req.GetVolumeId(), req.GetCapacityRange().GetRequiredBytes())
	if err != nil {
		return nil, err
	}
	if !direct {
		return s.Up.NodeExpandVolume(ctx, req)
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return &csi.NodeExpandVolumeResponse{}, nil
	}
	path := req.GetVolumePath()
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path not provided")
	}
	out, err := s.Mount.Exec.Command("findmnt", "-o", "source", "--first-only", "--noheadings", "--target", path).CombinedOutput()
	devicePath := strings.TrimSpace(string(out))
	if err != nil || devicePath == "" {
		return nil, status.Errorf(codes.Internal, "could not find the device mounted at %s: %v %s", path, err, out)
	}
	if _, err := mount.NewResizeFs(s.Mount.Exec).Resize(devicePath, path); err != nil {
		return nil, status.Errorf(codes.Internal, "could not resize volume %q: %v", req.GetVolumeId(), err)
	}
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (s *Node) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	return s.Up.NodeUnpublishVolume(ctx, req)
}

func (s *Node) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return s.Up.NodeGetVolumeStats(ctx, req)
}

func (s *Node) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return s.Up.NodeGetCapabilities(ctx, req)
}

func (s *Node) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return s.Up.NodeGetInfo(ctx, req)
}
