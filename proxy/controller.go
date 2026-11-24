package proxy

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/singular-group/csi-direct-iscsi/directiscsi"
	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
	cpoerrors "k8s.io/cloud-provider-openstack/pkg/util/errors"
)

// Controller handles creation type, deletion, publish and the published nodes
// of direct volumes; everything else, Nova volumes included, is upstream's.
type Controller struct {
	csi.UnimplementedControllerServer
	Up    csi.ControllerClient
	Cloud openstack.IOpenStack
}

func (s *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volType := req.GetParameters()["type"]
	if t := directiscsi.VolumeTypeForCreate(ctx, s.Cloud, volType); t != volType {
		req = proto.Clone(req).(*csi.CreateVolumeRequest)
		if req.Parameters == nil {
			req.Parameters = map[string]string{}
		}
		req.Parameters["type"] = t
	}
	return s.Up.CreateVolume(ctx, req)
}

func (s *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if err := directiscsi.BeforeDeleteVolume(ctx, s.Cloud, req.GetVolumeId()); err != nil {
		return nil, err
	}
	return s.Up.DeleteVolume(ctx, req)
}

func (s *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if resp, handled, err := directiscsi.ControllerPublish(ctx, s.Cloud, req.GetVolumeId(), req.GetNodeId()); handled {
		return resp, err
	}
	return s.Up.ControllerPublishVolume(ctx, req)
}

func (s *Controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	direct, err := directiscsi.ControllerUnpublish(ctx, s.Cloud, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if direct {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	return s.Up.ControllerUnpublishVolume(ctx, req)
}

// ListVolumes: upstream lists an attachment's server as its node, which a direct
// attachment lacks; without its node the attacher would mark the volume detached.
func (s *Controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	resp, err := s.Up.ListVolumes(ctx, req)
	if err != nil {
		return nil, err
	}
	for _, e := range resp.GetEntries() {
		if err := s.publishedNodes(ctx, e.GetVolume().GetVolumeId(), e.GetStatus()); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	resp, err := s.Up.ControllerGetVolume(ctx, req)
	if err != nil {
		return nil, err
	}
	st := resp.GetStatus()
	if err := s.publishedNodes(ctx, req.GetVolumeId(), st); err != nil {
		return nil, err
	}
	return resp, nil
}

type publishedStatus interface {
	GetPublishedNodeIds() []string
}

// publishedNodes rewrites a status that lists a server-less attachment: upstream
// gives it an empty node ID.
func (s *Controller) publishedNodes(ctx context.Context, volumeID string, st publishedStatus) error {
	if st == nil || !hasEmpty(st.GetPublishedNodeIds()) {
		return nil
	}
	vol, err := s.Cloud.GetVolume(ctx, volumeID)
	if err != nil {
		if cpoerrors.IsNotFound(err) {
			return nil
		}
		return status.Errorf(codes.Internal, "direct iSCSI: get volume %s: %v", volumeID, err)
	}
	ids := directiscsi.PublishedNodeIDs(vol.Attachments)
	switch v := st.(type) {
	case *csi.ListVolumesResponse_VolumeStatus:
		v.PublishedNodeIds = ids
	case *csi.ControllerGetVolumeResponse_VolumeStatus:
		v.PublishedNodeIds = ids
	}
	return nil
}

func hasEmpty(ids []string) bool {
	for _, id := range ids {
		if id == "" {
			return true
		}
	}
	return false
}

func (s *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return s.Up.ValidateVolumeCapabilities(ctx, req)
}

func (s *Controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return s.Up.GetCapacity(ctx, req)
}

func (s *Controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return s.Up.ControllerGetCapabilities(ctx, req)
}

func (s *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return s.Up.CreateSnapshot(ctx, req)
}

func (s *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return s.Up.DeleteSnapshot(ctx, req)
}

func (s *Controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return s.Up.ListSnapshots(ctx, req)
}

func (s *Controller) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	return s.Up.GetSnapshot(ctx, req)
}

func (s *Controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return s.Up.ControllerExpandVolume(ctx, req)
}

func (s *Controller) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return s.Up.ControllerModifyVolume(ctx, req)
}
