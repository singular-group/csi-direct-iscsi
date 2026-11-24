package proxy

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
	cpoerrors "k8s.io/cloud-provider-openstack/pkg/util/errors"
)

// fakeUpstream records the calls that reach upstream's socket.
type fakeUpstream struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
	mu    sync.Mutex
	calls []string
	types []string
	list  *csi.ListVolumesResponse
}

func (f *fakeUpstream) record(m string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, m)
}

func (f *fakeUpstream) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	f.record("GetPluginInfo")
	return &csi.GetPluginInfoResponse{Name: "cinder.csi.openstack.org", VendorVersion: "upstream"}, nil
}

func (f *fakeUpstream) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	f.record("CreateVolume")
	f.types = append(f.types, req.GetParameters()["type"])
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: "v1"}}, nil
}

func (f *fakeUpstream) DeleteVolume(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	f.record("DeleteVolume")
	return &csi.DeleteVolumeResponse{}, nil
}

func (f *fakeUpstream) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	f.record("ControllerPublishVolume")
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"DevicePath": "/dev/vdb"}}, nil
}

func (f *fakeUpstream) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	f.record("ControllerUnpublishVolume")
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (f *fakeUpstream) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	f.record("ListVolumes")
	return f.list, nil
}

func (f *fakeUpstream) ControllerGetVolume(_ context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	f.record("ControllerGetVolume")
	for _, e := range f.list.Entries {
		if e.Volume.VolumeId == req.GetVolumeId() {
			return &csi.ControllerGetVolumeResponse{
				Volume: &csi.Volume{VolumeId: req.GetVolumeId()},
				Status: &csi.ControllerGetVolumeResponse_VolumeStatus{PublishedNodeIds: e.Status.PublishedNodeIds},
			}, nil
		}
	}
	return nil, status.Error(codes.NotFound, req.GetVolumeId())
}

func (f *fakeUpstream) NodeStageVolume(context.Context, *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	f.record("NodeStageVolume")
	return &csi.NodeStageVolumeResponse{}, nil
}

func (f *fakeUpstream) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	f.record("NodeUnstageVolume")
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (f *fakeUpstream) NodePublishVolume(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	f.record("NodePublishVolume")
	return &csi.NodePublishVolumeResponse{}, nil
}

func (f *fakeUpstream) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	f.record("NodeExpandVolume")
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (f *fakeUpstream) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	f.record("NodeGetInfo")
	return &csi.NodeGetInfoResponse{NodeId: "uuid-1"}, nil
}

// fakeCloud is upstream's cloud with only the volume lookup the proxy makes.
type fakeCloud struct {
	openstack.IOpenStack
	vols map[string]*volumes.Volume
	gets int
}

func (c *fakeCloud) GetVolume(_ context.Context, id string) (*volumes.Volume, error) {
	c.gets++
	v, ok := c.vols[id]
	if !ok {
		return nil, cpoerrors.ErrNotFound
	}
	return v, nil
}

type env struct {
	up    *fakeUpstream
	cloud *fakeCloud
	conn  *grpc.ClientConn
}

// newEnv starts a fake upstream and the proxy on unix sockets, as in the pods.
func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	upSock, proxySock := "unix://"+filepath.Join(dir, "upstream.sock"), "unix://"+filepath.Join(dir, "csi.sock")
	e := &env{up: &fakeUpstream{}, cloud: &fakeCloud{vols: map[string]*volumes.Volume{}}}

	us := grpc.NewServer()
	csi.RegisterIdentityServer(us, e.up)
	csi.RegisterControllerServer(us, e.up)
	csi.RegisterNodeServer(us, e.up)
	ul, err := Listen(upSock)
	if err != nil {
		t.Fatal(err)
	}
	go us.Serve(ul)
	t.Cleanup(us.Stop)

	up, err := Dial(upSock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { up.Close() })
	ps := grpc.NewServer()
	csi.RegisterIdentityServer(ps, &Identity{Up: up.Identity})
	csi.RegisterControllerServer(ps, &Controller{Up: up.Controller, Cloud: e.cloud})
	csi.RegisterNodeServer(ps, &Node{Up: up.Node})
	pl, err := Listen(proxySock)
	if err != nil {
		t.Fatal(err)
	}
	go ps.Serve(pl)
	t.Cleanup(ps.Stop)

	e.conn, err = grpc.NewClient(proxySock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.conn.Close() })
	return e
}

func (e *env) called(t *testing.T, want ...string) {
	t.Helper()
	e.up.mu.Lock()
	defer e.up.mu.Unlock()
	if len(e.up.calls) != len(want) {
		t.Fatalf("upstream calls %v, want %v", e.up.calls, want)
	}
	for i := range want {
		if e.up.calls[i] != want[i] {
			t.Fatalf("upstream calls %v, want %v", e.up.calls, want)
		}
	}
}

func TestIdentityIsUpstreams(t *testing.T) {
	e := newEnv(t)
	r, err := csi.NewIdentityClient(e.conn).GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil || r.Name != "cinder.csi.openstack.org" || r.VendorVersion != "upstream" {
		t.Fatalf("%v %v", r, err)
	}
}

func TestWithoutDirectISCSIEverythingIsForwarded(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cc, nc := csi.NewControllerClient(e.conn), csi.NewNodeClient(e.conn)
	if _, err := cc.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc", Parameters: map[string]string{"type": "msa-fast"}}); err != nil {
		t.Fatal(err)
	}
	if e.up.types[0] != "msa-fast" {
		t.Fatalf("type %q reached upstream", e.up.types[0])
	}
	if _, err := cc.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "v1"}); err != nil {
		t.Fatal(err)
	}
	r, err := cc.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: "v1", NodeId: "uuid-1"})
	if err != nil || r.PublishContext["DevicePath"] != "/dev/vdb" {
		t.Fatalf("%v %v", r, err)
	}
	if _, err := cc.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "v1", NodeId: "uuid-1"}); err != nil {
		t.Fatal(err)
	}
	cap := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}}
	if _, err := nc.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: "v1", StagingTargetPath: "/s", VolumeCapability: cap, PublishContext: map[string]string{"DevicePath": "/dev/vdb"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: "v1", TargetPath: "/t", VolumeCapability: cap}); err != nil {
		t.Fatal(err)
	}
	block := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}
	if _, err := nc.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: "v1", TargetPath: "/t", VolumeCapability: block}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: "v1", VolumePath: "/t"}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: "v1", StagingTargetPath: "/s"}); err != nil {
		t.Fatal(err)
	}
	if r, err := nc.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{}); err != nil || r.NodeId != "uuid-1" {
		t.Fatalf("%v %v", r, err)
	}
	e.called(t, "CreateVolume", "DeleteVolume", "ControllerPublishVolume", "ControllerUnpublishVolume",
		"NodeStageVolume", "NodePublishVolume", "NodePublishVolume", "NodeExpandVolume", "NodeUnstageVolume", "NodeGetInfo")
}

func TestDirectStageNeverReachesUpstream(t *testing.T) {
	e := newEnv(t)
	cap := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}}
	_, err := csi.NewNodeClient(e.conn).NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: "v1", StagingTargetPath: "/s", VolumeCapability: cap,
		PublishContext: map[string]string{"directISCSI": "true", "lun": "3", "targetIQN": "iqn.x"},
	})
	// This node has no [DirectISCSI]: the stage fails here instead of going to upstream.
	if status.Code(err) == codes.OK {
		t.Fatal("direct stage succeeded on a node without direct iSCSI")
	}
	e.called(t)
}

func TestPublishedNodesOfServerlessAttachments(t *testing.T) {
	e := newEnv(t)
	e.cloud.vols["direct"] = &volumes.Volume{ID: "direct", Attachments: []volumes.Attachment{{HostName: "node-1"}, {ServerID: "uuid-2"}}}
	e.up.list = &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{
		{Volume: &csi.Volume{VolumeId: "nova"}, Status: &csi.ListVolumesResponse_VolumeStatus{PublishedNodeIds: []string{"uuid-1"}}},
		{Volume: &csi.Volume{VolumeId: "direct"}, Status: &csi.ListVolumesResponse_VolumeStatus{PublishedNodeIds: []string{"", "uuid-2"}}},
	}}
	r, err := csi.NewControllerClient(e.conn).ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Entries[0].Status.PublishedNodeIds; len(got) != 1 || got[0] != "uuid-1" {
		t.Fatalf("Nova volume's nodes rewritten: %v", got)
	}
	// Without the controller's CSINodes a server-less attachment keeps upstream's empty ID.
	if got := r.Entries[1].Status.PublishedNodeIds; len(got) != 2 || got[1] != "uuid-2" {
		t.Fatalf("direct volume's nodes %v", got)
	}
	if e.cloud.gets != 1 {
		t.Fatalf("%d volume lookups, want 1 (only the entry upstream left without a node)", e.cloud.gets)
	}
	g, err := csi.NewControllerClient(e.conn).ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "direct"})
	if err != nil || len(g.Status.PublishedNodeIds) != 2 || e.cloud.gets != 2 {
		t.Fatalf("%v %v, %d lookups", g, err, e.cloud.gets)
	}
}

func TestPublishedNodesOfADeletedVolume(t *testing.T) {
	e := newEnv(t)
	e.up.list = &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{
		{Volume: &csi.Volume{VolumeId: "gone"}, Status: &csi.ListVolumesResponse_VolumeStatus{PublishedNodeIds: []string{""}}},
	}}
	if _, err := csi.NewControllerClient(e.conn).ListVolumes(context.Background(), &csi.ListVolumesRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamDownIsUnavailable(t *testing.T) {
	up, err := Dial("unix://" + filepath.Join(t.TempDir(), "nobody.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	_, err = (&Identity{Up: up.Identity}).Probe(context.Background(), &csi.ProbeRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("%v, want Unavailable", err)
	}
}

func TestEndpoint(t *testing.T) {
	for in, want := range map[string]string{"unix:///csi/csi.sock": "/csi/csi.sock", "unix://csi/csi.sock": "/csi/csi.sock"} {
		if got, err := socketPath(in); err != nil || got != want {
			t.Fatalf("%s: %s %v", in, got, err)
		}
	}
	for _, bad := range []string{"tcp://1.2.3.4:5", "unix://", "/csi/csi.sock"} {
		if _, err := socketPath(bad); err == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
}
