package sanity

import (
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"google.golang.org/grpc"

	"github.com/singular-group/csi-direct-iscsi/proxy"
	"k8s.io/cloud-provider-openstack/pkg/csi/cinder"
	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
)

// The CSI sanity suite through the proxy, with upstream's driver (on upstream's
// fake cloud and mounts, as upstream runs it) behind: no direct iSCSI, so every
// call is upstream's and must come back unchanged.
func TestSanityThroughTheProxy(t *testing.T) {
	dir := t.TempDir()
	upstream, endpoint := "unix://"+filepath.Join(dir, "upstream.sock"), "unix://"+filepath.Join(dir, "csi.sock")

	d := cinder.NewDriver(&cinder.DriverOpts{Endpoint: upstream, ClusterID: "kubernetes", WithTopology: true})
	cloud := getfakecloud()
	openstack.OsInstances = map[string]openstack.IOpenStack{"": cloud}
	d.SetupControllerService(openstack.OsInstances)
	d.SetupNodeService(GetFakeMountProvider(), &fakemetadata{}, openstack.BlockStorageOpts{NodeVolumeAttachLimit: 200}, map[string]string{})
	go d.Run()

	up, err := proxy.Dial(upstream)
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	s := grpc.NewServer(grpc.UnaryInterceptor(proxy.LogErrors))
	csi.RegisterIdentityServer(s, &proxy.Identity{Up: up.Identity})
	csi.RegisterControllerServer(s, &proxy.Controller{Up: up.Controller, Cloud: cloud})
	csi.RegisterNodeServer(s, &proxy.Node{Up: up.Node})
	l, err := proxy.Listen(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(l)
	defer s.Stop()

	config := sanity.NewTestConfig()
	config.Address = endpoint
	config.TargetPath = filepath.Join(dir, "target")
	config.StagingPath = filepath.Join(dir, "staging")
	sanity.Test(t, config)
}
