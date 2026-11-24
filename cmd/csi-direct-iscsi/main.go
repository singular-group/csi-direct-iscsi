// csi-direct-iscsi listens on the CSI socket in front of upstream's Cinder CSI
// plugin, answers the calls of direct iSCSI volumes and forwards the rest.
package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/v2"
	gopherstack "github.com/gophercloud/gophercloud/v2/openstack"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	"github.com/singular-group/csi-direct-iscsi/directiscsi"
	"github.com/singular-group/csi-direct-iscsi/proxy"
	"k8s.io/cloud-provider-openstack/pkg/client"
	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
)

var version = "dev"

type files []string

func (f *files) String() string     { return strings.Join(*f, ",") }
func (f *files) Set(v string) error { *f = append(*f, v); return nil }

func main() {
	klog.InitFlags(nil)
	var cloudConfig files
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI socket to serve")
	upstream := flag.String("upstream-endpoint", "unix:///csi/upstream.sock", "upstream plugin's CSI socket")
	mode := flag.String("mode", "", "controller or node")
	flag.Var(&cloudConfig, "cloud-config", "upstream's --cloud-config files, [DirectISCSI] included (repeatable)")
	flag.Parse()
	klog.Infof("csi-direct-iscsi %s, %s", version, *mode)
	if err := run(*mode, *endpoint, *upstream, cloudConfig); err != nil {
		klog.Fatal(err)
	}
}

func run(mode, endpoint, upstream string, cloudConfig []string) error {
	if len(cloudConfig) == 0 {
		return fmt.Errorf("--cloud-config is required")
	}
	up, err := proxy.Dial(upstream)
	if err != nil {
		return err
	}
	defer up.Close()
	server := grpc.NewServer(grpc.UnaryInterceptor(proxy.LogErrors))
	csi.RegisterIdentityServer(server, &proxy.Identity{Up: up.Identity})
	switch mode {
	case "controller":
		cloud, bs, err := openStack(cloudConfig)
		if err != nil {
			return err
		}
		if err := directiscsi.SetupController(cloudConfig, bs); err != nil {
			return err
		}
		csi.RegisterControllerServer(server, &proxy.Controller{Up: up.Controller, Cloud: cloud})
	case "node":
		if err := directiscsi.SetupNode(cloudConfig); err != nil {
			return err
		}
		m := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
		csi.RegisterNodeServer(server, &proxy.Node{Up: up.Node, Mount: m})
	default:
		return fmt.Errorf("--mode is controller or node, not %q", mode)
	}
	l, err := proxy.Listen(endpoint)
	if err != nil {
		return err
	}
	klog.Infof("serving %s, forwarding to %s", endpoint, upstream)
	return server.Serve(l)
}

// openStack builds upstream's cloud from the same files, and a Cinder v3 client
// for the attachment calls upstream's cloud does not expose.
func openStack(cloudConfig []string) (openstack.IOpenStack, *gophercloud.ServiceClient, error) {
	openstack.InitOpenStackProvider(cloudConfig, "")
	cloud, err := openstack.GetOpenStackProvider("")
	if err != nil {
		return nil, nil, err
	}
	cfg, err := openstack.GetConfigFromFiles(cloudConfig)
	if err != nil {
		return nil, nil, err
	}
	global := cfg.Global[""]
	if global == nil {
		return nil, nil, fmt.Errorf("no [Global] section in %v", cloudConfig)
	}
	provider, err := client.NewOpenStackClient(global, "csi-direct-iscsi", version)
	if err != nil {
		return nil, nil, err
	}
	bs, err := gopherstack.NewBlockStorageV3(provider, gophercloud.EndpointOpts{Region: global.Region, Availability: global.EndpointType})
	if err != nil {
		return nil, nil, err
	}
	return cloud, bs, nil
}
