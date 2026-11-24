// Package proxy serves CSI in front of upstream's Cinder CSI plugin: calls for
// direct iSCSI volumes are answered here through directiscsi, every other call
// is forwarded to upstream's socket unchanged.
package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

// Upstream holds the clients of upstream's plugin socket.
type Upstream struct {
	conn       *grpc.ClientConn
	Identity   csi.IdentityClient
	Controller csi.ControllerClient
	Node       csi.NodeClient
}

// Dial connects to upstream's socket lazily: a call made before upstream
// listens fails Unavailable and the sidecar retries it.
func Dial(endpoint string) (*Upstream, error) {
	path, err := socketPath(endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Upstream{
		conn:       conn,
		Identity:   csi.NewIdentityClient(conn),
		Controller: csi.NewControllerClient(conn),
		Node:       csi.NewNodeClient(conn),
	}, nil
}

// Close closes the connection to upstream.
func (u *Upstream) Close() error { return u.conn.Close() }

// Listen opens endpoint's unix socket, removing one a previous run left.
func Listen(endpoint string) (net.Listener, error) {
	path, err := socketPath(endpoint)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return net.Listen("unix", path)
}

func socketPath(endpoint string) (string, error) {
	path, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok || path == "" {
		return "", fmt.Errorf("endpoint %q is not unix://<path>", endpoint)
	}
	// unix:///csi/csi.sock and unix://csi/csi.sock both name /csi/csi.sock, as upstream reads them.
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path, nil
}

// LogErrors logs every call that fails, with its method.
func LogErrors(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("%s: %v", info.FullMethod, err)
	}
	return resp, err
}

// Identity forwards the Identity service: driver name, capabilities and probe
// are upstream's.
type Identity struct {
	csi.UnimplementedIdentityServer
	Up csi.IdentityClient
}

func (s *Identity) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return s.Up.GetPluginInfo(ctx, req)
}

func (s *Identity) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return s.Up.GetPluginCapabilities(ctx, req)
}

func (s *Identity) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return s.Up.Probe(ctx, req)
}
