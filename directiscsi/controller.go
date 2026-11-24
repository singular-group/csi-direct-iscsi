package directiscsi

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/attachments"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
	cpoerrors "k8s.io/cloud-provider-openstack/pkg/util/errors"
)

// Controller is the controller-plugin side: it creates and converts volumes
// as multiattach and maps them, through Cinder attachments without an
// instance, to every node of the cluster.
type Controller struct {
	cfg       Config
	nodes     nodeSource
	cinderFor func(openstack.IOpenStack) (cinderAPI, bool)

	// One array-changing operation at a time: a drain converts PVs one by
	// one and the MSA never gets a burst of mappings.
	sem   chan struct{}
	locks keyedMutex

	mu    sync.Mutex
	nics  map[string]nicCheck // CSI node ID -> storage NIC
	types map[string]bool     // multiattach types seen in Cinder

	retypeTimeout time.Duration
	pollInterval  time.Duration
	negativeTTL   time.Duration
}

type nicCheck struct {
	ok bool
	at time.Time
}

// nodeSource maps the name of every node that registers the driver to its CSI node ID.
type nodeSource interface {
	nodes() map[string]string
}

type csiNodes struct {
	lister storagelisters.CSINodeLister
}

func (c csiNodes) nodes() map[string]string {
	list, err := c.lister.List(labels.Everything())
	if err != nil {
		klog.Errorf("direct iSCSI: list CSINodes: %v", err)
	}
	m := map[string]string{}
	for _, cn := range list {
		for _, d := range cn.Spec.Drivers {
			if d.Name == driverName && d.NodeID != "" {
				m[cn.Name] = d.NodeID
			}
		}
	}
	return m
}

var controller *Controller

// SetupController enables the controller side when the config files carry
// [DirectISCSI]; blockStorage is a Cinder v3 client of the same cloud.
func SetupController(cloudConfig []string, blockStorage *gophercloud.ServiceClient) error {
	cfg, ok, err := readConfig(cloudConfig)
	if err != nil || !ok {
		return err
	}
	if cfg.StorageNetwork == "" {
		return fmt.Errorf("[DirectISCSI] storage-network is required by the controller")
	}
	rc, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	factory := informers.NewSharedInformerFactory(cs, 0)
	inf := factory.Storage().V1().CSINodes()
	synced := inf.Informer().HasSynced
	factory.Start(make(chan struct{}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !cache.WaitForCacheSync(ctx.Done(), synced) {
		return fmt.Errorf("direct iSCSI: CSINode informer did not sync")
	}
	api := gopherCinder{blockStorage}
	controller = newController(cfg, csiNodes{inf.Lister()}, func(openstack.IOpenStack) (cinderAPI, bool) { return api, true })
	klog.Infof("direct iSCSI: controller on, storage network %s", cfg.StorageNetwork)
	return nil
}

func newController(cfg Config, nodes nodeSource, cinderFor func(openstack.IOpenStack) (cinderAPI, bool)) *Controller {
	return &Controller{
		cfg:           cfg,
		nodes:         nodes,
		cinderFor:     cinderFor,
		sem:           make(chan struct{}, 1),
		nics:          map[string]nicCheck{},
		types:         map[string]bool{},
		retypeTimeout: 120 * time.Second,
		pollInterval:  time.Second,
		negativeTTL:   60 * time.Second,
	}
}

// VolumeTypeForCreate returns the multiattach type for a new volume once the
// whole cluster can attach it directly; the requested type otherwise.
func VolumeTypeForCreate(ctx context.Context, cloud openstack.IOpenStack, volType string) string {
	c := controller
	if c == nil {
		return volType
	}
	target, ok := multiattachType(volType)
	if !ok {
		return volType
	}
	api, ok := c.cinderFor(cloud)
	if !ok {
		return volType
	}
	if ready, err := c.clusterReady(ctx, cloud); err != nil || !ready {
		if err != nil {
			klog.Warningf("direct iSCSI: creating as %s: %v", volType, err)
		}
		return volType
	}
	if exists, err := c.typeReady(ctx, api, target); err != nil || !exists {
		if err != nil {
			klog.Warningf("direct iSCSI: creating as %s: %v", volType, err)
		}
		return volType
	}
	return target
}

// ControllerPublish handles direct and convertible volumes; handled is false
// when upstream's Nova attach applies.
func ControllerPublish(ctx context.Context, cloud openstack.IOpenStack, volumeID, nodeID string) (resp *csi.ControllerPublishVolumeResponse, handled bool, err error) {
	c := controller
	if c == nil {
		return nil, false, nil
	}
	api, ok := c.cinderFor(cloud)
	if !ok {
		return nil, false, nil
	}
	defer c.locks.lock(volumeID)()
	vol, err := cloud.GetVolume(ctx, volumeID)
	if err != nil {
		if cpoerrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, status.Errorf(codes.Internal, "direct iSCSI: get volume %s: %v", volumeID, err)
	}
	if !c.isDirect(vol) {
		// By type alone: a failed retype leaves Cinder's multiattach flag set on
		// the old type, and that volume must still convert on the next try.
		target, ok := multiattachType(vol.VolumeType)
		if !ok {
			return nil, false, nil
		}
		ready, err := c.clusterReady(ctx, cloud)
		if err != nil {
			return nil, true, status.Errorf(codes.Unavailable, "direct iSCSI: %v", err)
		}
		if !ready {
			return nil, false, nil
		}
		exists, err := c.typeReady(ctx, api, target)
		if err != nil {
			return nil, true, status.Errorf(codes.Unavailable, "direct iSCSI: %v", err)
		}
		if !exists {
			return nil, false, nil
		}
		switch vol.Status {
		case openstack.VolumeAvailableStatus:
			if vol, err = c.convert(ctx, cloud, api, vol, target); err != nil {
				return nil, true, err
			}
		case openstack.VolumeInUseStatus:
			if attachedToServer(vol, nodeID) {
				return nil, false, nil
			}
			return nil, true, status.Errorf(codes.FailedPrecondition, "direct iSCSI: volume %s is attached through Nova to another server; it converts once detached", volumeID)
		default:
			return nil, true, status.Errorf(codes.Unavailable, "direct iSCSI: volume %s is %s", volumeID, vol.Status)
		}
	}
	resp, err = c.publish(ctx, cloud, api, vol, nodeID)
	return resp, true, err
}

// ControllerUnpublish reports a direct volume, whose unpublish is a no-op:
// it stays mapped to every node and the next node's NodeStage preempts.
func ControllerUnpublish(ctx context.Context, cloud openstack.IOpenStack, volumeID string) (direct bool, err error) {
	c := controller
	if c == nil {
		return false, nil
	}
	vol, err := cloud.GetVolume(ctx, volumeID)
	if err != nil {
		if cpoerrors.IsNotFound(err) {
			return false, nil
		}
		return false, status.Errorf(codes.Internal, "direct iSCSI: get volume %s: %v", volumeID, err)
	}
	return c.isDirect(vol), nil
}

// BeforeDeleteVolume removes the attachments of a direct volume, which
// Cinder would otherwise refuse to delete as in-use.
func BeforeDeleteVolume(ctx context.Context, cloud openstack.IOpenStack, volumeID string) error {
	c := controller
	if c == nil {
		return nil
	}
	api, ok := c.cinderFor(cloud)
	if !ok {
		return nil
	}
	vol, err := cloud.GetVolume(ctx, volumeID)
	if err != nil {
		if cpoerrors.IsNotFound(err) {
			return nil
		}
		return status.Errorf(codes.Internal, "direct iSCSI: get volume %s: %v", volumeID, err)
	}
	if !c.isDirect(vol) {
		return nil
	}
	list, err := api.listAttachments(ctx, volumeID)
	if err != nil {
		return status.Errorf(codes.Internal, "direct iSCSI: list attachments of %s: %v", volumeID, err)
	}
	for _, a := range list {
		if err := api.deleteAttachment(ctx, a.ID); err != nil && !cpoerrors.IsNotFound(err) {
			return status.Errorf(codes.Internal, "direct iSCSI: delete attachment %s of %s: %v", a.ID, volumeID, err)
		}
	}
	if len(list) > 0 {
		if err := cloud.WaitVolumeTargetStatus(ctx, volumeID, []string{openstack.VolumeAvailableStatus}); err != nil {
			return status.Errorf(codes.Internal, "direct iSCSI: volume %s after removing its attachments: %v", volumeID, err)
		}
	}
	return nil
}

// PublishedNodeIDs maps the attachments of a volume to CSI node IDs. A direct
// attachment has no server, only the host name of the node it maps.
func PublishedNodeIDs(atts []volumes.Attachment) []string {
	c := controller
	ids := make([]string, 0, len(atts))
	var nodes map[string]string
	for _, a := range atts {
		id := a.ServerID
		if id == "" && c != nil && a.HostName != "" {
			if nodes == nil {
				nodes = c.nodes.nodes()
			}
			if id = nodes[a.HostName]; id == "" {
				continue
			}
		}
		ids = append(ids, id)
	}
	return ids
}

// A direct volume's type is its legacy type's multiattach counterpart.
const multiattachSuffix = "-multiattach"

func (c *Controller) isDirect(v *volumes.Volume) bool {
	return v.Multiattach && strings.HasSuffix(v.VolumeType, multiattachSuffix)
}

// multiattachType returns the type a volume of type t converts to.
func multiattachType(t string) (string, bool) {
	if t == "" || strings.HasSuffix(t, multiattachSuffix) {
		return "", false
	}
	return t + multiattachSuffix, true
}

// clusterReady is true when every node that registers the driver has an
// address on the storage network, i.e. every node can log in to the MSA.
func (c *Controller) clusterReady(ctx context.Context, cloud openstack.IOpenStack) (bool, error) {
	nodes := c.nodes.nodes()
	if len(nodes) == 0 {
		return false, nil
	}
	for name, id := range nodes {
		ok, err := c.hasStorageNIC(ctx, cloud, id)
		if err != nil {
			return false, fmt.Errorf("server of node %s: %w", name, err)
		}
		if !ok {
			klog.V(2).Infof("direct iSCSI: node %s has no address on %s", name, c.cfg.StorageNetwork)
			return false, nil
		}
	}
	return true, nil
}

func (c *Controller) hasStorageNIC(ctx context.Context, cloud openstack.IOpenStack, nodeID string) (bool, error) {
	c.mu.Lock()
	e, cached := c.nics[nodeID]
	c.mu.Unlock()
	if cached && (e.ok || time.Since(e.at) < c.negativeTTL) {
		return e.ok, nil
	}
	ok := false
	srv, err := cloud.GetInstanceByID(ctx, nodeID)
	switch {
	case err == nil:
		addrs, _ := srv.Addresses[c.cfg.StorageNetwork].([]any)
		ok = len(addrs) > 0
	case !cpoerrors.IsNotFound(err):
		return false, err
	}
	c.mu.Lock()
	c.nics[nodeID] = nicCheck{ok: ok, at: time.Now()}
	c.mu.Unlock()
	return ok, nil
}

func (c *Controller) typeReady(ctx context.Context, api cinderAPI, name string) (bool, error) {
	c.mu.Lock()
	known := c.types[name]
	c.mu.Unlock()
	if known {
		return true, nil
	}
	exists, err := api.typeExists(ctx, name)
	if err != nil || !exists {
		return false, err
	}
	c.mu.Lock()
	c.types[name] = true
	c.mu.Unlock()
	return true, nil
}

// convert retypes an available legacy volume to its multiattach type. The
// Cinder backend must do that without touching the array (README.md).
func (c *Controller) convert(ctx context.Context, cloud openstack.IOpenStack, api cinderAPI, vol *volumes.Volume, target string) (*volumes.Volume, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	deadline := time.Now().Add(c.retypeTimeout)
	if err := api.changeType(ctx, vol.ID, target); err != nil {
		return nil, status.Errorf(codes.Unavailable, "direct iSCSI: retype %s to %s: %v", vol.ID, target, err)
	}
	for {
		v, err := cloud.GetVolume(ctx, vol.ID)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "direct iSCSI: get volume %s: %v", vol.ID, err)
		}
		switch {
		case v.Status == openstack.VolumeAvailableStatus && v.VolumeType == target && v.Multiattach:
			return v, nil
		case v.Status == openstack.VolumeAvailableStatus && v.VolumeType != target:
			return nil, status.Errorf(codes.Unavailable, "direct iSCSI: retype of %s to %s failed, it is still %s", vol.ID, target, v.VolumeType)
		case strings.HasPrefix(v.Status, "error"):
			return nil, status.Errorf(codes.Internal, "direct iSCSI: volume %s is %s after the retype to %s", vol.ID, v.Status, target)
		}
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.Unavailable, "direct iSCSI: retype of %s to %s not done after %s (status %s)", vol.ID, target, c.retypeTimeout, v.Status)
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(c.pollInterval):
		}
	}
}

func (c *Controller) publish(ctx context.Context, cloud openstack.IOpenStack, api cinderAPI, vol *volumes.Volume, nodeID string) (*csi.ControllerPublishVolumeResponse, error) {
	nodes := c.nodes.nodes()
	self := ""
	for name, id := range nodes {
		if id == nodeID {
			self = name
		}
	}
	if self == "" {
		return nil, status.Errorf(codes.Unavailable, "direct iSCSI: no CSINode registers node ID %s", nodeID)
	}
	lun, err := c.ensureAttachments(ctx, cloud, api, vol, self, nodes)
	if err != nil {
		return nil, err
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{
		ctxDirect:    "true",
		ctxLUN:       strconv.Itoa(lun),
		ctxTargetIQN: c.cfg.TargetIQN,
	}}, nil
}

// ensureAttachments maps the volume to every node that has no attachment
// yet, the publishing node first, and returns that node's LUN. Another node
// failing is left for the next publish of the volume.
func (c *Controller) ensureAttachments(ctx context.Context, cloud openstack.IOpenStack, api cinderAPI, vol *volumes.Volume, self string, nodes map[string]string) (int, error) {
	stuck := vol.Status != openstack.VolumeAvailableStatus && vol.Status != openstack.VolumeInUseStatus
	have, err := attachedHosts(vol)
	if err != nil {
		return 0, err
	}
	// Before mapping a node again, finish or remove what a failed create or
	// complete left: an attachment the volume does not list as attached. A
	// second create for its node could map the volume at a second LUN.
	missing := false
	for name := range nodes {
		if _, ok := have[name]; !ok {
			missing = true
		}
	}
	if stuck || missing {
		n, err := c.resumeAttachments(ctx, api, vol.ID)
		if err != nil {
			return 0, status.Errorf(codes.Unavailable, "direct iSCSI: %v", err)
		}
		if stuck || n > 0 {
			if vol, err = cloud.GetVolume(ctx, vol.ID); err != nil {
				return 0, status.Errorf(codes.Unavailable, "direct iSCSI: get volume: %v", err)
			}
			if have, err = attachedHosts(vol); err != nil {
				return 0, err
			}
		}
	}
	order := []string{self}
	for name := range nodes {
		if name != self {
			order = append(order, name)
		}
	}
	sort.Strings(order[1:])
	for _, name := range order {
		if _, ok := have[name]; ok {
			continue
		}
		a, err := c.attach(ctx, api, vol.ID, name)
		if err != nil {
			if name == self {
				return 0, status.Errorf(codes.Unavailable, "direct iSCSI: attach %s for node %s: %v", vol.ID, name, err)
			}
			klog.Warningf("direct iSCSI: attach %s for node %s, left for its next publish: %v", vol.ID, name, err)
			continue
		}
		have[name] = a.ID
	}
	a, err := api.getAttachment(ctx, have[self])
	if err != nil {
		return 0, status.Errorf(codes.Unavailable, "direct iSCSI: get attachment %s: %v", have[self], err)
	}
	lun, ok := targetLUN(a.ConnectionInfo)
	if !ok {
		return 0, status.Errorf(codes.Internal, "direct iSCSI: attachment %s of %s has no target_lun", a.ID, vol.ID)
	}
	return lun, nil
}

// attachedHosts maps each node holding an attachment of the volume to it.
func attachedHosts(vol *volumes.Volume) (map[string]string, error) {
	have := map[string]string{}
	for _, a := range vol.Attachments {
		if a.ServerID != "" {
			continue
		}
		if a.HostName == "" {
			return nil, status.Errorf(codes.FailedPrecondition, "direct iSCSI: Cinder hides the host of attachment %s; the cloud-config user needs the admin role", a.AttachmentID)
		}
		have[a.HostName] = a.AttachmentID
	}
	return have, nil
}

// resumeAttachments finishes what a crash between create and complete left:
// an attachment that is mapped is completed, one that is not is removed. It
// returns how many it touched.
func (c *Controller) resumeAttachments(ctx context.Context, api cinderAPI, volumeID string) (int, error) {
	list, err := api.listAttachments(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, a := range list {
		switch a.Status {
		case "attaching":
			err = api.completeAttachment(ctx, a.ID)
		case "reserved", "error_attaching":
			err = api.deleteAttachment(ctx, a.ID)
		default:
			continue
		}
		if err != nil {
			return done, fmt.Errorf("resume attachment %s (%s) of %s: %w", a.ID, a.Status, volumeID, err)
		}
		done++
		klog.Infof("direct iSCSI: resumed attachment %s (%s) of %s", a.ID, a.Status, volumeID)
	}
	return done, nil
}

func (c *Controller) attach(ctx context.Context, api cinderAPI, volumeID, node string) (*attachments.Attachment, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	a, err := api.createAttachment(ctx, volumeID, c.connector(node))
	if err != nil {
		return nil, err
	}
	if err := api.completeAttachment(ctx, a.ID); err != nil {
		return nil, err
	}
	return a, nil
}

// connector is what Nova's os-brick would send for the node, minus the instance.
func (c *Controller) connector(node string) map[string]any {
	return map[string]any{
		"initiator":       c.cfg.InitiatorPrefix + node,
		"host":            node,
		"platform":        "x86_64",
		"os_type":         "linux2",
		"multipath":       true,
		"do_local_attach": false,
	}
}

func (c *Controller) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (c *Controller) release() { <-c.sem }

func attachedToServer(v *volumes.Volume, serverID string) bool {
	for _, a := range v.Attachments {
		if a.ServerID == serverID {
			return true
		}
	}
	return false
}

// targetLUN reads target_lun from connection_info, which Cinder stores
// flattened (driver data at the top level); JSON gives numbers as float64.
func targetLUN(info map[string]any) (int, bool) {
	l, ok := info["target_lun"].(float64)
	return int(l), ok && l >= 0
}
