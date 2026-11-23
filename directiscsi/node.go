package directiscsi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// Node is the node-plugin side: it logs in, scans the one LUN, builds the
// multipath map and holds the persistent reservation of a direct volume.
type Node struct {
	cfg      Config
	nodeName string
	key      uint64
	exec     executor
	sys      string // sysfs
	dev      string // the host /dev, mounted by the DaemonSet
	host     string // the host root, mounted into the node plugin
	stateDir string
	write    func(path, data string) error

	pathTimeout   time.Duration
	pollInterval  time.Duration
	retryInterval time.Duration

	// background runs the login to a portal missing at boot (go f() outside tests).
	background func(f func())
	relogin    atomic.Bool

	locks keyedMutex
}

// state is what NodeStage leaves for unstage, block publish and expand.
type state struct {
	WWID      string `json:"wwid"`
	LUN       int    `json:"lun"`
	TargetIQN string `json:"targetIQN"`
}

var node *Node

// SetupNode enables the node side when the config files carry [DirectISCSI].
func SetupNode(cloudConfig []string) error {
	cfg, ok, err := readConfig(cloudConfig)
	if err != nil || !ok {
		return err
	}
	name := os.Getenv("NODE_NAME")
	if name == "" {
		return fmt.Errorf("direct iSCSI: NODE_NAME is not set")
	}
	n, err := newNode(cfg, name)
	if err != nil {
		return err
	}
	node = n
	klog.Infof("direct iSCSI: node %s, initiator %s%s, PR key %s", name, cfg.InitiatorPrefix, name, keyArg(n.key))
	return nil
}

func newNode(cfg Config, name string) (*Node, error) {
	key, err := prKey(name)
	if err != nil {
		return nil, err
	}
	return &Node{
		cfg:           cfg,
		nodeName:      name,
		key:           key,
		exec:          hostExec{root: "/host"},
		sys:           "/sys",
		dev:           "/dev",
		host:          "/host",
		stateDir:      "/var/lib/kubelet/plugins/" + driverName + "/direct-iscsi",
		write:         writeSysfs,
		pathTimeout:   15 * time.Second,
		pollInterval:  100 * time.Millisecond,
		retryInterval: time.Second,
		background:    func(f func()) { go f() },
	}, nil
}

// NodeStage attaches a direct volume and returns its multipath device.
func NodeStage(ctx context.Context, volumeID string, publishContext map[string]string) (string, error) {
	if node == nil {
		return "", status.Error(codes.FailedPrecondition, "direct iSCSI volume, but this node plugin has no [DirectISCSI] config")
	}
	return node.stage(ctx, volumeID, publishContext)
}

// NodeUnstage detaches a volume NodeStage attached; a no-op for any other.
func NodeUnstage(ctx context.Context, volumeID string) error {
	if node == nil {
		return nil
	}
	return node.unstage(ctx, volumeID)
}

// NodeExpand grows the paths and map of a direct volume; direct is false for any other.
func NodeExpand(ctx context.Context, volumeID string, size int64) (direct bool, err error) {
	if node == nil {
		return false, nil
	}
	return node.expand(ctx, volumeID, size)
}

// BlockDevicePath returns the multipath device of a staged direct volume, "" for any other.
func BlockDevicePath(volumeID string) (string, error) {
	if node == nil {
		return "", nil
	}
	st, ok, err := node.readState(volumeID)
	if err != nil || !ok {
		return "", err
	}
	return node.mapperPath(st.WWID), nil
}

func (n *Node) stage(ctx context.Context, volumeID string, pc map[string]string) (string, error) {
	defer n.locks.lock(volumeID)()
	if !validVolumeID(volumeID) {
		return "", status.Errorf(codes.InvalidArgument, "direct iSCSI: bad volume ID %q", volumeID)
	}
	lun, err := strconv.Atoi(pc[ctxLUN])
	if err != nil || lun < 0 {
		return "", status.Errorf(codes.InvalidArgument, "direct iSCSI: bad LUN %q in the publish context", pc[ctxLUN])
	}
	target := pc[ctxTargetIQN]
	if target == "" {
		return "", status.Error(codes.InvalidArgument, "direct iSCSI: no target IQN in the publish context")
	}
	if err := n.checkInitiator(); err != nil {
		return "", status.Error(codes.FailedPrecondition, err.Error())
	}

	hosts, err := n.ensureSessions(ctx, target)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "direct iSCSI: login to %s: %v", target, err)
	}
	if len(hosts) == 0 {
		return "", status.Errorf(codes.Unavailable, "direct iSCSI: no logged-in session to %s", target)
	}

	if err := n.scanLUN(hosts, lun); err != nil {
		return "", status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	paths, err := n.waitPaths(ctx, hosts, lun)
	if err != nil {
		return "", status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	if len(paths) == 0 {
		return "", status.Errorf(codes.Unavailable, "direct iSCSI: LUN %d did not show up on any session", lun)
	}
	other, err := n.stagedAs(paths[0].wwid, volumeID)
	if err != nil {
		return "", status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	if other != "" {
		return "", status.Errorf(codes.FailedPrecondition, "direct iSCSI: LUN %d is %s, which volume %s holds on this node", lun, paths[0].wwid, other)
	}

	mapper, err := n.assembleMap(ctx, paths)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "direct iSCSI: %v", err)
	}

	devs := make([]string, len(paths))
	for i, p := range paths {
		devs[i] = "/dev/" + p.dev
	}
	if err := n.takeReservation(ctx, devs); err != nil {
		return "", status.Errorf(codes.Unavailable, "direct iSCSI: persistent reservation: %v", err)
	}

	if err := n.writeState(volumeID, state{WWID: paths[0].wwid, LUN: lun, TargetIQN: target}); err != nil {
		return "", status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	return mapper, nil
}

func (n *Node) unstage(ctx context.Context, volumeID string) error {
	defer n.locks.lock(volumeID)()
	st, ok, err := n.readState(volumeID)
	if err != nil {
		return status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	if !ok {
		return nil
	}
	devs, err := n.devicesOf(st)
	if err != nil {
		return status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	mapper := n.mapperPath(st.WWID)
	if exists(mapper) {
		if _, _, err := n.exec.run(ctx, "blockdev", "--flushbufs", mapper); err != nil {
			// A fenced node cannot write what it still has; that data is lost
			// either way, and retrying would wedge the unstage.
			if !n.fencedOut(ctx, pathsOf(devs)) {
				return status.Errorf(codes.Internal, "direct iSCSI: %v", err)
			}
			klog.Warningf("direct iSCSI: volume %s: flush failed, another node holds the reservation: %v", volumeID, err)
		}
	}
	if err := n.dropReservation(ctx, pathsOf(devs)); err != nil {
		return status.Errorf(codes.Internal, "direct iSCSI: persistent reservation: %v", err)
	}
	if err := n.flushMap(ctx, st.WWID); err != nil {
		return status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	for _, d := range devs {
		if err := n.deletePath(d); err != nil {
			return status.Errorf(codes.Internal, "direct iSCSI: delete %s: %v", d, err)
		}
	}
	if err := os.Remove(n.statePath(volumeID)); err != nil && !os.IsNotExist(err) {
		return status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	return nil
}

func (n *Node) expand(ctx context.Context, volumeID string, size int64) (bool, error) {
	st, ok, err := n.readState(volumeID)
	if err != nil {
		return true, status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	if !ok {
		return false, nil
	}
	slaves, err := n.mapSlaves(st.WWID)
	if err != nil {
		return true, status.Errorf(codes.Internal, "direct iSCSI: %v", err)
	}
	if len(slaves) == 0 {
		return true, status.Errorf(codes.Internal, "direct iSCSI: multipath map %s of volume %s is gone", st.WWID, volumeID)
	}
	if err := n.resizeMap(ctx, st.WWID, slaves, size); err != nil {
		return true, status.Errorf(codes.Internal, "direct iSCSI: resize %s: %v", st.WWID, err)
	}
	return true, nil
}

// devicesOf returns the sdX paths of a staged volume: the map's members, or,
// once the map is gone, the disks still present for its LUN.
func (n *Node) devicesOf(st state) ([]string, error) {
	slaves, err := n.mapSlaves(st.WWID)
	if err != nil || slaves != nil {
		return slaves, err
	}
	hosts, err := n.sessionHosts(st.TargetIQN)
	if err != nil {
		return nil, err
	}
	var devs []string
	for _, h := range hosts {
		blocks, _ := os.ReadDir(filepath.Join(n.sys, "class/scsi_device", hctl(h, st.LUN), "device/block"))
		for _, b := range blocks {
			devs = append(devs, b.Name())
		}
	}
	return devs, nil
}

// checkInitiator refuses a node whose IQN is not the one Cinder mapped volumes to.
func (n *Node) checkInitiator() error {
	want := n.cfg.InitiatorPrefix + n.nodeName
	path := filepath.Join(n.host, "etc/iscsi/initiatorname.iscsi")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("node %s is not provisioned for direct iSCSI: %v", n.nodeName, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if v, ok := strings.CutPrefix(line, "InitiatorName="); ok {
			if v != want {
				return fmt.Errorf("node %s is not provisioned for direct iSCSI: initiator is %q, want %q", n.nodeName, v, want)
			}
			return nil
		}
	}
	return fmt.Errorf("node %s is not provisioned for direct iSCSI: no InitiatorName in %s", n.nodeName, path)
}

// reVolumeID keeps a volume ID usable as a file name (Cinder's are UUIDs).
var reVolumeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*$`)

func validVolumeID(id string) bool { return reVolumeID.MatchString(id) }

func (n *Node) statePath(volumeID string) string {
	return filepath.Join(n.stateDir, volumeID+".json")
}

// readState reports no state for an ID that cannot name a state file.
func (n *Node) readState(volumeID string) (state, bool, error) {
	var st state
	if !validVolumeID(volumeID) {
		return st, false, nil
	}
	b, err := os.ReadFile(n.statePath(volumeID))
	if err != nil {
		if os.IsNotExist(err) {
			return st, false, nil
		}
		return st, false, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, false, fmt.Errorf("state of volume %s: %w", volumeID, err)
	}
	return st, true, nil
}

// stagedAs returns the other volume whose state file names wwid, "" if none:
// a LUN remapped to another volume must not be staged as this one.
func (n *Node) stagedAs(wwid, volumeID string) (string, error) {
	files, err := filepath.Glob(filepath.Join(n.stateDir, "*.json"))
	if err != nil {
		return "", err
	}
	for _, f := range files {
		id := strings.TrimSuffix(filepath.Base(f), ".json")
		if id == volumeID {
			continue
		}
		st, ok, err := n.readState(id)
		if err != nil {
			return "", err
		}
		if ok && st.WWID == wwid {
			return id, nil
		}
	}
	return "", nil
}

func (n *Node) writeState(volumeID string, st state) error {
	if err := os.MkdirAll(n.stateDir, 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := n.statePath(volumeID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, n.statePath(volumeID))
}

// keyedMutex serializes the calls for one volume.
type keyedMutex struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*sync.Mutex{}
	}
	l, ok := k.m[key]
	if !ok {
		l = &sync.Mutex{}
		k.m[key] = l
	}
	k.mu.Unlock()
	l.Lock()
	return l.Unlock
}
