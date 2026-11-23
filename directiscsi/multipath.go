package directiscsi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// assembleMap builds the dm-multipath map of the paths and returns its device.
func (n *Node) assembleMap(ctx context.Context, paths []scsiPath) (string, error) {
	wwid := paths[0].wwid
	// find_multipaths strict only builds maps for WWIDs in /etc/multipath/wwids.
	if _, _, err := n.exec.run(ctx, "multipath", "-a", "/dev/"+paths[0].dev); err != nil {
		return "", err
	}
	for _, p := range paths {
		// Fails when udev already handed the path to multipathd.
		if _, _, err := n.exec.run(ctx, "multipathd", "add", "path", p.dev); err != nil {
			klog.V(4).Infof("direct iSCSI: multipathd add path %s: %v", p.dev, err)
		}
	}
	mapper := n.mapperPath(wwid)
	if err := n.poll(ctx, n.pathTimeout, func() (bool, error) { return exists(mapper), nil }); err != nil {
		return "", fmt.Errorf("multipath map %s: %w", wwid, err)
	}
	slaves, err := n.mapSlaves(wwid)
	if err != nil {
		return "", err
	}
	var active []string
	for _, s := range slaves {
		if readTrim(filepath.Join(n.sys, "block", s, "device/state")) == "running" {
			active = append(active, s)
		}
	}
	switch {
	case len(active) == 0:
		return "", fmt.Errorf("multipath map %s has no running path (paths %v)", wwid, slaves)
	case len(active) < 2:
		klog.Warningf("direct iSCSI: multipath map %s has %d running path(s) of %v", wwid, len(active), slaves)
	}
	return mapper, nil
}

func (n *Node) mapperPath(wwid string) string { return filepath.Join(n.dev, "mapper", wwid) }

// mapSlaves returns the sdX paths under the map, nil when the map is gone.
func (n *Node) mapSlaves(wwid string) ([]string, error) {
	dm, err := n.mapDM(wwid)
	if err != nil || dm == "" {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(n.sys, "block", dm, "slaves"))
	if err != nil {
		return nil, err
	}
	var slaves []string
	for _, e := range entries {
		slaves = append(slaves, e.Name())
	}
	return slaves, nil
}

// mapDM returns dm-N of the map, "" when it does not exist. udev's
// /dev/mapper/<wwid> is a symlink to it.
func (n *Node) mapDM(wwid string) (string, error) {
	target, err := os.Readlink(n.mapperPath(wwid))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return filepath.Base(target), nil
}

// flushMap removes the map; a map still settling after the unmount can refuse once or twice.
func (n *Node) flushMap(ctx context.Context, wwid string) error {
	if _, _, err := n.exec.run(ctx, "udevadm", "settle"); err != nil {
		klog.Warningf("direct iSCSI: udevadm settle: %v", err)
	}
	var err error
	for i := 0; i < 3; i++ {
		if !exists(n.mapperPath(wwid)) {
			err = nil
			break
		}
		if _, _, err = n.exec.run(ctx, "multipath", "-f", wwid); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(n.retryInterval):
		}
	}
	if err != nil {
		return err
	}
	if _, _, err := n.exec.run(ctx, "multipath", "-w", wwid); err != nil {
		klog.Warningf("direct iSCSI: multipath -w %s: %v", wwid, err)
	}
	return nil
}

// resizeMap grows the paths and the map to the size Cinder gave the volume.
func (n *Node) resizeMap(ctx context.Context, wwid string, slaves []string, size int64) error {
	for _, s := range slaves {
		if err := n.write(filepath.Join(n.sys, "block", s, "device/rescan"), "1"); err != nil {
			return err
		}
	}
	if _, _, err := n.exec.run(ctx, "multipathd", "resize", "map", wwid); err != nil {
		return err
	}
	if size <= 0 {
		return nil
	}
	dm, err := n.mapDM(wwid)
	if err != nil {
		return err
	}
	return n.poll(ctx, n.pathTimeout, func() (bool, error) {
		sectors, err := strconv.ParseInt(readTrim(filepath.Join(n.sys, "block", dm, "size")), 10, 64)
		if err != nil {
			return false, nil
		}
		return sectors*512 >= size, nil
	})
}

func pathsOf(slaves []string) []string {
	devs := make([]string, len(slaves))
	for i, s := range slaves {
		devs[i] = "/dev/" + strings.TrimPrefix(s, "/dev/")
	}
	return devs
}
