package directiscsi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// iscsiadm: a session to that target already exists.
const exitSessExists = 15

var reHost = regexp.MustCompile(`^host(\d+)$`)

// sessionHosts returns the SCSI host number of every logged-in session to
// target. A failed session is skipped: a scan on its host blocks until the
// session recovers or times out.
func (n *Node) sessionHosts(target string) ([]int, error) {
	dir := filepath.Join(n.sys, "class/iscsi_session")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var hosts []int
	for _, e := range entries {
		sess := filepath.Join(dir, e.Name())
		if readTrim(filepath.Join(sess, "targetname")) != target {
			continue
		}
		if st := readTrim(filepath.Join(sess, "state")); st != "" && st != "LOGGED_IN" {
			continue
		}
		real, err := filepath.EvalSymlinks(sess)
		if err != nil {
			return nil, err
		}
		h := -1
		for _, part := range strings.Split(real, string(filepath.Separator)) {
			if m := reHost.FindStringSubmatch(part); m != nil {
				h, _ = strconv.Atoi(m[1])
			}
		}
		if h < 0 {
			return nil, fmt.Errorf("no SCSI host in the sysfs path of %s", real)
		}
		hosts = append(hosts, h)
	}
	sort.Ints(hosts)
	return hosts, nil
}

// ensureSessions returns the hosts of the sessions to target. With none it
// logs in from the node records cloud-init wrote. With fewer sessions than
// records (a portal was down at boot) it logs in again in the background, so
// this stage goes on with the paths it has instead of waiting out iscsiadm's
// login timeout; the next stages get the new path.
func (n *Node) ensureSessions(ctx context.Context, target string) ([]int, error) {
	hosts, err := n.sessionHosts(target)
	if err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		if err := n.login(ctx, target); err != nil {
			return nil, err
		}
		return n.sessionHosts(target)
	}
	if len(hosts) < n.portalRecords(target) && n.relogin.CompareAndSwap(false, true) {
		n.background(func() {
			defer n.relogin.Store(false)
			if err := n.login(context.Background(), target); err != nil {
				klog.Warningf("direct iSCSI: login to the missing portals of %s: %v", target, err)
			}
		})
	}
	return hosts, nil
}

func (n *Node) login(ctx context.Context, target string) error {
	if _, code, err := n.exec.run(ctx, "iscsiadm", "-m", "node", "-T", target, "--login"); err != nil && code != exitSessExists {
		return err
	}
	return nil
}

// portalRecords counts the node records of target, one per portal.
func (n *Node) portalRecords(target string) int {
	entries, _ := os.ReadDir(filepath.Join(n.host, "etc/iscsi/nodes", target))
	return len(entries)
}

// scanLUN adds H:0:0:lun on each session host. Only that LUN: a wildcard scan
// or iscsiadm --rescan would create a device for every volume of the cluster
// that is mapped to this node. A device already there is rescanned instead,
// so its WWID is that of the volume mapped at the LUN now, not of the one a
// failed unstage left behind.
func (n *Node) scanLUN(hosts []int, lun int) error {
	for _, h := range hosts {
		sdev := filepath.Join(n.sys, "class/scsi_device", hctl(h, lun))
		if exists(sdev) {
			if err := n.write(filepath.Join(sdev, "device/rescan"), "1"); err != nil {
				return fmt.Errorf("rescan %s: %w", hctl(h, lun), err)
			}
			continue
		}
		scan := filepath.Join(n.sys, "class/scsi_host", fmt.Sprintf("host%d", h), "scan")
		if err := n.write(scan, fmt.Sprintf("0 0 %d", lun)); err != nil {
			return fmt.Errorf("scan %s: %w", scan, err)
		}
	}
	return nil
}

type scsiPath struct {
	dev  string // sdX
	wwid string // multipath WWID
}

// waitPaths waits for the block device of H:0:0:lun on each host. Hosts whose
// device does not show up in time are left out.
func (n *Node) waitPaths(ctx context.Context, hosts []int, lun int) ([]scsiPath, error) {
	var paths []scsiPath
	for _, h := range hosts {
		dir := filepath.Join(n.sys, "class/scsi_device", hctl(h, lun), "device")
		var p scsiPath
		err := n.poll(ctx, n.pathTimeout, func() (bool, error) {
			blocks, err := os.ReadDir(filepath.Join(dir, "block"))
			if err != nil || len(blocks) == 0 {
				return false, nil
			}
			raw := readTrim(filepath.Join(dir, "wwid"))
			if raw == "" {
				return false, nil
			}
			wwid, err := mpathWWID(raw)
			if err != nil {
				return false, err
			}
			p = scsiPath{dev: blocks[0].Name(), wwid: wwid}
			return true, nil
		})
		if err != nil {
			klog.Warningf("direct iSCSI: path %s did not show up: %v", hctl(h, lun), err)
			continue
		}
		paths = append(paths, p)
	}
	for _, p := range paths[min(1, len(paths)):] {
		if p.wwid != paths[0].wwid {
			return nil, fmt.Errorf("paths of LUN %d disagree on the WWID: %s on %s, %s on %s", lun, paths[0].wwid, paths[0].dev, p.wwid, p.dev)
		}
	}
	return paths, nil
}

// mpathWWID turns the sysfs WWID into the one multipath names the map after
// (the udev ID_SERIAL of the disk).
func mpathWWID(sysfsWWID string) (string, error) {
	if hex, ok := strings.CutPrefix(sysfsWWID, "naa."); ok {
		return "3" + strings.ToLower(hex), nil
	}
	return "", fmt.Errorf("unsupported WWID %q", sysfsWWID)
}

// deletePath removes a SCSI disk that nothing holds anymore.
func (n *Node) deletePath(dev string) error {
	holders, _ := os.ReadDir(filepath.Join(n.sys, "block", dev, "holders"))
	if len(holders) > 0 {
		klog.V(2).Infof("direct iSCSI: keeping %s, still held by %s", dev, holders[0].Name())
		return nil
	}
	del := filepath.Join(n.sys, "block", dev, "device/delete")
	if !exists(del) {
		return nil
	}
	return n.write(del, "1")
}

func hctl(host, lun int) string { return fmt.Sprintf("%d:0:0:%d", host, lun) }

func readTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func writeSysfs(path, data string) error {
	return os.WriteFile(path, []byte(data), 0o644)
}

// poll calls f every pollInterval until it reports done, fails, or timeout passes.
func (n *Node) poll(ctx context.Context, timeout time.Duration, f func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		done, err := f()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(n.pollInterval):
		}
	}
}
