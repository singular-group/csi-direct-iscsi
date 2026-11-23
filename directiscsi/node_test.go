package directiscsi

import (
	"context"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testWWID = "naa.600c0ff000db9a3c1f0a2b6501000000"
	testMap  = "3600c0ff000db9a3c1f0a2b6501000000"
)

func pc(h *fakeHost, lun string) map[string]string {
	return map[string]string{ctxDirect: "true", ctxLUN: lun, ctxTargetIQN: h.target}
}

// twoPathHost is a node logged in to both MSA portals, with LUN 7 (the
// volume) and LUN 9 (another volume of the cluster) mapped to it.
func twoPathHost(t *testing.T, name string, lu *fakeLU) *fakeHost {
	h := newFakeHost(t, name)
	h.array[7] = fakeArrayLUN{wwid: testWWID, lu: lu}
	h.array[9] = fakeArrayLUN{wwid: "naa.600c0ff000db9a3c99999999", lu: newFakeLU()}
	h.addSession(1, 3, h.target, "LOGGED_IN")
	h.addSession(2, 4, h.target, "LOGGED_IN")
	return h
}

func code(err error) codes.Code { return status.Code(err) }

func TestStageScansOnlyItsLUNAndReserves(t *testing.T) {
	lu := newFakeLU()
	h := twoPathHost(t, "node-1", lu)
	h.addSession(3, 5, "iqn.other:target", "LOGGED_IN") // another target: never scanned
	n := h.node(t)

	dev, err := n.stage(context.Background(), "vol-1", pc(h, "7"))
	if err != nil {
		t.Fatal(err)
	}
	if want := h.path("dev/mapper/" + testMap); dev != want {
		t.Fatalf("device %s, want %s", dev, want)
	}
	scans := h.wrote("/scan")
	if len(scans) != 2 || !strings.Contains(scans[0], "host3/scan=0 0 7") || !strings.Contains(scans[1], "host4/scan=0 0 7") {
		t.Fatalf("scans %v, want LUN 7 on host3 and host4 only", scans)
	}
	if exists(h.path("sys/class/scsi_device/3:0:0:9")) {
		t.Fatal("LUN 9 of another volume was scanned")
	}
	if got := h.called("iscsiadm"); len(got) != 0 {
		t.Fatalf("logged in although sessions exist: %v", got)
	}
	if got := h.called("multipath -a"); len(got) != 1 {
		t.Fatalf("multipath -a calls %v", got)
	}
	keys := lu.keys()
	if len(keys) != 2 || keys[0] != n.key || keys[1] != n.key {
		t.Fatalf("registered keys %x, want this node on both paths", keys)
	}
	if lu.holder == "" || lu.resKey != n.key || lu.resType != 5 {
		t.Fatalf("reservation %+v", lu)
	}
	if !lu.write("node-1/sdb") || lu.write("node-2/sdb") {
		t.Fatal("owner must write, others must get a reservation conflict")
	}
	st, ok, err := n.readState("vol-1")
	if err != nil || !ok || st.WWID != testMap || st.LUN != 7 || st.TargetIQN != h.target {
		t.Fatalf("state %+v %v %v", st, ok, err)
	}
	node = n
	defer func() { node = nil }()
	if p, err := BlockDevicePath("vol-1"); err != nil || p != dev {
		t.Fatalf("BlockDevicePath %q %v", p, err)
	}
	if p, err := BlockDevicePath("legacy"); err != nil || p != "" {
		t.Fatalf("BlockDevicePath of a legacy volume %q %v", p, err)
	}

	// Kubelet retries NodeStage: nothing is scanned or preempted again.
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.wrote("/scan"); len(got) != 2 {
		t.Fatalf("restage scanned again: %v", got)
	}
	if got := h.called("sg_persist --no-inquiry --out --preempt"); len(got) != 0 {
		t.Fatalf("restage preempted: %v", got)
	}
}

func TestMovePreemptsAndOldNodeUnstages(t *testing.T) {
	lu := newFakeLU()
	a := twoPathHost(t, "node-1", lu)
	b := twoPathHost(t, "node-2", lu)
	na, nb := a.node(t), b.node(t)
	ctx := context.Background()
	if _, err := na.stage(ctx, "vol-1", pc(a, "7")); err != nil {
		t.Fatal(err)
	}
	// The old node is gone without unstaging (node failure): B fences it.
	if _, err := nb.stage(ctx, "vol-1", pc(b, "7")); err != nil {
		t.Fatal(err)
	}
	pre := b.called("sg_persist --no-inquiry --out --preempt")
	if len(pre) != 1 || !strings.Contains(pre[0], "--param-sark="+keyArg(na.key)) || strings.Contains(strings.Join(b.calls, " "), "preempt-abort") {
		t.Fatalf("preempts %v, want one plain PREEMPT of %s", pre, keyArg(na.key))
	}
	for _, k := range lu.keys() {
		if k != nb.key {
			t.Fatalf("keys %x after the move, want only %s", lu.keys(), keyArg(nb.key))
		}
	}
	if lu.write("node-1/sdb") || !lu.write("node-2/sdc") {
		t.Fatal("the old node can still write, or the new one cannot")
	}
	// The old node comes back and unstages: it no longer holds the
	// reservation, so it only unregisters, which hits a unit attention and
	// then RESERVATION CONFLICT: the state it wanted anyway.
	a.calls = nil
	if err := na.unstage(ctx, "vol-1"); err != nil {
		t.Fatal(err)
	}
	if got := a.called("sg_persist --no-inquiry --out --release"); len(got) != 0 {
		t.Fatalf("a preempted node sent RELEASE: %v", got)
	}
	if lu.resKey != nb.key {
		t.Fatal("the old node's unstage dropped the new node's reservation")
	}
}

func TestStagePreemptsEveryForeignKey(t *testing.T) {
	lu := newFakeLU()
	lu.regs["node-2/sdb"] = 0x22
	lu.regs["node-3/sdb"] = 0x33
	lu.holder, lu.resKey, lu.resType = "node-2/sdb", 0x22, 5
	h := twoPathHost(t, "node-1", lu)
	n := h.node(t)
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.called("sg_persist --no-inquiry --out --preempt"); len(got) != 2 {
		t.Fatalf("preempts %v, want the holder and the other registrant", got)
	}
	if lu.write("node-3/sdb") {
		t.Fatal("a stale registrant can still write")
	}
}

func TestUnstageTearsDownInOrder(t *testing.T) {
	lu := newFakeLU()
	h := twoPathHost(t, "node-1", lu)
	h.flushFail = 2
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	h.calls = nil
	if err := n.unstage(ctx, "vol-1"); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, c := range h.calls {
		f := strings.Fields(c)
		step := f[0] + " " + f[1]
		if f[0] == "sg_persist" {
			step = f[0] + " " + f[3]
		}
		if len(order) == 0 || order[len(order)-1] != step {
			order = append(order, step)
		}
	}
	want := "blockdev --flushbufs,sg_persist --read-reservation,sg_persist --release,sg_persist --register,udevadm settle,multipath -f,multipath -w"
	if strings.Join(order, ",") != want {
		t.Fatalf("teardown order\n %s\nwant\n %s", strings.Join(order, ","), want)
	}
	if got := h.wrote("/device/delete"); len(got) != 2 {
		t.Fatalf("deleted paths %v", got)
	}
	if len(lu.keys()) != 0 || lu.holder != "" {
		t.Fatalf("reservation left behind: %+v", lu)
	}
	if exists(n.statePath("vol-1")) {
		t.Fatal("state file left behind")
	}
	// Idempotent: a second unstage, or one for a legacy volume, does nothing.
	h.calls = nil
	if err := n.unstage(ctx, "vol-1"); err != nil || len(h.calls) != 0 {
		t.Fatalf("second unstage: %v %v", err, h.calls)
	}
}

func TestUnstageKeepsStateWhenTheMapStaysBusy(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	h.flushFail = 5
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if err := n.unstage(ctx, "vol-1"); err == nil {
		t.Fatal("unstage succeeded with the map still there")
	}
	if !exists(n.statePath("vol-1")) {
		t.Fatal("state file removed although the map is still there")
	}
	if got := h.called("multipath -f"); len(got) != 3 {
		t.Fatalf("multipath -f tried %d times, want 3", len(got))
	}
	if got := h.wrote("/device/delete"); len(got) != 0 {
		t.Fatalf("paths deleted under a live map: %v", got)
	}
}

// After a reboot (scan=manual) or a partial unstage the state outlives the map.
func TestUnstageWithTheMapGone(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	h.removeMap(testMap)
	if err := n.unstage(ctx, "vol-1"); err != nil || exists(n.statePath("vol-1")) {
		t.Fatalf("unstage: %v", err)
	}
}

func TestStageRefusesAWrongInitiator(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	h.writeFile("host/etc/iscsi/initiatorname.iscsi", "InitiatorName=iqn.2004-10.com.ubuntu:01:abc\n")
	_, err := h.node(t).stage(context.Background(), "vol-1", pc(h, "7"))
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("err %v, want FailedPrecondition", err)
	}
	os.Remove(h.path("host/etc/iscsi/initiatorname.iscsi"))
	_, err = h.node(t).stage(context.Background(), "vol-1", pc(h, "7"))
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("err %v, want FailedPrecondition", err)
	}
	if len(h.calls) != 0 {
		t.Fatalf("ran %v before checking the initiator", h.calls)
	}
}

func TestStageLogsInWhenNoSession(t *testing.T) {
	h := newFakeHost(t, "node-1")
	h.array[7] = fakeArrayLUN{wwid: testWWID, lu: newFakeLU()}
	h.addSession(1, 3, h.target, "FAILED") // a failed session is not used
	h.loginTo = []int{6, 7}
	if _, err := h.node(t).stage(context.Background(), "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.called("iscsiadm"); len(got) != 1 || got[0] != "iscsiadm -m node -T "+h.target+" --login" {
		t.Fatalf("iscsiadm calls %v", got)
	}
	if got := h.wrote("/scan"); len(got) != 2 || strings.Contains(strings.Join(got, ","), "host3") {
		t.Fatalf("scans %v", got)
	}
}

func TestStageWithOnePathDown(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	// Host 4's scan does not produce a disk (its port lost the LUN).
	h.array[7] = fakeArrayLUN{wwid: testWWID, lu: h.array[7].lu}
	n := h.node(t)
	orig := n.write
	n.write = func(p, data string) error {
		if strings.Contains(p, "host4/scan") {
			return nil
		}
		return orig(p, data)
	}
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.called("sg_persist --no-inquiry --out --register-ignore"); len(got) != 1 {
		t.Fatalf("registered on %v, want the one path", got)
	}
}

func TestStageRejectsMismatchedPaths(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	n := h.node(t)
	orig := n.write
	n.write = func(p, data string) error {
		err := orig(p, data)
		if strings.Contains(p, "host4/scan") {
			h.writeFile("sys/devices/4:0:0:7/wwid", "naa.600c0ff0000000000000000000000001\n")
		}
		return err
	}
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("err %v, want a WWID mismatch", err)
	}
}

func TestStageBadPublishContext(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	for _, ctxMap := range []map[string]string{
		{ctxDirect: "true", ctxTargetIQN: h.target},
		{ctxDirect: "true", ctxLUN: "-1", ctxTargetIQN: h.target},
		{ctxDirect: "true", ctxLUN: "7"},
	} {
		if _, err := h.node(t).stage(context.Background(), "vol-1", ctxMap); code(err) != codes.InvalidArgument {
			t.Fatalf("%v: err %v, want InvalidArgument", ctxMap, err)
		}
	}
}

func TestStageWithoutMultipathMap(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	h.noMap = true
	if _, err := h.node(t).stage(context.Background(), "vol-1", pc(h, "7")); code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
	if got := h.called("sg_persist"); len(got) != 0 {
		t.Fatalf("reserved without a map: %v", got)
	}
}

func TestStageNeedsARunningPath(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	n := h.node(t)
	orig := n.write
	n.write = func(p, data string) error {
		err := orig(p, data)
		if strings.HasSuffix(p, "/scan") {
			for _, d := range []string{"3:0:0:7", "4:0:0:7"} {
				if exists(h.path("sys/devices/" + d)) {
					h.writeFile("sys/devices/"+d+"/state", "offline\n")
				}
			}
		}
		return err
	}
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable with every path offline", err)
	}
	if got := h.called("sg_persist"); len(got) != 0 {
		t.Fatalf("reserved without a running path: %v", got)
	}
}

func TestExpandGrowsPathsAndMap(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	h.dmSize = 1 << 30
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	// The map does not grow: the size check times out.
	if direct, err := n.expand(ctx, "vol-1", 2<<30); !direct || err == nil {
		t.Fatalf("expand %v %v, want a size error", direct, err)
	}
	h.writeFile("sys/block/dm-0/size", "4194304") // 2 GiB in sectors
	if direct, err := n.expand(ctx, "vol-1", 2<<30); !direct || err != nil {
		t.Fatalf("expand %v %v", direct, err)
	}
	if got := h.wrote("/device/rescan=1"); len(got) != 4 {
		t.Fatalf("rescans %v, want both paths twice", got)
	}
	if got := h.called("multipathd resize map " + testMap); len(got) != 2 {
		t.Fatalf("resize calls %v", got)
	}
	if direct, err := n.expand(ctx, "legacy", 2<<30); direct || err != nil {
		t.Fatalf("legacy volume: %v %v", direct, err)
	}
}

func TestStageRescansAnExistingDisk(t *testing.T) {
	h := twoPathHost(t, "node-1", newFakeLU())
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	// A failed teardown left LUN 7's disks behind, without map or state, and
	// Cinder has since mapped another volume at LUN 7.
	h.removeMap(testMap)
	os.Remove(n.statePath("vol-1"))
	const otherWWID = "naa.600c0ff000db9a3c2222222201000000"
	h.array[7] = fakeArrayLUN{wwid: otherWWID, lu: newFakeLU()}
	if _, err := n.stage(ctx, "vol-2", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.wrote("/device/rescan=1"); len(got) != 2 {
		t.Fatalf("rescans %v, want both existing paths", got)
	}
	if got := h.wrote("/scan"); len(got) != 2 {
		t.Fatalf("scans %v, want only the first stage's", got)
	}
	want, _ := mpathWWID(otherWWID)
	if st, ok, err := n.readState("vol-2"); err != nil || !ok || st.WWID != want {
		t.Fatalf("state %+v %v %v, want the WWID read after the rescan", st, ok, err)
	}
}

func TestStageRefusesALUNAnotherVolumeHolds(t *testing.T) {
	lu := newFakeLU()
	h := twoPathHost(t, "node-1", lu)
	n := h.node(t)
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	h.calls = nil
	if _, err := n.stage(ctx, "vol-2", pc(h, "7")); code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "vol-1") {
		t.Fatalf("err %v, want FailedPrecondition naming vol-1", err)
	}
	if got := h.called("sg_persist"); len(got) != 0 {
		t.Fatalf("touched the reservation of vol-1's LUN: %v", got)
	}
	if exists(n.statePath("vol-2")) {
		t.Fatal("state written for the refused volume")
	}
}

func TestStageRetriesAPortalMissingAtBoot(t *testing.T) {
	h := newFakeHost(t, "node-1")
	h.array[7] = fakeArrayLUN{wwid: testWWID, lu: newFakeLU()}
	h.addSession(1, 3, h.target, "LOGGED_IN")
	h.addRecords("198.51.100.101", "198.51.100.103")
	h.loginTo = []int{4}
	n := h.node(t)
	var later []func()
	n.background = func(f func()) { later = append(later, f) }
	ctx := context.Background()
	if _, err := n.stage(ctx, "vol-1", pc(h, "7")); err != nil {
		t.Fatal(err)
	}
	if got := h.wrote("/scan"); len(got) != 1 || !strings.Contains(got[0], "host3") {
		t.Fatalf("scans %v, want the one path this stage had", got)
	}
	if len(later) != 1 {
		t.Fatalf("%d background logins, want 1", len(later))
	}
	// Another stage while that login runs does not start a second one.
	if _, err := n.ensureSessions(ctx, h.target); err != nil || len(later) != 1 {
		t.Fatalf("second login started: %v %d", err, len(later))
	}
	later[0]()
	if got := h.called("iscsiadm"); len(got) != 1 || got[0] != "iscsiadm -m node -T "+h.target+" --login" {
		t.Fatalf("iscsiadm calls %v", got)
	}
	if hosts, err := n.ensureSessions(ctx, h.target); err != nil || len(hosts) != 2 || len(later) != 1 {
		t.Fatalf("hosts %v %v, logins %d: want both paths and no further login", hosts, err, len(later))
	}
}

func TestUnstageOfAFencedNodeGetsPastTheFlush(t *testing.T) {
	lu := newFakeLU()
	a := twoPathHost(t, "node-1", lu)
	b := twoPathHost(t, "node-2", lu)
	na, nb := a.node(t), b.node(t)
	ctx := context.Background()
	if _, err := na.stage(ctx, "vol-1", pc(a, "7")); err != nil {
		t.Fatal(err)
	}
	a.bufsFail = true
	// Not fenced: a failed flush stops the unstage, the state stays.
	if err := na.unstage(ctx, "vol-1"); err == nil || !exists(na.statePath("vol-1")) {
		t.Fatalf("unstage %v with a failed flush on the holder", err)
	}
	if _, err := nb.stage(ctx, "vol-1", pc(b, "7")); err != nil {
		t.Fatal(err)
	}
	if err := na.unstage(ctx, "vol-1"); err != nil {
		t.Fatalf("fenced node's unstage: %v", err)
	}
	if exists(na.statePath("vol-1")) || lu.resKey != nb.key {
		t.Fatalf("state left behind or reservation lost: %+v", lu)
	}
}
