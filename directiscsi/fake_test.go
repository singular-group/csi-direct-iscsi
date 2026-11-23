package directiscsi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeLU models the SPC-3 persistent reservation state of one MSA LUN,
// shared by every node that reaches it. A nexus is node + path device.
type fakeLU struct {
	mu      sync.Mutex
	gen     int
	regs    map[string]uint64
	holder  string
	resKey  uint64
	resType int
	ua      map[string]bool // pending unit attention per nexus
}

func newFakeLU() *fakeLU { return &fakeLU{regs: map[string]uint64{}, ua: map[string]bool{}} }

// write reports whether a WRITE from the nexus would be accepted.
func (l *fakeLU) write(nexus string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == "" {
		return true
	}
	_, registered := l.regs[nexus]
	return registered // type 5: every registrant may write
}

func (l *fakeLU) keys() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var nexus []string
	for n := range l.regs {
		nexus = append(nexus, n)
	}
	sort.Strings(nexus)
	var keys []uint64
	for _, n := range nexus {
		keys = append(keys, l.regs[n])
	}
	return keys
}

func (l *fakeLU) release() {
	l.holder, l.resKey, l.resType = "", 0, 0
}

// sgPersist executes one sg_persist call from a nexus.
func (l *fakeLU) sgPersist(nexus string, args []string) (string, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	opt := map[string]string{}
	for _, a := range args {
		k, v, _ := strings.Cut(a, "=")
		opt[k] = v
	}
	num := func(k string) uint64 {
		v, _ := strconv.ParseUint(opt[k], 0, 64)
		return v
	}
	conflict := func() (string, int, error) {
		return "persistent reserve out: scsi status: Reservation Conflict", exitResConflict, fmt.Errorf("sg_persist: exit %d", exitResConflict)
	}
	if l.ua[nexus] {
		delete(l.ua, nexus)
		return "Fixed format, current; Sense key: Unit Attention", exitUnitAttn, fmt.Errorf("sg_persist: exit %d", exitUnitAttn)
	}
	cur, registered := l.regs[nexus]
	switch {
	case has(args, "--read-keys"):
		if len(l.regs) == 0 {
			return fmt.Sprintf("  PR generation=0x%x, there are NO registered reservation keys", l.gen), 0, nil
		}
		var nexus []string
		for n := range l.regs {
			nexus = append(nexus, n)
		}
		sort.Strings(nexus)
		out := fmt.Sprintf("  PR generation=0x%x, %d registered reservation keys follow:\n", l.gen, len(nexus))
		if len(nexus) == 1 { // sg_persist's singular form, a one-path node's case
			out = fmt.Sprintf("  PR generation=0x%x, 1 registered reservation key follows:\n", l.gen)
		}
		for _, n := range nexus {
			out += fmt.Sprintf("    0x%x\n", l.regs[n])
		}
		return out, 0, nil
	case has(args, "--read-reservation"):
		if l.holder == "" {
			return fmt.Sprintf("  PR generation=0x%x, there is NO reservation held", l.gen), 0, nil
		}
		typ := map[int]string{5: prTypeName, 1: "Write Exclusive"}[l.resType]
		return fmt.Sprintf("  PR generation=0x%x, Reservation follows:\n    Key=0x%x\n    scope: LU_SCOPE,  type: %s", l.gen, l.resKey, typ), 0, nil
	case has(args, "--register-ignore"):
		l.gen++
		if sark := num("--param-sark"); sark == 0 {
			delete(l.regs, nexus)
			if l.holder == nexus {
				l.release()
			}
		} else {
			l.regs[nexus] = sark
		}
		return "", 0, nil
	case has(args, "--register"):
		if !registered && num("--param-rk") != 0 || registered && cur != num("--param-rk") {
			return conflict()
		}
		l.gen++
		if sark := num("--param-sark"); sark == 0 {
			delete(l.regs, nexus)
			if l.holder == nexus {
				l.release()
			}
		} else {
			l.regs[nexus] = sark
		}
		return "", 0, nil
	case has(args, "--reserve"):
		if !registered || cur != num("--param-rk") {
			return conflict()
		}
		typ, _ := strconv.Atoi(opt["--prout-type"])
		if l.holder != "" && (l.holder != nexus || l.resType != typ) {
			return conflict()
		}
		l.holder, l.resKey, l.resType = nexus, cur, typ
		return "", 0, nil
	case has(args, "--preempt"):
		if !registered || cur != num("--param-rk") {
			return conflict()
		}
		sark := num("--param-sark")
		l.gen++
		holderPreempted := l.holder != "" && sark == l.resKey
		for n, k := range l.regs {
			if k == sark && n != nexus {
				delete(l.regs, n)
				l.ua[n] = true // REGISTRATIONS PREEMPTED
			}
		}
		if holderPreempted {
			typ, _ := strconv.Atoi(opt["--prout-type"])
			l.holder, l.resKey, l.resType = nexus, cur, typ
		}
		return "", 0, nil
	case has(args, "--release"):
		if !registered {
			// What LIO answers (SPC says RESERVATION CONFLICT).
			return "Sense key: Not Ready, Logical unit communication failure", 2, fmt.Errorf("sg_persist: exit 2")
		}
		if cur != num("--param-rk") {
			return conflict()
		}
		if l.holder == nexus {
			l.release()
			for n := range l.regs {
				if n != nexus {
					l.ua[n] = true // RESERVATIONS RELEASED
				}
			}
		}
		return "", 0, nil
	}
	return "", 99, fmt.Errorf("fake sg_persist: unhandled %v", args)
}

func has(args []string, a string) bool {
	for _, x := range args {
		if x == a {
			return true
		}
	}
	return false
}

// fakeHost is one node: a sysfs and /dev tree in a temp dir plus the host
// binaries the node code runs. Scanning a LUN that the array maps to this
// node creates its disk, like the kernel would.
type fakeHost struct {
	t      *testing.T
	name   string
	root   string
	array  map[int]fakeArrayLUN // LUNs the array maps to this node
	target string

	mu        sync.Mutex
	calls     []string
	writes    []string
	nextDisk  int
	hosts     map[int]bool // logged-in SCSI hosts
	loginTo   []int        // hosts an iscsiadm login brings up
	flushFail int
	bufsFail  bool // blockdev --flushbufs fails
	dmSize    int64
	noMap     bool
}

type fakeArrayLUN struct {
	wwid string // sysfs form, naa.
	lu   *fakeLU
}

func newFakeHost(t *testing.T, name string) *fakeHost {
	h := &fakeHost{t: t, name: name, root: t.TempDir(), array: map[int]fakeArrayLUN{}, target: "iqn.1986-03.com.hp:storage.msa2040.test", hosts: map[int]bool{}, nextDisk: 1}
	for _, d := range []string{"sys/class/iscsi_session", "sys/class/scsi_device", "sys/class/scsi_host", "sys/block", "sys/devices", "dev/mapper", "host/etc/iscsi"} {
		h.mkdir(d)
	}
	h.writeFile("host/etc/iscsi/initiatorname.iscsi", "## generated\nInitiatorName=iqn.2005-01.com.example:"+name+"\n")
	return h
}

func (h *fakeHost) node(t *testing.T) *Node {
	n, err := newNode(Config{InitiatorPrefix: "iqn.2005-01.com.example:", TargetIQN: h.target}, h.name)
	if err != nil {
		t.Fatal(err)
	}
	n.exec = h
	n.sys = filepath.Join(h.root, "sys")
	n.dev = filepath.Join(h.root, "dev")
	n.host = filepath.Join(h.root, "host")
	n.stateDir = filepath.Join(h.root, "state")
	n.write = h.write
	n.pathTimeout = 300 * time.Millisecond
	n.pollInterval = 5 * time.Millisecond
	n.retryInterval = time.Millisecond
	return n
}

func (h *fakeHost) path(p string) string { return filepath.Join(h.root, p) }

func (h *fakeHost) mkdir(p string) {
	if err := os.MkdirAll(h.path(p), 0o755); err != nil {
		h.t.Fatal(err)
	}
}

func (h *fakeHost) writeFile(p, data string) {
	h.mkdir(filepath.Dir(p))
	if err := os.WriteFile(h.path(p), []byte(data), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

func (h *fakeHost) symlink(target, p string) {
	h.mkdir(filepath.Dir(p))
	if err := os.Symlink(target, h.path(p)); err != nil {
		h.t.Fatal(err)
	}
}

// addSession logs in one session to the target on SCSI host hostNum.
func (h *fakeHost) addSession(sess, hostNum int, target, state string) {
	real := fmt.Sprintf("sys/devices/platform/host%d/session%d/iscsi_session/session%d", hostNum, sess, sess)
	h.writeFile(real+"/targetname", target+"\n")
	h.writeFile(real+"/state", state+"\n")
	h.symlink(h.path(real), fmt.Sprintf("sys/class/iscsi_session/session%d", sess))
	h.mkdir(fmt.Sprintf("sys/class/scsi_host/host%d", hostNum))
	h.hosts[hostNum] = true
}

// addDisk creates H:0:0:lun and its sdX as the kernel would after a scan.
func (h *fakeHost) addDisk(hostNum, lun int) string {
	a, ok := h.array[lun]
	if !ok {
		return ""
	}
	dev := "sd" + string(rune('a'+h.nextDisk))
	h.nextDisk++
	hctlName := hctl(hostNum, lun)
	dir := "sys/devices/" + hctlName
	h.writeFile(dir+"/wwid", a.wwid+"\n")
	h.writeFile(dir+"/state", "running\n")
	h.writeFile(dir+"/delete", "")
	h.writeFile(dir+"/rescan", "")
	h.mkdir(dir + "/block/" + dev)
	h.symlink(h.path(dir), "sys/class/scsi_device/"+hctlName+"/device")
	h.symlink(h.path(dir), "sys/block/"+dev+"/device")
	h.mkdir("sys/block/" + dev + "/holders")
	return dev
}

func (h *fakeHost) write(p, data string) error {
	h.mu.Lock()
	rel, _ := filepath.Rel(h.root, p)
	h.writes = append(h.writes, rel+"="+data)
	h.mu.Unlock()
	switch {
	case strings.HasSuffix(rel, "/scan"):
		var hostNum int
		fmt.Sscanf(filepath.Base(filepath.Dir(rel)), "host%d", &hostNum)
		f := strings.Fields(data)
		if len(f) != 3 || f[0] != "0" || f[1] != "0" || f[2] == "-" {
			h.t.Errorf("wildcard or malformed scan %q", data)
		}
		lun, _ := strconv.Atoi(f[2])
		h.addDisk(hostNum, lun)
	case strings.HasSuffix(rel, "/device/rescan"):
		// The disk re-reads its identity: whatever the array maps at its LUN now.
		dir, _ := filepath.EvalSymlinks(filepath.Dir(p))
		var hostNum, lun int
		fmt.Sscanf(filepath.Base(dir), "%d:0:0:%d", &hostNum, &lun)
		if a, ok := h.array[lun]; ok {
			devRel, _ := filepath.Rel(h.root, dir)
			h.writeFile(filepath.Join(devRel, "wwid"), a.wwid+"\n")
		}
	case strings.HasSuffix(rel, "/device/delete"):
		dev := strings.Split(rel, "/")[2]
		dir, _ := filepath.EvalSymlinks(h.path("sys/block/" + dev + "/device"))
		hctlName := filepath.Base(dir)
		os.RemoveAll(h.path("sys/class/scsi_device/" + hctlName))
		os.RemoveAll(h.path("sys/block/" + dev))
		os.RemoveAll(dir)
	}
	return nil
}

// addRecords writes one iscsiadm node record per portal of the target.
func (h *fakeHost) addRecords(portals ...string) {
	for _, p := range portals {
		h.writeFile("host/etc/iscsi/nodes/"+h.target+"/"+p+",3260,1/default", "node.startup = automatic\n")
	}
}

func (h *fakeHost) wwidOf(dev string) string {
	raw := readTrim(h.path("sys/block/" + dev + "/device/wwid"))
	w, _ := mpathWWID(raw)
	return w
}

func (h *fakeHost) luOf(dev string) *fakeLU {
	raw := readTrim(h.path("sys/block/" + dev + "/device/wwid"))
	for _, a := range h.array {
		if a.wwid == raw {
			return a.lu
		}
	}
	return nil
}

func (h *fakeHost) run(_ context.Context, name string, args ...string) (string, int, error) {
	h.mu.Lock()
	h.calls = append(h.calls, name+" "+strings.Join(args, " "))
	h.mu.Unlock()
	fail := func(code int, msg string) (string, int, error) {
		return msg, code, fmt.Errorf("%s %s: exit %d: %s", name, strings.Join(args, " "), code, msg)
	}
	switch name {
	case "sg_persist":
		dev := strings.TrimPrefix(args[len(args)-1], "/dev/")
		lu := h.luOf(dev)
		if lu == nil {
			return fail(15, "open failed")
		}
		return lu.sgPersist(h.name+"/"+dev, args[:len(args)-1])
	case "iscsiadm":
		for i, hn := range h.loginTo {
			h.addSession(100+i, hn, h.target, "LOGGED_IN")
		}
		return "", 0, nil
	case "multipathd":
		if args[0] == "add" && args[1] == "path" && !h.noMap {
			h.addToMap(args[2])
			return "ok", 0, nil
		}
		if args[0] == "resize" {
			return "ok", 0, nil
		}
		return fail(1, "fail")
	case "multipath":
		switch args[0] {
		case "-a", "-w":
			return "", 0, nil
		case "-f":
			if h.flushFail > 0 {
				h.flushFail--
				return fail(1, "map in use")
			}
			h.removeMap(args[1])
			return "", 0, nil
		}
	case "blockdev":
		if h.bufsFail {
			return fail(1, "Input/output error")
		}
		return "", 0, nil
	case "udevadm":
		return "", 0, nil
	}
	return fail(127, "not found")
}

func (h *fakeHost) addToMap(dev string) {
	wwid := h.wwidOf(dev)
	if !exists(h.path("dev/mapper/" + wwid)) {
		h.symlink("../dm-0", "dev/mapper/"+wwid)
		h.writeFile("sys/block/dm-0/size", strconv.FormatInt(h.dmSize/512, 10))
		h.mkdir("sys/block/dm-0/slaves")
	}
	h.mkdir("sys/block/dm-0/slaves/" + dev)
	h.mkdir("sys/block/" + dev + "/holders/dm-0")
}

func (h *fakeHost) removeMap(wwid string) {
	slaves, _ := os.ReadDir(h.path("sys/block/dm-0/slaves"))
	for _, s := range slaves {
		os.RemoveAll(h.path("sys/block/" + s.Name() + "/holders/dm-0"))
	}
	os.RemoveAll(h.path("sys/block/dm-0"))
	os.Remove(h.path("dev/mapper/" + wwid))
}

func (h *fakeHost) called(prefix string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func (h *fakeHost) wrote(suffix string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, w := range h.writes {
		if strings.Contains(w, suffix) {
			out = append(out, w)
		}
	}
	return out
}
