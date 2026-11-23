package directiscsi

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SCSI-3 persistent reservation, Write Exclusive - Registrants Only. Only the
// node running the pod stays registered, so only it can write.
const (
	prType          = "5"
	prTypeName      = "Write Exclusive, registrants only"
	exitResConflict = 24 // sg_persist: RESERVATION CONFLICT
	exitUnitAttn    = 6  // sg_persist: UNIT ATTENTION, the command did not run
)

// prKey is the first 8 bytes of SHA-256(node name): stable across node
// rebuilds, distinct per node, never the zero key (which means "no key").
func prKey(nodeName string) (uint64, error) {
	sum := sha256.Sum256([]byte(nodeName))
	k := binary.BigEndian.Uint64(sum[:8])
	if k == 0 {
		return 0, fmt.Errorf("persistent reservation key of node %q is zero", nodeName)
	}
	return k, nil
}

func keyArg(k uint64) string { return fmt.Sprintf("0x%x", k) }

type reservation struct {
	held bool
	key  uint64
	typ  string
}

var (
	reKey     = regexp.MustCompile(`(?i)\bkey\s*=\s*(0x[0-9a-f]+)`)
	reType    = regexp.MustCompile(`type:\s*(.+?)\s*$`)
	reKeyLine = regexp.MustCompile(`(?i)^\s*(0x[0-9a-f]+)\s*$`)
)

// parseReservation parses `sg_persist --in --read-reservation`.
func parseReservation(out string) (reservation, error) {
	if strings.Contains(out, "NO reservation held") {
		return reservation{}, nil
	}
	if !strings.Contains(out, "Reservation follows") {
		return reservation{}, fmt.Errorf("unexpected read-reservation output: %q", out)
	}
	r := reservation{held: true}
	m := reKey.FindStringSubmatch(out)
	if m == nil {
		return r, fmt.Errorf("no key in read-reservation output: %q", out)
	}
	k, err := strconv.ParseUint(m[1], 0, 64)
	if err != nil {
		return r, err
	}
	r.key = k
	for _, line := range strings.Split(out, "\n") {
		if t := reType.FindStringSubmatch(line); t != nil {
			r.typ = t[1]
		}
	}
	return r, nil
}

// parseKeys parses `sg_persist --in --read-keys`: one entry per registered I_T nexus.
func parseKeys(out string) ([]uint64, error) {
	if strings.Contains(out, "NO registered reservation keys") {
		return nil, nil
	}
	if !strings.Contains(out, "registered reservation key") {
		return nil, fmt.Errorf("unexpected read-keys output: %q", out)
	}
	var keys []uint64
	for _, line := range strings.Split(out, "\n") {
		if m := reKeyLine.FindStringSubmatch(line); m != nil {
			k, err := strconv.ParseUint(m[1], 0, 64)
			if err != nil {
				return nil, err
			}
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// sg runs sg_persist. A unit attention (another path's release, a preempt,
// a reset) is reported once per path instead of running the command, so the
// command is sent again.
func (n *Node) sg(ctx context.Context, args ...string) (string, int, error) {
	args = append([]string{"--no-inquiry"}, args...)
	for i := 0; ; i++ {
		out, code, err := n.exec.run(ctx, "sg_persist", args...)
		if code != exitUnitAttn || i == 3 {
			return out, code, err
		}
	}
}

func (n *Node) readReservation(ctx context.Context, dev string) (reservation, error) {
	out, _, err := n.sg(ctx, "--in", "--read-reservation", dev)
	if err != nil {
		return reservation{}, err
	}
	return parseReservation(out)
}

func (n *Node) readKeys(ctx context.Context, dev string) ([]uint64, error) {
	out, _, err := n.sg(ctx, "--in", "--read-keys", dev)
	if err != nil {
		return nil, err
	}
	return parseKeys(out)
}

// takeReservation registers this node on every path and makes it the only
// registrant. A key held by another node is removed with a plain PREEMPT: the
// MSA has TAS=0, where PREEMPT AND ABORT also aborts unrelated commands.
func (n *Node) takeReservation(ctx context.Context, paths []string) error {
	k := keyArg(n.key)
	for _, p := range paths {
		if _, _, err := n.sg(ctx, "--out", "--register-ignore", "--param-sark="+k, p); err != nil {
			return err
		}
	}
	dev := paths[0]
	res, err := n.readReservation(ctx, dev)
	if err != nil {
		return err
	}
	switch {
	case !res.held:
		if _, _, err := n.sg(ctx, "--out", "--reserve", "--param-rk="+k, "--prout-type="+prType, dev); err != nil {
			return err
		}
	case res.key != n.key:
		if _, _, err := n.sg(ctx, "--out", "--preempt", "--param-rk="+k, "--param-sark="+keyArg(res.key), "--prout-type="+prType, dev); err != nil {
			return err
		}
	}
	keys, err := n.readKeys(ctx, dev)
	if err != nil {
		return err
	}
	done := map[uint64]bool{n.key: true}
	for _, key := range keys {
		if done[key] {
			continue
		}
		done[key] = true
		if _, _, err := n.sg(ctx, "--out", "--preempt", "--param-rk="+k, "--param-sark="+keyArg(key), "--prout-type="+prType, dev); err != nil {
			return err
		}
	}
	return n.verifyReservation(ctx, dev)
}

func (n *Node) verifyReservation(ctx context.Context, dev string) error {
	res, err := n.readReservation(ctx, dev)
	if err != nil {
		return err
	}
	if !res.held || res.key != n.key || res.typ != prTypeName {
		return fmt.Errorf("reservation on %s is %+v, want key %s type %q", dev, res, keyArg(n.key), prTypeName)
	}
	keys, err := n.readKeys(ctx, dev)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("no key registered on %s after reserving", dev)
	}
	for _, key := range keys {
		if key != n.key {
			return fmt.Errorf("foreign key %s still registered on %s", keyArg(key), dev)
		}
	}
	return nil
}

// fencedOut reports whether another node holds the reservation of the paths.
func (n *Node) fencedOut(ctx context.Context, paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	res, err := n.readReservation(ctx, paths[0])
	return err == nil && res.held && res.key != n.key
}

// dropReservation releases the reservation if this node holds it, then
// unregisters every path. RESERVATION CONFLICT on unregister means another
// node preempted this one already, which is the same end state. Release is
// only sent by the holder: arrays differ on what a non-registrant gets back
// (LIO answers NOT READY, not RESERVATION CONFLICT).
func (n *Node) dropReservation(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	k := keyArg(n.key)
	res, err := n.readReservation(ctx, paths[0])
	if err != nil {
		return err
	}
	if res.held && res.key == n.key {
		if _, code, err := n.sg(ctx, "--out", "--release", "--param-rk="+k, "--prout-type="+prType, paths[0]); err != nil && code != exitResConflict {
			return err
		}
	}
	for _, p := range paths {
		if _, code, err := n.sg(ctx, "--out", "--register", "--param-rk="+k, "--param-sark=0", p); err != nil && code != exitResConflict {
			return err
		}
	}
	return nil
}
