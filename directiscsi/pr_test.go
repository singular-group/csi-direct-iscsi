package directiscsi

import (
	"context"
	"fmt"
	"testing"
)

func TestPRKey(t *testing.T) {
	seen := map[uint64]string{}
	for _, name := range []string{"node-1", "node-2", "node-3", "stg-node-1", "stg-node-2", "stg-node-3"} {
		k, err := prKey(name)
		if err != nil || k == 0 {
			t.Fatalf("%s: %x %v", name, k, err)
		}
		if again, _ := prKey(name); again != k {
			t.Fatalf("%s: key not stable", name)
		}
		if other, dup := seen[k]; dup {
			t.Fatalf("%s and %s share key %x", name, other, k)
		}
		seen[k] = name
	}
	// Same as: printf %s node-1 | sha256sum | cut -c1-16
	if k, _ := prKey("node-1"); keyArg(k) != "0x35971be6e9bb024a" {
		t.Fatalf("key of node-1 is %s", keyArg(k))
	}
}

// Output of sg3_utils 1.48 (Ubuntu 24.04), with and without the INQUIRY line.
func TestParseReservation(t *testing.T) {
	for _, tc := range []struct {
		out  string
		want reservation
	}{
		{"  PR generation=0x4, there is NO reservation held", reservation{}},
		{"  HP        MSA 2040 SAN      GL22\n  Peripheral device type: disk\n  PR generation=0x7, Reservation follows:\n    Key=0x9d1a6b2f3c4d5e6f\n    scope: LU_SCOPE,  type: Write Exclusive, registrants only\n",
			reservation{held: true, key: 0x9d1a6b2f3c4d5e6f, typ: prTypeName}},
	} {
		got, err := parseReservation(tc.out)
		if err != nil || got != tc.want {
			t.Fatalf("%q: %+v %v, want %+v", tc.out, got, err, tc.want)
		}
	}
	if _, err := parseReservation("sg_persist: open error"); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestParseKeys(t *testing.T) {
	for _, tc := range []struct {
		out  string
		want []uint64
	}{
		{"  PR generation=0x0, there are NO registered reservation keys", nil},
		{"  HP        MSA 2040 SAN      GL22\n  PR generation=0x6, 3 registered reservation keys follow:\n    0x9d1a6b2f3c4d5e6f\n    0x9d1a6b2f3c4d5e6f\n    0x22\n", []uint64{0x9d1a6b2f3c4d5e6f, 0x9d1a6b2f3c4d5e6f, 0x22}},
	} {
		got, err := parseKeys(tc.out)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("%q: %v %v, want %v", tc.out, got, err, tc.want)
		}
	}
	if _, err := parseKeys("read failed"); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestReservationOfAnotherTypeIsNotAccepted(t *testing.T) {
	lu := newFakeLU()
	h := twoPathHost(t, "node-1", lu)
	n := h.node(t)
	// Something else holds a Write Exclusive (type 1) reservation with our key.
	lu.regs["node-1/sdb"] = n.key
	lu.holder, lu.resKey, lu.resType = "node-1/sdb", n.key, 1
	if _, err := n.stage(context.Background(), "vol-1", pc(h, "7")); err == nil {
		t.Fatal("staged on a reservation of the wrong type")
	}
}
