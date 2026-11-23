package directiscsi

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// copyWithLibs copies a binary and the shared libraries ldd lists into root.
func copyWithLibs(t *testing.T, root, bin, dest string) {
	out, err := exec.Command("ldd", bin).Output()
	if err != nil {
		t.Skipf("ldd %s: %v", bin, err)
	}
	files := map[string]string{bin: dest}
	for _, line := range strings.Split(string(out), "\n") {
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "/") {
				files[f] = f
			}
		}
	}
	for src, dst := range files {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(root, dst)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// The binary only exists inside the fake host root, so finding it proves the
// command ran chrooted and resolved through the host PATH.
func TestHostExecRunsInTheHostRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("chroot needs root")
	}
	root := t.TempDir()
	copyWithLibs(t, root, "/usr/bin/env", "/usr/bin/env")
	copyWithLibs(t, root, "/bin/sh", "/bin/sh")
	copyWithLibs(t, root, "/bin/false", "/usr/bin/false")
	// A script, not a copied binary: a multi-call coreutils picks its program by name.
	script := filepath.Join(root, "usr/sbin/only-on-the-host")
	if err := os.MkdirAll(filepath.Dir(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	h := hostExec{root: root}

	out, code, err := h.run(context.Background(), "only-on-the-host", "hello", "host")
	if err != nil || code != 0 || out != "hello host" {
		t.Fatalf("%q %d %v", out, code, err)
	}
	if _, err := exec.LookPath("only-on-the-host"); err == nil {
		t.Fatal("test binary leaked outside the root")
	}
	if _, code, err := h.run(context.Background(), "false"); err == nil || code != 1 {
		t.Fatalf("false: %d %v", code, err)
	}
	if _, code, err := h.run(context.Background(), "iscsiadm"); err == nil || code != 127 {
		t.Fatalf("missing binary: %d %v", code, err)
	}
}
