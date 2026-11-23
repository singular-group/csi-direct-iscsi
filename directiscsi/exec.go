package directiscsi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
)

// executor runs a host binary; exitCode is -1 when it did not run to completion.
type executor interface {
	run(ctx context.Context, name string, args ...string) (out string, exitCode int, err error)
}

// hostExec runs the node's own iscsiadm, multipath, sg_persist and udevadm,
// chrooted into the host root: the distroless image ships none of them, and
// they must match the node's iscsid and multipathd. Those daemons listen on
// abstract sockets, which hostNetwork shares with this container.
type hostExec struct {
	root string
}

func (h hostExec) run(ctx context.Context, name string, args ...string) (string, int, error) {
	// env resolves name against the host PATH after the chroot.
	cmd := exec.CommandContext(ctx, "/usr/bin/env", append([]string{name}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: h.root}
	cmd.Dir = "/"
	cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL=C"}
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	out := strings.TrimSpace(buf.String())
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return out, exitErr.ExitCode(), fmt.Errorf("%s %s: exit %d: %s", name, strings.Join(args, " "), exitErr.ExitCode(), out)
	}
	if err != nil {
		return out, -1, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return out, 0, nil
}
