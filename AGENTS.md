# AGENTS.md

How this repo is built and why. Applies to people and agents alike.

## What it is

A CSI proxy (`cmd/`, `proxy/`) in front of upstream's Cinder CSI plugin, which
runs its official image untouched; `directiscsi/` is the direct iSCSI logic. No
patches over an upstream release: what upstream does not expose, the proxy does
itself through the same `cloud.conf`.

## Rules

- English everywhere. Comments say why, in one line; no history.
- Fail fast: invalid `[DirectISCSI]` configuration exits non-zero with the
  reason; Kubernetes does the retrying.
- Every behaviour has a test beside the code; the race detector runs on
  `directiscsi` (as root: the chroot test needs it).
- No references to any particular deployment, company or infrastructure:
  this repo is generic and public. Test data uses `example.com` IQNs,
  `node-N` names and documentation IP ranges.
- `go.mod` pins the `k8s.io/cloud-provider-openstack` release the deployment
  runs as upstream's image; both move together, with the sanity suite.
- A call the proxy does not handle is forwarded unchanged; a new CSI method
  upstream implements needs its forwarder here.

## Versions

Image versions are derived from git, never written by hand: `<x.y.z tag>.<commits
since it>`, counted at the last commit that changed the image. CI builds each
version once on merge to `main` and pushes `:<version>` and `:latest`. A new
`x.y` is the only manual act: `git tag -a 1.2.0 -m '...' && git push origin 1.2.0`.

## Workflow

Branch from `main` (`feat/…`, `fix/…`, `test/…`, `docs/…`, `ci/…`), open a
PR, merge when CI is green. Conventional Commits with a scope; the subject says
what and why.
