# AGENTS.md

How this repo is built and why. Applies to people and agents alike.

## What it is

The `directiscsi` Go package. It is built into upstream's Cinder CSI plugin by
the `singular-group/cloud-provider-openstack` fork, whose branch adds only the
hooks that call it; the fork builds and publishes the image. No patches over an
upstream release here: a change to upstream code is a commit on the fork.

## Rules

- English everywhere. Comments say why, in one line; no history.
- Fail fast: invalid `[DirectISCSI]` configuration exits non-zero with the
  reason; Kubernetes does the retrying.
- Every behaviour has a test beside the code; the race detector runs on
  `directiscsi` (as root: the chroot test needs it).
- No references to any particular deployment, company or infrastructure:
  this repo is generic and public. Test data uses `example.com` IQNs,
  `node-N` names and documentation IP ranges.
- `go.mod` pins the upstream `k8s.io/cloud-provider-openstack` release the
  fork's branch is based on; both move together.

## Versions

Go module tags `vX.Y.Z`, cut after a merge to `main`. The fork's `go.mod`
then moves to the new tag; the image version is the fork's.

## Workflow

Branch from `main` (`feat/…`, `fix/…`, `test/…`, `docs/…`, `ci/…`), open a
PR, merge when CI is green. Conventional Commits with a scope; the subject says
what and why.
