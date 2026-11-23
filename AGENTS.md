# AGENTS.md

How this repo is built and why. Applies to people and agents alike.

## What it is

Upstream's Cinder CSI plugin plus the `directiscsi` package and three small
hooks into upstream (`patches/`). Everything else is upstream at `UPSTREAM`,
untouched: a change goes in `directiscsi/` whenever it can, and a patch stays a
hook that calls into it.

## Rules

- English everywhere. Comments say why, in one line; no history.
- Fail fast: invalid `[DirectISCSI]` configuration exits non-zero with the
  reason; Kubernetes does the retrying.
- Every behaviour has a test beside the code; the race detector runs on
  `directiscsi` (as root: the chroot test needs it).
- No references to any particular deployment, company or infrastructure:
  this repo is generic and public. Test data uses `example.com` IQNs,
  `node-N` names and documentation IP ranges.
- Bumping `UPSTREAM` means refreshing `patches/` against the new tag and the
  full test run.

## Versions

Derived from git, never written by hand: `<x.y.z tag>.<commits since it>`,
counted at the last commit that changed the image (`UPSTREAM`, `build.sh`,
`patches/`, `directiscsi/` outside `*_test.go`). CI builds each version once on
merge to `main` and pushes `:<version>` and `:latest`. A new `x.y` is the only
manual act: `git tag -a 1.1.0 -m '...' && git push origin 1.1.0`.

## Workflow

Branch from `main` (`feat/…`, `fix/…`, `test/…`, `docs/…`, `ci/…`), open a
PR, merge when CI is green. Conventional Commits with a scope; the subject says
what and why.
