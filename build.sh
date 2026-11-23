#!/usr/bin/env bash
# Assembles the patched cloud-provider-openstack tree in $1 (default .src):
# upstream at UPSTREAM, patches/ applied in order, directiscsi/ copied in.
set -eo pipefail
here=$(cd "$(dirname "$0")" && pwd)
dst=${1:-$here/.src}
. "$here/UPSTREAM"
rm -rf "$dst"
git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$UPSTREAM_TAG" "$UPSTREAM_REPO" "$dst"
got=$(git -C "$dst" rev-parse HEAD)
[ "$got" = "$UPSTREAM_COMMIT" ] || { echo "ERROR: $UPSTREAM_TAG is $got, pinned $UPSTREAM_COMMIT" >&2; exit 1; }
for p in "$here"/patches/*.patch; do
  git -C "$dst" apply --verbose "$p"
done
cp -r "$here/directiscsi" "$dst/pkg/csi/cinder/directiscsi"
echo "$dst"
