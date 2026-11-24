# csi-direct-iscsi

A CSI proxy in front of the [OpenStack Cinder CSI plugin](https://github.com/kubernetes/cloud-provider-openstack)
that adds a direct iSCSI path for HPE MSA multiattach volumes: the Kubernetes
nodes log in to the array themselves instead of going through Nova, every volume
is mapped to every node, and a SCSI-3 persistent reservation decides which node
may write. Moving a volume to another node is a reservation PREEMPT on that node,
with no Cinder, Nova or array mapping call on the way.

Upstream's plugin runs its official image, untouched, on another socket. The
proxy listens on the CSI socket the sidecars and kubelet use, answers the calls of
direct volumes and forwards everything else to upstream, so the driver name
(`cinder.csi.openstack.org`) and existing PVs stay as they are.

| Call | Proxy | Upstream |
|------|-------|----------|
| `CreateVolume` | swaps the class type for its `-multiattach` counterpart | creates |
| `DeleteVolume` | removes a direct volume's attachments | deletes |
| `ControllerPublishVolume` / `Unpublish` | direct volumes, and the conversion of Nova ones | Nova volumes |
| `ListVolumes` / `ControllerGetVolume` | gives a server-less attachment its node | lists |
| `NodeStageVolume` | direct: iSCSI, multipath, reservation, format and mount | Nova volumes |
| `NodeUnstageVolume` | direct: reservation, map and paths, after upstream | unmounts |
| `NodePublishVolume` | a direct raw block's bind mount | everything else |
| `NodeExpandVolume` | direct: paths, map and filesystem | Nova volumes |
| everything else | forwards | answers |

## How a volume is attached

One CSIDriver (`attachRequired: true`) and the same StorageClasses serve two
kinds of volume:

- **Direct.** A Cinder multiattach volume whose type ends in `-multiattach`, with
  one Cinder attachment per node and no instance, so the array maps it to every
  node, the publishing node first. The node running the pod scans only that
  volume's LUN, builds its dm-multipath map and takes a *Write Exclusive –
  Registrants Only* reservation with only its own key registered, so the array
  refuses other nodes' writes. A move is the new node's PREEMPT; unstage releases
  and unregisters. The VolumeAttachment carries `directISCSI: "true"` in
  `status.attachmentMetadata`.
- **Through Nova.** Upstream's path, unchanged.

Which one a volume is:

- `<t>-multiattach` is taken as `<t>`'s counterpart on the same backend, so the
  suffix is reserved for those types.
- A cluster is **ready** when every node registered with the driver has a Nova
  port on the storage network. Until it is, or while the class type's
  `-multiattach` counterpart does not exist, the plugin behaves as upstream.
- On a ready cluster a **new volume** gets the `-multiattach` counterpart of its
  class type, and a volume **through Nova converts at its next attach** once its
  Nova attachment is gone: a retype, then one attachment per node. One array
  operation at a time per controller; a failure leaves the volume as it was and
  the attacher retries. A volume that is never detached stays through Nova.
- A **direct volume stays direct**, whatever the cluster's state.

## Deployment

In both plugin pods, upstream's `cinder-csi-plugin` listens on
`unix:///csi/upstream.sock` and this proxy on the CSI socket the sidecars (and,
on the nodes, the kubelet registration) already use:

```
csi-direct-iscsi --mode=controller|node --endpoint=unix:///csi/csi.sock \
  --upstream-endpoint=unix:///csi/upstream.sock --cloud-config=<upstream's files> ...
```

The controller proxy reads upstream's `cloud.conf` for its own Cinder and Nova
clients. The node proxy runs privileged like upstream's node plugin, with the
kubelet directory (`Bidirectional`), `/dev`, and the node's root at `/host`.

## Requirements

- **Cinder**: an HPE MSA iSCSI backend, and for each class type `<t>` a type
  `<t>-multiattach` on the same backend with `multiattach='<is> True'`. The
  backend must retype a change of `multiattach` alone **in place**: the upstream
  HPE MSA driver copies the volume on the array for any retype.
- **Nodes**: a NIC on a Nova network that reaches the array's iSCSI portals,
  `iscsiadm`, `multipath` (with `multipathd` running) and `sg_persist`
  (sg3_utils), and `/etc/iscsi/initiatorname.iscsi` set to
  `<initiator-prefix><node name>`. Each node logs in to the portals at boot.
- **Node proxy**: the node's root mounted at `/host` with `HostToContainer`
  propagation (it chroots into it to run the node's iSCSI, multipath and sg3
  tools), and `NODE_NAME` from `spec.nodeName`.
- **Controller proxy**: a cloud-config user that sees attachment host names
  (Cinder hides them from non-admins), and read access to CSINodes.

## Configuration

A `[DirectISCSI]` section in any of the `--cloud-config` files; without
`target-iqn` the plugin is upstream's.

| Key | Value |
|-----|-------|
| `initiator-prefix` | A node's initiator is this plus its name; NodeStage refuses a node whose `/etc/iscsi/initiatorname.iscsi` differs |
| `target-iqn` | The array's target IQN |
| `storage-network` | Name of the Nova network of the nodes' storage NIC |

## Limits

- **254 direct volumes per cluster**: every direct volume is mapped to every
  node, and an MSA initiator sees at most 254 LUNs.
- **ReadWriteOnce only**: on a ReadWriteMany claim each node's stage would
  pre-empt the other's reservation.
- **A node that is down** keeps its reservation until another node pre-empts it;
  Kubernetes moves its pods once the node is tainted
  `node.kubernetes.io/out-of-service`.
- **The reservation stops a mistaken write, not root on a node**: without CHAP, a
  node can log in with another initiator's IQN.

## Building and testing

```bash
go vet ./...
go test ./...                 # unit tests and the CSI sanity suite through the proxy
sudo go test -race ./...      # the chroot test needs root
docker build -t csi-direct-iscsi .
```

`test/sanity` runs the CSI sanity suite against the proxy with upstream's driver
behind it, on upstream's fake cloud and mounts: every call is forwarded, and the
results match upstream's own run.

## Versions

Images are `ghcr.io/singular-group/csi-direct-iscsi:<x.y.z.N>`: the last `x.y.z`
tag and the commits since it, counted at the last commit that changed the image
(`cmd/`, `proxy/`, `directiscsi/` outside tests, `go.mod`, `go.sum`,
`Dockerfile`). CI builds each version once on merge to `main` and pushes
`:<x.y.z.N>` and `:latest`; a published version is never overwritten.

## License

Apache License 2.0, as upstream.
