# Remote Storage Gateway

A gateway product assembled from a patched OpenList control plane, independently
managed worker processes, and an optional independent VFS materializer.

The generic [Data Plane](https://github.com/wignerStan/network-data-plane-worker)
remains a separate repository. Gateway imports its Go module; this repository
does not contain a second copy of its runtime, contracts, or Rust source.

## Three applications

| Application | Source | Responsibility |
| --- | --- | --- |
| Control Plane | `go/controlplane`, patched OpenList | Desired configuration, exact plugin descriptors, process management and transfer decisions |
| Worker | `go/worker`, `go/worker/runtime`, `go/cmd/storage-worker` | Public TLS/reverse proxy, data ingress/egress and dynamic plugin execution |
| VFS | `go/vfs`, `go/cmd/storage-vfs` | Optional local materialization and cache lifecycle in its own process |

When a Gateway process file is configured, OpenList runs in managed mode: legacy
provider initialization, queued payload tasks, media/index execution and auxiliary
data listeners remain disabled. Existing identity, SQLite records and task history
are preserved. Typed control APIs and DB-only administration stay available; the
legacy SDKs remain linked, so this is execution isolation rather than dependency
elimination.

The importable Worker Box accepts typed configuration and starts a separate
`storage-worker` executable. The Control Plane composes Infra's process
supervisor rather than embedding payload runtimes. Metadata-only ttrpc ports
provide readiness, descriptors, observed generations, replacement and transfer
decisions. They have no file-body operation.

The Worker terminates public TLS and reverse proxies the OpenList management UI
and API. It does not need another nginx process. Schema contributions belong to
typed entries; Infra's application-support schema tool composes them and Gin
publishes the resulting routes.

## Two plugin axes

| Axis | Product implementations | Worker-owned port |
| --- | --- | --- |
| Client | `go/plugins/client/rclone`; OpenList client extracted by the ordered upstream patches | `client/v1` |
| Transfer / codec | `go/plugins/transfer/reapi` | `encoded/v1` |

Both axes publish dynamically loaded `.so` artifacts. The Data Plane owns contract
structure, lifecycle, admission and the native ABI. Concrete plugins publish
provider/backend fields, secret declarations and capabilities. OpenList consumes
those exact descriptors and does not create competing provider schemas.

The REAPI / ByteStream plugin owns method/cardinality selection, framing and
transfer state. Its generic carrier acts on bounded metadata actions. Payloads
remain host-owned gRPC buffer leases; byte bodies never cross the native control
RPC or the Control Plane. Application buffer reuse is distinct from a claim
about kernel or TLS copies.

## Independent materialization

```text
public client -> Worker /dav -> client plugin -> private VFS DAV
                                                    |
                                            cache miss only
                                                    |
                         Worker /remote-dav -> remote client -> provider
```

Infra `go/storage/cloud` and `go/storage/vfs` supply reusable mechanisms. The VFS
application owns their execution and local disk. Its advertised endpoint is pinned
to the ready Worker's control identity and selects a remote-only mount. For public
TLS Workers, `local_data_socket` supplies a separate private HTTP Unix data
listener. The manager pins that socket for VFS; rclone's existing `unix_socket`
transport carries bytes without a public DNS/CA override. This data listener is
separate from metadata-only ttrpc and accepts authenticated raw reads only.

Cached mounts are read-only; activation rejects writable mounts. Ordinary remote
streaming mounts retain their existing write operations. A cached mount has distinct `client` and `remote_client` assignments. The raw
`/remote-dav` route always selects the latter, preventing cache recursion. The
Control Plane starts the Worker first, then VFS; an optional `activate_client`
selection is published with a compare-and-swap against the observed generation
once VFS is ready. Provider configuration bytes remain plugin-owned.

VFS's private local DAV credential is separate from its outbound Data Plane
credential. Only explicitly approved client secret references can reach native
clients; Worker ingress and TLS secrets stay host-owned. Shutdown retires VFS
before its Worker and keeps ownership until actual process reaping completes.

## Source and build

OpenList and its UI are pristine upstream submodules with ordered, hash-checked
patches under `.git-source-relationships`. The builder derives external source
under `/tmp/trade-*` or `/var/tmp/trade-*`; it does not modify upstream checkouts.
The UI is built from the admitted frontend and embedded into the CP binary.

Rclone is an Infra Go package dependency, currently `v1.75.2`. Infra owns its
patch inventory. The builder verifies module checksums, derives the patches in
external build storage and leaves the Go module cache unchanged. Rclone has no
Git submodule or commit pin.

Use the selected, reviewed Infra and Data Plane sources explicitly:

```sh
make all MISE=mise INFRA_ROOT=/path/to/reviewed/infra \
  DATA_PLANE_ROOT=/develop/network/data-plane-worker \
  BUILD_DIR=/tmp/trade-remote-gateway-build
make test INFRA_ROOT=/path/to/reviewed/infra \
  DATA_PLANE_ROOT=/develop/network/data-plane-worker
```

Development tools are managed by mise; `build/toolchain.json` declares build
versions. Bootstrap also requires Git and jq. The generated development workspace
selects independent local modules; package versions remain in their owners'
`go.mod` files. Release builds require clean source and record measured source
and artifact identities.

`deploy/managed-gateway.yaml` runs the manager and its separate child processes in
one container, with a separately named Deployment/Service and explicitly selected
CP PVC. Reusing the legacy database requires retiring its previous writer and
migrating legacy HTTP/S3 consumers; this profile does not replace their Services.
It publishes only the Worker
TLS port; CP and VFS bind loopback. Its readiness probe reaches the manager
through the Worker and waits for completed activation and live children.

Bazel `rules_img` builds the Worker, CP-manager and VFS images through Buildbarn.
The CP image contains separate child binaries and native artifacts so it can
manage them as processes. `make images` admits exact input hashes before image
assembly. No Dockerfile builder daemon is used.

Validation receipts under `docs/validation` describe their exact revisions.
Older receipts and image candidates are historical evidence; they do not imply
that the current three-process architecture has been deployed.

Small-file admission, packed ZIP namespaces and independent cache placement are documented in [Small files](docs/small-files.md).
