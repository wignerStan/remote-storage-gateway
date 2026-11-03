# Remote Storage Gateway

A gateway product assembled from a patched OpenList control plane, independently
managed worker processes, and an optional independent VFS materializer.

The generic [Data Plane](https://github.com/wignerStan/network-data-plane-worker)
remains a separate repository. Gateway imports its Go module; this repository
does not contain a second copy of its runtime, contracts, or Rust source.

## Applications and module boundaries

| Application | Source | Responsibility |
| --- | --- | --- |
| Control Plane | `go/controlplane`, patched OpenList | Desired configuration, exact plugin descriptors, process management and transfer decisions |
| Worker | `go/worker`, `go/worker/runtime`, `go/cmd/storage-worker` | Public TLS/reverse proxy, data ingress/egress and dynamic plugin execution |
| VFS | Independent [`rclone`](https://github.com/wignerStan/rclone) module and patched native CLI | Optional local materialization through its ttrpc/shared-memory backend |

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
| Transfer / codec | `go/plugins/transfer/reapi` | `encoded/v2` |

Both axes publish dynamically loaded `.so` artifacts. The Data Plane owns contract
structure, lifecycle, admission and the native ABI. Concrete plugins publish
provider/backend fields, secret declarations and capabilities. OpenList consumes
those exact descriptors and does not create competing provider schemas.

The REAPI / ByteStream plugin owns method/cardinality selection, framing and
transfer state. Its generic carrier acts on bounded metadata actions. Payloads
remain host-owned gRPC buffer leases; byte bodies never cross the native control
RPC or the Control Plane. Application buffer reuse is distinct from a claim
about kernel or TLS copies.

## Independent rclone materialization

VFS is a separate rclone-derived product and Go module. This repository contains
no VFS implementation, filesystem wrapper or VFS executable. The Control Plane
imports the external module's Box to manage its native CLI process.

```text
public client -> Worker /dav -> client plugin -> private native rclone DAV/VFS
                                                          |
                                      ttrpc metadata + shared-memory leases
                                                          |
                         Worker raw namespace -> remote client -> provider
```

Rclone's new backend reads an explicitly assigned Worker namespace. It receives
no cloud credentials or origin URL. Worker authenticates the ttrpc and Unix
seqpacket bootstrap peer, obtains a scoped CP decision and retains the exact
client generation. Reusable shared memory carries bytes; ttrpc carries only
bounded metadata and references. The CP neither allocates nor reclaims arenas.

The external application reuses native rclone VFS and private serve behavior.
The backend leg no longer passes through WebDAV or the public TLS listener.
Its private frontend can be selected by a Worker client; public traffic still
enters the Worker. This is not a claim that both local directions use shared
memory or that legacy native client chunks avoid copying.

Cached mounts are read-only, with distinct cached `client` and raw
`remote_client` assignments. CP starts Worker first, pins its observed IPC
incarnation, starts rclone, then optionally activates the exact client selection
using generation compare-and-swap. Rclone retires before Worker. Native leases
remain held until actual borrowers finish or exact process-exit recovery permits
reclamation; cancellation and disconnect do not imply release.

## Source and build

OpenList and its UI are pristine upstream submodules with ordered, hash-checked
patches under `.git-source-relationships`. The builder derives external source
under `/tmp/trade-*` or `/var/tmp/trade-*`; it does not modify upstream checkouts.
The UI is built from the admitted frontend and embedded into the CP binary.

The rclone SDK is an Infra Go package dependency, currently `v1.75.2`. Infra owns its
patch inventory. The builder verifies module checksums, derives the patches in
external build storage and leaves the Go module cache unchanged. Rclone has no
Git submodule or commit pin.

Use the selected, reviewed Infra and Data Plane sources explicitly:

```sh
make all MISE=mise INFRA_ROOT=/path/to/reviewed/infra \
  DATA_PLANE_ROOT=/develop/network/data-plane-worker \
  RCLONE_ROOT=/develop/network/rclone \
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
