# Remote Storage Gateway

A gateway product with one executable for manager, patched OpenList control-plane
and Worker roles, plus an optional independent native rclone VFS materializer.

The generic [Data Plane](https://github.com/wignerStan/network-data-plane-worker)
remains a separate repository. Gateway imports its Go module; this repository
does not contain a second copy of its runtime, contracts, or Rust source.

The canonical workspace is `/develop/storage/remote-storage-gateway`;
`/develop/network/remote-storage-gateway` remains a compatibility symlink.

## Applications and module boundaries

| Application | Source | Responsibility |
| --- | --- | --- |
| Manager | `go/manager`, patched `cmd/gateway-bundle` | Start and reap sibling CP/Worker processes through Infra supervision |
| Control Plane | `go/controlplane`, patched OpenList | Desired configuration, exact plugin descriptors and transfer decisions |
| Worker | `go/worker`, `go/worker/runtime`, `go/cmd/storage-worker` | Public TLS/reverse proxy, data ingress/egress and dynamic plugin execution |
| VFS | Independent [`rclone`](https://github.com/wignerStan/rclone) module and patched native CLI | Optional local materialization through its ttrpc/shared-memory backend |

When `OPENLIST_GATEWAY_CONTROL_FILE` is configured, OpenList runs in managed mode: legacy
provider initialization, queued payload tasks, media/index execution and auxiliary
data listeners remain disabled. Existing identity, SQLite records and task history
are preserved. Typed control APIs and DB-only administration stay available; the
legacy SDKs remain linked, so this is execution isolation rather than dependency
elimination.

The bundle manager uses Infra's dependency-ordered process group. It self-execs
the same gateway ELF with `control-plane` and `worker` role arguments; child
configuration arrives on stdin. The CP never starts a Worker or VFS process.
Metadata-only ttrpc is local; authenticated gRPC is the cross-Pod control port.
Both expose finite management/decision operations with no file-body operation.

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
no VFS implementation, filesystem wrapper or VFS executable. The top-level manager
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
`remote_client` assignments. Manager starts CP, then Worker, pins its observed IPC
incarnation, starts rclone, then optionally activates the exact client selection
using generation compare-and-swap. Rclone retires before Worker. Native leases
remain held until actual borrowers finish or exact process-exit recovery permits
reclamation; cancellation and disconnect do not imply release.

## Source and build

OpenList and its UI are upstream products recorded by closed Git v5
`upstream_product_build` contracts. Their ignored `build/openlist` and
`build/openlist-ui` directories contain disposable derivations. The OpenList
package has one ordered, hash-checked patch per target, grouped by semantic
family under `patches/openlist`; it has no patch-over-patch stack. Acquisition
receipts bind immutable upstream commits and the final tree. The UI is built
from the admitted frontend and embedded into the executable.

The rclone SDK is an Infra Go package dependency, currently `v1.75.2`. Infra owns its
patch inventory. The builder verifies module checksums, derives the patches in
external build storage and leaves the Go module cache unchanged. Rclone has no
Git submodule or commit pin.

Use the selected, reviewed Infra and Data Plane sources explicitly:

```sh
make all GO=/opt/go1.27.3/bin/go NODE=/opt/node24.11.0/bin/node \
  PNPM=/opt/pnpm11.28.9/bin/pnpm JOBS=2 \
  NATIVE_SDK_ROOT=/build/native-sdk PKG_CONFIG=/usr/bin/pkg-config \
  INFRA_ROOT=/path/to/reviewed/infra \
  DATA_PLANE_ROOT=/path/to/reviewed/network-data-plane-worker \
  RCLONE_ROOT=/path/to/reviewed/rclone \
  BUILD_DIR=/build/storage-gateway
```

The native entry point validates the executable versions pinned by
`build/toolchain.json`. Mise is optional through `MISE=/path/to/mise`.
Bootstrap also requires Git, Bash, coreutils and jq; native plugins require a C
compiler and the canonical shared-memory SDK selected by `NATIVE_SDK_ROOT`;
the build checks the producer receipt, header/archive hashes and selected
pkg-config file. See the SDK install command in [build/README.md](build/README.md).
`BUILD_DIR` may be any absolute directory outside the selected source
repositories and module cache. `JOBS` defaults to 2 and `GOMEMLIMIT` to 768 MiB per
Go process. Existing Go and pnpm caches are reused.

The build derives exact sources, runs the upstream pnpm UI build with its frozen
lockfile, embeds that measured UI, and compiles the product ELF, component
executables and dynamic plugins with local Go. Infra `code/build/source`
captures and verifies the source/dependency closure before and after native
compilation. Official Huma and the current `go/patches/rclone` inventory are
consumed directly. Receipts bind actual source/artifact hashes, SDK/compiler/link inputs and local
execution. Set `WITH_RCLONE=1` to build and assemble the separate rclone executable
through its owner's recipe; the default product does not require that optional
artifact. See [Native product derivation](build/README.md) for stages, outputs
and validation commands.

`deploy/managed-gateway.yaml` runs the top-level manager and its separate child
processes in one container, with a dedicated Deployment/Service and explicitly
selected CP PVC. The public port belongs to Worker TLS; CP and VFS bind loopback.
`deploy/microservices.yaml` separates roles into Kubernetes workloads using gRPC
with mTLS workload identities. See [deployment.md](docs/deployment.md) for the two
assemblies, scaling boundaries, identities and readiness.

`make image-inputs` emits verified native artifact and public CA inputs for
separate OCI assembly. Native Make targets do not invoke Bazel, Buildbarn or
GitHub Actions. An input composition alone does not establish a built image or
a production deployment.

Validation receipts under `docs/validation` describe their exact revisions.
Older receipts and image candidates are historical evidence; they do not imply
that the current three-process architecture has been deployed.

Small-file admission, packed ZIP namespaces and independent cache placement are documented in [Small files](docs/small-files.md).
