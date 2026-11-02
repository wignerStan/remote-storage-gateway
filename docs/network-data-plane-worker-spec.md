# Network data-plane worker architecture specification

## Status

This document is the target architecture for the repository currently named
`pingora-worker`. It is a design contract, not a claim that every migration item
has already landed.

The canonical future repository and application name is:

```text
network-data-plane-worker
```

`Pingora` remains one Rust transport implementation. It is not the product
identity because the application also owns a Go implementation, optional L4 and
L7 lanes, plugin composition, client plugins, management entry, generation
replacement, drain, and shutdown.

## Decision summary

1. Infra exposes reusable L4 and L7 facilities. It does not own the runnable
   data-plane application.
2. Rust keeps one Pingora facility crate with independent `l4` and `l7` Cargo
   features and no default lane.
3. Go exposes exactly two data-plane-facing proxy facility packages: `l4` and
   `l7`. Application build tags select which packages are linked.
4. The application repository contains both Rust and Go implementations. They
   build as separate processes and share contracts, lifecycle semantics, build
   profiles, and conformance tests; they do not use FFI to combine runtimes.
5. Worker admission, generation publication, plugin lifecycle, management
   actions, and completion belong to the application worker core, not to either
   L4 or L7 facility.
6. L4 and L7 share lifecycle infrastructure but never share one untyped protocol
   plugin surface.
7. OpenList and rclone integrations are application-owned **client Tool
   plugins**. They do not enter infra and are not notifier callbacks.
8. OpenList and rclone run in-process in the Go worker build. This specification
   introduces no provider sidecar and no provider RPC service.
9. S3, WebDAV, bucket routing, SigV4, multipart state, ETags, and object metadata
   are application L7 protocol semantics, not client-plugin or infra semantics.
10. Compilation defines the maximum capability set. Runtime configuration may
    only select a subset and must fail closed when it names a capability absent
    from the binary.

## System boundary

```text
                         CONTROL PLANE
                               |
                 desired state / secret leases
                               |
                               v
+------------------------------------------------------------------+
|                 network-data-plane-worker                        |
|                                                                  |
| worker control / admission / generations / plugin lifecycle      |
| management actions / rollback / drain / completion               |
|                                                                  |
|  +--------------------------+  +-------------------------------+  |
|  | L4 lane                  |  | L7 lane                       |  |
|  | TCP / UDP / TLS          |  | HTTP/1.1 / HTTP/2            |  |
|  | SNI / ALPN / pre-read    |  | reverse proxy / Range        |  |
|  | raw stream / half-close  |  | trailers / upgrade / CONNECT |  |
|  +------------+-------------+  +---------------+---------------+  |
|               |                                |                  |
|       typed L4 policy plugins         typed L7/protocol plugins  |
|                                                |                  |
|                                      application Client port      |
|                                                |                  |
|                              +-----------------+----------------+ |
|                              |                                  | |
|                    OpenList client plugin              rclone client plugin
+------------------------------------------------------------------+
```

The control plane never carries ordinary object bytes and is not a synchronous
request-path dependency. A worker with a last-known-good revision continues to
serve while its control plane is temporarily unavailable.

## Repository layout after rename

```text
network-data-plane-worker/
├── README.md
├── SECURITY.md
├── contracts/
│   ├── control/
│   ├── worker/
│   ├── plugin/
│   └── client/
├── rust/
│   ├── Cargo.toml
│   ├── src/main.rs
│   └── crates/
│       ├── worker-core/
│       ├── worker-assembly/
│       ├── lane-l4/
│       ├── lane-l7/
│       └── plugin-bindings/
├── go/
│   ├── go.mod
│   ├── cmd/network-data-plane-worker/
│   └── internal/
│       ├── worker/
│       ├── assembly/
│       ├── entry/
│       ├── lanes/l4/
│       ├── lanes/l7/
│       ├── client/
│       └── plugins/
│           ├── openlist/
│           └── rclone/
├── tests/
│   ├── build-matrix/
│   ├── conformance/
│   ├── lifecycle/
│   └── cross-language/
└── docs/
```

The Rust and Go binaries are independent build profiles. Cross-language
interoperability is required only for shared management/control contracts that
are deliberately selected; object data does not cross a language bridge merely
because both implementations exist in the same repository.

# Infra contract

## Rust Pingora facility

The existing `proxy-pingora` crate remains an infra mechanism. Its public lane
surface becomes feature-gated:

```rust
#[cfg(feature = "l4")]
pub mod l4;

#[cfg(feature = "l7")]
pub mod l7;
```

Cargo features:

```toml
[features]
default = []
l4 = [
  # raw listener, stream, socket, TLS/SNI/ALPN and L4 dependencies
]
l7 = [
  # Pingora HTTP ingress/egress, proxy, bridge and HTTP dependencies
]
```

Required builds:

```sh
cargo check -p proxy-pingora --no-default-features --features l4
cargo check -p proxy-pingora --no-default-features --features l7
cargo check -p proxy-pingora --no-default-features --features l4,l7
```

Rules:

- `l4` and `l7` must compile independently.
- Neither public lane imports the other lane's public API.
- Private shared mechanisms are permitted, but may not become a public umbrella
  API.
- L4 owns raw listeners, streams, half-close, byte transfer, optional TLS
  termination, pre-read bytes, SNI/ALPN facts, and transport drain.
- L7 owns HTTP framing, request/response streaming, trailers, Range, reverse
  proxying, upgrades, CONNECT, and HTTP connection lifecycle.
- Worker control, callback ordering, application authorization, plugin
  membership, S3 semantics, client selection, and client plugins remain outside
  `proxy-pingora`.
- Protocol-specific application carriers such as S3 and MCP do not become root
  Pingora facilities merely because they use HTTP.

The existing worker semantic chain is migrated to this application's
`worker-core`; the Pingora crate supplies transport effects and native carrier
hooks only.

## Go L4 and L7 facilities

The application-facing Go proxy surface consists of exactly these packages:

```text
infra/go/network/proxy/l4
infra/go/network/proxy/l7
```

Existing focused protocol and transport leaves may remain independently
reusable. They are not re-exported through an umbrella package.

### `l4`

Owns:

- TCP/UDP listener custody;
- optional TLS termination;
- SNI, ALPN and bounded pre-read facts;
- raw connection lifecycle;
- stream backpressure and idle bounds;
- half-close and drain.

It must not import `net/http`, S3 code, OpenList, rclone, or the application
worker.

### `l7`

Owns:

- HTTP server lifecycle;
- HTTP/1.1 and HTTP/2 framing;
- streaming reverse proxying;
- Range, trailers, flushing, upgrades and CONNECT;
- request/response header trust boundaries;
- downstream/upstream cancellation and response-commit tracking;
- graceful drain.

Its selected standard mechanism is `net/http` plus
`net/http/httputil.ReverseProxy` and caller-owned `http.RoundTripper` values.
It must not own bucket policy, S3 authentication, provider accounts, client
plugins, or application business routing.

Infra Go packages compile normally. Build tags live in the application
composition, not inside these facilities.

## Reused infra components

The application composes existing focused infra owners instead of creating an
application framework inside infra:

```text
runtime/pluginlifecycle   reversible resource graph and typed bindings
runtime/nativeplugin      trusted native plugin generations and custody
network/actionroute       independent action publications and drain
network/localcontrol      bounded same-host control carrier
network/httpworker        HTTP envelope/profile execution where applicable
network/applicationstream bounded HTTP/message stream mechanisms
network/http/egressclient bounded control/API fetches, not object streaming
```

The buffered egress client must not be placed in the object data path. Safe DNS,
address-admission and dial behavior should be factored into a reusable transport
capability when the L7 facility needs the same protections.

# Application worker core

## Responsibilities

The generic worker core owns:

```text
accepting / draining state
immutable desired and applied revisions
lane generation publication
plugin and client generation publication
capability binding
management action publication
prepare / commit / rollback
completion and accounting
ordered shutdown
```

It does not parse HTTP, copy raw TCP streams, or implement provider APIs.

## Exchange pinning

Every admitted exchange pins all mutable owners that it may call:

```text
worker revision
lane generation
policy/protocol plugin generations
selected client generation
```

A replacement affects only new exchanges. In-flight exchanges finish against
their pinned owners. No request or plugin call is replayed across generations.

## Startup

```text
1. Parse and validate configuration.
2. Validate it against the binary capability manifest.
3. Construct client and plugin candidates.
4. Prepare management publications.
5. Prepare selected L4/L7 lane generations.
6. Install all reversible resources.
7. Commit the composition.
8. Open data listeners.
9. Publish readiness.
10. Set accepting=true.
```

No public listener opens before all mandatory application resources have been
prepared successfully.

## Replacement

```text
construct candidate
validate and initialize candidate
publish new generation atomically
route new exchanges to new generation
drain old generation
close old owner after actual work finishes
```

Candidate failure leaves the current generation active. A control request must
include an application/manager incarnation and an expected generation so stale
commands cannot withdraw a newer installation.

## Shutdown

```text
accepting=false
stop new lane admission
drain admitted exchanges
withdraw action and plugin publications
drain plugin generations
drain client generations
close lane facilities
close management facilities
flush observability
return aggregated cleanup failures
```

L4 and L7 are independently closable resources. Closing one lane does not
implicitly close the other lane or unrelated management/plugin publications.

# Typed plugin model

There is no universal `OnStage(string, any)` plugin interface.

## L4 policy surface

Representative operations:

```text
admit stream
select logical route
before dial
connected
complete
```

Inputs contain bounded typed stream facts, never an HTTP request object.

## L7 policy and protocol surfaces

Representative policy operations:

```text
authenticate
authorize
select logical route
prepare request metadata
complete
```

Protocol plugins such as S3 or WebDAV own their protocol semantics and call the
application Client port. They are not infra facilities.

## Client Tool plugins

A client plugin is a Tool provider, not a notifier participant. It does not join
L4/L7 authentication, routing, socket, header-patch or completion callback
chains unless a separate policy plugin explicitly implements those surfaces.

Representative Go application contract:

```go
type Plugin interface {
    ID() string
    Describe() Descriptor
    OpenClient(context.Context, json.RawMessage) (Client, error)
}

type Client interface {
    Stat(context.Context, ObjectRef) (ObjectInfo, error)
    List(context.Context, ListRequest) (ListResult, error)
    Open(context.Context, OpenRequest) (ReadPlan, error)
    Put(context.Context, PutRequest) (PutResult, error)
    Delete(context.Context, ObjectRef) error
    Close(context.Context) error
}

type PutRequest struct {
    Path        string
    Body        io.Reader
    Size        int64 // -1 means unknown
    ContentType string
    Metadata    map[string]string
}
```

A client slot publishes one current generation. Pinning a slot returns a client
and a release function. Replacement constructs the next client first, atomically
publishes it, then drains and closes the old generation.

# OpenList client plugin

OpenList-specific code exists only in the Go application:

```text
go/internal/plugins/openlist
```

It reuses selected OpenList provider adapters and their provider-specific value:

- authentication and token refresh;
- provider object identifiers;
- provider-native links;
- resumable/chunk upload behavior;
- `PutResult` or equivalent authoritative results;
- provider-specific retry and API quirks.

It does not expose or make application-core dependencies on OpenList mount paths,
`internal/fs`, the OpenList S3 server, router, UI, user model, or storage database
model.

OpenList dependencies are forbidden under infra:

```text
infra/go/**
infra/rust/**
```

The OpenList plugin is statically selected by the Go application build. This
specification does not create `openlist-provider`, a provider sidecar, or a
provider RPC protocol.

# rclone client plugin

The rclone integration is also application-owned:

```text
go/internal/plugins/rclone
```

It directly imports the Go module:

```go
import (
    "github.com/rclone/rclone/fs"
    "github.com/rclone/rclone/fs/object"

    // Only selected backends.
    _ "github.com/rclone/rclone/backend/drive"
    _ "github.com/rclone/rclone/backend/onedrive"
)
```

Do not invoke the `rclone` executable and do not route object bodies through
`copy` or `rcat`.

## Streaming write contract

For a known decoded object size:

```go
obj, err := remote.Put(ctx, request.Body, objectInfo)
```

For an unknown decoded object size:

```go
putStream := remote.Features().PutStream
if putStream == nil {
    return ErrUnknownSizeStreamingUnsupported
}
obj, err := putStream(ctx, request.Body, objectInfo)
```

The request body remains an `io.Reader`; no whole-object `[]byte` or temporary
file is required by the client contract.

If one selected backend lacks `PutStream`, application policy chooses one of:

1. reject unknown-size writes;
2. use an explicitly bounded disk spool owned by the application protocol
   workflow; or
3. patch that specific rclone backend to implement unknown-size streaming.

Do not patch rclone core merely to accept an `io.Reader`; it already does. Do not
silently introduce unbounded RAM buffering.

## S3 boundary

The S3 L7 plugin owns:

- SigV4 and AWS streaming-chunk verification/decoding;
- bucket/key and client selection;
- multipart upload sessions and part ordering;
- ETag and user metadata state;
- conditional requests and S3 errors.

The rclone client receives decoded object bytes. S3 multipart parts are not each
forwarded as an overwrite of the destination object. Completion must provide one
ordered logical object stream to `Client.Put`, or use a deliberately selected
protocol-level bounded spool/compose strategy.

# Application build selection

## Rust Cargo features

Representative application features:

```toml
[features]
default = []
l4 = ["proxy-pingora/l4"]
l7 = ["proxy-pingora/l7"]
control-local = ["dep:network-local-control"]
control-http = ["dep:network-actions"]
plugins-native = ["dep:plugin-loading", "dep:plugin-abi"]
plugins-wasm = ["plugin-loading/wasm", "dep:wasm-sandbox"]
```

At least one of `l4` or `l7` is required for the runnable data-plane binary.

Examples:

```sh
cargo build --no-default-features --features l4,control-local,plugins-native
cargo build --no-default-features --features l7,control-local,plugins-native
cargo build --no-default-features --features l4,l7,control-local,plugins-native
```

The Rust build does not claim OpenList or rclone support unless a future native
application client plugin is actually implemented. This specification does not
substitute a sidecar for that missing plugin.

## Go build tags

Application tags select compiled lanes and client plugins:

```text
dataplane_l4
dataplane_l7
control_local
control_http
plugins_native
client_openlist
client_rclone
```

Examples:

```sh
go build -tags=dataplane_l4,control_local ./cmd/network-data-plane-worker
go build -tags=dataplane_l7,control_local ./cmd/network-data-plane-worker
go build -tags=dataplane_l4,dataplane_l7,control_local ./cmd/network-data-plane-worker
go build -tags=dataplane_l7,client_openlist,client_rclone,control_local \
  ./cmd/network-data-plane-worker
```

Build-tagged files belong to application composition. The infra `l4` and `l7`
packages themselves do not use product-selection tags.

# Capability manifest

Every binary exposes a read-only build manifest:

```json
{
  "schema": "network-data-plane-worker/capabilities/v1",
  "implementation": "go",
  "lanes": ["l7"],
  "controls": ["local"],
  "plugin_backends": ["native"],
  "client_plugins": ["openlist", "rclone"],
  "build_digest": "..."
}
```

Required commands:

```text
network-data-plane-worker --print-capabilities
network-data-plane-worker --check-config <file>
```

`--check-config` validates without binding listeners, loading untrusted
artifacts, or starting plugin/client instances.

A configuration that names an absent lane, plugin backend, or client plugin is a
startup error. There is no implicit fallback to another transport or client.

# Build matrix

| Profile | Implementation | L4 | L7 | Client plugins |
| --- | --- | ---: | ---: | --- |
| `rust-l4` | Rust/Pingora | yes | no | selected Rust-native clients only |
| `rust-l7` | Rust/Pingora | no | yes | selected Rust-native clients only |
| `rust-full` | Rust/Pingora | yes | yes | selected Rust-native clients only |
| `go-l4` | Go | yes | no | none by default |
| `go-l7` | Go | no | yes | selected by build tags |
| `go-l7-cloud` | Go | no | yes | OpenList and/or rclone |
| `go-full-cloud` | Go | yes | yes | OpenList and/or rclone |

# Migration plan

## Phase 1: document and rename

- Add this architecture contract.
- Rename the repository and binary from `pingora-worker` to
  `network-data-plane-worker`.
- Preserve Git history; update manifests, images, services, scripts and docs in
  the rename change.

## Phase 2: split Rust facility features

- Add `l4` and `l7` features to `proxy-pingora`.
- Move public APIs behind the appropriate feature.
- Keep shared internals private.
- Establish the three independent build checks.

## Phase 3: move worker semantics to the application

- Move worker control, typed plugin surfaces and application callback ordering
  out of the Pingora facility crate.
- Keep Pingora-native forwarding and session mechanics in infra.

## Phase 4: establish Go L4/L7 packages

- Select the L4 facility boundary.
- Select `net/http` plus `httputil.ReverseProxy` for L7.
- Add streaming, cancellation, upgrade, trailer, header-trust and drain tests.

## Phase 5: build the Go worker assembly

- Compose existing infra lifecycle, native plugin, action-route and local-control
  owners.
- Add lane/client generation pinning and capability manifests.

## Phase 6: add application client plugins

- Implement the OpenList client adapter over selected provider adapters.
- Implement the rclone client adapter over selected imported backends.
- Add conformance tests against the common Client contract.

## Phase 7: protocol plugins

- Implement S3/WebDAV/object APIs above the Client port.
- Keep protocol metadata and multipart state outside clients and infra.

# Required validation

## Build validation

```text
Rust: l4 only, l7 only, l4+l7
Go: l4 only, l7 only, l4+l7
Go: l7+OpenList, l7+rclone, l7+both
```

## Lifecycle validation

- candidate failure preserves the current generation;
- new exchanges use the published generation;
- old exchanges finish on the old generation;
- cancelled replacement/shutdown waiters do not abandon cleanup ownership;
- L4 and L7 drain independently;
- worker admission changes do not withdraw unrelated management/plugin routes;
- stale incarnation/generation mutations are rejected.

## Streaming validation

- large known-size PUT reaches rclone `Fs.Put` without whole-body buffering;
- unknown-size PUT uses `PutStream` or fails according to explicit policy;
- client abort cancels upstream/client work;
- slow downstream applies backpressure;
- trailers, Range, chunked bodies, upgrade and CONNECT preserve their contracts;
- AWS streaming SigV4 is decoded before client invocation;
- multipart completion writes one ordered logical object;
- no secrets or signed provider URLs leak through redirects or logs.

# Non-goals

This architecture does not promise:

- that a cloud drive becomes a fully compliant object store;
- distributed transaction or conditional-write atomicity;
- cross-language in-process plugin loading;
- a universal plugin callback model;
- dynamic code installation through worker configuration;
- automatic retry of consumed uploads or committed responses;
- provider sidecars introduced merely to bridge Go and Rust;
- OpenList or rclone dependencies inside infra.

# Definition of done

- The repository and binary use the `network-data-plane-worker` identity.
- Rust `proxy-pingora` independently builds with `l4`, `l7`, and `l4,l7`.
- Go exposes application-facing `proxy/l4` and `proxy/l7` facilities.
- Application build tags, not infra tags, select Go lanes and client plugins.
- Worker semantics and plugin/client lifecycle live in the application.
- L4 and L7 use separate typed surfaces and independent drain ownership.
- OpenList and rclone exist only as application client Tool plugins.
- The rclone plugin directly imports rclone packages and streams `io.Reader`
  bodies through `Put` or `PutStream`; it does not use CLI `copy` or `rcat`.
- S3 owns SigV4, multipart, ETag and metadata semantics above the Client port.
- Every exchange pins worker, lane, plugin and client generations.
- Replacement is prepare-publish-drain; failure preserves the old generation.
- Capability manifests and configuration validation fail closed.
- CI covers the complete Rust feature, Go build-tag, lifecycle and client
  conformance matrices.
