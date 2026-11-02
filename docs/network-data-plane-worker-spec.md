# Network data-plane worker architecture specification

## Status

This document defines the target architecture for the repository currently named
`pingora-worker`. It is a design contract and migration plan. It does not claim
that every item has already landed.

The canonical future repository and application name is:

```text
network-data-plane-worker
```

`Pingora` is one Rust transport implementation. It is not the product identity.
The application is a generic, build-selectable data-plane worker with optional
L4 and L7 lanes, typed extension surfaces, managed generations, control entries,
drain, and shutdown.

## Specification ownership

This document owns only the generic data-plane execution model:

- L4 and L7 facility boundaries;
- worker admission, revision, generation, drain, and completion semantics;
- typed policy, protocol, and client extension contracts;
- plugin/runtime lifecycle mechanics;
- capability discovery and build-selection rules;
- generic streaming and cancellation requirements.

It does **not** specify a product's concrete plugin catalog. The following belong
to the owning product or control-plane specification:

- concrete client plugin IDs and names;
- provider or backend adapter inventories;
- plugin-specific configuration schemas;
- plugin-specific secret fields;
- product build profiles that select concrete clients;
- protocol inventory exposed by one product;
- provider-specific upload, retry, token, direct-link, and metadata behavior;
- migration of an existing product's storage/account model.

For the OpenList product, those decisions are canonical in:

```text
wignerStan/OpenList/docs/openlist-control-plane-spec.md
```

The data-plane worker consumes only versioned desired state and compiled
capabilities. It does not define OpenList, rclone, or any other product-specific
client integration.

## Decision summary

1. Infra exposes reusable L4 and L7 facilities. It does not own the runnable
   data-plane application.
2. Rust keeps one Pingora facility crate with independent `l4` and `l7` Cargo
   features and no default lane.
3. Go exposes exactly two data-plane-facing proxy facility packages: `l4` and
   `l7`. Application build tags select which lanes are linked.
4. The application repository contains Rust and Go implementations. They build
   as separate processes and share contracts, lifecycle semantics, build
   profiles, and conformance tests; they do not use FFI to combine runtimes.
5. Worker admission, generation publication, extension lifecycle, management
   actions, and completion belong to the application worker core, not to either
   L4 or L7 facility.
6. L4 and L7 are peer facilities. They may share private mechanisms but never
   share one untyped protocol surface.
7. Policy, protocol, and client capabilities are installed through typed
   application-owned extension contracts.
8. Concrete extension inventories are external product decisions and are not
   enumerated by this specification.
9. Compilation defines the maximum capability set. Runtime configuration may
   select only a subset and must fail closed when it names a capability absent
   from the binary.
10. A control plane distributes desired state and receives observed state. It is
    not synchronously consulted for ordinary data-path execution.

## System boundary

```text
                         PRODUCT CONTROL PLANE
                                  |
                signed desired state / secret lease
                                  |
                                  v
+---------------------------------------------------------------------+
|                  network-data-plane-worker                          |
|                                                                     |
| worker admission / revisions / generations / lifecycle / control   |
| rollback / drain / completion / capability reporting                |
|                                                                     |
|  +---------------------------+  +--------------------------------+  |
|  | L4 lane                   |  | L7 lane                        |  |
|  | TCP / UDP / TLS           |  | HTTP/1.1 / HTTP/2             |  |
|  | SNI / ALPN / pre-read     |  | reverse proxy / streaming     |  |
|  | raw stream / half-close   |  | trailers / upgrade / CONNECT  |  |
|  +-------------+-------------+  +----------------+---------------+  |
|                |                                 |                  |
|      typed L4 policy surface          typed L7 policy/protocol      |
|                                                  |                  |
|                                       application Client port       |
|                                                  |                  |
|                                  compiled product client set        |
+---------------------------------------------------------------------+
```

The control plane never carries ordinary payload bytes and is not a synchronous
request-path dependency. A worker with a last-known-good applied revision
continues to serve while the control plane is temporarily unavailable.

## Repository layout after rename

```text
network-data-plane-worker/
├── README.md
├── SECURITY.md
├── contracts/
│   ├── control/
│   ├── worker/
│   ├── extension/
│   └── client/
├── rust/
│   ├── Cargo.toml
│   ├── src/main.rs
│   └── crates/
│       ├── worker-core/
│       ├── worker-assembly/
│       ├── lane-l4/
│       ├── lane-l7/
│       └── extension-bindings/
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
│       └── extensions/
├── tests/
│   ├── build-matrix/
│   ├── conformance/
│   ├── lifecycle/
│   └── cross-language/
└── docs/
```

Concrete product extension packages may be linked by a selected application
composition, but their names, schemas, and behavior are not part of this generic
repository specification.

The Rust and Go binaries are independent build profiles. Cross-language
interoperability is required only for deliberately shared control contracts;
ordinary data streams do not cross a language bridge merely because both
implementations exist in the same repository.

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
  # raw listener, stream, socket, TLS/SNI/ALPN dependencies
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

- `l4` and `l7` compile independently.
- Neither public lane imports the other lane's public API.
- Private shared mechanisms are permitted but cannot become a public umbrella.
- L4 owns raw listeners, streams, half-close, byte transfer, optional TLS
  termination, bounded pre-read, SNI/ALPN facts, and transport drain.
- L7 owns HTTP framing, request/response streaming, trailers, Range mechanics,
  reverse proxying, upgrades, CONNECT, and HTTP connection lifecycle.
- Worker control, callback ordering, authorization, extension membership,
  protocol semantics, and client selection remain outside `proxy-pingora`.
- An application protocol does not become a root Pingora facility merely because
  it uses HTTP.

The existing worker semantic chain moves to this application's `worker-core`.
The Pingora crate supplies transport effects and native carrier hooks only.

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

It must not import `net/http`, product protocol code, product clients, or the
application worker.

### `l7`

Owns:

- HTTP server lifecycle;
- HTTP/1.1 and HTTP/2 framing;
- streaming reverse proxying;
- Range mechanics, trailers, flushing, upgrades, and CONNECT;
- request/response header trust boundaries;
- downstream/upstream cancellation and response-commit tracking;
- graceful drain.

Its selected standard mechanism is `net/http` plus
`net/http/httputil.ReverseProxy` and caller-owned `http.RoundTripper` values.
It must not own product credentials, provider accounts, client inventory, or
business routing.

Infra Go packages compile normally. Build tags live in application composition,
not inside these facilities.

## Reused infra components

The application composes focused infra owners instead of creating an application
framework inside infra:

```text
runtime/pluginlifecycle   reversible resource graph and typed bindings
runtime/nativeplugin      trusted native extension generations and custody
network/actionroute       independent action publications and drain
network/localcontrol      bounded same-host control carrier
network/httpworker        HTTP envelope/profile execution where applicable
network/applicationstream bounded HTTP/message stream mechanisms
network/http/egressclient bounded control/API fetches, not payload streaming
```

The buffered egress client must not be placed in a large-object data path. Safe
DNS, address-admission, cancellation, and dial behavior should be factored into a
reusable transport capability when the L7 facility needs the same protections.

# Application worker core

## Responsibilities

The generic worker core owns:

```text
accepting / draining state
immutable desired and applied revisions
lane generation publication
extension and client generation publication
typed capability binding
management action publication
prepare / commit / rollback
completion and accounting
ordered shutdown
```

It does not parse HTTP, copy raw TCP streams, or implement product/provider APIs.

## Exchange pinning

Every admitted exchange pins all mutable owners that it may call:

```text
worker revision
lane generation
policy/protocol extension generations
selected client generation
```

A replacement affects only new exchanges. In-flight exchanges finish against
their pinned owners. No request or extension call is replayed across generations.

## Startup

```text
1. Parse and validate configuration.
2. Validate it against the binary capability manifest.
3. Construct required extension and client candidates.
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
route new exchanges to the new generation
drain the old generation
close the old owner after actual work finishes
```

Candidate failure leaves the current generation active. A control mutation must
include a manager incarnation and expected generation so stale commands cannot
withdraw a newer installation.

## Shutdown

```text
accepting=false
stop new lane admission
drain admitted exchanges
withdraw action and extension publications
drain extension generations
drain client generations
close lane facilities
close management facilities
flush observability
return aggregated cleanup failures
```

L4 and L7 are independently closable resources. Closing one lane does not
implicitly close the other lane or unrelated management publications.

# Typed extension model

There is no universal `OnStage(string, any)` interface.

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

## L7 policy surface

Representative operations:

```text
authenticate
authorize
select logical route
prepare request metadata
complete
```

Inputs contain bounded typed HTTP facts. Sensitive headers, raw queries, bodies,
connections, and native handles are exposed only through explicit grants.

## Protocol surface

A protocol extension owns its own wire/application semantics above the L7
facility. It may authenticate requests, maintain protocol state, select a client,
and translate protocol operations into the generic Client contract.

The generic worker does not enumerate protocol extension IDs or product protocol
inventory. Those belong to the owning product specification.

## Client Tool surface

A client capability is a Tool provider, not a policy/notifier participant. It
does not join L4/L7 authentication, routing, socket, header-patch, or completion
chains unless a separate policy implementation explicitly participates there.

Representative Go execution contract:

```go
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

This specification intentionally does not define:

- concrete client plugin IDs;
- backend imports or adapter classes;
- plugin-specific configuration/secret schemas;
- backend-specific known- or unknown-size upload choices;
- product-specific fallback, spool, retry, or direct-link policy;
- concrete client build tags.

Those decisions are owned by the corresponding product/control-plane spec.

# Streaming contract

## Request bodies

Large request bodies remain streams across the protocol-to-client boundary.
The generic contract must not require:

```text
whole-body []byte
whole-object in-memory buffering
mandatory temporary files
CLI subprocess invocation
```

A client receives `io.Reader` plus a known size or `-1` for unknown size. The
selected client capability decides whether it can consume that stream directly.
If it cannot, the owning product protocol policy must explicitly choose rejection
or a bounded spool strategy. The generic worker never silently converts an
unbounded stream into memory or disk.

## Response bodies

Read plans preserve streaming, cancellation, Range behavior where supported,
required upstream headers, and close ownership. A plan may use a direct upstream
HTTP target or a reader owned by the client generation. The L7 lane applies
backpressure and never detaches the stream from its pinned generation.

## Retry

The default is one attempt. Retry requires explicit replayability proof:

- no committed downstream response;
- no consumed non-replayable request body;
- no already-committed upstream mutation;
- a retry target within the admitted route/client policy;
- a remaining deadline and retry budget.

Generic infrastructure must not infer that an upload or transformed stream is
safe to replay.

# Control and observed state

## Desired state

The worker accepts a versioned, validated desired snapshot containing only
resources admitted by its compiled capability manifest. Product-specific payloads
remain opaque to generic control plumbing until delivered to their owning typed
extension constructor.

## Applied state

The worker reports:

```text
worker incarnation
source/snapshot revision
applied digest
lane states
extension/client generation states
sanitized failure codes
readiness / accepting / draining
```

A control-plane ACK means the worker published the requested generation. It does
not mean every old generation has physically completed drain unless the observed
state explicitly reports that fact.

## Control-plane outage

The worker keeps its last-known-good applied generation. Existing listeners,
client sessions, and admitted streams continue independently of control-plane
availability. Control reconnection never replays data-path operations.

# Application build selection

## Rust Cargo features

Representative generic application features:

```toml
[features]
default = []
l4 = ["proxy-pingora/l4"]
l7 = ["proxy-pingora/l7"]
control-local = ["dep:network-local-control"]
control-http = ["dep:network-actions"]
extensions-native = ["dep:plugin-loading", "dep:plugin-abi"]
extensions-wasm = ["plugin-loading/wasm", "dep:wasm-sandbox"]
```

At least one of `l4` or `l7` is required for the runnable data-plane binary.

Examples:

```sh
cargo build --no-default-features --features l4,control-local,extensions-native
cargo build --no-default-features --features l7,control-local,extensions-native
cargo build --no-default-features --features l4,l7,control-local,extensions-native
```

Concrete product client features are declared by the product composition, not by
this generic specification.

## Go build tags

Generic application tags select lanes and runtime facilities:

```text
dataplane_l4
dataplane_l7
control_local
control_http
extensions_native
```

Examples:

```sh
go build -tags=dataplane_l4,control_local ./cmd/network-data-plane-worker
go build -tags=dataplane_l7,control_local ./cmd/network-data-plane-worker
go build -tags=dataplane_l4,dataplane_l7,control_local \
  ./cmd/network-data-plane-worker
```

Product-owned composition may add product-specific tags in its own specification.
The infra `l4` and `l7` packages themselves do not use product-selection tags.

# Capability manifest

Every binary exposes a read-only build manifest:

```json
{
  "schema": "network-data-plane-worker/capabilities/v1",
  "implementation": "go",
  "lanes": ["l7"],
  "controls": ["local"],
  "extension_backends": ["native"],
  "extensions": [
    {
      "kind": "client",
      "id": "product-defined",
      "build_digest": "..."
    }
  ],
  "build_digest": "..."
}
```

`product-defined` is illustrative; this specification does not reserve or define
concrete IDs.

Required commands:

```text
network-data-plane-worker --print-capabilities
network-data-plane-worker --check-config <file>
```

`--check-config` validates without binding listeners, loading untrusted artifacts,
or constructing live client sessions.

A configuration that names an absent lane, extension backend, or compiled
extension is a startup error. There is no implicit fallback to another transport
or client.

# Generic build matrix

| Profile | Implementation | L4 | L7 | Concrete product extensions |
| --- | --- | ---: | ---: | --- |
| `rust-l4` | Rust/Pingora | yes | no | product-selected |
| `rust-l7` | Rust/Pingora | no | yes | product-selected |
| `rust-full` | Rust/Pingora | yes | yes | product-selected |
| `go-l4` | Go | yes | no | product-selected |
| `go-l7` | Go | no | yes | product-selected |
| `go-full` | Go | yes | yes | product-selected |

The owning product specification defines any additional named profiles and their
concrete extension inventory.

# Security requirements

- Extension metadata exposure is deny-by-default and grant-based.
- Native extensions have process authority and are trusted artifacts.
- Sandboxed extension backends receive only explicitly bound capabilities.
- Management reachability is not authentication; production control requires a
  selected workload/peer identity mechanism.
- Artifact identity fields do not prove artifact provenance. Product builds must
  use reviewed digests, signatures, or an equivalent admission policy.
- Secrets never appear in capability manifests, logs, status messages, or audit
  summaries.
- Header trust boundaries remove or replace caller-controlled forwarding and
  credential headers before upstream execution.
- Redirect handling must not leak signed targets or provider credentials.
- Cancellation is not physical completion. Resource custody remains pinned until
  the owning operation actually settles or is quarantined.

# Migration plan

## Phase 1: document and rename

- Keep this generic architecture contract.
- Rename the repository and binary from `pingora-worker` to
  `network-data-plane-worker`.
- Preserve Git history and update manifests, images, services, scripts, and docs.

## Phase 2: split Rust facility features

- Add independent `l4` and `l7` features to `proxy-pingora`.
- Move public APIs behind the appropriate feature.
- Keep shared internals private.
- Establish the three independent build checks.

## Phase 3: move worker semantics to the application

- Move worker control, typed extension surfaces, and application callback order
  out of the Pingora facility crate.
- Keep Pingora-native forwarding and session mechanics in infra.

## Phase 4: establish Go L4/L7 packages

- Select the L4 facility boundary.
- Select `net/http` plus `httputil.ReverseProxy` for L7.
- Add streaming, cancellation, upgrade, trailer, header-trust, and drain tests.

## Phase 5: build generic worker assembly

- Compose existing infra lifecycle, native extension, action-route, and
  local-control owners.
- Add lane/extension/client generation pinning and capability manifests.

## Phase 6: establish generic client conformance

- Implement the application Client contract and client-generation registry.
- Add reusable conformance tests against fake/reference clients.
- Do not add product adapter inventory to this specification.

## Phase 7: integrate product compositions

- Let each product specification define its protocol and client extension set.
- Validate desired state against the exact binary manifest.
- Keep product migration and provider behavior in the owning product repository.

# Required validation

## Build validation

```text
Rust: l4 only, l7 only, l4+l7
Go: l4 only, l7 only, l4+l7
```

Product repositories add their own extension-specific build matrix.

## Lifecycle validation

- candidate failure preserves the current generation;
- new exchanges use the published generation;
- old exchanges finish on the old generation;
- cancelled replacement/shutdown waiters do not abandon cleanup ownership;
- L4 and L7 drain independently;
- worker admission changes do not withdraw unrelated management publications;
- stale incarnation/generation mutations are rejected.

## Streaming validation

- large known-size writes cross the Client boundary without whole-body buffering;
- unknown-size writes either use a declared streaming capability or fail/spool
  according to explicit product policy;
- client abort cancels upstream/client work;
- slow downstream applies backpressure;
- trailers, Range, chunked bodies, upgrade, and CONNECT preserve their contracts;
- protocol framing is decoded before client invocation;
- multipart-like protocol completion produces one ordered logical object or an
  explicitly selected bounded composition strategy;
- secrets and signed upstream targets do not leak through redirects or logs.

# Non-goals

This architecture does not promise:

- that any provider becomes a fully compliant object store;
- distributed transaction or conditional-write atomicity;
- cross-language in-process extension loading;
- a universal callback model;
- dynamic code installation through worker configuration;
- automatic retry of consumed uploads or committed responses;
- product-specific client inventories or adapter schemas;
- product-specific sidecars introduced merely to bridge Go and Rust.

# Definition of done

- The repository and binary use the `network-data-plane-worker` identity.
- Rust `proxy-pingora` independently builds with `l4`, `l7`, and `l4,l7`.
- Go exposes application-facing `proxy/l4` and `proxy/l7` facilities.
- Application build tags, not infra tags, select Go lanes and runtime facilities.
- Worker semantics and extension/client lifecycle live in the application.
- L4 and L7 use separate typed surfaces and independent drain ownership.
- The generic spec contains no concrete product plugin IDs, backend imports,
  provider inventories, or product build profiles.
- Concrete client/plugin behavior is specified by the owning product control
  plane and validated against the worker capability manifest.
- Large bodies stay streams across protocol and Client boundaries.
- Every exchange pins worker, lane, extension, and client generations.
- Replacement is prepare-publish-drain; failure preserves the old generation.
- Capability manifests and configuration validation fail closed.
- CI covers the complete Rust feature, Go build-tag, lifecycle, streaming, and
  generic client-conformance matrices.
