# Network data-plane worker architecture specification

## Status

This document defines the architecture and migration contract for
`network-data-plane-worker`. The Go Client contract, dynamic native adapter and
generation owner are implemented; the existing Rust L7 semantic chain remains a
separate application migration. It does not claim that every target item landed.

The canonical repository and application name is:

```text
network-data-plane-worker
```

Pingora is one Rust transport implementation. It is not the product identity.
The application is a generic, build-selectable data-plane worker with optional
L4 and L7 lanes, typed plugin contracts, managed generations, control entries,
drain, and shutdown.

## Specification ownership

This worker specification owns the data-plane plugin contract.

It defines:

- L4 and L7 facility boundaries;
- worker admission, revision, generation, drain, and completion semantics;
- policy, protocol, client, and management plugin contracts;
- the plugin descriptor and configuration-schema wire format;
- secret-field declaration and secret-resolution rules;
- plugin construction, publication, replacement, and cleanup semantics;
- capability manifests and control-plane compatibility checks;
- generic streaming, cancellation, and retry requirements.

A control plane consumes these contracts. It does not define or fork them.
A control plane may cache an approved descriptor, render a form from it, store
values that conform to it, and assign it to compatible workers, but the worker
contract remains the canonical schema authority.

This specification does not enumerate one product's concrete plugin inventory.
A concrete plugin implementation owns its own stable plugin ID, adapter IDs,
field declarations, secret declarations, capabilities, and provider behavior by
publishing a descriptor that conforms to this worker-owned contract.

## Decision summary

1. Infra exposes reusable L4 and L7 facilities. It does not own the runnable
   data-plane application.
2. Rust keeps one Pingora facility crate with independent `l4` and `l7` Cargo
   features and no default lane.
3. Go exposes exactly two data-plane-facing proxy facility packages: `l4` and
   `l7`. Application build tags select which lanes are linked.
4. The worker owns the plugin descriptor, configuration-schema, secret-field,
   capability, lifecycle, and execution contracts.
5. A product control plane consumes worker-published descriptors and desired
   state contracts. It cannot invent a plugin field, capability, or executable
   implementation that is absent from the approved installed artifact set.
6. L4 and L7 are peer facilities. They may share private mechanisms but never
   share one untyped protocol surface.
7. Policy, protocol, and client plugins use separate typed contracts. There is no
   universal `OnStage(string, any)` interface.
8. Installed, operator-approved shared-library artifacts define the plugin
   capability ceiling. Builds select lanes and native runtime mechanisms.
   Desired state may select only approved exact artifacts/descriptors and must
   fail closed when it names an absent capability. It cannot install code.
9. The control plane is not synchronously consulted for ordinary data-path
   execution.
10. Candidate replacement follows prepare, publish, pin, drain, and close. No
    consumed body or completed plugin operation is replayed across generations.

## System boundary

```text
                         PRODUCT CONTROL PLANE
                                  |
          desired state + sealed secrets + expected revisions
                                  |
                                  v
+---------------------------------------------------------------------+
|                  network-data-plane-worker                          |
|                                                                     |
| worker admission / revisions / generations / lifecycle / control   |
| plugin descriptor registry / rollback / drain / completion          |
|                                                                     |
|  +---------------------------+  +--------------------------------+  |
|  | L4 lane                   |  | L7 lane                        |  |
|  | TCP / UDP / TLS           |  | HTTP/1.1 / HTTP/2             |  |
|  | SNI / ALPN / pre-read     |  | reverse proxy / streaming     |  |
|  | raw stream / half-close   |  | trailers / upgrade / CONNECT  |  |
|  +-------------+-------------+  +----------------+---------------+  |
|                |                                 |                  |
|      typed L4 policy plugins          typed L7 policy/protocol      |
|                                                  |                  |
|                                      worker-owned Client contract   |
|                                                  |                  |
|                                  approved native client artifacts         |
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
│   ├── plugin/
│   │   ├── descriptor/
│   │   ├── policy/
│   │   ├── protocol/
│   │   └── client/
│   └── observed-state/
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
│       ├── plugincontract/
│       ├── client/
│       └── pluginruntime/
├── tests/
│   ├── build-matrix/
│   ├── contract-conformance/
│   ├── lifecycle/
│   └── cross-language/
└── docs/
```

Concrete product implementations are built as independent shared libraries and
loaded through the common native ABI. Their source location does not transfer
contract ownership: every implementation conforms to the worker-owned contract
and publishes a valid descriptor. The current Rust source remains at the root;
the illustrated `rust/` relocation is a separate source-layout migration.

# Infra contract

## Rust Pingora facility

The existing `proxy-pingora` crate remains an infra transport mechanism. Its
public lane surface becomes feature-gated:

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
- Worker control, plugin contracts, callback ordering, authorization, protocol
  semantics, and client selection remain outside `proxy-pingora`.
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

### `l4`

Owns:

- TCP/UDP listener custody;
- optional TLS termination;
- SNI, ALPN, and bounded pre-read facts;
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
It must not own product credentials, plugin schemas, provider accounts, client
inventory, or business routing.

Infra Go packages compile normally. Build tags live in application composition,
not inside these facilities.

## Reused infra components

The application composes focused infra owners instead of creating an infra-owned
product framework:

```text
runtime/pluginlifecycle   reversible resource graph and typed bindings
runtime/nativeplugin      trusted native plugin generations and custody
network/actionroute       independent action publications and drain
network/localcontrol      bounded same-host control carrier
network/httpworker        HTTP envelope/profile execution where applicable
network/applicationstream bounded HTTP/message stream mechanisms
network/http/egressclient bounded control/API fetches, not payload streaming
```

# Worker-owned plugin contract

## Contract namespaces

The first contract family is conceptually:

```text
network-data-plane-worker/plugin-descriptor/v2
network-data-plane-worker/client/v1
network-data-plane-worker/l4-policy/v1
network-data-plane-worker/l7-policy/v1
network-data-plane-worker/protocol/v1
network-data-plane-worker/observed-state/v1
network-data-plane-worker/management/v1
```

The exact Go, Rust, JSON, and control-wire representations may differ, but they
must preserve the same ownership, versioning, and lifecycle meaning.

## Plugin descriptor

Every installed plugin artifact publishes one passive descriptor. Reading the descriptor
must not open a socket, read a secret, authenticate to a provider, or construct a
live client.

Representative descriptor shape:

```json
{
  "schema": "network-data-plane-worker/plugin-descriptor/v2",
  "plugin_id": "vendor.capability",
  "plugin_version": "1.2.3",
  "contract": "network-data-plane-worker/client/v1",
  "runtime": "go",
  "build_digest": "sha256:...",
  "implementations": [
    {
      "id": "adapter-id",
      "display_name": "Adapter",
      "config_schema": { "type": "object", "additionalProperties": false },
      "capabilities": ["stat", "list", "open", "put", "delete"]
    }
  ]
}
```

The descriptor structure is worker-owned. The concrete values are plugin-owned.
The control plane may cache and approve a byte-identical descriptor, but it must
not produce a competing schema for the same plugin version.

## Configuration schema contract

Envelope v2 replaces the legacy custom field list with a standard JSON Schema
document. Each concrete plugin adapter generates its own public typed config
schema using Huma reflection, returns the structured schema from its owner,
and separately encodes it for the descriptor. Worker contract values remain
neutral JSON; they do not expose a framework-native schema type. The common
Infra configuration-codec facet uses an established sealed JSON Schema
validator without external schema loading. There is no legacy field-list
compatibility fallback.

The Worker interprets top-level string properties marked `writeOnly: true` as
secret fields and derives required presence from standard `required` metadata.
Secrets cannot publish defaults, examples, enumerations or constants. Nested
secret declarations fail closed until a corresponding reference contract exists.
Control planes consume these exact generated schemas without redefining them.

Rules:

- field paths and descriptor sizes are bounded;
- unknown descriptor fields are rejected for the declared contract version;
- secret fields are explicitly declared by the plugin descriptor;
- secret fields are write-only through ordinary management reads;
- a descriptor default must never contain a secret;
- descriptions and labels are untrusted display input and must be escaped;
- structural validation may occur in the control plane, but final typed
  validation belongs to the plugin constructor in the worker;
- a plugin version cannot silently change the meaning or secrecy of an existing
  field without a descriptor/version change;
- the descriptor digest is included in assignment and observed state.

## Secret resolution contract

A plugin constructor receives public configuration and a bounded secret resolver,
not a database record containing plaintext secrets.

Representative contract:

```go
type PluginConfig struct {
    Public  json.RawMessage
    Secrets SecretResolver
}

type SecretResolver interface {
    Resolve(context.Context, string) (Secret, error)
}

type Secret interface {
    Bytes() []byte
    Close()
}
```

The worker verifies that requested secret names were declared by the pinned
plugin descriptor. Secret buffers are scoped to construction or explicitly owned
runtime state, are excluded from logs and observed messages, and are cleared or
released according to the implementation's custody contract.

## Plugin factory and lifecycle

Representative generic plugin contract:

```go
type Plugin interface {
    Descriptor() PluginDescriptor
    Prepare(context.Context, PluginConfig) (Candidate, error)
}

type Candidate interface {
    Validate(context.Context) error
    Publish() (Instance, error)
    Abort(context.Context) error
}

type Instance interface {
    Close(context.Context) error
}
```

Concrete typed contracts narrow `Instance`, for example to `Client`, `L4Policy`,
`L7Policy`, or `Protocol`.

A plugin cannot publish arbitrary capabilities that are absent from its approved
descriptor. Registration validates exact contract versions and typed ports before
the instance is admitted.

## Client execution contract

A client capability is a Tool provider, not a policy/notifier participant.
It does not join L4/L7 authentication, routing, socket, header-patch, or completion
chains unless a separate policy plugin explicitly implements those contracts.

Representative Go contract:

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

The worker contract requires streaming input. It does not require a local file,
whole-body byte slice, CLI subprocess, or unbounded spool.

## Policy and protocol contracts

L4 policy receives bounded stream facts and may admit, select a logical route,
authorize a dial, observe connection establishment, and complete an exchange.
It does not receive an HTTP request object.

L7 policy receives bounded HTTP facts and may authenticate, authorize, select a
logical route, prepare allowed metadata, and complete an exchange. Sensitive
headers, raw queries, bodies, connections, and native handles require explicit
grants.

A protocol plugin owns its protocol semantics above L7. It may authenticate a
request, maintain protocol runtime state, select a client, and translate protocol
operations into the generic Client contract. The generic worker does not enumerate
one product's protocol IDs.

# Plugin registration and capability reporting

## Installed-artifact registration

Provider implementations are independently built `.so` artifacts, not a static
provider registry in the host executable. An operator installs an immutable
artifact and approves its actual ELF SHA-256. Passive descriptor inspection uses
that approved artifact without constructing provider clients or reading secrets.
The exact returned descriptor bytes are approved separately by SHA-256.
`build_digest` identifies build provenance; it is not falsely equated with a
self-embedded ELF hash.

The worker opens and hashes the artifact inode. Infra's native loader duplicates
and pins its descriptor for the complete mapping lifetime, including retained or
quarantined mappings. Immutable operator-governed installation remains a trust
boundary; an open file descriptor alone does not stop privileged in-place writes.
The worker's typed, bounded client wire reuses `InfraPluginV1` and the queried
`infra.async.v001` interface, including C-owned input/reply custody.

Go shared libraries declare Retain: instances drain and release owned resources,
but mapped Go runtime code remains retained. Explicit Rust unload is permitted
only after its runtime and callbacks are actually joined. Desired state can select
approved artifacts; it cannot download, write or install executable code.

The worker rejects:

- duplicate active plugin IDs for the same contract/version;
- a descriptor whose digest does not match the approved artifact;
- a plugin that publishes an undeclared typed capability;
- a configuration that names an implementation absent from the descriptor;
- a secret reference whose field is not declared secret;
- a control request for a contract version the worker does not support.

## Capability manifest

Every worker binary emits a passive build manifest:

```json
{
  "schema": "network-data-plane-worker/capabilities/v1",
  "implementation": "go",
  "lanes": ["l7"],
  "controls": ["local"],
  "plugin_runtimes": ["native"],
  "plugin_descriptors": [
    {
      "plugin_id": "vendor.capability",
      "contract": "network-data-plane-worker/client/v1",
      "plugin_version": "1.2.3",
      "descriptor_digest": "sha256:..."
    }
  ],
  "build_digest": "sha256:..."
}
```

The worker supplies the complete descriptor through its registration/control API
or an immutable artifact referenced by digest. The control plane does not infer a
plugin schema from a worker label or product name.

Required commands:

```text
network-data-plane-worker --print-capabilities
network-data-plane-worker --check-config <file>
```

`--check-config` validates descriptor references and structural desired state
without opening public listeners or constructing provider sessions.

# Worker lifecycle

## Exchange pinning

Every admitted exchange pins all mutable owners that it may call:

```text
worker revision
lane generation
policy/protocol plugin generations
selected client generation
plugin descriptor digest
```

A replacement affects only new exchanges. In-flight exchanges finish against
their pinned owners. No request or plugin call is replayed across generations.

## Startup

```text
1. Parse and validate bootstrap configuration.
2. Load approved installed native artifacts and validate exact descriptors.
3. Validate desired state against the pinned descriptor digests.
4. Resolve and construct required plugin candidates.
5. Prepare management publications.
6. Prepare selected L4/L7 lane generations.
7. Install all reversible resources.
8. Commit the composition.
9. Open data listeners.
10. Publish readiness and accepting=true.
```

No public listener opens before mandatory resources have been prepared.

## Replacement

```text
validate expected worker/plugin generation
resolve the exact descriptor digest
construct candidate with public config and sealed secrets
validate candidate
publish the new generation atomically
route new exchanges to the new generation
drain the old generation
close the old owner after actual work finishes
```

Candidate failure leaves the current generation active.

## Shutdown

```text
accepting=false
stop new lane admission
drain admitted exchanges
withdraw action and plugin publications
drain protocol/policy generations
drain client generations
close lane facilities
close management facilities
flush observability
return aggregated cleanup failures
```

L4 and L7 are independently closable resources. Closing one lane does not
implicitly close the other lane or unrelated management publications.

# Streaming and retry contract

## Request bodies

Large request bodies remain streams across protocol-to-client boundaries. The
worker must not silently convert an unbounded stream into memory or disk.

A client receives an `io.Reader` plus a known size or `-1`. A concrete plugin
publishes whether an implementation supports unknown-size streaming. If it does
not, the owning protocol/product policy must explicitly reject the request or use
a separately bounded spool owner.

## Response bodies

Read plans preserve streaming, cancellation, Range behavior where supported,
required upstream headers, and close ownership. A plan may use a direct upstream
HTTP target or a reader owned by the pinned client generation. The L7 lane applies
backpressure and never detaches the stream from its generation.

## Retry

The default is one attempt. Retry requires explicit replayability proof:

- no committed downstream response;
- no consumed non-replayable request body;
- no committed upstream mutation;
- a target within the admitted route/client policy;
- a remaining deadline and retry budget.

# Control-plane interaction

## Typed management registration

The Go `management` package owns the bounded HTTP SDK and fixed route
registrations for `network-data-plane-worker/management/v1`:

The `management/httpapi` boundary registers typed handlers directly with Huma's
official `humagin` adapter, independently of Gin. Huma derives schemas from
the boundary DTOs. `/v1/openapi.json` and `/v1/schemas` publish generated schemas;
the actual OpenAPI operation set determines route discovery. Provider payload
streams are outside the management DTO port. Descriptor responses use the
generated Worker descriptor schema while retaining byte-exact raw response data.

| Method | Route | Meaning |
| --- | --- | --- |
| GET | `/v1/manifest` | Passive Worker capability manifest |
| GET | `/v1/descriptors/{digest}` | Byte-exact approved descriptor |
| GET | `/v1/state` | Sanitized published generation and drain observations |
| POST | `/v1/clients/{name}` | One expected-generation CAS replacement |

The product composition authenticates and authorizes access before dispatching
to the registered handler. Client names and descriptor inventory are frozen from
operator-approved registration. Unknown methods, routes, query parameters,
unregistered client names, unknown JSON fields, and frames over 1 MiB fail closed.
Management contains no object-body method or arbitrary upstream proxy operation.
The descriptor endpoint preserves exact bytes and the SDK verifies the expected
SHA-256 before returning approval material. Updates never include resolved secret
values; they reference the Worker's existing operator-controlled resolver.

Control planes may register concrete rclone or origin-specific administrative
SDK clients under their own authenticated routes. Those clients own their own
provider-specific management vocabulary. They do not expand the generic payload
Client contract, redefine a Worker descriptor, or put management RPCs on the
ordinary data path. The common Worker SDK binds a configured origin, refuses
redirects, sanitizes remote diagnostics, and bounds each call to 30 seconds.
It sends one CAS attempt without automatic retry; a timed-out update can already
have published, so callers reconcile observations before another attempt.

## Desired state

The control plane sends a versioned snapshot that references exact worker-owned
contract versions and descriptor digests. Product-specific plugin configuration
remains typed by the selected plugin descriptor.

The generic control path treats plugin configuration as bounded bytes plus an
exact descriptor reference. It does not interpret provider behavior.

## Applied and observed state

The worker reports:

```text
worker incarnation
source and snapshot revision
applied digest
lane states
plugin ID / contract / descriptor digest
plugin and client runtime generations
sanitized failure codes
readiness / accepting / draining
```

An ACK means the requested generation was published. It does not prove that every
old generation has completed drain unless observed state explicitly says so.

## Control-plane outage

The worker keeps its last-known-good applied generation. Existing listeners,
client sessions, and admitted streams continue independently of control-plane
availability. Reconnection never replays data-path operations.

# Build selection

## Rust Cargo features

Representative generic application features:

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

At least one of `l4` or `l7` is required for a runnable data-plane binary.
Concrete providers are independent approved shared libraries, not Cargo features
in infra or a statically registered product inventory.

## Go build tags

Generic application tags select lanes and runtime facilities:

```text
dataplane_l4
dataplane_l7
control_local
control_http
plugins_native
```

Product compositions select lanes/runtime facilities with tags. Concrete provider
code is built into separate `.so` artifacts and loaded through this worker
contract, not imported into the host executable to register provider factories.

# Required validation

## Contract validation

- descriptor decoding rejects unknown or oversized fields;
- descriptor digests are stable and included in worker manifests;
- secret fields are write-only and cannot declare secret defaults;
- public configuration cannot request undeclared secrets;
- plugin constructors perform final typed validation;
- duplicate plugin IDs and incompatible contract versions fail closed;
- control-plane cached descriptors cannot override worker-published descriptors.

## Build validation

```text
Rust: l4 only, l7 only, l4+l7
Go: l4 only, l7 only, l4+l7
Each selected plugin composition: descriptor and contract conformance
```

## Lifecycle validation

- candidate failure preserves the current generation;
- new exchanges use the published generation;
- old exchanges finish on the old generation;
- cancelled replacement/shutdown waiters do not abandon cleanup ownership;
- L4 and L7 drain independently;
- worker admission changes do not withdraw unrelated management routes;
- stale incarnation/generation mutations are rejected.

## Streaming validation

- large known-size PUT stays streaming;
- unknown-size PUT follows the selected plugin's declared capability;
- client abort cancels upstream work;
- slow downstream applies backpressure;
- trailers, Range, chunked bodies, upgrade, and CONNECT preserve contracts;
- no secret or signed upstream URL leaks through logs or generic observed state.

# Non-goals

This architecture does not promise:

- a universal untyped plugin callback;
- control-plane ownership of plugin schemas;
- dynamic code installation through desired-state configuration;
- automatic retry of consumed uploads or committed responses;
- direct language-runtime ABI coupling instead of the versioned native C ABI;
- identical Go and Rust APIs;
- full object-store semantics from every client implementation.

# Definition of done

- The repository and binary use the `network-data-plane-worker` identity.
- Rust `proxy-pingora` independently builds with `l4`, `l7`, and `l4,l7`.
- Go exposes application-facing `proxy/l4` and `proxy/l7` facilities.
- The worker owns versioned plugin descriptor, config schema, secret, lifecycle,
  capability, and execution contracts.
- Control planes consume exact worker-published descriptors and cannot redefine
  them.
- L4 and L7 use separate typed surfaces and independent drain ownership.
- Every exchange pins worker, lane, plugin, descriptor, and client generations.
- Replacement is prepare-publish-drain; failure preserves the old generation.
- Large bodies remain streams and are never silently whole-buffered.
- Capability manifests and desired-state validation fail closed.
- CI covers feature/build matrices, contract conformance, lifecycle, and
  streaming behavior.
