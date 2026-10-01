# Pingora worker

A runnable Pingora application with explicit configuration, assembly, plugin
runtime and process entry. `main` loads configuration; `assembly` installs native
notifier Function/Object instances and optional management entry in `plugin-lifecycle`; the
native `pingora-proxy::ProxyHttp` engine owns forwarding, streaming, upgrades and
connection pooling. The library also exposes composition APIs for other selected
applications. Business policy belongs to separately installed plugins.

## Run the application

Save a JSON configuration, for example:

```json
{
  "listen": "127.0.0.1:6188",
  "upstream": "http://127.0.0.1:8080",
  "pool": { "enabled": true, "idle_timeout_seconds": 90, "isolation_key": 0 },
  "logging": {
    "filter": "info,proxy_pingora::worker::notifier=trace",
    "access_file": { "directory": "logs/access", "keep_files": 30 }
  },
  "management": { "listen": "127.0.0.1:6189" },
  "callbacks": []
}
```

```sh
cargo run --locked -- --config worker.json --check-config
cargo run --locked -- --config worker.json
```

Validation does not load plugins or bind listeners. The upstream is a fixed
HTTP(S) origin; request paths and queries are preserved. Optional `proxy` selects
an HTTP CONNECT, HTTPS CONNECT, SOCKS5 or SOCKS5h upstream bridge. A refused
selected proxy never falls back to direct access. TLS ingress uses
`tls: {"certificate": "server.crt", "key": "server.key"}`. Configuration rejects
unknown fields, invalid origins and unbounded timeouts/capacities.

Native Pingora types remain directly available through their own crates. Infra
adds lifecycle ordering, explicit resource ownership and fixes for transport
adaptation; forwarding and native pooling stay in Pingora.
Direct, HTTP CONNECT and SOCKS connections can use Pingora's pool. HTTPS proxy
tunnels currently bypass reuse because the custom L4 TLS stream lacks the native
socket identity required by idle-pool validation. Pool isolation includes selected
upstream, bridge and credentials; those keys are never logged. Admitted requests
are attempted once, with no automatic replay.

SIGTERM/SIGINT stop native workers and pools before plugin teardown. Management
routes are optional and loopback-only. Installed native calls retain their owners
through cancellation until the synchronous operation really returns.

## Native metadata callbacks

Each `callbacks` entry explicitly selects a trusted native Function or Object library:

```json
{
  "path": "plugins/policy.so",
  "package": "example.policy",
  "variant": "native",
  "language": "rust",
  "hooks": ["pre_routing", "authenticate", "admit", "complete"],
  "headers": ["x-request-id"],
  "patch_headers": ["x-request-id"],
  "config": {},
  "timeout_ms": 5000,
  "max_inflight": 32
}
```

Native `.so` callbacks default to Object, independently of Rust or Go language.
Object and Tool are native component roles; Tools are consumed through explicit
runtime dependency bindings, not installed as notifier participants. Function is
the LuaJIT/V8 execution role and is not inferred from a native library's language.
This native-only `callbacks` configuration does not select a LuaJIT/V8 engine.
Explicit `kind: "function"` remains available only for existing native Function ABI
artifacts; otherwise `kind` is `object`. Selection must match the artifact header. Object methods are serialized per instance within the callback deadline,
while Functions retain configured concurrency. Go libraries remain mapped after
shutdown; active calls drain before native instance cleanup.

Both kinds must implement the configured hook methods and the metadata/reply
contract below. Object loading does not translate arbitrary business method
contracts or grant dependency bindings; dependency-bearing instances belong in
runtime graph composition.

The native worker enforces this sequence:

1. Validate the already-parsed ingress protocol (`pre_ingress`). Raw negotiation
   and HTTP parsing belong to Pingora's carrier.
2. Apply metadata/header transforms in registration order (`pre_routing`).
3. Authenticate the resulting identity (`authenticate`).
4. Select the configured route and admit execution (`routing`, `admit`).
5. Configure native request-body transforms (`prepare_request_body`).
6. Finalize outbound headers and encoding (`before_upstream_send`).
7. Gate pool checkout and dialing, acquire/connect, authorize socket use, then
   send upstream (`pool`, `before_dial`, `socket`).
8. Process the final upstream response (`upstream_response`).
9. Configure native response-body transforms (`prepare_response_body`).
10. Finalize downstream headers and encoding (`before_downstream_write`).
11. Stream downstream and attempt the admitting callback's `complete` once.

Finalization runs before any upstream acquisition. Pingora's later
`upstream_request_filter` only publishes the prepared head with native protocol
framing; it does not replay notifier callbacks. An owned outbound head preserves
the original ingress framing used by Pingora's body decoder. Informational
responses such as `103` do not repeat final-response stages. Upgrade traffic stays
with Pingora's duplex carrier and bypasses HTTP body transforms and body dumps.
Callbacks cannot change the parsed method/protocol or manufacture an upgrade.

Native and compiled callbacks can gate pool checkout, dialing, and socket use.
Pingora owns pool return; this worker never fabricates `before_return` or
`before_reuse` events after that boundary has passed. Socket `before_use` runs on
both fresh and reused connections and exposes `upstream_reused` metadata.
Admission closure is checked again after pending request callbacks. Callback
phases have bounded waits, and acquisition shares its budget with control hooks
and name resolution. Failed/cancelled or repeated lifecycle phases cannot advance
or report completion. Automatic request replay remains disabled.

The callback method is the hook name. Its bounded JSON frame contains
`version: 1`, `hook`, `request_id`, `method`, query-free `path`, optional `status`,
`upstream_reused`, `completed`, and ordered `headers` pairs selected by the explicit
allowlist. No headers are exposed by default. Bodies, Futures and asynchronous
streams never cross this ABI. Input and reply budgets are 64 KiB; infra visits
the borrowed native reply before releasing its allocation.

Replies contain `{"version":1,"decision":"continue"}`, `accept`, or `deny`, with
optional `set_headers` pairs and `remove_headers` names. `accept` selects an
authentication/admission owner; `continue` defers those decisions. The final
compiled policy accepts the configured fixed route if no plugin denied it.
Network hooks add an optional bounded `network` object. `before_dial` includes a
structured host/port, TCP address, or Unix path plus timeout and TCP options;
`pool` includes phase metadata, and `socket` includes its ownership phase. Frames
exclude raw descriptors, pool entry IDs, URL credentials, and stream objects.

| Hook | `accept` | `deny` | `continue` |
| --- | --- | --- | --- |
| `before_dial` | Allow acquisition | Refuse acquisition | Ask the next participant |
| `pool` | Permit reuse within configured limits | Discard/bypass reuse | Ask the next participant |
| `socket` | Permit socket use | Close/refuse use | Ask the next participant |

Network replies forbid header patches. A pool discard still permits a fresh
connection; a dial refusal occurs before DNS/connection work. Native metadata
frames borrow the live request method/path or response status and headers while
encoding. Header snapshots for decision-only hooks are taken once per completed
request-head stage, rather than once per native callback patch.

Host configuration grants `headers` for reading and `patch_headers` for writing;
both default to empty and neither implies the other. Existing plugins that patch
headers must explicitly configure their write grant. Every set/remove operation
must be granted, and the entire batch is checked before any mutation. Plugins
cannot widen these grants in their reply. Configured `hooks` independently limits
which methods the host may invoke.

Transform stages apply validated patches to the native head; authentication,
admission and completion forbid patches. Framing/hop headers and authority cannot
be changed by these metadata replies. Errors and denial stop the chain; malformed
patch batches leave the head unchanged. A completion error or cancellation cannot
start completion again.

Diagnostics contain stage, callback and decision metadata. Access events use a
separate stdout/file sink. To capture bounded sanitized request JSON, explicitly
configure both `logging.body_file` and `logging.body_max_bytes` (at most 1 MiB).
Malformed, incomplete and oversized bodies emit an outcome without body text;
response bodies are never captured. Credential fields and selected session cookies
are fingerprinted while ordinary cookies stay useful. Global TRACE does not enable
capture, and raw native wire/header debug targets are excluded from this standard
logging path. Access and body retention are independent.

## WebSocket capability boundary

| Capability | Current native worker | Owner |
| --- | --- | --- |
| Upgrade request/response metadata decisions | Supported within configured hook/header grants | Worker notifier chain |
| Pool, dial, and acquired socket decisions | Supported before native dispatch | Worker controls and Pingora |
| Bidirectional bytes after 101 | Transparent native forwarding with backpressure | Pingora carrier |
| WebSocket frame parsing / per-frame Go calls | Not implemented by this worker | Requires a native codec/carrier adapter |
| Whole-message business decisions or rewrites | Not implemented by this worker | Plugin policy over an explicit message capability |

HTTP body transforms and body dumps are bypassed after upgrade. Handshake fields
(`Sec-WebSocket-Key`, `Accept`, `Version`, `Protocol`, `Extensions`, with the
`Sec-WebSocket-` prefix) cannot be changed by native metadata replies, even with a
write grant. Compiled hooks must preserve the original handshake metadata too.
Otherwise transparent forwarding could advertise an encoding or subprotocol it
does not implement.

Frame parsing, masking, fragmentation, ping/pong/close handling, limits and
backpressure are protocol infrastructure, not product policy. Go plugins should
own business interpretation and decisions. Existing async callbacks cannot simply
be inserted into both HTTP body filters: Pingora's downstream body filter is
synchronous, chunks are not WebSocket frames, and 101 can arrive with preread frame
bytes. A future frame capability needs an owned native codec/duplex loop, ordered
bounded callback waits, explicit directional grants and cancellation; detached
notifications must not pretend to gate bytes already forwarded. No body stream or
future crosses the C ABI.

These host grants constrain the notifier contract. Trusted in-process `.so` code
is not an OS sandbox and must not be treated as an untrusted isolated plugin.

## Ownership and calls

- **Worker axis:** infra's Pingora carrier, concurrency/drain mechanics, worker
  management state, semantic notifier contracts, and native egress provider port.
  A contract/trait belongs to the worker surface; its installed implementation
  and resource lifetime belong to plugin composition.
- **Plugin axis:** the runtime dynamically composes Object/Function/Tool instances,
  installs notifier implementations, retains tool/client resources, and owns
  HTTP/gRPC entry handles and reversible action registration. Each Object plugin
  orchestrates its own useflow through explicitly injected Tools.
- **Function callbacks:** compiled functions or optional V8/Lua functions return
  decisions from worker notification facts. They are not substitutes for the
  Object plugin's independently initiated workflow. No VM enters the proxy data
  path by default.

    Pingora ingress -> runtime-installed callback chain -> native egress provider
    Plugin Object useflow -> declared Tool binding -> effect
    Internal gRPC -> shared router -> worker action OR explicit plugin action

Tool calls do not traverse the notifier chain. Worker callback exposure is not an
HTTP/gRPC route allowlist. Action contracts are distinct from transport bindings;
entry registration belongs to plugin composition. PG/cache Tool dependencies are
injected directly in the plugin axis. There is no universal RequestPlan layer.

`entry::install` binds one selected internal endpoint. `Registration::worker_control`
explicitly chooses whether that listener installs worker management under the
reserved `worker` namespace. Use `None` for a plugin-only listener: it receives
no worker actions automatically. `SharedEntry::install_actions` and
`install_native_actions` register each axis's actions into that same router as
reversible resources in its own composition. Serving begins after `activate`;
additional owners can register while the entry is live.

`composition::mount_axis` mounts a committed child runtime as an explicit parent
resource. Each child keeps its own local resource names, bindings and useflow;
cross-axis/shared dependencies belong in the parent resource schema. Several axes
can therefore use the same internal resource names without sharing ownership.
The parent composition fixes its mounts at commit; the shared entry does not itself
hot-add child runtimes to that committed parent. `Process::close_axis` closes a
mounted runtime in dependency order. Live dependents prevent unsafe closure.

On a shared listener, closing an action owner withdraws only its routes, not the
listener or another owner's routes. An axis can instead own its own entry/UDS;
closing that axis withdraws and drains its routes, then releases its listener. In-flight unary calls and streaming producers retain leases until they
really finish; route revocation notifies streaming responses and interrupts
backpressure waits. The entry reserves each namespace through draining, then
allows reuse. Failed registration releases its reservation; old generations
cannot revoke replacement routes. Contract IDs are local to the namespace, while
actual HTTP paths/gRPC names stay explicit and unique within that listener. A conflicting
batch fails atomically. These namespaces express ownership, not permissions.

Different axes use different HTTP/gRPC bindings on a shared listener **or**
separate TCP/Unix endpoints with independent routers. Sharing entry implementation
does not require sharing a listening socket. Independent listeners may use the
same action/contract names; their publication and shutdown ownership stays local.
The `axis_endpoints` regression uses two Unix sockets in one parent composition,
checks that a duplicate socket bind cannot replace its owner, and closes one axis
while the other continues serving. No host-entry-to-plugin-entry forwarding hop
is introduced.

Whole-host closure stops registration, withdraws all routes, signals the listener,
and drains calls before releasing resources. A closed entry rejects new axes.
Socket bind, endpoint validation, Unix mode/group, stale-path policy and
inode-checked cleanup come from infra's `network-sockets`. This project selects
the compiled local-listener policy and owns its serving lease; it has no duplicate
socket implementation. Stale recovery is explicit and never removes a live
listener or a non-socket path. Socket mode/group are deployment inputs.

The worker notifier exposes typed socket decisions at ingress use/reuse
boundaries. An application Handler projects those into its installed chain;
infra applies closure without leaking raw descriptors to plugins. The same worker flow also exposes infra dial, keyed pool and byte-bridge controls;
see [worker networking](../infra/rust/crates/networking/proxy/pingora/src/worker/README.md).
These explicit operation hooks do not transfer ownership of a shared management listener to a business axis.

Callback order is fixed at chain construction. Adapters can declare supported
hooks with `Callbacks::supports`; the chain builds a per-hook index once and
skips unrelated callbacks before creating their futures or entering an ABI.
Each participating callback borrows the same facts. The first decision or error
stops dispatch. Completion stays with the admitting callback and can start only
once, including when its first attempt fails or is cancelled; this is not a
guarantee that completion finishes.

Run `just bdd` to exercise the actual native pipeline and see `DECISION`, `WHY`,
and `FACTS` for authentication, admission,
dial, completion, and a refused completion retry over a real TCP exchange.
The scenario buffers its trace and prints once. Shared infra credential policy
hashes API/auth credentials and selected session cookies while preserving ordinary
route/theme/affinity cookies. Additional credential field and session-cookie names
are explicit policy inputs. Runtime notifier traces use the
`proxy_pingora::worker::notifier` target and contain callback indices and decisions,
without serializing request facts.

For managed HTTP requests, `RequestFlow::execute_http` runs protocol validation,
header/metadata transforms, authentication, routing/admission, request-body setup,
upstream encoding, one installed egress call, upstream-response hooks,
response-body setup and downstream encoding. All interested transform callbacks
run in order on the same owned request/context. The returned `HttpExchange`
retains stream ownership; call `finish` only after downstream delivery completes.
Failures stop later hooks, and dropping the exchange cancels without reporting
successful completion. The carrier retains raw protocol parsing responsibility.

Body setup configures native pull-time transforms; it never collects or serializes
a body to notify plugins. Compiled callbacks register `stream_transfer::chunk::ChunkTransform`
implementations through `facts.request_body.push(...)` and
`facts.response_body.push(...)` during the respective preparation stages. The
worker seals each chain and moves it into carrier-owned state before final header
callbacks. Facts retain closed empty setup slots; later metadata callbacks cannot
replace an active upload or response processor. Late registration or replacement
fails before publishing the affected head. Transform errors or a violated length
promise poison the chain. At most 64 transforms run
in registration order per direction, using the borrowed `Option<Bytes>` slot;
the dispatcher creates no payload copies, intermediate buffers, or per-chunk
futures. Transforms may allocate when their own operation requires it.

The default transform length policy is variable. The worker removes stale fixed
lengths/digests and selects streaming framing. A transform that declares
`preserves_length` must preserve length on every invocation. Header callbacks
cannot override the body's original fixed length or sealed framing policy.
HTTP/1.0 variable uploads use HTTP/1.1 chunking upstream; variable responses to
HTTP/1.0 clients use connection-close framing. Transfer codings other than a
single `chunked` declaration are rejected before forwarding because the native
carrier does not decode them.
Header-only messages do not provide a payload/EOF transform call, so transforms
must not synthesize bodies or require EOF side effects for those messages.
Transforms on protocol upgrades are rejected; opaque tunnel bytes remain native.
Native Function/Object callbacks configure metadata/control only; their ABI cannot
install a Rust chunk processor or transport an asynchronous body. The C ABI carries bounded non-streaming metadata/control
frames and explicit resource handles while async response streams stay with the
native host/egress owner. Borrowed frame views can be passed through synchronous
callbacks without copying; they cannot outlive their invocation.

The reusable `diagnostic-output` runtime provides separate sinks for diagnostic
traces, `proxy_pingora::access` records, and opt-in `proxy_pingora::body_dump`
records. Assembly configures access-file retention independently from body dumps
and retains the logging guard through shutdown. Trace verbosity does not enable
body capture. The HTTP carrier emits access metadata without headers, queries or
bodies; consumed sessions keep unavailable status/byte counts explicitly unknown.

## Native boundary

The synchronous C ABI is the required effect boundary for Go Object plugins.
Arguments and replies use scoped byte views/opaque handles; no Go/Rust runtime object
or Future crosses it. Runtime binding dispatch verifies the calling instance owns
the binding ID. Method validation remains the Tool's contract. Caller-local
async/await retains leases until the native operation actually returns; cancelling
an await does not authorize freeing an in-flight handle. Logical closure,
resource draining, and physical library unloading are separate operations.

Native in-process libraries are **trusted code, not an OS sandbox**. Object code
must not create its own sockets, RPC transports or filesystem side channels.
The `tools/plugin-contract` build guard rejects direct net/rpc/fs entry references
(including aliases and function capture) and unauditable dot imports. It checks
selected first-party entry sources, not transitive dependencies, dynamically
constructed behavior, arbitrary C code, or malicious syscalls. Bootstrap config
and existing business dependencies still require review; this guard is not proof
that every transitive side effect has been migrated. Hard denial for untrusted
native libraries requires a separate constrained process and mediated tools;
that isolation mode is not implemented here. Do not advertise it as enforced.

The Rust native egress stream remains Rust-local through the provider trait.
For a client implemented in another language, reuse infra's framed stdio primitive
and provider adapter; do not add a front proxy or tunnel ordinary Tool calls
through the notification chain.

## Curated optional Tool plugins

`plugins/packages.json` catalogs PostgreSQL, Redis + memory cache and the browser
client. These are optional builtin distributions, each a separate loadable `.so`;
the generic worker does not statically link, automatically load or require them.
Their implementations stay in the single infra checkout.

Choose `just build-postgres`, `just build-cache` or `just build-browser` to build
only that Tool into `plugins/`. `just build-plugins` is an explicit convenience
selection of all three. Listing a package does not grant a capability: deployments
select artifacts, and loaded instances independently declare their provider bindings.

Sub2API's base business bundle contains only its Go Object. Its OAuth business
bundle contains two Go Objects. The application deployment separately selects
the needed curated Tools and injects their capabilities. Missing required bindings
fail before loading; there is no implicit backend fallback.

The worker and its release application must resolve the same infra checkout;
use the existing sibling `infra` workspace rather than a second clone. The host
provides its own executable; other release binaries may select their application at build time.
The selected build controls hook scope; environment variables only supply runtime
settings and credentials. Run `cargo test` for host checks and
`go test tools/plugin-contract/main.go tools/plugin-contract/main_test.go` for the
source boundary checker. Both axes retain independent useflows and meaning cores.
