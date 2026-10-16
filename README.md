# Pingora worker

See the [security model](SECURITY.md) for trust boundaries, implemented controls and deployment responsibilities.

A runnable Pingora application with explicit configuration, assembly, plugin
runtime and process entry. `main` loads configuration; `assembly` installs RPC
callback bindings by default, explicit direct callbacks, and optional management
entry in `plugin-lifecycle`; the
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
routes are optional and loopback-only. Configured callbacks default to RPC through
a shared axis client manager. Explicit direct bindings execute in the worker.
Native owners remain pinned until synchronous calls return. See
[remote axis bindings](docs/remote-axis-bindings.md) for configuration, replacement,
contract limits and migration.

## Graceful version updates with listener handoff

On Linux, configure both worker generations with the same native transfer socket:

```json
{
  "upgrade": {
    "socket": "/run/pingora-worker/upgrade.sock",
    "grace_period_seconds": 1
  },
  "shutdown_seconds": 60
}
```

Create the socket's parent directory beforehand, owned by the worker user with
mode `0700`. Keep the data `listen` address unchanged. Start the first generation
normally. For a binary, configuration or plugin update:

1. Validate the replacement with `--config next.json --check-config --upgrade`.
2. Start the replacement with `--config next.json --upgrade`. It loads plugins,
   installs management and validates TLS before waiting for native listener FDs.
3. Wait for the transfer socket to appear and verify the replacement is still
   alive, then promptly send **SIGQUIT to the old PID only**. Automate this wait:
   Pingora currently waits roughly six seconds for the sender. Do not connect a readiness
   probe to the transfer socket: its next peer must be the old Pingora process.
4. Verify the replacement serves the data listener. The old generation stops
   accepting after Pingora's native handoff delay, drains existing requests, then
   shuts down plugins and flushes logs. Use SIGTERM for shutdown without updating.

Run one replacement at a time, under the same OS identity. Pingora removes the
transfer socket after receiving descriptors. Its native SIGQUIT path can still
terminate the old generation if descriptor transfer fails, so never signal it
before the replacement is waiting. This is native process-generation handoff;
each generation retains Pingora's multithreaded scheduling and same-process
notifier callbacks. The update path adds no per-request work or body copies.

The transfer covers the Pingora data listener. The separate loopback management
server currently needs a different per-generation port, or must be disabled;
reusing its occupied port fails initialization before handoff. Existing sockets
and plugin state stay with the old generation; they are not migrated. WebSocket
and other long-lived connections may drain until `shutdown_seconds` expires,
then disconnect and require client reconnection. `grace_period_seconds` accepts
0..3600. The grace period precedes the
bounded shutdown window; native FD handoff has its own delay as well.

## WebSocket Ping/Pong keepalive

Transport keepalive is opt-in and independent of message-codec grants. For
example, merge these settings into the application configuration:

```json
{
  "io_timeout_ms": 60000,
  "websocket_keepalive": {
    "enabled": true,
    "upstream": true,
    "downstream": true,
    "interval_ms": 30000,
    "pong_timeout_ms": 10000
  },
  "http2": { "enabled": true, "ping_interval_ms": 30000 }
}
```

After a successful WebSocket upgrade, each enabled leg sends one periodic Ping
and requires its matching Pong before the deadline. Upstream probes use fresh
random client masks; downstream probes use server framing. Missing or wrong
Pongs terminate the connection when the probe deadline expires. Ordinary peer
Ping/Pong frames and business messages continue through the existing forwarding
and notifier sequence; transport probes never call business plugins. Disable
either direction independently when only one leg needs probing.

Infra owns the frame cursor and timer state; Pingora's existing duplex task owns
writes and cancellation. The cursor skips data payloads in place and retains
only bounded headers and a small Pong token. A generated Ping waits until the
outgoing frame is complete, including when a wire frame spans multiple chunks.
Fragmented messages may contain control frames between fragments. No body
collection, periodic task per socket, or additional C ABI call is introduced.
Observing a Close frame stops probes on that leg.

Choose an interval below the intermediary's idle cutoff. The interval plus Pong
timeout must be shorter than `io_timeout_ms`; a stalled partial frame, blocked
writer or expired inspection deadline still follows the existing timeout policy.
No frames are injected into ordinary HTTP, rejected upgrades or other upgraded
protocols. These settings cover HTTP/1 WebSocket upgrades, not RFC 8441 extended
CONNECT over HTTP/2.

## Upstream HTTP/2 keepalive

HTTP/1.1 remains the default. Set `"http2": {"enabled": true,
"ping_interval_ms": 30000}` to enable native upstream HTTP/2 PING/ACK probes.
HTTPS origins negotiate HTTP/2 through ALPN with HTTP/1.1 fallback; plaintext
origins require HTTP/2 prior knowledge (h2c). Upgrade requests, including
WebSockets, always select HTTP/1.1 independently of this setting.

Choose a positive interval below the selected proxy's idle cutoff. The native
Pingora connection driver sends probes through direct connections and selected
CONNECT/SOCKS tunnels, including while a pooled HTTP/2 connection is idle. It
closes connections when an ACK does not arrive within its fixed five-second
native timeout. Omit `ping_interval_ms` to disable probes while retaining
HTTP/2 selection. An interval requires `enabled: true`.

Keepalive does not extend `pool.idle_timeout_seconds` or the configured HTTP
body I/O deadline. A server can also reject excessively frequent probes; choose
an interval compatible with its policy. HTTP/1.1 fallback has no HTTP/2 probes.
These settings apply to the upstream connection. Pingora automatically answers
HTTP/2 PINGs from downstream clients; this Pingora version does not expose a
native server setting for scheduling downstream probes.

## Optional Wasm Object callbacks

Build with `cargo build --features wasm` to enable sandboxed Wasm Objects.
Select `"binding": "direct"` and `"backend": "wasm"`:

```json
{
  "binding": "direct",
  "backend": "wasm",
  "path": "plugins/policy.wasm",
  "package": "example.policy",
  "variant": "wasm",
  "kind": "object",
  "hooks": ["pre_routing"],
  "patch_headers": ["x-policy"],
  "wasm_limits": {
    "max_memory_bytes": 16777216,
    "fuel_per_call": 1000000,
    "max_module_bytes": 16777216,
    "max_table_elements": 4096
  }
}
```

These sandbox limits default to the displayed values and are per instance.
The total configured memory for one callback instance pool must not exceed
512 MiB; configuration checks this bound before loading any module.
`max_inflight`, `max_waiting`, `instances` and callback deadlines retain the same
host admission and lifecycle rules. Wasm uses blocking scheduling and Object
methods only. Configuration rejects Wasm when the build feature is disabled,
Function kind, inline scheduling, and `wasm_limits` on native entries. Native
entries may explicitly use `"backend": "native"`. Artifact suffixes never select
a backend. The guest descriptor must match ABI, Object kind, package and variant;
`language` and native unload policy have no effect on Wasm identity.

The sandbox accepts binary Wasm modules with no imports. No WASI, filesystem,
network, process, clock or Tool imports are available. Memory growth, table size,
module size and instruction fuel are bounded. Guest failures follow the existing
callback failure path. Metadata projection, header write grants, semantic codec
grants and explicitly published management actions use the same host contracts
as native callbacks; a guest cannot widen its configured authority.

Statically linked Tools are trusted host code with the worker's process authority. Future guest access
to a Tool must pass caller-specific method and resource capability checks at the
host boundary. Installing a Tool is not a blanket grant to every Object. This
initial Wasm backend denies all Tool imports and introduces no native helper
process or sidecar. Fuel bounds guest execution, including initialization and
cleanup; it is not a wall-clock limit on compilation or native host code.
See the [Wasm Object ABI](../infra/rust/crates/plugins/loading/README.md#optional-wasm-object-callbacks)
for the required exports and ownership rules.

## Native metadata callbacks

A `callbacks` entry with `"binding": "direct"` selects a trusted native library:

```json
{
  "binding": "direct",
  "path": "plugins/policy.so",
  "package": "example.policy",
  "variant": "native",
  "language": "rust",
  "hooks": ["pre_routing", "authenticate", "admit", "complete"],
  "headers": ["x-request-id"],
  "patch_headers": ["x-request-id"],
  "config": {},
  "timeout_ms": 5000,
  "scheduling": "blocking",
  "max_inflight": 32
}
```

Native `.so` callbacks default to Object, independently of Rust or Go language.
Object and Tool are native component roles; Tools are consumed through explicit
runtime dependency bindings, not installed as notifier participants. Function is
the LuaJIT/V8 execution role and is not inferred from a native library's language.
The `callbacks` configuration does not select a LuaJIT/V8 engine.
Explicit `kind: "function"` remains available only for existing native Function ABI
artifacts; otherwise `kind` is `object`. Selection must match the artifact header. Object methods are serialized per instance within the callback deadline,
while local Functions retain configured concurrency. `instances` (default 1) adds
independent Object instances with stable request affinity across metadata and
message callbacks. `max_waiting` bounds queued inputs separately from active calls.
Startup, calls and shutdown each have bounded host waiting deadlines. The plugin
runtime stops dispatch to an instance when a running call times out or loses
its waiter. Cancellation before native entry discards the queued job without
withdrawing a healthy instance. Cancellation after native return also leaves the
instance available. Queued callers are released promptly; active input, handle, library
and resource reservations remain pinned until actual completion. An affected
instance stays withdrawn until reinstallation; a queue timeout before admission
does not withdraw a healthy instance. This never forcibly kills a native thread.
Go library mappings remain retained until process exit.

`scheduling` defaults to `"blocking"`: bounded native worker threads run calls
after asynchronous admission. `"inline"` explicitly declares trusted, bounded,
nonblocking work and runs native entry and reply consumption on the calling
execution thread after the same asynchronous admission. Scheduling is independent
of Function/Object identity and Rust/Go language; both modes execute in this
process. Inline input and ABI reply owners remain local through native return.
The host cannot enforce the nonblocking declaration or preempt an inline call;
it checks elapsed deadlines after return. An inline call that blocks stalls its
calling execution thread. Management action handling already has its own executor;
inline scheduling uses that action's calling execution thread. This does not pin
an instance to one OS thread across calls; thread-affine VM adapters must own
their scheduling separately.

Direct callbacks may retain the legacy `"execution": "in_process"` declaration.
Remote callbacks reject that declaration. Select `binding` to choose a transport;
`native_helper` and `execution: "isolated"` remain unsupported. The worker does not
spawn per-callback helpers. Direct input owners move into blocking runtime jobs
without copying their bytes, and reply visitors consume the native allocation
before release. Remote metadata uses the shared axis manager described above.

Both kinds must implement the configured hook methods and the metadata/reply
contract below. A native Object may additionally select `graph` with an explicit
`callback_instance` and bounded provider `instances`. The selected Object's path,
identity and configuration must match its callback entry. The runtime validates
capabilities and dependency order before opening artifacts, injects scoped
bindings, and retains providers through callback drain and reverse closure.
Each affinity-pool member owns its own graph. Tools never become notifier
participants. See the [graph callback configuration](../infra/rust/crates/plugins/loading/README.md).

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

Granted native callbacks can opt into [controlled WebSocket sessions](docs/message-sessions.md):
approve the first message before dialing, replace complete messages, and retire or
replace an upstream while keeping the downstream connection open.

| Capability | Current native worker | Owner |
| --- | --- | --- |
| Upgrade request/response metadata decisions | Supported within configured hook/header grants | Worker notifier chain |
| Pool, dial, and acquired socket decisions | Supported before native dispatch | Worker controls and Pingora |
| Bidirectional bytes after 101 | Transparent native forwarding with backpressure | Pingora carrier |
| Async upgraded client-byte filter | Opt-in native `ProxyHttp::request_body_filter` adapter | Trusted compiled assembly |
| WebSocket client message inspection | Opt-in `websocket.v1` codec; native ABI call per complete message | Protocol codec and granted plugin |
| Whole-message decisions | Ordered allow/deny; granted WebSocket actions can rewrite messages and replace upstreams | Native plugin policy |

HTTP body transforms and body dumps are bypassed after upgrade. Handshake fields
(`Sec-WebSocket-Key`, `Accept`, `Version`, `Protocol`, `Extensions`, with the
`Sec-WebSocket-` prefix) cannot be changed by native metadata replies, even with a
write grant. Compiled hooks must preserve the original handshake metadata too.
Otherwise transparent forwarding could advertise an encoding or subprotocol it
does not implement.

Frame parsing, masking, fragmentation, ping/pong/close handling and backpressure
belong to infra; business interpretation belongs to the granted plugin. The
vendored Pingora 0.8.1 proxy adds an awaited `async_response_body_filter` whose
default delegates to the original synchronous hook, plus request/response assembly
deadlines. Its existing duplex engine still owns forwarding. No stream or future
crosses the C ABI. `message_responses: true` explicitly enables upstream message
inspection; omitted grants keep response payloads opaque to callbacks.

## Opt-in native Pingora request filter

Import actual Pingora types and the `ProxyHttp` trait directly from their owning
crates (`pingora_core`, `pingora_http`, and `pingora_proxy`). Compiled assembly can
attach a real implementation with its own per-request `CTX`:

```rust,ignore
let worker = assembly::assemble(config).await?;
let worker = worker.with_upgraded_request_body(std::sync::Arc::new(native_handler));
// Pass this worker to ProxyFacility::run_proxy or Pingora's HttpProxy as usual.
```

The returned `NativeHookProxy<P>` delegates the existing worker lifecycle. It
invokes only `P::request_body_filter`, after accepted 101 negotiation and native
upgrade completion, on client-to-upstream bytes. The handler context is created
lazily at the first authorized invocation. Normal HTTP bodies, upstream response
bytes, route selection and early responses do not enter the handler. Other trait
methods, including its required `upstream_peer`, are not delegated. This named
adapter is a single explicit hook capability, not a second competing proxy policy.

The eleven-stage notifier sequence still runs first: transformed identity,
authentication, admission, body setup, request finalization, network controls,
upstream response and downstream handshake finalization precede upgraded-byte
inspection. Completion stays with the original admission owner. The handler uses
the real mutable Pingora `Session` and `Option<Bytes>`; no byte conversion or
extra forwarding task is introduced by the adapter. It is trusted Rust code,
not a sandboxed C ABI plugin. Handler-owned resources must participate in the
caller's runtime composition and explicit shutdown policy.

Calls are awaited within the worker I/O timeout. Errors, timeouts or cancelled
invocations poison the wrapper context and cannot report successful completion.
When buffering incomplete messages, use `Some(Bytes::new())` to withhold output;
`None` has Pingora EOF semantics. Raw chunks are neither frames nor complete
messages: any codec must bound accumulation, retain preread bytes and account for
fragmentation/masking/extensions. The raw-hook API does not itself decode messages; the separately configured
message codec runs before an additional trusted raw handler. Response inspection
is independently granted with `message_responses`. An awaited client filter can still delay
same-connection response scheduling, so its latency must be bounded and measured.

## Protocol codec plugins and the message ABI

The common boundary owns byte lifetimes, ordered decisions, cancellation and
backpressure. Protocol codecs are separate compiled Rust plugins. The built-in
`websocket.v1` codec handles client WebSocket framing; it does not parse application
JSON, OpenAI events, or any other business format. Native Object plugins interpret
those complete message payloads. Response inspection is opt-in and preserves the
original wire after approval.

A callback opts in with `"message_codec": "websocket.v1"`. Without a codec grant,
the existing opaque upgraded-byte path is unchanged. Custom compiled codecs can
be supplied through `assembly::assemble_with_codecs(config, codecs)`; each implementation of
`proxy_pingora::http_ingress::message_codec::CodecPlugin` supplies its identifier, upgrade selection,
handshake validation and per-connection decoder. Registration resolves an explicit
identifier before native plugins are loaded. Several configured codecs may serve distinct routes; overlapping selections for
a request are rejected. Host configuration may also supply a `codec_descriptor`
selecting bounded built-in HTTP JSON or WebSocket framing and routes. The native
callback receives the resulting complete messages for semantic inspection. The
descriptor ID must match the explicit codec grant. A `.so` does not install an
incremental Rust decoder through this ABI. Arbitrary Rust vtables never cross C ABI.
A codec must bound pending input,
message output and frame bookkeeping and fail closed on malformed/incomplete input.

The initial native message contract uses the existing plugin ABI `call` operation:

| Field | Encoding |
| --- | --- |
| Method | `websocket.v1/text/<request>/<sequence>` or `websocket.v1/binary/<request>/<sequence>` |
| Request and sequence | Exactly 16 lowercase hexadecimal digits each; sequence starts at zero and counts data messages |
| Input | Complete unmasked payload; text is valid UTF-8, binary is arbitrary bytes |
| Reply | `0` allows; `1` denies and closes. Responses/Codex request callbacks may instead return a validated bounded JSON `type: error` document |

For Responses/Codex WebSocket requests, a structured error suppresses that request
message and is sent as one server text frame, preserving the socket for a later
client retry. The same document from an upgraded response callback replaces that
upstream frame and is written downstream; the socket stays open. Websocket text
classified as `response.completed`, `response.done`, or graceful
`response.incomplete` is offered to the
codec callback even when `message_responses` is false, so a plugin can replace
the complete terminal frame. Delta events (`response.event`) bypass notifier
callbacks even with `message_responses` or action grants; the carrier validates
framing and preserves their wire bytes. Other upstream message kinds require
`message_responses: true`, with separately granted session metadata/actions.
A reply that is not the error document still fails closed.
The error batch is bounded. Reply-capable protocols frame-gate the upstream
direction even without semantic response access, so a locally generated error
cannot be inserted into the middle of an upstream frame. Unknown or incomplete
upstream frames fail closed.

Control frames do not invoke business callbacks or advance the message sequence.
The same complete payload allocation is shared across callbacks in registration
order. Each native call borrows its input pointer only until that call returns;
Go code must not retain that pointer or slice. Explicitly copying is required if
plugin code needs ownership beyond the call. A timed-out host waiter does not free
input still used by native code: the loading owner retains it and its admission
permit until actual completion. Payloads are not JSON/base64 encoded across ABI.

The codec retains original masked wire bytes and decodes payload once. A single
contiguous accepted frame can reuse its original wire allocation. Fragmented or
batched output may need one bounded coalescing copy because Pingora's body hook
returns one `Bytes`; this is not a zero-copy claim for the entire path. Limits are
a configurable decoded payload (default 1 MiB, maximum 32 MiB), an additional 64 KiB wire budget, 1024 frames and 4096
retained wire segments. An emitted batch may contain both previous buffered wire
and current input, bounded to twice the wire budget. Whole messages are withheld until accepted. Ping/pong frames are released
immediately even during fragmented data, without invoking business callbacks. A valid close discards withheld unfinished data and forwards the close
frame. Pending work is bounded by carrier timeouts and codec limits.

The WebSocket codec validates masked client framing, continuation/control rules,
length encoding, text UTF-8 and close payloads. Inspected WS requests withdraw extension offers before upstream dispatch;
unsolicited negotiated extensions are rejected before downstream 101. Uninspected
connections preserve their original compression negotiation.
It does not invent application error documents. Responses/Codex callbacks may
return the CLIProxyAPI `writeResponsesWebsocketError` shape: `type` is `error`,
`error` is an object with optional string `code`, `type`, `message`, and `param`,
and optional `status`/`status_code` are unsigned 16-bit integers. Duplicate critical
fields and malformed documents are rejected. The codec preserves those JSON
bytes in one bounded unmasked server text frame; the host's upstream framing
guard and write ordering keep the socket valid for a client retry. Other
failures close the proxy path. The existing notifier authentication, admission,
`before_upstream_send` and `before_downstream_write` stages remain authoritative.

## AI API codecs from infra

`llm-protocols` provides the API adapters; `llm-client` shares its canonical chat
DTOs and endpoint constants. The worker owns selection, notifier ordering and
native callback execution, without its own API decoder implementation.

| Callback `message_codec` | Supported request paths |
| --- | --- |
| `openai.chat_completions.v1` | HTTP(S) POST `/chat/completions`, `/v1/chat/completions` |
| `openai.responses.v1` | HTTP(S) POST and WS GET `/responses`, `/v1/responses` |
| `anthropic.messages.v1` | HTTP(S) POST `/v1/messages` |
| `codex.responses.v1` | HTTP(S) POST and WS GET `/backend-api/codex/responses` |
| `google.gemini.v1` | HTTP(S) POST `/v1{beta}/models/{model}:generateContent` or `:streamGenerateContent` (`v1` and `v1beta`) |

For example, add `"message_codec": "openai.responses.v1"` to a trusted native
callback. Metadata hooks may be omitted for a message-only callback. All grants
resolve before loading native code. Only callbacks granted the selected codec
receive its messages, preserving their registration order.

The existing binary method form is `<codec>/<kind>/<request>/<sequence>`.
HTTP kinds are `chat_completion`, `message`, `response`, and `responses_lite.full`.
Gemini uses `generate_content` and `stream_generate_content`; the entire request
JSON is delivered once, including `contents`, tools, and unknown fields. Query
parameters such as `alt=sse` remain unchanged. This adapter covers Gemini REST,
not Gemini Live WebSocket or Vertex AI resource paths.
Responses WS kinds are `response.create`, `responses_lite.create`, and
`responses_lite.delta`. Lite is detected per message from the actual Codex wire
marker; incremental input and continuation IDs are preserved, not reconstructed.
JSON bodies are validated as complete objects without narrowing them to the
client's smaller chat DTOs.

HTTP body transforms precede codec inspection. The body is held until EOF and
native approval; upstream headers may already have been dispatched. Early origin
success cannot acknowledge an uninspected request; early origin rejection can be
relayed without successful completion. HTTP zstd is decoded within strict limits
for inspection while original compressed wire and headers are forwarded unchanged.
The codec delegates bounded zstd decoding to infra’s `compression` primitive wrapper.
The default input/decoded limit is 1 MiB with at most 4096 retained segments.
`inspection` configures `max_requests` (128), `max_buffered_bytes` (256 MiB),
`max_message_bytes` (1 MiB) and `message_timeout_ms` (30 seconds). Admission
reserves worst-case decoder/IPC/encoding buffers before accumulating data; budget
exhaustion returns 503. Idle framed connections release their assembly reservation;
new message bytes reacquire it. Assembly deadlines cover idle incomplete messages, not
an entire SSE/WS session. Completed messages release their deadline; a new partial
message starts a new one. Per-callback deadlines remain independently bounded.

With `message_responses: true`, complete JSON responses, structured errors
(including HTTP 200 error envelopes), plain non-JSON HTTP errors, and complete
terminal stream messages can be inspected. Full HTTP kinds append `.response`
or `.error` to the protocol kind. SSE, Gemini JSON-array and WebSocket delta
chunks classified as `response.event` bypass notifier callbacks. Complete SSE
and WebSocket `response.completed`, `response.done`, and interrupted
`response.incomplete` envelopes keep those terminal kinds; SSE `[DONE]` is `response.done`. Separately granted binary and
metadata kinds remain available. Chat finish/usage, Anthropic `message_delta`,
and Gemini finish/usage events use `response.metadata`; Anthropic `message_stop`
uses `response.done`. No aggregate business response is synthesized
from deltas by the worker.
Response sequences are independent of request sequences. HEAD and bodyless
statuses have no body decoder. Inspected HTTP responses request identity encoding;
unsupported encodings fail closed. WS inspection disables extension negotiation.
Unfragmented contiguous server frames share their original payload allocation.

The Go implementation in `infra/go/ai/llmprotocols` imports a pinned CLIProxyAPI
SDK. It supplies request/full-response translation and status/header-aware error
handling. It does not silently fall back when a conversion pair is unsupported.
The worker's message contract remains decisions and bounded protocol errors; installing this SDK does not
implicitly enable payload rewriting.

## Realtime / Live signaling codecs

Grant `openai.realtime.v1` for public Realtime/Live or `codex.realtime.v1` for
Codex backend routes. Both are infra registrations and use the existing notifier
sequence, native Object callback grants, and bounded binary message ABI.

- WebSocket: public `/realtime`, `/v1/realtime`, `/live`, `/v1/live`;
  Codex `/backend-api/codex/realtime` and `/backend-api/codex/live`.
- Live sideband: append one opaque call-ID segment to a Live route. Legacy
  Realtime sidebands retain the `call_id` query parameter unchanged.
- HTTP POST: public `/realtime/calls`, `/v1/realtime/calls`, `/live`, `/v1/live`;
  Codex `/backend-api/codex/realtime/calls`.

HTTP signaling accepts SDP, multipart with one valid boundary, and backend JSON.
ABI kinds are `realtime.call.sdp`, `realtime.call.multipart`, and
`realtime.call.json`. The callback receives the complete original payload
(decoded once for zstd) before any body is sent upstream. SDP/multipart semantics
are the plugin's responsibility; infra validates HTTP/MIME header selection and
bounds accumulation. For multipart parsing, also grant `headers: ["content-type"]`
at a metadata hook such as `before_upstream_send`; the binary payload has no
extra metadata envelope. The plugin must implement both granted interfaces.

Realtime/Live WS inspects complete client text messages. Known event types become
static ABI kinds; future types use `realtime.event` with the original JSON intact.
Audio/base64 and context data are not materialized into domain objects. Invalid
JSON, duplicate/missing event types and binary client frames fail closed.
Upstream events and binary audio messages can be inspected with an explicit
response grant. The same configurable message limits apply.

This supports WebRTC call signaling and WebSocket sidebands. Actual WebRTC media
and DataChannel traffic use the independent `infra/go/network/webrtc` carrier.
It owns Pion ICE/DTLS/SRTP/SCTP connections, bounded Opus RTP and DataChannel
forwarding, complete-message hooks and shutdown. Media stays in Go; it is not
copied through per-frame Rust IPC. Its session API is independent of HTTP and
can be mounted through infra action adapters for Axum or native Pingora L4.

Native action declarations (`actions`) explicitly bind unary plugin methods and
JSON schemas to the optional loopback management entry. Stateful actions require
`instances: 1`; plugins own opaque media session IDs. The entry revokes/drains
these actions before closing native instances. See
`infra/go/ai/llmprotocols/exampleplugin/worker-config.example.json` for signaling
actions. The example is a carrier fixture; provider routing and credentials remain
application plugin responsibilities.

Codex Live paths and shapes were verified against local `codex-api`
commit `444da310e108da16aaeb18fd790b0ac464f08aca`; they remain opt-in.

## Ownership and calls

- **Worker axis:** Pingora ingress, concurrency and drain mechanics, worker
  management, payload framing, callback grants, and the selected native transport.
- **Worker plugin scope:** general payload decoders and full client replacements.
  Decoder implementations reuse Rust protocol primitives. A full client owns
  complete request execution, response streams, pools, cancellation, and cleanup.
  Function/Object/Tool names in the generic plugin runtime describe component
  roles; they do not require business providers in the worker.
- **Business application:** remote bindings keep policy, storage, caches, and
  application workflows with their application owner. Worker metadata/message
  calls use the granted contract and retain custody through actual completion.

    Pingora ingress -> host decoder -> granted callback -> selected client
    Worker remote binding -> application-owned workflow and dependencies
    Internal control entry -> worker action OR explicitly registered axis action

Worker callback exposure is separate from HTTP/gRPC action publication. Action
contracts and transport bindings remain distinct; composition owns registration.
A remote RPC reconnect replaces that connection only. A peer connector hook also
retains Pingora's HTTP execution. Neither is a full client replacement.

`entry::install` binds one selected internal endpoint. `Registration::worker_control`
explicitly chooses whether that listener installs worker management under the
reserved `worker` namespace. Use `None` for a plugin-only listener: it receives
no worker actions automatically. `SharedEntry::install_actions` and
`install_native_actions` register each axis's actions into that same router as
reversible resources in its own composition. Application assembly also publishes
the read-only gRPC action `worker.metrics` with process-local operation counts,
in-flight requests, outcomes, consumed request/response chunk bytes of terminated
requests, cumulative latency in nanoseconds, and HTTP status classes (`0` is unknown, then `1xx` through
`5xx`). Counts reset on restart; concurrent snapshots are approximate. Byte counts
measure native HTTP body-filter chunks before transforms and omit headers,
separately controlled WebSocket sessions, and L4 tunnel traffic. Cancelled
requests contribute observed bytes but no latency or status observation.
`worker.events` exposes the same counters through gRPC `Actions/CallStream`
with action `worker.events` and JSON `null` input. Browser clients can use
`GET /worker/events` with `Accept: text/event-stream` on that same internal
management listener; an empty GET body is accepted. SSE emits `event: item`
with the snapshot as JSON `data`, idle keepalive comments every 15 seconds,
and a terminal `event: error` with an HTTP status code on failure. Without
the SSE Accept header, HTTP streaming retains the infra NDJSON format.

Subscriptions immediately return current state, then coalesce counter changes.
They retain no request bodies or per-request event history, and run outside the
notifier chain. A slow subscriber never blocks proxy operations. Counters wake
subscribers at request begin/termination, not on streamed deltas. Access logs
remain the per-request history. No SSE IDs or replay cursor are published;
a nonempty `Last-Event-ID` is rejected. Reconnection starts from current state.
gRPC and SSE share at most 32 live operation/logging event sources per entry, using an
infra resource reservation held until the source drops. Extra subscribers receive
gRPC `ResourceExhausted` or a terminal SSE error with code 503. This prevents idle
event subscriptions alone from taking all 128 shared entry slots; it does not
reserve capacity against other plugin actions or simultaneous request bursts.
The shared entry's 120-second absolute deadline still applies to observers:
handle terminal errors and reconnect with backoff.
Closing their action owner or the entry cancels live streams before draining.
Keep this read-only surface on the existing privileged internal listener.

Application assembly also exposes `worker.logging` through gRPC and
`worker.logging.events` through gRPC or `GET /worker/logging/events` SSE.
Logging snapshots report queue drops, oversized records, write/flush/rotation/
retention failures, active capture bytes and configured payload allowances.
Logging events return an initial snapshot, then sample changes once per second.
They share the same 32-source budget, cancellation and deadline as operation events.
Custom entries select the actual logging owner with `Registration::logging_health`;
`None` publishes no logging-health actions. Observation runs outside callbacks.

`worker.get` retains
its existing admission-state response. Custom compositions can share counters
using `NativeProxy::with_operations` and the counters argument of `entry::install`.
Serving begins after `activate`; additional owners can register while the entry is live.

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

The default native proxy suite includes a bounded concurrent TCP regression.
Three waves each overlap eight streamed uploads and four mid-upload disconnects.
Latches prove that all uploads reach the origin before release. Unique payloads,
correlation headers and exact body lengths detect crossed or truncated responses.
While response tails and client readers are paused, a fresh request must finish.
Each origin must acknowledge its validated exchange, all client tasks must join,
and operation counters must return to zero before the next wave.

Run `just stress` for 64 waves: 832 requests, including 256 mid-upload disconnects.
The workload uses the real native engine over loopback sockets in the test process.
It reports request counts, response bytes and elapsed time, with no throughput
pass threshold. The 30-second per-wave deadline detects a stuck exchange.
This is a short concurrency regression, not a production load or soak test.
It does not establish an RSS limit, socket-buffer backpressure, callback admission
saturation, sanitizer coverage, or production capacity. Pool reuse is disabled
in this workload so each connection belongs to one exchange; separate native
proxy tests cover reuse. Use deployment traffic, long durations, resource sampling
and a suitable sanitizer build for those additional claims.

Run `just bdd-observability` for the focused observation workflow. It reuses
the matching infra main checkout beside this repository, as the worker's Cargo
path dependencies require. It uses infra `WorkflowTrace` and prints bounded
`DECISION`, `WHY` and `FACTS` records
once per scenario, including earlier evidence when an assertion fails. The native
scenario forwards response bytes before origin EOF, checks exactly-once completion
and settled byte counts, reconnects to current counters, and closes idle observers.
It asserts that observation calls do not invoke notifier policy or become data
requests. Mixed gRPC/SSE scenarios check subscriber pressure, control availability,
disconnect capacity release, caller deadlines and rejection of SSE replay cursors.
The same recipe runs real access-file persistence and default-off/bounded response
dump tests, including credential hashing and visible routing cookies. `just bdd`
includes these tests alongside the notifier and complete native proxy suite.
See the [current hardening review](docs/hardening-review-20261009.md) for confirmed
remaining defects, known limits and missing integration evidence.

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

Explicit `logging.body_file` plus `body_max_bytes` enables bounded request JSON
and response JSON/SSE dumps; the limit applies separately to each direction.
Response capture observes consumed origin chunks before body transforms and emits
one sanitized record at EOF, after buffering within that limit. Streaming delivery
continues immediately. Credentials, session-cookie values and JWTs are hashed;
ordinary fields and load-balancer cookies stay visible. Oversized, malformed,
incomplete, unsupported, compressed, WebSocket and raw tunnel bodies emit metadata
only. Cancelling before EOF emits one incomplete record. Capture performs no
per-chunk output, notifier callback, blocking file I/O or lock acquisition; the
existing bounded logging queue writes the final event. Trace verbosity alone never
enables this body buffer.

All diagnostic destinations, including stdout and stderr, use infra's existing
lossy background writer. Each queue has at most 16 MiB of payload capacity;
trace/access records are capped at 64 KiB and body records at 4 MiB. Oversized
records are rejected whole. Request and response captures share a 64 MiB
allowance per assembled proxy and reserve twice their limit before allocation.
Pressure emits `capture_budget` metadata instead of retaining detail. These
allowances exclude temporary JSON trees, formatting, active writes and allocator
overhead; they are not a process RSS limit. Logging health exposes losses separately
from successful request delivery. Shutdown drain is bounded and best effort.

## Native boundary

The existing native callback ABI uses synchronous C calls with scoped byte views
and opaque handles. No Go/Rust runtime object or Future crosses that ABI. Runtime
binding dispatch verifies the calling instance owns the binding ID. Method
validation remains the selected provider's contract. Caller-local
async/await retains leases until the native operation actually returns; cancelling
an await does not authorize freeing an in-flight handle. Logical closure,
resource draining, and physical library unloading are separate operations.

Native libraries are **trusted code, not an OS sandbox**. Native decoder and
client implementations reuse installed infra primitives and retain their owners
through actual completion. Runtime admission and quarantine do not contain
process crashes or restrict syscalls. Bounded host waits cannot forcibly stop
arbitrary native CPU or allocator use.

## Decoder and client integration

The worker distributes no PostgreSQL, cache, or browser Tool artifacts. Business
applications select their own dependencies. Reusable implementations remain in
infra; their presence there does not make them worker requirements.

The Responses protocol codecs (`openai.responses.v1` and `codex.responses.v1`)
come from infra's native Rust `llm-protocols` crate. They parse protocol messages;
they do not instantiate a browser client. Browser client Tools belong to the
application's control plane and are not worker codec dependencies.

Current decoder integration accepts trusted Rust-local `CodecPlugin` values
through `assemble_with_codecs`, plus host-configured framing descriptors. Native
callbacks inspect bounded complete messages. This is distinct from loading an
incremental decoder implementation from a `.so`.

Current `NativeProxy` constructs a concrete native `HttpEgress`. The generic infra
`worker::Egress` port and provider adapters do not by themselves wire a full client
replacement into this executable. That boundary must include request execution,
stream ownership, admission, replacement, and drain before a deployment can claim
full client replacement. A different peer connector or remote RPC endpoint does
not establish it. Native response bytes must stay in the native carrier; the
notification contract is not a substitute transport for the response stream.

Build the worker against its selected sibling `infra` checkout. Remote
applications own their executables, dependencies, and resource composition.
Select contracts and hook scope explicitly; environment variables supply runtime
settings and credentials. Run `cargo test` for host checks. Worker and business
application owners retain independent workflows and lifecycle state.

See [the bridge and codec review](docs/bridge-codec-review-20261009.md) for actual
native carrier call sites, SDK reference versions, and Codex 0.162 interruption.
Run `just bdd-codecs` for the real TCP response and interrupt workflows.
