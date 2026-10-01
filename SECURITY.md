# Worker security and deployment policy

The worker installs explicitly selected trusted native callbacks or optional
untrusted Wasm Objects. The [common plugin security model](../infra/rust/crates/plugins/SECURITY.md)
defines artifact trust, memory/resource limits, native authority, scoped dependency
dispatch and the unimplemented future guest Tool bridge. This document defines
what the worker actually exposes and what its operator must protect.

## Deployment trust

Configuration, binaries, native libraries, TLS keys, log directories and the
upgrade runtime directory are trusted operator inputs. Protect them against
untrusted writes and run with only the OS privileges and credentials required by
the deployment. The default data listener is `127.0.0.1:6188`; configuration can
expose it more broadly. TLS termination and request authorization are deployment
choices. A configured upstream/proxy is host-selected; metadata callbacks cannot
replace the configured upstream or create arbitrary dial targets.

`callbacks[].backend` defaults to `native`. Native Rust/Go Objects or Functions
execute in this worker process (`execution: "in_process"` is the only supported
mode); malicious native code can bypass metadata grants and use process syscalls.
`backend: "wasm"` requires the worker `wasm` feature, Object kind and blocking
scheduling. `wasm_limits` controls module/memory/table/fuel bounds only for that
backend. Wasm receives no imports or Tools. Neither host code nor native codecs
and chunk transforms become sandboxed because another callback uses Wasm.
Optional V8/Lua crates elsewhere do not establish an available worker callback
backend: this configuration selects only native or Wasm.

Release curation should select reviewed versions and verified artifact hashes.
`path`, `package`, `variant`, `kind` and `language` are selection/ABI consistency
inputs, not implemented cryptographic artifact admission. The worker has no
signature/hash allowlist setting. Protect the selected path and release process;
self-reported identity and `plugins/packages.json` membership do not prove origin.
PostgreSQL, Redis/memory and browser Tools are optional curated `.so` distributions,
not automatically loaded or granted. Compiled/static host Tools, when an embedding
application selects them, retain trusted process authority.

## All notifier stages cross the same boundary

The host chooses a callback's hooks before chain construction. Supported hooks
are `pre_ingress`, `pre_routing`, `authenticate`, `routing`, `admit`,
`prepare_request_body`, `before_upstream_send`, `upstream_response`,
`prepare_response_body`, `before_downstream_write`, `complete`, `before_dial`,
`pool` and `socket`. Every enabled stage uses the same bounded callback owner,
backend and reply validation. No special stage implicitly grants a Tool, raw
socket descriptor, asynchronous body stream or unrestricted headers.

The metadata frame (64 KiB limit) contains version, hook, request ID, method,
path, optional status, upstream reuse/completion state and explicitly selected
header values. Network stages add bounded typed dial targets or pool/socket
phases, not native handles. Read grants (`headers`) and write grants
(`patch_headers`) are independent; both default empty. Repeated selected headers
remain repeated. Selecting authorization/cookie headers deliberately discloses
their raw values to that participant; notifier metadata is not a secret-redaction
layer. Treat selected configuration, paths and payloads as potentially sensitive.

The reply cannot enlarge its host grants. Header names/values and the complete
reply are validated before mutation; denied, invalid or failed replies apply no
partial patch. Transport/framing and WebSocket negotiation headers are never
patchable, including `host`, `content-length`, `transfer-encoding`, connection/
upgrade fields and `sec-websocket-*` negotiation fields. Authentication,
admission, completion and network stages forbid patches. Directional request and
response stages use their actual heads. A metadata-only body preparation hook
cannot install a Rust chunk transform or receive the body via its native ABI.

Interested callbacks run in fixed order. The first decision/error stops the
relevant decision chain; transform stages apply their documented continuation
rules. The installed compiled fallback accepts authentication and admission when all
earlier participants defer; a worker with no denying/authenticating callback is
therefore permissive. Deployments needing authorization must install an explicit
credential/tenant policy before that fallback. The framework does not supply a
credential validator. Completion starts at most once for the admitting
participant, but cancellation/failure can prevent its successful completion.
Denial or failure at a late response/completion stage cannot undo already sent
headers, bytes or upstream effects. Network decisions gate the specified operation;
they do not revoke direct syscall authority from trusted native code.

## Message data, resources and state

`message_codec` explicitly grants complete semantic request-message access for
host-registered routes/framing. `message_responses` separately grants response/
error access and requires a codec grant. An explicit descriptor must match its
codec and response grants. Payloads may include credentials or user content; no
generic payload redaction is promised. Without inspection, upgraded traffic can
remain opaque. Body transforms/dumps are bypassed after upgrade; WebSocket/message
inspection is a separate opt-in path. HTTP headers may already have been forwarded
while a body awaits semantic approval; a denied body does not imply zero upstream
contact. This worker is not a network-wide DLP or transaction rollback boundary.

`inspection` bounds active requests, retained buffers, complete message sizes and
incomplete-message assembly time. Defaults are 128 requests, 256 MiB retained
budget, 1 MiB messages and 30 seconds assembly timeout. Reservations precede
accumulation; exhaustion fails with 503. Idle framed sessions release reservations;
new partial messages reacquire them. These limits do not cap every host allocator,
compiler memory or native callback CPU use. Callback queues, call/startup/shutdown
deadlines and Wasm instruction fuel have separate meanings; see the common model.

Pools keep Object state across requests and use request affinity. Instances do
not automatically correspond to tenants. Upstream pool isolation incorporates
host policy and selected credentials, but does not create a per-tenant process,
fresh guest memory per request or authorization policy. Native/compiled callbacks
and shared Tools must implement their own tenant/state discipline. Strict tenant
separation may require separate instances, providers or worker deployments.

## Logs and secret handling

Diagnostic traces, access records and body dumps have separate sinks. Increasing
trace verbosity does not enable capture. Runtime notifier traces record hook/
participant indices and decisions without serializing request facts. Native
access records omit request headers, queries and bodies; unavailable consumed-
session status/byte counts remain unknown. Route paths and ordinary operational
metadata may still identify users or resources. Do not assume arbitrary native
plugin logs, returned error strings or application traces are sanitized.

Shared credential policy hashes API/auth credentials and selected session cookies
in the paths that explicitly use that policy (including pool/diagnostic policy);
it preserves ordinary route/theme/affinity cookies. Additional credential/session
names require explicit policy selection. This is not universal redaction of
selected notifier headers, action inputs or message payloads.

Body capture is off by default (`body_max_bytes: 0`, no `body_file`). Enabling it
requires a separate `body_file` and a positive `body_max_bytes` capped at 1 MiB.
Captured body prefixes can contain secrets even when metadata logs are redacted.
Protect access/body directories, retention, backups and access permissions according
to the data being captured. The logging guard remains owned through shutdown;
bounded queues and retention do not guarantee lossless logging under pressure.

## Management and publication

Configured `management.listen` must be loopback and differ from the data listener.
The worker publishes `worker.get` and revision-checked `worker.apply`, plus only
explicitly selected native action bindings. Native actions require one stateful
instance, bounded unary contracts and unique bindings. Revision checks avoid stale
state updates; they do not authenticate the caller. There is no built-in management
authentication here: any process with access to that listener can call its routes.
Loopback is a reachability restriction, not identity or authorization. Use a
trusted local environment or separately enforced access policy; do not expose or
forward this endpoint to untrusted clients.

Embedding APIs also support explicitly selected TCP/Unix entries and independent
axis publications. Namespaces express lifetime/ownership, not permissions. Unix
mode/group and endpoint policy belong to the installed listener. Stopping worker
admission does not stop independently published plugin actions. Closing an axis
withdraws/drains its routes before resources, without closing another owner's
listener/routes. Application action handlers own business authorization.

## Shutdown and graceful upgrade

Teardown stops new entry, withdraws actions/callbacks and drains admitted work
before closing dependent resources. Native cancellation keeps owners pinned until
actual return; unresolved work can stay quarantined. Shutdown errors are logged
and do not prove successful native cleanup or physical library unloading. Go code
remains mapped; a bounded host wait cannot force arbitrary native code to stop.

Linux upgrade transfers the Pingora data listener between separately started
process generations. Use one replacement at a time, the same OS identity and a
pre-created operator-owned `0700` parent directory for `upgrade.socket`. The native
FD-transfer channel is trusted local infrastructure, not an authenticated public
management API. Do not probe-connect to it: its peer must be the old generation.
Replacement configuration/plugin/management/TLS initialization occurs before
handoff. `--check-config` validates configuration; it does not establish artifact
authenticity or guarantee successful plugin loading.

Signal SIGQUIT only to the old process after verifying the replacement is waiting.
Native transfer failure can still terminate the old generation; handoff is not an
atomic rollback guarantee. Management listeners require a different per-generation
port or must be disabled. Existing connections and plugin state remain with the
old process; they are not migrated. Long-lived WebSocket/stream connections may
hit the shutdown deadline and reconnect. Grace period, native transfer delay and
shutdown window are separate. See the [upgrade procedure](README.md#graceful-version-updates-with-listener-handoff).

## Validation and limits of evidence

| Control | Existing tests |
| --- | --- |
| Backend/kind/feature/scheduling checks and bounded configuration | `src/config.rs` unit tests |
| Exact hooks, read/write grants, protected headers, atomic patches, network projection, at-most-once completion | `src/native_notifier_tests.rs` |
| Assembled Wasm callback obeys explicit header patch grant | `tests/native_proxy/wasm_callbacks.rs` (Wasm feature) |
| Incomplete-message deadlines, reservations and response approval | `tests/native_proxy/hardening.rs` |
| Access persistence without diagnostic/body capture | `tests/access_logging.rs` |
| Listener transfer/drain; invalid replacement leaves old process serving | `tests/graceful_upgrade.rs` (Linux) |
| Independent action/axis lifecycle | `tests/axis_endpoints.rs`, `tests/multiple_axes.rs`, `tests/notifier_lifecycle.rs` |
| Limited first-party source boundary guard | `tools/plugin-contract/main_test.go` |

The source guard rejects selected direct net/rpc/fs references, aliases/captures
and unauditable dot imports. It does not inspect all transitive dependencies,
malicious native code or dynamic syscalls. These regressions support specific
contracts; they do not prove OS confinement, universal secret redaction, tenant
isolation, business authorization, compiler limits or artifact provenance.
