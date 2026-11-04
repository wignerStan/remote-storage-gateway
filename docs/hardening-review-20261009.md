# Worker hardening review

Reviewed 2026-10-09, Asia/Singapore. Scope: worker infrastructure and its matching
infra dependency. Sidecar business policy is excluded. Native plugins remain
trusted code in the worker process. Host deadlines cannot stop native execution.

## Check native Pingora before adding wrappers

This review checked the installed Pingora 0.8.1 source and the local infra vendor.
The ordinary proxy path already delegates transport ownership to Pingora.
No second connection pool, retry engine, listener-transfer mechanism or worker
supervisor was added. The following source paths are relative to each named crate.

| Capability | Existing owner and source | Worker decision |
| --- | --- | --- |
| H1/H2 acquisition, reuse and idle eviction | `pingora-core/src/connectors/http/mod.rs`; `http/v2.rs` | Keep native connectors and pool. |
| ALPN and upstream H2 PING | `pingora-core/src/protocols/http/v2/client.rs` | Set native peer options. Native missing-PING-ACK timeout is 5 seconds. |
| Dial/TLS deadline | `pingora-core/src/connectors/mod.rs` | Keep native connection deadlines. They do not bound guest callback chains. |
| Proxy attempts | `pingora-proxy/src/lib.rs` | Keep native attempt loop; the initial attempt counts toward its limit. |
| Transparent H1 upgrade duplex | `pingora-proxy/src/proxy_h1.rs` | Keep native byte forwarding and backpressure. Semantic message control is separate. |
| Listener FD transfer and graceful shutdown | `pingora-core/src/server/mod.rs`, `server/transfer_fd/mod.rs`, `server/bootstrap_services.rs` | Keep native handoff, grace period and runtime shutdown. |
| Request logging hook | `pingora-proxy/src/proxy_trait.rs` | Emit application outcomes through the existing infra diagnostic runtime. |

Pool isolation includes ALPN because native `HttpPeer` hashing omits it. This is a
key correction around the native pool, not another pool. Native connection timeouts
cover L4/TLS work; message policy and owner cleanup need separate absolute deadlines.
Native logging hooks and daemon file redirection do not provide rotating,
nonblocking application sinks or expose their storage failures.

The local `infra/rust/vendor/pingora-proxy` extends the upstream trait with
WebSocket heartbeat hooks, absolute body-admission deadlines and an awaited
response-body filter. These are **local extensions**, not raw Pingora 0.8.1
features. Raw response-body filtering is synchronous. Infra owns protocol framing
and approved-message session control; it preserves native transport ownership.

Diagnostic queues and workers remain owned by `tracing-appender` 0.2.5. Its
nonblocking queue bounds record count and reports queue drops. It copies bytes
before enqueue and does not expose disk write failures. The existing rotating
file wrapper is retained: it opens and flushes before swapping and pruning, so a
failed reopen cannot prune the still-active file. `ResourceBudget` remains the
single exact weighted reservation primitive. Native `pingora-limits::Inflight`
provides estimated keyed counts, not weighted byte ownership.

## Fixes and contract evidence

| Previous issue | Fix | Validation |
| --- | --- | --- |
| Unbounded `hold=buffer`; reservation released while held wire survived | Held wire plus segment/object overhead is capped at twice the message limit. Message count uses the protocol frame limit. Each held message pins the inspection reservation through release writes. Payload storage stays shared. | Unit rollback/ownership checks; real repeated-hold exhaustion and held-response reservation across peer PING. |
| Diagnostic captures and queued records had no aggregate payload bound | One 64 MiB capture allowance per assembled proxy. Each capture reserves twice its limit before allocation. Each queue has at most 16 MiB payload allowance, using per-channel record limits and the existing lossy queue. | Concurrent request/response captures, denied reservations, overflow/drop/settlement release, escaping and oversized-record rejection. |
| stdout could block a request; storage failures were invisible | stdout and stderr use existing background sinks. Health separates queue drops, rejected records, write/flush/rotation/retention failures and capture usage. | Stalled stdout, full queue, failing writes/flushes and failed rollover/pruning tests; real process health binding. |
| Serial callback timeouts did not bound the complete chain | Message decisions use one absolute message deadline. All downstream cleanup owners are polled together; cleanup and completion share one I/O deadline. Native execution retains its owners after waiter expiry. | Two slow native callbacks exceed one total deadline before dial; concurrent cleanup and shared completion deadline checks. |
| Handoff tests lacked native callbacks | Real replacement-process tests track PID-tagged entry, return, output free, session close and instance destruction. | Successful handoff retains active call ownership; failed replacement initialization leaves old listener/call intact; stuck native code causes bounded failure exit without false cleanup claims. |

`worker.logging` exposes the selected logging owner over gRPC.
`worker.logging.events` uses the existing gRPC stream/SSE carrier and reports an
initial snapshot followed by changed health samples, once per second. It shares
the existing 32-source budget with `worker.events`; neither belongs to the notifier
chain. Entry cancellation and the 120-second carrier deadline remain authoritative.

`just bdd` includes native proxy, notifier, observation, capture and real process
handoff workflows. `just bdd-observability` covers real operation events, logging
health, shared observer capacity, persistence and response dumps. Infra tests
cover the reusable sink and capture primitives. Test success proves these
contracts; it does not establish production throughput or tail latency.

## Verification

The full worker suite passed 144 tests. One existing Go carrier integration test
requires an explicitly built `GO_CODEC_ARTIFACT` and remains ignored by default.
The diagnostic runtime suite passed 46 tests, including continuous-change sampling.
The Go plugin source guard and Rust formatting checks passed. Worker process and
observation tests were rerun after the final sampling fix.

## Remaining limits

Payload allowances are finite partitions, not a hard process RSS limit. With all
three sinks enabled, default queues account for at most 48 MiB, plus 64 MiB active
capture allowance. Active writes, concurrent formatting, parsed JSON trees,
allocator overhead and temporary copies need additional memory. Large native
allocations and syscalls remain outside host-interface admission.

Logging is lossy and shutdown drain is best effort. A health snapshot independently
samples counters. Usage changes can coalesce between samples. Error counters remain
cumulative, but no durable replay or per-record delivery acknowledgement is promised.

Native `.so` code cannot be forcibly stopped safely by a host callback deadline.
Its executing owner keeps inputs, handles and reservations until actual return.
A stuck native call can therefore consume capacity until process termination.
These changes add no helper process or claim of physical sandbox isolation.

Native listener transfer is not a transactional rollback protocol. Operator
sequencing and replacement readiness remain necessary. The tests do not claim
that every failed FD transfer resumes an old worker after SIGQUIT.

Operation counters omit controlled WebSocket-session bytes and L4 tunnel traffic.
Passive upgrades enter native body accounting; this is not semantic payload
measurement. Management reachability remains an operator trust boundary, without
built-in caller authentication. Observation cannot replace access-log history.
