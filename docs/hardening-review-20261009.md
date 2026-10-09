# Worker hardening review

Reviewed 2026-10-09, Asia/Singapore. The review covers the current worker and
its matching infra main dependency, `cd347723`. It excludes sidecar business
policy. Native plugins remain trusted code in the same process. Host limits do
not provide memory or syscall isolation and cannot stop native execution.

## Fixed in this change

Idle event subscriptions previously held all 128 shared management admission
slots. This could prevent `worker.get`, `worker.apply`, `worker.metrics` and
plugin actions from starting until disconnect or the 120-second deadline.

[Observability](../src/observability.rs) now uses the existing infra
`ResourceBudget` to cap live event sources at 32 per entry. gRPC and SSE share
the cap. A source owns its reservation through cancellation and cleanup. This
preserves shared entry capacity during steady event subscriptions. It does not
provide control priority against every request burst or other plugin action.

`just bdd-observability` verifies the cap with 16 gRPC and 16 SSE subscriptions,
rejects extra subscriptions, executes real control reads/updates, and proves
capacity reuse after disconnect. It also checks real native streaming delivery,
termination counts, observer reconnect, deadline, revocation and replay rejection.
Access persistence and response capture tests run in the same recipe. These tests
provide contract evidence, not production throughput or tail-latency measurements.

## Remaining work, in priority order

### 1. Bound held WebSocket messages — confirmed defect, P1

At [websocket_session.rs](../src/proxy/websocket_session.rs), the
`plan.hold == "buffer"` branch appends complete message wire buffers to `held`
without an aggregate byte or message limit. Repeated valid buffer decisions can
retain unbounded data. The fixed inspection reservation cannot cover that growth.
The loop also releases its reservation when `transport.has_buffered_data()` is
false, without checking `held`; a later event can leave held data unreserved.

Add aggregate held-byte/message bounds and retain reservations until both the
transport and held messages release their data. Charge shared allocations once
where possible. Preserve forward order and fail before retaining data beyond the
budget. Test repeated buffer decisions, intervening control/maintenance events,
release, discard and cancellation. Existing hold-order and partial-message tests
do not prove these bounds. This review does not fix this path.

### 2. Bound diagnostic memory across requests and sinks — confirmed limit, P2

[Logging configuration](../src/config.rs) permits a 1 MiB capture limit and
defaults to 8,192 queued records. Infra capture limits each request and response
buffer. The infra logging queue bounds record count, not retained bytes. With
explicitly enabled capture and a slow sink, 8,192 near-limit body details alone
can approach 8 GiB, before JSON escaping and allocation overhead. Inspection
reservations do not own diagnostic buffers or queued logging records.

Add an aggregate diagnostic byte budget in infra for active capture and queued
output. Make overload discard detail or drop records without waiting on request
tasks. Test a stalled sink, concurrent near-limit captures, hashing expansion and
shutdown. Raw/sanitized capture consent and access retention must remain separate.
Disabled capture already avoids these body allocations.

### 3. Make stdout nonblocking and expose log loss — existing limits, P2

Without `access_file`, infra `diagnostic-output::runtime::AccessWriter` writes to
stdout directly. A blocked pipe can stall request completion. Configured file
sinks already use a nonblocking lossy queue.

`LoggingGuard::dropped_records` reports queue losses, but assembly retains the
guard privately. Disk write/flush failures are excluded from that count. Current
`worker.metrics/events` expose operation counters only. Forwarding can therefore
look healthy while diagnostic records are lost.

Give stdout a background owner with bounded admission. Expose queue drops and
storage errors as separate health facts through infra observability, outside the
notifier chain. Test blocked stdout, queue exhaustion and disk failure separately;
they have different causes. Do not describe operation counters as log health.

### 4. Add a complete session-decision deadline — existing limit, P2

[Session evaluation](../src/proxy/websocket_session.rs) visits callbacks serially
with individual timeouts. `transport.drive_until(evaluate(...))` has no absolute
deadline for the entire chain. Cleanup also awaits participants serially. Healthy
heartbeats can keep a connection alive while its admission and buffers remain
held for the sum of participant timeouts.

Add explicit total decision and cleanup deadlines. Keep native call owners and
reservations pinned until actual return when a host wait expires. Test several
slow callbacks, heartbeat progress and cleanup retry. Existing single-slow-callback
tests do not establish a total chain bound.

### 5. Test native callbacks during process handoff — validation gap, P2

[Graceful upgrade tests](../tests/graceful_upgrade.rs) prove listener transfer
and ordinary HTTP draining with no native callbacks configured. Separate callback
tests prove native owner pinning and bounded host waits. Their combination is
not yet tested in a real replacement process.

Add handoff tests while native calls run. Cover failed replacement initialization,
an old generation reaching its drain deadline, and native return/cleanup markers.
These tests should verify resource ownership, rather than assume a signal stopped
native code. This is missing integration evidence, not a confirmed handoff defect.

## Intentional observation limits

Snapshots independently sample atomics and coalesce transitions. They reset on
restart and offer no durable replay. Access logs retain per-request history.
HTTP byte counters currently omit controlled WebSocket session bytes and L4
tunnels. Passive upgrades still enter the native body accounting hooks; their
counts are not semantic WebSocket payload measurements. Add separate counters after defining payload versus
wire-byte accounting; do not silently widen the current HTTP counter contract.
Management reachability remains an operator trust boundary, without built-in
caller authentication. These limits are distinct from the defects listed above.
