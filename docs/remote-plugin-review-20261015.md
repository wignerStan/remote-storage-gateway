# Remote plugin integration review — 2026-10-15

Historical review of the earlier dedicated-entry implementation. The later
[shared-entry and coordination review](plugin-coordination-review-20261015.md)
supersedes its entry-separation rule and exact-call evidence conclusion. Runtime
callback arrows below are not source dependency arrows.

The remote plugin path is now carrier facility → plugin entry → remote plugin
manager → axis surface. Worker data ingress and worker management remain separate.
The earlier connection map was only a transport primitive and did not implement
plugin membership. The earlier `SharedEntry::install_actions` guidance was wrong
for remote plugin ingress and has been removed from the current architecture.

## Changes checked in this iteration

- Infra owns bounded membership, immutable contract identity, manager authority,
  monotonic generations, activation, withdrawal and retained failed cleanup. It
  reuses the existing installation, invocation gates and runtime disposal.
- Worker use-flow selects host-issued publications through a pure surface. Plugin
  ingress accepts only existing axis/registration selections. It cannot install
  new grants, endpoints, native artifacts or header permissions.
- Incoming plugin and worker operations use separate real Unix listeners and
  routers. Integration tests reject cross-entry methods, stale incarnations,
  stale generations and unknown fields. Withdrawal fails callback admission;
  reactivation retains notifier order and grants.
- An owned callback task retains its membership pin after its waiter is cancelled.
  A real held RPC response verifies that withdrawal waits for local completion.
- Infra async actions retain their invocation through cancellation. Synchronous
  native actions retain their original blocking path. Each execution path validates
  input once. Async constructor panic becomes task failure.
- The existing WebSocket held-frame release now moves the Vec allocation instead
  of collecting into a second allocation. No body or delta RPC path was added.
- V10 source registration now names the actual candidate_snapshot.rs owner.
  The SQL port consistency check now uses v10 internal_port_kind/port fields.

## Remaining gaps

| Area | What is still missing | What the result permits |
| --- | --- | --- |
| Provider trust | The local RPC profile has no peer authentication or capability negotiation | Use operator-controlled local endpoints; package identity is not authentication |
| Remote completion | A local deadline does not stop or join the remote operation | No automatic retry of ambiguous requests; no shared-memory reclamation from timeout |
| Payload binding | RPC accepts at most 64 KiB of metadata; body/delta and shared-memory transfer are not integrated | Keep native Pingora body flow and the separate existing codec path |
| Lifecycle inspection | State reports availability, not a complete warming/retiring/quarantine model | Inspect errors and re-query after an ambiguous control result |
| Native isolation | Native libraries share host process privileges | Curate native providers; host interface grants are not a memory or syscall sandbox |
| Inference scope | A worker-only scan does not acquire the full infra provider graph | Local dependency coverage remains advisory rather than an invented proof |
| Semantic routing | The worker has no declared semantic-axis map; ordinary calls or generic type parameters do not prove exact runtime publication | Current source-first assessment remains inconclusive |
| C/FFI evidence | C fixtures lack complete preprocessing, type, linker and ABI evidence | Parsed syntax cannot certify FFI ownership or runtime safety |
| Performance | No production throughput/tail-latency measurements were run | Tests establish functional behavior only |

The checker now models explicit remote ingress separately from binding runtime and
protocol owners. A manager may also own its binding implementation; no extra wrapper
owner is required. Declarations are constraints, not call evidence. Exact observed
contract dispatch can establish misuse; unresolved, conditional or shadowed calls
must remain advisory. Python parity review found and corrected weak call witnesses
and inclusion of methods outside the declared meaning-port operations.

## Validation interpretation

Default worker tests and strict Clippy pass. The real RPC cancellation and ingress
separation cases are included. One parallel all-target run observed a connection
reset in the pre-existing invalid-replacement upgrade test. The isolated test and
a complete subsequent all-target run passed. The cause of that transient reset is
not established; this review does not claim the upgrade test is deterministic.

Raw results, including the failed run, are retained under
`/develop/.codex/work/remote-manager-20261015/reports`. The final checker report is
`checker-worker-architecture.json`; its evidence gaps are not waived by these tests.
This iteration did not deploy a production Pingora worker.
