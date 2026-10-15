# Shared entry and plugin coordination review — 2026-10-15

This review supersedes the entry-separation rule in the earlier
[remote plugin review](remote-plugin-review-20261015.md). Entry can be shared.
Transport delivery invokes an injected handler; it does not create a source
import from the RPC facility to entry.

## Implemented ownership

| Owner | Responsibility |
| --- | --- |
| General RPC facility | Unary/streaming delivery, transport limits, independent route publications and multiple subscribers |
| Entry and assembly | Select the listener or mount, decode requests, dispatch each route to its owning manager |
| Remote plugin manager | Apply host-issued axis grants, manage registration generations and retain admitted calls |
| Plugin coordinator | Bound process-local allocations and subscriber custody |
| Tool-provider manager | Bind typed providers and retain exact consumer dependencies; usable alone by a host |
| Full plugin manager | Optionally compose a Tool manager, coordinate App cleanup before provider destruction |

RPC has no shared-memory, allocator or plugin-coordinator dependency. The existing
bounded tonic service can mount on an API server. The existing Axum mount serves
HTTP actions, not generated gRPC. Dedicated listeners reuse the existing socket
and `ActionListener` owners.

`plugin_ingress: {"mount":"management"}` publishes plugin controls on the existing
management entry. `plugin_ingress: {"listen":"unix:///..."}` selects a separate
local listener. The configuration rejects mixed selections and missing shared
listeners. Route owners remain separate in both configurations. Unknown selections,
stale incarnations/generations and attempts to widen grants still fail.

Process assembly injects one memory coordinator into direct and RPC notifiers.
It bounds 64 subscribers, 128 retained allocations and 8 MiB of reserved capacity.
Serialization writes directly into a bounded buffer which grows as needed. Native
completion retains the lease through the C ABI call. The RPC adapter retains it in
its owned local exchange after caller cancellation. These are metadata buffers;
body inspection and streaming remain on their existing paths.

Review found and fixed an unread installation-result race in infra. Successful
channel send is not proof of delivery. A delivery guard now retires a successful
installation when its caller drops the unread result. The guard enters its owning
runtime when dropped from another thread. This prevents an orphan Full activation
from retaining Tool dependencies without its retirement watcher.

## Validation and limits

Worker tests cover both listener choices, independent route ownership, real RPC
withdrawal/reactivation, cancellation with retained memory custody, and native
budget rejection before the C ABI call. The final default run passed 154 tests, with one manual case ignored. The optional
Wasm integration case and worker strict Clippy also passed. Infra coordination
passed 35 tests and strict Clippy. RPC facility/mount suites passed 21 tests; their
focused Clippy check allowed existing warnings in unchanged files. Infra tests cover delivery cancellation, Tool/App retirement order, failed cleanup retention,
subscriber limits and independent RPC publications. Checker tests cover shared entry routes, strict evidence guards and the Tool
form/binding matrix. All 1,150 Python tests and the focused Rust tests passed.

The registration-free scan covers the worker and changed infra manager, lifecycle
and RPC crates. Its report has zero blockers, 48 advisories, three suppressions and no infrastructure
or parser failures. It remains inconclusive. Missing selected provider roots,
unknown semantic axes, generated tonic imports and C fixture semantic acquisition
limit the proof. A new manager classification also remains uncertain: one ambient
operation does not prove whether its owner is a Tool or a lifecycle facility.
Adding `install`/`withdraw` name hints would not supply that missing typed proof.
Declarations and passing runtime tests do not replace source evidence.

Physical shared-memory transfer is still a separate gap. Infra already has C
arena primitives, but this change does not add a Rust adapter or bind them to worker
RPC. Local timeout can end a control exchange; it does not prove remote completion
or authorize reclaiming shared pages. Tool adapters remain owner-supplied typed
ports, with no new automatic artifact loading or remote capability negotiation.
Native libraries remain trusted host code. No production deployment, throughput
benchmark or tail-latency measurement was performed.

Raw validation output is retained in
`/develop/.codex/work/plugin-coordination-20261015/reports`.
