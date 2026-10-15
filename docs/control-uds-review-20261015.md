# Local control and entry ownership review — 2026-10-15

## Implemented ownership

Entry decodes and dispatches `route_axes`. It has no shared/dedicated mounting
mode. Assembly selects carrier facilities and publishes entry handlers. Facilities
own listening, protocol multiplexing and connection/subscription lifetimes.

- `src/entry/{worker,plugins}.rs`: action declarations, decoding and axis dispatch.
- `src/assembly/control.rs`: facility selection and reversible installation.
- Infra `action-router`: namespace registration, publication and route retirement.
- Infra `action-execution`: actual call execution, cancellation, completion pins
  and injected dispatch. Carriers depend on this owner directly.
- Infra `network-local-control`: bounded direct Unix ttrpc transport.
- Infra `plugin-lifecycle::coordination`: memory custody, standalone Tool manager
  and full plugin coordination inside the existing lifecycle authority.

The separate plugin-manager crate and the carrier's forwarding RpcFacility were
removed. Foreign public re-exports from action-router were also removed. Consumers
now import contracts and execution types from action-execution. These are source
ownership changes, not role-label exceptions.

## Protocol and migration

Plugin control uses canonical ttrpc framing and Protobuf envelopes on UDS. It does
not use HTTP/2 or gRPC. Current action payloads remain bounded, versioned JSON;
they are not generated Protobuf domain messages. The general transport has no
shared-memory, allocation or coordinator dependency.

`plugin_ingress.facility=management` requires
`management.protocol=local_control`. A separate `plugin_ingress.listen` requires
a UDS endpoint. Outbound axis bindings also require UDS. Old `mount` fields and
TCP plugin endpoints fail configuration validation. Each native action needs at
least one binding supported by its selected management carrier; mixed bindings
are accepted. Independent HTTP/SSE/optional gRPC operational facilities remain
available through the existing management protocol.

## Validation

Worker tests cover both facility topologies, route isolation, explicit grants,
stale generations/incarnations, cancellation custody, replacement and shutdown.
The full suite passed 156 tests; one manual test remained ignored. The optional
Wasm object grant test passed. Worker strict Clippy passed with `--all-targets
--no-deps -- -D warnings`.

Official Go ttrpc v1.2.10 interoperability passed in both directions after the
execution extraction. It covers canonical protobuf fixtures, unary replies,
NotFound/PermissionDenied status and server streams. This external probe is not
yet a required cross-language CI job. The transcript and reproduction harness
are retained with this task's validation reports.

## Remaining limits

The Rust worker does not yet adapt the existing C arena/resource profile. That
same-kernel profile uses SOCK_SEQPACKET and SCM_RIGHTS bootstrap. Its physical
allocation and release proof remain separate from this owned-metadata transport.
A caller deadline, disconnect or local drain does not prove remote shared-memory
completion. No production throughput or tail-latency measurement is claimed.
