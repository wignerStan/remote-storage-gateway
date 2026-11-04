# Versioned REAPI / ByteStream transfer bridge

This shared library transparently forwards authorized encoded gRPC messages to
an operator-installed upstream. It is not a CAS implementation, action cache,
execution scheduler, or durable ByteStream upload store. The upstream owns digest
validation, resource names, committed offsets, finalization, caching, execution
and resumability. The control plane supplies only a bounded routing decision.

The current profile uses these official generated package versions:

- `github.com/bazelbuild/remote-apis v0.0.0-20260331222004-becdd8f9ff81`
- `google.golang.org/genproto/googleapis/bytestream v0.0.0-20260807164820-c8921c73eeea` selected by the reviewed build's Go module graph
- `cloud.google.com/go/longrunning v0.9.0`

The plugin derives method cardinality from those official descriptors. The exact
15-method inventory is guarded by `TestPinnedOfficialMethodInventory`. A CP
decision must separately authorize each invoked method and logical upstream.

| Service | Supported methods | Actual shared-library tests |
| --- | --- | --- |
| Capabilities | GetCapabilities | Unary reply; rich status/details and binary trailers |
| ActionCache | GetActionResult, UpdateActionResult | Round-trip action result and raw output bytes |
| ContentAddressableStorage | FindMissingBlobs, BatchUpdateBlobs, BatchReadBlobs | Missing/present blobs, payload bytes and per-item error status |
| ContentAddressableStorage | GetTree | Multiple pages/messages and clean EOF |
| ContentAddressableStorage | SplitBlob, SpliceBlob | Request/reply digest fields retained |
| Execution | Execute, WaitExecution | Queued and completed operation messages, response Any and EOF |
| Operations | GetOperation | Standard execution-operation polling |
| ByteStream | Read | Offsets/limits, negative limit, empty EOF, cancellation |
| ByteStream | Write | Initial headers before producer data, streaming before EOF, interruption, resume and finish |
| ByteStream | QueryWriteStatus | Missing resource, partial committed size and complete state |

Split/Splice fixtures verify forwarding, not the origin's chunking algorithm.
Execution fixtures verify operation transport, not actual process execution.
Resumption is a new client RPC using the origin's committed offset; the bridge
does not replay writes or promise exactly-once execution.

`encoded/v2` requires independent receipt of initial upstream headers, followed
by publishing those headers before any response body. This avoids waiting for a
body when a caller is waiting for headers before producing its first message.
Unary directions require exactly one message; upstream application errors still
retain their original status and details. Version 1 artifacts are rejected at
admission; there is no compatibility shim. The underlying Infra C ABI is unchanged.

This is a **versioned bridge profile**, not a claim to cover every latest REAPI
extension. The newer v2.13 GetChunkMapping/RegisterChunkMapping RPCs are absent
from the pinned generated bindings and are rejected. Optional Operations
Cancel/Delete/List/Wait are not granted by the GetOperation integration. Remote
Asset APIs and HTTP/JSON transcoding are outside this profile.

Incoming authorization and arbitrary request headers are not forwarded to the
origin. Upstream credentials come from local operator configuration. In this
profile the optional REAPI `requestmetadata-bin` header is also not forwarded;
the finite CP decision-metadata allowlist is not a binary-header transport.
Response headers, trailers, status details and encoded messages are preserved.

The no-copy claim covers application BufferSlice segment sharing inside the data
plane. It excludes official endpoint protobuf conversion, kernel/TLS/compression
and the existing client/v1 JSON chunk transport. Native control calls carry only
opaque frame references and bounded metadata. Aggregate admission covers host
leases and message decoding; it is not a bound on total process memory.
