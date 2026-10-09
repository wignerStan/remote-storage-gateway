# Bridge and codec review — 2026-10-09

The worker uses native Pingora carriers. It imports the bridge primitives where
those carriers need them. A primitive's availability does not establish a call
site in every transport.

| Worker operation | Actual owner and integration |
| --- | --- |
| HTTP/1 and HTTP/2 | Pingora `ProxyHttp` owns delivery, flow control, and pool reuse. The worker does not invoke the separate `http1::transfer` / `http2::transfer` adapters. |
| Ordinary WebSocket | Pingora's upgraded-body loop retains native I/O and runs message filters. It does not use the stream-extracting `websocket::upgrade` adapter. |
| Controlled WebSocket | Infra `connect_upstream::websocket_session::Session` uses the bridge handshake validators and retains native Pingora sessions for reads and writes. |
| L4 byte bridge | Infra `RequestFlow::bridge` uses `stream_transfer::bridge`. The current native HTTP application has no L4 call site. |
| Separate translated source | Infra HTTP transfer adapters exist; this application does not call them. |
| Plugin messages | The typed Object `plugin-abi` runtime owns callbacks. The separate interop `infra_bridge_v1_call/free` contract serves language byte adapters and is not this worker's ABI. |

Infra now follows Pingora's response-start rule: an informational response such
as 103 does not block a final HTTP response or 101 upgrade. Successful CONNECT
responses require a tunnel carrier; they cannot enter the ordinary body bridge.
Real Pingora session tests cover these boundaries, including controlled WebSocket
accept after 103. No replacement transport or connection pool was added.

## Protocol decisions

The reference translator is `CLIProxyAPI/v7 v7.3.20`, pinned by infra's Go module.
Its response functions distinguish complete body translation from stream chunks.
Rust codecs read decision fields and preserve the original provider wire. The
CPA SDK integration is separately pinned to a v8 upstream source.

Chat finish/usage, Anthropic `message_delta`, and Gemini finish/usage events emit
`response.metadata`. Anthropic `message_stop` emits `response.done`. Ordinary
deltas emit `response.event` and bypass notifier callbacks. Terminal messages can
reach callbacks; response metadata requires its existing grant. No business
message is assembled from response deltas.

The SDK error builder allows nullable `param` and numeric `code`. These stay
opaque. Required error type/message and optional HTTP status remain validated.
Known decision-field duplicates fail closed. Other providers' fields cannot
change a codec's classification. Failed semantic inspection releases the entire
withheld carrier buffer immediately. Resource limits remain bounded; no extra
whole-message JSON tree or cross-ABI payload copy was introduced.

## Codex 0.162 compatibility

Version 0.161 already supported `response.interrupt`, but its
`features.instant_interrupt` default was false. Version 0.162 enabled the feature
by default. On Responses Lite, new input can interrupt an active response while
retaining the socket and its `previous_response_id` continuation.

The codec accepts the exact control frame with a nonempty `response_id` and
`mode: discard_partial_items`. It preserves original wire. A real upstream
`response.incomplete` with `incomplete_details.reason: interrupted` is a graceful
terminal notification, even without a general response metadata grant. Other
incomplete reasons remain errors. Both raw and controlled native WebSocket
workflows check control forwarding while the response is active, the interrupted
boundary, and the client's next create on the same connection.

The observed `unsupported websocket request type: response.interrupt` 400 is
an upstream CLIProxyAPI adapter rejection. Codec support alone cannot fix it.
The adapter must relay interrupt on its existing authenticated duplex connection.
It must not convert interrupt to steer or fabricate an incomplete response.
The existing native SDK duplex route requires `codex.response-steering: true`
and an eligible WebSocket credential. Its serial and HTTP-only routes cannot
interrupt an active response by merely accepting another request type.

A temporary Codex compatibility setting restores the previous feature default:

```toml
[features]
instant_interrupt = false
```

This disables immediate preemption. No production configuration was changed.

Sources: [interrupt implementation](https://github.com/openai/codex/commit/12de0e395d3313bc564190d983cb4f5acf0be713),
[default change](https://github.com/openai/codex/commit/406b0c44460cf71e90c21c7abe6e581add3a6421),
[0.161 feature default](https://github.com/openai/codex/blob/979011409de0a60b52f179721948e65531d26144/codex-rs/features/src/lib.rs#L1121-L1126),
and [0.162 feature default](https://github.com/openai/codex/blob/c1382380de69521303b416720a52f42d51af6248/codex-rs/features/src/lib.rs#L1158-L1163).

## Validation

Infra bridge suites test native H1/H2 delivery, informational heads, owned and
controlled upgrades, and CONNECT rejection. Infra LLM tests cover every input
split, provider metadata, opaque SDK error fields, malformed control envelopes,
terminal release, and shared wire ownership. Infra Go builds and package tests
cover the complete response adapters.

`just bdd-codecs` runs real TCP/native Pingora codec tests. The response workflow
checks granted accept/deny and ungranted passthrough. Delta events and heartbeat
comments retain their bytes and do not enter the notifier. SDK error replies
retain the connection for a later request. The full worker suite also covers
lifecycle, admission, callback permissions, logging, and graceful upgrade.
These tests are correctness evidence; they are not a production throughput or
tail-latency measurement.
