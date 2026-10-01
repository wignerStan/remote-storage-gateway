# Controlled WebSocket sessions

Set `message_actions: true` on a callback with a selected WebSocket codec to
install independent native Pingora downstream and upstream sessions. This is
explicitly selected; ordinary HTTP proxying and opaque upgrade forwarding keep
their existing paths. The configured upstream **origin remains fixed**. Protocol
selection, framing, socket ownership and backpressure stay in infra. Plugins own
model/account policy, request contents and the decision to reconnect.

## Sequence and ownership

1. Validate parsed ingress, run metadata transforms, authenticate and admit using
   the ordinary notifier chain. The initial `before_upstream_send` records the
   admitted request context without opening a socket.
2. Finalize and write a local downstream 101. This does not manufacture an
   `upstream_response` event and does not establish a business upstream.
3. Assemble a bounded first client message and invoke callbacks in registration
   order. Each participant receives the prior participant's validated rewrite.
4. After the complete decision chain passes, apply metadata to a draft, rerun
   `before_upstream_send` for the selected generation, then run pool/dial/socket
   controls. Only then send the native upstream handshake and approved message.
5. Run `upstream_response` for the actual upstream 101, including explicitly
   granted response headers. Response completion/error/metadata decisions remain
   on the same Object instance and request ID.
6. An explicit disconnect drops the old upstream and its queued messages. The
   downstream remains open; the next approved client message can dial again with
   newly selected headers/proxy. The worker never invents a replay request.
7. Close or failed transport notifications let participants release session
   state. Clean downstream completion attempts the original admitting
   participant's `complete` once. Abrupt process termination or future
   cancellation cannot guarantee successful native cleanup.

The `before_upstream_send` participant must preserve original ingress identity
when called again for a new generation; its later input is outbound metadata.
Neither an error reply nor a change in cookie bytes implicitly reconnects a
socket. A metadata change on a live connection requires `replace` or explicit
retirement, so a plugin cannot report a new handshake that was never sent.

## Grants

```json
{
  "path": "plugins/policy.so",
  "package": "example.policy",
  "kind": "object",
  "hooks": ["before_upstream_send", "upstream_response"],
  "headers": ["authorization", "x-turn-state"],
  "patch_headers": ["authorization", "cookie", "x-turn-state"],
  "message_codec": "openai.responses.v1",
  "message_actions": true,
  "message_proxy": true,
  "message_responses": false
}
```

`message_actions` grants complete-message replacement and session actions only
on the controlled WebSocket path. `patch_headers` independently limits writable
headers; transport/framing/negotiation headers remain protected.
`message_proxy` additionally authorizes native callbacks to select an account
proxy or direct egress; it defaults false and is unavailable to Wasm. Proxy
failure never falls back to direct egress. The `path` result selects only an
origin-form path within the configured upstream origin.

Responses/Codex callbacks with action grants receive completion, error, creation
and metadata messages even when general `message_responses` is false. Ordinary
delta events bypass native callbacks unless response access is explicitly
enabled. Bodies are never added to metadata trace logs.

## Result wire format

The input method remains `codec/kind/<request_id hex>/<sequence hex>`, followed
by the original unframed message bytes. An unchanged result is the single byte
`0`; byte `1` rejects. Existing codec-validated JSON error replies remain valid.

An action result consists of:

| Bytes | Meaning |
| --- | --- |
| 0 | Discriminator `2` |
| 1–4 | Little-endian unsigned metadata length |
| next length bytes | Strict UTF-8 JSON metadata, at most 64 KiB |
| remaining bytes | Optional raw replacement/reply payload, at most the message limit |

Example metadata:

```json
{
  "version": 1,
  "decision": "forward",
  "upstream": "replace",
  "set_headers": [["cookie", "selected-session"]],
  "remove_headers": [],
  "proxy": "http://selected-proxy:3128",
  "path": "/v1/responses"
}
```

`decision` is `forward`, `reply`, or `drop`. Forward with an empty payload keeps
the original wire; nonempty payload replaces exactly one semantic message and
must pass the selected codec again. Reply suppresses the original message and
requires a codec-valid error payload. Drop requires an empty payload.

`upstream` is `keep` (default), `connect`, `replace`, or `disconnect`. Connect
ensures a connection exists; replace drops the old one before the next approved
send. Disconnect pairs with reply/drop and never forwards the rejected message.
Response callbacks can retain or disconnect an upstream, but cannot choose a
new destination or patch request headers. Conflicting decisions, ungranted
patches and malformed frames fail before any partial action is published.
Omitted `proxy` preserves selection; explicit `null` selects direct egress.

`transport.closed` is a host control event carrying bounded JSON with `side`
(`upstream` or `downstream`), `reason`, and an upstream `generation` when known.
Upstream closure may return a replay/error reply while retaining downstream.
Downstream closure is cleanup only; its returned output is not written to the
closed connection. Generation checks prevent an old queued frame or close
notification from affecting a replacement socket.

## Bounds and verification

The existing worker admission and inspection budgets apply. A message has one
assembly deadline from its first retained byte, separate from idle I/O and
callback deadlines. Idle sessions release inspection reservations; native calls
retain their input reservation through actual native return. Pending messages
during callback/dial work are bounded, while Ping/Pong maintenance continues.
No helper process, detached per-socket heartbeat task or asynchronous C ABI is
introduced. The allow path retains original wire segments; replacement data is
copied from its native reply allocation once and remasked for upstream framing.

Wire regressions in `tests/native_proxy/message_sessions.rs` exercise actual
shared-library callbacks and Pingora sessions: no dial before an approved first
message, fragmented messages with intervening Ping, rewritten upstream body and
headers, old-socket EOF after replay rejection, continued downstream connection,
new-cookie replay, and WebSocket closure without an HTTP error after 101.
Infra separately tests generation retirement, bounded maintenance and notifier
phase cancellation. These tests do not claim a production throughput benchmark.
