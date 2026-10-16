//! Registered host protocol codecs and bounded message callbacks.
use super::*;
use bytes::{Bytes, BytesMut};
use plugin_loading::Callback;
use proxy_protocols::message_codec::Decoder;
use proxy_protocols::websocket::Limits;

#[cfg(test)]
pub(crate) const MESSAGE_LIMIT: usize = 1024 * 1024;

/// Shares the metadata callback's native instance or remote registration owner.
#[derive(Clone)]
pub(crate) struct MessageCallback {
    transport: CallbackTransport,
    timeout: Duration,
    response_enabled: bool,
    grant: Arc<super::message_action::Grant>,
}
#[derive(Clone)]
enum CallbackTransport {
    Native(Arc<Callback>),
    Remote(crate::remote_axis::RemotePluginBinding),
}
impl MessageCallback {
    pub(crate) fn new(instance: Arc<Callback>, timeout: Duration, response_enabled: bool) -> Self {
        Self {
            transport: CallbackTransport::Native(instance),
            timeout,
            response_enabled,
            grant: Arc::new(super::message_action::Grant::default()),
        }
    }
    pub(crate) fn remote(
        binding: crate::remote_axis::RemotePluginBinding,
        timeout: Duration,
        response_enabled: bool,
    ) -> Self {
        Self {
            transport: CallbackTransport::Remote(binding),
            timeout,
            response_enabled,
            grant: Arc::new(super::message_action::Grant::default()),
        }
    }
    async fn exchange<T: Send + 'static>(
        &self,
        method: &str,
        input: Bytes,
        reservation: Option<Arc<admission_gate::budget::Reservation>>,
        decode: impl FnOnce(&[u8]) -> T + Send + 'static,
    ) -> std::result::Result<T, ()> {
        match &self.transport {
            CallbackTransport::Native(instance) => instance
                .start_call_with_reply(
                    &tokio::runtime::Handle::current(),
                    method.to_owned(),
                    input,
                    self.timeout,
                    reservation,
                    decode,
                )
                .await
                .map_err(|_| ()),
            CallbackTransport::Remote(binding) => binding
                .message(method, input, self.timeout, reservation)
                .await
                .map(|reply| decode(&reply))
                .map_err(|_| ()),
        }
    }
    pub(crate) fn with_actions(
        mut self,
        enabled: bool,
        proxy: bool,
        headers: Vec<http::header::HeaderName>,
    ) -> Self {
        self.grant = Arc::new(super::message_action::Grant {
            enabled,
            proxy,
            headers,
        });
        self
    }
    pub(super) fn interested(&self, response: bool, kind: &str) -> bool {
        response_kind_reaches_plugin(response, self.response_enabled, self.grant.enabled, kind)
    }
    pub(super) async fn decide(
        &self,
        method: &str,
        input: Bytes,
        response: bool,
        max_bytes: usize,
        reservation: Option<Arc<admission_gate::budget::Reservation>>,
    ) -> Result<super::message_action::Reply> {
        let grant = Arc::clone(&self.grant);
        self.exchange(method, input, reservation, move |reply| {
            super::message_action::decode(reply, &grant, response, max_bytes)
        })
        .await
        .map_err(|_| refusal(502, "message callback failed"))?
    }

    /// Teardown output cannot affect an already closed connection. Visit and
    /// free it in place instead of allocating a replacement or action plan.
    pub(super) async fn notify_closed(
        &self,
        method: &str,
        input: Bytes,
        reservation: Option<Arc<admission_gate::budget::Reservation>>,
    ) -> Result<()> {
        self.exchange(method, input, reservation, |_| ())
            .await
            .map_err(|_| refusal(502, "session cleanup callback failed"))
    }
}

/// Delta/event chunks remain in the carrier, regardless of callback grants.
/// Complete terminal messages and separately granted metadata can be notified.
fn response_kind_reaches_plugin(
    response: bool,
    response_enabled: bool,
    actions: bool,
    kind: &str,
) -> bool {
    if response && kind == "response.event" {
        return false;
    }
    !response
        || response_enabled
        || matches!(
            kind,
            "response.completed" | "response.done" | "response.incomplete"
        )
        || (actions
            && matches!(
                kind,
                "response.metadata" | "response.created" | "response.error" | "transport.closed"
            ))
}

pub(crate) use proxy_pingora::http_ingress::message_codec::{
    CodecPlugin, Transport, resolve_all, valid_id, validate_selection,
};

/// Validate components before native affinity parses the slash-separated method.
/// Delay formatting until a callback is interested, keeping delta bypass cheap.
pub(super) fn callback_method<'a>(
    codec: &'a str,
    kind: &'a str,
    request_id: u64,
    sequence: u64,
) -> Result<impl std::fmt::Display + 'a> {
    // Three separators and two fixed-width hexadecimal IDs accompany the names.
    if !valid_id(codec) || !valid_id(kind) || codec.len() + kind.len() + 35 > 256 {
        return Err(refusal(502, "invalid codec callback method"));
    }
    Ok(CallbackMethod {
        codec,
        kind,
        request_id,
        sequence,
    })
}

struct CallbackMethod<'a> {
    codec: &'a str,
    kind: &'a str,
    request_id: u64,
    sequence: u64,
}
impl std::fmt::Display for CallbackMethod<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{:016x}/{:016x}",
            self.codec, self.kind, self.request_id, self.sequence
        )
    }
}

#[derive(Clone)]
pub(crate) struct CodecRegistration {
    pub(crate) codec: Arc<dyn CodecPlugin>,
    pub(crate) callbacks: Vec<MessageCallback>,
}

impl CodecRegistration {
    pub(super) fn session_control(&self) -> bool {
        self.callbacks.iter().any(|callback| callback.grant.enabled)
    }
    pub(crate) fn response_enabled(&self) -> bool {
        self.callbacks.iter().any(|c| c.response_enabled)
    }
}
impl AsRef<dyn CodecPlugin> for CodecRegistration {
    fn as_ref(&self) -> &(dyn CodecPlugin + 'static) {
        self.codec.as_ref()
    }
}

pub(crate) fn select_request(
    registrations: &[CodecRegistration],
    request: &RequestHeader,
) -> Result<Option<(usize, Transport)>> {
    proxy_pingora::http_ingress::message_codec::select_request(registrations, request)
}

#[cfg(test)]
pub(super) async fn filter(
    callbacks: &[MessageCallback],
    decoder: &mut dyn Decoder,
    codec_id: &str,
    sequence: &mut u64,
    request_id: u64,
    body: &mut Option<Bytes>,
    end: bool,
) -> Result<()> {
    filter_owned(
        callbacks,
        decoder,
        sequence,
        body,
        end,
        FilterContext {
            codec_id,
            request_id,
            reservation: None,
            response: false,
            max_bytes: 1024 * 1024,
            reply_codec: None,
        },
    )
    .await
    .map(|_| ())
}

/// Identity, resource ownership and reply grants for one codec filter call.
pub(super) struct FilterContext<'a> {
    pub codec_id: &'a str,
    pub request_id: u64,
    pub reservation: Option<Arc<admission_gate::budget::Reservation>>,
    pub response: bool,
    pub max_bytes: usize,
    pub reply_codec: Option<&'a dyn CodecPlugin>,
}

pub(super) async fn filter_owned(
    callbacks: &[MessageCallback],
    decoder: &mut dyn Decoder,
    sequence: &mut u64,
    body: &mut Option<Bytes>,
    end: bool,
    context: FilterContext<'_>,
) -> Result<Option<Bytes>> {
    let FilterContext {
        codec_id,
        request_id,
        reservation,
        response,
        max_bytes,
        reply_codec,
    } = context;
    let had_body = body.is_some();
    let original = body.clone();
    let mut events = decoder
        .push(body.take().unwrap_or_default())
        .map_err(|_| refusal(if response { 502 } else { 400 }, "invalid protocol stream"))?;
    // EOF events must be validated before callbacks or forwarding any batch.
    if end {
        events.extend(decoder.finish_events().map_err(|_| {
            refusal(
                if response { 502 } else { 400 },
                "incomplete protocol stream",
            )
        })?);
    }
    let limits = Limits::default();
    let wire_bytes = events
        .iter()
        .flat_map(|event| &event.wire)
        .try_fold(0usize, |total, chunk| total.checked_add(chunk.len()));
    let segments = events
        .iter()
        .try_fold(0usize, |total, event| total.checked_add(event.wire.len()));
    let payload_bytes = events
        .iter()
        .filter_map(|event| event.message.as_ref())
        .try_fold(0usize, |total, message| {
            total.checked_add(message.payload.len())
        });
    // A push may complete previously held wire and emit the current chunk.
    let batch_bytes = 2 * (max_bytes + 64 * 1024);
    if events.len() > limits.max_frames
        || segments.is_none_or(|count| count > limits.max_segments)
        || payload_bytes.is_none_or(|length| length > batch_bytes)
        || wire_bytes.is_none_or(|length| length > batch_bytes)
    {
        return Err(refusal(502, "message codec output exceeds wire budget"));
    }
    let mut wire = Vec::new();
    let mut downstream = BytesMut::new();
    for event in events {
        let mut suppress = false;
        if let Some(message) = event.message {
            if !valid_id(message.kind)
                || message.kind.len() > 64
                || message.payload.len() > max_bytes
            {
                return Err(refusal(502, "invalid codec message kind"));
            }
            let method = callback_method(codec_id, message.kind, request_id, *sequence)?;
            let message_sequence = *sequence;
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| refusal(400, "message sequence exhausted"))?;
            // Validate sequence/kind but bypass delta callbacks before allocating
            // a method string or entering the native plugin runtime.
            if response && message.kind == "response.event" {
                wire.extend(event.wire);
                continue;
            }
            if !callbacks
                .iter()
                .any(|callback| callback.interested(response, message.kind))
            {
                wire.extend(event.wire);
                continue;
            }
            let method = method.to_string();
            for (callback_index, callback) in callbacks.iter().enumerate() {
                if !callback.interested(response, message.kind) {
                    continue;
                }
                let started = Instant::now();
                let decision = callback
                    .exchange(
                        &method,
                        message.payload.clone(),
                        reservation.clone(),
                        |reply| match reply {
                            [0] => NativeReply::Allow,
                            [1] => NativeReply::Deny,
                            other => NativeReply::Other(Bytes::copy_from_slice(other)),
                        },
                    )
                    .await;
                tracing::debug!(target: "proxy_pingora::message", request_id, sequence = message_sequence,
                    codec = codec_id, kind = message.kind, bytes = message.payload.len(), callback_index,
                    elapsed_us = started.elapsed().as_micros() as u64,
                    outcome = match &decision { Ok(NativeReply::Allow) => "allow", Ok(NativeReply::Deny) => "deny", Ok(NativeReply::Other(_)) => "protocol_reply", Err(_) => "error" },
                    "native message decision");
                match decision.map_err(|_| refusal(502, "message callback failed"))? {
                    NativeReply::Allow => {}
                    NativeReply::Deny => return Err(refusal(403, "message denied")),
                    NativeReply::Other(reply) => {
                        let frame = reply_codec
                            .and_then(|codec| codec.response_error(&reply))
                            .ok_or_else(|| refusal(502, "invalid message callback reply"))?;
                        if downstream
                            .len()
                            .checked_add(frame.len())
                            .is_none_or(|n| n > max_bytes + 64 * 1024)
                        {
                            return Err(refusal(502, "protocol reply exceeds batch budget"));
                        }
                        downstream.extend_from_slice(&frame);
                        suppress = true;
                        break;
                    }
                }
            }
        }
        if !suppress {
            wire.extend(event.wire);
        }
    }
    let downstream = (!downstream.is_empty()).then(|| downstream.freeze());
    let original_matches = original.as_ref().is_some_and(|original| {
        let mut offset = 0;
        for chunk in &wire {
            if offset + chunk.len() > original.len()
                || chunk.as_ptr() != original.as_ptr().wrapping_add(offset)
            {
                return false;
            }
            offset += chunk.len();
        }
        offset == original.len()
    });
    if original_matches {
        *body = original;
        return Ok(downstream);
    }
    *body = match wire.len() {
        0 if had_body => Some(Bytes::new()),
        0 => None,
        1 => wire.pop(),
        _ => {
            let length = wire.iter().map(Bytes::len).sum();
            let mut combined = BytesMut::with_capacity(length);
            for chunk in wire {
                combined.extend_from_slice(&chunk);
            }
            Some(combined.freeze())
        }
    };
    Ok(downstream)
}

enum NativeReply {
    Allow,
    Deny,
    Other(Bytes),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proxy_pingora::http_ingress::message_codec::{WebSocketCodec, resolve};

    #[tokio::test]
    async fn remote_callback_uses_native_action_grants_and_response_decoder() {
        use crate::remote_axis::{
            AxisBinding, RemotePluginManager,
            surface::{Grant, Surface},
        };
        use action_execution::{
            Action, AsyncHandler, Binding, Context, Contract, ResponseMode, Schema,
        };
        use network_local_control::{ControlService, listener::ControlListener};
        let router = action_router::Router::new();
        let registration = router
            .register(vec![Action {
                contract: Contract {
                    id: "worker.message".into(),
                    version: 1,
                    response: ResponseMode::Unary,
                    input: Schema::Bytes,
                    output: Schema::Bytes,
                    max_input: 1024,
                    max_output: 1024,
                },
                bindings: vec![Binding::LocalControl {
                    action: "worker.message.v1.policy".into(),
                }],
                handler: Arc::new(AsyncHandler(|_: Vec<u8>, _: Context| async {
                    let metadata =
                        br#"{"version":1,"decision":"forward","proxy":"http://127.0.0.1:8080"}"#;
                    let mut reply = vec![2];
                    reply.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
                    reply.extend_from_slice(metadata);
                    Ok(reply)
                })),
            }])
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("callback.sock");
        let listener = ControlListener::prepare(
            tokio::net::UnixListener::bind(&path).unwrap(),
            None,
            ControlService::new(Arc::new(router), 65541 + 65536, Duration::from_secs(5), 4)
                .unwrap(),
        )
        .unwrap();
        listener.activate().unwrap();
        let manager = RemotePluginManager::connect(
            std::collections::BTreeMap::from([(
                "axis".into(),
                AxisBinding {
                    endpoint: format!("unix://{}", path.display()),
                    max_inflight: 2,
                    max_message_bytes: 65536,
                    connect_timeout_ms: 5000,
                    shutdown_timeout_ms: 5000,
                },
            )]),
            Surface::new(vec![
                Grant::new("axis".into(), "policy".into(), &[])
                    .with_codec(Some("codex.responses.v1".into())),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
        let callback = MessageCallback::remote(
            manager.binding("axis", "policy").unwrap(),
            Duration::from_secs(5),
            false,
        );
        let method = "codex.responses.v1/response.create/0000000000000001/0000000000000002";
        assert!(
            callback
                .decide(method, Bytes::new(), false, 65536, None)
                .await
                .is_err()
        );
        let callback = callback.with_actions(true, false, vec![]);
        assert!(
            callback
                .decide(method, Bytes::new(), false, 65536, None)
                .await
                .is_err()
        );
        let callback = callback.with_actions(true, true, vec![]);
        assert!(matches!(
            callback
                .decide(method, Bytes::new(), false, 65536, None)
                .await
                .unwrap(),
            super::super::message_action::Reply::Action(_)
        ));
        assert!(
            callback
                .decide(method, Bytes::new(), true, 65536, None)
                .await
                .is_err()
        );
        assert!(!callback.interested(true, "response.event"));
        manager.close().await.unwrap();
        registration.revoke();
        listener.shutdown().await.unwrap();
    }

    #[test]
    fn deltas_bypass_notifier_regardless_of_response_or_action_grants() {
        for responses in [false, true] {
            for actions in [false, true] {
                assert!(!response_kind_reaches_plugin(
                    true,
                    responses,
                    actions,
                    "response.event"
                ));
                assert!(response_kind_reaches_plugin(
                    true,
                    responses,
                    actions,
                    "response.completed"
                ));
                assert!(response_kind_reaches_plugin(
                    true,
                    responses,
                    actions,
                    "response.done"
                ));
                assert!(response_kind_reaches_plugin(
                    false,
                    responses,
                    actions,
                    "response.create"
                ));
            }
        }
        assert!(response_kind_reaches_plugin(
            true,
            false,
            true,
            "response.created"
        ));
        assert!(!response_kind_reaches_plugin(
            true,
            false,
            true,
            "response.binary"
        ));
    }

    #[test]
    fn callback_method_rejects_ambiguous_components_and_honors_native_boundary() {
        for invalid in [
            "",
            "response/create",
            "response\0create",
            "Response",
            "réponse",
        ] {
            assert!(callback_method("codec.v1", invalid, 1, 2).is_err());
            assert!(callback_method(invalid, "response.create", 1, 2).is_err());
        }
        let codec = "c".repeat(128);
        let kind = "k".repeat(93);
        let method = callback_method(&codec, &kind, u64::MAX, 0)
            .unwrap()
            .to_string();
        assert_eq!(method.len(), 256);
        assert_eq!(
            method.split('/').collect::<Vec<_>>(),
            vec![
                codec.as_str(),
                kind.as_str(),
                "ffffffffffffffff",
                "0000000000000000"
            ]
        );
        assert!(callback_method(&codec, &"k".repeat(94), 1, 2).is_err());
        assert!(callback_method("c", &"k".repeat(129), 1, 2).is_err());
        assert!(callback_method(&"c".repeat(129), "k", 1, 2).is_err());
    }

    #[tokio::test]
    async fn http_json_gate_emits_original_wire_only_at_eof() {
        let mut decoder =
            proxy_protocols::json_message::JsonObjectDecoder::new(MESSAGE_LIMIT, 4096, "request")
                .unwrap();
        let wire = Bytes::from_static(b" {\"model\":\"test\",\"messages\":[]} ");
        let mut sequence = 0;
        let mut body = Some(wire.slice(..5));
        filter(
            &[],
            &mut decoder,
            "json.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(Bytes::new()));
        assert_eq!(sequence, 0);
        body = Some(wire.slice(5..));
        filter(
            &[],
            &mut decoder,
            "json.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(Bytes::new()));
        body = None;
        filter(
            &[],
            &mut decoder,
            "json.v1",
            &mut sequence,
            1,
            &mut body,
            true,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(wire));
        assert_eq!(sequence, 1);
    }

    struct CustomCodec;
    impl CodecPlugin for CustomCodec {
        fn id(&self) -> &'static str {
            "custom.v1"
        }
        fn upgrade(&self) -> &'static str {
            "custom"
        }
        fn validate_response(&self, _: &ResponseHeader) -> Result<()> {
            Ok(())
        }
        fn create(&self) -> Result<Box<dyn Decoder>> {
            WebSocketCodec.create()
        }
    }

    #[test]
    fn registry_resolves_custom_plugins_and_rejects_unknown_and_duplicate_ids() {
        let custom: Arc<dyn CodecPlugin> = Arc::new(CustomCodec);
        let codecs = vec![
            Arc::new(WebSocketCodec) as Arc<dyn CodecPlugin>,
            Arc::clone(&custom),
        ];
        assert_eq!(
            resolve(&codecs, Some("custom.v1")).unwrap().unwrap().id(),
            "custom.v1"
        );
        assert!(resolve(&codecs, Some("unknown.v1")).is_err());
        assert!(resolve(&[Arc::clone(&custom), custom], None).is_err());
        assert!(resolve(&codecs, None).unwrap().is_none());
    }

    #[test]
    fn codec_validation_only_applies_to_selected_upgrade() {
        let mut response = ResponseHeader::build(101, None).unwrap();
        response.insert_header("Upgrade", "websocket").unwrap();
        response
            .insert_header("Sec-WebSocket-Extensions", "permessage-deflate")
            .unwrap();
        assert!(validate_selection(Some(&WebSocketCodec), &response).is_err());
        assert!(!validate_selection(Some(&CustomCodec), &response).unwrap());
        assert!(!validate_selection(None, &response).unwrap());
    }

    #[tokio::test]
    async fn buffering_preserves_some_empty_and_forwards_original_masked_wire() {
        let mut decoder = WebSocketCodec.create().unwrap();
        let mut sequence = 0;
        let wire = Bytes::from_static(&[0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2]);
        let mut body = Some(wire.slice(..3));
        filter(
            &[],
            decoder.as_mut(),
            "websocket.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(Bytes::new()));
        body = Some(wire.slice(3..));
        filter(
            &[],
            decoder.as_mut(),
            "websocket.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(wire));
        assert_eq!(sequence, 1);
    }

    #[tokio::test]
    async fn complete_frames_in_one_chunk_keep_the_original_allocation() {
        let mut decoder = WebSocketCodec.create().unwrap();
        let mut sequence = 0;
        let wire = Bytes::from_static(&[
            0x81,
            0x81,
            1,
            2,
            3,
            4,
            b'h' ^ 1,
            0x82,
            0x81,
            4,
            3,
            2,
            1,
            9 ^ 4,
        ]);
        let pointer = wire.as_ptr();
        let mut body = Some(wire.clone());
        filter(
            &[],
            decoder.as_mut(),
            "websocket.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body.as_ref().unwrap().as_ptr(), pointer);
        assert_eq!(body, Some(wire));
        assert_eq!(sequence, 2);
    }

    #[tokio::test]
    async fn completed_carry_and_next_message_share_a_bounded_output_batch() {
        fn frame(opcode: u8, len: usize) -> Bytes {
            let mut bytes = vec![opcode, 0xff];
            bytes.extend_from_slice(&(len as u64).to_be_bytes());
            bytes.extend_from_slice(&[0; 4]);
            bytes.resize(bytes.len() + len, 0);
            Bytes::from(bytes)
        }
        let mut decoder = WebSocketCodec.create().unwrap();
        let mut sequence = 0;
        let first = frame(2, MESSAGE_LIMIT - 10);
        let first_len = first.len();
        let mut body = Some(first);
        filter(
            &[],
            decoder.as_mut(),
            "websocket.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body, Some(Bytes::new()));
        let next = frame(0x82, 100_000);
        let mut last = vec![0x80, 0x80, 0, 0, 0, 0];
        last.extend_from_slice(&next);
        let last_len = last.len();
        body = Some(Bytes::from(last));
        filter(
            &[],
            decoder.as_mut(),
            "websocket.v1",
            &mut sequence,
            1,
            &mut body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(body.unwrap().len(), first_len + last_len);
        assert_eq!(sequence, 2);
    }

    #[tokio::test]
    async fn incomplete_eof_fails_closed() {
        let mut decoder = WebSocketCodec.create().unwrap();
        let mut sequence = 0;
        let mut body = Some(Bytes::from_static(&[0x81]));
        assert!(
            filter(
                &[],
                decoder.as_mut(),
                "websocket.v1",
                &mut sequence,
                1,
                &mut body,
                true
            )
            .await
            .is_err()
        );
        assert!(body.is_none());
    }
}
