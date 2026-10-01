//! Registered host protocol codecs and bounded native message callbacks.
use super::*;
use bytes::{Bytes, BytesMut};
use plugin_loading::Callback;
use proxy_protocols::message_codec::Decoder;
use proxy_protocols::websocket::Limits;

#[cfg(test)]
pub(crate) const MESSAGE_LIMIT: usize = 1024 * 1024;

/// Shares the metadata callback's exact native instance and admission owner.
#[derive(Clone)]
pub(crate) struct MessageCallback {
    instance: Arc<Callback>,
    timeout: Duration,
    response_enabled: bool,
}
impl MessageCallback {
    pub(crate) fn new(instance: Arc<Callback>, timeout: Duration, response_enabled: bool) -> Self {
        Self {
            instance,
            timeout,
            response_enabled,
        }
    }
}

pub(crate) use proxy_pingora::http_ingress::message_codec::{
    CodecPlugin, Transport, resolve_all, valid_id, validate_selection,
};

#[derive(Clone)]
pub(crate) struct CodecRegistration {
    pub(crate) codec: Arc<dyn CodecPlugin>,
    pub(crate) callbacks: Vec<MessageCallback>,
}

impl CodecRegistration {
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
        codec_id,
        sequence,
        request_id,
        body,
        end,
        None,
        false,
        1024 * 1024,
        None,
    )
    .await
    .map(|_| ())
}

pub(super) async fn filter_owned(
    callbacks: &[MessageCallback],
    decoder: &mut dyn Decoder,
    codec_id: &str,
    sequence: &mut u64,
    request_id: u64,
    body: &mut Option<Bytes>,
    end: bool,
    reservation: Option<Arc<admission_gate::budget::Reservation>>,
    response: bool,
    max_bytes: usize,
    reply_codec: Option<&dyn CodecPlugin>,
) -> Result<Option<Bytes>> {
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
            let method = format!(
                "{codec_id}/{}/{request_id:016x}/{sequence:016x}",
                message.kind
            );
            let message_sequence = *sequence;
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| refusal(400, "message sequence exhausted"))?;
            // Completion frames are the cache decision point. Deltas stay on the
            // message_responses grant so a terminal reply does not inspect them.
            let terminal =
                response && matches!(message.kind, "response.completed" | "response.done");
            for (callback_index, callback) in callbacks
                .iter()
                .enumerate()
                .filter(|(_, callback)| !response || callback.response_enabled || terminal)
            {
                let started = Instant::now();
                let decision = callback
                    .instance
                    .call_with_reply(
                        &method,
                        message.payload.clone(),
                        callback.timeout,
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
