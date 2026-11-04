//! One full HTTP request, one fresh WS upstream, native HTTP response delivery.
use super::*;
use futures_util::{Stream, stream};
use http_bridge::response::{Frame, Limits};
use pingora_core::protocols::http::client::HttpSession;
use proxy_pingora::connect_upstream::bridge;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

fn request_mode(payload: &[u8]) -> Result<bool> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| refusal(400, "invalid Responses JSON"))?;
    let object = value
        .as_object()
        .ok_or_else(|| refusal(400, "Responses request must be an object"))?;
    match object.get("generate") {
        None | Some(serde_json::Value::Bool(true)) => {}
        Some(serde_json::Value::Bool(false)) => {
            return Err(refusal(400, "HTTP bridge requires a full request"));
        }
        _ => return Err(refusal(400, "generate must be boolean")),
    }
    if object
        .get("previous_response_id")
        .is_some_and(|v| !v.is_null() && v.as_str() != Some(""))
    {
        return Err(refusal(400, "HTTP bridge requires a full request"));
    }
    match object.get("stream") {
        None | Some(serde_json::Value::Bool(false)) => Ok(false),
        Some(serde_json::Value::Bool(true)) => Ok(true),
        _ => Err(refusal(400, "stream must be boolean")),
    }
}
fn terminal(kind: &str) -> bool {
    matches!(
        kind,
        "response.completed"
            | "response.failed"
            | "response.incomplete"
            | "response.error"
            | "response.cancelled"
            | "response.canceled"
            | "error"
    )
}
fn final_json(payload: &[u8]) -> Result<Vec<u8>> {
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        response: Option<&'a serde_json::value::RawValue>,
    }
    let envelope: Envelope<'_> =
        serde_json::from_slice(payload).map_err(|_| refusal(502, "invalid terminal JSON"))?;
    if let Some(value) = envelope.response {
        Ok(value.get().as_bytes().to_vec())
    } else {
        Ok(payload.to_vec())
    }
}
fn explicit_status(value: &serde_json::Value) -> Option<u16> {
    [
        value.get("status"),
        value.pointer("/error/status"),
        value.pointer("/response/error/status"),
    ]
    .into_iter()
    .flatten()
    .filter_map(serde_json::Value::as_u64)
    .find(|status| (400..=599).contains(status))
    .map(|status| status as u16)
}
fn policy_status(payload: &[u8], fallback: u16) -> u16 {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| explicit_status(&value))
        .unwrap_or(fallback)
}
fn preheader_status(payload: &[u8]) -> Result<u16> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| refusal(502, "invalid approved response"))?;
    if let Some(status) = explicit_status(&value) {
        return Ok(status);
    }
    let state = value
        .pointer("/response/status")
        .and_then(serde_json::Value::as_str);
    if matches!(state, Some("cancelled" | "canceled")) {
        return Ok(499);
    }
    if matches!(state, Some("failed" | "incomplete" | "error")) {
        return Ok(502);
    }
    Ok(
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("response.cancelled" | "response.canceled") => 499,
            Some("error" | "response.error" | "response.failed" | "response.incomplete") => 502,
            _ => 200,
        },
    )
}
fn reply_payload(
    registration: &message_codec::CodecRegistration,
    wire: Bytes,
    max: usize,
) -> Result<Bytes> {
    let mut decoder = response_decoder(registration.codec.as_ref(), max)?;
    let events = decoder
        .push(wire)
        .map_err(|_| refusal(502, "invalid policy error"))?;
    events
        .into_iter()
        .find_map(|event| event.message.map(|message| message.payload))
        .ok_or_else(|| refusal(502, "missing policy error"))
}
// The HTTP session can observe disconnect while an independent operation waits.
async fn while_connected<T>(
    session: &mut Session,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        result=operation=>result,
        _=session.downstream_session.read_body_or_idle(true)=>Err(refusal(499,"HTTP client disconnected")),
    }
}
struct Output {
    payload: Bytes,
    terminal: bool,
}
struct Source<'a> {
    owner: &'a NativeProxy,
    registration: &'a message_codec::CodecRegistration,
    ctx: &'a mut ProxyContext,
    exchange: &'a mut CarrierExchange<ProxyContract>,
    upstream: HttpSession,
    decoder: Box<dyn Decoder>,
    pending: VecDeque<proxy_protocols::message_codec::Event>,
    output: VecDeque<Output>,
    held: Vec<(Output, HeldMessage)>,
    held_bytes: Arc<ResourceBudget>,
    held_count: Arc<ResourceBudget>,
    reservation: Arc<Reservation>,
    assembly_deadline: Option<tokio::time::Instant>,
    response_hold: ResponseHold,
}
impl Source<'_> {
    async fn next(&mut self) -> Result<Option<Output>> {
        loop {
            if let Some(output) = self.output.pop_front() {
                return Ok(Some(output));
            }
            let event = if let Some(event) = self.pending.pop_front() {
                event
            } else {
                let deadline = self
                    .assembly_deadline
                    .unwrap_or_else(|| tokio::time::Instant::now() + self.owner.config.io_timeout);
                let chunk = tokio::time::timeout_at(deadline, self.upstream.read_response_body())
                    .await
                    .map_err(|_| refusal(504, "WS response deadline exceeded"))??;
                let Some(chunk) = chunk else {
                    return Err(refusal(502, "WS ended before terminal"));
                };
                let events = self
                    .decoder
                    .push(chunk)
                    .map_err(|_| refusal(502, "invalid upstream WS message"))?;
                self.assembly_deadline = if self.decoder.has_pending() {
                    Some(
                        self.assembly_deadline
                            .unwrap_or_else(|| self.owner.inspection.deadline()),
                    )
                } else {
                    None
                };
                self.pending.extend(events);
                continue;
            };
            let Some(semantic) = event.message else {
                let length = event.wire.iter().map(Bytes::len).sum::<usize>();
                if !(2..=127).contains(&length) {
                    return Err(refusal(502, "invalid upstream WS control frame"));
                }
                let wire: Vec<u8> = event
                    .wire
                    .iter()
                    .flat_map(|bytes| bytes.iter().copied())
                    .collect();
                match wire[0] & 15 {
                    8 => return Err(refusal(502, "WS closed before terminal")),
                    9 => {
                        let pong = websocket::encode_client(10, &wire[2..])
                            .map_err(|_| refusal(502, "invalid upstream ping"))?;
                        self.upstream.write_request_body(pong, false).await?;
                    }
                    _ => {}
                }
                continue;
            };
            if semantic.kind == "response.done" {
                continue;
            }
            let was_terminal = terminal(semantic.kind);
            let message = Message {
                side: Side::Upstream(1),
                message: Some(semantic),
                wire: event.wire,
            };
            let plan = within_deadline(
                self.owner.inspection.deadline(),
                evaluate(
                    self.registration,
                    &message,
                    &mut self.ctx.response_sequence,
                    self.ctx.request_id,
                    self.owner.inspection.limits.max_message_bytes,
                    Some(self.reservation.clone()),
                ),
                "HTTP response policy deadline exceeded",
            )
            .await?;
            self.response_hold.apply(&plan)?;
            if plan.hold == "discard" {
                self.held.clear();
            }
            let selected = plan.replacement.as_ref().unwrap_or(&message);
            validate_terminal_hold(selected, &plan, !self.held.is_empty())?;
            if plan.hold == "buffer" || self.response_hold.buffers(selected, &plan) {
                let semantic = selected
                    .message
                    .as_ref()
                    .ok_or_else(|| refusal(502, "missing held semantic message"))?;
                self.held.push((
                    Output {
                        payload: semantic.payload.clone(),
                        terminal: terminal(semantic.kind),
                    },
                    retain_message(
                        selected,
                        &self.held_bytes,
                        &self.held_count,
                        self.reservation.clone(),
                    )?,
                ));
                continue;
            }
            if plan.hold == "release" {
                self.output.extend(
                    release_hold(&mut self.held)
                        .into_iter()
                        .map(|(output, _)| output),
                );
            }
            match plan.decision {
                Decision::Forward => {
                    let semantic = selected
                        .message
                        .as_ref()
                        .ok_or_else(|| refusal(502, "missing approved semantic message"))?;
                    self.output.push_back(Output {
                        payload: semantic.payload.clone(),
                        terminal: terminal(semantic.kind),
                    });
                }
                Decision::Reply => {
                    let mut decoder = response_decoder(
                        self.registration.codec.as_ref(),
                        self.owner.inspection.limits.max_message_bytes,
                    )?;
                    let events = decoder
                        .push(plan.reply.expect("validated reply"))
                        .map_err(|_| refusal(502, "invalid policy reply"))?;
                    for event in events {
                        if let Some(message) = event.message {
                            self.output.push_back(Output {
                                payload: message.payload,
                                terminal: terminal(message.kind),
                            });
                        }
                    }
                }
                Decision::Drop => {
                    if was_terminal {
                        return Err(refusal(502, "policy declined terminal delivery"));
                    }
                }
            }
            if plan.upstream != Upstream::Keep || plan.close_downstream {
                if !self.output.iter().any(|output| output.terminal) {
                    return Err(refusal(502, "policy closed before terminal"));
                }
            }
        }
    }
}
struct OwnedStream<'a>(Option<Pin<Box<dyn Stream<Item = Result<Output>> + Send + 'a>>>);
impl Stream for OwnedStream<'_> {
    type Item = Result<Output>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.0 {
            Some(stream) => stream.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}
fn invalidate(stream: &mut OwnedStream<'_>) {
    stream.0 = None;
}

impl NativeProxy {
    pub(in crate::proxy) async fn serve_http_bridge(
        &self,
        session: &mut Session,
        ctx: &mut ProxyContext,
        index: usize,
    ) -> Result<()> {
        let registration = &self.message_codecs[index];
        let mut exchange = ctx
            .exchange
            .take()
            .ok_or_else(|| refusal(500, "missing admission"))?;
        let result = async {
            if session.req_header().method != http::Method::POST {
                return Err(refusal(405, "HTTP bridge requires POST"));
            }
            if !ctx.request_body.is_empty() {
                return Err(refusal(
                    400,
                    "HTTP body transforms cannot process a WS bridge",
                ));
            }
            let max = self.inspection.limits.max_message_bytes;
            let reservation = self.inspection.buffers()?;
            ctx.inspection_permit = Some(reservation.clone());
            let mut request = ctx
                .upstream_request
                .take()
                .ok_or_else(|| refusal(500, "missing request"))?;
            let mut decoder = registration.codec.create_http_with_limits(&request, max)?;
            let mut message = None;
            let deadline = self.inspection.deadline();
            loop {
                let chunk = tokio::time::timeout_at(deadline, session.read_request_body())
                    .await
                    .map_err(|_| refusal(408, "HTTP request assembly timeout"))??;
                let eof = chunk.is_none();
                let events = match chunk {
                    Some(bytes) => decoder.push(bytes),
                    None => decoder.finish_events(),
                }
                .map_err(|_| refusal(400, "invalid HTTP Responses body"))?;
                for event in events {
                    if let Some(semantic) = event.message {
                        if message.replace(semantic).is_some() {
                            return Err(refusal(400, "HTTP bridge accepts one full request"));
                        }
                    }
                }
                if eof {
                    break;
                }
            }
            let message = message.ok_or_else(|| refusal(400, "missing HTTP Responses request"))?;
            let streaming = request_mode(&message.payload)?;
            if message.kind != "response" {
                return Err(refusal(400, "unsupported HTTP bridge request"));
            }
            ctx.message_decoder = None;
            ctx.message_deadline = None;
            ctx.message_http_complete = true;
            if !session.is_http2() {
                session.set_keepalive(None);
            }
            self.run_http_bridge(
                session,
                ctx,
                registration,
                &mut exchange,
                &mut request,
                message,
                streaming,
                reservation,
            )
            .await
        }
        .await;
        let method = format!(
            "{}/transport.closed/{:016x}/{:016x}",
            registration.codec.id(),
            ctx.request_id,
            ctx.response_sequence
        );
        let closed = Bytes::from_static(if result.is_ok() {
            br#"{"side":"downstream","reason":"close"}"#
        } else {
            br#"{"side":"downstream","reason":"error"}"#
        });
        // Match WS teardown: poll every owner before waiting for one slow
        // callback. All cleanup and completion share one absolute deadline.
        let cleanup_deadline = tokio::time::Instant::now() + self.config.io_timeout;
        let cleanup_result = notify_session_closed(
            registration,
            &method,
            closed,
            ctx.inspection_permit.clone(),
            ctx.request_id,
            cleanup_deadline,
        )
        .await;
        match result.and(cleanup_result) {
            Ok(()) => {
                exchange.facts_mut().completed = true;
                let completion = within_deadline(
                    cleanup_deadline,
                    async { exchange.finish().await.map_err(lifecycle_error) },
                    "HTTP completion callback deadline exceeded",
                )
                .await;
                match completion {
                    Ok(_) => {
                        ctx.session_completed = true;
                        Ok(())
                    }
                    Err(error) => {
                        ctx.message_failed = true;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                exchange.fail();
                ctx.message_failed = true;
                Err(error)
            }
        }
    }
    async fn run_http_bridge(
        &self,
        session: &mut Session,
        ctx: &mut ProxyContext,
        registration: &message_codec::CodecRegistration,
        exchange: &mut CarrierExchange<ProxyContract>,
        request: &mut RequestHeader,
        message: proxy_protocols::message_codec::Message,
        streaming: bool,
        reservation: Arc<Reservation>,
    ) -> Result<()> {
        let original = Message {
            side: Side::Downstream,
            message: Some(message),
            wire: Vec::new(),
        };
        let plan = while_connected(
            session,
            within_deadline(
                self.inspection.deadline(),
                evaluate(
                    registration,
                    &original,
                    &mut ctx.message_sequence,
                    ctx.request_id,
                    self.inspection.limits.max_message_bytes,
                    Some(reservation.clone()),
                ),
                "HTTP request policy timeout",
            ),
        )
        .await?;
        if plan.socket_lifetime.is_some() || plan.retain_upstream_on_downstream_close.is_some() {
            return Err(refusal(
                502,
                "HTTP request cannot retain a physical upstream",
            ));
        }
        if plan.decision == Decision::Reply {
            let payload = reply_payload(
                registration,
                plan.reply
                    .ok_or_else(|| refusal(502, "missing policy reply"))?,
                self.inspection.limits.max_message_bytes,
            )?;
            let status = policy_status(&payload, 400);
            session.respond_error_with_body(status, payload).await?;
            return Err(refusal(status, "HTTP request declined"));
        }
        if plan.decision != Decision::Forward
            || !matches!(plan.upstream, Upstream::Connect | Upstream::Replace)
            || !plan.hold.is_empty()
            || plan.close_downstream
        {
            return Err(refusal(403, "HTTP full request was not admitted"));
        }
        let mut response_hold = ResponseHold::default();
        response_hold.apply(&plan)?;
        let approved = plan
            .replacement
            .ok_or_else(|| refusal(502, "HTTP bridge requires approved full create"))?;
        let payload = &approved
            .message
            .as_ref()
            .ok_or_else(|| refusal(502, "missing full create"))?
            .payload;
        let body: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|_| refusal(502, "invalid approved full create"))?;
        if body.get("type").and_then(|v| v.as_str()) != Some("response.create")
            || body.get("stream").is_some()
        {
            return Err(refusal(502, "invalid approved full create"));
        }
        request_mode(payload)?;
        let mut proxy = self.config.proxy.clone();
        for patch in plan.patches {
            patch.apply(request, &mut proxy)?;
        }
        request.method = http::Method::GET;
        request.version = http::Version::HTTP_11;
        carrier::handshake::prepare_request(request)?;
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        while_connected(
            session,
            within_deadline(
                deadline,
                async {
                    exchange
                        .before_upstream_send(request)
                        .await
                        .map_err(lifecycle_error)?;
                    snapshot_request(request, exchange.facts_mut());
                    let target = proxy.as_ref().unwrap_or(&self.config.upstream);
                    let dial = DialRequest {
                        target: DialTarget::Host {
                            name: target.host_str().unwrap_or_default().into(),
                            port: target.port_or_known_default().unwrap_or(443),
                        },
                        timeout: self.config.connect_timeout,
                        tcp_nodelay: true,
                    };
                    exchange.acquire(&dial).await.map_err(control_error)?;
                    Ok(())
                },
                "HTTP bridge admission deadline exceeded",
            ),
        )
        .await?;
        if request.method != http::Method::GET || request.version != http::Version::HTTP_11 {
            return Err(refusal(502, "callbacks changed WS carrier method"));
        }
        for name in ["content-type", "content-encoding"] {
            request.remove_header(name);
        }
        registration.codec.prepare_request(request)?;
        message_codec::validate_responses_upstream(registration.codec.as_ref(), request)?;
        let expected = carrier::handshake::prepare_request(request)?;
        http_bridge::websocket::validate_request::<()>(
            request.version,
            &request.method,
            &request.headers,
        )
        .map_err(|_| refusal(502, "invalid WS bridge request"))?;
        snapshot_request(request, exchange.facts_mut());
        let mut peer = while_connected(
            session,
            within_deadline(
                deadline,
                self.egress.peer(
                    &self.config.upstream,
                    proxy.as_ref(),
                    ctx.request_id,
                    self.config.connect_timeout,
                ),
                "HTTP bridge resolution timeout",
            ),
        )
        .await?;
        peer.options.set_http_version(1, 1);
        peer.options.idle_timeout = Some(Duration::ZERO);
        let upstream = while_connected(
            session,
            within_deadline(
                deadline,
                async {
                    let (mut upstream, reused) = self
                        .session_connector
                        .get_http_session(peer.as_ref())
                        .await?;
                    if !matches!(upstream, HttpSession::H1(_)) {
                        return Err(refusal(502, "WS bridge requires HTTP1 egress"));
                    }
                    upstream.set_read_timeout(Some(self.config.io_timeout));
                    upstream.set_write_timeout(Some(self.config.io_timeout));
                    exchange.facts_mut().upstream_reused = reused;
                    exchange.connected().await.map_err(control_error)?;
                    upstream
                        .write_request_header(Box::new(request.clone()))
                        .await?;
                    upstream.read_response_header().await?;
                    let mut response = upstream
                        .response_header()
                        .ok_or_else(|| refusal(502, "missing WS handshake"))?
                        .clone();
                    http_bridge::websocket::validate_response::<()>(
                        false,
                        response.status,
                        &response.headers,
                    )
                    .map_err(|_| refusal(502, "invalid WS bridge upgrade"))?;
                    registration.codec.validate_response(&response)?;
                    if response.status.as_u16() != 101
                        || response
                            .headers
                            .get("sec-websocket-accept")
                            .and_then(|v| v.to_str().ok())
                            != Some(expected.as_str())
                        || response.headers.contains_key("sec-websocket-extensions")
                        || response.headers.contains_key("sec-websocket-protocol")
                    {
                        return Err(refusal(502, "invalid WS bridge handshake"));
                    }
                    exchange
                        .upstream_response(&mut response)
                        .await
                        .map_err(control_error)?;
                    if let HttpSession::H1(session) = &mut upstream {
                        session.maybe_upgrade_body_writer();
                    }
                    for wire in approved.wire {
                        upstream.write_request_body(wire, false).await?;
                    }
                    Ok(upstream)
                },
                "HTTP bridge handshake timeout",
            ),
        )
        .await?;
        let (response_body, _) = body::take_plan(&mut exchange.facts_mut().response_body)?;
        if !response_body.is_empty() {
            return Err(refusal(
                400,
                "HTTP body transforms cannot process a WS bridge",
            ));
        }
        let max = self.inspection.limits.max_message_bytes;
        let mut source = Source {
            owner: self,
            registration,
            ctx,
            exchange,
            upstream,
            decoder: response_decoder(registration.codec.as_ref(), max)?,
            pending: VecDeque::new(),
            output: VecDeque::new(),
            held: Vec::new(),
            held_bytes: ResourceBudget::new(max.saturating_mul(2)),
            held_count: ResourceBudget::new(websocket::Limits::default().max_frames),
            reservation,
            assembly_deadline: None,
            response_hold,
        };
        let first = while_connected(session, async {
            loop {
                let output = source
                    .next()
                    .await?
                    .ok_or_else(|| refusal(502, "WS ended before terminal"))?;
                if streaming || output.terminal {
                    return Ok(output);
                }
            }
        })
        .await?;
        let status = preheader_status(&first.payload)?;
        source.output.push_front(first);
        let mut response = ResponseHeader::build(status, Some(3))?;
        response.insert_header(
            "content-type",
            if streaming {
                "text/event-stream"
            } else {
                "application/json"
            },
        )?;
        response.insert_header("cache-control", "no-cache")?;
        if !session.is_http2() {
            response.insert_header("connection", "close")?;
        }
        source.exchange.facts_mut().status = Some(status);
        while_connected(
            session,
            within_deadline(
                self.inspection.deadline(),
                async {
                    source
                        .exchange
                        .before_downstream_write(&mut response)
                        .await
                        .map_err(lifecycle_error)
                },
                "HTTP response callback timeout",
            ),
        )
        .await?;
        let stream = stream::try_unfold(source, |mut source| async move {
            source
                .next()
                .await
                .map(|output| output.map(|output| (output, source)))
        });
        let source = OwnedStream(Some(Box::pin(stream)));
        let guard = stream_transfer::DeliveryGuard::new(source, None, invalidate);
        let frame_limit = max.saturating_mul(8).saturating_add(64);
        let limits = Limits {
            read_timeout: self.config.io_timeout,
            write_timeout: self.config.io_timeout,
            max_frame_bytes: frame_limit,
        };
        let translate = move |output: Output| -> Result<Frame> {
            if streaming {
                let text = std::str::from_utf8(&output.payload)
                    .map_err(|_| refusal(502, "invalid WS JSON UTF8"))?;
                let bytes = http_bridge::event_stream::encode_sse_event(None, text, frame_limit)
                    .map_err(|_| refusal(502, "SSE event exceeds limit"))?;
                Ok(if output.terminal {
                    Frame::Finish(bytes)
                } else {
                    Frame::Data(bytes)
                })
            } else if output.terminal {
                Ok(Frame::Finish(final_json(&output.payload)?))
            } else {
                Ok(Frame::Skip)
            }
        };
        let delivered = if session.is_http2() {
            bridge::http2::transfer(
                guard,
                &mut session.downstream_session,
                response,
                limits,
                std::future::pending(),
                translate,
            )
            .await
        } else {
            bridge::http1::transfer(
                guard,
                &mut session.downstream_session,
                response,
                limits,
                std::future::pending(),
                translate,
            )
            .await
        };
        match delivered {
            Ok(guard) => {
                guard.release(|_| {});
                Ok(())
            }
            Err(_) => Err(refusal(502, "HTTP bridge delivery failed")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn http_bridge_full_request_modes_and_raw_terminal_bytes() {
        assert!(!request_mode(br#"{"input":[]}"#).unwrap());
        assert!(request_mode(br#"{"input":[],"stream":true}"#).unwrap());
        assert!(!request_mode(br#"{"input":[],"generate":true}"#).unwrap());
        for body in [
            br#"{"stream":null}"#.as_slice(),
            br#"{"stream":1}"#,
            br#"{"previous_response_id":12}"#,
            br#"{"generate":false}"#,
            br#"{"generate":null}"#,
            br#"{"generate":0}"#,
            br#"{"generate":1}"#,
            br#"{"generate":[]}"#,
            br#"{"generate":{}}"#,
            br#"{"generate":"true"}"#,
            br#"{"generate":"false"}"#,
        ] {
            assert!(request_mode(body).is_err());
        }
        assert_eq!(final_json(br#"{"type":"response.completed","response": {"number":12345678901234567890, "text":"\u00e9"}}"#).unwrap(),br#"{"number":12345678901234567890, "text":"\u00e9"}"#);
        assert_eq!(
            preheader_status(br#"{"type":"response.completed","response":{"status":"failed"}}"#)
                .unwrap(),
            502
        );
        assert_eq!(
            preheader_status(
                br#"{"type":"response.incomplete","response":{"error":{"status":429}}}"#
            )
            .unwrap(),
            429
        );
        assert_eq!(
            preheader_status(br#"{"type":"response.cancelled"}"#).unwrap(),
            499
        );
        assert!(!terminal("response.done"));
        assert_eq!(policy_status(br#"{"type":"error","status":429}"#, 400), 429);
        assert_eq!(policy_status(br#"{"status":200}"#, 502), 502);
    }
}
