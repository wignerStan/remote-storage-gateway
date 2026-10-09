//! Execute granted message decisions through separately owned native sessions.
//! Route, account, token and replay policy remain in the selected callbacks.
use super::message_action::{Decision, Metadata, Reply, Upstream};
use super::*;
use admission_gate::budget::{Reservation, ResourceBudget};
use bytes::Bytes;
use proxy_pingora::{
    connect_upstream::websocket_session::{self as carrier, Event, Message, Side},
    worker::SessionExchange,
};
use proxy_protocols::{message_codec::Decoder, websocket};

const REPLAY_REQUIRED: &[u8] = br#"{"type":"error","status":409,"error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"Previous response was not found. Retrying the full request.","param":"previous_response_id"}}"#;

/// Mutable route draft retained across upstream generations of one session.
struct SessionRoute<'a> {
    request: &'a mut RequestHeader,
    proxy: &'a mut Option<Url>,
}

struct Plan {
    decision: Decision,
    upstream: Upstream,
    hold: String,
    patches: Vec<Metadata>,
    replacement: Option<Message>,
    reply: Option<Bytes>,
}

/// Application-held wire is outside Pingora's decoder and pending-event queues.
/// Keep each reservation with its bytes, including while a release is writing.
struct HeldMessage {
    message: Message,
    _bytes: Reservation,
    _count: Reservation,
    _inspection: Arc<Reservation>,
}

fn retain_message(
    message: &Message,
    bytes: &Arc<ResourceBudget>,
    count: &Arc<ResourceBudget>,
    inspection: Arc<Reservation>,
) -> Result<HeldMessage> {
    let units = message
        .wire
        .iter()
        .try_fold(std::mem::size_of::<HeldMessage>(), |total, wire| {
            total
                .checked_add(wire.len())?
                .checked_add(std::mem::size_of::<Bytes>())
        })
        .ok_or_else(|| refusal(503, "held WebSocket wire budget overflow"))?;
    let bytes = bytes
        .try_reserve(units)
        .ok_or_else(|| refusal(503, "held WebSocket wire budget exhausted"))?;
    let count = count
        .try_reserve(1)
        .ok_or_else(|| refusal(503, "held WebSocket message budget exhausted"))?;
    Ok(HeldMessage {
        message: Message {
            side: message.side,
            message: None,
            wire: message.wire.clone(),
        },
        _bytes: bytes,
        _count: count,
        _inspection: inspection,
    })
}

/// Bound host waiting without claiming to interrupt an executing native callback.
/// The callback owner retains its input and inspection reservation until return.
async fn within_deadline<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = Result<T>>,
    reason: &'static str,
) -> Result<T> {
    stream_transfer::bounded_io(
        Some(deadline.into_std()),
        deadline.saturating_duration_since(tokio::time::Instant::now()),
        std::future::pending(),
        operation,
    )
    .await
    .map_err(|failure| match failure {
        stream_transfer::IoFailure::Transport(error) => error,
        _ => refusal(504, reason),
    })
}

type CleanupFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

/// Poll all cleanup owners before waiting for any one owner to finish.
async fn complete_all(mut operations: Vec<Option<CleanupFuture<'_>>>) {
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for operation in &mut operations {
            if let Some(future) = operation {
                if future.as_mut().poll(cx).is_ready() {
                    *operation = None;
                } else {
                    pending = true;
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
}

impl NativeProxy {
    pub(super) async fn serve_websocket_session(
        &self,
        session: &mut Session,
        ctx: &mut ProxyContext,
        index: usize,
    ) -> Result<()> {
        let registration = &self.message_codecs[index];
        let codec = registration.codec.as_ref();
        if codec.upgrade() != "websocket" {
            return Err(refusal(400, "message session control requires WebSocket"));
        }
        let mut exchange = ctx
            .exchange
            .take()
            .ok_or_else(|| refusal(500, "missing admission"))?
            .into_session()
            .map_err(lifecycle_error)?;
        let mut request = ctx
            .upstream_request
            .take()
            .ok_or_else(|| refusal(500, "missing upstream request"))?;
        let mut proxy = self.config.proxy.clone();
        let mut response = carrier::handshake::response(session.req_header())?;
        let framing = headers::ResponseFraming::capture(&response);
        let (response_body, _) = body::take_plan(&mut exchange.facts_mut().response_body)?;
        if !response_body.is_empty() {
            return Err(refusal(
                400,
                "HTTP body transforms cannot process a session",
            ));
        }
        exchange.facts_mut().status = Some(101);
        tokio::time::timeout(
            self.config.io_timeout,
            exchange.before_downstream_write(&mut response),
        )
        .await
        .map_err(|_| refusal(504, "downstream callback deadline exceeded"))?
        .map_err(lifecycle_error)?;
        framing.validate(&response, &request.method)?;
        body::ensure_closed(&exchange.facts().response_body)?;
        let max_bytes = self.inspection.limits.max_message_bytes;
        let limits = carrier::Limits {
            codec: websocket::Limits {
                max_message_bytes: max_bytes,
                max_wire_bytes: max_bytes + 64 * 1024,
                ..Default::default()
            },
            handshake_timeout: self.config.connect_timeout,
            write_timeout: self.config.io_timeout,
            read_idle_timeout: self.config.io_timeout,
            assembly_timeout: Duration::from_millis(self.inspection.limits.message_timeout_ms),
            ping_interval: Duration::from_millis(self.config.websocket_keepalive.interval_ms),
            pong_timeout: Duration::from_millis(self.config.websocket_keepalive.pong_timeout_ms),
            upstream_ping: self.config.websocket_keepalive.enabled
                && self.config.websocket_keepalive.upstream,
            downstream_ping: self.config.websocket_keepalive.enabled
                && self.config.websocket_keepalive.downstream,
        };
        let mut transport = carrier::Session::accept_with_decoder(
            &mut session.downstream_session,
            response,
            limits,
            codec.create_with_limits(max_bytes)?,
        )
        .await?;
        ctx.response_dump.set_status(101);
        ctx.response_dump.set_unavailable("protocol_upgrade");
        ctx.upgraded = true;
        ctx.response_received = true;
        ctx.dump.finish(ctx.request_id);
        // Idle sessions reserve no body memory. The carrier acquires a permit
        // before its first decoder allocation and preserves it for queued wire.
        ctx.inspection_permit = None;
        let mut reservation = None;
        let result = self
            .run_websocket_session(
                &mut transport,
                &mut exchange,
                registration,
                SessionRoute {
                    request: &mut request,
                    proxy: &mut proxy,
                },
                ctx,
                &mut reservation,
            )
            .await;
        if result.is_err() {
            // After 101 there is no HTTP error response. Close this WebSocket
            // with a protocol frame; never serialize internal errors/secrets.
            let _ = transport.send_downstream(8, &1011_u16.to_be_bytes()).await;
        }
        let closed = Bytes::from_static(if result.is_ok() {
            br#"{"side":"downstream","reason":"close"}"#
        } else {
            br#"{"side":"downstream","reason":"error"}"#
        });
        // All interested native owners can release session state, independently
        // of which participant owns the one execution-completion callback.
        // This bounded host event needs no decoder allocation. Even when idle
        // sessions exhaust the body budget, native owners must get a cleanup
        // attempt through their independently bounded callback queue.
        let method = format!(
            "{}/transport.closed/{:016x}/{:016x}",
            registration.codec.id(),
            ctx.request_id,
            ctx.response_sequence
        );
        let cleanup_deadline = tokio::time::Instant::now() + self.config.io_timeout;
        let mut cleanup = Vec::new();
        for (index, callback) in registration.callbacks.iter().enumerate() {
            if !callback.interested(true, "transport.closed") {
                continue;
            }
            let method = &method;
            let closed = closed.clone();
            let reservation = reservation.clone();
            let request_id = ctx.request_id;
            cleanup.push(Some(Box::pin(async move {
                let result = callback.notify_closed(method, closed, reservation).await;
                tracing::debug!(target: "proxy_pingora::session", request_id,
                    callback_index = index, outcome = if result.is_ok() { "complete" } else { "error" },
                    "session owner cleanup attempted");
            }) as CleanupFuture<'_>));
        }
        let cleanup_result = within_deadline(
            cleanup_deadline,
            async {
                complete_all(cleanup).await;
                Ok(())
            },
            "session cleanup callback deadline exceeded",
        )
        .await;
        drop(transport);
        drop(reservation);
        match result {
            Ok(()) => {
                exchange.facts_mut().completed = true;
                ctx.session_completed = within_deadline(
                    cleanup_deadline,
                    async {
                        cleanup_result?;
                        exchange.finish().await.map_err(lifecycle_error)
                    },
                    "completion callback deadline exceeded",
                )
                .await
                .map(|_| true)?;
                Ok(())
            }
            Err(error) => {
                exchange.fail();
                ctx.message_failed = true;
                Err(error)
            }
        }
    }

    async fn run_websocket_session(
        &self,
        transport: &mut carrier::Session<'_>,
        exchange: &mut SessionExchange<ProxyContract>,
        registration: &message_codec::CodecRegistration,
        route: SessionRoute<'_>,
        ctx: &mut ProxyContext,
        reservation: &mut Option<Arc<Reservation>>,
    ) -> Result<()> {
        let SessionRoute { request, proxy } = route;
        let mut held: Vec<HeldMessage> = Vec::new();
        let held_bytes =
            ResourceBudget::new(self.inspection.limits.max_message_bytes.saturating_mul(2));
        let held_count = ResourceBudget::new(websocket::Limits::default().max_frames);
        loop {
            let event = transport
                .next_reserved(|| {
                    if reservation.is_none() {
                        *reservation = Some(self.inspection.buffers()?);
                    }
                    Ok(())
                })
                .await?;
            match event {
                Event::Maintenance(maintenance) => {
                    if let Some(side) = transport.maintain(maintenance).await? {
                        match side {
                            Side::Downstream => return Ok(()),
                            Side::Upstream(generation) => {
                                held.clear();
                                exchange.upstream_closed().map_err(lifecycle_error)?;
                                upstream_closed(
                                    transport,
                                    registration,
                                    ctx,
                                    generation,
                                    "close",
                                    &self.inspection,
                                    reservation,
                                )
                                .await?;
                            }
                        }
                    }
                }
                Event::Closed(Side::Downstream) => return Ok(()),
                Event::Closed(Side::Upstream(generation)) => {
                    held.clear();
                    exchange.upstream_closed().map_err(lifecycle_error)?;
                    upstream_closed(
                        transport,
                        registration,
                        ctx,
                        generation,
                        "eof",
                        &self.inspection,
                        reservation,
                    )
                    .await?;
                }
                Event::Message(message) => {
                    let response = matches!(message.side, Side::Upstream(_));
                    let sequence = if response {
                        &mut ctx.response_sequence
                    } else {
                        &mut ctx.message_sequence
                    };
                    let deadline = self.inspection.deadline();
                    let pending = within_deadline(
                        deadline,
                        evaluate(
                            registration,
                            &message,
                            sequence,
                            ctx.request_id,
                            self.inspection.limits.max_message_bytes,
                            reservation.clone(),
                        ),
                        "message callback chain deadline exceeded",
                    );
                    let plan = transport
                        .drive_until(pending, || {
                            if reservation.is_none() {
                                *reservation = Some(self.inspection.buffers()?);
                            }
                            Ok(())
                        })
                        .await??;
                    // Apply the full chain to a draft only after every result is validated.
                    if !plan.patches.is_empty() {
                        let mut next_request = request.clone();
                        let mut next_proxy = proxy.clone();
                        for patch in &plan.patches {
                            patch.apply(&mut next_request, &mut next_proxy)?;
                        }
                        if transport.generation().is_some()
                            && !matches!(
                                plan.upstream,
                                Upstream::Replace | Upstream::Disconnect | Upstream::Close
                            )
                            && (next_request.uri != request.uri
                                || next_request.headers != request.headers
                                || next_proxy != *proxy)
                        {
                            return Err(refusal(
                                502,
                                "live upstream metadata changes require replace",
                            ));
                        }
                        *request = next_request;
                        *proxy = next_proxy;
                    }
                    if matches!(
                        plan.upstream,
                        Upstream::Replace | Upstream::Disconnect | Upstream::Close
                    ) && let Some(generation) = transport.generation()
                    {
                        transport.disconnect(generation)?;
                        if plan.hold != "release" {
                            held.clear();
                        }
                        if plan.upstream != Upstream::Close && plan.hold != "discard" {
                            exchange.upstream_closed().map_err(lifecycle_error)?;
                        }
                        tracing::debug!(target: "proxy_pingora::session", request_id = ctx.request_id,
                            generation, decision = "disconnect", "upstream generation retired");
                    }
                    if plan.hold == "buffer" {
                        held.push(retain_message(
                            &message,
                            &held_bytes,
                            &held_count,
                            reservation
                                .clone()
                                .ok_or_else(|| refusal(500, "missing held wire reservation"))?,
                        )?);
                        continue;
                    }
                    if plan.hold == "discard" {
                        held.clear();
                    }
                    if plan.hold == "release" {
                        // Front to back. Vec::pop would send the newest frame first.
                        for saved in release_hold(&mut held) {
                            let _ = transport.forward_upstream(&saved.message).await;
                        }
                    }
                    match plan.decision {
                        Decision::Reply => {
                            // Codec validation occurred before any connection side effect.
                            transport
                                .send_downstream_wire(plan.reply.clone().expect("validated reply"))
                                .await?;
                        }
                        Decision::Drop => {}
                        Decision::Forward => {
                            let selected = plan.replacement.as_ref().unwrap_or(&message);
                            if response {
                                let sent = transport.forward_upstream(selected).await;
                                if plan.hold != "release" {
                                    sent?;
                                }
                            } else {
                                let generation = match transport.generation() {
                                    Some(generation) => generation,
                                    None => {
                                        self.connect_websocket_generation(
                                            transport,
                                            exchange,
                                            registration.codec.as_ref(),
                                            request,
                                            proxy.as_ref(),
                                            ctx.request_id,
                                        )
                                        .await?
                                    }
                                };
                                transport.forward_client(generation, selected).await?;
                            }
                        }
                    }
                    if plan.upstream == Upstream::Close {
                        let _ = transport.send_downstream(8, &1000_u16.to_be_bytes()).await;
                        return Ok(());
                    }
                }
            }
            if !transport.has_buffered_data() && held.is_empty() {
                *reservation = None;
            }
        }
    }

    async fn connect_websocket_generation(
        &self,
        transport: &mut carrier::Session<'_>,
        exchange: &mut SessionExchange<ProxyContract>,
        codec: &dyn message_codec::CodecPlugin,
        request: &RequestHeader,
        proxy: Option<&Url>,
        request_id: u64,
    ) -> Result<u64> {
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let mut head = request.clone();
        transport
            .drive_until(
                tokio::time::timeout_at(deadline, exchange.before_upstream_send(&mut head)),
                || Ok(()),
            )
            .await?
            .map_err(|_| refusal(504, "upstream metadata deadline exceeded"))?
            .map_err(lifecycle_error)?;
        if head.method != http::Method::GET || head.version != request.version {
            return Err(refusal(
                400,
                "callbacks cannot change WebSocket carrier method",
            ));
        }
        headers::request_head(
            &mut head,
            request,
            &self.config.upstream,
            false,
            false,
            false,
        )?;
        codec.prepare_request(&mut head)?;
        let expected_accept = carrier::handshake::prepare_request(&mut head)?;
        snapshot_request(&head, exchange.facts_mut());
        let target = proxy.unwrap_or(&self.config.upstream);
        let dial = DialRequest {
            target: DialTarget::Host {
                name: target.host_str().unwrap_or_default().into(),
                port: target.port_or_known_default().unwrap_or(1080),
            },
            timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
            tcp_nodelay: true,
        };
        transport
            .drive_until(
                tokio::time::timeout_at(deadline, exchange.acquire(&dial)),
                || Ok(()),
            )
            .await?
            .map_err(|_| refusal(504, "upstream control deadline exceeded"))?
            .map_err(control_error)?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let mut peer = transport
            .drive_until(
                tokio::time::timeout_at(
                    deadline,
                    self.egress
                        .peer(&self.config.upstream, proxy, request_id, remaining),
                ),
                || Ok(()),
            )
            .await?
            .map_err(|_| refusal(504, "upstream resolution deadline exceeded"))??;
        peer.options.idle_timeout = Some(Duration::ZERO);
        let decoder = response_decoder(codec, self.inspection.limits.max_message_bytes)?;
        let (generation, mut response) = tokio::time::timeout_at(
            deadline,
            transport.connect_authorized(
                &self.session_connector,
                &peer,
                head,
                &expected_accept,
                decoder,
                || async { exchange.connected().await.map_err(control_error) },
            ),
        )
        .await
        .map_err(|_| refusal(504, "upstream handshake deadline exceeded"))??;
        codec.validate_response(&response)?;
        exchange.facts_mut().status = Some(response.status.as_u16());
        if let Err(error) = transport
            .drive_until(
                tokio::time::timeout(
                    self.config.io_timeout,
                    exchange.upstream_response(&mut response),
                ),
                || Ok(()),
            )
            .await?
            .map_err(|_| refusal(504, "upstream response callback deadline exceeded"))?
            .map_err(control_error)
        {
            if error.to_string().contains("upstream control denied") {
                let _ = transport.disconnect(generation);
                if let Some(frame) = codec.response_error(REPLAY_REQUIRED) {
                    let _ = transport.send_downstream_wire(Bytes::from(frame)).await;
                }
            }
            return Err(error);
        }
        body::ensure_closed(&exchange.facts().response_body)?;
        tracing::debug!(target: "proxy_pingora::session", request_id, generation, decision = "connect",
            "upstream generation admitted");
        Ok(generation)
    }
}

fn release_hold<T>(held: &mut Vec<T>) -> Vec<T> {
    held.drain(..).collect()
}

fn response_decoder(
    codec: &dyn message_codec::CodecPlugin,
    max_bytes: usize,
) -> Result<Box<dyn Decoder>> {
    use proxy_pingora::http_ingress::message_codec::{CodecPlugin, WebSocketCodec};
    match codec.create_upgrade_response_with_limits(max_bytes)? {
        Some(decoder) => Ok(decoder),
        None => WebSocketCodec
            .create_upgrade_response_with_limits(max_bytes)?
            .ok_or_else(|| refusal(500, "missing response decoder")),
    }
}

async fn evaluate(
    registration: &message_codec::CodecRegistration,
    original: &Message,
    sequence: &mut u64,
    request_id: u64,
    max_bytes: usize,
    reservation: Option<Arc<admission_gate::budget::Reservation>>,
) -> Result<Plan> {
    let mut plan = Plan {
        decision: Decision::Forward,
        upstream: Upstream::Keep,
        hold: String::new(),
        patches: Vec::new(),
        replacement: None,
        reply: None,
    };
    let Some(message) = &original.message else {
        return Ok(plan);
    };
    let response = matches!(original.side, Side::Upstream(_));
    let number = *sequence;
    // Validate even bypassed custom events, without formatting delta methods.
    message_codec::callback_method(registration.codec.id(), message.kind, request_id, number)?;
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| refusal(400, "message sequence exhausted"))?;
    for (index, callback) in registration.callbacks.iter().enumerate() {
        let current = plan
            .replacement
            .as_ref()
            .unwrap_or(original)
            .message
            .as_ref()
            .unwrap_or(message);
        let method = message_codec::callback_method(
            registration.codec.id(),
            current.kind,
            request_id,
            number,
        )?;
        if !callback.interested(response, current.kind) {
            continue;
        }
        let method = method.to_string();
        let started = Instant::now();
        let reply = callback
            .decide(
                &method,
                current.payload.clone(),
                response,
                max_bytes,
                reservation.clone(),
            )
            .await;
        tracing::debug!(target: "proxy_pingora::message", request_id, sequence = number, callback_index = index,
            codec = registration.codec.id(), kind = current.kind, bytes = current.payload.len(),
            elapsed_us = started.elapsed().as_micros() as u64,
            outcome = match &reply { Ok(Reply::Allow) => "allow", Ok(Reply::Deny) => "deny", Ok(Reply::ProtocolError(_)) => "reply", Ok(Reply::Action(_)) => "action", Err(_) => "error" },
            "native message decision");
        match reply? {
            Reply::Allow => {}
            Reply::Deny => return Err(refusal(403, "message denied")),
            Reply::ProtocolError(payload) => {
                let frame = validate_reply(registration.codec.as_ref(), &payload, max_bytes)?;
                plan.decision = Decision::Reply;
                plan.reply = Some(frame);
                break;
            }
            Reply::Action(action) => {
                tracing::debug!(target: "proxy_pingora::message", request_id, sequence = number,
                    callback_index = index, decision = ?action.metadata.decision,
                    upstream = ?action.metadata.upstream, replacement_bytes = action.payload.len(),
                    header_patches = action.metadata.set_headers.len() + action.metadata.remove_headers.len(),
                    "native message action validated");
                if action.metadata.upstream != Upstream::Keep {
                    if plan.upstream != Upstream::Keep
                        && plan.upstream != action.metadata.upstream
                        && action.metadata.decision == Decision::Forward
                    {
                        return Err(refusal(502, "conflicting upstream message decisions"));
                    }
                    plan.upstream = action.metadata.upstream;
                }
                if !action.metadata.hold.is_empty() {
                    plan.hold = action.metadata.hold.clone();
                }
                plan.decision = action.metadata.decision;
                match plan.decision {
                    Decision::Forward if !action.payload.is_empty() => {
                        plan.replacement = Some(replacement(
                            registration.codec.as_ref(),
                            original.side,
                            action.payload,
                            max_bytes,
                        )?);
                    }
                    Decision::Reply => {
                        plan.reply = Some(validate_reply(
                            registration.codec.as_ref(),
                            &action.payload,
                            max_bytes,
                        )?);
                    }
                    _ => {}
                }
                if !action.metadata.set_headers.is_empty()
                    || !action.metadata.remove_headers.is_empty()
                    || action.metadata.proxy.is_some()
                    || action.metadata.path.is_some()
                {
                    plan.patches.push(action.metadata);
                }
                if plan.decision != Decision::Forward {
                    break;
                }
            }
        }
    }
    Ok(plan)
}

fn validate_reply(
    codec: &dyn message_codec::CodecPlugin,
    payload: &[u8],
    max_bytes: usize,
) -> Result<Bytes> {
    let frame = codec
        .response_error(payload)
        .ok_or_else(|| refusal(502, "invalid protocol error reply"))?;
    if frame.len() > max_bytes + 64 * 1024 {
        return Err(refusal(502, "protocol reply exceeds wire budget"));
    }
    Ok(Bytes::from(frame))
}

async fn upstream_closed(
    transport: &mut carrier::Session<'_>,
    registration: &message_codec::CodecRegistration,
    ctx: &mut ProxyContext,
    generation: u64,
    reason: &str,
    budget: &inspection::InspectionBudget,
    reservation: &mut Option<Arc<Reservation>>,
) -> Result<()> {
    // Keep the same reservation for any client data queued while this callback
    // runs. Releasing it here would leave deferred wire outside the budget.
    if reservation.is_none() {
        *reservation = Some(budget.buffers()?);
    }
    let message = Message {
        side: Side::Upstream(generation),
        wire: Vec::new(),
        message: Some(proxy_protocols::message_codec::Message {
            kind: "transport.closed",
            payload: Bytes::from(
                serde_json::to_vec(
                    &serde_json::json!({"side":"upstream","generation":generation,"reason":reason}),
                )
                .expect("metadata serialization"),
            ),
        }),
    };
    let plan = transport
        .drive_until(
            within_deadline(
                budget.deadline(),
                evaluate(
                    registration,
                    &message,
                    &mut ctx.response_sequence,
                    ctx.request_id,
                    budget.limits.max_message_bytes,
                    reservation.clone(),
                ),
                "upstream cleanup callback chain deadline exceeded",
            ),
            || Ok(()),
        )
        .await??;
    if let Some(reply) = plan.reply {
        transport.send_downstream_wire(reply).await?;
    }
    Ok(())
}
fn replacement(
    codec: &dyn message_codec::CodecPlugin,
    side: Side,
    payload: Bytes,
    max_bytes: usize,
) -> Result<Message> {
    let (wire, mut decoder) = match side {
        Side::Downstream => (
            websocket::encode_client(1, &payload),
            codec.create_with_limits(max_bytes)?,
        ),
        Side::Upstream(_) => (
            websocket::encode_server(1, &payload),
            response_decoder(codec, max_bytes)?,
        ),
    };
    let wire = wire.map_err(|_| refusal(502, "invalid replacement frame"))?;
    let mut events = decoder
        .push(wire)
        .map_err(|_| refusal(502, "replacement violates selected codec"))?;
    decoder
        .finish()
        .map_err(|_| refusal(502, "incomplete replacement message"))?;
    if events.len() != 1 || events[0].message.is_none() {
        return Err(refusal(
            502,
            "replacement must contain exactly one semantic message",
        ));
    }
    let event = events.pop().expect("single event");
    Ok(Message {
        side,
        message: event.message,
        wire: event.wire,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn released_hold_sends_the_oldest_frame_first() {
        let mut held = Vec::new();
        for frame in ["delta-1", "delta-2", "delta-3"] {
            held.push(frame);
        }
        assert_eq!(release_hold(&mut held), ["delta-1", "delta-2", "delta-3"]);
        assert!(held.is_empty());
    }

    fn held_test_message() -> Message {
        Message {
            side: Side::Upstream(1),
            message: None,
            wire: vec![Bytes::from_static(b"wire")],
        }
    }

    #[test]
    fn held_wire_bounds_bytes_and_count_without_copying_or_releasing_ownership() {
        let original = held_test_message();
        let bytes = ResourceBudget::new(4096);
        let count = ResourceBudget::new(1);
        let inspection = ResourceBudget::new(100);
        let pin = Arc::new(inspection.try_reserve(100).unwrap());
        let saved = retain_message(&original, &bytes, &count, pin.clone()).unwrap();
        assert_eq!(saved.message.wire[0].as_ptr(), original.wire[0].as_ptr());
        let held_units = bytes.used();
        assert!(held_units >= original.wire[0].len());
        assert!(retain_message(&original, &bytes, &count, pin.clone()).is_err());
        assert_eq!(
            bytes.used(),
            held_units,
            "failed count reservation must roll back bytes"
        );
        drop(pin);
        let mut held = vec![saved];
        let released = release_hold(&mut held);
        assert!(held.is_empty());
        assert_eq!(
            inspection.used(),
            100,
            "draining the hold does not release pending writes"
        );
        assert_eq!(count.used(), 1);
        drop(released);
        assert_eq!(inspection.used(), 0);
        assert_eq!(count.used(), 0);
        assert_eq!(bytes.used(), 0);
        let too_small = ResourceBudget::new(held_units - 1);
        let pin = Arc::new(inspection.try_reserve(100).unwrap());
        assert!(retain_message(&original, &too_small, &count, pin).is_err());
        assert_eq!(too_small.used(), 0);
        assert_eq!(count.used(), 0);
        assert_eq!(inspection.used(), 0);
    }

    #[tokio::test]
    async fn absolute_callback_deadline_bounds_a_whole_chain_and_rejects_expired_work() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let result = within_deadline(
            deadline,
            async {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(12)).await;
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
            "test chain expired",
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        let result = within_deadline(
            tokio::time::Instant::now(),
            async {
                panic!("expired chain must never poll a callback");
                #[allow(unreachable_code)]
                Ok(())
            },
            "test chain expired",
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cleanup_polls_all_owners_with_one_shared_deadline() {
        let notified = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observer = notified.clone();
        let operations: Vec<Option<CleanupFuture<'_>>> = vec![
            Some(Box::pin(std::future::pending())),
            Some(Box::pin(async move {
                observer.store(true, std::sync::atomic::Ordering::SeqCst);
            })),
        ];
        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        assert!(
            within_deadline(
                deadline,
                async {
                    complete_all(operations).await;
                    Ok(())
                },
                "test cleanup expired"
            )
            .await
            .is_err()
        );
        assert!(notified.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            within_deadline(deadline, async { Ok(()) }, "test completion expired")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_codec_kind_is_rejected_even_without_interested_callbacks() {
        let registration = message_codec::CodecRegistration {
            codec: Arc::new(proxy_pingora::http_ingress::message_codec::WebSocketCodec),
            callbacks: vec![],
        };
        for side in [Side::Downstream, Side::Upstream(1)] {
            let message = Message {
                side,
                message: Some(proxy_protocols::message_codec::Message {
                    kind: "response/create",
                    payload: Bytes::from_static(b"{}"),
                }),
                wire: vec![],
            };
            let mut sequence = 7;
            assert!(
                evaluate(&registration, &message, &mut sequence, 1, 1024, None)
                    .await
                    .is_err()
            );
            assert_eq!(sequence, 7);
        }
    }
}
