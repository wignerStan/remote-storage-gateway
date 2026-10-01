//! Execute granted message decisions through separately owned native sessions.
//! Route, account, token and replay policy remain in the selected callbacks.
use super::message_action::{Decision, Metadata, Reply, Upstream};
use super::*;
use admission_gate::budget::Reservation;
use bytes::Bytes;
use proxy_pingora::{
    connect_upstream::websocket_session::{self as carrier, Event, Message, Side},
    worker::SessionExchange,
};
use proxy_protocols::{message_codec::Decoder, websocket};

struct Plan {
    decision: Decision,
    upstream: Upstream,
    patches: Vec<Metadata>,
    replacement: Option<Message>,
    reply: Option<Bytes>,
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
            ..Default::default()
        };
        let mut transport = carrier::Session::accept_with_decoder(
            &mut session.downstream_session,
            response,
            limits,
            codec.create_with_limits(max_bytes)?,
        )
        .await?;
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
                &mut request,
                &mut proxy,
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
        for (index, callback) in registration.callbacks.iter().enumerate() {
            if !callback.interested(true, "transport.closed") {
                continue;
            }
            let result = callback
                .notify_closed(&method, closed.clone(), reservation.clone())
                .await;
            tracing::debug!(target: "proxy_pingora::session", request_id = ctx.request_id,
                callback_index = index, outcome = if result.is_ok() { "complete" } else { "error" },
                "session owner cleanup attempted");
        }
        drop(transport);
        drop(reservation);
        match result {
            Ok(()) => {
                exchange.facts_mut().completed = true;
                ctx.session_completed =
                    tokio::time::timeout(self.config.io_timeout, exchange.finish())
                        .await
                        .map_err(|_| refusal(504, "completion callback deadline exceeded"))?
                        .map_err(lifecycle_error)
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
        request: &mut RequestHeader,
        proxy: &mut Option<Url>,
        ctx: &mut ProxyContext,
        reservation: &mut Option<Arc<Reservation>>,
    ) -> Result<()> {
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
                    let pending = evaluate(
                        registration,
                        &message,
                        sequence,
                        ctx.request_id,
                        self.inspection.limits.max_message_bytes,
                        reservation.clone(),
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
                            && !matches!(plan.upstream, Upstream::Replace | Upstream::Disconnect)
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
                    if matches!(plan.upstream, Upstream::Replace | Upstream::Disconnect)
                        && let Some(generation) = transport.generation()
                    {
                        transport.disconnect(generation)?;
                        exchange.upstream_closed().map_err(lifecycle_error)?;
                        tracing::debug!(target: "proxy_pingora::session", request_id = ctx.request_id,
                            generation, decision = "disconnect", "upstream generation retired");
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
                                transport.forward_upstream(selected).await?;
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
                }
            }
            if !transport.has_buffered_data() {
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
        transport
            .drive_until(
                tokio::time::timeout(
                    self.config.io_timeout,
                    exchange.upstream_response(&mut response),
                ),
                || Ok(()),
            )
            .await?
            .map_err(|_| refusal(504, "upstream response callback deadline exceeded"))?
            .map_err(lifecycle_error)?;
        body::ensure_closed(&exchange.facts().response_body)?;
        tracing::debug!(target: "proxy_pingora::session", request_id, generation, decision = "connect",
            "upstream generation admitted");
        Ok(generation)
    }
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
        patches: Vec::new(),
        replacement: None,
        reply: None,
    };
    let Some(message) = &original.message else {
        return Ok(plan);
    };
    let response = matches!(original.side, Side::Upstream(_));
    let number = *sequence;
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
        if !callback.interested(response, current.kind) {
            continue;
        }
        let method = format!(
            "{}/{}/{request_id:016x}/{number:016x}",
            registration.codec.id(),
            current.kind
        );
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
            evaluate(
                registration,
                &message,
                &mut ctx.response_sequence,
                ctx.request_id,
                budget.limits.max_message_bytes,
                reservation.clone(),
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
