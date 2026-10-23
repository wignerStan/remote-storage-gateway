//! Execute granted message decisions through separately owned native sessions.
//! Route, account, token and replay policy remain in the selected callbacks.
mod generation_lifetime;
mod http_bridge_session;
use super::message_action::{Decision, Metadata, Reply, Upstream};
use super::*;
use admission_gate::budget::{Reservation, ResourceBudget};
use bytes::Bytes;
use generation_lifetime::GenerationLifetime;
use proxy_pingora::{
    connect_upstream::websocket_session::{self as carrier, Event, Message, Side},
    worker::SessionExchange,
};
use proxy_protocols::{message_codec::Decoder, websocket};

/// Mutable route draft retained across upstream generations of one session.
struct SessionRoute<'a> {
    request: &'a mut RequestHeader,
    proxy: &'a mut Option<Url>,
}

struct Plan {
    decision: Decision,
    socket_lifetime: Option<super::message_action::SocketLifetime>,
    retain_upstream_on_downstream_close: Option<bool>,
    upstream: Upstream,
    hold: String,
    hold_response_at_unix_ms: Option<u64>,
    close_downstream: bool,
    patches: Vec<Metadata>,
    replacement: Option<Message>,
    reply: Option<Bytes>,
}

/// Convert an application wall-clock boundary once. Runtime comparisons use the
/// monotonic clock, so a later host clock adjustment cannot move the boundary.
#[derive(Default)]
struct ResponseHold {
    deadline: Option<tokio::time::Instant>,
}
impl ResponseHold {
    fn clear(&mut self) {
        self.deadline = None;
    }

    fn apply(&mut self, plan: &Plan) -> Result<()> {
        if matches!(plan.hold.as_str(), "release" | "discard")
            || matches!(
                plan.upstream,
                Upstream::Replace | Upstream::Disconnect | Upstream::Close
            )
        {
            self.clear();
        }
        if let Some(at) = plan.hold_response_at_unix_ms
            && !matches!(plan.hold.as_str(), "release" | "discard")
        {
            self.deadline = if at == 0 {
                None
            } else {
                super::message_action::validate_response_hold_time(at)?;
                Some(
                    tokio::time::Instant::now()
                        + Duration::from_millis(
                            at.saturating_sub(super::message_action::unix_ms()?),
                        ),
                )
            };
        }
        Ok(())
    }

    fn buffers(&self, message: &Message, plan: &Plan) -> bool {
        plan.decision == Decision::Forward
            && plan.hold.is_empty()
            && plan.upstream == Upstream::Keep
            && matches!(message.side, Side::Upstream(_))
            && message.message.is_some()
            && !response_terminal(message)
            && self
                .deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
    }
}

async fn shutdown_event(
    stop: &mut Option<tokio::sync::watch::Receiver<Option<tokio::time::Instant>>>,
) {
    if let Some(stop) = stop {
        let deadline = *stop.borrow();
        if let Some(at) = deadline {
            tokio::time::sleep_until(at).await;
        } else {
            let _ = stop.changed().await;
        }
    } else {
        std::future::pending::<()>().await;
    }
}
fn shutdown_deadline(
    stop: &Option<tokio::sync::watch::Receiver<Option<tokio::time::Instant>>>,
) -> Option<tokio::time::Instant> {
    stop.as_ref().and_then(|stop| *stop.borrow())
}

fn starts_turn(message: &Message) -> bool {
    message.side == Side::Downstream
        && message.message.as_ref().is_some_and(|semantic| {
            matches!(
                semantic.kind,
                "response.create" | "responses_lite.create" | "responses_lite.delta"
            )
        })
}

fn response_terminal(message: &Message) -> bool {
    matches!(message.side, Side::Upstream(_))
        && message.message.as_ref().is_some_and(|semantic| {
            matches!(
                semantic.kind,
                "response.completed"
                    | "response.done"
                    | "response.failed"
                    | "response.incomplete"
                    | "response.error"
                    | "response.cancelled"
                    | "response.canceled"
                    | "error"
            )
        })
}

fn validate_terminal_hold(message: &Message, plan: &Plan, held: bool) -> Result<()> {
    if held
        && response_terminal(message)
        && plan.decision == Decision::Forward
        && plan.hold.is_empty()
    {
        return Err(refusal(
            502,
            "terminal delivery requires release or discard of held messages",
        ));
    }
    Ok(())
}

/// Application-held wire is outside Pingora's decoder and pending-event queues.
/// Keep each reservation with its bytes, including while a release is writing.
struct HeldMessage {
    message: Message,
    starts_turn: bool,
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
        starts_turn: starts_turn(message),
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

// Poll every teardown owner before waiting for any one owner. Outputs cannot
// alter a closed transport, but failed cleanup must not report delivery success.
async fn notify_session_closed(
    registration: &message_codec::CodecRegistration,
    method: &str,
    closed: Bytes,
    reservation: Option<Arc<Reservation>>,
    request_id: u64,
    deadline: tokio::time::Instant,
) -> Result<()> {
    notify_session_event(
        registration,
        "transport.closed",
        method,
        closed,
        reservation,
        request_id,
        deadline,
    )
    .await
}

async fn notify_session_event(
    registration: &message_codec::CodecRegistration,
    kind: &str,
    method: &str,
    closed: Bytes,
    reservation: Option<Arc<Reservation>>,
    request_id: u64,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let failed = std::sync::atomic::AtomicBool::new(false);
    let mut cleanup = Vec::new();
    for (index, callback) in registration.callbacks.iter().enumerate() {
        if !callback.interested(true, kind) {
            continue;
        }
        let closed = closed.clone();
        let reservation = reservation.clone();
        let failed = &failed;
        cleanup.push(Some(Box::pin(async move {
            let result = callback.notify_closed(method, closed, reservation).await;
            if result.is_err() {
                failed.store(true, Ordering::Relaxed);
            }
            tracing::debug!(target: "proxy_pingora::session", request_id,
                callback_index = index, outcome = if result.is_ok() { "complete" } else { "error" },
                "session owner cleanup attempted");
        }) as CleanupFuture<'_>));
    }
    within_deadline(
        deadline,
        async {
            complete_all(cleanup).await;
            if failed.load(Ordering::Relaxed) {
                Err(refusal(502, "session owner cleanup failed"))
            } else {
                Ok(())
            }
        },
        "session cleanup callback deadline exceeded",
    )
    .await
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
        let mut lifetime = GenerationLifetime::default();
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
                &mut lifetime,
            )
            .await;
        if transport.downstream_closed() && lifetime.may_detach() {
            // Signal admission cancellation before any callback can wait on the
            // Go session mutex. No active reservation is released by this event.
            let method = format!(
                "{}/transport.detached/{:016x}/{:016x}",
                registration.codec.id(),
                ctx.request_id,
                ctx.response_sequence
            );
            let detached = notify_session_event(
                registration,
                "transport.detached",
                &method,
                Bytes::from_static(br#"{"side":"downstream","reason":"close"}"#),
                reservation.clone(),
                ctx.request_id,
                tokio::time::Instant::now() + self.config.io_timeout,
            )
            .await;
            if let (Ok(()), Some(generation), Some(owner)) = (
                detached,
                lifetime.take(&mut transport),
                &self.retained_sessions,
            ) {
                let stop = owner.subscribe();
                let detached = generation_lifetime::DetachedOwner {
                    registration: registration.clone(),
                    inspection: Arc::clone(&self.inspection),
                    reservation: reservation.take(),
                    request_id: ctx.request_id,
                    sequence: ctx.response_sequence,
                    exchange,
                    io_timeout: self.config.io_timeout,
                };
                let inspection = Arc::clone(&self.inspection);
                let io_timeout = self.config.io_timeout;
                owner.spawn(generation.run(Some(detached), stop, io_timeout, inspection, None));
                // Downstream delivery failed. Upstream settlement remains owned by
                // the resource task and cannot count this local 101 as delivery.
                ctx.message_failed = true;
                drop(transport);
                return Ok(());
            }
        }
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
        let cleanup_result = notify_session_closed(
            registration,
            &method,
            closed,
            reservation.clone(),
            ctx.request_id,
            cleanup_deadline,
        )
        .await;
        drop(transport);
        drop(reservation);
        match result.and(cleanup_result) {
            Ok(()) => {
                exchange.facts_mut().completed = true;
                let completion = within_deadline(
                    cleanup_deadline,
                    async { exchange.finish().await.map_err(lifecycle_error) },
                    "completion callback deadline exceeded",
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

    async fn run_websocket_session(
        &self,
        transport: &mut carrier::Session<'_>,
        exchange: &mut SessionExchange<ProxyContract>,
        registration: &message_codec::CodecRegistration,
        route: SessionRoute<'_>,
        ctx: &mut ProxyContext,
        reservation: &mut Option<Arc<Reservation>>,
        lifetime: &mut GenerationLifetime,
    ) -> Result<()> {
        let SessionRoute { request, proxy } = route;
        let mut held: Vec<HeldMessage> = Vec::new();
        let mut response_hold = ResponseHold::default();
        let mut stop = self
            .retained_sessions
            .as_ref()
            .map(|owner| owner.subscribe());
        let held_bytes =
            ResourceBudget::new(self.inspection.limits.max_message_bytes.saturating_mul(2));
        let held_count = ResourceBudget::new(websocket::Limits::default().max_frames);
        loop {
            if let Some(deadline) = shutdown_deadline(&stop) {
                if !lifetime.active {
                    if let Some(generation) = lifetime.take(transport) {
                        let owner = self
                            .retained_sessions
                            .as_ref()
                            .expect("reserved physical owner");
                        owner.spawn(generation.run(
                            None,
                            owner.subscribe(),
                            self.config.io_timeout,
                            Arc::clone(&self.inspection),
                            reservation.clone(),
                        ));
                    }
                    transport
                        .send_downstream(8, &1001_u16.to_be_bytes())
                        .await?;
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(refusal(
                        504,
                        "active session exceeded process drain deadline",
                    ));
                }
            }
            if lifetime.retirement_due() && !lifetime.active && transport.generation().is_some() {
                self.retire_websocket_generation(
                    transport,
                    exchange,
                    registration,
                    ctx,
                    reservation,
                    lifetime,
                )
                .await?;
                held.clear();
                response_hold.clear();
            }
            let timer = async {
                match lifetime.retire_at() {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let event = tokio::select! {
                _ = shutdown_event(&mut stop) => continue,
                _ = timer => { lifetime.retire(); continue; },
                event = transport.next_reserved(|| {
                    if reservation.is_none() { *reservation = Some(self.inspection.buffers()?); }
                    Ok(())
                }) => event?,
            };
            match event {
                Event::Maintenance(maintenance) => {
                    if let Some(side) = transport.maintain(maintenance).await? {
                        match side {
                            Side::Downstream => return Ok(()),
                            Side::Upstream(generation) => {
                                held.clear();
                                response_hold.clear();
                                lifetime.reset();
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
                    response_hold.clear();
                    lifetime.reset();
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
                    if starts_turn(&message) && shutdown_deadline(&stop).is_some() {
                        transport.send_downstream(1,br#"{"type":"error","status":503,"error":{"code":"server_shutting_down","message":"Retry the full request on a new connection"}}"#).await?;
                        continue;
                    }
                    let retirement = if !response && !lifetime.active {
                        lifetime.retire_at()
                    } else {
                        None
                    };
                    let approved = {
                        let sequence = if response {
                            &mut ctx.response_sequence
                        } else {
                            &mut ctx.message_sequence
                        };
                        let pending = within_deadline(
                            self.inspection.deadline(),
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
                        let driving = transport.drive_until(pending, || {
                            if reservation.is_none() {
                                *reservation = Some(self.inspection.buffers()?);
                            }
                            Ok(())
                        });
                        let timer = async {
                            match retirement {
                                Some(at) => tokio::time::sleep_until(at).await,
                                None => std::future::pending().await,
                            }
                        };
                        tokio::pin!(driving);
                        tokio::pin!(timer);
                        loop {
                            tokio::select! {
                                biased;
                                _=shutdown_event(&mut stop)=>{
                                    if !lifetime.active || shutdown_deadline(&stop).is_some_and(|at|tokio::time::Instant::now()>=at) {break None;}
                                },
                                _=&mut timer=>break None,
                                plan=&mut driving=>break Some(plan??),
                            }
                        }
                    };
                    let Some(plan) = approved else {
                        if shutdown_deadline(&stop).is_some() {
                            if let Some(generation) = lifetime.take(transport) {
                                let owner = self
                                    .retained_sessions
                                    .as_ref()
                                    .expect("reserved physical owner");
                                owner.spawn(generation.run(
                                    None,
                                    owner.subscribe(),
                                    self.config.io_timeout,
                                    Arc::clone(&self.inspection),
                                    reservation.clone(),
                                ));
                            }
                            transport
                                .send_downstream(8, &1001_u16.to_be_bytes())
                                .await?;
                            return Err(refusal(
                                503,
                                "pending turn cancelled during process drain",
                            ));
                        }
                        self.retire_websocket_generation(
                            transport,
                            exchange,
                            registration,
                            ctx,
                            reservation,
                            lifetime,
                        )
                        .await?;
                        return Err(refusal(
                            409,
                            "physical generation retired during admission; full retry required",
                        ));
                    };
                    if !response
                        && !lifetime.active
                        && lifetime.retirement_due()
                        && transport.generation().is_some()
                    {
                        self.retire_websocket_generation(
                            transport,
                            exchange,
                            registration,
                            ctx,
                            reservation,
                            lifetime,
                        )
                        .await?;
                        return Err(refusal(
                            409,
                            "physical generation retired during admission; full retry required",
                        ));
                    }
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
                        lifetime.reset();
                        if plan.hold != "release" {
                            held.clear();
                        }
                        if plan.upstream != Upstream::Close && plan.hold != "discard" {
                            exchange.upstream_closed().map_err(lifecycle_error)?;
                        }
                        tracing::debug!(target: "proxy_pingora::session", request_id = ctx.request_id,
                            generation, decision = "disconnect", "upstream generation retired");
                    }
                    lifetime.apply(&plan, transport)?;
                    response_hold.apply(&plan)?;
                    // Callback settlement precedes any downstream write. A failed
                    // write after a terminal must not keep the Go owner active.
                    if response_terminal(&message) {
                        lifetime.active = false;
                    }
                    let selected = plan.replacement.as_ref().unwrap_or(&message);
                    validate_terminal_hold(selected, &plan, !held.is_empty())?;
                    if plan.hold == "buffer" || response_hold.buffers(selected, &plan) {
                        held.push(retain_message(
                            selected,
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
                            match saved.message.side {
                                Side::Downstream => {
                                    if matches!(
                                        plan.upstream,
                                        Upstream::Disconnect | Upstream::Close
                                    ) {
                                        return Err(refusal(
                                            502,
                                            "held request conflicts with upstream retirement",
                                        ));
                                    }
                                    let generation = match transport.generation() {
                                        Some(generation) => generation,
                                        None => {
                                            lifetime.reserve(self)?;
                                            let generation = self
                                                .connect_websocket_generation(
                                                    transport,
                                                    exchange,
                                                    registration.codec.as_ref(),
                                                    request,
                                                    proxy.as_ref(),
                                                    ctx.request_id,
                                                )
                                                .await?;
                                            lifetime.connected(transport);
                                            generation
                                        }
                                    };
                                    lifetime.active |= saved.starts_turn;
                                    transport.forward_client(generation, &saved.message).await?;
                                }
                                Side::Upstream(generation) => {
                                    if transport.generation() == Some(generation) {
                                        transport.forward_upstream(&saved.message).await?;
                                    }
                                }
                            }
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
                                let current = matches!(selected.side, Side::Upstream(generation)
                                    if transport.generation() == Some(generation));
                                if plan.hold != "release" || current {
                                    transport.forward_upstream(selected).await?;
                                }
                            } else {
                                let generation = match transport.generation() {
                                    Some(generation) => generation,
                                    None => {
                                        lifetime.reserve(self)?;
                                        let generation = self
                                            .connect_websocket_generation(
                                                transport,
                                                exchange,
                                                registration.codec.as_ref(),
                                                request,
                                                proxy.as_ref(),
                                                ctx.request_id,
                                            )
                                            .await?;
                                        lifetime.connected(transport);
                                        generation
                                    }
                                };
                                lifetime.active |= starts_turn(selected);
                                transport.forward_client(generation, selected).await?;
                            }
                        }
                    }
                    if close_after_decision(plan.decision, plan.close_downstream) {
                        transport.close_downstream_keep_upstream().await?;
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

    async fn retire_websocket_generation(
        &self,
        transport: &mut carrier::Session<'_>,
        exchange: &mut SessionExchange<ProxyContract>,
        registration: &message_codec::CodecRegistration,
        ctx: &mut ProxyContext,
        reservation: &mut Option<Arc<Reservation>>,
        lifetime: &mut GenerationLifetime,
    ) -> Result<()> {
        let generation = transport.generation().expect("live retiring generation");
        if let Some(retained) = lifetime.take(transport) {
            let owner = self
                .retained_sessions
                .as_ref()
                .expect("reserved physical owner");
            owner.spawn(retained.run(
                None,
                owner.subscribe(),
                self.config.io_timeout,
                Arc::clone(&self.inspection),
                reservation.clone(),
            ));
        }
        exchange.upstream_closed().map_err(lifecycle_error)?;
        upstream_closed(
            transport,
            registration,
            ctx,
            generation,
            "retired",
            &self.inspection,
            reservation,
        )
        .await?;
        lifetime.reset();
        Ok(())
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
        message_codec::validate_responses_upstream(codec, &head)?;
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
        let connecting = tokio::time::timeout_at(
            deadline,
            transport.connect_authorized(
                &self.session_connector,
                &peer,
                head,
                &expected_accept,
                decoder,
                || async { exchange.connected().await.map_err(control_error) },
            ),
        );
        let connected = match &self.retained_sessions {
            Some(owner) => owner.on_runtime(connecting).await,
            None => connecting.await,
        };
        let (generation, mut response) =
            connected.map_err(|_| refusal(504, "upstream handshake deadline exceeded"))??;
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
            }
            return Err(error);
        }
        body::ensure_closed(&exchange.facts().response_body)?;
        tracing::debug!(target: "proxy_pingora::session", request_id, generation, decision = "connect",
            "upstream generation admitted");
        Ok(generation)
    }
}

fn close_after_decision(decision: Decision, requested: bool) -> bool {
    requested && matches!(decision, Decision::Reply | Decision::Drop)
}

fn opaque_text_frames(payload: &[u8]) -> Result<Bytes> {
    let mut out = Vec::new();
    let mut rest = payload;
    let mut count = 0usize;
    while !rest.is_empty() {
        if count == 4 || rest.len() < 4 {
            return Err(refusal(502, "invalid text frame list"));
        }
        let len = u32::from_le_bytes(rest[..4].try_into().expect("checked prefix")) as usize;
        rest = &rest[4..];
        if len == 0 || len > rest.len() {
            return Err(refusal(502, "invalid text frame list"));
        }
        let frame = websocket::encode_server(1, &rest[..len])
            .map_err(|_| refusal(502, "text frame encode failed"))?;
        out.extend_from_slice(&frame);
        rest = &rest[len..];
        count += 1;
    }
    if count == 0 {
        return Err(refusal(502, "invalid text frame list"));
    }
    Ok(Bytes::from(out))
}

fn release_hold<T>(held: &mut Vec<T>) -> Vec<T> {
    std::mem::take(held)
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
        socket_lifetime: None,
        retain_upstream_on_downstream_close: None,
        upstream: Upstream::Keep,
        hold: String::new(),
        hold_response_at_unix_ms: None,
        close_downstream: false,
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
                if let Some(lifetime) = action.metadata.socket_lifetime {
                    if plan
                        .socket_lifetime
                        .is_some_and(|previous| previous != lifetime)
                    {
                        return Err(refusal(502, "conflicting physical socket lifetimes"));
                    }
                    plan.socket_lifetime = Some(lifetime);
                }
                if let Some(retain) = action.metadata.retain_upstream_on_downstream_close {
                    if plan
                        .retain_upstream_on_downstream_close
                        .is_some_and(|previous| previous != retain)
                    {
                        return Err(refusal(502, "conflicting downstream retention decisions"));
                    }
                    plan.retain_upstream_on_downstream_close = Some(retain);
                }
                if !action.metadata.hold.is_empty() {
                    plan.hold = action.metadata.hold.clone();
                }
                if let Some(at) = action.metadata.hold_response_at_unix_ms {
                    if plan
                        .hold_response_at_unix_ms
                        .is_some_and(|previous| previous != at)
                    {
                        return Err(refusal(502, "conflicting response hold schedules"));
                    }
                    plan.hold_response_at_unix_ms = Some(at);
                }
                plan.close_downstream = action.metadata.close_downstream;
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
                    Decision::Reply if action.metadata.text_frames => {
                        plan.reply = Some(opaque_text_frames(&action.payload)?);
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
    super::message_action::validate_buffer_decision(&plan.hold, plan.decision)?;
    // A later terminal callback decision supersedes an earlier forward draft.
    if plan.decision != Decision::Forward {
        plan.socket_lifetime = None;
        plan.retain_upstream_on_downstream_close = None;
        plan.hold_response_at_unix_ms = None;
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
    if plan.upstream == Upstream::Close {
        transport
            .send_downstream(8, &1000_u16.to_be_bytes())
            .await?;
    }
    Ok(())
}
// Fragment large configured messages using the existing bounded frame encoder.
fn encode_text(client: bool, payload: &[u8]) -> std::result::Result<Bytes, websocket::Error> {
    if std::str::from_utf8(payload).is_err() {
        return Err(websocket::Error::Protocol("invalid text UTF-8"));
    }
    if payload.is_empty() {
        return if client {
            websocket::encode_client(1, payload)
        } else {
            websocket::encode_server(1, payload)
        };
    }
    let mut output = Vec::new();
    let chunks = payload.chunks(1024 * 1024);
    let count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        let frame = if client {
            websocket::encode_client(2, chunk)
        } else {
            websocket::encode_server(2, chunk)
        }?;
        let mut frame = frame.to_vec();
        frame[0] = if index + 1 == count { 0x80 } else { 0 } | if index == 0 { 1 } else { 0 };
        output.extend(frame);
    }
    Ok(Bytes::from(output))
}

fn replacement(
    codec: &dyn message_codec::CodecPlugin,
    side: Side,
    payload: Bytes,
    max_bytes: usize,
) -> Result<Message> {
    let (wire, mut decoder) = match side {
        Side::Downstream => (
            encode_text(true, &payload),
            codec.create_with_limits(max_bytes)?,
        ),
        Side::Upstream(_) => (
            encode_text(false, &payload),
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
    #[test]
    fn drop_and_prewarm_drop_close_downstream_without_replacing_upstream() {
        for decision in ["drop", "reply"] {
            let metadata = format!(
                r#"{{"version":1,"decision":"{decision}","upstream":"keep","close_downstream":true}}"#
            );
            let action: crate::proxy::message_action::Metadata =
                serde_json::from_str(&metadata).unwrap();
            assert!(close_after_decision(
                action.decision,
                action.close_downstream
            ));
            assert_eq!(action.upstream, Upstream::Keep);
        }
        assert!(!close_after_decision(Decision::Drop, false));
        assert!(!close_after_decision(Decision::Forward, true));
    }

    #[test]
    fn replay_reply_preserves_multiple_text_frames_and_close_metadata() {
        let action: crate::proxy::message_action::Metadata = serde_json::from_str(
            r#"{"version":1,"decision":"reply","text_frames":true,"close_downstream":true}"#,
        )
        .unwrap();
        assert!(action.text_frames);
        assert!(action.close_downstream);
        let messages: [&[u8]; 2] = [
            br#"{"type":"error","status":409}"#,
            br#"{"type":"response.failed"}"#,
        ];
        let mut payload = Vec::new();
        let mut expected = Vec::new();
        for message in messages {
            payload.extend_from_slice(&(message.len() as u32).to_le_bytes());
            payload.extend_from_slice(message);
            expected.extend_from_slice(&websocket::encode_server(1, message).unwrap());
        }
        assert_eq!(opaque_text_frames(&payload).unwrap().as_ref(), expected);
        assert!(opaque_text_frames(&[]).is_err());
        assert!(opaque_text_frames(&[0, 0, 0, 0]).is_err());
        assert!(opaque_text_frames(&[5, 0, 0, 0, 1]).is_err());
    }
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

    #[tokio::test]
    async fn scheduled_response_hold_excludes_controls_and_clears_with_explicit_actions() {
        let registration = message_codec::CodecRegistration {
            codec: Arc::new(proxy_pingora::http_ingress::message_codec::WebSocketCodec),
            callbacks: vec![],
        };
        let mut delta = held_test_message();
        delta.message = Some(proxy_protocols::message_codec::Message {
            kind: "response.event",
            payload: Bytes::from_static(br#"{"delta":"bypassed"}"#),
        });
        let mut sequence = 0;
        let mut plan = evaluate(&registration, &delta, &mut sequence, 1, 1024, None)
            .await
            .unwrap();
        let mut schedule = ResponseHold::default();
        plan.hold_response_at_unix_ms =
            Some(super::super::message_action::unix_ms().unwrap() + 100);
        schedule.apply(&plan).unwrap();
        plan.hold_response_at_unix_ms = None;
        assert!(!schedule.buffers(&delta, &plan));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(schedule.buffers(&delta, &plan));
        let terminal = Message {
            side: Side::Upstream(1),
            message: Some(proxy_protocols::message_codec::Message {
                kind: "response.completed",
                payload: Bytes::from_static(b"{}"),
            }),
            wire: vec![],
        };
        assert!(
            !schedule.buffers(&terminal, &plan),
            "an allowed terminal must not wait for another callback"
        );
        assert!(validate_terminal_hold(&terminal, &plan, false).is_ok());
        assert!(validate_terminal_hold(&terminal, &plan, true).is_err());
        assert!(
            !schedule.buffers(&held_test_message(), &plan),
            "control wire must progress"
        );
        let request = Message {
            side: Side::Downstream,
            message: delta.message.as_ref().map(|message| {
                proxy_protocols::message_codec::Message {
                    kind: message.kind,
                    payload: message.payload.clone(),
                }
            }),
            wire: delta.wire.clone(),
        };
        assert!(!schedule.buffers(&request, &plan));
        for decision in [Decision::Reply, Decision::Drop] {
            plan.decision = decision;
            assert!(!schedule.buffers(&delta, &plan));
        }
        plan.decision = Decision::Forward;
        for hold in ["release", "discard"] {
            schedule.deadline = Some(tokio::time::Instant::now());
            plan.hold = hold.into();
            assert!(!schedule.buffers(&delta, &plan));
            schedule.apply(&plan).unwrap();
            assert!(schedule.deadline.is_none());
        }
        plan.hold.clear();
        for retirement in [Upstream::Replace, Upstream::Disconnect, Upstream::Close] {
            schedule.deadline = Some(tokio::time::Instant::now());
            plan.upstream = retirement;
            schedule.apply(&plan).unwrap();
            assert!(
                schedule.deadline.is_none(),
                "new generations cannot inherit the deadline"
            );
        }
        plan.upstream = Upstream::Keep;
        schedule.deadline = Some(tokio::time::Instant::now());
        plan.hold_response_at_unix_ms = Some(0);
        schedule.apply(&plan).unwrap();
        assert!(schedule.deadline.is_none());
    }
}
