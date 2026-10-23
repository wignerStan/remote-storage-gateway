//! Provider-supplied physical lifetime, separate from response hold and heartbeat.
use super::super::message_action::SocketLifetime;
use super::*;
use tokio::time::Instant as TokioInstant;

#[derive(Default)]
pub(super) struct GenerationLifetime {
    policy: Option<SocketLifetime>,
    retain: bool,
    born: Option<TokioInstant>,
    pub active: bool,
    retired: bool,
    slot: Option<super::super::retained_sessions::GenerationPermit>,
}
impl GenerationLifetime {
    pub fn apply(&mut self, plan: &Plan, transport: &mut carrier::Session<'_>) -> Result<()> {
        if let Some(policy) = plan.socket_lifetime {
            if self.born.is_some() && self.policy != Some(policy) {
                return Err(refusal(502, "live physical socket lifetime cannot change"));
            }
            self.policy = Some(policy);
        }
        if let Some(retain) = plan.retain_upstream_on_downstream_close {
            if self.born.is_some() && self.retain != retain {
                return Err(refusal(502, "live physical socket retention cannot change"));
            }
            self.retain = retain;
        }
        if self.retain && self.policy.is_none() {
            return Err(refusal(
                502,
                "retained upstream requires a bounded physical lifetime",
            ));
        }
        transport.set_retain_upstream_on_downstream_close(self.retain);
        Ok(())
    }
    pub fn reserve(&mut self, owner: &NativeProxy) -> Result<()> {
        if self.policy.is_some() && self.slot.is_none() {
            self.slot = Some(
                owner
                    .retained_sessions
                    .as_ref()
                    .ok_or_else(|| refusal(503, "physical lifetime owner unavailable"))?
                    .reserve()?,
            );
        }
        Ok(())
    }
    pub fn connected(&mut self, transport: &carrier::Session<'_>) {
        self.born = transport.upstream_connected_at();
        self.retired = false;
    }
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    pub fn retire_at(&self) -> Option<TokioInstant> {
        if self.retired {
            None
        } else {
            self.born
                .zip(self.policy)
                .map(|(born, policy)| born + Duration::from_millis(policy.retire_after_ms))
        }
    }
    pub fn retirement_due(&self) -> bool {
        self.retired || self.retire_at().is_some_and(|at| TokioInstant::now() >= at)
    }
    pub fn retire(&mut self) {
        self.retired = true;
    }
    pub fn may_detach(&self) -> bool {
        self.retain && self.born.is_some() && self.slot.is_some()
    }
    pub fn take(&mut self, transport: &mut carrier::Session<'_>) -> Option<RetainedGeneration> {
        let policy = self.policy?;
        let born = self.born?;
        let socket = transport.take_upstream()?;
        let slot = self.slot.take()?;
        Some(RetainedGeneration {
            socket,
            _slot: slot,
            close_at: born + Duration::from_millis(policy.close_after_ms),
            active: self.active,
        })
    }
}

pub(super) struct RetainedGeneration {
    socket: carrier::OwnedUpstream,
    _slot: super::super::retained_sessions::GenerationPermit,
    close_at: TokioInstant,
    active: bool,
}

/// Capabilities needed to settle a detached active turn. No proxy/facility Arc.
pub(super) struct DetachedOwner {
    pub registration: message_codec::CodecRegistration,
    pub inspection: Arc<inspection::InspectionBudget>,
    pub reservation: Option<Arc<Reservation>>,
    pub request_id: u64,
    pub sequence: u64,
    pub exchange: SessionExchange<ProxyContract>,
    pub io_timeout: Duration,
}
impl RetainedGeneration {
    pub async fn run(
        mut self,
        mut owner: Option<DetachedOwner>,
        mut stop: tokio::sync::watch::Receiver<Option<TokioInstant>>,
        io_timeout: Duration,
        inspection: Arc<inspection::InspectionBudget>,
        initial_reservation: Option<Arc<Reservation>>,
    ) -> std::result::Result<(), String> {
        let mut reservation = initial_reservation
            .or_else(|| owner.as_ref().and_then(|owner| owner.reservation.clone()));
        let request_id = owner.as_ref().map(|owner| owner.request_id);
        let mut outcome = Ok(());
        let mut cleanup_result = Ok(());
        if !self.active {
            self.socket.set_read_idle_timeout(None);
        }
        loop {
            // Process shutdown overrides idle retention, while an active turn gets
            // the remaining shared process grace. Provider age never cuts a turn.
            let stopping = *stop.borrow();
            if (!self.active && (stopping.is_some() || TokioInstant::now() >= self.close_at))
                || stopping.is_some_and(|at| TokioInstant::now() >= at)
            {
                break;
            }
            if !self.active && owner.is_some() {
                cleanup_result =
                    cleanup_detached(owner.take().expect("detached owner"), stopping).await;
                if !self.socket.has_buffered_data() {
                    reservation = None;
                }
                if cleanup_result.is_err() {
                    break;
                }
            }
            let deadline = if self.active {
                stopping
            } else {
                Some(stopping.map_or(self.close_at, |at| at.min(self.close_at)))
            };
            let timer = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let next = self.socket.next_reserved(|| {
                if reservation.is_none() {
                    reservation = Some(inspection.buffers()?);
                }
                Ok(())
            });
            let event = tokio::select! {
                biased;
                _ = stop.changed() => continue,
                _ = timer => continue,
                event = next => match event { Ok(event) => event, Err(error) => { outcome = Err(error); break; } },
            };
            match event {
                Event::Maintenance(token) => match self.socket.maintain(token).await {
                    Ok(false) => {}
                    Ok(true) => break,
                    Err(error) => {
                        outcome = Err(error);
                        break;
                    }
                },
                Event::Closed(_) => break,
                Event::Message(message) => {
                    if let Some(owner) = &mut owner {
                        let deadline = stopping.map_or_else(
                            || owner.inspection.deadline(),
                            |at| at.min(owner.inspection.deadline()),
                        );
                        let pending = within_deadline(
                            deadline,
                            evaluate(
                                &owner.registration,
                                &message,
                                &mut owner.sequence,
                                owner.request_id,
                                owner.inspection.limits.max_message_bytes,
                                reservation.clone(),
                            ),
                            "detached response callback deadline exceeded",
                        );
                        let result = {
                            let driving = self.socket.drive_until(pending, || {
                                if reservation.is_none() {
                                    reservation = Some(inspection.buffers()?);
                                }
                                Ok(())
                            });
                            tokio::pin!(driving);
                            // A shutdown signal can arrive after the callback starts.
                            // Its absolute deadline bounds that already-running wait.
                            loop {
                                let stop_at = *stop.borrow();
                                let timer = async {
                                    match stop_at {
                                        Some(at) => tokio::time::sleep_until(at).await,
                                        None => std::future::pending().await,
                                    }
                                };
                                tokio::select! {
                                    result=&mut driving=>break result.and_then(|result|result),
                                    _=stop.changed()=>continue,
                                    _=timer=>break Err(refusal(504,"retained response exceeded process drain")),
                                }
                            }
                        };
                        match result {
                            Ok(plan) => {
                                if response_terminal(&message) {
                                    self.active = false;
                                    self.socket.set_read_idle_timeout(None);
                                }
                                if matches!(
                                    plan.upstream,
                                    Upstream::Disconnect
                                        | Upstream::Close
                                        | Upstream::Replace
                                        | Upstream::Connect
                                ) {
                                    break;
                                }
                                // The client is gone. Do not retain business wire or deliver a reply.
                            }
                            Err(error) => {
                                outcome = Err(error);
                                break;
                            }
                        }
                    }
                }
            }
            if !self.socket.has_buffered_data() {
                reservation = None;
            }
        }
        if let Some(owner) = owner.take() {
            let stopping = *stop.borrow();
            cleanup_result = cleanup_result.and(cleanup_detached(owner, stopping).await);
        }
        // A close write has its native bound. Shutdown shares the process deadline.
        let deadline = (*stop.borrow()).unwrap_or_else(|| TokioInstant::now() + io_timeout);
        let closing = tokio::time::timeout_at(deadline, self.socket.close()).await;
        outcome = outcome.and(match closing {
            Ok(result) => result,
            Err(_) => Err(refusal(504, "retained close exceeded drain deadline")),
        });
        if let Err(error) = &outcome {
            tracing::error!(target:"proxy_pingora::session",request_id,generation=self.socket.generation(),error=%error,"detached generation failed before retirement");
        }
        tracing::debug!(target: "proxy_pingora::session", request_id, generation=self.socket.generation(), failed=outcome.is_err(), "retained physical generation retired");
        cleanup_result.map_err(|error| error.to_string())
    }
}

async fn cleanup_detached(mut owner: DetachedOwner, stop: Option<TokioInstant>) -> Result<()> {
    let deadline = stop.map_or_else(
        || TokioInstant::now() + owner.io_timeout,
        |at| at.min(TokioInstant::now() + owner.io_timeout),
    );
    let method = format!(
        "{}/transport.closed/{:016x}/{:016x}",
        owner.registration.codec.id(),
        owner.request_id,
        owner.sequence
    );
    let result = notify_session_closed(
        &owner.registration,
        &method,
        Bytes::from_static(br#"{"side":"downstream","reason":"detached"}"#),
        owner.reservation.take(),
        owner.request_id,
        deadline,
    )
    .await;
    // Delivery ended at detach. A successful upstream terminal can settle billing,
    // but the local 101 must never become a successful business delivery.
    owner.exchange.fail();
    result
}
