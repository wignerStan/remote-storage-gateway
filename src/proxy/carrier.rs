//! Native Pingora callbacks drive the ordered worker lifecycle.
use super::*;

#[async_trait]
impl ProxyHttp for NativeProxy {
    type CTX = ProxyContext;
    fn upstream_upgrade_keepalive(
        &self,
        session: &Session,
        _ctx: &Self::CTX,
    ) -> Result<Option<Box<dyn pingora_proxy::UpgradedKeepalive>>> {
        self.config.websocket_keepalive.build(session, true)
    }
    fn downstream_upgrade_keepalive(
        &self,
        session: &Session,
        _ctx: &Self::CTX,
    ) -> Result<Option<Box<dyn pingora_proxy::UpgradedKeepalive>>> {
        self.config.websocket_keepalive.build(session, false)
    }

    fn new_ctx(&self) -> Self::CTX {
        let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        ProxyContext {
            exchange: None,
            request_id,
            span: tracing::info_span!(target: "proxy_pingora::request", "http_request", request_id),
            started: Instant::now(),
            reuse: false,
            request_started: false,
            peer_selected: false,
            connected: false,
            upstream_request: None,
            inspection_request: None,
            request_has_body: false,
            request_body: ChunkChain::default(),
            response_body: ChunkChain::default(),
            response_variable: false,
            response_framing: None,
            upgraded: false,
            message_selection: None,
            message_decoder: None,
            message_sequence: 0,
            response_sequence: 0,
            response_decoder: None,
            response_message_complete: false,
            request_permit: None,
            inspection_permit: None,
            message_deadline: None,
            response_deadline: None,
            message_failed: false,
            message_http_complete: false,
            response_received: false,
            logged: false,
            dump: JsonBodyCapture::new(self.body_dump_limit),
        }
    }
    fn request_body_deadline(&self, ctx: &Self::CTX) -> Option<tokio::time::Instant> {
        ctx.message_deadline
    }
    fn response_body_deadline(&self, ctx: &Self::CTX) -> Option<tokio::time::Instant> {
        ctx.response_deadline
    }
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        if std::mem::replace(&mut ctx.request_started, true) {
            return Err(refusal(500, "repeated request admission"));
        }
        if !self.control.accepting() {
            return Err(refusal(503, "worker admission closed"));
        }
        ctx.request_permit = Some(self.inspection.request()?);
        headers::validate_ingress(session.req_header())?;
        // Preserve the parsed ingress head: Pingora's body decoder and HEAD /
        // upgrade semantics depend on it. Only outbound metadata is cloned.
        let mut request = session.req_header().clone();
        ctx.request_has_body = !session.is_body_empty();
        let facts = RequestFacts {
            request_id: ctx.request_id,
            method: session.req_header().method.to_string(),
            path: session.req_header().uri.path().to_owned(),
            status: None,
            upstream_reused: false,
            completed: false,
            headers: http::HeaderMap::new(),
            request_body: ChunkChain::default(),
            response_body: ChunkChain::default(),
        };
        let prepare = async {
            let mut exchange = self
                .lifecycle
                .begin(ENDPOINT, &mut request, facts)
                .await
                .map_err(|_| refusal(403, "request denied by installed notifier"))?;
            let (request_body, preserves_length) =
                body::take_plan(&mut exchange.facts_mut().request_body)?;
            let variable = !preserves_length;
            if request.headers.contains_key("upgrade") && !request_body.is_empty() {
                return Err(refusal(
                    400,
                    "HTTP body transforms cannot process an upgrade",
                ));
            }
            headers::request_head(
                &mut request,
                session.req_header(),
                &self.config.upstream,
                self.config.reuse,
                variable,
                ctx.request_has_body,
            )?;
            exchange
                .before_upstream_send(&mut request)
                .await
                .map_err(lifecycle_error)?;
            // Final policy also checks compiled callbacks: an origin route and
            // body framing cannot be invalidated after configuration is sealed.
            if request.method != session.req_header().method
                || request.version != session.req_header().version
            {
                return Err(refusal(
                    400,
                    "callbacks cannot change carrier method or protocol",
                ));
            }
            headers::validate_request_handshake(&request, session.req_header())?;
            headers::request_head(
                &mut request,
                session.req_header(),
                &self.config.upstream,
                self.config.reuse,
                variable,
                ctx.request_has_body,
            )?;
            body::ensure_closed(&exchange.facts().request_body)?;
            snapshot_request(&request, exchange.facts_mut());
            Ok((exchange, request, request_body))
        };
        let (exchange, mut request, request_body) =
            tokio::time::timeout(self.config.io_timeout, prepare)
                .instrument(ctx.span.clone())
                .await
                .map_err(|_| refusal(504, "request callback deadline exceeded"))??;
        if !self.control.accepting() {
            return Err(refusal(503, "worker admission closed during callbacks"));
        }
        ctx.message_selection = message_codec::select_request(&self.message_codecs, &request)?;
        if let Some((index, _)) = ctx.message_selection {
            ctx.inspection_permit = Some(self.inspection.buffers()?);
            if self.message_codecs[index].response_enabled() {
                request.insert_header("accept-encoding", "identity")?;
            }
            // Callback handshake integrity has already been checked. Only the
            // selected protocol owner may now narrow optional negotiation.
            self.message_codecs[index]
                .codec
                .prepare_request(&mut request)?;
        }
        if let Some((index, message_codec::Transport::HttpBody)) = ctx.message_selection {
            if !ctx.request_has_body {
                return Err(refusal(
                    400,
                    "protocol message requires an HTTP request body",
                ));
            }
            ctx.message_decoder = Some(
                self.message_codecs[index]
                    .codec
                    .create_http_with_limits(&request, self.inspection.limits.max_message_bytes)?,
            );
            ctx.message_deadline = Some(self.inspection.deadline());
        }
        if ctx
            .message_selection
            .is_some_and(|(index, _)| self.message_codecs[index].response_enabled())
        {
            ctx.inspection_request = Some(request.clone());
        }
        ctx.upstream_request = Some(request);
        ctx.request_body = request_body;
        ctx.exchange = Some(exchange);
        Ok(false)
    }
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        if ctx.peer_selected {
            return Err(refusal(502, "upstream retries are disabled"));
        }
        ctx.peer_selected = true;
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let dial = self.config.proxy.as_ref().unwrap_or(&self.config.upstream);
        let request = DialRequest {
            target: DialTarget::Host {
                name: dial.host_str().unwrap_or_default().into(),
                port: dial.port_or_known_default().unwrap_or(1080),
            },
            timeout: self.config.connect_timeout,
            tcp_nodelay: true,
        };
        let span = ctx.span.clone();
        let selected = tokio::time::timeout_at(deadline, ctx.exchange()?.acquire(&request))
            .instrument(span)
            .await
            .map_err(|_| refusal(504, "upstream control deadline exceeded"))?
            .map_err(control_error)?;
        // Virtual TLS-to-proxy streams cannot satisfy native idle-pool validation.
        let poolable = self
            .config
            .proxy
            .as_ref()
            .is_none_or(|proxy| proxy.scheme() != "https");
        ctx.reuse = self.config.reuse && selected == PoolDecision::Reuse && poolable;
        // The installation key is never logged. Bypass adds a request-private
        // scope, without rehashing fixed URLs, and sets zero idle time below.
        let use_http2 =
            self.config.http2.enabled && !session.req_header().headers.contains_key("upgrade");
        let key = if ctx.reuse {
            self.pool_key
        } else {
            let mut key = std::collections::hash_map::DefaultHasher::new();
            self.pool_key.hash(&mut key);
            ctx.request_id.hash(&mut key);
            key.finish()
        };
        // Native HttpPeer hashing omits ALPN. Explicitly separate H1 upgrade
        // peers from H2-capable peers, including H1 fallback connections.
        let mut protocol_key = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut protocol_key);
        use_http2.hash(&mut protocol_key);
        let key = protocol_key.finish();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let mut peer = tokio::time::timeout_at(
            deadline,
            self.egress.peer(
                &self.config.upstream,
                self.config.proxy.as_ref(),
                key,
                remaining,
            ),
        )
        .await
        .map_err(|_| refusal(504, "upstream resolution deadline exceeded"))??;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(refusal(504, "upstream acquisition deadline exceeded"));
        }
        // HTTP/1 upgrades cannot travel on an HTTP/2 stream.
        if use_http2 {
            peer.options.set_http_version(
                2,
                if self.config.upstream.scheme() == "https" {
                    1
                } else {
                    2
                },
            );
            peer.options.h2_ping_interval = self.config.http2.ping_interval();
        }
        peer.options.connection_timeout = Some(remaining);
        peer.options.total_connection_timeout = Some(remaining);
        peer.options.read_timeout = Some(self.config.io_timeout);
        peer.options.write_timeout = Some(self.config.io_timeout);
        peer.options.idle_timeout = Some(if ctx.reuse {
            self.config.pool_idle_timeout
        } else {
            Duration::ZERO
        });
        Ok(peer)
    }
    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _socket: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.exchange()?.facts_mut().upstream_reused = reused;
        let span = ctx.span.clone();
        tokio::time::timeout(self.config.io_timeout, ctx.exchange()?.connected())
            .instrument(span)
            .await
            .map_err(|_| refusal(504, "socket control deadline exceeded"))?
            .map_err(control_error)?;
        ctx.connected = true;
        Ok(())
    }
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if !ctx.connected {
            return Err(refusal(500, "upstream socket has not been authorized"));
        }
        let version = request.version;
        *request = ctx
            .upstream_request
            .take()
            .ok_or_else(|| refusal(500, "outbound header already consumed"))?;
        headers::wire_request(request, version, ctx.reuse, ctx.request_has_body)
    }
    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if ctx.message_failed {
            ctx.exchange()?.fail();
            return Err(refusal(502, "message inspection is terminal"));
        }
        if ctx.upgraded && !session.was_upgraded() {
            return Ok(());
        }
        if !ctx.upgraded {
            ctx.dump
                .observe(body.as_deref().unwrap_or_default(), end, ctx.request_id);
            if !ctx.request_body.is_empty() {
                if let Err(error) = ctx.request_body.filter_bounded(
                    body,
                    end,
                    self.inspection.limits.max_message_bytes.max(64 * 1024),
                ) {
                    ctx.exchange()?.fail();
                    return Err(lifecycle_error(error));
                }
            }
        }
        if ctx.message_decoder.is_some()
            && ctx.inspection_permit.is_none()
            && body.as_ref().is_some_and(|bytes| !bytes.is_empty())
        {
            ctx.inspection_permit = Some(self.inspection.buffers()?);
        }
        if let Some(decoder) = &mut ctx.message_decoder {
            if ctx.message_deadline.is_none() && body.as_ref().is_some_and(|b| !b.is_empty()) {
                ctx.message_deadline = Some(self.inspection.deadline());
            }
            if ctx
                .message_deadline
                .is_some_and(|d| tokio::time::Instant::now() >= d)
            {
                ctx.message_failed = true;
                ctx.exchange()?.fail();
                return Err(refusal(408, "protocol message assembly deadline exceeded"));
            }
            let (index, _) = ctx.message_selection.expect("selected message decoder");
            let registration = &self.message_codecs[index];
            // Cancellation poisons both HTTP and upgraded message admission.
            ctx.message_failed = true;
            let io_deadline = tokio::time::Instant::now() + self.config.io_timeout;
            let deadline = ctx
                .message_deadline
                .map_or(io_deadline, |d| d.min(io_deadline));
            let result = tokio::time::timeout_at(
                deadline,
                message_codec::filter_owned(
                    &registration.callbacks,
                    decoder.as_mut(),
                    registration.codec.id(),
                    &mut ctx.message_sequence,
                    ctx.request_id,
                    body,
                    end,
                    ctx.inspection_permit.clone(),
                    false,
                    self.inspection.limits.max_message_bytes,
                    (ctx.upgraded && registration.codec.supports_message_replies())
                        .then_some(registration.codec.as_ref()),
                ),
            )
            .instrument(ctx.span.clone())
            .await
            .map_err(|_| refusal(504, "message callback deadline exceeded"))
            .and_then(|result| result);
            if let Ok(Some(frame)) = &result {
                // The upstream direction is frame-gated for reply-capable codecs,
                // so a local message cannot bisect a partially forwarded frame.
                let sent = tokio::time::timeout_at(
                    deadline,
                    session.write_response_body(Some(frame.clone()), false),
                )
                .await
                .map_err(|_| refusal(504, "protocol reply write deadline exceeded"))
                .and_then(|r| r);
                if let Err(error) = sent {
                    ctx.exchange()?.fail();
                    return Err(error);
                }
            }
            if result.is_err() {
                ctx.exchange()?.fail();
            } else {
                ctx.message_failed = false;
                if !decoder.has_pending() {
                    ctx.message_deadline = None;
                    if ctx.upgraded
                        && ctx
                            .response_decoder
                            .as_ref()
                            .is_none_or(|d| !d.has_pending())
                    {
                        ctx.inspection_permit = None;
                    }
                }
                if end
                    && ctx.message_selection.is_some_and(|(_, transport)| {
                        transport == message_codec::Transport::HttpBody
                    })
                {
                    ctx.message_http_complete = true;
                }
            }
            return result.map(|_| ());
        }
        Ok(())
    }
    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if response.status.is_informational() && response.status.as_u16() != 101 {
            return Ok(());
        }
        let incomplete_http_message = ctx
            .message_selection
            .is_some_and(|(_, transport)| transport == message_codec::Transport::HttpBody)
            && !ctx.message_http_complete;
        if incomplete_http_message {
            ctx.message_failed = true;
            ctx.exchange()?.fail();
            if response.status.as_u16() < 400 {
                return Err(refusal(
                    502,
                    "upstream response preceded protocol message admission",
                ));
            }
            // An origin may reject a request from its headers. Relay its error
            // with native framing, while the poisoned exchange skips callbacks
            // and can never acknowledge an uninspected request as complete.
            headers::validate_upstream_upgrade(session.req_header(), response)?;
            headers::validate_transfer_coding(&response.headers, 502)?;
            ctx.response_framing = Some(headers::ResponseFraming::capture(response));
            ctx.exchange()?.facts_mut().status = Some(response.status.as_u16());
            ctx.response_received = true;
            ctx.reuse = false;
            session.set_keepalive(None);
            return Ok(());
        }
        headers::validate_upstream_upgrade(session.req_header(), response)?;
        if let Err(error) =
            message_codec::validate_selection(self.selected_upgrade_codec(ctx), response)
        {
            ctx.exchange()?.fail();
            return Err(error);
        }
        headers::validate_transfer_coding(&response.headers, 502)?;
        let original = headers::ResponseFraming::capture(response);
        ctx.exchange()?.facts_mut().status = Some(response.status.as_u16());
        let span = ctx.span.clone();
        tokio::time::timeout(
            self.config.io_timeout,
            ctx.exchange()?.upstream_response(response),
        )
        .instrument(span)
        .await
        .map_err(|_| refusal(504, "response callback deadline exceeded"))?
        .map_err(lifecycle_error)?;
        body::ensure_closed(&ctx.exchange()?.facts().request_body)?;
        original.validate(response, &session.req_header().method)?;
        if response.status.as_u16() == 101
            && (!ctx.exchange()?.facts().response_body.is_empty() || !ctx.request_body.is_empty())
        {
            ctx.exchange()?.fail();
            return Err(refusal(
                502,
                "HTTP body transforms cannot process an upgrade",
            ));
        }
        let (response_body, preserves_length) =
            body::take_plan(&mut ctx.exchange()?.facts_mut().response_body)?;
        ctx.response_body = response_body;
        ctx.response_variable = !preserves_length;
        ctx.upgraded = response.status.as_u16() == 101;
        if ctx
            .message_selection
            .is_some_and(|(_, transport)| transport == message_codec::Transport::Upgrade)
        {
            let selection =
                message_codec::validate_selection(self.selected_upgrade_codec(ctx), response)
                    .and_then(|selected| {
                        if selected {
                            self.selected_upgrade_codec(ctx)
                                .expect("selected upgrade codec")
                                .create_with_limits(self.inspection.limits.max_message_bytes)
                                .map(Some)
                        } else {
                            Ok(None)
                        }
                    });
            match selection {
                Ok(decoder) => ctx.message_decoder = decoder,
                Err(error) => {
                    ctx.exchange()?.fail();
                    return Err(error);
                }
            }
        }
        if ctx.upgraded {
            ctx.dump.finish(ctx.request_id);
            ctx.inspection_permit = None;
        }
        ctx.response_framing = Some(original);
        ctx.exchange()?.facts_mut().status = Some(response.status.as_u16());
        ctx.response_received = true;
        Ok(())
    }
    async fn response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if response.status.is_informational() && response.status.as_u16() != 101 {
            return Ok(());
        }
        // Engine-generated failure responses have no admitted upstream response.
        if !ctx.response_received {
            return Ok(());
        }
        headers::response_head(
            response,
            ctx.response_framing
                .as_ref()
                .ok_or_else(|| refusal(500, "missing response framing"))?,
            &session.req_header().method,
            session.req_header().version,
            ctx.response_variable,
        )?;
        if ctx.message_failed
            && !ctx.message_http_complete
            && ctx
                .message_selection
                .is_some_and(|(_, transport)| transport == message_codec::Transport::HttpBody)
        {
            // The early origin rejection was already framed above. Its failed
            // exchange has no remaining metadata or completion authority.
            session.set_keepalive(None);
            self.configure_response_inspection(session, response, ctx)?;
            return Ok(());
        }
        let span = ctx.span.clone();
        tokio::time::timeout(
            self.config.io_timeout,
            ctx.exchange()?.before_downstream_write(response),
        )
        .instrument(span)
        .await
        .map_err(|_| refusal(504, "downstream callback deadline exceeded"))?
        .map_err(lifecycle_error)?;
        body::ensure_closed(&ctx.exchange()?.facts().request_body)?;
        body::ensure_closed(&ctx.exchange()?.facts().response_body)?;
        if let Err(error) =
            message_codec::validate_selection(self.selected_upgrade_codec(ctx), response)
        {
            ctx.exchange()?.fail();
            return Err(error);
        }
        ctx.exchange()?.facts_mut().status = Some(response.status.as_u16());
        self.configure_response_inspection(session, response, ctx)?;
        let close_delimited = headers::response_head(
            response,
            ctx.response_framing
                .as_ref()
                .ok_or_else(|| refusal(500, "missing response framing"))?,
            &session.req_header().method,
            session.req_header().version,
            ctx.response_variable,
        )?;
        if close_delimited {
            // Pingora stamps Connection before initializing its body writer.
            // Disable reuse now so an HTTP/1.0 EOF body also advertises close.
            session.set_keepalive(None);
        }
        Ok(())
    }
    async fn async_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        if !ctx.response_received {
            return Ok(None);
        }
        if !ctx.response_body.is_empty() {
            if let Err(error) = ctx.response_body.filter_bounded(
                body,
                end,
                self.inspection.limits.max_message_bytes.max(64 * 1024),
            ) {
                ctx.exchange()?.fail();
                return Err(lifecycle_error(error));
            }
        }
        if ctx.response_decoder.is_some()
            && ctx.inspection_permit.is_none()
            && body.as_ref().is_some_and(|bytes| !bytes.is_empty())
        {
            ctx.inspection_permit = Some(self.inspection.buffers()?);
        }
        if let Some(decoder) = &mut ctx.response_decoder {
            if ctx.response_deadline.is_none() && body.as_ref().is_some_and(|b| !b.is_empty()) {
                ctx.response_deadline = Some(self.inspection.deadline());
            }
            if ctx
                .response_deadline
                .is_some_and(|d| tokio::time::Instant::now() >= d)
            {
                ctx.message_failed = true;
                ctx.exchange()?.fail();
                return Err(refusal(504, "response message assembly deadline exceeded"));
            }
            let (index, _) = ctx.message_selection.expect("response codec selection");
            let registration = &self.message_codecs[index];
            let previously_failed = ctx.message_failed;
            ctx.message_failed = true;
            let io_deadline = tokio::time::Instant::now() + self.config.io_timeout;
            let deadline = ctx
                .response_deadline
                .map_or(io_deadline, |d| d.min(io_deadline));
            let result = tokio::time::timeout_at(
                deadline,
                message_codec::filter_owned(
                    &registration.callbacks,
                    decoder.as_mut(),
                    registration.codec.id(),
                    &mut ctx.response_sequence,
                    ctx.request_id,
                    body,
                    end,
                    ctx.inspection_permit.clone(),
                    true,
                    self.inspection.limits.max_message_bytes,
                    None,
                ),
            )
            .await
            .map_err(|_| refusal(504, "response callback deadline exceeded"))
            .and_then(|r| r);
            if let Err(error) = result {
                ctx.exchange()?.fail();
                return Err(error);
            }
            ctx.message_failed = previously_failed;
            if !decoder.has_pending() {
                ctx.response_deadline = None;
                if ctx
                    .message_decoder
                    .as_ref()
                    .is_none_or(|d| !d.has_pending())
                {
                    ctx.inspection_permit = None;
                }
            }
            if end {
                ctx.response_message_complete = true;
                ctx.response_deadline = None;
            }
        }
        Ok(None)
    }
    async fn logging(&self, session: &mut Session, error: Option<&Error>, ctx: &mut Self::CTX) {
        if ctx.logged {
            return;
        }
        ctx.dump.finish(ctx.request_id);
        let status = session.response_written().map(|head| head.status.as_u16());
        let mut completed = false;
        if ctx.message_failed || (ctx.response_decoder.is_some() && !ctx.response_message_complete)
        {
            ctx.message_failed = true;
            if let Some(exchange) = &mut ctx.exchange {
                exchange.fail();
            }
        }
        if error.is_none()
            && !ctx.message_failed
            && let Some(mut exchange) = ctx.exchange.take()
        {
            exchange.facts_mut().completed = true;
            completed = matches!(
                tokio::time::timeout(self.config.io_timeout, exchange.finish())
                    .instrument(ctx.span.clone())
                    .await,
                Ok(Ok(_))
            );
        }
        tracing::info!(target: "proxy_pingora::access", request_id = ctx.request_id, status, completed,
            elapsed_ms = ctx.started.elapsed().as_millis() as u64,
            upstream_reuse_allowed = ctx.reuse, outcome = if error.is_some() || ctx.message_failed { "failed" } else if completed { "delivered" } else { "completion_failed" },
            "proxy request completed");
        ctx.logged = true;
    }
    fn error_while_proxy(
        &self,
        _peer: &HttpPeer,
        _session: &mut Session,
        mut error: Box<Error>,
        ctx: &mut Self::CTX,
        _reused: bool,
    ) -> Box<Error> {
        if let Some(exchange) = &mut ctx.exchange {
            exchange.fail();
        }
        error.retry = false.into();
        error
    }
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        ctx: &mut Self::CTX,
        mut error: Box<Error>,
    ) -> Box<Error> {
        if let Some(exchange) = &mut ctx.exchange {
            exchange.fail();
        }
        error.retry = false.into();
        error
    }
    fn request_summary(&self, _session: &Session, ctx: &Self::CTX) -> String {
        format!("request_id={}", ctx.request_id)
    }
    fn suppress_error_log(&self, _session: &Session, _ctx: &Self::CTX, _error: &Error) -> bool {
        // The structured access outcome above avoids dumping arbitrary peer/header errors.
        true
    }
}

impl NativeProxy {
    fn configure_response_inspection(
        &self,
        session: &Session,
        response: &ResponseHeader,
        ctx: &mut ProxyContext,
    ) -> Result<()> {
        if let Some((index, _)) = ctx.message_selection {
            let registration = &self.message_codecs[index];
            if ctx.upgraded
                && !registration.response_enabled()
                && registration.codec.supports_message_replies()
            {
                use proxy_pingora::http_ingress::message_codec::CodecPlugin;
                ctx.response_decoder = proxy_pingora::http_ingress::message_codec::WebSocketCodec
                    .create_upgrade_response_with_limits(
                    self.inspection.limits.max_message_bytes,
                )?;
                ctx.inspection_permit = None;
                return Ok(());
            }
            if registration.response_enabled() && session.req_header().method != http::Method::HEAD
            {
                if ctx.upgraded {
                    ctx.response_decoder = registration.codec.create_upgrade_response_with_limits(
                        self.inspection.limits.max_message_bytes,
                    )?;
                    if registration.codec.supports_message_replies()
                        && ctx.response_decoder.is_none()
                    {
                        return Err(refusal(
                            502,
                            "reply-capable codec requires upstream frame inspection",
                        ));
                    }
                    ctx.inspection_permit = None; // Idle upgraded connections retain no assembly buffers.
                    return Ok(());
                }
                ctx.response_decoder = registration.codec.create_http_response_with_limits(
                    ctx.inspection_request
                        .as_ref()
                        .unwrap_or_else(|| session.req_header()),
                    response,
                    self.inspection.limits.max_message_bytes,
                )?;
                if ctx
                    .response_decoder
                    .as_ref()
                    .is_some_and(|d| d.has_pending())
                {
                    ctx.response_deadline = Some(self.inspection.deadline());
                } else if ctx
                    .message_decoder
                    .as_ref()
                    .is_none_or(|d| !d.has_pending())
                {
                    ctx.inspection_permit = None;
                }
            }
        }
        Ok(())
    }
}
