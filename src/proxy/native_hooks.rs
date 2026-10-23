//! One explicit native Pingora hook layered after the managed worker boundaries.
use super::*;

/// Native engine adapter preserving the worker's request and response lifecycle.
/// Only the installed handler's upgraded `request_body_filter` is exposed.
pub struct NativeHookProxy<P> {
    worker: NativeProxy,
    handler: Arc<P>,
}
impl<P> Clone for NativeHookProxy<P> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            handler: Arc::clone(&self.handler),
        }
    }
}
impl<P> NativeHookProxy<P> {
    pub(super) fn new(worker: NativeProxy, handler: Arc<P>) -> Self {
        Self { worker, handler }
    }
}

/// Distinct worker and native-handler state for one native engine request.
/// The handler state is constructed only when its granted hook first runs.
pub struct NativeHookContext<C> {
    worker: ProxyContext,
    native: Option<C>,
    native_failed: bool,
}
fn active<C>(ctx: &NativeHookContext<C>) -> Result<()> {
    if ctx.native_failed {
        Err(refusal(502, "native upgraded request hook is terminal"))
    } else {
        Ok(())
    }
}

#[async_trait]
impl<P> ProxyHttp for NativeHookProxy<P>
where
    P: ProxyHttp + Send + Sync + 'static,
    P::CTX: Send + Sync,
{
    type CTX = NativeHookContext<P::CTX>;
    fn upstream_upgrade_keepalive(
        &self,
        session: &Session,
        ctx: &Self::CTX,
    ) -> Result<Option<Box<dyn pingora_proxy::UpgradedKeepalive>>> {
        self.worker.upstream_upgrade_keepalive(session, &ctx.worker)
    }
    fn downstream_upgrade_keepalive(
        &self,
        session: &Session,
        ctx: &Self::CTX,
    ) -> Result<Option<Box<dyn pingora_proxy::UpgradedKeepalive>>> {
        self.worker
            .downstream_upgrade_keepalive(session, &ctx.worker)
    }
    fn new_ctx(&self) -> Self::CTX {
        NativeHookContext {
            worker: self.worker.new_ctx(),
            native: None,
            native_failed: false,
        }
    }
    fn request_body_deadline(&self, ctx: &Self::CTX) -> Option<tokio::time::Instant> {
        self.worker.request_body_deadline(&ctx.worker)
    }
    fn response_body_deadline(&self, ctx: &Self::CTX) -> Option<tokio::time::Instant> {
        self.worker.response_body_deadline(&ctx.worker)
    }
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        active(ctx)?;
        self.worker.request_filter(session, &mut ctx.worker).await
    }
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        active(ctx)?;
        self.worker.upstream_peer(session, &mut ctx.worker).await
    }
    async fn connected_to_upstream(
        &self,
        session: &mut Session,
        reused: bool,
        peer: &HttpPeer,
        #[cfg(unix)] fd: std::os::unix::io::RawFd,
        #[cfg(windows)] socket: std::os::windows::io::RawSocket,
        digest: Option<&Digest>,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        active(ctx)?;
        self.worker
            .connected_to_upstream(
                session,
                reused,
                peer,
                #[cfg(unix)]
                fd,
                #[cfg(windows)]
                socket,
                digest,
                &mut ctx.worker,
            )
            .await
    }
    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        active(ctx)?;
        self.worker
            .upstream_request_filter(session, request, &mut ctx.worker)
            .await
    }
    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        active(ctx)?;
        // The admitted 101 is known before downstream publication. Require the
        // native session's completed switch too, excluding ordinary HTTP EOF.
        if !ctx.worker.upgraded || !session.was_upgraded() {
            return self
                .worker
                .request_body_filter(session, body, end, &mut ctx.worker)
                .await;
        }
        // Apply configured protocol inspection before exposing approved wire
        // to this explicitly granted raw native hook.
        self.worker
            .request_body_filter(session, body, end, &mut ctx.worker)
            .await?;
        if body.as_ref().is_some_and(bytes::Bytes::is_empty) && !end {
            return Ok(());
        }
        // Set before invoking eager handler code or polling its future. Dropping
        // this future leaves the context terminal and forbids later completion.
        ctx.native_failed = true;
        let native = ctx.native.get_or_insert_with(|| self.handler.new_ctx());
        let result = tokio::time::timeout(
            self.worker.config.io_timeout,
            self.handler.request_body_filter(session, body, end, native),
        )
        .instrument(ctx.worker.span.clone())
        .await;
        match result {
            Ok(Ok(())) => {
                ctx.native_failed = false;
                Ok(())
            }
            Ok(Err(error)) => {
                ctx.worker.exchange()?.fail();
                Err(error)
            }
            Err(_) => {
                ctx.worker.exchange()?.fail();
                Err(refusal(
                    504,
                    "native upgraded request hook deadline exceeded",
                ))
            }
        }
    }
    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        active(ctx)?;
        self.worker
            .upstream_response_filter(session, response, &mut ctx.worker)
            .await
    }
    async fn response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        active(ctx)?;
        self.worker
            .response_filter(session, response, &mut ctx.worker)
            .await
    }
    async fn async_response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        active(ctx)?;
        self.worker
            .async_response_body_filter(session, body, end, &mut ctx.worker)
            .await
    }
    async fn logging(&self, session: &mut Session, error: Option<&Error>, ctx: &mut Self::CTX) {
        if ctx.native_failed {
            if let Some(exchange) = &mut ctx.worker.exchange {
                exchange.fail();
            }
            let native_error = refusal(502, "native upgraded request hook failed or was cancelled");
            self.worker
                .logging(
                    session,
                    error.or(Some(native_error.as_ref())),
                    &mut ctx.worker,
                )
                .await;
        } else {
            self.worker.logging(session, error, &mut ctx.worker).await;
        }
    }
    fn error_while_proxy(
        &self,
        peer: &HttpPeer,
        session: &mut Session,
        error: Box<Error>,
        ctx: &mut Self::CTX,
        reused: bool,
    ) -> Box<Error> {
        self.worker
            .error_while_proxy(peer, session, error, &mut ctx.worker, reused)
    }
    fn fail_to_connect(
        &self,
        session: &mut Session,
        peer: &HttpPeer,
        ctx: &mut Self::CTX,
        error: Box<Error>,
    ) -> Box<Error> {
        self.worker
            .fail_to_connect(session, peer, &mut ctx.worker, error)
    }
    fn request_summary(&self, session: &Session, ctx: &Self::CTX) -> String {
        self.worker.request_summary(session, &ctx.worker)
    }
    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        error: &Error,
        ctx: &mut Self::CTX,
    ) -> pingora_proxy::FailToProxy {
        self.worker
            .fail_to_proxy(session, error, &mut ctx.worker)
            .await
    }
    fn suppress_error_log(&self, session: &Session, ctx: &Self::CTX, error: &Error) -> bool {
        self.worker.suppress_error_log(session, &ctx.worker, error)
    }
}

#[async_trait]
impl<P> ProxyLifecycle for NativeHookProxy<P>
where
    P: ProxyHttp + Send + Sync + 'static,
    P::CTX: Send + Sync,
{
    async fn shutdown(&self) -> std::result::Result<(), String> {
        self.worker.shutdown().await
    }
    async fn shutdown_requested(&self) {
        self.worker.shutdown_requested().await;
    }
    async fn maintain(&self) {
        self.worker.maintain().await;
    }
}
