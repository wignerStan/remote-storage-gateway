//! Application reverse-proxy policy over Pingora's native duplex HTTP engine.
use diagnostic_output::runtime::JsonBodyCapture;

use async_trait::async_trait;
use network_sockets::{DialRequest, DialTarget, SocketDecision, SocketPhase};
use pingora_core::{protocols::Digest, upstreams::peer::HttpPeer};
use pingora_error::{Error, ErrorType, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use plugin_lifecycle::Runtime;
use proxy_pingora::{
    http_egress::HttpEgress,
    http_ingress::ProxyLifecycle,
    worker::{
        CarrierExchange, HttpLifecycle, WorkerControl,
        surface::{
            Callbacks, Contract, DialDecision, Exposure, NotifierChain, PoolDecision, PoolPhase,
            Rejection,
        },
    },
};
use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::Instrument;
use url::Url;

const ENDPOINT: u8 = 1;
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

/// Fixed application route; request headers cannot choose another destination.
#[derive(Clone)]
pub struct ProxyConfig {
    pub upstream: Url,
    pub proxy: Option<Url>,
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub pool_key: u64,
    pub reuse: bool,
}
impl ProxyConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !matches!(self.upstream.scheme(), "http" | "https")
            || self.upstream.host_str().is_none()
            || !self.upstream.username().is_empty()
            || self.upstream.password().is_some()
            || self.upstream.query().is_some()
            || self.upstream.fragment().is_some()
            || self.upstream.path() != "/"
        {
            return Err(
                "upstream must be an HTTP(S) origin without credentials, path, query or fragment"
                    .into(),
            );
        }
        if self.proxy.as_ref().is_some_and(|proxy| {
            !matches!(proxy.scheme(), "http" | "https" | "socks5" | "socks5h")
                || proxy.host_str().is_none()
                || proxy.query().is_some()
                || proxy.fragment().is_some()
                || !matches!(proxy.path(), "" | "/")
        }) {
            return Err("unsupported selected upstream proxy".into());
        }
        if [
            self.connect_timeout,
            self.io_timeout,
            self.pool_idle_timeout,
        ]
        .iter()
        .any(|timeout| timeout.is_zero() || Instant::now().checked_add(*timeout).is_none())
        {
            return Err("proxy deadlines must be positive and representable".into());
        }
        Ok(())
    }
}

/// Metadata only. Native request/response bodies never enter notifier facts.
#[derive(serde::Serialize)]
pub struct RequestFacts {
    pub request_id: u64,
    pub method: String,
    pub path: String,
    pub status: Option<u16>,
    pub upstream_reused: bool,
    pub completed: bool,
    /// Final pre-routing headers retained for authentication and admission.
    /// Not serialized by default; ABI adapters select an explicit allowlist.
    #[serde(skip)]
    pub headers: http::HeaderMap,
}
pub struct ProxyContract;
impl Contract for ProxyContract {
    type Endpoint = u8;
    type Facts = RequestFacts;
    type Execution = ();
    type Error = String;
    type HttpRequest = RequestHeader;
    type HttpResponse = ResponseHeader;
    type ChannelRequest = ();
    type ChannelResponse = ();
    fn rejection(reason: Rejection) -> String {
        match reason {
            Rejection::OutsideExposure => "request outside worker exposure",
            Rejection::NoAuthentication => "request authentication absent",
            Rejection::NoExecution => "request admission absent",
            Rejection::ForeignAdmission => "foreign admission",
            Rejection::CompletionAlreadyStarted => "completion already started",
        }
        .into()
    }
}

/// Cloning shares installed resources, never live request or body state.
#[derive(Clone)]
pub struct NativeProxy {
    config: ProxyConfig,
    pool_key: u64,
    egress: Arc<HttpEgress>,
    lifecycle: Arc<HttpLifecycle<ProxyContract>>,
    control: Arc<WorkerControl>,
    runtime: Option<Runtime>,
    body_dump_limit: usize,
}
impl NativeProxy {
    pub fn new(
        config: ProxyConfig,
        control: Arc<WorkerControl>,
        mut callbacks: Vec<Arc<dyn Callbacks<ProxyContract>>>,
    ) -> std::result::Result<Self, String> {
        config.validate()?;
        // Route and proxy credentials are immutable for this installation. Hash
        // their pool scope once instead of revisiting URLs on every request.
        let mut pool_key = std::collections::hash_map::DefaultHasher::new();
        config.pool_key.hash(&mut pool_key);
        config.upstream.as_str().hash(&mut pool_key);
        config.proxy.as_ref().map(Url::as_str).hash(&mut pool_key);
        callbacks.push(Arc::new(CompiledPolicy {
            reuse: config.reuse,
        }));
        Ok(Self {
            config,
            pool_key: pool_key.finish(),
            egress: Arc::new(HttpEgress::new().map_err(str::to_owned)?),
            lifecycle: Arc::new(HttpLifecycle::new(
                NotifierChain::new(callbacks),
                Exposure::new(vec![ENDPOINT]),
            )),
            control,
            runtime: None,
            body_dump_limit: 0,
        })
    }
    pub fn with_runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }
    /// Opt in to bounded sanitized JSON request-body capture. Response bodies
    /// stay native and are not captured. Disabled sinks never allocate a buffer.
    pub fn with_body_dump_limit(mut self, limit: usize) -> Self {
        self.body_dump_limit = limit.min(1024 * 1024);
        self
    }
}

pub struct ProxyContext {
    exchange: Option<CarrierExchange<ProxyContract>>,
    request_id: u64,
    span: tracing::Span,
    started: Instant,
    reuse: bool,
    peer_selected: bool,
    response_received: bool,
    logged: bool,
    dump: JsonBodyCapture,
}
fn refusal(status: u16, message: &'static str) -> Box<Error> {
    Error::explain(ErrorType::HTTPStatus(status), message)
}
fn lifecycle_error(_: impl std::fmt::Debug) -> Box<Error> {
    refusal(502, "notifier lifecycle failed")
}
impl ProxyContext {
    fn facts(&self) -> Result<&RequestFacts> {
        self.exchange
            .as_ref()
            .map(CarrierExchange::facts)
            .ok_or_else(|| refusal(500, "missing request admission"))
    }
    fn exchange(&mut self) -> Result<&mut CarrierExchange<ProxyContract>> {
        self.exchange
            .as_mut()
            .ok_or_else(|| refusal(500, "missing request admission"))
    }
}

#[async_trait]
impl ProxyHttp for NativeProxy {
    type CTX = ProxyContext;
    fn new_ctx(&self) -> Self::CTX {
        let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        ProxyContext {
            exchange: None,
            request_id,
            span: tracing::info_span!(target: "proxy_pingora::request", "http_request", request_id),
            started: Instant::now(),
            reuse: false,
            peer_selected: false,
            response_received: false,
            logged: false,
            dump: JsonBodyCapture::new(self.body_dump_limit),
        }
    }
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        if !self.control.accepting() {
            return Err(refusal(503, "worker admission closed"));
        }
        if ctx.exchange.is_some() {
            return Err(refusal(500, "repeated request admission"));
        }
        // This application is a fixed reverse route, not an open forward proxy.
        if session.req_header().method.as_str() == "CONNECT" {
            return Err(refusal(
                405,
                "CONNECT requires a configured tunnel listener",
            ));
        }
        let facts = RequestFacts {
            request_id: ctx.request_id,
            method: session.req_header().method.to_string(),
            path: session.req_header().uri.path().to_owned(),
            status: None,
            upstream_reused: false,
            completed: false,
            headers: http::HeaderMap::new(),
        };
        let exchange = self
            .lifecycle
            .begin(ENDPOINT, session.req_header_mut(), facts)
            .instrument(ctx.span.clone())
            .await
            .map_err(|_| refusal(403, "request denied by installed notifier"))?;
        ctx.exchange = Some(exchange);
        Ok(false)
    }
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        if ctx.peer_selected {
            return Err(refusal(502, "upstream retries are disabled"));
        }
        ctx.peer_selected = true;
        let facts = ctx.facts()?;
        let selected = self
            .lifecycle
            .chain
            .pool(
                &self.lifecycle.exposure,
                ENDPOINT,
                PoolPhase::BeforeCheckout,
                facts,
            )
            .instrument(ctx.span.clone())
            .await
            .map_err(lifecycle_error)?;
        // TLS-to-proxy streams cross Pingora's custom L4 boundary as virtual
        // streams, whose descriptors cannot pass native idle-pool validation.
        // Keep that transport usable without retaining unreachable idle entries.
        let poolable = self
            .config
            .proxy
            .as_ref()
            .is_none_or(|proxy| proxy.scheme() != "https");
        ctx.reuse = self.config.reuse && selected == PoolDecision::Reuse && poolable;
        let dial = self.config.proxy.as_ref().unwrap_or(&self.config.upstream);
        let request = DialRequest {
            target: DialTarget::Host {
                name: dial.host_str().unwrap_or_default().into(),
                port: dial.port_or_known_default().unwrap_or(1080),
            },
            timeout: self.config.connect_timeout,
            tcp_nodelay: true,
        };
        if self
            .lifecycle
            .chain
            .before_dial(&self.lifecycle.exposure, ENDPOINT, &request, ctx.facts()?)
            .instrument(ctx.span.clone())
            .await
            .map_err(lifecycle_error)?
            == DialDecision::Deny
        {
            return Err(refusal(403, "upstream dial denied"));
        }
        // The installation key is never logged. Bypass adds a request-private
        // scope, without rehashing fixed URLs, and sets zero idle time below.
        let key = if ctx.reuse {
            self.pool_key
        } else {
            let mut key = std::collections::hash_map::DefaultHasher::new();
            self.pool_key.hash(&mut key);
            ctx.request_id.hash(&mut key);
            key.finish()
        };
        let mut peer = self
            .egress
            .peer(
                &self.config.upstream,
                self.config.proxy.as_ref(),
                key,
                self.config.connect_timeout,
            )
            .await?;
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
        if self
            .lifecycle
            .chain
            .socket(
                &self.lifecycle.exposure,
                ENDPOINT,
                SocketPhase::BeforeUse,
                ctx.facts()?,
            )
            .instrument(ctx.span.clone())
            .await
            .map_err(lifecycle_error)?
            == SocketDecision::Close
        {
            return Err(refusal(403, "upstream socket denied"));
        }
        Ok(())
    }
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        let authority = &self.config.upstream[url::Position::BeforeHost..url::Position::AfterPort];
        request.insert_header("host", authority)?;
        // Preserve origin-form path/query, including escaped path bytes.
        let path = request
            .uri
            .path_and_query()
            .map_or("/", |path| path.as_str())
            .parse()
            .map_err(|_| refusal(400, "invalid request target"))?;
        request.set_uri(path);
        // Hop-by-hop connection policy belongs to this application route. Never
        // relay downstream proxy credentials to the configured origin.
        let upgrade = request.headers.contains_key("upgrade");
        let nominated = request
            .headers
            .get_all("connection")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .map(|value| value.trim().to_ascii_lowercase())
            .collect::<Vec<_>>();
        for name in nominated {
            if matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "host"
            ) {
                return Err(refusal(400, "invalid hop-by-hop framing declaration"));
            }
            if name != "upgrade" {
                request.remove_header(&name);
            }
        }
        request.remove_header("proxy-authorization");
        request.remove_header("proxy-connection");
        request.remove_header("keep-alive");
        request.insert_header(
            "connection",
            if upgrade {
                "upgrade"
            } else if ctx.reuse {
                "keep-alive"
            } else {
                "close"
            },
        )?;
        let span = ctx.span.clone();
        ctx.exchange()?
            .before_upstream_send(request)
            .instrument(span)
            .await
            .map_err(lifecycle_error)
    }
    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.dump
            .observe(body.as_deref().unwrap_or_default(), end, ctx.request_id);
        Ok(())
    }
    async fn upstream_response_filter(
        &self,
        _session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if response.status.is_informational() && response.status.as_u16() != 101 {
            return Ok(());
        }
        ctx.exchange()?.facts_mut().status = Some(response.status.as_u16());
        let span = ctx.span.clone();
        ctx.exchange()?
            .upstream_response(response)
            .instrument(span)
            .await
            .map_err(lifecycle_error)?;
        ctx.response_received = true;
        Ok(())
    }
    async fn response_filter(
        &self,
        _session: &mut Session,
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
        let span = ctx.span.clone();
        ctx.exchange()?
            .before_downstream_write(response)
            .instrument(span)
            .await
            .map_err(lifecycle_error)
    }
    async fn logging(&self, session: &mut Session, error: Option<&Error>, ctx: &mut Self::CTX) {
        ctx.dump.finish(ctx.request_id);
        let status = session.response_written().map(|head| head.status.as_u16());
        let mut completed = false;
        if error.is_none()
            && let Some(mut exchange) = ctx.exchange.take()
        {
            exchange.facts_mut().completed = true;
            completed = exchange.finish().instrument(ctx.span.clone()).await.is_ok();
        }
        tracing::info!(target: "proxy_pingora::access", request_id = ctx.request_id, status, completed,
            elapsed_ms = ctx.started.elapsed().as_millis() as u64,
            upstream_reuse_allowed = ctx.reuse, outcome = if error.is_some() { "failed" } else { "delivered" },
            "proxy request completed");
        ctx.logged = true;
    }
    fn error_while_proxy(
        &self,
        _peer: &HttpPeer,
        _session: &mut Session,
        mut error: Box<Error>,
        _ctx: &mut Self::CTX,
        _reused: bool,
    ) -> Box<Error> {
        error.retry = false.into();
        error
    }
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        _ctx: &mut Self::CTX,
        mut error: Box<Error>,
    ) -> Box<Error> {
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

impl Drop for ProxyContext {
    fn drop(&mut self) {
        if !self.logged {
            self.dump.finish(self.request_id);
            tracing::info!(target: "proxy_pingora::access", request_id = self.request_id,
                completed = false, outcome = "cancelled",
                elapsed_ms = self.started.elapsed().as_millis() as u64, "proxy request cancelled");
        }
    }
}

#[async_trait]
impl ProxyLifecycle for NativeProxy {
    async fn shutdown(&self) -> std::result::Result<(), String> {
        if let Some(runtime) = &self.runtime {
            runtime
                .shutdown()
                .await
                .map_err(|errors| errors.join("; "))?;
        }
        Ok(())
    }
    async fn maintain(&self) {
        self.lifecycle.chain.maintain().await;
    }
}
struct CompiledPolicy {
    reuse: bool,
}
#[async_trait]
impl Callbacks<ProxyContract> for CompiledPolicy {
    fn supports(&self, hook: proxy_pingora::worker::surface::CallbackHook) -> bool {
        use proxy_pingora::worker::surface::CallbackHook;
        matches!(
            hook,
            CallbackHook::PreRouting
                | CallbackHook::Authenticate
                | CallbackHook::Admit
                | CallbackHook::Pool
        )
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> std::result::Result<(), String> {
        facts.headers = request.headers.clone();
        facts.method = request.method.to_string();
        facts.path = request.uri.path().to_owned();
        Ok(())
    }
    async fn authenticate(
        &self,
        _: u8,
        _: &RequestFacts,
    ) -> std::result::Result<Option<()>, String> {
        Ok(Some(()))
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> std::result::Result<Option<()>, String> {
        Ok(Some(()))
    }
    async fn complete(&self, _: &RequestFacts) -> std::result::Result<u32, String> {
        Ok(0)
    }
    async fn pool(
        &self,
        _: u8,
        _: PoolPhase,
        _: &RequestFacts,
    ) -> std::result::Result<Option<PoolDecision>, String> {
        Ok(Some(if self.reuse {
            PoolDecision::Reuse
        } else {
            PoolDecision::Discard
        }))
    }
    async fn maintain(&self) {}
}
