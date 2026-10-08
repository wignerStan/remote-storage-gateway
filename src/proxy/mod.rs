//! Application reverse-proxy policy over Pingora's native duplex HTTP engine.
mod body;
mod carrier;
mod headers;
mod inspection;
mod message_action;
pub mod message_codec;
mod native_hooks;
mod policy;
mod websocket_session;
pub use native_hooks::{NativeHookContext, NativeHookProxy};

use diagnostic_output::{
    operations::{Operation, Operations, Outcome},
    runtime::JsonBodyCapture,
};
use policy::{CompiledPolicy, snapshot_request};

use async_trait::async_trait;
use network_sockets::{DialRequest, DialTarget};
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
            Callbacks, Contract, Exposure, NotifierChain, PoolDecision, PoolPhase, Rejection,
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
use stream_transfer::chunk::ChunkChain;
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
    pub http2: crate::config::Http2,
    pub websocket_keepalive: crate::keepalive::WebSocketKeepalive,
}
impl ProxyConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        self.http2.validate()?;
        self.websocket_keepalive.validate(self.io_timeout)?;
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
    /// IP of the accepted socket peer, never a caller-supplied forwarding header.
    /// Non-IP transports have no address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<std::net::IpAddr>,
    pub method: String,
    pub path: String,
    pub status: Option<u16>,
    pub upstream_reused: bool,
    pub completed: bool,
    /// Last completed request-head stage, projected once for authentication,
    /// admission, or network controls; never cloned by each native callback.
    /// Not serialized by default; ABI adapters select an explicit allowlist.
    #[serde(skip)]
    pub headers: http::HeaderMap,
    /// Request setup slot, moved into the carrier before upstream finalization.
    #[serde(skip)]
    pub request_body: ChunkChain,
    /// Response setup slot, moved into the carrier before downstream finalization.
    #[serde(skip)]
    pub response_body: ChunkChain,
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
    session_connector: Arc<pingora_core::connectors::http::Connector>,
    lifecycle: Arc<HttpLifecycle<ProxyContract>>,
    control: Arc<WorkerControl>,
    operations: Arc<Operations>,
    runtime: Option<Runtime>,
    body_dump_limit: usize,
    message_codecs: Arc<[message_codec::CodecRegistration]>,
    inspection: Arc<inspection::InspectionBudget>,
}
impl NativeProxy {
    /// Grant a trusted native Pingora implementation its async request-body hook
    /// after an admitted protocol upgrade. All other handler hooks stay inactive;
    /// this worker continues to own routing, admission and metadata boundaries.
    ///
    /// Chunks are raw native bytes, not complete WebSocket frames or messages.
    /// A handler buffering partial frames must leave `Some(Bytes::new())`, since
    /// `None` signals EOF to Pingora. Its own context is created lazily per request.
    pub fn with_upgraded_request_body<P>(self, handler: Arc<P>) -> NativeHookProxy<P>
    where
        P: ProxyHttp + Send + Sync + 'static,
        P::CTX: Send + Sync,
    {
        NativeHookProxy::new(self, handler)
    }
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
            session_connector: Arc::new(pingora_core::connectors::http::Connector::new(None)),
            lifecycle: Arc::new(HttpLifecycle::new(
                NotifierChain::new(callbacks),
                Exposure::new(vec![ENDPOINT]),
            )),
            control,
            operations: Arc::new(Operations::default()),
            runtime: None,
            body_dump_limit: 0,
            message_codecs: Arc::from([]),
            inspection: Arc::new(inspection::InspectionBudget::new(
                crate::config::Inspection::default(),
            )),
        })
    }
    pub(crate) fn with_message_callbacks(
        mut self,
        callbacks: Vec<(String, message_codec::MessageCallback)>,
        codecs: Vec<Arc<dyn message_codec::CodecPlugin>>,
    ) -> Self {
        self.message_codecs = codecs
            .into_iter()
            .map(|codec| {
                let selected = callbacks
                    .iter()
                    .filter(|(id, _)| id == codec.id())
                    .map(|(_, callback)| callback.clone())
                    .collect();
                message_codec::CodecRegistration {
                    codec,
                    callbacks: selected,
                }
            })
            .collect::<Vec<_>>()
            .into();
        self
    }
    fn selected_upgrade_codec(
        &self,
        ctx: &ProxyContext,
    ) -> Option<&dyn message_codec::CodecPlugin> {
        ctx.message_selection
            .filter(|(_, transport)| *transport == message_codec::Transport::Upgrade)
            .map(|(index, _)| self.message_codecs[index].codec.as_ref())
    }
    /// Share process operation counters with management or another observer.
    pub fn with_operations(mut self, operations: Arc<Operations>) -> Self {
        self.operations = operations;
        self
    }
    pub fn operations(&self) -> Arc<Operations> {
        Arc::clone(&self.operations)
    }
    pub fn with_inspection_limits(mut self, limits: crate::config::Inspection) -> Self {
        self.inspection = Arc::new(inspection::InspectionBudget::new(limits));
        self
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
    operation: Operation,
    exchange: Option<CarrierExchange<ProxyContract>>,
    request_id: u64,
    span: tracing::Span,
    started: Instant,
    reuse: bool,
    request_started: bool,
    peer_selected: bool,
    connected: bool,
    upstream_request: Option<RequestHeader>,
    inspection_request: Option<RequestHeader>,
    request_has_body: bool,
    request_body: ChunkChain,
    response_body: ChunkChain,
    response_variable: bool,
    response_framing: Option<headers::ResponseFraming>,
    upgraded: bool,
    message_selection: Option<(usize, message_codec::Transport)>,
    message_decoder: Option<Box<dyn proxy_protocols::message_codec::Decoder>>,
    message_sequence: u64,
    response_sequence: u64,
    response_decoder: Option<Box<dyn proxy_protocols::message_codec::Decoder>>,
    response_message_complete: bool,
    request_permit: Option<admission_gate::budget::Reservation>,
    inspection_permit: Option<Arc<admission_gate::budget::Reservation>>,
    message_deadline: Option<tokio::time::Instant>,
    response_deadline: Option<tokio::time::Instant>,
    message_failed: bool,
    message_http_complete: bool,
    response_received: bool,
    logged: bool,
    session_completed: bool,
    dump: JsonBodyCapture,
}
fn refusal(status: u16, message: &'static str) -> Box<Error> {
    Error::explain(ErrorType::HTTPStatus(status), message)
}
fn lifecycle_error(_: impl std::fmt::Debug) -> Box<Error> {
    refusal(502, "notifier lifecycle failed")
}
fn control_error(error: proxy_pingora::worker::LifecycleError<String>) -> Box<Error> {
    match error {
        proxy_pingora::worker::LifecycleError::Denied => refusal(403, "upstream control denied"),
        _ => lifecycle_error(error),
    }
}
impl ProxyContext {
    fn exchange(&mut self) -> Result<&mut CarrierExchange<ProxyContract>> {
        self.exchange
            .as_mut()
            .ok_or_else(|| refusal(500, "missing request admission"))
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
