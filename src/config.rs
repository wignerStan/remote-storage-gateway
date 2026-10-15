//! Application configuration, decoded by the reusable configuration owner.
use crate::proxy::ProxyConfig;
use diagnostic_output::runtime::{FileLogConfig, LoggingConfig};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use url::Url;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub upstream: String,
    #[serde(default = "listen")]
    pub listen: SocketAddr,
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub tls: Option<Tls>,
    #[serde(default = "threads")]
    pub threads: usize,
    #[serde(default = "shutdown_seconds")]
    pub shutdown_seconds: u64,
    /// Native listener handoff between independently started worker generations.
    #[serde(default)]
    pub upgrade: Option<Upgrade>,
    #[serde(default = "connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "io_timeout")]
    pub io_timeout_ms: u64,
    #[serde(default)]
    pub pool: Pool,
    #[serde(default)]
    pub http2: Http2,
    #[serde(default)]
    pub websocket_keepalive: crate::keepalive::WebSocketKeepalive,
    #[serde(default)]
    pub logging: Logging,
    #[serde(default)]
    pub callbacks: Vec<NativeCallback>,
    /// One shared remote transport owner per host axis.
    #[serde(default)]
    pub axis_bindings: std::collections::BTreeMap<String, crate::remote_axis::AxisBinding>,
    #[serde(default)]
    pub inspection: Inspection,
    #[serde(default)]
    pub management: Option<Management>,
    /// Select the local control facility used to deliver plugin entry requests.
    #[serde(default)]
    pub plugin_ingress: Option<PluginControl>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upgrade {
    /// Shared Unix socket in a private, operator-owned runtime directory.
    pub socket: String,
    #[serde(default = "upgrade_grace")]
    pub grace_period_seconds: u64,
}
fn upgrade_grace() -> u64 {
    1
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: String,
    pub key: String,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Pool {
    pub enabled: bool,
    pub idle_timeout_seconds: u64,
    pub isolation_key: u64,
}
impl Default for Pool {
    fn default() -> Self {
        Self {
            enabled: true,
            idle_timeout_seconds: 90,
            isolation_key: 0,
        }
    }
}
/// Upstream HTTP/2 selection; native Pingora owns connection-level PING/ACK.
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Http2 {
    pub enabled: bool,
    pub ping_interval_ms: Option<u64>,
}
impl Http2 {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(milliseconds) = self.ping_interval_ms {
            let interval = Duration::from_millis(milliseconds);
            if !self.enabled
                || interval.is_zero()
                || std::time::Instant::now().checked_add(interval).is_none()
            {
                return Err("http2 ping interval requires enabled HTTP/2 and a positive representable duration".into());
            }
        }
        Ok(())
    }
    pub fn ping_interval(&self) -> Option<Duration> {
        self.ping_interval_ms.map(Duration::from_millis)
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logging {
    pub filter: String,
    pub access_file: Option<LogFile>,
    pub body_file: Option<LogFile>,
    pub body_max_bytes: usize,
    pub queue_capacity: usize,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            access_file: None,
            body_file: None,
            body_max_bytes: 0,
            queue_capacity: 8192,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogFile {
    pub directory: PathBuf,
    pub keep_files: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Management {
    /// Local endpoint for the selected facility; local_control requires UDS.
    pub listen: String,
    #[serde(default)]
    pub protocol: ControlProtocol,
}
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlProtocol {
    /// Existing HTTP actions, SSE and optional gRPC management clients.
    #[default]
    Http,
    /// ttrpc control messages on a same-host Unix socket, without HTTP/2.
    LocalControl,
}
#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PluginControl {
    Shared { facility: SharedControl },
    Dedicated { listen: String },
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedControl {
    Management,
}
impl PluginControl {
    pub(crate) fn shared(&self) -> bool {
        matches!(self, Self::Shared { .. })
    }
}

/// A host-selected transport and schema grant; plugins cannot publish routes themselves.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAction {
    pub method: String,
    pub contract: action_execution::Contract,
    pub bindings: Vec<action_execution::Binding>,
}

/// Host-wide bounds on concurrent semantic inspection and retained message data.
#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inspection {
    pub max_requests: usize,
    pub max_buffered_bytes: usize,
    pub message_timeout_ms: u64,
    pub max_message_bytes: usize,
}
impl Default for Inspection {
    fn default() -> Self {
        Self {
            max_requests: 128,
            max_buffered_bytes: 256 * 1024 * 1024,
            message_timeout_ms: 30_000,
            max_message_bytes: 1024 * 1024,
        }
    }
}

/// Process configuration explicitly selects the callback backend and its grants.
/// Native Function/Object and optional Wasm Object callbacks share metadata contracts.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCallback {
    /// RPC is the default. Direct execution requires an explicit selection.
    #[serde(default)]
    pub binding: CallbackBinding,
    #[serde(default)]
    pub axis: Option<String>,
    /// Host-assigned participant identity within the axis.
    #[serde(default)]
    pub registration: Option<String>,
    #[serde(default)]
    pub backend: CallbackBackend,
    /// Sandbox bounds apply only to the explicitly selected Wasm backend.
    #[serde(default)]
    pub wasm_limits: Option<WasmLimits>,
    #[serde(default)]
    pub path: PathBuf,
    pub package: String,
    #[serde(default = "variant")]
    pub variant: String,
    #[serde(default)]
    pub language: Language,
    /// Native artifacts default to Object, independently of implementation language.
    #[serde(default)]
    pub kind: Option<CallbackKind>,
    #[serde(default)]
    pub hooks: Vec<String>,
    /// Explicit unary methods published on the loopback management listener.
    #[serde(default)]
    pub actions: Vec<NativeAction>,
    /// Explicit protocol codec grant, resolved against the host codec registry.
    #[serde(default)]
    pub message_codec: Option<String>,
    /// Explicit host framing contract for this selected native codec callback.
    #[serde(default)]
    pub codec_descriptor: Option<proxy_pingora::http_ingress::message_codec::CodecDescriptor>,
    /// Grant response and error payload access in addition to request messages.
    #[serde(default)]
    pub message_responses: bool,
    #[serde(default)]
    pub message_actions: bool,
    #[serde(default)]
    pub message_proxy: bool,
    /// Inject explicitly selected native providers into this Object callback.
    #[serde(default)]
    pub graph: Option<CallbackGraph>,
    /// Header metadata explicitly exposed to this trusted callback.
    #[serde(default)]
    pub headers: Vec<String>,
    /// Host-selected header names this callback may set or remove. Empty by default.
    /// Transport framing and WebSocket negotiation fields cannot be granted.
    #[serde(default)]
    pub patch_headers: Vec<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default = "callback_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "callback_capacity")]
    pub max_inflight: usize,
    /// Waiting calls retain inputs, so their count is bounded separately.
    #[serde(default = "callback_capacity")]
    pub max_waiting: usize,
    #[serde(default = "callback_timeout")]
    pub startup_timeout_ms: u64,
    #[serde(default = "callback_timeout")]
    pub shutdown_timeout_ms: u64,
    /// Separate mutable state and queue per instance, with stable request affinity.
    #[serde(default = "callback_instances")]
    pub instances: usize,
    #[serde(default)]
    pub execution: Option<CallbackExecution>,
    /// Dispatch blocking callbacks to bounded native workers; inline is a trusted
    /// declaration that calls finish promptly without blocking the caller.
    #[serde(default)]
    pub scheduling: CallbackScheduling,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackGraph {
    pub callback_instance: String,
    pub instances: Vec<GraphInstance>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphInstance {
    pub id: String,
    pub path: PathBuf,
    pub package: String,
    #[serde(default = "variant")]
    pub variant: String,
    #[serde(default)]
    pub language: Language,
    pub kind: GraphKind,
    pub provides: Vec<String>,
    #[serde(default)]
    pub requires: Vec<GraphRequirement>,
    #[serde(default)]
    pub config: serde_json::Value,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphKind {
    Object,
    Tool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRequirement {
    pub instance: String,
    pub capability: String,
}
impl CallbackGraph {
    pub fn runtime(&self) -> Result<plugin_loading::RuntimeGraph, String> {
        let instances = self
            .instances
            .iter()
            .map(|node| {
                let (language, unload) = node.language.native()?;
                Ok(plugin_loading::RuntimeInstance {
                    id: node.id.clone(),
                    path: node.path.clone(),
                    expected: plugin_abi::Expected::v1(
                        &node.package,
                        &node.variant,
                        match node.kind {
                            GraphKind::Object => plugin_abi::PluginType::Object,
                            GraphKind::Tool => plugin_abi::PluginType::Tool,
                        },
                        language,
                        unload,
                    ),
                    provides: node.provides.clone(),
                    requires: node
                        .requires
                        .iter()
                        .map(|r| plugin_loading::RuntimeRequirement {
                            instance: r.instance.clone(),
                            capability: r.capability.clone(),
                        })
                        .collect(),
                    config: text_processing::json::to_vec_bounded(
                        &node.config,
                        crate::native_notifier::FRAME_LIMIT,
                    )
                    .map_err(|_| "graph instance config exceeds frame budget".to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        plugin_loading::RuntimeGraph::new(instances)
    }
}
/// Binding selection is independent of the native artifact backend.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackBinding {
    #[default]
    Remote,
    Direct,
    /// Use only a native bridge implemented and tested by this worker.
    Auto,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackBackend {
    #[default]
    Native,
    Wasm,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WasmLimits {
    pub max_memory_bytes: usize,
    pub fuel_per_call: u64,
    pub max_module_bytes: usize,
    pub max_table_elements: usize,
}
impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024,
            fuel_per_call: 1_000_000,
            max_module_bytes: 16 * 1024 * 1024,
            max_table_elements: 4096,
        }
    }
}
impl WasmLimits {
    fn validate(self) -> Result<(), String> {
        #[cfg(feature = "wasm")]
        {
            self.sandbox().validate().map_err(|error| error.to_string())
        }
        #[cfg(not(feature = "wasm"))]
        {
            let _ = self;
            Err("Wasm callbacks require the worker wasm feature".into())
        }
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn sandbox(self) -> wasm_sandbox::Limits {
        wasm_sandbox::Limits {
            max_memory_bytes: self.max_memory_bytes,
            fuel_per_call: self.fuel_per_call,
            max_module_bytes: self.max_module_bytes,
            max_table_elements: self.max_table_elements,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackScheduling {
    #[default]
    Blocking,
    Inline,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackExecution {
    /// Notifier callbacks share the worker process and runtime ownership.
    #[default]
    InProcess,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    #[default]
    Rust,
    Go,
    Python,
    #[serde(rename = "javascript")]
    JavaScript,
}
impl Language {
    pub(crate) fn native(self) -> Result<(plugin_abi::Language, plugin_abi::UnloadPolicy), String> {
        match self {
            Self::Rust => Ok((
                plugin_abi::Language::Rust,
                plugin_abi::UnloadPolicy::Explicit,
            )),
            Self::Go => Ok((plugin_abi::Language::Go, plugin_abi::UnloadPolicy::Retain)),
            Self::Python | Self::JavaScript => {
                Err("this language has no worker native bridge; select remote binding".into())
            }
        }
    }
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackKind {
    Function,
    Object,
}
impl NativeCallback {
    pub fn resolved_binding(&self) -> CallbackBinding {
        match self.binding {
            CallbackBinding::Auto if self.language.native().is_ok() => CallbackBinding::Direct,
            CallbackBinding::Auto => CallbackBinding::Remote,
            selected => selected,
        }
    }
    pub fn plugin_kind(&self) -> plugin_abi::PluginType {
        match self.kind {
            Some(CallbackKind::Function) => plugin_abi::PluginType::Function,
            Some(CallbackKind::Object) | None => plugin_abi::PluginType::Object,
        }
    }
}
fn listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 6188))
}
fn threads() -> usize {
    2
}
fn shutdown_seconds() -> u64 {
    30
}
fn connect_timeout() -> u64 {
    10_000
}
fn io_timeout() -> u64 {
    60_000
}
pub(crate) fn callback_timeout() -> u64 {
    5_000
}
pub(crate) fn callback_capacity() -> usize {
    32
}
fn callback_instances() -> usize {
    1
}
fn variant() -> String {
    "native".into()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let (config, _) = configuration::load_typed::<Self>(path, configuration::FileFormat::Json)
            .map_err(|_| "cannot load configuration: expected a readable, bounded JSON document with the documented fields".to_string())?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        self.process_options(false)?;
        if !(1..=4096).contains(&self.inspection.max_requests)
            || !(1..=1024 * 1024 * 1024).contains(&self.inspection.max_buffered_bytes)
            || !(1..=300_000).contains(&self.inspection.message_timeout_ms)
            || !(1..=32 * 1024 * 1024).contains(&self.inspection.max_message_bytes)
        {
            return Err("inspection concurrency, memory, message size and timeout must be positive and bounded".into());
        }
        let reservation = self.inspection.max_message_bytes * 8 + 8 * 1024 * 1024 + 512 * 1024;
        if self.inspection.max_buffered_bytes < reservation {
            return Err(
                "inspection memory budget must hold at least one message inspection reservation"
                    .into(),
            );
        }
        if !(1..=256).contains(&self.threads) || !(1..=3600).contains(&self.shutdown_seconds) {
            return Err("threads must be 1..256 and shutdown_seconds 1..3600".into());
        }
        if !(1..=300_000).contains(&self.connect_timeout_ms)
            || !(1..=3_600_000).contains(&self.io_timeout_ms)
            || !(1..=86_400).contains(&self.pool.idle_timeout_seconds)
        {
            return Err("connection, I/O and pool timeouts must be positive and bounded".into());
        }
        if self.callbacks.len() > 64 {
            return Err("at most 64 callbacks may be installed".into());
        }
        crate::remote_axis::validate(&self.axis_bindings)?;
        let mut remote_registrations = std::collections::BTreeSet::new();
        let mut action_bindings = std::collections::BTreeSet::new();
        for callback in &self.callbacks {
            if callback.resolved_binding() == CallbackBinding::Remote {
                crate::remote_axis::validate_callback(callback, &self.axis_bindings)?;
                if !remote_registrations
                    .insert((callback.axis.as_ref(), callback.registration.as_ref()))
                {
                    return Err("remote registration must be unique within its axis".into());
                }
            } else {
                callback.language.native()?;
                if callback.axis.is_some() || callback.registration.is_some() {
                    return Err(
                        "direct callbacks cannot select a remote axis or registration".into(),
                    );
                }
            }
            if callback.message_actions && callback.message_codec.is_none() {
                return Err("message actions require an explicit codec grant".into());
            }
            if callback.message_proxy
                && (!callback.message_actions || callback.backend != CallbackBackend::Native)
            {
                return Err("message proxy requires message actions and native backend".into());
            }
            if let Some(graph) = &callback.graph {
                if callback.backend != CallbackBackend::Native
                    || callback.plugin_kind() != plugin_abi::PluginType::Object
                {
                    return Err("dependency graphs require native Object callbacks".into());
                }
                let runtime = graph.runtime()?;
                let selected = runtime
                    .instances()
                    .iter()
                    .find(|n| n.id == graph.callback_instance)
                    .ok_or("graph callback instance is absent")?;
                if selected.expected.kind != plugin_abi::PluginType::Object
                    || selected.path != callback.path
                    || selected.expected.package != callback.package
                    || selected.expected.variant != callback.variant
                    || selected.expected.language != callback.language.native()?.0
                    || selected.config
                        != text_processing::json::to_vec_bounded(
                            &callback.config,
                            crate::native_notifier::FRAME_LIMIT,
                        )
                        .map_err(|_| "callback config exceeds frame budget")?
                    || runtime
                        .instances()
                        .len()
                        .checked_mul(callback.instances)
                        .is_none_or(|n| n > 64)
                {
                    return Err("graph selected Object must match callback identity/config and pool must own at most 64 nodes".into());
                }
            }
            match callback.backend {
                CallbackBackend::Native if callback.wasm_limits.is_some() => {
                    return Err("wasm_limits requires backend wasm".into());
                }
                CallbackBackend::Wasm => {
                    if !cfg!(feature = "wasm") {
                        return Err("Wasm callbacks require the worker wasm feature".into());
                    }
                    if matches!(callback.kind, Some(CallbackKind::Function)) {
                        return Err("Wasm callbacks support only Object kind".into());
                    }
                    if callback.scheduling == CallbackScheduling::Inline {
                        return Err("Wasm callbacks require blocking scheduling".into());
                    }
                    let sandbox = callback.wasm_limits.unwrap_or_default();
                    sandbox.validate()?;
                    if sandbox
                        .max_memory_bytes
                        .checked_mul(callback.instances)
                        .is_none_or(|bytes| bytes > 512 * 1024 * 1024)
                    {
                        return Err(
                            "Wasm callback instance pool memory must not exceed 512 MiB".into()
                        );
                    }
                }
                CallbackBackend::Native => {}
            }
            if let Some(descriptor) = &callback.codec_descriptor {
                if callback.message_codec.as_deref() != Some(&descriptor.id) {
                    return Err(
                        "native codec descriptor requires the same explicit message codec grant"
                            .into(),
                    );
                }
                if descriptor.response_json && !callback.message_responses {
                    return Err(
                        "native response codec framing requires an explicit response message grant"
                            .into(),
                    );
                }
                proxy_pingora::http_ingress::message_codec::RegisteredCodec::new(
                    descriptor.clone(),
                )?;
            }
            if callback.message_responses && callback.message_codec.is_none() {
                return Err("response message access requires an explicit codec grant".into());
            }
            if let Some(codec) = &callback.message_codec
                && !crate::proxy::message_codec::valid_id(codec)
            {
                return Err("invalid message codec identifier".into());
            }
            if (callback.resolved_binding() == CallbackBinding::Direct
                && callback.path.as_os_str().is_empty())
                || callback.path.as_os_str().len() > 4096
                || callback.package.is_empty()
                || callback.package.len() > 256
                || callback.variant.is_empty()
                || callback.variant.len() > 256
                || !(1..=1024).contains(&callback.max_inflight)
                || callback.max_waiting > 1024
                || !(1..=64).contains(&callback.instances)
                || !(1..=300_000).contains(&callback.timeout_ms)
                || !(1..=300_000).contains(&callback.startup_timeout_ms)
                || !(1..=300_000).contains(&callback.shutdown_timeout_ms)
            {
                return Err("invalid native callback identity, capacity or timeout".into());
            }
            if callback.actions.len() > 32 {
                return Err("at most 32 native actions per callback".into());
            }
            if !callback.actions.is_empty()
                && (self.management.is_none() || callback.instances != 1)
            {
                return Err(
                    "native actions require a management listener and one stateful instance".into(),
                );
            }
            for action in &callback.actions {
                action.contract.check().map_err(|e| e.to_string())?;
                if action.method.is_empty()
                    || action.method.len() > 256
                    || action.contract.response != action_execution::ResponseMode::Unary
                    || action.contract.max_input > 1024 * 1024
                    || action.contract.max_output > 1024 * 1024
                    || action.bindings.is_empty()
                    || action.bindings.len() > 8
                {
                    return Err(
                        "native actions require bounded unary methods and transport bindings"
                            .into(),
                    );
                }
                for binding in &action.bindings {
                    binding.check().map_err(|e| e.to_string())?;
                    if !action_bindings.insert(binding.clone()) {
                        return Err("native action transport bindings must be unique".into());
                    }
                }
                let reachable = self.management.as_ref().is_some_and(|management| {
                    action.bindings.iter().any(|binding| {
                        matches!(
                            (management.protocol, binding),
                            (
                                ControlProtocol::Http,
                                action_execution::Binding::Http { .. }
                                    | action_execution::Binding::Grpc { .. }
                            ) | (
                                ControlProtocol::LocalControl,
                                action_execution::Binding::LocalControl { .. }
                            )
                        )
                    })
                });
                if !reachable {
                    return Err("native action has no binding reachable through the selected management protocol".into());
                }
            }
            if !callback.hooks.is_empty()
                || (callback.message_codec.is_none() && callback.actions.is_empty())
            {
                crate::native_notifier::validate_hooks(&callback.hooks)?;
            }
            crate::native_notifier::validate_patch_headers(&callback.patch_headers)?;
            text_processing::json::to_vec_bounded(
                &callback.config,
                crate::native_notifier::FRAME_LIMIT,
            )
            .map_err(|_| "native initialization metadata exceeds its frame budget")?;
            if callback.headers.len() > 64
                || callback
                    .headers
                    .iter()
                    .any(|name| http::header::HeaderName::from_bytes(name.as_bytes()).is_err())
            {
                return Err("callback headers must be a bounded list of valid names".into());
            }
        }
        if self.logging.queue_capacity == 0
            || self.logging.queue_capacity > 1_048_576
            || [&self.logging.access_file, &self.logging.body_file]
                .into_iter()
                .flatten()
                .any(|file| file.keep_files == 0 || file.directory.as_os_str().is_empty())
        {
            return Err(
                "logging queue, directories and file retention must be bounded and nonempty".into(),
            );
        }
        if let Some(management) = &self.management {
            if management.protocol == ControlProtocol::LocalControl {
                crate::remote_axis::control_path(&management.listen)?;
            }
            match network_sockets::LocalEndpoint::parse(&management.listen)? {
                network_sockets::LocalEndpoint::Tcp(address) if address == self.listen => {
                    return Err("data and management listeners must differ".into());
                }
                network_sockets::LocalEndpoint::Tcp(_)
                | network_sockets::LocalEndpoint::Unix(_) => {}
            }
        }
        if let Some(PluginControl::Shared { .. }) = &self.plugin_ingress {
            let management = self
                .management
                .as_ref()
                .ok_or("shared plugin control requires management facility")?;
            if management.protocol != ControlProtocol::LocalControl {
                return Err("plugin control requires management protocol local_control".into());
            }
        }
        if let Some(PluginControl::Dedicated { listen }) = &self.plugin_ingress {
            crate::remote_axis::control_path(listen)?;
            let endpoint = network_sockets::LocalEndpoint::parse(listen)?;
            if matches!(&endpoint, network_sockets::LocalEndpoint::Tcp(address) if *address == self.listen)
            {
                return Err("plugin ingress and data listeners must differ".into());
            }
            if let Some(management) = &self.management {
                let other = network_sockets::LocalEndpoint::parse(&management.listen)?;
                let same = match (&endpoint, &other) {
                    (
                        network_sockets::LocalEndpoint::Tcp(a),
                        network_sockets::LocalEndpoint::Tcp(b),
                    ) => a == b,
                    (
                        network_sockets::LocalEndpoint::Unix(a),
                        network_sockets::LocalEndpoint::Unix(b),
                    ) => a == b,
                    _ => false,
                };
                if same {
                    return Err("duplicate listener; select plugin_ingress facility management to reuse the local control facility".into());
                }
            }
        }
        if self.logging.body_file.is_some() != (self.logging.body_max_bytes > 0)
            || self.logging.body_max_bytes > 1024 * 1024
        {
            return Err(
                "body capture requires a separate body_file and body_max_bytes in 1..1048576"
                    .into(),
            );
        }
        self.proxy_config()?.validate()
    }
    pub fn proxy_config(&self) -> Result<ProxyConfig, String> {
        Ok(ProxyConfig {
            upstream: Url::parse(&self.upstream).map_err(|_| "invalid upstream URL")?,
            proxy: self
                .proxy
                .as_ref()
                .map(|value| Url::parse(value).map_err(|_| "invalid selected proxy URL"))
                .transpose()?,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            io_timeout: Duration::from_millis(self.io_timeout_ms),
            pool_idle_timeout: Duration::from_secs(self.pool.idle_timeout_seconds),
            pool_key: self.pool.isolation_key,
            reuse: self.pool.enabled,
            http2: self.http2,
            websocket_keepalive: self.websocket_keepalive,
        })
    }
    pub fn process_options(
        &self,
        upgrade: bool,
    ) -> Result<proxy_pingora::http_ingress::ProcessOptions, String> {
        let options = proxy_pingora::http_ingress::ProcessOptions {
            upgrade_socket: self.upgrade.as_ref().map(|options| options.socket.clone()),
            upgrade,
            grace_period_seconds: self
                .upgrade
                .as_ref()
                .map_or(1, |options| options.grace_period_seconds),
        };
        options.validate()?;
        Ok(options)
    }

    pub fn listener(&self) -> proxy_pingora::http_ingress::ListenerConfig {
        proxy_pingora::http_ingress::ListenerConfig {
            addr: self.listen,
            tls: self
                .tls
                .as_ref()
                .map(|tls| (tls.certificate.clone(), tls.key.clone())),
            threads: self.threads,
            shutdown_seconds: self.shutdown_seconds,
        }
    }
    pub fn logging(&self) -> LoggingConfig {
        fn file(value: &LogFile) -> FileLogConfig {
            FileLogConfig {
                directory: value.directory.clone(),
                keep_files: value.keep_files,
            }
        }
        LoggingConfig {
            stderr_filter: self.logging.filter.clone(),
            access_file: self.logging.access_file.as_ref().map(file),
            body_file: self.logging.body_file.as_ref().map(file),
            queue_capacity: self.logging.queue_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn remote_is_default_and_direct_or_language_auto_are_explicit() {
        let mut callback = json!({"package":"policy", "axis":"request_policy", "registration":"policy", "hooks":["routing"]});
        let remote = |callback: serde_json::Value| -> Config {
            serde_json::from_value(json!({"upstream":"http://127.0.0.1:9", "axis_bindings":{"request_policy":{"endpoint":"unix:///run/policy.sock"}}, "callbacks":[callback]})).unwrap()
        };
        let config = remote(callback.clone());
        config.validate().unwrap();
        assert_eq!(
            config.callbacks[0].resolved_binding(),
            CallbackBinding::Remote
        );
        callback["binding"] = json!("auto");
        for language in ["python", "javascript"] {
            callback["language"] = json!(language);
            let config = remote(callback.clone());
            config.validate().unwrap();
            assert_eq!(
                config.callbacks[0].resolved_binding(),
                CallbackBinding::Remote
            );
        }
        for language in ["rust", "go"] {
            let mut direct = selected_callback();
            direct["binding"] = json!("auto");
            direct["language"] = json!(language);
            let config = configured(direct);
            config.validate().unwrap();
            assert_eq!(
                config.callbacks[0].resolved_binding(),
                CallbackBinding::Direct
            );
        }
        let mut legacy = selected_callback();
        legacy.as_object_mut().unwrap().remove("binding");
        assert!(
            configured(legacy)
                .validate()
                .unwrap_err()
                .contains("requires an axis")
        );
    }

    #[test]
    fn remote_metadata_rejects_bulk_grants_duplicate_registrations_and_local_artifacts() {
        let input = json!({"upstream":"http://127.0.0.1:9", "axis_bindings":{"request_policy":{"endpoint":"unix:///run/policy.sock"}},
            "callbacks":[{"package":"policy", "axis":"request_policy", "registration":"policy", "hooks":["routing"]}]});
        for (field, value) in [
            ("message_codec", json!("websocket.v1")),
            ("path", json!("policy.so")),
            (
                "graph",
                json!({"callback_instance":"policy","instances":[]}),
            ),
            ("execution", json!("in_process")),
            ("max_inflight", json!(1)),
            ("scheduling", json!("inline")),
            ("registration", json!("../escape")),
            ("axis", json!("unknown")),
        ] {
            let mut invalid = input.clone();
            invalid["callbacks"][0][field] = value;
            let config: Config = serde_json::from_value(invalid).unwrap();
            assert!(config.validate().is_err(), "{field} was accepted");
        }
        let mut duplicate = input;
        let callback = duplicate["callbacks"][0].clone();
        duplicate["callbacks"]
            .as_array_mut()
            .unwrap()
            .push(callback);
        assert!(
            serde_json::from_value::<Config>(duplicate)
                .unwrap()
                .validate()
                .unwrap_err()
                .contains("unique")
        );
    }

    #[test]
    fn native_callbacks_default_to_object_independent_of_language() {
        for (language, kind, expected) in [
            (None, None, plugin_abi::PluginType::Object),
            (Some("rust"), None, plugin_abi::PluginType::Object),
            (Some("go"), None, plugin_abi::PluginType::Object),
            (Some("rust"), Some("object"), plugin_abi::PluginType::Object),
            (
                Some("go"),
                Some("function"),
                plugin_abi::PluginType::Function,
            ),
        ] {
            let mut input = json!({"binding":"direct", "path":"callback.so","package":"trusted.metadata","hooks":["pre_routing"]});
            if let Some(language) = language {
                input["language"] = json!(language);
            }
            if let Some(kind) = kind {
                input["kind"] = json!(kind);
            }
            let callback: NativeCallback = serde_json::from_value(input).unwrap();
            assert_eq!(callback.plugin_kind(), expected);
            assert!(callback.message_codec.is_none());
        }
    }

    #[test]
    fn callback_backend_defaults_to_native_and_rejects_sandbox_on_native() {
        let mut input = selected_callback();
        let callback: NativeCallback = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(callback.backend, CallbackBackend::Native);
        assert!(configured(input.clone()).validate().is_ok());
        input["wasm_limits"] = json!({});
        assert!(
            configured(input)
                .validate()
                .unwrap_err()
                .contains("requires backend wasm")
        );
    }

    #[test]
    fn wasm_backend_requires_feature_object_and_blocking_execution() {
        let mut input = selected_callback();
        input["backend"] = json!("wasm");
        let result = configured(input.clone()).validate();
        if cfg!(feature = "wasm") {
            assert!(result.is_ok());
            input["kind"] = json!("function");
            assert!(
                configured(input.clone())
                    .validate()
                    .unwrap_err()
                    .contains("only Object")
            );
            input["kind"] = json!("object");
            input["scheduling"] = json!("inline");
            assert!(
                configured(input.clone())
                    .validate()
                    .unwrap_err()
                    .contains("blocking scheduling")
            );
            input["scheduling"] = json!("blocking");
            for limits in [
                json!({"fuel_per_call":0}),
                json!({"max_memory_bytes":0}),
                json!({"max_module_bytes":0}),
                json!({"max_table_elements":65537}),
                json!({"fuel_per_call":1000000001}),
            ] {
                input["wasm_limits"] = limits;
                assert!(configured(input.clone()).validate().is_err());
            }
        } else {
            assert!(result.unwrap_err().contains("wasm feature"));
        }
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn wasm_defaults_match_primitive_and_pool_memory_is_bounded_before_loading() {
        let local = WasmLimits::default().sandbox();
        let primitive = wasm_sandbox::Limits::default();
        assert_eq!(local.max_memory_bytes, primitive.max_memory_bytes);
        assert_eq!(local.max_module_bytes, primitive.max_module_bytes);
        assert_eq!(local.max_table_elements, primitive.max_table_elements);
        assert_eq!(local.fuel_per_call, primitive.fuel_per_call);
        let mut input = selected_callback();
        input["backend"] = json!("wasm");
        input["instances"] = json!(32);
        assert!(configured(input.clone()).validate().is_ok());
        input["instances"] = json!(33);
        assert!(
            configured(input.clone())
                .validate()
                .unwrap_err()
                .contains("512 MiB")
        );
        input["instances"] = json!(1);
        input["wasm_limits"] = json!({"max_memory_bytes": 1024 * 1024 * 1024});
        assert!(
            configured(input)
                .validate()
                .unwrap_err()
                .contains("512 MiB")
        );
    }

    #[test]
    fn message_access_requires_an_explicit_codec_grant() {
        let input = json!({"binding":"direct", "path":"callback.so", "package":"trusted.metadata", "hooks":[], "message_codec":"websocket.v1"});
        let callback: NativeCallback = serde_json::from_value(input).unwrap();
        assert_eq!(callback.message_codec.as_deref(), Some("websocket.v1"));
        let input = json!({"binding":"direct", "path":"callback.so", "package":"trusted.metadata", "hooks":[], "message_codec":true});
        assert!(serde_json::from_value::<NativeCallback>(input).is_err());
    }

    #[test]
    fn tool_plugins_are_not_worker_metadata_callbacks() {
        let input = json!({"binding":"direct", "path":"callback.so","package":"trusted.metadata", "kind":"tool", "hooks":["pre_routing"]});
        assert!(serde_json::from_value::<NativeCallback>(input).is_err());
    }

    fn configured(callback: serde_json::Value) -> Config {
        serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "callbacks":[callback]}))
            .unwrap()
    }
    fn selected_callback() -> serde_json::Value {
        json!({"binding":"direct", "path":"callback.so", "package":"trusted.metadata", "hooks":["pre_routing"]})
    }
    fn action_callback() -> serde_json::Value {
        json!({"binding":"direct", "path":"carrier.so", "package":"trusted.carrier", "actions":[{
            "method":"peer.open",
            "contract":{"id":"carrier.peer.open", "version":1, "response":"unary",
                "input":{"type":"json"}, "output":{"type":"json"}, "max_input":1024,"max_output":1024},
            "bindings":[{"transport":"http", "method":"POST", "path":"/carrier/peer/open"}]
        }]})
    }

    #[test]
    fn http2_configuration_defaults_and_validates_native_ping_interval() {
        let defaults: Config =
            serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080"})).unwrap();
        assert!(!defaults.http2.enabled);
        assert!(defaults.http2.ping_interval_ms.is_none());
        for (http2, valid) in [
            (json!({"enabled":true}), true),
            (json!({"enabled":true,"ping_interval_ms":30_000}), true),
            (json!({"enabled":true,"ping_interval_ms":0}), false),
            (json!({"ping_interval_ms":30_000}), false),
        ] {
            let config: Config =
                serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "http2":http2}))
                    .unwrap();
            assert_eq!(config.validate().is_ok(), valid);
        }
    }

    #[test]
    fn callback_defaults_keep_native_execution_in_process_and_queues_finite() {
        let config = configured(selected_callback());
        config.validate().unwrap();
        let callback = &config.callbacks[0];
        assert!(callback.execution.is_none());
        assert_eq!(callback.scheduling, CallbackScheduling::Blocking);
        assert_eq!(callback.instances, 1);
        assert_eq!(callback.max_inflight, 32);
        assert_eq!(callback.max_waiting, 32);
        assert_eq!(callback.startup_timeout_ms, 5000);
        assert_eq!(callback.shutdown_timeout_ms, 5000);
        assert!(!callback.message_responses);
        assert_eq!(config.inspection.max_requests, 128);
        assert_eq!(config.inspection.max_buffered_bytes, 256 * 1024 * 1024);
        let mut explicit = selected_callback();
        explicit["execution"] = json!("in_process");
        explicit["max_waiting"] = json!(0);
        let config = configured(explicit);
        config.validate().unwrap();
        assert!(matches!(
            config.callbacks[0].execution,
            Some(CallbackExecution::InProcess)
        ));
    }

    #[test]
    fn notifier_configuration_rejects_process_splitting() {
        let mut callback = selected_callback();
        callback["execution"] = json!("isolated");
        assert!(
            serde_json::from_value::<Config>(
                json!({"upstream":"http://127.0.0.1:9", "callbacks":[callback]})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<Config>(
                json!({"upstream":"http://127.0.0.1:9", "native_helper":"worker"})
            )
            .is_err()
        );
    }

    #[test]
    fn callback_scheduling_is_explicit_and_independent_of_native_kind() {
        for kind in ["object", "function"] {
            for (scheduling, expected) in [
                ("blocking", CallbackScheduling::Blocking),
                ("inline", CallbackScheduling::Inline),
            ] {
                let mut callback = selected_callback();
                callback["kind"] = json!(kind);
                callback["scheduling"] = json!(scheduling);
                let config = configured(callback);
                config.validate().unwrap();
                assert_eq!(config.callbacks[0].scheduling, expected);
            }
        }
        let mut callback = selected_callback();
        callback["scheduling"] = json!("unbounded");
        assert!(serde_json::from_value::<NativeCallback>(callback).is_err());
    }

    #[test]
    fn callback_queue_pool_and_lifecycle_limits_are_checked_before_loading() {
        for (field, value) in [
            ("max_inflight", 0),
            ("max_inflight", 1025),
            ("max_waiting", 1025),
            ("instances", 0),
            ("instances", 65),
            ("timeout_ms", 0),
            ("startup_timeout_ms", 0),
            ("startup_timeout_ms", 300001),
            ("shutdown_timeout_ms", 0),
            ("shutdown_timeout_ms", 300001),
        ] {
            let mut callback = selected_callback();
            callback[field] = json!(value);
            assert!(configured(callback).validate().is_err(), "{field}={value}");
        }
        let mut callback = selected_callback();
        callback["package"] = json!("a".repeat(257));
        assert!(configured(callback).validate().is_err());
    }

    #[test]
    fn inspection_limits_require_one_full_reservation_and_reject_unknown_fields() {
        for (field, value) in [
            ("max_requests", 0),
            ("max_requests", 4097),
            ("max_buffered_bytes", 0),
            ("max_buffered_bytes", 1073741825u64),
            ("max_message_bytes", 0),
            ("max_message_bytes", 33554433),
            ("message_timeout_ms", 0),
            ("message_timeout_ms", 300001),
        ] {
            let config: Config = serde_json::from_value(
                json!({"upstream":"http://127.0.0.1:8080", "inspection":{field:value}}),
            )
            .unwrap();
            assert!(config.validate().is_err(), "{field}={value}");
        }
        let mut config = configured(selected_callback());
        let reservation = 8 * config.inspection.max_message_bytes + 8 * 1024 * 1024 + 512 * 1024;
        config.inspection.max_buffered_bytes = reservation - 1;
        assert!(config.validate().unwrap_err().contains("reservation"));
        config.inspection.max_buffered_bytes = reservation;
        config.validate().unwrap();
        assert!(
            serde_json::from_value::<Config>(
                json!({"upstream":"http://127.0.0.1:8080", "inspection":{"unknown":1}})
            )
            .is_err()
        );
    }

    #[test]
    fn response_and_extension_framing_grants_must_be_explicit_and_match() {
        let mut callback = selected_callback();
        callback["message_responses"] = json!(true);
        assert!(configured(callback.clone()).validate().is_err());
        callback["message_codec"] = json!("websocket.v1");
        configured(callback).validate().unwrap();
        let mut callback = json!({"binding":"direct", "path":"codec.so", "package":"trusted.codec", "message_codec":"custom.chat.v1",
            "codec_descriptor":{"id":"custom.chat.v1", "routes":[{"method":"POST","path":"/custom/chat","carrier":"http_json"}], "response_json":true}});
        assert!(configured(callback.clone()).validate().is_err());
        callback["message_responses"] = json!(true);
        configured(callback.clone()).validate().unwrap();
        callback["message_codec"] = json!("custom.other.v1");
        assert!(configured(callback).validate().is_err());
    }

    #[test]
    fn native_actions_require_explicit_loopback_bindings_bounded_unary_contracts_and_one_instance()
    {
        let callback = action_callback();
        assert!(configured(callback.clone()).validate().is_err());
        let config: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:6199"}, "callbacks":[callback.clone()]})).unwrap();
        config.validate().unwrap();
        let mut invalid = callback.clone();
        invalid["instances"] = json!(2);
        let mut config: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:6199"}, "callbacks":[invalid]})).unwrap();
        assert!(config.validate().is_err());
        config.callbacks[0].instances = 1;
        config.callbacks[0].actions[0].contract.response =
            action_execution::ResponseMode::ServerStream;
        assert!(config.validate().is_err());
        config.callbacks[0].actions[0].contract.response = action_execution::ResponseMode::Unary;
        config.callbacks[0].actions[0].contract.max_output = 1024 * 1024 + 1;
        assert!(config.validate().is_err());
        let duplicate: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:6199"}, "callbacks":[callback.clone(),callback]})).unwrap();
        assert!(duplicate.validate().unwrap_err().contains("unique"));
        let unix: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"unix:///run/sub2api/actions.sock"}})).unwrap();
        unix.validate().unwrap();
        let external: Config = serde_json::from_value(
            json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"0.0.0.0:6199"}}),
        )
        .unwrap();
        assert!(external.validate().unwrap_err().contains("loopback"));
    }
    #[test]
    fn native_actions_require_a_binding_reachable_through_selected_management_protocol() {
        for (protocol, bindings, accepted) in [
            (
                "http",
                json!([{"transport":"http", "method":"POST", "path":"/native"}]),
                true,
            ),
            (
                "http",
                json!([{"transport":"grpc", "action":"native"}]),
                true,
            ),
            (
                "http",
                json!([{"transport":"local_control", "action":"native"}]),
                false,
            ),
            (
                "local_control",
                json!([{"transport":"http", "method":"POST", "path":"/native"}]),
                false,
            ),
            (
                "local_control",
                json!([{"transport":"grpc", "action":"native"}]),
                false,
            ),
            (
                "local_control",
                json!([{"transport":"local_control", "action":"native"}]),
                true,
            ),
            (
                "local_control",
                json!([{"transport":"grpc", "action":"native"}, {"transport":"local_control", "action":"native"}]),
                true,
            ),
            (
                "http",
                json!([{"transport":"grpc", "action":"native"}, {"transport":"local_control", "action":"native"}]),
                true,
            ),
        ] {
            let mut callback = action_callback();
            callback["actions"][0]["bindings"] = bindings;
            let config: Config = serde_json::from_value(json!({
                "upstream":"http://127.0.0.1:8080",
                "management":{"listen":"unix:///run/worker.sock", "protocol":protocol},
                "callbacks":[callback],
            }))
            .unwrap();
            let result = config.validate();
            assert_eq!(result.is_ok(), accepted, "{protocol}: {result:?}");
            if !accepted {
                assert!(result.unwrap_err().contains("no binding reachable"));
            }
        }
    }

    #[test]
    fn each_native_action_needs_its_own_reachable_binding() {
        let mut callback = action_callback();
        let mut second = callback["actions"][0].clone();
        second["method"] = json!("second");
        second["contract"]["id"] = json!("second");
        second["bindings"] = json!([{"transport":"local_control", "action":"second"}]);
        callback["actions"].as_array_mut().unwrap().push(second);
        let config: Config = serde_json::from_value(json!({
            "upstream":"http://127.0.0.1:8080",
            "management":{"listen":"unix:///run/worker.sock"},
            "callbacks":[callback],
        }))
        .unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("no binding reachable")
        );
    }

    #[test]
    fn runtime_graph_checks_selected_object_bindings_cycles_and_bounds() {
        let callback = json!({"binding":"direct", "path":"agent.so", "package":"agent", "hooks":["pre_ingress"], "graph": {
            "callback_instance":"agent", "instances":[
                {"id":"agent", "path":"agent.so", "package":"agent", "kind":"object", "provides":["invoke"], "requires":[{"instance":"store", "capability":"read"}]},
                {"id":"store", "path":"store.so", "package":"store", "kind":"tool", "provides":["read"]}
            ]
        }});
        configured(callback.clone()).validate().unwrap();
        for mutate in 0..5 {
            let mut bad = callback.clone();
            match mutate {
                0 => bad["graph"]["callback_instance"] = json!("store"),
                1 => bad["graph"]["instances"][0]["requires"][0]["capability"] = json!("ungranted"),
                2 => {
                    bad["graph"]["instances"][1]["requires"] =
                        json!([{"instance":"agent", "capability":"invoke"}])
                }
                3 => bad["graph"]["instances"][0]["config"] = json!({"different":true}),
                _ => bad["instances"] = json!(64),
            }
            assert!(configured(bad).validate().is_err());
        }
    }
}

#[cfg(test)]
mod plugin_ingress_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn plugin_control_selects_only_an_explicit_same_host_facility() {
        for input in [
            json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:9191"}, "plugin_ingress":{"listen":"127.0.0.1:9191"}}),
            json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"unix:///run/worker.sock"}, "plugin_ingress":{"listen":"unix:///run/worker.sock"}}),
            json!({"upstream":"http://127.0.0.1:8080", "plugin_ingress":{"listen":"0.0.0.0:9191"}}),
        ] {
            assert!(
                serde_json::from_value::<Config>(input)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        for input in [
            json!({"upstream":"http://127.0.0.1:8080", "axis_bindings":{"policy":{"endpoint":"127.0.0.1:9191"}}}),
            json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"unix:///run/worker.sock"}, "plugin_ingress":{"facility":"management"}}),
            json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:9191", "protocol":"local_control"}}),
        ] {
            assert!(
                serde_json::from_value::<Config>(input)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        let config: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"unix:///run/worker.sock"}, "plugin_ingress":{"listen":"unix:///run/plugin.sock"}})).unwrap();
        config.validate().unwrap();
        let shared: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"unix:///run/worker.sock", "protocol":"local_control"}, "plugin_ingress":{"facility":"management"}})).unwrap();
        shared.validate().unwrap();
        let missing: Config = serde_json::from_value(
            json!({"upstream":"http://127.0.0.1:8080", "plugin_ingress":{"facility":"management"}}),
        )
        .unwrap();
        assert!(
            missing
                .validate()
                .unwrap_err()
                .contains("requires management")
        );
        for ingress in [
            json!({"facility":"management", "listen":"unix:///run/plugin.sock"}),
            json!({"facility":"unpublished"}),
            json!({"listen":"unix:///run/plugin.sock", "unknown":true}),
        ] {
            assert!(
                serde_json::from_value::<Config>(
                    json!({"upstream":"http://127.0.0.1:8080", "plugin_ingress":ingress})
                )
                .is_err()
            );
        }
    }
}
