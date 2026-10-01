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
    #[serde(default)]
    pub inspection: Inspection,
    /// Executable implementing the private callback helper entry. `run` defaults
    /// this to the worker executable; library assembly requires an explicit path.
    #[serde(default)]
    pub native_helper: Option<PathBuf>,
    #[serde(default)]
    pub management: Option<Management>,
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
    pub listen: SocketAddr,
}

/// A host-selected transport and schema grant; plugins cannot publish routes themselves.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAction {
    pub method: String,
    pub contract: action_router::Contract,
    pub bindings: Vec<action_router::Binding>,
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

/// Native code is explicitly selected by trusted process configuration.
/// Function and Object plugins expose the worker's bounded metadata contract.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCallback {
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
    pub execution: CallbackExecution,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackExecution {
    #[default]
    Isolated,
    /// Explicit full-trust opt-in. No crash or syscall isolation is provided.
    InProcess,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    #[default]
    Rust,
    Go,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackKind {
    Function,
    Object,
}
impl NativeCallback {
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
fn callback_timeout() -> u64 {
    5_000
}
fn callback_capacity() -> usize {
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
        if self
            .native_helper
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err("native helper executable path must be nonempty".into());
        }
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
        let mut action_bindings = std::collections::BTreeSet::new();
        for callback in &self.callbacks {
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
            if let Some(codec) = &callback.message_codec {
                if !crate::proxy::message_codec::valid_id(codec) {
                    return Err("invalid message codec identifier".into());
                }
            }
            if callback.path.as_os_str().is_empty()
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
                    || action.contract.response != action_router::ResponseMode::Unary
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
            if !management.listen.ip().is_loopback() {
                return Err("management.listen must use loopback".into());
            }
            if management.listen == self.listen {
                return Err("data and management listeners must differ".into());
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
            let mut input =
                json!({"path":"callback.so","package":"trusted.metadata","hooks":["pre_routing"]});
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
    fn message_access_requires_an_explicit_codec_grant() {
        let input = json!({"path":"callback.so", "package":"trusted.metadata", "hooks":[], "message_codec":"websocket.v1"});
        let callback: NativeCallback = serde_json::from_value(input).unwrap();
        assert_eq!(callback.message_codec.as_deref(), Some("websocket.v1"));
        let input = json!({"path":"callback.so", "package":"trusted.metadata", "hooks":[], "message_codec":true});
        assert!(serde_json::from_value::<NativeCallback>(input).is_err());
    }

    #[test]
    fn tool_plugins_are_not_worker_metadata_callbacks() {
        let input = json!({"path":"callback.so","package":"trusted.metadata", "kind":"tool", "hooks":["pre_routing"]});
        assert!(serde_json::from_value::<NativeCallback>(input).is_err());
    }

    fn configured(callback: serde_json::Value) -> Config {
        serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "callbacks":[callback]}))
            .unwrap()
    }
    fn selected_callback() -> serde_json::Value {
        json!({"path":"callback.so", "package":"trusted.metadata", "hooks":["pre_routing"]})
    }
    fn action_callback() -> serde_json::Value {
        json!({"path":"carrier.so", "package":"trusted.carrier", "actions":[{
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
    fn callback_defaults_keep_native_execution_isolated_and_queues_finite() {
        let config = configured(selected_callback());
        config.validate().unwrap();
        let callback = &config.callbacks[0];
        assert!(matches!(callback.execution, CallbackExecution::Isolated));
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
            CallbackExecution::InProcess
        ));
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
        let mut callback = json!({"path":"codec.so", "package":"trusted.codec", "message_codec":"custom.chat.v1",
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
            action_router::ResponseMode::ServerStream;
        assert!(config.validate().is_err());
        config.callbacks[0].actions[0].contract.response = action_router::ResponseMode::Unary;
        config.callbacks[0].actions[0].contract.max_output = 1024 * 1024 + 1;
        assert!(config.validate().is_err());
        let duplicate: Config = serde_json::from_value(json!({"upstream":"http://127.0.0.1:8080", "management":{"listen":"127.0.0.1:6199"}, "callbacks":[callback.clone(),callback]})).unwrap();
        assert!(duplicate.validate().unwrap_err().contains("unique"));
    }
}
