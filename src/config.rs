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
    pub logging: Logging,
    #[serde(default)]
    pub callbacks: Vec<NativeCallback>,
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
    pub hooks: Vec<String>,
    /// Header metadata explicitly exposed to this trusted callback.
    #[serde(default)]
    pub headers: Vec<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default = "callback_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "callback_capacity")]
    pub max_inflight: usize,
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
        for callback in &self.callbacks {
            if callback.path.as_os_str().is_empty()
                || callback.package.is_empty()
                || callback.variant.is_empty()
                || !(1..=1024).contains(&callback.max_inflight)
                || !(1..=300_000).contains(&callback.timeout_ms)
            {
                return Err("invalid native callback identity, capacity or timeout".into());
            }
            crate::native_notifier::validate_hooks(&callback.hooks)?;
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
        }
    }

    #[test]
    fn tool_plugins_are_not_worker_metadata_callbacks() {
        let input = json!({"path":"callback.so","package":"trusted.metadata", "kind":"tool", "hooks":["pre_routing"]});
        assert!(serde_json::from_value::<NativeCallback>(input).is_err());
    }
}
