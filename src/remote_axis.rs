//! Worker metadata binding. Infra owns connections; the host owns axis grants.
use crate::config::{CallbackBackend, NativeCallback};
use network_actions::client::{Limits, RemoteBindings};
use network_sockets::LocalEndpoint;
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// A control-only RPC profile. Bodies and message payloads are not admitted.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxisBinding {
    pub endpoint: String,
    #[serde(default = "capacity")]
    pub max_inflight: usize,
    #[serde(default = "timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "timeout")]
    pub shutdown_timeout_ms: u64,
}
fn capacity() -> usize {
    32
}
fn timeout() -> u64 {
    5000
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
pub(crate) fn validate(bindings: &BTreeMap<String, AxisBinding>) -> Result<(), String> {
    if bindings.len() > 64 {
        return Err("at most 64 remote axes may be installed".into());
    }
    for (axis, binding) in bindings {
        if !identifier(axis)
            || !(1..=1024).contains(&binding.max_inflight)
            || !(1..=300_000).contains(&binding.connect_timeout_ms)
            || !(1..=300_000).contains(&binding.shutdown_timeout_ms)
        {
            return Err("invalid remote axis identity, capacity or timeout".into());
        }
        LocalEndpoint::parse(&binding.endpoint)?;
    }
    Ok(())
}
pub(crate) fn validate_callback(
    callback: &NativeCallback,
    bindings: &BTreeMap<String, AxisBinding>,
) -> Result<(), String> {
    let axis = callback
        .axis
        .as_ref()
        .ok_or("remote callback requires an axis")?;
    if !bindings.contains_key(axis) {
        return Err("remote callback axis is not configured".into());
    }
    if !callback.registration.as_deref().is_some_and(identifier) {
        return Err("remote callback requires a bounded registration identity".into());
    }
    if !callback.path.as_os_str().is_empty()
        || callback.graph.is_some()
        || callback.backend != CallbackBackend::Native
        || callback.wasm_limits.is_some()
        || callback.execution.is_some()
        || callback.kind.is_some()
        || !callback.config.is_null()
        || callback.instances != 1
    {
        return Err("remote metadata callbacks cannot configure a local artifact, graph, instance pool or initialization".into());
    }
    if callback.max_inflight != crate::config::callback_capacity()
        || callback.max_waiting != crate::config::callback_capacity()
        || callback.startup_timeout_ms != crate::config::callback_timeout()
        || callback.shutdown_timeout_ms != crate::config::callback_timeout()
        || callback.scheduling != crate::config::CallbackScheduling::Blocking
    {
        return Err("remote admission and connection limits belong to axis_bindings; native scheduling is not supported".into());
    }
    if callback.message_codec.is_some()
        || callback.codec_descriptor.is_some()
        || callback.message_responses
        || callback.message_actions
        || callback.message_proxy
        || !callback.actions.is_empty()
    {
        return Err(
            "remote metadata profile does not admit body codecs or native action publication"
                .into(),
        );
    }
    Ok(())
}

/// Stable host handles share one replaceable connection generation per axis.
/// Replacing a client changes no hook grant or participant identity.
#[derive(Default)]
pub struct RemoteAxisBindings {
    axes: BTreeMap<String, (Arc<RemoteBindings>, Duration)>,
}
impl RemoteAxisBindings {
    pub(crate) async fn connect(config: BTreeMap<String, AxisBinding>) -> Result<Self, String> {
        let mut result = Self::default();
        for (axis, binding) in config {
            let connected = RemoteBindings::connect(
                LocalEndpoint::parse(&binding.endpoint)?,
                Limits {
                    max_input: crate::native_notifier::FRAME_LIMIT,
                    max_output: crate::native_notifier::FRAME_LIMIT,
                    max_inflight: binding.max_inflight,
                    connect_timeout: Duration::from_millis(binding.connect_timeout_ms),
                },
            )
            .await;
            match connected {
                Ok(client) => {
                    result.axes.insert(
                        axis,
                        (
                            Arc::new(client),
                            Duration::from_millis(binding.shutdown_timeout_ms),
                        ),
                    );
                }
                Err(error) => {
                    let _ = result.close().await;
                    return Err(format!("remote axis connection failed: {error}"));
                }
            }
        }
        Ok(result)
    }
    pub(crate) fn client(&self, axis: &str) -> Result<Arc<RemoteBindings>, String> {
        self.axes
            .get(axis)
            .map(|(client, _)| Arc::clone(client))
            .ok_or_else(|| "remote axis is absent".into())
    }
    /// Connect first, then replace atomically. Admitted calls finish on the old client.
    pub async fn replace(&self, axis: &str, endpoint: &str) -> Result<(), String> {
        self.client(axis)?
            .replace(LocalEndpoint::parse(endpoint)?)
            .await
            .map_err(|e| e.to_string())
    }
    pub(crate) async fn close(&self) -> Result<(), String> {
        let mut pending = tokio::task::JoinSet::new();
        for (client, timeout) in self.axes.values() {
            let client = Arc::clone(client);
            let timeout = *timeout;
            pending.spawn(async move { client.close(timeout).await });
        }
        let mut errors = Vec::new();
        while let Some(result) = pending.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error.to_string()),
                Err(error) => errors.push(error.to_string()),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

/// One method namespace names the host's versioned metadata contract.
pub(crate) fn method(registration: &str, hook: &str) -> String {
    format!("worker.metadata.v1.{registration}.{hook}")
}
