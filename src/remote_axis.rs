//! Remote plugin membership and its outbound RPC binding.
//! Plugin ingress selects configured grants; the notifier interprets hook decisions.
pub(crate) mod surface;
#[cfg(test)]
mod tests;
use crate::config::{CallbackBackend, CallbackBinding, NativeCallback};
use event_stamp::StampSource;
use network_local_control::client::{Limits, RemoteBindings};
use network_sockets::LocalEndpoint;
use plugin_lifecycle::Reversal;
use plugin_lifecycle::manager::{PluginManager, RegistrationSpec};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use surface::{Grant, Surface};

/// An explicitly bounded local RPC profile for metadata and selected codec messages.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxisBinding {
    pub endpoint: String,
    #[serde(default = "capacity")]
    pub max_inflight: usize,
    #[serde(default = "message_limit")]
    pub max_message_bytes: usize,
    #[serde(default = "timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "timeout")]
    pub shutdown_timeout_ms: u64,
}
fn message_limit() -> usize {
    64 * 1024
}
fn capacity() -> usize {
    32
}
fn timeout() -> u64 {
    5000
}
pub(crate) fn control_path(endpoint: &str) -> Result<std::path::PathBuf, String> {
    match LocalEndpoint::parse(endpoint)? {
        LocalEndpoint::Unix(path) => Ok(path),
        LocalEndpoint::Tcp(_) => Err("plugin control requires a same-host Unix socket".into()),
    }
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
            || !(1..=32 * 1024 * 1024).contains(&binding.max_message_bytes)
            || !(1..=300_000).contains(&binding.connect_timeout_ms)
            || !(1..=300_000).contains(&binding.shutdown_timeout_ms)
        {
            return Err("invalid remote axis identity, capacity or timeout".into());
        }
        control_path(&binding.endpoint)?;
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
        return Err("remote callbacks cannot configure a local artifact, graph, instance pool or initialization".into());
    }
    if callback.max_inflight != crate::config::callback_capacity()
        || callback.max_waiting != crate::config::callback_capacity()
        || callback.startup_timeout_ms != crate::config::callback_timeout()
        || callback.shutdown_timeout_ms != crate::config::callback_timeout()
        || callback.scheduling != crate::config::CallbackScheduling::Blocking
    {
        return Err("remote admission and connection limits belong to axis_bindings; native scheduling is not supported".into());
    }
    if !callback.actions.is_empty() {
        return Err("remote callbacks cannot publish arbitrary native actions".into());
    }
    Ok(())
}

/// Interpret configuration once before constructing the pure publication surface.
pub(crate) fn publication(callbacks: &[NativeCallback]) -> Result<Surface, String> {
    Surface::new(
        callbacks
            .iter()
            .filter(|c| c.resolved_binding() == CallbackBinding::Remote)
            .map(|callback| {
                Ok(Grant::new(
                    callback.axis.clone().ok_or("remote axis is absent")?,
                    callback
                        .registration
                        .clone()
                        .ok_or("remote registration is absent")?,
                    &callback.hooks,
                )
                .with_codec(callback.message_codec.clone()))
            })
            .collect::<Result<Vec<_>, String>>()?,
    )
}

/// One installed participant pins the shared outbound connection owner.
struct Participant {
    client: Arc<RemoteBindings>,
}
struct Axis {
    client: Arc<RemoteBindings>,
    members: PluginManager<Participant>,
    shutdown_timeout: Duration,
    max_message_bytes: usize,
}

/// Remote plugin membership, independent of the worker management entry.
/// Infra owns registration generations and draining; this owner selects configured grants.
#[derive(Default)]
pub struct RemotePluginManager {
    axes: BTreeMap<String, Arc<Axis>>,
    surface: Surface,
    incarnation: String,
}
#[derive(serde::Serialize)]
pub struct PluginState {
    pub incarnation: String,
    pub axis: String,
    pub registration: String,
    pub contract: &'static str,
    pub available: bool,
    pub generation: Option<u64>,
}
impl RemotePluginManager {
    pub(crate) async fn connect(
        config: BTreeMap<String, AxisBinding>,
        surface: Surface,
    ) -> Result<Self, String> {
        let mut result = Self {
            axes: BTreeMap::new(),
            surface,
            incarnation: event_stamp::SystemSource.new_id().to_string(),
        };
        for (axis, binding) in config {
            let connected = RemoteBindings::connect(
                control_path(&binding.endpoint)?,
                Limits {
                    max_input: binding
                        .max_message_bytes
                        .max(crate::native_notifier::FRAME_LIMIT)
                        + 258,
                    max_output: binding
                        .max_message_bytes
                        .max(crate::native_notifier::FRAME_LIMIT)
                        + 65541,
                    max_inflight: binding.max_inflight,
                    connect_timeout: Duration::from_millis(binding.connect_timeout_ms),
                },
            )
            .await;
            match connected {
                Ok(client) => {
                    result.axes.insert(
                        axis,
                        Arc::new(Axis {
                            client: Arc::new(client),
                            members: PluginManager::new(64)?,
                            shutdown_timeout: Duration::from_millis(binding.shutdown_timeout_ms),
                            max_message_bytes: binding.max_message_bytes,
                        }),
                    );
                }
                Err(error) => {
                    let _ = result.close().await;
                    return Err(format!("remote axis connection failed: {error}"));
                }
            }
        }
        for grant in result.surface.grants() {
            if let Err(error) = result.activate(&grant.axis, &grant.registration).await {
                let cleanup = result.close().await;
                return Err(format!(
                    "remote plugin installation failed: {error}; cleanup: {cleanup:?}"
                ));
            }
        }
        Ok(result)
    }
    pub(crate) fn check_incarnation(&self, incarnation: &str) -> Result<(), String> {
        if incarnation != self.incarnation {
            return Err("stale plugin manager incarnation".into());
        }
        Ok(())
    }
    fn axis(&self, name: &str) -> Result<&Arc<Axis>, String> {
        self.axes
            .get(name)
            .ok_or_else(|| "remote axis is absent".into())
    }
    pub(crate) fn binding(
        &self,
        axis: &str,
        registration: &str,
    ) -> Result<RemotePluginBinding, String> {
        let grant = self.surface.select(axis, registration)?;
        Ok(RemotePluginBinding {
            axis: Arc::clone(self.axis(axis)?),
            grant,
        })
    }
    /// Activate only the existing host publication. No remote grant or endpoint is accepted.
    pub async fn activate(&self, axis: &str, registration: &str) -> Result<PluginState, String> {
        self.surface.select(axis, registration)?;
        let axis_owner = self.axis(axis)?;
        let participant = Arc::new(Participant {
            client: Arc::clone(&axis_owner.client),
        });
        let installed = axis_owner
            .members
            .install(
                RegistrationSpec {
                    name: registration.into(),
                    contract: surface::CONTRACT.into(),
                },
                async move {
                    let inverse: Reversal = Box::new(|| Box::pin(async { Ok(()) }));
                    Ok((participant, inverse))
                },
            )
            .await?;
        Ok(PluginState {
            incarnation: self.incarnation.clone(),
            axis: axis.into(),
            registration: registration.into(),
            contract: surface::CONTRACT,
            available: true,
            generation: Some(installed.identity().generation),
        })
    }
    pub fn state(&self, axis: &str, registration: &str) -> Result<PluginState, String> {
        self.surface.select(axis, registration)?;
        let active = self.axis(axis)?.members.resolve(registration).ok();
        Ok(PluginState {
            incarnation: self.incarnation.clone(),
            axis: axis.into(),
            registration: registration.into(),
            contract: surface::CONTRACT,
            available: active.is_some(),
            generation: active.map(|r| r.identity().generation),
        })
    }
    /// Fence an exact registration incarnation before joining its local exchanges.
    pub async fn withdraw(
        &self,
        axis: &str,
        registration: &str,
        generation: u64,
    ) -> Result<(), String> {
        self.surface.select(axis, registration)?;
        let members = &self.axis(axis)?.members;
        let selected = members.resolve(registration)?;
        if selected.identity().generation != generation {
            return Err("stale plugin registration generation".into());
        }
        members
            .withdraw(selected.identity())
            .await
            .map_err(|errors| errors.join("; "))
    }
    /// Connect first; transport replacement changes no grant or participant incarnation.
    /// Pending exchanges stay on their old connection; new calls use the replacement.
    /// Connection-local peer request state does not migrate or replay. Stateful
    /// participants must reject later calls whose request state is absent.
    /// This method is host-controlled and is not published by the plugin ingress.
    pub async fn replace(&self, axis: &str, endpoint: &str) -> Result<(), String> {
        self.axis(axis)?
            .client
            .replace(control_path(endpoint)?)
            .await
            .map_err(|e| e.to_string())
    }
    pub(crate) async fn close(&self) -> Result<(), String> {
        let mut pending = tokio::task::JoinSet::new();
        for axis in self.axes.values() {
            let axis = Arc::clone(axis);
            pending.spawn(async move {
                let membership =
                    tokio::time::timeout(axis.shutdown_timeout, axis.members.close()).await;
                let transport = axis.client.close(axis.shutdown_timeout).await;
                let mut errors = match membership {
                    Ok(Ok(())) => Vec::new(),
                    Ok(Err(errors)) => errors,
                    Err(_) => vec!["remote plugin memberships still draining".into()],
                };
                if let Err(error) = transport {
                    errors.push(error.to_string());
                }
                errors
            });
        }
        let mut errors = Vec::new();
        while let Some(result) = pending.join_next().await {
            match result {
                Ok(failures) => errors.extend(failures),
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

/// A stable slot handle. Each call pins the currently admitted incarnation.
#[derive(Clone)]
pub(crate) struct RemotePluginBinding {
    axis: Arc<Axis>,
    grant: Arc<Grant>,
}
impl RemotePluginBinding {
    pub async fn call(
        &self,
        hook: &str,
        input: Vec<u8>,
        timeout: Duration,
        custody: impl Send + 'static,
    ) -> Result<Vec<u8>, String> {
        if input.len() > crate::native_notifier::FRAME_LIMIT {
            return Err("remote metadata exceeds frame limit".into());
        }
        let method = self.grant.method(hook)?.to_owned();
        let reply = self.exchange(method, input, timeout, custody).await?;
        if reply.len() > crate::native_notifier::FRAME_LIMIT {
            return Err("remote metadata reply exceeds frame limit".into());
        }
        Ok(reply)
    }
    pub async fn message(
        &self,
        method: &str,
        input: bytes::Bytes,
        timeout: Duration,
        custody: impl Send + 'static,
    ) -> Result<Vec<u8>, String> {
        if input.len() > self.axis.max_message_bytes {
            return Err("remote message exceeds axis payload limit".into());
        }
        let binding = self.grant.message_method(method)?;
        let mut envelope = Vec::with_capacity(2 + method.len() + input.len());
        envelope.extend_from_slice(&(method.len() as u16).to_be_bytes());
        envelope.extend_from_slice(method.as_bytes());
        envelope.extend_from_slice(&input);
        self.exchange(binding, envelope, timeout, custody).await
    }
    async fn exchange(
        &self,
        method: String,
        input: Vec<u8>,
        timeout: Duration,
        custody: impl Send + 'static,
    ) -> Result<Vec<u8>, String> {
        let participant = self
            .axis
            .members
            .resolve(&self.grant.registration)?
            .enter()?;

        // Retain membership until the owned local exchange actually settles, even
        // when the caller stops waiting. A peer timeout still has unknown outcome.
        tokio::spawn(async move {
            let client = Arc::clone(&participant.client);
            client
                .call_with_custody(method, input, timeout, (participant, custody))
                .await
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("remote plugin exchange task failed: {e}"))?
    }
}
