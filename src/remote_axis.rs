//! Remote plugin membership and its outbound RPC binding.
//! Plugin ingress calls this use-flow, which selects the pure axis surface.
pub mod entry;
pub(crate) mod surface;
#[cfg(test)]
mod tests;
use crate::config::{CallbackBackend, CallbackBinding, NativeCallback};
use event_stamp::StampSource;
use network_actions::client::{Limits, RemoteBindings};
use network_sockets::LocalEndpoint;
use plugin_lifecycle::Reversal;
use plugin_lifecycle::manager::{PluginManager, RegistrationSpec};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use surface::{Grant, Surface};

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
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
    )
}

/// One installed participant retains its immutable host-issued hook grant.
struct Participant {
    client: Arc<RemoteBindings>,
    grant: Arc<Grant>,
}
struct Axis {
    client: Arc<RemoteBindings>,
    members: PluginManager<Participant>,
    shutdown_timeout: Duration,
}

/// Remote plugin membership, independent of the worker management entry.
/// Infra owns registration generations and draining; this use-flow applies the surface.
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
                        Arc::new(Axis {
                            client: Arc::new(client),
                            members: PluginManager::new(64)?,
                            shutdown_timeout: Duration::from_millis(binding.shutdown_timeout_ms),
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
        let grant = self.surface.select(axis, registration)?;
        let axis_owner = self.axis(axis)?;
        let participant = Arc::new(Participant {
            client: Arc::clone(&axis_owner.client),
            grant,
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
    /// This method is host-controlled and is not published by the plugin ingress.
    pub async fn replace(&self, axis: &str, endpoint: &str) -> Result<(), String> {
        self.axis(axis)?
            .client
            .replace(LocalEndpoint::parse(endpoint)?)
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
        let participant = self
            .axis
            .members
            .resolve(&self.grant.registration)?
            .enter()?;
        let method = participant.grant.method(hook)?.to_owned();
        // Retain membership until the owned local exchange actually settles, even
        // when the caller stops waiting. A peer timeout still has unknown outcome.
        tokio::spawn(async move {
            let _custody = custody;
            participant
                .client
                .call(method, input, timeout)
                .await
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("remote plugin exchange task failed: {e}"))?
    }
}
