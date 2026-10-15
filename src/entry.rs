//! Reversible shared management entry owned by the plugin composition.
mod publications;
use network_actions::{facility::RpcFacility, listener::ActionListener};
use network_sockets::{LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy};
use plugin_lifecycle::{Composition, Resource, Reversal};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

pub struct SharedEntry {
    listener: Arc<ActionListener>,
    facility: RpcFacility,
}
impl SharedEntry {
    /// Actual TCP address, including a kernel-selected port when configured as zero.
    pub fn local_address(&self) -> Option<std::net::SocketAddr> {
        self.listener.local_address()
    }

    pub fn activate(&self) -> Result<(), String> {
        self.listener.activate()
    }
}
/// Application-selected contracts are independent of the worker's notifier surface.
pub struct Registration {
    /// Some installs worker management; None exposes only explicitly registered actions.
    pub worker_control: Option<Arc<proxy_pingora::worker::WorkerControl>>,
    /// Selected logging owner. None publishes no logging-health contracts.
    pub logging_health: Option<diagnostic_output::runtime::LoggingHealth>,
    pub endpoint: LocalEndpoint,
    pub permissions: SocketPermissions,
}

/// The caller supplies counters shared with the native proxy.
pub async fn install(
    composition: &mut Composition,
    resource: Resource,
    registration: Registration,
    operations: Arc<diagnostic_output::operations::Operations>,
) -> Result<Arc<SharedEntry>, String> {
    let (send, receive) = oneshot::channel();
    composition
        .install(resource, async move {
            let Registration {
                worker_control,
                logging_health,
                endpoint,
                permissions,
            } = registration;
            // Bind before registering routes; pathname ownership follows the socket primitive.
            let (bound, socket) = network_sockets::bind_local(
                endpoint,
                permissions,
                StaleUnixSocketPolicy::RemoveIfStale,
            )
            .await?;
            let facility = RpcFacility::new(1024 * 1024, Duration::from_secs(120), 128)
                .map_err(|error| error.to_string())?;
            if let Some(control) = worker_control {
                facility
                    .publish_actions(
                        "worker",
                        crate::management::actions(&control, &operations, logging_health),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let listener = Arc::new(ActionListener::prepare(bound, socket, facility.service())?);
            let activation = Arc::new(SharedEntry {
                listener: Arc::clone(&listener),
                facility: facility.clone(),
            });
            let _ = send.send(activation.clone());
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    // Mark closed and revoke every owner before waiting on the listener.
                    let cleanup = facility.close();
                    listener.request_stop();
                    let served = listener.shutdown().await;
                    cleanup.await.map_err(|error| error.to_string())?;
                    served
                })
            });
            Ok((activation, inverse))
        })
        .await?;
    receive.await.map_err(|e| e.to_string())
}
