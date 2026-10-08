//! Reversible shared management entry owned by the plugin composition.
mod publications;
use action_router::{Publication, PublicationRegistry, Router};
use network_actions::{ActionService, listener::ActionListener};
use network_sockets::{LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy};
use plugin_lifecycle::{Composition, Resource, Reversal};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

pub struct SharedEntry {
    listener: Arc<ActionListener>,
    router: Router,
    publications: PublicationRegistry,
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
            let router = Router::new();
            let carrier = ActionService::new(router.clone(), 1024 * 1024, Duration::from_secs(120))
                .map_err(|e| e.to_string())?;
            let worker_actions = worker_control
                .map(|control| {
                    router
                        .with_contract_namespace("worker")
                        .map_err(|e| e.to_string())?
                        .register(crate::management::actions(&control, &operations))
                        .map_err(|e| e.to_string())
                })
                .transpose()?;
            let publications = PublicationRegistry::new();
            if let Some(worker_actions) = worker_actions {
                publications
                    .install("worker", move || {
                        Ok(Arc::new(worker_actions) as Arc<dyn Publication>)
                    })
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let listener = Arc::new(ActionListener::prepare(bound, socket, carrier)?);
            let activation = Arc::new(SharedEntry {
                listener: Arc::clone(&listener),
                router,
                publications: publications.clone(),
            });
            let _ = send.send(activation.clone());
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    // Mark closed and revoke every owner before waiting on the listener.
                    let cleanup = publications.close_all();
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
