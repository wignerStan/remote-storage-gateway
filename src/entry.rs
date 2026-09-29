//! Reversible shared management entry owned by the plugin composition.
mod publications;
mod socket;
use action_router::Router;
use network_actions::ActionService;
use network_sockets::{BoundListener, LocalEndpoint, SocketPermissions};
use plugin_lifecycle::{Composition, Resource, Reversal};
pub use publications::AxisRoutes;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

pub struct SharedEntry {
    activation: Mutex<Option<oneshot::Sender<()>>>,
    router: Router,
    local_address: Option<std::net::SocketAddr>,
    publications: Arc<Mutex<publications::State>>,
}
impl SharedEntry {
    /// Actual TCP address, including a kernel-selected port when configured as zero.
    pub fn local_address(&self) -> Option<std::net::SocketAddr> {
        self.local_address
    }

    pub fn activate(&self) -> Result<(), String> {
        self.activation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or("action entry already activated")?
            .send(())
            .map_err(|_| "action entry stopped before activation".into())
    }
}
/// Application-selected contracts are independent of the worker's notifier surface.
pub struct Registration {
    pub control: Arc<proxy_pingora::worker::WorkerControl>,
    pub endpoint: LocalEndpoint,
    pub permissions: SocketPermissions,
}

pub async fn install(
    composition: &mut Composition,
    resource: Resource,
    registration: Registration,
) -> Result<Arc<SharedEntry>, String> {
    let (send, receive) = oneshot::channel();
    composition
        .install(resource, async move {
            let Registration {
                control,
                endpoint,
                permissions,
            } = registration;
            // Bind before registering anything; existing sockets/files are never removed.
            let (bound, socket) = socket::bind(endpoint, permissions).await?;
            let local_address = match &bound {
                BoundListener::Tcp(listener) => {
                    Some(listener.local_addr().map_err(|e| e.to_string())?)
                }
                BoundListener::Unix(_) => None,
            };
            let router = Router::new();
            let carrier = ActionService::new(router.clone(), 1024 * 1024, Duration::from_secs(120))
                .map_err(|e| e.to_string())?;
            let worker_actions = router
                .with_contract_namespace("worker")
                .map_err(|e| e.to_string())?
                .register(crate::management::actions(&control))
                .map_err(|e| e.to_string())?;
            let publications = publications::State::with_worker(worker_actions);
            let (start, started) = oneshot::channel();
            let (stop, mut stopped) = oneshot::channel();
            let task = tokio::spawn(async move {
                let _socket = socket;
                tokio::select! {
                    result = started => { if result.is_err() { return Ok(()); } }
                    _ = &mut stopped => return Ok(()),
                }
                match bound {
                    BoundListener::Tcp(listener) => {
                        network_actions::serve_local(listener, carrier, async {
                            let _ = stopped.await;
                        })
                        .await
                    }
                    BoundListener::Unix(listener) => {
                        network_actions::serve_unix(listener, carrier, async {
                            let _ = stopped.await;
                        })
                        .await
                    }
                }
                .map_err(|e| e.to_string())
            });
            let activation = Arc::new(SharedEntry {
                activation: Mutex::new(Some(start)),
                router,
                local_address,
                publications: Arc::clone(&publications),
            });
            let _ = send.send(activation.clone());
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    // Mark closed and revoke every owner before waiting on the listener.
                    let cleanup = publications::close_all(publications);
                    let _ = stop.send(());
                    let served = task
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(|result| result);
                    cleanup.await?;
                    served
                })
            });
            Ok((activation, inverse))
        })
        .await?;
    receive.await.map_err(|e| e.to_string())
}
