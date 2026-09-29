//! Reversible shared management entry owned by the plugin composition.
mod socket;
use action_router::Router;
use network_actions::ActionService;
use plugin_lifecycle::{Composition, Resource, Reversal};
use plugin_loading::{Graph, InstanceActions};
pub use socket::{ActionEndpoint, SocketPermissions};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

pub struct Activation(Mutex<Option<oneshot::Sender<()>>>);
impl Activation {
    pub fn activate(&self) -> Result<(), String> {
        self.0
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
    pub graph: Arc<Graph>,
    pub control: Arc<proxy_pingora::worker::WorkerControl>,
    pub actions: Vec<InstanceActions>,
    pub endpoint: ActionEndpoint,
    pub permissions: SocketPermissions,
}

pub async fn install(
    composition: &mut Composition,
    resource: Resource,
    registration: Registration,
) -> Result<Arc<Activation>, String> {
    let (send, receive) = oneshot::channel();
    composition
        .install(resource, async move {
            let Registration {
                graph,
                control,
                actions,
                endpoint,
                permissions,
            } = registration;
            // Bind before registering anything; existing sockets/files are never removed.
            let (bound, socket) = socket::bind(endpoint, permissions).await?;
            let router = Router::new();
            let carrier = ActionService::new(router.clone(), 1024 * 1024, Duration::from_secs(120))
                .map_err(|e| e.to_string())?;
            let worker_actions = router
                .register(crate::management::actions(&control))
                .map_err(|e| e.to_string())?;
            if let Err(error) = graph.register_actions(&router, actions) {
                worker_actions.drain();
                return Err(error.to_string());
            }
            let (start, started) = oneshot::channel();
            let (stop, mut stopped) = oneshot::channel();
            let task = tokio::spawn(async move {
                let _socket = socket;
                tokio::select! {
                    result = started => { if result.is_err() { return Ok(()); } }
                    _ = &mut stopped => return Ok(()),
                }
                match bound {
                    socket::Bound::Tcp(listener) => {
                        network_actions::serve_local(listener, carrier, async {
                            let _ = stopped.await;
                        })
                        .await
                    }
                    socket::Bound::Unix(listener) => {
                        network_actions::serve_unix(listener, carrier, async {
                            let _ = stopped.await;
                        })
                        .await
                    }
                }
                .map_err(|e| e.to_string())
            });
            let activation = Arc::new(Activation(Mutex::new(Some(start))));
            let _ = send.send(activation.clone());
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    graph.withdraw_actions();
                    worker_actions.revoke();
                    let _ = stop.send(());
                    // Always drain registrations even when serving ended with an error.
                    let served = task
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(|result| result);
                    tokio::task::spawn_blocking(move || worker_actions.drain())
                        .await
                        .map_err(|e| e.to_string())?;
                    served
                })
            });
            Ok((activation, inverse))
        })
        .await?;
    receive.await.map_err(|e| e.to_string())
}
