//! Assembly selects carrier facilities and their reversible listener ownership.
//! Entry handlers contain no socket, mount or shutdown owner.
mod publications;
use action_router::RouteFacility;
use network_actions::listener::ActionListener;
use network_local_control::listener::ControlListener;
use network_sockets::{BoundListener, LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy};
use plugin_lifecycle::{Composition, Resource, Reversal};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

pub struct ControlServer {
    listener: Listener,
    facility: RouteFacility,
}
enum Listener {
    Http(Arc<ActionListener>),
    Local(Arc<ControlListener>),
}
impl ControlServer {
    /// Actual TCP address of the optional HTTP facility; UDS has no TCP address.
    pub fn local_address(&self) -> Option<std::net::SocketAddr> {
        match &self.listener {
            Listener::Http(listener) => listener.local_address(),
            Listener::Local(_) => None,
        }
    }
    pub fn activate(&self) -> Result<(), String> {
        match &self.listener {
            Listener::Http(listener) => listener.activate(),
            Listener::Local(listener) => listener.activate(),
        }
    }
    fn request_stop(&self) {
        match &self.listener {
            Listener::Http(listener) => listener.request_stop(),
            Listener::Local(listener) => listener.request_stop(),
        }
    }
    async fn shutdown(&self) -> Result<(), String> {
        match &self.listener {
            Listener::Http(listener) => listener.shutdown().await,
            Listener::Local(listener) => listener.shutdown().await,
        }
    }
}
/// Assembly choices. The carrier receives only handlers and transport bounds.
pub struct Registration {
    pub protocol: crate::config::ControlProtocol,
    pub worker_control: Option<Arc<proxy_pingora::worker::WorkerControl>>,
    pub logging_health: Option<diagnostic_output::runtime::LoggingHealth>,
    pub endpoint: LocalEndpoint,
    pub permissions: SocketPermissions,
}

pub async fn install(
    composition: &mut Composition,
    resource: Resource,
    registration: Registration,
    operations: Arc<diagnostic_output::operations::Operations>,
) -> Result<Arc<ControlServer>, String> {
    let actions = registration.worker_control.as_ref().map(|control| {
        crate::entry::worker::actions(control, &operations, registration.logging_health.clone())
    });
    install_selected(
        composition,
        resource,
        registration,
        actions.map(|actions| ("worker", actions)),
    )
    .await
}

pub(crate) async fn install_plugins(
    composition: &mut Composition,
    resource: Resource,
    listen: String,
    manager: Arc<crate::remote_axis::RemotePluginManager>,
) -> Result<Arc<ControlServer>, String> {
    install_selected(
        composition,
        resource,
        Registration {
            protocol: crate::config::ControlProtocol::LocalControl,
            worker_control: None,
            logging_health: None,
            endpoint: LocalEndpoint::Unix(crate::remote_axis::control_path(&listen)?),
            permissions: SocketPermissions::default(),
        },
        Some(("plugin", crate::entry::plugins::actions(manager))),
    )
    .await
}

async fn install_selected(
    composition: &mut Composition,
    resource: Resource,
    registration: Registration,
    actions: Option<(&'static str, Vec<action_execution::Action>)>,
) -> Result<Arc<ControlServer>, String> {
    let (send, receive) = oneshot::channel();
    composition
        .install(resource, async move {
            let (bound, socket) = network_sockets::bind_local(
                registration.endpoint,
                registration.permissions,
                StaleUnixSocketPolicy::RemoveIfStale,
            )
            .await?;
            let (facility, listener) = match registration.protocol {
                crate::config::ControlProtocol::Http => {
                    let facility = RouteFacility::new();
                    let service = network_actions::ActionService::new(
                        Arc::new(facility.router()),
                        1024 * 1024,
                        Duration::from_secs(120),
                    )
                    .and_then(|service| service.with_request_limit(128))
                    .map_err(|error| error.to_string())?;
                    let listener = Arc::new(ActionListener::prepare(bound, socket, service)?);
                    (facility, Listener::Http(listener))
                }
                crate::config::ControlProtocol::LocalControl => {
                    let BoundListener::Unix(bound) = bound else {
                        return Err("local control requires a Unix listener".into());
                    };
                    let facility = RouteFacility::new();
                    let service = network_local_control::ControlService::new(
                        Arc::new(facility.router()),
                        65536,
                        Duration::from_secs(30),
                        128,
                    )
                    .map_err(|error| error.to_string())?;
                    let listener = Arc::new(ControlListener::prepare(bound, socket, service)?);
                    (facility, Listener::Local(listener))
                }
            };
            if let Some((namespace, actions)) = actions {
                facility
                    .publish_actions(namespace, actions)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let server = Arc::new(ControlServer { listener, facility });
            let _ = send.send(Arc::clone(&server));
            let owner = Arc::clone(&server);
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    let cleanup = owner.facility.close();
                    owner.request_stop();
                    let served = owner.shutdown().await;
                    cleanup.await.map_err(|error| error.to_string())?;
                    served
                })
            });
            Ok((server, inverse))
        })
        .await?;
    receive.await.map_err(|error| error.to_string())
}
