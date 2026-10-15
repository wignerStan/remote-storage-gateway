//! Plugin-axis ingress. This router never mounts on the worker application entry.
use super::RemotePluginManager;
use action_router::{
    Action, AsyncHandler, Binding, Context, Contract, Error, Publication, PublicationRegistry,
    ResponseMode, Router, Schema,
};
use network_actions::{ActionService, listener::ActionListener};
use network_sockets::{LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy};
use plugin_lifecycle::{Composition, Resource, Reversal};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Dedicated Unix or loopback plugin endpoint, separate from worker management.
    pub listen: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    axis: String,
    registration: String,
    #[serde(default)]
    generation: Option<u64>,
    #[serde(default)]
    incarnation: Option<String>,
}
#[derive(Clone, Copy)]
enum Operation {
    Get,
    Activate,
    Withdraw,
}

fn actions(manager: Arc<RemotePluginManager>) -> Vec<Action> {
    [
        ("plugin.get", Operation::Get),
        ("plugin.activate", Operation::Activate),
        ("plugin.withdraw", Operation::Withdraw),
    ]
    .into_iter()
    .map(|(method, operation)| {
        let manager = Arc::clone(&manager);
        Action {
            contract: Contract {
                id: method.into(),
                version: 1,
                response: ResponseMode::Unary,
                input: Schema::Json,
                output: Schema::Json,
                max_input: 1024,
                max_output: 1024,
            },
            bindings: vec![Binding::Grpc {
                action: method.into(),
            }],
            handler: Arc::new(AsyncHandler(move |input: Vec<u8>, context: Context| {
                let manager = Arc::clone(&manager);
                async move {
                    context.remaining()?;
                    let request: Selection = serde_json::from_slice(&input)
                        .map_err(|_| Error::Invalid("invalid plugin selection".into()))?;
                    if request.axis.len() > 64 || request.registration.len() > 64 {
                        return Err(Error::Invalid("plugin identity limit".into()));
                    }
                    if !matches!(operation, Operation::Get) {
                        let incarnation = request.incarnation.as_deref().ok_or_else(|| {
                            Error::Invalid("mutation requires manager incarnation".into())
                        })?;
                        manager
                            .check_incarnation(incarnation)
                            .map_err(|_| Error::Conflict)?;
                    }
                    let state = match operation {
                        Operation::Get | Operation::Activate if request.generation.is_some() => {
                            return Err(Error::Invalid(
                                "generation is valid only for withdrawal".into(),
                            ));
                        }
                        Operation::Get => manager.state(&request.axis, &request.registration),
                        Operation::Activate => {
                            manager.activate(&request.axis, &request.registration).await
                        }
                        Operation::Withdraw => {
                            let generation = request.generation.ok_or_else(|| {
                                Error::Invalid("withdrawal requires exact generation".into())
                            })?;
                            manager
                                .withdraw(&request.axis, &request.registration, generation)
                                .await
                                .map_err(Error::Failed)?;
                            manager.state(&request.axis, &request.registration)
                        }
                    }
                    .map_err(Error::Failed)?;
                    serde_json::to_vec(&state)
                        .map_err(|_| Error::Failed("plugin state encoding".into()))
                }
            })),
        }
    })
    .collect()
}

pub struct PluginEntry {
    listener: Arc<ActionListener>,
}
impl PluginEntry {
    pub(crate) fn activate(&self) -> Result<(), String> {
        self.listener.activate()
    }
}

/// Bind a plugin-owned carrier and install only its manager operations.
/// The lifecycle resource depends on the manager, so ingress retires first.
pub(crate) async fn install(
    composition: &mut Composition,
    resource: Resource,
    config: Config,
    manager: Arc<RemotePluginManager>,
) -> Result<Arc<PluginEntry>, String> {
    let (send, receive) = tokio::sync::oneshot::channel();
    composition
        .install(resource, async move {
            let (bound, socket) = network_sockets::bind_local(
                LocalEndpoint::parse(&config.listen)?,
                SocketPermissions::default(),
                StaleUnixSocketPolicy::RemoveIfStale,
            )
            .await?;
            let router = Router::new()
                .with_contract_namespace("plugin")
                .map_err(|e| e.to_string())?;
            let carrier = ActionService::new(router.clone(), 1024, Duration::from_secs(30))
                .and_then(|s| s.with_request_limit(32))
                .map_err(|e| e.to_string())?;
            let publications = PublicationRegistry::new();
            let routes = router
                .register(actions(manager))
                .map_err(|e| e.to_string())?;
            publications
                .install("plugin", move || {
                    Ok(Arc::new(routes) as Arc<dyn Publication>)
                })
                .await
                .map_err(|e| e.to_string())?;
            let listener = Arc::new(ActionListener::prepare(bound, socket, carrier)?);
            let entry = Arc::new(PluginEntry {
                listener: Arc::clone(&listener),
            });
            let _ = send.send(Arc::clone(&entry));
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    let cleanup = publications.close_all();
                    listener.request_stop();
                    let served = listener.shutdown().await;
                    cleanup.await.map_err(|e| e.to_string())?;
                    served
                })
            });
            Ok((entry, inverse))
        })
        .await?;
    receive
        .await
        .map_err(|_| "plugin ingress setup cancelled".into())
}
