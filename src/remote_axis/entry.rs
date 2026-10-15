//! Plugin-axis decoding and dispatch, on a shared or dedicated RPC facility.
use super::RemotePluginManager;
use action_router::{
    Action, AsyncHandler, Binding, Context, Contract, Error, ResponseMode, Schema,
};
use network_actions::{facility::RpcFacility, listener::ActionListener};
use network_sockets::{LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy};
use plugin_lifecycle::{Composition, Resource, Reversal};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Config {
    /// Publish plugin controls on the configured management listener.
    Shared { mount: SharedMount },
    /// Bind a separate local listener when no shared server is selected.
    Dedicated { listen: String },
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedMount {
    Management,
}
impl Config {
    pub(crate) fn shared(&self) -> bool {
        matches!(self, Self::Shared { .. })
    }
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

pub(crate) fn actions(manager: Arc<RemotePluginManager>) -> Vec<Action> {
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
    listen: String,
    manager: Arc<RemotePluginManager>,
) -> Result<Arc<PluginEntry>, String> {
    let (send, receive) = tokio::sync::oneshot::channel();
    composition
        .install(resource, async move {
            let (bound, socket) = network_sockets::bind_local(
                LocalEndpoint::parse(&listen)?,
                SocketPermissions::default(),
                StaleUnixSocketPolicy::RemoveIfStale,
            )
            .await?;
            let facility = RpcFacility::new(1024, Duration::from_secs(30), 32)
                .map_err(|error| error.to_string())?;
            facility
                .publish_actions("plugin", actions(manager))
                .await
                .map_err(|error| error.to_string())?;
            let listener = Arc::new(ActionListener::prepare(bound, socket, facility.service())?);
            let entry = Arc::new(PluginEntry {
                listener: Arc::clone(&listener),
            });
            let _ = send.send(Arc::clone(&entry));
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    let cleanup = facility.close();
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
