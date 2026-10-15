//! Plugin control entry: decode a selection and dispatch to its owning manager.
use crate::remote_axis::RemotePluginManager;
use action_execution::{
    Action, AsyncHandler, Binding, Context, Contract, Error, ResponseMode, Schema,
};
use serde::Deserialize;
use std::sync::Arc;

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
            bindings: vec![Binding::LocalControl {
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
