//! Read-only operational event exposure, independent of request policy callbacks.
use action_execution::{
    Action, Binding, Context, Contract, Error, Field, JsonHandler, ResponseMode, Schema, Source,
    StreamHandler,
};
use admission_gate::budget::{Reservation, ResourceBudget};
use diagnostic_output::operations::{Operations, Updates};
use diagnostic_output::runtime::{LoggingHealth, LoggingUpdates};
use std::sync::Arc;

// Event streams must leave capacity for control calls on the shared 128-call entry.
pub(crate) const MAX_EVENT_SUBSCRIBERS: usize = 32;

fn output_schema() -> Schema {
    Schema::Object {
        fields: [
            "started",
            "inflight",
            "delivered",
            "failed",
            "completion_failed",
            "cancelled",
            "request_bytes",
            "response_bytes",
            "latency_count",
            "latency_ns",
        ]
        .into_iter()
        .map(|name| Field::required(name, Schema::Integer))
        .chain(std::iter::once(Field::required(
            "status_classes",
            Schema::Array {
                items: Box::new(Schema::Integer),
            },
        )))
        .collect(),
    }
}
pub(crate) fn actions(operations: &Arc<Operations>, logging: Option<LoggingHealth>) -> Vec<Action> {
    let metrics = Contract {
        id: "worker.metrics".into(),
        version: 1,
        response: ResponseMode::Unary,
        input: Schema::Object { fields: vec![] },
        output: output_schema(),
        max_input: 1024,
        max_output: 1024,
    };
    let events = Contract {
        id: "worker.events".into(),
        response: ResponseMode::ServerStream,
        input: Schema::Null,
        ..metrics.clone()
    };
    let reader = Arc::clone(operations);
    let publisher = Arc::clone(operations);
    let subscribers = ResourceBudget::new(MAX_EVENT_SUBSCRIBERS);
    let stream_budget = Arc::clone(&subscribers);
    let mut actions = vec![
        Action {
            contract: metrics,
            bindings: vec![Binding::Grpc {
                action: "worker.metrics".into(),
            }],
            handler: Arc::new(JsonHandler::<serde_json::Value, serde_json::Value, _>::new(
                move |_: serde_json::Value, _: &Context| Ok(reader.snapshot().to_json()),
            )),
        },
        Action {
            contract: events,
            bindings: vec![
                Binding::Grpc {
                    action: "worker.events".into(),
                },
                Binding::Http {
                    method: "GET".into(),
                    path: "/worker/events".into(),
                },
            ],
            handler: Arc::new(StreamHandler(
                move |_: &[u8], _: &Context| -> Result<Box<dyn Source>, Error> {
                    Ok(Box::new(OperationalEvents {
                        _reservation: subscribers.try_reserve(1).ok_or(Error::Busy)?,
                        updates: ObservedUpdates::Operations(publisher.subscribe()),
                        runtime: tokio::runtime::Handle::current(),
                    }))
                },
            )),
        },
    ];
    if let Some(logging) = logging {
        let contract = Contract {
            id: "worker.logging".into(),
            output: Schema::Object {
                fields: [
                    "queue_dropped_records",
                    "rejected_records",
                    "write_failures",
                    "flush_failures",
                    "rotation_failures",
                    "retention_failures",
                    "active_capture_bytes",
                    "queue_max_bytes",
                    "record_max_bytes",
                    "capture_max_bytes",
                ]
                .into_iter()
                .map(|name| Field::required(name, Schema::Integer))
                .collect(),
            },
            ..actions[0].contract.clone()
        };
        let reader = logging.clone();
        actions.push(Action {
            contract: contract.clone(),
            bindings: vec![Binding::Grpc {
                action: "worker.logging".into(),
            }],
            handler: Arc::new(JsonHandler::<serde_json::Value, serde_json::Value, _>::new(
                move |_: serde_json::Value, _: &Context| Ok(reader.snapshot().to_json()),
            )),
        });
        actions.push(Action {
            contract: Contract {
                id: "worker.logging.events".into(),
                response: ResponseMode::ServerStream,
                input: Schema::Null,
                ..contract
            },
            bindings: vec![
                Binding::Grpc {
                    action: "worker.logging.events".into(),
                },
                Binding::Http {
                    method: "GET".into(),
                    path: "/worker/logging/events".into(),
                },
            ],
            handler: Arc::new(StreamHandler(
                move |_: &[u8], _: &Context| -> Result<Box<dyn Source>, Error> {
                    Ok(Box::new(OperationalEvents {
                        _reservation: stream_budget.try_reserve(1).ok_or(Error::Busy)?,
                        updates: ObservedUpdates::Logging(logging.subscribe()),
                        runtime: tokio::runtime::Handle::current(),
                    }))
                },
            )),
        });
    }
    actions
}

enum ObservedUpdates {
    Operations(Updates),
    Logging(LoggingUpdates),
}
impl ObservedUpdates {
    async fn next(&mut self) -> serde_json::Value {
        match self {
            Self::Operations(updates) => updates.next().await.to_json(),
            Self::Logging(updates) => updates.next().await.to_json(),
        }
    }
}
struct OperationalEvents {
    // Retain admission through the source's actual cancellation/cleanup.
    _reservation: Reservation,
    updates: ObservedUpdates,
    runtime: tokio::runtime::Handle,
}
impl Source for OperationalEvents {
    fn next(&mut self, context: &Context) -> Result<Option<Vec<u8>>, Error> {
        let remaining = context.remaining()?;
        let snapshot = self.runtime.block_on(async {
            tokio::select! {
                biased;
                () = context.cancelled() => Err(Error::Cancelled),
                () = tokio::time::sleep(remaining) => Err(Error::Timeout),
                snapshot = self.updates.next() => Ok(snapshot),
            }
        })?;
        serde_json::to_vec(&snapshot)
            .map(Some)
            .map_err(|_| Error::Failed("operation snapshot encoding".into()))
    }
}
