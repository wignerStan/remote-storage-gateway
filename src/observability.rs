//! Read-only operational event exposure, independent of request policy callbacks.
use action_router::{
    Action, Binding, Context, Contract, Error, Field, JsonHandler, ResponseMode, Schema, Source,
    StreamHandler,
};
use diagnostic_output::operations::{Operations, Updates};
use std::sync::Arc;

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
pub(crate) fn actions(operations: &Arc<Operations>) -> Vec<Action> {
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
    vec![
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
                        updates: publisher.subscribe(),
                        runtime: tokio::runtime::Handle::current(),
                    }))
                },
            )),
        },
    ]
}
struct OperationalEvents {
    updates: Updates,
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
        serde_json::to_vec(&snapshot.to_json())
            .map(Some)
            .map_err(|_| Error::Failed("operation snapshot encoding".into()))
    }
}
