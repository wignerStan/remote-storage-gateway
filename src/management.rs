//! Runtime-installed transport adapters for worker-axis management operations.
use diagnostic_output::operations::Operations;
use proxy_pingora::worker::{Snapshot, Update, UpdateError, WorkerControl};
use serde::Deserialize;

#[derive(Deserialize)]
struct UpdateInput {
    expected_revision: u64,
    accepting: bool,
}
fn snapshot(value: Snapshot) -> serde_json::Value {
    serde_json::json!({"revision": value.revision, "accepting": value.accepting})
}
use action_router::{
    Action, Binding, Context, Contract, Error, Field, JsonHandler, ResponseMode, Schema,
};
use std::sync::Arc;

pub fn actions(control: &Arc<WorkerControl>, operations: &Arc<Operations>) -> Vec<Action> {
    let get = Contract {
        id: "worker.get".into(),
        version: 1,
        response: ResponseMode::Unary,
        input: Schema::Object { fields: vec![] },
        output: Schema::Object {
            fields: vec![
                Field::required("revision", Schema::Integer),
                Field::required("accepting", Schema::Bool),
            ],
        },
        max_input: 1024,
        max_output: 1024,
    };
    let apply = Contract {
        id: "worker.apply".into(),
        input: Schema::Object {
            fields: vec![
                Field::required("expected_revision", Schema::Integer),
                Field::required("accepting", Schema::Bool),
            ],
        },
        ..get.clone()
    };
    let reader = Arc::clone(control);
    let writer = Arc::clone(control);
    let mut actions = crate::observability::actions(operations);
    actions.extend([
        Action {
            contract: get,
            bindings: vec![Binding::Grpc {
                action: "worker.get".into(),
            }],
            handler: Arc::new(JsonHandler::<serde_json::Value, serde_json::Value, _>::new(
                move |_: serde_json::Value, _: &Context| Ok(snapshot(reader.snapshot())),
            )),
        },
        Action {
            contract: apply,
            bindings: vec![Binding::Grpc {
                action: "worker.apply".into(),
            }],
            handler: Arc::new(JsonHandler::<UpdateInput, serde_json::Value, _>::new(
                move |update: UpdateInput, _: &Context| {
                    writer
                        .apply(Update {
                            expected_revision: update.expected_revision,
                            accepting: update.accepting,
                        })
                        .map(snapshot)
                        .map_err(|error| match error {
                            UpdateError::RevisionConflict => Error::Conflict,
                            UpdateError::RevisionExhausted => Error::Busy,
                        })
                },
            )),
        },
    ]);
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use action_router::Router;
    use std::time::Duration;

    #[test]
    fn worker_metrics_reads_shared_counters_without_mutating_control() {
        use diagnostic_output::operations::Outcome;
        let control = Arc::new(WorkerControl::default());
        let operations = Arc::new(Operations::default());
        let router = Router::new();
        let _routes = router.register(actions(&control, &operations)).unwrap();
        let context = Context::new(Duration::from_secs(2)).unwrap();
        let binding = Binding::Grpc {
            action: "worker.metrics".into(),
        };
        let read = || {
            serde_json::from_slice::<serde_json::Value>(
                &router.call(&binding, b"{}", &context).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(read()["started"], 0);
        let mut operation = operations.begin();
        operation.record_bytes(7, 11);
        assert_eq!(read()["inflight"], 1);
        operation.finish(Outcome::Delivered, Some(201), Duration::from_millis(4));
        let value = read();
        assert_eq!(value["started"], 1);
        assert_eq!(value["inflight"], 0);
        assert_eq!(value["delivered"], 1);
        assert_eq!(value["request_bytes"], 7);
        assert_eq!(value["response_bytes"], 11);
        assert_eq!(value["latency_count"], 1);
        assert_eq!(value["latency_ns"], 4_000_000);
        assert_eq!(value["status_classes"][2], 1);
        assert_eq!(read(), value);
        assert!(control.accepting());
        assert_eq!(control.snapshot().revision, 1);
    }

    #[test]
    fn worker_mutation_and_withdrawal_do_not_govern_plugin_routes() {
        let control = Arc::new(WorkerControl::default());
        let router = Router::new();
        let worker = router
            .register(actions(&control, &Arc::new(Operations::default())))
            .unwrap();
        let binding = Binding::Grpc {
            action: "plugin.status".into(),
        };
        let plugin = router
            .register(vec![Action {
                contract: Contract {
                    id: "plugin.status".into(),
                    version: 1,
                    response: ResponseMode::Unary,
                    input: Schema::Object { fields: vec![] },
                    output: Schema::Bool,
                    max_input: 128,
                    max_output: 128,
                },
                bindings: vec![binding.clone()],
                handler: Arc::new(|_: &[u8], _: &Context| Ok(b"true".to_vec())),
            }])
            .unwrap();
        let context = Context::new(Duration::from_secs(2)).unwrap();
        let apply = Binding::Grpc {
            action: "worker.apply".into(),
        };
        router
            .call(
                &apply,
                br#"{"expected_revision":1,"accepting":false}"#,
                &context,
            )
            .unwrap();
        assert!(!control.accepting());
        assert_eq!(
            router.call(
                &apply,
                br#"{"expected_revision":1,"accepting":true}"#,
                &context
            ),
            Err(Error::Conflict)
        );
        assert!(!control.accepting());
        assert_eq!(router.call(&binding, b"{}", &context).unwrap(), b"true");
        worker.drain();
        assert_eq!(router.call(&apply, b"{}", &context), Err(Error::NotFound));
        assert_eq!(router.call(&binding, b"{}", &context).unwrap(), b"true");
        plugin.drain();
    }
}
