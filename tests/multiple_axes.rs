#![cfg(feature = "l7")]
use action_execution::{
    Action, Binding, Context, Contract, ResponseMode, Schema, Source, StreamHandler,
};
use network_actions::proto;
use network_data_plane_worker::{
    assembly::control::{self as entry, ControlServer},
    composition::mount_axis,
};
use plugin_lifecycle::{
    Composition, Invocation, Resource, ResourceRequirement, ResourceSchema, Reversal,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const ENTRY: Resource = Resource {
    id: "entry",
    provides: &["entry"],
    requires: &[],
};
const A: Resource = Resource {
    id: "axis-a",
    provides: &["axis"],
    requires: &[ResourceRequirement {
        resource: "entry",
        capability: "entry",
    }],
};
const B: Resource = Resource { id: "axis-b", ..A };
// Intentionally identical local IDs in both axes.
const TOOL: Resource = Resource {
    id: "tool",
    provides: &["count"],
    requires: &[],
};
const ACTIONS: Resource = Resource {
    id: "actions",
    provides: &["actions"],
    requires: &[ResourceRequirement {
        resource: "tool",
        capability: "count",
    }],
};
fn binding(name: &str) -> Binding {
    Binding::Grpc {
        action: name.into(),
    }
}
fn contract(output: Schema) -> Contract {
    Contract {
        id: "status".into(),
        version: 1,
        response: ResponseMode::Unary,
        input: Schema::Null,
        output,
        max_input: 16,
        max_output: 128,
    }
}
fn child() -> Composition {
    Composition::new(ResourceSchema::new(&[TOOL, ACTIONS]).unwrap())
}
async fn host(schema: &[Resource]) -> (Composition, Arc<ControlServer>) {
    let mut composition = Composition::new(ResourceSchema::new(schema).unwrap());
    let entry = entry::install(
        &mut composition,
        ENTRY,
        entry::Registration {
            protocol: network_data_plane_worker::config::ControlProtocol::Http,
            worker_control: Some(Arc::default()),
            logging_health: None,
            endpoint: network_sockets::LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            permissions: network_sockets::SocketPermissions::default(),
        },
        Arc::new(diagnostic_output::operations::Operations::default()),
    )
    .await
    .unwrap();
    (composition, entry)
}
struct Logs {
    _lease: Invocation<AtomicUsize>,
}
impl Source for Logs {
    fn next(&mut self, _: &Context) -> Result<Option<Vec<u8>>, action_execution::Error> {
        Ok(Some(br#""line""#.to_vec()))
    }
}
async fn axis(
    entry: &ControlServer,
    name: &str,
    output: Schema,
    bytes: &[u8],
    closed: Arc<AtomicUsize>,
    logs: bool,
) -> Composition {
    let mut composition = child();
    let port = composition
        .install(TOOL, async move {
            let value = Arc::clone(&closed);
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move {
                    closed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
            Ok((value, inverse))
        })
        .await
        .unwrap();
    let value = bytes.to_vec();
    let reader = port.clone();
    let mut actions = vec![Action {
        contract: contract(output),
        bindings: vec![binding(&format!("{name}.status"))],
        handler: Arc::new(move |_: &[u8], _: &Context| {
            let _lease = reader.enter().map_err(action_execution::Error::Failed)?;
            Ok(value.clone())
        }),
    }];
    if logs {
        let mut log_contract = contract(Schema::String);
        log_contract.id = "logs".into();
        log_contract.response = ResponseMode::ServerStream;
        actions.push(Action {
            contract: log_contract,
            bindings: vec![binding(&format!("{name}.logs"))],
            handler: Arc::new(StreamHandler(move |_: &[u8], _: &Context| {
                Ok(Box::new(Logs {
                    _lease: port.enter().map_err(action_execution::Error::Failed)?,
                }) as Box<dyn Source>)
            })),
        });
    }
    entry
        .install_actions(&mut composition, ACTIONS, name, actions)
        .await
        .unwrap();
    composition
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_listener_serves_two_axes_and_closing_a_streaming_axis_preserves_other_axes() {
    let (mut parent, entry) = host(&[ENTRY, A, B]).await;
    let closed_a = Arc::new(AtomicUsize::new(0));
    let closed_b = Arc::new(AtomicUsize::new(0));
    let a = axis(
        &entry,
        "alpha",
        Schema::String,
        br#""alpha""#,
        Arc::clone(&closed_a),
        true,
    )
    .await;
    let b = axis(
        &entry,
        "beta",
        Schema::Integer,
        b"42",
        Arc::clone(&closed_b),
        false,
    )
    .await;
    mount_axis(&mut parent, A, async move { a.commit().await })
        .await
        .unwrap();
    mount_axis(&mut parent, B, async move { b.commit().await })
        .await
        .unwrap();
    let runtime = parent.commit().await.unwrap();
    entry.activate().unwrap();
    let mut client = proto::actions_client::ActionsClient::connect(format!(
        "http://{}",
        entry.local_address().unwrap()
    ))
    .await
    .unwrap();
    let request = |name: &str| proto::CallRequest {
        binding: name.into(),
        input: b"null".to_vec(),
    };
    assert_eq!(
        client
            .call(request("alpha.status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        br#""alpha""#
    );
    assert_eq!(
        client
            .call(request("beta.status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"42"
    );
    let mut stream = client
        .call_stream(request("alpha.logs"))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().is_some());
    tokio::time::timeout(Duration::from_secs(3), runtime.close_resources(&[A.id]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed_a.load(Ordering::SeqCst), 1);
    assert_eq!(closed_b.load(Ordering::SeqCst), 0);
    assert!(client.call(request("alpha.status")).await.is_err());
    assert_eq!(
        client
            .call(request("beta.status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"42"
    );
    assert!(
        client
            .call(proto::CallRequest {
                binding: "worker.get".into(),
                input: b"{}".to_vec()
            })
            .await
            .is_ok()
    );
    // A namespace can be reused after its old calls and resources drain.
    let reopened = axis(
        &entry,
        "alpha",
        Schema::Integer,
        b"7",
        Arc::clone(&closed_a),
        false,
    )
    .await
    .commit()
    .await
    .unwrap();
    assert_eq!(
        client
            .call(request("alpha.status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"7"
    );
    reopened.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(closed_a.load(Ordering::SeqCst), 2);
    assert_eq!(closed_b.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn conflicts_roll_back_only_the_attempted_namespace_and_shutdown_rejects_new_axes() {
    let (parent, entry) = host(&[ENTRY]).await;
    let root = parent.commit().await.unwrap();
    entry.activate().unwrap();
    let one = axis(
        &entry,
        "alpha",
        Schema::Integer,
        b"1",
        Arc::new(AtomicUsize::new(0)),
        false,
    )
    .await
    .commit()
    .await
    .unwrap();
    let local = Resource {
        id: "actions",
        provides: &["actions"],
        requires: &[],
    };
    let mut failed = Composition::new(ResourceSchema::new(&[local]).unwrap());
    let action = |name: &str| Action {
        contract: contract(Schema::Integer),
        bindings: vec![binding(name)],
        handler: Arc::new(|_: &[u8], _: &Context| Ok(b"2".to_vec())),
    };
    assert!(
        entry
            .install_actions(
                &mut failed,
                local,
                "collision",
                vec![action("orphan"), action("alpha.status")]
            )
            .await
            .is_err()
    );
    failed.rollback().await.unwrap_err();
    let mut duplicate = Composition::new(ResourceSchema::new(&[local]).unwrap());
    assert!(
        entry
            .install_actions(&mut duplicate, local, "alpha", vec![action("other")])
            .await
            .is_err()
    );
    let _ = duplicate.rollback().await;
    let mut client = proto::actions_client::ActionsClient::connect(format!(
        "http://{}",
        entry.local_address().unwrap()
    ))
    .await
    .unwrap();
    assert!(
        client
            .call(proto::CallRequest {
                binding: "orphan".into(),
                input: b"null".to_vec()
            })
            .await
            .is_err()
    );
    assert_eq!(
        client
            .call(proto::CallRequest {
                binding: "alpha.status".into(),
                input: b"null".to_vec()
            })
            .await
            .unwrap()
            .into_inner()
            .output,
        b"1"
    );
    let mut replacement = Composition::new(ResourceSchema::new(&[local]).unwrap());
    entry
        .install_actions(&mut replacement, local, "collision", vec![action("new")])
        .await
        .unwrap();
    replacement
        .commit()
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    one.shutdown().await.unwrap();
    root.shutdown().await.unwrap();
    let mut late = Composition::new(ResourceSchema::new(&[local]).unwrap());
    assert!(
        entry
            .install_actions(&mut late, local, "late", vec![action("late")])
            .await
            .is_err()
    );
    let _ = late.rollback().await;
}
