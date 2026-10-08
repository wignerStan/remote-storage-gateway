//! Independent axis listeners use the same entry implementation, not chained routers.
#![cfg(unix)]
use action_router::{Action, Binding, Context, Contract, ResponseMode, Schema};
use network_actions::proto::{self, actions_client::ActionsClient};
use network_sockets::{LocalEndpoint, SocketPermissions};
use pingora_worker::{
    composition::mount_axis,
    entry::{self, SharedEntry},
};
use plugin_lifecycle::{Composition, Resource, ResourceSchema, Runtime};
use std::{path::Path, sync::Arc, time::Duration};
use tonic::transport::{Channel, Endpoint};

const ENTRY: Resource = Resource {
    id: "entry",
    provides: &["entry"],
    requires: &[],
};
const ACTIONS: Resource = Resource {
    id: "actions",
    provides: &["actions"],
    requires: &[],
};
const A: Resource = Resource {
    id: "a",
    provides: &["axis"],
    requires: &[],
};
const B: Resource = Resource { id: "b", ..A };
async fn axis(path: &Path, value: u8) -> (Runtime, Arc<SharedEntry>) {
    let mut composition = Composition::new(ResourceSchema::new(&[ENTRY, ACTIONS]).unwrap());
    let entry = entry::install(
        &mut composition,
        ENTRY,
        entry::Registration {
            worker_control: None,
            endpoint: LocalEndpoint::Unix(path.to_owned()),
            permissions: SocketPermissions::default(),
        },
        Arc::new(diagnostic_output::operations::Operations::default()),
    )
    .await
    .unwrap();
    entry
        .install_actions(
            &mut composition,
            ACTIONS,
            "plugin",
            vec![Action {
                contract: Contract {
                    id: "status".into(),
                    version: 1,
                    response: ResponseMode::Unary,
                    input: Schema::Null,
                    output: Schema::Integer,
                    max_input: 16,
                    max_output: 16,
                },
                bindings: vec![Binding::Grpc {
                    action: "status".into(),
                }],
                handler: Arc::new(move |_: &[u8], _: &Context| Ok(value.to_string().into_bytes())),
            }],
        )
        .await
        .unwrap();
    let runtime = composition.commit().await.unwrap();
    entry.activate().unwrap();
    (runtime, entry)
}
async fn client(path: &Path) -> ActionsClient<Channel> {
    let path = path.to_owned();
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .unwrap();
    ActionsClient::new(channel)
}
fn request(name: &str) -> proto::CallRequest {
    proto::CallRequest {
        binding: name.into(),
        input: b"null".to_vec(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_axis_uds_and_identical_action_names_do_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let a_path = dir.path().join("a.sock");
    let b_path = dir.path().join("b.sock");
    let (a, _) = axis(&a_path, 1).await;
    let (b, _) = axis(&b_path, 2).await;
    let mut parent = Composition::new(ResourceSchema::new(&[A, B]).unwrap());
    mount_axis(&mut parent, A, async move { Ok(a) })
        .await
        .unwrap();
    mount_axis(&mut parent, B, async move { Ok(b) })
        .await
        .unwrap();
    let runtime = parent.commit().await.unwrap();
    let mut ca = client(&a_path).await;
    let mut cb = client(&b_path).await;
    assert_eq!(
        ca.call(request("status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"1"
    );
    assert_eq!(
        cb.call(request("status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"2"
    );
    assert_eq!(
        ca.call(request("worker.get")).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    // A colliding bind cannot replace an existing owner or its live socket.
    let mut duplicate = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
    assert!(
        entry::install(
            &mut duplicate,
            ENTRY,
            entry::Registration {
                worker_control: None,
                endpoint: LocalEndpoint::Unix(a_path.clone()),
                permissions: SocketPermissions::default(),
            },
            Arc::new(diagnostic_output::operations::Operations::default()),
        )
        .await
        .is_err()
    );
    let _ = duplicate.rollback().await;
    assert_eq!(
        ca.call(request("status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"1"
    );
    drop(ca);
    tokio::time::timeout(Duration::from_secs(3), runtime.close_resources(&[A.id]))
        .await
        .unwrap()
        .unwrap();
    assert!(!a_path.exists());
    assert!(b_path.exists());
    assert_eq!(
        cb.call(request("status"))
            .await
            .unwrap()
            .into_inner()
            .output,
        b"2"
    );
    drop(cb);
    tokio::time::timeout(Duration::from_secs(3), runtime.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(!b_path.exists());
}
