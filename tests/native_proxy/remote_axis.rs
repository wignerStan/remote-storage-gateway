//! Real worker hooks use the shared axis control contract over a Unix socket.
use super::*;
use action_router::{Action, Binding, Context, Contract, ResponseMode, Router, Schema};
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::{Value, json};
use std::sync::Mutex;

struct Axis {
    endpoint: String,
    task: JoinHandle<()>,
    _registration: action_router::Registration,
}
impl Drop for Axis {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn axis(path: &std::path::Path, reply: Value, seen: Arc<Mutex<Vec<Value>>>) -> Axis {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    let router = Router::new();
    let registration = router
        .register(vec![Action {
            contract: Contract {
                id: "worker.metadata".into(),
                version: 1,
                response: ResponseMode::Unary,
                input: Schema::Json,
                output: Schema::Json,
                max_input: 65536,
                max_output: 65536,
            },
            bindings: vec![Binding::Grpc {
                action: "worker.metadata.v1.policy.before_upstream_send".into(),
            }],
            handler: Arc::new(move |input: &[u8], _: &Context| {
                seen.lock()
                    .unwrap()
                    .push(serde_json::from_slice(input).unwrap());
                Ok(serde_json::to_vec(&reply).unwrap())
            }),
        }])
        .unwrap();
    let service = network_actions::ActionService::new(router, 65536, WAIT).unwrap();
    let task = tokio::spawn(async move {
        network_actions::serve_unix(listener, service, std::future::pending())
            .await
            .unwrap();
    });
    Axis {
        endpoint: format!("unix://{}", path.display()),
        task,
        _registration: registration,
    }
}
fn config(upstream: SocketAddr, endpoint: &str) -> pingora_worker::config::Config {
    serde_json::from_value(json!({
        "upstream":format!("http://{upstream}"),
        "axis_bindings":{"request_policy":{"endpoint":endpoint}},
        "callbacks":[{"axis":"request_policy", "registration":"policy", "package":"policy",
            "hooks":["before_upstream_send"], "headers":["x-visible"], "patch_headers":["x-selected"]}]
    })).unwrap()
}
async fn request(address: SocketAddr) -> Vec<u8> {
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(b"GET /resource?secret=hidden HTTP/1.1\r\nHost: downstream\r\nX-Visible: admitted\r\nAuthorization: hidden\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_remote_axis_projects_grants_and_replaces_client_without_reinstalling_hooks() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let input = head(&mut socket).await;
            let value = if String::from_utf8_lossy(&input).contains("x-selected: second") {
                "second"
            } else {
                "first"
            };
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{value}",
                        value.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let first = axis(
            &directory.path().join("first.sock"),
            json!({"version":1,"decision":"continue","set_headers":[["x-selected","first"]]}),
            Arc::clone(&seen),
        )
        .await;
        let second = axis(
            &directory.path().join("second.sock"),
            json!({"version":1,"decision":"continue","set_headers":[["x-selected","second"]]}),
            Arc::clone(&seen),
        )
        .await;
        let proxy = pingora_worker::assembly::assemble(config(origin.address, &first.endpoint))
            .await
            .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        assert!(request(worker.address).await.ends_with(b"first"));
        assert!(
            proxy
                .remote_bindings()
                .replace("request_policy", "unix:///missing/remote-axis.sock")
                .await
                .is_err()
        );
        assert!(request(worker.address).await.ends_with(b"first"));
        proxy
            .remote_bindings()
            .replace("request_policy", &second.endpoint)
            .await
            .unwrap();
        assert!(request(worker.address).await.ends_with(b"second"));
        for metadata in seen.lock().unwrap().iter() {
            assert_eq!(metadata["version"], 1);
            assert_eq!(metadata["hook"], "before_upstream_send");
            assert_eq!(metadata["path"], "/resource");
            assert_eq!(metadata["headers"], json!([["x-visible", "admitted"]]));
            assert!(!metadata.to_string().contains("hidden"));
        }
        assert_eq!(seen.lock().unwrap().len(), 3);
        drop(worker);
        proxy.shutdown().await.unwrap();
        assert!(
            proxy
                .remote_bindings()
                .replace("request_policy", &first.endpoint)
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_reply_cannot_widen_header_grant_or_contract_version() {
    timeout(WAIT, async {
        let reached = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&reached);
        let origin = listen(move |_socket| {
            count.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .await;
        for reply in [
            json!({"version":1,"decision":"continue","set_headers":[["authorization","stolen"]]}),
            json!({"version":2,"decision":"continue"}),
            json!({"version":1,"decision":"deny"}),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let remote = axis(&directory.path().join("axis.sock"), reply, Arc::default()).await;
            let proxy =
                pingora_worker::assembly::assemble(config(origin.address, &remote.endpoint))
                    .await
                    .unwrap();
            let worker = serve_engine(proxy.clone()).await;
            assert!(!request(worker.address).await.starts_with(b"HTTP/1.1 200"));
            drop(worker);
            proxy.shutdown().await.unwrap();
        }
        assert_eq!(reached.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
}
