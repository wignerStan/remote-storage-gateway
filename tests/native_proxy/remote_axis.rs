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
                .remote_plugins()
                .replace("request_policy", "unix:///missing/remote-axis.sock")
                .await
                .is_err()
        );
        assert!(request(worker.address).await.ends_with(b"first"));
        proxy
            .remote_plugins()
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
                .remote_plugins()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_ingress_owns_membership_and_never_uses_worker_entry() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let _ = head(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let remote = axis(
            &directory.path().join("provider.sock"),
            json!({"version":1,"decision":"continue"}),
            Arc::default(),
        )
        .await;
        let plugin_endpoint = format!("unix://{}", directory.path().join("plugin.sock").display());
        let host_endpoint = format!("unix://{}", directory.path().join("host.sock").display());
        let mut config = config(origin.address, &remote.endpoint);
        config.plugin_ingress = Some(pingora_worker::remote_axis::entry::Config {
            listen: plugin_endpoint.clone(),
        });
        config.management = Some(pingora_worker::config::Management {
            listen: host_endpoint.clone(),
        });
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let connect = |endpoint: String| async move {
            network_actions::client::RemoteBindings::connect(
                network_sockets::LocalEndpoint::parse(&endpoint).unwrap(),
                network_actions::client::Limits::default(),
            )
            .await
            .unwrap()
        };
        let plugin = connect(plugin_endpoint).await;
        let host = connect(host_endpoint).await;
        let selection = json!({"axis":"request_policy", "registration":"policy"});
        let state: Value = serde_json::from_slice(
            &plugin
                .call(
                    "plugin.get".into(),
                    serde_json::to_vec(&selection).unwrap(),
                    WAIT,
                )
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(state["available"], true);
        assert!(
            host.call(
                "plugin.get".into(),
                serde_json::to_vec(&selection).unwrap(),
                WAIT
            )
            .await
            .is_err()
        );
        assert!(
            plugin
                .call("worker.get".into(), b"{}".to_vec(), WAIT)
                .await
                .is_err()
        );
        assert!(
            host.call("worker.get".into(), b"{}".to_vec(), WAIT)
                .await
                .is_ok()
        );
        let mut withdraw = selection.clone();
        withdraw["generation"] = state["generation"].clone();
        withdraw["incarnation"] = state["incarnation"].clone();
        let mut stale_owner = withdraw.clone();
        stale_owner["incarnation"] = json!("previous-process");
        assert!(
            plugin
                .call(
                    "plugin.withdraw".into(),
                    serde_json::to_vec(&stale_owner).unwrap(),
                    WAIT
                )
                .await
                .is_err()
        );
        assert!(request(worker.address).await.starts_with(b"HTTP/1.1 200"));
        plugin
            .call(
                "plugin.withdraw".into(),
                serde_json::to_vec(&withdraw).unwrap(),
                WAIT,
            )
            .await
            .unwrap();
        assert!(!request(worker.address).await.starts_with(b"HTTP/1.1 200"));
        let mut activation = selection.clone();
        activation["incarnation"] = state["incarnation"].clone();
        let replacement: Value = serde_json::from_slice(
            &plugin
                .call(
                    "plugin.activate".into(),
                    serde_json::to_vec(&activation).unwrap(),
                    WAIT,
                )
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(
            replacement["generation"].as_u64().unwrap() > state["generation"].as_u64().unwrap()
        );
        assert!(
            plugin
                .call(
                    "plugin.withdraw".into(),
                    serde_json::to_vec(&withdraw).unwrap(),
                    WAIT
                )
                .await
                .is_err()
        );
        assert!(request(worker.address).await.starts_with(b"HTTP/1.1 200"));
        for invalid in [
            json!({"axis":"request_policy","registration":"unpublished"}),
            json!({"axis":"request_policy","registration":"policy","endpoint":"127.0.0.1:1"}),
            json!({"axis":"request_policy","registration":"policy","headers":["authorization"]}),
        ] {
            assert!(
                plugin
                    .call(
                        "plugin.get".into(),
                        serde_json::to_vec(&invalid).unwrap(),
                        WAIT
                    )
                    .await
                    .is_err()
            );
        }
        plugin.close(WAIT).await.unwrap();
        host.close(WAIT).await.unwrap();
        drop(worker);
        proxy.shutdown().await.unwrap();
        assert!(!directory.path().join("plugin.sock").exists());
        assert!(!directory.path().join("host.sock").exists());
    })
    .await
    .unwrap();
}
