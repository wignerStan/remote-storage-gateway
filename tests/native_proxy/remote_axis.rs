//! Real worker hooks use the shared axis control contract over a Unix socket.
use super::*;
use action_execution::{Action, Binding, Context, Contract, ResponseMode, Schema};
use action_router::Router;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::{Value, json};
use std::sync::Mutex;

struct Axis {
    endpoint: String,
    _listener: network_local_control::listener::ControlListener,
    _registration: action_router::Registration,
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
            bindings: vec![Binding::LocalControl {
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
    let service =
        network_local_control::ControlService::new(Arc::new(router), 65536, WAIT, 32).unwrap();
    let carrier =
        network_local_control::listener::ControlListener::prepare(listener, None, service).unwrap();
    carrier.activate().unwrap();
    Axis {
        endpoint: format!("unix://{}", path.display()),
        _listener: carrier,
        _registration: registration,
    }
}
fn config(upstream: SocketAddr, endpoint: &str) -> network_data_plane_worker::config::Config {
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
        let proxy =
            network_data_plane_worker::assembly::assemble(config(origin.address, &first.endpoint))
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
            let proxy = network_data_plane_worker::assembly::assemble(config(
                origin.address,
                &remote.endpoint,
            ))
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
async fn dedicated_plugin_ingress_retains_independent_routes() {
    plugin_ingress_membership(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_plugin_ingress_dispatches_both_owners_without_widening_grants() {
    plugin_ingress_membership(true).await;
}

async fn plugin_ingress_membership(shared: bool) {
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
        config.plugin_ingress = Some(if shared {
            network_data_plane_worker::config::PluginControl::Shared {
                facility: network_data_plane_worker::config::SharedControl::Management,
            }
        } else {
            network_data_plane_worker::config::PluginControl::Dedicated {
                listen: plugin_endpoint.clone(),
            }
        });
        config.management = Some(network_data_plane_worker::config::Management {
            protocol: network_data_plane_worker::config::ControlProtocol::LocalControl,
            listen: host_endpoint.clone(),
        });
        let proxy = network_data_plane_worker::assembly::assemble(config)
            .await
            .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let connect = |endpoint: String| async move {
            network_local_control::client::RemoteBindings::connect(
                std::path::PathBuf::from(endpoint.strip_prefix("unix://").unwrap()),
                network_local_control::client::Limits::default(),
            )
            .await
            .unwrap()
        };
        let plugin = connect(if shared {
            host_endpoint.clone()
        } else {
            plugin_endpoint
        })
        .await;
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
        assert_eq!(
            host.call(
                "plugin.get".into(),
                serde_json::to_vec(&selection).unwrap(),
                WAIT
            )
            .await
            .is_ok(),
            shared
        );
        assert_eq!(
            plugin
                .call("worker.get".into(), b"{}".to_vec(), WAIT)
                .await
                .is_ok(),
            shared
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
        // Retiring a plugin participant must not retire the shared host routes.
        assert!(
            host.call("worker.get".into(), b"{}".to_vec(), WAIT)
                .await
                .is_ok()
        );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_remote_metadata_prevents_message_callback_and_upstream_dispatch() {
    timeout(WAIT, async {
        let reached = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&reached);
        let origin = listen(move |_socket| { count.fetch_add(1, Ordering::SeqCst); async {} }).await;
        let messages = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&messages);
        let router = Router::new();
        let registration = router.register(vec![
            Action {
                contract: Contract { id: "worker.metadata".into(), version: 1, response: ResponseMode::Unary, input: Schema::Json, output: Schema::Json, max_input: 65536, max_output: 65536 },
                bindings: vec![Binding::LocalControl { action: "worker.metadata.v1.policy.pre_routing".into() }],
                handler: Arc::new(|_: &[u8], _: &Context| Ok(br#"{"version":1,"decision":"deny"}"#.to_vec())),
            },
            Action {
                contract: Contract { id: "worker.message".into(), version: 1, response: ResponseMode::Unary, input: Schema::Bytes, output: Schema::Bytes, max_input: 1048834, max_output: 1114117 },
                bindings: vec![Binding::LocalControl { action: "worker.message.v1.policy".into() }],
                handler: Arc::new(move |_: &[u8], _: &Context| { observed.fetch_add(1, Ordering::SeqCst); Ok(b"null".to_vec()) }),
            },
        ]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("policy.sock");
        let listener = network_local_control::listener::ControlListener::prepare(tokio::net::UnixListener::bind(&path).unwrap(), None,
            network_local_control::ControlService::new(Arc::new(router), 1114117, WAIT, 4).unwrap()).unwrap();
        listener.activate().unwrap();
        let mut cfg = config(origin.address, &format!("unix://{}", path.display()));
        cfg.axis_bindings.get_mut("request_policy").unwrap().max_message_bytes = 1048576;
        cfg.callbacks[0].hooks = vec!["pre_routing".into()];
        cfg.callbacks[0].message_codec = Some("openai.chat_completions.v1".into());
        let proxy = network_data_plane_worker::assembly::assemble(cfg).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        let body = br#"{"messages":[],"model":"test"}"#;
        client.write_all(format!("POST /v1/chat/completions HTTP/1.1\r\nHost: downstream\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let mut response = Vec::new();
        if let Err(error) = client.read_to_end(&mut response).await {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert!(response.starts_with(b"HTTP/1.1 403"));
        assert_eq!(messages.load(Ordering::SeqCst), 0);
        assert_eq!(reached.load(Ordering::SeqCst), 0);
        drop(worker);
        proxy.shutdown().await.unwrap();
        registration.revoke();
        listener.shutdown().await.unwrap();
    }).await.unwrap();
}
