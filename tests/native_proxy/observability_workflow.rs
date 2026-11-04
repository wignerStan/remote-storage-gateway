//! Given a real native request, observers see lifecycle facts without owning policy.
use super::*;
use diagnostic_output::operations::Operations;
use network_actions::proto::{CallRequest, actions_client::ActionsClient};
use network_data_plane_worker::{
    assembly::control as entry,
    proxy::{ProxyContract, RequestFacts},
};
use network_sockets::{LocalEndpoint, SocketPermissions};
use plugin_lifecycle::{Composition, Resource, ResourceSchema};
use proxy_pingora::worker::surface::Callbacks;
use serde_json::{Value, json};

#[path = "../support/workflow.rs"]
mod workflow;

const ENTRY: Resource = Resource {
    id: "observability_bdd",
    provides: &["observability_bdd"],
    requires: &[],
};

struct PolicyCalls(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for PolicyCalls {
    async fn authenticate(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(()))
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(()))
    }
    async fn complete(&self, _: &RequestFacts) -> Result<u32, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }
    async fn maintain(&self) {}
}

fn call(binding: &str, input: &[u8]) -> CallRequest {
    CallRequest {
        binding: binding.into(),
        input: input.to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_delivery_completion_and_reconnect_have_independent_observers() {
    timeout(Duration::from_secs(15), async {
        let mut evidence = workflow::Evidence::new();
        let operations = Arc::new(Operations::default());
        let control = Arc::new(WorkerControl::default());
        let policy_calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let upstream_release = Arc::clone(&release);
        let upstream = listen(move |mut socket| {
            let release = Arc::clone(&upstream_release);
            async move {
                assert!(head(&mut socket).await.starts_with(b"GET /resource HTTP/1.1"));
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nfirst").await.unwrap();
                release.notified().await;
                socket.write_all(b"-last").await.unwrap();
            }
        }).await;
        let native = NativeProxy::new(
            settings(upstream.address, None, "", false),
            Arc::clone(&control),
            vec![Arc::new(PolicyCalls(Arc::clone(&policy_calls)))],
        ).unwrap().with_operations(Arc::clone(&operations));
        let worker = serve_engine(native).await;
        let mut composition = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
        let entry = entry::install(&mut composition, ENTRY, entry::Registration {
            protocol: network_data_plane_worker::config::ControlProtocol::Http,
            worker_control: Some(Arc::clone(&control)),
            logging_health: None,
            endpoint: LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            permissions: SocketPermissions::default(),
        }, Arc::clone(&operations)).await.unwrap();
        let runtime = composition.commit().await.unwrap();
        entry.activate().unwrap();
        let mut observer = ActionsClient::connect(format!("http://{}", entry.local_address().unwrap())).await.unwrap();
        let mut events = observer.call_stream(call("worker.events", b"null")).await.unwrap().into_inner();
        let initial: Value = serde_json::from_slice(&events.message().await.unwrap().unwrap().output).unwrap();
        assert_eq!(initial["started"], 0);
        assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
        evidence.record("given", "observe", "the internal observer starts from current state without invoking policy", initial);

        let mut downstream = TcpStream::connect(worker.address).await.unwrap();
        downstream.write_all(b"GET /resource HTTP/1.1\r\nHost: downstream\r\nAuthorization: Bearer bdd-request-secret\r\nConnection: close\r\n\r\n").await.unwrap();
        assert!(head(&mut downstream).await.starts_with(b"HTTP/1.1 200"));
        let mut first = [0; 5];
        downstream.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"first");
        let active = operations.snapshot();
        assert_eq!((active.started, active.inflight, active.delivered), (1, 1, 0));
        assert_eq!(active.response_bytes, 0, "byte totals settle at termination");
        assert_eq!(policy_calls.load(Ordering::SeqCst), 2);
        evidence.record("when", "forward", "the first response bytes arrive before origin EOF; completion counters remain pending", active.to_json());

        // Neither a slow observer nor a lost observer controls native delivery.
        drop(events);
        release.notify_one();
        let mut tail = Vec::new();
        downstream.read_to_end(&mut tail).await.unwrap();
        assert_eq!(&tail, b"-last");
        while operations.snapshot().inflight != 0 { tokio::task::yield_now().await; }
        let complete = operations.snapshot();
        assert_eq!((complete.started, complete.delivered, complete.inflight), (1, 1, 0));
        assert_eq!((complete.request_bytes, complete.response_bytes, complete.latency_count), (0, 10, 1));
        assert_eq!(complete.status_classes[2], 1);
        assert_eq!(policy_calls.load(Ordering::SeqCst), 3);

        let mut reconnected = observer.call_stream(call("worker.events", b"null")).await.unwrap().into_inner();
        let current: Value = serde_json::from_slice(&reconnected.message().await.unwrap().unwrap().output).unwrap();
        assert_eq!(current, complete.to_json());
        let metrics: Value = serde_json::from_slice(&observer.call(call("worker.metrics", b"{}")).await.unwrap().into_inner().output).unwrap();
        assert_eq!(metrics, current);
        assert!(!current.to_string().contains("bdd-request-secret"));
        evidence.record("then", "reconnect", "a new subscription returns current counters; it does not replay requests or call the notifier", current);
        assert_eq!(policy_calls.load(Ordering::SeqCst), 3);
        assert_eq!(control.snapshot().revision, 1);

        timeout(WAIT, runtime.shutdown()).await.unwrap().unwrap();
        assert!(matches!(timeout(WAIT, reconnected.message()).await.unwrap(), Err(_) | Ok(None)));
        assert_eq!(operations.snapshot().started, 1);
        evidence.record("then", "close", "entry revocation cancels idle observers and drains their sources", json!({"data_requests": 1, "policy_calls": 3, "entry_closed": true}));
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_worker_control_live_mutates_admission_and_controls_proxy_traffic() {
    timeout(Duration::from_secs(15), async {
        let operations = Arc::new(Operations::default());
        let control = Arc::new(WorkerControl::default());
        let upstream_handled = Arc::new(AtomicUsize::new(0));
        let upstream_counter = Arc::clone(&upstream_handled);

        let upstream = listen(move |mut socket| {
            let counter = Arc::clone(&upstream_counter);
            async move {
                let _ = head(&mut socket).await;
                counter.fetch_add(1, Ordering::SeqCst);
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .unwrap();
            }
        })
        .await;

        let native = NativeProxy::new(
            settings(upstream.address, None, "", false),
            Arc::clone(&control),
            vec![],
        )
        .unwrap()
        .with_operations(Arc::clone(&operations));
        let worker = serve_engine(native).await;

        let mut composition = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
        let entry = entry::install(
            &mut composition,
            ENTRY,
            entry::Registration {
                protocol: network_data_plane_worker::config::ControlProtocol::Http,
                worker_control: Some(Arc::clone(&control)),
                logging_health: None,
                endpoint: LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
                permissions: SocketPermissions::default(),
            },
            Arc::clone(&operations),
        )
        .await
        .unwrap();
        let runtime = composition.commit().await.unwrap();
        entry.activate().unwrap();

        let mut client =
            ActionsClient::connect(format!("http://{}", entry.local_address().unwrap()))
                .await
                .unwrap();

        // 1. Initial state inspection via gRPC
        let initial: Value = serde_json::from_slice(
            &client
                .call(call("worker.get", b"{}"))
                .await
                .unwrap()
                .into_inner()
                .output,
        )
        .unwrap();
        assert_eq!(initial["revision"], 1);
        assert_eq!(initial["accepting"], true);
        assert!(control.accepting());

        // 2. Data plane: traffic passes while accepting=true
        let mut conn1 = TcpStream::connect(worker.address).await.unwrap();
        conn1
            .write_all(b"GET /test HTTP/1.1\r\nHost: downstream\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let resp1 = head(&mut conn1).await;
        assert!(resp1.starts_with(b"HTTP/1.1 200 OK"), "{resp1:?}");
        assert_eq!(upstream_handled.load(Ordering::SeqCst), 1);

        // 3. Live in-memory mutation: close admission via gRPC worker.apply
        let updated: Value = serde_json::from_slice(
            &client
                .call(call(
                    "worker.apply",
                    br#"{"expected_revision":1,"accepting":false}"#,
                ))
                .await
                .unwrap()
                .into_inner()
                .output,
        )
        .unwrap();
        assert_eq!(updated["revision"], 2);
        assert_eq!(updated["accepting"], false);
        assert!(!control.accepting());

        // 4. Data plane: new request immediately rejected with 503 without hitting origin
        let mut conn2 = TcpStream::connect(worker.address).await.unwrap();
        conn2
            .write_all(b"GET /test HTTP/1.1\r\nHost: downstream\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp2 = Vec::new();
        conn2.read_to_end(&mut resp2).await.unwrap();
        assert!(
            resp2.starts_with(b"HTTP/1.1 503 Service Unavailable"),
            "{resp2:?}"
        );
        // Upstream was never contacted for the rejected request
        assert_eq!(upstream_handled.load(Ordering::SeqCst), 1);

        // 5. Stale revision CAS conflict rejected over gRPC
        let conflict = client
            .call(call(
                "worker.apply",
                br#"{"expected_revision":1,"accepting":true}"#,
            ))
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), tonic::Code::Aborted);
        assert!(!control.accepting());
        assert_eq!(control.snapshot().revision, 2);

        // 6. Live in-memory mutation: restore admission via gRPC worker.apply with current revision
        let restored: Value = serde_json::from_slice(
            &client
                .call(call(
                    "worker.apply",
                    br#"{"expected_revision":2,"accepting":true}"#,
                ))
                .await
                .unwrap()
                .into_inner()
                .output,
        )
        .unwrap();
        assert_eq!(restored["revision"], 3);
        assert_eq!(restored["accepting"], true);
        assert!(control.accepting());

        // 7. Data plane: traffic passes again immediately
        let mut conn3 = TcpStream::connect(worker.address).await.unwrap();
        conn3
            .write_all(b"GET /test HTTP/1.1\r\nHost: downstream\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let resp3 = head(&mut conn3).await;
        assert!(resp3.starts_with(b"HTTP/1.1 200 OK"), "{resp3:?}");
        assert_eq!(upstream_handled.load(Ordering::SeqCst), 2);

        // 8. Confirm active metrics counters via gRPC
        let metrics: Value = serde_json::from_slice(
            &client
                .call(call("worker.metrics", b"{}"))
                .await
                .unwrap()
                .into_inner()
                .output,
        )
        .unwrap();
        assert!(metrics["started"].as_u64().unwrap() >= 2);

        timeout(WAIT, runtime.shutdown()).await.unwrap().unwrap();
    })
    .await
    .unwrap();
}
