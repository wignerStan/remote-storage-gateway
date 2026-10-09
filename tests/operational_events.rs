//! Operational streams share one passive source and never enter request callbacks.
use diagnostic_output::operations::{Operations, Outcome};
use network_actions::proto::{self, actions_client::ActionsClient};
use network_sockets::{LocalEndpoint, SocketPermissions};
use pingora_worker::entry::{self, Registration};
use plugin_lifecycle::{Composition, Resource, ResourceSchema};
use proxy_pingora::worker::WorkerControl;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);
#[path = "support/workflow.rs"]
mod workflow;
const ENTRY: Resource = Resource {
    id: "ops_entry",
    provides: &["ops_entry"],
    requires: &[],
};
async fn sse_item(socket: &mut TcpStream) -> Value {
    timeout(WAIT, async {
        let mut wire = Vec::new();
        loop {
            wire.push(socket.read_u8().await.unwrap());
            assert!(wire.len() < 8192);
            if let Some(start) = wire.windows(6).position(|part| part == b"data: ") {
                if let Some(end) = wire[start + 6..].iter().position(|byte| *byte == b'\n') {
                    return serde_json::from_slice(&wire[start + 6..start + 6 + end]).unwrap();
                }
            }
        }
    })
    .await
    .unwrap()
}
fn request(binding: &str, input: &[u8]) -> proto::CallRequest {
    proto::CallRequest {
        binding: binding.into(),
        input: input.to_vec(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_and_sse_observe_coalesced_operations_and_close_without_notifiers() {
    timeout(Duration::from_secs(10), async {
        let counters = Arc::new(Operations::default());
        let control = Arc::new(WorkerControl::default());
        let mut composition = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
        let entry = entry::install(&mut composition, ENTRY, Registration {
            worker_control: Some(Arc::clone(&control)),
            endpoint: LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            permissions: SocketPermissions::default(),
        }, Arc::clone(&counters)).await.unwrap();
        let runtime = composition.commit().await.unwrap();
        entry.activate().unwrap();
        let address = entry.local_address().unwrap();
        let mut client = ActionsClient::connect(format!("http://{address}")).await.unwrap();
        assert_eq!(client.call(request("worker.events", b"null")).await.unwrap_err().code(), tonic::Code::InvalidArgument);
        let mut events = client.call_stream(request("worker.events", b"null")).await.unwrap().into_inner();
        let initial: Value = serde_json::from_slice(&timeout(WAIT, events.message()).await.unwrap().unwrap().unwrap().output).unwrap();
        assert_eq!(initial["started"], 0);
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(b"GET /worker/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n").await.unwrap();
        assert_eq!(sse_item(&mut socket).await["started"], 0);
        for _ in 0..200 {
            let mut operation = counters.begin();
            operation.record_bytes(3, 5);
            operation.finish(Outcome::Delivered, Some(200), Duration::from_millis(1));
        }
        let latest: Value = loop {
            let value: Value = serde_json::from_slice(&timeout(WAIT, events.message()).await.unwrap().unwrap().unwrap().output).unwrap();
            if value["delivered"] == 200 && value["inflight"] == 0 && value["request_bytes"] == 600 && value["response_bytes"] == 1000 { break value; }
        };
        let sse = loop {
            let value = sse_item(&mut socket).await;
            if value["delivered"] == 200 && value["inflight"] == 0 && value["request_bytes"] == 600 && value["response_bytes"] == 1000 { break value; }
        };
        assert_eq!(sse, latest);
        assert_eq!(latest["inflight"], 0);
        assert_eq!(latest["request_bytes"], 600);
        let metrics: Value = serde_json::from_slice(&client.call(request("worker.metrics", b"{}")).await.unwrap().into_inner().output).unwrap();
        assert_eq!(metrics, latest);
        assert!(control.accepting());
        assert_eq!(control.snapshot().revision, 1);
        // Keep subscriptions open while entry revocation cancels idle sources.
        timeout(WAIT, runtime.shutdown()).await.unwrap().unwrap();
        assert!(matches!(timeout(WAIT, events.message()).await.unwrap(), Err(_)|Ok(None)));
        drop(events); drop(socket); drop(client);
        assert_eq!(counters.snapshot().started, 200, "observation traffic must not become data-plane operations");
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_capacity_keeps_control_available_and_disconnect_returns_a_slot() {
    timeout(Duration::from_secs(15), async {
        let mut evidence = workflow::Evidence::new();
        let counters = Arc::new(Operations::default());
        let control = Arc::new(WorkerControl::default());
        let mut composition = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
        let entry = entry::install(&mut composition, ENTRY, Registration {
            worker_control: Some(Arc::clone(&control)),
            endpoint: LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            permissions: SocketPermissions::default(),
        }, Arc::clone(&counters)).await.unwrap();
        let runtime = composition.commit().await.unwrap();
        entry.activate().unwrap();
        let address = entry.local_address().unwrap();
        let mut client = ActionsClient::connect(format!("http://{address}")).await.unwrap();
        let mut grpc = Vec::new();
        let mut sse = Vec::new();
        for _ in 0..16 {
            let mut stream = client.call_stream(request("worker.events", b"null")).await.unwrap().into_inner();
            assert!(stream.message().await.unwrap().is_some());
            grpc.push(stream);
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(b"GET /worker/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n").await.unwrap();
            assert_eq!(sse_item(&mut socket).await["started"], 0);
            sse.push(socket);
        }
        evidence.record("given", "subscribe", "gRPC and SSE share one bounded observer allowance", json!({"grpc_observers": grpc.len(), "sse_observers": sse.len(), "observer_limit": 32}));

        let mut extra = client.call_stream(request("worker.events", b"null")).await.unwrap().into_inner();
        let rejected = extra.message().await.unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::ResourceExhausted);
        drop(extra);
        let mut extra_sse = TcpStream::connect(address).await.unwrap();
        extra_sse.write_all(b"GET /worker/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n").await.unwrap();
        assert_eq!(sse_item(&mut extra_sse).await["code"], 503);
        drop(extra_sse);

        let read: Value = serde_json::from_slice(&client.call(request("worker.get", b"{}")).await.unwrap().into_inner().output).unwrap();
        assert_eq!(read["accepting"], true);
        let update: Value = serde_json::from_slice(&client.call(request("worker.apply", br#"{"expected_revision":1,"accepting":false}"#)).await.unwrap().into_inner().output).unwrap();
        assert_eq!(update["accepting"], false);
        assert_eq!(update["revision"], 2);
        let metrics: Value = serde_json::from_slice(&client.call(request("worker.metrics", b"{}")).await.unwrap().into_inner().output).unwrap();
        assert_eq!(metrics["started"], 0);
        evidence.record("when", "reject_extra_observer", "observer exhaustion rejects only extra subscriptions; control reads and admission changes remain callable", json!({"grpc_code": format!("{:?}", rejected.code()), "sse_code": 503, "control": update, "data_requests": 0}));

        // Observe actual cancellation and source release, rather than sleeping.
        drop(grpc.pop());
        let mut replacement = timeout(WAIT, async {
            loop {
                let mut stream = client.call_stream(request("worker.events", b"null")).await.unwrap().into_inner();
                match stream.message().await {
                    Ok(Some(_)) => break stream,
                    Err(error) if error.code() == tonic::Code::ResourceExhausted => tokio::task::yield_now().await,
                    result => panic!("unexpected reconnect result: {result:?}"),
                }
            }
        }).await.unwrap();
        evidence.record("then", "reuse_capacity", "disconnect drops the source reservation so a new observer can start", json!({"observer_limit": 32, "replacement_open": true}));

        timeout(WAIT, runtime.shutdown()).await.unwrap().unwrap();
        assert!(matches!(timeout(WAIT, replacement.message()).await.unwrap(), Err(_) | Ok(None)));
        assert_eq!(counters.snapshot().started, 0);
        drop(grpc);
        drop(sse);
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_grpc_deadline_and_sse_replay_rejection_keep_observations_live_only() {
    timeout(Duration::from_secs(10), async {
        let mut evidence = workflow::Evidence::new();
        let counters = Arc::new(Operations::default());
        let control = Arc::new(WorkerControl::default());
        let mut composition = Composition::new(ResourceSchema::new(&[ENTRY]).unwrap());
        let entry = entry::install(&mut composition, ENTRY, Registration {
            worker_control: Some(control),
            endpoint: LocalEndpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            permissions: SocketPermissions::default(),
        }, counters).await.unwrap();
        let runtime = composition.commit().await.unwrap();
        entry.activate().unwrap();
        let address = entry.local_address().unwrap();
        let mut client = ActionsClient::connect(format!("http://{address}")).await.unwrap();
        let mut idle = Vec::new();
        for _ in 0..31 {
            let mut stream = client.call_stream(request("worker.events", b"null")).await.unwrap().into_inner();
            assert!(stream.message().await.unwrap().is_some());
            idle.push(stream);
        }
        let mut request = tonic::Request::new(request("worker.events", b"null"));
        request.set_timeout(Duration::from_millis(500));
        let mut stream = client.call_stream(request).await.unwrap().into_inner();
        assert!(stream.message().await.unwrap().is_some());
        let error = timeout(WAIT, stream.message()).await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        evidence.record("when", "expire", "the caller sees deadline expiry on its idle subscription", json!({"deadline_ms": 500, "grpc_code": format!("{:?}", error.code())}));
        let mut replacement = timeout(WAIT, async {
            loop {
                let call = tonic::Request::new(network_actions::proto::CallRequest {
                    binding: "worker.events".into(), input: b"null".to_vec(),
                });
                let mut candidate = client.call_stream(call).await.unwrap().into_inner();
                match candidate.message().await {
                    Ok(Some(_)) => break candidate,
                    Err(error) if error.code() == tonic::Code::ResourceExhausted => tokio::task::yield_now().await,
                    result => panic!("unexpected deadline capacity result: {result:?}"),
                }
            }
        }).await.unwrap();
        evidence.record("then", "reuse_expired_capacity", "with 31 other observers still open, a replacement proves the expired source released its reservation", json!({"idle_observers": idle.len(), "replacement_open": true}));

        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(b"GET /worker/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nLast-Event-ID: old-cursor\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut wire = Vec::new();
        socket.read_to_end(&mut wire).await.unwrap();
        assert!(wire.starts_with(b"HTTP/1.1 400"));
        evidence.record("then", "reject_replay", "SSE does not claim durable replay and rejects a resume cursor", json!({"status": 400, "resume_supported": false}));
        timeout(WAIT, runtime.shutdown()).await.unwrap().unwrap();
        assert!(matches!(timeout(WAIT, replacement.message()).await.unwrap(), Err(_) | Ok(None)));
    }).await.unwrap();
}
