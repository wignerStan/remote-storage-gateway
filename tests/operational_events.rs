//! Operational streams share one passive source and never enter request callbacks.
use diagnostic_output::operations::{Operations, Outcome};
use network_actions::proto::{self, actions_client::ActionsClient};
use network_sockets::{LocalEndpoint, SocketPermissions};
use pingora_worker::entry::{self, Registration};
use plugin_lifecycle::{Composition, Resource, ResourceSchema};
use proxy_pingora::worker::WorkerControl;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);
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
