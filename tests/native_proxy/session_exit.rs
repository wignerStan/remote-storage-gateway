//! Session cleanup owners run independently and share one bounded deadline.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use proxy_protocols::websocket;
use serde_json::json;
use std::path::Path;
#[path = "../support/session_exit_fixture.rs"]
mod fixture;
use fixture::{Fault, artifact, artifact_with_fault};

async fn completed_origin() -> Listening {
    listen(|mut socket|async move {
        let request=String::from_utf8(head(&mut socket).await).unwrap();
        let key=request.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("sec-websocket-key")).map(|(_,value)|value.trim())).unwrap();
        let accept=proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
        socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
        let mut input=[0;4096];assert!(socket.read(&mut input).await.unwrap()>0);
        socket.write_all(&websocket::encode_server(1,br#"{"type":"response.completed","response":{"id":"finished","output":[]}}"#).unwrap()).await.unwrap();
        let mut tail=Vec::new();let _=socket.read_to_end(&mut tail).await;
    }).await
}

async fn proxy(origin: SocketAddr, dir: &Path, io_timeout_ms: u64) -> NativeProxy {
    proxy_with_fault(origin, dir, io_timeout_ms, 350_000_000, Fault::None).await
}
async fn proxy_with_fault(
    origin: SocketAddr,
    dir: &Path,
    io_timeout_ms: u64,
    delay_ns: u64,
    fault: Fault,
) -> NativeProxy {
    let callbacks=[false,true].into_iter().map(|observer|json!({
        "binding":"direct","path":if observer{artifact(dir,true,0)}else{artifact_with_fault(dir,false,delay_ns,fault)},
        "package":"worker.metadata.test","hooks":if !observer && matches!(fault,Fault::Completion|Fault::CompletionDelay){vec!["admit","complete"]}else{vec![]},
        "message_codec":"codex.responses.v1","message_actions":true,"message_http_bridge":!observer,
        "timeout_ms":1000,"shutdown_timeout_ms":2000,
        "config":{"version":1,"decision":"forward","upstream":"connect","path":"/backend-api/codex/responses"}
    })).collect::<Vec<_>>();
    network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{origin}"),"io_timeout_ms":io_timeout_ms,"callbacks":callbacks})).unwrap()).await.unwrap()
}

async fn post(address: SocketAddr) -> TcpStream {
    let mut client = TcpStream::connect(address).await.unwrap();
    let body = br#"{"input":[]}"#;
    client.write_all(format!("POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    client.write_all(body).await.unwrap();
    assert!(head(&mut client).await.starts_with(b"HTTP/1.1 200"));
    client
}
async fn delivered_body(client: &mut TcpStream) -> Vec<u8> {
    let mut body = Vec::new();
    loop {
        let chunk = super::pipeline::chunk(client).await;
        if chunk.is_empty() {
            break;
        }
        body.extend(chunk);
    }
    let mut trailing = Vec::new();
    client.read_to_end(&mut trailing).await.unwrap();
    assert!(
        trailing.is_empty(),
        "cleanup appended another HTTP response: {:?}",
        trailing
    );
    body
}
async fn marker(path: &Path) {
    timeout(WAIT, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_http_bridge_cleanup_cannot_delay_another_owner() {
    let origin = completed_origin().await;
    let directory = tempfile::tempdir().unwrap();
    let proxy = proxy(origin.address, directory.path(), 1000).await;
    let worker = serve_engine(proxy.clone()).await;
    let mut client = post(worker.address).await;
    marker(&directory.path().join("actor.closed")).await;
    timeout(
        Duration::from_millis(100),
        marker(&directory.path().join("observer.closed")),
    )
    .await
    .expect("later owner was starved behind the first cleanup callback");
    let _ = delivered_body(&mut client).await;
    drop(worker);
    proxy.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_bridge_cleanup_deadline_does_not_report_success() {
    let origin = completed_origin().await;
    let directory = tempfile::tempdir().unwrap();
    let proxy = proxy(origin.address, directory.path(), 100).await;
    let worker = serve_engine(proxy.clone()).await;
    let mut client = post(worker.address).await;
    let body = delivered_body(&mut client).await;
    assert_eq!(
        body, br#"{"id":"finished","output":[]}"#,
        "cleanup must not append another HTTP error after delivery"
    );
    timeout(WAIT, async {
        while proxy.operations().snapshot().inflight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let outcome = proxy.operations().snapshot();
    assert_eq!(
        outcome.failed, 1,
        "cleanup timeout was reported as successful delivery"
    );
    assert_eq!(outcome.delivered, 0);
    marker(&directory.path().join("observer.closed")).await;
    drop(worker);
    proxy.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_cleanup_failure_notifies_other_owners_and_fails_delivery() {
    let origin = completed_origin().await;
    let directory = tempfile::tempdir().unwrap();
    let proxy = proxy_with_fault(origin.address, directory.path(), 1000, 0, Fault::Cleanup).await;
    let worker = serve_engine(proxy.clone()).await;
    let mut client = post(worker.address).await;
    let body = delivered_body(&mut client).await;
    assert_eq!(
        body, br#"{"id":"finished","output":[]}"#,
        "cleanup must not append another HTTP error after delivery"
    );
    timeout(WAIT, async {
        while proxy.operations().snapshot().inflight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let outcome = proxy.operations().snapshot();
    assert_eq!(outcome.failed, 1);
    assert_eq!(outcome.delivered, 0);
    marker(&directory.path().join("observer.closed")).await;
    drop(worker);
    proxy.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_failure_and_timeout_fail_delivery_once() {
    for fault in [Fault::Completion, Fault::CompletionDelay] {
        let origin = completed_origin().await;
        let directory = tempfile::tempdir().unwrap();
        let proxy = proxy_with_fault(origin.address, directory.path(), 100, 0, fault).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut client = post(worker.address).await;
        let body = delivered_body(&mut client).await;
        assert_eq!(
            body, br#"{"id":"finished","output":[]}"#,
            "cleanup must not append another HTTP error after delivery"
        );
        timeout(WAIT, async {
            while proxy.operations().snapshot().inflight != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let outcome = proxy.operations().snapshot();
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.delivered, 0);
        assert_eq!(
            std::fs::read(directory.path().join("actor.finish")).unwrap(),
            b"x"
        );
        drop(worker);
        proxy.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_cleanup_failure_does_not_report_delivery() {
    let origin = listen(|_| async { panic!("idle WebSocket must not dial") }).await;
    let directory = tempfile::tempdir().unwrap();
    let proxy = proxy_with_fault(origin.address, directory.path(), 1000, 0, Fault::Cleanup).await;
    let worker = serve_engine(proxy.clone()).await;
    let mut client = TcpStream::connect(worker.address).await.unwrap();
    client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
    client
        .write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap())
        .await
        .unwrap();
    let mut tail = Vec::new();
    client.read_to_end(&mut tail).await.unwrap();
    timeout(WAIT, async {
        while proxy.operations().snapshot().inflight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let outcome = proxy.operations().snapshot();
    assert_eq!(outcome.failed, 1);
    assert_eq!(outcome.delivered, 0);
    marker(&directory.path().join("observer.closed")).await;
    drop(worker);
    proxy.shutdown().await.unwrap();
}
