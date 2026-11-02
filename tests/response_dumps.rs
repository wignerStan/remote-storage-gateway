#![cfg(feature = "l7")]
//! Explicit response capture observes the native stream without delaying delivery.
use diagnostic_output::runtime::{FileLogConfig, LoggingConfig, build_logging};
use network_data_plane_worker::proxy::{NativeProxy, ProxyConfig};
use pingora_core::{
    apps::HttpServerApp, protocols::http::ServerSession, server::configuration::ServerConf,
};
use pingora_proxy::HttpProxy;
use proxy_pingora::worker::WorkerControl;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tracing::instrument::WithSubscriber;

const WAIT: Duration = Duration::from_secs(5);

async fn capture_response(
    content_type: &str,
    encoding: Option<&str>,
    chunks: Vec<Vec<u8>>,
    limit: usize,
    enabled: bool,
) -> (String, Vec<Value>) {
    tokio::time::timeout(WAIT, async {
        let directory = tempfile::tempdir().unwrap();
        let body_directory = directory.path().join("body");
        let (subscriber, guard) = build_logging(LoggingConfig {
            stderr_filter: "off".into(),
            access_file: Some(FileLogConfig { directory: directory.path().join("access"), keep_files: 1 }),
            body_file: enabled.then(|| FileLogConfig { directory: body_directory.clone(), keep_files: 1 }),
            queue_capacity: 128,
        }).unwrap();
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = origin.local_addr().unwrap();
        let release = Arc::new(Notify::new());
        let ready_to_finish = Arc::clone(&release);
        let first_payload = chunks[0].clone();
        let content_type = content_type.to_owned();
        let encoding = encoding.map(str::to_owned);
        let origin_task = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") { head.push(socket.read_u8().await.unwrap()); }
            let encoding = encoding.map(|value| format!("Content-Encoding: {value}\r\n")).unwrap_or_default();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n{encoding}Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            for (index, chunk) in chunks.iter().enumerate() {
                socket.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
                socket.write_all(chunk).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                if index == 0 { ready_to_finish.notified().await; }
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = NativeProxy::new(ProxyConfig {
            upstream: format!("http://{upstream}").parse().unwrap(), proxy: None,
            connect_timeout: WAIT, io_timeout: WAIT, pool_idle_timeout: WAIT,
            pool_key: 0, reuse: false, http2: Default::default(), websocket_keepalive: Default::default(),
        }, Arc::new(WorkerControl::default()), vec![]).unwrap().with_body_dump_limit(limit);
        let mut engine = HttpProxy::new(proxy, Arc::new(ServerConf::default()));
        engine.handle_init_modules();
        let engine = Arc::new(engine);
        let address = downstream.local_addr().unwrap();
        let worker = tokio::spawn(async move {
            let (socket, _) = downstream.accept().await.unwrap();
            let session = ServerSession::new_http1(Box::new(pingora_core::protocols::l4::stream::Stream::from(socket)));
            let (_sender, shutdown) = tokio::sync::watch::channel(false);
            let _ = engine.process_new_http(session, &shutdown).await;
        }.with_subscriber(subscriber));
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"GET /stream HTTP/1.1\r\nHost: downstream\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut wire = Vec::new();
        // The origin cannot send EOF until the first complete chunk reaches us.
        while !wire.windows(first_payload.len()).any(|part| part == first_payload) {
            wire.push(client.read_u8().await.unwrap());
        }
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        release.notify_one();
        client.read_to_end(&mut wire).await.unwrap();
        origin_task.await.unwrap();
        worker.await.unwrap();
        assert_eq!(guard.dropped_records(), 0);
        drop(guard);
        if !enabled {
            assert!(!body_directory.exists(), "disabled capture created a body sink");
            return (String::new(), vec![]);
        }
        let mut text = String::new();
        for file in std::fs::read_dir(body_directory).unwrap() {
            text.push_str(&std::fs::read_to_string(file.unwrap().path()).unwrap());
        }
        let records = text.lines().map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|record| record["fields"]["direction"] == "response").collect();
        (text, records)
    }).await.unwrap()
}

#[tokio::test]
async fn sse_response_dump_emits_once_after_streaming_and_hashes_only_credentials() {
    let first = br#"data: {"type":"response.output_text.delta","delta":"streamed","api_key":"response-api-secret","jwt":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature","cookie":"access_token=session-secret; route=node-2; theme=light"}

"#.to_vec();
    let last =
        b"data: {\"type\":\"response.completed\",\"ordinary\":\"visible\"}\n\ndata: [DONE]\n\n"
            .to_vec();
    let (text, records) = capture_response(
        "text/event-stream; charset=utf-8",
        None,
        vec![first, last],
        4096,
        true,
    )
    .await;
    assert_eq!(records.len(), 1, "{text}");
    let fields = &records[0]["fields"];
    assert_eq!(fields["outcome"], "captured");
    assert_eq!(fields["status"], 200);
    let detail = fields["detail"].as_str().unwrap();
    assert!(detail.contains("sha256:"));
    assert!(detail.contains("streamed") && detail.contains("visible"));
    assert!(detail.contains("route=node-2") && detail.contains("theme=light"));
    for secret in [
        "response-api-secret",
        "session-secret",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature",
    ] {
        assert!(!text.contains(secret), "raw credential leaked");
    }
}

#[tokio::test]
async fn oversized_encoded_and_disabled_response_dumps_preserve_native_delivery() {
    let (_, oversized) = capture_response(
        "application/json",
        None,
        vec![format!("{{\"ordinary\":\"{}\"}}", "x".repeat(256)).into_bytes()],
        64,
        true,
    )
    .await;
    assert_eq!(oversized.len(), 1);
    assert_eq!(oversized[0]["fields"]["outcome"], "size_limit");
    assert_eq!(oversized[0]["fields"]["detail"], "null");
    let (_, encoded) = capture_response(
        "application/json",
        Some("gzip"),
        vec![b"opaque-compressed-bytes".to_vec()],
        1024,
        true,
    )
    .await;
    assert_eq!(encoded.len(), 1);
    assert_eq!(encoded[0]["fields"]["outcome"], "content_encoding");
    assert_eq!(encoded[0]["fields"]["detail"], "null");
    let (_, disabled) = capture_response(
        "text/event-stream",
        None,
        vec![b"data: {\"visible\":true}\n\n".to_vec()],
        1024,
        false,
    )
    .await;
    assert!(disabled.is_empty());
}
