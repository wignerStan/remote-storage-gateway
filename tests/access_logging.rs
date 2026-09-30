//! Real carrier completion reaches the independently configured file sink.
use diagnostic_output::runtime::{FileLogConfig, LoggingConfig, build_logging};
use pingora_core::{protocols::http::ServerSession, server::ShutdownWatch};
use proxy_pingora::http_ingress::{Handler, Response, Worker};
use serde_json::Value;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tracing::instrument::WithSubscriber;

struct Respond;
#[async_trait::async_trait]
impl Handler for Respond {
    async fn handle(&self, mut session: ServerSession, _: &ShutdownWatch) -> Option<ServerSession> {
        Response {
            status: 201,
            headers: vec![],
            body: b"response-body-not-for-access-log".to_vec(),
        }
        .write(&mut session, true)
        .await
        .unwrap();
        Some(session)
    }
    fn capacity_response(&self) -> Response {
        Response {
            status: 503,
            headers: vec![],
            body: Vec::new(),
        }
    }
}

#[tokio::test]
async fn real_access_record_persists_with_diagnostics_off_and_bodies_disabled() {
    let directory = tempfile::tempdir().unwrap();
    let access = directory.path().join("access");
    let (subscriber, guard) = build_logging(LoggingConfig {
        stderr_filter: "off".into(),
        access_file: Some(FileLogConfig {
            directory: access.clone(),
            keep_files: 30,
        }),
        body_file: None,
        queue_capacity: 128,
    })
    .unwrap();
    let run = async {
        assert!(!tracing::enabled!(target: "proxy_pingora::body_dump", tracing::Level::TRACE));
        tracing::trace!(target: "proxy_pingora::body_dump", body = "disabled-body-sentinel");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        client.write_all(b"GET /resource?api_key=query-secret HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer header-secret\r\nConnection: close\r\n\r\n").await.unwrap();
        let stream = pingora_core::protocols::l4::stream::Stream::from(stream);
        let mut session = ServerSession::new_http1(Box::new(stream));
        assert!(session.read_request().await.unwrap());
        let worker = Arc::new(Worker::new(Respond, NonZeroUsize::new(1).unwrap()));
        let (_send, shutdown) = watch::channel(false);
        let _reuse = worker.serve(session, &shutdown).await;
        let mut wire = Vec::new();
        client.read_to_end(&mut wire).await.unwrap();
        assert!(wire.starts_with(b"HTTP/1.1 201"));
    };
    tokio::time::timeout(Duration::from_secs(5), run.with_subscriber(subscriber))
        .await
        .unwrap();
    assert_eq!(guard.dropped_records(), 0);
    drop(guard);

    let files = std::fs::read_dir(&access)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    let text = std::fs::read_to_string(&files[0]).unwrap();
    let records = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    let fields = &records[0]["fields"];
    assert_eq!(fields["method"], "GET");
    assert_eq!(fields["path"], "/resource");
    assert_eq!(fields["status"], 201);
    assert!(fields["request_id"].as_u64().unwrap() > 0);
    assert!(!text.contains("query-secret"));
    assert!(!text.contains("header-secret"));
    assert!(!text.contains("response-body-not-for-access-log"));
    assert!(!text.contains("disabled-body-sentinel"));
}
