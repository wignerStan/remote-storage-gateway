//! Actual native Pingora engine: protocol selection, reuse and lazy byte flow.
use pingora_core::{
    apps::HttpServerApp, protocols::http::ServerSession, server::configuration::ServerConf,
};
use pingora_proxy::HttpProxy;
use pingora_worker::proxy::{NativeProxy, ProxyConfig};
use proxy_pingora::worker::WorkerControl;
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
struct Listening {
    address: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn listen<F, Fut>(serve: F) -> Listening
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (socket, _) = result.unwrap();
                    connections.spawn(serve(socket));
                }
                result = connections.join_next(), if !connections.is_empty() => { result.unwrap().unwrap(); }
            }
        }
    });
    Listening { address, task }
}
fn settings(
    upstream: SocketAddr,
    proxy: Option<SocketAddr>,
    protocol: &str,
    reuse: bool,
) -> ProxyConfig {
    ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        proxy: proxy.map(|proxy| format!("{protocol}://{proxy}").parse().unwrap()),
        connect_timeout: WAIT,
        io_timeout: WAIT,
        pool_idle_timeout: WAIT,
        pool_key: 42,
        reuse,
        http2: Default::default(),
        websocket_keepalive: Default::default(),
    }
}
#[cfg(unix)]
#[path = "native_proxy/api_codecs.rs"]
mod api_codecs;
#[cfg(unix)]
#[path = "native_proxy/hardening.rs"]
mod hardening;
#[path = "native_proxy/http2_keepalive.rs"]
mod http2_keepalive;
#[path = "native_proxy/native_hooks.rs"]
mod native_hooks;
#[path = "native_proxy/observability_workflow.rs"]
mod observability_workflow;
#[path = "native_proxy/pipeline.rs"]
mod pipeline;
#[cfg(unix)]
#[path = "native_proxy/realtime_codecs.rs"]
mod realtime_codecs;
#[cfg(unix)]
#[path = "native_proxy/remote_axis.rs"]
mod remote_axis;
#[cfg(unix)]
#[path = "native_proxy/response_streams.rs"]
mod response_streams;
#[cfg(unix)]
#[path = "native_proxy/websocket_abi.rs"]
mod websocket_abi;

async fn worker(config: ProxyConfig) -> Listening {
    worker_with_callbacks(config, vec![]).await
}
async fn worker_with_callbacks(
    config: ProxyConfig,
    callbacks: Vec<
        Arc<dyn proxy_pingora::worker::surface::Callbacks<pingora_worker::proxy::ProxyContract>>,
    >,
) -> Listening {
    worker_with_control(config, Arc::new(WorkerControl::default()), callbacks).await
}
async fn worker_with_control(
    config: ProxyConfig,
    control: Arc<WorkerControl>,
    callbacks: Vec<
        Arc<dyn proxy_pingora::worker::surface::Callbacks<pingora_worker::proxy::ProxyContract>>,
    >,
) -> Listening {
    let proxy = NativeProxy::new(config, control, callbacks).unwrap();
    serve_engine(proxy).await
}
async fn serve_engine<P>(proxy: P) -> Listening
where
    P: pingora_proxy::ProxyHttp + Send + Sync + 'static,
    P::CTX: Send + Sync,
{
    let mut engine = HttpProxy::new(
        proxy,
        Arc::new(ServerConf {
            max_retries: 1,
            ..ServerConf::default()
        }),
    );
    engine.handle_init_modules();
    let engine = Arc::new(engine);
    listen(move |socket| {
        let engine = Arc::clone(&engine);
        async move {
            let native = pingora_core::protocols::l4::stream::Stream::from(socket);
            #[cfg(unix)]
            let native = {
                use pingora_core::protocols::{GetSocketDigest, SocketDigest};
                use std::os::fd::AsRawFd;
                // Match native Listener::accept: metadata comes from the live socket.
                let digest = SocketDigest::from_raw_fd(native.as_raw_fd());
                let mut native = native;
                native.set_socket_digest(digest);
                native
            };
            let session = ServerSession::new_http1(Box::new(native));
            let (_stop, shutdown) = tokio::sync::watch::channel(false);
            let _ = engine.process_new_http(session, &shutdown).await;
        }
    })
    .await
}
async fn head(socket: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let Ok(byte) = socket.read_u8().await else {
            return Vec::new();
        };
        head.push(byte);
        assert!(head.len() <= 65536);
    }
    head
}
async fn get(address: SocketAddr) -> Vec<u8> {
    timeout(WAIT, async {
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"GET /resource?q=yes HTTP/1.1\r\nHost: downstream\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        output
    })
    .await
    .unwrap()
}
async fn origin(count: Arc<AtomicUsize>) -> Listening {
    listen(move |mut socket| {
        count.fetch_add(1, Ordering::SeqCst);
        async move {
            loop {
                let request = head(&mut socket).await;
                if request.is_empty() {
                    break;
                }
                assert!(request.starts_with(b"GET /resource?q=yes HTTP/1.1"));
                if socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    })
    .await
}

#[tokio::test]
async fn direct_origin_reuses_complete_connections_and_explicit_bypass_does_not() {
    for reuse in [true, false] {
        let connections = Arc::new(AtomicUsize::new(0));
        let origin = origin(Arc::clone(&connections)).await;
        let worker = worker(settings(origin.address, None, "", reuse)).await;
        for _ in 0..3 {
            let response = get(worker.address).await;
            assert!(response.starts_with(b"HTTP/1.1 200"), "{response:?}");
            assert!(response.ends_with(b"hello"));
        }
        assert_eq!(
            connections.load(Ordering::SeqCst),
            if reuse { 1 } else { 3 }
        );
    }
}

async fn tunnel(
    protocol: &'static str,
    upstream: SocketAddr,
    accepted: Arc<AtomicUsize>,
    deny: bool,
) -> Listening {
    listen(move |mut client| {
        accepted.fetch_add(1, Ordering::SeqCst);
        async move {
            if protocol == "http" {
                let request = head(&mut client).await;
                assert!(request.starts_with(format!("CONNECT {upstream} HTTP/1.1").as_bytes()));
                if deny {
                    client
                        .write_all(b"HTTP/1.1 403 Denied\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                    return;
                }
                client
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
            } else {
                assert_eq!(client.read_u8().await.unwrap(), 5);
                let count = client.read_u8().await.unwrap();
                let mut methods = vec![0; usize::from(count)];
                client.read_exact(&mut methods).await.unwrap();
                assert!(methods.contains(&0));
                client.write_all(&[5, 0]).await.unwrap();
                let mut prefix = [0; 4];
                client.read_exact(&mut prefix).await.unwrap();
                assert_eq!(&prefix[..3], &[5, 1, 0]);
                let host = match prefix[3] {
                    1 => {
                        let mut ip = [0; 4];
                        client.read_exact(&mut ip).await.unwrap();
                        std::net::Ipv4Addr::from(ip).to_string()
                    }
                    3 => {
                        let len = client.read_u8().await.unwrap();
                        let mut name = vec![0; usize::from(len)];
                        client.read_exact(&mut name).await.unwrap();
                        String::from_utf8(name).unwrap()
                    }
                    other => panic!("unexpected address kind {other}"),
                };
                assert_eq!(host, upstream.ip().to_string());
                assert_eq!(client.read_u16().await.unwrap(), upstream.port());
                client
                    .write_all(&[5, u8::from(deny), 0, 1, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
                if deny {
                    return;
                }
            }
            let mut destination = TcpStream::connect(upstream).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut destination).await;
        }
    })
    .await
}

#[tokio::test]
async fn selected_connect_and_socks_tunnels_are_pooled_and_never_fall_back_on_denial() {
    for protocol in ["http", "socks5", "socks5h"] {
        for deny in [false, true] {
            let connections = Arc::new(AtomicUsize::new(0));
            let origin = origin(Arc::clone(&connections)).await;
            let tunnels = Arc::new(AtomicUsize::new(0));
            let tunnel = tunnel(protocol, origin.address, Arc::clone(&tunnels), deny).await;
            let worker = worker(settings(
                origin.address,
                Some(tunnel.address),
                protocol,
                true,
            ))
            .await;
            let response = get(worker.address).await;
            if deny {
                assert!(!response.starts_with(b"HTTP/1.1 200"));
                assert_eq!(
                    connections.load(Ordering::SeqCst),
                    0,
                    "selected proxy must not fall back"
                );
            } else {
                assert!(response.ends_with(b"hello"), "{response:?}");
                assert!(get(worker.address).await.ends_with(b"hello"));
                assert_eq!(connections.load(Ordering::SeqCst), 1);
            }
            assert_eq!(tunnels.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn request_and_response_bodies_move_before_eof_with_native_backpressure() {
    const LENGTH: usize = 256 * 1024;
    let first_upload = Arc::new(tokio::sync::Notify::new());
    let upload_seen = Arc::clone(&first_upload);
    let release_download = Arc::new(tokio::sync::Notify::new());
    let release = Arc::clone(&release_download);
    let origin = listen(move |mut socket| {
        let upload_seen = Arc::clone(&upload_seen);
        let release = Arc::clone(&release);
        async move {
            assert!(
                head(&mut socket)
                    .await
                    .starts_with(b"POST /upload HTTP/1.1")
            );
            let mut chunk = [0; 1024];
            socket.read_exact(&mut chunk).await.unwrap();
            assert_eq!(chunk, [b'x'; 1024]);
            upload_seen.notify_one();
            for _ in 1..LENGTH / chunk.len() {
                socket.read_exact(&mut chunk).await.unwrap();
                assert_eq!(chunk, [b'x'; 1024]);
            }
            socket
                .write_all(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {LENGTH}\r\n\r\n").as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&[b'y'; 1024]).await.unwrap();
            release.notified().await;
            for _ in 1..LENGTH / chunk.len() {
                socket.write_all(&[b'y'; 1024]).await.unwrap();
            }
        }
    })
    .await;
    let worker = worker(settings(origin.address, None, "", true)).await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(format!("POST /upload HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: {LENGTH}\r\n\r\n").as_bytes()).await.unwrap();
        client.write_all(&[b'x'; 1024]).await.unwrap();
        first_upload.notified().await; // A collecting proxy would deadlock here.
        for _ in 1..LENGTH / 1024 { client.write_all(&[b'x'; 1024]).await.unwrap(); }
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 200"));
        let mut first = [0; 1024];
        client.read_exact(&mut first).await.unwrap();
        assert_eq!(first, [b'y'; 1024]);
        release_download.notify_one(); // Response forwarding is also lazy.
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail.len(), LENGTH - 1024);
        assert!(tail.iter().all(|byte| *byte == b'y'));
    }).await.unwrap();
}

#[tokio::test]
async fn websocket_upgrade_keeps_native_full_duplex_stream() {
    let origin = listen(|mut socket| async move {
        let request = head(&mut socket).await;
        assert!(String::from_utf8_lossy(&request).to_ascii_lowercase().contains("upgrade: websocket"));
        socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
        let mut frame = [0; 8];
        socket.read_exact(&mut frame).await.unwrap();
        assert_eq!(frame, [0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2]);
        socket.write_all(&[0x81, 2, b'h', b'i']).await.unwrap();
    }).await;
    let mut config = settings(origin.address, None, "", true);
    config.http2.enabled = true;
    config.http2.ping_interval_ms = Some(20);
    let worker = worker(config).await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client.write_all(&[0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2]).await.unwrap();
        let mut reply = [0; 4];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0x81, 2, b'h', b'i']);
    }).await.unwrap();
}

#[cfg(unix)]
#[path = "native_proxy/go_carrier.rs"]
mod go_carrier;

#[path = "native_proxy/websocket_keepalive.rs"]
mod websocket_keepalive;

#[cfg(unix)]
#[path = "native_proxy/runtime_ownership.rs"]
mod runtime_ownership;

#[cfg(feature = "wasm")]
#[path = "native_proxy/wasm_callbacks.rs"]
mod wasm_callbacks;

#[cfg(unix)]
#[path = "native_proxy/message_sessions.rs"]
mod message_sessions;

#[cfg(unix)]
#[path = "native_proxy/peer_metadata.rs"]
mod peer_metadata;

#[tokio::test]
async fn native_operations_count_upload_response_and_completion() {
    use diagnostic_output::operations::Operations;
    let operations = Arc::new(Operations::default());
    let upstream = listen(|mut socket| async move {
        let request = head(&mut socket).await;
        assert!(request.starts_with(b"POST /resource HTTP/1.1"));
        let mut upload = [0; 7];
        socket.read_exact(&mut upload).await.unwrap();
        assert_eq!(&upload, b"payload");
        socket
            .write_all(
                b"HTTP/1.1 201 Created\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            )
            .await
            .unwrap();
    })
    .await;
    let proxy = NativeProxy::new(
        settings(upstream.address, None, "", false),
        Arc::new(WorkerControl::default()),
        vec![],
    )
    .unwrap()
    .with_operations(Arc::clone(&operations));
    assert!(Arc::ptr_eq(&proxy.operations(), &operations));
    let worker = serve_engine(proxy).await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"POST /resource HTTP/1.1\r\nHost: downstream\r\nContent-Length: 7\r\nConnection: close\r\n\r\npayload").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 201"));
        assert!(response.ends_with(b"hello"));
        while operations.snapshot().inflight != 0 {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let value = operations.snapshot();
    assert_eq!(value.started, 1);
    assert_eq!(value.delivered, 1);
    assert_eq!(value.failed, 0);
    assert_eq!(value.completion_failed, 0);
    assert_eq!(value.cancelled, 0);
    assert_eq!(value.request_bytes, 7);
    assert_eq!(value.response_bytes, 5);
    assert_eq!(value.latency_count, 1);
    assert_eq!(value.status_classes[2], 1);
}

#[tokio::test]
async fn native_operation_drop_cancels_and_duplicate_logging_finishes_once() {
    use pingora_proxy::{ProxyHttp, Session};
    let proxy = NativeProxy::new(
        settings("127.0.0.1:1".parse().unwrap(), None, "", false),
        Arc::new(WorkerControl::default()),
        vec![],
    )
    .unwrap();
    let operations = proxy.operations();
    let cancelled = proxy.new_ctx();
    assert_eq!(operations.snapshot().inflight, 1);
    drop(cancelled);
    assert_eq!(operations.snapshot().cancelled, 1);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let mut session = Session::new_h1(Box::new(pingora_core::protocols::l4::stream::Stream::from(
        socket,
    )));
    let mut context = proxy.new_ctx();
    let error =
        pingora_error::Error::explain(pingora_error::ErrorType::HTTPStatus(503), "test failure");
    proxy
        .logging(&mut session, Some(&error), &mut context)
        .await;
    proxy
        .logging(&mut session, Some(&error), &mut context)
        .await;
    drop(context);
    let value = operations.snapshot();
    assert_eq!(value.started, 2);
    assert_eq!(value.inflight, 0);
    assert_eq!(value.failed, 1);
    assert_eq!(value.cancelled, 1);
    assert_eq!(value.latency_count, 1);
    assert_eq!(value.status_classes[0], 1);
}

#[tokio::test]
async fn native_operation_completion_failure_preserves_delivered_status() {
    use diagnostic_output::operations::Operations;
    use pingora_worker::proxy::{ProxyContract, RequestFacts};
    use proxy_pingora::worker::surface::Callbacks;
    struct FailCompletion;
    #[async_trait::async_trait]
    impl Callbacks<ProxyContract> for FailCompletion {
        async fn authenticate(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
            Ok(Some(()))
        }
        async fn admit(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
            Ok(Some(()))
        }
        async fn complete(&self, _: &RequestFacts) -> Result<u32, String> {
            Err("completion rejected".into())
        }
        async fn maintain(&self) {}
    }
    let operations = Arc::new(Operations::default());
    let upstream = origin(Arc::new(AtomicUsize::new(0))).await;
    let proxy = NativeProxy::new(
        settings(upstream.address, None, "", false),
        Arc::new(WorkerControl::default()),
        vec![Arc::new(FailCompletion)],
    )
    .unwrap()
    .with_operations(Arc::clone(&operations));
    let worker = serve_engine(proxy).await;
    assert!(get(worker.address).await.starts_with(b"HTTP/1.1 200"));
    timeout(WAIT, async {
        while operations.snapshot().inflight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let value = operations.snapshot();
    assert_eq!(value.started, 1);
    assert_eq!(value.delivered, 0);
    assert_eq!(value.failed, 0);
    assert_eq!(value.completion_failed, 1);
    assert_eq!(value.cancelled, 0);
    assert_eq!(value.response_bytes, 5);
    assert_eq!(value.status_classes[2], 1);
}

#[path = "native_proxy/stress.rs"]
mod stress;
