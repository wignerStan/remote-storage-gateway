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
    }
}
#[path = "native_proxy/native_hooks.rs"]
mod native_hooks;
#[path = "native_proxy/pipeline.rs"]
mod pipeline;

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
    let worker = worker(settings(origin.address, None, "", true)).await;
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
