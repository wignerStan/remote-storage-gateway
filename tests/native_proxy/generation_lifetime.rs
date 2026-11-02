//! Physical age, detached settlement, private ownership, and bounded shutdown.
use super::*;
use proxy_pingora::{connect_upstream::websocket_session::handshake, http_ingress::ProxyLifecycle};
use proxy_protocols::websocket;
use serde_json::json;
use std::path::Path;
use tokio::sync::{Notify, mpsc};
const CREATE: &[u8] = br#"{"type":"response.create","model":"m","input":[]}"#;
const COMPLETE: &[u8] = br#"{"type":"response.completed","response":{"id":"done","output":[]}}"#;

#[path = "../support/generation_lifetime_fixture.rs"]
mod fixture;
use fixture::artifact;
async fn proxy(
    origin: SocketAddr,
    dir: &Path,
    retire: u64,
    close: u64,
    retain: bool,
    capacity: usize,
) -> NativeProxy {
    proxy_with_artifact(origin, artifact(dir, retire, close, retain), capacity).await
}
async fn proxy_with_artifact(
    origin: SocketAddr,
    artifact: std::path::PathBuf,
    capacity: usize,
) -> NativeProxy {
    proxy_with_io(origin, artifact, capacity, 3000).await
}
async fn proxy_with_io(
    origin: SocketAddr,
    artifact: std::path::PathBuf,
    capacity: usize,
    io_timeout_ms: u64,
) -> NativeProxy {
    network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({
        "upstream":format!("http://{origin}"), "shutdown_seconds":1,"io_timeout_ms":io_timeout_ms,
        "inspection":{"max_requests":capacity},
        "callbacks":[{"binding":"direct","path":artifact,"kind":if artifact.file_name().unwrap()=="lifetime-function.so" {"function"}else{"object"},"package":"worker.sessions.test",
        "message_codec":"openai.responses.v1","message_actions":true,"message_http_bridge":true}]
    })).unwrap()).await.unwrap()
}
async fn client(address: SocketAddr) -> TcpStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    assert!(head(&mut socket).await.starts_with(b"HTTP/1.1 101"));
    socket
}
async fn frame(socket: &mut TcpStream, masked: bool) -> (u8, Vec<u8>) {
    let op = socket.read_u8().await.unwrap() & 15;
    let second = socket.read_u8().await.unwrap();
    assert_eq!(second & 128 != 0, masked);
    let len = match second & 127 {
        126 => socket.read_u16().await.unwrap() as usize,
        127 => socket.read_u64().await.unwrap() as usize,
        n => n as usize,
    };
    assert!(len < 4096);
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await.unwrap();
    }
    let mut body = vec![0; len];
    socket.read_exact(&mut body).await.unwrap();
    if masked {
        for (i, b) in body.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    (op, body)
}
async fn upstream(socket: &mut TcpStream) {
    let request = String::from_utf8(head(socket).await).unwrap();
    assert!(request.starts_with("GET /v1/responses HTTP/1.1\r\n"));
    let key = request
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, value)| value.trim())
        })
        .unwrap();
    let accept = handshake::accept_key(key).unwrap();
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
    assert_eq!(frame(socket, true).await, (1, CREATE.to_vec()));
}
async fn create(socket: &mut TcpStream) {
    socket
        .write_all(&websocket::encode_client(1, CREATE).unwrap())
        .await
        .unwrap();
}
async fn terminal(socket: &mut TcpStream) {
    socket
        .write_all(&websocket::encode_server(1, COMPLETE).unwrap())
        .await
        .unwrap();
}
fn events(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("events")).unwrap_or_default()
}
async fn marker(dir: &Path, expected: char) {
    timeout(WAIT, async {
        while !events(dir).contains(expected) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_active_turn_drains_past_close_age_then_cleans_once() {
    timeout(Duration::from_secs(15), async {
        let sent = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());
        let origin = listen({
            let sent = sent.clone();
            let release = release.clone();
            let closed = closed.clone();
            move |mut socket| {
                let sent = sent.clone();
                let release = release.clone();
                let closed = closed.clone();
                async move {
                    upstream(&mut socket).await;
                    sent.notify_one();
                    release.notified().await;
                    terminal(&mut socket).await;
                    assert_eq!(frame(&mut socket, true).await.0, 8);
                    closed.notify_one();
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path(), 40, 80, true, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        sent.notified().await;
        drop(socket);
        marker(dir.path(), 'd').await;
        assert!(
            !events(dir.path()).contains('c'),
            "active lease cleaned before actual terminal"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!events(dir.path()).contains('c'));
        release.notify_one();
        closed.notified().await;
        marker(dir.path(), 'c').await;
        assert_eq!(events(dir.path()), "dtc");
        assert_eq!(
            proxy.operations().snapshot().delivered,
            0,
            "local 101 was counted as business delivery"
        );
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_idle_generation_services_ping_and_new_client_gets_new_socket() {
    timeout(Duration::from_secs(15), async {
        let (send, mut received) = mpsc::unbounded_channel();
        let first_ping = Arc::new(Notify::new());
        let start = Arc::new(AtomicUsize::new(0));
        let origin = listen({
            let first_ping = first_ping.clone();
            move |mut socket| {
                let send = send.clone();
                let first_ping = first_ping.clone();
                let start = start.clone();
                async move {
                    let id = start.fetch_add(1, Ordering::SeqCst);
                    upstream(&mut socket).await;
                    terminal(&mut socket).await;
                    send.send((id, "turn")).unwrap();
                    if id == 0 {
                        first_ping.notified().await;
                        socket
                            .write_all(&websocket::encode_server(9, b"still-owned").unwrap())
                            .await
                            .unwrap();
                        assert_eq!(
                            frame(&mut socket, true).await,
                            (10, b"still-owned".to_vec())
                        );
                        send.send((id, "pong")).unwrap();
                    }
                    assert_eq!(frame(&mut socket, true).await.0, 8);
                    send.send((id, "close")).unwrap();
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path(), 800, 1200, true, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut first = client(worker.address).await;
        create(&mut first).await;
        assert_eq!(frame(&mut first, false).await, (1, COMPLETE.to_vec()));
        assert_eq!(received.recv().await.unwrap(), (0, "turn"));
        drop(first);
        marker(dir.path(), 'c').await;
        assert_eq!(events(dir.path()), "tdc");
        first_ping.notify_one();
        assert_eq!(received.recv().await.unwrap(), (0, "pong"));
        let mut second = client(worker.address).await;
        create(&mut second).await;
        assert_eq!(frame(&mut second, false).await, (1, COMPLETE.to_vec()));
        assert_eq!(received.recv().await.unwrap(), (1, "turn"));
        drop(second);
        // An idle socket is not closed at detach, nor handed to another client.
        assert!(
            timeout(Duration::from_millis(100), received.recv())
                .await
                .is_err()
        );
        proxy.shutdown().await.unwrap();
        let a = received.recv().await.unwrap();
        let b = received.recv().await.unwrap();
        assert_ne!(a.0, b.0);
        assert_eq!(a.1, "close");
        assert_eq!(b.1, "close");
        drop(worker);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_uses_physical_age_and_stops_attached_reuse() {
    timeout(Duration::from_secs(15), async {
        let (send, mut received) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let origin = listen(move |mut socket| {
            let send = send.clone();
            let count = count.clone();
            async move {
                let id = count.fetch_add(1, Ordering::SeqCst);
                upstream(&mut socket).await;
                terminal(&mut socket).await;
                send.send((id, "turn", tokio::time::Instant::now()))
                    .unwrap();
                if id == 0 {
                    assert_eq!(frame(&mut socket, true).await.0, 8);
                    send.send((id, "close", tokio::time::Instant::now()))
                        .unwrap();
                } else {
                    assert_eq!(
                        socket.read_u8().await.unwrap_err().kind(),
                        std::io::ErrorKind::UnexpectedEof
                    );
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path(), 150, 300, false, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        assert_eq!(frame(&mut socket, false).await.0, 1);
        let first = received.recv().await.unwrap();
        marker(dir.path(), 'u').await;
        assert!(!events(dir.path()).contains('c'));
        create(&mut socket).await;
        assert_eq!(frame(&mut socket, false).await.0, 1);
        let second = received.recv().await.unwrap();
        assert_eq!(second.0, 1, "retired physical generation was reused");
        let close = received.recv().await.unwrap();
        assert_eq!((close.0, close.1), (0, "close"));
        assert!(
            close.2.duration_since(first.2) >= Duration::from_millis(250),
            "retire closed before provider close age"
        );
        drop(socket);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_capacity_is_reserved_before_dial_and_http_cannot_retain() {
    timeout(Duration::from_secs(15),async {
        let count=Arc::new(AtomicUsize::new(0));
        let origin=listen({let count=count.clone();move|mut socket|{let count=count.clone();async move {count.fetch_add(1,Ordering::SeqCst);upstream(&mut socket).await;terminal(&mut socket).await;let _=frame(&mut socket,true).await;}}}).await;
        let dir=tempfile::tempdir().unwrap();let proxy=proxy(origin.address,dir.path(),1000,2000,true,1).await;let worker=serve_engine(proxy.clone()).await;
        let mut first=client(worker.address).await;create(&mut first).await;assert_eq!(frame(&mut first,false).await.0,1);drop(first);marker(dir.path(),'c').await;
        let mut second=client(worker.address).await;create(&mut second).await;assert_eq!(frame(&mut second,false).await.0,8);assert_eq!(count.load(Ordering::SeqCst),1,"capacity refusal dialed another business upstream");drop(second);
        // Let the failed downstream release request capacity before HTTP ingress.
        timeout(WAIT,async {while proxy.operations().snapshot().inflight!=0 {tokio::task::yield_now().await;}}).await.unwrap();
        let mut http=TcpStream::connect(worker.address).await.unwrap();let body=br#"{"model":"m","input":[]}"#;
        http.write_all(format!("POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();http.write_all(body).await.unwrap();assert!(head(&mut http).await.starts_with(b"HTTP/1.1 502"));assert_eq!(count.load(Ordering::SeqCst),1);
        drop(worker);proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_fin_keeps_received_metadata_and_slow_terminal_settlement_owned() {
    timeout(Duration::from_secs(15), async {
        let sent = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let origin = listen({
            let sent = sent.clone();
            let release = release.clone();
            move |mut socket| {
                let sent = sent.clone();
                let release = release.clone();
                async move {
                    upstream(&mut socket).await;
                    sent.notify_one();
                    release.notified().await;
                    socket
                        .write_all(
                            &websocket::encode_server(
                                1,
                                br#"{"type":"response.created","response":{"id":"inflight"}}"#,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    terminal(&mut socket).await;
                    // FIN arrives while the created callback is still running. Both
                    // received events retain their native owner through settlement.
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let artifact = fixture::artifact_with_delay(dir.path(), 40, 80, true, 200_000_000);
        let proxy = proxy_with_artifact(origin.address, artifact, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        sent.notified().await;
        drop(socket);
        marker(dir.path(), 'd').await;
        release.notify_one();
        marker(dir.path(), 'c').await;
        assert_eq!(events(dir.path()), "dmtc");
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_that_crosses_retirement_cannot_forward_to_old_or_new_socket() {
    timeout(Duration::from_secs(15), async {
        let (send, mut received) = mpsc::unbounded_channel();
        let origin = listen(move |mut socket| {
            let send = send.clone();
            async move {
                upstream(&mut socket).await;
                terminal(&mut socket).await;
                send.send("first").unwrap();
                // There must be no second create on this retired physical socket.
                assert_eq!(frame(&mut socket, true).await.0, 8);
                send.send("close").unwrap();
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let artifact =
            fixture::artifact_with_admission_delay(dir.path(), 150, 450, true, 800_000_000);
        let proxy = proxy_with_artifact(origin.address, artifact, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        assert_eq!(frame(&mut socket, false).await.0, 1);
        assert_eq!(received.recv().await.unwrap(), "first");
        create(&mut socket).await;
        timeout(Duration::from_millis(400), marker(dir.path(), 'u'))
            .await
            .expect("retire waited for the 800ms admission callback instead of cancelling it");
        assert_eq!(
            frame(&mut socket, false).await.0,
            8,
            "stale admission must get an explicit close"
        );
        marker(dir.path(), 'u').await;
        assert_eq!(received.recv().await.unwrap(), "close");
        assert_eq!(
            events(dir.path()).matches('t').count(),
            1,
            "stale create reached a new upstream"
        );
        drop(socket);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_idle_interrupt_does_not_extend_turn_or_native_read_timeout() {
    timeout(Duration::from_secs(15), async {
        let ready = Arc::new(Notify::new());
        let ping = Arc::new(Notify::new());
        let done = Arc::new(Notify::new());
        let origin = listen({
            let ready = ready.clone();
            let ping = ping.clone();
            let done = done.clone();
            move |mut socket| {
                let ready = ready.clone();
                let ping = ping.clone();
                let done = done.clone();
                async move {
                    upstream(&mut socket).await;
                    terminal(&mut socket).await;
                    assert_eq!(
                        frame(&mut socket, true).await.1,
                        br#"{"type":"response.interrupt","response_id":"done","mode":"discard_partial_items"}"#
                    );
                    ready.notify_one();
                    ping.notified().await;
                    socket
                        .write_all(
                            &websocket::encode_server(9, b"idle-after-read-timeout").unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        frame(&mut socket, true).await,
                        (10, b"idle-after-read-timeout".to_vec())
                    );
                    assert_eq!(frame(&mut socket, true).await.0, 8);
                    done.notify_one();
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let artifact = artifact(dir.path(), 400, 650, true);
        let proxy = proxy_with_io(origin.address, artifact, 8, 120).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        assert_eq!(frame(&mut socket, false).await.0, 1);
        socket
            .write_all(&websocket::encode_client(1, br#"{"type":"response.interrupt","response_id":"done","mode":"discard_partial_items"}"#).unwrap())
            .await
            .unwrap();
        ready.notified().await;
        drop(socket);
        marker(dir.path(), 'c').await;
        assert_eq!(
            events(dir.path()),
            "tdc",
            "idle interrupt invented an active turn"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        ping.notify_one();
        done.notified().await;
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_old_terminal_cannot_make_the_next_turn_idle_at_expiry() {
    timeout(Duration::from_secs(20), async {
        for detach_before_stale in [false, true] {
            let first = br#"{"type":"response.completed","response":{"id":"first","output":[]}}"#;
            let second = br#"{"type":"response.completed","response":{"id":"second","output":[]}}"#;
            let second_started = Arc::new(Notify::new());
            let stale = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let closed = Arc::new(Notify::new());
            let origin = listen({
                let second_started = second_started.clone();
                let stale = stale.clone();
                let release = release.clone();
                let closed = closed.clone();
                move |mut socket| {
                    let second_started = second_started.clone();
                    let stale = stale.clone();
                    let release = release.clone();
                    let closed = closed.clone();
                    async move {
                        upstream(&mut socket).await;
                        socket
                            .write_all(&websocket::encode_server(1, first).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(frame(&mut socket, true).await, (1, CREATE.to_vec()));
                        socket
                            .write_all(
                                &websocket::encode_server(
                                    1,
                                    br#"{"type":"response.created","response":{"id":"second"}}"#,
                                )
                                .unwrap(),
                            )
                            .await
                            .unwrap();
                        second_started.notify_one();
                        stale.notified().await;
                        socket
                            .write_all(&websocket::encode_server(1, first).unwrap())
                            .await
                            .unwrap();
                        release.notified().await;
                        socket
                            .write_all(&websocket::encode_server(1, second).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(frame(&mut socket, true).await.0, 8);
                        closed.notify_one();
                    }
                }
            })
            .await;
            let dir = tempfile::tempdir().unwrap();
            let artifact = fixture::artifact_with_stale_terminal(dir.path(), 150, 250);
            let proxy = proxy_with_artifact(origin.address, artifact, 8).await;
            let worker = serve_engine(proxy.clone()).await;
            let mut socket = client(worker.address).await;
            create(&mut socket).await;
            assert_eq!(frame(&mut socket, false).await, (1, first.to_vec()));
            create(&mut socket).await;
            assert_eq!(frame(&mut socket, false).await.0, 1); // response.created for second
            second_started.notified().await;
            let mut socket = Some(socket);
            if detach_before_stale {
                drop(socket.take());
                marker(dir.path(), 'd').await;
            }
            stale.notify_one();
            marker(dir.path(), 's').await;
            tokio::time::sleep(Duration::from_millis(350)).await;
            assert_eq!(
                events(dir.path()),
                if detach_before_stale { "tds" } else { "ts" },
                "dropped duplicate terminal retired the live second turn"
            );
            drop(socket.take());
            marker(dir.path(), 'd').await;
            release.notify_one();
            closed.notified().await;
            marker(dir.path(), 'c').await;
            assert_eq!(
                events(dir.path()),
                if detach_before_stale {
                    "tdstc"
                } else {
                    "tsdtc"
                }
            );
            drop(worker);
            proxy.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_eof_drop_keep_has_no_replay_and_keeps_downstream_until_client_close() {
    timeout(Duration::from_secs(15), async {
        let count = Arc::new(AtomicUsize::new(0));
        let ended = Arc::new(Notify::new());
        let origin = listen({
            let count = count.clone();
            let ended = ended.clone();
            move |mut socket| {
                let count = count.clone();
                let ended = ended.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    upstream(&mut socket).await;
                    // The physical upstream sends EOF without a semantic terminal.
                    socket.shutdown().await.unwrap();
                    ended.notify_one();
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path(), 1000, 2000, true, 8).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = client(worker.address).await;
        create(&mut socket).await;
        ended.notified().await;
        marker(dir.path(), 'u').await;
        assert_eq!(
            events(dir.path()),
            "u",
            "EOF fabricated a semantic completion or downstream cleanup"
        );
        assert!(
            timeout(Duration::from_millis(100), socket.read_u8())
                .await
                .is_err(),
            "Drop/Keep fabricated a replay, body, Close, or downstream EOF"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "EOF silently dialed another business connection"
        );
        socket
            .write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            frame(&mut socket, false).await,
            (8, 1000_u16.to_be_bytes().to_vec())
        );
        assert_eq!(
            socket.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        marker(dir.path(), 'c').await;
        assert_eq!(events(dir.path()), "uc");
        drop(socket);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
