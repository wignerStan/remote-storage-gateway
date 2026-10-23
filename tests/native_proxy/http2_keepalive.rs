//! Real HTTP/2 wire exchanges through an idle-expiring HTTP CONNECT bridge.
use super::*;

const PING: Duration = Duration::from_millis(40);
const BRIDGE_IDLE: Duration = Duration::from_millis(400);

async fn read_frame(socket: &mut TcpStream) -> std::io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await?;
    let length = ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
    assert!(length <= 65536);
    let mut payload = vec![0; length];
    socket.read_exact(&mut payload).await?;
    Ok((
        header[3],
        header[4],
        u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
        payload,
    ))
}
async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = payload.len() as u32;
    let mut frame = vec![
        (length >> 16) as u8,
        (length >> 8) as u8,
        length as u8,
        kind,
        flags,
    ];
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await.unwrap();
}

// A minimal HTTP/2 origin uses the static HPACK :status 200 entry. Inspecting
// native frames directly proves PING is connection-level and carries no DATA.
async fn origin(
    acknowledge: bool,
    pings: tokio::sync::mpsc::UnboundedSender<()>,
    startup: Duration,
) -> Listening {
    listen(move |mut socket| {
        let pings = pings.clone();
        async move {
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            tokio::time::sleep(startup).await;
            write_frame(&mut socket, 4, 0, 0, &[]).await;
            while let Ok((kind, flags, stream, payload)) = read_frame(&mut socket).await {
                match kind {
                    4 if flags & 1 == 0 => write_frame(&mut socket, 4, 1, 0, &[]).await,
                    1 => {
                        assert_ne!(stream, 0);
                        write_frame(&mut socket, 1, 5, stream, &[0x88]).await;
                    }
                    6 if flags & 1 == 0 => {
                        assert_eq!(stream, 0);
                        assert_eq!(payload.len(), 8);
                        pings.send(()).unwrap();
                        if acknowledge {
                            write_frame(&mut socket, 6, 1, 0, &payload).await;
                        }
                    }
                    0 => panic!("keepalive must never become HTTP body data"),
                    _ => {}
                }
            }
        }
    })
    .await
}

async fn bridge(
    origin: SocketAddr,
    connections: Arc<AtomicUsize>,
    closed: tokio::sync::mpsc::UnboundedSender<()>,
    idle: tokio::sync::watch::Receiver<bool>,
) -> Listening {
    listen(move |mut client| {
        let connections = Arc::clone(&connections);
        let closed = closed.clone();
        let mut idle = idle.clone();
        async move {
            assert!(head(&mut client).await.starts_with(b"CONNECT "));
            let mut upstream = TcpStream::connect(origin).await.unwrap();
            connections.fetch_add(1, Ordering::SeqCst);
            client
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut from_client = [0; 8192];
            let mut from_origin = [0; 8192];
            let mut deadline = None;
            loop {
                if *idle.borrow() {
                    deadline.get_or_insert_with(|| tokio::time::Instant::now() + BRIDGE_IDLE);
                } else {
                    deadline = None;
                }
                let read = tokio::select! {
                    changed = idle.changed() => {
                        if changed.is_err() { return; }
                        deadline = None;
                        continue;
                    }
                    _ = async {
                        match deadline {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    } => None,
                    read = client.read(&mut from_client) => read.ok().filter(|n| *n > 0).map(|n| (true, n)),
                    read = upstream.read(&mut from_origin) => read.ok().filter(|n| *n > 0).map(|n| (false, n)),
                };
                let Some((from_downstream, length)) = read else {
                    // Wait for the H2 driver's FIN acknowledgement before the
                    // next request can select a pooled connection.
                    let _ = client.shutdown().await;
                    timeout(WAIT, async {
                        loop {
                            match client.read(&mut from_client).await {
                                Ok(0) => break,
                                Ok(_) => {},
                                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                                Err(error) => panic!("H2 close acknowledgement failed: {error}"),
                            }
                        }
                    }).await.expect("native H2 driver did not close after bridge FIN");
                    let _ = closed.send(());
                    break;
                };
                if from_downstream {
                    upstream.write_all(&from_client[..length]).await.unwrap();
                } else {
                    client.write_all(&from_origin[..length]).await.unwrap();
                }
                if *idle.borrow() {
                    deadline = Some(tokio::time::Instant::now() + BRIDGE_IDLE);
                }
            }
        }
    })
    .await
}

#[tokio::test]
async fn native_http2_ping_preserves_idle_connect_tunnel_and_pool_reuse() {
    for enabled in [false, true] {
        let (ping_tx, mut ping_rx) = tokio::sync::mpsc::unbounded_channel();
        // Bootstrap delay is longer than the idle budget on purpose.
        // The measured idle phase starts only after the first response.
        let origin = origin(true, ping_tx, BRIDGE_IDLE * 2).await;
        let connections = Arc::new(AtomicUsize::new(0));
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();
        let (idle_tx, idle_rx) = tokio::sync::watch::channel(false);
        let bridge = bridge(origin.address, Arc::clone(&connections), close_tx, idle_rx).await;
        let mut config = settings(origin.address, Some(bridge.address), "http", true);
        config.http2.enabled = true;
        config.http2.ping_interval_ms = enabled.then_some(PING.as_millis() as u64);
        let worker = worker(config).await;
        let first = get(worker.address).await;
        assert!(
            first.starts_with(b"HTTP/1.1 200"),
            "initial request failed: enabled={enabled}, bridge_closed={}, connections={}, response={}",
            close_rx.try_recv().is_ok(),
            connections.load(Ordering::SeqCst),
            String::from_utf8_lossy(&first)
        );
        if enabled {
            // Bootstrap probes do not count toward the measured idle phase.
            while ping_rx.try_recv().is_ok() {}
        }
        let idle_started = tokio::time::Instant::now();
        idle_tx.send(true).unwrap();
        if enabled {
            // Observed ACKed probes span more than the bridge's idle budget.
            timeout(WAIT, async {
                for _ in 0..15 {
                    ping_rx.recv().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert!(idle_started.elapsed() > BRIDGE_IDLE);
            assert!(close_rx.try_recv().is_err());
        } else {
            timeout(WAIT, close_rx.recv()).await.unwrap().unwrap();
            assert!(ping_rx.try_recv().is_err());
        }
        idle_tx.send(false).unwrap();
        let next = get(worker.address).await;
        assert!(
            next.starts_with(b"HTTP/1.1 200"),
            "idle request failed: enabled={enabled}, connections={}, response={}",
            connections.load(Ordering::SeqCst),
            String::from_utf8_lossy(&next)
        );
        assert_eq!(
            connections.load(Ordering::SeqCst),
            if enabled { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn native_http2_missing_ping_ack_closes_and_replaces_connection() {
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::unbounded_channel();
    let origin = origin(false, ping_tx, Duration::ZERO).await;
    let connections = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&connections);
    let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();
    // No bridge idle cutoff here: native Pingora's fixed five-second ACK timeout
    // must be the component that closes the connection.
    let bridge = listen(move |mut client| {
        let count = Arc::clone(&count);
        let close_tx = close_tx.clone();
        async move {
            assert!(head(&mut client).await.starts_with(b"CONNECT "));
            let mut upstream = TcpStream::connect(origin.address).await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            client
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            let _ = close_tx.send(());
        }
    })
    .await;
    let mut config = settings(origin.address, Some(bridge.address), "http", true);
    config.pool_idle_timeout = Duration::from_secs(20);
    config.http2.enabled = true;
    config.http2.ping_interval_ms = Some(PING.as_millis() as u64);
    let worker = worker(config).await;
    assert!(get(worker.address).await.starts_with(b"HTTP/1.1 200"));
    timeout(WAIT, ping_rx.recv()).await.unwrap().unwrap();
    timeout(Duration::from_secs(8), close_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(get(worker.address).await.starts_with(b"HTTP/1.1 200"));
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}
