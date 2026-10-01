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
async fn origin(acknowledge: bool, pings: tokio::sync::mpsc::UnboundedSender<()>) -> Listening {
    listen(move |mut socket| {
        let pings = pings.clone();
        async move {
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
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
) -> Listening {
    listen(move |mut client| {
        let connections = Arc::clone(&connections);
        let closed = closed.clone();
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
            loop {
                let forward = async {
                    tokio::select! {
                        read = client.read(&mut from_client) => {
                            let length = read.ok()?;
                            if length == 0 { return None; }
                            upstream.write_all(&from_client[..length]).await.ok()?;
                        }
                        read = upstream.read(&mut from_origin) => {
                            let length = read.ok()?;
                            if length == 0 { return None; }
                            client.write_all(&from_origin[..length]).await.ok()?;
                        }
                    }
                    Some(())
                };
                if !matches!(timeout(BRIDGE_IDLE, forward).await, Ok(Some(()))) {
                    let _ = closed.send(());
                    break;
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
        let origin = origin(true, ping_tx).await;
        let connections = Arc::new(AtomicUsize::new(0));
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();
        let bridge = bridge(origin.address, Arc::clone(&connections), close_tx).await;
        let mut config = settings(origin.address, Some(bridge.address), "http", true);
        config.http2.enabled = true;
        config.http2.ping_interval_ms = enabled.then_some(PING.as_millis() as u64);
        let worker = worker(config).await;
        assert!(get(worker.address).await.starts_with(b"HTTP/1.1 200"));
        if enabled {
            // Observed ACKed probes span more than the bridge's idle budget.
            timeout(WAIT, async {
                for _ in 0..15 {
                    ping_rx.recv().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert!(close_rx.try_recv().is_err());
        } else {
            timeout(WAIT, close_rx.recv()).await.unwrap().unwrap();
            assert!(ping_rx.try_recv().is_err());
        }
        assert!(get(worker.address).await.starts_with(b"HTTP/1.1 200"));
        assert_eq!(
            connections.load(Ordering::SeqCst),
            if enabled { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn native_http2_missing_ping_ack_closes_and_replaces_connection() {
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::unbounded_channel();
    let origin = origin(false, ping_tx).await;
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
