//! Real native duplex loop, including idle CONNECT transport and split wire frames.
use super::*;
use pingora_worker::keepalive::WebSocketKeepalive;
const REQUEST: &[u8] = b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
const RESPONSE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
fn config(origin: SocketAddr, upstream: bool, downstream: bool) -> ProxyConfig {
    let mut config = settings(origin, None, "", false);
    config.websocket_keepalive = WebSocketKeepalive {
        enabled: true,
        upstream,
        downstream,
        interval_ms: 40,
        pong_timeout_ms: 250,
    };
    config
}
fn frame(opcode: u8, data: &[u8], masked: bool) -> Vec<u8> {
    assert!(data.len() < 126);
    let mut wire = vec![
        0x80 | opcode,
        data.len() as u8 | if masked { 0x80 } else { 0 },
    ];
    if masked {
        wire.extend_from_slice(&[1, 2, 3, 4]);
        wire.extend(
            data.iter()
                .enumerate()
                .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
        );
    } else {
        wire.extend_from_slice(data);
    }
    wire
}
async fn read_frame(socket: &mut TcpStream, masked: bool) -> std::io::Result<(u8, Vec<u8>)> {
    let first = socket.read_u8().await?;
    let len = socket.read_u8().await?;
    assert_eq!(first & 0xf0, 0x80);
    assert_eq!(len & 0x80 != 0, masked);
    assert!(len & 0x7f < 126);
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await?;
    }
    let mut body = vec![0; usize::from(len & 0x7f)];
    socket.read_exact(&mut body).await?;
    if masked {
        for (i, b) in body.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Ok((first & 0x0f, body))
}
async fn connect(worker: &Listening) -> TcpStream {
    let mut client = TcpStream::connect(worker.address).await.unwrap();
    client.set_nodelay(true).unwrap();
    client.write_all(REQUEST).await.unwrap();
    assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probes_keep_both_websocket_legs_alive_through_idle_connect_proxy() {
    timeout(WAIT, async {
        let probes = Arc::new(AtomicUsize::new(0));
        let observed = probes.clone();
        let origin = listen(move |mut socket| {
            let observed = observed.clone();
            async move {
                head(&mut socket).await;
                socket.write_all(RESPONSE).await.unwrap();
                while let Ok((opcode,body)) = read_frame(&mut socket,true).await {
                    match opcode {
                        9 => { observed.fetch_add(1,Ordering::SeqCst); if socket.write_all(&frame(10,&body,false)).await.is_err() { break; } },
                        1 => { socket.write_all(&frame(1,&body,false)).await.unwrap(); },
                        10 => {},
                        _ => break,
                    }
                }
            }
        }).await;
        let origin_address = origin.address;
        let tunnel = listen(move |mut client| async move {
            assert!(head(&mut client).await.starts_with(b"CONNECT "));
            let mut server = TcpStream::connect(origin_address).await.unwrap();
            client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await.unwrap();
            let mut a = [0;4096]; let mut b = [0;4096];
            loop {
                // A proxy closes either otherwise healthy socket after no wire traffic.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(400)) => break,
                    n = client.read(&mut a) => { let Ok(n) = n else { break }; if n==0 || server.write_all(&a[..n]).await.is_err() { break; } },
                    n = server.read(&mut b) => { let Ok(n) = n else { break }; if n==0 || client.write_all(&b[..n]).await.is_err() { break; } },
                }
            }
        }).await;
        let mut cfg = config(origin.address,true,true);
        cfg.proxy = Some(format!("http://{}",tunnel.address).parse().unwrap());
        let worker = worker(cfg).await;
        let mut client = connect(&worker).await;
        let started = tokio::time::Instant::now();
        let mut pings = 0;
        while started.elapsed() < Duration::from_millis(850) {
            let (opcode,body) = read_frame(&mut client,false).await.unwrap();
            if opcode==9 {
                pings+=1;
                client.write_all(&frame(10,&body,true)).await.unwrap();
            } else { assert_eq!(opcode,10); }
        }
        assert!(pings>=3); assert!(probes.load(Ordering::SeqCst)>=3);
        client.write_all(&frame(1,b"still alive",true)).await.unwrap();
        loop {
            let (opcode,body) = read_frame(&mut client,false).await.unwrap();
            if opcode==1 { assert_eq!(body,b"still alive"); break; }
            if opcode==9 { client.write_all(&frame(10,&body,true)).await.unwrap(); }
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_or_wrong_upstream_pong_closes_the_duplex_connection() {
    for wrong_pong in [false, true] {
        timeout(WAIT, async {
            let probes = Arc::new(AtomicUsize::new(0));
            let observed = probes.clone();
            let origin = listen(move |mut socket| {
                let observed = observed.clone();
                async move {
                    head(&mut socket).await;
                    socket.write_all(RESPONSE).await.unwrap();
                    while let Ok((opcode, mut body)) = read_frame(&mut socket, true).await {
                        if opcode == 9 {
                            observed.fetch_add(1, Ordering::SeqCst);
                            if wrong_pong {
                                body[0] ^= 1;
                                let _ = socket.write_all(&frame(10, &body, false)).await;
                            }
                        }
                    }
                }
            })
            .await;
            let worker = worker(config(origin.address, true, false)).await;
            let mut client = connect(&worker).await;
            // Wrong pongs may be forwarded transparently; they must not acknowledge our probe.
            timeout(Duration::from_secs(2), async {
                while read_frame(&mut client, false).await.is_ok() {}
            })
            .await
            .unwrap();
            assert_eq!(probes.load(Ordering::SeqCst), 1);
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downstream_ping_waits_for_the_end_of_a_partially_forwarded_frame() {
    timeout(WAIT, async {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let release = gate.clone();
        let origin = listen(move |mut socket| {
            let gate = gate.clone();
            async move {
                head(&mut socket).await;
                socket.write_all(RESPONSE).await.unwrap();
                socket.write_all(&[0x81, 3, b'a']).await.unwrap();
                gate.acquire().await.unwrap().forget();
                socket.write_all(b"bc").await.unwrap();
                let mut discard = [0; 256];
                while socket.read(&mut discard).await.is_ok_and(|n| n > 0) {}
            }
        })
        .await;
        let worker = worker(config(origin.address, false, true)).await;
        let mut client = connect(&worker).await;
        let mut prefix = [0; 3];
        client.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, [0x81, 3, b'a']);
        assert!(
            timeout(Duration::from_millis(130), client.read_u8())
                .await
                .is_err(),
            "ping inserted inside data frame"
        );
        release.add_permits(1);
        let mut tail = [0; 2];
        client.read_exact(&mut tail).await.unwrap();
        assert_eq!(&tail, b"bc");
        let (opcode, token) = read_frame(&mut client, false).await.unwrap();
        assert_eq!(opcode, 9);
        client.write_all(&frame(10, &token, true)).await.unwrap();
        // Matching pongs permit another cycle; leaving it unanswered must close the socket.
        assert_eq!(read_frame(&mut client, false).await.unwrap().0, 9);
        assert!(
            timeout(Duration::from_secs(2), read_frame(&mut client, false))
                .await
                .unwrap()
                .is_err()
        );
    })
    .await
    .unwrap();
}
