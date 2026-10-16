//! Bounded loopback stress of the actual native engine, not a capacity benchmark.
use super::*;
use diagnostic_output::operations::Operations;
use std::collections::HashSet;
use tokio::sync::{Semaphore, mpsc};

const COMPLETE: usize = 8;
const ABORT: usize = 4;
const PREFIX: usize = 1024;
const UPLOAD: usize = 32 * 1024;
const DOWNLOAD: usize = 96 * 1024;
const DEADLINE: Duration = Duration::from_secs(30);

// Position-dependent bytes expose crossed requests and corruption beyond the first chunk.
fn payload(id: usize, length: usize, salt: usize) -> Vec<u8> {
    (0..length)
        .map(|position| ((id * 37 + position * 13 + position / 251 + salt) % 251) as u8)
        .collect()
}

async fn workload(rounds: usize) {
    let operations = Arc::new(Operations::default());
    let (arrived, mut uploads) = mpsc::unbounded_channel();
    let (finished, mut origins) = mpsc::unbounded_channel();
    let release_response = Arc::new(Semaphore::new(0));
    let response_gate = Arc::clone(&release_response);
    let upstream = listen(move |mut socket| {
        let arrived = arrived.clone();
        let finished = finished.clone();
        let gate = Arc::clone(&response_gate);
        async move {
            let request = head(&mut socket).await;
            let request = std::str::from_utf8(&request).unwrap();
            let path = request.split_whitespace().nth(1).unwrap();
            let mut route = path.trim_start_matches('/').split('/');
            let mode = route.next().unwrap();
            let id: usize = route.next().unwrap().parse().unwrap();
            assert!(route.next().is_none());
            assert!(request.starts_with("POST "));
            let mut body = vec![0; PREFIX];
            socket.read_exact(&mut body).await.unwrap();
            assert_eq!(body, payload(id, PREFIX, 17), "upload prefix {id}");
            if mode != "recover" {
                arrived.send(id).unwrap();
            }
            let mut tail = vec![0; UPLOAD - PREFIX];
            if mode == "abort" {
                // A truncated downstream upload must terminate its upstream too.
                assert!(socket.read_exact(&mut tail).await.is_err(), "upload {id} unexpectedly completed");
                finished.send(id).unwrap();
                return;
            }
            socket.read_exact(&mut tail).await.unwrap();
            assert_eq!(tail, payload(id, UPLOAD, 17)[PREFIX..], "upload tail {id}");
            let response = payload(id, DOWNLOAD, 89);
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {DOWNLOAD}\r\nX-Correlation: {id}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            socket.write_all(&response[..PREFIX]).await.unwrap();
            if mode != "recover" {
                assert_eq!(mode, "hold");
                gate.acquire().await.unwrap().forget();
            }
            socket.write_all(&response[PREFIX..]).await.unwrap();
            socket.shutdown().await.unwrap();
            // Required by the test: an origin callback panic cannot pass silently.
            finished.send(id).unwrap();
        }
    }).await;
    let mut config = settings(upstream.address, None, "", false);
    config.io_timeout = DEADLINE;
    let proxy = NativeProxy::new(config, Arc::new(WorkerControl::default()), vec![])
        .unwrap()
        .with_operations(Arc::clone(&operations));
    let worker = serve_engine(proxy).await;
    let start = std::time::Instant::now();
    for round in 0..rounds {
        timeout(DEADLINE, async {
            let base = round * (COMPLETE + ABORT + 1);
            let upload_gate = Arc::new(Semaphore::new(0));
            let read_gate = Arc::new(Semaphore::new(0));
            let (prefix_seen, mut prefixes) = mpsc::unbounded_channel();
            let mut clients = JoinSet::new();
            for slot in 0..COMPLETE + ABORT {
                let id = base + slot;
                let abort = slot >= COMPLETE;
                let address = worker.address;
                let gate = Arc::clone(&upload_gate);
                let read_gate = Arc::clone(&read_gate);
                let prefix_seen = prefix_seen.clone();
                clients.spawn(async move {
                    let mut client = TcpStream::connect(address).await.unwrap();
                    let mode = if abort { "abort" } else { "hold" };
                    client.write_all(format!("POST /{mode}/{id} HTTP/1.1\r\nHost: local\r\nContent-Length: {UPLOAD}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                    let body = payload(id, UPLOAD, 17);
                    client.write_all(&body[..PREFIX]).await.unwrap();
                    gate.acquire().await.unwrap().forget();
                    if abort { return; }
                    client.write_all(&body[PREFIX..]).await.unwrap();
                    verify_prefix(&mut client, id).await;
                    prefix_seen.send(id).unwrap();
                    // Deliberately pause these readers while a fresh request succeeds.
                    read_gate.acquire().await.unwrap().forget();
                    verify_tail(&mut client, id).await;
                });
            }
            let mut seen = HashSet::new();
            for _ in 0..COMPLETE + ABORT {
                assert!(seen.insert(uploads.recv().await.unwrap()));
            }
            assert_eq!(seen, (base..base + COMPLETE + ABORT).collect());
            assert_eq!(operations.snapshot().inflight, (COMPLETE + ABORT) as u64);
            upload_gate.add_permits(COMPLETE + ABORT);
            let mut seen = HashSet::new();
            for _ in 0..COMPLETE {
                assert!(seen.insert(prefixes.recv().await.unwrap()));
            }
            assert_eq!(seen, (base..base + COMPLETE).collect());
            // All normal responses are still held. Cancellation must settle separately.
            while operations.snapshot().inflight != COMPLETE as u64 {
                tokio::task::yield_now().await;
            }
            let recovery_id = base + COMPLETE + ABORT;
            let mut recovery = TcpStream::connect(worker.address).await.unwrap();
            recovery.write_all(format!("POST /recover/{recovery_id} HTTP/1.1\r\nHost: local\r\nContent-Length: {UPLOAD}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            recovery.write_all(&payload(recovery_id, UPLOAD, 17)).await.unwrap();
            verify_prefix(&mut recovery, recovery_id).await;
            verify_tail(&mut recovery, recovery_id).await;
            release_response.add_permits(COMPLETE);
            read_gate.add_permits(COMPLETE);
            while let Some(result) = clients.join_next().await { result.unwrap(); }
            let mut done = HashSet::new();
            for _ in 0..COMPLETE + ABORT + 1 {
                assert!(done.insert(origins.recv().await.unwrap()));
            }
            assert_eq!(done, (base..=recovery_id).collect());
            while operations.snapshot().inflight != 0 { tokio::task::yield_now().await; }
            let snapshot = operations.snapshot();
            let rounds = (round + 1) as u64;
            assert_eq!(snapshot.started, rounds * (COMPLETE + ABORT + 1) as u64);
            assert_eq!(snapshot.delivered, rounds * (COMPLETE + 1) as u64);
            assert_eq!(snapshot.failed + snapshot.cancelled, rounds * ABORT as u64);
            assert_eq!(snapshot.completion_failed, 0);
            assert_eq!(snapshot.request_bytes, rounds * ((COMPLETE + 1) * UPLOAD + ABORT * PREFIX) as u64);
            assert_eq!(snapshot.response_bytes, rounds * (COMPLETE + 1) as u64 * DOWNLOAD as u64);
        }).await.unwrap_or_else(|_| panic!("stress round {round} did not settle: {}", operations.snapshot().to_json()));
    }
    eprintln!(
        "native stress: rounds={rounds}, requests={}, delivered={}, upload_disconnects={}, response_bytes={}, elapsed={:?}",
        operations.snapshot().started,
        operations.snapshot().delivered,
        rounds * ABORT,
        operations.snapshot().response_bytes,
        start.elapsed()
    );
}

async fn verify_prefix(client: &mut TcpStream, id: usize) {
    let response = head(client).await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "response {id}: {response:?}"
    );
    assert!(
        std::str::from_utf8(&response)
            .unwrap()
            .to_ascii_lowercase()
            .contains(&format!("\r\nx-correlation: {id}\r\n"))
    );
    let mut prefix = vec![0; PREFIX];
    client.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix, payload(id, PREFIX, 89), "response prefix {id}");
}

async fn verify_tail(client: &mut TcpStream, id: usize) {
    let mut tail = Vec::new();
    client.read_to_end(&mut tail).await.unwrap();
    assert_eq!(
        tail,
        payload(id, DOWNLOAD, 89)[PREFIX..],
        "response tail {id}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_streams_disconnects_and_recovery_release_requests() {
    workload(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "optional repeated loopback workload; run with just stress"]
async fn repeated_concurrent_streams_and_disconnects() {
    workload(64).await;
}
