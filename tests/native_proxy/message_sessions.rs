//! Separate native WebSocket transports exercised through real shared-library callbacks.
use super::*;
use proxy_pingora::{connect_upstream::websocket_session::handshake, http_ingress::ProxyLifecycle};
use proxy_protocols::websocket;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

const REQUEST: &[u8] = b"GET /v1/responses HTTP/1.1\r\nHost: downstream\r\nCookie: unapproved\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
const CREATE: &[u8] = br#"{"type":"response.create","model":"unapproved","input":[]}"#;
const DELTA: &[u8] = br#"{"type":"response.output_text.delta","delta":"streamed"}"#;
const COMPLETED: &[u8] = br#"{"type":"response.completed","response":{"id":"r1","usage":{"input_tokens_details":{"cached_tokens":0}}}}"#;
fn artifact(dir: &Path, mode: u8) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join(format!("message_actions_{mode}.so"));
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(format!("-DTEST_MODE={mode}"))
            .arg(format!(
                "-DTEST_MARKER_PATH={}",
                serde_json::to_string(&dir.join(format!("closed_{mode}")).to_str().unwrap())
                    .unwrap()
            ))
            .arg(root.join("tests/fixtures/message_actions.c"))
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}
fn config(upstream: SocketAddr, path: PathBuf) -> pingora_worker::config::Config {
    serde_json::from_value(json!({
        "upstream":format!("http://{upstream}"),
        "callbacks":[{"path":path,"package":"worker.sessions.test", "message_codec":"openai.responses.v1",
            "message_actions":true,"patch_headers":["cookie","x-request-id"]}]
    })).unwrap()
}
async fn frame(socket: &mut TcpStream, masked: bool) -> (u8, Vec<u8>) {
    let first = socket.read_u8().await.unwrap();
    let second = socket.read_u8().await.unwrap();
    assert_eq!(second & 0x80 != 0, masked);
    let len = match second & 127 {
        126 => usize::from(socket.read_u16().await.unwrap()),
        127 => usize::try_from(socket.read_u64().await.unwrap()).unwrap(),
        len => usize::from(len),
    };
    assert!(len < 1024 * 1024);
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await.unwrap();
    }
    let mut payload = vec![0; len];
    socket.read_exact(&mut payload).await.unwrap();
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[index % 4];
    }
    (first & 15, payload)
}
async fn upgrade(socket: &mut TcpStream, request: &[u8]) {
    let request = String::from_utf8(request.to_vec()).unwrap();
    let key = request
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, value)| value.trim())
        })
        .unwrap();
    let accept = handshake::accept_key(key).unwrap();
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSet-Cookie: observed=upstream\r\n\r\n").as_bytes()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_message_selects_and_rewrites_before_dial_then_replay_replaces_socket_headers() {
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let retired = Arc::new(tokio::sync::Notify::new());
        let closed = retired.clone();
        let origin = listen(move |mut socket| {
            let generation = count.fetch_add(1, Ordering::SeqCst) + 1;
            let closed = closed.clone();
            async move {
                assert!(generation <= 2);
                let request = head(&mut socket).await;
                let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
                assert!(text.starts_with(&format!("get /selected?generation={generation} http/1.1\r\n")), "{text}");
                let suffix = if generation == 1 { "one" } else { "two" };
                assert!(text.contains(&format!("cookie: session={suffix}\r\n")), "{text}");
                assert!(text.contains(&format!("x-request-id: request-{suffix}\r\n")), "{text}");
                assert!(!text.contains("unapproved"));
                upgrade(&mut socket, &request).await;
                let (opcode, payload) = frame(&mut socket, true).await;
                assert_eq!(opcode, 1);
                assert_eq!(serde_json::from_slice::<serde_json::Value>(&payload).unwrap(), json!({"type":"response.create","model":format!("approved-{suffix}"),"input":[]}));
                socket.write_all(&websocket::encode_server(1, DELTA).unwrap()).await.unwrap();
                socket.write_all(&websocket::encode_server(1, COMPLETED).unwrap()).await.unwrap();
                let mut byte = [0];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
                if generation == 1 { closed.notify_one(); }
            }
        }).await;
        let directory = tempfile::tempdir().unwrap();
        let proxy = pingora_worker::assembly::assemble(config(origin.address, artifact(directory.path(), 0))).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(connections.load(Ordering::SeqCst), 0, "local upgrade must not dial");
        let mut first = websocket::encode_client(1, &CREATE[..12]).unwrap().to_vec(); first[0] &= 0x7f;
        client.write_all(&first).await.unwrap();
        client.write_all(&websocket::encode_client(9, b"probe").unwrap()).await.unwrap();
        assert_eq!(frame(&mut client, false).await, (10, b"probe".to_vec()));
        assert_eq!(connections.load(Ordering::SeqCst), 0, "fragment must not authorize a dial");
        let mut last = websocket::encode_client(1, &CREATE[12..]).unwrap().to_vec(); last[0] = 0x80;
        client.write_all(&last).await.unwrap();
        assert_eq!(frame(&mut client, false).await, (1, DELTA.to_vec()));
        let (opcode, error) = frame(&mut client, false).await;
        assert_eq!(opcode, 1);
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&error).unwrap()["error"]["code"], "previous_response_not_found");
        retired.notified().await;
        client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
        assert_eq!(frame(&mut client, false).await, (1, DELTA.to_vec()));
        assert_eq!(frame(&mut client, false).await, (1, COMPLETED.to_vec()));
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        client.write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap()).await.unwrap();
        assert_eq!(frame(&mut client, false).await, (8, 1000_u16.to_be_bytes().to_vec()));
        drop(client); drop(worker); proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_metadata_is_rejected_before_dial_and_closes_websocket_without_http_error() {
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let origin = listen(move |_socket| {
            count.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let proxy = pingora_worker::assembly::assemble(config(
            origin.address,
            artifact(directory.path(), 1),
        ))
        .await
        .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        let (opcode, payload) = frame(&mut client, false).await;
        assert_eq!(opcode, 8, "post-upgrade failures must use WebSocket close");
        assert!(payload.len() >= 2);
        let mut tail = Vec::new();
        let _ = client.read_to_end(&mut tail).await;
        assert!(
            tail.is_empty(),
            "HTTP errors must never enter the upgraded stream"
        );
        assert_eq!(connections.load(Ordering::SeqCst), 0);
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_heartbeat_progresses_while_native_message_policy_waits() {
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let origin = listen(move |mut socket| {
            count.fetch_add(1, Ordering::SeqCst);
            async move {
                let request = head(&mut socket).await;
                upgrade(&mut socket, &request).await;
                frame(&mut socket, true).await;
                socket
                    .write_all(&websocket::encode_server(1, DELTA).unwrap())
                    .await
                    .unwrap();
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
            }
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let proxy = pingora_worker::assembly::assemble(config(
            origin.address,
            artifact(directory.path(), 2),
        ))
        .await
        .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        client
            .write_all(&websocket::encode_client(9, b"during policy").unwrap())
            .await
            .unwrap();
        let pong = timeout(Duration::from_millis(100), frame(&mut client, false))
            .await
            .expect("native policy must not block pong delivery");
        assert_eq!(pong, (10, b"during policy".to_vec()));
        assert_eq!(
            connections.load(Ordering::SeqCst),
            0,
            "pending policy must not dial"
        );
        assert_eq!(frame(&mut client, false).await, (1, DELTA.to_vec()));
        client
            .write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap())
            .await
            .unwrap();
        assert_eq!(frame(&mut client, false).await.0, 8);
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

async fn cleanup_case(mode: u8, exhaust_body_budget: bool) {
    timeout(WAIT, async {
        let origin = listen(|_socket| async { panic!("idle session must not dial"); }).await;
        let directory = tempfile::tempdir().unwrap();
        let first = artifact(directory.path(), mode);
        let observer = artifact(directory.path(), 6);
        let max_message_bytes = 1024;
        let one_reservation = 8 * max_message_bytes + 8 * 1024 * 1024 + 512 * 1024;
        let config = serde_json::from_value(json!({
            "upstream":format!("http://{}", origin.address),
            "inspection":{"max_message_bytes":max_message_bytes, "max_buffered_bytes":one_reservation},
            "callbacks":[
                {"path":first,"package":"worker.sessions.test","message_codec":"openai.responses.v1","message_actions":true},
                {"path":observer,"package":"worker.sessions.test","message_codec":"openai.responses.v1","message_actions":true}
            ]
        })).unwrap();
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut idle = TcpStream::connect(worker.address).await.unwrap();
        idle.write_all(REQUEST).await.unwrap();
        assert!(head(&mut idle).await.starts_with(b"HTTP/1.1 101"));
        let mut holding = None;
        if exhaust_body_budget {
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(REQUEST).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            let mut incomplete = websocket::encode_client(1, b"{\"type\":").unwrap().to_vec();
            incomplete[0] &= 0x7f;
            client.write_all(&incomplete).await.unwrap();
            client.write_all(&websocket::encode_client(9, b"reserved").unwrap()).await.unwrap();
            assert_eq!(frame(&mut client, false).await, (10, b"reserved".to_vec()));
            // The pong proves the unfinished frame was decoded and retains the
            // sole body reservation before the idle session attempts teardown.
            holding = Some(client);
        }
        idle.write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap()).await.unwrap();
        assert_eq!(frame(&mut idle, false).await.0, 8);
        let mut tail = Vec::new(); idle.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty(), "ignored teardown replies must not append frames");
        for callback in [mode, 6] {
            assert_eq!(std::fs::read_to_string(directory.path().join(format!("closed_{callback}"))).unwrap(), "closed\n", "all state owners must be notified once, regardless of earlier teardown output or body capacity");
        }
        drop(idle); drop(holding); drop(worker); proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn teardown_notifies_all_owners_despite_prior_deny_failure_or_action() {
    for mode in [3, 4, 5] {
        cleanup_case(mode, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_teardown_notifies_all_owners_while_another_session_holds_body_budget() {
    cleanup_case(4, true).await;
}
