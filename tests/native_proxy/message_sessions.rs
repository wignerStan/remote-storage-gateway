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
    artifact_for_codec(dir, mode, "openai.responses.v1")
}
fn artifact_for_codec(dir: &Path, mode: u8, codec: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join(format!("message_actions_{mode}.so"));
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(format!("-DTEST_MODE={mode}"))
            .arg(format!(
                "-DTEST_CODEC_PREFIX={}",
                serde_json::to_string(codec).unwrap()
            ))
            .arg(format!(
                "-DTEST_MARKER_PATH={}",
                serde_json::to_string(&dir.join(format!("closed_{mode}")).to_str().unwrap())
                    .unwrap()
            ))
            .arg(format!(
                "-DTEST_DELTA_MARKER_PATH={}",
                serde_json::to_string(&dir.join(format!("delta_{mode}")).to_str().unwrap())
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
        "callbacks":[{"binding":"direct", "path":path,"package":"worker.sessions.test", "message_codec":"openai.responses.v1",
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
        assert!(!directory.path().join("delta_0").exists(), "replay generations must bypass delta callbacks");
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
        assert!(
            !directory.path().join("delta_2").exists(),
            "delta must bypass the action policy"
        );
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
                {"binding":"direct", "path":first,"package":"worker.sessions.test","message_codec":"openai.responses.v1","message_actions":true},
                {"binding":"direct", "path":observer,"package":"worker.sessions.test","message_codec":"openai.responses.v1","message_actions":true}
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_websocket_aliases_accept_locally_and_wait_for_an_admitted_create() {
    for path in [
        "/responses",
        "/v1/responses?trace=1",
        "/backend-api/codex/responses",
    ] {
        timeout(WAIT, async {
            let connections = Arc::new(AtomicUsize::new(0));
            let count = connections.clone();
            let origin = listen(move |mut socket| {
                count.fetch_add(1, Ordering::SeqCst);
                async move {
                    let request = head(&mut socket).await;
                    assert!(request.starts_with(b"GET /selected?generation=1 HTTP/1.1"));
                    upgrade(&mut socket, &request).await;
                    let (opcode, payload) = frame(&mut socket, true).await;
                    assert_eq!(opcode, 1);
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&payload).unwrap()["model"],
                        "approved-one"
                    );
                    socket
                        .write_all(&websocket::encode_server(1, DELTA).unwrap())
                        .await
                        .unwrap();
                    // Keep the business socket live until downstream closes.
                    let mut byte = [0];
                    let _ = socket.read(&mut byte).await;
                }
            })
            .await;
            let directory = tempfile::tempdir().unwrap();
            let mut cfg = config(
                origin.address,
                artifact_for_codec(directory.path(), 0, "codex.responses.v1"),
            );
            cfg.callbacks[0].message_codec = Some("codex.responses.v1".into());
            let proxy = pingora_worker::assembly::assemble(cfg).await.unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            let request =
                String::from_utf8(REQUEST.to_vec())
                    .unwrap()
                    .replacen("/v1/responses", path, 1);
            client.write_all(request.as_bytes()).await.unwrap();
            assert!(
                head(&mut client).await.starts_with(b"HTTP/1.1 101"),
                "{path}"
            );
            client
                .write_all(&websocket::encode_client(9, b"before-create").unwrap())
                .await
                .unwrap();
            assert_eq!(
                frame(&mut client, false).await,
                (10, b"before-create".to_vec())
            );
            assert_eq!(
                connections.load(Ordering::SeqCst),
                0,
                "{path} dialed before admission"
            );
            client
                .write_all(&websocket::encode_client(1, CREATE).unwrap())
                .await
                .unwrap();
            assert_eq!(frame(&mut client, false).await, (1, DELTA.to_vec()));
            assert!(
                !directory.path().join("delta_0").exists(),
                "{path} must bypass delta callbacks"
            );
            assert_eq!(connections.load(Ordering::SeqCst), 1);
            client
                .write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                frame(&mut client, false).await,
                (8, 1000_u16.to_be_bytes().to_vec())
            );
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }
}

fn holding_artifact(dir: &Path) -> PathBuf {
    session_budget_artifact(dir, true)
}

fn session_budget_artifact(dir: &Path, hold: bool) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
    let start = source
        .find("    if (begins(method, TEST_CODEC_PREFIX \"/response.completed/\"))")
        .unwrap();
    let end = source[start..].find("    /* A delta callback").unwrap() + start;
    let mut source = source;
    if hold {
        source.replace_range(
            start..end,
            r#"
    if (begins(method, TEST_CODEC_PREFIX "/response.completed/")) {
        FILE *marker = fopen(TEST_MARKER_PATH, "a");
        if (!marker) return INFRA_FAILED;
        fputs("held\n", marker); fclose(marker);
        return action("{\"version\":1,\"decision\":\"forward\",\"hold\":\"buffer\"}", "", out);
    }
"#,
        );
    } else {
        source = source.replace("#if TEST_MODE == 2", "#if 1");
    }
    compile_session_source(dir, &source)
}

fn compile_session_source(dir: &Path, source: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = dir.join("holding.c");
    std::fs::write(&source_path, source).unwrap();
    let path = dir.join("holding.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(format!(
                "-DTEST_MARKER_PATH={}",
                serde_json::to_string(&dir.join("held").to_str().unwrap()).unwrap()
            ))
            .arg(format!(
                "-DTEST_DELTA_MARKER_PATH={}",
                serde_json::to_string(&dir.join("delta").to_str().unwrap()).unwrap()
            ))
            .arg(source_path)
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_response_keeps_body_reservation_across_peer_ping() {
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
                    .write_all(&websocket::encode_server(1, COMPLETED).unwrap())
                    .await
                    .unwrap();
                let mut tail = Vec::new();
                let _ = socket.read_to_end(&mut tail).await;
            }
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config(origin.address, holding_artifact(directory.path()));
        cfg.inspection.max_message_bytes = 1024;
        cfg.inspection.max_buffered_bytes = 8 * 1024 + 8 * 1024 * 1024 + 512 * 1024;
        let proxy = pingora_worker::assembly::assemble(cfg).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut first = TcpStream::connect(worker.address).await.unwrap();
        first.write_all(REQUEST).await.unwrap();
        assert!(head(&mut first).await.starts_with(b"HTTP/1.1 101"));
        // Both handshakes need a temporary inspection reservation. Accept the
        // idle peer before the first session consumes the sole reservation.
        let mut second = TcpStream::connect(worker.address).await.unwrap();
        second.write_all(REQUEST).await.unwrap();
        assert!(head(&mut second).await.starts_with(b"HTTP/1.1 101"));
        first
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        while !directory.path().join("held").exists() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // This maintenance event used to drop the reservation while held wire remained.
        first
            .write_all(&websocket::encode_client(9, b"held").unwrap())
            .await
            .unwrap();
        assert_eq!(frame(&mut first, false).await, (10, b"held".to_vec()));
        second
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        assert_eq!(frame(&mut second, false).await.0, 8);
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "held wire must keep the sole memory reservation"
        );
        first
            .write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap())
            .await
            .unwrap();
        assert_eq!(frame(&mut first, false).await.0, 8);
        drop(first);
        drop(second);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_held_responses_close_at_cumulative_wire_limit() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let request = head(&mut socket).await;
            upgrade(&mut socket, &request).await;
            frame(&mut socket, true).await;
            for _ in 0..32 {
                if socket
                    .write_all(&websocket::encode_server(1, COMPLETED).unwrap())
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let mut tail = Vec::new();
            let _ = socket.read_to_end(&mut tail).await;
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config(origin.address, holding_artifact(directory.path()));
        cfg.inspection.max_message_bytes = 1024;
        let proxy = pingora_worker::assembly::assemble(cfg).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        let (opcode, payload) = frame(&mut client, false).await;
        assert_eq!(opcode, 8, "held responses must not accumulate forever");
        assert_eq!(&payload[..2], &1011_u16.to_be_bytes());
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_chain_uses_one_message_deadline_before_any_upstream_dial() {
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let origin = listen(move |_socket| {
            count.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .await;
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let mut cfg: pingora_worker::config::Config = serde_json::from_value(json!({
            "upstream":format!("http://{}", origin.address),
            "callbacks":[
                {"binding":"direct", "path":session_budget_artifact(first.path(), false),"package":"worker.sessions.test", "message_codec":"openai.responses.v1", "message_actions":true,"patch_headers":["cookie","x-request-id"]},
                {"binding":"direct", "path":session_budget_artifact(second.path(), false),"package":"worker.sessions.test", "message_codec":"openai.responses.v1", "message_actions":true,"patch_headers":["cookie","x-request-id"]}
            ]
        })).unwrap();
        cfg.inspection.message_timeout_ms = 200;
        for callback in &mut cfg.callbacks {
            callback.timeout_ms = 1000;
        }
        let proxy = pingora_worker::assembly::assemble(cfg).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        assert_eq!(
            frame(&mut client, false).await.0,
            8,
            "two 150ms callbacks must exhaust one 200ms chain deadline"
        );
        assert_eq!(
            connections.load(Ordering::SeqCst),
            0,
            "an expired callback chain cannot authorize dialing"
        );
        // Native execution can outlive the host deadline. Shutdown awaits its owner.
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

fn buffer_decision_artifact(dir: &Path, first: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
    let start = source
        .find("    if (begins(method, TEST_CODEC_PREFIX \"/response.completed/\"))")
        .unwrap();
    let end = source[start..].find("    /* A delta callback").unwrap() + start;
    source.replace_range(
        start..end,
        &format!(
            r#"
    if (begins(method, TEST_CODEC_PREFIX "/response.completed/")) {{
        if (++completed == 1) {{ {first} }}
        return action("{{\"version\":1,\"decision\":\"forward\",\"hold\":\"release\"}}", "", out);
    }}
"#
        ),
    );
    compile_session_source(dir, &source)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_response_releases_the_approved_replacement_before_the_next_message() {
    const REDACTED: &[u8] = br#"{"type":"response.completed","response":{"id":"redacted"}}"#;
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let request = head(&mut socket).await;
            upgrade(&mut socket, &request).await;
            frame(&mut socket, true).await;
            for _ in 0..2 {
                socket
                    .write_all(&websocket::encode_server(1, COMPLETED).unwrap())
                    .await
                    .unwrap();
            }
            let mut tail = Vec::new();
            let _ = socket.read_to_end(&mut tail).await;
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let metadata = json!({"version":1,"decision":"forward","hold":"buffer"}).to_string();
        let first = format!(
            "return action({}, {}, out);",
            serde_json::to_string(&metadata).unwrap(),
            serde_json::to_string(std::str::from_utf8(REDACTED).unwrap()).unwrap()
        );
        let proxy = pingora_worker::assembly::assemble(config(
            origin.address,
            buffer_decision_artifact(directory.path(), &first),
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
        assert_eq!(frame(&mut client, false).await, (1, REDACTED.to_vec()));
        assert_eq!(frame(&mut client, false).await, (1, COMPLETED.to_vec()));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_response_cannot_override_a_later_terminal_callback_decision() {
    for terminal in [
        r#"return action("{\"version\":1,\"decision\":\"drop\"}", "", out);"#,
        r#"return action("{\"version\":1,\"decision\":\"reply\"}", "{\"type\":\"error\",\"error\":{\"message\":\"blocked\"}}", out);"#,
        r#"const char *reply = "{\"type\":\"error\",\"error\":{\"message\":\"blocked\"}}"; return copy(reply, strlen(reply), out);"#,
    ] {
        timeout(WAIT, async {
            let origin = listen(|mut socket| async move {
                let request = head(&mut socket).await;
                upgrade(&mut socket, &request).await;
                frame(&mut socket, true).await;
                socket.write_all(&websocket::encode_server(1, COMPLETED).unwrap()).await.unwrap();
                let mut tail = Vec::new();
                let _ = socket.read_to_end(&mut tail).await;
            }).await;
            let first = tempfile::tempdir().unwrap();
            let second = tempfile::tempdir().unwrap();
            let mut cfg = config(origin.address, buffer_decision_artifact(first.path(),
                r#"return action("{\"version\":1,\"decision\":\"forward\",\"hold\":\"buffer\"}", "", out);"#));
            let mut next = config(origin.address, buffer_decision_artifact(second.path(), terminal));
            cfg.callbacks.append(&mut next.callbacks);
            let proxy = pingora_worker::assembly::assemble(cfg).await.unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(REQUEST).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
            assert_eq!(frame(&mut client, false).await, (8, 1011_u16.to_be_bytes().to_vec()),
                "a conflicting callback chain must fail before holding or sending response bytes");
            drop(client); drop(worker); proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

fn request_buffer_artifact(dir: &Path) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
    let start = source
        .find("    if (begins(method, TEST_CODEC_PREFIX \"/response.create/\"))")
        .unwrap();
    let end = source[start..]
        .find("    if (begins(method, TEST_CODEC_PREFIX \"/response.completed/\"))")
        .unwrap()
        + start;
    source.replace_range(
        start..end,
        r#"
    if (begins(method, TEST_CODEC_PREFIX "/response.create/")) {
        if (++requests == 1) return action(
            "{\"version\":1,\"decision\":\"forward\",\"hold\":\"buffer\"}",
            "{\"type\":\"response.create\",\"model\":\"buffered\",\"input\":[]}", out);
        return action("{\"version\":1,\"decision\":\"forward\",\"hold\":\"release\"}", "", out);
    }
"#,
    );
    compile_session_source(dir, &source)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_request_releases_to_upstream_before_the_current_request() {
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let (send, mut received) = tokio::sync::mpsc::unbounded_channel();
        let origin = listen(move |mut socket| {
            count.fetch_add(1, Ordering::SeqCst);
            let send = send.clone();
            async move {
                let request = head(&mut socket).await;
                upgrade(&mut socket, &request).await;
                for _ in 0..2 {
                    send.send(frame(&mut socket, true).await).unwrap();
                }
                let mut tail = Vec::new();
                let _ = socket.read_to_end(&mut tail).await;
            }
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let proxy = pingora_worker::assembly::assemble(config(
            origin.address,
            request_buffer_artifact(directory.path()),
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
            .write_all(&websocket::encode_client(9, b"held").unwrap())
            .await
            .unwrap();
        assert_eq!(frame(&mut client, false).await, (10, b"held".to_vec()));
        assert_eq!(
            connections.load(Ordering::SeqCst),
            0,
            "buffering must not dial"
        );
        client
            .write_all(&websocket::encode_client(1, CREATE).unwrap())
            .await
            .unwrap();
        assert_eq!(
            received.recv().await.unwrap(),
            (
                1,
                br#"{"type":"response.create","model":"buffered","input":[]}"#.to_vec()
            )
        );
        assert_eq!(received.recv().await.unwrap(), (1, CREATE.to_vec()));
        assert_eq!(connections.load(Ordering::SeqCst), 1);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_closed_action_replies_then_closes_downstream() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let request = head(&mut socket).await;
            upgrade(&mut socket, &request).await;
            frame(&mut socket, true).await;
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut source =
            std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
        let start = source
            .find("    if (begins(method, TEST_CODEC_PREFIX \"/response.create/\"))")
            .unwrap();
        source.insert_str(
            start,
            r#"
    if (begins(method, TEST_CODEC_PREFIX "/transport.closed/")) {
        return action("{\"version\":1,\"decision\":\"reply\",\"upstream\":\"close\"}",
            "{\"type\":\"error\",\"error\":{\"message\":\"upstream closed\"}}", out);
    }
"#,
        );
        let proxy = pingora_worker::assembly::assemble(config(
            origin.address,
            compile_session_source(directory.path(), &source),
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
        assert_eq!(opcode, 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&payload).unwrap(),
            json!({"type":"error","error":{"message":"upstream closed"}})
        );
        assert_eq!(
            frame(&mut client, false).await,
            (8, 1000_u16.to_be_bytes().to_vec())
        );
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_request_release_cannot_dial_after_explicit_upstream_retirement() {
    for retirement in ["disconnect", "close"] {
        timeout(WAIT, async {
            let connections = Arc::new(AtomicUsize::new(0));
            let count = connections.clone();
            let origin = listen(move |_socket| {
                count.fetch_add(1, Ordering::SeqCst);
                async {}
            })
            .await;
            let directory = tempfile::tempdir().unwrap();
            request_buffer_artifact(directory.path());
            let source = std::fs::read_to_string(directory.path().join("holding.c")).unwrap();
            let release =
                json!({"version":1,"decision":"drop","hold":"release","upstream":retirement})
                    .to_string();
            let source = source.replace(
                r#""{\"version\":1,\"decision\":\"forward\",\"hold\":\"release\"}""#,
                &serde_json::to_string(&release).unwrap(),
            );
            let proxy = pingora_worker::assembly::assemble(config(
                origin.address,
                compile_session_source(directory.path(), &source),
            ))
            .await
            .unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(REQUEST).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            for _ in 0..2 {
                client
                    .write_all(&websocket::encode_client(1, CREATE).unwrap())
                    .await
                    .unwrap();
            }
            assert_eq!(
                frame(&mut client, false).await,
                (8, 1011_u16.to_be_bytes().to_vec())
            );
            assert_eq!(connections.load(Ordering::SeqCst), 0);
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }
}

struct FailingDownstream {
    socket: TcpStream,
    fail: Arc<std::sync::atomic::AtomicBool>,
    attempted: Arc<tokio::sync::Notify>,
}
impl tokio::io::AsyncRead for FailingDownstream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().socket).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for FailingDownstream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.fail.load(Ordering::SeqCst) {
            self.attempted.notify_one();
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        std::pin::Pin::new(&mut self.get_mut().socket).poll_write(cx, bytes)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_release_stops_on_current_generation_downstream_write_failure() {
    for hold in ["buffer", "release"] {
        timeout(WAIT, async {
            let origin = listen(|mut socket| async move {
                let request = head(&mut socket).await;
                upgrade(&mut socket, &request).await;
                frame(&mut socket, true).await;
                for _ in 0..2 {
                    socket
                        .write_all(&websocket::encode_server(1, COMPLETED).unwrap())
                        .await
                        .unwrap();
                }
                let mut tail = Vec::new();
                let _ = socket.read_to_end(&mut tail).await;
            })
            .await;
            let directory = tempfile::tempdir().unwrap();
            let metadata = json!({"version":1,"decision":"forward","hold":hold}).to_string();
            let action = format!(
                "return action({}, \"\", out);",
                serde_json::to_string(&metadata).unwrap()
            );
            let proxy = pingora_worker::assembly::assemble(config(
                origin.address,
                buffer_decision_artifact(directory.path(), &action),
            ))
            .await
            .unwrap();
            let mut engine = HttpProxy::new(proxy.clone(), Arc::new(ServerConf::default()));
            engine.handle_init_modules();
            let engine = Arc::new(engine);
            let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let attempted = Arc::new(tokio::sync::Notify::new());
            let finished = Arc::new(tokio::sync::Notify::new());
            let worker = listen({
                let fail = fail.clone();
                let attempted = attempted.clone();
                let finished = finished.clone();
                move |socket| {
                    let native =
                        proxy_pingora::connect_upstream::ProtocolL4Connect::box_stream_to_pingora(
                            Box::new(FailingDownstream {
                                socket,
                                fail: fail.clone(),
                                attempted: attempted.clone(),
                            }),
                        );
                    let engine = engine.clone();
                    let finished = finished.clone();
                    async move {
                        let session = ServerSession::new_http1(Box::new(native));
                        let (_stop, shutdown) = tokio::sync::watch::channel(false);
                        let _ = engine.process_new_http(session, &shutdown).await;
                        finished.notify_one();
                    }
                }
            })
            .await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(REQUEST).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            fail.store(true, Ordering::SeqCst);
            client
                .write_all(&websocket::encode_client(1, CREATE).unwrap())
                .await
                .unwrap();
            attempted.notified().await;
            finished.notified().await;
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        })
        .await
        .expect("a failed release write must end the session without waiting for another message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_release_skips_held_frames_from_a_retired_upstream() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let request = head(&mut socket).await;
            upgrade(&mut socket, &request).await;
            frame(&mut socket, true).await;
            for _ in 0..2 {
                socket.write_all(&websocket::encode_server(1, COMPLETED).unwrap()).await.unwrap();
            }
            let mut tail = Vec::new();
            let _ = socket.read_to_end(&mut tail).await;
        }).await;
        let directory = tempfile::tempdir().unwrap();
        buffer_decision_artifact(directory.path(),
            r#"return action("{\"version\":1,\"decision\":\"forward\",\"hold\":\"buffer\"}", "", out);"#);
        let source = std::fs::read_to_string(directory.path().join("holding.c")).unwrap();
        let replacement = r#"return action("{\"version\":1,\"decision\":\"reply\",\"hold\":\"release\",\"upstream\":\"disconnect\"}", "{\"type\":\"error\",\"error\":{\"message\":\"retired\"}}", out);"#;
        let source = source.replace(
            r#"return action("{\"version\":1,\"decision\":\"forward\",\"hold\":\"release\"}", "", out);"#,
            replacement);
        let proxy = pingora_worker::assembly::assemble(config(origin.address,
            compile_session_source(directory.path(), &source))).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
        let (opcode, payload) = frame(&mut client, false).await;
        assert_eq!(opcode, 1);
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&payload).unwrap(),
            json!({"type":"error","error":{"message":"retired"}}));
        client.write_all(&websocket::encode_client(9, b"still-open").unwrap()).await.unwrap();
        assert_eq!(frame(&mut client, false).await, (10, b"still-open".to_vec()));
        client.write_all(&websocket::encode_client(8, &1000_u16.to_be_bytes()).unwrap()).await.unwrap();
        assert_eq!(frame(&mut client, false).await.0, 8);
        drop(client); drop(worker); proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
