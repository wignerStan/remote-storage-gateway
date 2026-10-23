//! A request action schedules carrier-local holding without calling delta policy.
use super::*;
use proxy_pingora::{connect_upstream::websocket_session::handshake, http_ingress::ProxyLifecycle};
use proxy_protocols::websocket;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

// Includes C compilation and four native scenarios. Wire assertions keep their own short bounds.
const FIXTURE_WAIT: Duration = Duration::from_secs(20);

const CREATE: &[u8] = br#"{"type":"response.create","model":"m","input":[]}"#;
const BEFORE: &[u8] = br#"{"type":"response.output_text.delta","delta":"before"}"#;
const AFTER1: &[u8] = br#"{"type":"response.output_text.delta","delta":"after-one"}"#;
const AFTER2: &[u8] = br#"{"type":"response.output_text.delta","delta":"after-two"}"#;
const COMPLETE: &[u8] = br#"{"type":"response.completed","response":{"id":"done","output":[]}}"#;

fn artifact(dir: &Path, hold: &str, http: bool) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
    let start = source.find("static uint32_t call(").unwrap();
    let end = source.find("static uint32_t next(").unwrap();
    let request_kind = if http {
        "response/"
    } else {
        "response.create/"
    };
    let delay = if matches!(hold, "allow" | "replacement_terminal") {
        0
    } else {
        200
    };
    let terminal_action = if matches!(hold, "allow" | "replacement_terminal") {
        String::new()
    } else {
        format!(
            r#"if (++completed == 1) return action("{{\"version\":1,\"decision\":\"forward\",\"hold\":\"{hold}\"}}", "", out);"#
        )
    };
    let replacement_action = if hold == "replacement_terminal" {
        format!(
            r#"if (begins(method, "openai.responses.v1/response.created/")) return action("{{\"version\":1,\"decision\":\"forward\"}}", {}, out);"#,
            serde_json::to_string(std::str::from_utf8(COMPLETE).unwrap()).unwrap()
        )
    } else {
        String::new()
    };
    // The first request has a near-future boundary. The next request explicitly
    // clears it and replaces the generation. Unexpected delta callbacks fail.
    source.replace_range(start..end, &format!(r#"
static uint32_t call(uint64_t state, uint64_t handle, InfraBytes method, InfraBytes input, InfraCallMeta meta, InfraOwnedBytes *out) {{
    (void)state; (void)handle; (void)input; (void)meta;
    if (begins(method, "openai.responses.v1/{request_kind}")) {{
        struct timespec now; clock_gettime(CLOCK_REALTIME, &now);
        unsigned long long at = ++requests == 1 ? (unsigned long long)now.tv_sec * 1000 + now.tv_nsec / 1000000 + {delay} : 0;
        char metadata[512];
        snprintf(metadata, sizeof(metadata), "{{\"version\":1,\"decision\":\"forward\",\"upstream\":\"%s\",\"hold_response_at_unix_ms\":%llu}}", requests == 1 ? "connect" : "replace", at);
        return action(metadata, "{{\"type\":\"response.create\",\"model\":\"m\",\"input\":[]}}", out);
    }}
    {replacement_action}
    if (begins(method, "openai.responses.v1/response.completed/")) {{
        {terminal_action}
    }} else if (begins(method, "openai.responses.v1/response.event/") || begins(method, "openai.responses.v1/response.output_text.delta/")) {{
        return INFRA_FAILED;
    }}
    const uint8_t allow = 0; return copy(&allow, 1, out);
}}
"#));
    let source_path = dir.join("scheduled.c");
    std::fs::write(&source_path, source).unwrap();
    let path = dir.join("scheduled.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(source_path)
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}

async fn proxy(origin: SocketAddr, dir: &Path, hold: &str, http: bool) -> NativeProxy {
    pingora_worker::assembly::assemble(serde_json::from_value(json!({
        "upstream":format!("http://{origin}"), "callbacks":[{
            "binding":"direct", "path":artifact(dir, hold, http), "package":"worker.sessions.test",
            "message_codec":"openai.responses.v1", "message_actions":true,
            "message_http_bridge":http, "message_responses":true
        }]
    })).unwrap()).await.unwrap()
}
async fn frame(socket: &mut TcpStream, masked: bool) -> (u8, Vec<u8>) {
    let opcode = socket.read_u8().await.unwrap() & 15;
    let second = socket.read_u8().await.unwrap();
    assert_eq!(second & 128 != 0, masked);
    let length = match second & 127 {
        126 => socket.read_u16().await.unwrap() as usize,
        127 => usize::try_from(socket.read_u64().await.unwrap()).unwrap(),
        n => n as usize,
    };
    assert!(length < 4096);
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await.unwrap();
    }
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await.unwrap();
    if masked {
        for (i, byte) in body.iter_mut().enumerate() {
            *byte ^= mask[i % 4];
        }
    }
    (opcode, body)
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
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
    assert_eq!(frame(socket, true).await, (1, CREATE.to_vec()));
}
async fn send(socket: &mut TcpStream, body: &[u8]) {
    socket
        .write_all(&websocket::encode_server(1, body).unwrap())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allowed_terminal_never_waits_on_schedule_and_missing_release_fails_closed() {
    timeout(FIXTURE_WAIT, async {
        for http in [false, true] {
            for held in [false, true] {
                let origin = listen(move |mut socket| async move {
                    upstream(&mut socket).await;
                    if held { send(&mut socket, AFTER1).await; }
                    send(&mut socket, COMPLETE).await;
                    let mut tail = Vec::new(); let _ = socket.read_to_end(&mut tail).await;
                }).await;
                let dir = tempfile::tempdir().unwrap();
                let proxy = proxy(origin.address, dir.path(), "allow", http).await;
                let worker = serve_engine(proxy.clone()).await;
                let mut client = TcpStream::connect(worker.address).await.unwrap();
                if http {
                    let body = br#"{"model":"m","input":[]}"#;
                    client.write_all(format!("POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                    client.write_all(body).await.unwrap();
                    let mut reply = Vec::new(); client.read_to_end(&mut reply).await.unwrap();
                    assert!(reply.starts_with(if held { b"HTTP/1.1 502" } else { b"HTTP/1.1 200" }));
                    if !held { assert!(String::from_utf8_lossy(&reply).contains("done")); }
                    assert!(!String::from_utf8_lossy(&reply).contains("after-one"));
                } else {
                    client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
                    assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
                    client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
                    let expected = if held { (8, 1011_u16.to_be_bytes().to_vec()) } else { (1, COMPLETE.to_vec()) };
                    assert_eq!(frame(&mut client, false).await, expected);
                }
                drop(client); drop(worker); proxy.shutdown().await.unwrap();
            }
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_terminal_replacement_obeys_schedule_and_pending_hold_rules() {
    timeout(FIXTURE_WAIT, async {
        for http in [false, true] {
            for held in [false, true] {
                let origin = listen(move |mut socket| async move {
                    upstream(&mut socket).await;
                    if held { send(&mut socket, AFTER1).await; }
                    send(&mut socket, br#"{"type":"response.created","response":{"id":"pending"}}"#).await;
                    let mut tail = Vec::new(); let _ = socket.read_to_end(&mut tail).await;
                }).await;
                let dir = tempfile::tempdir().unwrap();
                let proxy = proxy(origin.address, dir.path(), "replacement_terminal", http).await;
                let worker = serve_engine(proxy.clone()).await;
                let mut client = TcpStream::connect(worker.address).await.unwrap();
                if http {
                    let body = br#"{"model":"m","input":[]}"#;
                    client.write_all(format!("POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                    client.write_all(body).await.unwrap();
                    let mut reply = Vec::new(); client.read_to_end(&mut reply).await.unwrap();
                    assert!(reply.starts_with(if held { b"HTTP/1.1 502" } else { b"HTTP/1.1 200" }));
                    if !held { assert!(String::from_utf8_lossy(&reply).contains("done")); }
                    assert!(!String::from_utf8_lossy(&reply).contains("after-one"));
                } else {
                    client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
                    assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
                    client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
                    let expected = if held { (8, 1011_u16.to_be_bytes().to_vec()) } else { (1, COMPLETE.to_vec()) };
                    assert_eq!(frame(&mut client, false).await, expected);
                }
                drop(client); drop(worker); proxy.shutdown().await.unwrap();
            }
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_scheduled_hold_preserves_delta_bypass_release_order_discard_and_next_generation() {
    timeout(FIXTURE_WAIT, async {
        for hold in ["release", "discard"] {
            let sent = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let generation = Arc::new(AtomicUsize::new(0));
            let origin = listen({ let sent = sent.clone(); let finish = finish.clone(); let generation = generation.clone(); move |mut socket| {
                let sent = sent.clone(); let finish = finish.clone();
                let generation = generation.fetch_add(1, Ordering::SeqCst);
                async move {
                    upstream(&mut socket).await;
                    send(&mut socket, BEFORE).await;
                    if generation == 0 {
                        tokio::time::sleep(Duration::from_millis(350)).await;
                        send(&mut socket, AFTER1).await;
                        send(&mut socket, AFTER2).await;
                        sent.notify_one();
                        finish.notified().await;
                    }
                    send(&mut socket, COMPLETE).await;
                    let mut tail = Vec::new(); let _ = socket.read_to_end(&mut tail).await;
                }
            }}).await;
            let dir = tempfile::tempdir().unwrap();
            let proxy = proxy(origin.address, dir.path(), hold, false).await;
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
            assert_eq!(frame(&mut client, false).await, (1, BEFORE.to_vec()));
            sent.notified().await;
            assert!(timeout(Duration::from_millis(50), client.read_u8()).await.is_err(), "late delta escaped the carrier hold");
            client.write_all(&websocket::encode_client(9, b"still-live").unwrap()).await.unwrap();
            assert_eq!(frame(&mut client, false).await, (10, b"still-live".to_vec()));
            finish.notify_one();
            if hold == "release" {
                assert_eq!(frame(&mut client, false).await, (1, AFTER1.to_vec()));
                assert_eq!(frame(&mut client, false).await, (1, AFTER2.to_vec()));
            }
            assert_eq!(frame(&mut client, false).await, (1, COMPLETE.to_vec()));
            client.write_all(&websocket::encode_client(1, CREATE).unwrap()).await.unwrap();
            assert_eq!(frame(&mut client, false).await, (1, BEFORE.to_vec()));
            assert_eq!(frame(&mut client, false).await, (1, COMPLETE.to_vec()));
            assert_eq!(generation.load(Ordering::SeqCst), 2);
            drop(client); drop(worker); proxy.shutdown().await.unwrap();
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_scheduled_hold_preserves_sse_delta_bypass_and_release_order_or_discard() {
    timeout(FIXTURE_WAIT, async {
        for hold in ["release", "discard"] {
            let sent = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let origin = listen({
                let sent = sent.clone();
                let finish = finish.clone();
                move |mut socket| {
                    let sent = sent.clone();
                    let finish = finish.clone();
                    async move {
                        upstream(&mut socket).await;
                        send(&mut socket, BEFORE).await;
                        tokio::time::sleep(Duration::from_millis(350)).await;
                        send(&mut socket, AFTER1).await;
                        send(&mut socket, AFTER2).await;
                        sent.notify_one();
                        finish.notified().await;
                        send(&mut socket, COMPLETE).await;
                        let mut tail = Vec::new();
                        let _ = socket.read_to_end(&mut tail).await;
                    }
                }
            })
            .await;
            let dir = tempfile::tempdir().unwrap();
            let proxy = proxy(origin.address, dir.path(), hold, true).await;
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            let body = br#"{"model":"m","input":[],"stream":true}"#;
            client
                .write_all(
                    format!(
                        "POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            client.write_all(body).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 200"));
            // Read the first SSE chunk completely before checking the late hold.
            let mut response = Vec::new();
            loop {
                response.push(client.read_u8().await.unwrap());
                if response.ends_with(b"\n\n\r\n") {
                    break;
                }
            }
            assert!(String::from_utf8_lossy(&response).contains("before"));
            sent.notified().await;
            assert!(
                timeout(Duration::from_millis(50), client.read_u8())
                    .await
                    .is_err(),
                "late SSE delta escaped the carrier hold"
            );
            finish.notify_one();
            client.read_to_end(&mut response).await.unwrap();
            let text = String::from_utf8(response).unwrap();
            if hold == "release" {
                assert!(text.find("after-one").unwrap() < text.find("after-two").unwrap());
                assert!(text.find("after-two").unwrap() < text.find("response.completed").unwrap());
            } else {
                assert!(!text.contains("after-one") && !text.contains("after-two"));
            }
            assert!(text.contains("response.completed"));
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
