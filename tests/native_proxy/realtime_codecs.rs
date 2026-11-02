//! Realtime signaling and sideband messages through actual native Object callbacks.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};

const SDP: &str = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";
const MULTIPART: &str = "--offer\r\nContent-Disposition: form-data; name=\"sdp\"\r\nContent-Type: application/sdp\r\n\r\nv=0\r\n\r\n--offer\r\nContent-Disposition: form-data; name=\"session\"\r\nContent-Type: application/json\r\n\r\n{\"model\":\"gpt-realtime\",\"unknown\":true}\r\n--offer--\r\n";
const BACKEND: &str =
    r#"{"sdp":"v=0\r\n","session":{"delegation":{"type":"client"},"unknown":true}}"#;
const EVENT: &str = r#"{"type":"session.update","session":{"unknown":"preserved"}}"#;

fn artifact(dir: &Path, id: &str, kind: &str, body: &str, action: u8) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join("realtime.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(format!("-DTEST_REALTIME_REPLY={action}"))
            .arg(format!("-DTEST_REALTIME_METHOD=\"{id}/{kind}/\""))
            .arg(format!(
                "-DTEST_REALTIME_BODY={}",
                serde_json::to_string(body).unwrap()
            ))
            .arg(root.join("tests/fixtures/metadata_callback.c"))
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webrtc_signaling_gates_complete_offers_and_preserves_answer_and_location() {
    for (id, route, content_type, kind, body) in [
        (
            "openai.realtime.v1",
            "/v1/realtime/calls",
            "application/sdp",
            "realtime.call.sdp",
            SDP,
        ),
        (
            "openai.realtime.v1",
            "/v1/realtime/calls",
            "multipart/form-data; boundary=offer",
            "realtime.call.multipart",
            MULTIPART,
        ),
        (
            "openai.realtime.v1",
            "/v1/live",
            "multipart/form-data; boundary=offer",
            "realtime.call.multipart",
            MULTIPART,
        ),
        (
            "codex.realtime.v1",
            "/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas",
            "application/json",
            "realtime.call.json",
            BACKEND,
        ),
    ] {
        timeout(WAIT, async {
            let wire = if body == BACKEND { zstd::stream::encode_all(body.as_bytes(), 3).unwrap() } else { body.as_bytes().to_vec() };
            let expected = wire.clone();
            let received = Arc::new(AtomicUsize::new(0));
            let observed = received.clone();
            let origin = listen(move |mut socket| {
                let expected = expected.clone();
                let observed = observed.clone();
                async move {
                    let request = String::from_utf8(head(&mut socket).await).unwrap();
                    assert!(request.starts_with(&format!("POST {route} HTTP/1.1\r\n")));
                    assert!(request.to_ascii_lowercase().contains(&format!("content-type: {content_type}")));
                    let mut actual = vec![0; expected.len()];
                    for b in &mut actual { *b = socket.read_u8().await.unwrap(); observed.fetch_add(1, Ordering::SeqCst); }
                    assert_eq!(actual, expected);
                    socket.write_all(format!("HTTP/1.1 201 Created\r\nContent-Type: application/sdp\r\nLocation: /v1/live/rtc_test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{SDP}", SDP.len()).as_bytes()).await.unwrap();
                }
            }).await;
            let dir = tempfile::tempdir().unwrap();
            let callback = artifact(dir.path(), id, kind, body, 0);
            let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":callback,"package":"worker.metadata.test","message_codec":id}]})).unwrap()).await.unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            let coding = if body == BACKEND { "Content-Encoding: zstd\r\n" } else { "" };
            client.write_all(format!("POST {route} HTTP/1.1\r\nHost: local\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{coding}\r\n", wire.len()).as_bytes()).await.unwrap();
            client.write_all(&wire[..8]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(received.load(Ordering::SeqCst), 0);
            client.write_all(&wire[8..]).await.unwrap();
            let mut response = Vec::new(); client.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 201"), "{}", String::from_utf8_lossy(&response));
            assert!(response.ends_with(SDP.as_bytes()));
            assert!(String::from_utf8(response).unwrap().to_ascii_lowercase().contains("location: /v1/live/rtc_test\r\n"));
            drop(worker); proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

fn frame(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 126);
    let mut wire = vec![
        (if fin { 0x80 } else { 0 }) | opcode,
        0x80 | payload.len() as u8,
        1,
        2,
        3,
        4,
    ];
    wire.extend(
        payload
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
    );
    wire
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_and_live_sidebands_inspect_complete_messages_and_keep_server_deltas_opaque() {
    for (id, route) in [
        ("openai.realtime.v1", "/v1/realtime?call_id=rtc_test"),
        ("openai.realtime.v1", "/v1/live"),
        ("openai.realtime.v1", "/v1/live/rtc_test"),
        ("codex.realtime.v1", "/backend-api/codex/realtime"),
        ("codex.realtime.v1", "/backend-api/codex/live/rtc_test"),
    ] {
        timeout(WAIT, async {
            let first = frame(1, false, &EVENT.as_bytes()[..8]);
            let ping = frame(9, true, b"p");
            let last = frame(0, true, &EVENT.as_bytes()[8..]);
            let expected = [ping.clone(), first.clone(), last.clone()].concat();
            let received = Arc::new(AtomicUsize::new(0)); let observed = received.clone();
            let origin = listen(move |mut socket| {
                let expected = expected.clone(); let observed = observed.clone();
                async move {
                    let request = String::from_utf8(head(&mut socket).await).unwrap();
                    assert!(request.starts_with(&format!("GET {route} HTTP/1.1\r\n")));
                    assert!(!request.to_ascii_lowercase().contains("sec-websocket-extensions"));
                    socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
                    let mut actual = vec![0; expected.len()];
                    for b in &mut actual { *b = socket.read_u8().await.unwrap(); observed.fetch_add(1, Ordering::SeqCst); }
                    assert_eq!(actual, expected);
                    socket.write_all(&[0x82, 3, 0, 255, 7]).await.unwrap();
                }
            }).await;
            let dir = tempfile::tempdir().unwrap();
            let callback = artifact(dir.path(), id, "session.update", EVENT, 0);
            let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":callback,"package":"worker.metadata.test","message_codec":id}]})).unwrap()).await.unwrap();
            let worker = serve_engine(proxy.clone()).await; let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(format!("GET {route} HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n").as_bytes()).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            client.write_all(&first).await.unwrap(); client.write_all(&ping).await.unwrap();
            timeout(WAIT, async {
                while received.load(Ordering::SeqCst) < ping.len() { tokio::time::sleep(Duration::from_millis(1)).await; }
            }).await.expect("heartbeat blocked behind incomplete message");
            assert_eq!(received.load(Ordering::SeqCst), ping.len());
            client.write_all(&last).await.unwrap(); let mut delta = [0;5]; client.read_exact(&mut delta).await.unwrap();
            assert_eq!(delta, [0x82,3,0,255,7]);
            drop(client); drop(worker); proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

#[tokio::test]
async fn denied_webrtc_offer_never_reaches_upstream_body() {
    timeout(WAIT, async {
        let received = Arc::new(AtomicUsize::new(0)); let observed = received.clone();
        let origin = listen(move |mut socket| {
            let observed = observed.clone();
            async move {
                head(&mut socket).await;
                let mut byte = [0];
                if socket.read(&mut byte).await.unwrap_or(0) > 0 { observed.fetch_add(1, Ordering::SeqCst); }
            }
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let id = "openai.realtime.v1";
        let callback = artifact(dir.path(), id, "realtime.call.sdp", SDP, 1);
        let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":callback,"package":"worker.metadata.test","message_codec":id}]})).unwrap()).await.unwrap();
        let worker = serve_engine(proxy.clone()).await; let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(format!("POST /v1/realtime/calls HTTP/1.1\r\nHost: local\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{SDP}", SDP.len()).as_bytes()).await.unwrap();
        let mut response = Vec::new(); let _ = client.read_to_end(&mut response).await;
        assert!(!response.starts_with(b"HTTP/1.1 201"));
        assert_eq!(received.load(Ordering::SeqCst), 0);
        drop(client); drop(worker); proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
