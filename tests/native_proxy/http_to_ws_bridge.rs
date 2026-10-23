//! Native HTTP clients, approved full creates and fresh WS upstreams.
use super::*;
use bytes::Bytes;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};
const DELTA:&[u8]=b"{\n\"type\":\"response.output_text.delta\",\n\"delta\":\"exact\",\"number\":12345678901234567890\n}";
const COMPLETE:&[u8]=br#"{"type":"response.completed","response":{"id":"r1","output":[],"number":12345678901234567890}}"#;
fn artifact(dir: &Path) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join("http-bridge.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .args([
                "-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT",
                "-DTEST_HTTP_TO_WS"
            ])
            .arg(format!(
                "-DTEST_CLEANUP_PATH=\"{}\"",
                dir.join("closed").display()
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
async fn proxy(origin: SocketAddr, dir: &Path) -> NativeProxy {
    pingora_worker::assembly::assemble(serde_json::from_value(json!({
        "upstream":format!("http://{origin}"),"inspection":{"max_message_bytes":6291456},"callbacks":[{"binding":"direct","path":artifact(dir),"package":"worker.metadata.test",
        "hooks":[],"message_codec":"codex.responses.v1","message_actions":true,"message_http_bridge":true,"message_responses":true,
        "patch_headers":["authorization","x-approved"],"config":{"version":1,"decision":"forward","upstream":"connect",
        "path":"/backend-api/codex/responses","set_headers":[["x-approved","yes"],["authorization","Bearer fixture"]]}}]
    })).unwrap()).await.unwrap()
}
async fn frame(socket: &mut TcpStream) -> Vec<u8> {
    let mut output = Vec::new();
    let mut first = true;
    loop {
        let opcode = socket.read_u8().await.unwrap();
        assert_eq!(opcode & 15, if first { 1 } else { 0 });
        first = false;
        let second = socket.read_u8().await.unwrap();
        assert_ne!(second & 128, 0);
        let length = match second & 127 {
            126 => socket.read_u16().await.unwrap() as usize,
            127 => socket.read_u64().await.unwrap() as usize,
            n => n as usize,
        };
        let mut mask = [0; 4];
        socket.read_exact(&mut mask).await.unwrap();
        let mut bytes = vec![0; length];
        socket.read_exact(&mut bytes).await.unwrap();
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte ^= mask[i % 4];
        }
        output.extend(bytes);
        if opcode & 128 != 0 {
            return output;
        }
    }
}
async fn upgrade(socket: &mut TcpStream) {
    let request = String::from_utf8(head(socket).await).unwrap();
    assert!(request.starts_with("GET /backend-api/codex/responses HTTP/1.1"));
    let lower = request.to_ascii_lowercase();
    assert!(lower.contains("x-approved: yes"));
    assert!(lower.contains("authorization: bearer fixture"));
    assert!(!lower.contains("content-length:"));
    let key = request
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, v)| v.trim())
        })
        .unwrap();
    let accept =
        proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
    let body = frame(socket).await;
    if body.len() < 1024 * 1024 {
        assert_eq!(
            body,
            br#"{"type":"response.create","model":"approved","input":[]}"#
        );
    } else {
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["type"], "response.create");
        assert_eq!(body["opaque"].as_str().unwrap().len(), 2_400_000);
    }
}
async fn send(socket: &mut TcpStream, payload: &[u8]) {
    socket
        .write_all(&proxy_protocols::websocket::encode_server(1, payload).unwrap())
        .await
        .unwrap();
}
async fn post(address: SocketAddr, body: &[u8]) -> TcpStream {
    post_path(address, "/backend-api/codex/responses", body).await
}
async fn post_path(address: SocketAddr, path: &str, body: &[u8]) -> TcpStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST {path} HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket
}
async fn h2_worker(proxy: NativeProxy) -> Listening {
    let mut engine = HttpProxy::new(proxy, Arc::new(ServerConf::default()));
    engine.handle_init_modules();
    let engine = Arc::new(engine);
    listen(move |socket| {
        let engine = engine.clone();
        async move {
            let native = pingora_core::protocols::l4::stream::Stream::from(socket);
            let mut connection =
                pingora_core::protocols::http::v2::server::handshake(Box::new(native), None)
                    .await
                    .unwrap();
            while let Ok(Some(session)) =
                pingora_core::protocols::http::v2::server::HttpSession::from_h2_conn(
                    &mut connection,
                    Arc::new(pingora_core::protocols::Digest::default()),
                )
                .await
            {
                let engine = engine.clone();
                tokio::spawn(async move {
                    let (_stop, shutdown) = tokio::sync::watch::channel(false);
                    let _ = engine
                        .process_new_http(ServerSession::new_http2(session), &shutdown)
                        .await;
                });
            }
        }
    })
    .await
}
async fn h2_post(
    sender: &h2::client::SendRequest<Bytes>,
    body: &'static [u8],
) -> (h2::client::ResponseFuture, h2::SendStream<Bytes>) {
    let mut sender = sender.clone().ready().await.unwrap();
    let request = http::Request::builder()
        .method("POST")
        .uri("https://local/backend-api/codex/responses")
        .header("content-type", "application/json")
        .body(())
        .unwrap();
    let (response, mut send) = sender.send_request(request, false).unwrap();
    send.send_data(Bytes::from_static(body), true).unwrap();
    (response, send)
}
async fn h2_body(response: h2::client::ResponseFuture) -> Vec<u8> {
    let response = response.await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(!response.headers().contains_key("connection"));
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        body.flow_control().release_capacity(chunk.len()).unwrap();
        bytes.extend(chunk);
    }
    bytes
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_h1_streams_sse_and_returns_raw_final_json() {
    timeout(WAIT, async {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let origin = listen(move |mut socket| {
            let count = count.clone();
            async move {
                upgrade(&mut socket).await;
                count.fetch_add(1, Ordering::SeqCst);
                send(&mut socket, DELTA).await;
                send(&mut socket, COMPLETE).await;
                let mut byte = [0];
                assert!(!matches!(socket.read(&mut byte).await,Ok(n)if n>0));
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = serve_engine(proxy.clone()).await;
        for (path, body) in [
            (
                "/backend-api/codex/responses",
                br#"{"model":"m","input":[],"stream":true}"#.as_slice(),
            ),
            (
                "/responses",
                br#"{"model":"m","input":[],"stream":false}"#.as_slice(),
            ),
            ("/v1/responses", br#"{"model":"m","input":[]}"#.as_slice()),
        ] {
            let mut socket = post_path(worker.address, path, body).await;
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            assert!(
                response.starts_with(b"HTTP/1.1 200"),
                "{}",
                String::from_utf8_lossy(&response)
            );
            let text = String::from_utf8(response).unwrap();
            assert!(text.contains("12345678901234567890"));
            if body.ends_with(b"true}") {
                assert!(text.contains("text/event-stream"));
                assert!(text.contains("data: \"delta\":\"exact\""));
            } else {
                assert!(text.contains("application/json"));
                assert!(!text.contains("response.output_text.delta"));
                assert!(text.contains(r#"{"id":"r1","output":[],"number":12345678901234567890}"#));
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(std::fs::read(dir.path().join("closed")).unwrap(), b"xxx");
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_rejects_continuation_and_invalid_mode_before_dial() {
    timeout(WAIT, async {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let origin = listen(move |_| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = serve_engine(proxy.clone()).await;
        for body in [
            br#"{"input":[],"generate":false}"#.as_slice(),
            br#"{"input":[],"previous_response_id":"other"}"#,
            br#"{"input":[],"stream":"true"}"#,
            br#"{"input":[]"#,
            b"[1,2]",
        ] {
            let mut socket = post(worker.address, body).await;
            let mut reply = Vec::new();
            socket.read_to_end(&mut reply).await.unwrap();
            assert!(reply.starts_with(b"HTTP/1.1 400"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(dir.path().join("closed")).unwrap(), b"xxxxx");
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_h2_concurrent_requests_are_isolated() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            upgrade(&mut socket).await;
            send(&mut socket, DELTA).await;
            send(&mut socket, COMPLETE).await;
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = h2_worker(proxy.clone()).await;
        let (sender, connection) =
            h2::client::handshake(TcpStream::connect(worker.address).await.unwrap())
                .await
                .unwrap();
        let task = tokio::spawn(connection);
        let (first, _) = h2_post(&sender, br#"{"input":[],"stream":true}"#).await;
        let (second, _) = h2_post(&sender, br#"{"input":[]}"#).await;
        let (first, second) = tokio::join!(h2_body(first), h2_body(second));
        assert!(first.starts_with(b"data: "));
        assert_eq!(
            second,
            br#"{"id":"r1","output":[],"number":12345678901234567890}"#
        );
        task.abort();
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_large_full_request_uses_configured_fragment_limit() {
    timeout(Duration::from_secs(10), async {
        let origin = listen(|mut socket| async move {
            upgrade(&mut socket).await;
            send(&mut socket, COMPLETE).await;
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = serve_engine(proxy.clone()).await;
        let body =
            serde_json::to_vec(&json!({"model":"large","input":[],"opaque":"é".repeat(1_200_000)}))
                .unwrap();
        assert!(body.len() > 2_360_000);
        let mut socket = post(worker.address, &body).await;
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_eof_and_error_do_not_fabricate_completion() {
    timeout(WAIT,async {
        let calls=Arc::new(AtomicUsize::new(0));let count=calls.clone();
        let origin=listen(move|mut socket|{let count=count.clone();async move {
            upgrade(&mut socket).await;let turn=count.fetch_add(1,Ordering::SeqCst);
            if turn==0 {send(&mut socket,DELTA).await;} else {send(&mut socket,br#"{"type":"error","status":429,"error":{"type":"rate_limit_error","message":"limited"}}"#).await;}
        }}).await;
        let dir=tempfile::tempdir().unwrap();let proxy=proxy(origin.address,dir.path()).await;let worker=serve_engine(proxy.clone()).await;
        let mut socket=post(worker.address,br#"{"input":[]}"#).await;let mut response=Vec::new();let _=socket.read_to_end(&mut response).await;
        assert!(!response.starts_with(b"HTTP/1.1 200"));assert!(!String::from_utf8_lossy(&response).contains("response.completed"));
        let mut socket=post(worker.address,br#"{"input":[]}"#).await;let mut response=Vec::new();socket.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 429"),"{}",String::from_utf8_lossy(&response));assert!(String::from_utf8_lossy(&response).contains("rate_limit_error"));
        assert_eq!(calls.load(Ordering::SeqCst),2,"EOF and errors must not retry requests");drop(worker);proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_h1_disconnect_cancels_stalled_ws() {
    timeout(WAIT, async {
        let (closed_tx, mut closed) = tokio::sync::mpsc::unbounded_channel();
        let origin = listen(move |mut socket| {
            let closed = closed_tx.clone();
            async move {
                upgrade(&mut socket).await;
                send(&mut socket, DELTA).await;
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
                closed.send(()).unwrap();
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = post(worker.address, br#"{"input":[],"stream":true}"#).await;
        assert!(head(&mut socket).await.starts_with(b"HTTP/1.1 200"));
        drop(socket);
        timeout(Duration::from_secs(1), closed.recv())
            .await
            .unwrap()
            .unwrap();
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_h2_reset_cancels_only_its_ws() {
    timeout(WAIT, async {
        let count = Arc::new(AtomicUsize::new(0));
        let turns = count.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let finish = release.clone();
        let (closed_tx, mut closed) = tokio::sync::mpsc::unbounded_channel();
        let origin = listen(move |mut socket| {
            let turn = turns.fetch_add(1, Ordering::SeqCst);
            let release = finish.clone();
            let closed = closed_tx.clone();
            async move {
                upgrade(&mut socket).await;
                send(&mut socket, DELTA).await;
                if turn == 0 {
                    let mut byte = [0];
                    let _ = socket.read(&mut byte).await;
                    closed.send(()).unwrap();
                } else {
                    release.notified().await;
                    send(&mut socket, COMPLETE).await;
                }
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = h2_worker(proxy.clone()).await;
        let (sender, connection) =
            h2::client::handshake(TcpStream::connect(worker.address).await.unwrap())
                .await
                .unwrap();
        let task = tokio::spawn(connection);
        let (first, mut cancel) = h2_post(&sender, br#"{"input":[],"stream":true}"#).await;
        let mut first = first.await.unwrap().into_body();
        assert!(first.data().await.unwrap().unwrap().starts_with(b"data: "));
        let (second, _) = h2_post(&sender, br#"{"input":[],"stream":true}"#).await;
        let mut second = second.await.unwrap().into_body();
        assert!(second.data().await.unwrap().unwrap().starts_with(b"data: "));
        cancel.send_reset(h2::Reason::CANCEL);
        timeout(Duration::from_secs(1), closed.recv())
            .await
            .unwrap()
            .unwrap();
        release.notify_one();
        let mut bytes = Vec::new();
        while let Some(chunk) = second.data().await {
            bytes.extend(chunk.unwrap());
        }
        assert!(String::from_utf8_lossy(&bytes).contains("response.completed"));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        task.abort();
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_replies_to_split_upstream_ping() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            upgrade(&mut socket).await;
            socket.write_all(&[0x89]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket.write_all(&[4, b'p']).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket.write_all(b"ing").await.unwrap();
            assert_eq!(socket.read_u8().await.unwrap(), 0x8a);
            assert_eq!(socket.read_u8().await.unwrap(), 0x84);
            let mut mask = [0; 4];
            socket.read_exact(&mut mask).await.unwrap();
            let mut pong = [0; 4];
            socket.read_exact(&mut pong).await.unwrap();
            for (i, byte) in pong.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
            assert_eq!(&pong, b"ping");
            send(&mut socket, COMPLETE).await;
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = proxy(origin.address, dir.path()).await;
        let worker = serve_engine(proxy.clone()).await;
        let mut socket = post(worker.address, br#"{"input":[]}"#).await;
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_failed_terminal_preserves_raw_json_and_http_status() {
    for (payload,status) in [
        (br#"{"type":"response.failed","response":{"id":"bad","status":"failed","number":12345678901234567890}}"#.as_slice(),502),
        (br#"{"type":"response.incomplete","response":{"id":"short","status":"incomplete"}}"#.as_slice(),502),
        (br#"{"type":"response.completed","response":{"id":"failed","status":"failed","error":{"status":429}}}"#.as_slice(),429),
        (br#"{"type":"response.cancelled","response":{"id":"cancelled","status":"cancelled"}}"#.as_slice(),499),
    ] {
        timeout(WAIT,async {
            let origin=listen(move|mut socket|async move {upgrade(&mut socket).await;send(&mut socket,payload).await;}).await;
            let dir=tempfile::tempdir().unwrap();let proxy=proxy(origin.address,dir.path()).await;let worker=serve_engine(proxy.clone()).await;
            let mut socket=post(worker.address,br#"{"input":[]}"#).await;let mut response=Vec::new();socket.read_to_end(&mut response).await.unwrap();
            let text=String::from_utf8(response).unwrap();assert!(text.starts_with(&format!("HTTP/1.1 {status}")),"{text}");
            let marker=br#""response":"#;let start=payload.windows(marker.len()).position(|window|window==marker).unwrap()+marker.len();
            assert!(text.contains(std::str::from_utf8(&payload[start..payload.len()-1]).unwrap()));
            drop(worker);proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_to_ws_bridge_process_listener_accepts_h2_and_h1() {
    struct Process(std::process::Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    timeout(Duration::from_secs(10),async {
        let origin=listen(|mut socket|async move {upgrade(&mut socket).await;send(&mut socket,COMPLETE).await;}).await;
        let dir=tempfile::tempdir().unwrap();
        let reserve=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=reserve.local_addr().unwrap();drop(reserve);
        let config=dir.path().join("config.json");
        std::fs::write(&config,json!({"upstream":format!("http://{}",origin.address),"listen":address,"threads":1,
            "callbacks":[{"binding":"direct","path":artifact(dir.path()),"package":"worker.metadata.test","hooks":[],
            "message_codec":"codex.responses.v1","message_actions":true,"message_http_bridge":true,"message_responses":true,
            "patch_headers":["authorization","x-approved"],"config":{"version":1,"decision":"forward","upstream":"connect",
            "path":"/backend-api/codex/responses","set_headers":[["x-approved","yes"],["authorization","Bearer fixture"]]}}]}).to_string()).unwrap();
        let log=dir.path().join("worker.log");
        let mut process=Process(Command::new(env!("CARGO_BIN_EXE_pingora-worker")).arg("--config").arg(&config)
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::from(std::fs::File::create(&log).unwrap())).spawn().unwrap());
        let socket=loop {
            if let Ok(socket)=TcpStream::connect(address).await {break socket;}
            assert!(process.0.try_wait().unwrap().is_none(),"{}",std::fs::read_to_string(&log).unwrap());
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let(sender,connection)=h2::client::handshake(socket).await.unwrap();let task=tokio::spawn(connection);
        let(response,_)=h2_post(&sender,br#"{"input":[]}"#).await;
        assert_eq!(h2_body(response).await,br#"{"id":"r1","output":[],"number":12345678901234567890}"#);
        let mut socket=post(address,br#"{"input":[]}"#).await;let mut bytes=Vec::new();socket.read_to_end(&mut bytes).await.unwrap();assert!(bytes.starts_with(b"HTTP/1.1 200"));
        task.abort();
    }).await.unwrap();
}
