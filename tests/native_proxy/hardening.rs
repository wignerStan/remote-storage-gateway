//! Aggregate admission, absolute deadlines and response-side native approval.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};

fn artifact(dir: &Path, kind: &str, payload: &str, deny: bool) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join("response.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(format!("-DTEST_RESPONSE_REPLY={}", u8::from(deny)))
            .arg(format!(
                "-DTEST_RESPONSE_METHOD=\"openai.chat_completions.v1/{kind}/\""
            ))
            .arg(format!(
                "-DTEST_RESPONSE_BODY={}",
                serde_json::to_string(payload).unwrap()
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
fn config(origin: SocketAddr, artifact: &Path, responses: bool) -> serde_json::Value {
    json!({"upstream":format!("http://{origin}"),"native_helper":env!("CARGO_BIN_EXE_pingora-worker"),
        "callbacks":[{"path":artifact,"package":"worker.metadata.test","message_codec":"openai.chat_completions.v1","message_responses":responses}]})
}
async fn post(client: &mut TcpStream, length: usize) {
    client.write_all(format!("POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_success_and_errors_require_explicit_response_approval() {
    for (status, kind, payload) in [
        (
            200,
            "chat_completion.response",
            r#"{"choices":[],"future":"preserved"}"#,
        ),
        (
            429,
            "chat_completion.error",
            r#"{"error":{"message":"limited","code":"quota","future":7}}"#,
        ),
        (
            200,
            "chat_completion.error",
            r#"{"error":{"message":"logical failure"}}"#,
        ),
        (
            502,
            "chat_completion.error",
            "upstream temporarily unavailable",
        ),
    ] {
        for (grant, deny) in [(true, false), (true, true), (false, true)] {
            timeout(WAIT, async {
            let origin = listen(move |mut socket| async move {
                head(&mut socket).await; let mut request=[0;2]; socket.read_exact(&mut request).await.unwrap();
                assert_eq!(&request,b"{}");
                let content_type=if payload.starts_with('{') {"application/json"} else {"text/plain"};
                socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Type: {content_type}\r\nRetry-After: 3\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",payload.len()).as_bytes()).await.unwrap();
                socket.write_all(&payload.as_bytes()[..5]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                socket.write_all(&payload.as_bytes()[5..]).await.unwrap();
            }).await;
            let dir=tempfile::tempdir().unwrap(); let artifact=artifact(dir.path(),kind,payload,deny);
            let proxy=pingora_worker::assembly::assemble(serde_json::from_value(config(origin.address,&artifact,grant)).unwrap()).await.unwrap();
            let worker=serve_engine(proxy.clone()).await; let mut client=TcpStream::connect(worker.address).await.unwrap();
            post(&mut client,2).await; client.write_all(b"{}").await.unwrap();
            let mut response=Vec::new(); let _=client.read_to_end(&mut response).await;
            assert_eq!(response.ends_with(payload.as_bytes()), !grant || !deny, "{status} grant={grant} deny={deny}: {}",String::from_utf8_lossy(&response));
            if !grant || !deny { assert!(String::from_utf8_lossy(&response).to_ascii_lowercase().contains("retry-after: 3")); }
            drop(client); drop(worker); proxy.shutdown().await.unwrap();
        }).await.unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspection_assembly_deadline_expires_while_client_is_idle() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let mut bytes = [0; 1];
            let _ = socket.read(&mut bytes).await;
            tokio::time::sleep(WAIT).await;
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let artifact = artifact(dir.path(), "chat_completion.response", "{}", false);
        let mut cfg = config(origin.address, &artifact, false);
        cfg["inspection"] = json!({"message_timeout_ms":80});
        let proxy = pingora_worker::assembly::assemble(serde_json::from_value(cfg).unwrap())
            .await
            .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        post(&mut client, 100).await;
        client.write_all(b"{").await.unwrap();
        let mut response = Vec::new();
        let _ = timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("idle partial body retained beyond absolute deadline");
        assert!(!response.starts_with(b"HTTP/1.1 200"));
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_request_budget_rejects_another_waiting_body() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            let mut bytes = [0; 1];
            let _ = socket.read(&mut bytes).await;
            tokio::time::sleep(WAIT).await;
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let artifact = artifact(dir.path(), "chat_completion.response", "{}", false);
        let mut cfg = config(origin.address, &artifact, false);
        cfg["inspection"] = json!({"max_requests":1});
        let proxy = pingora_worker::assembly::assemble(serde_json::from_value(cfg).unwrap())
            .await
            .unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut first = TcpStream::connect(worker.address).await.unwrap();
        post(&mut first, 100).await;
        first.write_all(b"{").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut second = TcpStream::connect(worker.address).await.unwrap();
        post(&mut second, 2).await;
        second.write_all(b"{}").await.unwrap();
        assert!(head(&mut second).await.starts_with(b"HTTP/1.1 503"));
        drop(first);
        drop(second);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_websockets_release_buffers_but_partial_messages_reserve_them() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            head(&mut socket).await;
            socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
            let mut ping = [0; 6];
            if socket.read_exact(&mut ping).await.is_ok() {
                assert_eq!(ping, [0x89, 0x80, 1, 2, 3, 4]);
                socket.write_all(&[0x8a, 0]).await.unwrap();
                let _ = socket.read_u8().await;
            }
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let artifact = artifact(dir.path(), "response.text", "unused", false);
        let cfg = json!({"upstream":format!("http://{}",origin.address),"native_helper":env!("CARGO_BIN_EXE_pingora-worker"),
            "inspection":{"max_buffered_bytes":8*1024*1024+8*1024*1024+512*1024},
            "callbacks":[{"path":artifact,"package":"worker.metadata.test","message_codec":"websocket.v1"}]});
        let proxy = pingora_worker::assembly::assemble(serde_json::from_value(cfg).unwrap()).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        async fn upgrade(address: SocketAddr) -> (TcpStream, Vec<u8>) {
            let mut client = TcpStream::connect(address).await.unwrap();
            client.set_nodelay(true).unwrap();
            client.write_all(b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
            let reply = head(&mut client).await;
            (client, reply)
        }
        let (mut first, response) = upgrade(worker.address).await;
        assert!(response.starts_with(b"HTTP/1.1 101"));
        let (second, response) = upgrade(worker.address).await;
        assert!(response.starts_with(b"HTTP/1.1 101"), "idle connection retained buffer reservation");
        first.write_all(&[0x01, 0x81, 1, 2, 3, 4, b'a'^1, 0x89, 0x80, 1, 2, 3, 4]).await.unwrap();
        let mut pong = [0; 2]; first.read_exact(&mut pong).await.unwrap();
        assert_eq!(pong, [0x8a, 0]);
        let (third, response) = upgrade(worker.address).await;
        assert!(response.starts_with(b"HTTP/1.1 503"), "partial message escaped global buffer budget");
        drop(first); drop(second); drop(third); drop(worker);
        proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_capable_custom_codec_cannot_omit_its_upstream_frame_guard() {
    use proxy_pingora::http_ingress::message_codec::{CodecPlugin, WebSocketCodec};
    struct MissingGuard;
    impl CodecPlugin for MissingGuard {
        fn id(&self) -> &str {
            "test.reply"
        }
        fn supports_message_replies(&self) -> bool {
            true
        }
        fn create(
            &self,
        ) -> pingora_error::Result<Box<dyn proxy_protocols::message_codec::Decoder>> {
            WebSocketCodec.create()
        }
    }
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            head(&mut socket).await;
            socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let library = artifact(dir.path(), "response.text", "unused", false);
        let cfg = json!({"upstream":format!("http://{}",origin.address),"native_helper":env!("CARGO_BIN_EXE_pingora-worker"),
            "callbacks":[{"path":library,"package":"worker.metadata.test","message_codec":"test.reply","message_responses":true}]});
        let proxy = pingora_worker::assembly::assemble_with_codecs(serde_json::from_value(cfg).unwrap(), vec![Arc::new(MissingGuard)]).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        let mut response = Vec::new(); let _ = client.read_to_end(&mut response).await;
        assert!(!response.starts_with(b"HTTP/1.1 101"));
        drop(client); drop(worker); proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
