//! The assembled native Object path receives complete decoded messages only.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};
const REQUEST: &[u8] = b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
const RESPONSE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
// Two fragments; neither may enter native code or upstream before the full message.
const FIRST: &[u8] = &[0x01, 0x81, 1, 2, 3, 4, b'h' ^ 1];
const LAST: &[u8] = &[0x80, 0x81, 4, 3, 2, 1, b'i' ^ 4];
fn artifact(directory: &Path, action: &str) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = directory.join("websocket.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(format!("-DTEST_WEBSOCKET_REPLY={action}"))
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
async fn assembled_object_checks_complete_messages_and_forwards_original_wire() {
    for action in ["0", "1", "0,0"] {
        timeout(WAIT, async {
            let received = Arc::new(AtomicUsize::new(0));
            let observed = received.clone();
            let origin = listen(move |mut socket| {
                let observed = observed.clone();
                async move {
                    head(&mut socket).await;
                    socket.write_all(RESPONSE).await.unwrap();
                    let mut wire = Vec::new();
                    let mut byte = [0u8];
                    while wire.len() < FIRST.len() + LAST.len() {
                        if socket.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        wire.push(byte[0]);
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                    assert_eq!(wire, [FIRST, LAST].concat());
                    socket.write_all(&[0x81, 2, b'o', b'k']).await.unwrap();
                }
            })
            .await;
            let directory = tempfile::tempdir().unwrap();
            let config = serde_json::from_value(json!({
                "native_helper":env!("CARGO_BIN_EXE_pingora-worker"),"upstream": format!("http://{}", origin.address),
                "callbacks": [{"path": artifact(directory.path(), action),
                    "package":"worker.metadata.test", "hooks":["pre_routing"],
                    "message_codec":"websocket.v1",
                    "config":{"version":1,"decision":"continue"}}]
            }))
            .unwrap();
            let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            let mut preread = REQUEST.to_vec();
            preread.extend_from_slice(FIRST);
            client.write_all(&preread).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(received.load(Ordering::SeqCst), 0);
            client.write_all(LAST).await.unwrap();
            let mut response = Vec::new();
            let _ = client.read_to_end(&mut response).await;
            if action == "0" {
                assert_eq!(response, [0x81, 2, b'o', b'k']);
                assert_eq!(received.load(Ordering::SeqCst), FIRST.len() + LAST.len());
            } else {
                assert!(response.is_empty());
                assert_eq!(received.load(Ordering::SeqCst), 0);
            }
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn configured_codec_rejects_extensions_before_downstream_upgrade() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            head(&mut socket).await;
            let mut response = RESPONSE[..RESPONSE.len() - 2].to_vec();
            response.extend_from_slice(b"Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n");
            socket.write_all(&response).await.unwrap();
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let config = serde_json::from_value(json!({
            "native_helper":env!("CARGO_BIN_EXE_pingora-worker"),"upstream":format!("http://{}",origin.address),
            "callbacks":[{"path":artifact(directory.path(),"0"),
                "package":"worker.metadata.test", "hooks":["pre_routing"],
                "message_codec":"websocket.v1", "config":{"version":1,"decision":"continue"}}]
        }))
        .unwrap();
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(REQUEST).await.unwrap();
        let response = head(&mut client).await;
        assert!(!response.starts_with(b"HTTP/1.1 101"));
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
