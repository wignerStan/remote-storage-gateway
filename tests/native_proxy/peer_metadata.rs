//! Actual socket identity crosses the native ABI independently of forwarding headers.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_receives_socket_peer_despite_spoofed_forwarding_headers() {
    timeout(WAIT, async {
        let origin = listen(|mut socket| async move {
            head(&mut socket).await;
            socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        }).await;
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("peer.so");
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert!(Command::new("cc").args(["-shared", "-fPIC", "-std=c11", "-DTEST_REQUIRE_CLIENT_IP", "-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(root.join("tests/fixtures/metadata_callback.c"))
            .arg("-o").arg(&artifact).status().unwrap().success());
        let config = serde_json::from_value(json!({
            "upstream": format!("http://{}", origin.address),
            "callbacks": [{"path": artifact, "package": "worker.metadata.test",
                "hooks": ["before_upstream_send"], "headers": ["forwarded", "x-forwarded-for"],
                "config": {"version": 1, "decision": "continue"}}]
        })).unwrap();
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /resource HTTP/1.1\r\nHost: downstream\r\nForwarded: for=198.51.100.8\r\nX-Forwarded-For: 203.0.113.9\r\nConnection: close\r\n\r\n").await.unwrap();
        let response = head(&mut client).await;
        assert!(response.starts_with(b"HTTP/1.1 204"), "native callback rejected socket peer: {}", String::from_utf8_lossy(&response));
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
