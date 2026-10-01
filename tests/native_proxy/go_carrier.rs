//! Real Go shared-object deployment through the worker's isolated action entry.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires GO_CODEC_ARTIFACT built from infra/go/ai/llmprotocols/exampleplugin"]
async fn go_object_webrtc_signaling_is_explicitly_published_and_revoked() {
    timeout(Duration::from_secs(30), async {
        let artifact = std::env::var("GO_CODEC_ARTIFACT").expect("build Go c-shared codec first");
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let management = reserved.local_addr().unwrap();
        drop(reserved);
        let methods = ["webrtc.open", "webrtc.offer_upstream", "webrtc.close"];
        let actions = methods.map(|method| json!({
            "method": method,
            "contract": {"id":method,"version":1,"response":"unary","input":{"type":"json"},"output":{"type":"json"},"max_input":65536,"max_output":65536},
            "bindings":[{"transport":"http","method":"POST","path":format!("/{method}")}]
        }));
        let cfg = json!({"native_helper":env!("CARGO_BIN_EXE_pingora-worker"),"upstream":"http://127.0.0.1:9",
            "management":{"listen":management},
            "callbacks":[{"path":artifact,"package":"example.go.llmcodec","language":"go","timeout_ms":10000,"actions":actions}]});
        let proxy = pingora_worker::assembly::assemble(serde_json::from_value(cfg).unwrap()).await.unwrap();
        for method in methods {
            let mut socket = TcpStream::connect(management).await.unwrap();
            socket.write_all(format!("POST /{method} HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
            let mut reply = Vec::new(); socket.read_to_end(&mut reply).await.unwrap();
            assert!(reply.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&reply));
            if method == "webrtc.offer_upstream" {
                let response = String::from_utf8_lossy(&reply);
                assert!(response.contains("m=audio") && response.contains("m=application"), "{response}");
            }
        }
        let mut socket = TcpStream::connect(management).await.unwrap();
        socket.write_all(b"POST /webrtc.accept_upstream_answer HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        assert!(head(&mut socket).await.starts_with(b"HTTP/1.1 404"));
        drop(socket);
        proxy.shutdown().await.unwrap();
        assert!(TcpStream::connect(management).await.is_err());
    }).await.unwrap();
}
