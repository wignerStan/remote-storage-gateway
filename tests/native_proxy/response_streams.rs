//! Explicit native Object approval for complete SSE and upstream WS messages.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};

fn artifact(
    dir: &Path,
    codec: &str,
    request_kind: &str,
    response_kind: &str,
    payload: &str,
    deny: bool,
) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join("response-stream.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(format!("-DTEST_RESPONSE_REPLY={}", u8::from(deny)))
            .arg(format!(
                "-DTEST_RESPONSE_REQUEST_METHOD=\"{codec}/{request_kind}/\""
            ))
            .arg(format!(
                "-DTEST_RESPONSE_METHOD=\"{codec}/{response_kind}/\""
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
fn config(origin: SocketAddr, artifact: &Path, codec: &str, grant: bool) -> serde_json::Value {
    json!({"upstream":format!("http://{origin}"),"callbacks":[{"binding":"direct", "path":artifact,"package":"worker.metadata.test","message_codec":codec,"message_responses":grant,"message_actions":matches!(codec,"openai.responses.v1"|"codex.responses.v1")}]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_delta_bypasses_notifiers_while_complete_terminals_and_errors_require_approval() {
    const HEARTBEAT: &[u8] = b": heartbeat\n\n";
    for (codec, path, request_kind, payload, kind) in [
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            r#"{"choices":[{"delta":{"content":"hello"}}],"future":7}"#,
            "response.event",
        ),
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            r#"{"type":"error","message":"overloaded","unknown":true}"#,
            "response.error",
        ),
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            // A Responses event on a Chat stream remains opaque Chat data.
            r#"{"type":"response.completed","response":{"id":"complete-1","output":[]}}"#,
            "response.event",
        ),
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            "[DONE]",
            "response.done",
        ),
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            r#"{"choices":[{"finish_reason":"stop"}]}"#,
            "response.metadata",
        ),
        (
            "openai.chat_completions.v1",
            "/v1/chat/completions",
            "chat_completion",
            r#"{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}"#,
            "response.metadata",
        ),
        (
            "anthropic.messages.v1",
            "/v1/messages",
            "message",
            r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}"#,
            "response.event",
        ),
        (
            "anthropic.messages.v1",
            "/v1/messages",
            "message",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
            "response.metadata",
        ),
        (
            "anthropic.messages.v1",
            "/v1/messages",
            "message",
            r#"{"type":"message_stop"}"#,
            "response.done",
        ),
        (
            "google.gemini.v1",
            "/v1/models/test:streamGenerateContent?alt=sse",
            "stream_generate_content",
            r#"{"candidates":[{"content":{"parts":[{"text":"hello"}]}}]}"#,
            "response.event",
        ),
        (
            "google.gemini.v1",
            "/v1/models/test:streamGenerateContent?alt=sse",
            "stream_generate_content",
            r#"{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":2}}"#,
            "response.metadata",
        ),
    ] {
        for (grant, deny) in [(true, false), (true, true), (false, true)] {
            timeout(WAIT, async {
                let event = format!("data: {payload}\n\n").into_bytes();
                let gate = Arc::new(tokio::sync::Semaphore::new(0));
                let release = gate.clone();
                let origin_event = event.clone();
                let origin = listen(move |mut socket| {
                    let gate = gate.clone();
                    let event = origin_event.clone();
                    async move {
                        head(&mut socket).await;
                        let mut request = [0;2]; socket.read_exact(&mut request).await.unwrap();
                        assert_eq!(&request,b"{}");
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",HEARTBEAT.len()+event.len()).as_bytes()).await.unwrap();
                        socket.write_all(HEARTBEAT).await.unwrap();
                        socket.write_all(&event[..12]).await.unwrap();
                        gate.acquire().await.unwrap().forget();
                        let _ = socket.write_all(&event[12..]).await;
                    }
                }).await;
                let dir=tempfile::tempdir().unwrap();
                let library=artifact(dir.path(),codec,request_kind,kind,payload,deny);
                let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(config(origin.address,&library,codec,grant)).unwrap()).await.unwrap();
                let worker=serve_engine(proxy.clone()).await;
                let mut client=TcpStream::connect(worker.address).await.unwrap();
                client.write_all(format!("POST {path} HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
                assert!(head(&mut client).await.starts_with(b"HTTP/1.1 200"));
                let mut heartbeat=vec![0;HEARTBEAT.len()]; client.read_exact(&mut heartbeat).await.unwrap();
                assert_eq!(heartbeat,HEARTBEAT,"standalone heartbeat waited for data approval");
                let mut byte=[0];
                let first=timeout(Duration::from_millis(40),client.read(&mut byte)).await;
                let mut body=heartbeat;
                if grant { assert!(first.is_err(),"partial SSE event escaped inspection"); }
                else { assert_eq!(first.unwrap().unwrap(),1);body.push(byte[0]); }
                release.add_permits(1);
                let _=client.read_to_end(&mut body).await;
                let expected=if grant && deny && kind != "response.event" { HEARTBEAT.to_vec() } else { [HEARTBEAT,event.as_slice()].concat() };
                assert_eq!(body,expected,"grant={grant} deny={deny}");
                drop(client);drop(worker);proxy.shutdown().await.unwrap();
            }).await.unwrap();
        }
    }
}

fn frame(payload: &[u8], masked: bool, binary: bool) -> Vec<u8> {
    assert!(payload.len() < 126);
    let mut bytes = vec![
        if binary { 0x82 } else { 0x81 },
        payload.len() as u8 | if masked { 0x80 } else { 0 },
    ];
    if masked {
        bytes.extend_from_slice(&[1, 2, 3, 4]);
        bytes.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, byte)| byte ^ [1, 2, 3, 4][i % 4]),
        );
    } else {
        bytes.extend_from_slice(payload);
    }
    bytes
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_websocket_messages_gate_independently_and_client_wire_stays_masked() {
    for (payload, binary, kind) in [
        (
            r#"{"type":"response.output_text.delta","delta":"hello"}"#,
            false,
            "response.output_text.delta",
        ),
        ("audio", true, "response.binary"),
    ] {
        for (grant, deny) in [(true, false), (true, true), (false, true)] {
            timeout(WAIT,async {
                let client_frame=frame(br#"{"type":"response.create","input":[]}"#,true,false);
                let server_frame=frame(payload.as_bytes(),false,binary);
                let expected_upload=client_frame.clone();let expected_download=server_frame.clone();
                let gate=Arc::new(tokio::sync::Semaphore::new(0));let release=gate.clone();
                let origin=listen(move|mut socket|{
                    let gate=gate.clone();let upload=expected_upload.clone();let download=expected_download.clone();
                    async move {
                        let request=String::from_utf8(head(&mut socket).await).unwrap();
                        let key=request.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("sec-websocket-key")).map(|(_,value)|value.trim())).unwrap();
                        let accept=proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
                        socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
                        let mut actual=vec![0;upload.len()];socket.read_exact(&mut actual).await.unwrap();assert_eq!(actual,upload);
                        socket.write_all(&download[..3]).await.unwrap();
                        gate.acquire().await.unwrap().forget();socket.write_all(&download[3..]).await.unwrap();
                        std::future::pending::<()>().await;
                    }
                }).await;
                let dir=tempfile::tempdir().unwrap();let library=artifact(dir.path(),"openai.responses.v1","response.create",kind,payload,deny);
                let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(config(origin.address,&library,"openai.responses.v1",grant)).unwrap()).await.unwrap();
                let worker=serve_engine(proxy.clone()).await;let mut client=TcpStream::connect(worker.address).await.unwrap();
                client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
                assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));client.write_all(&client_frame).await.unwrap();
                let mut byte=[0];let first=timeout(Duration::from_millis(40),client.read(&mut byte)).await;let mut body=Vec::new();
                assert!(first.is_err(),"partial upstream WS frame escaped response/reply framing guard");
                release.add_permits(1);
                let expected=if grant&&deny && kind != "response.event" && !kind.ends_with(".delta") {vec![0x88,2,3,243]}else{server_frame};
                body.resize(expected.len(),0);client.read_exact(&mut body).await.unwrap();
                assert_eq!(body,expected,"grant={grant} deny={deny} binary={binary}");
                if !(grant&&deny && kind != "response.event" && !kind.ends_with(".delta")) {
                    assert!(timeout(Duration::from_millis(40),client.read_u8()).await.is_err(),"unexpected extra data on open upstream");
                }
                drop(client);drop(worker);proxy.shutdown().await.unwrap();
            }).await.unwrap();
        }
    }
}
