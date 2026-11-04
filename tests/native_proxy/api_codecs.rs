//! Native Object callback selection and full HTTP body gating for API codecs.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};
const BODY: &[u8] = br#"{"messages":[],"unknown":"preserved"}"#;
const GEMINI_BODY: &[u8] = br#"{"contents":[{"parts":[{"text":"hello"}]}],"unknown":"preserved"}"#;
const GEMINI_SSE: &[u8] =
    b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}]}}]}\n\n";
const ROUTES: [(&str, &str); 5] = [
    ("openai.chat_completions.v1", "/v1/chat/completions"),
    ("openai.responses.v1", "/v1/responses"),
    ("anthropic.messages.v1", "/v1/messages"),
    ("codex.responses.v1", "/backend-api/codex/responses"),
    (
        "google.gemini.v1",
        "/v1beta/models/gemini-test:generateContent",
    ),
];
fn artifact(dir: &Path, id: &str, action: &str) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join(format!("{id}.so"));
    let source = std::fs::read_to_string(root.join("tests/fixtures/metadata_callback.c")).unwrap();
    let fixture = dir.join(format!("{id}.c"));
    std::fs::write(&fixture, source).unwrap();
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(if action == "responses" {
                "-DTEST_RESPONSES_REPLY=0".to_owned()
            } else {
                format!("-DTEST_API_REPLY={action}")
            })
            .arg(format!("-DTEST_API_PREFIX=\"{id}/\""))
            .arg(format!(
                "-DTEST_GEMINI_BODY={}",
                u8::from(id == "google.gemini.v1")
            ))
            .arg(fixture)
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_routes_gate_http_bodies_and_refuse_ungranted_responses() {
    timeout(WAIT,async{
        let received=Arc::new(AtomicUsize::new(0));
        let connections=Arc::new(AtomicUsize::new(0));
        let accepted=connections.clone();
        let observed=received.clone();
        let origin=listen(move|mut socket|{let observed=observed.clone();let accepted=accepted.clone();async move{
            accepted.fetch_add(1,Ordering::SeqCst);
            let request=head(&mut socket).await;
            assert!(request.starts_with(b"POST "));
            let head_text=String::from_utf8(request).unwrap();
            let compressed=head_text.to_ascii_lowercase().contains("content-encoding: zstd");
            let payload=if head_text.contains("/models/"){GEMINI_BODY}else{BODY};
            let expected=if compressed{zstd::stream::encode_all(payload,3).unwrap()}else{payload.to_vec()};
            let mut body=vec![0;expected.len()];
            for byte in &mut body { *byte=socket.read_u8().await.unwrap(); observed.fetch_add(1,Ordering::SeqCst); }
            assert_eq!(body,expected);
            // Upstream event bytes remain untouched; no event-to-chat projection.
            let reply=if head_text.contains("/models/"){GEMINI_SSE}else{b"data: [DONE]\n\n"};
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",reply.len()).as_bytes()).await.unwrap();
            socket.write_all(reply).await.unwrap();
        }}).await;
        let dir=tempfile::tempdir().unwrap();
        let callbacks=ROUTES.iter().map(|(id,_)|json!({"binding":"direct", "path":artifact(dir.path(),id,"0"),"package":"worker.metadata.test","message_codec":id})).collect::<Vec<_>>();
        let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":callbacks})).unwrap()).await.unwrap();
        let worker=serve_engine(proxy.clone()).await;
        let mut previous_bytes=0;
        let mut previous_connections=0;
        for (i,(id,path)) in ROUTES.iter().chain([("google.gemini.v1", "/v1/models/gemini-test:streamGenerateContent?alt=sse")].iter()).enumerate(){
            let payload=if *id=="google.gemini.v1"{GEMINI_BODY}else{BODY};
            let wire=if i==0{zstd::stream::encode_all(payload,3).unwrap()}else{payload.to_vec()};
            let coding=if i==0{"Content-Encoding: zstd\r\n"}else{""};
            let mut client=TcpStream::connect(worker.address).await.unwrap();
            let marker=if *path=="/v1/responses"{"x-openai-internal-codex-responses-lite: true\r\n"}else{""};
            client.write_all(format!("POST {path} HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{coding}{marker}\r\n",wire.len()).as_bytes()).await.unwrap();
            if matches!(*id,"openai.responses.v1"|"codex.responses.v1") {
                client.write_all(&wire).await.unwrap();
                let mut reply=Vec::new();let _=client.read_to_end(&mut reply).await;
                assert!(reply.starts_with(b"HTTP/1.1 403"),"ungranted Responses must not use native HTTP: {}",String::from_utf8_lossy(&reply));
                assert_eq!(received.load(Ordering::SeqCst),previous_bytes);
                assert_eq!(connections.load(Ordering::SeqCst),previous_connections,"rejected Responses opened upstream TCP");
                continue;
            }
            client.write_all(&wire[..8]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(received.load(Ordering::SeqCst),previous_bytes,"partial request escaped codec gate");
            client.write_all(&wire[8..]).await.unwrap();
            let mut reply=Vec::new();client.read_to_end(&mut reply).await.unwrap();
            assert!(reply.starts_with(b"HTTP/1.1 200"),"{}",String::from_utf8_lossy(&reply));
            assert!(reply.ends_with(if *id=="google.gemini.v1"{GEMINI_SSE}else{b"data: [DONE]\n\n"}));
            previous_bytes+=wire.len();
            previous_connections+=1;
        }
        drop(worker);proxy.shutdown().await.unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn http_message_denial_forwards_no_body_bytes() {
    timeout(WAIT,async{
        let received=Arc::new(AtomicUsize::new(0));let observed=received.clone();
        let origin=listen(move|mut socket|{let observed=observed.clone();async move{
            head(&mut socket).await;
            let mut byte=[0];if socket.read(&mut byte).await.unwrap_or(0)>0{observed.fetch_add(1,Ordering::SeqCst);}
        }}).await;
        let dir=tempfile::tempdir().unwrap();let id=ROUTES[0].0;
        let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":artifact(dir.path(),id,"1"),"package":"worker.metadata.test","message_codec":id}]})).unwrap()).await.unwrap();
        let worker=serve_engine(proxy.clone()).await;let mut client=TcpStream::connect(worker.address).await.unwrap();
        client.write_all(format!("POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",BODY.len()).as_bytes()).await.unwrap();client.write_all(BODY).await.unwrap();
        let mut response=Vec::new();let _=client.read_to_end(&mut response).await;
        assert!(!response.starts_with(b"HTTP/1.1 200"));assert_eq!(received.load(Ordering::SeqCst),0);
        drop(client);drop(worker);proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn early_upstream_success_cannot_bypass_http_message_inspection() {
    for status in [200, 413] {
        timeout(WAIT,async{
            let origin=listen(move|mut socket|async move{
                // Respond as soon as connected, before reading the held request body.
                socket.write_all(format!("HTTP/1.1 {status} Early\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            }).await;
            let dir=tempfile::tempdir().unwrap();let id=ROUTES[0].0;
            let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":artifact(dir.path(),id,"0"),"package":"worker.metadata.test","message_codec":id}]})).unwrap()).await.unwrap();
            let worker=serve_engine(proxy.clone()).await;let mut client=TcpStream::connect(worker.address).await.unwrap();
            client.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{").await.unwrap();
            let response=head(&mut client).await;
            let expected=if status==200{502}else{413};
            assert!(response.starts_with(format!("HTTP/1.1 {expected}").as_bytes()),"{}",String::from_utf8_lossy(&response));
            drop(client);drop(worker);proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_ws_selects_lite_per_message_and_keeps_upstream_delta_opaque() {
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x81];
        if payload.len() < 126 {
            out.push(0x80 | payload.len() as u8);
        } else {
            out.push(0xfe);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        out.extend_from_slice(&[1, 2, 3, 4]);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
        );
        out
    }
    timeout(WAIT, async {
        let frames = Arc::new(vec![
            // This case selects normal creation, independently of prewarm classification.
            frame(br#"{"type":"response.create","input":[],"generate":true}"#),
            frame(br#"{"type":"response.create","previous_response_id":"r1","input":[],"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}"#),
            frame(br#"{"type":"response.create","input":[]}"#),
            frame(br#"{"type":"response.append","input":[{"type":"function_call_output","output":"ok"}]}"#),
        ]);
        let upstream_frames = frames.clone();
        let (verified, mut origin_frames) = tokio::sync::mpsc::unbounded_channel();
        let origin = listen(move |mut socket| {
            let frames = upstream_frames.clone();
            let verified = verified.clone();
            async move {
                let request = String::from_utf8(head(&mut socket).await).unwrap();
                assert!(!request.to_ascii_lowercase().contains("sec-websocket-extensions"));
                accept_upstream(&mut socket, &request).await;
                for (index, expected) in frames.iter().enumerate() {
                    let mut actual=vec![0;expected.len()];socket.read_exact(&mut actual).await.unwrap_or_else(|error| panic!("origin frame {index}: {error}"));
                    assert_eq!(&actual,expected,"origin frame {index}");
                    // The request classifier must never see/deny upstream binary deltas.
                    socket.write_all(&[0x82,3,0,255,7]).await.unwrap();
                    verified.send(index).unwrap();
                }
            }
        }).await;
        let dir=tempfile::tempdir().unwrap();let id="openai.responses.v1";
        let proxy=network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({"upstream":format!("http://{}",origin.address),"callbacks":[{"binding":"direct", "path":artifact(dir.path(),id,"responses"),"package":"worker.metadata.test","message_codec":id,"message_actions":true}]})).unwrap()).await.unwrap();
        let worker=serve_engine(proxy.clone()).await;let mut client=TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n").await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        for (index, frame) in frames.iter().enumerate() {
            client.write_all(frame).await.unwrap();
            let mut delta=[0;5];
            client.read_exact(&mut delta).await.unwrap_or_else(|error| panic!("downstream delta {index}: {error}"));
            assert_eq!(delta,[0x82,3,0,255,7]);
            assert_eq!(origin_frames.recv().await, Some(index));
        }
        drop(client);drop(worker);proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

const REPLAY_DOCUMENT: &[u8] = br#"{"type":"error","status":409,"error":{"message":"Previous response was not found. Retrying the full request.","type":"invalid_request_error","code":"previous_response_not_found","param":"previous_response_id"}}"#;

// SDK BuildOpenAIResponsesStreamErrorChunk emits null param and preserves provider codes.
const SDK_ERROR: &[u8] = br#"{"type":"error","error":{"type":"server_error","code":"request_timeout","message":"stream interrupted","param":null},"sequence_number":0}"#;
const SDK_PROVIDER_ERROR: &[u8] = br#"{"type":"error","error":{"code":429,"message":"busy","param":null,"status":"RESOURCE_EXHAUSTED","details":[]},"sequence_number":0}"#;

fn responses_plugin(dir: &Path, mode: &str) -> std::path::PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = dir.join(format!("codex.responses.v1.{mode}.so"));
    let define = match mode {
        "error" | "sdk_error" | "sdk_provider_error" => "-DTEST_RESPONSES_ERROR=1",
        "bad" => "-DTEST_RESPONSES_BAD=1",
        "terminal" => "-DTEST_RESPONSES_TERMINAL=1",
        "steering" => "-DTEST_STEERING_REPLY=1",
        _ => panic!("unknown responses plugin mode"),
    };
    let mut command = Command::new("cc");
    command
        .args(["-shared", "-fPIC", "-std=c11", "-I"])
        .arg(root.join("../../infra/rust/crates/interop/plugin-abi/include"))
        .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
        .arg(define)
        .arg("-DTEST_API_PREFIX=\"codex.responses.v1/\"");
    let document = match mode {
        "sdk_error" => Some(SDK_ERROR),
        "sdk_provider_error" => Some(SDK_PROVIDER_ERROR),
        _ => None,
    };
    if let Some(document) = document {
        command.arg(format!(
            "-DTEST_RESPONSES_ERROR_DOCUMENT={}",
            serde_json::to_string(std::str::from_utf8(document).unwrap()).unwrap()
        ));
    }
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/metadata_callback.c")).unwrap();
    if matches!(mode, "error" | "sdk_error" | "sdk_provider_error") {
        let start = source.find("#ifdef TEST_RESPONSES_ERROR\n").unwrap();
        let end = source[start..]
            .find("#ifdef TEST_RESPONSES_TERMINAL\n")
            .unwrap()
            + start;
        let section = source[start..end].replace("'0' + sequence", "'1' + sequence").replace(
            "        static size_t sequence;",
            "        static size_t sequence;\n        const char *prime = \"{\\\"type\\\":\\\"response.create\\\",\\\"input\\\":[]}\";\n        if (input.len == strlen(prime) && !memcmp(input.ptr, prime, input.len)) { const uint8_t allow[] = {0}; return copy_reply(allow,sizeof(allow),out); }");
        source.replace_range(start..end, &section);
    }
    let fixture = dir.join("responses-fixture.c");
    std::fs::write(&fixture, source).unwrap();
    assert!(
        command
            .arg(fixture)
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}

fn server_text(payload: &[u8]) -> Vec<u8> {
    let mut wire = vec![0x81];
    if payload.len() < 126 {
        wire.push(payload.len() as u8);
    } else {
        wire.push(126);
        wire.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    wire.extend_from_slice(payload);
    wire
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_error_document_is_written_and_the_socket_stays_open() {
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x81];
        out.push(0x80 | payload.len() as u8);
        out.extend_from_slice(&[9, 8, 7, 6]);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ [9, 8, 7, 6][i % 4]),
        );
        out
    }
    for (mode, document) in [
        ("error", REPLAY_DOCUMENT),
        ("sdk_error", SDK_ERROR),
        ("sdk_provider_error", SDK_PROVIDER_ERROR),
    ] {
        timeout(WAIT, async {
        let prime = frame(br#"{"type":"response.create","input":[]}"#);
        let continued = frame(br#"{"type":"response.create","previous_response_id":"resp_old","input":[]}"#);
        let replay = frame(br#"{"type":"response.create","input":[{"type":"message","role":"user","content":"continue the patch"}]}"#);
        let prime_wire = prime.clone();
        let continued_wire = continued.clone();
        let replay_wire = replay.clone();
        let origin = listen(move |mut socket| {
            let prime_wire = prime_wire.clone();
            let continued_wire = continued_wire.clone();
            let replay_wire = replay_wire.clone();
            async move {
                let request = String::from_utf8(head(&mut socket).await).unwrap();
                assert!(request.starts_with("GET /backend-api/codex/responses "));
                accept_upstream(&mut socket, &request).await;
                let mut initial = vec![0; prime_wire.len()];
                socket.read_exact(&mut initial).await.unwrap();
                assert_eq!(initial, prime_wire);
                // Leave one upstream frame incomplete while the client receives
                // its local structured error. Injection must not split this frame.
                socket.write_all(&[0x82]).await.unwrap();
                let mut actual = vec![0; replay_wire.len()];
                socket.read_exact(&mut actual).await.unwrap();
                assert_ne!(actual, continued_wire, "previous_response frame reached upstream");
                assert_eq!(actual, replay_wire);
                socket.write_all(&[1, 7]).await.unwrap();
            }
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({
            "upstream": format!("http://{}", origin.address),
            "callbacks": [{
                "binding":"direct", "path": responses_plugin(dir.path(), mode),
                "package": "worker.metadata.test",
                "message_codec": "codex.responses.v1", "message_actions": true
            }]
        })).unwrap()).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /backend-api/codex/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client.write_all(&prime).await.unwrap();
        assert!(timeout(Duration::from_millis(30), client.read_u8()).await.is_err(), "partial upstream frame escaped reply guard");
        client.write_all(&continued).await.unwrap();
        let expected = server_text(document);
        let mut error_frame = vec![0; expected.len()];
        client.read_exact(&mut error_frame).await.unwrap();
        assert_eq!(error_frame, expected);
        client.write_all(&replay).await.unwrap();
        let mut delta = [0; 3];
        client.read_exact(&mut delta).await.unwrap();
        assert_eq!(delta, [0x82, 1, 7]);
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    }).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_non_error_reply_forwards_nothing() {
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x81, 0x80 | payload.len() as u8];
        out.extend_from_slice(&[1, 2, 3, 4]);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
        );
        out
    }
    timeout(WAIT, async {
        let connections = Arc::new(AtomicUsize::new(0));
        let observed = connections.clone();
        let origin = listen(move |mut socket| {
            let observed = observed.clone();
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                let request = String::from_utf8(head(&mut socket).await).unwrap();
                accept_upstream(&mut socket, &request).await;
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
            }
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({
            "upstream": format!("http://{}", origin.address),
            "callbacks": [{
                "binding":"direct", "path": responses_plugin(dir.path(), "bad"),
                "package": "worker.metadata.test",
                "message_codec": "codex.responses.v1", "message_actions": true
            }]
        })).unwrap()).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /backend-api/codex/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client.write_all(&frame(br#"{"type":"response.create","input":[]}"#)).await.unwrap();
        let mut rest = Vec::new();
        let _ = client.read_to_end(&mut rest).await;
        assert_eq!(rest, [0x88, 2, 3, 243], "only a protocol Close may follow a rejected reply");
        assert_eq!(connections.load(Ordering::SeqCst), 0, "invalid reply opened upstream TCP");
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_terminal_frame_can_replace_a_completion_without_gating_deltas() {
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x81, 0x80 | payload.len() as u8];
        out.extend_from_slice(&[1, 2, 3, 4]);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
        );
        out
    }
    timeout(WAIT, async {
        let create = frame(br#"{"type":"response.create","input":[]}"#);
        let again = frame(br#"{"type":"response.create","input":[{"type":"message"}]}"#);
        let delta_payload = br#"{"type":"response.output_text.delta","delta":"hello"}"#;
        let zero_payload = br#"{"type":"response.completed","response":{"usage":{"input_tokens_details":{"cached_tokens":0}}}}"#;
        let warm_payload = br#"{"type":"response.completed","response":{"usage":{"input_tokens_details":{"cached_tokens":12}}}}"#;
        let delta_frame = server_text(delta_payload);
        let warm_frame = server_text(warm_payload);
        let error_frame = server_text(REPLAY_DOCUMENT);
        let create_wire = create.clone();
        let again_wire = again.clone();
        let delta_up = delta_frame.clone();
        let zero_up = server_text(zero_payload);
        let warm_up = warm_frame.clone();
        let origin = listen(move |mut socket| {
            let create_wire = create_wire.clone();
            let again_wire = again_wire.clone();
            let delta_up = delta_up.clone();
            let zero_up = zero_up.clone();
            let warm_up = warm_up.clone();
            async move {
                let request = String::from_utf8(head(&mut socket).await).unwrap();
                accept_upstream(&mut socket, &request).await;
                let mut actual = vec![0; create_wire.len()];
                socket.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, create_wire);
                socket.write_all(&delta_up).await.unwrap();
                socket.write_all(&zero_up).await.unwrap();
                let mut actual = vec![0; again_wire.len()];
                socket.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, again_wire);
                socket.write_all(&warm_up).await.unwrap();
            }
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({
            "upstream": format!("http://{}", origin.address),
            "callbacks": [{
                "binding":"direct", "path": responses_plugin(dir.path(), "terminal"),
                "package": "worker.metadata.test",
                "message_codec": "codex.responses.v1", "message_actions": true
            }]
        })).unwrap()).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"GET /backend-api/codex/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        client.write_all(&create).await.unwrap();
        let mut got = vec![0; delta_frame.len()];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(got, delta_frame, "delta was gated or rewritten");
        let mut got = vec![0; error_frame.len()];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(got, error_frame);
        client.write_all(&again).await.unwrap();
        let mut got = vec![0; warm_frame.len()];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(got, warm_frame, "nonzero completion was replaced");
        drop(client);
        drop(worker);
        proxy.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_0162_interrupt_relays_during_response_and_preserves_interrupted_boundary() {
    const INTERRUPT: &[u8] =
        br#"{"type":"response.interrupt","response_id":"resp_1","mode":"discard_partial_items"}"#;
    const CREATED: &[u8] = br#"{"type":"response.created","response":{"id":"resp_1"}}"#;
    const INCOMPLETE: &[u8] = br#"{"type":"response.incomplete","response":{"id":"resp_1","status":"incomplete","incomplete_details":{"reason":"interrupted"},"error":null}}"#;
    const COMPLETED: &[u8] =
        br#"{"type":"response.completed","response":{"id":"resp_2","output":[]}}"#;
    for response_grant in [false, true] {
        timeout(WAIT, async {
            let frame = |payload: &[u8]| proxy_protocols::websocket::encode_client(1, payload).unwrap();
            let first = frame(br#"{"type":"response.create","input":[]}"#);
            let interrupt = frame(INTERRUPT);
            let followup = frame(br#"{"type":"response.create","previous_response_id":"resp_1","input":[{"role":"user","content":"steer"}]}"#);
            let origin_first = first.clone();
            let origin_interrupt = interrupt.clone();
            let origin_followup = followup.clone();
            let origin = listen(move |mut socket| {
                let first = origin_first.clone();
                let interrupt = origin_interrupt.clone();
                let followup = origin_followup.clone();
                async move {
                    let request = String::from_utf8(head(&mut socket).await).unwrap();
                    let key = request.lines().find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                            .map(|(_, value)| value.trim())
                    }).unwrap();
                    // Controlled sessions use a fresh upstream nonce.
                    let accept = proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
                    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
                    for (request, response) in [(first, CREATED), (interrupt, INCOMPLETE), (followup, COMPLETED)] {
                        let mut actual = vec![0; request.len()];
                        socket.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, request, "worker changed the active response control or continuation");
                        socket.write_all(&server_text(response)).await.unwrap();
                    }
                }
            }).await;
            let dir = tempfile::tempdir().unwrap();
            let proxy = network_data_plane_worker::assembly::assemble(serde_json::from_value(json!({
                "upstream":format!("http://{}",origin.address),
                "callbacks":[{"binding":"direct", "path":responses_plugin(dir.path(),"steering"),"package":"worker.metadata.test",
                    "message_codec":"codex.responses.v1","message_actions":true,"message_responses":response_grant}]
            })).unwrap()).await.unwrap();
            let worker = serve_engine(proxy.clone()).await;
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(b"GET /backend-api/codex/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            for (request, response) in [(first, CREATED), (interrupt, INCOMPLETE), (followup, COMPLETED)] {
                client.write_all(&request).await.unwrap();
                let expected = server_text(response);
                let mut actual = vec![0; expected.len()];
                client.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, expected);
            }
            drop(client);
            drop(worker);
            proxy.shutdown().await.unwrap();
        }).await.unwrap();
    }
}

async fn accept_upstream(socket: &mut TcpStream, request: &str) {
    let key = request
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, value)| value.trim())
        })
        .unwrap();
    let accept =
        proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
}
