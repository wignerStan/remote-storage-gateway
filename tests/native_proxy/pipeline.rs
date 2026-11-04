//! Installed hooks run through the actual Pingora carrier, including lazy bodies.
use super::*;
use network_data_plane_worker::proxy::{ProxyContract, RequestFacts};
use pingora_http::{RequestHeader, ResponseHeader};
use proxy_pingora::worker::surface::{Callbacks, DialDecision, PoolDecision, PoolPhase};
use std::sync::Mutex;
use stream_transfer::chunk::ChunkTransform;

#[derive(Default)]
struct Seen {
    stages: Mutex<Vec<&'static str>>,
    completed: tokio::sync::Notify,
}
impl Seen {
    fn record(&self, stage: &'static str) {
        self.stages.lock().unwrap().push(stage);
    }
}
#[derive(Clone, Copy, Default)]
enum Mode {
    #[default]
    Observe,
    RefuseSend,
    TransformBodies,
    RewriteLength,
    ChangeUpgrade,
    DenyDial,
    ReplaceRequestBeforeSend,
    ReplaceResponseBeforeWrite,
    ReplaceRequestAfterResponse,
    PatchRequestHandshake,
    PatchResponseHandshake,
    PatchResponseConnection,
}
struct Hooks {
    seen: Arc<Seen>,
    mode: Mode,
}
struct SecondIdentity(Arc<Seen>);
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for SecondIdentity {
    fn supports(&self, hook: proxy_pingora::worker::surface::CallbackHook) -> bool {
        hook == proxy_pingora::worker::surface::CallbackHook::PreRouting
    }
    async fn authenticate(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        Ok(None)
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        Ok(None)
    }
    async fn complete(&self, _: &RequestFacts) -> Result<u32, String> {
        unreachable!("metadata participant never admits execution")
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        _: &mut RequestFacts,
    ) -> Result<(), String> {
        assert_eq!(request.headers["x-identity"], "first");
        request.insert_header("x-identity", "second").unwrap();
        self.0.record("transform_second");
        Ok(())
    }
    async fn maintain(&self) {}
}
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for Hooks {
    async fn pre_ingress(
        &self,
        _: u8,
        _: &mut RequestHeader,
        _: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("parsed_validation");
        Ok(())
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        _: &mut RequestFacts,
    ) -> Result<(), String> {
        request.insert_header("x-identity", "first").unwrap();
        self.seen.record("transform_first");
        Ok(())
    }
    async fn authenticate(&self, _: u8, facts: &RequestFacts) -> Result<Option<()>, String> {
        assert_eq!(
            facts.headers["x-identity"], "second",
            "authentication sees all installed transforms"
        );
        self.seen.record("authenticate");
        Ok(Some(()))
    }
    async fn routing(
        &self,
        _: u8,
        _: &mut RequestHeader,
        _: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("routing");
        Ok(())
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        self.seen.record("admit");
        Ok(Some(()))
    }
    async fn prepare_request_body(
        &self,
        _: u8,
        _: &mut RequestHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("request_body_setup");
        if matches!(self.mode, Mode::TransformBodies) {
            facts
                .request_body
                .push(DropFirst(false))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    async fn before_upstream_send(
        &self,
        _: u8,
        request: &mut RequestHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("before_send");
        match self.mode {
            Mode::RefuseSend => return Err("finalization refused".into()),
            Mode::RewriteLength => {
                request.insert_header("content-length", "1").unwrap();
            }
            Mode::ReplaceRequestBeforeSend => facts.request_body = sealed_drop_first(),
            Mode::PatchRequestHandshake => {
                request
                    .insert_header("sec-websocket-key", "replaced")
                    .unwrap();
            }
            _ => {}
        }
        Ok(())
    }
    async fn pool(
        &self,
        _: u8,
        phase: PoolPhase,
        _: &RequestFacts,
    ) -> Result<Option<PoolDecision>, String> {
        assert_eq!(phase, PoolPhase::BeforeCheckout);
        self.seen.record("pool_gate");
        Ok(Some(PoolDecision::Discard))
    }
    async fn before_dial(
        &self,
        _: u8,
        _: &network_sockets::DialRequest,
        _: &RequestFacts,
    ) -> Result<Option<DialDecision>, String> {
        self.seen.record("dial_gate");
        Ok(Some(if matches!(self.mode, Mode::DenyDial) {
            DialDecision::Deny
        } else {
            DialDecision::Allow
        }))
    }
    async fn socket(
        &self,
        _: u8,
        phase: network_sockets::SocketPhase,
        _: &RequestFacts,
    ) -> Result<Option<network_sockets::SocketDecision>, String> {
        assert_eq!(phase, network_sockets::SocketPhase::BeforeUse);
        self.seen.record("socket_gate");
        Ok(Some(network_sockets::SocketDecision::Continue))
    }
    async fn upstream_response(
        &self,
        _: u8,
        response: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("upstream_response");
        if matches!(self.mode, Mode::ChangeUpgrade) {
            response.set_status(101).unwrap();
        }
        if matches!(self.mode, Mode::ReplaceRequestAfterResponse) {
            facts.request_body = sealed_drop_first();
        }
        if matches!(self.mode, Mode::PatchResponseHandshake) {
            response
                .insert_header("sec-websocket-accept", "replaced")
                .unwrap();
        }
        Ok(())
    }
    async fn prepare_response_body(
        &self,
        _: u8,
        _: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("response_body_setup");
        if matches!(self.mode, Mode::TransformBodies) {
            facts
                .response_body
                .push(DropFirst(false))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    async fn before_downstream_write(
        &self,
        _: u8,
        response: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.seen.record("before_write");
        if matches!(self.mode, Mode::ReplaceResponseBeforeWrite) {
            facts.response_body = sealed_drop_first();
        }
        if matches!(self.mode, Mode::PatchResponseConnection) {
            response.insert_header("connection", "close").unwrap();
        }
        Ok(())
    }
    async fn complete(&self, facts: &RequestFacts) -> Result<u32, String> {
        assert!(facts.completed);
        self.seen.record("complete");
        self.seen.completed.notify_one();
        Ok(0)
    }
    async fn maintain(&self) {}
}
fn hooks(seen: &Arc<Seen>, mode: Mode) -> Vec<Arc<dyn Callbacks<ProxyContract>>> {
    vec![
        Arc::new(Hooks {
            seen: seen.clone(),
            mode,
        }),
        Arc::new(SecondIdentity(seen.clone())),
    ]
}

#[tokio::test]
async fn native_pipeline_orders_transforms_auth_network_and_one_final_response() {
    let seen = Arc::new(Seen::default());
    let origin_seen = seen.clone();
    let origin = listen(move |mut socket| {
        origin_seen.record("origin_accept");
        let seen = origin_seen.clone();
        async move {
            let request = String::from_utf8(head(&mut socket).await).unwrap();
            assert!(request.to_ascii_lowercase().contains("x-identity: second"));
            seen.record("origin_request");
            socket.write_all(b"HTTP/1.1 103 Early Hints\r\nLink: </asset>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").await.unwrap();
        }
    }).await;
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::Observe),
    )
    .await;
    let response = get(worker.address).await;
    assert!(response.starts_with(b"HTTP/1.1 103"));
    assert!(String::from_utf8_lossy(&response).contains("HTTP/1.1 200"));
    assert!(response.ends_with(b"hello"));
    timeout(WAIT, seen.completed.notified()).await.unwrap();
    let stages = seen.stages.lock().unwrap();
    let position = |stage| stages.iter().position(|actual| *actual == stage).unwrap();
    assert!(position("dial_gate") < position("origin_accept"));
    eprintln!(
        "DECISION native pipeline completed\nWHY transformed identity was authenticated before gated acquisition; final response was delivered\nFACTS stages={stages:?}"
    );
    assert_eq!(
        stages
            .iter()
            .copied()
            .filter(|stage| *stage != "origin_accept")
            .collect::<Vec<_>>(),
        [
            "parsed_validation",
            "transform_first",
            "transform_second",
            "authenticate",
            "routing",
            "admit",
            "request_body_setup",
            "before_send",
            "pool_gate",
            "dial_gate",
            "socket_gate",
            "origin_request",
            "upstream_response",
            "response_body_setup",
            "before_write",
            "complete",
        ]
    );
}

#[tokio::test]
async fn refusing_finalization_never_acquires_or_connects_origin() {
    let count = Arc::new(AtomicUsize::new(0));
    let origin = origin(count.clone()).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::RefuseSend),
    )
    .await;
    assert!(!get(worker.address).await.starts_with(b"HTTP/1.1 200"));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        *seen.stages.lock().unwrap(),
        [
            "parsed_validation",
            "transform_first",
            "transform_second",
            "authenticate",
            "routing",
            "admit",
            "request_body_setup",
            "before_send"
        ]
    );
}

struct DropFirst(bool);
impl ChunkTransform for DropFirst {
    fn transform(&mut self, chunk: &mut Option<bytes::Bytes>, _: bool) -> std::io::Result<()> {
        if !self.0
            && let Some(bytes) = chunk
            && !bytes.is_empty()
        {
            *bytes = bytes.slice(1..);
            self.0 = true;
        }
        Ok(())
    }
}
fn sealed_drop_first() -> stream_transfer::chunk::ChunkChain {
    let mut chain = stream_transfer::chunk::ChunkChain::default();
    chain.push(DropFirst(false)).unwrap();
    assert!(!chain.seal().unwrap());
    chain
}
pub(super) async fn chunk(socket: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    while !line.ends_with(b"\r\n") {
        line.push(socket.read_u8().await.unwrap());
    }
    let line = std::str::from_utf8(&line).unwrap().trim();
    let length = usize::from_str_radix(line.split(';').next().unwrap(), 16).unwrap();
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.unwrap();
    let mut ending = [0; 2];
    socket.read_exact(&mut ending).await.unwrap();
    assert_eq!(&ending, b"\r\n");
    bytes
}

#[tokio::test]
async fn variable_body_transforms_stream_before_eof_with_original_ingress_length() {
    assert_variable_body_streaming("1.1").await;
}

#[tokio::test]
async fn http10_variable_bodies_upgrade_upload_and_close_delimit_response() {
    assert_variable_body_streaming("1.0").await;
}

async fn assert_variable_body_streaming(version: &str) {
    let upload_seen = Arc::new(tokio::sync::Notify::new());
    let release_response = Arc::new(tokio::sync::Notify::new());
    let upload = upload_seen.clone();
    let release = release_response.clone();
    let origin = listen(move |mut socket| {
        let upload = upload.clone();
        let release = release.clone();
        async move {
            let request = String::from_utf8(head(&mut socket).await)
                .unwrap()
                .to_ascii_lowercase();
            assert!(request.starts_with("post /upload http/1.1"));
            assert!(request.contains("transfer-encoding: chunked"));
            assert!(!request.contains("content-length:"));
            let mut body = chunk(&mut socket).await;
            assert_eq!(body, b"bc");
            upload.notify_one();
            loop {
                let bytes = chunk(&mut socket).await;
                if bytes.is_empty() {
                    break;
                }
                body.extend(bytes);
            }
            assert_eq!(body, b"bcdef");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nxyz")
                .await
                .unwrap();
            release.notified().await;
            socket.write_all(b"uvw").await.unwrap();
        }
    })
    .await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::TransformBodies),
    )
    .await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        let connection = if version == "1.0" { "keep-alive" } else { "close" };
        client.write_all(format!("POST /upload HTTP/{version}\r\nHost: local\r\nConnection: {connection}\r\nContent-Length: 6\r\n\r\nabc").as_bytes()).await.unwrap();
        upload_seen.notified().await; // No upload EOF has been sent yet.
        client.write_all(b"def").await.unwrap();
        let response = String::from_utf8(head(&mut client).await).unwrap().to_ascii_lowercase();
        assert!(response.starts_with(&format!("http/{version} 200")), "{response}");
        assert!(response.contains("connection: close"), "{response}");
        assert!(!response.contains("content-length:"));
        let mut body = if version == "1.0" {
            assert!(!response.contains("transfer-encoding:"), "{response}");
            let mut bytes = vec![0; 2];
            client.read_exact(&mut bytes).await.unwrap();
            bytes
        } else {
            assert!(response.contains("transfer-encoding: chunked"), "{response}");
            chunk(&mut client).await
        };
        assert_eq!(body, b"yz");
        release_response.notify_one(); // Response EOF is still blocked upstream.
        if version == "1.0" {
            client.read_to_end(&mut body).await.unwrap();
        } else {
            loop { let bytes = chunk(&mut client).await; if bytes.is_empty() { break; } body.extend(bytes); }
        }
        assert_eq!(body, b"yzuvw");
        timeout(WAIT, seen.completed.notified()).await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn metadata_length_rewrite_cannot_truncate_the_native_upload() {
    let origin = listen(|mut socket| async move {
        let request = String::from_utf8(head(&mut socket).await)
            .unwrap()
            .to_ascii_lowercase();
        assert!(request.contains("content-length: 6"), "{request}");
        assert!(!request.contains("transfer-encoding:"));
        let mut body = [0; 6];
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"abcdef");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    })
    .await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::RewriteLength),
    )
    .await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        client.write_all(b"POST /upload HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: 6\r\n\r\nabcdef").await.unwrap();
        let mut response = Vec::new(); client.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200")); assert!(response.ends_with(b"ok"));
        timeout(WAIT, seen.completed.notified()).await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn callbacks_cannot_change_an_ordinary_response_into_an_upgrade() {
    let origin = origin(Arc::new(AtomicUsize::new(0))).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::ChangeUpgrade),
    )
    .await;
    let response = get(worker.address).await;
    assert!(!response.starts_with(b"HTTP/1.1 101"), "{response:?}");
    assert!(!response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    let stages = seen.stages.lock().unwrap();
    assert!(stages.contains(&"upstream_response"));
    assert!(!stages.contains(&"before_write"));
    assert!(!stages.contains(&"complete"));
}

#[tokio::test]
async fn compiled_callbacks_cannot_replace_websocket_handshake_negotiation() {
    for (mode, status, expected_connections, before_write) in [
        (Mode::PatchRequestHandshake, 400, 0, false),
        (Mode::PatchResponseHandshake, 502, 1, false),
        (Mode::PatchResponseConnection, 502, 1, true),
    ] {
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let origin = listen(move |mut socket| {
            accepted.fetch_add(1, Ordering::SeqCst);
            async move {
                let request = request_head_for_upgrade(&mut socket).await;
                assert!(request.contains(&"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==".to_ascii_lowercase()));
                socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
            }
        }).await;
        let seen = Arc::new(Seen::default());
        let worker = worker_with_callbacks(
            settings(origin.address, None, "", false),
            hooks(&seen, mode),
        )
        .await;
        timeout(WAIT, async {
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
            let response = head(&mut client).await;
            assert!(response.starts_with(format!("HTTP/1.1 {status}").as_bytes()), "{response:?}");
            assert_eq!(connections.load(Ordering::SeqCst), expected_connections);
            let stages = seen.stages.lock().unwrap();
            assert_eq!(stages.contains(&"before_write"), before_write);
            assert!(!stages.contains(&"complete"));
        }).await.unwrap();
    }
}

async fn request_head_for_upgrade(socket: &mut TcpStream) -> String {
    String::from_utf8(head(socket).await)
        .unwrap()
        .to_ascii_lowercase()
}

#[tokio::test]
async fn dial_refusal_stops_before_origin_connection_and_completion() {
    let count = Arc::new(AtomicUsize::new(0));
    let origin = origin(count.clone()).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::DenyDial),
    )
    .await;
    let response = get(worker.address).await;
    assert!(response.starts_with(b"HTTP/1.1 403"), "{response:?}");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        *seen.stages.lock().unwrap(),
        [
            "parsed_validation",
            "transform_first",
            "transform_second",
            "authenticate",
            "routing",
            "admit",
            "request_body_setup",
            "before_send",
            "pool_gate",
            "dial_gate",
        ]
    );
}

#[derive(Default)]
struct PauseSend {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for PauseSend {
    fn supports(&self, hook: proxy_pingora::worker::surface::CallbackHook) -> bool {
        hook == proxy_pingora::worker::surface::CallbackHook::BeforeUpstreamSend
    }
    async fn authenticate(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        Ok(None)
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> Result<Option<()>, String> {
        Ok(None)
    }
    async fn complete(&self, _: &RequestFacts) -> Result<u32, String> {
        unreachable!("finalization participant never admits execution")
    }
    async fn before_upstream_send(
        &self,
        _: u8,
        _: &mut RequestHeader,
        _: &(),
        _: &mut RequestFacts,
    ) -> Result<(), String> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(())
    }
    async fn maintain(&self) {}
}

#[tokio::test]
async fn admission_closing_during_finalization_prevents_network_and_completion() {
    let count = Arc::new(AtomicUsize::new(0));
    let origin = origin(count.clone()).await;
    let seen = Arc::new(Seen::default());
    let pause = Arc::new(PauseSend::default());
    let control = Arc::new(WorkerControl::default());
    let mut callbacks = hooks(&seen, Mode::Observe);
    callbacks.push(pause.clone());
    let worker = worker_with_control(
        settings(origin.address, None, "", false),
        control.clone(),
        callbacks,
    )
    .await;
    timeout(WAIT, async {
        let response = tokio::spawn(get(worker.address));
        pause.entered.notified().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "pending finalization cannot acquire upstream"
        );
        control
            .apply(proxy_pingora::worker::Update {
                expected_revision: control.snapshot().revision,
                accepting: false,
            })
            .unwrap();
        pause.resume.notify_one();
        let response = response.await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 503"), "{response:?}");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(
            *seen.stages.lock().unwrap(),
            [
                "parsed_validation",
                "transform_first",
                "transform_second",
                "authenticate",
                "routing",
                "admit",
                "request_body_setup",
                "before_send",
            ]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn replacing_a_sealed_request_body_before_send_never_connects_origin() {
    let count = Arc::new(AtomicUsize::new(0));
    let origin = origin(count.clone()).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::ReplaceRequestBeforeSend),
    )
    .await;
    let response = get(worker.address).await;
    assert!(!response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let stages = seen.stages.lock().unwrap();
    assert!(stages.contains(&"before_send"));
    assert!(!stages.contains(&"pool_gate"));
    assert!(!stages.contains(&"dial_gate"));
    assert!(!stages.contains(&"complete"));
}

#[tokio::test]
async fn replacing_a_sealed_response_body_before_write_cannot_publish_success() {
    assert_late_body_replacement_refused(Mode::ReplaceResponseBeforeWrite, "before_write").await;
}

#[tokio::test]
async fn replacing_request_body_after_upstream_response_cannot_publish_success() {
    assert_late_body_replacement_refused(Mode::ReplaceRequestAfterResponse, "upstream_response")
        .await;
}

async fn assert_late_body_replacement_refused(mode: Mode, stage: &'static str) {
    let count = Arc::new(AtomicUsize::new(0));
    let origin = origin(count.clone()).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, mode),
    )
    .await;
    let response = get(worker.address).await;
    assert!(!response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let stages = seen.stages.lock().unwrap();
    assert!(
        stages.contains(&stage),
        "replacement hook did not run: {stages:?}"
    );
    assert!(
        !stages.contains(&"complete"),
        "replaced body plan completed: {stages:?}"
    );
}

#[tokio::test]
async fn unsupported_native_upload_transfer_codings_never_connect_origin() {
    for coding in ["gzip, chunked", "chunked, gzip", "chunked, chunked"] {
        let count = Arc::new(AtomicUsize::new(0));
        let origin = origin(count.clone()).await;
        let seen = Arc::new(Seen::default());
        let worker = worker_with_callbacks(
            settings(origin.address, None, "", false),
            hooks(&seen, Mode::Observe),
        )
        .await;
        let response = timeout(WAIT, async {
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(format!("POST /upload HTTP/1.1\r\nHost: local\r\nConnection: close\r\nTransfer-Encoding: {coding}\r\n\r\n1\r\nx\r\n0\r\n\r\n").as_bytes()).await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            response
        }).await.unwrap();
        assert!(
            !response.starts_with(b"HTTP/1.1 200"),
            "accepted {coding}: {response:?}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "connected origin for {coding}"
        );
        assert!(!seen.stages.lock().unwrap().contains(&"complete"));
    }
}

#[tokio::test]
async fn unsupported_origin_transfer_coding_is_rejected_before_response_callbacks() {
    let origin = listen(|mut socket| async move {
        let _ = head(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n").await.unwrap();
    }).await;
    let seen = Arc::new(Seen::default());
    let worker = worker_with_callbacks(
        settings(origin.address, None, "", false),
        hooks(&seen, Mode::Observe),
    )
    .await;
    let response = get(worker.address).await;
    assert!(!response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    let stages = seen.stages.lock().unwrap();
    assert!(stages.contains(&"socket_gate"));
    assert!(!stages.contains(&"upstream_response"));
    assert!(!stages.contains(&"before_write"));
    assert!(!stages.contains(&"complete"));
}
