//! Opt-in real ProxyHttp upgraded upload hooks inside the existing native engine.
use super::*;
use bytes::Bytes;
use network_data_plane_worker::proxy::{ProxyContract, RequestFacts};
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use proxy_pingora::worker::surface::{CallbackHook, Callbacks};
use std::sync::{Mutex, atomic::AtomicBool};

const UPGRADE: &[u8] = b"GET /socket HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
const ACCEPT: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
const FRAME: &[u8] = &[0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2];

#[derive(Clone, Copy)]
enum Mode {
    Observe,
    Gate,
    Fail,
    Stall,
}
struct Observation {
    context: usize,
    pointer: usize,
    bytes: Bytes,
}
struct Handler {
    mode: Mode,
    created: AtomicUsize,
    dropped: Arc<AtomicUsize>,
    entered: tokio::sync::Notify,
    approvals: tokio::sync::Semaphore,
    hold_next: AtomicBool,
    observations: Mutex<Vec<Observation>>,
}
impl Handler {
    fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            created: AtomicUsize::new(0),
            dropped: Arc::new(AtomicUsize::new(0)),
            entered: tokio::sync::Notify::new(),
            approvals: tokio::sync::Semaphore::new(0),
            hold_next: AtomicBool::new(true),
            observations: Mutex::new(Vec::new()),
        })
    }
}
struct HandlerContext {
    id: usize,
    dropped: Arc<AtomicUsize>,
}
impl Drop for HandlerContext {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl ProxyHttp for Handler {
    type CTX = HandlerContext;
    fn new_ctx(&self) -> Self::CTX {
        HandlerContext {
            id: self.created.fetch_add(1, Ordering::SeqCst),
            dropped: self.dropped.clone(),
        }
    }
    async fn upstream_peer(&self, _: &mut Session, _: &mut Self::CTX) -> Result<Box<HttpPeer>> {
        panic!("unexposed handler routing called")
    }
    async fn request_filter(&self, _: &mut Session, _: &mut Self::CTX) -> Result<bool> {
        panic!("unexposed handler request filter called")
    }
    async fn early_request_filter(&self, _: &mut Session, _: &mut Self::CTX) -> Result<()> {
        panic!("unexposed early handler called")
    }
    async fn upstream_request_filter(
        &self,
        _: &mut Session,
        _: &mut RequestHeader,
        _: &mut Self::CTX,
    ) -> Result<()> {
        panic!("unexposed upstream handler called")
    }
    async fn upstream_response_filter(
        &self,
        _: &mut Session,
        _: &mut ResponseHeader,
        _: &mut Self::CTX,
    ) -> Result<()> {
        panic!("unexposed upstream response handler called")
    }
    async fn response_filter(
        &self,
        _: &mut Session,
        _: &mut ResponseHeader,
        _: &mut Self::CTX,
    ) -> Result<()> {
        panic!("unexposed response handler called")
    }
    fn response_body_filter(
        &self,
        _: &mut Session,
        _: &mut Option<Bytes>,
        _: bool,
        _: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        panic!("unexposed response body handler called")
    }
    async fn logging(&self, _: &mut Session, _: Option<&Error>, _: &mut Self::CTX) {
        panic!("unexposed handler logging called")
    }
    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        _: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        assert!(session.was_upgraded(), "native hook ran before accepted101");
        let Some(bytes) = body.as_ref().filter(|bytes| !bytes.is_empty()) else {
            return Ok(());
        };
        let pointer = bytes.as_ptr() as usize;
        let original = bytes.clone(); // Share the observed native allocation, no forced payload copy.
        match self.mode {
            Mode::Gate if self.hold_next.swap(false, Ordering::SeqCst) => {
                self.entered.notify_one();
                self.approvals.acquire().await.unwrap().forget();
            }
            Mode::Fail => {
                self.entered.notify_one();
                return Err(Error::explain(
                    pingora_error::ErrorType::Custom("native-hook-test"),
                    "request hook denied",
                ));
            }
            Mode::Stall => {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            Mode::Observe | Mode::Gate => {}
        }
        let bytes = body.as_ref().unwrap();
        assert_eq!(
            bytes.as_ptr() as usize,
            pointer,
            "raw Bytes allocation changed while handler awaited"
        );
        self.observations.lock().unwrap().push(Observation {
            context: ctx.id,
            pointer,
            bytes: original,
        });
        Ok(())
    }
}
#[derive(Default)]
struct Completion {
    completed: AtomicUsize,
}
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for Completion {
    fn supports(&self, hook: CallbackHook) -> bool {
        hook == CallbackHook::Admit
    }
    async fn authenticate(
        &self,
        _: u8,
        _: &RequestFacts,
    ) -> std::result::Result<Option<()>, String> {
        Ok(None)
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> std::result::Result<Option<()>, String> {
        Ok(Some(()))
    }
    async fn complete(&self, _: &RequestFacts) -> std::result::Result<u32, String> {
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(0)
    }
    async fn maintain(&self) {}
}
async fn hooked_worker(
    address: SocketAddr,
    handler: Arc<Handler>,
    completion: Arc<Completion>,
    io: Duration,
) -> Listening {
    let mut config = settings(address, None, "", false);
    config.io_timeout = io;
    let proxy =
        NativeProxy::new(config, Arc::new(WorkerControl::default()), vec![completion]).unwrap();
    serve_engine(proxy.with_upgraded_request_body(handler)).await
}
async fn dropped(handler: &Handler, count: usize) {
    timeout(WAIT, async {
        while handler.dropped.load(Ordering::SeqCst) < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn upgraded_request_hook_awaits_approval_and_keeps_preread_frames_and_native_bytes() {
    let observed = Arc::new(AtomicUsize::new(0));
    let origin_observed = observed.clone();
    let arrived = Arc::new(tokio::sync::Notify::new());
    let origin_arrived = arrived.clone();
    let origin = listen(move |mut socket| {
        let observed = origin_observed.clone();
        let arrived = origin_arrived.clone();
        async move {
            assert!(head(&mut socket).await.starts_with(b"GET /socket"));
            socket.write_all(ACCEPT).await.unwrap();
            for _ in 0..2 {
                let mut frame = [0; 8];
                socket.read_exact(&mut frame[..1]).await.unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
                arrived.notify_waiters();
                socket.read_exact(&mut frame[1..]).await.unwrap();
                observed.fetch_add(7, Ordering::SeqCst);
                assert_eq!(frame, FRAME);
                // Raw upstream data must pass untouched: no response hook was granted.
                socket.write_all(&[0x81, 2, b'h', b'i']).await.unwrap();
            }
        }
    })
    .await;
    let handler = Handler::new(Mode::Gate);
    let worker = hooked_worker(
        origin.address,
        handler.clone(),
        Arc::new(Completion::default()),
        WAIT,
    )
    .await;
    timeout(WAIT, async {
        let mut client = TcpStream::connect(worker.address).await.unwrap();
        let mut preread = UPGRADE.to_vec();
        preread.extend_from_slice(FRAME);
        client.write_all(&preread).await.unwrap();
        assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
        handler.entered.notified().await;
        assert_eq!(
            observed.load(Ordering::SeqCst),
            0,
            "request reached origin while hook awaited"
        );
        assert!(
            timeout(Duration::from_millis(30), arrived.notified())
                .await
                .is_err(),
            "origin received bytes while approval remained pending"
        );
        handler.approvals.add_permits(1);
        let mut reply = [0; 4];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, &[0x81, 2, b'h', b'i']);
        handler.hold_next.store(true, Ordering::SeqCst);
        client.write_all(FRAME).await.unwrap();
        handler.entered.notified().await;
        assert_eq!(
            observed.load(Ordering::SeqCst),
            FRAME.len(),
            "next request chunk passed without its own approval"
        );
        assert!(
            timeout(Duration::from_millis(30), arrived.notified())
                .await
                .is_err(),
            "origin received bytes while approval remained pending"
        );
        handler.approvals.add_permits(1);
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, &[0x81, 2, b'h', b'i']);
    })
    .await
    .unwrap();
    dropped(&handler, 1).await;
    let chunks = handler.observations.lock().unwrap();
    assert!(chunks.len() >= 2);
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.context == 0 && chunk.pointer == chunk.bytes.as_ptr() as usize)
    );
    assert_eq!(
        chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>(),
        [FRAME, FRAME].concat()
    );
}

#[tokio::test]
async fn regular_http_never_instantiates_or_calls_the_upgraded_handler() {
    let origin = origin(Arc::new(AtomicUsize::new(0))).await;
    let handler = Handler::new(Mode::Fail);
    let worker = hooked_worker(
        origin.address,
        handler.clone(),
        Arc::new(Completion::default()),
        WAIT,
    )
    .await;
    let response = get(worker.address).await;
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert!(response.ends_with(b"hello"));
    assert_eq!(handler.created.load(Ordering::SeqCst), 0);
    assert!(handler.observations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn upgraded_hook_failure_and_deadline_stop_payload_and_completion() {
    for mode in [Mode::Fail, Mode::Stall] {
        let forwarded = Arc::new(AtomicUsize::new(0));
        let origin_forwarded = forwarded.clone();
        let origin = listen(move |mut socket| {
            let forwarded = origin_forwarded.clone();
            async move {
                head(&mut socket).await;
                socket.write_all(ACCEPT).await.unwrap();
                let mut byte = [0];
                if socket.read(&mut byte).await.unwrap_or(0) > 0 {
                    forwarded.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
        .await;
        let handler = Handler::new(mode);
        let completion = Arc::new(Completion::default());
        let worker = hooked_worker(
            origin.address,
            handler.clone(),
            completion.clone(),
            Duration::from_millis(100),
        )
        .await;
        timeout(WAIT, async {
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(UPGRADE).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            client.write_all(FRAME).await.unwrap();
            handler.entered.notified().await;
            let mut tail = Vec::new();
            let _ = client.read_to_end(&mut tail).await;
            assert!(
                tail.is_empty(),
                "failed hook produced downstream success payload"
            );
        })
        .await
        .unwrap();
        dropped(&handler, 1).await;
        assert_eq!(forwarded.load(Ordering::SeqCst), 0);
        assert_eq!(completion.completed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn upgraded_native_context_is_private_to_each_request() {
    let origin = listen(|mut socket| async move {
        head(&mut socket).await;
        socket.write_all(ACCEPT).await.unwrap();
        let mut frame = [0; 8];
        socket.read_exact(&mut frame).await.unwrap();
        assert_eq!(frame, FRAME);
        socket.write_all(&[0x81, 2, b'h', b'i']).await.unwrap();
    })
    .await;
    let handler = Handler::new(Mode::Observe);
    let worker = hooked_worker(
        origin.address,
        handler.clone(),
        Arc::new(Completion::default()),
        WAIT,
    )
    .await;
    for count in 1..=2 {
        timeout(WAIT, async {
            let mut client = TcpStream::connect(worker.address).await.unwrap();
            client.write_all(UPGRADE).await.unwrap();
            assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
            client.write_all(FRAME).await.unwrap();
            let mut reply = [0; 4];
            client.read_exact(&mut reply).await.unwrap();
        })
        .await
        .unwrap();
        dropped(&handler, count).await;
    }
    assert_eq!(handler.created.load(Ordering::SeqCst), 2);
    let contexts = handler
        .observations
        .lock()
        .unwrap()
        .iter()
        .map(|chunk| chunk.context)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(contexts, [0, 1].into_iter().collect());
    let observations = handler.observations.lock().unwrap();
    for id in 0..2 {
        let payload = observations
            .iter()
            .filter(|chunk| chunk.context == id)
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(payload, FRAME);
    }
}
