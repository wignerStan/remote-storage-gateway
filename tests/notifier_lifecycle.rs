//! Real HTTP sessions use the managed callback sequence and native pull streams.
//! Foreign ABI bodies are deliberately absent: only native leases own these streams.
use bytes::{Bytes, BytesMut};
use diagnostic_output::CredentialPolicy;
use pingora_core::{
    apps::{HttpServerApp, ReusedHttpStream},
    protocols::http::ServerSession,
    server::ShutdownWatch,
};
use pingora_http::ResponseHeader;
use proxy_pingora::http_ingress::{Application, Handler, Ingress, Worker};
use proxy_pingora::worker::{
    RequestFlow,
    surface::{CallbackHook, Callbacks, Contract, Egress, Exposure, NotifierChain, Rejection},
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use test_support::workflow::WorkflowTrace;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
};

#[derive(Default)]
struct Probe {
    preparations: AtomicUsize,
    request_polls: AtomicUsize,
    response_polls: AtomicUsize,
    egress_calls: AtomicUsize,
    completions: AtomicUsize,
    request_identity: AtomicUsize,
    response_identity: AtomicUsize,
    facts_identity: AtomicUsize,
    events: Mutex<Vec<(&'static str, usize)>>,
    rejection: Mutex<Option<&'static str>>,
    upstream_first_chunk: Notify,
    ready_for_body: Notify,
    release_response_tail: Notify,
}

// None of the request/context/response types implement Clone. Each owns its
// original carrier or response lease; transforms mutate one stable allocation.
struct Request {
    session: Box<ServerSession>,
    head_read: bool,
    protocol: &'static str,
    route: String,
    prepared_body: usize,
    encoded: usize,
}
struct Facts {
    probe: Arc<Probe>,
    phase: &'static str,
    next_participant: usize,
    authenticated: bool,
    delivered: bool,
}
struct Response {
    downstream: Box<ServerSession>,
    upstream: TcpStream,
    remaining: usize,
    marker: usize,
    probe: Arc<Probe>,
}
impl Response {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, &'static str> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.probe.response_polls.fetch_add(1, Ordering::SeqCst);
        let mut chunk = BytesMut::zeroed(self.remaining.min(4));
        let length = self
            .upstream
            .read(&mut chunk)
            .await
            .map_err(|_| "response_read")?;
        if length == 0 {
            return Err("response_truncated");
        }
        self.remaining -= length;
        chunk.truncate(length);
        let pointer = chunk.as_ptr();
        let chunk = chunk.freeze();
        assert_eq!(
            pointer,
            chunk.as_ptr(),
            "freezing a chunk transfers its allocation"
        );
        Ok(Some(chunk))
    }
}
struct Protocol;
impl Contract for Protocol {
    type Endpoint = u8;
    type Facts = Box<Facts>;
    type Execution = ();
    type Error = &'static str;
    type HttpRequest = Box<Request>;
    type HttpResponse = Box<Response>;
    type ChannelRequest = ();
    type ChannelResponse = ();
    fn rejection(_: Rejection) -> Self::Error {
        "worker_rejection"
    }
}

fn same_allocation(identity: &AtomicUsize, pointer: usize) {
    let first = identity
        .compare_exchange(0, pointer, Ordering::SeqCst, Ordering::SeqCst)
        .unwrap_or_else(|first| first);
    assert!(
        first == 0 || first == pointer,
        "a callback replaced the prepared allocation"
    );
}

struct Participant {
    index: usize,
    reject: Option<&'static str>,
}
fn decision(phase: &'static str, participant: usize, action: &'static str, reason: &'static str) {
    WorkflowTrace::new(std::io::stderr().lock(), CredentialPolicy::default(), 4096)
        .decision(
            phase,
            action,
            reason,
            serde_json::json!({"participant": participant}),
        )
        .unwrap();
}
impl Participant {
    fn observe(&self, phase: &'static str, facts: &mut Box<Facts>) -> Result<(), &'static str> {
        assert_ne!(self.index, 0, "uninterested callbacks never run");
        same_allocation(
            &facts.probe.facts_identity,
            (&**facts as *const Facts) as usize,
        );
        if facts.phase != phase {
            assert!(facts.phase.is_empty() || facts.next_participant == 3);
            facts.phase = phase;
            facts.next_participant = 1;
        }
        assert_eq!(
            facts.next_participant, self.index,
            "later participant sees the prior mutation"
        );
        facts.next_participant += 1;
        facts.probe.events.lock().unwrap().push((phase, self.index));
        if self.reject == Some(phase) && self.index == 2 {
            decision(
                phase,
                self.index,
                "reject",
                "the selected callback refuses this lifecycle stage",
            );
            return Err(phase);
        }
        decision(
            phase,
            self.index,
            "continue",
            "the next interested callback observes this context mutation without pulling a body",
        );
        Ok(())
    }
    fn request(
        &self,
        phase: &'static str,
        request: &mut Box<Request>,
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        same_allocation(
            &facts.probe.request_identity,
            (&**request as *const Request) as usize,
        );
        assert_eq!(
            facts.probe.request_polls.load(Ordering::SeqCst),
            0,
            "transformation must not pull request bodies"
        );
        assert_eq!(request.session.body_bytes_read(), 0);
        self.observe(phase, facts)
    }
    fn response(
        &self,
        phase: &'static str,
        response: &mut Box<Response>,
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        same_allocation(
            &facts.probe.response_identity,
            (&**response as *const Response) as usize,
        );
        assert_eq!(
            facts.probe.response_polls.load(Ordering::SeqCst),
            0,
            "response transforms operate on stream metadata"
        );
        self.observe(phase, facts)
    }
}
#[async_trait::async_trait]
impl Callbacks<Protocol> for Participant {
    fn supports(&self, _: CallbackHook) -> bool {
        self.index != 0
    }
    async fn pre_ingress(
        &self,
        _: u8,
        request: &mut Box<Request>,
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.request("protocol", request, facts)?;
        assert!(
            request.head_read,
            "the real ingress has parsed HTTP framing once"
        );
        assert_eq!(
            format!("{:?}", request.session.req_header().version),
            "HTTP/1.1"
        );
        if self.index == 1 {
            request.protocol = "http1";
        } else {
            assert_eq!(request.protocol, "http1");
        }
        Ok(())
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut Box<Request>,
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.request("headers_metadata", request, facts)?;
        if self.index == 1 {
            request.route = request.session.req_header().uri.path().into();
        } else {
            assert!(request.head_read);
            assert_eq!(request.route, "/original");
        }
        Ok(())
    }
    async fn authenticate(&self, _: u8, facts: &Box<Facts>) -> Result<Option<()>, &'static str> {
        // Existing gates intentionally see immutable facts. Record their actual
        // order separately; successful gates do not stop mutable transform phases.
        facts
            .probe
            .events
            .lock()
            .unwrap()
            .push(("auth", self.index));
        if self.reject == Some("auth") && self.index == 2 {
            decision(
                "auth",
                self.index,
                "reject",
                "authentication refuses the request before any body pull or egress",
            );
            return Err("auth");
        }
        decision(
            "auth",
            self.index,
            if self.index == 2 { "accept" } else { "defer" },
            "authentication gate selects an accepting participant before routing",
        );
        Ok((self.index == 2).then_some(()))
    }
    async fn routing(
        &self,
        _: u8,
        request: &mut Box<Request>,
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        // authenticate is a gate, so the route participant records that gate's
        // accepted ordering in the mutable state after it has completed.
        facts.authenticated = true;
        self.request("route", request, facts)?;
        if self.index == 1 {
            request.route = "/intermediate".into();
        } else {
            assert_eq!(request.route, "/intermediate");
            request.route = "/transformed".into();
        }
        Ok(())
    }
    async fn admit(&self, _: u8, facts: &Box<Facts>) -> Result<Option<()>, &'static str> {
        assert!(facts.authenticated);
        facts
            .probe
            .events
            .lock()
            .unwrap()
            .push(("admit", self.index));
        decision(
            "admit",
            self.index,
            if self.index == 2 { "accept" } else { "defer" },
            "execution admission pins its owner for downstream completion",
        );
        Ok((self.index == 2).then_some(()))
    }
    async fn prepare_request_body(
        &self,
        _: u8,
        request: &mut Box<Request>,
        _: &(),
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.request("body", request, facts)?;
        assert_eq!(request.prepared_body + 1, self.index);
        request.prepared_body = self.index;
        Ok(())
    }
    async fn before_upstream_send(
        &self,
        _: u8,
        request: &mut Box<Request>,
        _: &(),
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.request("encoding_egress", request, facts)?;
        assert_eq!(request.prepared_body, 2);
        assert_eq!(request.encoded + 1, self.index);
        request.encoded = self.index;
        Ok(())
    }
    async fn upstream_response(
        &self,
        _: u8,
        response: &mut Box<Response>,
        _: &(),
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.response("upstream_response", response, facts)?;
        assert_eq!(response.marker + 1, self.index);
        response.marker = self.index;
        Ok(())
    }
    async fn prepare_response_body(
        &self,
        _: u8,
        response: &mut Box<Response>,
        _: &(),
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.response("response_body", response, facts)?;
        assert_eq!(response.marker + 1, self.index + 2);
        response.marker += 1;
        Ok(())
    }
    async fn before_downstream_write(
        &self,
        _: u8,
        response: &mut Box<Response>,
        _: &(),
        facts: &mut Box<Facts>,
    ) -> Result<(), &'static str> {
        self.response("downstream_write", response, facts)?;
        assert_eq!(response.marker + 1, self.index + 4);
        response.marker += 1;
        Ok(())
    }
    async fn complete(&self, facts: &Box<Facts>) -> Result<u32, &'static str> {
        assert_eq!(self.index, 2, "only the execution owner completes");
        assert!(
            facts.delivered,
            "completion acknowledges actual downstream EOF"
        );
        facts.probe.completions.fetch_add(1, Ordering::SeqCst);
        facts
            .probe
            .events
            .lock()
            .unwrap()
            .push(("complete", self.index));
        decision(
            "complete",
            self.index,
            "delivered",
            "the native response reached EOF and downstream finish_body succeeded before acknowledgement",
        );
        Ok(7)
    }
    async fn maintain(&self) {}
}

struct NativeEgress {
    address: std::net::SocketAddr,
    probe: Arc<Probe>,
}
#[async_trait::async_trait]
impl Egress<Box<Request>> for NativeEgress {
    type Response = Box<Response>;
    type Error = &'static str;
    async fn execute(&self, mut request: Box<Request>) -> Result<Box<Response>, &'static str> {
        self.probe.egress_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.encoded, 2);
        assert_eq!(request.route, "/transformed");
        same_allocation(
            &self.probe.request_identity,
            (&*request as *const Request) as usize,
        );
        let mut upstream = TcpStream::connect(self.address).await.map_err(|_| "dial")?;
        upstream
            .write_all(
                format!(
                    "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n\r\n",
                    request.route
                )
                .as_bytes(),
            )
            .await
            .map_err(|_| "send_head")?;
        self.probe.ready_for_body.notify_one();
        loop {
            self.probe.request_polls.fetch_add(1, Ordering::SeqCst);
            let chunk = request
                .session
                .read_request_body()
                .await
                .map_err(|_| "request_body")?;
            let Some(chunk) = chunk else {
                break;
            };
            let original = chunk.as_ptr() as usize;
            upstream.write_all(&chunk).await.map_err(|_| "send_chunk")?;
            assert_eq!(
                original,
                chunk.as_ptr() as usize,
                "the native body chunk is not cloned or materialized"
            );
        }
        let head = read_head(&mut upstream).await?;
        assert!(head.starts_with(b"HTTP/1.1 200"));
        Ok(Box::new(Response {
            downstream: request.session,
            upstream,
            remaining: 8,
            marker: 0,
            probe: self.probe.clone(),
        }))
    }
}
struct NoChannel;
#[async_trait::async_trait]
impl Egress<()> for NoChannel {
    type Response = ();
    type Error = &'static str;
    async fn execute(&self, _: ()) -> Result<(), &'static str> {
        panic!("no fallback channel egress")
    }
}
fn flow(
    address: std::net::SocketAddr,
    probe: Arc<Probe>,
    reject: Option<&'static str>,
) -> RequestFlow<Protocol> {
    let callbacks = [0, 1, 2]
        .into_iter()
        .map(|index| Arc::new(Participant { index, reject }) as Arc<dyn Callbacks<Protocol>>)
        .collect();
    RequestFlow::new(
        NotifierChain::new(callbacks),
        Exposure::new(vec![1]),
        Arc::new(NativeEgress { address, probe }),
        Arc::new(NoChannel),
    )
}
fn prepare(session: ServerSession, probe: Arc<Probe>) -> (Box<Request>, Box<Facts>) {
    probe.preparations.fetch_add(1, Ordering::SeqCst);
    (
        Box::new(Request {
            session: Box::new(session),
            head_read: true,
            protocol: "unset",
            route: String::new(),
            prepared_body: 0,
            encoded: 0,
        }),
        Box::new(Facts {
            probe,
            phase: "",
            next_participant: 1,
            authenticated: false,
            delivered: false,
        }),
    )
}

struct LifecycleHandler {
    address: std::net::SocketAddr,
    probe: Arc<Probe>,
    reject: Option<&'static str>,
}
#[async_trait::async_trait]
impl Handler for LifecycleHandler {
    async fn handle(&self, session: ServerSession, _: &ShutdownWatch) -> Option<ServerSession> {
        let flow = flow(self.address, self.probe.clone(), self.reject);
        let (request, facts) = prepare(session, self.probe.clone());
        let mut exchange = match flow.execute_http(1, request, facts).await {
            Ok(exchange) => exchange,
            Err(error) => {
                *self.probe.rejection.lock().unwrap() = Some(error);
                return None;
            }
        };
        assert_eq!(exchange.response().marker, 6);
        let mut head = ResponseHeader::build(200, None).unwrap();
        head.insert_header("content-length", "8").unwrap();
        exchange.response_mut().downstream.set_keepalive(None);
        exchange
            .response_mut()
            .downstream
            .write_response_header(Box::new(head))
            .await
            .unwrap();
        while let Some(chunk) = exchange.response_mut().next_chunk().await.unwrap() {
            exchange
                .response_mut()
                .downstream
                .write_response_body(chunk, false)
                .await
                .unwrap();
        }
        exchange
            .response_mut()
            .downstream
            .finish_body()
            .await
            .unwrap();
        exchange.facts_mut().delivered = true;
        assert_eq!(exchange.finish().await.unwrap(), 7);
        None
    }
    fn capacity_response(&self) -> proxy_pingora::http_ingress::Response {
        proxy_pingora::http_ingress::Response {
            status: 503,
            headers: vec![],
            body: vec![],
        }
    }
}
struct HttpApplication(Arc<Worker<LifecycleHandler>>);
#[async_trait::async_trait]
impl Application for HttpApplication {
    async fn serve(
        self: &Arc<Self>,
        session: ServerSession,
        shutdown: &ShutdownWatch,
    ) -> Option<ReusedHttpStream> {
        self.0.serve(session, shutdown).await
    }
    async fn shutdown(&self) {}
}
async fn run_carrier(
    stream: TcpStream,
    address: std::net::SocketAddr,
    probe: Arc<Probe>,
    reject: Option<&'static str>,
) {
    let worker = Arc::new(Worker::new(
        LifecycleHandler {
            address,
            probe,
            reject,
        },
        NonZeroUsize::new(1).unwrap(),
    ));
    let ingress = Arc::new(Ingress::new(Arc::new(HttpApplication(worker))));
    let (_sender, shutdown) = watch::channel(false);
    let stream = pingora_core::protocols::l4::stream::Stream::from(stream);
    let session = ServerSession::new_http1(Box::new(stream));
    assert!(ingress.process_new_http(session, &shutdown).await.is_none());
}
async fn read_head(stream: &mut TcpStream) -> Result<Vec<u8>, &'static str> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() == 4096 {
            return Err("head_too_large");
        }
        head.push(stream.read_u8().await.map_err(|_| "read_head")?);
    }
    Ok(head)
}

#[tokio::test]
async fn managed_tcp_lifecycle_preserves_shared_mutations_and_lazy_streams() {
    tokio::time::timeout(Duration::from_secs(5), streaming_scenario())
        .await
        .unwrap();
}
async fn streaming_scenario() {
    let probe = Arc::new(Probe::default());
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = upstream.local_addr().unwrap();
    let upstream_probe = probe.clone();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let head = read_head(&mut stream).await.unwrap();
        assert!(head.starts_with(b"POST /transformed HTTP/1.1"));
        let mut chunk = [0; 4];
        stream.read_exact(&mut chunk).await.unwrap();
        assert_eq!(&chunk, b"abcd");
        upstream_probe.upstream_first_chunk.notify_one();
        stream.read_exact(&mut chunk).await.unwrap();
        assert_eq!(&chunk, b"efgh");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nWXYZ")
            .await
            .unwrap();
        upstream_probe.release_response_tail.notified().await;
        stream.write_all(b"QRST").await.unwrap();
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let server_probe = probe.clone();
    let server_task = tokio::spawn(async move {
        run_carrier(server, address, server_probe, None).await;
    });
    client
        .write_all(b"POST /original HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n\r\n")
        .await
        .unwrap();
    // Protocol, headers, auth, route and stream preparation need no body.
    probe.ready_for_body.notified().await;
    client.write_all(b"abcd").await.unwrap();
    probe.upstream_first_chunk.notified().await;
    assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
    client.write_all(b"efgh").await.unwrap();
    assert!(
        read_head(&mut client)
            .await
            .unwrap()
            .starts_with(b"HTTP/1.1 200")
    );
    let mut chunk = [0; 4];
    client.read_exact(&mut chunk).await.unwrap();
    assert_eq!(&chunk, b"WXYZ");
    assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
    probe.release_response_tail.notify_one();
    client.read_exact(&mut chunk).await.unwrap();
    assert_eq!(&chunk, b"QRST");
    server_task.await.unwrap();
    upstream_task.await.unwrap();
    assert_eq!(probe.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.egress_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.completions.load(Ordering::SeqCst), 1);
    let expected = [
        "protocol",
        "headers_metadata",
        "auth",
        "route",
        "admit",
        "body",
        "encoding_egress",
        "upstream_response",
        "response_body",
        "downstream_write",
    ]
    .into_iter()
    .flat_map(|phase| [(phase, 1), (phase, 2)])
    .chain([("complete", 2)])
    .collect::<Vec<_>>();
    assert_eq!(*probe.events.lock().unwrap(), expected);
}

#[tokio::test]
async fn auth_and_transform_rejections_do_not_poll_body_or_call_egress() {
    for rejection in ["auth", "headers_metadata", "body"] {
        let probe = Arc::new(Probe::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        // Declare a body but send none: eager materialization would hang here.
        client
            .write_all(b"POST /original HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            run_carrier(
                server,
                listener.local_addr().unwrap(),
                probe.clone(),
                Some(rejection),
            ),
        )
        .await
        .unwrap();
        assert_eq!(*probe.rejection.lock().unwrap(), Some(rejection));
        assert_eq!(probe.preparations.load(Ordering::SeqCst), 1);
        assert_eq!(probe.request_polls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.response_polls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.egress_calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
        assert!(
            !probe
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|(phase, _)| *phase == "encoding_egress")
        );
    }
}
