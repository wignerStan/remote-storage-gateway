#![cfg(feature = "l7")]
//! A host-selected callback sequence, real network effect, and readable BDD evidence.
use diagnostic_output::CredentialPolicy;
use network_sockets::{DialRequest, DialTarget};
use proxy_pingora::worker::{
    Egress, NotifierChain, RequestFlow,
    surface::{CallbackHook, Callbacks, Contract, DialDecision, Exposure, Rejection},
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use test_support::workflow::WorkflowTrace;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Protocol;
// Intentionally not Clone: all callbacks inspect the same request allocation.
struct Facts {
    payload: Vec<u8>,
}
impl Contract for Protocol {
    type Endpoint = u8;
    type Facts = Facts;
    type Execution = ();
    type Error = &'static str;
    type HttpRequest = ();
    type HttpResponse = ();
    type ChannelRequest = ();
    type ChannelResponse = ();
    fn rejection(reason: Rejection) -> Self::Error {
        match reason {
            Rejection::CompletionAlreadyStarted => "completion_already_started",
            Rejection::OutsideExposure => "outside_exposure",
            Rejection::NoAuthentication => "no_authentication",
            Rejection::NoExecution => "no_execution",
            Rejection::ForeignAdmission => "foreign_admission",
        }
    }
}
struct UnusedEgress;
#[async_trait::async_trait]
impl Egress<()> for UnusedEgress {
    type Response = ();
    type Error = &'static str;
    async fn execute(&self, _: ()) -> Result<(), Self::Error> {
        panic!("no implicit egress")
    }
}
type Trace = Arc<Mutex<WorkflowTrace<Vec<u8>>>>;
// Keep partial decisions visible on assertion failure or scenario timeout too.
struct TraceReport(Trace);
impl TraceReport {
    fn take(&self) -> String {
        let mut trace = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let trace = std::mem::replace(
            &mut *trace,
            WorkflowTrace::new(Vec::new(), CredentialPolicy::default(), 8192),
        );
        String::from_utf8(trace.into_inner()).unwrap()
    }
}
impl Drop for TraceReport {
    fn drop(&mut self) {
        let rendered = self.take();
        if !rendered.is_empty() {
            eprintln!("{rendered}");
        }
    }
}
struct Participant {
    name: &'static str,
    interested: bool,
    accepts: bool,
    calls: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
    completions: Arc<AtomicUsize>,
    payload_address: usize,
    trace: Trace,
}
impl Participant {
    fn observe(&self, hook: &'static str, facts: &Facts) {
        assert!(self.interested, "uninterested callback must never run");
        assert_eq!(facts.payload.as_ptr() as usize, self.payload_address);
        self.calls.lock().unwrap().push((self.name, hook));
        self.trace
            .lock()
            .unwrap()
            .decision(
                hook,
                if self.accepts { "accept" } else { "defer" },
                if self.accepts {
                    "selected participant supplied a decision"
                } else {
                    "participant deferred to the next callback"
                },
                json!({"callback": self.name, "payload_bytes": facts.payload.len()}),
            )
            .unwrap();
    }
}
#[async_trait::async_trait]
impl Callbacks<Protocol> for Participant {
    fn supports(&self, hook: CallbackHook) -> bool {
        self.interested
            && matches!(
                hook,
                CallbackHook::Authenticate | CallbackHook::Admit | CallbackHook::BeforeDial
            )
    }
    async fn authenticate(&self, _: u8, facts: &Facts) -> Result<Option<()>, &'static str> {
        self.observe("authenticate", facts);
        Ok(self.accepts.then_some(()))
    }
    async fn admit(&self, _: u8, facts: &Facts) -> Result<Option<()>, &'static str> {
        self.observe("admit", facts);
        Ok(self.accepts.then_some(()))
    }
    async fn before_dial(
        &self,
        _: u8,
        _: &DialRequest,
        facts: &Facts,
    ) -> Result<Option<DialDecision>, &'static str> {
        self.observe("before_dial", facts);
        Ok(self.accepts.then_some(DialDecision::Allow))
    }
    async fn complete(&self, facts: &Facts) -> Result<u32, &'static str> {
        self.observe("complete", facts);
        self.completions.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }
    async fn maintain(&self) {}
}

#[tokio::test]
async fn admitted_request_dials_once_and_completion_cannot_repeat() {
    tokio::time::timeout(Duration::from_secs(5), scenario())
        .await
        .unwrap();
}
async fn scenario() {
    // Given one request and ordered callbacks, capture facts once per scenario.
    let facts = Facts {
        payload: b"one request body".to_vec(),
    };
    let trace = Arc::new(Mutex::new(WorkflowTrace::new(
        Vec::new(),
        CredentialPolicy::default(),
        8192,
    )));
    let report = TraceReport(Arc::clone(&trace));
    trace
        .lock()
        .unwrap()
        .decision(
            "given",
            "ready",
            "one prepared request is shared by the installed callbacks",
            json!({
                "authorization": "Bearer fixture-api-key",
                "cookie": "route=blue; __oailb=eyJhbGciOiJub25lIn0.eyJub2RlIjo3fQ.affinity; theme=dark; access_token=fixture-session-secret; jwt=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmaXh0dXJlIn0.signature",
                "request_id": "workflow-1",
            }),
        )
        .unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let completions = Arc::new(AtomicUsize::new(0));
    let participants = [
        ("uninterested", false, true),
        ("observer", true, false),
        ("owner", true, true),
        ("late", true, true),
    ]
    .into_iter()
    .map(|(name, interested, accepts)| {
        Arc::new(Participant {
            name,
            interested,
            accepts,
            calls: Arc::clone(&calls),
            completions: Arc::clone(&completions),
            payload_address: facts.payload.as_ptr() as usize,
            trace: Arc::clone(&trace),
        }) as Arc<dyn Callbacks<Protocol>>
    })
    .collect();
    let flow = RequestFlow::new(
        NotifierChain::new(participants),
        Exposure::new(vec![1]),
        Arc::new(UnusedEgress),
        Arc::new(UnusedEgress),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let request = DialRequest {
        target: DialTarget::Tcp(listener.local_addr().unwrap()),
        timeout: Duration::from_secs(3),
        tcp_nodelay: true,
    };
    let server = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        peer.write_all(b"acknowledged").await.unwrap();
        received
    });

    // When authentication and admission accept, perform the authorized effect.
    flow.authenticate(1, &facts).await.unwrap();
    let admission = flow.admit(1, &facts).await.unwrap();
    let mut socket = flow.dial(1, &request, &facts).await.unwrap();
    socket.write_all(&facts.payload).await.unwrap();
    socket.shutdown().await.unwrap();
    let mut reply = Vec::new();
    socket.read_to_end(&mut reply).await.unwrap();
    assert_eq!(reply, b"acknowledged");
    assert_eq!(server.await.unwrap(), facts.payload);
    assert_eq!(flow.complete(&admission, &facts).await.unwrap(), 1);

    // Then the original owner completes once; retries cannot repeat its effect.
    assert_eq!(
        flow.complete(&admission, &facts).await,
        Err("completion_already_started")
    );
    assert_eq!(completions.load(Ordering::SeqCst), 1);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            ("observer", "authenticate"),
            ("owner", "authenticate"),
            ("observer", "admit"),
            ("owner", "admit"),
            ("observer", "before_dial"),
            ("owner", "before_dial"),
            ("owner", "complete"),
        ]
    );
    trace
        .lock()
        .unwrap()
        .decision(
            "complete_retry",
            "reject",
            "completion has already started; no callback or network effect was repeated",
            json!({"completion_calls": completions.load(Ordering::SeqCst)}),
        )
        .unwrap();
    let rendered = report.take();
    // One flush to the test output, after the complete decision sequence.
    eprintln!("{rendered}");
    assert!(rendered.contains("route=blue"));
    assert!(rendered.contains("theme=dark"));
    assert!(rendered.contains("__oailb=eyJhbGciOiJub25lIn0.eyJub2RlIjo3fQ.affinity"));
    assert!(rendered.contains("sha256:"));
    assert!(!rendered.contains("fixture-api-key"));
    assert!(!rendered.contains("fixture-session-secret"));
    assert!(!rendered.contains("eyJhbGciOiJIUzI1NiJ9"));
    assert!(!rendered.contains("<redacted>"));
    assert!(rendered.contains(r#"DECISION step="authenticate" action="defer""#));
    assert!(rendered.contains(r#"DECISION step="authenticate" action="accept""#));
    assert!(rendered.contains("complete_retry"));
}
