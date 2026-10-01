//! The native fixture records exactly the metadata received across the C ABI.
use super::*;
use plugin_abi::{Expected, Language, Limits, PluginType, UnloadPolicy};
use serde_json::{Value, json};
use std::{path::Path, process::Command};

const CONTINUE: &str = r#"{"version":1,"decision":"continue"}"#;

struct Fixture {
    notifier: NativeNotifier,
    function: Arc<Callback>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    async fn new(reply: &str, hooks: &[&str], headers: &[&str]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("metadata.so");
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert!(
            Command::new("cc")
                .args(["-shared", "-fPIC", "-std=c11", "-I"])
                .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
                .arg(root.join("tests/fixtures/metadata_callback.c"))
                .arg("-o")
                .arg(&artifact)
                .status()
                .unwrap()
                .success()
        );
        let config: NativeCallback = serde_json::from_value(json!({
            "path":artifact, "package":"worker.metadata.test", "hooks":hooks,
            "headers":headers, "timeout_ms":2000, "max_inflight":1,
        }))
        .unwrap();
        let function = Arc::new(
            unsafe {
                Callback::load(
                    config.path.clone(),
                    Expected::v1(
                        &config.package,
                        &config.variant,
                        PluginType::Function,
                        Language::Rust,
                        UnloadPolicy::Explicit,
                    ),
                    Limits::new(FRAME_LIMIT, FRAME_LIMIT).unwrap(),
                    reply.as_bytes().to_vec(),
                    1,
                )
            }
            .await
            .unwrap(),
        );
        let notifier = NativeNotifier::new(Arc::clone(&function), &config).unwrap();
        Self {
            notifier,
            function,
            _directory: directory,
        }
    }
    async fn reply(&self, reply: &str) {
        self.function
            .call_with_reply(
                "set_reply",
                reply.as_bytes().to_vec(),
                Duration::from_secs(2),
                (),
                |_| (),
            )
            .await
            .unwrap();
    }
    async fn metadata(&self) -> Value {
        self.function
            .call_with_reply("inspect", vec![], Duration::from_secs(2), (), |bytes| {
                serde_json::from_slice::<Value>(bytes)
            })
            .await
            .unwrap()
            .unwrap()
    }
    async fn shutdown(self) {
        self.function.shutdown().await.unwrap();
    }
}

fn request() -> RequestHeader {
    let mut request =
        RequestHeader::build("POST", b"/resource?private-query=hidden", None).unwrap();
    request.insert_header("host", "origin.example").unwrap();
    request.append_header("x-visible", "first").unwrap();
    request.append_header("x-visible", "second").unwrap();
    request
        .insert_header("authorization", "Bearer private-credential")
        .unwrap();
    request
        .insert_header("cookie", "session=private-cookie")
        .unwrap();
    request.insert_header("x-remove", "original").unwrap();
    request
}
fn facts(request: &RequestHeader) -> RequestFacts {
    RequestFacts {
        request_id: 42,
        method: request.method.to_string(),
        path: request.uri.path().into(),
        status: None,
        upstream_reused: false,
        completed: false,
        headers: request.headers.clone(),
        request_body: Default::default(),
        response_body: Default::default(),
    }
}

#[tokio::test]
async fn native_callback_receives_only_metadata_and_explicit_repeated_headers() {
    let fixture = Fixture::new(
        CONTINUE,
        &["pre_ingress", "pre_routing"],
        &["X-Visible", "x-visible"],
    )
    .await;
    let mut request = request();
    let mut facts = facts(&request);
    facts.headers.clear();
    facts.method = "stale-method".into();
    facts.path = "/stale-path".into();
    fixture
        .notifier
        .pre_ingress(1, &mut request, &mut facts)
        .await
        .unwrap();
    assert_eq!(
        fixture.metadata().await["headers"],
        json!([["x-visible", "first"], ["x-visible", "second"]])
    );
    fixture
        .notifier
        .pre_routing(1, &mut request, &mut facts)
        .await
        .unwrap();
    assert_eq!(
        fixture.metadata().await,
        json!({
            "version":1, "hook":"pre_routing", "request_id":42,
            "method":"POST", "path":"/resource", "status":null,
            "upstream_reused":false, "completed":false,
            "headers":[["x-visible","first"],["x-visible","second"]],
        })
    );
    // Exact schema equality excludes bodies, private query and undeclared credentials.
    fixture.shutdown().await;
}

#[tokio::test]
async fn native_request_and_response_patches_apply_only_after_full_validation() {
    let patch = r#"{"version":1,"decision":"continue","remove_headers":["x-remove"],"set_headers":[["x-added","patched"]]}"#;
    let fixture = Fixture::new(patch, &["routing", "upstream_response"], &["x-visible"]).await;
    let mut request = request();
    let mut facts = facts(&request);
    fixture
        .notifier
        .routing(1, &mut request, &mut facts)
        .await
        .unwrap();
    assert!(!request.headers.contains_key("x-remove"));
    assert_eq!(request.headers["x-added"], "patched");
    assert_eq!(request.headers["host"], "origin.example");
    assert_eq!(
        facts.headers["x-remove"], "original",
        "native hooks leave metadata snapshots to the compiled stage boundary"
    );
    let mut response = ResponseHeader::build(201, None).unwrap();
    response.insert_header("x-remove", "original").unwrap();
    response
        .insert_header("x-visible", "response-only")
        .unwrap();
    facts.status = Some(200);
    facts.upstream_reused = true;
    fixture
        .notifier
        .upstream_response(1, &mut response, &(), &mut facts)
        .await
        .unwrap();
    assert!(!response.headers.contains_key("x-remove"));
    assert_eq!(response.headers["x-added"], "patched");
    let metadata = fixture.metadata().await;
    assert_eq!(metadata["hook"], "upstream_response");
    assert_eq!(
        metadata["status"], 201,
        "native frame must project the live response status"
    );
    assert_eq!(
        facts.status,
        Some(200),
        "native frame projection must not clone or mutate stage facts"
    );
    assert_eq!(metadata["upstream_reused"], true);
    assert_eq!(
        metadata["headers"],
        json!([["x-visible", "response-only"]]),
        "response stages must project the actual response head"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn native_invalid_denied_or_failed_reply_never_partially_mutates_headers() {
    let fixture = Fixture::new(CONTINUE, &["routing", "upstream_response"], &[]).await;
    let mut replies = vec![
        "not-json".to_owned(),
        r#"{"version":2,"decision":"continue","remove_headers":["x-remove"]}"#.into(),
        r#"{"version":1,"decision":"deny","set_headers":[["x-added","bad"]]}"#.into(),
        r#"{"version":1,"decision":"continue","body":"undeclared"}"#.into(),
        r#"{"version":1,"decision":"continue","set_headers":[["x-added","ok"],["bad name","no"]],"remove_headers":["x-remove"]}"#.into(),
        r#"{"version":1,"decision":"continue","set_headers":[["x-added","ok"],["x-invalid","bad\r\nvalue"]],"remove_headers":["x-remove"]}"#.into(),
        "FAIL".into(),
    ];
    for name in [
        "HOST",
        "Content-Length",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "proxy-connection",
        "upgrade",
        "trailer",
        "te",
    ] {
        for remove in [false, true] {
            let patch = if remove {
                json!({"remove_headers":["x-remove",name]})
            } else {
                json!({"set_headers":[["x-added","ok"],[name,"bad"]],"remove_headers":["x-remove"]})
            };
            let mut reply = json!({"version":1,"decision":"continue"});
            reply
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            replies.push(reply.to_string());
        }
    }
    replies.push(
        json!({"version":1,"decision":"continue","remove_headers":vec!["x-remove";65]}).to_string(),
    );
    for reply in replies {
        fixture.reply(&reply).await;
        let mut request = request();
        let mut facts = facts(&request);
        let original = request.headers.clone();
        assert!(
            fixture
                .notifier
                .routing(1, &mut request, &mut facts)
                .await
                .is_err(),
            "accepted {reply}"
        );
        assert_eq!(request.headers, original, "partially applied {reply}");
        let mut response = ResponseHeader::build(200, None).unwrap();
        response.insert_header("x-remove", "original").unwrap();
        let original = response.headers.clone();
        assert!(
            fixture
                .notifier
                .upstream_response(1, &mut response, &(), &mut facts)
                .await
                .is_err()
        );
        assert_eq!(
            response.headers, original,
            "partially applied response {reply}"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn hook_interest_and_acceptance_are_exact_and_nonpatch_stages_reject_mutations() {
    let fixture = Fixture::new(
        CONTINUE,
        &["authenticate", "admit", "before_upstream_send"],
        &[],
    )
    .await;
    for hook in [
        CallbackHook::Authenticate,
        CallbackHook::Admit,
        CallbackHook::BeforeUpstreamSend,
    ] {
        assert!(fixture.notifier.supports(hook));
    }
    for hook in [
        CallbackHook::PreIngress,
        CallbackHook::PreRouting,
        CallbackHook::Routing,
        CallbackHook::PrepareRequestBody,
        CallbackHook::UpstreamResponse,
        CallbackHook::PrepareResponseBody,
        CallbackHook::BeforeDownstreamWrite,
        CallbackHook::Notify,
        CallbackHook::Socket,
        CallbackHook::BeforeDial,
        CallbackHook::Pool,
        CallbackHook::BeforeBridge,
    ] {
        assert!(
            !fixture.notifier.supports(hook),
            "unexpected interest in {hook:?}"
        );
    }
    let request = request();
    let facts = facts(&request);
    assert_eq!(
        fixture.notifier.authenticate(1, &facts).await.unwrap(),
        None
    );
    assert_eq!(fixture.notifier.admit(1, &facts).await.unwrap(), None);
    let before = fixture.metadata().await;
    assert_eq!(fixture.notifier.complete(&facts).await.unwrap(), 0);
    assert_eq!(
        fixture.metadata().await,
        before,
        "unregistered completion entered native code"
    );
    fixture.reply(r#"{"version":1,"decision":"accept"}"#).await;
    assert_eq!(
        fixture.notifier.authenticate(1, &facts).await.unwrap(),
        Some(())
    );
    assert_eq!(fixture.notifier.admit(1, &facts).await.unwrap(), Some(()));
    fixture
        .reply(r#"{"version":1,"decision":"accept","remove_headers":["x-remove"]}"#)
        .await;
    assert!(fixture.notifier.authenticate(1, &facts).await.is_err());
    assert!(fixture.notifier.admit(1, &facts).await.is_err());
    fixture.shutdown().await;
}

#[tokio::test]
async fn selected_completion_enters_native_code_once_and_forbids_patches() {
    let fixture = Fixture::new(CONTINUE, &["complete"], &["x-visible"]).await;
    let request = request();
    let mut facts = facts(&request);
    facts.completed = true;
    facts.status = Some(204);
    assert_eq!(fixture.notifier.complete(&facts).await.unwrap(), 0);
    let metadata = fixture.metadata().await;
    assert_eq!(metadata["hook"], "complete");
    assert_eq!(metadata["completed"], true);
    assert_eq!(metadata["status"], 204);
    fixture
        .reply(r#"{"version":1,"decision":"continue","set_headers":[["x-added","bad"]]}"#)
        .await;
    assert!(fixture.notifier.complete(&facts).await.is_err());
    fixture.shutdown().await;
}

#[tokio::test]
async fn callback_input_size_and_textual_header_checks_fail_before_native_entry() {
    let fixture = Fixture::new(CONTINUE, &["routing"], &["x-visible"]).await;
    let mut request = request();
    let mut facts = facts(&request);
    fixture
        .notifier
        .routing(1, &mut request, &mut facts)
        .await
        .unwrap();
    let previous = fixture.metadata().await;
    request
        .insert_header("x-visible", "x".repeat(FRAME_LIMIT))
        .unwrap();
    let original = request.headers.clone();
    assert!(
        fixture
            .notifier
            .routing(1, &mut request, &mut facts)
            .await
            .is_err()
    );
    assert_eq!(request.headers, original);
    assert_eq!(fixture.metadata().await, previous);
    request.set_uri("/resource".parse().unwrap());
    request
        .headers
        .insert("x-visible", http::HeaderValue::from_bytes(b"\xff").unwrap());
    let original = request.headers.clone();
    assert!(
        fixture
            .notifier
            .routing(1, &mut request, &mut facts)
            .await
            .is_err()
    );
    assert_eq!(request.headers, original);
    assert_eq!(fixture.metadata().await, previous);
    fixture.shutdown().await;
}

#[test]
fn configuration_rejects_invalid_hooks_header_names_bounds_and_body_capture_settings() {
    let base = json!({"upstream":"http://origin.example", "callbacks":[{
        "path":"/trusted/metadata.so", "package":"worker.metadata.test", "hooks":["routing"]
    }]});
    let valid: crate::config::Config = serde_json::from_value(base.clone()).unwrap();
    valid.validate().unwrap();
    for (field, value) in [
        ("hooks", json!([])),
        ("hooks", json!(["routing", "routing"])),
        ("hooks", json!(["request_body_dump"])),
        ("headers", json!(["bad name"])),
        ("headers", json!(vec!["x-visible"; 65])),
        ("max_inflight", json!(0)),
        ("max_inflight", json!(1025)),
        ("timeout_ms", json!(0)),
        ("timeout_ms", json!(300001)),
        ("path", json!("")),
        ("package", json!("")),
        ("variant", json!("")),
    ] {
        let mut invalid = base.clone();
        invalid["callbacks"][0][field] = value;
        let config: crate::config::Config = serde_json::from_value(invalid).unwrap();
        assert!(
            config.validate().is_err(),
            "accepted invalid callback {field}"
        );
    }
    for logging in [
        json!({"body_max_bytes":1}),
        json!({"body_file":{"directory":"logs", "keep_files":2}}),
        json!({"body_file":{"directory":"logs","keep_files":2},"body_max_bytes":1048577}),
        json!({"queue_capacity":0}),
    ] {
        let mut invalid = base.clone();
        invalid["logging"] = logging;
        let config: crate::config::Config = serde_json::from_value(invalid).unwrap();
        assert!(config.validate().is_err());
    }
}

fn dial_request() -> DialRequest {
    DialRequest {
        target: DialTarget::Host {
            name: "origin.example".into(),
            port: 443,
        },
        timeout: Duration::from_millis(1500),
        tcp_nodelay: true,
    }
}

#[tokio::test]
async fn native_network_controls_map_decisions_and_project_typed_safe_phases() {
    let fixture = Fixture::new(CONTINUE, &["before_dial", "pool", "socket"], &[]).await;
    for hook in [
        CallbackHook::BeforeDial,
        CallbackHook::Pool,
        CallbackHook::Socket,
    ] {
        assert!(fixture.notifier.supports(hook));
    }
    let facts = facts(&request());
    let dial = dial_request();
    for (reply, dial_decision, pool_decision, socket_decision) in [
        (CONTINUE, None, None, None),
        (
            r#"{"version":1,"decision":"accept"}"#,
            Some(DialDecision::Allow),
            Some(PoolDecision::Reuse),
            Some(SocketDecision::Continue),
        ),
        (
            r#"{"version":1,"decision":"deny"}"#,
            Some(DialDecision::Deny),
            Some(PoolDecision::Discard),
            Some(SocketDecision::Close),
        ),
    ] {
        fixture.reply(reply).await;
        assert_eq!(
            fixture
                .notifier
                .before_dial(1, &dial, &facts)
                .await
                .unwrap(),
            dial_decision
        );
        assert_eq!(
            fixture.metadata().await["network"],
            json!({
                "kind":"before_dial", "target":{"kind":"host","name":"origin.example","port":443},
                "timeout_ms":1500, "tcp_nodelay":true,
            })
        );
        assert_eq!(
            fixture
                .notifier
                .pool(1, PoolPhase::BeforeCheckout, &facts)
                .await
                .unwrap(),
            pool_decision
        );
        assert_eq!(
            fixture.metadata().await["network"],
            json!({"kind":"pool", "phase":{"name":"before_checkout"}})
        );
        assert_eq!(
            fixture
                .notifier
                .pool(
                    1,
                    PoolPhase::BeforeReturn {
                        entry_id: 987654321,
                        reused: true,
                        healthy: false
                    },
                    &facts
                )
                .await
                .unwrap(),
            pool_decision
        );
        assert_eq!(
            fixture.metadata().await["network"],
            json!({"kind":"pool", "phase":{"name":"before_return","reused":true,"healthy":false}})
        );
        for (phase, name) in [
            (SocketPhase::BeforeUse, "before_use"),
            (SocketPhase::BeforeReuse, "before_reuse"),
        ] {
            assert_eq!(
                fixture.notifier.socket(1, phase, &facts).await.unwrap(),
                socket_decision
            );
            assert_eq!(
                fixture.metadata().await["network"],
                json!({"kind":"socket","phase":name})
            );
        }
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn native_network_controls_reject_patches_errors_and_oversized_or_secret_targets() {
    let fixture = Fixture::new(CONTINUE, &["before_dial", "pool", "socket"], &[]).await;
    let facts = facts(&request());
    let mut dial = dial_request();
    fixture
        .notifier
        .before_dial(1, &dial, &facts)
        .await
        .unwrap();
    let previous = fixture.metadata().await;
    for name in [
        "x".repeat(FRAME_LIMIT),
        "user:private-password@origin.example".into(),
        "https://origin.example".into(),
    ] {
        dial.target = DialTarget::Host { name, port: 443 };
        assert!(
            fixture
                .notifier
                .before_dial(1, &dial, &facts)
                .await
                .is_err()
        );
        assert_eq!(
            fixture.metadata().await,
            previous,
            "invalid target crossed ABI"
        );
    }
    dial = dial_request();
    for reply in [
        "not-json",
        "FAIL",
        r#"{"version":2,"decision":"continue"}"#,
        r#"{"version":1,"decision":"accept","set_headers":[["x-added","value"]]}"#,
        r#"{"version":1,"decision":"deny","remove_headers":["x-remove"]}"#,
        r#"{"version":1,"decision":"continue","phase":"unsupported"}"#,
    ] {
        fixture.reply(reply).await;
        assert!(
            fixture
                .notifier
                .before_dial(1, &dial, &facts)
                .await
                .is_err(),
            "accepted {reply}"
        );
        assert!(
            fixture
                .notifier
                .pool(1, PoolPhase::BeforeCheckout, &facts)
                .await
                .is_err()
        );
        assert!(
            fixture
                .notifier
                .socket(1, SocketPhase::BeforeUse, &facts)
                .await
                .is_err()
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn native_network_chain_defers_stops_at_first_decision_and_propagates_failure() {
    use proxy_pingora::worker::surface::{Exposure, NotifierChain};
    let first = Fixture::new(CONTINUE, &["before_dial", "pool", "socket"], &[]).await;
    let second = Fixture::new(
        r#"{"version":1,"decision":"deny"}"#,
        &["before_dial", "pool", "socket"],
        &[],
    )
    .await;
    // Move just the notifiers into the chain; retain native handles for snapshot inspection.
    let chain = NotifierChain::<ProxyContract>::new(vec![
        Arc::new(first.notifier),
        Arc::new(second.notifier),
    ]);
    let exposure = Exposure::new(vec![1]);
    let facts = facts(&request());
    let dial = dial_request();
    assert_eq!(
        chain
            .before_dial(&exposure, 1, &dial, &facts)
            .await
            .unwrap(),
        DialDecision::Deny
    );
    assert_eq!(
        chain
            .pool(&exposure, 1, PoolPhase::BeforeCheckout, &facts)
            .await
            .unwrap(),
        PoolDecision::Discard
    );
    assert_eq!(
        chain
            .socket(&exposure, 1, SocketPhase::BeforeUse, &facts)
            .await
            .unwrap(),
        SocketDecision::Close
    );
    let inspect = |function: Arc<Callback>| async move {
        function
            .call_with_reply("inspect", vec![], Duration::from_secs(2), (), |bytes| {
                serde_json::from_slice::<Value>(bytes)
            })
            .await
            .unwrap()
            .unwrap()
    };
    let previous = inspect(Arc::clone(&second.function)).await;
    for reply in [r#"{"version":1,"decision":"accept"}"#, "FAIL"] {
        first
            .function
            .call_with_reply(
                "set_reply",
                reply.as_bytes().to_vec(),
                Duration::from_secs(2),
                (),
                |_| (),
            )
            .await
            .unwrap();
        let result = chain.before_dial(&exposure, 1, &dial, &facts).await;
        if reply == "FAIL" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), DialDecision::Allow);
        }
        let result = chain
            .pool(
                &exposure,
                1,
                PoolPhase::BeforeReturn {
                    entry_id: 1,
                    reused: false,
                    healthy: true,
                },
                &facts,
            )
            .await;
        if reply == "FAIL" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), PoolDecision::Reuse);
        }
        let result = chain
            .socket(&exposure, 1, SocketPhase::BeforeReuse, &facts)
            .await;
        if reply == "FAIL" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), SocketDecision::Continue);
        }
        assert_eq!(
            inspect(Arc::clone(&second.function)).await,
            previous,
            "later native participant entered after decision or failure"
        );
    }
    drop(chain);
    first.function.shutdown().await.unwrap();
    second.function.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_native_stage_is_rejected_before_entry() {
    let fixture = Fixture::new(CONTINUE, &["routing"], &[]).await;
    let mut request = request();
    let mut facts = facts(&request);
    fixture
        .notifier
        .routing(1, &mut request, &mut facts)
        .await
        .unwrap();
    let previous = fixture.metadata().await;
    assert!(
        fixture
            .notifier
            .invoke("before_bridge", &facts)
            .await
            .is_err()
    );
    assert_eq!(fixture.metadata().await, previous);
    assert!(validate_hooks(&["before_bridge".into()]).is_err());
    fixture.shutdown().await;
}

#[tokio::test]
async fn directional_native_hooks_patch_heads_and_preserve_transform_acceptance() {
    let fixture = Fixture::new(
        r#"{"version":1,"decision":"accept","set_headers":[["x-direction","patched"]]}"#,
        &["before_upstream_send", "before_downstream_write"],
        &["x-visible"],
    )
    .await;
    let mut request = request();
    let mut facts = facts(&request);
    fixture
        .notifier
        .before_upstream_send(1, &mut request, &(), &mut facts)
        .await
        .unwrap();
    assert_eq!(request.headers["x-direction"], "patched");
    assert_eq!(fixture.metadata().await["hook"], "before_upstream_send");
    let mut response = ResponseHeader::build(200, None).unwrap();
    response
        .insert_header("x-visible", "downstream-value")
        .unwrap();
    fixture
        .notifier
        .before_downstream_write(1, &mut response, &(), &mut facts)
        .await
        .unwrap();
    assert_eq!(response.headers["x-direction"], "patched");
    let metadata = fixture.metadata().await;
    assert_eq!(metadata["hook"], "before_downstream_write");
    assert_eq!(
        metadata["headers"],
        json!([["x-visible", "downstream-value"]])
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn native_dial_metadata_projects_numeric_and_unix_targets_without_handles() {
    let fixture = Fixture::new(CONTINUE, &["before_dial"], &[]).await;
    let facts = facts(&request());
    let mut dial = dial_request();
    dial.target = DialTarget::Tcp("[::1]:8443".parse().unwrap());
    assert_eq!(
        fixture
            .notifier
            .before_dial(1, &dial, &facts)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        fixture.metadata().await["network"]["target"],
        json!({"kind":"tcp","address":"[::1]:8443"})
    );
    #[cfg(unix)]
    {
        dial.target = DialTarget::Unix("/run/worker-origin.sock".into());
        assert_eq!(
            fixture
                .notifier
                .before_dial(1, &dial, &facts)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            fixture.metadata().await["network"]["target"],
            json!({"kind":"unix","path":"/run/worker-origin.sock"})
        );
    }
    fixture.shutdown().await;
}
