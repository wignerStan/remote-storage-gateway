use super::*;
use action_execution::{Action, AsyncHandler, Binding, Context, Contract, ResponseMode, Schema};
use action_router::Router;
use network_local_control::{ControlService, listener::ControlListener};
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_waiter_keeps_membership_pinned_until_remote_exchange_settles() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let router = Router::new();
        let registration = router
            .register(vec![Action {
                contract: Contract {
                    id: "worker.metadata".into(),
                    version: 1,
                    response: ResponseMode::Unary,
                    input: Schema::Null,
                    output: Schema::Null,
                    max_input: 4,
                    max_output: 4,
                },
                bindings: vec![Binding::LocalControl {
                    action: "worker.metadata.v1.policy.routing".into(),
                }],
                handler: Arc::new(AsyncHandler({
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move |_: Vec<u8>, _: Context| {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            Ok(b"null".to_vec())
                        }
                    }
                })),
            }])
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let socket = tokio::net::UnixListener::bind(&path).unwrap();
        let endpoint = format!("unix://{}", path.display());
        let listener = ControlListener::prepare(
            socket,
            None,
            ControlService::new(Arc::new(router), 1024, Duration::from_secs(5), 32).unwrap(),
        )
        .unwrap();
        listener.activate().unwrap();
        let surface = Surface::new(vec![Grant::new(
            "request_policy".into(),
            "policy".into(),
            &["routing".into()],
        )])
        .unwrap();
        let bindings = BTreeMap::from([(
            "request_policy".into(),
            AxisBinding {
                endpoint,
                max_inflight: 2,
                max_message_bytes: 65536,
                connect_timeout_ms: 5000,
                shutdown_timeout_ms: 5000,
            },
        )]);
        let manager = Arc::new(
            RemotePluginManager::connect(bindings, surface)
                .await
                .unwrap(),
        );
        let generation = manager
            .state("request_policy", "policy")
            .unwrap()
            .generation
            .unwrap();
        let binding = manager.binding("request_policy", "policy").unwrap();
        let memory = plugin_lifecycle::coordination::memory::Coordinator::new(
            plugin_lifecycle::coordination::memory::Limits::default(),
        )
        .unwrap();
        let subscriber = memory
            .subscribe(RegistrationSpec {
                name: "policy".into(),
                contract: "worker.metadata.v1".into(),
            })
            .await
            .unwrap();
        let mut allocation = subscriber.allocate(4).unwrap();
        std::io::Write::write_all(&mut allocation, b"null").unwrap();
        let (input, custody) = allocation.finish().into_parts();
        let waiter = tokio::spawn(async move {
            binding
                .call("routing", input, Duration::from_secs(5), custody)
                .await
        });
        entered.notified().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let memory_retirement = memory.request_close();
        assert!(subscriber.allocate(1).is_err());
        assert_eq!(memory.stats().allocations, 1);
        assert_eq!(memory.stats().bytes, 4);

        let mut withdrawal = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .withdraw("request_policy", "policy", generation)
                    .await
            }
        });
        // Observe admission closing before checking that the live exchange holds retirement.
        while manager.state("request_policy", "policy").unwrap().available {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut withdrawal)
                .await
                .is_err()
        );
        release.notify_one();
        withdrawal.await.unwrap().unwrap();
        memory_retirement.wait().await.unwrap();
        assert_eq!(memory.stats().allocations, 0);
        assert_eq!(memory.stats().bytes, 0);
        let replacement = manager.activate("request_policy", "policy").await.unwrap();
        assert!(replacement.generation.unwrap() > generation);

        manager.close().await.unwrap();
        registration.revoke();
        listener.shutdown().await.unwrap();
    })
    .await
    .expect("remote membership cancellation test timed out");
}

#[test]
fn message_grants_bind_codec_and_method_shape() {
    let grant = Grant::new("sub2api".into(), "openai-oauth".into(), &[])
        .with_codec(Some("codex.responses.v1".into()));
    assert_eq!(
        grant
            .message_method("codex.responses.v1/response.create/0000000000000001/0000000000000002")
            .unwrap(),
        "worker.message.v1.openai-oauth"
    );
    for method in [
        "other.v1/response.create/0000000000000001/0000000000000002",
        "codex.responses.v1/native.action/1/2",
        "codex.responses.v1/response/create/0000000000000001/0000000000000002",
    ] {
        assert!(grant.message_method(method).is_err());
    }
    assert!(
        Grant::new("a".into(), "b".into(), &[])
            .message_method("codex.responses.v1/response.create/0000000000000001/0000000000000002")
            .is_err()
    );
}

#[tokio::test]
async fn remote_message_carries_large_raw_payload_and_keeps_metadata_bounded() {
    use bytes::Bytes;
    let max = 6 * 1024 * 1024;
    let method = "codex.responses.v1/response.create/0000000000000001/0000000000000002";
    let router = Router::new();
    let registration = router
        .register(vec![Action {
            contract: Contract {
                id: "worker.message".into(),
                version: 1,
                response: ResponseMode::Unary,
                input: Schema::Bytes,
                output: Schema::Bytes,
                max_input: max + 258,
                max_output: max + 65541,
            },
            bindings: vec![Binding::LocalControl {
                action: "worker.message.v1.openai-oauth".into(),
            }],
            handler: Arc::new(AsyncHandler(move |input: Vec<u8>, _: Context| async move {
                let n = u16::from_be_bytes(input[..2].try_into().unwrap()) as usize;
                assert_eq!(&input[2..2 + n], method.as_bytes());
                assert_eq!(input.len() - 2 - n, max);
                assert!(input[2 + n..].iter().all(|b| *b == 0xa5));
                Ok(vec![0])
            })),
        }])
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("messages.sock");
    let listener = ControlListener::prepare(
        tokio::net::UnixListener::bind(&path).unwrap(),
        None,
        ControlService::new(Arc::new(router), max + 65541, Duration::from_secs(5), 4).unwrap(),
    )
    .unwrap();
    listener.activate().unwrap();
    let manager = RemotePluginManager::connect(
        BTreeMap::from([(
            "sub2api".into(),
            AxisBinding {
                endpoint: format!("unix://{}", path.display()),
                max_inflight: 2,
                max_message_bytes: max,
                connect_timeout_ms: 5000,
                shutdown_timeout_ms: 5000,
            },
        )]),
        Surface::new(vec![
            Grant::new("sub2api".into(), "openai-oauth".into(), &["routing".into()])
                .with_codec(Some("codex.responses.v1".into())),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let binding = manager.binding("sub2api", "openai-oauth").unwrap();
    assert_eq!(
        binding
            .message(
                method,
                Bytes::from(vec![0xa5; max]),
                Duration::from_secs(5),
                ()
            )
            .await
            .unwrap(),
        vec![0]
    );
    assert!(
        binding
            .message(
                method,
                Bytes::from(vec![0; max + 1]),
                Duration::from_secs(5),
                ()
            )
            .await
            .is_err()
    );
    assert!(
        binding
            .call("routing", vec![0; 65537], Duration::from_secs(5), ())
            .await
            .is_err()
    );
    manager.close().await.unwrap();
    listener.shutdown().await.unwrap();
    drop(registration);
}
