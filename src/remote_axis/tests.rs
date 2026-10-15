use super::*;
use action_router::{
    Action, AsyncHandler, Binding, Context, Contract, ResponseMode, Router, Schema,
};
use network_actions::{ActionService, listener::ActionListener};
use network_sockets::BoundListener;
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
                bindings: vec![Binding::Grpc {
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
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = socket.local_addr().unwrap().to_string();
        let listener = ActionListener::prepare(
            BoundListener::Tcp(socket),
            None,
            ActionService::new(router, 1024, Duration::from_secs(5)).unwrap(),
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
        let memory =
            plugin_manager::memory::Coordinator::new(plugin_manager::memory::Limits::default())
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
