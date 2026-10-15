//! Optional Wasm Objects use the same assembled worker metadata and grants.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assembled_wasm_object_patches_only_explicitly_granted_headers() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join("metadata.wasm");
    std::fs::write(
        &artifact,
        wat::parse_str(include_str!("../fixtures/wasm_metadata_callback.wat")).unwrap(),
    )
    .unwrap();
    let received = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&received);
    let origin = listen(move |mut socket| {
        let observed = Arc::clone(&observed);
        async move {
            let request = String::from_utf8(head(&mut socket).await).unwrap();
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-policy: wasm-approved\r\n")
            );
            observed.fetch_add(1, Ordering::SeqCst);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await
                .unwrap();
        }
    })
    .await;
    for (grants, succeeds) in [(vec!["x-policy"], true), (vec![], false)] {
        let config = serde_json::from_value(json!({
            "upstream":format!("http://{}", origin.address),
            "callbacks":[{
                "backend":"wasm", "binding":"direct", "path":artifact,
                "package":"worker.wasm.metadata.test", "variant":"wasm",
                "language":"go", "hooks":["pre_routing"],
                "patch_headers":grants,
                "wasm_limits":{"max_memory_bytes":131072,"fuel_per_call":10000}
            }]
        }))
        .unwrap();
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        let response = get(worker.address).await;
        assert_eq!(response.starts_with(b"HTTP/1.1 200"), succeeds);
        if succeeds {
            assert!(response.ends_with(b"hello"));
        }
        drop(worker);
        proxy.shutdown().await.unwrap();
    }
    assert_eq!(
        received.load(Ordering::SeqCst),
        1,
        "denied patch must fail before upstream dispatch"
    );
}
