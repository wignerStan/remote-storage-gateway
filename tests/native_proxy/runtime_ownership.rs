//! A native notifier must execute inside the assembling worker process.
use super::*;
use proxy_pingora::http_ingress::ProxyLifecycle;
use serde_json::json;
use std::{path::Path, process::Command};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notifier_initialization_and_dispatch_share_the_worker_process() {
    let dir = tempfile::tempdir().unwrap();
    let library = dir.path().join("same-process.so");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg(format!("-DTEST_EXPECT_PID={}", std::process::id()))
            .arg(root.join("tests/fixtures/metadata_callback.c"))
            .arg("-o")
            .arg(&library)
            .status()
            .unwrap()
            .success()
    );
    let origin = origin(Arc::new(AtomicUsize::new(0))).await;
    for scheduling in [None, Some("inline")] {
        let mut callback = json!({"path":library,"package":"worker.metadata.test","hooks":["pre_routing"],
        "config":{"version":1,"decision":"continue"}});
        if let Some(scheduling) = scheduling {
            callback["scheduling"] = json!(scheduling);
        }
        let config = serde_json::from_value(json!({
            "upstream":format!("http://{}",origin.address),
            "callbacks":[callback]
        }))
        .unwrap();
        let proxy = pingora_worker::assembly::assemble(config).await.unwrap();
        let worker = serve_engine(proxy.clone()).await;
        assert!(get(worker.address).await.ends_with(b"hello"));
        drop(worker);
        proxy.shutdown().await.unwrap();
    }
}
