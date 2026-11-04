check:
    cargo fmt -- --check
    cargo test --locked --features l7

# Show the callback decisions and reasons from a real TCP workflow.
bdd:
    cargo test --locked --features l7 --test notifier_workflow --test notifier_lifecycle --test native_proxy --test operational_events --test access_logging --test response_dumps --test graceful_upgrade -- --nocapture

# Native traffic, event subscriptions, access persistence and bounded body dumps.
bdd-observability:
    cargo test --locked --features l7 --test native_proxy observability_workflow:: -- --nocapture
    cargo test --locked --features l7 --test operational_events --test access_logging --test response_dumps -- --nocapture

# Actual native carriers, SDK error frames, and Codex interrupt continuation.
bdd-codecs:
    cargo test --locked --features l7 --test native_proxy api_codecs:: -- --nocapture
    cargo test --locked --features l7 --test native_proxy response_streams:: -- --nocapture
    cargo test --locked --features l7 --test native_proxy realtime_codecs:: -- --nocapture

# Optional bounded loopback workload; reports counts and elapsed time, not capacity.
stress:
    cargo test --locked --features l7 --test native_proxy stress::repeated_concurrent_streams_and_disconnects -- --ignored --exact --nocapture
