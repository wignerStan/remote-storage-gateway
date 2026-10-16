check:
    cargo fmt -- --check
    cargo test --locked

# Show the callback decisions and reasons from a real TCP workflow.
bdd:
    cargo test --locked --test notifier_workflow --test notifier_lifecycle --test native_proxy --test operational_events --test access_logging --test response_dumps --test graceful_upgrade -- --nocapture

# Native traffic, event subscriptions, access persistence and bounded body dumps.
bdd-observability:
    cargo test --locked --test native_proxy observability_workflow:: -- --nocapture
    cargo test --locked --test operational_events --test access_logging --test response_dumps -- --nocapture

# Actual native carriers, SDK error frames, and Codex interrupt continuation.
bdd-codecs:
    cargo test --locked --test native_proxy api_codecs:: -- --nocapture
    cargo test --locked --test native_proxy response_streams:: -- --nocapture
    cargo test --locked --test native_proxy realtime_codecs:: -- --nocapture
