check:
    cargo fmt -- --check
    cargo test --locked
    go test tools/plugin-contract/main.go tools/plugin-contract/main_test.go

# Show the callback decisions and reasons from a real TCP workflow.
bdd:
    cargo test --locked --test notifier_workflow --test notifier_lifecycle --test native_proxy --test operational_events --test access_logging --test response_dumps --test graceful_upgrade -- --nocapture

# Native traffic, event subscriptions, access persistence and bounded body dumps.
bdd-observability:
    cargo test --locked --test native_proxy observability_workflow:: -- --nocapture
    cargo test --locked --test operational_events --test access_logging --test response_dumps -- --nocapture

# Curated, optional runtime-loadable Tools; no static linkage into the worker.
build-postgres:
    cargo build --locked --release --manifest-path ../infra/rust/Cargo.toml -p storage-postgres-plugin
    mkdir -p plugins
    cp ../infra/rust/target/release/libstorage_postgres_plugin.so plugins/

build-cache:
    cargo build --locked --release --manifest-path ../infra/rust/Cargo.toml -p cache-redis-plugin
    mkdir -p plugins
    cp ../infra/rust/target/release/libcache_redis_plugin.so plugins/

build-browser:
    cargo build --locked --release --manifest-path ../infra/rust/Cargo.toml -p browser-client-plugin
    mkdir -p plugins
    cp ../infra/rust/target/release/libbrowser_client_plugin.so plugins/

# Explicit convenience selection; individual recipes remain independent.
build-plugins: build-postgres build-cache build-browser
