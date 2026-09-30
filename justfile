check:
    cargo fmt -- --check
    cargo test --locked
    go test tools/plugin-contract/main.go tools/plugin-contract/main_test.go

# Show the callback decisions and reasons from a real TCP workflow.
bdd:
    cargo test --locked --test notifier_workflow --test notifier_lifecycle --test native_proxy -- --nocapture

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
