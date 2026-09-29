check:
    cargo fmt -- --check
    cargo test --locked
    go test tools/plugin-contract/main.go tools/plugin-contract/main_test.go

# Implementations stay in the single infra checkout. This project owns artifacts only.
build-plugins:
    cargo build --locked --release --manifest-path ../infra/rust/Cargo.toml -p storage-postgres-plugin -p cache-redis-plugin
    mkdir -p plugins
    cp ../infra/rust/target/release/libstorage_postgres_plugin.so ../infra/rust/target/release/libcache_redis_plugin.so plugins/
