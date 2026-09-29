# Pingora worker

A reusable worker-host project built from infra components. It contains no
Sub2API, OAuth, billing, model, mimic, or provider scheduling policy. Its library
is used by a release application's executable: run the selected composition with
`pingora_worker::run`, and give `Process<H>` the installed request handler and
committed plugin runtime. This is a separate host project, not another proxy hop.

## Ownership and calls

- **Worker axis:** infra's Pingora carrier, concurrency/drain mechanics, worker
  management state, semantic notifier contracts, and native egress provider port.
  A contract/trait belongs to the worker surface; its installed implementation
  and resource lifetime belong to plugin composition.
- **Plugin axis:** the runtime dynamically composes Object/Function/Tool instances,
  installs notifier implementations, retains tool/client resources, and owns
  HTTP/gRPC entry handles and reversible action registration. Each Object plugin
  orchestrates its own useflow through explicitly injected Tools.
- **Function callbacks:** compiled functions or optional V8/Lua functions return
  decisions from worker notification facts. They are not substitutes for the
  Object plugin's independently initiated workflow. No VM enters the proxy data
  path by default.

    Pingora ingress -> runtime-installed callback chain -> native egress provider
    Plugin Object useflow -> declared Tool binding -> effect
    Internal gRPC -> shared router -> worker action OR explicit plugin action

Tool calls do not traverse the notifier chain. Worker callback exposure is not an
HTTP/gRPC route allowlist. Action contracts are distinct from transport bindings;
entry registration belongs to plugin composition. PG/cache Tool dependencies are
injected directly in the plugin axis. There is no universal RequestPlan layer.

`entry::install` binds an internal listener, registers worker management and
application-supplied plugin actions transactionally, and delays serving until
composition commits. Closure revokes routes, signals the listener, drains calls,
and releases resources. TCP is loopback-only. A Unix listener never removes an
existing socket, symlink or regular file to take its place. Cleanup removes only
the inode this installation created; stale sockets require explicit operator
cleanup. Socket mode/group are deployment inputs, not business names in the host.

## Native boundary

The synchronous C ABI is the required effect boundary for Go Object plugins.
Arguments and replies use copied values/opaque handles; no Go/Rust runtime object
or Future crosses it. Runtime binding dispatch verifies the calling instance owns
the binding ID. Method validation remains the Tool's contract. Caller-local
async/await retains leases until the native operation actually returns; cancelling
an await does not authorize freeing an in-flight handle. Logical closure,
resource draining, and physical library unloading are separate operations.

Native in-process libraries are **trusted code, not an OS sandbox**. Object code
must not create its own sockets, RPC transports or filesystem side channels.
The `tools/plugin-contract` build guard rejects direct net/rpc/fs entry references
(including aliases and function capture) and unauditable dot imports. It checks
selected first-party entry sources, not transitive dependencies, dynamically
constructed behavior, arbitrary C code, or malicious syscalls. Bootstrap config
and existing business dependencies still require review; this guard is not proof
that every transitive side effect has been migrated. Hard denial for untrusted
native libraries requires a separate constrained process and mediated tools;
that isolation mode is not implemented here. Do not advertise it as enforced.

The Rust native egress stream remains Rust-local through the provider trait.
For a client implemented in another language, reuse infra's framed stdio primitive
and provider adapter; do not add a front proxy or tunnel ordinary Tool calls
through the notification chain.

## Curated optional Tool plugins

`plugins/packages.json` catalogs PostgreSQL, Redis + memory cache and the browser
client. These are optional builtin distributions, each a separate loadable `.so`;
the generic worker does not statically link, automatically load or require them.
Their implementations stay in the single infra checkout.

Choose `just build-postgres`, `just build-cache` or `just build-browser` to build
only that Tool into `plugins/`. `just build-plugins` is an explicit convenience
selection of all three. Listing a package does not grant a capability: deployments
select artifacts, and loaded instances independently declare their provider bindings.

Sub2API's base business bundle contains only its Go Object. Its OAuth business
bundle contains two Go Objects. The application deployment separately selects
the needed curated Tools and injects their capabilities. Missing required bindings
fail before loading; there is no implicit backend fallback.

The worker and its release application must resolve the same infra checkout;
use the existing sibling `infra` workspace rather than a second clone. The host
is a library project; release binaries select their application at build time.
The selected build controls hook scope; environment variables only supply runtime
settings and credentials. Run `cargo test` for host checks and
`go test tools/plugin-contract/main.go tools/plugin-contract/main_test.go` for the
source boundary checker. Both axes retain independent useflows and meaning cores.
