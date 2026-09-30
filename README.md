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

`entry::install` binds one selected internal endpoint. `Registration::worker_control`
explicitly chooses whether that listener installs worker management under the
reserved `worker` namespace. Use `None` for a plugin-only listener: it receives
no worker actions automatically. `SharedEntry::install_actions` and
`install_native_actions` register each axis's actions into that same router as
reversible resources in its own composition. Serving begins after `activate`;
additional owners can register while the entry is live.

`composition::mount_axis` mounts a committed child runtime as an explicit parent
resource. Each child keeps its own local resource names, bindings and useflow;
cross-axis/shared dependencies belong in the parent resource schema. Several axes
can therefore use the same internal resource names without sharing ownership.
The parent composition fixes its mounts at commit; the shared entry does not itself
hot-add child runtimes to that committed parent. `Process::close_axis` closes a
mounted runtime in dependency order. Live dependents prevent unsafe closure.

On a shared listener, closing an action owner withdraws only its routes, not the
listener or another owner's routes. An axis can instead own its own entry/UDS;
closing that axis withdraws and drains its routes, then releases its listener. In-flight unary calls and streaming producers retain leases until they
really finish; route revocation notifies streaming responses and interrupts
backpressure waits. The entry reserves each namespace through draining, then
allows reuse. Failed registration releases its reservation; old generations
cannot revoke replacement routes. Contract IDs are local to the namespace, while
actual HTTP paths/gRPC names stay explicit and unique within that listener. A conflicting
batch fails atomically. These namespaces express ownership, not permissions.

Different axes use different HTTP/gRPC bindings on a shared listener **or**
separate TCP/Unix endpoints with independent routers. Sharing entry implementation
does not require sharing a listening socket. Independent listeners may use the
same action/contract names; their publication and shutdown ownership stays local.
The `axis_endpoints` regression uses two Unix sockets in one parent composition,
checks that a duplicate socket bind cannot replace its owner, and closes one axis
while the other continues serving. No host-entry-to-plugin-entry forwarding hop
is introduced.

Whole-host closure stops registration, withdraws all routes, signals the listener,
and drains calls before releasing resources. A closed entry rejects new axes.
Socket bind, endpoint validation, Unix mode/group, stale-path policy and
inode-checked cleanup come from infra's `network-sockets`. This project selects
the compiled local-listener policy and owns its serving lease; it has no duplicate
socket implementation. Stale recovery is explicit and never removes a live
listener or a non-socket path. Socket mode/group are deployment inputs.

The worker notifier exposes typed socket decisions at ingress use/reuse
boundaries. An application Handler projects those into its installed chain;
infra applies closure without leaking raw descriptors to plugins. The same worker flow also exposes infra dial, keyed pool and byte-bridge controls;
see [worker networking](../infra/rust/crates/networking/proxy/pingora/src/worker/README.md).
These explicit operation hooks do not transfer ownership of a shared management listener to a business axis.

Callback order is fixed at chain construction. Adapters can declare supported
hooks with `Callbacks::supports`; the chain builds a per-hook index once and
skips unrelated callbacks before creating their futures or entering an ABI.
Each participating callback borrows the same facts. The first decision or error
stops dispatch. Completion stays with the admitting callback and can start only
once, including when its first attempt fails or is cancelled; this is not a
guarantee that completion finishes.

Run `just bdd` to see `DECISION`, `WHY`, and `FACTS` for authentication, admission,
dial, completion, and a refused completion retry over a real TCP exchange.
The scenario buffers its trace and prints once. Shared infra credential policy
hashes API/auth credentials and selected session cookies while preserving ordinary
route/theme/affinity cookies. Additional credential field and session-cookie names
are explicit policy inputs. Runtime notifier traces use the
`proxy_pingora::worker::notifier` target and contain callback indices and decisions,
without serializing request facts.

For managed HTTP requests, `RequestFlow::execute_http` runs protocol validation,
header/metadata transforms, authentication, routing/admission, request-body setup,
upstream encoding, one installed egress call, upstream-response hooks,
response-body setup and downstream encoding. All interested transform callbacks
run in order on the same owned request/context. The returned `HttpExchange`
retains stream ownership; call `finish` only after downstream delivery completes.
Failures stop later hooks, and dropping the exchange cancels without reporting
successful completion. The carrier retains raw protocol parsing responsibility.

Body setup configures native pull-time transforms; it never collects or serializes
a body to notify plugins. The C ABI carries bounded non-streaming metadata/control
frames and explicit resource handles while async response streams stay with the
native host/egress owner. Borrowed frame views can be passed through synchronous
callbacks without copying; they cannot outlive their invocation.

The reusable `diagnostic-output` runtime provides separate sinks for diagnostic
traces, `proxy_pingora::access` records, and opt-in `proxy_pingora::body_dump`
records. Assembly configures access-file retention independently from body dumps
and retains the logging guard through shutdown. Trace verbosity does not enable
body capture. The HTTP carrier emits access metadata without headers, queries or
bodies; consumed sessions keep unavailable status/byte counts explicitly unknown.

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
