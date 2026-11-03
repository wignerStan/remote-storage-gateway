# Bundle and Kubernetes deployment

The same product has two process assemblies. Control planes own desired state,
plugin approval and transfer decisions. The Worker owns public TLS, reverse proxy
and dynamic client/codec execution. The manager is the parent process, separate
from both. Infra `process/supervisor` owns self-exec, dependency readiness,
process-group signals, actual child reaping and failure cleanup.

## Same host: one executable

`gateway manager --config /run/control-secrets/bundle.json` starts the same ELF as
`gateway control-plane server ...` and `gateway worker --config=-`. They have
separate PIDs and address spaces. This is Go self-exec through Infra, with no
shell or raw post-runtime fork. The manager supplies structured child config on
stdin. The CP receives metadata-only configuration; it imports no process box.

Startup waits for CP readiness, then Worker readiness, then optional independent
native rclone materializers. One Worker loads multiple approved `.so` plugins.
Multiple CPs can manage distinct usages through explicit assignments; overlapping
writers still use Worker generation CAS. The manager never makes a transfer
decision. Shutdown retires VFS first, then drains Workers, then stops CPs. Any
unexpected core role exit retires its group; an outer service/container owner
restarts the complete bundle. It does not silently revive a partial generation.

The optional rclone executable remains an independently built product. It is not
linked into the manager/CP/Worker ELF. A bundle image includes that optional tool
and native plugins beside the single gateway executable. No child executable is
extracted into `/tmp` at startup.

Each `control_planes` entry contains `id`, absolute `data_directory` and
`config_file`, a loopback `ready_url` such as `http://127.0.0.1:5244/ping`, and
`control`: the typed CP metadata assignment. `workers` entries contain a Worker
`config`; omit `executable` for same-ELF self-exec. Use the Worker configuration
in [gateway-runtime.md](gateway-runtime.md), including exact approved artifact
and descriptor digests. `runtime_directory` must be a private child of the mounted
runtime volume. Never put provider credentials in public config.

`deploy/managed-gateway.yaml` selects `//:bundle_image` and this manager command.
The public route ends at Worker TLS. CP management and optional VFS listeners
stay private. The CP SQLite PVC has exactly one writer.

## Kubernetes: independent process roles, gRPC between Pods

`deploy/microservices.yaml` runs Worker and CP in independent Deployments. Their
process lifecycles and replicas belong to Kubernetes; the CP starts no child.
`OPENLIST_GATEWAY_CONTROL_FILE` selects CP metadata-only mode. The separate
scheduler/build system publishes images, approved artifact inventories,
assignments and workload certificates; neither CP nor Worker builds an image.

Cross-Pod control uses generated protobuf Worker and Decisions services over
mutually authenticated **gRPC**. `contracts/control/rpc/control.proto` owns the finite wire
service. Info, manifest, observed state, exact descriptor bytes and generation-CAS
replacement go CP to Worker. Bounded transfer decisions go Worker to CP. No
object body, remote command, loader pointer or cloud credential operation exists.

The CP config uses `workers[].remote = {address, tls}` and optional `rpc`:

```json
{
  "workers": [{"id": "worker-0", "data_endpoint": "https://storage.example:8443",
    "remote": {"address": "storage-worker:9443", "tls": {
      "ca_file": "/run/workload/ca.crt", "certificate_file": "/run/workload/tls.crt",
      "private_key_file": "/run/workload/tls.key", "server_name": "storage-worker",
      "peer_uri": "spiffe://gateway/worker-0"}}}],
  "rpc": {"listen": ":9444", "tls": {
    "ca_file": "/run/workload/ca.crt", "certificate_file": "/run/workload/tls.crt",
    "private_key_file": "/run/workload/tls.key"},
    "peers": {"spiffe://gateway/worker-0": "worker-0"}},
  "rules": []
}
```

The Worker process config selects `rpc` with its server identity and exact allowed
CP URI identities, and `remote_broker` with the CP gRPC address, TLS identity and
CP `peer_uri`. `broker_socket` and `remote_broker` are mutually exclusive. Empty
rules deny every transfer. Principals come from verified certificate URI SANs,
not JSON fields or request headers. Server DNS and the exact peer URI are both
checked; plaintext, anonymous and unassigned certificates have no fallback.

Do not put a shared `emptyDir` or shared-memory path between different Pods.
Native VFS shared memory stays inside the bundle container (or an explicitly
co-located Pod with shared process namespace and private UDS volume), using the
existing UDS/ttrpc lease path; remote CP decisions still use gRPC. Until that co-location
is selected, the supplied remote profile omits VFS rather than substituting
network file payloads through the CP.

The example starts one Worker with multiple plugins and one CP. Additional CPs
have distinct configuration/state and explicit Worker assignments. Multiple
Worker replicas require distinct instance IDs and workload certificate mappings,
and concrete per-Worker endpoints; blindly increasing replicas behind one shared
identity/service is not a correct assignment or generation model. SQLite CPs
must not share their database file across replicas.

## Build and evidence

`make all` uses the existing shared Buildbarn Go/UI graph. `make images` includes
`//:bundle_image`, separate Worker and CP images and the independent rclone image;
OCI assembly uses `rules_img`. Source inputs and toolchains use the shared
AC/CAS, with no per-task Bazel user root or persistent private compiler cache.

A successful source build alone does not prove a live deployment. Validate the
actual executable identity/PIDs, CP database readiness, public TLS/authentication,
child-failure cleanup and remote mTLS denial paths before replacing a service.
