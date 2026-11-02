# Remote storage gateway

The gateway is a product composition of the Worker client contract and Infra's
L7 server. It loads `client-openlist.so` and `client-rclone.so` through Infra's native ABI. Concrete
providers are absent from the gateway executable's dependency graph. OpenList
control-plane code is not on the payload path.

Build with the workspace described in the repository README:

```sh
make INFRA_ROOT=/path/to/reviewed/infra BUILD_DIR=/tmp/trade-remote-gateway-build plugin worker vfs rclone transfer
```

The ELF SHA256 approves executable bytes; the descriptor SHA256 approves exact
descriptor bytes. The descriptor's build digest identifies source/build inputs,
not the ELF itself. Inspect an installed ELF with the Worker CLI before placing
both hashes in the operator bootstrap. Installation must be immutable and
operator controlled; the native ABI is not a sandbox.

The private JSON configuration embeds the Worker-owned bootstrap directly:

```json
{
  "listen": ":8080",
  "username": "operator",
  "auth_secret": "ingress-password",
  "worker": {
    "schema": "network-data-plane-worker/bootstrap/v1",
    "artifacts": [{
      "path": "/opt/gateway/client-openlist.so",
      "sha256": "<ELF SHA256>",
      "descriptor_digest": "sha256:<exact descriptor SHA256>",
      "plugin_id": "client.openlist",
      "language": "go"
    }],
    "clients": [{
      "name": "backups",
      "plugin_id": "client.openlist",
      "descriptor_digest": "sha256:<exact descriptor SHA256>",
      "implementation_id": "s3",
      "public": {"endpoint": "http://storage:18443", "bucket": "forgejo", "region": "us-east-1", "force_path_style": true},
      "secret_refs": {"access_key_id": "s3-access", "secret_access_key": "s3-secret"}
    }],
    "secret_files": {
      "ingress-password": "/run/gateway-secrets/ingress-password",
      "s3-access": "/run/gateway-secrets/s3-access",
      "s3-secret": "/run/gateway-secrets/s3-secret"
    }
  },
  "mounts": [{"name": "backups", "client": "backups"}]
}
```

Secret files must be regular, owner-only files (0400/0600), at most64KiB.
Provider instances can resolve only the credential references selected in the
operator's initial client assignments or explicit `client_secret_refs` preapproval.
Reserved ingress/TLS references and their path/inode aliases are rejected. Ingress authentication and TLS-key
references stay outside that resolver and cannot be granted by desired state.
Ingress passwords need at least16bytes. The initialized deployment terminates
TLS directly in the data-plane L7 server. Configurations and
credentials do not belong in Git. `-check` prepares real clients and can contact
providers; the Worker CLI's `--check-config` validates passive descriptors only.

Authenticated management endpoints are `/v1/manifest`,
`/v1/descriptors/{digest}` (exact bytes), `/v1/state`, and
`POST /v1/clients/{name}` with `{ "expected": <current Version>,
"selection": <Worker Selection> }`. Only preinstalled approved artifacts and
configured names are available. A stale incarnation/generation is rejected.
Each HTTP exchange pins its generation through transfer completion; replacement
publishes a validated candidate, then drains the old instance. Cancelling a
waiter never proves native resource release.

For the control-plane UI and administration API, configure the same ingress:

```json
{
  "listen": ":8443",
  "tls": {
    "certificate_file": "/run/gateway-secrets/tls.crt",
    "private_key_secret": "tls-key"
  },
  "control_plane": {
    "host": "incus.tail5ced8c.ts.net",
    "upstream": "http://openlist.selfhost.svc.cluster.local:5244"
  }
}
```

This fragment extends the full configuration above; declare `tls-key` in the
Worker bootstrap's secret-file map. A control-plane route requires TLS and an
exact host. Infra's native reverse proxy preserves request bodies, cookies,
authorization, paths and queries, strips incoming forwarding identity, and
rebuilds it from the actual connection. The control plane retains its existing
login/admin authorization. Worker `/v1` and storage `/dav` use their own
registrations/authentication and never fall through to the control plane.
The proxy admits static administration UI, identity and `/api/admin` routes;
legacy download/media/file-data routes do not fall through to it. No Nginx or
second TLS terminator is required. An optional Tailscale **raw TCP** forwarder
can expose the NodePort while retaining data-plane TLS termination.

The OpenList process configures management targets through
`OPENLIST_CONTROL_TARGETS_FILE`. It registers `/api/admin/control` inside its
existing administrator-authenticated router. The target kinds are `worker`,
`rclone`, `origin-s3` and `origin-webdav`; only configured endpoint identities
and private secret-file references are accepted. The initial APIs are:

| Method and relative path below `/api/admin/control` | Purpose |
| --- | --- |
| `GET /targets` | Passive names/kinds/operations, without credentials |
| `GET /targets/{worker}/manifest` | Worker-owned manifest |
| `GET /targets/{worker}/descriptors/{digest}` | Exact approved descriptor bytes |
| `GET /targets/{worker}/observed` | Current and draining generations |
| `POST /targets/{worker}/clients/{name}` | Typed expected-version replacement |
| `GET /targets/{rclone}/version`, `/stats`, `/jobs`, `/jobs/{id}` | Bounded RC metadata |
| `POST /targets/{rclone}/jobs/{id}/stop` | Explicit job control with process identity check |
| `GET /targets/{origin}/probe` | S3 HeadBucket or WebDAV root metadata |

Concrete origin SDKs initially cover S3 and WebDAV. Target registration is
operator controlled and does not define plugin configuration forms or schemas.
No arbitrary RC method, remote URL or object-body forwarding API is registered.
The management SDK and secret-file resolver are pure Go and do not import the
native loader. Existing OpenList legacy WebDAV/S3 addresses remain available
for clients whose storage plugins have not yet migrated; this is the staged
management integration, not completion of every legacy provider migration.

`make admin` builds the native operator. The supplied systemd service/timer
runs `gateway-admin tls-sync` every12hours: obtain/check the Tailscale certificate,
validate its key/domain/validity, publish only allowlisted configuration/secrets,
then roll the Deployment. The successful payload fingerprint is committed after
the rollout succeeds; unchanged payloads skip mutation and failed rollouts are
retryable. Provider configuration does not pass through a shell runner.

`/dav/{mount}` supports GET, HEAD, PROPFIND (Depth0/1), PUT, DELETE and OPTIONS.
It streams object bodies with backpressure. Root browsing lists configured
mounts. Listings stop at10,000entries. Unsupported recursive deletion, locks,
MKCOL, MOVE/COPY and atomic conditional mutations are rejected. This ingress is
not a complete WebDAV server or S3 frontend. It does not implement backup,
retention, hot/cold placement or a Kopia repository protocol.

Go native runtimes use Retain: closing instances joins their work and releases
instance resources, while loaded code remains mapped. Infra bounds native owner
slots at64; exhausted capacity fails closed and requires a drained process
replacement. This is not unlimited in-process Go code unloading.

The native OCI recipes use rules_img and Buildbarn, with local fallback disabled.
Reviewed binary/DSO/frontend/CA inputs stay outside the source tree. All three images
include their programs and trust bundle, run as1001:1001 and export complete OCI
archives. See the monorepo candidate receipt under `docs/validation/` and `build/README.md`.
The supplied deployments use verified image digests, private Secret volumes and
the existing database PVC; executable/frontend host mounts are unnecessary.

## Source-built UI and optional local materialization

Infra `storage/cloud` owns the backend and remote operations; `storage/vfs`
owns optional cache handles and retirement. The product adapts their results to
the Worker port. Infra's `go.mod` selects the rclone package version, and its
ordered patch inventory supplies the VFS source profile during assembly.

The control-plane binary embeds the pinned frontend built from source. Its
deployment must omit `OPENLIST_DIST_DIR`, and an existing persisted config's
`dist_dir` must be empty before selecting the embedded-UI candidate. The supplied
YAML is a deployment template; the candidate image receipt is not a rollout.
Keep the existing PVC and administrator identity. The managed template has a
separate Deployment/Service identity and requires an explicit PVC selection; it
does not silently replace existing HTTP/S3 endpoints. It supplies private HTTP
bind and disabled-HTTPS environment values; keep persisted `force:false` so the
upstream configuration loader applies those overrides. Reusing a database requires
retiring its previous writer and completing endpoint migration. This candidate
does not implement a legacy-compatible S3 frontend.

`client.rclone` streams remotely. Its former `materialization` option is rejected.
Materialization now runs only in the independent `storage-vfs` process.

The Control Plane loads `OPENLIST_GATEWAY_PROCESSES_FILE`. Its `workers` list
contains executable paths and passive Worker process configuration; optional
`vfs` entries contain a separate executable and VFS process configuration.
`runtime_directory` can create a private child of an existing runtime volume.
The supplied managed deployment uses `/run/gateway/control`.

A VFS entry names `worker_instance_id` and `worker_control_socket`; its
`data_endpoint` selects `/remote-dav/<mount>/`. The manager derives the real
origin from the ready Worker's typed Info response, so a Worker can bind port0.
Standalone VFS config requires that complete observed Data Plane URL. VFS rejects
provider URLs, foreign worker identities and recursive cache paths.

VFS has separate `local_username` / `local_password_file` and outbound
`data_username` / `data_password_file`. Its local listener is loopback-only.
`directory`, `max_bytes` and `max_age_seconds` govern the regenerable cache.
The byte target controls eviction; the deployment volume separately bounds disk
capacity. This remains distinct from backup, retention and hot/cold placement.

An optional `activate_client` contains an exact Worker-owned Selection. After VFS
is ready the manager compares against the observed generation and publishes that
selection. Its mount must be read-only and bind a distinct `remote_client`;
activation of writable or recursive assignments fails closed. Public cache reads
traverse Worker `/dav`; remote cache misses traverse its `/remote-dav` route.
The source-built process test verifies cache hits without extra origin reads and
both actual child reaps. It does not claim write-through cache support.

`/api/admin/control/instances` and its manifest/descriptor/observed/client routes
manage local children over ttrpc. Legacy configured remote management targets
remain separate from these local instances. Entries generate schema through the
existing schema effect tool; no server or provider owns another copy of it.

For encoded transfer, `encoded.artifact` selects the independently installed
`transfer-reapi.so`. The operator supplies a finite logical upstream inventory,
source/target IDs and metadata allowlist. The Control Plane grants method and
budget decisions; Worker resolves an approved upstream locally. TLS and ingress
authentication apply to the exposed gRPC listener. Official REAPI and ByteStream
method/cardinality descriptors belong to the concrete codec plugin, while Core
owns frame leases, native admission and mechanical carrier operations.
