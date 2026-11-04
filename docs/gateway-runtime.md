# Remote storage gateway

The gateway is a product composition of the Worker client contract and Infra's
L7 server. It loads `client-openlist.so` and `client-rclone.so` through Infra's native ABI. Provider instances are constructed only in the approved plugin runtime. The
standalone Worker excludes CP dependencies; the combined ELF links both roles
while keeping their execution in separate processes. CP code is not on the
payload path.

Build with the local Go/Node/pnpm entry described in the repository README and
[build instructions](../build/README.md). Select current independent Infra,
Worker and rclone checkouts explicitly. Derived upstream sources, temporary
local module replacements and build receipts stay in the selected build output;
the canonical module cache is not patched.

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

Concrete origin metadata probes cover S3 and WebDAV. Target registration is
operator controlled and does not define plugin configuration forms or schemas.
No arbitrary RC method, remote URL or object-body forwarding API is registered.
The management SDK and secret-file resolver are pure Go and do not import the
native loader. In managed mode, OpenList retains identity, SQLite storage
records, task history and administration, but disables legacy provider
initialization, payload tasks and file-serving routes. An existing unmanaged
OpenList deployment has a separate migration/retirement boundary; its legacy
HTTP/S3 listeners are not part of the managed Gateway profile.

`make admin` builds the standalone-Worker operator. Its Secret allowlist and
supplied systemd unit target that standalone profile; they must not be pointed at
the managed CP Secret containing bundle/control/targets configuration. Bundle profile
Secret publication and rollout stay an explicit deployment operation. The timer
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

Local native builds produce the Gateway ELF, independent Worker/CP entry points,
dynamic client/transfer plugins, embedded UI and optionally the independent
rclone executable. Their receipts identify actual source and tool inputs. The
retained Bazel/BuildBarn OCI recipes and historical candidate receipts describe
an earlier build path; the native entry does not invoke them or GitHub Actions.
Deployment templates require explicitly selected image artifacts, private Secret
volumes and a database PVC. Building does not select or replace a live deployment.

## Source-built UI and optional local materialization

Infra `storage/blob/rclone` owns generic bounded filesystem/object access and
archive access. Gateway selects S3/WebDAV/Drive backends and declared secret
references, then maps entries, cursors, errors and lifecycle to the Worker port.
The independent rclone product owns VFS, cache handles and process retirement.
Infra's `go.mod` selects the upstream rclone module version; its ordered
`go/patches/rclone` inventory supplies the archive/VFS source profile during
assembly. Retired `storage/cloud` and `storage/vfs` packages are not restored.

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
VFS lives in the independent `github.com/wignerStan/rclone/go` module. Gateway
imports its passive Box configuration; the external source build produces the
native `rclone serve managed-vfs` command. There is no Gateway VFS runtime.

The Manager loads `gateway manager --config /path/to/bundle.json`.
`control_planes` select CP configuration and private data directories; `workers`
select Worker executables/configurations; optional `materializers` select
external rclone executables and `box.ProcessConfig`. The CP loads only
`OPENLIST_GATEWAY_CONTROL_FILE`, which supplies assigned Worker endpoints and
finite transfer policy. It never launches, signals or reaps another role.

A materializer's `remote` names only the Worker IPC/offer sockets, raw namespace
and expected Worker ID/incarnation. After CP and Worker readiness, the Manager
pins actual endpoints from the Worker's Info, starts the external Box and
checks its reported assignment before the optional single generation-CAS
activation. Origin URLs and provider credentials have no representation in that
backend assignment. `runtime_directory` can create a private child of a runtime
volume. `gateway manager --config /path/to/bundle.json --check-config` passively
checks the assembly without starting children or initializing providers.

The backend uses bounded ttrpc metadata plus an authenticated Unix seqpacket
shared-memory bootstrap. Worker owns allocation, exact generation leases and
process recovery. CP supplies routing decisions without carrying bytes. The
private native rclone frontend remains an optional read-only Worker client
endpoint. Its local DAV credential is independent of Worker ingress/TLS secrets.
The previous private raw HTTP Unix data socket is removed.

For cache controls and immutable ZIP placement, see [Small files](small-files.md)
and the external rclone application's configuration. The cache remains
regenerable and distinct from backup, retention and hot/cold placement. A
`materializers[].activate_client` selection must bind a read-only mount with a
distinct `remote_client` and the same mount name as `remote.namespace`; it is
published only after external Box readiness. Ordinary mounts can share the
Worker without a `remote_client`. Only explicitly assigned raw clients enter
the storage IPC namespace map; it never falls back to a public cached client.

A materializer session pins its raw client's exact generation. Replacing that
raw generation makes the old session stale, including cache-hit checks. The
current runtime requires a new materializer/session to consume the replacement;
it does not migrate an existing cache session automatically. Activating the
separate cached client changes only that client's generation and preserves the
raw session used by the materializer.

`/api/admin/control/instances` and its manifest/descriptor/observed/client routes
coordinate assigned Workers over local ttrpc or remote mTLS/gRPC. They expose no
process lifecycle operations. Legacy configured metadata targets remain separate
from these assignments. Exact descriptor bytes and expected versions cross the
control boundary unchanged. A rejected replacement returns HTTP 409 and does
not retry with a refreshed generation. Entries generate schema through the
existing schema effect tool; no server or provider owns another copy of it.

For encoded transfer, `encoded.artifact` selects the independently installed
`transfer-reapi.so`. The operator supplies a finite logical upstream inventory,
source/target IDs and metadata allowlist. The Control Plane grants method and
budget decisions; Worker resolves an approved upstream locally. TLS and ingress
authentication apply to the exposed gRPC listener. Official REAPI and ByteStream
method/cardinality descriptors belong to the concrete codec plugin, while Core
owns frame leases, native admission and mechanical carrier operations.
