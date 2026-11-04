# Reproducible product derivation

This monorepo owns the Worker, gateway composition, concrete client plugin
entries and the OpenList patch stack. Official OpenList and its frontend are
pristine exact submodules below the product's `third_party/` directory. Infra
owns the cloud filesystem and optional VFS adapters, the required rclone Go
package version and its ordered patch stack. The product consumes that selected
Infra input read-only; neither repository pins a rclone Git commit or submodule,
and the product has no second rclone patch stack. The
former whole OpenList client fork is not a build dependency.

`SOURCE_RELATIONSHIPS.md` in the Git skill defines this relationship. The
product records are `.git-source-relationships/{openlist,openlist-ui}.json`
and `build/source-history.json`. OpenList records bind the official URL, exact gitlink,
ordered patch SHA-256 values and resulting Git tree. The source tool rejects a
dirty upstream, mismatched pin, patch hash, patch order or resulting tree. It
uses reference clones with dissociation in an external build directory. Git
cannot use a shallow source as a reference alternate, so an explicitly detected
shallow upstream uses a local clone preserving that shallow boundary; the
receipt records this mode. It
never sets Git `safe.directory` or applies patches in the canonical submodule.

## Preserved implementation and extraction

The first OpenList patch exactly reproduces the accepted control-plane source
tree at `cdddb733`, including the existing object-ID and Drive fixes and typed
management entries. The second moves the selected client adapters and tests
from `f5d3a4ce` into that same derived upstream Go module. Its plugin main uses
the shared first-party `nativeclient/provider` bridge. It does not carry another
copy of the C ABI implementation.

The third patch extracts the existing `drivers/s3.initSession` static-credential
initializer into public `pkg/storage/s3session`. Both the legacy driver and the
native client call that kernel. The streaming S3 port adapter is parent-owned
code; it is not presented as extraction of the complete upstream S3 driver.
WebDAV reuses the upstream `pkg/gowebdav` client with request-local context and
response cleanup patches. Other OpenList drivers remain source dependencies of
the legacy control plane, not advertised native client implementations.

The fourth patch puts schema derivation and descriptor publication in
`pluginentry`. The implementation continues to own its exported configuration
models, while `clientplugin` receives immutable descriptor values and performs
provider operations. Its descriptor getter returns independent copies; it
does not import the schema application-support tool. Invalid native publication
leaves the shared bridge unregistered so the loader can reject the artifact
without aborting the host during `dlopen`.

Go's internal-package boundary remains valid because CP and selected OpenList
client code are built from the same derived `github.com/OpenListTeam/OpenList/v4`
module. Worker contracts remain first-party and authoritative. The Worker host
dynamically loads the independently built `.so`; it does not import the
OpenList factory. Entry types and named operations still contribute to the
existing schema effect tool. Moving the sources does not introduce a second
descriptor, secret-field schema or HTTP framework contract.

## Build stages

Initialize the exact submodules, then select the reviewed Infra checkout and an
external output directory. The `-release` source-tool option additionally
requires clean exact product and Infra commits. An unfinished dev checkout is
recorded as dirty and cannot satisfy that release gate.

```sh
git submodule update --init --recursive
make MISE=mise INFRA_ROOT=/path/to/reviewed/infra \
  BUILD_DIR=/tmp/trade-remote-gateway-build source test-build
make MISE=mise INFRA_ROOT=/path/to/reviewed/infra \
  BUILD_DIR=/tmp/trade-remote-gateway-build plugin gateway rclone admin
make MISE=mise INFRA_ROOT=/path/to/reviewed/infra \
  BUILD_DIR=/tmp/trade-remote-gateway-build control-plane test-derived
```

OpenList source selection is closed to the two product-owned Git records.
Rclone is selected exclusively from the package version required by
`<INFRA_ROOT>/go/go.mod`. The version-free
`<INFRA_ROOT>/patches/rclone/series.json` records only the module name and
ordered patch checksums. Legacy rclone Git relationships, competing product
patches, patch path escapes and checksum mismatches fail admission.

`go mod download -json module@version` resolves that package outside either
owner's repository. The tool verifies the canonical module directory using
Go's `dirhash.HashDir` against the stock module checksums in Infra `go.sum`.
It copies verified files into external `source/rclone-module` before applying
patches. The global module cache remains untouched. The module-source receipt
records the selected version, integrity sums and measured derived content hash;
there is no expected Git commit or result-tree gate. Changed snapshots fail
integrity checks rather than being silently reset. Historical Git-derived
snapshots and receipts are not reused as module-version authority.

The native fingerprint binds Infra's own commit for library provenance plus its
actual Go module inputs, cloud/VFS storage sources, patch inventory and patch
bytes, together with product sources and recipe. Checksums protect integrity;
they do not select a competing package version.

The bootstrap script binds only enough existing first-party modules to run the
Go build tool. The tool then discovers actual module declarations using
`go mod edit -json`, resolves the required reviewed Infra modules and derived
third-party modules, and writes an isolated `go.work`. It never binds the old
standalone CP or client-fork workspaces or edits a global module cache.

For Infra validation with `GOWORK=off`, use an external `-modfile` derived from
the same reviewed module graph with only the admitted rclone source replacement.
Do not commit an absolute replacement. Go ignores replacement directives from
dependency modules, so this workflow does not claim that a plain downstream
module download automatically includes Infra's upstream patches. See the
[Go module replacement rules](https://go.dev/ref/mod#go-mod-file-replace). The owning
source assembly and the selected module version are distinct inputs.

`build/toolchain.json` pins development tools managed by mise. Install those
versions before building. Frontend dependencies use the pinned source's
`pnpm-lock.yaml` and `pnpm install --frozen-lockfile`; Vite builds that source.
The output is copied into the disposable OpenList `public/dist` before Go
compiles its existing `go:embed all:dist`. A content receipt binds the assets to
the UI commit, lockfile and toolchain, and CP compilation rejects changed
assets. No downloaded frontend release archive is a source-build result.

All materialized sources, caches, receipts and binaries stay under the chosen
`/tmp/trade-*` or `/var/tmp/trade-*` directory. A successful source receipt is
published only after tree verification. Reusing the directory rechecks admitted
source and rejects unrecorded code. A failed changed-input replay needs a fresh
output directory rather than an automatic destructive reset.

The built programs are:

| Output | Role |
| --- | --- |
| `bin/openlist-control-plane` | CP with the built UI embedded |
| `bin/client-openlist.so` | Selected OpenList-derived native client |
| `bin/client-rclone.so` | Default rclone remote client; optional local materialization |
| `bin/remote-storage-gateway` | Worker/gateway composition |
| `bin/gateway-admin` | Administrative bootstrap tool |

## Native OCI assembly

Root `BUILD.bazel` uses native `rules_img` 0.3.23. The official Debian
trixie-slim index is pinned to
`sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4`.
The gateway image contains its program, both independent client `.so` files and
the public CA bundle. The CP image contains its UI-embedded binary and CA
bundle; there is no external frontend layer or `OPENLIST_DIST_DIR` override.

Set `GATEWAY_IMAGE_INPUTS` and `CONTROL_PLANE_IMAGE_INPUTS` to external reviewed
input directories. Each needs `inputs.json`, an exact filename-to-SHA-256 map.
The repository rule verifies bytes and exposes only the allowlisted names.
Never include operator configuration, credentials, databases or keys.

```sh
bazel build --config=buildbarn --lockfile_mode=off \
  --output_groups=+oci_tarball,+digest //:gateway_image //:control_plane_image
```

Remote image actions have local fallback disabled. They use neither BuildKit,
Podman nor Buildah. A successful native OCI build and deployed runtime parity
are separate checks; a generated recipe alone establishes neither.

## Updating upstream

Update the exact official gitlink on a product topic, recreate the ordered
patch stack in a fresh disposable source tree, review conflicts as source
changes, and update the patch hashes and resulting tree in the relationship
record. Preserve the recorded old/new source tips. Run the source admission
tests, selected-provider streaming/native tests, typed management/auth tests,
source frontend build, embedded CP build and native OCI assembly. Publication
belongs to the product's reviewed PR, not an upstream fork release workflow.

Rclone updates follow the package lane: change the required module version in
Infra `go/go.mod`, let Go maintain stock checksum integrity, review/rebase the
ordered patches and update only their inventory checksums. Re-materialize in a
fresh external module snapshot and measure its resulting bytes. Do not add a
Git pin, hard-coded expected tree, modified global module cache or committed
absolute replacement.
