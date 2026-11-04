# Native product derivation

The Gateway build owns product assembly: the manager ELF, its independent CP and
Worker child modes, source-built UI, concrete client plugins and the REAPI /
ByteStream transfer plugin. Generic Worker contracts and runtime remain in the
independent Data Plane repository. The optional VFS executable is compiled by
the independent rclone repository's native recipe.

## Source authority

OpenList and its frontend retain their immutable upstream commits in the closed
Git v5 `upstream_product_build` records. The OpenList semantic patch inventory
under `patches/openlist` remains the only product patch order authority. Every
patch hash and the resulting tree are verified before a source receipt is
published. Existing object references are optional acquisition inputs; they do
not authorize mutable source selection. A verified reference is cloned with
dissociation, and no source checkout or module cache is rewritten.

Infra `go/go.mod` selects the exact official rclone SDK version. The Go module
checksum and its `go.mod` checksum must both match Infra `go/go.sum`. Infra's
`go/patches/rclone/series.json` supplies the ordered common patches; the canonical
module cache is copied into an external derivation before any patch is applied.
The independent rclone build applies those common patches followed by its own
managed CLI/backend patch. A missing upstream module, patch mismatch or changed
derivation is an error, never a request to synthesize a dependency or change its
version. Huma remains the official versioned Go dependency selected by Infra.
The retired `third_party/huma` relationship is not consumed.

Generated workspaces discover selected first-party module identities from their
actual `go.mod` files, then bind version-qualified local replacements only for
modules in the required graph. They include the current focused Infra adapter
modules. No package version is inferred from a neighboring checkout, changed by
a recipe or replaced with a compatibility stub.

## Local build

```sh
make all \
  GO=/opt/go1.27.3/bin/go NODE=/opt/node24.11.0/bin/node \
  PNPM=/opt/pnpm11.28.9/bin/pnpm JOBS=2 \
  NATIVE_SDK_ROOT=/build/native-sdk PKG_CONFIG=/usr/bin/pkg-config \
  INFRA_ROOT=/sources/infra \
  DATA_PLANE_ROOT=/sources/network-data-plane-worker \
  RCLONE_ROOT=/sources/rclone \
  BUILD_DIR=/build/storage-gateway
```

`build/toolchain.json` pins Go, Node and pnpm. The recipe validates the actual
executables; `MISE=/path/to/mise` is an optional tool selector. Bootstrap needs
Git, Bash, coreutils and jq. CGO builds need a Linux C compiler and canonical
Infra `c/interop/plugins/include` headers. Build output may use any absolute
caller-owned directory outside the selected repositories and shared module
cache. Source, patch or recipe changes require a fresh derivation directory when
an admitted source snapshot has changed.

The shared-memory C SDK must be a real install of the selected canonical Infra
producer. Build it separately before the product, or select an existing verified
installation:

```sh
make -C /sources/infra/c/shared-memory \
  BUILD=/build/native-sdk-objects PREFIX=/build/native-sdk \
  SOURCE_COMMIT="$(git -C /sources/infra rev-parse HEAD)" install
```

`NATIVE_SDK_ROOT` selects that installation and `PKG_CONFIG` selects the actual
pkg-config executable. The recipe verifies the SDK producer receipt, canonical
source identity, ABI profile, header and archive hashes, and the exact `.pc` file
selected by pkg-config. Expanded Cflags/Libs must remain inside the verified SDK.
The source manifest captures these SDK files and the actual C compiler and
pkg-config executables. The product build digest also binds their hashes and the
selected CGO flags. Caller `PKG_CONFIG_PATH` remains an explicit input. The
current native recipe accepts only its canonical plugin include and ordinary
optimization, debug, PIC and pthread flags in `CGO_CFLAGS`, `CGO_CPPFLAGS`,
`CGO_CXXFLAGS` and `CGO_LDFLAGS`. It rejects extra include/library paths,
archives, response files and compiler plugins because their bytes are absent
from the declared input manifest. Ambient compiler header/library search paths
are cleared for this recipe. Additional native inputs need an explicit source
capture contract before the recipe can support them.

The pipeline performs these operations in order:

1. `make source` verifies and derives the pinned OpenList/UI products and common
   rclone module, then writes the required development workspace.
2. The UI recipe executes the pinned pnpm `install --frozen-lockfile`, then the
   upstream `pnpm run build`. It checks that the lockfile and admitted source are
   unchanged, rejects nonregular dist files, and installs the measured dist tree
   into the derived OpenList `public/dist` used by `go:embed`.
3. Infra `code/build/source` captures the exact source, compiler and Go dependency
   closure. Dependency acquisition is allowed during this admission step using
   the caller's configured package sources. Versions and source module files
   remain read-only.
4. Native Go builds run sequentially with `-mod=readonly -trimpath`, bounded
   compiler parallelism and dependency resolution disabled. Each resulting ELF
   must carry the selected compiler and owning module identity. The recorded
   closure is verified again before the complete artifact directory is
   published.

`JOBS` defaults to 2 and `GOMEMLIMIT` to 768 MiB per Go process. The UI Node heap is
bounded to 1536 MiB. Existing Go module/build caches and pnpm content stores are
reused. `GOMODCACHE`, `GOCACHE`, `GOPROXY` and `GOSUMDB` may be set explicitly.
The native entry points do not execute Bazel, Buildbarn or GitHub Actions.

| Output | Owner and role |
| --- | --- |
| `bin/gateway` | One product ELF; manager self-execs separate CP and Worker PIDs |
| `bin/openlist-control-plane` | Standalone CP with the built UI embedded |
| `bin/storage-worker` | Standalone Worker process |
| `bin/client-openlist.so` | OpenList-derived concrete native client |
| `bin/client-rclone.so` | Current Infra-backed rclone native client |
| `bin/transfer-reapi.so` | Dynamic REAPI / ByteStream codec |
| `bin/gateway-admin` | Administrative bootstrap tool |
| `bin/rclone` | Optional independently compiled native VFS executable |

Set `WITH_RCLONE=1` on `make all` to request the optional executable. Gateway
invokes the rclone source owner's native recipe, validates its source identity,
and retains its owner receipt. `make rclone` builds that independent executable
alone under `BUILD_DIR/materializer`; it does not move VFS ownership into the
Gateway ELF. The default Gateway build omits the optional rclone artifact.

`receipts/ui.json` binds frontend commit, frozen lock, toolchain and generated
assets. Per-artifact receipts and `receipts/native-release.json` bind source
identity, actual output hash and `execution: local-go`; UI records
`execution: local-node-pnpm`. Source receipts record measured provenance without
claiming a package release or a Buildbarn execution. A release requested with
`-release` additionally requires clean selected source repositories.

## Validation and packaging inputs

`make test-build` exercises source admission, ordered patch integrity, workspaces,
artifact receipts, output isolation and UI archive admission without a product
build. `make test-derived` runs the derived OpenList tests. `make test` and
`make test-core` use the freshly built native artifacts. Tests that require the
optional independent VFS must select `WITH_RCLONE=1`.

`make image-inputs` checks a clean release, the current source/link manifest,
complete release receipt, individual owner receipts and binary hashes. It stages
and revalidates the complete composition before replacing the prior input tree.
It emits the local executable,
plugins and public CA bundle into the existing bundle / CP / Worker input
compositions. The bundle includes rclone only when selected. OCI packaging and
production deployment are separate operations; producing inputs is not evidence
that an image or deployment has run. Historical Bazel files and remote-action
fixtures are retained as historical inputs and are not selected by native Make
targets.

## Updates

An upstream OpenList/UI update changes its immutable relationship, reviews and
rebases the semantic patch set in a fresh disposable tree, and records the
resulting admitted tree. An official rclone SDK update changes the owning Go
manifest/checksums and rebases the ordered patch inventory. Rebuild and verify
source UI, ELFs, native plugins and optional VFS together whenever a cross-owner
contract changes. Published versions and PR promotion remain owned by their
repositories, not by the build recipe.
