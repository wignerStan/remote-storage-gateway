# Validation records

The current candidate runs one gateway ELF as separate manager, control-plane
and Worker processes, with optional independent rclone materialization. Buildbarn
on Incus produced the native programs, dynamic plugins and embedded UI without a
local native-build fallback. The four verified OCI archives are `bundle_image`,
`gateway_image` (standalone Worker), `control_plane_image` and the independent
`rclone_image`.

The collected artifact source is Gateway
`2d4babee5f2f7e4422d32d0b331d6c1832b5667e`, with Infra
`e8aeaddb9b05b37a9400e366a43861fbc45a336c` and Data Plane
`3cc9c98db6e46efbb6b8d016c83adf38a40eba4a` (`go/v0.3.1`). Native receipts are in
`/tmp/trade-gateway-manager-build/receipts`; the four-image payload and archive
checks are in
`/tmp/trade-gateway-manager-build/bundle-and-components-oci-verification.json`.
These records retain the actual artifact source identity.

Runtime evidence is preserved under
`/var/tmp/trade-manager-deployment-20261103/proofs/gateway-native-final` and
`/var/tmp/trade-manager-deployment-20261103/proofs/gateway-bundle-final`. The full
native suite recorded 90 passing test events and no individual-test skips, but
returned failure because its initial bundle fixture observed only one Go
thread's child processes. Gateway
`5b4c42e2113de294ed25825b42fcd522ed833b02` corrected only that test observer; the
production source remained identical to the artifact source. The corrected
focused run passed all four bundle scenarios (five Go test events including the
parent), with no failures or skips: distinct PIDs executing the same ELF, CP
readiness and default SQLite creation, public TLS/UI/auth and legacy-payload
boundaries, real plugin WebDAV reads, SIGTERM exchange draining and reaping,
Worker startup-failure cleanup, and whole-group retirement after either child's
SIGKILL. This is combined evidence from the unchanged full-suite results and
the corrected focused run, not a claim of a fresh all-green full-suite run.

The candidate still uses an unreleased Infra workspace composition. Infra
[PR96](https://github.com/wignerStan/infra/pull/96) (process-role supervision) and
[PR102](https://github.com/wignerStan/infra/pull/102) (scoped native buffers) were
open at the 2026-11-03 audit. The frozen `e8aeadd` composition also includes their
required build/schema/RPC prerequisites; it is not a claim to match the latest
PR heads. The successful build selected `/tmp/trade-scoped-sdk` explicitly.
Gateway's Infra `v0.0.0` requirements are resolved through the generated workspace,
and the default `/develop/infra` lacks required candidate APIs. These receipts
therefore establish acceptance of the selected source composition, not a
reproducible release through published ordinary module versions. Integration may
proceed through `repo/main` to `testing`; stable `main` remains held until the
required SDK capabilities and consumable module versions are published, the
consumer requirements are updated, and that dependency configuration is verified.

`make test` passes the collected bundle, standalone Worker, rclone and all three
plugin paths explicitly, and enables both `dataplane_l7` and `infra_grpc`. This
executes bundle and gRPC acceptance and avoids the existing integration fixture's
local Worker/plugin build fallbacks. It retains the remote `all` prerequisite;
the entry/documentation correction does not relabel or rebuild the accepted
`2d4babe` artifacts.

The earlier three-application/two-plugin records
[three-axis-delivery.json](three-axis-delivery.json),
[program checks](three-axis-artifacts.json) and
[three native OCI images](three-axis-oci-verification.json) are historical. Their
CGO0 CP executable owned the separate Worker/VFS processes, unlike the current
top-level manager. `cloud-vfs-*`, `monorepo-*` and `historical-deployment-*` are
also historical receipts. Runtime application-buffer sharing applies to encoded
gRPC, not all storage ports or kernel/TLS/protobuf copies.

The older rename kept GitHub repository ID1442271176; it now belongs to the
independent `network-data-plane-worker` again. Historical PR1/PR2 links recorded
under the temporary Gateway name refer to that original repository identity.
The new Gateway is repository ID1443344039. Original receipt observations remain
unchanged.

No receipt here asserts a production rollout, complete legacy endpoint/PVC
cutover, complete architecture evidence, or all repository CI passing.
