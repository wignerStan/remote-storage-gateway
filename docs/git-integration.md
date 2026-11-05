# Git integration and native releases

The repository owner is `repo`; its integration branch is `repo/main`.
Topic branches use semantic owner/topic tokens and an explicit review base.
Normal integration preserves history with the exact two parents
`[target-before, reviewed-source]`. Repository source integration from
`repo/main` into `testing` is distinct from release designation and promotion.

The closed `git-branch-policy/v2` policy follows the
[pinned Git contract](https://github.com/wignerStan/dotagents/blob/c9b35cf42b25787a6bda25480c03a9546f4050f4/packages/git/source/skills/git/references/CONTRACT.md).
The migration retains the existing reserved type-prefix list, release lanes
and promotion order. Repository ownership is declared here because that
closed schema has no owner-role fields; the previous `owners` map was empty.
Published legacy branch names and histories are preserved.

| Branch | Immutable native product version | Explicit project channel |
| --- | --- | --- |
| `testing` | `X.Y.Z-alpha.N` | `alpha` |
| `develop` | `X.Y.Z-beta.N` | `project-latest` |
| `main` | `X.Y.Z` | `stable` |

Promotion is adjacent and preserves the same `X.Y.Z` core. Native product
versions belong to the `native` artifact namespace, independently of Go module
tags and upstream package versions. An immutable artifact manifest records the
version and channel, selected source identities, original build receipts and
output digests. `project-latest` is project metadata, not a provider's special
latest-release flag. A manifest does not imply a Git tag or GitHub Release.

Repository PR admission binds each native product version once to an exact
manifest SHA256 and durable artifact URL. After admission, changed manifest or
artifact bytes require a new version. The channel is a separate, explicit
pointer to that immutable identity; a filename alone does not publish a version.

The current default native bundle contains seven artifacts and does not bundle
the independent rclone executable or BuildBarn source-view CLI. Their separate
owner artifacts remain explicit external inputs when selected. Metadata-only
policy or documentation changes must retain the actual built commit and
receipts, with an exact input-equivalence record for the candidate source.

Gateway beta promotion still requires the actual IPC and generation-lifecycle
acceptance gate. Passive configuration checks or in-process tests do not close
that gate. Alpha artifact designation does not qualify beta, deployment or
normal published SDK/Go module closure.
