# Validation records

The current cloud/VFS extraction is recorded in
[cloud-vfs-delivery-20261102.json](cloud-vfs-delivery-20261102.json), with separate
artifact, image and CI source-profile receipts. Rclone's package version comes
from Infra `go/go.mod`; the product has no rclone Git source pin.

The earlier `monorepo-*` receipts describe the superseded candidate before the
Infra extraction. `historical-deployment-20261102.json` preserves the previous
observed production rollout. Candidate build verification does not assert that
production was updated or that incomplete architecture/CI evidence passed.
