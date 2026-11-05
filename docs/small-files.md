# Small files and packed read namespaces

The Gateway has two independent choices: remote namespace representation and
optional local materialization. Neither choice defines backup or hot/cold policy.

For mutable individual files, keep an ordinary client implementation. Remote
objects remain separate. Independent VFS can reuse downloaded bytes, but does
not combine those files into an archive. Its byte and file-count targets evict
inactive cache entries; the volume must provide a separate hard disk/inode limit.

For immutable small-file datasets, publish a versioned ZIP object, then select
`client.rclone` implementation `archive-s3`, `archive-webdav` or `archive-drive`.
These implementations add the required `archive_path` to their ordinary backend
configuration and retain that backend's existing secret declarations. Entry
derives their schema and publishes only stat/list/open/range/pagination
capabilities. The archive view rejects put/delete. Ordinary implementations do
not accept archive options.

The bounded initial profile supports ZIP32, Store and Deflate. ZIP64, split,
encrypted and unsupported-compression archives fail admission. Central index,
entry count, inflated member size, total inflated size and expansion ratio have
finite adapter limits. Unsafe/duplicate names and corrupt full-read CRC fail
closed. Partial reads cannot certify the checksum of an entire member. Member
ranges may require decompressing from the beginning of that member; an archive
reduces remote object count without guaranteeing fewer HTTP requests.

To keep the local cache packed as well, put the independent VFS **before** the
archive view:

```text
Worker archive client -> native rclone DAV/VFS (packed ZIP cache)
                      -> ttrpc metadata + shared-memory leases
                      -> Worker raw namespace -> ordinary origin client
```

VFS is the separate patched-rclone application and process; its filesystem/serve code is upstream native rclone. It caches the ZIP object rather than extracting
members. The archive plugin consumes its authenticated WebDAV endpoint; VFS
misses fetch through the Worker's separately admitted ttrpc/shared-memory backend.
Control Plane coordinates process/configuration metadata through ttrpc. It never
carries ZIP bytes. Public access remains on the Worker. Each cache assignment
must retain distinct raw origin and cached client identities, avoiding recursion.

For example, the archive client's public configuration for an independently
assigned VFS endpoint is:

```json
{
  "url": "http://127.0.0.1:5247/dav/",
  "username": "archive-reader",
  "archive_path": "releases/dataset-v1.zip"
}
```

Supply `password` through the already approved client secret reference; never
reuse host ingress/TLS credentials as provider secrets. The endpoint is illustrative,
not an ambient default. CP must supply the observed private endpoint and matching
credentials. Select an immutable versioned archive path and replace the client
generation when publishing a new archive. Do not overwrite an archive while its
index and members are being served.

## Source archive view

Add `"source_view": true` to an `archive-*` public configuration to preserve
source file types and executable permissions. This optional field is available
for all three archive providers. It is absent from ordinary provider schemas.
The shared Infra archive reader then returns typed modes for every member and
directory. The default archive profile retains its original behavior and rejects
symlinks.

The source profile admits real ZIP symlink entries, keeps their logical names,
and serves the link target as ordinary bounded member bytes. It never follows a
link or interprets an ordinary `.rclonelink` filename as a link. Special files
are rejected and privileged permission bits are discarded. Source link bodies
are limited to 4096 bytes; complete reads retain the existing CRC checks.

Expose the archive client through an explicit mount `remote_client` assignment
and consume that namespace with the BuildBarn materializer's `source_view`
profile. The materializer requests `Endpoint.FileModes` in the Worker storage
IPC Hello; Worker validates the typed modes and keeps arbitrary provider
metadata out of the replies. Link content continues through the existing shared
memory byte channel. The materializer filters logical source paths, validates
the selected link graph, and presents read-only native files. The Gateway's
ordinary WebDAV adapter does not perform that source link projection.

An old Worker or a provider without typed modes fails the source profile. It
does not silently turn an executable or link into an ordinary file. A new
archive requires a new immutable object path, a successful archive candidate
admission and a generation-CAS client replacement. Existing storage IPC
sessions become stale after replacement and require a new materializer session.
Retain the old object while any old generation still has active readers or an
operator rollback needs it. Automatic archive retention and deletion are not
provided by this binding.

## Packed cache bounds

The independent VFS process accepts these optional bounds:

| Configuration | Default | Meaning |
| --- | --- | --- |
| `max_concurrent_requests` | 8 | Fail-fast HTTP admission, capped by `max_open_files`; busy replies use503/Retry-After |
| `max_open_files` | 8 | Pending plus live cache handles; no unbounded internal open wait queue |
| `max_cache_entries` | 4096 | Inactive cache-entry eviction target, including zero-byte files |
| `max_directory_entries` | 10000 | Per-directory inventory admission bound |

`max_bytes` and `max_age_seconds` remain required. Active files can temporarily
exceed eviction targets. The directory bound is not a total-process memory bound
and cannot prevent allocations made inside a provider page before its callback.
Request and open limits are further clamped to the Worker session’s negotiated
handle capacity. The native CLI uses no private prefetch buffer or read-ahead,
1MiB chunks and no parallel chunk streams. These are per-transfer controls, not
a total-process RAM quota.
Metadata HEAD, PROPFIND, conditional GET and invalid ranges do not fetch file
payloads to sniff MIME type. An extension-derived type or application/octet-stream
is published by the DAV entry instead.

Provider failures before any response payload produce `503` with `Retry-After`,
including cache admission pressure. Real missing files, conditional responses
and invalid ranges retain their normal statuses. Once streaming has begun, the
entry preserves the committed response and never replays provider I/O.

## Publication

Creating an archive is a separate, explicit batch operation. Upstream
[`rclone archive create`](https://rclone.org/commands/rclone_archive_create/)
can write ZIP to a remote and permits replacing an existing destination. That
write is not an immutable-object, atomic-snapshot or retention guarantee. The
source publication profile must first complete a local archive, upload under a
new versioned path, verify that object and only then replace the admitted client
generation. A mutable input tree must be frozen by its owner while it is being
packed. The Gateway does not silently pack/delete existing
objects or turn a mutable directory into a writable ZIP filesystem. For selective
cloud reads see the upstream [archive backend](https://rclone.org/archive/).
