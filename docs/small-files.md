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
Worker archive client -> private VFS DAV (ZIP object/ranges)
                      -> Worker raw data socket -> ordinary origin client
```

VFS is still a separate process. It caches the ZIP object rather than extracting
members. The archive plugin consumes its authenticated WebDAV endpoint; VFS
misses fetch through the Worker's separately admitted raw HTTP Unix data socket.
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

The independent VFS process accepts these optional bounds:

| Configuration | Default | Meaning |
| --- | --- | --- |
| `max_concurrent_requests` | 64 | Fail-fast HTTP admission, capped by `max_open_files`; busy replies use503/Retry-After |
| `max_open_files` | 128 | Pending plus live cache handles; no unbounded internal open wait queue |
| `max_cache_entries` | 4096 | Inactive cache-entry eviction target, including zero-byte files |
| `max_directory_entries` | 10000 | Per-directory inventory admission bound |

`max_bytes` and `max_age_seconds` remain required. Active files can temporarily
exceed eviction targets. The directory bound is not a total-process memory bound
and cannot prevent allocations made inside a provider page before its callback.
The private cache context uses a 1MiB transfer buffer, no read-ahead and a single
chunk stream. These are per-transfer controls, not a total-process RAM quota.
Metadata HEAD, PROPFIND, conditional GET and invalid ranges do not fetch file
payloads to sniff MIME type. An extension-derived type or application/octet-stream
is published by the DAV entry instead.

Creating an archive is a separate, explicit batch operation. Upstream
[`rclone archive create`](https://rclone.org/commands/rclone_archive_create/)
can publish ZIP to a remote. The Gateway does not silently pack/delete existing
objects or turn a mutable directory into a writable ZIP filesystem. For selective
cloud reads see the upstream [archive backend](https://rclone.org/archive/).
