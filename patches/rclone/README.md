# rclone VFS cache patches

Upstream is the official `rclone/rclone` tag `v1.75.2`, commit
`be8d6044eacb843b74d335db0c5fbe09222031fe`. The tracked submodule remains
pristine. Apply the ordered patches only in a temporary derived checkout:

```sh
git apply --index /path/to/patches/rclone/0001-isolate-vfs-cache-directory.patch
git apply --index /path/to/patches/rclone/0002-cancel-individual-vfs-read-waits.patch
git write-tree
```

The resulting tree must be `5a82d5a61ec61cbd94c075e03be5940632391d68`.
Patch identity and license are recorded in
`.git-source-relationships/rclone.json`. Updates require an explicit upstream
pin, regenerated patch digest/result tree, and the checks below.

`vfscommon.Options.CacheDir` is an optional per-VFS cache directory. An empty
value retains the upstream `--cache-dir` default. The cache captures its selected
parent at construction and uses it for free-space checks. Since `Options` is
part of active VFS equality, distinct directories cannot share an active VFS.
This patch does not change cache modes, writeback or read-only behavior.

The second patch adds context-aware reads to `RWFileHandle` and cache items,
and a context-aware downloader wait. Cancellation removes only that caller's
waiter. A buffered completion prevents cancellation racing with dispatch from
blocking the downloader under its mutex. Shared download work remains owned by
the cache until its normal close; peer handles are not canceled. Existing Read,
ReadAt and Download APIs retain their background-context behavior.
Final cache-item closure cancels the downloader's own context before joining
its buffered input, so a stalled backend read cannot deadlock the last close.

Verified in the temporary derived checkout with the configured Mise Go toolchain:

```sh
GOWORK=off mise exec -- go test -mod=readonly -race -count=1 \
  -run 'TestVFSNew$|TestVFSNewPerInstanceCacheDirectories$|TestCacheNew$|TestCachePerInstanceDirectories$|TestCachePurgeMinFreeSpace$' \
  ./vfs ./vfs/vfscache ./vfs/vfscommon
```

The tests cover distinct read-only full-cache VFS instances over the same Fs,
normal same-directory active-instance reuse, separate data/metadata directories,
unchanged process cache configuration, default fallback and free-space quota.

Context regressions use a real gated local-backend download with two concurrent
waiters: canceling one returns promptly, unregisters its waiter, and allows the
peer to finish the same physical download. The file-handle test verifies that a
canceled sequential read leaves its offset unchanged.

```sh
GOWORK=off mise exec -- go test -mod=readonly -race -count=1 \
  -run 'TestRWFileHandle|TestDownloaders$|TestDownloadersCloseCancelsBlockedInput$|TestDownloadContextDoesNotCancelPeer$|TestCacheNew$|TestCachePerInstanceDirectories$' \
  ./vfs ./vfs/vfscache ./vfs/vfscache/downloaders
```
