#!/bin/sh
set -eu
product_root=$1
infra_root=$2
build_root=$3
mise=$4
case "$build_root" in /tmp/trade-*|/var/tmp/trade-*) ;; *) echo 'Build directory must be /tmp/trade-* or /var/tmp/trade-*' >&2; exit 1 ;; esac
workspace_dir="$build_root/bootstrap"
mkdir -p "$workspace_dir"
# Only bootstrap the build tool. The Go source tool derives the complete actual
# module inventory and writes the final build workspace independently.
set -- "$product_root/go" "$infra_root/go"
for relative in runtime/nativeplugin runtime/pluginlifecycle; do
    if test -f "$infra_root/go/$relative/go.mod"; then
        set -- "$@" "$infra_root/go/$relative"
    fi
done
printf 'go 1.27.3\n' > "$workspace_dir/go.work"
for directory do
    GOWORK=off "$mise" exec go@1.27.3 -- go work edit "-use=$directory" "$workspace_dir/go.work"
done
GOWORK=off "$mise" exec go@1.27.3 -- go work edit \
    "-replace=github.com/wignerStan/infra/go@v0.0.0=$infra_root/go" "$workspace_dir/go.work"
for relative in runtime/nativeplugin runtime/pluginlifecycle; do
    if test -f "$infra_root/go/$relative/go.mod"; then
        GOWORK=off "$mise" exec go@1.27.3 -- go work edit \
            "-replace=github.com/wignerStan/infra/go/$relative@v0.0.0=$infra_root/go/$relative" "$workspace_dir/go.work"
    fi
done
