#!/bin/sh
set -eu
product_root=$1
infra_root=$2
build_root=$3
mise=$4
data_plane_root=$5
rclone_root=$6
case "$build_root" in /tmp/trade-*|/var/tmp/trade-*) ;; *) echo 'Build directory must be /tmp/trade-* or /var/tmp/trade-*' >&2; exit 1 ;; esac
workspace_dir="$build_root/bootstrap"
mkdir -p "$workspace_dir"
# Only bootstrap the build tool. The Go source tool derives the complete actual
# module inventory and writes the final build workspace independently.
set -- "$product_root/go" "$infra_root/go" "$data_plane_root/go" "$rclone_root/go" "$infra_root/third_party/huma"
for relative in plugins/native runtime/lifecycle network/rpc/ttrpc network/rpc/routes; do
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
for relative in plugins/native runtime/lifecycle network/rpc/ttrpc network/rpc/routes; do
    if test -f "$infra_root/go/$relative/go.mod"; then
        GOWORK=off "$mise" exec go@1.27.3 -- go work edit \
            "-replace=github.com/wignerStan/infra/go/$relative@v0.0.0=$infra_root/go/$relative" "$workspace_dir/go.work"
    fi
done

# Match every declared development requirement version; Go workspaces reject an
# unversioned replacement of a workspace module. No remote version lookup is
# needed to parse these owner-local module declarations.
for declaration in "$product_root/go" "$data_plane_root/go" "$rclone_root/go" "$infra_root/go"; do
    GOWORK=off "$mise" exec go@1.27.3 -- go mod edit -json "$declaration/go.mod" |
        jq -r '.Require[] | select(.Path == "github.com/wignerStan/network-data-plane-worker/go" or .Path == "github.com/wignerStan/rclone/go" or .Path == "github.com/danielgtaylor/huma/v2") | "\(.Path)@\(.Version)"' |
        while IFS= read -r requirement; do
            case "$requirement" in
                github.com/danielgtaylor/huma/v2@*) selected="$infra_root/third_party/huma" ;;
                github.com/wignerStan/network-data-plane-worker/go@*) selected="$data_plane_root/go" ;;
                github.com/wignerStan/rclone/go@*) selected="$rclone_root/go" ;;
                *) exit 1 ;;
            esac
            GOWORK=off "$mise" exec go@1.27.3 -- go work edit "-replace=$requirement=$selected" "$workspace_dir/go.work"
        done
done
