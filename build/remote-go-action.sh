#!/usr/bin/env bash
# One declared remote action; scratch compiler state is never a persistent cache.
set -euo pipefail
export PATH=/usr/bin:/bin
umask 022
fail() { printf 'remote Go action: %s\n' "$*" >&2; exit 1; }
[[ $# == 5 ]] || fail 'expected source-map build.env UI-archive output-directory materializer'
manifest=$(realpath "$1")
parameters=$(realpath "$2")
ui=$(realpath "$3")
materializer=$(realpath "$5")
mkdir -p "$4"
out=$(realpath "$4")
inputs=$(dirname "$manifest")
work=$(mktemp -d "$PWD/.remote-go.XXXXXXXX")
trap 'rm -rf "$work"' EXIT
stage="$work/stage"
mkdir -p "$stage" "$work/home" "$work/tmp" "$work/go-cache"
declare -A paths=() verified=() values=() directories=()
stage_started=$SECONDS
[[ -f "$inputs/payload.sha256" ]] || fail 'missing payload checksum manifest'
while read -r digest payload extra; do
    [[ "$digest" =~ ^[0-9a-f]{64}$ && "$payload" == "payload/$digest" && -z ${extra:-} ]] || fail 'invalid payload checksum row'
    [[ -z ${verified[$payload]+yes} ]] || fail 'duplicate payload checksum row'
    verified[$payload]=1
done < "$inputs/payload.sha256"
(cd "$inputs" && sha256sum --strict --status -c payload.sha256) || fail 'payload hash mismatch'

while IFS=$'\t' read -r destination payload mode extra || [[ -n ${destination:-} ]]; do
    [[ -n "$destination" && -z ${extra:-} ]] || fail 'invalid source-map row'
    case "/$destination/" in *'/../'*|*'/./'*|*'//'*) fail 'noncanonical staged path';; esac
    [[ "$destination" != /* && "$destination" != *$'\r'* && "$destination" != *$'\n'* ]] || fail 'invalid staged path'
    [[ "$payload" =~ ^payload/[0-9a-f]{64}$ ]] || fail 'invalid payload identity'
    [[ "$mode" =~ ^0?(444|555|644|755)$ ]] || fail 'invalid staged mode'
    [[ -z ${paths[$destination]+yes} ]] || fail 'duplicate staged path'
    paths[$destination]=1
    source="$inputs/$payload"
    [[ -f "$source" && ! -L "$source" ]] || fail 'missing regular payload'
    [[ -n ${verified[$payload]+yes} ]] || fail 'payload missing from checksum manifest'
    # Source maps contain many files per directory. Avoid dirname/mkdir/chmod
    # child processes on every ordinary source file; never mutate CAS inputs.
    parent=${destination%/*}
    if [[ "$parent" == "$destination" ]]; then parent="$stage"; else parent="$stage/$parent"; fi
    if [[ -z ${directories[$parent]+yes} ]]; then
        mkdir -p "$parent"
        directories[$parent]=1
    fi
    cp --reflink=auto --no-preserve=mode -- "$source" "$stage/$destination"
    case "$mode" in 644|0644) ;; *) chmod "$mode" "$stage/$destination";; esac
done < "$manifest"
printf 'Staged %s declared files in %ss\n' "${#paths[@]}" "$((SECONDS - stage_started))"
while IFS= read -r row || [[ -n "$row" ]]; do
    [[ "$row" == *=* ]] || fail 'invalid build.env row'
    key=${row%%=*}; value=${row#*=}
    case "$key" in gateway_digest|rclone_digest|product_commit|ui_version|rclone_version) ;; *) fail 'unknown build.env key';; esac
    [[ -z ${values[$key]+yes} && -n "$value" && "$value" != *$'\r'* ]] || fail 'duplicate or empty build.env key'
    values[$key]=$value
done < "$parameters"
for key in gateway_digest rclone_digest product_commit ui_version rclone_version; do
    [[ -n ${values[$key]:-} ]] || fail "missing $key"
done
[[ ${values[gateway_digest]} =~ ^sha256:[0-9a-f]{64}$ && ${values[rclone_digest]} =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'invalid provenance digest'
[[ ${values[product_commit]} =~ ^[0-9a-f]{40}(-dirty)?$ ]] || fail 'invalid product commit'
[[ ${values[ui_version]} =~ ^[A-Za-z0-9._+-]+$ && ${values[rclone_version]} =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.+-][A-Za-z0-9.+-]+)?$ ]] || fail 'invalid version'
for required in tools/go/bin/go go.work workspace/gateway/go/go.mod workspace/openlist/go.mod; do
    [[ -f "$stage/$required" ]] || fail "missing staged $required"
done
# The UI producer supplies a dist-content archive, never a source checkout.
tar -tf "$ui" > "$work/ui-members"
tar -tvf "$ui" > "$work/ui-kinds"
while IFS= read -r member; do
    [[ "$member" != /* ]] || fail 'absolute UI archive member'
    case "/$member/" in *'/../'*) fail 'traversing UI archive member';; esac
done < "$work/ui-members"
while IFS= read -r member; do
    [[ "$member" == -* || "$member" == d* ]] || fail 'UI archive contains a link or special file'
done < "$work/ui-kinds"

ui_dir="$stage/workspace/openlist/public/dist"
mkdir -p "$ui_dir"
tar --no-same-owner --no-same-permissions -xf "$ui" -C "$ui_dir"
[[ -f "$ui_dir/index.html" ]] || fail 'UI archive has no root index.html'
# Clear inherited process settings; paths and compiler tools are action-owned.
run_go() {
    local module=$1 cgo=$2 workspace=$3
    shift 3
    (
        cd "$stage/workspace/$module"
        env -i PATH="$stage/tools/go/bin:/usr/bin:/bin" HOME="$work/home" TMPDIR="$work/tmp" \
            GOROOT="$stage/tools/go" GOPATH="$work/gopath" GOCACHE="$work/go-cache" \
            GOMODCACHE="$stage/modulecache" GOWORK="$stage/$workspace" \
            GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off GOTELEMETRY=off \
            GOOS=linux GOARCH=amd64 CGO_ENABLED="$cgo" GOMAXPROCS=4 \
            CC='/usr/bin/gcc -B/usr/bin/' CXX='/usr/bin/g++ -B/usr/bin/' \
            CGO_CFLAGS="-I$stage/workspace/infra/c/interop/plugins/include" \
            "$stage/tools/go/bin/go" build -mod=readonly -p=4 -trimpath -buildvcs=false "$@"
    )
}
run_go openlist 0 go.work -tags infra_grpc -ldflags "-X github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=${values[ui_version]} -X github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=${values[product_commit]}" -o "$out/openlist-control-plane" .
run_go gateway/go 1 go.work -tags dataplane_l7,infra_grpc -o "$out/storage-worker" ./cmd/storage-worker
run_go openlist 1 go.work -tags dataplane_l7,infra_grpc -ldflags "-X github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=${values[ui_version]} -X github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=${values[product_commit]}" -o "$out/gateway" ./cmd/gateway-bundle
run_go openlist 1 go.work -buildmode=c-shared -ldflags "-X main.buildDigest=${values[gateway_digest]}" -o "$out/client-openlist.so" ./cmd/openlist-client-plugin
run_go gateway/go 1 go.work -buildmode=c-shared -ldflags "-X main.buildDigest=${values[gateway_digest]}" -o "$out/client-rclone.so" ./cmd/plugin-rclone
run_go gateway/go 1 go.work -buildmode=c-shared -ldflags "-X main.buildDigest=${values[gateway_digest]}" -o "$out/transfer-reapi.so" ./cmd/plugin-reapi
run_go gateway/go 1 go.work -o "$out/gateway-admin" ./cmd/gateway-admin
cp --reflink=auto "$materializer" "$out/rclone"
chmod 0755 "$out/rclone"
# The standalone producer is a declared action input, not another Go compilation.
# c-shared also generates C headers, which are not release outputs.
rm -f "$out/client-openlist.h" "$out/client-rclone.h" "$out/transfer-reapi.h"
for artifact in gateway openlist-control-plane storage-worker client-openlist.so client-rclone.so transfer-reapi.so gateway-admin rclone; do
    [[ -s "$out/$artifact" ]] || fail "missing output $artifact"
done
