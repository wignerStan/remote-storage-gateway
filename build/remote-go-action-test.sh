#!/usr/bin/env bash
# Contract tests use a fake compiler; actual ELF/RBE acceptance is separate.
set -euo pipefail
export PATH=/usr/bin:/bin
runner=$(realpath "$(dirname "$0")/remote-go-action.sh")
work=$(mktemp -d /tmp/trade-remote-go-action-test.XXXXXXXX)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/inputs/payload" "$work/ui" "$work/exec"
printf '<html>fixture</html>\n' > "$work/ui/index.html"
tar -cf "$work/ui.tar" -C "$work/ui" .
cat > "$work/fake-go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
[[ "$GOPROXY" == off && "$GOSUMDB" == off && "$GOTOOLCHAIN" == local && "$GOENV" == off ]]
[[ "$CC" == '/usr/bin/gcc -B/usr/bin/' && "$CXX" == '/usr/bin/g++ -B/usr/bin/' ]]
[[ -z ${SHOULD_NOT_LEAK:-} && "$GOCACHE" == */.remote-go.*/go-cache && "$GOMODCACHE" == */stage/modulecache ]]
[[ "$1" == build && "$2" == -mod=readonly ]]
for mode in 444 555 644 755; do
    [[ $(stat -c '%a' "$GOROOT/../../workspace/gateway/go/mode-$mode") == "$mode" ]]
done

args=("$@"); output=''
for ((i=0;i<${#args[@]};i++)); do
    if [[ ${args[i]} == -o ]]; then output=${args[i+1]}; fi
done
[[ -n "$output" ]]
if [[ "$output" == */openlist-control-plane ]]; then
    [[ "$CGO_ENABLED" == 0 && -f "$PWD/public/dist/index.html" ]]
else
    [[ "$CGO_ENABLED" == 1 ]]
fi
if [[ "$output" == */rclone ]]; then [[ "$GOWORK" == */go-native.work ]]; else [[ "$GOWORK" == */go.work ]]; fi
printf '%s\n' "$CGO_ENABLED" "$GOCACHE" "$GOWORK" "$@" > "$output"
GO
printf 'module fixture\n' > "$work/module"
printf 'go 1.27.3\n' > "$work/workspace"
: > "$work/inputs/source-map.tsv"
: > "$work/inputs/payload.sha256"
add() {
    local path=$1 file=$2 mode=$3 sum
    sum=$(sha256sum "$file"); sum=${sum%% *}
    if [[ ! -e "$work/inputs/payload/$sum" ]]; then
        cp "$file" "$work/inputs/payload/$sum"
        printf '%s  payload/%s\n' "$sum" "$sum" >> "$work/inputs/payload.sha256"
    fi
    printf '%s\tpayload/%s\t%s\n' "$path" "$sum" "$mode" >> "$work/inputs/source-map.tsv"
}
add tools/go/bin/go "$work/fake-go" 0755
for module in gateway/go openlist rclone-native; do add "workspace/$module/go.mod" "$work/module" 0644; done
add go.work "$work/workspace" 0644
add go-native.work "$work/workspace" 0644
for mode in 444 555 644 755; do add "workspace/gateway/go/mode-$mode" "$work/module" "0$mode"; done

cat > "$work/inputs/build.env" <<'ENV'
gateway_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
rclone_digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
product_commit=cccccccccccccccccccccccccccccccccccccccc
ui_version=v4.2.4
rclone_version=v1.75.2
ENV
printf 'standalone materializer fixture\n' > "$work/materializer"
(umask 077; cd "$work/exec" && SHOULD_NOT_LEAK=sentinel "$runner" "$work/inputs/source-map.tsv" "$work/inputs/build.env" "$work/ui.tar" "$work/output" "$work/materializer")
[[ $(find "$work/output" -type f | wc -l) == 8 ]]
[[ $(sed -n '2p' "$work/output/gateway" "$work/output/openlist-control-plane" "$work/output/storage-worker" "$work/output/"*.so "$work/output/gateway-admin" | sort -u | wc -l) == 1 ]]
grep -q -- '-buildmode=c-shared' "$work/output/client-openlist.so"
grep -q -- 'dataplane_l7' "$work/output/storage-worker"
cmp "$work/materializer" "$work/output/rclone"
[[ -z $(find "$work/exec" -name '.remote-go.*' -print -quit) ]]
cp "$work/inputs/source-map.tsv" "$work/original-map"
cp "$work/inputs/build.env" "$work/original-env"
reject() {
    if (cd "$work/exec" && "$runner" "$work/inputs/source-map.tsv" "$work/inputs/build.env" "$work/ui.tar" "$work/rejected" "$work/materializer") > "$work/rejection.log" 2>&1; then
        printf 'negative control unexpectedly succeeded: %s\n' "$1" >&2; exit 1
    fi
    grep -q "$2" "$work/rejection.log"
    [[ -z $(find "$work/exec" -name '.remote-go.*' -print -quit) ]]
}
head -1 "$work/original-map" >> "$work/inputs/source-map.tsv"
reject duplicate 'duplicate staged path'
cp "$work/original-map" "$work/inputs/source-map.tsv"
sed -i '1s|tools/go/bin/go|../escaped|' "$work/inputs/source-map.tsv"
reject traversal 'noncanonical staged path'
cp "$work/original-map" "$work/inputs/source-map.tsv"
printf 'untrusted=$(touch unwanted)\n' >> "$work/inputs/build.env"
reject env 'unknown build.env key'
cp "$work/original-env" "$work/inputs/build.env"
first=$(head -1 "$work/inputs/payload.sha256"); payload=${first##* }
printf 'corrupt\n' >> "$work/inputs/$payload"
reject corruption 'payload hash mismatch'
printf 'remote Go action contract: PASS (seven compilations plus standalone materializer, isolated environment/cache cleanup, duplicate/path/env/hash rejection)\n'
