#!/usr/bin/env bash
# Bootstrap only the selected first-party module graph. All generated files stay
# in a caller-owned build directory; third-party dependencies keep their versions.
set -euo pipefail
[[ $# -ge 6 ]] || { echo 'usage: bootstrap OUTPUT GO MISE PRODUCT INFRA WORKER [RCLONE]' >&2; exit 2; }
output=$(realpath -m "$1")
go_bin=$2
mise=$3
product=$(realpath "$4")
infra=$(realpath "$5")
worker=$(realpath "$6")
shift 6
run_go() {
  if [[ -n "$mise" ]]; then
    GOWORK=off GOTOOLCHAIN=local "$mise" exec go@1.27.3 -- go "$@"
  else
    GOWORK=off GOTOOLCHAIN=local "$go_bin" "$@"
  fi
}
[[ $(run_go env GOVERSION) == go1.27.3 ]] || { echo 'native build requires Go 1.27.3' >&2; exit 1; }
roots=("$product" "$infra" "$worker")
for root in "$@"; do roots+=("$(realpath "$root")"); done
cache=$(run_go env GOMODCACHE)
for root in "${roots[@]}" "$cache"; do
  case "$output/" in "$root/"*) echo "build output must be disjoint from source/cache $root" >&2; exit 1;; esac
  case "$root/" in "$output/"*) echo "build output must be disjoint from source/cache $root" >&2; exit 1;; esac
done
[[ "$output" != / ]] || { echo 'build output cannot be filesystem root' >&2; exit 1; }
mkdir -p "$output/bootstrap"
workspace="$output/bootstrap/go.work"
printf 'go 1.27.3\n' > "$workspace"
declare -A available=() seen=()
modules=("$product/go" "$worker/go")
for root in "$@"; do modules+=("$(realpath "$root")/go"); done
while IFS= read -r -d '' file; do modules+=("${file%/go.mod}"); done < <(find "$infra/go" -type d \( -name .git -o -name testdata \) -prune -o -type f -name go.mod -print0)
for directory in "${modules[@]}"; do
  module=$(cd "$directory" && run_go mod edit -json | jq -er .Module.Path)
  [[ -z ${available[$module]+present} || ${available[$module]} == "$directory" ]] || { echo "duplicate selected module $module" >&2; exit 1; }
  available[$module]=$directory
done
queue=("$product/go")
for ((index=0; index<${#queue[@]}; index++)); do
  directory=${queue[$index]}
  [[ -z ${seen[$directory]+present} ]] || continue
  seen[$directory]=1
  run_go work edit "-use=$directory" "$workspace"
  while IFS=$'\t' read -r module version; do
    [[ -n "$module" && -n ${available[$module]+present} ]] || continue
    selected=${available[$module]}
    queue+=("$selected")
    run_go work edit "-replace=$module@$version=$selected" "$workspace"
  done < <(cd "$directory" && run_go mod edit -json | jq -r '(.Require // [])[] | [.Path,.Version] | @tsv')
done
