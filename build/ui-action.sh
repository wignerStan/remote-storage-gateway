#!/bin/bash
set -euo pipefail

# Dependencies were resolved from the frozen lock and materialized by rules_js.
# This action performs no installation, dependency resolution, or download.
exec_root=$PWD
source_map="$exec_root/$1"
npm_root="$exec_root/$2"
node="$exec_root/$3"
pnpm="$exec_root/$4"
output="$exec_root/$5"
node_version=$6
pnpm_version=$7
scratch=$(mktemp -d "${TMPDIR:-/tmp}/trade-remote-ui.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
project="$scratch/project"
action_home="$scratch/home"
mkdir -p "$project" "$action_home" "$scratch/bin" "$scratch/cache" "$scratch/config" "$scratch/data" "$scratch/pnpm-store" "$scratch/pnpm-home"

while IFS=$'\t' read -r input relative; do
    case "$relative" in
        ''|/*|../*|*/../*|*/..|./*|*/./*) echo "invalid UI source-map path" >&2; exit 1 ;;
    esac
    mkdir -p "$(dirname "$project/$relative")"
    cp -- "$exec_root/$input" "$project/$relative"
done < "$source_map"

# Keep the npm CAS inputs read-only and avoid copying the entire package store.
# The private node_modules root remains writable for Vite's temporary config.
mkdir -p "$project/node_modules"
shopt -s nullglob dotglob
for entry in "$npm_root/node_modules/"*; do
    ln -s "$entry" "$project/node_modules/${entry##*/}"
done
shopt -u nullglob dotglob
if [[ ! -f "$project/node_modules/vite/bin/vite.js" ]]; then
    echo "frozen npm inputs do not contain the Vite command" >&2
    exit 1
fi
ln -s "$node" "$scratch/bin/node"
cd "$project"
if ! actual_node=$(env HOME="$action_home" "$node" --version); then
    echo "Node version probe failed" >&2; exit 1
fi
if [[ "$actual_node" != "v$node_version" ]]; then
    echo "declared Node tool has the wrong version" >&2; exit 1
fi
if ! actual_pnpm=$(env HOME="$action_home" "$node" "$pnpm" --version); then
    echo "pnpm version probe failed" >&2; exit 1
fi
if [[ "$actual_pnpm" != "$pnpm_version" ]]; then
    echo "declared pnpm tool has the wrong version" >&2; exit 1
fi
"$node" -e 'const p=require("./package.json"); if(p.packageManager!=="pnpm@"+process.argv[1])process.exit(1)' "$pnpm_version"

# Clear inherited credentials/configuration. Offline/frozen policy also applies
# if package-manager configuration is consulted by the run command.
env -i \
    HOME="$action_home" \
    PATH="$scratch/bin:/usr/bin:/bin" \
    CI=true HUSKY=0 NODE_ENV=production TZ=UTC LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    XDG_CACHE_HOME="$scratch/cache" XDG_CONFIG_HOME="$scratch/config" XDG_DATA_HOME="$scratch/data" PNPM_HOME="$scratch/pnpm-home" \
    COREPACK_ENABLE_NETWORK=0 \
    npm_config_offline=true npm_config_frozen_lockfile=true \
    npm_config_manage_package_manager_versions=false \
    npm_config_store_dir="$scratch/pnpm-store" \
    npm_config_userconfig=/dev/null npm_config_globalconfig=/dev/null \
    "$node" "$pnpm" --offline run build

test -f dist/index.html
if [[ -n "$(find dist ! -type f ! -type d -print -quit)" ]]; then
    echo "UI output contains a nonregular entry" >&2; exit 1
fi
find dist -type f -exec chmod 0644 {} +
find dist -type d -exec chmod 0755 {} +
mkdir -p "$(dirname "$output")"
tar --sort=name --hard-dereference --mtime=@0 --owner=0 --group=0 --numeric-owner \
    --format=posix --pax-option=delete=atime,delete=ctime \
    -cf "$output" -C dist .
