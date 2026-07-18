#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

dry_run=false
for argument in "$@"; do
  case "$argument" in
    --dry-run) dry_run=true ;;
    *)
      echo "unknown argument: $argument" >&2
      exit 2
      ;;
  esac
done

if "$dry_run"; then
  success "sharding smoke dry-run"
  exit 0
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/pg-kinetic-sharding-smoke.XXXXXX")"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT

config_path="$temp_dir/sharding.toml"
cat >"$config_path" <<'CONFIG'
[sharding]
sharding_enabled = true
multi_shard_policy = "first_match"
route_map_reload_strict = true
route_preview_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "schema_table"
schema = "public"
table = "orders"

[sharding.route_maps.strategy]
kind = "list"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
CONFIG

output="$(
  cd "$REPO_ROOT"
  cargo run --quiet -p pg-kinetic -- route-preview \
    --config "$config_path" \
    --database billing \
    --user reporter \
    --sql "select * from public.orders where tenant_id = 'tenant-a'"
)"

if [[ "$output" != *'"ok":true'* || "$output" != *'"shard_id":"tenant-a"'* ]]; then
  echo "sharding route preview did not select tenant-a: $output" >&2
  exit 1
fi

success "sharding smoke passed"
