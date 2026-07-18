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

if ! CARGO="$(resolve_command cargo)"; then
  skip "cargo is not available"
  exit 0
fi

temp_dir_rel="target/smoke/sharding-$$"
temp_dir="$REPO_ROOT/$temp_dir_rel"
mkdir -p "$temp_dir"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT

config_path_rel="$temp_dir_rel/sharding.toml"
config_path="$REPO_ROOT/$config_path_rel"
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
  "$CARGO" run --quiet -p pg-kinetic -- route-preview \
    --config "$config_path_rel" \
    --database billing \
    --user reporter \
    --sql "select * from public.orders where tenant_id = 'tenant-a'"
)"

if [[ "$output" != *'"ok":true'* || "$output" != *'"shard_id":"tenant-a"'* ]]; then
  echo "sharding route preview did not select tenant-a: $output" >&2
  exit 1
fi

success "sharding smoke passed"
