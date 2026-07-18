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
  success "mirroring smoke dry-run"
  exit 0
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/pg-kinetic-mirroring-smoke.XXXXXX")"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT

config_path="$temp_dir/mirroring.toml"
cat >"$config_path" <<'CONFIG'
[connection]
listen_addr = "127.0.0.1:0"
backend_addr = "127.0.0.1:1"

[runtime.lifecycle]
startup_backend_checks_enabled = false

[runtime.production]
control_plane_enabled = true
mirroring_enabled = true
adaptive_enabled = false

[tls]
client_tls_mode = "disable"
backend_tls_mode = "disable"

[auth]
auth_mode = "trust"

[mirror]
mirroring_enabled = true
mirror_mode = "read_only"
mirror_timeout_ms = 50
mirror_max_in_flight = 8

[mirror.target]
address = "127.0.0.1:1"
isolated = true

[mirror.sampling]
mirror_sample_rate = 0.25

[mirror.safety]
mirror_require_isolated_target = true
CONFIG

output="$(cd "$REPO_ROOT" && cargo run --quiet -p pg-kinetic -- preflight --config "$config_path" --format json)"
if [[ "$output" != *'"ok":true'* ]]; then
  echo "mirroring preflight failed: $output" >&2
  exit 1
fi

success "mirroring smoke passed"
