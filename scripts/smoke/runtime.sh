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
  success "runtime smoke dry-run"
  exit 0
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/pg-kinetic-runtime-smoke.XXXXXX")"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT

config_path="$temp_dir/runtime.toml"
cat >"$config_path" <<'CONFIG'
[connection]
listen_addr = "127.0.0.1:0"
backend_addr = "127.0.0.1:1"

[runtime.lifecycle]
startup_backend_checks_enabled = false
readiness_fail_during_drain = true
pre_stop_drain_enabled = true

[runtime.node]
node_id = "local-runtime-smoke"

[runtime.engine]
runtime_engine = "tokio_current_thread"

[runtime.production]
control_plane_enabled = true
mirroring_enabled = false
adaptive_enabled = false

[tls]
client_tls_mode = "disable"
backend_tls_mode = "disable"

[auth]
auth_mode = "trust"
CONFIG

output="$(cd "$REPO_ROOT" && cargo run --quiet -p pg-kinetic -- preflight --config "$config_path" --format json)"
if [[ "$output" != *'"ok":true'* ]]; then
  echo "runtime preflight failed: $output" >&2
  exit 1
fi

success "runtime smoke passed"
