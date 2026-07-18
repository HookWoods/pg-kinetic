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

if ! CARGO="$(resolve_command cargo)"; then
  skip "cargo is not available"
  exit 0
fi
if ! PYTHON="$(resolve_command python)"; then
  skip "python is not available"
  exit 0
fi

free_tcp_port() {
  "$PYTHON" - <<'PY' | tr -d '\r'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

to_native_path() {
  local path="$1"

  if [[ "$path" == /mnt/[a-zA-Z]/* ]]; then
    local drive="${path:5:1}"
    local rest="${path:6}"
    drive="$(printf '%s' "$drive" | tr '[:lower:]' '[:upper:]')"
    printf '%s:%s\n' "$drive" "${rest//\//\\}"
    return 0
  fi

  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$path"
    return 0
  fi

  printf '%s\n' "$path"
}

pg_kinetic_command=("$REPO_ROOT/target/debug/pg-kinetic")
if [[ ! -x "${pg_kinetic_command[0]}" ]]; then
  pg_kinetic_command=("$CARGO" run --quiet -p pg-kinetic --)
fi

admin_wire_path="$SCRIPT_DIR/../lib/admin_wire.py"
if [[ "$PYTHON" == *.exe ]]; then
  admin_wire_path="$(to_native_path "$admin_wire_path")"
fi

assert_contains() {
  local text="$1"
  local needle="$2"
  local message="$3"

  if [[ "$text" != *"$needle"* ]]; then
    printf '%s\nExpected to find: %s\nActual: %s\n' "$message" "$needle" "$text" >&2
    exit 1
  fi
}

wait_for_admin_response() {
  local admin_port="$1"
  local sql="$2"
  local needle="$3"
  local deadline=$((SECONDS + 30))
  local error_path="$temp_dir/admin-query.err"
  local last_error=""
  local response

  while (( SECONDS < deadline )); do
    if response="$("$PYTHON" "$admin_wire_path" "$admin_port" postgres pgkinetic "$sql" 2>"$error_path")" && [[ "$response" == *"$needle"* ]]; then
      printf '%s\n' "$response"
      return 0
    fi
    if [[ -s "$error_path" ]]; then
      last_error="$(cat "$error_path")"
    fi
    sleep 0.2
  done

  echo "admin query '$sql' did not return '$needle' on port $admin_port" >&2
  if [[ -n "$last_error" ]]; then
    printf 'last admin error: %s\n' "$last_error" >&2
  fi
  if [[ -s "$temp_dir/proxy.log" ]]; then
    printf 'proxy log:\n' >&2
    cat "$temp_dir/proxy.log" >&2
  fi
  return 1
}

temp_dir_rel="target/smoke/runtime-$$"
temp_dir="$REPO_ROOT/$temp_dir_rel"
mkdir -p "$temp_dir"
proxy_pid=""
cleanup() {
  if [[ -n "$proxy_pid" ]] && kill -0 "$proxy_pid" 2>/dev/null; then
    kill "$proxy_pid" 2>/dev/null || true
    wait "$proxy_pid" 2>/dev/null || true
  fi
  rm -rf "$temp_dir"
}
trap cleanup EXIT

config_path_rel="$temp_dir_rel/runtime.toml"
config_path="$REPO_ROOT/$config_path_rel"
listen_port="$(free_tcp_port)"
backend_port="$(free_tcp_port)"
admin_port="$(free_tcp_port)"
cat >"$config_path" <<CONFIG
[connection]
listen_addr = "127.0.0.1:$listen_port"
backend_addr = "127.0.0.1:$backend_port"

[runtime.lifecycle]
startup_grace_ms = 1000
shutdown_grace_ms = 1000
startup_backend_checks_enabled = false
readiness_fail_during_drain = true
pre_stop_drain_enabled = true
pre_stop_drain_endpoint = "/drain"
termination_grace_period_seconds = 5

[runtime.node]
node_id = "local-runtime-smoke"

[runtime.engine]
runtime_engine = "tokio_current_thread"

[runtime.production]
control_plane_enabled = true
mirroring_enabled = false
adaptive_enabled = false

[drain]
drain_timeout_ms = 1000

[admin]
admin_addr = "127.0.0.1:$admin_port"
admin_require_tls = false
admin_allowed_user = "postgres"
admin_query_timeout_ms = 3000
admin_max_clients = 8

[tls]
client_tls_mode = "disable"
backend_tls_mode = "disable"

[auth]
auth_mode = "trust"
CONFIG

output="$(cd "$REPO_ROOT" && "${pg_kinetic_command[@]}" preflight --config "$config_path_rel" --format json)"
if [[ "$output" != *'"ok":true'* ]]; then
  echo "runtime preflight failed: $output" >&2
  exit 1
fi

(
  cd "$REPO_ROOT"
  "${pg_kinetic_command[@]}" --config-file "$config_path_rel"
) >"$temp_dir/proxy.log" 2>&1 &
proxy_pid="$!"

runtime_response="$(wait_for_admin_response "$admin_port" "SHOW RUNTIME;" "tokio_current_thread")"
assert_contains "$runtime_response" "local-runtime-smoke" "runtime should report the configured node id"
assert_contains "$runtime_response" "tokio_current_thread" "runtime should report the selected engine"
assert_contains "$runtime_response" "starting" "runtime should expose the lifecycle state"
assert_contains "$runtime_response" "not_ready" "runtime should expose the readiness state"

success "runtime smoke passed"
