#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

HOST_NAME="${HOST_NAME:-127.0.0.1}"
PORT="${PORT:-58432}"
USER_NAME="${USER_NAME:-postgres}"
DATABASE="${DATABASE:-pgkinetic}"
PASSWORD="${PASSWORD:-postgres}"
SSL_MODE="${SSL_MODE:-disable}"
GSS_ENC_MODE="${GSS_ENC_MODE:-disable}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-10}"
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
  success "psql smoke dry-run"
  exit 0
fi

if ! require_command psql; then
  exit 0
fi

export PGPASSWORD="$PASSWORD"
export PGSSLMODE="$SSL_MODE"
export PGGSSENCMODE="$GSS_ENC_MODE"
export PGCONNECT_TIMEOUT="$CONNECT_TIMEOUT_SECONDS"

result="$(
  psql \
    --no-psqlrc \
    -v ON_ERROR_STOP=1 \
    -h "$HOST_NAME" \
    -p "$PORT" \
    -U "$USER_NAME" \
    -d "$DATABASE" \
    -Atc "select count(*) from accounts;"
)"

if [[ "$result" != "2" ]]; then
  echo "expected account count 2, got '$result'" >&2
  exit 1
fi

success "psql smoke passed on $HOST_NAME:$PORT"
