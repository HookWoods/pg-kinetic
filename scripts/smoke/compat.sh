#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:58432/pgkinetic}"
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
  success "compat smoke dry-run"
  exit 0
fi

for dependency in cargo go npm python; do
  if ! require_command "$dependency"; then
    exit 0
  fi
done

CARGO="$(resolve_command cargo)"
GO="$(resolve_command go)"
NPM="$(resolve_command npm)"
PYTHON="$(resolve_command python)"

(
  cd "$REPO_ROOT/compat/rust-tokio-postgres"
  DATABASE_URL="host=127.0.0.1 port=58432 user=postgres password=postgres dbname=pgkinetic" "$CARGO" run
)

(
  cd "$REPO_ROOT/compat/go-pgx"
  DATABASE_URL="$DATABASE_URL" "$GO" run .
)

(
  cd "$REPO_ROOT/compat/node-pg"
  DATABASE_URL="$DATABASE_URL" "$NPM" install --no-audit --no-fund --package-lock=false
  DATABASE_URL="$DATABASE_URL" "$NPM" run smoke
)

(
  cd "$REPO_ROOT/compat/python-psycopg"
  "$PYTHON" -m pip install --disable-pip-version-check -r requirements.txt
  DATABASE_URL="$DATABASE_URL" "$PYTHON" smoke.py
)

success "compat smoke passed"
