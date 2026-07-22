#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
EVIDENCE_DIR="$REPO_ROOT/target/release-evidence"
SUMMARY_FILE="$EVIDENCE_DIR/summary.json"
COMPOSE=(docker compose -f bench/compose.yml)
COMPAT_ENV=(
  env
  PG_KINETIC_COMPAT_LIVE=1
  PG_KINETIC_COMPAT_SERVICES=direct-postgres,pg-kinetic
  DATABASE_URL_DIRECT=postgres://postgres:postgres@127.0.0.1:55432/pgkinetic
  DATABASE_URL_PROXY=postgres://postgres:postgres@127.0.0.1:58432/pgkinetic
)
stage="initialization"
exit_code=0

mkdir -p "$EVIDENCE_DIR"

write_summary() {
  local outcome="$1"
  local log_file="$EVIDENCE_DIR/compose.log"
  local log_status="absent"
  if [[ -f "$log_file" ]]; then
    log_status="target/release-evidence/compose.log"
  fi

  cat >"$SUMMARY_FILE" <<EOF
{
  "schema_version": 1,
  "workflow": "stable-gate",
  "platform": "linux-docker",
  "outcome": "$outcome",
  "failed_stage": "$stage",
  "compose_log": "$log_status",
  "commands": [
    "cargo fmt --check",
    "cargo test --workspace --locked",
    "docker compose -f bench/compose.yml up --detach --wait --build postgres pg-kinetic",
    "docker compose -f bench/compose.yml exec -T postgres env PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h pg-kinetic -p 6543 -U postgres -d pgkinetic -c 'select 1'",
    "cat compat/common/schema.sql compat/common/seed.sql | docker compose -f bench/compose.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d pgkinetic",
    "PG_KINETIC_COMPAT_LIVE=1 PG_KINETIC_COMPAT_SERVICES=direct-postgres,pg-kinetic DATABASE_URL_DIRECT=postgres://postgres:postgres@127.0.0.1:55432/pgkinetic DATABASE_URL_PROXY=postgres://postgres:postgres@127.0.0.1:58432/pgkinetic cargo run -p xtask -- compat --language rust --target direct-postgres --smoke",
    "PG_KINETIC_COMPAT_LIVE=1 PG_KINETIC_COMPAT_SERVICES=direct-postgres,pg-kinetic DATABASE_URL_DIRECT=postgres://postgres:postgres@127.0.0.1:55432/pgkinetic DATABASE_URL_PROXY=postgres://postgres:postgres@127.0.0.1:58432/pgkinetic cargo run -p xtask -- compat --language rust --target pg-kinetic --smoke"
  ]
}
EOF
}

on_exit() {
  exit_code=$?
  if ((exit_code != 0)); then
    mkdir -p "$EVIDENCE_DIR"
    "${COMPOSE[@]}" logs --no-color --timestamps pg-kinetic postgres >"$EVIDENCE_DIR/compose.log" 2>&1 || true
    write_summary "fail"
    printf 'stable gate failed during %s; see %s\n' "$stage" "$EVIDENCE_DIR/compose.log" >&2
  else
    write_summary "pass"
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$exit_code"
}
trap on_exit EXIT

cd "$REPO_ROOT"
stage="cargo fmt --check"
cargo fmt --check

stage="cargo test --workspace --locked"
cargo test --workspace --locked

stage="start PostgreSQL and pg-kinetic"
"${COMPOSE[@]}" down --volumes --remove-orphans
"${COMPOSE[@]}" up --detach --wait --build postgres pg-kinetic

stage="verify pg-kinetic query path"
"${COMPOSE[@]}" exec -T postgres env PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 \
  -h pg-kinetic -p 6543 -U postgres -d pgkinetic -c 'select 1'

stage="load compatibility fixtures"
cat compat/common/schema.sql compat/common/seed.sql \
  | "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d pgkinetic

stage="direct-postgres compatibility smoke"
"${COMPAT_ENV[@]}" cargo run -p xtask -- compat --language rust --target direct-postgres --smoke

stage="pg-kinetic compatibility smoke"
"${COMPAT_ENV[@]}" cargo run -p xtask -- compat --language rust --target pg-kinetic --smoke
