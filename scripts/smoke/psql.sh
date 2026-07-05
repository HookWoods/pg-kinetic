#!/usr/bin/env bash
set -euo pipefail

HOST_NAME="${HOST_NAME:-127.0.0.1}"
PORT="${PORT:-58432}"
USER_NAME="${USER_NAME:-postgres}"
DATABASE="${DATABASE:-pgkinetic}"
PASSWORD="${PASSWORD:-postgres}"

if command -v powershell.exe >/dev/null 2>&1; then
  powershell.exe -ExecutionPolicy Bypass -File scripts\\smoke\\psql.ps1 \
    -HostName "${HOST_NAME}" \
    -Port "${PORT}" \
    -User "${USER_NAME}" \
    -Database "${DATABASE}" \
    -Password "${PASSWORD}"
  exit 0
fi

export PGPASSWORD="${PASSWORD}"

result="$(
  psql \
    -h "${HOST_NAME}" \
    -p "${PORT}" \
    -U "${USER_NAME}" \
    -d "${DATABASE}" \
    -Atc "select count(*) from accounts;"
)"

if [[ "${result}" != "2" ]]; then
  echo "expected account count 2, got '${result}'" >&2
  exit 1
fi

echo "psql smoke passed on ${HOST_NAME}:${PORT}"
