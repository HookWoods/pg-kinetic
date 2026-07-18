#!/usr/bin/env bash
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"go"}\n'
  exit 0
fi

if ! go list -m github.com/jackc/pgx/v5 >/dev/null 2>&1; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"go"}\n'
  exit 0
fi

go run ./compat/go
