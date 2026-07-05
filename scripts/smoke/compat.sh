#!/usr/bin/env bash
set -euo pipefail

DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:58432/pgkinetic}"

(
  cd compat/rust-tokio-postgres
  DATABASE_URL="host=127.0.0.1 port=58432 user=postgres password=postgres dbname=pgkinetic" cargo run
)

(
  cd compat/go-pgx
  DATABASE_URL="${DATABASE_URL}" go run .
)

(
  cd compat/node-pg
  DATABASE_URL="${DATABASE_URL}" npm install
  DATABASE_URL="${DATABASE_URL}" npm run smoke
)

(
  cd compat/python-psycopg
  python -m pip install -r requirements.txt
  DATABASE_URL="${DATABASE_URL}" python smoke.py
)

echo "compat smoke passed"
