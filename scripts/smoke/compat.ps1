param(
    [string]$DatabaseUrl = "postgres://postgres:postgres@127.0.0.1:58432/pgkinetic"
)

$ErrorActionPreference = "Stop"

Push-Location compat\rust-tokio-postgres
$env:DATABASE_URL = "host=127.0.0.1 port=58432 user=postgres password=postgres dbname=pgkinetic"
cargo run
Pop-Location

Push-Location compat\go-pgx
$env:DATABASE_URL = $DatabaseUrl
go run .
Pop-Location

Push-Location compat\node-pg
$env:DATABASE_URL = $DatabaseUrl
npm install
npm run smoke
Pop-Location

Push-Location compat\python-psycopg
$env:DATABASE_URL = $DatabaseUrl
python -m pip install -r requirements.txt
python smoke.py
Pop-Location

Write-Host "compat smoke passed"
