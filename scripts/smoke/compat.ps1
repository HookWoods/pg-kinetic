param(
    [string]$DatabaseUrl = "postgres://postgres:postgres@host.docker.internal:58432/pgkinetic"
)

$ErrorActionPreference = "Stop"

function Assert-LastCommand {
    param([string]$Name)

    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

Push-Location compat\rust-tokio-postgres
$env:DATABASE_URL = "host=127.0.0.1 port=58432 user=postgres password=postgres dbname=pgkinetic"
cargo run
Assert-LastCommand "rust tokio-postgres smoke"
Pop-Location

Push-Location compat\go-pgx
$env:DATABASE_URL = $DatabaseUrl
go run .
Assert-LastCommand "go pgx smoke"
Pop-Location

Push-Location compat\node-pg
$env:DATABASE_URL = $DatabaseUrl
npm install
Assert-LastCommand "node pg install"
npm run smoke
Assert-LastCommand "node pg smoke"
Pop-Location

Push-Location compat\python-psycopg
$env:DATABASE_URL = $DatabaseUrl
python -m pip install -r requirements.txt
Assert-LastCommand "python psycopg install"
python smoke.py
Assert-LastCommand "python psycopg smoke"
Pop-Location

Write-Host "compat smoke passed"
