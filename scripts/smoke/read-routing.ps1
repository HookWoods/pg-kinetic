param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 58432,
    [string]$User = "postgres",
    [string]$Database = "pgkinetic",
    [string]$Password = "postgres",
    [string]$SslMode = "disable",
    [string]$GssEncMode = "disable",
    [int]$ConnectTimeoutSeconds = 10
)

$ErrorActionPreference = "Stop"
$env:PGPASSWORD = $Password
$env:PGSSLMODE = $SslMode
$env:PGGSSENCMODE = $GssEncMode
$env:PGCONNECT_TIMEOUT = $ConnectTimeoutSeconds

function Test-DockerAvailable {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        return $false
    }

    $stdout = Join-Path ([System.IO.Path]::GetTempPath()) ("pg-kinetic-docker-version-" + [guid]::NewGuid().ToString("n") + ".out")
    $stderr = Join-Path ([System.IO.Path]::GetTempPath()) ("pg-kinetic-docker-version-" + [guid]::NewGuid().ToString("n") + ".err")
    try {
        $process = Start-Process -FilePath "docker" -ArgumentList @("version", "--format", "{{.Server.Version}}") -NoNewWindow -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        if (-not $process.WaitForExit(5000)) {
            Stop-Process -Id $process.Id -Force
            return $false
        }

        return $process.ExitCode -eq 0
    } finally {
        Remove-Item -Force -ErrorAction SilentlyContinue $stdout, $stderr
    }
}

function Invoke-PgScalar {
    param([string]$Sql)

    if (Get-Command psql -ErrorAction SilentlyContinue) {
        $output = (& psql `
            -X `
            -v ON_ERROR_STOP=1 `
            -h $HostName `
            -p $Port `
            -U $User `
            -d $Database `
            -Atq `
            -c $Sql) -join "`n"
    } elseif (Test-DockerAvailable) {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
        Push-Location $repoRoot
        try {
            $output = (& docker compose -f bench/compose.yml exec -T postgres env `
                "PGPASSWORD=$Password" `
                "PGSSLMODE=$SslMode" `
                "PGGSSENCMODE=$GssEncMode" `
                "PGCONNECT_TIMEOUT=$ConnectTimeoutSeconds" `
                psql -X -v ON_ERROR_STOP=1 -h pg-kinetic -p 6543 -U $User -d $Database -Atq -c $Sql) -join "`n"
        } finally {
            Pop-Location
        }
    } else {
        Write-Host "SKIP: psql is not available and Docker fallback is unavailable"
        exit 0
    }

    if ($LASTEXITCODE -ne 0) {
        throw "psql failed with exit code $LASTEXITCODE for SQL: $Sql"
    }

    return $output.Trim()
}

function Assert-CountTwo {
    param(
        [string]$Name,
        [string]$Sql
    )

    $result = Invoke-PgScalar $Sql
    if ($result -ne "2") {
        throw "$Name expected account count 2, got '$result'"
    }
}

Assert-CountTwo "primary hint" "/* pg-kinetic: primary */ select count(*) from accounts;"
Assert-CountTwo "replica hint" "/* pg-kinetic: replica */ select count(*) from accounts;"
Assert-CountTwo "stale-ok hint" "/* pg-kinetic: stale-ok */ select count(*) from accounts;"
Assert-CountTwo "strict-fresh hint" "/* pg-kinetic: strict-fresh */ select count(*) from accounts;"
Assert-CountTwo "read-only transaction" "begin read only; select count(*) from accounts; commit;"

Write-Host "read routing smoke passed on ${HostName}:${Port}"
