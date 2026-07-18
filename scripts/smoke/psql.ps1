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

function Invoke-PsqlScalar {
    param([string]$Sql)

    if (Get-Command psql -ErrorAction SilentlyContinue) {
        return (& psql -X -v ON_ERROR_STOP=1 -h $HostName -p $Port -U $User -d $Database -Atc $Sql) -join "`n"
    }

    if (Test-DockerAvailable) {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
        Push-Location $repoRoot
        try {
            return (& docker compose -f bench/compose.yml exec -T postgres env `
                "PGPASSWORD=$Password" `
                "PGSSLMODE=$SslMode" `
                "PGGSSENCMODE=$GssEncMode" `
                "PGCONNECT_TIMEOUT=$ConnectTimeoutSeconds" `
                psql -X -v ON_ERROR_STOP=1 -h pg-kinetic -p 6543 -U $User -d $Database -Atc $Sql) -join "`n"
        } finally {
            Pop-Location
        }
    }

    Write-Host "SKIP: psql is not available and Docker fallback is unavailable"
    exit 0
}

$result = Invoke-PsqlScalar "select count(*) from accounts;"

if ($result.Trim() -ne "2") {
    throw "expected account count 2, got '$result'"
}

Write-Host "psql smoke passed on ${HostName}:${Port}"
