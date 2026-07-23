param(
    [string]$Scenario = "bench/scenarios/benchmark-simple-query.toml",
    [string]$Baseline,
    [string]$Current
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$reportPath = Join-Path ([System.IO.Path]::GetTempPath()) ("pg-kinetic-performance-smoke-" + [System.Guid]::NewGuid() + ".json")

if ([string]::IsNullOrWhiteSpace($Baseline) -ne [string]::IsNullOrWhiteSpace($Current)) {
    throw "-Baseline and -Current must be supplied together"
}

Push-Location $repoRoot
try {
    & powershell.exe -ExecutionPolicy Bypass -File scripts\bench\run-performance.ps1 `
        -Scenario $Scenario `
        -Output $reportPath `
        -DryRun
    if ($LASTEXITCODE -ne 0) {
        throw "benchmark report smoke failed with exit code $LASTEXITCODE"
    }

    $reportJson = Get-Content -Raw $reportPath
    $report = $reportJson | ConvertFrom-Json
    if (-not $report.ok -or -not $report.dry_run -or [string]::IsNullOrWhiteSpace($report.scenario.name) -or @($report.results).Count -eq 0) {
        throw "benchmark smoke did not produce a valid dry-run report"
    }
    if ($reportJson.Contains("benchmark-secret")) {
        throw "benchmark smoke report contains an unredacted credential"
    }

    & powershell.exe -ExecutionPolicy Bypass -File scripts\bench\profile-performance.ps1 -Validate
    if ($LASTEXITCODE -ne 0) {
        throw "profile tool validation failed with exit code $LASTEXITCODE"
    }

    if ($Baseline) {
        if (-not (Test-Path $Baseline)) {
            throw "baseline report does not exist: $Baseline"
        }
        if (-not (Test-Path $Current)) {
            throw "current report does not exist: $Current"
        }

        & cargo run -p pg-kinetic -- benchmark score `
            --baseline $Baseline `
            --current $Current `
            --release
        if ($LASTEXITCODE -ne 0) {
            throw "benchmark regression gate failed with exit code $LASTEXITCODE"
        }
    }
} finally {
    if (Test-Path $reportPath) {
        Remove-Item -Force $reportPath
    }
    Pop-Location
}

Write-Host "performance smoke passed"
