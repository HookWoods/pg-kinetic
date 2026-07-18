param(
    [string]$Scenario = "bench/scenarios/benchmark-simple-query.toml",
    [string]$Output = "bench/results/benchmark-simple-query.json",
    [switch]$DryRun
)

$arguments = @(
    "run", "-p", "pg-kinetic", "--",
    "benchmark", "run",
    "--scenario", $Scenario,
    "--format", "json",
    "--output", $Output
)

$arguments += "--dry-run"

& cargo @arguments
exit $LASTEXITCODE
