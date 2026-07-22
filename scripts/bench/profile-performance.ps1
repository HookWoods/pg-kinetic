param(
    [ValidateSet("flamegraph", "perf", "ebpf")]
    [string]$Kind = "flamegraph",
    [string]$Scenario = "bench/scenarios/benchmark-simple-query.toml",
    [string]$Target = "pg-kinetic",
    [string]$Output,
    [switch]$Validate
)

if ($Validate) {
    & cargo run -p pg-kinetic -- profile validate
    exit $LASTEXITCODE
}

$arguments = @(
    "run", "-p", "pg-kinetic", "--",
    "profile", "run",
    "--scenario", $Scenario,
    "--kind", $Kind,
    "--target", $Target
)

if ($Output) {
    $arguments += "--output", $Output
}

& cargo @arguments
exit $LASTEXITCODE
