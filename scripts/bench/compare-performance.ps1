param(
    [Parameter(Mandatory = $true)]
    [string]$Baseline,
    [Parameter(Mandatory = $true)]
    [string]$Current
)

& cargo run -p pg-kinetic -- benchmark compare --baseline $Baseline --current $Current
exit $LASTEXITCODE
