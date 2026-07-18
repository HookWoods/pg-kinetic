param(
    [string]$Manifest = "regression/manifest.toml",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Arguments
)

& cargo run -p pg-kinetic -- regression run --manifest $Manifest @Arguments
exit $LASTEXITCODE
