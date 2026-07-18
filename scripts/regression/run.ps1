param(
    [string]$Manifest = "regression/manifest.toml",
    [switch]$List,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Arguments
)

$Mode = if ($List) { "list" } else { "run" }
& cargo run -p pg-kinetic -- regression $Mode --manifest $Manifest @Arguments
exit $LASTEXITCODE
