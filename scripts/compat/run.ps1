param(
    [string]$Language,
    [string]$Library,
    [string]$Target,
    [string]$Category,
    [switch]$Smoke,
    [switch]$List
)

$ErrorActionPreference = "Stop"

$arguments = @("run", "-p", "pg-kinetic", "--", "compat")
if ($List) {
    $arguments += "list"
} else {
    $arguments += "run"
}
$arguments += @("--manifest", "regression/manifest.toml")

if ($Language) { $arguments += @("--language", $Language) }
if ($Library) { $arguments += @("--library", $Library) }
if ($Target) { $arguments += @("--target", $Target) }
if ($Category) { $arguments += @("--category", $Category) }
if ($Smoke) { $arguments += "--smoke" }

cargo @arguments
if ($LASTEXITCODE -ne 0) {
    throw "compatibility runner failed with exit code $LASTEXITCODE"
}
