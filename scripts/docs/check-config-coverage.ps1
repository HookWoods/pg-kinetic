[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
python (Join-Path $repoRoot 'scripts/docs/check-config-docs.py')
