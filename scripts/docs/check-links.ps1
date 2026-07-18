[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$markdownFiles = @(
    (Join-Path $repoRoot 'README.md'),
    (Join-Path $repoRoot 'docs-site/README.md')
) + @(Get-ChildItem -Path (Join-Path $repoRoot 'docs') -Filter '*.md' -File -Recurse)
$brokenLinks = @()

foreach ($file in $markdownFiles) {
    $filePath = if ($file -is [System.IO.FileInfo]) { $file.FullName } else { $file }
    $content = Get-Content -Raw -LiteralPath $filePath

    foreach ($match in [regex]::Matches($content, '\[[^\]]+\]\(([^)\s]+)')) {
        $target = $match.Groups[1].Value.Trim('<>')
        $targetPath = ($target -split '#', 2)[0]

        if ([string]::IsNullOrWhiteSpace($targetPath) -or
            $targetPath -match '^[a-zA-Z][a-zA-Z0-9+.-]*:' -or
            $targetPath.StartsWith('//')) {
            continue
        }

        $resolvedPath = if ([System.IO.Path]::IsPathRooted($targetPath)) {
            Join-Path $repoRoot $targetPath.TrimStart('/', '\\')
        } else {
            Join-Path (Split-Path -Parent $filePath) $targetPath
        }

        if (-not (Test-Path -LiteralPath $resolvedPath)) {
            $relativeFile = [System.IO.Path]::GetRelativePath($repoRoot, $filePath)
            $brokenLinks += "$relativeFile -> $target"
        }
    }
}

if ($brokenLinks.Count -gt 0) {
    $brokenLinks | ForEach-Object { Write-Error "broken link: $_" }
    exit 1
}

Write-Output 'Markdown links are valid.'
