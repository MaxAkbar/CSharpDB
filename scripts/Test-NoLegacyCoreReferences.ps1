Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $root

$allowList = @(
    'docs/migrations/core-to-primitives.md',
    'RELEASE_NOTES.md',
    'docs/releases/v2.0.0-pr-notes.md'
)

$legacyPattern = 'CSharpDB' + '\.Core'
$matches = @(rg -n --hidden --glob '!**/bin/**' --glob '!**/obj/**' $legacyPattern .)

if ($LASTEXITCODE -gt 1) {
    throw 'ripgrep failed while scanning for legacy primitives references.'
}

$violations = @()

foreach ($match in $matches) {
    if ([string]::IsNullOrWhiteSpace($match)) {
        continue
    }

    $parts = $match -split ':', 2
    if ($parts.Count -lt 2) {
        $violations += $match
        continue
    }

    $path = $parts[0] -replace '^[.][/\\]', ''
    $path = $path.Replace('\', '/')

    if ($allowList -notcontains $path) {
        $violations += $match
    }
}

if ($violations.Count -gt 0) {
    Write-Host 'Found disallowed legacy primitives references:'
    $violations | ForEach-Object { Write-Host $_ }
    exit 1
}

Write-Host 'No disallowed legacy primitives references found.'
