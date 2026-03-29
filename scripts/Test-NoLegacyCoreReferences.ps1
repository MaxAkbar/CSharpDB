Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
Set-Location $root

$allowList = @(
    'RELEASE_NOTES.md',
    'docs/releases/v2.0.0-pr-notes.md',
    'www/docs/migrations.html',
    'www/docs/index.html'
)

$legacyPattern = 'CSharpDB' + '\.Core'

function Get-RepoRelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return [System.IO.Path]::GetRelativePath($root, $Path).Replace('\', '/')
}

function Get-LegacyMatchesWithRipgrep {
    $rawMatches = @(
        & rg -n --hidden --color never `
            --glob '!**/.git/**' `
            --glob '!**/bin/**' `
            --glob '!**/obj/**' `
            $legacyPattern .
    )

    if ($LASTEXITCODE -gt 1) {
        throw 'ripgrep failed while scanning for legacy primitives references.'
    }

    $scanResults = @()

    foreach ($match in $rawMatches) {
        if ([string]::IsNullOrWhiteSpace($match)) {
            continue
        }

        $parts = $match -split ':', 3
        $path = $parts[0] -replace '^[.][/\\]', ''
        $path = $path.Replace('\', '/')

        $scanResults += [pscustomobject]@{
            Path = $path
            Display = $match
        }
    }

    return $scanResults
}

function Get-LegacyMatchesWithPowerShell {
    $scanResults = @()
    $scanErrors = @()
    $excludedDirectoryPattern = '(^|/)(?:\.git|bin|obj)(/|$)'

    $files = @(
        & git -C $root ls-files --cached --others --exclude-standard
    ) | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_) -and $_ -notmatch $excludedDirectoryPattern
    }

    if ($LASTEXITCODE -ne 0) {
        throw 'git ls-files failed while preparing the PowerShell fallback scan.'
    }

    foreach ($file in $files) {
        $relativePath = $file.Replace('\', '/')
        $fullPath = Join-Path $root $file

        try {
            $lineNumber = 0

            foreach ($line in [System.IO.File]::ReadLines($fullPath)) {
                $lineNumber++

                if ($line -match $legacyPattern) {
                    $scanResults += [pscustomobject]@{
                        Path = $relativePath
                        Display = '{0}:{1}:{2}' -f $relativePath, $lineNumber, $line
                    }
                }
            }
        }
        catch {
            $scanErrors += '{0}: {1}' -f $relativePath, $_.Exception.Message
        }
    }

    if ($scanErrors.Count -gt 0) {
        throw ("PowerShell fallback scan failed while reading files:`n{0}" -f ($scanErrors -join "`n"))
    }

    return $scanResults
}

$scanResults = if (Get-Command rg -ErrorAction SilentlyContinue) {
    Write-Host 'Using ripgrep to scan for legacy primitives references.'
    @(Get-LegacyMatchesWithRipgrep)
}
else {
    Write-Host 'ripgrep not found; falling back to a PowerShell file scan.'
    @(Get-LegacyMatchesWithPowerShell)
}

$violations = @()

foreach ($scanResult in $scanResults) {
    if ($allowList -notcontains $scanResult.Path) {
        $violations += $scanResult.Display
    }
}

if ($violations.Count -gt 0) {
    Write-Host 'Found disallowed legacy primitives references:'
    $violations | ForEach-Object { Write-Host $_ }
    exit 1
}

Write-Host 'No disallowed legacy primitives references found.'
