#requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
$temporaryParent = Join-Path $root '.tmp'
[System.IO.Directory]::CreateDirectory($temporaryParent) | Out-Null
$temporaryParentItem = Get-Item -LiteralPath $temporaryParent -Force
if (($temporaryParentItem.Attributes -band
        [System.IO.FileAttributes]::ReparsePoint) -ne 0)
{
    throw "The isolation workspace parent cannot be a reparse point: $temporaryParent"
}
$workspace = Join-Path `
    $temporaryParent `
    ("mysql-migration-isolation-" + [Guid]::NewGuid().ToString('N'))
[System.IO.Directory]::CreateDirectory($workspace) | Out-Null

function Invoke-DotNetPublish {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Project,

        [Parameter(Mandatory = $true)]
        [string] $Destination
    )

    $arguments = @(
        'publish',
        $Project,
        '-c',
        $Configuration,
        '--nologo',
        '-o',
        $Destination,
        '-p:GenerateDependencyFile=true',
        '--self-contained',
        'false'
    )
    if ($NoRestore) {
        $arguments += '--no-restore'
    }

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed for $Project."
    }
}

function Assert-NoMySqlAssets {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Directory
    )

    $forbidden = @(
        'CSharpDB.Migration.MySql',
        'MySqlConnector'
    )
    $files = @(Get-ChildItem -LiteralPath $Directory -File -Recurse)
    foreach ($token in $forbidden) {
        $matchingFiles = @($files | Where-Object {
            $_.Name.Contains($token, [StringComparison]::OrdinalIgnoreCase)
        })
        if ($matchingFiles.Count -gt 0) {
            throw "The base CLI output contains MySQL-only asset '$token'."
        }
    }

    $dependencyFiles = @($files | Where-Object {
        $_.Extension -eq '.json' -and
        $_.Name.EndsWith('.deps.json', [StringComparison]::OrdinalIgnoreCase)
    })
    foreach ($dependencyFile in $dependencyFiles) {
        $content = [System.IO.File]::ReadAllText($dependencyFile.FullName)
        foreach ($token in $forbidden) {
            if ($content.Contains($token, [StringComparison]::OrdinalIgnoreCase)) {
                throw "The base CLI dependency graph contains MySQL-only dependency '$token'."
            }
        }
    }
}

function Assert-ReviewedAdapterPackageClosure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DependencyPath,

        [Parameter(Mandatory = $true)]
        [string] $NoticePath
    )

    $expectedPackages = @(
        'Microsoft.Extensions.DependencyInjection.Abstractions/8.0.2'
        'Microsoft.Extensions.Logging.Abstractions/8.0.2'
        'MySqlConnector/2.6.1'
    ) | Sort-Object

    $dependencies = [System.IO.File]::ReadAllText($DependencyPath) |
        ConvertFrom-Json -AsHashtable
    $actualPackages = @(
        $dependencies['libraries'].GetEnumerator() |
            Where-Object { $_.Value['type'] -eq 'package' } |
            ForEach-Object { $_.Key } |
            Sort-Object
    )
    if (-not [string]::Equals(
            ($expectedPackages -join "`n"),
            ($actualPackages -join "`n"),
            [StringComparison]::Ordinal))
    {
        $difference = Compare-Object `
            -ReferenceObject $expectedPackages `
            -DifferenceObject $actualPackages `
            -CaseSensitive
        throw "The MySQL adapter package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
    }

    $notice = [System.IO.File]::ReadAllText($NoticePath)
    foreach ($package in $expectedPackages) {
        $separator = $package.LastIndexOf('/')
        $noticeEntry = '| ' +
            $package.Substring(0, $separator) +
            ' | ' +
            $package.Substring($separator + 1) +
            ' |'
        if (-not $notice.Contains(
                $noticeEntry,
                [StringComparison]::Ordinal))
        {
            throw "The reviewed package is absent from THIRD-PARTY-NOTICES.md: $package"
        }
    }
}

try {
    $baseOutput = Join-Path $workspace 'base'
    $adapterOutput = Join-Path $workspace 'adapter'
    [System.IO.Directory]::CreateDirectory($baseOutput) | Out-Null
    [System.IO.Directory]::CreateDirectory($adapterOutput) | Out-Null

    Invoke-DotNetPublish `
        -Project (Join-Path $root 'src/CSharpDB.Cli/CSharpDB.Cli.csproj') `
        -Destination $baseOutput
    Invoke-DotNetPublish `
        -Project (Join-Path $root 'src/CSharpDB.Migration.MySql/CSharpDB.Migration.MySql.csproj') `
        -Destination $adapterOutput

    Assert-NoMySqlAssets -Directory $baseOutput

    $dependencyPath = Join-Path `
        $adapterOutput `
        'CSharpDB.Migration.MySql.deps.json'
    $noticePath = Join-Path `
        $root `
        'src/CSharpDB.Migration.MySql/THIRD-PARTY-NOTICES.md'
    $requiredAdapterFiles = @(
        (Join-Path $adapterOutput 'CSharpDB.Migration.MySql.dll'),
        (Join-Path $adapterOutput 'MySqlConnector.dll'),
        (Join-Path $adapterOutput 'Microsoft.Extensions.DependencyInjection.Abstractions.dll'),
        (Join-Path $adapterOutput 'Microsoft.Extensions.Logging.Abstractions.dll'),
        $dependencyPath,
        $noticePath
    )
    foreach ($requiredFile in $requiredAdapterFiles) {
        if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
            throw "The optional MySQL adapter output is missing $requiredFile."
        }
    }

    Assert-ReviewedAdapterPackageClosure `
        -DependencyPath $dependencyPath `
        -NoticePath $noticePath

    Write-Host 'MySQL migration adapter isolation is valid.'
}
finally {
    $resolvedWorkspace = [System.IO.Path]::GetFullPath($workspace)
    $resolvedParent = [System.IO.Path]::GetFullPath($temporaryParent)
    $expectedPrefix = $resolvedParent.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar) +
        [System.IO.Path]::DirectorySeparatorChar
    $leaf = [System.IO.Path]::GetFileName($resolvedWorkspace)
    $pathComparison = if ($IsWindows) {
        [StringComparison]::OrdinalIgnoreCase
    }
    else {
        [StringComparison]::Ordinal
    }

    if ($resolvedWorkspace.StartsWith(
            $expectedPrefix,
            $pathComparison) -and
        $leaf.StartsWith(
            'mysql-migration-isolation-',
            [StringComparison]::Ordinal))
    {
        $workspaceItem = Get-Item -LiteralPath $resolvedWorkspace -Force `
            -ErrorAction SilentlyContinue
        if ($null -ne $workspaceItem) {
            if (($workspaceItem.Attributes -band
                    [System.IO.FileAttributes]::ReparsePoint) -ne 0)
            {
                throw "Refusing to clean a reparse-point workspace: $resolvedWorkspace"
            }

            Remove-Item -LiteralPath $resolvedWorkspace -Recurse -Force `
                -ErrorAction Stop
        }

        if (Test-Path -LiteralPath $resolvedWorkspace) {
            throw "The isolation workspace could not be removed: $resolvedWorkspace"
        }
    }
    else {
        throw "Refusing to clean an unexpected isolation workspace: $resolvedWorkspace"
    }
}
