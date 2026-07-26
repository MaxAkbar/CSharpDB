#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $OutputPath,

    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [ValidateSet('win-x64')]
    [string] $RuntimeIdentifier = 'win-x64',

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-ExistingPathHasNoReparsePoints {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Description
    )

    $current = [IO.Path]::GetFullPath($Path)
    while (-not [string]::IsNullOrWhiteSpace($current)) {
        $item = Get-Item `
            -LiteralPath $current `
            -Force `
            -ErrorAction SilentlyContinue
        if ($null -ne $item -and
            ($item.Attributes -band
                [IO.FileAttributes]::ReparsePoint) -ne 0)
        {
            throw "$Description cannot pass through a link or reparse point: $current"
        }

        $parent = [IO.Directory]::GetParent($current)
        if ($null -eq $parent -or
            [string]::Equals(
                [IO.Path]::TrimEndingDirectorySeparator($current),
                [IO.Path]::TrimEndingDirectorySeparator(
                    $parent.FullName),
                [StringComparison]::OrdinalIgnoreCase))
        {
            break
        }
        $current = $parent.FullName
    }
}

function Invoke-Publish {
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
        '-p:UseAppHost=true',
        '-r',
        $RuntimeIdentifier,
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

function Assert-NoAccessAssets {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Directory,

        [string] $ExcludedDirectory
    )

    $forbidden = @(
        'CSharpDB.Migration.Access',
        'System.Data.OleDb',
        'System.Configuration.ConfigurationManager',
        'System.Diagnostics.EventLog',
        'System.Diagnostics.PerformanceCounter',
        'System.Security.Cryptography.ProtectedData'
    )
    $files = @(
        Get-ChildItem `
            -LiteralPath $Directory `
            -File `
            -Recurse
    )
    if (-not [string]::IsNullOrWhiteSpace(
            $ExcludedDirectory))
    {
        $excludedPrefix =
            [IO.Path]::GetFullPath(
                $ExcludedDirectory).TrimEnd(
                    [IO.Path]::DirectorySeparatorChar,
                    [IO.Path]::AltDirectorySeparatorChar) +
            [IO.Path]::DirectorySeparatorChar
        $files = @($files | Where-Object {
            -not $_.FullName.StartsWith(
                $excludedPrefix,
                [StringComparison]::OrdinalIgnoreCase)
        })
    }

    foreach ($token in $forbidden) {
        $matchingFiles = @(
            $files |
                Where-Object {
                    $_.Name.Contains(
                        $token,
                        [StringComparison]::OrdinalIgnoreCase)
                })
        if ($matchingFiles.Count -gt 0)
        {
            throw "The base host output contains Access-only asset '$token'."
        }
    }
    foreach ($dependencyFile in @(
        $files |
            Where-Object {
                $_.Name.EndsWith(
                    '.deps.json',
                    [StringComparison]::OrdinalIgnoreCase)
            }))
    {
        $content =
            [IO.File]::ReadAllText(
                $dependencyFile.FullName)
        foreach ($token in $forbidden) {
            if ($content.Contains(
                    $token,
                    [StringComparison]::OrdinalIgnoreCase))
            {
                throw "The base host dependency graph contains Access-only dependency '$token'."
            }
        }
    }
}

function Assert-ReviewedWorkerPackageClosure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DependencyPath,

        [Parameter(Mandatory = $true)]
        [string] $NoticePath
    )

    $expectedPackages = @(
        'System.Configuration.ConfigurationManager/10.0.9'
        'System.Data.OleDb/10.0.9'
        'System.Diagnostics.EventLog/10.0.9'
        'System.Diagnostics.PerformanceCounter/10.0.9'
        'System.Security.Cryptography.ProtectedData/10.0.9'
    ) | Sort-Object
    $dependencies =
        [IO.File]::ReadAllText($DependencyPath) |
            ConvertFrom-Json -AsHashtable
    $actualPackages = @(
        ($dependencies['libraries']).GetEnumerator() |
            Where-Object {
                $_.Value['type'] -eq 'package'
            } |
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
        throw "The Access worker package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
    }

    $notice =
        [IO.File]::ReadAllText($NoticePath)
    foreach ($package in $expectedPackages) {
        $separator = $package.LastIndexOf('/')
        $entry = '| ' +
            $package.Substring(0, $separator) +
            ' | ' +
            $package.Substring($separator + 1) +
            ' |'
        if (-not $notice.Contains(
                $entry,
                [StringComparison]::Ordinal))
        {
            throw "The reviewed package is absent from THIRD-PARTY-NOTICES.md: $package"
        }
    }
}

$root = (
    Resolve-Path (Join-Path $PSScriptRoot '..')
).ProviderPath
$output = [IO.Path]::GetFullPath(
    $(if ([IO.Path]::IsPathRooted($OutputPath)) {
        $OutputPath
    }
    else {
        Join-Path $root $OutputPath
    }))
$filesystemRoot = [IO.Path]::GetPathRoot($output)
if ([string]::Equals(
        [IO.Path]::TrimEndingDirectorySeparator(
            $output),
        [IO.Path]::TrimEndingDirectorySeparator(
            $root),
        [StringComparison]::OrdinalIgnoreCase) -or
    [string]::Equals(
        [IO.Path]::TrimEndingDirectorySeparator(
            $output),
        [IO.Path]::TrimEndingDirectorySeparator(
            $filesystemRoot),
        [StringComparison]::OrdinalIgnoreCase))
{
    throw 'The bundle destination cannot be the repository root or a filesystem root.'
}

Assert-ExistingPathHasNoReparsePoints `
    -Path $root `
    -Description 'The repository root'
Assert-ExistingPathHasNoReparsePoints `
    -Path $output `
    -Description 'The bundle destination'
if (Test-Path -LiteralPath $output) {
    if (-not (Test-Path `
            -LiteralPath $output `
            -PathType Container))
    {
        throw "The bundle destination is not a directory: $output"
    }
    if (Get-ChildItem `
            -LiteralPath $output `
            -Force |
        Select-Object -First 1)
    {
        throw "The bundle destination must not already contain files: $output"
    }
}
else {
    [IO.Directory]::CreateDirectory($output) |
        Out-Null
}

$workerOutput =
    Join-Path $output 'adapters/access'
[IO.Directory]::CreateDirectory(
    $workerOutput) | Out-Null

Invoke-Publish `
    -Project (Join-Path `
        $root `
        'src/CSharpDB.Cli/CSharpDB.Cli.csproj') `
    -Destination $output
Invoke-Publish `
    -Project (Join-Path `
        $root `
        'src/CSharpDB.Migration.Access.Worker/CSharpDB.Migration.Access.Worker.csproj') `
    -Destination $workerOutput

Copy-Item `
    -LiteralPath (Join-Path $root 'LICENSE') `
    -Destination (Join-Path $output 'LICENSE') `
    -Force

$requiredFiles = @(
    (Join-Path $output 'csharpdb.exe'),
    (Join-Path $output 'LICENSE'),
    (Join-Path $workerOutput 'csharpdb-migration-access-worker.exe'),
    (Join-Path $workerOutput 'CSharpDB.Migration.Access.dll'),
    (Join-Path $workerOutput 'CSharpDB.Migration.Retained.dll'),
    (Join-Path $workerOutput 'System.Data.OleDb.dll'),
    (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md'),
    (Join-Path $workerOutput 'csharpdb-migration-access-worker.deps.json')
)
$missing = @($requiredFiles | Where-Object {
    -not (Test-Path `
        -LiteralPath $_ `
        -PathType Leaf)
})
if ($missing.Count -gt 0) {
    throw "The Access migration bundle is incomplete: $($missing -join ', ')"
}

Assert-NoAccessAssets `
    -Directory $output `
    -ExcludedDirectory $workerOutput
Assert-ReviewedWorkerPackageClosure `
    -DependencyPath (Join-Path `
        $workerOutput `
        'csharpdb-migration-access-worker.deps.json') `
    -NoticePath (Join-Path `
        $workerOutput `
        'THIRD-PARTY-NOTICES.md')

Write-Host (
    'Created the Windows-only, non-packable ' +
    "Microsoft Access migration bundle at $output")
