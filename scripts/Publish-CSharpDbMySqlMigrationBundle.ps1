#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $OutputPath,

    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [string] $RuntimeIdentifier,

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
$output = [System.IO.Path]::GetFullPath(
    $(if ([System.IO.Path]::IsPathRooted($OutputPath)) {
        $OutputPath
    }
    else {
        Join-Path $root $OutputPath
    }))

if (Test-Path -LiteralPath $output) {
    if (-not (Test-Path -LiteralPath $output -PathType Container)) {
        throw "The bundle destination is not a directory: $output"
    }

    if ((Get-ChildItem -LiteralPath $output -Force | Select-Object -First 1)) {
        throw "The bundle destination must not already contain files: $output"
    }
}
else {
    [System.IO.Directory]::CreateDirectory($output) | Out-Null
}

$workerOutput = Join-Path $output 'adapters/mysql'
[System.IO.Directory]::CreateDirectory($workerOutput) | Out-Null

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
        '-p:UseAppHost=true'
    )

    if ($NoRestore) {
        $arguments += '--no-restore'
    }

    if (-not [string]::IsNullOrWhiteSpace($RuntimeIdentifier)) {
        $arguments += @('-r', $RuntimeIdentifier)
    }

    $arguments += @(
        '--self-contained',
        'false'
    )

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed for $Project."
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
        throw "The MySQL worker package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
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

Invoke-Publish `
    -Project (Join-Path $root 'src/CSharpDB.Cli/CSharpDB.Cli.csproj') `
    -Destination $output
Invoke-Publish `
    -Project (Join-Path $root 'src/CSharpDB.Migration.MySql.Worker/CSharpDB.Migration.MySql.Worker.csproj') `
    -Destination $workerOutput

Copy-Item `
    -LiteralPath (Join-Path $root 'LICENSE') `
    -Destination (Join-Path $output 'LICENSE') `
    -Force

$targetIsWindows = if ([string]::IsNullOrWhiteSpace($RuntimeIdentifier)) {
    $IsWindows
}
else {
    $RuntimeIdentifier.StartsWith(
        'win-',
        [StringComparison]::OrdinalIgnoreCase)
}

$workerExecutableName = if ($targetIsWindows) {
    'csharpdb-migration-mysql-worker.exe'
}
else {
    'csharpdb-migration-mysql-worker'
}

$requiredFiles = @(
    (Join-Path $output $(if ($targetIsWindows) { 'csharpdb.exe' } else { 'csharpdb' })),
    (Join-Path $output 'LICENSE'),
    (Join-Path $workerOutput $workerExecutableName),
    (Join-Path $workerOutput 'CSharpDB.Migration.Retained.dll'),
    (Join-Path $workerOutput 'CSharpDB.Migration.MySql.dll'),
    (Join-Path $workerOutput 'MySqlConnector.dll'),
    (Join-Path $workerOutput 'Microsoft.Extensions.DependencyInjection.Abstractions.dll'),
    (Join-Path $workerOutput 'Microsoft.Extensions.Logging.Abstractions.dll'),
    (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md'),
    (Join-Path $workerOutput 'csharpdb-migration-mysql-worker.deps.json')
)

$missing = @($requiredFiles | Where-Object {
    -not (Test-Path -LiteralPath $_ -PathType Leaf)
})
if ($missing.Count -gt 0) {
    throw "The MySQL migration bundle is incomplete: $($missing -join ', ')"
}

Assert-ReviewedWorkerPackageClosure `
    -DependencyPath (Join-Path $workerOutput 'csharpdb-migration-mysql-worker.deps.json') `
    -NoticePath (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md')

Write-Host "Created the non-packable MySQL migration bundle at $output"
