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

$workerOutput = Join-Path $output 'adapters/sqlserver'
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
        'Microsoft.Bcl.Cryptography/9.0.13'
        'Microsoft.Data.SqlClient.Extensions.Abstractions/7.0.2'
        'Microsoft.Data.SqlClient.Internal.Logging/7.0.2'
        'Microsoft.Data.SqlClient.SNI.runtime/6.0.2'
        'Microsoft.Data.SqlClient/7.0.2'
        'Microsoft.Extensions.Caching.Abstractions/9.0.13'
        'Microsoft.Extensions.Caching.Memory/9.0.13'
        'Microsoft.Extensions.DependencyInjection.Abstractions/10.0.0'
        'Microsoft.Extensions.Logging.Abstractions/10.0.0'
        'Microsoft.Extensions.Options/9.0.13'
        'Microsoft.Extensions.Primitives/9.0.13'
        'Microsoft.IdentityModel.Abstractions/8.16.0'
        'Microsoft.IdentityModel.JsonWebTokens/8.16.0'
        'Microsoft.IdentityModel.Logging/8.16.0'
        'Microsoft.IdentityModel.Protocols.OpenIdConnect/8.16.0'
        'Microsoft.IdentityModel.Protocols/8.16.0'
        'Microsoft.IdentityModel.Tokens/8.16.0'
        'Microsoft.SqlServer.Server/1.0.0'
        'Microsoft.SqlServer.TransactSql.ScriptDom/180.59.2'
        'System.Configuration.ConfigurationManager/9.0.13'
        'System.Diagnostics.EventLog/9.0.13'
        'System.IdentityModel.Tokens.Jwt/8.16.0'
        'System.Security.Cryptography.Pkcs/9.0.13'
        'System.Security.Cryptography.ProtectedData/9.0.13'
    ) | Sort-Object

    $dependencies = [System.IO.File]::ReadAllText($DependencyPath) |
        ConvertFrom-Json -AsHashtable
    $forbiddenLibraryPrefixes = @(
        'CSharpDB.Migration.CSharpDb/',
        'CSharpDB.Migration.Files/',
        'CsvHelper/'
    )
    $unexpectedLibraries = @(
        $dependencies['libraries'].Keys |
            Where-Object {
                $library = $_
                $forbiddenLibraryPrefixes |
                    Where-Object {
                        $library.StartsWith($_, [StringComparison]::Ordinal)
                    }
            }
    )
    if ($unexpectedLibraries.Count -gt 0) {
        throw "The SQL Server worker contains excluded migration/file-import dependencies: $($unexpectedLibraries -join ', ')"
    }

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
        throw "The SQL Server worker package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
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
    -Project (Join-Path $root 'src/CSharpDB.Migration.SqlServer.Worker/CSharpDB.Migration.SqlServer.Worker.csproj') `
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
    'csharpdb-migration-sqlserver-worker.exe'
}
else {
    'csharpdb-migration-sqlserver-worker'
}

$requiredFiles = @(
    (Join-Path $output $(if ($targetIsWindows) { 'csharpdb.exe' } else { 'csharpdb' })),
    (Join-Path $output 'LICENSE'),
    (Join-Path $workerOutput $workerExecutableName),
    (Join-Path $workerOutput 'CSharpDB.Migration.CSharpDb.Ddl.dll'),
    (Join-Path $workerOutput 'CSharpDB.Migration.SqlServer.dll'),
    (Join-Path $workerOutput 'Microsoft.Data.SqlClient.dll'),
    (Join-Path $workerOutput 'Microsoft.SqlServer.TransactSql.ScriptDom.dll'),
    (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md'),
    (Join-Path $workerOutput 'licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'),
    (Join-Path $workerOutput 'csharpdb-migration-sqlserver-worker.deps.json')
)

$missing = @($requiredFiles | Where-Object {
    -not (Test-Path -LiteralPath $_ -PathType Leaf)
})
if ($missing.Count -gt 0) {
    throw "The SQL Server migration bundle is incomplete: $($missing -join ', ')"
}

$sniLicense = Join-Path `
    $workerOutput `
    'licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'
$expectedSniLicenseHash =
    '9335E8BAD875DD7BE4EEBD55D2335EB6433D1CEA61AADB3817AF7807BEF8932A'
$actualSniLicenseHash = (Get-FileHash `
        -LiteralPath $sniLicense `
        -Algorithm SHA256).Hash
if (-not $actualSniLicenseHash.Equals(
        $expectedSniLicenseHash,
        [StringComparison]::OrdinalIgnoreCase))
{
    throw 'The SQL Server SNI runtime license does not match the reviewed 6.0.2 terms.'
}

Assert-ReviewedWorkerPackageClosure `
    -DependencyPath (Join-Path $workerOutput 'csharpdb-migration-sqlserver-worker.deps.json') `
    -NoticePath (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md')

Write-Host "Created the non-packable SQL Server migration bundle at $output"
