#requires -Version 7.4

<#
.SYNOPSIS
Builds framework-dependent combined CSharpDB migration CLI release archives.

.DESCRIPTION
Composes the reviewed SQL Server and MySQL migration bundle publishers for
each selected runtime identifier and the Windows-only Access publisher for
win-x64. The script proves that every applicable publisher produced the same
base CLI bytes before it merges their fixed adapter directories, adds
installation assets, creates one archive per runtime, and writes
SHA256SUMS.txt.

These archives require the .NET 10 runtime. Creating an archive does not
qualify a runtime, live database version, authentication mode, or deployment
environment.

POSIX tar headers are host-independent: directories, the CLI, the SQL Server
and MySQL workers, and the POSIX installer use mode 0755; all other regular
files use mode 0644. Access assets are never staged for a POSIX runtime. Every
emitted tarball is reopened and checked against that contract before its
checksum is written.

.PARAMETER Version
Release version used in archive names. Defaults to the current release value
configured by the script. A leading v is accepted and removed.

.PARAMETER Runtime
Runtime identifiers to package. Defaults to win-x64, linux-x64, and osx-arm64.

.PARAMETER Configuration
Build configuration passed to every applicable audited bundle publisher.

.PARAMETER OutputRoot
Caller-selected root for publish, staging, and archive outputs. Defaults to
artifacts/migration-release.

.PARAMETER NoRestore
Passes -NoRestore to every applicable audited bundle publisher.

.PARAMETER Force
Allows this script to replace its own nonempty publish, stage, and archive
directories beneath OutputRoot. It never removes OutputRoot itself.

.EXAMPLE
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Publish-CSharpDbMigrationRelease.ps1 -Version $Version

.EXAMPLE
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Publish-CSharpDbMigrationRelease.ps1 `
  -Version $Version `
  -Runtime win-x64 `
  -OutputRoot artifacts\migration-release-local
#>
[CmdletBinding()]
param(
    [string] $Version = '4.6.1',

    [ValidateSet('win-x64', 'linux-x64', 'osx-arm64')]
    [string[]] $Runtime = @(
        'win-x64',
        'linux-x64',
        'osx-arm64'
    ),

    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [string] $OutputRoot,

    [switch] $NoRestore,

    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-ReleaseVersion {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RequestedVersion
    )

    $resolved = $RequestedVersion.Trim()
    if ($resolved.StartsWith(
            'v',
            [StringComparison]::OrdinalIgnoreCase))
    {
        $resolved = $resolved.Substring(1)
    }

    if ($resolved -notmatch
        '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$')
    {
        throw 'Version must be a safe semantic version in major.minor.patch form.'
    }

    return $resolved
}

function Test-EquivalentPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Left,

        [Parameter(Mandatory = $true)]
        [string] $Right
    )

    $comparison = if ($IsWindows -or $IsMacOS) {
        [StringComparison]::OrdinalIgnoreCase
    }
    else {
        [StringComparison]::Ordinal
    }
    return [string]::Equals(
        [IO.Path]::TrimEndingDirectorySeparator(
            [IO.Path]::GetFullPath($Left)),
        [IO.Path]::TrimEndingDirectorySeparator(
            [IO.Path]::GetFullPath($Right)),
        $comparison)
}

function Test-PathWithin {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Parent
    )

    $comparison = if ($IsWindows -or $IsMacOS) {
        [StringComparison]::OrdinalIgnoreCase
    }
    else {
        [StringComparison]::Ordinal
    }
    $fullPath = [IO.Path]::TrimEndingDirectorySeparator(
        [IO.Path]::GetFullPath($Path))
    $fullParent = [IO.Path]::TrimEndingDirectorySeparator(
        [IO.Path]::GetFullPath($Parent))
    $prefix = $fullParent + [IO.Path]::DirectorySeparatorChar
    return $fullPath.StartsWith($prefix, $comparison)
}

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
            (Test-EquivalentPath `
                -Left $current `
                -Right $parent.FullName))
        {
            break
        }

        $current = $parent.FullName
    }
}

function Assert-ManagedChildPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $ManagedRoot
    )

    if (-not (Test-PathWithin -Path $Path -Parent $ManagedRoot)) {
        throw "Refusing to manage a path outside OutputRoot: $Path"
    }
}

function Initialize-ManagedDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $ManagedRoot,

        [Parameter(Mandatory = $true)]
        [bool] $AllowReplacement
    )

    Assert-ExistingPathHasNoReparsePoints `
        -Path $ManagedRoot `
        -Description 'OutputRoot'
    Assert-ExistingPathHasNoReparsePoints `
        -Path $Path `
        -Description 'The managed release path'
    Assert-ManagedChildPath -Path $Path -ManagedRoot $ManagedRoot
    if (Test-Path -LiteralPath $Path) {
        if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
            throw "The managed release path is not a directory: $Path"
        }
        $managedItem = Get-Item -LiteralPath $Path -Force
        if (($managedItem.Attributes -band
                [IO.FileAttributes]::ReparsePoint) -ne 0)
        {
            throw "The managed release path cannot be a link or reparse point: $Path"
        }

        $hasContent = $null -ne (
            Get-ChildItem -LiteralPath $Path -Force |
                Select-Object -First 1)
        if ($hasContent -and -not $AllowReplacement) {
            throw "The managed release directory is not empty. Pass -Force to replace it: $Path"
        }

        if ($hasContent) {
            Assert-ExistingPathHasNoReparsePoints `
                -Path $Path `
                -Description 'The managed release path'
            $reparsePoints = @(
                Get-ChildItem `
                    -LiteralPath $Path `
                    -Recurse `
                    -Force |
                    Where-Object {
                        ($_.Attributes -band
                            [IO.FileAttributes]::ReparsePoint) -ne 0
                    })
            if ($reparsePoints.Count -gt 0) {
                throw "Refusing to replace a managed release directory containing links or reparse points: $Path"
            }
            Remove-Item -LiteralPath $Path -Recurse -Force
        }
    }

    [IO.Directory]::CreateDirectory($Path) | Out-Null
}

function Get-BaseRootManifest {
    param(
        [Parameter(Mandatory = $true)]
        [string] $BundleRoot
    )

    $fullRoot = [IO.Path]::GetFullPath($BundleRoot)
    return @(
        Get-ChildItem -LiteralPath $fullRoot -Recurse -File -Force |
            ForEach-Object {
                $relative = [IO.Path]::GetRelativePath(
                    $fullRoot,
                    $_.FullName).Replace(
                        [IO.Path]::DirectorySeparatorChar,
                        '/')
                if (-not $relative.StartsWith(
                        'adapters/',
                        [StringComparison]::Ordinal))
                {
                    [pscustomobject]@{
                        RelativePath = $relative
                        Length = $_.Length
                        Sha256 = (
                            Get-FileHash `
                                -LiteralPath $_.FullName `
                                -Algorithm SHA256
                        ).Hash.ToLowerInvariant()
                    }
                }
            } |
            Sort-Object RelativePath
    )
}

function Assert-ByteIdenticalBaseRoots {
    param(
        [Parameter(Mandatory = $true)]
        [string] $SqlServerBundle,

        [Parameter(Mandatory = $true)]
        [string] $MySqlBundle,

        [string] $AccessBundle
    )

    $sqlServerManifest = @(
        Get-BaseRootManifest -BundleRoot $SqlServerBundle)
    $mySqlManifest = @(
        Get-BaseRootManifest -BundleRoot $MySqlBundle)
    if ($sqlServerManifest.Count -ne $mySqlManifest.Count) {
        throw 'The SQL Server and MySQL bundle base CLI file sets differ.'
    }

    for ($index = 0;
         $index -lt $sqlServerManifest.Count;
         $index++)
    {
        $left = $sqlServerManifest[$index]
        $right = $mySqlManifest[$index]
        if (-not [string]::Equals(
                $left.RelativePath,
                $right.RelativePath,
                [StringComparison]::Ordinal) -or
            $left.Length -ne $right.Length -or
            -not [string]::Equals(
                $left.Sha256,
                $right.Sha256,
                [StringComparison]::Ordinal))
        {
            throw "The audited bundle publishers produced different base CLI bytes at '$($left.RelativePath)'."
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($AccessBundle)) {
        $accessManifest = @(
            Get-BaseRootManifest -BundleRoot $AccessBundle)
        if ($sqlServerManifest.Count -ne $accessManifest.Count) {
            throw 'The SQL Server and Access bundle base CLI file sets differ.'
        }

        for ($index = 0;
             $index -lt $sqlServerManifest.Count;
             $index++)
        {
            $left = $sqlServerManifest[$index]
            $right = $accessManifest[$index]
            if (-not [string]::Equals(
                    $left.RelativePath,
                    $right.RelativePath,
                    [StringComparison]::Ordinal) -or
                $left.Length -ne $right.Length -or
                -not [string]::Equals(
                    $left.Sha256,
                    $right.Sha256,
                    [StringComparison]::Ordinal))
            {
                throw "The audited Access bundle produced different base CLI bytes at '$($left.RelativePath)'."
            }
        }
    }
}

function Copy-DirectoryContents {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Source,

        [Parameter(Mandatory = $true)]
        [string] $Destination
    )

    [IO.Directory]::CreateDirectory($Destination) | Out-Null
    foreach ($item in Get-ChildItem -LiteralPath $Source -Force) {
        Copy-Item `
            -LiteralPath $item.FullName `
            -Destination $Destination `
            -Recurse `
            -Force
    }
}

function Assert-DotNetTenRuntimeConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "A required .NET runtime configuration is missing: $Path"
    }

    $configuration = [IO.File]::ReadAllText($Path) |
        ConvertFrom-Json -AsHashtable
    $runtimeOptions = $configuration['runtimeOptions']
    if ($null -eq $runtimeOptions) {
        throw "The .NET runtime configuration has no runtimeOptions object: $Path"
    }
    $frameworks = @()
    if ($runtimeOptions.ContainsKey('framework')) {
        $frameworks += $runtimeOptions['framework']
    }
    if ($runtimeOptions.ContainsKey('frameworks')) {
        $frameworks += @($runtimeOptions['frameworks'])
    }
    $dotNetRuntime = @(
        $frameworks |
            Where-Object {
                $_['name'] -eq 'Microsoft.NETCore.App' -and
                $_['version'] -match '^10\.'
            })
    if ($dotNetRuntime.Count -ne 1) {
        throw "The release is not bound to the required .NET 10 runtime: $Path"
    }
}

function Assert-FrameworkDependentStage {
    param(
        [Parameter(Mandatory = $true)]
        [string] $StageRoot,

        [Parameter(Mandatory = $true)]
        [bool] $TargetIsWindows
    )

    Assert-DotNetTenRuntimeConfig `
        -Path (Join-Path $StageRoot 'csharpdb.runtimeconfig.json')
    Assert-DotNetTenRuntimeConfig `
        -Path (Join-Path $StageRoot 'adapters/sqlserver/csharpdb-migration-sqlserver-worker.runtimeconfig.json')
    Assert-DotNetTenRuntimeConfig `
        -Path (Join-Path $StageRoot 'adapters/mysql/csharpdb-migration-mysql-worker.runtimeconfig.json')
    if ($TargetIsWindows) {
        Assert-DotNetTenRuntimeConfig `
            -Path (Join-Path $StageRoot 'adapters/access/csharpdb-migration-access-worker.runtimeconfig.json')
    }
    else {
        $accessAdapter =
            Join-Path $StageRoot 'adapters/access'
        if (Test-Path -LiteralPath $accessAdapter) {
            throw 'A POSIX migration release cannot contain the Windows-only Access adapter.'
        }

        $accessOnlyTokens = @(
            'CSharpDB.Migration.Access',
            'System.Data.OleDb',
            'csharpdb-migration-access-worker'
        )
        $stagedFiles = @(
            Get-ChildItem `
                -LiteralPath $StageRoot `
                -Recurse `
                -File `
                -Force)
        foreach ($token in $accessOnlyTokens) {
            $matchingFiles = @(
                $stagedFiles |
                    Where-Object {
                        $_.Name.Contains(
                            $token,
                            [StringComparison]::OrdinalIgnoreCase)
                    })
            if ($matchingFiles.Count -gt 0) {
                throw "A POSIX migration release contains the Windows-only Access asset '$token'."
            }
        }
        foreach ($dependencyFile in @(
            $stagedFiles |
                Where-Object {
                    $_.Name.EndsWith(
                        '.deps.json',
                        [StringComparison]::OrdinalIgnoreCase)
                }))
        {
            $dependencyText =
                [IO.File]::ReadAllText(
                    $dependencyFile.FullName)
            foreach ($token in $accessOnlyTokens) {
                if ($dependencyText.Contains(
                        $token,
                        [StringComparison]::OrdinalIgnoreCase))
                {
                    throw "A POSIX migration release dependency graph contains the Windows-only Access dependency '$token'."
                }
            }
        }
    }

    $selfContainedRuntimeFiles = @(
        'hostfxr.dll',
        'hostpolicy.dll',
        'coreclr.dll',
        'System.Private.CoreLib.dll',
        'libhostfxr.so',
        'libhostpolicy.so',
        'libcoreclr.so',
        'libhostfxr.dylib',
        'libhostpolicy.dylib',
        'libcoreclr.dylib'
    )
    $unexpected = @(
        Get-ChildItem -LiteralPath $StageRoot -Recurse -File -Force |
            Where-Object {
                $selfContainedRuntimeFiles -contains $_.Name
            })
    if ($unexpected.Count -gt 0) {
        throw "Self-contained runtime files were found in the framework-dependent migration release: $($unexpected.FullName -join ', ')"
    }

    $executableSuffix = if ($TargetIsWindows) {
        '.exe'
    }
    else {
        ''
    }
    $requiredFiles = @(
        (Join-Path $StageRoot "csharpdb$executableSuffix"),
        (Join-Path $StageRoot 'LICENSE'),
        (Join-Path $StageRoot 'README.md'),
        (Join-Path $StageRoot 'VERSION.txt'),
        (Join-Path $StageRoot "adapters/sqlserver/csharpdb-migration-sqlserver-worker$executableSuffix"),
        (Join-Path $StageRoot 'adapters/sqlserver/THIRD-PARTY-NOTICES.md'),
        (Join-Path $StageRoot 'adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'),
        (Join-Path $StageRoot "adapters/mysql/csharpdb-migration-mysql-worker$executableSuffix"),
        (Join-Path $StageRoot 'adapters/mysql/THIRD-PARTY-NOTICES.md'),
        (Join-Path $StageRoot 'install/windows/install-csharpdb-migration-tool.ps1'),
        (Join-Path $StageRoot 'install/posix/install-csharpdb-migration-tool.sh')
    )
    if ($TargetIsWindows) {
        $requiredFiles += @(
            (Join-Path $StageRoot 'adapters/access/csharpdb-migration-access-worker.exe'),
            (Join-Path $StageRoot 'adapters/access/CSharpDB.Migration.Access.dll'),
            (Join-Path $StageRoot 'adapters/access/CSharpDB.Migration.Retained.dll'),
            (Join-Path $StageRoot 'adapters/access/System.Data.OleDb.dll'),
            (Join-Path $StageRoot 'adapters/access/THIRD-PARTY-NOTICES.md'),
            (Join-Path $StageRoot 'adapters/access/csharpdb-migration-access-worker.deps.json')
        )
    }
    $missing = @(
        $requiredFiles |
            Where-Object {
                -not (Test-Path -LiteralPath $_ -PathType Leaf)
            })
    if ($missing.Count -gt 0) {
        throw "The combined migration release is incomplete: $($missing -join ', ')"
    }
}

function New-ZipArchive {
    param(
        [Parameter(Mandatory = $true)]
        [string] $SourceDirectory,

        [Parameter(Mandatory = $true)]
        [string] $DestinationPath
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [IO.Compression.ZipFile]::CreateFromDirectory(
        $SourceDirectory,
        $DestinationPath,
        [IO.Compression.CompressionLevel]::Optimal,
        $false)
}

function New-TarGzArchive {
    param(
        [Parameter(Mandatory = $true)]
        [string] $SourceDirectory,

        [Parameter(Mandatory = $true)]
        [string] $DestinationPath
    )

    Add-Type -AssemblyName System.Formats.Tar

    $sourceRoot = [IO.Path]::TrimEndingDirectorySeparator(
        [IO.Path]::GetFullPath($SourceDirectory))
    $archiveStream = $null
    $gzipStream = $null
    $tarWriter = $null
    try {
        $archiveStream = [IO.File]::Create($DestinationPath)
        $gzipStream = [IO.Compression.GZipStream]::new(
            $archiveStream,
            [IO.Compression.CompressionLevel]::Optimal,
            $true)
        $tarWriter = [System.Formats.Tar.TarWriter]::new(
            $gzipStream,
            [System.Formats.Tar.TarEntryFormat]::Pax,
            $true)

        $items = @(
            Get-ChildItem `
                -LiteralPath $sourceRoot `
                -Recurse `
                -Force |
                Sort-Object {
                    [IO.Path]::GetRelativePath(
                        $sourceRoot,
                        $_.FullName).Replace(
                            [IO.Path]::DirectorySeparatorChar,
                            '/')
                })
        foreach ($item in $items) {
            if (($item.Attributes -band
                    [IO.FileAttributes]::ReparsePoint) -ne 0)
            {
                throw "The POSIX release stage cannot contain links or reparse points: $($item.FullName)"
            }

            $relativePath = [IO.Path]::GetRelativePath(
                $sourceRoot,
                $item.FullName).Replace(
                    [IO.Path]::DirectorySeparatorChar,
                    '/')
            if ($relativePath -eq '..' -or
                $relativePath.StartsWith(
                    '../',
                    [StringComparison]::Ordinal) -or
                [IO.Path]::IsPathRooted($relativePath))
            {
                throw "A staged archive path escapes the release root: $relativePath"
            }

            $entryType = if ($item.PSIsContainer) {
                [System.Formats.Tar.TarEntryType]::Directory
            }
            elseif ($item -is [IO.FileInfo]) {
                [System.Formats.Tar.TarEntryType]::RegularFile
            }
            else {
                throw "The POSIX release stage contains an unsupported entry: $($item.FullName)"
            }
            $entry = [System.Formats.Tar.PaxTarEntry]::new(
                $entryType,
                $relativePath)
            $entry.Uid = 0
            $entry.Gid = 0
            $entry.ModificationTime =
                [DateTimeOffset]::new($item.LastWriteTimeUtc)
            $entry.Mode = Get-PosixArchiveMode `
                -RelativePath $relativePath `
                -IsDirectory $item.PSIsContainer

            if ($item.PSIsContainer) {
                $tarWriter.WriteEntry($entry)
                continue
            }

            $dataStream = [IO.File]::OpenRead($item.FullName)
            try {
                $entry.DataStream = $dataStream
                $tarWriter.WriteEntry($entry)
            }
            finally {
                $dataStream.Dispose()
            }
        }
    }
    catch {
        if ($null -ne $tarWriter) {
            $tarWriter.Dispose()
            $tarWriter = $null
        }
        if ($null -ne $gzipStream) {
            $gzipStream.Dispose()
            $gzipStream = $null
        }
        if ($null -ne $archiveStream) {
            $archiveStream.Dispose()
            $archiveStream = $null
        }
        if (Test-Path -LiteralPath $DestinationPath -PathType Leaf) {
            Remove-Item -LiteralPath $DestinationPath -Force
        }
        throw
    }
    finally {
        if ($null -ne $tarWriter) {
            $tarWriter.Dispose()
        }
        if ($null -ne $gzipStream) {
            $gzipStream.Dispose()
        }
        if ($null -ne $archiveStream) {
            $archiveStream.Dispose()
        }
    }
}

function Get-PosixArchiveMode {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RelativePath,

        [Parameter(Mandatory = $true)]
        [bool] $IsDirectory
    )

    if ($IsDirectory -or
        $RelativePath -in @(
            'csharpdb',
            'adapters/sqlserver/csharpdb-migration-sqlserver-worker',
            'adapters/mysql/csharpdb-migration-mysql-worker',
            'install/posix/install-csharpdb-migration-tool.sh'))
    {
        return [IO.UnixFileMode] 0x1ED
    }

    return [IO.UnixFileMode] 0x1A4
}

function Assert-PosixTarArchiveModes {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ArchivePath
    )

    Add-Type -AssemblyName System.Formats.Tar

    $requiredExecutables = @(
        'csharpdb',
        'adapters/sqlserver/csharpdb-migration-sqlserver-worker',
        'adapters/mysql/csharpdb-migration-mysql-worker',
        'install/posix/install-csharpdb-migration-tool.sh'
    )
    $foundExecutables =
        [Collections.Generic.HashSet[string]]::new(
            [StringComparer]::Ordinal)
    $archiveStream = [IO.File]::OpenRead($ArchivePath)
    $gzipStream = $null
    $tarReader = $null
    try {
        $gzipStream = [IO.Compression.GZipStream]::new(
            $archiveStream,
            [IO.Compression.CompressionMode]::Decompress,
            $true)
        $tarReader = [System.Formats.Tar.TarReader]::new(
            $gzipStream,
            $true)
        while ($null -ne (
                $entry = $tarReader.GetNextEntry($false)))
        {
            $isDirectory =
                $entry.EntryType -eq
                [System.Formats.Tar.TarEntryType]::Directory
            if (-not $isDirectory -and
                $entry.EntryType -ne
                [System.Formats.Tar.TarEntryType]::RegularFile)
            {
                throw "The POSIX archive contains an unsupported entry type at '$($entry.Name)'."
            }

            $expectedMode = Get-PosixArchiveMode `
                -RelativePath $entry.Name `
                -IsDirectory $isDirectory
            if ($entry.Mode -ne $expectedMode) {
                throw "The POSIX archive mode at '$($entry.Name)' is $($entry.Mode); expected $expectedMode."
            }
            if ($requiredExecutables -contains $entry.Name) {
                $foundExecutables.Add($entry.Name) | Out-Null
            }
        }
    }
    finally {
        if ($null -ne $tarReader) {
            $tarReader.Dispose()
        }
        if ($null -ne $gzipStream) {
            $gzipStream.Dispose()
        }
        $archiveStream.Dispose()
    }

    $missingExecutables = @(
        $requiredExecutables |
            Where-Object {
                -not $foundExecutables.Contains($_)
            })
    if ($missingExecutables.Count -gt 0) {
        throw "The POSIX archive is missing direct-run executables: $($missingExecutables -join ', ')"
    }
}

$repoRoot = (
    Resolve-Path (Join-Path $PSScriptRoot '..')
).ProviderPath
$releaseVersion = Resolve-ReleaseVersion $Version
$sqlServerPublisher = Join-Path `
    $PSScriptRoot `
    'Publish-CSharpDbSqlServerMigrationBundle.ps1'
$mySqlPublisher = Join-Path `
    $PSScriptRoot `
    'Publish-CSharpDbMySqlMigrationBundle.ps1'
$accessPublisher = Join-Path `
    $PSScriptRoot `
    'Publish-CSharpDbAccessMigrationBundle.ps1'
$installAssets = Join-Path `
    $repoRoot `
    'deploy/migration-tool'

foreach ($requiredInput in @(
        $sqlServerPublisher,
        $mySqlPublisher,
        $accessPublisher,
        (Join-Path $repoRoot 'LICENSE'),
        (Join-Path $installAssets 'README.md'),
        (Join-Path $installAssets 'windows/install-csharpdb-migration-tool.ps1'),
        (Join-Path $installAssets 'posix/install-csharpdb-migration-tool.sh')))
{
    if (-not (Test-Path -LiteralPath $requiredInput)) {
        throw "A required migration release input is missing: $requiredInput"
    }
}

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path `
        $repoRoot `
        'artifacts/migration-release'
}
elseif (-not [IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
$filesystemRoot = [IO.Path]::GetPathRoot($OutputRoot)
Assert-ExistingPathHasNoReparsePoints `
    -Path $repoRoot `
    -Description 'The repository root'
Assert-ExistingPathHasNoReparsePoints `
    -Path $OutputRoot `
    -Description 'OutputRoot'
if ((Test-EquivalentPath -Left $OutputRoot -Right $repoRoot) -or
    (Test-EquivalentPath -Left $OutputRoot -Right $filesystemRoot))
{
    throw 'OutputRoot cannot be the repository root or a filesystem root.'
}
if (Test-Path -LiteralPath $OutputRoot) {
    $outputRootItem = Get-Item -LiteralPath $OutputRoot -Force
    if (-not $outputRootItem.PSIsContainer -or
        ($outputRootItem.Attributes -band
            [IO.FileAttributes]::ReparsePoint) -ne 0)
    {
        throw 'An existing OutputRoot must be a real directory, not a link or reparse point.'
    }
}

$resolvedRuntimes = @(
    $Runtime |
        ForEach-Object { $_.Trim() })
if ($resolvedRuntimes.Count -eq 0 -or
    @(
        $resolvedRuntimes |
            Sort-Object -Unique
    ).Count -ne $resolvedRuntimes.Count)
{
    throw 'At least one unique runtime identifier is required.'
}

[IO.Directory]::CreateDirectory($OutputRoot) | Out-Null
Assert-ExistingPathHasNoReparsePoints `
    -Path $OutputRoot `
    -Description 'OutputRoot'
$publishRoot = Join-Path $OutputRoot 'publish'
$stageRoot = Join-Path $OutputRoot 'stage'
$archiveRoot = Join-Path $OutputRoot 'archives'
foreach ($managedDirectory in @(
        $publishRoot,
        $stageRoot,
        $archiveRoot))
{
    Initialize-ManagedDirectory `
        -Path $managedDirectory `
        -ManagedRoot $OutputRoot `
        -AllowReplacement $Force.IsPresent
}

$createdArchives =
    [Collections.Generic.List[string]]::new()
foreach ($rid in $resolvedRuntimes) {
    $targetIsWindows = $rid.StartsWith(
        'win-',
        [StringComparison]::OrdinalIgnoreCase)
    $ridPublishRoot = Join-Path $publishRoot $rid
    $sqlServerBundle = Join-Path `
        $ridPublishRoot `
        'sqlserver'
    $mySqlBundle = Join-Path `
        $ridPublishRoot `
        'mysql'
    $accessBundle = Join-Path `
        $ridPublishRoot `
        'access'
    $stageDirectory = Join-Path $stageRoot $rid
    Assert-ManagedChildPath `
        -Path $sqlServerBundle `
        -ManagedRoot $OutputRoot
    Assert-ManagedChildPath `
        -Path $mySqlBundle `
        -ManagedRoot $OutputRoot
    if ($targetIsWindows) {
        Assert-ManagedChildPath `
            -Path $accessBundle `
            -ManagedRoot $OutputRoot
    }
    Assert-ManagedChildPath `
        -Path $stageDirectory `
        -ManagedRoot $OutputRoot

    $publisherArguments = @{
        Configuration = $Configuration
        RuntimeIdentifier = $rid
    }
    if ($NoRestore.IsPresent) {
        $publisherArguments['NoRestore'] = $true
    }

    Write-Host "Publishing audited SQL Server migration bundle for $rid..."
    & $sqlServerPublisher `
        -OutputPath $sqlServerBundle `
        @publisherArguments

    Write-Host "Publishing audited MySQL migration bundle for $rid..."
    & $mySqlPublisher `
        -OutputPath $mySqlBundle `
        @publisherArguments

    if ($targetIsWindows) {
        Write-Host "Publishing audited Microsoft Access migration bundle for $rid..."
        & $accessPublisher `
            -OutputPath $accessBundle `
            @publisherArguments
    }

    $identityArguments = @{
        SqlServerBundle = $sqlServerBundle
        MySqlBundle = $mySqlBundle
    }
    if ($targetIsWindows) {
        $identityArguments['AccessBundle'] =
            $accessBundle
    }
    Assert-ByteIdenticalBaseRoots `
        @identityArguments

    [IO.Directory]::CreateDirectory($stageDirectory) |
        Out-Null
    Copy-DirectoryContents `
        -Source $sqlServerBundle `
        -Destination $stageDirectory

    $stageAdapters = Join-Path `
        $stageDirectory `
        'adapters'
    $stagedMySqlAdapter = Join-Path `
        $stageAdapters `
        'mysql'
    if (Test-Path -LiteralPath $stagedMySqlAdapter) {
        throw 'The SQL Server bundle unexpectedly contained the MySQL adapter path.'
    }
    Copy-Item `
        -LiteralPath (Join-Path $mySqlBundle 'adapters/mysql') `
        -Destination $stageAdapters `
        -Recurse

    if ($targetIsWindows) {
        $stagedAccessAdapter = Join-Path `
            $stageAdapters `
            'access'
        if (Test-Path -LiteralPath $stagedAccessAdapter) {
            throw 'An audited bundle unexpectedly contained the Access adapter path.'
        }
        Copy-Item `
            -LiteralPath (Join-Path $accessBundle 'adapters/access') `
            -Destination $stageAdapters `
            -Recurse
    }

    Copy-Item `
        -LiteralPath (Join-Path $installAssets 'README.md') `
        -Destination (Join-Path $stageDirectory 'README.md')
    Copy-Item `
        -LiteralPath $installAssets `
        -Destination (Join-Path $stageDirectory 'install') `
        -Recurse
    [IO.File]::WriteAllText(
        (Join-Path $stageDirectory 'VERSION.txt'),
        "v$releaseVersion`n",
        [Text.UTF8Encoding]::new($false))

    Assert-FrameworkDependentStage `
        -StageRoot $stageDirectory `
        -TargetIsWindows $targetIsWindows

    $archiveExtension = if ($targetIsWindows) {
        'zip'
    }
    else {
        'tar.gz'
    }
    $archiveName =
        "csharpdb-migration-tool-v$releaseVersion-$rid.$archiveExtension"
    $archivePath = Join-Path `
        $archiveRoot `
        $archiveName
    if (Test-Path -LiteralPath $archivePath) {
        throw "The archive destination already exists: $archivePath"
    }

    Write-Host "Creating $archiveName..."
    if ($targetIsWindows) {
        New-ZipArchive `
            -SourceDirectory $stageDirectory `
            -DestinationPath $archivePath
    }
    else {
        New-TarGzArchive `
            -SourceDirectory $stageDirectory `
            -DestinationPath $archivePath
        Assert-PosixTarArchiveModes `
            -ArchivePath $archivePath
    }
    $createdArchives.Add($archivePath) | Out-Null
}

$checksumLines = foreach ($archive in (
        $createdArchives |
            Sort-Object {
                [IO.Path]::GetFileName($_)
            }))
{
    $hash = Get-FileHash `
        -LiteralPath $archive `
        -Algorithm SHA256
    "$($hash.Hash.ToLowerInvariant())  $([IO.Path]::GetFileName($archive))"
}
$checksumsPath = Join-Path `
    $archiveRoot `
    'SHA256SUMS.txt'
[IO.File]::WriteAllLines(
    $checksumsPath,
    $checksumLines,
    [Text.Encoding]::ASCII)

Write-Host 'CSharpDB migration release archives complete.'
Write-Host "  Version: $releaseVersion"
Write-Host '  Deployment: framework-dependent (.NET 10 runtime required)'
Write-Host "  Archive root: $archiveRoot"
foreach ($archive in $createdArchives) {
    Write-Host "  Created: $archive"
}
Write-Host "  Checksums: $checksumsPath"
