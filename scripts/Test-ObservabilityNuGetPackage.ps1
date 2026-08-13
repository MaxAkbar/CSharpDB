[CmdletBinding()]
param(
    [string]$FeedPath = 'artifacts/nuget',

    [string]$Version,

    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',

    [switch]$KeepWorkingDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$fixtureRoot = Join-Path $repoRoot 'tests/package-smoke/observability'
$globalJson = Join-Path $repoRoot 'global.json'
$versionResolver = Join-Path $PSScriptRoot 'Get-NuGetPackageIdentityVersion.ps1'
$standaloneProjectName = 'CSharpDB.Observability.PackageSmoke.csproj'
$metapackageProjectName = 'CSharpDB.Observability.MetaPackageSmoke.csproj'

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }

    return [IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

function Read-PackageManifest {
    param(
        [Parameter(Mandatory)]
        [string]$PackagePath
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [IO.Compression.ZipFile]::OpenRead($PackagePath)
    try {
        $nuspecEntries = @($archive.Entries | Where-Object { $_.FullName -like '*.nuspec' })
        if ($nuspecEntries.Count -ne 1) {
            throw "Expected exactly one nuspec in $PackagePath."
        }

        $reader = [IO.StreamReader]::new($nuspecEntries[0].Open())
        try {
            [xml]$nuspec = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }

        $entries = @($archive.Entries | ForEach-Object { $_.FullName })
    }
    finally {
        $archive.Dispose()
    }

    return [pscustomobject]@{
        Path = $PackagePath
        Xml = $nuspec
        Entries = $entries
    }
}

function Get-PackageMetadata {
    param(
        [Parameter(Mandatory)]
        [object]$Manifest
    )

    $metadata = $Manifest.Xml.SelectSingleNode(
        "/*[local-name()='package']/*[local-name()='metadata']")
    if ($null -eq $metadata) {
        throw "Package metadata was not found in $($Manifest.Path)."
    }

    return $metadata
}

function Get-PackageDependencyNodes {
    param(
        [Parameter(Mandatory)]
        [object]$Manifest
    )

    return @($Manifest.Xml.SelectNodes(
        "/*[local-name()='package']/*[local-name()='metadata']/*[local-name()='dependencies']//*[local-name()='dependency']"))
}

function Assert-PackageIdentity {
    param(
        [Parameter(Mandatory)]
        [object]$Manifest,

        [Parameter(Mandatory)]
        [string]$ExpectedId,

        [Parameter(Mandatory)]
        [string]$ExpectedVersion
    )

    $metadata = Get-PackageMetadata $Manifest
    if ([string]$metadata.id -cne $ExpectedId -or
        [string]$metadata.version -cne $ExpectedVersion) {
        throw "Package identity mismatch in $($Manifest.Path). Expected $ExpectedId $ExpectedVersion, found $($metadata.id) $($metadata.version)."
    }
}

function Assert-PackageDependency {
    param(
        [Parameter(Mandatory)]
        [object]$Manifest,

        [Parameter(Mandatory)]
        [string]$ExpectedId
    )

    $match = Get-PackageDependencyNodes $Manifest |
        Where-Object { [string]$_.id -ceq $ExpectedId } |
        Select-Object -First 1
    if ($null -eq $match) {
        throw "$($Manifest.Path) does not declare its required $ExpectedId package dependency."
    }
}

function Invoke-DotNet {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter(Mandatory)]
        [string]$WorkingDirectory,

        [Parameter(Mandatory)]
        [string]$Description
    )

    Write-Host $Description
    $exitCode = 0
    Push-Location -LiteralPath $WorkingDirectory
    try {
        & dotnet @Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($exitCode -ne 0) {
        throw "$Description failed with exit code $exitCode."
    }
}

if (-not (Test-Path -LiteralPath $fixtureRoot -PathType Container)) {
    throw "Package smoke fixtures were not found: $fixtureRoot"
}
if (-not (Test-Path -LiteralPath $globalJson -PathType Leaf)) {
    throw "The repository SDK selection file was not found: $globalJson"
}
if (-not (Test-Path -LiteralPath $versionResolver -PathType Leaf)) {
    throw "The NuGet package-version resolver was not found: $versionResolver"
}

$fixtureProjects = @(Get-ChildItem -LiteralPath $fixtureRoot -Filter '*.csproj' -File -Recurse)
if ($fixtureProjects.Count -ne 2) {
    throw "Expected exactly two observability package smoke fixture projects."
}
foreach ($fixtureProject in $fixtureProjects) {
    [xml]$fixtureXml = Get-Content -Raw -LiteralPath $fixtureProject.FullName
    if ($null -ne $fixtureXml.SelectSingleNode('/Project/ItemGroup/ProjectReference')) {
        throw "Package smoke fixtures must not contain ProjectReference items: $($fixtureProject.FullName)"
    }
}

$resolvedFeed = Resolve-RepoPath $FeedPath
if (-not (Test-Path -LiteralPath $resolvedFeed -PathType Container)) {
    throw "The local NuGet feed was not found: $resolvedFeed"
}

$packagePrefix = 'CSharpDB.Observability.'
$observabilityCandidates = @(
    Get-ChildItem -LiteralPath $resolvedFeed -Filter "$packagePrefix*.nupkg" -File |
        Where-Object { $_.Name -notlike '*.symbols.nupkg' }
)

if ([string]::IsNullOrWhiteSpace($Version)) {
    if ($observabilityCandidates.Count -ne 1) {
        $candidateNames = if ($observabilityCandidates.Count -eq 0) {
            '(none)'
        }
        else {
            $observabilityCandidates.Name -join ', '
        }
        throw "Specify -Version when the feed does not contain exactly one observability package. Found: $candidateNames"
    }

    $candidateManifest = Read-PackageManifest $observabilityCandidates[0].FullName
    $Version = [string](Get-PackageMetadata $candidateManifest).version
}
$Version = $Version.Trim()
$packageIdentityVersion = (& $versionResolver -Version $Version | Select-Object -Last 1)
if ([string]::IsNullOrWhiteSpace($packageIdentityVersion)) {
    throw "Could not resolve the NuGet package identity version for $Version."
}
$packageIdentityVersion = $packageIdentityVersion.Trim()

$observabilityPackage = Join-Path $resolvedFeed "CSharpDB.Observability.$packageIdentityVersion.nupkg"
$sqlPackage = Join-Path $resolvedFeed "CSharpDB.Sql.$packageIdentityVersion.nupkg"
$metapackage = Join-Path $resolvedFeed "CSharpDB.$packageIdentityVersion.nupkg"
foreach ($package in @($observabilityPackage, $sqlPackage, $metapackage)) {
    if (-not (Test-Path -LiteralPath $package -PathType Leaf)) {
        throw "Required package was not found in the local feed: $package"
    }
}

$observabilityManifest = Read-PackageManifest $observabilityPackage
$sqlManifest = Read-PackageManifest $sqlPackage
$metapackageManifest = Read-PackageManifest $metapackage
Assert-PackageIdentity $observabilityManifest 'CSharpDB.Observability' $Version
Assert-PackageIdentity $sqlManifest 'CSharpDB.Sql' $Version
Assert-PackageIdentity $metapackageManifest 'CSharpDB' $Version

$unexpectedDependencies = @(Get-PackageDependencyNodes $observabilityManifest)
if ($unexpectedDependencies.Count -ne 0) {
    throw 'CSharpDB.Observability must remain dependency-free.'
}

foreach ($requiredEntry in @(
    'lib/net10.0/CSharpDB.Observability.dll',
    'README.md',
    'icon.png'
)) {
    if ($observabilityManifest.Entries -cnotcontains $requiredEntry) {
        throw "CSharpDB.Observability is missing required package content: $requiredEntry"
    }
}

Assert-PackageDependency $sqlManifest 'CSharpDB.Observability'
Assert-PackageDependency $metapackageManifest 'CSharpDB.Observability'

$systemTempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$workingRoot = [IO.Path]::GetFullPath((Join-Path $systemTempRoot (
    "csharpdb-observability-package-smoke-$([Guid]::NewGuid().ToString('N'))")))
if (-not $workingRoot.StartsWith(
        $systemTempRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase)) {
    throw "The package smoke working directory must be inside the system temporary directory."
}

$workingFixtureRoot = Join-Path $workingRoot 'fixtures'
$standaloneRoot = Join-Path $workingFixtureRoot 'standalone'
$metapackageRoot = Join-Path $workingFixtureRoot 'metapackage'
$packagesPath = Join-Path $workingRoot 'packages'
$nugetConfig = Join-Path $workingRoot 'NuGet.Config'
$previousNuGetPackages = $env:NUGET_PACKAGES

try {
    New-Item -ItemType Directory -Path $workingFixtureRoot -Force | Out-Null
    Copy-Item -LiteralPath $globalJson -Destination (Join-Path $workingRoot 'global.json') -Force
    Copy-Item -LiteralPath (Join-Path $fixtureRoot 'standalone') `
        -Destination $standaloneRoot `
        -Recurse `
        -Force
    Copy-Item -LiteralPath (Join-Path $fixtureRoot 'metapackage') `
        -Destination $metapackageRoot `
        -Recurse `
        -Force

    $escapedFeed = [Security.SecurityElement]::Escape($resolvedFeed)
    $escapedPackages = [Security.SecurityElement]::Escape($packagesPath)
    @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <config>
    <add key="globalPackagesFolder" value="$escapedPackages" />
  </config>
  <packageSources>
    <clear />
    <add key="csharpdb-local" value="$escapedFeed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="csharpdb-local">
      <package pattern="CSharpDB*" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="Google.*" />
      <package pattern="Grpc.*" />
      <package pattern="Microsoft.*" />
      <package pattern="NETStandard.Library" />
      <package pattern="runtime.*" />
      <package pattern="System.*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
"@ | Set-Content -LiteralPath $nugetConfig -Encoding utf8

    $env:NUGET_PACKAGES = $packagesPath
    $commonProperties = @(
        "/p:CSharpDbPackageVersion=$packageIdentityVersion",
        '/p:NuGetAudit=false'
    )

    foreach ($fixture in @(
        [pscustomobject]@{
            Root = $standaloneRoot
            Project = $standaloneProjectName
            Description = 'standalone CSharpDB.Observability package'
        },
        [pscustomobject]@{
            Root = $metapackageRoot
            Project = $metapackageProjectName
            Description = 'CSharpDB metapackage observability surface'
        }
    )) {
        Invoke-DotNet `
            -WorkingDirectory $fixture.Root `
            -Description "Restoring the $($fixture.Description) from a clean package cache." `
            -Arguments (@(
                'restore',
                $fixture.Project,
                '--configfile',
                $nugetConfig,
                '--packages',
                $packagesPath,
                '--no-cache'
            ) + $commonProperties)

        Invoke-DotNet `
            -WorkingDirectory $fixture.Root `
            -Description "Building the $($fixture.Description)." `
            -Arguments (@(
                'build',
                $fixture.Project,
                '-c',
                $Configuration,
                '--no-restore'
            ) + $commonProperties)

        Invoke-DotNet `
            -WorkingDirectory $fixture.Root `
            -Description "Running the $($fixture.Description)." `
            -Arguments (@(
                'run',
                '--project',
                $fixture.Project,
                '-c',
                $Configuration,
                '--no-build',
                '--no-restore'
            ) + $commonProperties)
    }

    Write-Host 'Observability package content, dependency, and clean-consumer qualification passed.'
}
finally {
    $env:NUGET_PACKAGES = $previousNuGetPackages

    if ($KeepWorkingDirectory) {
        Write-Host "Package smoke working directory retained at $workingRoot"
    }
    elseif (Test-Path -LiteralPath $workingRoot) {
        Remove-Item -LiteralPath $workingRoot -Recurse -Force
    }
}
