[CmdletBinding()]
param(
    [string]$FeedPath = 'artifacts/nuget',

    [string]$Version,

    [string]$ManifestPath,

    [switch]$ValidateExistingManifest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$versionResolver = Join-Path $PSScriptRoot 'Get-NuGetPackageIdentityVersion.ps1'
$releaseWorkflow = Join-Path $repoRoot '.github/workflows/release.yml'

function Resolve-RepoPath {
    param([Parameter(Mandatory)][string]$Path)

    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }

    return [IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

function Read-PackageManifest {
    param([Parameter(Mandatory)][IO.FileInfo]$Package)

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [IO.Compression.ZipFile]::OpenRead($Package.FullName)
    try {
        $nuspecEntries = @($archive.Entries | Where-Object { $_.FullName -like '*.nuspec' })
        if ($nuspecEntries.Count -ne 1) {
            throw "Expected exactly one nuspec in $($Package.FullName)."
        }

        $reader = [IO.StreamReader]::new($nuspecEntries[0].Open())
        try {
            [xml]$nuspec = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $archive.Dispose()
    }

    $metadata = $nuspec.SelectSingleNode(
        "/*[local-name()='package']/*[local-name()='metadata']")
    if ($null -eq $metadata) {
        throw "Package metadata was not found in $($Package.FullName)."
    }

    return [pscustomobject]@{
        File = $Package
        Metadata = $metadata
        Id = [string]$metadata.id
        Version = [string]$metadata.version
        Dependencies = @($metadata.SelectNodes(
            "*[local-name()='dependencies']//*[local-name()='dependency']"))
    }
}

function Get-MetadataText {
    param(
        [Parameter(Mandatory)]
        [object]$Metadata,

        [Parameter(Mandatory)]
        [string]$Name
    )

    $node = $Metadata.SelectSingleNode("*[local-name()='$Name']")
    if ($null -eq $node) {
        return ''
    }

    return [string]$node.InnerText
}

function Test-ExactCandidateRange {
    param(
        [Parameter(Mandatory)]
        [string]$Range,

        [Parameter(Mandatory)]
        [string]$CandidateVersion
    )

    $normalized = $Range.Trim()
    return $normalized -ceq "[$CandidateVersion]"
}

function Test-PrereleaseVersion {
    param([Parameter(Mandatory)][string]$Value)

    return $Value -match '(?<![A-Za-z0-9])\d+\.\d+(?:\.\d+)?-[0-9A-Za-z]'
}

function Get-ReleasePackageOrder {
    $order = [Collections.Generic.List[string]]::new()
    foreach ($line in [IO.File]::ReadLines($releaseWorkflow)) {
        if ($line -notmatch 'dotnet\s+pack\s+(?<Project>src/[^\s]+?\.csproj)\b') {
            continue
        }

        $projectPath = Resolve-RepoPath $Matches.Project
        if (-not (Test-Path -LiteralPath $projectPath -PathType Leaf)) {
            throw "Release workflow pack project does not exist: $($Matches.Project)"
        }
        [xml]$project = Get-Content -Raw -LiteralPath $projectPath
        $packageIdNode = $project.SelectSingleNode('/Project/PropertyGroup/PackageId')
        $packageId = if ($null -eq $packageIdNode -or
            [string]::IsNullOrWhiteSpace([string]$packageIdNode.InnerText)) {
            [IO.Path]::GetFileNameWithoutExtension($projectPath)
        }
        else {
            [string]$packageIdNode.InnerText
        }
        if ($order.Contains($packageId)) {
            throw "Release workflow packs $packageId more than once."
        }
        $order.Add($packageId)
    }

    if ($order.Count -eq 0) {
        throw 'Release workflow contains no package candidates.'
    }
    return $order
}

$resolvedFeed = Resolve-RepoPath $FeedPath
if (-not (Test-Path -LiteralPath $resolvedFeed -PathType Container)) {
    throw "The local NuGet feed was not found: $resolvedFeed"
}
if (-not (Test-Path -LiteralPath $versionResolver -PathType Leaf)) {
    throw "The NuGet package-version resolver was not found: $versionResolver"
}
if (-not (Test-Path -LiteralPath $releaseWorkflow -PathType Leaf)) {
    throw "The release workflow was not found: $releaseWorkflow"
}

$packageFiles = @(
    Get-ChildItem -LiteralPath $resolvedFeed -Filter '*.nupkg' -File |
        Where-Object { $_.Name -notlike '*.symbols.nupkg' } |
        Sort-Object Name
)
if ($packageFiles.Count -eq 0) {
    throw "The local NuGet feed contains no package candidates: $resolvedFeed"
}

$manifests = @($packageFiles | ForEach-Object { Read-PackageManifest $_ })
if ([string]::IsNullOrWhiteSpace($Version)) {
    $versions = @($manifests.Version | Sort-Object -Unique)
    if ($versions.Count -ne 1) {
        throw "Specify -Version when the feed contains more than one package version: $($versions -join ', ')"
    }
    $Version = $versions[0]
}
$Version = $Version.Trim()
$packageIdentityVersion = (& $versionResolver -Version $Version | Select-Object -Last 1).Trim()

$candidatePackages = @($manifests | Where-Object { $_.Version -ceq $Version })
if ($candidatePackages.Count -eq 0) {
    throw "No package in $resolvedFeed has candidate version $Version."
}

$packagesById = @{}
foreach ($package in $candidatePackages) {
    if ([string]::IsNullOrWhiteSpace($package.Id)) {
        throw "A package in the candidate feed has no package id: $($package.File.Name)"
    }
    if ($packagesById.ContainsKey($package.Id)) {
        throw "The candidate feed contains duplicate package id $($package.Id)."
    }
    if (-not $package.Id.StartsWith('CSharpDB', [StringComparison]::Ordinal)) {
        throw "The release candidate feed contains an unexpected package id: $($package.Id)"
    }
    $packagesById[$package.Id] = $package

    $expectedFileName = "$($package.Id).$packageIdentityVersion.nupkg"
    if ($package.File.Name -cne $expectedFileName) {
        throw "Package file name mismatch. Expected $expectedFileName, found $($package.File.Name)."
    }

    foreach ($field in @(
        'id',
        'version',
        'authors',
        'description',
        'license',
        'projectUrl',
        'tags',
        'readme',
        'icon'
    )) {
        if ([string]::IsNullOrWhiteSpace((Get-MetadataText $package.Metadata $field))) {
            throw "$($package.Id) is missing required package metadata: $field"
        }
    }

    $repository = $package.Metadata.SelectSingleNode("*[local-name()='repository']")
    if ([string]::IsNullOrWhiteSpace([string]$repository.url) -or
        [string]::IsNullOrWhiteSpace([string]$repository.type)) {
        throw "$($package.Id) repository metadata must include type and url."
    }
}

$releasePackageOrder = @(Get-ReleasePackageOrder)
$missingReleasePackages = @(
    $releasePackageOrder | Where-Object { -not $packagesById.ContainsKey($_) }
)
$extraCandidatePackages = @(
    $packagesById.Keys | Where-Object { $releasePackageOrder -cnotcontains $_ }
)
if ($missingReleasePackages.Count -gt 0 -or $extraCandidatePackages.Count -gt 0) {
    throw "Candidate package set does not match the release workflow. Missing: $($missingReleasePackages -join ', '); extra: $($extraCandidatePackages -join ', ')."
}

$releaseIndexes = @{}
for ($index = 0; $index -lt $releasePackageOrder.Count; $index++) {
    $releaseIndexes[$releasePackageOrder[$index]] = $index
}

foreach ($package in $candidatePackages) {
    foreach ($dependency in $package.Dependencies) {
        $dependencyId = [string]$dependency.id
        $dependencyRange = [string]$dependency.version
        if ([string]::IsNullOrWhiteSpace($dependencyId) -or
            [string]::IsNullOrWhiteSpace($dependencyRange)) {
            throw "$($package.Id) contains an incomplete dependency declaration."
        }
        if ($dependencyRange.Contains('*', [StringComparison]::Ordinal)) {
            throw "$($package.Id) uses floating dependency $dependencyId $dependencyRange."
        }

        if ($dependencyId.StartsWith('CSharpDB', [StringComparison]::Ordinal)) {
            if (-not $packagesById.ContainsKey($dependencyId)) {
                throw "$($package.Id) references in-release dependency $dependencyId, but its candidate package is absent."
            }
            if (-not (Test-ExactCandidateRange $dependencyRange $Version)) {
                throw "$($package.Id) must reference exact candidate dependency $dependencyId $Version; found $dependencyRange."
            }
            if ($releaseIndexes[$dependencyId] -ge $releaseIndexes[$package.Id]) {
                throw "Release package order is not topological: $($package.Id) appears before dependency $dependencyId."
            }
        }
        elseif (Test-PrereleaseVersion $dependencyRange) {
            throw "$($package.Id) contains unreviewed prerelease dependency $dependencyId $dependencyRange."
        }
    }
}

$visitState = @{}
function Visit-Package {
    param(
        [Parameter(Mandatory)][string]$PackageId,
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [Collections.Generic.List[string]]$Path
    )

    $state = $visitState[$PackageId]
    if ($state -eq 2) {
        return
    }
    if ($state -eq 1) {
        throw "The candidate package dependency graph contains a cycle: $($Path -join ' -> ') -> $PackageId"
    }

    $visitState[$PackageId] = 1
    $Path.Add($PackageId)
    foreach ($dependency in $packagesById[$PackageId].Dependencies) {
        $dependencyId = [string]$dependency.id
        if ($packagesById.ContainsKey($dependencyId)) {
            Visit-Package -PackageId $dependencyId -Path $Path
        }
    }
    $Path.RemoveAt($Path.Count - 1)
    $visitState[$PackageId] = 2
}

foreach ($packageId in @($packagesById.Keys | Sort-Object)) {
    Visit-Package -PackageId $packageId -Path ([Collections.Generic.List[string]]::new())
}

$manifest = [ordered]@{
    schemaVersion = '1.0'
    candidateVersion = $Version
    packages = @(
        $releasePackageOrder |
            ForEach-Object { $packagesById[$_] } |
            ForEach-Object {
                [ordered]@{
                    id = $_.Id
                    version = $_.Version
                    file = $_.File.Name
                    sha256 = (Get-FileHash -LiteralPath $_.File.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
                    dependencies = @(
                        $_.Dependencies |
                            Sort-Object { [string]$_.id }, { [string]$_.version } |
                            ForEach-Object {
                                [ordered]@{
                                    id = [string]$_.id
                                    version = [string]$_.version
                                }
                            }
                    )
                }
            }
    )
}

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $resolvedFeed 'CSharpDB-PACKAGE-MANIFEST.json'
}
else {
    $ManifestPath = Resolve-RepoPath $ManifestPath
}
$manifestJson = $manifest | ConvertTo-Json -Depth 10
if ($ValidateExistingManifest) {
    if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) {
        throw "The existing package manifest was not found: $ManifestPath"
    }
    $existingJson = (Get-Content -Raw -LiteralPath $ManifestPath | ConvertFrom-Json) |
        ConvertTo-Json -Depth 10
    if ($existingJson -cne $manifestJson) {
        throw "The existing package manifest does not match the qualified package graph: $ManifestPath"
    }
}
else {
    $manifestDirectory = Split-Path -Parent $ManifestPath
    New-Item -ItemType Directory -Path $manifestDirectory -Force | Out-Null
    $manifestJson | Set-Content -LiteralPath $ManifestPath -Encoding utf8
}

Write-Host "Candidate package graph, metadata, dependency policy, and hashes passed for $($candidatePackages.Count) packages."
$manifestAction = if ($ValidateExistingManifest) { 'Validated' } else { 'Wrote' }
Write-Host "$manifestAction deterministic package manifest: $ManifestPath"
