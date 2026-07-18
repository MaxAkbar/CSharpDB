[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$')]
    [string]$Version,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$TagCommit,

    [string]$SnapshotRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$releaseVersion = $Version.TrimStart('v')
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$docsRoot = Join-Path $repoRoot 'www/docs'
$canonicalManifestPath = Join-Path $docsRoot 'sql-compatibility.json'
$canonicalSchemaPath = Join-Path $docsRoot 'sql-compatibility.schema.json'
if ([string]::IsNullOrWhiteSpace($SnapshotRoot)) {
    $SnapshotRoot = Join-Path $docsRoot 'sql-compatibility/releases'
}
$SnapshotRoot = [IO.Path]::GetFullPath($SnapshotRoot)
$relativeSnapshotRoot = [IO.Path]::GetRelativePath($repoRoot, $SnapshotRoot).Replace('\', '/')
if ([IO.Path]::IsPathRooted($relativeSnapshotRoot) -or
    $relativeSnapshotRoot -eq '..' -or
    $relativeSnapshotRoot.StartsWith('../', [StringComparison]::Ordinal)) {
    throw "Compatibility release snapshots must be inside the repository: $SnapshotRoot"
}

$snapshotDirectory = Join-Path $SnapshotRoot $releaseVersion
$snapshotManifestPath = Join-Path $snapshotDirectory 'sql-compatibility.json'
$snapshotSchemaPath = Join-Path $snapshotDirectory 'sql-compatibility.schema.json'
$qualificationPaths = @(
    'src',
    'tests',
    'www/docs/sql-compatibility.json',
    'www/docs/sql-compatibility.schema.json',
    'scripts/Build-SqlCompatibilityMatrix.ps1',
    'scripts/Test-Documentation.ps1',
    'scripts/New-SqlCompatibilityReleaseSnapshot.ps1',
    'scripts/Test-SqlCompatibilityRelease.ps1',
    '.github/workflows/release.yml'
)

& git -C $repoRoot cat-file -e "$TagCommit^{commit}"
if ($LASTEXITCODE -ne 0) {
    throw "Tag commit '$TagCommit' is not available in repository history."
}
$resolvedTagCommit = (& git -C $repoRoot rev-parse "$TagCommit^{commit}").Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolvedTagCommit)) {
    throw "Could not resolve tag commit '$TagCommit'."
}
$headCommit = (& git -C $repoRoot rev-parse 'HEAD^{commit}').Trim()
if ($LASTEXITCODE -ne 0 -or $headCommit -cne $resolvedTagCommit) {
    throw "Release qualification must run from tag commit '$resolvedTagCommit'; current HEAD is '$headCommit'."
}
$releaseTagName = "v$releaseVersion"
$resolvedReleaseTagCommit = (& git -C $repoRoot rev-parse "$releaseTagName^{commit}" 2>$null)
if ($LASTEXITCODE -ne 0 -or
    [string]::IsNullOrWhiteSpace($resolvedReleaseTagCommit) -or
    $resolvedReleaseTagCommit.Trim() -cne $resolvedTagCommit) {
    throw "Release tag '$releaseTagName' must point to tag commit '$resolvedTagCommit'."
}
$workingTreeChanges = @(
    & git -C $repoRoot status --porcelain=v1 --untracked-files=all
)
if ($LASTEXITCODE -ne 0) {
    throw "Could not inspect the release qualification working tree."
}
if ($workingTreeChanges.Count -gt 0) {
    throw "Release qualification requires a clean working tree: $($workingTreeChanges -join ', ')"
}

& (Join-Path $PSScriptRoot 'Test-Documentation.ps1')

foreach ($requiredPath in @(
    $canonicalManifestPath,
    $canonicalSchemaPath,
    $snapshotManifestPath,
    $snapshotSchemaPath
)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "Required compatibility release artifact is missing: $requiredPath"
    }
}

if (-not [string]::Equals(
        [IO.File]::ReadAllText($canonicalSchemaPath),
        [IO.File]::ReadAllText($snapshotSchemaPath),
        [StringComparison]::Ordinal)) {
    throw "Release snapshot schema differs from the canonical compatibility schema: $snapshotSchemaPath"
}

$snapshotJson = [IO.File]::ReadAllText($snapshotManifestPath)
if (-not ($snapshotJson | Test-Json -SchemaFile $snapshotSchemaPath -ErrorAction Stop)) {
    throw "Release snapshot does not satisfy its compatibility schema: $snapshotManifestPath"
}

$canonical = [IO.File]::ReadAllText($canonicalManifestPath) | ConvertFrom-Json -Depth 100
$snapshot = $snapshotJson | ConvertFrom-Json -Depth 100
if ([string]$snapshot.verified_against.state -cne 'released') {
    throw "Release snapshot state must be 'released', found '$($snapshot.verified_against.state)'."
}
if ([string]$snapshot.verified_against.package_version -cne $releaseVersion) {
    throw "Release snapshot version '$($snapshot.verified_against.package_version)' does not match tag version '$releaseVersion'."
}
if ([string]$canonical.verified_against.package_version -cne $releaseVersion) {
    throw "Canonical manifest version '$($canonical.verified_against.package_version)' does not match tag version '$releaseVersion'."
}

$generatedAt = [DateTimeOffset]::MinValue
if (-not [DateTimeOffset]::TryParse(
        [string]$snapshot.verified_against.generated_at,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AssumeUniversal,
        [ref]$generatedAt)) {
    throw "Release snapshot generated_at is not a valid timestamp."
}
if ($generatedAt.ToUniversalTime() -gt [DateTimeOffset]::UtcNow.AddMinutes(5)) {
    throw "Release snapshot generated_at cannot be in the future."
}

foreach ($propertyName in @('$schema', 'manifest_version', 'dialect', 'title', 'description')) {
    if ([string]$snapshot.$propertyName -cne [string]$canonical.$propertyName) {
        throw "Release snapshot property '$propertyName' differs from the canonical manifest."
    }
}
foreach ($propertyName in @('proofs', 'features')) {
    $canonicalJson = $canonical.$propertyName | ConvertTo-Json -Depth 100 -Compress
    $snapshotValueJson = $snapshot.$propertyName | ConvertTo-Json -Depth 100 -Compress
    if ($snapshotValueJson -cne $canonicalJson) {
        throw "Release snapshot '$propertyName' differs from the canonical manifest."
    }
}

$verifiedCommit = [string]$snapshot.verified_against.commit
& git -C $repoRoot cat-file -e "$verifiedCommit^{commit}"
if ($LASTEXITCODE -ne 0) {
    throw "Release snapshot verified commit '$verifiedCommit' is not available in repository history."
}

& git -C $repoRoot merge-base --is-ancestor $verifiedCommit $resolvedTagCommit
if ($LASTEXITCODE -ne 0) {
    throw "Release snapshot verified commit '$verifiedCommit' is not an ancestor of tag commit '$resolvedTagCommit'."
}

& git -C $repoRoot diff --quiet $verifiedCommit $resolvedTagCommit -- $qualificationPaths
if ($LASTEXITCODE -ne 0) {
    throw "Compatibility implementation, proofs, or canonical manifest changed after verified commit '$verifiedCommit'. Create a snapshot against the final qualified code."
}

foreach ($trackedPath in @($snapshotManifestPath, $snapshotSchemaPath)) {
    $relativePath = [IO.Path]::GetRelativePath($repoRoot, $trackedPath).Replace('\', '/')
    & git -C $repoRoot cat-file -e "${resolvedTagCommit}:$relativePath"
    if ($LASTEXITCODE -ne 0) {
        throw "Release snapshot artifact is not present in tag commit '$resolvedTagCommit': $relativePath"
    }
}

$releasePathDirectory = if ($relativeSnapshotRoot -eq '.') {
    $releaseVersion
}
else {
    "$($relativeSnapshotRoot.TrimEnd('/'))/$releaseVersion"
}
$releasePathPrefix = "$releasePathDirectory/"
$taggedSnapshotFiles = @(
    & git -C $repoRoot ls-tree -r --name-only $resolvedTagCommit -- $releasePathDirectory
)
if ($LASTEXITCODE -ne 0) {
    throw "Could not enumerate the tagged compatibility snapshot '$releaseVersion'."
}
$expectedSnapshotFiles = @(
    "$releasePathDirectory/sql-compatibility.json",
    "$releasePathDirectory/sql-compatibility.schema.json"
)
$taggedSnapshotFileList = ($taggedSnapshotFiles | Sort-Object) -join "`n"
$expectedSnapshotFileList = ($expectedSnapshotFiles | Sort-Object) -join "`n"
if ($taggedSnapshotFileList -cne $expectedSnapshotFileList) {
    throw "Release snapshot '$releaseVersion' must contain exactly the manifest and schema files."
}

$semanticReleaseTagPattern = '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$'
$previousTag = $null
$validReleaseTags = @(
    & git -C $repoRoot tag --list |
        Where-Object { $_ -cmatch $semanticReleaseTagPattern } |
        Sort-Object
)
if ($LASTEXITCODE -ne 0) {
    throw "Could not enumerate semantic release tags."
}
if ($validReleaseTags.Count -gt 0) {
    $describeArguments = @('-C', $repoRoot, 'describe', '--tags', '--abbrev=0')
    foreach ($releaseTag in $validReleaseTags) {
        $describeArguments += @('--match', $releaseTag)
    }
    $currentCommitReleaseTags = @(
        & git -C $repoRoot tag --points-at $resolvedTagCommit |
            Where-Object { $_ -cmatch $semanticReleaseTagPattern }
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect semantic release tags at '$resolvedTagCommit'."
    }
    foreach ($currentCommitReleaseTag in $currentCommitReleaseTags) {
        $describeArguments += @('--exclude', $currentCommitReleaseTag)
    }
    $describeArguments += $resolvedTagCommit
    $describedTag = (& git @describeArguments 2>$null)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($describedTag)) {
        $previousTag = $describedTag.Trim()
    }
}

if (-not [string]::IsNullOrWhiteSpace($previousTag)) {
    $currentSemanticVersion = [System.Management.Automation.SemanticVersion]::new($releaseVersion)
    $previousSemanticVersion = [System.Management.Automation.SemanticVersion]::new(
        $previousTag.Substring(1))
    if ($currentSemanticVersion.CompareTo($previousSemanticVersion) -le 0) {
        throw "Release version '$releaseVersion' must be newer than previous release '$previousTag'."
    }

    $preexistingCurrentSnapshot = @(
        & git -C $repoRoot ls-tree -r --name-only $previousTag -- $releasePathDirectory
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect compatibility snapshot '$releaseVersion' at previous release '$previousTag'."
    }
    if ($preexistingCurrentSnapshot.Count -gt 0) {
        throw "Compatibility release snapshot '$releaseVersion' already existed at '$previousTag' and cannot be changed or republished."
    }

    $currentSnapshotChanges = @(
        & git -C $repoRoot diff --name-status $previousTag $resolvedTagCommit -- $releasePathDirectory
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Could not verify that compatibility snapshot '$releaseVersion' is new."
    }
    $nonAdditiveCurrentSnapshotChanges = @(
        $currentSnapshotChanges |
            Where-Object { $_ -notmatch "^A`t" }
    )
    if ($currentSnapshotChanges.Count -eq 0 -or $nonAdditiveCurrentSnapshotChanges.Count -gt 0) {
        throw "Compatibility release snapshot '$releaseVersion' must be newly added after '$previousTag'."
    }

    $changedSnapshotPaths = @(
        & git -C $repoRoot diff --name-only $previousTag $resolvedTagCommit -- $relativeSnapshotRoot
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Could not compare compatibility snapshots with previous release tag '$previousTag'."
    }

    $mutatedPriorSnapshots = @(
        $changedSnapshotPaths |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace($_) -and
                -not $_.StartsWith($releasePathPrefix, [StringComparison]::Ordinal)
            }
    )
    if ($mutatedPriorSnapshots.Count -gt 0) {
        throw "Previously released compatibility snapshots are immutable. Unexpected changes since '$previousTag': $($mutatedPriorSnapshots -join ', ')"
    }
}

Write-Host "SQL compatibility release qualification passed for v$releaseVersion at $resolvedTagCommit."
