[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$')]
    [string]$Version,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$VerifiedCommit,

    [DateTimeOffset]$GeneratedAt = [DateTimeOffset]::UtcNow,

    [string]$SnapshotRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$docsRoot = Join-Path $repoRoot 'www/docs'
$manifestPath = Join-Path $docsRoot 'sql-compatibility.json'
$schemaPath = Join-Path $docsRoot 'sql-compatibility.schema.json'
if ([string]::IsNullOrWhiteSpace($SnapshotRoot)) {
    $SnapshotRoot = Join-Path $docsRoot 'sql-compatibility/releases'
}
$SnapshotRoot = [IO.Path]::GetFullPath($SnapshotRoot)
$relativeSnapshotRoot = [IO.Path]::GetRelativePath($repoRoot, $SnapshotRoot).Replace('\', '/')
if ([IO.Path]::IsPathRooted($relativeSnapshotRoot) -or
    $relativeSnapshotRoot -eq '..' -or
    $relativeSnapshotRoot.StartsWith('../', [StringComparison]::Ordinal)) {
    throw "Compatibility release snapshots must be created inside the repository: $SnapshotRoot"
}

foreach ($requiredPath in @($manifestPath, $schemaPath)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "Required compatibility artifact is missing: $requiredPath"
    }
}

& (Join-Path $PSScriptRoot 'Test-Documentation.ps1')

& git -C $repoRoot cat-file -e "$VerifiedCommit^{commit}"
if ($LASTEXITCODE -ne 0) {
    throw "Verified commit '$VerifiedCommit' is not available in repository history."
}
$resolvedVerifiedCommit = (& git -C $repoRoot rev-parse "$VerifiedCommit^{commit}").Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolvedVerifiedCommit)) {
    throw "Could not resolve verified commit '$VerifiedCommit'."
}
& git -C $repoRoot merge-base --is-ancestor $resolvedVerifiedCommit HEAD
if ($LASTEXITCODE -ne 0) {
    throw "Verified commit '$resolvedVerifiedCommit' is not an ancestor of the current release-preparation commit."
}

$manifest = [IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json -Depth 100
if ([string]$manifest.verified_against.package_version -cne $Version) {
    throw "Compatibility manifest package version '$($manifest.verified_against.package_version)' does not match requested release '$Version'."
}

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
& git -C $repoRoot diff --quiet $resolvedVerifiedCommit -- $qualificationPaths
if ($LASTEXITCODE -ne 0) {
    throw "Compatibility implementation, proofs, or canonical manifest differ from verified commit '$resolvedVerifiedCommit'. Commit the qualified state before creating its release snapshot."
}
$uncommittedQualificationPaths = @(
    & git -C $repoRoot status --porcelain=v1 --untracked-files=all -- $qualificationPaths
)
if ($LASTEXITCODE -ne 0) {
    throw "Could not inspect the compatibility qualification paths for uncommitted changes."
}
if ($uncommittedQualificationPaths.Count -gt 0) {
    throw "Compatibility qualification paths contain uncommitted or untracked changes: $($uncommittedQualificationPaths -join ', ')"
}

$snapshotDirectory = Join-Path $SnapshotRoot $Version
if (Test-Path -LiteralPath $snapshotDirectory) {
    throw "Compatibility release snapshot '$Version' already exists. Released snapshots are immutable."
}

$manifest.verified_against.commit = $resolvedVerifiedCommit
$manifest.verified_against.package_version = $Version
$manifest.verified_against.generated_at = $GeneratedAt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$manifest.verified_against.state = 'released'
$json = $manifest | ConvertTo-Json -Depth 100
if (-not ($json | Test-Json -SchemaFile $schemaPath -ErrorAction Stop)) {
    throw "Generated release snapshot does not satisfy the compatibility schema."
}

$snapshotManifestPath = Join-Path $snapshotDirectory 'sql-compatibility.json'
$snapshotSchemaPath = Join-Path $snapshotDirectory 'sql-compatibility.schema.json'
if (-not $PSCmdlet.ShouldProcess($snapshotDirectory, "Create immutable SQL compatibility release snapshot $Version")) {
    return
}

New-Item -ItemType Directory -Path $snapshotDirectory -ErrorAction Stop | Out-Null
try {
    [IO.File]::WriteAllText(
        $snapshotManifestPath,
        $json + [Environment]::NewLine,
        [Text.UTF8Encoding]::new($false))
    [IO.File]::Copy($schemaPath, $snapshotSchemaPath, $false)
}
catch {
    Remove-Item -LiteralPath $snapshotDirectory -Recurse -Force -ErrorAction SilentlyContinue
    throw
}

Write-Host "Created immutable SQL compatibility snapshot for $Version at $snapshotDirectory"
