[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$')]
    [string]$Version,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$TagCommit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$releaseVersion = $Version.TrimStart('v')
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$docsRoot = Join-Path $repoRoot 'www/docs'
$manifestPath = Join-Path $docsRoot 'sql-compatibility.json'
$schemaPath = Join-Path $docsRoot 'sql-compatibility.schema.json'

foreach ($requiredPath in @($manifestPath, $schemaPath)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "Required SQL compatibility artifact is missing: $requiredPath"
    }
}

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
    throw "Release validation must run from tag commit '$resolvedTagCommit'; current HEAD is '$headCommit'."
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
    throw 'Could not inspect the release validation working tree.'
}
if ($workingTreeChanges.Count -gt 0) {
    throw "Release validation requires a clean working tree: $($workingTreeChanges -join ', ')"
}

& (Join-Path $PSScriptRoot 'Test-Documentation.ps1')

$manifestJson = [IO.File]::ReadAllText($manifestPath)
if (-not ($manifestJson | Test-Json -SchemaFile $schemaPath -ErrorAction Stop)) {
    throw "SQL compatibility manifest does not satisfy its schema: $manifestPath"
}
$manifest = $manifestJson | ConvertFrom-Json -Depth 100

if ([string]$manifest.verified_against.package_version -cne $releaseVersion) {
    throw "Canonical SQL compatibility version '$($manifest.verified_against.package_version)' does not match tag version '$releaseVersion'."
}

$generatedAt = [DateTimeOffset]::MinValue
if (-not [DateTimeOffset]::TryParse(
        [string]$manifest.verified_against.generated_at,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AssumeUniversal,
        [ref]$generatedAt)) {
    throw 'SQL compatibility generated_at is not a valid timestamp.'
}
if ($generatedAt.ToUniversalTime() -gt [DateTimeOffset]::UtcNow.AddMinutes(5)) {
    throw 'SQL compatibility generated_at cannot be in the future.'
}

$verifiedCommit = [string]$manifest.verified_against.commit
& git -C $repoRoot cat-file -e "$verifiedCommit^{commit}"
if ($LASTEXITCODE -ne 0) {
    throw "SQL compatibility verified commit '$verifiedCommit' is not available in repository history."
}
& git -C $repoRoot merge-base --is-ancestor $verifiedCommit $resolvedTagCommit
if ($LASTEXITCODE -ne 0) {
    throw "SQL compatibility verified commit '$verifiedCommit' is not an ancestor of tag commit '$resolvedTagCommit'."
}

$semanticReleaseTagPattern = '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$'
$validReleaseTags = @(
    & git -C $repoRoot tag --list |
        Where-Object { $_ -cmatch $semanticReleaseTagPattern } |
        Sort-Object
)
if ($LASTEXITCODE -ne 0) {
    throw 'Could not enumerate semantic release tags.'
}

$previousTag = $null
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
}

Write-Host "SQL compatibility release validation passed for v$releaseVersion at $resolvedTagCommit."
