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
$buildPropsPath = Join-Path $repoRoot 'src/Directory.Build.props'

if (-not (Test-Path -LiteralPath $buildPropsPath -PathType Leaf)) {
    throw "Shared package properties were not found at '$buildPropsPath'."
}

[xml]$buildProps = [IO.File]::ReadAllText($buildPropsPath)
$versionNodes = @($buildProps.SelectNodes('/Project/PropertyGroup/Version'))
if ($versionNodes.Count -ne 1) {
    throw "Expected exactly one Version property in '$buildPropsPath'; found $($versionNodes.Count)."
}

$packageVersion = $versionNodes[0].InnerText.Trim()
if ($packageVersion -cne $releaseVersion) {
    throw "Release tag version '$releaseVersion' does not match package version '$packageVersion' in '$buildPropsPath'."
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

Write-Host "Release tag validation passed for v$releaseVersion at $resolvedTagCommit."
