#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$')]
    [string] $Version
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$tagValidator = Join-Path $PSScriptRoot 'Test-ReleaseTag.ps1'
$releaseVersion = $Version.TrimStart('v')
$releaseTag = "v$releaseVersion"

if (-not (Test-Path -LiteralPath $tagValidator -PathType Leaf)) {
    throw "Required release script was not found: $tagValidator"
}

function Invoke-Git {
    param(
        [Parameter(Mandatory)]
        [string[]] $Arguments,

        [Parameter(Mandatory)]
        [string] $FailureMessage
    )

    $output = @(& git -C $repositoryRoot @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        $details = ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        if ([string]::IsNullOrWhiteSpace($details)) {
            throw $FailureMessage
        }
        throw "$FailureMessage$([Environment]::NewLine)$details"
    }

    return ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
}

function Resolve-GitCommitOrNull {
    param(
        [Parameter(Mandatory)]
        [string] $Ref
    )

    $output = @(& git -C $repositoryRoot rev-parse --verify "$Ref`^{commit}" 2>$null)
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        return $null
    }

    $resolved = (($output | ForEach-Object { [string] $_ }) -join '').Trim()
    if ($resolved -cnotmatch '^[0-9a-f]{40}$') {
        throw "Git ref '$Ref' did not resolve to a canonical commit SHA."
    }
    return $resolved
}

function Resolve-RemoteTagCommitOrNull {
    param(
        [Parameter(Mandatory)]
        [string] $TagName
    )

    $output = Invoke-Git `
        -Arguments @(
            'ls-remote',
            '--tags',
            'origin',
            "refs/tags/$TagName",
            "refs/tags/$TagName`^{}") `
        -FailureMessage "Could not inspect remote release tag '$TagName'."
    if ([string]::IsNullOrWhiteSpace($output)) {
        return $null
    }

    $records = @(
        $output -split "`r?`n" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object {
                if ($_ -cnotmatch '^(?<sha>[0-9a-f]{40})\s+(?<ref>refs/tags/.+)$') {
                    throw "Remote tag query returned an invalid record: $_"
                }
                [pscustomobject]@{
                    Sha = $Matches.sha
                    Ref = $Matches.ref
                }
            }
    )
    $peeledRef = "refs/tags/$TagName`^{}"
    $peeled = @($records | Where-Object { $_.Ref -ceq $peeledRef })
    if ($peeled.Count -gt 1) {
        throw "Remote release tag '$TagName' returned multiple peeled commits."
    }
    if ($peeled.Count -eq 1) {
        return [string] $peeled[0].Sha
    }

    $directRef = "refs/tags/$TagName"
    $direct = @($records | Where-Object { $_.Ref -ceq $directRef })
    if ($direct.Count -ne 1) {
        throw "Remote release tag '$TagName' did not resolve unambiguously."
    }
    return [string] $direct[0].Sha
}

function Get-ExactCurrentMainCommit {
    $workingTreeChanges = Invoke-Git `
        -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
        -FailureMessage 'Could not inspect the repository worktree.'
    if (-not [string]::IsNullOrWhiteSpace($workingTreeChanges)) {
        throw 'Release tagging requires a clean repository worktree.'
    }

    $currentBranch = (Invoke-Git `
        -Arguments @('branch', '--show-current') `
        -FailureMessage 'Could not determine the current branch.').Trim()
    if ($currentBranch -cne 'main') {
        throw "Release tagging must run from branch 'main'; current branch is '$currentBranch'."
    }

    Invoke-Git `
        -Arguments @(
            'fetch',
            '--quiet',
            'origin',
            '+refs/heads/main:refs/remotes/origin/main') `
        -FailureMessage "Could not refresh 'origin/main'." |
        Out-Null

    $localMainCommit = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', 'HEAD^{commit}') `
        -FailureMessage 'Could not resolve the current HEAD commit.').Trim()
    $remoteMainCommit = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', 'origin/main^{commit}') `
        -FailureMessage "Could not resolve 'origin/main'.").Trim()
    if ($localMainCommit -cne $remoteMainCommit) {
        throw (
            "Release tagging requires local main to equal origin/main exactly. " +
            "Local HEAD is $localMainCommit; origin/main is $remoteMainCommit.")
    }

    return $localMainCommit
}

$headCommit = Get-ExactCurrentMainCommit

$buildPropsPath = Join-Path $repositoryRoot 'src/Directory.Build.props'
if (-not (Test-Path -LiteralPath $buildPropsPath -PathType Leaf)) {
    throw "Shared package properties were not found at '$buildPropsPath'."
}
[xml] $buildProps = [IO.File]::ReadAllText($buildPropsPath)
$versionNodes = @($buildProps.SelectNodes('/Project/PropertyGroup/Version'))
if ($versionNodes.Count -ne 1) {
    throw "Expected exactly one Version property in '$buildPropsPath'; found $($versionNodes.Count)."
}
$packageVersion = $versionNodes[0].InnerText.Trim()
if ($packageVersion -cne $releaseVersion) {
    throw (
        "Release tag version '$releaseVersion' does not match package version " +
        "'$packageVersion' in '$buildPropsPath'.")
}

$currentMainCommit = Get-ExactCurrentMainCommit
if ($currentMainCommit -cne $headCommit) {
    throw (
        "The exact main commit changed during release validation. Validated $headCommit, " +
        "but current main is $currentMainCommit. Restart release tagging for the current commit.")
}

$localTagCommit = Resolve-GitCommitOrNull -Ref "refs/tags/$releaseTag"
if ($null -ne $localTagCommit -and $localTagCommit -cne $headCommit) {
    throw (
        "Local release tag '$releaseTag' points to $localTagCommit, not exact release " +
        "commit $headCommit.")
}

$remoteTagCommit = Resolve-RemoteTagCommitOrNull -TagName $releaseTag
if ($null -ne $remoteTagCommit -and $remoteTagCommit -cne $headCommit) {
    throw (
        "Remote release tag '$releaseTag' points to $remoteTagCommit, not exact release " +
        "commit $headCommit.")
}

if ($null -eq $localTagCommit) {
    if ($null -ne $remoteTagCommit) {
        Invoke-Git `
            -Arguments @(
                'fetch',
                '--quiet',
                'origin',
                "refs/tags/$releaseTag`:refs/tags/$releaseTag") `
            -FailureMessage "Could not fetch existing remote release tag '$releaseTag'." |
            Out-Null
    }
    else {
        Invoke-Git `
            -Arguments @('tag', $releaseTag, $headCommit) `
            -FailureMessage "Could not create local release tag '$releaseTag'." |
            Out-Null
    }

    $localTagCommit = Resolve-GitCommitOrNull -Ref "refs/tags/$releaseTag"
    if ($localTagCommit -cne $headCommit) {
        throw "Local release tag '$releaseTag' was not created at exact commit $headCommit."
    }
}
else {
    Write-Host "Reusing local release tag '$releaseTag' at exact commit $headCommit."
}

& $tagValidator -Version $releaseTag -TagCommit $headCommit

if ($null -eq $remoteTagCommit) {
    Invoke-Git `
        -Arguments @(
            'push',
            'origin',
            "refs/tags/$releaseTag`:refs/tags/$releaseTag") `
        -FailureMessage "Could not push release tag '$releaseTag'." |
        Out-Null

    $remoteTagCommit = Resolve-RemoteTagCommitOrNull -TagName $releaseTag
    if ($remoteTagCommit -cne $headCommit) {
        throw "Remote release tag '$releaseTag' was not published at exact commit $headCommit."
    }
    Write-Host "Published release tag '$releaseTag' at exact commit $headCommit."
}
else {
    Write-Host "Remote release tag '$releaseTag' already points to exact commit $headCommit."
}
