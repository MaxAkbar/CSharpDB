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
$releaseVersion = $Version.TrimStart('v')
$releaseTag = "v$releaseVersion"
$qualificationWorkflow = 'release.yml'
$publicationWorkflow = 'publish-release.yml'
$tagValidator = Join-Path $PSScriptRoot 'Test-ReleaseTag.ps1'

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
        if ([string]::IsNullOrWhiteSpace($details)) { throw $FailureMessage }
        throw "$FailureMessage$([Environment]::NewLine)$details"
    }
    return ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
}

function Invoke-Gh {
    param(
        [Parameter(Mandatory)]
        [string[]] $Arguments,

        [Parameter(Mandatory)]
        [string] $FailureMessage
    )

    Push-Location $repositoryRoot
    try {
        $output = @(& gh @Arguments 2>&1)
        if ($LASTEXITCODE -ne 0) {
            $details = ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
            if ([string]::IsNullOrWhiteSpace($details)) { throw $FailureMessage }
            throw "$FailureMessage$([Environment]::NewLine)$details"
        }
        return ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
    }
    finally {
        Pop-Location
    }
}

function Resolve-GitCommitOrNull {
    param([Parameter(Mandatory)][string] $Ref)

    $output = @(& git -C $repositoryRoot rev-parse --verify "$Ref`^{commit}" 2>$null)
    if ($LASTEXITCODE -ne 0) { return $null }
    $resolved = (($output | ForEach-Object { [string] $_ }) -join '').Trim()
    if ($resolved -cnotmatch '^[0-9a-f]{40}$') {
        throw "Git ref '$Ref' did not resolve to a canonical commit SHA."
    }
    return $resolved
}

function Resolve-RemoteTagCommitOrNull {
    param([Parameter(Mandatory)][string] $TagName)

    $output = Invoke-Git `
        -Arguments @(
            'ls-remote',
            '--tags',
            'origin',
            "refs/tags/$TagName",
            "refs/tags/$TagName`^{}") `
        -FailureMessage "Could not inspect remote release tag '$TagName'."
    if ([string]::IsNullOrWhiteSpace($output)) { return $null }

    $records = @(
        $output -split "`r?`n" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object {
                if ($_ -cnotmatch '^(?<sha>[0-9a-f]{40})\s+(?<ref>refs/tags/.+)$') {
                    throw "Remote tag query returned an invalid record: $_"
                }
                [pscustomobject]@{ Sha = $Matches.sha; Ref = $Matches.ref }
            }
    )
    $peeled = @($records | Where-Object { $_.Ref -ceq "refs/tags/$TagName`^{}" })
    if ($peeled.Count -eq 1) { return [string] $peeled[0].Sha }
    $direct = @($records | Where-Object { $_.Ref -ceq "refs/tags/$TagName" })
    if ($peeled.Count -gt 1 -or $direct.Count -ne 1) {
        throw "Remote release tag '$TagName' did not resolve unambiguously."
    }
    return [string] $direct[0].Sha
}

function Get-ExactCurrentMainCommit {
    $workingTreeChanges = Invoke-Git `
        -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
        -FailureMessage 'Could not inspect the repository worktree.'
    if (-not [string]::IsNullOrWhiteSpace($workingTreeChanges)) {
        throw 'Release publication requires a clean repository worktree.'
    }

    $currentBranch = (Invoke-Git `
        -Arguments @('branch', '--show-current') `
        -FailureMessage 'Could not determine the current branch.').Trim()
    if ($currentBranch -cne 'main') {
        throw "Release publication must run from branch 'main'; current branch is '$currentBranch'."
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
            'Release publication requires local main to equal origin/main exactly. ' +
            "Local HEAD is $localMainCommit; origin/main is $remoteMainCommit.")
    }
    return $localMainCommit
}

function Get-WorkflowRuns {
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][string] $Workflow
    )

    $json = Invoke-Gh `
        -Arguments @(
            'run', 'list',
            '--repo', $Repository,
            '--workflow', $Workflow,
            '--event', 'workflow_dispatch',
            '--commit', $Commit,
            '--limit', '30',
            '--json', 'databaseId,displayTitle,event,headSha,status,conclusion,url') `
        -FailureMessage 'Could not enumerate hosted release runs.'
    return @($json | ConvertFrom-Json)
}

Invoke-Gh -Arguments @('auth', 'status') -FailureMessage 'GitHub CLI authentication is required.' |
    Out-Null
$repository = (Invoke-Gh `
    -Arguments @('repo', 'view', '--json', 'nameWithOwner', '--jq', '.nameWithOwner') `
    -FailureMessage 'Could not resolve the GitHub repository identity.').Trim()
if ([string]::IsNullOrWhiteSpace($repository)) {
    throw 'GitHub repository identity was empty.'
}


function Get-GitHubReleaseOrNull {
    $tags = @(
        (Invoke-Gh `
            -Arguments @(
                'api', '--paginate', "repos/$repository/releases", '--jq', '.[].tag_name') `
            -FailureMessage 'Could not inspect existing GitHub Releases.') `
            -split "`r?`n" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($tags -cnotcontains $releaseTag) { return $null }
    $json = Invoke-Gh `
        -Arguments @(
            'release', 'view', $releaseTag, '--repo', $repository,
            '--json', 'isDraft,tagName,url') `
        -FailureMessage "Could not inspect GitHub Release '$releaseTag'."
    return ($json | ConvertFrom-Json)
}

function Wait-HostedRun {
    param([Parameter(Mandatory)] $Run, [Parameter(Mandatory)][string] $Kind)

    Push-Location $repositoryRoot
    try {
        & gh run watch ([string] $Run.databaseId) `
            --repo $repository `
            --compact `
            --exit-status
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    if ($exitCode -ne 0) {
        throw "$Kind failed: $($Run.url)"
    }
}

function Find-OrDispatchRun {
    param(
        [Parameter(Mandatory)][string] $Workflow,
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][string] $Title,
        [Parameter(Mandatory)][string] $Ref,
        [Parameter(Mandatory)][string[]] $Fields,
        [Parameter(Mandatory)][string] $Kind
    )

    $existing = @(Get-WorkflowRuns -Repository $repository -Commit $Commit -Workflow $Workflow)
    $active = @($existing | Where-Object {
        $_.displayTitle -ceq $Title -and $_.status -cne 'completed'
    })
    if ($active.Count -gt 1) { throw "Multiple active $Kind runs exist for '$Title'." }
    if ($active.Count -eq 1) {
        Write-Host "Reusing active $Kind run $($active[0].databaseId): $($active[0].url)"
        return $active[0]
    }
    if ($Kind -ceq 'release qualification') {
        $successful = @($existing | Where-Object {
            $_.displayTitle -ceq $Title -and
            $_.status -ceq 'completed' -and
            $_.conclusion -ceq 'success'
        } | Sort-Object databaseId -Descending)
        foreach ($candidate in $successful) {
            $artifactCount = [int] (Invoke-Gh `
                -Arguments @(
                    'api',
                    "repos/$repository/actions/runs/$($candidate.databaseId)/artifacts",
                    '--jq', '[.artifacts[] | select(.name == "release-bundle" and .expired == false)] | length') `
                -FailureMessage "Could not inspect artifacts for qualification run $($candidate.databaseId).").Trim()
            if ($artifactCount -eq 1) {
                Write-Host "Reusing successful release qualification $($candidate.databaseId): $($candidate.url)"
                return $candidate
            }
        }
    }

    $knownIds = [Collections.Generic.HashSet[long]]::new()
    foreach ($known in $existing) { $knownIds.Add([long] $known.databaseId) | Out-Null }
    $arguments = @('workflow', 'run', $Workflow, '--repo', $repository, '--ref', $Ref)
    foreach ($field in $Fields) { $arguments += @('--raw-field', $field) }
    Invoke-Gh -Arguments $arguments -FailureMessage "Could not dispatch the $Kind workflow." |
        Out-Null

    $deadline = [Diagnostics.Stopwatch]::StartNew()
    while ($deadline.Elapsed -lt [TimeSpan]::FromMinutes(2)) {
        $new = @(Get-WorkflowRuns -Repository $repository -Commit $Commit -Workflow $Workflow |
            Where-Object {
                $_.displayTitle -ceq $Title -and
                -not $knownIds.Contains([long] $_.databaseId)
            })
        if ($new.Count -gt 1) { throw "$Kind dispatch created multiple matching runs." }
        if ($new.Count -eq 1) {
            Write-Host "Dispatched $Kind run $($new[0].databaseId): $($new[0].url)"
            return $new[0]
        }
        Start-Sleep -Seconds 3
    }
    throw "The $Kind run did not appear within two minutes."
}

function Start-PublicationRun {
    param(
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][long] $QualificationRunId,
        [Parameter(Mandatory)][bool] $PreflightOnly
    )

    $mode = if ($PreflightOnly) { 'preflight' } else { 'publication' }
    $modeLabel = if ($PreflightOnly) { 'preflight' } else { 'release' }
    $title = "Publish $releaseTag from qualification $QualificationRunId ($modeLabel)"
    $run = Find-OrDispatchRun `
        -Workflow $publicationWorkflow `
        -Commit $Commit `
        -Title $title `
        -Ref $(if ($PreflightOnly) { 'main' } else { $releaseTag }) `
        -Fields @(
            "release_tag=$releaseTag",
            "release_commit=$Commit",
            "qualification_run_id=$QualificationRunId",
            "preflight_only=$($PreflightOnly.ToString().ToLowerInvariant())") `
        -Kind "release $mode"
    Wait-HostedRun -Run $run -Kind "Release $mode"
    return $run
}

$remoteTagCommit = Resolve-RemoteTagCommitOrNull -TagName $releaseTag
$qualificationRun = $null
if ($null -eq $remoteTagCommit) {
    $headCommit = Get-ExactCurrentMainCommit
    $buildPropsPath = Join-Path $repositoryRoot 'src/Directory.Build.props'
    [xml] $buildProps = [IO.File]::ReadAllText($buildPropsPath)
    $versionNodes = @($buildProps.SelectNodes('/Project/PropertyGroup/Version'))
    if ($versionNodes.Count -ne 1) {
        throw "Expected exactly one Version property; found $($versionNodes.Count)."
    }
    if ($versionNodes[0].InnerText.Trim() -cne $releaseVersion) {
        throw "Requested release '$releaseVersion' does not match the package version."
    }

    $localTagCommit = Resolve-GitCommitOrNull -Ref "refs/tags/$releaseTag"
    if ($null -ne $localTagCommit -and $localTagCommit -cne $headCommit) {
        throw "Local release tag '$releaseTag' does not target exact main $headCommit."
    }

    $qualificationTitle = "Qualify release $releaseTag at $headCommit"
    $qualificationRun = Find-OrDispatchRun `
        -Workflow $qualificationWorkflow `
        -Commit $headCommit `
        -Title $qualificationTitle `
        -Ref 'main' `
        -Fields @("release_tag=$releaseTag", "release_commit=$headCommit") `
        -Kind 'release qualification'
    Write-Host 'Every reversible gate runs before tag creation. A qualification failure leaves the version retryable.'
    Wait-HostedRun -Run $qualificationRun -Kind 'Release qualification'

    Start-PublicationRun `
        -Commit $headCommit `
        -QualificationRunId ([long] $qualificationRun.databaseId) `
        -PreflightOnly $true |
        Out-Null

    if ((Get-ExactCurrentMainCommit) -cne $headCommit) {
        throw 'Main changed while qualification ran; rerun the command for current main.'
    }
    & $tagValidator -Version $releaseTag -TagCommit $headCommit -AllowMissingTag
    if ($null -eq $localTagCommit) {
        Invoke-Git -Arguments @('tag', $releaseTag, $headCommit) `
            -FailureMessage "Could not create local release tag '$releaseTag'." |
            Out-Null
    }
    Invoke-Git `
        -Arguments @('push', 'origin', "refs/tags/$releaseTag`:refs/tags/$releaseTag") `
        -FailureMessage "Could not push qualified release tag '$releaseTag'." |
        Out-Null
    $remoteTagCommit = Resolve-RemoteTagCommitOrNull -TagName $releaseTag
    if ($remoteTagCommit -cne $headCommit) {
        throw "Remote release tag '$releaseTag' was not created at exact commit $headCommit."
    }
}
else {
    $headCommit = $remoteTagCommit
    $publishRuns = @(
        Get-WorkflowRuns -Repository $repository -Commit $headCommit -Workflow $publicationWorkflow |
            Where-Object {
                $_.displayTitle -match "^Publish $([regex]::Escape($releaseTag)) from qualification (?<id>[1-9][0-9]*) \(preflight\)$" -and
                $_.status -ceq 'completed' -and
                $_.conclusion -ceq 'success'
            }
    )
    $qualificationIds = @($publishRuns | ForEach-Object {
        if ($_.displayTitle -match 'qualification (?<id>[1-9][0-9]*) \(preflight\)$') { [long] $Matches.id }
    } | Sort-Object -Unique)
    if ($qualificationIds.Count -ne 1) {
        throw "Existing tag '$releaseTag' is not bound to exactly one qualification run."
    }
    $qualificationId = $qualificationIds[0]
    $qualificationRun = @(
        Get-WorkflowRuns -Repository $repository -Commit $headCommit -Workflow $qualificationWorkflow |
            Where-Object {
                [long] $_.databaseId -eq $qualificationId -and
                $_.displayTitle -ceq "Qualify release $releaseTag at $headCommit" -and
                $_.status -ceq 'completed' -and
                $_.conclusion -ceq 'success'
            }
    ) | Select-Object -First 1
    if ($null -eq $qualificationRun) {
        throw "Existing tag '$releaseTag' is not bound to a successful exact qualification run."
    }
    Write-Host "Resuming publication for qualified tag $releaseTag."
}

Start-PublicationRun `
    -Commit $headCommit `
    -QualificationRunId ([long] $qualificationRun.databaseId) `
    -PreflightOnly $false |
    Out-Null

$remoteTagCommit = Resolve-RemoteTagCommitOrNull -TagName $releaseTag
if ($remoteTagCommit -cne $headCommit) {
    throw "Release tag '$releaseTag' no longer targets exact commit $headCommit."
}
$release = Get-GitHubReleaseOrNull
if ($null -eq $release -or $release.isDraft) {
    throw "Successful publication did not create a published GitHub Release '$releaseTag'."
}
Write-Host "Published $releaseTag at exact commit $headCommit."
Write-Host "GitHub Release: $($release.url)"
