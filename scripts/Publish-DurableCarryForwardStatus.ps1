#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('4.4.0', 'v4.4.0')]
    [string] $Version,

    [ValidatePattern('^$|^[^/\s]+/[^/\s]+$')]
    [string] $GitHubRepository = '',

    [string] $ExpectedStatusCreator = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$statusVerifier = Join-Path $PSScriptRoot 'Test-LocalDurableStatus.ps1'
$releaseVersion = $Version.TrimStart('v')
$carryForwardContext =
    'csharpdb/local-durable-performance-carry-forward-v4.4.0'
$carryForwardDescription =
    'policy=durable-v2-carry-forward-v4.4.0; source=61e4d025; ' +
    'success=51598901859; failed-v3=51664261883; tree=bee4859c'

if (-not (Test-Path -LiteralPath $statusVerifier -PathType Leaf)) {
    throw "Required status verifier was not found: $statusVerifier"
}
if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required to publish the approved carry-forward status.'
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

function Get-ExactCurrentMainCommit {
    $workingTreeChanges = Invoke-Git `
        -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
        -FailureMessage 'Could not inspect the repository worktree.'
    if (-not [string]::IsNullOrWhiteSpace($workingTreeChanges)) {
        throw 'Carry-forward publication requires a clean repository worktree.'
    }

    $currentBranch = (Invoke-Git `
        -Arguments @('branch', '--show-current') `
        -FailureMessage 'Could not determine the current branch.').Trim()
    if ($currentBranch -cne 'main') {
        throw (
            "Carry-forward publication must run from branch 'main'; current branch is " +
            "'$currentBranch'.")
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
            "Carry-forward publication requires local main to equal origin/main exactly. " +
            "Local HEAD is $localMainCommit; origin/main is $remoteMainCommit.")
    }
    return $localMainCommit
}

function Invoke-StatusVerifier {
    param(
        [switch] $EligibilityOnly
    )

    $parameters = @{
        Commit = $headCommit
        GitHubRepository = $GitHubRepository
        ExpectedCreator = $ExpectedStatusCreator
        ReleaseVersion = $releaseVersion
    }
    if ($EligibilityOnly) {
        $parameters.ValidateApprovedCarryForwardEligibility = $true
    }
    & $statusVerifier @parameters
}

$headCommit = Get-ExactCurrentMainCommit

$buildPropsPath = Join-Path $repositoryRoot 'src/Directory.Build.props'
[xml] $buildProps = [IO.File]::ReadAllText($buildPropsPath)
$versionNodes = @($buildProps.SelectNodes('/Project/PropertyGroup/Version'))
if ($versionNodes.Count -ne 1 -or
    $versionNodes[0].InnerText.Trim() -cne $releaseVersion) {
    throw (
        "The approved carry-forward requires package version $releaseVersion in " +
        "$buildPropsPath.")
}

if ([string]::IsNullOrWhiteSpace($GitHubRepository)) {
    $repositoryOutput = @(
        & gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        $details = ($repositoryOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        throw "Could not resolve the GitHub repository. $details"
    }
    $GitHubRepository = (($repositoryOutput | ForEach-Object { [string] $_ }) -join '').Trim()
}
if ($GitHubRepository -cnotmatch '^[^/\s]+/[^/\s]+$') {
    throw "GitHubRepository must use the owner/name form: $GitHubRepository"
}

if ([string]::IsNullOrWhiteSpace($ExpectedStatusCreator)) {
    $variableOutput = @(
        & gh variable get LOCAL_DURABLE_ATTESTOR --repo $GitHubRepository 2>&1
    )
    $variableExitCode = $LASTEXITCODE
    if ($variableExitCode -eq 0) {
        $ExpectedStatusCreator =
            (($variableOutput | ForEach-Object { [string] $_ }) -join '').Trim()
    }
    else {
        $variableFailure =
            ($variableOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        if ($variableFailure -notmatch '(?i)(HTTP\s+404|not\s+found)') {
            throw (
                'Could not resolve the LOCAL_DURABLE_ATTESTOR repository variable. ' +
                $variableFailure)
        }
    }
    if ([string]::IsNullOrWhiteSpace($ExpectedStatusCreator)) {
        $ExpectedStatusCreator = $GitHubRepository.Split('/', 2)[0]
    }
}
else {
    $ExpectedStatusCreator = $ExpectedStatusCreator.Trim()
}

$authenticatedOutput = @(& gh api user --jq '.login' 2>&1)
if ($LASTEXITCODE -ne 0) {
    $details = ($authenticatedOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
    throw "Could not identify the authenticated GitHub user. $details"
}
$authenticatedCreator =
    (($authenticatedOutput | ForEach-Object { [string] $_ }) -join '').Trim()
if (-not $authenticatedCreator.Equals(
    $ExpectedStatusCreator,
    [StringComparison]::OrdinalIgnoreCase)) {
    throw (
        "Authenticated GitHub user '$authenticatedCreator' is not the configured durable " +
        "attestor '$ExpectedStatusCreator'.")
}

try {
    Invoke-StatusVerifier
    Write-Host (
        "Exact commit $headCommit already has an accepted release status; no " +
        'carry-forward status was added.')
    return
}
catch {
    Write-Host (
        "Exact commit $headCommit does not yet have an accepted release status: " +
        "$($_.Exception.Message)")
}

Invoke-StatusVerifier -EligibilityOnly

$currentMainCommit = Get-ExactCurrentMainCommit
if ($currentMainCommit -cne $headCommit) {
    throw (
        "Main changed while carry-forward eligibility was verified. Expected $headCommit; " +
        "found $currentMainCommit.")
}

$apiPath = "repos/$GitHubRepository/statuses/$headCommit"
$publishOutput = @(
    & gh api `
        --method POST `
        $apiPath `
        -f "state=success" `
        -f "context=$carryForwardContext" `
        -f "description=$carryForwardDescription" 2>&1
)
if ($LASTEXITCODE -ne 0) {
    $details = ($publishOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
    throw "Could not publish the approved carry-forward status for $headCommit. $details"
}

$currentMainCommit = Get-ExactCurrentMainCommit
if ($currentMainCommit -cne $headCommit) {
    throw (
        "Main changed after carry-forward publication. Attested $headCommit; current main " +
        "is $currentMainCommit. Do not tag either commit from this invocation.")
}

Invoke-StatusVerifier
Write-Host (
    "Published and reverified the explicit one-time $releaseVersion carry-forward status " +
    "for exact commit $headCommit without changing the durable-v3 failure context.")
