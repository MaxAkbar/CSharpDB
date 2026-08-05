#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string] $Commit,

    [Parameter(Mandatory)]
    [ValidatePattern('^[^/\s]+/[^/\s]+$')]
    [string] $GitHubRepository,

    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string] $ExpectedCreator,

    [ValidatePattern('^$|^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')]
    [string] $ReleaseVersion = '',

    [switch] $ValidateApprovedCarryForwardEligibility
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$canonicalContext = 'csharpdb/local-durable-performance'
$carryForwardContext =
    'csharpdb/local-durable-performance-carry-forward-v4.4.0'
$durableV3AttestationPattern =
    '^policy=durable-v3; baseline=[0-9a-f]{40}; design=[0-9A-F]{8}; reports=[0-9A-F]{8}/[0-9A-F]{8}$'

# This is a one-release exception. These values deliberately pin both the last
# successful durable-v2 evidence and the later durable-v3 failure it does not replace.
$approvedReleaseVersion = '4.4.0'
$approvedCreatorLogin = 'MaxAkbar'
$approvedCreatorId = [Int64] 13856299
$sourceCommit = '61e4d025087f4fae7208381288fba6115f0d1e30'
$sourceStatusId = [Int64] 51598901859
$sourceStatusTimestamp = [DateTimeOffset]::Parse('2026-08-04T07:38:51Z')
$sourceStatusDescription =
    'policy=durable-v2; baseline=7880dad112f3fdf011c134db2f8a08ec646ee326; ' +
    'reports=BFF306E7/B9C20AD6'
$failedV3Commit = 'ee1ea0e996fc22e093e950ec32e14543cd5caeca'
$failedV3StatusId = [Int64] 51664261883
$failedV3StatusTimestamp = [DateTimeOffset]::Parse('2026-08-05T02:52:58Z')
$failedV3StatusDescription =
    'policy=durable-v3; design=6B500421; local durable qualification failed'
$allowedProductPath =
    'src/CSharpDB.Migration/MigrationRejectArtifactPublication.cs'
$approvedProductBlob = '8e43642cfcd3e523046302b99253673ceb5a33ce'
$approvedProductTree = 'bee4859c14381fc2dbe209e2e0c84909dc98adc9'
$carryForwardDescription =
    'policy=durable-v2-carry-forward-v4.4.0; source=61e4d025; ' +
    'success=51598901859; failed-v3=51664261883; tree=bee4859c'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$commitSha = $Commit.ToLowerInvariant()
$expectedCreatorLogin = $ExpectedCreator.Trim()
$normalizedReleaseVersion = $ReleaseVersion.TrimStart('v')

if ([string]::IsNullOrWhiteSpace($expectedCreatorLogin)) {
    throw 'ExpectedCreator cannot be empty or whitespace.'
}
if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required to verify the local durable performance status.'
}

function Get-CommitStatuses {
    param(
        [Parameter(Mandatory)]
        [string] $CommitSha
    )

    $apiPath = "repos/$GitHubRepository/commits/$CommitSha/statuses?per_page=100"
    $apiOutput = @(& gh api $apiPath 2>&1)
    if ($LASTEXITCODE -ne 0) {
        $details = ($apiOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        throw "Could not read GitHub statuses for commit $CommitSha. $details"
    }

    try {
        return @(($apiOutput -join [Environment]::NewLine) | ConvertFrom-Json -Depth 20)
    }
    catch {
        throw "GitHub returned invalid status JSON for commit $CommitSha. $($_.Exception.Message)"
    }
}

function Get-LatestStatus {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [object[]] $Statuses,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $matching = @(
        $Statuses |
            Where-Object { [string] $_.context -ceq $Context } |
            Sort-Object -Property @(
                @{ Expression = { [DateTimeOffset] $_.created_at }; Descending = $true },
                @{ Expression = { [Int64] $_.id }; Descending = $true }
            )
    )
    if ($matching.Count -eq 0) {
        return $null
    }
    return $matching[0]
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

function Assert-PinnedStatus {
    param(
        [Parameter(Mandatory)]
        [object] $Status,

        [Parameter(Mandatory)]
        [Int64] $Id,

        [Parameter(Mandatory)]
        [string] $State,

        [Parameter(Mandatory)]
        [DateTimeOffset] $Timestamp,

        [Parameter(Mandatory)]
        [string] $Description,

        [Parameter(Mandatory)]
        [string] $EvidenceName
    )

    $actualCreator = [string] $Status.creator.login
    $actualCreatorId = [Int64] $Status.creator.id
    if ([Int64] $Status.id -ne $Id -or
        [string] $Status.state -cne $State -or
        [DateTimeOffset] $Status.created_at -ne $Timestamp -or
        [DateTimeOffset] $Status.updated_at -ne $Timestamp -or
        -not $actualCreator.Equals($approvedCreatorLogin, [StringComparison]::OrdinalIgnoreCase) -or
        $actualCreatorId -ne $approvedCreatorId -or
        [string] $Status.description -cne $Description) {
        throw "$EvidenceName no longer matches the exact approved GitHub status record."
    }
}

function Compare-StatusOrder {
    param(
        [Parameter(Mandatory)]
        [object] $Left,

        [Parameter(Mandatory)]
        [object] $Right
    )

    $timeComparison = ([DateTimeOffset] $Left.created_at).CompareTo(
        [DateTimeOffset] $Right.created_at)
    if ($timeComparison -ne 0) {
        return $timeComparison
    }
    return ([Int64] $Left.id).CompareTo([Int64] $Right.id)
}

function Assert-ApprovedCarryForwardEligibility {
    if ($normalizedReleaseVersion -cne $approvedReleaseVersion) {
        throw (
            "The approved durable-v2 carry-forward applies only to release " +
            "$approvedReleaseVersion, not '$ReleaseVersion'.")
    }
    if (-not $expectedCreatorLogin.Equals(
        $approvedCreatorLogin,
        [StringComparison]::OrdinalIgnoreCase)) {
        throw (
            "The approved durable-v2 carry-forward is pinned to creator " +
            "'$approvedCreatorLogin', not '$expectedCreatorLogin'.")
    }
    if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) {
        throw 'Git is required to verify the approved durable-v2 carry-forward.'
    }

    $sourceStatuses = @(Get-CommitStatuses -CommitSha $sourceCommit)
    $sourceStatus = Get-LatestStatus -Statuses $sourceStatuses -Context $canonicalContext
    if ($null -eq $sourceStatus) {
        throw "Approved source commit $sourceCommit has no $canonicalContext status."
    }
    Assert-PinnedStatus `
        -Status $sourceStatus `
        -Id $sourceStatusId `
        -State 'success' `
        -Timestamp $sourceStatusTimestamp `
        -Description $sourceStatusDescription `
        -EvidenceName 'The durable-v2 source evidence'

    $failedStatuses = @(Get-CommitStatuses -CommitSha $failedV3Commit)
    $failedStatus = Get-LatestStatus -Statuses $failedStatuses -Context $canonicalContext
    if ($null -eq $failedStatus) {
        throw "Known durable-v3 commit $failedV3Commit has no $canonicalContext status."
    }
    Assert-PinnedStatus `
        -Status $failedStatus `
        -Id $failedV3StatusId `
        -State 'failure' `
        -Timestamp $failedV3StatusTimestamp `
        -Description $failedV3StatusDescription `
        -EvidenceName 'The preserved durable-v3 failure'

    Invoke-Git `
        -Arguments @('merge-base', '--is-ancestor', $sourceCommit, $commitSha) `
        -FailureMessage "Approved source commit $sourceCommit is not an ancestor of $commitSha." |
        Out-Null
    Invoke-Git `
        -Arguments @('merge-base', '--is-ancestor', $failedV3Commit, $commitSha) `
        -FailureMessage "Known durable-v3 commit $failedV3Commit is not an ancestor of $commitSha." |
        Out-Null

    $productDiff = Invoke-Git `
        -Arguments @(
            'diff',
            '--name-status',
            '--no-renames',
            $sourceCommit,
            $commitSha,
            '--',
            'src') `
        -FailureMessage 'Could not inspect product-source changes for carry-forward.'
    $expectedProductDiff = "M`t$allowedProductPath"
    if ($productDiff -cne $expectedProductDiff) {
        throw (
            "Approved durable-v2 carry-forward requires exactly '$expectedProductDiff'; " +
            "the actual product diff was '$productDiff'.")
    }

    $productBlob = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', "${commitSha}:$allowedProductPath") `
        -FailureMessage "Could not resolve $allowedProductPath for commit $commitSha.").Trim()
    if ($productBlob -cne $approvedProductBlob) {
        throw (
            "Approved product file blob is $approvedProductBlob, but commit $commitSha " +
            "contains $productBlob.")
    }

    $productTree = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', "${commitSha}:src") `
        -FailureMessage "Could not resolve the src tree for commit $commitSha.").Trim()
    if ($productTree -cne $approvedProductTree) {
        throw (
            "Approved product source tree is $approvedProductTree, but commit $commitSha " +
            "contains $productTree.")
    }

    Write-Host (
        "Validated the one-time durable-v2 carry-forward eligibility for release " +
        "$approvedReleaseVersion at exact commit $commitSha.")
}

$candidateStatuses = @(Get-CommitStatuses -CommitSha $commitSha)
$latestCanonicalStatus =
    Get-LatestStatus -Statuses $candidateStatuses -Context $canonicalContext
$canonicalDiagnostic = $null

if ($null -eq $latestCanonicalStatus) {
    $canonicalDiagnostic = "Commit $commitSha has no $canonicalContext status."
}
elseif ([string] $latestCanonicalStatus.state -cne 'success') {
    $canonicalDiagnostic =
        "Latest $canonicalContext status for commit $commitSha is " +
        "'$([string] $latestCanonicalStatus.state)', not 'success'."
}
elseif (-not ([string] $latestCanonicalStatus.creator.login).Equals(
    $expectedCreatorLogin,
    [StringComparison]::OrdinalIgnoreCase)) {
    $canonicalDiagnostic =
        "Latest $canonicalContext status for commit $commitSha was created by " +
        "'$([string] $latestCanonicalStatus.creator.login)', not expected creator " +
        "'$expectedCreatorLogin'."
}
elseif ([string] $latestCanonicalStatus.description -cnotmatch $durableV3AttestationPattern) {
    $canonicalDiagnostic =
        "Latest $canonicalContext status for commit $commitSha does not contain a " +
        'canonical durable-v3 attestation.'
}
else {
    Write-Host (
        "Verified canonical durable-v3 status for exact commit $commitSha " +
        "from $([string] $latestCanonicalStatus.creator.login).")
    if (-not $ValidateApprovedCarryForwardEligibility) {
        return
    }
}

if (-not $ValidateApprovedCarryForwardEligibility -and
    $normalizedReleaseVersion -cne $approvedReleaseVersion) {
    throw $canonicalDiagnostic
}

Assert-ApprovedCarryForwardEligibility
if ($ValidateApprovedCarryForwardEligibility) {
    return
}

$latestCarryForwardStatus =
    Get-LatestStatus -Statuses $candidateStatuses -Context $carryForwardContext
if ($null -eq $latestCarryForwardStatus) {
    throw (
        "$canonicalDiagnostic No approved $carryForwardContext status exists for " +
        "release $approvedReleaseVersion.")
}
if ([string] $latestCarryForwardStatus.state -cne 'success') {
    throw (
        "Latest $carryForwardContext status for commit $commitSha is " +
        "'$([string] $latestCarryForwardStatus.state)', not 'success'.")
}
$carryForwardCreator = [string] $latestCarryForwardStatus.creator.login
$carryForwardCreatorId = [Int64] $latestCarryForwardStatus.creator.id
if (-not $carryForwardCreator.Equals(
    $expectedCreatorLogin,
    [StringComparison]::OrdinalIgnoreCase) -or
    $carryForwardCreatorId -ne $approvedCreatorId) {
    throw (
        "Latest $carryForwardContext status for commit $commitSha was not created by " +
        "the approved attestor $approvedCreatorLogin ($approvedCreatorId).")
}
if ([string] $latestCarryForwardStatus.description -cne $carryForwardDescription) {
    throw (
        "Latest $carryForwardContext status for commit $commitSha does not contain the " +
        'exact approved v4.4.0 carry-forward attestation.')
}

$failedStatusRecord = [pscustomobject]@{
    created_at = $failedV3StatusTimestamp
    id = $failedV3StatusId
}
if ((Compare-StatusOrder -Left $latestCarryForwardStatus -Right $failedStatusRecord) -le 0) {
    throw 'The approved carry-forward status does not postdate the preserved durable-v3 failure.'
}
if ($null -ne $latestCanonicalStatus -and
    (Compare-StatusOrder -Left $latestCarryForwardStatus -Right $latestCanonicalStatus) -le 0) {
    throw (
        "A $canonicalContext status at or after the approved carry-forward requires " +
        'fresh release qualification.')
}

Write-Host (
    "Verified the explicit one-time durable-v2 carry-forward for release " +
    "$approvedReleaseVersion at exact commit $commitSha from $carryForwardCreator; " +
    "durable-v3 failure $failedV3StatusId remains preserved.")
