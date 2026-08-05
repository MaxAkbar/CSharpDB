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
    [string] $ExpectedCreator
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$statusContext = 'csharpdb/local-durable-performance'
$canonicalAttestationPattern =
    '^policy=durable-v3; baseline=[0-9a-f]{40}; design=[0-9A-F]{8}; reports=[0-9A-F]{8}/[0-9A-F]{8}$'
$commitSha = $Commit.ToLowerInvariant()
$expectedCreatorLogin = $ExpectedCreator.Trim()

if ([string]::IsNullOrWhiteSpace($expectedCreatorLogin)) {
    throw 'ExpectedCreator cannot be empty or whitespace.'
}
if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required to verify the local durable performance status.'
}

$apiPath = "repos/$GitHubRepository/commits/$commitSha/statuses?per_page=100"
$apiOutput = @(& gh api $apiPath 2>&1)
if ($LASTEXITCODE -ne 0) {
    $details = ($apiOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
    throw "Could not read GitHub statuses for commit $commitSha. $details"
}

try {
    $statuses = @(($apiOutput -join [Environment]::NewLine) | ConvertFrom-Json -Depth 20)
}
catch {
    throw "GitHub returned invalid status JSON for commit $commitSha. $($_.Exception.Message)"
}

$matchingStatuses = @(
    $statuses |
        Where-Object { [string] $_.context -ceq $statusContext } |
        Sort-Object -Property @(
            @{ Expression = { [DateTimeOffset] $_.created_at }; Descending = $true },
            @{ Expression = { [Int64] $_.id }; Descending = $true }
        )
)
if ($matchingStatuses.Count -eq 0) {
    throw "Commit $commitSha has no $statusContext status."
}

$latestStatus = $matchingStatuses[0]
$state = [string] $latestStatus.state
if ($state -cne 'success') {
    throw (
        "Latest $statusContext status for commit $commitSha is '$state', not 'success'.")
}

$creator = [string] $latestStatus.creator.login
if (-not $creator.Equals($expectedCreatorLogin, [StringComparison]::OrdinalIgnoreCase)) {
    throw (
        "Latest $statusContext status for commit $commitSha was created by '$creator', " +
        "not expected creator '$expectedCreatorLogin'.")
}

$description = [string] $latestStatus.description
if ($description -cnotmatch $canonicalAttestationPattern) {
    throw (
        "Latest $statusContext status for commit $commitSha does not contain a canonical " +
        'durable-v3 attestation.')
}

Write-Host (
    "Verified canonical durable-v3 status for exact commit $commitSha " +
    "from $creator.")
