#requires -Version 7.0

[CmdletBinding()]
param(
    [string] $PreviousRef = '',

    [string] $CandidateRef = 'HEAD',

    [string] $OutputPath = '',

    [ValidateSet(3, 5, 7, 9)]
    [int] $RepeatCount = 3,

    [ValidateRange(0, 3600)]
    [int] $PostBuildQuiescenceSeconds = 30,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

    [ValidateRange(0, 1000)]
    [double] $MaxP99RegressionMilliseconds = 0.05,

    [ValidateSet('P95', 'P99')]
    [string] $BlockingLatencyPercentile = 'P95',

    [switch] $ConfirmDedicatedFixedSsd,

    [string] $GitHubRepository = '',

    [switch] $NoGitHubStatus
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$statusPolicy = 'durable-v2'
$canonicalRepeatCount = 3
$canonicalPostBuildQuiescenceSeconds = 30
$canonicalMaxThroughputRegressionPercent = 15.0
$canonicalMaxP99RegressionPercent = 25.0
$canonicalMaxP99RegressionMilliseconds = 0.05
$canonicalBlockingLatencyPercentile = 'P95'

if (-not $IsWindows) {
    throw 'Local durable performance qualification requires a dedicated Windows machine with a fixed SSD.'
}
if (-not $ConfirmDedicatedFixedSsd) {
    throw (
        'Confirm that this Windows machine is idle and its temporary directory is on a fixed SSD, ' +
        'then rerun with -ConfirmDedicatedFixedSsd.')
}
if (-not $NoGitHubStatus) {
    $nonCanonicalSettings = [Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($PreviousRef)) {
        $nonCanonicalSettings.Add('PreviousRef must be automatically discovered')
    }
    if ($RepeatCount -ne $canonicalRepeatCount) {
        $nonCanonicalSettings.Add("RepeatCount must be $canonicalRepeatCount")
    }
    if ($PostBuildQuiescenceSeconds -ne $canonicalPostBuildQuiescenceSeconds) {
        $nonCanonicalSettings.Add(
            "PostBuildQuiescenceSeconds must be $canonicalPostBuildQuiescenceSeconds")
    }
    if ($MaxThroughputRegressionPercent -ne $canonicalMaxThroughputRegressionPercent) {
        $nonCanonicalSettings.Add(
            "MaxThroughputRegressionPercent must be $canonicalMaxThroughputRegressionPercent")
    }
    if ($MaxP99RegressionPercent -ne $canonicalMaxP99RegressionPercent) {
        $nonCanonicalSettings.Add(
            "MaxP99RegressionPercent must be $canonicalMaxP99RegressionPercent")
    }
    if ($MaxP99RegressionMilliseconds -ne $canonicalMaxP99RegressionMilliseconds) {
        $nonCanonicalSettings.Add(
            "MaxP99RegressionMilliseconds must be $canonicalMaxP99RegressionMilliseconds")
    }
    if ($BlockingLatencyPercentile -cne $canonicalBlockingLatencyPercentile) {
        $nonCanonicalSettings.Add(
            "BlockingLatencyPercentile must be $canonicalBlockingLatencyPercentile")
    }
    if ($nonCanonicalSettings.Count -gt 0) {
        throw (
            "The official local durable status requires canonical policy '$statusPolicy': " +
            "$($nonCanonicalSettings -join '; '). Use -NoGitHubStatus for diagnostic overrides.")
    }
}

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$repositoryRoot = [IO.Path]::GetFullPath(
    [IO.Path]::Combine($scriptDirectory, '..', '..', '..'))
$comparisonScript = Join-Path $scriptDirectory 'Test-PreviousReleasePerformance.ps1'
if (-not (Test-Path -LiteralPath $comparisonScript -PathType Leaf)) {
    throw "Previous-release performance script not found: $comparisonScript"
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

function Invoke-GitHubStatus {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('pending', 'success', 'failure')]
        [string] $State,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $statusOutput = @(
        & gh api `
            --method POST `
            "repos/$GitHubRepository/statuses/$candidateCommit" `
            --field "state=$State" `
            --field "context=$statusContext" `
            --field "description=$Description" `
            --silent 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        $details = ($statusOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        throw "Could not publish GitHub status '$statusContext' for $candidateCommit. $details"
    }
}

$status = Invoke-Git `
    -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
    -FailureMessage 'Could not inspect the repository worktree.'
if (-not [string]::IsNullOrWhiteSpace($status)) {
    throw 'Local durable performance qualification requires a clean repository worktree.'
}

$candidateCommit = (Invoke-Git `
    -Arguments @('rev-parse', '--verify', "$CandidateRef^{commit}") `
    -FailureMessage "Candidate ref '$CandidateRef' does not resolve to a commit.").Trim()
$previousCommit = ''
if (-not [string]::IsNullOrWhiteSpace($PreviousRef)) {
    $previousCommit = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', "$PreviousRef^{commit}") `
        -FailureMessage "Previous release ref '$PreviousRef' does not resolve to a commit.").Trim()
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path `
        ([IO.Path]::GetTempPath()) `
        "csharpdb-local-durable-performance-$([Guid]::NewGuid().ToString('N'))"
}
$outputRoot = [IO.Path]::GetFullPath($OutputPath)
$benchmarkTemporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$pathComparison = [StringComparison]::OrdinalIgnoreCase
$normalizedRepositoryRoot = $repositoryRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$normalizedOutputRoot = $outputRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$repositoryPrefix = $normalizedRepositoryRoot + [IO.Path]::DirectorySeparatorChar
if ($normalizedOutputRoot.Equals($normalizedRepositoryRoot, $pathComparison) -or
    $normalizedOutputRoot.StartsWith($repositoryPrefix, $pathComparison)) {
    throw "Local durable performance output must be outside the repository: $outputRoot"
}
if (Test-Path -LiteralPath $outputRoot) {
    if (-not (Test-Path -LiteralPath $outputRoot -PathType Container)) {
        throw "Local durable performance output must be a directory: $outputRoot"
    }
    if ($null -ne (Get-ChildItem -LiteralPath $outputRoot -Force | Select-Object -First 1)) {
        throw "Local durable performance output must be absent or empty: $outputRoot"
    }
}
else {
    New-Item -ItemType Directory -Path $outputRoot | Out-Null
}

$statusContext = 'csharpdb/local-durable-performance'
if (-not $NoGitHubStatus) {
    $authOutput = @(& gh auth status 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw (
            'GitHub authentication is required before the local release gate starts. ' +
            (($authOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine))
    }
    if ([string]::IsNullOrWhiteSpace($GitHubRepository)) {
        Push-Location $repositoryRoot
        try {
            $repositoryOutput = @(
                & gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>&1
            )
            if ($LASTEXITCODE -ne 0) {
                throw (
                    'Could not resolve the GitHub repository for release status. ' +
                    (($repositoryOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine))
            }
            $GitHubRepository = (($repositoryOutput | ForEach-Object { [string] $_ }) -join '').Trim()
        }
        finally {
            Pop-Location
        }
    }
    if ($GitHubRepository -cnotmatch '^[^/\s]+/[^/\s]+$') {
        throw "GitHubRepository must use the owner/name form: $GitHubRepository"
    }
    Invoke-GitHubStatus `
        -State pending `
        -Description "policy=$statusPolicy; local durable qualification running"
}

$startedUtc = [DateTimeOffset]::UtcNow
$result = 'FAIL'
$failureMessage = ''
$passFailures = [Collections.Generic.List[string]]::new()
$durabilityVariable = 'CSHARPDB_BENCH_DURABILITY'
$priorDurability = [Environment]::GetEnvironmentVariable(
    $durabilityVariable,
    [EnvironmentVariableTarget]::Process)
[Environment]::SetEnvironmentVariable(
    $durabilityVariable,
    'Durable',
    [EnvironmentVariableTarget]::Process)

try {
    Write-Host 'Running two sequential local durable performance passes.'
    Write-Host 'Expected duration on an idle fixed-SSD machine: approximately 75-100 minutes.'
    Write-Host "Evidence root: $outputRoot"

    foreach ($qualificationPass in 1, 2) {
        $passOutput = Join-Path $outputRoot "pass-$qualificationPass"
        $parameters = @{
            CandidateRef = $candidateCommit
            OutputPath = $passOutput
            QualificationPass = $qualificationPass
            Paired = $true
            SuiteName = @('master-table-durable-writes')
            RepeatCount = $RepeatCount
            PostBuildQuiescenceSeconds = $PostBuildQuiescenceSeconds
            MaxThroughputRegressionPercent = $MaxThroughputRegressionPercent
            MaxP99RegressionPercent = $MaxP99RegressionPercent
            MaxP99RegressionMilliseconds = $MaxP99RegressionMilliseconds
            BlockingLatencyPercentile = $BlockingLatencyPercentile
        }
        if (-not [string]::IsNullOrWhiteSpace($previousCommit)) {
            $parameters.PreviousRef = $previousCommit
        }

        Write-Host "Starting local durable performance pass $qualificationPass of 2."
        try {
            & $comparisonScript @parameters
        }
        catch {
            $passMessage = $_.Exception.Message -replace '\r?\n', ' '
            $passFailures.Add("Pass $qualificationPass failed: $passMessage")
            if ($qualificationPass -eq 1) {
                Write-Warning 'Pass 1 failed; continuing to collect the second pass.'
            }
            else {
                Write-Warning 'Pass 2 failed.'
            }
        }

        if ($qualificationPass -eq 1 -and [string]::IsNullOrWhiteSpace($previousCommit)) {
            $preflightPath = Join-Path $passOutput 'previous-release-performance-preflight.md'
            if (Test-Path -LiteralPath $preflightPath -PathType Leaf) {
                $previousLine = Select-String `
                    -LiteralPath $preflightPath `
                    -Pattern '^- Previous ref: `[^`]+` \(`(?<commit>[0-9a-f]{40})`\)$' |
                    Select-Object -First 1
                if ($null -ne $previousLine -and $previousLine.Matches[0].Groups['commit'].Success) {
                    $previousCommit = $previousLine.Matches[0].Groups['commit'].Value
                }
            }
            if ([string]::IsNullOrWhiteSpace($previousCommit)) {
                $passFailures.Add('Could not pin the previous-release commit from pass 1 evidence.')
            }
        }
    }
}
catch {
    $unexpectedMessage = $_.Exception.Message -replace '\r?\n', ' '
    $passFailures.Add("Unexpected wrapper failure: $unexpectedMessage")
}
finally {
    [Environment]::SetEnvironmentVariable(
        $durabilityVariable,
        $priorDurability,
        [EnvironmentVariableTarget]::Process)
}

if ($passFailures.Count -eq 0) {
    $result = 'PASS'
}
else {
    $failureMessage = $passFailures -join ' | '
}

$completedUtc = [DateTimeOffset]::UtcNow
$summaryPath = Join-Path $outputRoot 'local-durable-performance.md'
function Write-LocalSummary {
    $summaryLines = @(
        '# Local durable performance qualification',
        '',
        "- Result: **$result**",
        '- Execution: two sequential balanced paired passes on one Windows machine',
        '- Suite: `master-table-durable-writes` (10 durable write rows)',
        "- Candidate commit: ``$candidateCommit``",
        $(if ([string]::IsNullOrWhiteSpace($previousCommit)) {
            '- Previous release commit: unresolved'
        }
        else {
            "- Previous release commit: ``$previousCommit``"
        }),
        "- Repeat count per order: $RepeatCount",
        '- Durability mode: `Durable`',
        "- Status policy: ``$statusPolicy``",
        "- Blocking latency percentile: ``$BlockingLatencyPercentile``",
        $(if ($BlockingLatencyPercentile -ceq 'P99') {
            '- P99 latency: blocking for this diagnostic run'
        }
        else {
            '- P99 latency: diagnostic only'
        }),
        '- Dedicated fixed SSD: confirmed by the release operator',
        "- Machine: ``$env:COMPUTERNAME``",
        "- Benchmark temporary root: ``$benchmarkTemporaryRoot``",
        "- Evidence root: ``$outputRoot``",
        $(if ($NoGitHubStatus) {
            '- GitHub release status: disabled (diagnostic run only)'
        }
        else {
            "- GitHub release status: ``$statusContext`` in ``$GitHubRepository``"
        }),
        "- Started UTC: $($startedUtc.ToString('O'))",
        "- Completed UTC: $($completedUtc.ToString('O'))",
        "- Elapsed: $([Math]::Round(($completedUtc - $startedUtc).TotalMinutes, 1)) minutes",
        '- Pass 1 report: `pass-1/previous-release-performance.md`',
        '- Pass 2 report: `pass-2/previous-release-performance.md`'
    )
    if (-not [string]::IsNullOrWhiteSpace($failureMessage)) {
        $summaryLines += "- Failure: $failureMessage"
    }
    [IO.File]::WriteAllLines($summaryPath, $summaryLines)
}

Write-LocalSummary

if (-not $NoGitHubStatus) {
    try {
        if ($result -eq 'PASS') {
            $reportHashes = @(
                foreach ($pass in 1, 2) {
                    $reportPath = Join-Path `
                        (Join-Path $outputRoot "pass-$pass") `
                        'previous-release-performance.md'
                    (Get-FileHash -LiteralPath $reportPath -Algorithm SHA256).Hash.Substring(0, 8)
                }
            )
            Invoke-GitHubStatus `
                -State success `
                -Description (
                    "policy=$statusPolicy; baseline=$previousCommit; " +
                    "reports=$($reportHashes -join '/')")
        }
        else {
            Invoke-GitHubStatus `
                -State failure `
                -Description "policy=$statusPolicy; local durable qualification failed"
        }
    }
    catch {
        $statusMessage = $_.Exception.Message -replace '\r?\n', ' '
        $result = 'FAIL'
        $failureMessage = @($failureMessage, $statusMessage) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Join-String -Separator ' | '
        Write-LocalSummary
    }
}

Write-Host "Local durable performance summary: $summaryPath"
if ($result -ne 'PASS') {
    throw "Local durable performance qualification failed. $failureMessage"
}

Write-Host 'Local durable performance qualification passed.'
