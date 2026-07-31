#requires -Version 7.0

[CmdletBinding()]
param(
    [string] $PreviousRef = '',

    [string] $CandidateRef = 'HEAD',

    [string] $OutputPath = '',

    [ValidateSet(1, 2)]
    [int] $QualificationPass = 1,

    [ValidateRange(2, 9)]
    [int] $RepeatCount = 3,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

    [ValidateRange(0, 1000)]
    [double] $MaxP99RegressionMilliseconds = 0.05,

    [switch] $PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$repositoryRoot = [IO.Path]::GetFullPath(
    [IO.Path]::Combine($scriptDirectory, '..', '..', '..'))
$temporaryParent = if (-not [string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
    $env:RUNNER_TEMP
}
else {
    [IO.Path]::GetTempPath()
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $temporaryParent "csharpdb-previous-release-performance-$([Guid]::NewGuid().ToString('N'))"
}
$outputRoot = [IO.Path]::GetFullPath($OutputPath)
$pathComparison = if ($IsWindows) {
    [StringComparison]::OrdinalIgnoreCase
}
else {
    [StringComparison]::Ordinal
}
$normalizedRepositoryRoot = $repositoryRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$normalizedOutputRoot = $outputRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$repositoryPrefix = $normalizedRepositoryRoot + [IO.Path]::DirectorySeparatorChar
if ($normalizedOutputRoot.Equals($normalizedRepositoryRoot, $pathComparison) -or
    $normalizedOutputRoot.StartsWith($repositoryPrefix, $pathComparison)) {
    throw "Performance qualification output must be outside the repository: $outputRoot"
}

if (Test-Path -LiteralPath $outputRoot) {
    if (-not (Test-Path -LiteralPath $outputRoot -PathType Container)) {
        throw "Performance qualification output must be a directory: $outputRoot"
    }
    if ($null -ne (Get-ChildItem -LiteralPath $outputRoot -Force | Select-Object -First 1)) {
        throw "Performance qualification output must be absent or empty: $outputRoot"
    }
}

function Invoke-Git {
    param(
        [Parameter(Mandatory)]
        [string[]] $Arguments,

        [Parameter(Mandatory)]
        [string] $FailureMessage
    )

    $output = @(& git -C $repositoryRoot @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        $details = ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        if ([string]::IsNullOrWhiteSpace($details)) {
            throw $FailureMessage
        }
        throw "$FailureMessage$([Environment]::NewLine)$details"
    }

    return ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
}

function Assert-CleanRepository {
    $status = Invoke-Git `
        -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
        -FailureMessage 'Could not inspect the repository worktree.'
    if (-not [string]::IsNullOrWhiteSpace($status)) {
        throw 'Previous-release performance qualification requires a clean repository worktree.'
    }
}

function Assert-BenchmarkProjectAtCommit {
    param(
        [Parameter(Mandatory)]
        [string] $Commit,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $objectName = "${Commit}:tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj"
    Invoke-Git `
        -Arguments @('cat-file', '-e', $objectName) `
        -FailureMessage "$Description does not contain the release-core benchmark project." |
        Out-Null
}

Assert-CleanRepository
$candidateCommit = (Invoke-Git `
    -Arguments @('rev-parse', '--verify', "$CandidateRef^{commit}") `
    -FailureMessage "Candidate ref '$CandidateRef' does not resolve to a commit.").Trim()

if ([string]::IsNullOrWhiteSpace($PreviousRef)) {
    $semanticReleaseTagPattern =
        '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$'
    $allTags = Invoke-Git `
        -Arguments @('tag', '--list') `
        -FailureMessage 'Could not enumerate semantic release tags.'
    $semanticReleaseTags = @(
        $allTags -split '\r?\n' |
            Where-Object { $_ -cmatch $semanticReleaseTagPattern }
    )
    if ($semanticReleaseTags.Count -eq 0) {
        throw (
            'PreviousRef was not supplied and no semantic release tag ' +
            'is available for automatic baseline discovery.')
    }

    $describeArguments = @('describe', '--tags', '--abbrev=0')
    foreach ($releaseTag in $semanticReleaseTags) {
        $describeArguments += @('--match', $releaseTag)
    }

    $tagsAtCandidate = Invoke-Git `
        -Arguments @('tag', '--points-at', $candidateCommit) `
        -FailureMessage "Could not inspect release tags at '$candidateCommit'."
    $candidateReleaseTags = @(
        $tagsAtCandidate -split '\r?\n' |
            Where-Object { $_ -cmatch $semanticReleaseTagPattern }
    )
    foreach ($candidateReleaseTag in $candidateReleaseTags) {
        $describeArguments += @('--exclude', $candidateReleaseTag)
    }
    $describeArguments += $candidateCommit

    $PreviousRef = (Invoke-Git `
        -Arguments $describeArguments `
        -FailureMessage (
            "Could not discover a previous semantic release reachable from " +
            "candidate ref '$CandidateRef'. Supply -PreviousRef explicitly.")).Trim()
    Write-Host "Discovered previous release baseline '$PreviousRef'."
}

$previousCommit = (Invoke-Git `
    -Arguments @('rev-parse', '--verify', "$PreviousRef^{commit}") `
    -FailureMessage "Previous release ref '$PreviousRef' does not resolve to a commit.").Trim()
if ($previousCommit -eq $candidateCommit) {
    throw 'Previous and candidate refs resolve to the same commit.'
}

& git -C $repositoryRoot merge-base --is-ancestor $previousCommit $candidateCommit 2>&1 |
    Out-Null
$ancestorExitCode = $LASTEXITCODE
if ($ancestorExitCode -eq 1) {
    throw "Previous release ref '$PreviousRef' is not an ancestor of candidate ref '$CandidateRef'."
}
if ($ancestorExitCode -ne 0) {
    throw "Could not verify ancestry between '$PreviousRef' and '$CandidateRef'."
}

$comparisonScript = Join-Path $scriptDirectory 'Compare-ReleaseCore.ps1'
if (-not (Test-Path -LiteralPath $comparisonScript -PathType Leaf)) {
    throw "Release-core comparison script not found: $comparisonScript"
}
Assert-BenchmarkProjectAtCommit -Commit $previousCommit -Description "Previous release ref '$PreviousRef'"
Assert-BenchmarkProjectAtCommit -Commit $candidateCommit -Description "Candidate ref '$CandidateRef'"

New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null
$revisionOrder = if ($QualificationPass -eq 1) {
    @('previous', 'candidate')
}
else {
    @('candidate', 'previous')
}
$suiteDefinitions = @(
    [pscustomobject]@{ Name = 'master-table'; Argument = '--master-table' }
    [pscustomobject]@{ Name = 'durable-sql-batching'; Argument = '--durable-sql-batching' }
    [pscustomobject]@{ Name = 'concurrent-write-diagnostics'; Argument = '--concurrent-write-diagnostics' }
    [pscustomobject]@{ Name = 'hybrid-storage-mode'; Argument = '--hybrid-storage-mode' }
    [pscustomobject]@{ Name = 'hybrid-hot-set-read'; Argument = '--hybrid-hot-set-read' }
    [pscustomobject]@{ Name = 'hybrid-cold-open'; Argument = '--hybrid-cold-open' }
    [pscustomobject]@{ Name = 'sqlite-compare'; Argument = '--sqlite-compare' }
)
$executionPlan = @(
    foreach ($suite in $suiteDefinitions) {
        foreach ($revision in $revisionOrder) {
            [pscustomobject]@{
                Suite = $suite
                Revision = $revision
            }
        }
    }
)
$suiteOrder = ($suiteDefinitions.Name -join ', ')
$executionOrder = (
    $executionPlan |
        ForEach-Object { "$($_.Suite.Name)/$($_.Revision)" }
) -join ', '
$p99AbsoluteAllowance = $MaxP99RegressionMilliseconds.ToString(
    '0.0000',
    [Globalization.CultureInfo]::InvariantCulture)
$logRoot = Join-Path $outputRoot 'logs'
$executionLogPath = Join-Path $logRoot 'execution-order.log'

$preflightPath = Join-Path $outputRoot 'previous-release-performance-preflight.md'
$preflight = @(
    '# Previous-release performance preflight',
    '',
    '- Result: **PASS**',
    "- Qualification pass: $QualificationPass",
    '- Execution strategy: suite-interleaved',
    "- Revision order within each suite: $($revisionOrder -join ' then ')",
    "- Suite order: $suiteOrder",
    "- Execution order: $executionOrder",
    "- Repeat count: $RepeatCount",
    "- Throughput regression limit: $MaxThroughputRegressionPercent%",
    "- P99 regression limit: $MaxP99RegressionPercent%",
    "- P99 absolute regression allowance: $p99AbsoluteAllowance ms",
    '- P99 failure rule: relative and absolute limits must both be exceeded',
    "- Previous ref: ``$PreviousRef`` (``$previousCommit``)",
    "- Candidate ref: ``$CandidateRef`` (``$candidateCommit``)",
    "- Planned execution log: ``$executionLogPath``",
    "- Output root: ``$outputRoot``"
)
[IO.File]::WriteAllLines($preflightPath, $preflight)
Write-Host 'Previous-release performance preflight passed.'
Write-Host "Evidence: $preflightPath"
if ($PreflightOnly) {
    return
}

$baselineWorktree = Join-Path $outputRoot 'baseline-source'
$candidateWorktree = Join-Path $outputRoot 'candidate-source'
$baselineResults = Join-Path $outputRoot 'baseline-results'
$candidateResults = Join-Path $outputRoot 'candidate-results'
$reportPath = Join-Path $outputRoot 'previous-release-performance.md'
$baselineAdded = $false
$candidateAdded = $false
$primaryFailure = $null
$cleanupFailures = [Collections.Generic.List[string]]::new()
$originalNuGetPackages = $env:NUGET_PACKAGES
$originalDotnetCliHome = $env:DOTNET_CLI_HOME
$env:NUGET_PACKAGES = Join-Path $outputRoot '.nuget-packages'
$env:DOTNET_CLI_HOME = Join-Path $outputRoot '.dotnet-home'
New-Item -ItemType Directory -Path $env:NUGET_PACKAGES -Force | Out-Null
New-Item -ItemType Directory -Path $env:DOTNET_CLI_HOME -Force | Out-Null
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
[IO.File]::WriteAllLines(
    $executionLogPath,
    @('TimestampUtc|Ordinal|Suite|Revision|State|Detail'))

function Write-ExecutionEvent {
    param(
        [Parameter(Mandatory)]
        [int] $Ordinal,

        [Parameter(Mandatory)]
        [string] $Suite,

        [Parameter(Mandatory)]
        [string] $Revision,

        [Parameter(Mandatory)]
        [ValidateSet('START', 'PASS', 'FAIL')]
        [string] $State,

        [string] $Detail = ''
    )

    $safeDetail = $Detail.
        Replace('|', '/').
        Replace("`r", ' ').
        Replace("`n", ' ')
    $timestamp = [DateTimeOffset]::UtcNow.ToString(
        'o',
        [Globalization.CultureInfo]::InvariantCulture)
    Add-Content `
        -LiteralPath $executionLogPath `
        -Value "$timestamp|$Ordinal|$Suite|$Revision|$State|$safeDetail"
}

function Get-BenchmarkProject {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot
    )

    return [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'CSharpDB.Benchmarks.csproj')
}

function Invoke-BenchmarkBuild {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $RunName
    )

    $project = Get-BenchmarkProject -SourceRoot $SourceRoot
    $logPath = Join-Path $logRoot "$RunName.log"
    [IO.File]::WriteAllLines(
        $logPath,
        @(
            "=== BUILD $RunName ===",
            "Project: $project",
            ''
        ))
    Push-Location $SourceRoot
    try {
        & dotnet build $project -c Release --nologo 2>&1 |
            Tee-Object -FilePath $logPath -Append |
            Write-Host
        $buildExitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    if ($buildExitCode -ne 0) {
        throw "Release-core benchmark build failed in '$SourceRoot'."
    }
}

function Invoke-ReleaseCoreSuite {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $Destination,

        [Parameter(Mandatory)]
        [string] $RunName,

        [Parameter(Mandatory)]
        [object] $Suite
    )

    $project = Get-BenchmarkProject -SourceRoot $SourceRoot
    $logPath = Join-Path $logRoot "$RunName.log"
    $resultRoot = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    $resultPattern = "$($Suite.Name)-*-median-of-$RepeatCount.csv"
    $existingResults = @(
        if (Test-Path -LiteralPath $resultRoot -PathType Container) {
            Get-ChildItem `
                -LiteralPath $resultRoot `
                -File `
                -Recurse `
                -Filter $resultPattern
        }
    )
    if ($existingResults.Count -ne 0) {
        throw (
            "Release-core suite '$($Suite.Name)' found " +
            "$($existingResults.Count) pre-existing median CSV file(s) in " +
            "'$resultRoot'.")
    }

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $destinationPath = Join-Path $Destination "$($Suite.Name).csv"
    if (Test-Path -LiteralPath $destinationPath) {
        throw "Release-core destination already exists: $destinationPath"
    }

    Add-Content `
        -LiteralPath $logPath `
        -Value @(
            '',
            "=== SUITE $($Suite.Name) / $RunName ===",
            "Argument: $($Suite.Argument)",
            ''
        )
    Push-Location $SourceRoot
    try {
        & dotnet run `
            -c Release `
            --no-build `
            --no-restore `
            --project $project `
            -- `
            $Suite.Argument `
            --repeat $RepeatCount `
            --repro 2>&1 |
                Tee-Object -FilePath $logPath -Append |
                Write-Host
        $benchmarkExitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    if ($benchmarkExitCode -ne 0) {
        throw (
            "Release-core suite '$($Suite.Name)' failed in " +
            "'$SourceRoot'.")
    }

    if (-not (Test-Path -LiteralPath $resultRoot -PathType Container)) {
        throw "Release-core benchmark output directory not found: $resultRoot"
    }
    $results = @(
        Get-ChildItem `
            -LiteralPath $resultRoot `
            -File `
            -Recurse `
            -Filter $resultPattern
    )
    if ($results.Count -ne 1) {
        throw (
            "Release-core suite '$($Suite.Name)' produced " +
            "$($results.Count) median CSV file(s); expected exactly one.")
    }
    Copy-Item -LiteralPath $results[0].FullName -Destination $destinationPath
}

try {
    & git -C $repositoryRoot worktree add --detach $baselineWorktree $previousCommit
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the previous-release worktree."
    }
    $baselineAdded = $true

    & git -C $repositoryRoot worktree add --detach $candidateWorktree $candidateCommit
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the candidate worktree."
    }
    $candidateAdded = $true

    foreach ($revision in $revisionOrder) {
        if ($revision -eq 'previous') {
            Invoke-BenchmarkBuild `
                -SourceRoot $baselineWorktree `
                -RunName 'previous-release'
        }
        else {
            Invoke-BenchmarkBuild `
                -SourceRoot $candidateWorktree `
                -RunName 'candidate'
        }
    }

    $executionOrdinal = 0
    foreach ($entry in $executionPlan) {
        $executionOrdinal++
        Write-ExecutionEvent `
            -Ordinal $executionOrdinal `
            -Suite $entry.Suite.Name `
            -Revision $entry.Revision `
            -State 'START'
        try {
            if ($entry.Revision -eq 'previous') {
                Invoke-ReleaseCoreSuite `
                    -SourceRoot $baselineWorktree `
                    -Destination $baselineResults `
                    -RunName 'previous-release' `
                    -Suite $entry.Suite
            }
            else {
                Invoke-ReleaseCoreSuite `
                    -SourceRoot $candidateWorktree `
                    -Destination $candidateResults `
                    -RunName 'candidate' `
                    -Suite $entry.Suite
            }
        }
        catch {
            Write-ExecutionEvent `
                -Ordinal $executionOrdinal `
                -Suite $entry.Suite.Name `
                -Revision $entry.Revision `
                -State 'FAIL' `
                -Detail $_.Exception.Message
            throw
        }
        Write-ExecutionEvent `
            -Ordinal $executionOrdinal `
            -Suite $entry.Suite.Name `
            -Revision $entry.Revision `
            -State 'PASS'
    }

    try {
        & $comparisonScript `
            -BaselineResultsPath $baselineResults `
            -CandidateResultsPath $candidateResults `
            -ReportPath $reportPath `
            -MaxThroughputRegressionPercent $MaxThroughputRegressionPercent `
            -MaxP99RegressionPercent $MaxP99RegressionPercent `
            -MaxP99RegressionMilliseconds $MaxP99RegressionMilliseconds
    }
    finally {
        if (Test-Path -LiteralPath $reportPath -PathType Leaf) {
            $metadata = @(
                '',
                '## Qualification identity',
                '',
                "- Previous ref: ``$PreviousRef`` (``$previousCommit``)",
                "- Candidate ref: ``$CandidateRef`` (``$candidateCommit``)",
                "- Qualification pass: $QualificationPass",
                '- Execution strategy: suite-interleaved',
                "- Revision order within each suite: $($revisionOrder -join ' then ')",
                "- Suite order: $suiteOrder",
                "- Execution order: $executionOrder",
                "- Execution log: ``$executionLogPath``",
                "- Repeat count: $RepeatCount",
                "- Runner: ``$([Environment]::MachineName)``",
                "- OS: ``$([Runtime.InteropServices.RuntimeInformation]::OSDescription)``",
                "- Process architecture: ``$([Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture)``",
                "- .NET SDK: ``$((& dotnet --version).Trim())``"
            )
            Add-Content -LiteralPath $reportPath -Value $metadata
        }
    }
}
catch {
    $primaryFailure = $_
}
finally {
    if ($candidateAdded) {
        & git -C $repositoryRoot worktree remove --force $candidateWorktree 2>&1 |
            Out-Null
        if ($LASTEXITCODE -ne 0) {
            $cleanupFailures.Add("Could not remove candidate worktree '$candidateWorktree'.")
        }
    }
    if ($baselineAdded) {
        & git -C $repositoryRoot worktree remove --force $baselineWorktree 2>&1 |
            Out-Null
        if ($LASTEXITCODE -ne 0) {
            $cleanupFailures.Add("Could not remove previous-release worktree '$baselineWorktree'.")
        }
    }

    if ($null -eq $originalNuGetPackages) {
        Remove-Item Env:NUGET_PACKAGES -ErrorAction SilentlyContinue
    }
    else {
        $env:NUGET_PACKAGES = $originalNuGetPackages
    }
    if ($null -eq $originalDotnetCliHome) {
        Remove-Item Env:DOTNET_CLI_HOME -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_CLI_HOME = $originalDotnetCliHome
    }
}

if ($null -ne $primaryFailure) {
    if ($cleanupFailures.Count -gt 0) {
        Write-Warning ($cleanupFailures -join [Environment]::NewLine)
    }
    throw $primaryFailure
}
if ($cleanupFailures.Count -gt 0) {
    throw ($cleanupFailures -join [Environment]::NewLine)
}

Assert-CleanRepository
Write-Host "Previous-release performance qualification passed."
Write-Host "Evidence: $reportPath"
