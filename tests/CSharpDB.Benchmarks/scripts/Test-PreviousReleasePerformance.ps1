#requires -Version 7.0

[CmdletBinding()]
param(
    [string] $PreviousRef = '',

    [string] $CandidateRef = 'HEAD',

    [string] $OutputPath = '',

    [ValidateSet(1, 2)]
    [int] $QualificationPass = 1,

    [ValidateRange(1, 9)]
    [int] $RepeatCount = 3,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

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
$runOrder = if ($QualificationPass -eq 1) {
    @('previous', 'candidate')
}
else {
    @('candidate', 'previous')
}

if ($PreflightOnly) {
    $preflightPath = Join-Path $outputRoot 'previous-release-performance-preflight.md'
    $preflight = @(
        '# Previous-release performance preflight',
        '',
        '- Result: **PASS**',
        "- Qualification pass: $QualificationPass",
        "- Run order: $($runOrder -join ' then ')",
        "- Previous ref: ``$PreviousRef`` (``$previousCommit``)",
        "- Candidate ref: ``$CandidateRef`` (``$candidateCommit``)",
        "- Output root: ``$outputRoot``"
    )
    [IO.File]::WriteAllLines($preflightPath, $preflight)
    Write-Host 'Previous-release performance preflight passed.'
    Write-Host "Evidence: $preflightPath"
    return
}

$baselineWorktree = Join-Path $outputRoot 'baseline-source'
$candidateWorktree = Join-Path $outputRoot 'candidate-source'
$baselineResults = Join-Path $outputRoot 'baseline-results'
$candidateResults = Join-Path $outputRoot 'candidate-results'
$logRoot = Join-Path $outputRoot 'logs'
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

function Invoke-ReleaseCore {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $Destination,

        [Parameter(Mandatory)]
        [string] $RunName
    )

    $project = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'CSharpDB.Benchmarks.csproj')
    $logPath = Join-Path $logRoot "$RunName.log"
    Push-Location $SourceRoot
    try {
        & dotnet run -c Release --project $project -- --release-core --repeat $RepeatCount --repro 2>&1 |
            Tee-Object -FilePath $logPath |
            Write-Host
        $benchmarkExitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    if ($benchmarkExitCode -ne 0) {
        throw "Release-core benchmark failed in '$SourceRoot'."
    }

    $resultRoot = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    if (-not (Test-Path -LiteralPath $resultRoot -PathType Container)) {
        throw "Release-core benchmark output directory not found: $resultRoot"
    }
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $suiteNames = @(
        'master-table',
        'durable-sql-batching',
        'concurrent-write-diagnostics',
        'hybrid-storage-mode',
        'hybrid-hot-set-read',
        'hybrid-cold-open',
        'sqlite-compare'
    )
    foreach ($suiteName in $suiteNames) {
        $result = Get-ChildItem `
            -LiteralPath $resultRoot `
            -File `
            -Recurse `
            -Filter "$suiteName-*-median-of-$RepeatCount.csv" |
                Sort-Object LastWriteTimeUtc, FullName |
                Select-Object -Last 1
        if ($null -eq $result) {
            throw "Release-core suite '$suiteName' did not produce a median CSV."
        }
        Copy-Item -LiteralPath $result.FullName -Destination (Join-Path $Destination "$suiteName.csv")
    }
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

    foreach ($run in $runOrder) {
        if ($run -eq 'previous') {
            Invoke-ReleaseCore `
                -SourceRoot $baselineWorktree `
                -Destination $baselineResults `
                -RunName 'previous-release'
        }
        else {
            Invoke-ReleaseCore `
                -SourceRoot $candidateWorktree `
                -Destination $candidateResults `
                -RunName 'candidate'
        }
    }

    try {
        & $comparisonScript `
            -BaselineResultsPath $baselineResults `
            -CandidateResultsPath $candidateResults `
            -ReportPath $reportPath `
            -MaxThroughputRegressionPercent $MaxThroughputRegressionPercent `
            -MaxP99RegressionPercent $MaxP99RegressionPercent
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
                "- Run order: $($runOrder -join ' then ')",
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
