#requires -Version 7.0

[CmdletBinding()]
param(
    [string] $PreviousRef = '',

    [string] $CandidateRef = 'HEAD',

    [string] $OutputPath = '',

    [ValidateSet(1, 2)]
    [int] $QualificationPass = 1,

    [ValidateSet(3, 5, 7, 9)]
    [int] $RepeatCount = 3,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

    [ValidateRange(0, 1000)]
    [double] $MaxP99RegressionMilliseconds = 0.05,

    [switch] $Paired,

    [string[]] $SuiteName = @(),

    [switch] $AllowSameRevision,

    [string] $HybridStorageScenarioName = '',

    [switch] $ShareSameRevisionArtifact,

    [ValidateRange(0, 3600)]
    [int] $PostBuildQuiescenceSeconds = 0,

    [switch] $PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

[string[]] $supportedHybridStorageScenarioNames = @(
    foreach ($prefix in @(
            'Storage_FileBacked',
            'Storage_InMemory',
            'Storage_HybridIncrementalDurable')) {
        "${prefix}_Sql_SingleInsert_5s"
        "${prefix}_Sql_Batch100_5s"
        "${prefix}_Sql_PointLookup_20000"
        "${prefix}_Sql_ConcurrentReads_8readers"
        "${prefix}_Sql_ConcurrentReadsBurst32_8readers"
        "${prefix}_Collection_Put_5s"
        "${prefix}_Collection_Batch100_5s"
        "${prefix}_Collection_Get_20000"
    }
    'StoragePlan2_FileBackedDurableWriteOptimized_InsertBatch_B1000_Seed20000_10s'
    'StoragePlan2_FileBackedDurableLowLatency_InsertBatch_B1000_Seed20000_10s'
    'StoragePlan2_FileBackedBufferedWriteOptimized_InsertBatch_B1000_Seed20000_10s'
    'StoragePlan2_InMemoryFresh_InsertBatch_B1000_Seed20000_10s'
    'StoragePlan2_LoadIntoMemory_InsertBatch_B1000_Seed20000_10s'
    'StoragePlan2_HybridIncrementalDurable_InsertBatch_B1000_Seed20000_10s'
)
$hasHybridStorageScenario =
    $PSBoundParameters.ContainsKey('HybridStorageScenarioName')
if ($hasHybridStorageScenario -and
    [string]::IsNullOrWhiteSpace($HybridStorageScenarioName)) {
    throw 'HybridStorageScenarioName must be a non-empty exact benchmark row name.'
}
if ($hasHybridStorageScenario -and -not $Paired) {
    throw 'HybridStorageScenarioName is valid only with -Paired.'
}
if ($hasHybridStorageScenario -and
    [Array]::IndexOf[string](
        $supportedHybridStorageScenarioNames,
        $HybridStorageScenarioName) -lt 0) {
    throw (
        "Unknown hybrid storage scenario '$HybridStorageScenarioName'. " +
        'Use an exact row name from HybridStorageModeBenchmark.ScenarioNames.')
}
if ($ShareSameRevisionArtifact -and -not $Paired) {
    throw 'ShareSameRevisionArtifact is valid only with -Paired.'
}
if ($ShareSameRevisionArtifact -and -not $AllowSameRevision) {
    throw 'ShareSameRevisionArtifact requires -AllowSameRevision.'
}

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
if ($previousCommit -eq $candidateCommit -and -not $AllowSameRevision) {
    throw 'Previous and candidate refs resolve to the same commit.'
}
if ($ShareSameRevisionArtifact -and $previousCommit -cne $candidateCommit) {
    throw (
        'ShareSameRevisionArtifact requires previous and candidate refs ' +
        'to resolve to the same commit.')
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
    [pscustomobject]@{ Name = 'master-table'; Arguments = @('--master-table'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'durable-sql-batching'; Arguments = @('--durable-sql-batching'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'concurrent-write-diagnostics'; Arguments = @('--concurrent-write-diagnostics'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-storage-mode'; Arguments = @('--hybrid-storage-mode'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-hot-set-read'; Arguments = @('--hybrid-hot-set-read'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-cold-open'; Arguments = @('--hybrid-cold-open'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'sqlite-compare'; Arguments = @('--sqlite-compare'); ExpectedRowName = $null }
)
if ($hasHybridStorageScenario) {
    $suiteDefinitions = @(
        [pscustomobject]@{
            Name = 'hybrid-storage-mode-scenario'
            Arguments = @(
                '--hybrid-storage-mode-scenario',
                $HybridStorageScenarioName)
            ExpectedRowName = $HybridStorageScenarioName
        }
    )
}
elseif ($SuiteName.Count -gt 0) {
    $requestedSuites = @(
        $SuiteName |
            ForEach-Object { $_.Trim().ToLowerInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )
    $unknownSuites = @(
        $requestedSuites | Where-Object { $_ -cnotin $suiteDefinitions.Name }
    )
    if ($unknownSuites.Count -gt 0) {
        throw (
            "Unknown release-core suite name(s): $($unknownSuites -join ', '). " +
            "Supported suites: $($suiteDefinitions.Name -join ', ').")
    }
    $suiteDefinitions = @(
        $suiteDefinitions | Where-Object { $_.Name -cin $requestedSuites }
    )
}
if ($suiteDefinitions.Count -eq 0) {
    throw 'At least one release-core suite must be selected.'
}

$pairDefinitions = @()
if ($Paired) {
    $totalPairCount = 2 * $RepeatCount
    $pairDefinitions = @(
        for ($pairIndex = 1; $pairIndex -le $totalPairCount; $pairIndex++) {
            $useStartingOrder = ($pairIndex % 2) -eq 1
            $pairOrder = if ($useStartingOrder) {
                $revisionOrder
            }
            else {
                @($revisionOrder[1], $revisionOrder[0])
            }
            [pscustomobject]@{
                Number = $pairIndex
                Id = "pair-$($pairIndex.ToString('00', [Globalization.CultureInfo]::InvariantCulture))"
                FirstRevision = $pairOrder[0]
                SecondRevision = $pairOrder[1]
                Order = "$($pairOrder[0])-$($pairOrder[1])"
            }
        }
    )
}

$executionPlan = @(
    foreach ($suite in $suiteDefinitions) {
        if ($Paired) {
            foreach ($pair in $pairDefinitions) {
                foreach ($position in 0, 1) {
                    [pscustomobject]@{
                        Suite = $suite
                        Revision = if ($position -eq 0) {
                            $pair.FirstRevision
                        }
                        else {
                            $pair.SecondRevision
                        }
                        Pair = $pair
                        Position = $position + 1
                    }
                }
            }
        }
        else {
            foreach ($revision in $revisionOrder) {
                [pscustomobject]@{
                    Suite = $suite
                    Revision = $revision
                    Pair = $null
                    Position = 0
                }
            }
        }
    }
)
$suiteOrder = ($suiteDefinitions.Name -join ', ')
$executionOrder = (
    $executionPlan |
        ForEach-Object {
            if ($Paired) {
                "$($_.Suite.Name)/$($_.Pair.Id)/$($_.Revision)"
            }
            else {
                "$($_.Suite.Name)/$($_.Revision)"
            }
        }
) -join ', '
$p99AbsoluteAllowance = $MaxP99RegressionMilliseconds.ToString(
    '0.0000',
    [Globalization.CultureInfo]::InvariantCulture)
$baselineWorktree = Join-Path $outputRoot 'baseline-source'
$candidateWorktree = Join-Path $outputRoot 'candidate-source'
$candidateArtifactSearchRoot = [IO.Path]::Combine(
    $candidateWorktree,
    'tests',
    'CSharpDB.Benchmarks',
    'bin',
    'Release')
$logRoot = Join-Path $outputRoot 'logs'
$executionLogPath = Join-Path $logRoot 'execution-order.log'
$pairManifestPath = Join-Path $logRoot 'paired-execution.csv'
$pairedRawDigestManifestPath = Join-Path $logRoot 'paired-raw-evidence.sha256'
$harnessManifestPath = Join-Path $logRoot 'candidate-benchmark-harness.sha256'
$candidateBuildInputsManifestPath = Join-Path $logRoot 'candidate-effective-build-inputs.sha256'
$previousBuildInputsManifestPath = Join-Path $logRoot 'previous-effective-build-inputs.sha256'
$executionStrategy = if ($Paired) {
    'balanced-paired-raw-repeats'
}
else {
    'suite-interleaved'
}
$revisionOrderDescription = if ($Paired) {
    "$($revisionOrder -join ' then ') for the first pair, alternating thereafter"
}
else {
    $revisionOrder -join ' then '
}
$repeatDescription = if ($Paired) {
    "- Paired repeats per order: $RepeatCount (total pairs per suite: $($pairDefinitions.Count); " +
        "recorded samples per revision: $($pairDefinitions.Count))"
}
else {
    "- Repeat count: $RepeatCount"
}
$artifactSharingDescription = if ($ShareSameRevisionArtifact) {
    '- Same-revision artifact sharing: enabled; both logical revisions will ' +
        "invoke the candidate worktree ``$candidateWorktree``"
}
else {
    '- Same-revision artifact sharing: disabled'
}
$quiescenceDescription = if ($PostBuildQuiescenceSeconds -gt 0) {
    '- Post-build quiescence: dotnet build servers will be shut down, then ' +
        "measurements will wait $PostBuildQuiescenceSeconds second(s)"
}
else {
    '- Post-build quiescence: disabled'
}

$preflightPath = Join-Path $outputRoot 'previous-release-performance-preflight.md'
$preflight = @(
    '# Previous-release performance preflight',
    '',
    '- Result: **PASS**',
    "- Qualification pass: $QualificationPass",
    "- Execution strategy: $executionStrategy",
    "- Revision order within each suite: $revisionOrderDescription",
    "- Suite order: $suiteOrder",
    $(if ($hasHybridStorageScenario) {
            "- Hybrid storage scenario: ``$HybridStorageScenarioName``"
        }),
    "- Execution order: $executionOrder",
    $repeatDescription,
    $artifactSharingDescription,
    $(if ($ShareSameRevisionArtifact) {
            "- Planned shared artifact search root: ``$candidateArtifactSearchRoot``"
        }),
    $quiescenceDescription,
    "- Throughput regression limit: $MaxThroughputRegressionPercent%",
    "- P99 regression limit: $MaxP99RegressionPercent%",
    "- P99 absolute regression allowance: $p99AbsoluteAllowance ms",
    '- P99 failure rule: relative and absolute limits must both be exceeded',
    '- Benchmark source harness: candidate benchmark-project files synchronized to both engines; revision-specific effective build inputs are recorded separately during execution',
    "- Previous ref: ``$PreviousRef`` (``$previousCommit``)",
    "- Candidate ref: ``$CandidateRef`` (``$candidateCommit``)",
    "- Planned execution log: ``$executionLogPath``",
    $(if ($Paired) { "- Planned pair manifest: ``$pairManifestPath``" }),
    $(if ($Paired) {
            "- Planned paired raw SHA-256 manifest: ``$pairedRawDigestManifestPath``"
        }),
    "- Output root: ``$outputRoot``"
)
[IO.File]::WriteAllLines($preflightPath, $preflight)
Write-Host 'Previous-release performance preflight passed.'
Write-Host "Evidence: $preflightPath"
if ($PreflightOnly) {
    return
}

$baselineResults = Join-Path $outputRoot 'baseline-results'
$candidateResults = Join-Path $outputRoot 'candidate-results'
$baselineRawResults = Join-Path $baselineResults 'raw'
$candidateRawResults = Join-Path $candidateResults 'raw'
$reportPath = Join-Path $outputRoot 'previous-release-performance.md'
$baselineAdded = $false
$candidateAdded = $false
$candidateHarnessIdentity = ''
$candidateBuildInputsIdentity = ''
$previousBuildInputsIdentity = ''
$sharedArtifactPath = ''
$sharedArtifactSha256 = ''
$pairedRawDigestCount = 0
$primaryFailure = $null
$cleanupFailures = [Collections.Generic.List[string]]::new()
$originalNuGetPackages = $env:NUGET_PACKAGES
$originalDotnetCliHome = $env:DOTNET_CLI_HOME

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

function Get-BenchmarkHarnessFileRecords {
    param(
        [Parameter(Mandatory)]
        [string] $HarnessRoot
    )

    $resolvedHarnessRoot = [IO.Path]::GetFullPath($HarnessRoot)
    if (-not (Test-Path -LiteralPath $resolvedHarnessRoot -PathType Container)) {
        throw "Benchmark harness directory not found: $resolvedHarnessRoot"
    }

    $records = @(
        Get-ChildItem -LiteralPath $resolvedHarnessRoot -File -Recurse -Force |
            Where-Object {
                $relativePath = [IO.Path]::GetRelativePath(
                    $resolvedHarnessRoot,
                    $_.FullName)
                $extension = [IO.Path]::GetExtension($_.Name)
                $relativePath -notmatch `
                    '(^|[\\/])(artifacts|baselines|bin|obj|results|run-logs)([\\/]|$)' -and
                    $extension -cin @('.cs', '.csproj', '.json')
            } |
            ForEach-Object {
                if (($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw "Benchmark harness file cannot be a reparse point: $($_.FullName)"
                }

                $relativePath = [IO.Path]::GetRelativePath(
                    $resolvedHarnessRoot,
                    $_.FullName).
                        Replace('\', '/')
                [pscustomobject]@{
                    RelativePath = $relativePath
                    FullName = $_.FullName
                    Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).
                        Hash.
                        ToLowerInvariant()
                }
            } |
            Sort-Object -CaseSensitive RelativePath
    )

    if ($records.Count -eq 0) {
        throw "Benchmark harness contains no project, source, or configuration files: $resolvedHarnessRoot"
    }
    foreach ($requiredFile in @('CSharpDB.Benchmarks.csproj', 'Program.cs')) {
        if ($requiredFile -cnotin $records.RelativePath) {
            throw "Benchmark harness is missing required file '$requiredFile': $resolvedHarnessRoot"
        }
    }

    return $records
}

function Get-BenchmarkHarnessIdentity {
    param(
        [Parameter(Mandatory)]
        [object[]] $Records
    )

    $manifestLines = @(
        $Records | ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    $manifestPayload = ($manifestLines -join "`n") + "`n"
    $identityBytes = [Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes($manifestPayload))
    return [Convert]::ToHexString($identityBytes).ToLowerInvariant()
}

function Get-EffectiveBuildInputFileRecords {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot
    )

    $effectiveInputNames = @(
        'Directory.Build.props',
        'Directory.Build.targets',
        'Directory.Packages.props',
        'global.json',
        'NuGet.config')
    return @(
        Get-ChildItem -LiteralPath $SourceRoot -File -Force |
            Where-Object { $_.Name -iin $effectiveInputNames } |
            ForEach-Object {
                if (($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw "Effective build input cannot be a reparse point: $($_.FullName)"
                }
                [pscustomobject]@{
                    RelativePath = $_.Name
                    FullName = $_.FullName
                    Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).
                        Hash.
                        ToLowerInvariant()
                }
            } |
            Sort-Object -CaseSensitive RelativePath
    )
}

function Write-EffectiveBuildInputManifest {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $Commit,

        [Parameter(Mandatory)]
        [string] $Description,

        [Parameter(Mandatory)]
        [string] $ManifestPath
    )

    $records = @(Get-EffectiveBuildInputFileRecords -SourceRoot $SourceRoot)
    $identity = if ($records.Count -eq 0) {
        [Convert]::ToHexString(
            [Security.Cryptography.SHA256]::HashData([byte[]]::new(0))).
                ToLowerInvariant()
    }
    else {
        Get-BenchmarkHarnessIdentity -Records $records
    }
    $manifestLines = @(
        "# $Description effective build inputs",
        "Commit=$Commit",
        "BuildInputsSha256=$identity",
        "FileCount=$($records.Count)",
        ''
        $records | ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    [IO.File]::WriteAllLines($ManifestPath, $manifestLines)
    return $identity
}

function Sync-CandidateBenchmarkHarness {
    param(
        [Parameter(Mandatory)]
        [string] $CandidateSourceRoot,

        [Parameter(Mandatory)]
        [string] $PreviousSourceRoot
    )

    $candidateHarnessRoot = [IO.Path]::GetFullPath(
        [IO.Path]::Combine($CandidateSourceRoot, 'tests', 'CSharpDB.Benchmarks'))
    $previousHarnessRoot = [IO.Path]::GetFullPath(
        [IO.Path]::Combine($PreviousSourceRoot, 'tests', 'CSharpDB.Benchmarks'))
    $candidateSourcePrefix = [IO.Path]::GetFullPath($CandidateSourceRoot).
        TrimEnd(
            [IO.Path]::DirectorySeparatorChar,
            [IO.Path]::AltDirectorySeparatorChar) +
        [IO.Path]::DirectorySeparatorChar
    $previousSourcePrefix = [IO.Path]::GetFullPath($PreviousSourceRoot).
        TrimEnd(
            [IO.Path]::DirectorySeparatorChar,
            [IO.Path]::AltDirectorySeparatorChar) +
        [IO.Path]::DirectorySeparatorChar
    if (-not $candidateHarnessRoot.StartsWith($candidateSourcePrefix, $pathComparison) -or
        -not $previousHarnessRoot.StartsWith($previousSourcePrefix, $pathComparison)) {
        throw 'Benchmark harness synchronization resolved outside its detached worktree.'
    }

    $candidateRecords = @(
        Get-BenchmarkHarnessFileRecords -HarnessRoot $candidateHarnessRoot
    )
    $candidateIdentity = Get-BenchmarkHarnessIdentity -Records $candidateRecords

    $previousRecords = @(
        Get-BenchmarkHarnessFileRecords -HarnessRoot $previousHarnessRoot
    )
    foreach ($record in $previousRecords) {
        Remove-Item -LiteralPath $record.FullName -Force
    }

    foreach ($record in $candidateRecords) {
        $destinationPath = [IO.Path]::GetFullPath(
            [IO.Path]::Combine(
                $previousHarnessRoot,
                $record.RelativePath.Replace('/', [IO.Path]::DirectorySeparatorChar)))
        $previousHarnessPrefix = $previousHarnessRoot.TrimEnd(
            [IO.Path]::DirectorySeparatorChar,
            [IO.Path]::AltDirectorySeparatorChar) +
            [IO.Path]::DirectorySeparatorChar
        if (-not $destinationPath.StartsWith($previousHarnessPrefix, $pathComparison)) {
            throw "Benchmark harness destination resolved outside the target directory: $destinationPath"
        }

        $destinationDirectory = Split-Path -Parent $destinationPath
        New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
        Copy-Item -LiteralPath $record.FullName -Destination $destinationPath
    }

    $copiedRecords = @(
        Get-BenchmarkHarnessFileRecords -HarnessRoot $previousHarnessRoot
    )
    $candidateManifest = @(
        $candidateRecords | ForEach-Object { "$($_.RelativePath)|$($_.Sha256)" }
    )
    $copiedManifest = @(
        $copiedRecords | ForEach-Object { "$($_.RelativePath)|$($_.Sha256)" }
    )
    $copyDifferences = @(
        Compare-Object `
            -ReferenceObject $candidateManifest `
            -DifferenceObject $copiedManifest `
            -CaseSensitive
    )
    $copiedIdentity = Get-BenchmarkHarnessIdentity -Records $copiedRecords
    if ($copyDifferences.Count -ne 0 -or
        $copiedIdentity -cne $candidateIdentity) {
        $differenceSummary = @(
            $copyDifferences |
                Select-Object -First 10 |
                ForEach-Object { "$($_.InputObject) $($_.SideIndicator)" }
        ) -join '; '
        throw (
            'Candidate benchmark harness synchronization did not reproduce ' +
            "the source manifest exactly. Differences: $differenceSummary")
    }

    $manifestLines = @(
        '# Candidate benchmark harness identity',
        "CandidateCommit=$candidateCommit",
        "HarnessSha256=$candidateIdentity",
        "FileCount=$($candidateRecords.Count)",
        ''
        $candidateRecords | ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    [IO.File]::WriteAllLines($harnessManifestPath, $manifestLines)
    return $candidateIdentity
}

function Invoke-BenchmarkBuild {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $RunName,

        [Parameter(Mandatory)]
        [string] $HarnessIdentity,

        [Parameter(Mandatory)]
        [string] $BuildInputsIdentity,

        [Parameter(Mandatory)]
        [string] $BuildInputsManifestPath
    )

    $project = Get-BenchmarkProject -SourceRoot $SourceRoot
    $logPath = Join-Path $logRoot "$RunName.log"
    [IO.File]::WriteAllLines(
        $logPath,
        @(
            "=== BUILD $RunName ===",
            "Project: $project",
            "Synchronized benchmark source harness SHA-256: $HarnessIdentity",
            "Benchmark source harness manifest: $harnessManifestPath",
            "Revision effective build inputs: $BuildInputsIdentity",
            "Revision effective build inputs manifest: $BuildInputsManifestPath",
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

function Get-BenchmarkArtifactIdentity {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot
    )

    $artifactRoot = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    if (-not (Test-Path -LiteralPath $artifactRoot -PathType Container)) {
        throw "Shared benchmark artifact directory not found: $artifactRoot"
    }
    $artifacts = @(
        Get-ChildItem `
            -LiteralPath $artifactRoot `
            -File `
            -Recurse `
            -Filter 'CSharpDB.Benchmarks.dll' |
            Where-Object {
                $relativePath = [IO.Path]::GetRelativePath(
                    $artifactRoot,
                    $_.FullName)
                $relativePath -notmatch '(^|[\\/])(ref|refint)([\\/]|$)'
            }
    )
    if ($artifacts.Count -ne 1) {
        throw (
            "Shared benchmark build produced $($artifacts.Count) runnable " +
            "CSharpDB.Benchmarks.dll artifact(s) under '$artifactRoot'; expected one.")
    }

    return [pscustomobject]@{
        Path = [IO.Path]::GetFullPath($artifacts[0].FullName)
        Sha256 = (Get-FileHash `
                -LiteralPath $artifacts[0].FullName `
                -Algorithm SHA256).
            Hash.
            ToLowerInvariant()
    }
}

function Invoke-PostBuildQuiescence {
    if ($PostBuildQuiescenceSeconds -le 0) {
        return
    }

    $logPath = Join-Path $logRoot 'post-build-quiescence.log'
    [IO.File]::WriteAllLines(
        $logPath,
        @(
            '=== POST-BUILD QUIESCENCE ===',
            'Command: dotnet build-server shutdown',
            "WaitSeconds: $PostBuildQuiescenceSeconds",
            ''
        ))
    & dotnet build-server shutdown 2>&1 |
        Tee-Object -FilePath $logPath -Append |
        Write-Host
    $shutdownExitCode = $LASTEXITCODE
    if ($shutdownExitCode -ne 0) {
        throw 'Could not shut down dotnet build servers before benchmark measurements.'
    }

    Write-Host (
        "Waiting $PostBuildQuiescenceSeconds second(s) after shutting down " +
        'dotnet build servers.')
    Start-Sleep -Seconds $PostBuildQuiescenceSeconds
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
    $nativeArguments = [string[]] @($Suite.Arguments)
    $logPath = Join-Path $logRoot "$RunName.log"
    $resultRoot = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    $resultPattern = "$($Suite.Name)-*-median-of-$RepeatCount.csv"
    $rawResultPattern = "$($Suite.Name)-*-run*.csv"
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
    $existingRawResults = @(
        if (Test-Path -LiteralPath $resultRoot -PathType Container) {
            Get-ChildItem `
                -LiteralPath $resultRoot `
                -File `
                -Recurse `
                -Filter $rawResultPattern
        }
    )
    if ($existingRawResults.Count -ne 0) {
        throw (
            "Release-core suite '$($Suite.Name)' found " +
            "$($existingRawResults.Count) pre-existing raw CSV file(s) in " +
            "'$resultRoot'.")
    }

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $destinationPath = Join-Path $Destination "$($Suite.Name).csv"
    $rawDestinationPath = Join-Path (Join-Path $Destination 'raw') $Suite.Name
    if (Test-Path -LiteralPath $destinationPath) {
        throw "Release-core destination already exists: $destinationPath"
    }
    if (Test-Path -LiteralPath $rawDestinationPath) {
        throw "Release-core raw destination already exists: $rawDestinationPath"
    }

    Add-Content `
        -LiteralPath $logPath `
        -Value @(
            '',
            "=== SUITE $($Suite.Name) / $RunName ===",
            "Arguments: $($nativeArguments -join ' ')",
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
            @nativeArguments `
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
    $medianResult = $results[0]
    if ($medianResult.Length -eq 0) {
        throw "Release-core suite '$($Suite.Name)' produced an empty median CSV."
    }

    $medianNamePattern = '^' +
        [regex]::Escape($Suite.Name) +
        '-(?<stamp>.+)-median-of-' +
        $RepeatCount +
        '\.csv$'
    $medianNameMatch = [regex]::Match(
        $medianResult.Name,
        $medianNamePattern,
        [Text.RegularExpressions.RegexOptions]::CultureInvariant)
    if (-not $medianNameMatch.Success) {
        throw (
            "Release-core suite '$($Suite.Name)' produced median CSV " +
            "with an invalid name: $($medianResult.Name)")
    }
    $runStamp = $medianNameMatch.Groups['stamp'].Value

    $rawResults = @(
        Get-ChildItem `
            -LiteralPath $resultRoot `
            -File `
            -Recurse `
            -Filter $rawResultPattern
    )
    if ($rawResults.Count -ne $RepeatCount) {
        throw (
            "Release-core suite '$($Suite.Name)' produced " +
            "$($rawResults.Count) raw CSV file(s); expected exactly " +
            "$RepeatCount sharing median timestamp '$runStamp'.")
    }

    $orderedRawResults = [Collections.Generic.List[IO.FileInfo]]::new()
    for ($runIndex = 1; $runIndex -le $RepeatCount; $runIndex++) {
        $expectedRawName = "$($Suite.Name)-$runStamp-run$runIndex.csv"
        $matchingRawResults = @(
            $rawResults | Where-Object Name -CEQ $expectedRawName
        )
        if ($matchingRawResults.Count -ne 1) {
            throw (
                "Release-core suite '$($Suite.Name)' produced " +
                "$($matchingRawResults.Count) raw CSV file(s) named " +
                "'$expectedRawName'; expected exactly one.")
        }
        $rawResult = $matchingRawResults[0]
        if (-not $rawResult.DirectoryName.Equals(
            $medianResult.DirectoryName,
            $pathComparison)) {
            throw (
                "Release-core suite '$($Suite.Name)' raw and median CSV " +
                'files were written to different output directories.')
        }
        if ($rawResult.Length -eq 0) {
            throw (
                "Release-core suite '$($Suite.Name)' produced an empty " +
                "raw CSV for run $runIndex.")
        }
        $orderedRawResults.Add($rawResult)
    }

    Copy-Item -LiteralPath $medianResult.FullName -Destination $destinationPath
    $sourceMedianHash = (Get-FileHash -LiteralPath $medianResult.FullName -Algorithm SHA256).Hash
    $copiedMedianHash = (Get-FileHash -LiteralPath $destinationPath -Algorithm SHA256).Hash
    if ($copiedMedianHash -cne $sourceMedianHash) {
        throw "Release-core median evidence copy verification failed: $destinationPath"
    }

    New-Item -ItemType Directory -Path $rawDestinationPath -Force | Out-Null
    for ($rawIndex = 0; $rawIndex -lt $orderedRawResults.Count; $rawIndex++) {
        $rawResult = $orderedRawResults[$rawIndex]
        $rawDestinationFile = Join-Path `
            $rawDestinationPath `
            "run-$($rawIndex + 1).csv"
        Copy-Item -LiteralPath $rawResult.FullName -Destination $rawDestinationFile
        $sourceRawHash = (Get-FileHash -LiteralPath $rawResult.FullName -Algorithm SHA256).Hash
        $copiedRawHash = (Get-FileHash -LiteralPath $rawDestinationFile -Algorithm SHA256).Hash
        if ($copiedRawHash -cne $sourceRawHash) {
            throw "Release-core raw evidence copy verification failed: $rawDestinationFile"
        }
    }
    $copiedRawResults = @(
        Get-ChildItem -LiteralPath $rawDestinationPath -File -Filter '*.csv'
    )
    if ($copiedRawResults.Count -ne $RepeatCount) {
        throw (
            "Release-core raw evidence destination '$rawDestinationPath' " +
            "contains $($copiedRawResults.Count) CSV file(s); expected " +
            "$RepeatCount.")
    }

    Add-Content `
        -LiteralPath $logPath `
        -Value @(
            "Median evidence: $destinationPath",
            "Raw evidence: $rawDestinationPath",
            "Raw source timestamp: $runStamp"
        )
}

function Convert-ToCsvCell {
    param([AllowNull()][object] $Value)

    $text = if ($null -eq $Value) { '' } else { [string] $Value }
    if ($text.Contains(',') -or $text.Contains('"') -or
        $text.Contains("`r") -or $text.Contains("`n")) {
        return '"' + $text.Replace('"', '""') + '"'
    }
    return $text
}

function Get-PairedRawEvidenceRecord {
    param([Parameter(Mandatory)][string] $RawPath)

    $fullPath = [IO.Path]::GetFullPath($RawPath)
    $relativePath = [IO.Path]::GetRelativePath($outputRoot, $fullPath)
    $parentPrefix = '..' + [IO.Path]::DirectorySeparatorChar
    $alternateParentPrefix = '..' + [IO.Path]::AltDirectorySeparatorChar
    if ([IO.Path]::IsPathFullyQualified($relativePath) -or
        $relativePath -ceq '..' -or
        $relativePath.StartsWith($parentPrefix, [StringComparison]::Ordinal) -or
        $relativePath.StartsWith(
            $alternateParentPrefix,
            [StringComparison]::Ordinal)) {
        throw "Paired raw evidence resolves outside the output root: $fullPath"
    }
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "Paired raw evidence is missing: $fullPath"
    }

    $normalizedRelativePath = $relativePath.Replace('\', '/')
    if ($normalizedRelativePath.Contains("`r") -or
        $normalizedRelativePath.Contains("`n")) {
        throw "Paired raw evidence path cannot contain a line break: $fullPath"
    }
    return [pscustomobject]@{
        FullPath = $fullPath
        RelativePath = $normalizedRelativePath
        Sha256 = (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).
            Hash.
            ToLowerInvariant()
    }
}

function Write-LinesAtomically {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $Lines
    )

    $temporaryPath = "$Path.$([Guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllLines($temporaryPath, $Lines)
        [IO.File]::Move($temporaryPath, $Path, $true)
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
}

function Add-PairedRawEvidenceDigest {
    param([Parameter(Mandatory)][string] $RawPath)

    $record = Get-PairedRawEvidenceRecord -RawPath $RawPath
    $existingLines = if (Test-Path -LiteralPath $pairedRawDigestManifestPath -PathType Leaf) {
        [IO.File]::ReadAllLines($pairedRawDigestManifestPath)
    }
    else {
        [string[]] @()
    }
    $digestPattern = '^(?<hash>[0-9a-f]{64}) \*(?<path>.+)$'
    $pathComparer = if ($IsWindows) {
        [StringComparer]::OrdinalIgnoreCase
    }
    else {
        [StringComparer]::Ordinal
    }
    $seenPaths = [Collections.Generic.HashSet[string]]::new($pathComparer)
    foreach ($line in $existingLines) {
        $match = [regex]::Match(
            $line,
            $digestPattern,
            [Text.RegularExpressions.RegexOptions]::CultureInvariant)
        if (-not $match.Success) {
            throw "Paired raw SHA-256 manifest contains an invalid line: $line"
        }
        if (-not $seenPaths.Add($match.Groups['path'].Value)) {
            throw (
                'Paired raw SHA-256 manifest contains a duplicate path: ' +
                $match.Groups['path'].Value)
        }
    }
    if (-not $seenPaths.Add($record.RelativePath)) {
        throw "Paired raw evidence digest already exists: $($record.RelativePath)"
    }

    $updatedLines = [string[]] @(
        (@($existingLines) + "$($record.Sha256) *$($record.RelativePath)") |
            Sort-Object -CaseSensitive
    )
    Write-LinesAtomically -Path $pairedRawDigestManifestPath -Lines $updatedLines
    $persistedLines = [IO.File]::ReadAllLines($pairedRawDigestManifestPath)
    if ($persistedLines.Count -ne $updatedLines.Count) {
        throw 'Paired raw SHA-256 manifest persistence verification failed.'
    }
    for ($lineIndex = 0; $lineIndex -lt $updatedLines.Count; $lineIndex++) {
        if ($persistedLines[$lineIndex] -cne $updatedLines[$lineIndex]) {
            throw 'Paired raw SHA-256 manifest persistence verification failed.'
        }
    }
    $verifiedHash = (Get-FileHash -LiteralPath $record.FullPath -Algorithm SHA256).
        Hash.
        ToLowerInvariant()
    if ($verifiedHash -cne $record.Sha256) {
        throw "Paired raw evidence changed during digest persistence: $($record.FullPath)"
    }
}

function Assert-PairedRawEvidenceDigestManifest {
    $manifestRows = @(Import-Csv -LiteralPath $pairManifestPath)
    if ($manifestRows.Count -eq 0) {
        throw "Paired execution manifest contains no pairs: $pairManifestPath"
    }
    $digestLines = [IO.File]::ReadAllLines($pairedRawDigestManifestPath)
    $digestPattern = '^(?<hash>[0-9a-f]{64}) \*(?<path>.+)$'
    $pathComparer = if ($IsWindows) {
        [StringComparer]::OrdinalIgnoreCase
    }
    else {
        [StringComparer]::Ordinal
    }
    $digestByPath = [Collections.Generic.Dictionary[string, string]]::new(
        $pathComparer)
    foreach ($line in $digestLines) {
        $match = [regex]::Match(
            $line,
            $digestPattern,
            [Text.RegularExpressions.RegexOptions]::CultureInvariant)
        if (-not $match.Success -or
            -not $digestByPath.TryAdd(
                $match.Groups['path'].Value,
                $match.Groups['hash'].Value)) {
            throw "Paired raw SHA-256 manifest contains an invalid or duplicate line: $line"
        }
    }

    $expectedPaths = [Collections.Generic.HashSet[string]]::new($pathComparer)
    foreach ($manifestRow in $manifestRows) {
        foreach ($column in @('BaselineRaw', 'CandidateRaw')) {
            $rawValue = [string] $manifestRow.$column
            if ([string]::IsNullOrWhiteSpace($rawValue)) {
                throw (
                    "Paired execution manifest $($manifestRow.Suite)/" +
                    "$($manifestRow.PairId) has an empty $column path.")
            }
            $record = Get-PairedRawEvidenceRecord -RawPath $rawValue
            if (-not $expectedPaths.Add($record.RelativePath)) {
                throw "Paired raw evidence is referenced more than once: $($record.FullPath)"
            }
            [string] $persistedHash = ''
            if (-not $digestByPath.TryGetValue($record.RelativePath, [ref] $persistedHash) -or
                $persistedHash -cne $record.Sha256) {
                throw (
                    'Paired raw SHA-256 manifest does not match raw evidence: ' +
                    $record.RelativePath)
            }
        }
    }

    if ($digestByPath.Count -ne $expectedPaths.Count) {
        throw (
            "Paired raw SHA-256 manifest covers $($digestByPath.Count) files; " +
            "the pair manifest references $($expectedPaths.Count).")
    }
    return $digestByPath.Count
}

function Convert-ToInvariantDecimal {
    param(
        [Parameter(Mandatory)]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [decimal] $parsed = 0
    if (-not [decimal]::TryParse(
            $Value,
            [Globalization.NumberStyles]::Float,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref] $parsed)) {
        throw "Benchmark metric '$Description' is not an invariant decimal: '$Value'."
    }
    return $parsed
}

function Get-MedianDecimal {
    param([Parameter(Mandatory)][decimal[]] $Values)

    if ($Values.Count -eq 0) {
        throw 'Cannot calculate a median from an empty value set.'
    }
    $ordered = @($Values | Sort-Object)
    $middle = [int] [Math]::Floor($ordered.Count / 2)
    if (($ordered.Count % 2) -eq 1) {
        return [decimal] $ordered[$middle]
    }
    return ([decimal] $ordered[$middle - 1] + [decimal] $ordered[$middle]) / 2
}

function Write-AggregatedBenchmarkCsv {
    param(
        [Parameter(Mandatory)]
        [string[]] $RawPaths,

        [Parameter(Mandatory)]
        [string] $DestinationPath
    )

    $expectedHeader =
        'Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo'
    $metricColumns = @('P50', 'P90', 'P95', 'P99', 'P999', 'Min', 'Max', 'Mean', 'StdDev')
    $runs = [Collections.Generic.List[object[]]]::new()
    $expectedNames = $null
    foreach ($rawPath in $RawPaths) {
        if (-not (Test-Path -LiteralPath $rawPath -PathType Leaf)) {
            throw "Paired raw evidence is missing: $rawPath"
        }
        $header = Get-Content -LiteralPath $rawPath -TotalCount 1
        if ($header -cne $expectedHeader) {
            throw "Paired raw evidence has an unexpected CSV schema: $rawPath"
        }
        $rows = @(Import-Csv -LiteralPath $rawPath)
        if ($rows.Count -eq 0) {
            throw "Paired raw evidence contains no rows: $rawPath"
        }
        $names = @($rows | ForEach-Object { $_.Name })
        if (@($names | Sort-Object -Unique).Count -ne $names.Count) {
            throw "Paired raw evidence contains duplicate row names: $rawPath"
        }
        if ($null -eq $expectedNames) {
            $expectedNames = $names
        }
        elseif (@(
                Compare-Object $expectedNames $names -CaseSensitive
            ).Count -ne 0) {
            throw "Paired raw evidence row names do not match: $rawPath"
        }
        $runs.Add($rows)
    }

    $lines = [Collections.Generic.List[string]]::new()
    $lines.Add($expectedHeader)
    foreach ($name in $expectedNames) {
        $samples = @(
            for ($runIndex = 0; $runIndex -lt $runs.Count; $runIndex++) {
                $matching = @($runs[$runIndex] | Where-Object Name -CEQ $name)
                if ($matching.Count -ne 1) {
                    throw (
                        "Paired raw evidence run $($runIndex + 1) contains " +
                        "$($matching.Count) rows named '$name'.")
                }
                [pscustomobject]@{
                    Index = $runIndex
                    Row = $matching[0]
                    OpsPerSec = Convert-ToInvariantDecimal `
                        -Value $matching[0].OpsPerSec `
                        -Description "$name/OpsPerSec/run-$($runIndex + 1)"
                }
            }
        )
        $orderedByThroughput = @(
            $samples | Sort-Object OpsPerSec, Index
        )
        $medianThroughputSample = $orderedByThroughput[
            [int] [Math]::Floor($orderedByThroughput.Count / 2)].Row

        $latencySampleMedian = Get-MedianDecimal -Values ([decimal[]] @(
                $samples | ForEach-Object {
                    Convert-ToInvariantDecimal `
                        -Value $_.Row.LatencySamples `
                        -Description "$name/LatencySamples"
                }
            ))
        $values = [ordered]@{
            Name = $name
            TotalOps = $medianThroughputSample.TotalOps
            LatencySamples = ([Math]::Round($latencySampleMedian)).ToString(
                [Globalization.CultureInfo]::InvariantCulture)
            ElapsedMs = $medianThroughputSample.ElapsedMs
            OpsPerSec = $medianThroughputSample.OpsPerSec
        }
        foreach ($column in $metricColumns) {
            $median = Get-MedianDecimal -Values ([decimal[]] @(
                    $samples | ForEach-Object {
                        Convert-ToInvariantDecimal `
                            -Value $_.Row.$column `
                            -Description "$name/$column"
                    }
                ))
            $values[$column] = $median.ToString(
                'F4',
                [Globalization.CultureInfo]::InvariantCulture)
        }
        $aggregateTag = "Aggregate=median-of-$($samples.Count)"
        $sourceInfo = [string] $medianThroughputSample.ExtraInfo
        $values['ExtraInfo'] = if ([string]::IsNullOrWhiteSpace($sourceInfo)) {
            $aggregateTag
        }
        else {
            "$sourceInfo; $aggregateTag"
        }
        $lines.Add((@($values.Values | ForEach-Object { Convert-ToCsvCell $_ }) -join ','))
    }

    $destinationDirectory = Split-Path -Parent $DestinationPath
    New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
    [IO.File]::WriteAllLines($DestinationPath, $lines)
}

function Invoke-ReleaseCoreSample {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot,

        [Parameter(Mandatory)]
        [string] $Destination,

        [Parameter(Mandatory)]
        [string] $RunName,

        [Parameter(Mandatory)]
        [object] $Suite,

        [Parameter(Mandatory)]
        [string] $PairId,

        [string] $ArtifactPath = '',

        [string] $ExpectedArtifactSha256 = ''
    )

    $useDirectArtifact =
        -not [string]::IsNullOrWhiteSpace($ArtifactPath) -or
        -not [string]::IsNullOrWhiteSpace($ExpectedArtifactSha256)
    if ($useDirectArtifact -and
        ([string]::IsNullOrWhiteSpace($ArtifactPath) -or
            [string]::IsNullOrWhiteSpace($ExpectedArtifactSha256))) {
        throw 'Direct benchmark execution requires both an artifact path and SHA-256.'
    }
    if ($useDirectArtifact -and
        $ExpectedArtifactSha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'Direct benchmark execution requires a lowercase SHA-256 identity.'
    }
    $resolvedArtifactPath = if ($useDirectArtifact) {
        [IO.Path]::GetFullPath($ArtifactPath)
    }
    else {
        ''
    }
    if ($useDirectArtifact -and
        -not (Test-Path -LiteralPath $resolvedArtifactPath -PathType Leaf)) {
        throw "Shared benchmark artifact is missing: $resolvedArtifactPath"
    }
    $project = if ($useDirectArtifact) {
        ''
    }
    else {
        Get-BenchmarkProject -SourceRoot $SourceRoot
    }
    $nativeArguments = [string[]] @($Suite.Arguments)
    $logPath = Join-Path $logRoot "$RunName.log"
    $resultRoot = [IO.Path]::Combine(
        $SourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    $resultPattern = "$($Suite.Name)-*.csv"
    $existingResults = @(
        if (Test-Path -LiteralPath $resultRoot -PathType Container) {
            Get-ChildItem -LiteralPath $resultRoot -File -Recurse -Filter $resultPattern
        }
    )
    if ($existingResults.Count -ne 0) {
        throw (
            "Paired release-core suite '$($Suite.Name)' found " +
            "$($existingResults.Count) pre-existing CSV file(s) in '$resultRoot'.")
    }

    $rawDestinationPath = Join-Path (Join-Path $Destination 'raw') $Suite.Name
    New-Item -ItemType Directory -Path $rawDestinationPath -Force | Out-Null
    $destinationPath = Join-Path $rawDestinationPath "$PairId.csv"
    if (Test-Path -LiteralPath $destinationPath) {
        throw "Paired raw destination already exists: $destinationPath"
    }

    Add-Content -LiteralPath $logPath -Value @(
        '',
        "=== PAIRED SAMPLE $($Suite.Name) / $PairId / $RunName ===",
        $(if ($useDirectArtifact) {
                "Direct artifact: $resolvedArtifactPath"
            }
            else {
                "Project: $project"
            }),
        $(if ($useDirectArtifact) {
                "Expected artifact SHA-256: $ExpectedArtifactSha256"
            }),
        "Arguments: $($nativeArguments -join ' ')",
        ''
    )
    Push-Location $SourceRoot
    try {
        if ($useDirectArtifact) {
            $beforeHash = (Get-FileHash `
                    -LiteralPath $resolvedArtifactPath `
                    -Algorithm SHA256).
                Hash.
                ToLowerInvariant()
            if ($beforeHash -cne $ExpectedArtifactSha256) {
                throw (
                    'Shared benchmark artifact changed before invocation. ' +
                    "Expected $ExpectedArtifactSha256; found $beforeHash.")
            }
            try {
                & dotnet $resolvedArtifactPath `
                    @nativeArguments `
                    --repeat 1 `
                    --warmup-single-sample `
                    --repro 2>&1 |
                        Tee-Object -FilePath $logPath -Append |
                        Write-Host
                $benchmarkExitCode = $LASTEXITCODE
            }
            finally {
                $afterHash = (Get-FileHash `
                        -LiteralPath $resolvedArtifactPath `
                        -Algorithm SHA256).
                    Hash.
                    ToLowerInvariant()
                if ($afterHash -cne $ExpectedArtifactSha256) {
                    throw (
                        'Shared benchmark artifact changed during invocation. ' +
                        "Expected $ExpectedArtifactSha256; found $afterHash.")
                }
            }
        }
        else {
            & dotnet run `
                -c Release `
                --no-build `
                --no-restore `
                --project $project `
                -- `
                @nativeArguments `
                --repeat 1 `
                --warmup-single-sample `
                --repro 2>&1 |
                    Tee-Object -FilePath $logPath -Append |
                    Write-Host
            $benchmarkExitCode = $LASTEXITCODE
        }
    }
    finally {
        Pop-Location
    }
    if ($benchmarkExitCode -ne 0) {
        throw "Paired release-core suite '$($Suite.Name)' failed in '$SourceRoot'."
    }

    $results = @(
        if (Test-Path -LiteralPath $resultRoot -PathType Container) {
            Get-ChildItem -LiteralPath $resultRoot -File -Recurse -Filter $resultPattern
        }
    )
    if ($results.Count -ne 1) {
        throw (
            "Paired release-core suite '$($Suite.Name)' produced " +
            "$($results.Count) sample CSV file(s); expected exactly one.")
    }
    if ($results[0].Length -eq 0) {
        throw "Paired release-core suite '$($Suite.Name)' produced an empty sample CSV."
    }
    if (-not [string]::IsNullOrWhiteSpace([string] $Suite.ExpectedRowName)) {
        $scenarioRows = @(Import-Csv -LiteralPath $results[0].FullName)
        if ($scenarioRows.Count -ne 1) {
            throw (
                "Paired release-core scenario '$($Suite.ExpectedRowName)' produced " +
                "$($scenarioRows.Count) CSV data rows; expected exactly one.")
        }
        if (-not [string]::Equals(
                [string] $scenarioRows[0].Name,
                [string] $Suite.ExpectedRowName,
                [StringComparison]::Ordinal)) {
            throw (
                "Paired release-core scenario expected row '$($Suite.ExpectedRowName)' " +
                "but received '$($scenarioRows[0].Name)'.")
        }
    }

    Copy-Item -LiteralPath $results[0].FullName -Destination $destinationPath
    $sourceHash = (Get-FileHash -LiteralPath $results[0].FullName -Algorithm SHA256).Hash
    $copiedHash = (Get-FileHash -LiteralPath $destinationPath -Algorithm SHA256).Hash
    if ($copiedHash -cne $sourceHash) {
        throw "Paired raw evidence copy verification failed: $destinationPath"
    }
    Add-PairedRawEvidenceDigest -RawPath $destinationPath
    Remove-Item -LiteralPath $results[0].FullName -Force
    return [IO.Path]::GetFullPath($destinationPath)
}

try {
    $env:NUGET_PACKAGES = Join-Path $outputRoot '.nuget-packages'
    $env:DOTNET_CLI_HOME = Join-Path $outputRoot '.dotnet-home'
    New-Item -ItemType Directory -Path $env:NUGET_PACKAGES -Force | Out-Null
    New-Item -ItemType Directory -Path $env:DOTNET_CLI_HOME -Force | Out-Null
    New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    [IO.File]::WriteAllLines(
        $executionLogPath,
        @('TimestampUtc|Ordinal|Suite|Revision|State|Detail'))
    if ($Paired) {
        [IO.File]::WriteAllLines(
            $pairManifestPath,
            @('Suite,PairId,Order,FirstRevision,SecondRevision,BaselineRaw,CandidateRaw'))
        [IO.File]::WriteAllLines($pairedRawDigestManifestPath, [string[]] @())
    }

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

    $previousBuildInputsIdentity = Write-EffectiveBuildInputManifest `
        -SourceRoot $baselineWorktree `
        -Commit $previousCommit `
        -Description 'Previous revision' `
        -ManifestPath $previousBuildInputsManifestPath
    $candidateBuildInputsIdentity = Write-EffectiveBuildInputManifest `
        -SourceRoot $candidateWorktree `
        -Commit $candidateCommit `
        -Description 'Candidate revision' `
        -ManifestPath $candidateBuildInputsManifestPath
    Add-Content `
        -LiteralPath $preflightPath `
        -Value @(
            "- Previous effective build-input SHA-256: ``$previousBuildInputsIdentity``",
            "- Previous effective build-input manifest: ``$previousBuildInputsManifestPath``",
            "- Candidate effective build-input SHA-256: ``$candidateBuildInputsIdentity``",
            "- Candidate effective build-input manifest: ``$candidateBuildInputsManifestPath``",
            '- Effective build-input identities are revision-specific and are not asserted equal.'
        )

    $candidateHarnessIdentity = Sync-CandidateBenchmarkHarness `
        -CandidateSourceRoot $candidateWorktree `
        -PreviousSourceRoot $baselineWorktree
    Add-Content `
        -LiteralPath $preflightPath `
        -Value @(
            "- Candidate benchmark harness SHA-256: ``$candidateHarnessIdentity``",
            "- Candidate benchmark harness manifest: ``$harnessManifestPath``"
        )
    Write-Host "Candidate benchmark harness synchronized: $candidateHarnessIdentity"

    if ($ShareSameRevisionArtifact) {
        Invoke-BenchmarkBuild `
            -SourceRoot $candidateWorktree `
            -RunName 'candidate-shared' `
            -HarnessIdentity $candidateHarnessIdentity `
            -BuildInputsIdentity $candidateBuildInputsIdentity `
            -BuildInputsManifestPath $candidateBuildInputsManifestPath
        $sharedArtifact = Get-BenchmarkArtifactIdentity `
            -SourceRoot $candidateWorktree
        $sharedArtifactPath = $sharedArtifact.Path
        $sharedArtifactSha256 = $sharedArtifact.Sha256
        Add-Content `
            -LiteralPath $preflightPath `
            -Value @(
                "- Shared benchmark artifact path: ``$sharedArtifactPath``",
                "- Shared benchmark artifact SHA-256: ``$sharedArtifactSha256``"
            )
        Write-Host (
            'Shared same-revision benchmark artifact verified: ' +
            "$sharedArtifactSha256")
    }
    else {
        foreach ($revision in $revisionOrder) {
            if ($revision -eq 'previous') {
                Invoke-BenchmarkBuild `
                    -SourceRoot $baselineWorktree `
                    -RunName 'previous-release' `
                    -HarnessIdentity $candidateHarnessIdentity `
                    -BuildInputsIdentity $previousBuildInputsIdentity `
                    -BuildInputsManifestPath $previousBuildInputsManifestPath
            }
            else {
                Invoke-BenchmarkBuild `
                    -SourceRoot $candidateWorktree `
                    -RunName 'candidate' `
                    -HarnessIdentity $candidateHarnessIdentity `
                    -BuildInputsIdentity $candidateBuildInputsIdentity `
                    -BuildInputsManifestPath $candidateBuildInputsManifestPath
            }
        }
    }
    Invoke-PostBuildQuiescence

    $executionOrdinal = 0
    if ($Paired) {
        foreach ($suite in $suiteDefinitions) {
            $baselineAggregatePath = Join-Path $baselineResults "$($suite.Name).csv"
            $candidateAggregatePath = Join-Path $candidateResults "$($suite.Name).csv"
            $baselineSuiteRawDirectory = Join-Path $baselineRawResults $suite.Name
            $candidateSuiteRawDirectory = Join-Path $candidateRawResults $suite.Name
            foreach ($reservedPath in @(
                    $baselineAggregatePath,
                    $candidateAggregatePath,
                    $baselineSuiteRawDirectory,
                    $candidateSuiteRawDirectory)) {
                if (Test-Path -LiteralPath $reservedPath) {
                    throw "Paired release-core destination already exists: $reservedPath"
                }
            }

            $baselineSuiteRawPaths = [Collections.Generic.List[string]]::new()
            $candidateSuiteRawPaths = [Collections.Generic.List[string]]::new()
            foreach ($pair in $pairDefinitions) {
                $pairRawPaths = @{}
                $pairRevisionOrder = @($pair.FirstRevision, $pair.SecondRevision)
                for ($position = 0; $position -lt $pairRevisionOrder.Count; $position++) {
                    $revision = $pairRevisionOrder[$position]
                    $executionOrdinal++
                    $eventDetail = (
                        "PairId=$($pair.Id);Order=$($pair.Order);" +
                        "Position=$($position + 1)")
                    if ($ShareSameRevisionArtifact) {
                        $eventDetail += (
                            ";SourceRoot=$candidateWorktree;" +
                            "ArtifactPath=$sharedArtifactPath;" +
                            "ArtifactSha256=$sharedArtifactSha256")
                    }
                    Write-ExecutionEvent `
                        -Ordinal $executionOrdinal `
                        -Suite $suite.Name `
                        -Revision $revision `
                        -State 'START' `
                        -Detail $eventDetail
                    try {
                        $sourceRoot = if ($ShareSameRevisionArtifact) {
                            $candidateWorktree
                        }
                        else {
                            if ($revision -eq 'previous') {
                                $baselineWorktree
                            }
                            else {
                                $candidateWorktree
                            }
                        }
                        $destination = if ($revision -eq 'previous') {
                            $baselineResults
                        }
                        else {
                            $candidateResults
                        }
                        $runName = if ($revision -eq 'previous') {
                            'previous-release'
                        }
                        else {
                            'candidate'
                        }
                        $sampleParameters = @{
                            SourceRoot = $sourceRoot
                            Destination = $destination
                            RunName = $runName
                            Suite = $suite
                            PairId = $pair.Id
                        }
                        if ($ShareSameRevisionArtifact) {
                            $sampleParameters['ArtifactPath'] = $sharedArtifactPath
                            $sampleParameters['ExpectedArtifactSha256'] =
                                $sharedArtifactSha256
                        }
                        $samplePath = Invoke-ReleaseCoreSample @sampleParameters
                        $pairRawPaths[$revision] = $samplePath
                    }
                    catch {
                        Write-ExecutionEvent `
                            -Ordinal $executionOrdinal `
                            -Suite $suite.Name `
                            -Revision $revision `
                            -State 'FAIL' `
                            -Detail "$eventDetail;$($_.Exception.Message)"
                        throw
                    }
                    Write-ExecutionEvent `
                        -Ordinal $executionOrdinal `
                        -Suite $suite.Name `
                        -Revision $revision `
                        -State 'PASS' `
                        -Detail $eventDetail
                }

                if (-not $pairRawPaths.ContainsKey('previous') -or
                    -not $pairRawPaths.ContainsKey('candidate')) {
                    throw "Paired release-core pair '$($pair.Id)' is incomplete."
                }
                $baselineRawPath = [IO.Path]::GetFullPath(
                    [string] $pairRawPaths['previous'])
                $candidateRawPath = [IO.Path]::GetFullPath(
                    [string] $pairRawPaths['candidate'])
                $baselineSuiteRawPaths.Add($baselineRawPath)
                $candidateSuiteRawPaths.Add($candidateRawPath)
                $manifestValues = @(
                    $suite.Name,
                    $pair.Id,
                    $pair.Order,
                    $pair.FirstRevision,
                    $pair.SecondRevision,
                    $baselineRawPath,
                    $candidateRawPath)
                Add-Content `
                    -LiteralPath $pairManifestPath `
                    -Value (@(
                            $manifestValues |
                                ForEach-Object { Convert-ToCsvCell $_ }
                        ) -join ',')
            }

            if ($baselineSuiteRawPaths.Count -ne $pairDefinitions.Count -or
                $candidateSuiteRawPaths.Count -ne $pairDefinitions.Count) {
                throw (
                    "Paired release-core suite '$($suite.Name)' retained an " +
                    'unexpected number of raw samples.')
            }
            Write-AggregatedBenchmarkCsv `
                -RawPaths $baselineSuiteRawPaths.ToArray() `
                -DestinationPath $baselineAggregatePath
            Write-AggregatedBenchmarkCsv `
                -RawPaths $candidateSuiteRawPaths.ToArray() `
                -DestinationPath $candidateAggregatePath
        }
    }
    else {
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
    }

    if ($Paired) {
        $pairedRawDigestCount = Assert-PairedRawEvidenceDigestManifest
        Add-Content `
            -LiteralPath $preflightPath `
            -Value (
                "- Paired raw SHA-256 manifest: ``$pairedRawDigestManifestPath`` " +
                "($pairedRawDigestCount files verified)")
    }

    try {
        $comparisonParameters = @{
            BaselineResultsPath = $baselineResults
            CandidateResultsPath = $candidateResults
            BaselineRawResultsPath = $baselineRawResults
            CandidateRawResultsPath = $candidateRawResults
            RepeatCount = $RepeatCount
            ReportPath = $reportPath
            MaxThroughputRegressionPercent = $MaxThroughputRegressionPercent
            MaxP99RegressionPercent = $MaxP99RegressionPercent
            MaxP99RegressionMilliseconds = $MaxP99RegressionMilliseconds
        }
        if ($Paired) {
            $comparisonParameters['PairManifestPath'] = $pairManifestPath
        }
        & $comparisonScript @comparisonParameters
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
                "- Execution strategy: $executionStrategy",
                "- Revision order within each suite: $revisionOrderDescription",
                "- Suite order: $suiteOrder",
                $(if ($hasHybridStorageScenario) {
                        "- Hybrid storage scenario: ``$HybridStorageScenarioName``"
                    }),
                "- Execution order: $executionOrder",
                "- Execution log: ``$executionLogPath``",
                $repeatDescription,
                $artifactSharingDescription,
                $(if ($ShareSameRevisionArtifact) {
                        "- Shared benchmark artifact path: ``$sharedArtifactPath``"
                    }),
                $(if ($ShareSameRevisionArtifact) {
                        "- Shared benchmark artifact SHA-256: ``$sharedArtifactSha256``"
                    }),
                $quiescenceDescription,
                $(if ($Paired) { "- Pair manifest: ``$pairManifestPath``" }),
                $(if ($Paired) {
                        "- Paired raw SHA-256 manifest: " +
                            "``$pairedRawDigestManifestPath`` " +
                            "($pairedRawDigestCount files verified)"
                    }),
                "- Candidate benchmark harness SHA-256: ``$candidateHarnessIdentity``",
                "- Candidate benchmark harness manifest: ``$harnessManifestPath``",
                "- Previous effective build-input SHA-256: ``$previousBuildInputsIdentity``",
                "- Previous effective build-input manifest: ``$previousBuildInputsManifestPath``",
                "- Candidate effective build-input SHA-256: ``$candidateBuildInputsIdentity``",
                "- Candidate effective build-input manifest: ``$candidateBuildInputsManifestPath``",
                '- Effective build-input identities are revision-specific and are not asserted equal.',
                "- Previous raw results: ``$baselineRawResults``",
                "- Candidate raw results: ``$candidateRawResults``",
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
