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

    [ValidateSet('P95', 'P99')]
    [string] $BlockingLatencyPercentile = 'P99',

    [switch] $Paired,

    [string[]] $SuiteName = @(),

    [switch] $AllowSameRevision,

    [string] $HybridStorageScenarioName = '',

    [switch] $ShareSameRevisionArtifact,

    [ValidateRange(0, 3600)]
    [int] $PostBuildQuiescenceSeconds = 0,

    [ValidateRange(0, 300)]
    [int] $InterSampleQuiescenceSeconds = 0,

    [switch] $MonitorLocalEnvironment,

    [string] $EnvironmentMonitorScript = '',

    [ValidateRange(250, 10000)]
    [int] $MonitorSampleIntervalMilliseconds = 1000,

    [ValidateRange(0, 100)]
    [double] $MaxExternalCpuPercent = 8,

    [ValidateRange(0, 64)]
    [double] $MaxExternalCpuCoreEquivalent = 0.5,

    [ValidateRange(0, [long]::MaxValue)]
    [long] $MaxExternalIoBytesPerSecond = 4194304,

    [ValidateRange(1, 60)]
    [int] $RequiredConsecutiveBusySamples = 5,

    [ValidateNotNullOrEmpty()]
    [string] $ProhibitedExternalProcessNames =
        'devenv;msbuild;vbcscompiler;testhost;vstest.console;msiexec;' +
        'trustedinstaller;tiworker;mousocoreworker;usoclient;winget;nuget',

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

function Assert-PersistedLines {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $ExpectedLines,
        [Parameter(Mandatory)][string] $Description
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Description is missing after persistence: $Path"
    }
    [string[]] $actualLines = [IO.File]::ReadAllLines($Path)
    if ($actualLines.Count -ne $ExpectedLines.Count) {
        throw (
            "$Description persistence verification failed. " +
            "Expected $($ExpectedLines.Count) lines; found $($actualLines.Count).")
    }
    for ($lineIndex = 0; $lineIndex -lt $ExpectedLines.Count; $lineIndex++) {
        if ($actualLines[$lineIndex] -cne $ExpectedLines[$lineIndex]) {
            throw "$Description persistence verification failed at line $($lineIndex + 1)."
        }
    }
}

function Get-DeterministicLinesSha256 {
    param([Parameter(Mandatory)][AllowEmptyCollection()][string[]] $Lines)

    $payload = ($Lines -join "`n") + "`n"
    $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
    return [Convert]::ToHexString(
        [Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
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
[string[]] $exactMasterDurableRowNames = @(
    'MasterComparison_Sql_FileBacked_SingleInsert'
    'MasterComparison_Sql_FileBacked_BatchInsertRows'
    'MasterComparison_Sql_HybridIncrementalDurable_SingleInsert'
    'MasterComparison_Sql_HybridIncrementalDurable_BatchInsertRows'
    'MasterComparison_Sql_DirectClientLocalProcess_SingleInsert'
    'MasterComparison_Sql_DirectClientLocalProcess_BatchInsertRows'
    'MasterComparison_Collection_FileBacked_SinglePut'
    'MasterComparison_Collection_FileBacked_BatchPutDocs'
    'MasterComparison_Collection_HybridIncrementalDurable_SinglePut'
    'MasterComparison_Collection_HybridIncrementalDurable_BatchPutDocs'
)
$exactMasterDurableSelector = 'master-table-durable-write-scenarios'
$exactMasterDurableResultPrefix = 'master-table-durable-write-scenario'
$defaultSuiteDefinitions = @(
    [pscustomobject]@{ Name = 'master-table'; Arguments = @('--master-table'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'durable-sql-batching'; Arguments = @('--durable-sql-batching'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'concurrent-write-diagnostics'; Arguments = @('--concurrent-write-diagnostics'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-storage-mode'; Arguments = @('--hybrid-storage-mode'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-hot-set-read'; Arguments = @('--hybrid-hot-set-read'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'hybrid-cold-open'; Arguments = @('--hybrid-cold-open'); ExpectedRowName = $null }
    [pscustomobject]@{ Name = 'sqlite-compare'; Arguments = @('--sqlite-compare'); ExpectedRowName = $null }
)
$selectableSuiteDefinitions = @(
    $defaultSuiteDefinitions
    [pscustomobject]@{
        Name = 'master-table-durable-writes'
        Arguments = @('--master-table-durable-writes')
        ExpectedRowName = $null
    }
    [pscustomobject]@{
        Name = 'master-table-hosted-stable'
        Arguments = @('--master-table-hosted-stable')
        ExpectedRowName = $null
    }
    [pscustomobject]@{
        Name = $exactMasterDurableSelector
        Arguments = @()
        ExpectedRowName = $null
    }
)
$suiteDefinitions = @($defaultSuiteDefinitions)
$isExactMasterDurableMode = $false
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
        $requestedSuites | Where-Object { $_ -cnotin $selectableSuiteDefinitions.Name }
    )
    if ($unknownSuites.Count -gt 0) {
        throw (
            "Unknown release-core suite name(s): $($unknownSuites -join ', '). " +
            "Supported suites: $($selectableSuiteDefinitions.Name -join ', ').")
    }
    $isExactMasterDurableMode = $requestedSuites -ccontains $exactMasterDurableSelector
    if ($isExactMasterDurableMode) {
        if ($requestedSuites.Count -ne 1) {
            throw (
                "Suite '$exactMasterDurableSelector' is a complete canonical " +
                'qualification mode and cannot be combined with other suites.')
        }
        if (-not $Paired) {
            throw "Suite '$exactMasterDurableSelector' requires -Paired."
        }
        $suiteDefinitions = @(
            foreach ($rowName in $exactMasterDurableRowNames) {
                [pscustomobject]@{
                    Name = $rowName
                    Arguments = @(
                        '--master-table-durable-write-scenario',
                        $rowName)
                    ExpectedRowName = $rowName
                    ResultPrefix = $exactMasterDurableResultPrefix
                }
            }
        )
    }
    else {
        $suiteDefinitions = @(
            $selectableSuiteDefinitions | Where-Object { $_.Name -cin $requestedSuites }
        )
    }
}
if ($suiteDefinitions.Count -eq 0) {
    throw 'At least one release-core suite must be selected.'
}
if ($InterSampleQuiescenceSeconds -gt 0 -and -not $isExactMasterDurableMode) {
    throw (
        'InterSampleQuiescenceSeconds is valid only with the canonical ' +
        "'$exactMasterDurableSelector' suite.")
}
if ($MonitorLocalEnvironment -and -not $isExactMasterDurableMode) {
    throw (
        'MonitorLocalEnvironment is valid only with the canonical ' +
        "'$exactMasterDurableSelector' suite.")
}
if ($MonitorLocalEnvironment -and -not $IsWindows) {
    throw 'MonitorLocalEnvironment requires Windows.'
}
if ($MonitorLocalEnvironment) {
    if ([string]::IsNullOrWhiteSpace($EnvironmentMonitorScript)) {
        throw 'MonitorLocalEnvironment requires EnvironmentMonitorScript.'
    }
    $EnvironmentMonitorScript = [IO.Path]::GetFullPath($EnvironmentMonitorScript)
    if (-not (Test-Path -LiteralPath $EnvironmentMonitorScript -PathType Leaf)) {
        throw "Local performance environment monitor not found: $EnvironmentMonitorScript"
    }
}
elseif ($PSBoundParameters.ContainsKey('EnvironmentMonitorScript') -and
    -not [string]::IsNullOrWhiteSpace($EnvironmentMonitorScript)) {
    throw 'EnvironmentMonitorScript requires -MonitorLocalEnvironment.'
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

$exactMasterDurablePairSchedule = @()
if ($isExactMasterDurableMode) {
    $rotationStep = 3
    $passRoundOffset = ($QualificationPass - 1) * $pairDefinitions.Count
    $exactMasterDurablePairSchedule = @(
        for ($pairOffset = 0; $pairOffset -lt $pairDefinitions.Count; $pairOffset++) {
            $pair = $pairDefinitions[$pairOffset]
            $rotationOffset = (
                ($passRoundOffset + $pairOffset) * $rotationStep) %
                $suiteDefinitions.Count
            for ($rowTimePosition = 0;
                $rowTimePosition -lt $suiteDefinitions.Count;
                $rowTimePosition++) {
                $suiteIndex = (
                    $rowTimePosition + $rotationOffset) %
                    $suiteDefinitions.Count
                [pscustomobject]@{
                    Suite = $suiteDefinitions[$suiteIndex]
                    Pair = $pair
                    PairRound = $pairOffset + 1
                    RowTimePosition = $rowTimePosition + 1
                    RotationOffset = $rotationOffset
                }
            }
        }
    )
}

$executionPlan = @(
    if ($isExactMasterDurableMode) {
        foreach ($scheduledPair in $exactMasterDurablePairSchedule) {
            foreach ($position in 0, 1) {
                [pscustomobject]@{
                    Suite = $scheduledPair.Suite
                    Revision = if ($position -eq 0) {
                        $scheduledPair.Pair.FirstRevision
                    }
                    else {
                        $scheduledPair.Pair.SecondRevision
                    }
                    Pair = $scheduledPair.Pair
                    Position = $position + 1
                    PairRound = $scheduledPair.PairRound
                    RowTimePosition = $scheduledPair.RowTimePosition
                    RotationOffset = $scheduledPair.RotationOffset
                }
            }
        }
    }
    else {
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
    }
)
$suiteOrder = if ($isExactMasterDurableMode) {
    $exactMasterDurableSelector
}
else {
    $suiteDefinitions.Name -join ', '
}
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
$blockingLatencyLabel = $BlockingLatencyPercentile.ToUpperInvariant()
$latencyAbsoluteAllowance = $MaxP99RegressionMilliseconds.ToString(
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
$pairedArtifactManifestPath = Join-Path $logRoot 'paired-benchmark-artifacts.sha256'
$pairedArtifactCloseoutPath = Join-Path $logRoot 'paired-benchmark-artifact-closeout.log'
$exactMasterDurableSchedulePath = Join-Path $logRoot 'durable-v3-exact-master-schedule.csv'
$exactMasterDurableDesignPath = Join-Path $logRoot 'durable-v3-exact-master-design.txt'
$exactMasterDurableConditioningPath = Join-Path $logRoot 'durable-v3-conditioning.csv'
$environmentMonitorCsvPath = Join-Path $logRoot 'durable-v3-environment-monitor.csv'
$environmentMonitorReadyPath = Join-Path $logRoot 'durable-v3-environment-monitor.ready'
$environmentMonitorStopPath = Join-Path $logRoot 'durable-v3-environment-monitor.stop'
$environmentMonitorStdoutPath = Join-Path $logRoot 'durable-v3-environment-monitor.stdout.log'
$environmentMonitorStderrPath = Join-Path $logRoot 'durable-v3-environment-monitor.stderr.log'
$environmentMonitorSummaryPath = Join-Path $logRoot 'durable-v3-environment-monitor-summary.txt'
$artifactStageParent = Join-Path $outputRoot 'benchmark-artifact-slots'
$artifactSlotNames = @('artifact-slot-a', 'artifact-slot-b')
$revisionArtifactSlots = @{
    $revisionOrder[0] = $artifactSlotNames[0]
    $revisionOrder[1] = $artifactSlotNames[1]
}
$harnessManifestPath = Join-Path $logRoot 'candidate-benchmark-harness.sha256'
$candidateBuildInputsManifestPath = Join-Path $logRoot 'candidate-effective-build-inputs.sha256'
$previousBuildInputsManifestPath = Join-Path $logRoot 'previous-effective-build-inputs.sha256'
$executionStrategy = if ($isExactMasterDurableMode) {
    'durable-v3-exact-master-row-adjacent'
}
elseif ($Paired) {
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
    '- Same-revision artifact sharing: disabled; each paired revision will ' +
        'invoke its own verified build artifact'
}
$quiescenceDescription = if ($PostBuildQuiescenceSeconds -gt 0) {
    '- Post-build quiescence: dotnet build servers will be shut down, then ' +
        "measurements will wait $PostBuildQuiescenceSeconds second(s)"
}
else {
    '- Post-build quiescence: disabled'
}

$exactMasterDurableDesignFingerprint = ''
$exactMasterDurableScheduleSha256 = ''
if ($isExactMasterDurableMode) {
    New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    [string[]] $designLines = @(
        'FormatVersion=csharpdb-durable-v3-exact-master-design/v1'
        "Selector=$exactMasterDurableSelector"
        'CliMode=--master-table-durable-write-scenario <exact-master-row-name>'
        'Schedule=pair-rounds; row-adjacent revisions; ten-position rotation step 3 continued across qualification passes'
        "PairRepeatsPerOrder=$RepeatCount"
        "InterSampleQuiescenceSeconds=$InterSampleQuiescenceSeconds"
        'RecordedAttemptPolicy=no-discard; no-replacement; one scheduled attempt per raw row side'
        'ArtifactStaging=equal-length sibling slots; pass-starting revision in artifact-slot-a; every staged closure file hash-read verified'
        'Conditioning=one non-recorded exact-row invocation per staged artifact before post-build quiescence'
        "EnvironmentMonitorEnabled=$MonitorLocalEnvironment"
        "MonitorSampleIntervalMilliseconds=$MonitorSampleIntervalMilliseconds"
        "MaxExternalCpuPercent=$MaxExternalCpuPercent"
        "MaxExternalCpuCoreEquivalent=$MaxExternalCpuCoreEquivalent"
        'ExternalCpuSignal=observable external process CPU; system busy CPU minus observable allowed runner-tree CPU is retained as a diagnostic; unobservable allowed CPU contaminates immediately'
        "MaxExternalIoBytesPerSecond=$MaxExternalIoBytesPerSecond"
        "RequiredConsecutiveBusySamples=$RequiredConsecutiveBusySamples"
        "ProhibitedExternalProcessNames=$ProhibitedExternalProcessNames"
        'MonitorCoveragePolicy=ready-to-first and inter-sample gaps at most max(5000ms,3x sample interval); final sample at or after final declared measurement end'
        'MinimumMeasuredSeconds=30'
        'MinimumRetainedLatencySamples=10000'
        'RequiredPolicyMetadata=qualification=true;unrecorded-warmup-seconds=2;minimum-measured-seconds=30;minimum-retained-latency-samples=10000;measurement-cap-seconds=120;measurement-begin-utc;measurement-end-utc'
        foreach ($rowName in $exactMasterDurableRowNames) {
            "ExactRow=$rowName"
        }
    )
    $exactMasterDurableDesignFingerprint = Get-DeterministicLinesSha256 -Lines $designLines
    Write-LinesAtomically -Path $exactMasterDurableDesignPath -Lines $designLines
    Assert-PersistedLines `
        -Path $exactMasterDurableDesignPath `
        -ExpectedLines $designLines `
        -Description 'Durable-v3 design manifest'

    [string[]] $scheduleLines = @(
        'Ordinal,QualificationPass,PairRound,RowTimePosition,RotationOffset,Suite,ExpectedRowName,PairId,PairOrder,PairPosition,Revision,ArtifactSlot,Arguments,InterSampleQuiescenceSeconds,Recorded'
        for ($entryIndex = 0; $entryIndex -lt $executionPlan.Count; $entryIndex++) {
            $entry = $executionPlan[$entryIndex]
            $scheduleValues = @(
                $entryIndex + 1
                $QualificationPass
                $entry.PairRound
                $entry.RowTimePosition
                $entry.RotationOffset
                $entry.Suite.Name
                $entry.Suite.ExpectedRowName
                $entry.Pair.Id
                $entry.Pair.Order
                $entry.Position
                $entry.Revision
                $revisionArtifactSlots[$entry.Revision]
                ($entry.Suite.Arguments -join ' ')
                $InterSampleQuiescenceSeconds
                'true')
            @($scheduleValues | ForEach-Object { Convert-ToCsvCell $_ }) -join ','
        }
    )
    Write-LinesAtomically -Path $exactMasterDurableSchedulePath -Lines $scheduleLines
    Assert-PersistedLines `
        -Path $exactMasterDurableSchedulePath `
        -ExpectedLines $scheduleLines `
        -Description 'Durable-v3 execution schedule'
    $exactMasterDurableScheduleSha256 = (
        Get-FileHash -LiteralPath $exactMasterDurableSchedulePath -Algorithm SHA256
    ).Hash.ToLowerInvariant()
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
    $(if ($isExactMasterDurableMode) {
            "- Canonical exact-master durable mode: ``$exactMasterDurableSelector`` (10 exact one-row suites)"
        }),
    $(if ($isExactMasterDurableMode) {
            '- Recorded evidence policy: no discard, no replacement, and one predeclared attempt per logical side'
        }),
    $(if ($isExactMasterDurableMode) {
            "- Inter-sample quiescence before every recorded logical side: $InterSampleQuiescenceSeconds second(s)"
        }),
    $(if ($isExactMasterDurableMode) {
            if ($MonitorLocalEnvironment) {
                ("- Durable-v3 external environment monitor: enabled; interval=" +
                    "$MonitorSampleIntervalMilliseconds ms; external CPU limit=" +
                    "$MaxExternalCpuPercent% or $MaxExternalCpuCoreEquivalent CPU-core equivalent; external observable-process I/O limit=" +
                    "$MaxExternalIoBytesPerSecond bytes/sec; sustained samples=" +
                    "$RequiredConsecutiveBusySamples; prohibited external processes contaminate immediately: " +
                    $ProhibitedExternalProcessNames)
            }
            else {
                '- Durable-v3 external environment monitor: disabled'
            }
        }),
    $(if ($isExactMasterDurableMode) {
            "- Durable-v3 design fingerprint: ``$exactMasterDurableDesignFingerprint``"
        }),
    $(if ($isExactMasterDurableMode) {
            "- Durable-v3 design manifest: ``$exactMasterDurableDesignPath``"
        }),
    $(if ($isExactMasterDurableMode) {
            "- Predeclared durable-v3 schedule: ``$exactMasterDurableSchedulePath``; SHA-256 ``$exactMasterDurableScheduleSha256``"
        }),
    $(if ($isExactMasterDurableMode) {
            "- Symmetric artifact slot assignment: previous=``$($revisionArtifactSlots['previous'])``; candidate=``$($revisionArtifactSlots['candidate'])``"
        }),
    "- Execution order: $executionOrder",
    $repeatDescription,
    $artifactSharingDescription,
    $(if ($ShareSameRevisionArtifact) {
            "- Planned shared artifact search root: ``$candidateArtifactSearchRoot``"
        }),
    $quiescenceDescription,
    "- Throughput regression limit: $MaxThroughputRegressionPercent%",
    "- $blockingLatencyLabel regression limit: $MaxP99RegressionPercent%",
    "- $blockingLatencyLabel absolute regression allowance: $latencyAbsoluteAllowance ms",
    "- $blockingLatencyLabel failure rule: relative and absolute limits must both be exceeded",
    $(if ($blockingLatencyLabel -cne 'P99') {
            '- Blocking latency percentile: P95. P99 remains recorded as a non-blocking diagnostic.'
        }),
    '- Benchmark source harness: candidate benchmark-project files synchronized to both engines; revision-specific effective build inputs are recorded separately during execution',
    "- Previous ref: ``$PreviousRef`` (``$previousCommit``)",
    "- Candidate ref: ``$CandidateRef`` (``$candidateCommit``)",
    "- Planned execution log: ``$executionLogPath``",
    $(if ($Paired) { "- Planned pair manifest: ``$pairManifestPath``" }),
    $(if ($Paired) {
            "- Planned paired raw SHA-256 manifest: ``$pairedRawDigestManifestPath``"
        }),
    $(if ($Paired) {
            "- Planned paired benchmark artifact manifest: ``$pairedArtifactManifestPath``"
        }),
    $(if ($Paired) {
            "- Planned paired benchmark artifact closeout: ``$pairedArtifactCloseoutPath``"
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
$pairedArtifacts = @{}
$pairedArtifactManifestLines = [string[]] @()
$pairedArtifactManifestPersisted = $false
$pairedRawDigestCount = 0
$primaryFailure = $null
$cleanupFailures = [Collections.Generic.List[string]]::new()
$environmentMonitorProcess = $null
$environmentMonitorReadyUtc = [DateTimeOffset]::MinValue
$environmentMonitorStopped = $false
$environmentMonitorSha256 = ''
$environmentMonitorSampleCount = 0
$originalNuGetPackages = $env:NUGET_PACKAGES
$originalDotnetCliHome = $env:DOTNET_CLI_HOME
$originalDotnetCliWorkloadUpdateNotifyDisable =
    $env:DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE
$originalDotnetSkipWorkloadIntegrityCheck =
    $env:DOTNET_SKIP_WORKLOAD_INTEGRITY_CHECK
$originalDotnetGenerateAspNetCertificate =
    $env:DOTNET_GENERATE_ASPNET_CERTIFICATE
$originalDotnetAddGlobalToolsToPath =
    $env:DOTNET_ADD_GLOBAL_TOOLS_TO_PATH
$originalDotnetNoLogo = $env:DOTNET_NOLOGO
$originalDotnetCliTelemetryOptOut = $env:DOTNET_CLI_TELEMETRY_OPTOUT

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

function Test-BenchmarkArtifactClosureLink {
    param(
        [Parameter(Mandatory)]
        [IO.FileSystemInfo] $Item
    )

    if (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        return $true
    }
    try {
        return -not [string]::IsNullOrEmpty([string] $Item.LinkTarget)
    }
    catch {
        throw "Could not inspect benchmark artifact closure link metadata: $($Item.FullName)"
    }
}

function Get-BenchmarkArtifactClosureDirectories {
    param(
        [Parameter(Mandatory)]
        [string] $ClosureRoot
    )

    $rootDirectory = Get-Item -LiteralPath $ClosureRoot -Force
    if (Test-BenchmarkArtifactClosureLink -Item $rootDirectory) {
        throw "Benchmark artifact closure root cannot be a reparse point or link: $ClosureRoot"
    }

    $pendingDirectories = [Collections.Generic.Queue[IO.DirectoryInfo]]::new()
    $closureDirectories = [Collections.Generic.List[IO.DirectoryInfo]]::new()
    $pendingDirectories.Enqueue($rootDirectory)
    while ($pendingDirectories.Count -ne 0) {
        $currentDirectory = $pendingDirectories.Dequeue()
        $closureDirectories.Add($currentDirectory)
        $childDirectoryList = [Collections.Generic.List[object]]::new()
        foreach ($childDirectory in @(
                Get-ChildItem `
                    -LiteralPath $currentDirectory.FullName `
                    -Directory `
                    -Force)) {
            $childDirectoryList.Add($childDirectory)
        }
        $childDirectoryList.Sort(
            [Comparison[object]] {
                param($left, $right)
                return [StringComparer]::Ordinal.Compare(
                    [string] $left.FullName,
                    [string] $right.FullName)
            })
        foreach ($childDirectory in $childDirectoryList) {
            if (Test-BenchmarkArtifactClosureLink -Item $childDirectory) {
                throw (
                    'Benchmark artifact closure directory cannot be a reparse ' +
                    "point or link: $($childDirectory.FullName)")
            }
            $pendingDirectories.Enqueue($childDirectory)
        }
    }

    return $closureDirectories.ToArray()
}

function Test-PathStrictlyWithinRoot {
    param(
        [Parameter(Mandatory)]
        [string] $Root,

        [Parameter(Mandatory)]
        [string] $Path
    )

    $relativePath = [IO.Path]::GetRelativePath(
        [IO.Path]::GetFullPath($Root),
        [IO.Path]::GetFullPath($Path))
    $parentPrefix = '..' + [IO.Path]::DirectorySeparatorChar
    $alternateParentPrefix = '..' + [IO.Path]::AltDirectorySeparatorChar
    return $relativePath -cne '.' -and
        -not [IO.Path]::IsPathFullyQualified($relativePath) -and
        $relativePath -cne '..' -and
        -not $relativePath.StartsWith($parentPrefix, [StringComparison]::Ordinal) -and
        -not $relativePath.StartsWith(
            $alternateParentPrefix,
            [StringComparison]::Ordinal)
}

function Get-WorktreeLinkEntriesForCleanup {
    param(
        [Parameter(Mandatory)]
        [string] $WorktreeRoot
    )

    $resolvedWorktreeRoot = [IO.Path]::GetFullPath($WorktreeRoot)
    $rootDirectory = Get-Item -LiteralPath $resolvedWorktreeRoot -Force
    if (Test-BenchmarkArtifactClosureLink -Item $rootDirectory) {
        throw "Detached worktree root is a reparse point or link: $resolvedWorktreeRoot"
    }

    $pendingDirectories = [Collections.Generic.Queue[IO.DirectoryInfo]]::new()
    $linkEntries = [Collections.Generic.List[object]]::new()
    $pendingDirectories.Enqueue($rootDirectory)
    while ($pendingDirectories.Count -ne 0) {
        $queuedDirectory = $pendingDirectories.Dequeue()
        $currentDirectory = Get-Item -LiteralPath $queuedDirectory.FullName -Force
        if (Test-BenchmarkArtifactClosureLink -Item $currentDirectory) {
            if (-not (Test-PathStrictlyWithinRoot `
                    -Root $resolvedWorktreeRoot `
                    -Path $currentDirectory.FullName)) {
                throw (
                    'Detached worktree link entry is not strictly inside its ' +
                    "worktree: $($currentDirectory.FullName)")
            }
            $relativePath = [IO.Path]::GetRelativePath(
                $resolvedWorktreeRoot,
                $currentDirectory.FullName).
                    Replace('\', '/')
            $linkEntries.Add([pscustomobject]@{
                FullName = [IO.Path]::GetFullPath($currentDirectory.FullName)
                RelativePath = $relativePath
                Depth = $relativePath.Split('/').Length
                IsDirectory = $true
            })
            continue
        }

        $childEntryList = [Collections.Generic.List[object]]::new()
        foreach ($childEntry in @(
                Get-ChildItem -LiteralPath $currentDirectory.FullName -Force)) {
            $childEntryList.Add($childEntry)
        }
        $childEntryList.Sort(
            [Comparison[object]] {
                param($left, $right)
                return [StringComparer]::Ordinal.Compare(
                    [string] $left.FullName,
                    [string] $right.FullName)
            })
        foreach ($childEntry in $childEntryList) {
            $fullEntryPath = [IO.Path]::GetFullPath($childEntry.FullName)
            if (-not (Test-PathStrictlyWithinRoot `
                    -Root $resolvedWorktreeRoot `
                    -Path $fullEntryPath)) {
                throw (
                    'Detached worktree entry is not strictly inside its ' +
                    "worktree: $fullEntryPath")
            }
            if (Test-BenchmarkArtifactClosureLink -Item $childEntry) {
                $relativePath = [IO.Path]::GetRelativePath(
                    $resolvedWorktreeRoot,
                    $fullEntryPath).
                        Replace('\', '/')
                $linkEntries.Add([pscustomobject]@{
                    FullName = $fullEntryPath
                    RelativePath = $relativePath
                    Depth = $relativePath.Split('/').Length
                    IsDirectory = $childEntry -is [IO.DirectoryInfo]
                })
                continue
            }
            if ($childEntry -is [IO.DirectoryInfo]) {
                $pendingDirectories.Enqueue($childEntry)
            }
        }
    }

    $linkEntries.Sort(
        [Comparison[object]] {
            param($left, $right)
            $depthComparison = ([int] $right.Depth).CompareTo([int] $left.Depth)
            if ($depthComparison -ne 0) {
                return $depthComparison
            }
            return [StringComparer]::Ordinal.Compare(
                [string] $left.RelativePath,
                [string] $right.RelativePath)
        })
    return $linkEntries.ToArray()
}

function Disconnect-WorktreeLinksForCleanup {
    param(
        [Parameter(Mandatory)]
        [string] $WorktreeRoot
    )

    $resolvedWorktreeRoot = [IO.Path]::GetFullPath($WorktreeRoot)
    [object[]] $linkEntries = @(
        Get-WorktreeLinkEntriesForCleanup -WorktreeRoot $resolvedWorktreeRoot
    )
    foreach ($linkEntry in $linkEntries) {
        if (-not (Test-PathStrictlyWithinRoot `
                -Root $resolvedWorktreeRoot `
                -Path $linkEntry.FullName)) {
            throw (
                'Refusing to detach a worktree link entry outside the exact ' +
                "worktree: $($linkEntry.FullName)")
        }
        $currentEntry = Get-Item `
            -LiteralPath $linkEntry.FullName `
            -Force `
            -ErrorAction Stop
        if (-not (Test-BenchmarkArtifactClosureLink -Item $currentEntry)) {
            throw (
                'Worktree cleanup entry is no longer a link; refusing to ' +
                "delete it: $($linkEntry.FullName)")
        }
        if ($currentEntry -is [IO.DirectoryInfo]) {
            [IO.Directory]::Delete($linkEntry.FullName, $false)
        }
        else {
            [IO.File]::Delete($linkEntry.FullName)
        }
        $remainingEntry = Get-Item `
            -LiteralPath $linkEntry.FullName `
            -Force `
            -ErrorAction SilentlyContinue
        if ($null -ne $remainingEntry) {
            throw "Worktree link entry remains after non-recursive detachment: $($linkEntry.FullName)"
        }
    }

    [object[]] $remainingLinks = @(
        Get-WorktreeLinkEntriesForCleanup -WorktreeRoot $resolvedWorktreeRoot
    )
    if ($remainingLinks.Count -ne 0) {
        throw (
            "Detached worktree still contains $($remainingLinks.Count) link entry or entries: " +
            (($remainingLinks | ForEach-Object RelativePath) -join ', '))
    }
    return $linkEntries.Count
}

function Test-BenchmarkArtifactClosureExcludedPath {
    param(
        [Parameter(Mandatory)]
        [string] $RelativePath
    )

    $segments = $RelativePath.Replace('\', '/').Split(
        '/',
        [StringSplitOptions]::RemoveEmptyEntries)
    if ($segments.Length -lt 2) {
        return $false
    }
    $topLevelDirectory = $segments[0]
    return $topLevelDirectory -ceq 'results' -or
        $topLevelDirectory.StartsWith(
            'CSharpDB.Benchmarks-Job-',
            [StringComparison]::Ordinal)
}

function Get-BenchmarkArtifactClosure {
    param(
        [Parameter(Mandatory)]
        [string] $ArtifactPath
    )

    $resolvedArtifactPath = [IO.Path]::GetFullPath($ArtifactPath)
    if (-not (Test-Path -LiteralPath $resolvedArtifactPath -PathType Leaf)) {
        throw "Benchmark entry artifact is missing: $resolvedArtifactPath"
    }
    $closureRoot = [IO.Path]::GetFullPath((Split-Path -Parent $resolvedArtifactPath))
    [object[]] $closureDirectories = @(
        Get-BenchmarkArtifactClosureDirectories -ClosureRoot $closureRoot
    )

    $unsortedRecords = @(
        foreach ($closureDirectory in $closureDirectories) {
            $currentDirectory = Get-Item -LiteralPath $closureDirectory.FullName -Force
            if (Test-BenchmarkArtifactClosureLink -Item $currentDirectory) {
                throw (
                    'Benchmark artifact closure directory became a reparse ' +
                    "point or link before file enumeration: $($currentDirectory.FullName)")
            }
            Get-ChildItem -LiteralPath $currentDirectory.FullName -File -Force |
            ForEach-Object {
                $relativePath = [IO.Path]::GetRelativePath(
                    $closureRoot,
                    $_.FullName).
                        Replace('\', '/')
                if (-not (Test-BenchmarkArtifactClosureExcludedPath `
                        -RelativePath $relativePath)) {
                    if (Test-BenchmarkArtifactClosureLink -Item $_) {
                        throw (
                            'Benchmark artifact closure file cannot be a ' +
                            "reparse point or link: $($_.FullName)")
                    }
                    if ($relativePath.Contains("`r") -or $relativePath.Contains("`n")) {
                        throw "Benchmark artifact closure path cannot contain a line break: $($_.FullName)"
                    }
                    [pscustomobject]@{
                        RelativePath = $relativePath
                        FullName = [IO.Path]::GetFullPath($_.FullName)
                        Sha256 = (Get-FileHash `
                                -LiteralPath $_.FullName `
                                -Algorithm SHA256).
                            Hash.
                            ToLowerInvariant()
                    }
                }
            }
        }
    )
    $recordList = [Collections.Generic.List[object]]::new()
    foreach ($record in $unsortedRecords) {
        $recordList.Add($record)
    }
    $recordList.Sort(
        [Comparison[object]] {
            param($left, $right)
            return [StringComparer]::Ordinal.Compare(
                [string] $left.RelativePath,
                [string] $right.RelativePath)
        })
    [object[]] $records = $recordList.ToArray()
    if ($records.Count -eq 0) {
        throw "Benchmark artifact closure contains no immutable files: $closureRoot"
    }
    $entryRelativePath = [IO.Path]::GetRelativePath(
        $closureRoot,
        $resolvedArtifactPath).
            Replace('\', '/')
    $entryRecords = @($records | Where-Object RelativePath -CEQ $entryRelativePath)
    if ($entryRecords.Count -ne 1) {
        throw (
            "Benchmark artifact closure contains $($entryRecords.Count) entry DLL " +
            "record(s) named '$entryRelativePath'; expected one.")
    }

    [string[]] $identityLines = @(
        $records | ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    $identityPayload = ($identityLines -join "`n") + "`n"
    $identityBytes = [Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes($identityPayload))
    return [pscustomobject]@{
        Root = $closureRoot
        FileCount = $records.Count
        Sha256 = [Convert]::ToHexString($identityBytes).ToLowerInvariant()
        Records = [object[]] $records
        EntrySha256 = $entryRecords[0].Sha256
    }
}

function Get-BenchmarkArtifactIdentity {
    param(
        [Parameter(Mandatory)]
        [string] $SourceRoot
    )

    $resolvedSourceRoot = [IO.Path]::GetFullPath($SourceRoot)
    $artifactRoot = [IO.Path]::Combine(
        $resolvedSourceRoot,
        'tests',
        'CSharpDB.Benchmarks',
        'bin',
        'Release')
    if (-not (Test-Path -LiteralPath $artifactRoot -PathType Container)) {
        throw "Benchmark artifact directory not found: $artifactRoot"
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
            "Benchmark build produced $($artifacts.Count) runnable " +
            "CSharpDB.Benchmarks.dll artifact(s) under '$artifactRoot'; expected one.")
    }

    $resolvedArtifactPath = [IO.Path]::GetFullPath($artifacts[0].FullName)
    $closure = Get-BenchmarkArtifactClosure -ArtifactPath $resolvedArtifactPath
    return [pscustomobject]@{
        SourceRoot = $resolvedSourceRoot
        Path = $resolvedArtifactPath
        Sha256 = $closure.EntrySha256
        ClosureRoot = $closure.Root
        ClosureFileCount = $closure.FileCount
        ClosureSha256 = $closure.Sha256
        ClosureRecords = [object[]] $closure.Records
    }
}

function Copy-BenchmarkArtifactToSymmetricStage {
    param(
        [Parameter(Mandatory)]
        [object] $Artifact,

        [Parameter(Mandatory)]
        [string] $StageRoot,

        [Parameter(Mandatory)]
        [string] $SlotName
    )

    $resolvedStageRoot = [IO.Path]::GetFullPath($StageRoot)
    if (Test-Path -LiteralPath $resolvedStageRoot) {
        throw "Benchmark artifact staging slot already exists: $resolvedStageRoot"
    }
    Assert-BenchmarkArtifactHash `
        -ArtifactPath $Artifact.Path `
        -ExpectedSha256 $Artifact.Sha256 `
        -Context "before staging into $SlotName"
    Assert-BenchmarkArtifactClosure `
        -Artifact $Artifact `
        -Context "before staging into $SlotName"

    New-Item -ItemType Directory -Path $resolvedStageRoot -Force | Out-Null
    $readFileCount = 0
    foreach ($record in $Artifact.ClosureRecords) {
        $sourcePath = [IO.Path]::GetFullPath([string] $record.FullName)
        $sourceHash = (Get-FileHash `
                -LiteralPath $sourcePath `
                -Algorithm SHA256).
            Hash.
            ToLowerInvariant()
        if ($sourceHash -cne [string] $record.Sha256) {
            throw (
                "Benchmark artifact source closure changed while staging ${SlotName}: " +
                "$($record.RelativePath)")
        }

        $destinationPath = [IO.Path]::GetFullPath([IO.Path]::Combine(
                $resolvedStageRoot,
                ([string] $record.RelativePath).Replace(
                    '/',
                    [IO.Path]::DirectorySeparatorChar)))
        if (-not (Test-PathStrictlyWithinRoot `
                -Path $destinationPath `
                -Root $resolvedStageRoot)) {
            throw (
                "Benchmark artifact staged closure path escapes its slot: " +
                "$($record.RelativePath)")
        }
        $destinationDirectory = Split-Path -Parent $destinationPath
        New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
        Copy-Item -LiteralPath $sourcePath -Destination $destinationPath

        $destinationHash = (Get-FileHash `
                -LiteralPath $destinationPath `
                -Algorithm SHA256).
            Hash.
            ToLowerInvariant()
        if ($destinationHash -cne [string] $record.Sha256) {
            throw (
                "Benchmark artifact staged closure verification failed in ${SlotName}: " +
                "$($record.RelativePath)")
        }

        $readStream = [IO.File]::OpenRead($destinationPath)
        try {
            $readStream.CopyTo([IO.Stream]::Null)
        }
        finally {
            $readStream.Dispose()
        }
        $readFileCount++
    }
    if ($readFileCount -ne $Artifact.ClosureFileCount) {
        throw (
            "Benchmark artifact staging slot $SlotName read $readFileCount closure " +
            "files; expected $($Artifact.ClosureFileCount).")
    }

    $entryRelativePath = [IO.Path]::GetRelativePath(
        [string] $Artifact.ClosureRoot,
        [string] $Artifact.Path)
    $stagedArtifactPath = [IO.Path]::GetFullPath(
        [IO.Path]::Combine($resolvedStageRoot, $entryRelativePath))
    $stagedClosure = Get-BenchmarkArtifactClosure -ArtifactPath $stagedArtifactPath
    if ($stagedClosure.FileCount -ne $Artifact.ClosureFileCount -or
        $stagedClosure.Sha256 -cne $Artifact.ClosureSha256 -or
        $stagedClosure.EntrySha256 -cne $Artifact.Sha256) {
        throw "Benchmark artifact staged closure identity differs in $SlotName."
    }

    return [pscustomobject]@{
        SourceRoot = [string] $Artifact.SourceRoot
        SourceArtifactPath = [string] $Artifact.Path
        Path = $stagedArtifactPath
        Sha256 = $stagedClosure.EntrySha256
        ClosureRoot = $stagedClosure.Root
        ClosureFileCount = $stagedClosure.FileCount
        ClosureSha256 = $stagedClosure.Sha256
        ClosureRecords = [object[]] $stagedClosure.Records
        StagingSlot = $SlotName
        StagedReadFileCount = $readFileCount
    }
}

function Assert-BenchmarkArtifactHash {
    param(
        [Parameter(Mandatory)]
        [string] $ArtifactPath,

        [Parameter(Mandatory)]
        [string] $ExpectedSha256,

        [Parameter(Mandatory)]
        [string] $Context
    )

    if ($ExpectedSha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'Benchmark artifact verification requires a lowercase SHA-256 identity.'
    }
    $resolvedArtifactPath = [IO.Path]::GetFullPath($ArtifactPath)
    if (-not (Test-Path -LiteralPath $resolvedArtifactPath -PathType Leaf)) {
        throw "Benchmark artifact is missing $($Context): $resolvedArtifactPath"
    }
    $actualSha256 = (Get-FileHash `
            -LiteralPath $resolvedArtifactPath `
            -Algorithm SHA256).
        Hash.
        ToLowerInvariant()
    if ($actualSha256 -cne $ExpectedSha256) {
        throw (
            "Benchmark artifact changed $Context. " +
            "Expected $ExpectedSha256; found $actualSha256.")
    }
}

function Assert-BenchmarkArtifactClosure {
    param(
        [Parameter(Mandatory)]
        [object] $Artifact,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $currentClosure = Get-BenchmarkArtifactClosure -ArtifactPath $Artifact.Path
    [string[]] $expectedLines = @(
        $Artifact.ClosureRecords |
            ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    [string[]] $currentLines = @(
        $currentClosure.Records |
            ForEach-Object { "$($_.Sha256) *$($_.RelativePath)" }
    )
    $differences = @(
        Compare-Object `
            -ReferenceObject $expectedLines `
            -DifferenceObject $currentLines `
            -CaseSensitive
    )
    if ($currentClosure.FileCount -ne $Artifact.ClosureFileCount -or
        $currentClosure.Sha256 -cne $Artifact.ClosureSha256 -or
        $differences.Count -ne 0) {
        $differenceSummary = @(
            $differences |
                Select-Object -First 10 |
                ForEach-Object { "$($_.InputObject) $($_.SideIndicator)" }
        ) -join '; '
        if ([string]::IsNullOrWhiteSpace($differenceSummary)) {
            $differenceSummary = 'composite identity or file count differs'
        }
        throw (
            "Benchmark artifact closure changed $Context. " +
            "Expected $($Artifact.ClosureFileCount) files/$($Artifact.ClosureSha256); " +
            "found $($currentClosure.FileCount) files/$($currentClosure.Sha256). " +
            "Differences: $differenceSummary")
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

function Start-LocalEnvironmentMonitor {
    if (-not $MonitorLocalEnvironment) {
        return
    }
    if ($null -ne $environmentMonitorProcess) {
        throw 'The local performance environment monitor has already started.'
    }

    foreach ($reservedPath in @(
            $environmentMonitorCsvPath,
            $environmentMonitorReadyPath,
            $environmentMonitorStopPath,
            $environmentMonitorStdoutPath,
            $environmentMonitorStderrPath,
            $environmentMonitorSummaryPath)) {
        if (Test-Path -LiteralPath $reservedPath) {
            throw "Environment monitor evidence path already exists: $reservedPath"
        }
    }

    $pwshCommand = Get-Command pwsh -ErrorAction Stop
    $runnerProcess = [Diagnostics.Process]::GetCurrentProcess()
    try {
        $runnerStartedUtc = [DateTimeOffset] $runnerProcess.StartTime.ToUniversalTime()
    }
    finally {
        $runnerProcess.Dispose()
    }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $pwshCommand.Source
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            $EnvironmentMonitorScript,
            '-OutputPath',
            $environmentMonitorCsvPath,
            '-StopSignalPath',
            $environmentMonitorStopPath,
            '-ReadySignalPath',
            $environmentMonitorReadyPath,
            '-AllowedRootProcessId',
            $PID.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-AllowedRootStartTimeUtc',
            $runnerStartedUtc.ToString('O', [Globalization.CultureInfo]::InvariantCulture),
            '-SampleIntervalMilliseconds',
            $MonitorSampleIntervalMilliseconds.ToString(
                [Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalCpuPercent',
            $MaxExternalCpuPercent.ToString(
                [Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalCpuCoreEquivalent',
            $MaxExternalCpuCoreEquivalent.ToString(
                [Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalIoBytesPerSecond',
            $MaxExternalIoBytesPerSecond.ToString(
                [Globalization.CultureInfo]::InvariantCulture),
            '-RequiredConsecutiveBusySamples',
            $RequiredConsecutiveBusySamples.ToString(
                [Globalization.CultureInfo]::InvariantCulture),
            '-ProhibitedExternalProcessNames',
            $ProhibitedExternalProcessNames)) {
        [void] $startInfo.ArgumentList.Add([string] $argument)
    }

    $script:environmentMonitorProcess = [Diagnostics.Process]::new()
    $script:environmentMonitorProcess.StartInfo = $startInfo
    if (-not $script:environmentMonitorProcess.Start()) {
        throw 'Could not start the local performance environment monitor.'
    }

    $readyDeadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    while (-not (Test-Path -LiteralPath $environmentMonitorReadyPath -PathType Leaf) -and
        -not $environmentMonitorProcess.HasExited -and
        [DateTimeOffset]::UtcNow -lt $readyDeadline) {
        Start-Sleep -Milliseconds 100
    }
    if (-not (Test-Path -LiteralPath $environmentMonitorReadyPath -PathType Leaf)) {
        if (-not $environmentMonitorProcess.HasExited) {
            $environmentMonitorProcess.Kill($true)
            $environmentMonitorProcess.WaitForExit()
        }
        $stderr = $environmentMonitorProcess.StandardError.ReadToEnd()
        throw "Local performance environment monitor did not become ready. $stderr"
    }

    $readyText = [IO.File]::ReadAllText($environmentMonitorReadyPath).Trim()
    $parsedReadyUtc = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse(
        $readyText,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind,
        [ref] $parsedReadyUtc)) {
        throw "Environment monitor ready signal contains an invalid UTC timestamp: $readyText"
    }
    $script:environmentMonitorReadyUtc = $parsedReadyUtc.ToUniversalTime()
    Write-Host (
        'Durable-v3 external environment monitor ready at ' +
        $environmentMonitorReadyUtc.ToString('O'))
}

function Get-MaxEnvironmentMonitorGapMilliseconds {
    return [Math]::Max(
        5000.0,
        $MonitorSampleIntervalMilliseconds * 3.0)
}

function Get-LatestEnvironmentMonitorRow {
    if (-not $MonitorLocalEnvironment) {
        return $null
    }
    if ($null -eq $environmentMonitorProcess -or
        $environmentMonitorProcess.HasExited) {
        throw 'The local performance environment monitor exited before closeout.'
    }

    for ($attempt = 1; $attempt -le 5; $attempt++) {
        if (Test-Path -LiteralPath $environmentMonitorCsvPath -PathType Leaf) {
            [string[]] $lines = [IO.File]::ReadAllLines($environmentMonitorCsvPath)
            if ($lines.Count -ge 2) {
                try {
                    $latest = @(($lines[0], $lines[-1]) | ConvertFrom-Csv)[0]
                    if ([string] $latest.Contaminated -cnotin @('True', 'False')) {
                        throw 'The latest environment monitor sample is incomplete.'
                    }
                    $latestTimestamp = [DateTimeOffset]::MinValue
                    if (-not [DateTimeOffset]::TryParse(
                            [string] $latest.TimestampUtc,
                            [Globalization.CultureInfo]::InvariantCulture,
                            [Globalization.DateTimeStyles]::RoundtripKind,
                            [ref] $latestTimestamp)) {
                        throw 'The latest environment monitor sample has an invalid timestamp.'
                    }
                    $latestTimestamp = $latestTimestamp.ToUniversalTime()
                    $sampleAgeMilliseconds =
                        ([DateTimeOffset]::UtcNow - $latestTimestamp).TotalMilliseconds
                    if ($sampleAgeMilliseconds -lt 0 -or
                        $sampleAgeMilliseconds -gt (Get-MaxEnvironmentMonitorGapMilliseconds)) {
                        throw (
                            'The latest environment monitor sample is stale: ' +
                            "$sampleAgeMilliseconds ms old.")
                    }
                    return $latest
                }
                catch {
                    if ($attempt -eq 5) {
                        throw
                    }
                }
            }
        }
        Start-Sleep -Milliseconds 200
    }

    throw 'The local performance environment monitor produced no complete sample.'
}

function Assert-LocalEnvironmentMonitorClean {
    param([Parameter(Mandatory)][string] $Stage)

    if (-not $MonitorLocalEnvironment) {
        return
    }
    $latest = Get-LatestEnvironmentMonitorRow
    if ([string] $latest.Contaminated -ceq 'True') {
        throw (
            "Local performance environment contamination detected $Stage. " +
            "Timestamp=$($latest.TimestampUtc); reason=$($latest.BusyReason); " +
            "prohibited=$($latest.ProhibitedExternalProcesses); " +
            "externalCpu=$($latest.ExternalCpuPercent)%/" +
            "$($latest.ExternalCpuCoreEquivalent) core-equivalent; " +
            "systemResidualCpu=" +
            "$($latest.SystemResidualCpuCoreEquivalent) core-equivalent; " +
            "externalRead=$($latest.ExternalReadBytesPerSecond) bytes/sec; " +
            "externalWrite=$($latest.ExternalWriteBytesPerSecond) bytes/sec; " +
            "unobservableCpuProcesses=$($latest.UnobservableExternalCpuProcessCount); " +
            "unobservableIoProcesses=$($latest.UnobservableExternalIoProcessCount); " +
            "unobservableAllowedCpuProcesses=" +
            "$($latest.UnobservableAllowedCpuProcessCount).")
    }
}

function Stop-AndAuditLocalEnvironmentMonitor {
    if (-not $MonitorLocalEnvironment -or $environmentMonitorStopped) {
        return
    }
    $auditFailure = $null
    $summaryLines = [Collections.Generic.List[string]]::new()
    $summaryLines.Add('Schema=csharpdb-local-performance-environment-summary/v1')
    $summaryLines.Add("ReadyUtc=$($environmentMonitorReadyUtc.ToString('O'))")
    $summaryLines.Add("MaximumCoverageGapMilliseconds=$(Get-MaxEnvironmentMonitorGapMilliseconds)")
    $summaryLines.Add("MaxExternalCpuPercent=$MaxExternalCpuPercent")
    $summaryLines.Add("MaxExternalCpuCoreEquivalent=$MaxExternalCpuCoreEquivalent")
    $summaryLines.Add("MaxObservableExternalProcessIoBytesPerSecond=$MaxExternalIoBytesPerSecond")
    $summaryLines.Add("RequiredConsecutiveBusySamples=$RequiredConsecutiveBusySamples")
    $summaryLines.Add("ProhibitedExternalProcessNames=$ProhibitedExternalProcessNames")

    try {
        [IO.File]::WriteAllText(
            $environmentMonitorStopPath,
            [DateTimeOffset]::UtcNow.ToString('O'))
        if (-not $environmentMonitorProcess.WaitForExit(30000)) {
            $environmentMonitorProcess.Kill($true)
            $environmentMonitorProcess.WaitForExit()
            throw 'Local performance environment monitor did not stop within 30 seconds.'
        }
        $stdout = $environmentMonitorProcess.StandardOutput.ReadToEnd()
        $stderr = $environmentMonitorProcess.StandardError.ReadToEnd()
        [IO.File]::WriteAllText($environmentMonitorStdoutPath, $stdout)
        [IO.File]::WriteAllText($environmentMonitorStderrPath, $stderr)
        if ($environmentMonitorProcess.ExitCode -ne 0) {
            throw (
                "Local performance environment monitor exited with code " +
                "$($environmentMonitorProcess.ExitCode). $stderr")
        }
        if (-not (Test-Path -LiteralPath $environmentMonitorCsvPath -PathType Leaf)) {
            throw 'Local performance environment monitor CSV is missing.'
        }

        $expectedMonitorHeader =
            'TimestampUtc,IntervalMilliseconds,ExternalCpuPercent,' +
            'ExternalCpuCoreEquivalent,SystemResidualCpuCoreEquivalent,' +
            'ExternalReadBytesPerSecond,' +
            'ExternalWriteBytesPerSecond,ExternalProcessCount,AllowedProcessCount,' +
            'UnobservableAllowedCpuProcessCount,UnobservableExternalCpuProcessCount,' +
            'UnobservableExternalIoProcessCount,' +
            'ProhibitedExternalProcesses,BusyReason,ConsecutiveBusySamples,Contaminated'
        $actualMonitorHeader = Get-Content `
            -LiteralPath $environmentMonitorCsvPath `
            -TotalCount 1
        if ($actualMonitorHeader -cne $expectedMonitorHeader) {
            throw 'Local performance environment monitor CSV has an unexpected schema.'
        }

        $rows = @(Import-Csv -LiteralPath $environmentMonitorCsvPath)
        if ($rows.Count -eq 0) {
            throw 'Local performance environment monitor CSV contains no samples.'
        }
        $timestamps = [Collections.Generic.List[DateTimeOffset]]::new()
        $previousTimestamp = [DateTimeOffset]::MinValue
        $maximumGapMilliseconds = Get-MaxEnvironmentMonitorGapMilliseconds
        foreach ($row in $rows) {
            if ([string] $row.Contaminated -cnotin @('True', 'False')) {
                throw (
                    'Environment monitor contains an invalid contamination flag: ' +
                    "'$($row.Contaminated)'.")
            }
            $timestamp = [DateTimeOffset]::MinValue
            if (-not [DateTimeOffset]::TryParse(
                [string] $row.TimestampUtc,
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::RoundtripKind,
                [ref] $timestamp)) {
                throw "Environment monitor contains an invalid timestamp: $($row.TimestampUtc)"
            }
            $timestamp = $timestamp.ToUniversalTime()
            if ($previousTimestamp -ne [DateTimeOffset]::MinValue) {
                $gapMilliseconds = ($timestamp - $previousTimestamp).TotalMilliseconds
                if ($gapMilliseconds -le 0 -or
                    $gapMilliseconds -gt $maximumGapMilliseconds) {
                    throw (
                        'Environment monitor coverage is discontinuous: ' +
                        "$gapMilliseconds ms between $($previousTimestamp.ToString('O')) " +
                        "and $($timestamp.ToString('O')).")
                }
            }
            $timestamps.Add($timestamp)
            $previousTimestamp = $timestamp
        }
        $readyToFirstSampleGapMilliseconds =
            ($timestamps[0] - $environmentMonitorReadyUtc).TotalMilliseconds
        if ($readyToFirstSampleGapMilliseconds -le 0 -or
            $readyToFirstSampleGapMilliseconds -gt $maximumGapMilliseconds) {
            throw (
                'Environment monitor coverage is discontinuous between its ' +
                'ready signal and first sample: ' +
                "$readyToFirstSampleGapMilliseconds ms.")
        }
        $contaminatedRows = @($rows | Where-Object { [string] $_.Contaminated -ceq 'True' })
        if ($contaminatedRows.Count -gt 0) {
            $first = $contaminatedRows[0]
            throw (
                'Local performance environment evidence is contaminated: ' +
                "timestamp=$($first.TimestampUtc); reason=$($first.BusyReason); " +
                "prohibited=$($first.ProhibitedExternalProcesses); " +
                "externalCpu=$($first.ExternalCpuPercent)%/" +
                "$($first.ExternalCpuCoreEquivalent) core-equivalent; " +
                "systemResidualCpu=" +
                "$($first.SystemResidualCpuCoreEquivalent) core-equivalent; " +
                "externalRead=$($first.ExternalReadBytesPerSecond) bytes/sec; " +
                "externalWrite=$($first.ExternalWriteBytesPerSecond) bytes/sec; " +
                "unobservableCpuProcesses=$($first.UnobservableExternalCpuProcessCount); " +
                "unobservableIoProcesses=$($first.UnobservableExternalIoProcessCount); " +
                "unobservableAllowedCpuProcesses=" +
                "$($first.UnobservableAllowedCpuProcessCount).")
        }

        $measurementBegins = [Collections.Generic.List[DateTimeOffset]]::new()
        $measurementEnds = [Collections.Generic.List[DateTimeOffset]]::new()
        foreach ($rawRoot in $baselineRawResults, $candidateRawResults) {
            if (-not (Test-Path -LiteralPath $rawRoot -PathType Container)) {
                continue
            }
            foreach ($rawPath in Get-ChildItem -LiteralPath $rawRoot -File -Recurse -Filter '*.csv') {
                $rawRows = @(Import-Csv -LiteralPath $rawPath.FullName)
                if ($rawRows.Count -ne 1) {
                    continue
                }
                $extraInfo = [string] $rawRows[0].ExtraInfo
                $beginMatch = [regex]::Match(
                    $extraInfo,
                    '(?:^|;\s*)measurement-begin-utc=(?<value>[^;]+)')
                $endMatch = [regex]::Match(
                    $extraInfo,
                    '(?:^|;\s*)measurement-end-utc=(?<value>[^;]+)')
                if (-not $beginMatch.Success -or -not $endMatch.Success) {
                    continue
                }
                $begin = [DateTimeOffset]::Parse(
                    $beginMatch.Groups['value'].Value,
                    [Globalization.CultureInfo]::InvariantCulture,
                    [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
                $end = [DateTimeOffset]::Parse(
                    $endMatch.Groups['value'].Value,
                    [Globalization.CultureInfo]::InvariantCulture,
                    [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
                $measurementBegins.Add($begin)
                $measurementEnds.Add($end)
            }
        }
        if ($isExactMasterDurableMode -and
            $null -eq $primaryFailure -and
            $measurementBegins.Count -ne $executionPlan.Count) {
            throw (
                'Environment monitor closeout found ' +
                "$($measurementBegins.Count) declared measurement intervals; " +
                "$($executionPlan.Count) were predeclared.")
        }
        if ($measurementBegins.Count -gt 0) {
            $firstMeasurementBegin = @($measurementBegins | Sort-Object)[0]
            $lastMeasurementEnd = @($measurementEnds | Sort-Object)[-1]
            if ($environmentMonitorReadyUtc -gt $firstMeasurementBegin) {
                throw 'Environment monitor started after the first recorded measurement began.'
            }
            if ($timestamps[-1] -lt $lastMeasurementEnd) {
                throw 'Environment monitor stopped before the final recorded measurement ended.'
            }
            $summaryLines.Add("FirstMeasurementBeginUtc=$($firstMeasurementBegin.ToString('O'))")
            $summaryLines.Add("LastMeasurementEndUtc=$($lastMeasurementEnd.ToString('O'))")
            $summaryLines.Add("CoveredMeasurementIntervals=$($measurementBegins.Count)")
        }

        $script:environmentMonitorSampleCount = $rows.Count
        $script:environmentMonitorSha256 = (Get-FileHash `
                -LiteralPath $environmentMonitorCsvPath `
                -Algorithm SHA256).Hash.ToLowerInvariant()
        $summaryLines.Add('Result=PASS')
        $summaryLines.Add("SampleCount=$environmentMonitorSampleCount")
        $summaryLines.Add("FirstSampleUtc=$($timestamps[0].ToString('O'))")
        $summaryLines.Add("LastSampleUtc=$($timestamps[-1].ToString('O'))")
        $summaryLines.Add("TelemetrySha256=$environmentMonitorSha256")
    }
    catch {
        $auditFailure = $_
        $summaryLines.Add('Result=FAIL')
        $summaryLines.Add("Failure=$($_.Exception.Message -replace '\r?\n', ' ')")
    }
    finally {
        try {
            if ($null -ne $environmentMonitorProcess -and
                -not $environmentMonitorProcess.HasExited) {
                $environmentMonitorProcess.Kill($true)
                $environmentMonitorProcess.WaitForExit()
            }
            $script:environmentMonitorStopped =
                $null -ne $environmentMonitorProcess -and
                $environmentMonitorProcess.HasExited
        }
        catch {
            $script:environmentMonitorStopped = $false
            if ($null -eq $auditFailure) {
                $auditFailure = $_
                $summaryLines.Add('Result=FAIL')
            }
            $summaryLines.Add(
                "MonitorStopFailure=$($_.Exception.Message -replace '\r?\n', ' ')")
        }
        Write-LinesAtomically `
            -Path $environmentMonitorSummaryPath `
            -Lines $summaryLines.ToArray()
    }

    if (Test-Path -LiteralPath $reportPath -PathType Leaf) {
        Add-Content `
            -LiteralPath $reportPath `
            -Value @(
                '',
                '## External environment monitor closeout',
                '',
                $(if ($null -eq $auditFailure) {
                    '- Result: **PASS**'
                }
                else {
                    '- Result: **FAIL**'
                }),
                "- Evidence: ``$environmentMonitorCsvPath``",
                "- Summary: ``$environmentMonitorSummaryPath``",
                ("- Coverage: monitor ready before the first declared measurement; " +
                    "ready-to-first and inter-sample gaps no greater than " +
                    "$(Get-MaxEnvironmentMonitorGapMilliseconds) ms; final sample at or after the final declared measurement end"),
                ("- Busy limits: external CPU above $MaxExternalCpuPercent% or " +
                    "$MaxExternalCpuCoreEquivalent CPU-core equivalent, or observable " +
                    "external process I/O above $MaxExternalIoBytesPerSecond bytes/sec " +
                    "for $RequiredConsecutiveBusySamples consecutive samples"),
                "- Immediate prohibited-process contamination: ``$ProhibitedExternalProcessNames``",
                $(if (-not [string]::IsNullOrWhiteSpace($environmentMonitorSha256)) {
                    "- Evidence SHA-256: ``$environmentMonitorSha256``"
                }),
                $(if ($environmentMonitorSampleCount -gt 0) {
                    "- Samples: $environmentMonitorSampleCount"
                })
            )
    }

    if ($null -ne $auditFailure) {
        throw $auditFailure
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

function Write-PairedBenchmarkArtifactManifest {
    param(
        [Parameter(Mandatory)]
        [Collections.IDictionary] $Artifacts,

        [Parameter(Mandatory)]
        [string] $PreviousCommit,

        [Parameter(Mandatory)]
        [string] $CandidateCommit,

        [Parameter(Mandatory)]
        [string] $ManifestPath,

        [switch] $SharedSameRevisionArtifact,

        [switch] $SymmetricStaging
    )

    if ($Artifacts.Count -ne 2 -or
        -not $Artifacts.Contains('previous') -or
        -not $Artifacts.Contains('candidate')) {
        throw 'Paired benchmark artifact identity requires previous and candidate entries.'
    }

    $previousArtifact = $Artifacts['previous']
    $candidateArtifact = $Artifacts['candidate']
    foreach ($entry in @(
            [pscustomobject]@{ Name = 'previous'; Artifact = $previousArtifact },
            [pscustomobject]@{ Name = 'candidate'; Artifact = $candidateArtifact })) {
        Assert-BenchmarkArtifactHash `
            -ArtifactPath $entry.Artifact.Path `
            -ExpectedSha256 $entry.Artifact.Sha256 `
            -Context "before $($entry.Name) manifest persistence"
        Assert-BenchmarkArtifactClosure `
            -Artifact $entry.Artifact `
            -Context "before $($entry.Name) manifest persistence"
    }

    $sharingValue = if ($SharedSameRevisionArtifact) { 'true' } else { 'false' }
    $manifestFormatVersion = if ($SymmetricStaging) {
        'csharpdb-paired-benchmark-artifacts/v3'
    }
    else {
        'csharpdb-paired-benchmark-artifacts/v2'
    }
    [string[]] $manifestLines = @(
        "FormatVersion=$manifestFormatVersion"
        "SharedSameRevisionArtifact=$sharingValue"
        "SymmetricStaging=$(if ($SymmetricStaging) { 'true' } else { 'false' })"
        $(if ($SymmetricStaging) {
                "DesignSha256=$exactMasterDurableDesignFingerprint"
            })
        $(if ($SymmetricStaging) {
                "ScheduleSha256=$exactMasterDurableScheduleSha256"
            })
        'ClosureDefinition=all files recursively under the entry DLL directory, sorted by normalized relative path'
        'ClosureExclusion=top-level directory segment results'
        'ClosureExclusion=top-level directory segment CSharpDB.Benchmarks-Job-*'
        "PreviousCommit=$PreviousCommit"
        $(if ($SymmetricStaging) {
                "PreviousSourceArtifactPath=$($previousArtifact.SourceArtifactPath)"
            })
        $(if ($SymmetricStaging) {
                "PreviousStagingSlot=$($previousArtifact.StagingSlot)"
            })
        $(if ($SymmetricStaging) {
                "PreviousStagedReadFileCount=$($previousArtifact.StagedReadFileCount)"
            })
        "PreviousArtifactPath=$($previousArtifact.Path)"
        "PreviousArtifactSha256=$($previousArtifact.Sha256)"
        "PreviousClosureRoot=$($previousArtifact.ClosureRoot)"
        "PreviousClosureFileCount=$($previousArtifact.ClosureFileCount)"
        "PreviousClosureSha256=$($previousArtifact.ClosureSha256)"
        foreach ($record in $previousArtifact.ClosureRecords) {
            "PreviousClosureFile=$($record.Sha256) *$($record.RelativePath)"
        }
        "CandidateCommit=$CandidateCommit"
        $(if ($SymmetricStaging) {
                "CandidateSourceArtifactPath=$($candidateArtifact.SourceArtifactPath)"
            })
        $(if ($SymmetricStaging) {
                "CandidateStagingSlot=$($candidateArtifact.StagingSlot)"
            })
        $(if ($SymmetricStaging) {
                "CandidateStagedReadFileCount=$($candidateArtifact.StagedReadFileCount)"
            })
        "CandidateArtifactPath=$($candidateArtifact.Path)"
        "CandidateArtifactSha256=$($candidateArtifact.Sha256)"
        "CandidateClosureRoot=$($candidateArtifact.ClosureRoot)"
        "CandidateClosureFileCount=$($candidateArtifact.ClosureFileCount)"
        "CandidateClosureSha256=$($candidateArtifact.ClosureSha256)"
        foreach ($record in $candidateArtifact.ClosureRecords) {
            "CandidateClosureFile=$($record.Sha256) *$($record.RelativePath)"
        }
    )
    Write-LinesAtomically -Path $ManifestPath -Lines $manifestLines

    $persistedLines = [IO.File]::ReadAllLines($ManifestPath)
    if ($persistedLines.Count -ne $manifestLines.Count) {
        throw 'Paired benchmark artifact manifest persistence verification failed.'
    }
    for ($lineIndex = 0; $lineIndex -lt $manifestLines.Count; $lineIndex++) {
        if ($persistedLines[$lineIndex] -cne $manifestLines[$lineIndex]) {
            throw 'Paired benchmark artifact manifest persistence verification failed.'
        }
    }

    foreach ($entry in @(
            [pscustomobject]@{ Name = 'previous'; Artifact = $previousArtifact },
            [pscustomobject]@{ Name = 'candidate'; Artifact = $candidateArtifact })) {
        Assert-BenchmarkArtifactHash `
            -ArtifactPath $entry.Artifact.Path `
            -ExpectedSha256 $entry.Artifact.Sha256 `
            -Context "after $($entry.Name) manifest persistence"
        Assert-BenchmarkArtifactClosure `
            -Artifact $entry.Artifact `
            -Context "after $($entry.Name) manifest persistence"
    }
    return $manifestLines
}

function Assert-PairedBenchmarkArtifactCloseout {
    param(
        [Parameter(Mandatory)]
        [Collections.IDictionary] $Artifacts,

        [Parameter(Mandatory)]
        [string] $ManifestPath,

        [Parameter(Mandatory)]
        [string[]] $ExpectedManifestLines
    )

    $integrityFailures = [Collections.Generic.List[string]]::new()
    try {
        if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) {
            throw "Paired benchmark artifact manifest is missing: $ManifestPath"
        }
        $actualManifestLines = [IO.File]::ReadAllLines($ManifestPath)
        if ($actualManifestLines.Count -ne $ExpectedManifestLines.Count) {
            throw (
                'Paired benchmark artifact manifest changed before closeout. ' +
                "Expected $($ExpectedManifestLines.Count) lines; " +
                "found $($actualManifestLines.Count).")
        }
        for ($lineIndex = 0; $lineIndex -lt $ExpectedManifestLines.Count; $lineIndex++) {
            if ($actualManifestLines[$lineIndex] -cne $ExpectedManifestLines[$lineIndex]) {
                throw (
                    'Paired benchmark artifact manifest changed before closeout at ' +
                    "line $($lineIndex + 1).")
            }
        }
    }
    catch {
        $integrityFailures.Add($_.Exception.Message)
    }

    foreach ($revision in @('previous', 'candidate')) {
        if (-not $Artifacts.Contains($revision)) {
            $integrityFailures.Add("Paired benchmark artifact identity is missing '$revision'.")
            continue
        }
        $artifact = $Artifacts[$revision]
        try {
            Assert-BenchmarkArtifactHash `
                -ArtifactPath $artifact.Path `
                -ExpectedSha256 $artifact.Sha256 `
                -Context "at $revision qualification closeout"
        }
        catch {
            $integrityFailures.Add($_.Exception.Message)
        }
        try {
            Assert-BenchmarkArtifactClosure `
                -Artifact $artifact `
                -Context "at $revision qualification closeout"
        }
        catch {
            $integrityFailures.Add($_.Exception.Message)
        }
    }

    if ($integrityFailures.Count -ne 0) {
        throw (
            'Paired benchmark artifact closeout integrity failed: ' +
            ($integrityFailures -join ' | '))
    }
}

function Write-PairedBenchmarkArtifactCloseoutEvidence {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('PASS', 'FAIL')]
        [string] $Result,

        [Parameter(Mandatory)]
        [string] $Detail
    )

    $safeDetail = $Detail.
        Replace("`r", ' ').
        Replace("`n", ' ')
    $timestamp = [DateTimeOffset]::UtcNow.ToString(
        'o',
        [Globalization.CultureInfo]::InvariantCulture)
    Write-LinesAtomically `
        -Path $pairedArtifactCloseoutPath `
        -Lines @(
            'FormatVersion=csharpdb-paired-benchmark-artifact-closeout/v1',
            "Result=$Result",
            "TimestampUtc=$timestamp",
            "ArtifactManifest=$pairedArtifactManifestPath",
            "Detail=$safeDetail"
        )
    $summary = (
        "- Paired benchmark artifact closeout: **$Result**; " +
        "evidence ``$pairedArtifactCloseoutPath``; $safeDetail")
    Add-Content -LiteralPath $preflightPath -Value $summary
    if (Test-Path -LiteralPath $reportPath -PathType Leaf) {
        Add-Content -LiteralPath $reportPath -Value $summary
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

function Get-SuiteResultPrefix {
    param([Parameter(Mandatory)][object] $Suite)

    if ($Suite.PSObject.Properties.Name -contains 'ResultPrefix' -and
        -not [string]::IsNullOrWhiteSpace([string] $Suite.ResultPrefix)) {
        return [string] $Suite.ResultPrefix
    }
    return [string] $Suite.Name
}

function Assert-DurableV3RawRow {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][string] $ExpectedRowName
    )

    $expectedHeader =
        'Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo'
    $header = Get-Content -LiteralPath $Path -TotalCount 1
    if ($header -cne $expectedHeader) {
        throw "Durable-v3 raw evidence has an unexpected CSV schema: $Path"
    }
    $rows = @(Import-Csv -LiteralPath $Path)
    if ($rows.Count -ne 1) {
        throw (
            "Durable-v3 exact row '$ExpectedRowName' produced $($rows.Count) " +
            'CSV rows; expected exactly one.')
    }
    $row = $rows[0]
    if (-not [string]::Equals(
            [string] $row.Name,
            $ExpectedRowName,
            [StringComparison]::Ordinal)) {
        throw (
            "Durable-v3 expected row '$ExpectedRowName' but received " +
            "'$($row.Name)'.")
    }

    [decimal] $elapsedMilliseconds = 0
    if (-not [decimal]::TryParse(
            [string] $row.ElapsedMs,
            [Globalization.NumberStyles]::Float,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref] $elapsedMilliseconds) -or
        $elapsedMilliseconds -lt 30000 -or
        $elapsedMilliseconds -gt 120000) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' must declare between " +
            "30000 and 120000 elapsed milliseconds; received '$($row.ElapsedMs)'.")
    }
    [long] $latencySamples = 0
    if (-not [long]::TryParse(
            [string] $row.LatencySamples,
            [Globalization.NumberStyles]::Integer,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref] $latencySamples) -or
        $latencySamples -lt 10000) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' must retain at least " +
            "10000 latency samples; received '$($row.LatencySamples)'.")
    }

    $metadata = [Collections.Generic.Dictionary[string, string]]::new(
        [StringComparer]::Ordinal)
    foreach ($tokenText in ([string] $row.ExtraInfo).Split(';')) {
        $token = $tokenText.Trim()
        if ([string]::IsNullOrWhiteSpace($token)) {
            continue
        }
        $equalsIndex = $token.IndexOf('=')
        if ($equalsIndex -le 0) {
            continue
        }
        $name = $token.Substring(0, $equalsIndex).Trim()
        $value = $token.Substring($equalsIndex + 1).Trim()
        if ($metadata.ContainsKey($name)) {
            throw (
                "Durable-v3 raw row '$ExpectedRowName' contains duplicate " +
                "policy metadata '$name'.")
        }
        $metadata.Add($name, $value)
    }
    $requiredValues = [ordered]@{
        'qualification' = 'true'
        'unrecorded-warmup-seconds' = '2'
        'minimum-measured-seconds' = '30'
        'minimum-retained-latency-samples' = '10000'
        'measurement-cap-seconds' = '120'
    }
    foreach ($requiredName in $requiredValues.Keys) {
        if (-not $metadata.ContainsKey($requiredName) -or
            $metadata[$requiredName] -cne $requiredValues[$requiredName]) {
            throw (
                "Durable-v3 raw row '$ExpectedRowName' must declare " +
                "'$requiredName=$($requiredValues[$requiredName])'.")
        }
    }

    foreach ($timestampName in @('measurement-begin-utc', 'measurement-end-utc')) {
        if (-not $metadata.ContainsKey($timestampName) -or
            [string]::IsNullOrWhiteSpace($metadata[$timestampName])) {
            throw (
                "Durable-v3 raw row '$ExpectedRowName' must declare " +
                "'$timestampName'.")
        }
    }
    [DateTimeOffset] $measurementBegin = [DateTimeOffset]::MinValue
    [DateTimeOffset] $measurementEnd = [DateTimeOffset]::MinValue
    $dateStyles = [Globalization.DateTimeStyles]::RoundtripKind
    if (-not [DateTimeOffset]::TryParseExact(
            $metadata['measurement-begin-utc'],
            'O',
            [Globalization.CultureInfo]::InvariantCulture,
            $dateStyles,
            [ref] $measurementBegin) -or
        $measurementBegin.Offset -ne [TimeSpan]::Zero) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' has an invalid UTC " +
            "measurement-begin-utc value '$($metadata['measurement-begin-utc'])'.")
    }
    if (-not [DateTimeOffset]::TryParseExact(
            $metadata['measurement-end-utc'],
            'O',
            [Globalization.CultureInfo]::InvariantCulture,
            $dateStyles,
            [ref] $measurementEnd) -or
        $measurementEnd.Offset -ne [TimeSpan]::Zero) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' has an invalid UTC " +
            "measurement-end-utc value '$($metadata['measurement-end-utc'])'.")
    }
    $declaredMeasurementMilliseconds =
        ($measurementEnd - $measurementBegin).TotalMilliseconds
    if ($declaredMeasurementMilliseconds -lt 30000 -or
        $declaredMeasurementMilliseconds -gt 120000) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' declares a measurement " +
            'interval outside the 30-to-120-second policy bounds.')
    }
    if ([Math]::Abs(
            [double] $declaredMeasurementMilliseconds -
            [double] $elapsedMilliseconds) -gt 1.0) {
        throw (
            "Durable-v3 raw row '$ExpectedRowName' elapsed time does not " +
            'match its declared UTC measurement interval.')
    }

    return [pscustomobject]@{
        Row = $row
        ElapsedMilliseconds = $elapsedMilliseconds
        LatencySamples = $latencySamples
        MeasurementBeginUtc = $measurementBegin.ToString(
            'O',
            [Globalization.CultureInfo]::InvariantCulture)
        MeasurementEndUtc = $measurementEnd.ToString(
            'O',
            [Globalization.CultureInfo]::InvariantCulture)
    }
}

function Invoke-InterSampleQuiescence {
    param(
        [Parameter(Mandatory)][int] $Ordinal,
        [Parameter(Mandatory)][string] $Suite,
        [Parameter(Mandatory)][string] $Revision
    )

    if ($InterSampleQuiescenceSeconds -gt 0) {
        $timestamp = [DateTimeOffset]::UtcNow.ToString(
            'O',
            [Globalization.CultureInfo]::InvariantCulture)
        Add-Content `
            -LiteralPath (Join-Path $logRoot 'durable-v3-inter-sample-quiescence.log') `
            -Value (
                "$timestamp|$Ordinal|$Suite|$Revision|" +
                "$InterSampleQuiescenceSeconds")
        Start-Sleep -Seconds $InterSampleQuiescenceSeconds
    }
    Assert-LocalEnvironmentMonitorClean `
        -Stage "before logical side $Ordinal ($Suite/$Revision)"
}

function Invoke-DurableV3ArtifactConditioning {
    param(
        [Parameter(Mandatory)][string] $Revision,
        [Parameter(Mandatory)][object] $ArtifactIdentity,
        [Parameter(Mandatory)][object] $Suite
    )

    $resolvedArtifactPath = [IO.Path]::GetFullPath([string] $ArtifactIdentity.Path)
    $resultRoot = [IO.Path]::GetFullPath([string] $ArtifactIdentity.ClosureRoot)
    $resultPrefix = Get-SuiteResultPrefix -Suite $Suite
    $resultPattern = "$resultPrefix-*.csv"
    $existingResults = @(
        Get-ChildItem `
            -LiteralPath $resultRoot `
            -File `
            -Recurse `
            -Filter $resultPattern
    )
    if ($existingResults.Count -ne 0) {
        throw (
            "Durable-v3 conditioning for '$Revision' found " +
            "$($existingResults.Count) pre-existing output file(s).")
    }

    $logPath = Join-Path $logRoot "durable-v3-conditioning-$Revision.log"
    Add-Content -LiteralPath $logPath -Value @(
        "=== NON-RECORDED ARTIFACT CONDITIONING / $Revision ==="
        "Artifact: $resolvedArtifactPath"
        "Slot: $($ArtifactIdentity.StagingSlot)"
        "Arguments: $($Suite.Arguments -join ' ')"
        'RecordedQualificationEvidence: false'
        '')
    Push-Location ([string] $ArtifactIdentity.SourceRoot)
    try {
        Assert-BenchmarkArtifactHash `
            -ArtifactPath $resolvedArtifactPath `
            -ExpectedSha256 $ArtifactIdentity.Sha256 `
            -Context 'before non-recorded conditioning'
        Assert-BenchmarkArtifactClosure `
            -Artifact $ArtifactIdentity `
            -Context 'before non-recorded conditioning'
        try {
            & dotnet $resolvedArtifactPath `
                @($Suite.Arguments) `
                --repeat 1 `
                --warmup-single-sample `
                --repro 2>&1 |
                    Tee-Object -FilePath $logPath -Append |
                    Write-Host
            $conditioningExitCode = $LASTEXITCODE
        }
        finally {
            Assert-BenchmarkArtifactHash `
                -ArtifactPath $resolvedArtifactPath `
                -ExpectedSha256 $ArtifactIdentity.Sha256 `
                -Context 'after non-recorded conditioning'
            Assert-BenchmarkArtifactClosure `
                -Artifact $ArtifactIdentity `
                -Context 'after non-recorded conditioning'
        }
    }
    finally {
        Pop-Location
    }
    if ($conditioningExitCode -ne 0) {
        throw "Durable-v3 non-recorded conditioning failed for '$Revision'."
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
            "Durable-v3 conditioning for '$Revision' produced " +
            "$($results.Count) output files; expected exactly one.")
    }
    $measurement = Assert-DurableV3RawRow `
        -Path $results[0].FullName `
        -ExpectedRowName $Suite.ExpectedRowName
    $conditioningDirectory = Join-Path $logRoot 'conditioning'
    New-Item -ItemType Directory -Path $conditioningDirectory -Force | Out-Null
    $destinationPath = Join-Path $conditioningDirectory "$Revision.csv"
    if (Test-Path -LiteralPath $destinationPath) {
        throw "Durable-v3 conditioning evidence already exists: $destinationPath"
    }
    Copy-Item -LiteralPath $results[0].FullName -Destination $destinationPath
    $sourceHash = (Get-FileHash `
            -LiteralPath $results[0].FullName `
            -Algorithm SHA256).Hash.ToLowerInvariant()
    $destinationHash = (Get-FileHash `
            -LiteralPath $destinationPath `
            -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($destinationHash -cne $sourceHash) {
        throw "Durable-v3 conditioning evidence copy verification failed: $destinationPath"
    }
    Remove-Item -LiteralPath $results[0].FullName -Force
    Add-Content `
        -LiteralPath $exactMasterDurableConditioningPath `
        -Value ((@(
                    $Revision
                    $ArtifactIdentity.StagingSlot
                    $resolvedArtifactPath
                    $Suite.ExpectedRowName
                    $destinationPath
                    $destinationHash
                    $measurement.MeasurementBeginUtc
                    $measurement.MeasurementEndUtc
                    'false') |
                ForEach-Object { Convert-ToCsvCell $_ }) -join ',')
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

        [Parameter(Mandatory)]
        [object] $ArtifactIdentity,

        [Parameter(Mandatory)]
        [string] $ArtifactPath,

        [Parameter(Mandatory)]
        [string] $ExpectedArtifactSha256
    )

    if ([string]::IsNullOrWhiteSpace($ArtifactPath) -or
        [string]::IsNullOrWhiteSpace($ExpectedArtifactSha256)) {
        throw 'Direct benchmark execution requires both an artifact path and SHA-256.'
    }
    if ($ExpectedArtifactSha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'Direct benchmark execution requires a lowercase SHA-256 identity.'
    }
    $resolvedArtifactPath = [IO.Path]::GetFullPath($ArtifactPath)
    if (-not $resolvedArtifactPath.Equals(
            [IO.Path]::GetFullPath([string] $ArtifactIdentity.Path),
            $pathComparison) -or
        $ExpectedArtifactSha256 -cne [string] $ArtifactIdentity.Sha256) {
        throw 'Paired benchmark artifact parameters do not match the captured closure identity.'
    }
    $nativeArguments = [string[]] @($Suite.Arguments)
    $logPath = Join-Path $logRoot "$RunName.log"
    $resultRoot = [IO.Path]::GetFullPath([string] $ArtifactIdentity.ClosureRoot)
    $resultPrefix = Get-SuiteResultPrefix -Suite $Suite
    $resultPattern = "$resultPrefix-*.csv"
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
        "Direct artifact: $resolvedArtifactPath",
        "Expected artifact SHA-256: $ExpectedArtifactSha256",
        "Expected runnable closure file count: $($ArtifactIdentity.ClosureFileCount)",
        "Expected runnable closure SHA-256: $($ArtifactIdentity.ClosureSha256)",
        "Arguments: $($nativeArguments -join ' ')",
        ''
    )
    Push-Location $SourceRoot
    try {
        Assert-BenchmarkArtifactHash `
            -ArtifactPath $resolvedArtifactPath `
            -ExpectedSha256 $ExpectedArtifactSha256 `
            -Context 'before invocation'
        Assert-BenchmarkArtifactClosure `
            -Artifact $ArtifactIdentity `
            -Context 'before invocation'
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
            Assert-BenchmarkArtifactHash `
                -ArtifactPath $resolvedArtifactPath `
                -ExpectedSha256 $ExpectedArtifactSha256 `
                -Context 'after invocation'
            Assert-BenchmarkArtifactClosure `
                -Artifact $ArtifactIdentity `
                -Context 'after invocation'
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
    if ($isExactMasterDurableMode) {
        Assert-DurableV3RawRow `
            -Path $results[0].FullName `
            -ExpectedRowName $Suite.ExpectedRowName |
            Out-Null
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
    # A fresh CLI home must not initiate workload maintenance or mutate user state.
    $env:DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE = 'true'
    $env:DOTNET_SKIP_WORKLOAD_INTEGRITY_CHECK = 'true'
    $env:DOTNET_GENERATE_ASPNET_CERTIFICATE = 'false'
    $env:DOTNET_ADD_GLOBAL_TOOLS_TO_PATH = 'false'
    $env:DOTNET_NOLOGO = 'true'
    $env:DOTNET_CLI_TELEMETRY_OPTOUT = 'true'
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

    & git -C $repositoryRoot -c core.longpaths=true `
        worktree add --detach $baselineWorktree $previousCommit
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the previous-release worktree."
    }
    $baselineAdded = $true

    & git -C $repositoryRoot -c core.longpaths=true `
        worktree add --detach $candidateWorktree $candidateCommit
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
        $pairedArtifacts['previous'] = $sharedArtifact
        $pairedArtifacts['candidate'] = $sharedArtifact
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
                if ($Paired) {
                    $pairedArtifacts['previous'] = Get-BenchmarkArtifactIdentity `
                        -SourceRoot $baselineWorktree
                }
            }
            else {
                Invoke-BenchmarkBuild `
                    -SourceRoot $candidateWorktree `
                    -RunName 'candidate' `
                    -HarnessIdentity $candidateHarnessIdentity `
                    -BuildInputsIdentity $candidateBuildInputsIdentity `
                    -BuildInputsManifestPath $candidateBuildInputsManifestPath
                if ($Paired) {
                    $pairedArtifacts['candidate'] = Get-BenchmarkArtifactIdentity `
                        -SourceRoot $candidateWorktree
                }
            }
        }
    }
    if ($Paired) {
        if ($pairedArtifacts.Count -ne 2 -or
            -not $pairedArtifacts.ContainsKey('previous') -or
            -not $pairedArtifacts.ContainsKey('candidate')) {
            throw 'Paired benchmark builds did not produce both revision artifacts.'
        }
        if ($isExactMasterDurableMode) {
            $builtArtifacts = $pairedArtifacts
            $stagedArtifacts = @{}
            New-Item `
                -ItemType Directory `
                -Path $artifactStageParent `
                -Force |
                Out-Null
            foreach ($revision in @('previous', 'candidate')) {
                $slotName = [string] $revisionArtifactSlots[$revision]
                $slotRoot = Join-Path $artifactStageParent $slotName
                $stagedArtifacts[$revision] =
                    Copy-BenchmarkArtifactToSymmetricStage `
                        -Artifact $builtArtifacts[$revision] `
                        -StageRoot $slotRoot `
                        -SlotName $slotName
            }
            $previousStagedArtifact = $stagedArtifacts['previous']
            $candidateStagedArtifact = $stagedArtifacts['candidate']
            if ($previousStagedArtifact.Path.Length -ne
                    $candidateStagedArtifact.Path.Length -or
                $previousStagedArtifact.ClosureRoot.Length -ne
                    $candidateStagedArtifact.ClosureRoot.Length) {
                throw (
                    'Durable-v3 symmetric staging produced unequal execution ' +
                    'path lengths for previous and candidate artifacts.')
            }
            $pairedArtifacts = $stagedArtifacts
        }
        $pairedArtifactManifestLines = [string[]] @(
            Write-PairedBenchmarkArtifactManifest `
                -Artifacts $pairedArtifacts `
                -PreviousCommit $previousCommit `
                -CandidateCommit $candidateCommit `
                -ManifestPath $pairedArtifactManifestPath `
                -SharedSameRevisionArtifact:$ShareSameRevisionArtifact `
                -SymmetricStaging:$isExactMasterDurableMode
        )
        $pairedArtifactManifestPersisted = $true
        $previousArtifact = $pairedArtifacts['previous']
        $candidateArtifact = $pairedArtifacts['candidate']
        Add-Content `
            -LiteralPath $preflightPath `
            -Value @(
                "- Previous benchmark artifact execution path: ``$($previousArtifact.Path)``",
                "- Previous benchmark artifact SHA-256: ``$($previousArtifact.Sha256)``",
                "- Previous runnable closure: $($previousArtifact.ClosureFileCount) files; SHA-256 ``$($previousArtifact.ClosureSha256)``",
                "- Candidate benchmark artifact execution path: ``$($candidateArtifact.Path)``",
                "- Candidate benchmark artifact SHA-256: ``$($candidateArtifact.Sha256)``",
                "- Candidate runnable closure: $($candidateArtifact.ClosureFileCount) files; SHA-256 ``$($candidateArtifact.ClosureSha256)``",
                "- Paired benchmark artifact manifest: ``$pairedArtifactManifestPath``",
                $(if ($isExactMasterDurableMode) {
                        '- Benchmark artifacts execute from equal-length sibling staging slots retained with the evidence.'
                    }
                    else {
                        '- Benchmark artifact paths identify detached execution worktrees and may not exist after cleanup.'
                    })
            )
        Write-Host "Paired benchmark artifact identities persisted: $pairedArtifactManifestPath"
    }
    if ($isExactMasterDurableMode) {
        [string[]] $conditioningHeader = @(
            'Revision,ArtifactSlot,ArtifactPath,Scenario,ConditioningOutput,Sha256,MeasurementBeginUtc,MeasurementEndUtc,Recorded')
        Write-LinesAtomically `
            -Path $exactMasterDurableConditioningPath `
            -Lines $conditioningHeader
        Assert-PersistedLines `
            -Path $exactMasterDurableConditioningPath `
            -ExpectedLines $conditioningHeader `
            -Description 'Durable-v3 conditioning manifest header'
        foreach ($revision in $revisionOrder) {
            Invoke-DurableV3ArtifactConditioning `
                -Revision $revision `
                -ArtifactIdentity $pairedArtifacts[$revision] `
                -Suite $suiteDefinitions[0]
        }
        if ([IO.File]::ReadAllLines($exactMasterDurableConditioningPath).Count -ne 3) {
            throw 'Durable-v3 must retain exactly one conditioning record per artifact.'
        }
        if ($InterSampleQuiescenceSeconds -gt 0) {
            [IO.File]::WriteAllLines(
                (Join-Path $logRoot 'durable-v3-inter-sample-quiescence.log'),
                @('TimestampUtc|Ordinal|Suite|Revision|WaitSeconds'))
        }
    }
    Invoke-PostBuildQuiescence
    Start-LocalEnvironmentMonitor

    $executionOrdinal = 0
    if ($Paired -and $isExactMasterDurableMode) {
        $suiteEvidence = @{}
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
            $suiteEvidence[$suite.Name] = [pscustomobject]@{
                Suite = $suite
                BaselineAggregatePath = $baselineAggregatePath
                CandidateAggregatePath = $candidateAggregatePath
                BaselineRawPaths = [Collections.Generic.List[string]]::new()
                CandidateRawPaths = [Collections.Generic.List[string]]::new()
            }
        }

        foreach ($scheduledPair in $exactMasterDurablePairSchedule) {
            $suite = $scheduledPair.Suite
            $pair = $scheduledPair.Pair
            $pairRawPaths = @{}
            $pairRevisionOrder = @($pair.FirstRevision, $pair.SecondRevision)
            for ($position = 0; $position -lt $pairRevisionOrder.Count; $position++) {
                $revision = $pairRevisionOrder[$position]
                $nextExecutionOrdinal = $executionOrdinal + 1
                Invoke-InterSampleQuiescence `
                    -Ordinal $nextExecutionOrdinal `
                    -Suite $suite.Name `
                    -Revision $revision
                $executionOrdinal = $nextExecutionOrdinal
                $artifactIdentity = $pairedArtifacts[$revision]
                $sourceRoot = [string] $artifactIdentity.SourceRoot
                $eventDetail = (
                    "PairId=$($pair.Id);Order=$($pair.Order);" +
                    "PairRound=$($scheduledPair.PairRound);" +
                    "RowTimePosition=$($scheduledPair.RowTimePosition);" +
                    "RotationOffset=$($scheduledPair.RotationOffset);" +
                    "Position=$($position + 1);" +
                    "InterSampleQuiescenceSeconds=$InterSampleQuiescenceSeconds;" +
                    "ArtifactSlot=$($artifactIdentity.StagingSlot);" +
                    "SourceRoot=$sourceRoot;" +
                    "ArtifactPath=$($artifactIdentity.Path);" +
                    "ArtifactSha256=$($artifactIdentity.Sha256);" +
                    "ClosureFileCount=$($artifactIdentity.ClosureFileCount);" +
                    "ClosureSha256=$($artifactIdentity.ClosureSha256)")
                Write-ExecutionEvent `
                    -Ordinal $executionOrdinal `
                    -Suite $suite.Name `
                    -Revision $revision `
                    -State 'START' `
                    -Detail $eventDetail
                try {
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
                        ArtifactIdentity = $artifactIdentity
                        ArtifactPath = [string] $artifactIdentity.Path
                        ExpectedArtifactSha256 = [string] $artifactIdentity.Sha256
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
                throw (
                    "Durable-v3 exact row '$($suite.Name)' pair " +
                    "'$($pair.Id)' is incomplete; recorded evidence is never replaced.")
            }
            $baselineRawPath = [IO.Path]::GetFullPath(
                [string] $pairRawPaths['previous'])
            $candidateRawPath = [IO.Path]::GetFullPath(
                [string] $pairRawPaths['candidate'])
            $suiteEvidence[$suite.Name].BaselineRawPaths.Add($baselineRawPath)
            $suiteEvidence[$suite.Name].CandidateRawPaths.Add($candidateRawPath)
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

        foreach ($suite in $suiteDefinitions) {
            $evidence = $suiteEvidence[$suite.Name]
            if ($evidence.BaselineRawPaths.Count -ne $pairDefinitions.Count -or
                $evidence.CandidateRawPaths.Count -ne $pairDefinitions.Count) {
                throw (
                    "Durable-v3 exact row '$($suite.Name)' retained an " +
                    'unexpected number of raw samples; no discard or replacement is allowed.')
            }
            Write-AggregatedBenchmarkCsv `
                -RawPaths $evidence.BaselineRawPaths.ToArray() `
                -DestinationPath $evidence.BaselineAggregatePath
            Write-AggregatedBenchmarkCsv `
                -RawPaths $evidence.CandidateRawPaths.ToArray() `
                -DestinationPath $evidence.CandidateAggregatePath
        }
    }
    elseif ($Paired) {
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
                    $artifactIdentity = $pairedArtifacts[$revision]
                    $sourceRoot = [string] $artifactIdentity.SourceRoot
                    $eventDetail = (
                        "PairId=$($pair.Id);Order=$($pair.Order);" +
                        "Position=$($position + 1);" +
                        "SourceRoot=$sourceRoot;" +
                        "ArtifactPath=$($artifactIdentity.Path);" +
                        "ArtifactSha256=$($artifactIdentity.Sha256);" +
                        "ClosureFileCount=$($artifactIdentity.ClosureFileCount);" +
                        "ClosureSha256=$($artifactIdentity.ClosureSha256)")
                    Write-ExecutionEvent `
                        -Ordinal $executionOrdinal `
                        -Suite $suite.Name `
                        -Revision $revision `
                        -State 'START' `
                        -Detail $eventDetail
                    try {
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
                            ArtifactIdentity = $artifactIdentity
                            ArtifactPath = [string] $artifactIdentity.Path
                            ExpectedArtifactSha256 = [string] $artifactIdentity.Sha256
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
            BlockingLatencyPercentile = $BlockingLatencyPercentile
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
                $(if ($isExactMasterDurableMode) {
                        "- Canonical exact-master durable mode: ``$exactMasterDurableSelector`` (10 exact one-row suites)"
                    }),
                $(if ($isExactMasterDurableMode) {
                        '- Recorded evidence policy: no discard, no replacement, and one predeclared attempt per logical side'
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Inter-sample quiescence before every recorded logical side: $InterSampleQuiescenceSeconds second(s)"
                    }),
                $(if ($isExactMasterDurableMode) {
                        if ($MonitorLocalEnvironment) {
                            "- External environment monitor evidence: ``$environmentMonitorCsvPath``; closeout ``$environmentMonitorSummaryPath``"
                        }
                        else {
                            '- External environment monitor: disabled'
                        }
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Durable-v3 design fingerprint: ``$exactMasterDurableDesignFingerprint``"
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Durable-v3 design manifest: ``$exactMasterDurableDesignPath``"
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Predeclared durable-v3 schedule: ``$exactMasterDurableSchedulePath``; SHA-256 ``$exactMasterDurableScheduleSha256``"
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Symmetric artifact slot assignment: previous=``$($revisionArtifactSlots['previous'])``; candidate=``$($revisionArtifactSlots['candidate'])``"
                    }),
                $(if ($isExactMasterDurableMode) {
                        "- Durable-v3 conditioning manifest: ``$exactMasterDurableConditioningPath``"
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
                $(if ($Paired) {
                        "- Previous benchmark artifact execution path: " +
                            "``$(($pairedArtifacts['previous']).Path)``"
                    }),
                $(if ($Paired) {
                        "- Previous benchmark artifact SHA-256: " +
                            "``$(($pairedArtifacts['previous']).Sha256)``"
                    }),
                $(if ($Paired) {
                        "- Previous runnable closure: " +
                            "$(($pairedArtifacts['previous']).ClosureFileCount) files; " +
                            "SHA-256 ``$(($pairedArtifacts['previous']).ClosureSha256)``"
                    }),
                $(if ($Paired) {
                        "- Candidate benchmark artifact execution path: " +
                            "``$(($pairedArtifacts['candidate']).Path)``"
                    }),
                $(if ($Paired) {
                        "- Candidate benchmark artifact SHA-256: " +
                            "``$(($pairedArtifacts['candidate']).Sha256)``"
                    }),
                $(if ($Paired) {
                        "- Candidate runnable closure: " +
                            "$(($pairedArtifacts['candidate']).ClosureFileCount) files; " +
                            "SHA-256 ``$(($pairedArtifacts['candidate']).ClosureSha256)``"
                    }),
                $(if ($Paired) {
                        "- Paired benchmark artifact manifest: ``$pairedArtifactManifestPath``"
                    }),
                $(if ($Paired -and $isExactMasterDurableMode) {
                        '- Benchmark artifacts execute from equal-length sibling staging slots retained with the evidence.'
                    }
                    elseif ($Paired) {
                        '- Benchmark artifact paths identify detached execution worktrees and may not exist after cleanup.'
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
    if ($MonitorLocalEnvironment -and
        $null -ne $environmentMonitorProcess -and
        -not $environmentMonitorStopped) {
        try {
            Stop-AndAuditLocalEnvironmentMonitor
        }
        catch {
            if ($null -eq $primaryFailure) {
                $primaryFailure = $_
            }
            else {
                $cleanupFailures.Add(
                    'Local performance environment monitor also failed: ' +
                    $_.Exception.Message)
            }
        }
    }
    if ($Paired -and $pairedArtifactManifestPersisted) {
        $artifactCloseoutFailure = $null
        $artifactCloseoutDetail = ''
        try {
            if ($isExactMasterDurableMode) {
                Assert-PersistedLines `
                    -Path $exactMasterDurableDesignPath `
                    -ExpectedLines $designLines `
                    -Description 'Durable-v3 design manifest at closeout'
                Assert-PersistedLines `
                    -Path $exactMasterDurableSchedulePath `
                    -ExpectedLines $scheduleLines `
                    -Description 'Durable-v3 execution schedule at closeout'
                $closeoutScheduleSha256 = (
                    Get-FileHash `
                        -LiteralPath $exactMasterDurableSchedulePath `
                        -Algorithm SHA256
                ).Hash.ToLowerInvariant()
                if ($closeoutScheduleSha256 -cne $exactMasterDurableScheduleSha256) {
                    throw 'Durable-v3 schedule SHA-256 changed before closeout.'
                }
            }
            Assert-PairedBenchmarkArtifactCloseout `
                -Artifacts $pairedArtifacts `
                -ManifestPath $pairedArtifactManifestPath `
                -ExpectedManifestLines $pairedArtifactManifestLines
            $artifactCloseoutDetail = if ($isExactMasterDurableMode) {
                'Persisted design, schedule, artifact manifest, and both runnable ' +
                'closures match their predeclared identities.'
            }
            else {
                'Persisted manifest and both runnable closures match their ' +
                'post-build identities.'
            }
        }
        catch {
            $artifactCloseoutFailure = $_
            $artifactCloseoutDetail = $_.Exception.Message
        }

        $artifactCloseoutResult = if ($null -eq $artifactCloseoutFailure) {
            'PASS'
        }
        else {
            'FAIL'
        }
        try {
            Write-PairedBenchmarkArtifactCloseoutEvidence `
                -Result $artifactCloseoutResult `
                -Detail $artifactCloseoutDetail
        }
        catch {
            if ($null -eq $artifactCloseoutFailure) {
                $artifactCloseoutFailure = $_
            }
            else {
                $cleanupFailures.Add(
                    'Could not persist paired benchmark artifact closeout evidence: ' +
                    $_.Exception.Message)
            }
        }

        if ($null -ne $artifactCloseoutFailure) {
            if ($null -eq $primaryFailure) {
                $primaryFailure = $artifactCloseoutFailure
            }
            else {
                $cleanupFailures.Add(
                    'Paired benchmark artifact closeout also failed: ' +
                    $artifactCloseoutFailure.Exception.Message)
            }
        }
    }

    if ($candidateAdded) {
        $candidateCleanupSafe = $true
        try {
            $detachedCandidateLinks = Disconnect-WorktreeLinksForCleanup `
                -WorktreeRoot $candidateWorktree
            if ($detachedCandidateLinks -ne 0) {
                Write-Warning (
                    "Detached $detachedCandidateLinks candidate worktree link " +
                    'entry or entries before cleanup.')
            }
        }
        catch {
            $candidateCleanupSafe = $false
            $cleanupFailures.Add(
                'Skipped candidate worktree removal because its link-safety ' +
                "audit failed. Manual cleanup is required at '$candidateWorktree'. " +
                $_.Exception.Message)
        }
        if ($candidateCleanupSafe) {
            $candidateRemoveOutput = @(
                & git -C $repositoryRoot -c core.longpaths=true `
                    worktree remove --force $candidateWorktree 2>&1
            )
            $candidateRemoveExitCode = $LASTEXITCODE
            if ($candidateRemoveExitCode -ne 0) {
                $candidateRemoveDetails = (
                    $candidateRemoveOutput | ForEach-Object { [string] $_ }
                ) -join [Environment]::NewLine
                $candidateRemoveFailure =
                    "Could not remove candidate worktree '$candidateWorktree'."
                if (-not [string]::IsNullOrWhiteSpace($candidateRemoveDetails)) {
                    $candidateRemoveFailure +=
                        [Environment]::NewLine + $candidateRemoveDetails
                }
                $cleanupFailures.Add($candidateRemoveFailure)
            }
        }
    }
    if ($baselineAdded) {
        $baselineCleanupSafe = $true
        try {
            $detachedBaselineLinks = Disconnect-WorktreeLinksForCleanup `
                -WorktreeRoot $baselineWorktree
            if ($detachedBaselineLinks -ne 0) {
                Write-Warning (
                    "Detached $detachedBaselineLinks previous-release worktree link " +
                    'entry or entries before cleanup.')
            }
        }
        catch {
            $baselineCleanupSafe = $false
            $cleanupFailures.Add(
                'Skipped previous-release worktree removal because its ' +
                "link-safety audit failed. Manual cleanup is required at '$baselineWorktree'. " +
                $_.Exception.Message)
        }
        if ($baselineCleanupSafe) {
            $baselineRemoveOutput = @(
                & git -C $repositoryRoot -c core.longpaths=true `
                    worktree remove --force $baselineWorktree 2>&1
            )
            $baselineRemoveExitCode = $LASTEXITCODE
            if ($baselineRemoveExitCode -ne 0) {
                $baselineRemoveDetails = (
                    $baselineRemoveOutput | ForEach-Object { [string] $_ }
                ) -join [Environment]::NewLine
                $baselineRemoveFailure =
                    "Could not remove previous-release worktree '$baselineWorktree'."
                if (-not [string]::IsNullOrWhiteSpace($baselineRemoveDetails)) {
                    $baselineRemoveFailure +=
                        [Environment]::NewLine + $baselineRemoveDetails
                }
                $cleanupFailures.Add($baselineRemoveFailure)
            }
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
    if ($null -eq $originalDotnetCliWorkloadUpdateNotifyDisable) {
        Remove-Item `
            Env:DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE `
            -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE =
            $originalDotnetCliWorkloadUpdateNotifyDisable
    }
    if ($null -eq $originalDotnetSkipWorkloadIntegrityCheck) {
        Remove-Item `
            Env:DOTNET_SKIP_WORKLOAD_INTEGRITY_CHECK `
            -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_SKIP_WORKLOAD_INTEGRITY_CHECK =
            $originalDotnetSkipWorkloadIntegrityCheck
    }
    if ($null -eq $originalDotnetGenerateAspNetCertificate) {
        Remove-Item `
            Env:DOTNET_GENERATE_ASPNET_CERTIFICATE `
            -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_GENERATE_ASPNET_CERTIFICATE =
            $originalDotnetGenerateAspNetCertificate
    }
    if ($null -eq $originalDotnetAddGlobalToolsToPath) {
        Remove-Item `
            Env:DOTNET_ADD_GLOBAL_TOOLS_TO_PATH `
            -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_ADD_GLOBAL_TOOLS_TO_PATH =
            $originalDotnetAddGlobalToolsToPath
    }
    if ($null -eq $originalDotnetNoLogo) {
        Remove-Item Env:DOTNET_NOLOGO -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_NOLOGO = $originalDotnetNoLogo
    }
    if ($null -eq $originalDotnetCliTelemetryOptOut) {
        Remove-Item Env:DOTNET_CLI_TELEMETRY_OPTOUT -ErrorAction SilentlyContinue
    }
    else {
        $env:DOTNET_CLI_TELEMETRY_OPTOUT = $originalDotnetCliTelemetryOptOut
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
