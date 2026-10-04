[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string]$OutputRoot = "",
    [int]$MacroRepeatCount = 3,
    [int]$DurabilityRepeatCount = 3,
    [string[]]$MicroFilters = @(
        "*PointLookupBenchmarks*",
        "*InMemorySqlBenchmarks*",
        "*InMemoryCollectionBenchmarks*",
        "*InMemoryAdoNetBenchmarks*",
        "*InMemoryPersistenceBenchmarks*",
        "*CollectionPayloadBenchmarks*",
        "*CollectionSchemaBreadthBenchmarks*",
        "*MemoryMappedReadBenchmarks*",
        "*WalReadCacheBenchmarks*",
        "*BTreeCursorBenchmarks*",
        "*ColdLookupBenchmarks*",
        "*CollectionIndexBenchmarks*",
        "*CollationIndexBenchmarks*",
        "*StorageTuningBenchmarks*",
        "*InsertBenchmarks*",
        "*JoinBenchmarks*",
        "*OrderByIndexBenchmarks*",
        "*IndexAggregateBenchmarks*",
        "*ScanProjectionBenchmarks*",
        "*ScalarAggregateBenchmarks*",
        "*ScalarAggregateLookupBenchmarks*",
        "*WalBenchmarks*",
        "*AdoNetBenchmarks*"
    ),
    [switch]$SkipMicro,
    [switch]$SkipMacro,
    [switch]$SkipStress,
    [switch]$SkipScaling,
    [switch]$SkipWriteDiagnostics,
    [switch]$SkipConcurrentWriteDiagnostics,
    [switch]$SkipWriteTransactionDiagnostics,
    [switch]$SkipDurableSqlBatching,
    [switch]$SkipRepro
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path

if ([string]::IsNullOrWhiteSpace($OutputRoot))
{
    $OutputRoot = Join-Path $benchDir "baselines"
}

$runTimestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$snapshotDir = Join-Path $OutputRoot $runTimestamp
if (Test-Path -LiteralPath $snapshotDir) { throw "Snapshot output already exists: $snapshotDir" }
New-Item -ItemType Directory -Path $snapshotDir -Force | Out-Null

$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"
$startUtc = (Get-Date).ToUniversalTime()

$script:CaptureRuns = [Collections.Generic.List[object]]::new()
New-Item -ItemType Directory -Path (Join-Path $snapshotDir 'logs') -Force | Out-Null

function Invoke-BenchmarkRun
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Host ""
    Write-Host "=== $Label ==="
    $runStart = [DateTime]::UtcNow
    $relativeLog = 'logs/{0:D2}.log' -f ($script:CaptureRuns.Count + 1)
    $logPath = Join-Path $snapshotDir $relativeLog
    Push-Location $benchDir
    try {
        & dotnet run -c $Configuration --project $benchmarkProject -- @Arguments 2>&1 | Tee-Object -FilePath $logPath
        if ($LASTEXITCODE -ne 0) { throw "Benchmark step failed: $Label" }
    }
    finally { Pop-Location }
    if ($Arguments -contains '--micro') { $count = Get-PerfExecutedMicroCases $logPath }
    else {
        $count = 0
        $resultsDir = Join-Path $benchDir "bin/$Configuration/net10.0/results"
        foreach ($csv in @(Get-ChildItem -LiteralPath $resultsDir -Filter '*.csv' -File | Where-Object LastWriteTimeUtc -ge $runStart)) {
            $count += @(Import-Csv -LiteralPath $csv.FullName).Count
        }
        if ($count -le 0) { throw "Benchmark step produced zero executed cases: $Label" }
    }
    $script:CaptureRuns.Add([pscustomobject]@{ label = $Label; arguments = $Arguments; executedCases = $count; logPath = $relativeLog })
}

function Get-LatestArtifactSince
{
    param(
        [Parameter(Mandatory = $true)][string]$SourceDir,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][datetime]$NotBeforeUtc
    )

    if (-not (Test-Path $SourceDir))
    {
        throw "Benchmark results directory not found: $SourceDir"
    }

    return Get-ChildItem -Path $SourceDir -File -Filter $Pattern |
        Where-Object { $_.LastWriteTimeUtc -ge $NotBeforeUtc } |
        Sort-Object LastWriteTimeUtc, Name |
        Select-Object -Last 1
}

function Copy-LatestArtifactToSnapshot
{
    param(
        [Parameter(Mandatory = $true)][string]$SourceDir,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][datetime]$NotBeforeUtc,
        [Parameter(Mandatory = $true)][string]$TargetSubDir,
        [Parameter(Mandatory = $true)][string]$TargetFileName
    )

    $artifact = Get-LatestArtifactSince -SourceDir $SourceDir -Pattern $Pattern -NotBeforeUtc $NotBeforeUtc
    if ($null -eq $artifact)
    {
        throw "No benchmark artifact matching '$Pattern' was produced after $NotBeforeUtc."
    }

    $targetDir = Join-Path $snapshotDir $TargetSubDir
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
    $destinationPath = Join-Path $targetDir $TargetFileName
    Copy-Item -Path $artifact.FullName -Destination $destinationPath -Force
    return $destinationPath
}

if (-not $SkipMicro)
{
    foreach ($filter in $MicroFilters)
    {
        Invoke-BenchmarkRun -Label "Micro ($filter)" -Arguments @("--micro", "--filter", $filter)
    }
}

if (-not $SkipMacro)
{
    $macroArgs = @("--macro")
    if ($MacroRepeatCount -gt 1)
    {
        $macroArgs += @("--repeat", $MacroRepeatCount.ToString([System.Globalization.CultureInfo]::InvariantCulture))
    }
    if (-not $SkipRepro)
    {
        $macroArgs += "--repro"
    }

    Invoke-BenchmarkRun -Label "Macro" -Arguments $macroArgs
}

if (-not $SkipStress)
{
    $stressArgs = @("--stress")
    if (-not $SkipRepro)
    {
        $stressArgs += "--repro"
    }

    Invoke-BenchmarkRun -Label "Stress" -Arguments $stressArgs
}

if (-not $SkipScaling)
{
    $scalingArgs = @("--scaling")
    if (-not $SkipRepro)
    {
        $scalingArgs += "--repro"
    }

    Invoke-BenchmarkRun -Label "Scaling" -Arguments $scalingArgs
}

$writeDiagnosticsCapturePath = ""
if (-not $SkipWriteDiagnostics)
{
    $writeDiagnosticsArgs = @("--write-diagnostics")
    if ($DurabilityRepeatCount -gt 1)
    {
        $writeDiagnosticsArgs += @("--repeat", $DurabilityRepeatCount.ToString([System.Globalization.CultureInfo]::InvariantCulture))
    }
    if (-not $SkipRepro)
    {
        $writeDiagnosticsArgs += "--repro"
    }

    $writeDiagnosticsStartUtc = (Get-Date).ToUniversalTime()
    Invoke-BenchmarkRun -Label "Write Diagnostics" -Arguments $writeDiagnosticsArgs
    $writeDiagnosticsCapturePath = Copy-LatestArtifactToSnapshot `
        -SourceDir (Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)) `
        -Pattern ("write-diagnostics-*-median-of-{0}.csv" -f $DurabilityRepeatCount) `
        -NotBeforeUtc $writeDiagnosticsStartUtc `
        -TargetSubDir "macro-stress-scaling" `
        -TargetFileName ("write-diagnostics-median-of-{0}.csv" -f $DurabilityRepeatCount)
}

$durableSqlBatchingCapturePath = ""
if (-not $SkipDurableSqlBatching)
{
    $durableSqlBatchingArgs = @("--durable-sql-batching")
    if ($DurabilityRepeatCount -gt 1)
    {
        $durableSqlBatchingArgs += @("--repeat", $DurabilityRepeatCount.ToString([System.Globalization.CultureInfo]::InvariantCulture))
    }
    if (-not $SkipRepro)
    {
        $durableSqlBatchingArgs += "--repro"
    }

    $durableSqlBatchingStartUtc = (Get-Date).ToUniversalTime()
    Invoke-BenchmarkRun -Label "Durable SQL Batching" -Arguments $durableSqlBatchingArgs
    $durableSqlBatchingCapturePath = Copy-LatestArtifactToSnapshot `
        -SourceDir (Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)) `
        -Pattern ("durable-sql-batching-*-median-of-{0}.csv" -f $DurabilityRepeatCount) `
        -NotBeforeUtc $durableSqlBatchingStartUtc `
        -TargetSubDir "macro-stress-scaling" `
        -TargetFileName ("durable-sql-batching-median-of-{0}.csv" -f $DurabilityRepeatCount)
}

$writeTransactionDiagnosticsCapturePath = ""
if (-not $SkipWriteTransactionDiagnostics)
{
    $writeTransactionDiagnosticsArgs = @("--write-transaction-diagnostics")
    if ($DurabilityRepeatCount -gt 1)
    {
        $writeTransactionDiagnosticsArgs += @("--repeat", $DurabilityRepeatCount.ToString([System.Globalization.CultureInfo]::InvariantCulture))
    }
    if (-not $SkipRepro)
    {
        $writeTransactionDiagnosticsArgs += "--repro"
    }

    $writeTransactionDiagnosticsStartUtc = (Get-Date).ToUniversalTime()
    Invoke-BenchmarkRun -Label "WriteTransaction Diagnostics" -Arguments $writeTransactionDiagnosticsArgs
    $writeTransactionDiagnosticsCapturePath = Copy-LatestArtifactToSnapshot `
        -SourceDir (Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)) `
        -Pattern ("write-transaction-diagnostics-*-median-of-{0}.csv" -f $DurabilityRepeatCount) `
        -NotBeforeUtc $writeTransactionDiagnosticsStartUtc `
        -TargetSubDir "macro-stress-scaling" `
        -TargetFileName ("write-transaction-diagnostics-median-of-{0}.csv" -f $DurabilityRepeatCount)
}

$concurrentWriteDiagnosticsCapturePath = ""
if (-not $SkipConcurrentWriteDiagnostics)
{
    $concurrentWriteDiagnosticsArgs = @("--concurrent-write-diagnostics")
    if ($DurabilityRepeatCount -gt 1)
    {
        $concurrentWriteDiagnosticsArgs += @("--repeat", $DurabilityRepeatCount.ToString([System.Globalization.CultureInfo]::InvariantCulture))
    }
    if (-not $SkipRepro)
    {
        $concurrentWriteDiagnosticsArgs += "--repro"
    }

    $concurrentWriteDiagnosticsStartUtc = (Get-Date).ToUniversalTime()
    Invoke-BenchmarkRun -Label "Concurrent Write Diagnostics" -Arguments $concurrentWriteDiagnosticsArgs
    $concurrentWriteDiagnosticsCapturePath = Copy-LatestArtifactToSnapshot `
        -SourceDir (Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)) `
        -Pattern ("concurrent-write-diagnostics-*-median-of-{0}.csv" -f $DurabilityRepeatCount) `
        -NotBeforeUtc $concurrentWriteDiagnosticsStartUtc `
        -TargetSubDir "macro-stress-scaling" `
        -TargetFileName ("concurrent-write-diagnostics-median-of-{0}.csv" -f $DurabilityRepeatCount)
}

function Copy-NewArtifacts
{
    param(
        [Parameter(Mandatory = $true)][string]$SourceDir,
        [Parameter(Mandatory = $true)][string]$TargetSubDir,
        [string]$Pattern = "*"
    )

    if (-not (Test-Path $SourceDir))
    {
        return 0
    }

    $targetDir = Join-Path $snapshotDir $TargetSubDir
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

    $copied = 0
    Get-ChildItem -Path $SourceDir -File -Filter $Pattern |
        Where-Object { $_.LastWriteTimeUtc -ge $startUtc } |
        ForEach-Object {
            Copy-Item -Path $_.FullName -Destination (Join-Path $targetDir $_.Name) -Force
            $copied++
        }

    return $copied
}

$bdnRoot = Join-Path $repoRoot "BenchmarkDotNet.Artifacts"
$benchResults = Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)

$copiedMicroCsv = 0
$copiedMicroLogs = 0
foreach ($artifactRoot in @($bdnRoot, (Join-Path $benchDir 'BenchmarkDotNet.Artifacts'))) {
    $copiedMicroCsv += Copy-NewArtifacts -SourceDir (Join-Path $artifactRoot 'results') -TargetSubDir 'micro-results' -Pattern '*.csv'
    $copiedMicroLogs += Copy-NewArtifacts -SourceDir $artifactRoot -TargetSubDir 'micro-logs' -Pattern '*.log'
}

$copiedMacro = Copy-NewArtifacts -SourceDir $benchResults -TargetSubDir "macro-stress-scaling" -Pattern "*.csv"

$metadataPath = Join-Path $snapshotDir "metadata.txt"
$machineFingerprintPath = Join-Path $snapshotDir "machine.json"
$gitHead = (& git -C $repoRoot rev-parse HEAD).Trim()
$gitStatus = & git -C $repoRoot status --short
Push-Location $benchDir
try {
    $dotnetInfo = & dotnet --info
    $machineFingerprint = Get-PerfMachineFingerprint
}
finally { Pop-Location }

@(
    "UTC Timestamp: $runTimestamp"
    "Repository: $repoRoot"
    "Benchmark project: $benchmarkProject"
    "Configuration: $Configuration"
    "Macro repeat count: $MacroRepeatCount"
    "Durability repeat count: $DurabilityRepeatCount"
    "Repro mode: $(if ($SkipRepro) { "disabled" } else { "enabled" })"
    "Commit: $gitHead"
    "Micro CSV copied: $copiedMicroCsv"
    "Micro logs copied: $copiedMicroLogs"
    "Macro/Stress/Scaling CSV copied: $copiedMacro"
    "Write diagnostics capture: $(if ([string]::IsNullOrWhiteSpace($writeDiagnosticsCapturePath)) { "skipped" } else { $writeDiagnosticsCapturePath })"
    "Durable SQL batching capture: $(if ([string]::IsNullOrWhiteSpace($durableSqlBatchingCapturePath)) { "skipped" } else { $durableSqlBatchingCapturePath })"
    "WriteTransaction diagnostics capture: $(if ([string]::IsNullOrWhiteSpace($writeTransactionDiagnosticsCapturePath)) { "skipped" } else { $writeTransactionDiagnosticsCapturePath })"
    "Concurrent write diagnostics capture: $(if ([string]::IsNullOrWhiteSpace($concurrentWriteDiagnosticsCapturePath)) { "skipped" } else { $concurrentWriteDiagnosticsCapturePath })"
    "Machine fingerprint: $(Split-Path -Leaf $machineFingerprintPath)"
    ""
    "git status --short:"
    ($gitStatus -join [Environment]::NewLine)
    ""
    "dotnet --info:"
    $dotnetInfo
) | Set-Content -Path $metadataPath -Encoding UTF8

$machineFingerprint | ConvertTo-Json -Depth 4 | Set-Content -Path $machineFingerprintPath -Encoding UTF8

Write-Host ""
$runtime = Get-PerfRuntimeIdentity -Project $benchmarkProject -Configuration $Configuration
Write-PerfSnapshotManifest -SnapshotDir $snapshotDir -ProductCommit $gitHead -HarnessRoot $benchDir -Runtime $runtime -Runs @($script:CaptureRuns)
Write-Host "Baseline snapshot written to: $snapshotDir"
