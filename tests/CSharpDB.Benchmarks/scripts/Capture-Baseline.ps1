[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string]$OutputRoot = "",
    [int]$MacroRepeatCount = 3,
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
        "*StorageTuningBenchmarks*",
        "*InsertBenchmarks*",
        "*JoinBenchmarks*",
        "*OrderByIndexBenchmarks*",
        "*ScalarAggregateBenchmarks*",
        "*ScalarAggregateLookupBenchmarks*",
        "*WalBenchmarks*",
        "*AdoNetBenchmarks*"
    ),
    [switch]$SkipMicro,
    [switch]$SkipMacro,
    [switch]$SkipStress,
    [switch]$SkipScaling,
    [switch]$SkipRepro
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path

if ([string]::IsNullOrWhiteSpace($OutputRoot))
{
    $OutputRoot = Join-Path $benchDir "baselines"
}

$runTimestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$snapshotDir = Join-Path $OutputRoot $runTimestamp
New-Item -ItemType Directory -Path $snapshotDir -Force | Out-Null

$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"
$startUtc = (Get-Date).ToUniversalTime()

function Invoke-BenchmarkRun
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Host ""
    Write-Host "=== $Label ==="
    & dotnet run -c $Configuration --project $benchmarkProject -- @Arguments
    if ($LASTEXITCODE -ne 0)
    {
        throw "Benchmark step failed: $Label"
    }
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

$copiedMicroCsv = Copy-NewArtifacts -SourceDir (Join-Path $bdnRoot "results") -TargetSubDir "micro-results" -Pattern "*.csv"
$copiedMicroLogs = Copy-NewArtifacts -SourceDir $bdnRoot -TargetSubDir "micro-logs" -Pattern "*.log"
$copiedMacro = Copy-NewArtifacts -SourceDir $benchResults -TargetSubDir "macro-stress-scaling" -Pattern "*.csv"

$metadataPath = Join-Path $snapshotDir "metadata.txt"
$gitHead = (& git -C $repoRoot rev-parse HEAD).Trim()
$gitStatus = & git -C $repoRoot status --short
$dotnetInfo = & dotnet --info

@(
    "UTC Timestamp: $runTimestamp"
    "Repository: $repoRoot"
    "Benchmark project: $benchmarkProject"
    "Configuration: $Configuration"
    "Macro repeat count: $MacroRepeatCount"
    "Repro mode: $(if ($SkipRepro) { "disabled" } else { "enabled" })"
    "Commit: $gitHead"
    "Micro CSV copied: $copiedMicroCsv"
    "Micro logs copied: $copiedMicroLogs"
    "Macro/Stress/Scaling CSV copied: $copiedMacro"
    ""
    "git status --short:"
    ($gitStatus -join [Environment]::NewLine)
    ""
    "dotnet --info:"
    $dotnetInfo
) | Set-Content -Path $metadataPath -Encoding UTF8

Write-Host ""
Write-Host "Baseline snapshot written to: $snapshotDir"
