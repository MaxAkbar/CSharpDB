[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string]$BaselineSnapshot = "",
    [string]$ThresholdsPath = "",
    [ValidateSet("pr", "release")]
    [string]$Mode = "release",
    [switch]$NoFailOnRegression,
    [switch]$SkipMicroRun,
    [string]$SnapshotOutputPath = "",
    [switch]$CaptureOnly,
    [switch]$NoBuild,
    [string[]]$MicroArguments = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-OptionalProperty
{
    param(
        [Parameter(Mandatory = $true)][object]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [object]$DefaultValue = $null
    )

    if ($null -eq $Object)
    {
        return $DefaultValue
    }

    if ($Object.PSObject.Properties.Name -contains $Name)
    {
        return ,$Object.$Name
    }

    return $DefaultValue
}

function Get-BenchmarkFilterFromCheck
{
    param([Parameter(Mandatory = $true)]$Check)

    $configuredFilter = [string](Get-OptionalProperty -Object $Check -Name "benchmarkFilter" -DefaultValue "")
    if (-not [string]::IsNullOrWhiteSpace($configuredFilter))
    {
        return $configuredFilter
    }

    $csvName = [string](Get-OptionalProperty -Object $Check -Name "csv" -DefaultValue "")
    if ($csvName -match "^CSharpDB\.Benchmarks\.Micro\.([A-Za-z0-9_]+)-report\.csv$")
    {
        $benchmarkType = $matches[1]
        return "*Micro.$benchmarkType*"
    }

    return ""
}

function Convert-ToStringArray
{
    param([Parameter(Mandatory = $false)]$Value)

    if ($null -eq $Value)
    {
        return ,([string[]]@())
    }

    $items = New-Object System.Collections.Generic.List[string]
    foreach ($entry in @($Value))
    {
        if ($null -ne $entry)
        {
            $items.Add([string]$entry) | Out-Null
        }
    }

    return ,([string[]]$items.ToArray())
}

function Resolve-RepoRelativePath
{
    param(
        [Parameter(Mandatory = $true)][string]$RepoRoot,
        [Parameter(Mandatory = $true)][string]$Path,
        [switch]$CreateIfMissing
    )

    $resolvedPath = $Path
    if (-not [System.IO.Path]::IsPathRooted($resolvedPath))
    {
        $resolvedPath = Join-Path $RepoRoot $resolvedPath
    }

    if ($CreateIfMissing)
    {
        New-Item -ItemType Directory -Path $resolvedPath -Force | Out-Null
    }
    elseif (-not (Test-Path $resolvedPath))
    {
        throw "Path not found: $resolvedPath"
    }

    return (Resolve-Path $resolvedPath).Path
}

function Get-ConfiguredSuiteDefinitions
{
    param(
        [Parameter(Mandatory = $true)][object[]]$Checks,
        [Parameter(Mandatory = $true)][string]$RepoRoot
    )

    $suiteDefinitions = @{}

    foreach ($check in $Checks)
    {
        $suiteArgs = Convert-ToStringArray (Get-OptionalProperty -Object $check -Name "suiteArgs" -DefaultValue $null)
        if ($suiteArgs.Count -eq 0)
        {
            continue
        }

        $csvName = [string](Get-OptionalProperty -Object $check -Name "csv" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($csvName))
        {
            throw "A guardrail check with suiteArgs is missing its csv property."
        }

        $suiteKey = [string](Get-OptionalProperty -Object $check -Name "suiteKey" -DefaultValue $csvName)
        $suiteLabel = [string](Get-OptionalProperty -Object $check -Name "suiteLabel" -DefaultValue $suiteKey)
        $outputPattern = [string](Get-OptionalProperty -Object $check -Name "outputPattern" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($outputPattern))
        {
            throw "Check '$csvName' defines suiteArgs but is missing outputPattern."
        }

        $currentResultsDir = [string](Get-OptionalProperty -Object $check -Name "currentResultsDir" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($currentResultsDir))
        {
            throw "Check '$csvName' defines suiteArgs but is missing currentResultsDir."
        }

        if (-not $suiteDefinitions.ContainsKey($suiteKey))
        {
            $suiteDefinitions[$suiteKey] = [pscustomobject]@{
                Key = $suiteKey
                Label = $suiteLabel
                OutputPattern = $outputPattern
                Arguments = @($suiteArgs)
                Targets = New-Object System.Collections.Generic.List[object]
            }
        }
        else
        {
            $existing = $suiteDefinitions[$suiteKey]
            $sameArgs = ($existing.Arguments.Count -eq $suiteArgs.Count)
            if ($sameArgs)
            {
                for ($i = 0; $i -lt $suiteArgs.Count; $i++)
                {
                    if ($existing.Arguments[$i] -ne $suiteArgs[$i])
                    {
                        $sameArgs = $false
                        break
                    }
                }
            }

            if ($existing.OutputPattern -ne $outputPattern -or -not $sameArgs)
            {
                throw "Suite key '$suiteKey' is configured inconsistently across guardrail checks."
            }
        }

        $suiteDefinitions[$suiteKey].Targets.Add([pscustomobject]@{
                Csv = $csvName
                CurrentResultsDir = Resolve-RepoRelativePath -RepoRoot $RepoRoot -Path $currentResultsDir -CreateIfMissing
            }) | Out-Null
    }

    return @($suiteDefinitions.Values | Sort-Object Key)
}

function Get-MicroBenchmarkDefinitions
{
    param([Parameter(Mandatory = $true)][object[]]$Checks)

    $definitions = @{}

    foreach ($check in $Checks)
    {
        $suiteArgs = Convert-ToStringArray (Get-OptionalProperty -Object $check -Name "suiteArgs" -DefaultValue $null)
        if ($suiteArgs.Count -gt 0)
        {
            continue
        }

        $filter = Get-BenchmarkFilterFromCheck -Check $check
        if ([string]::IsNullOrWhiteSpace($filter))
        {
            continue
        }

        $csvName = [string](Get-OptionalProperty -Object $check -Name "csv" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($csvName))
        {
            continue
        }

        if (-not $definitions.ContainsKey($filter))
        {
            $definitions[$filter] = [pscustomobject]@{
                Filter = $filter
                Csvs = New-Object System.Collections.Generic.List[string]
            }
        }

        if (-not ($definitions[$filter].Csvs -contains $csvName))
        {
            $definitions[$filter].Csvs.Add($csvName) | Out-Null
        }
    }

    return @($definitions.Values | Sort-Object Filter)
}

function Get-LatestGeneratedArtifact
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

function Get-LatestGeneratedArtifactFromDirectories
{
    param(
        [Parameter(Mandatory = $true)][string[]]$SourceDirs,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][datetime]$NotBeforeUtc
    )

    $matches = New-Object System.Collections.Generic.List[object]
    foreach ($sourceDir in ($SourceDirs | Select-Object -Unique))
    {
        if (-not (Test-Path $sourceDir))
        {
            continue
        }

        foreach ($file in (Get-ChildItem -Path $sourceDir -File -Filter $Pattern | Where-Object { $_.LastWriteTimeUtc -ge $NotBeforeUtc }))
        {
            $matches.Add($file) | Out-Null
        }
    }

    return $matches |
        Sort-Object LastWriteTimeUtc, FullName |
        Select-Object -Last 1
}

function Resolve-MicroArtifactResultsDirectories
{
    param(
        [Parameter(Mandatory = $true)][string]$BenchDir,
        [Parameter(Mandatory = $true)][string]$RepoRoot
    )

    $candidateDirs = @(
        (Join-Path $RepoRoot "BenchmarkDotNet.Artifacts\\results"),
        (Join-Path $BenchDir "BenchmarkDotNet.Artifacts\\results")
    )

    return @(
        $candidateDirs |
        Select-Object -Unique
    )
}

function Add-SelectPlanDiagnosticsToReport
{
    param(
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)]$Diagnostics
    )

    if (-not (Test-Path $ReportPath))
    {
        return
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add("## Select Plan Cache Diagnostics")
    $lines.Add("")

    $entries = New-Object System.Collections.Generic.List[object]
    if ($null -ne $Diagnostics)
    {
        foreach ($entry in $Diagnostics)
        {
            $entries.Add($entry) | Out-Null
        }
    }

    if ($entries.Count -eq 0)
    {
        $lines.Add("No select-plan cache diagnostics were emitted by the executed benchmark runs.")
    }
    else
    {
        $lines.Add("| Run | Sample | Hits | Misses | Reclassifications | Stores | Entries |")
        $lines.Add("|---|---:|---:|---:|---:|---:|---:|")
        foreach ($entry in $entries)
        {
            $runCell = [string]$entry.RunLabel
            if ([string]::IsNullOrWhiteSpace($runCell))
            {
                $runCell = "<unknown>"
            }
            $runCell = $runCell -replace "\|", "\\|"

            $lines.Add(
                "| $runCell | $($entry.Sequence) | $($entry.Hits) | $($entry.Misses) | $($entry.Reclassifications) | $($entry.Stores) | $($entry.Entries) |")
        }
    }

    Add-Content -Path $ReportPath -Value $lines -Encoding UTF8
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path
$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"
$benchResultsDir = Join-Path $benchDir ("bin/{0}/net10.0/results" -f $Configuration)
$compareScript = Join-Path $scriptDir "Compare-Baseline.ps1"
$reportPath = Join-Path $benchDir "results\\perf-guardrails-last.md"
$runLogsDir = Join-Path $benchDir "results\\perf-guardrails-run-logs"
$microResultsStagingDir = Join-Path $benchDir "results\\.tmp-current-micro-run"

$script:SelectPlanStatsPattern =
    'Select plan cache stats:\s*hits=(?<hits>\d+),\s*misses=(?<misses>\d+),\s*reclassifications=(?<reclass>\d+),\s*stores=(?<stores>\d+),\s*entries=(?<entries>\d+)'
$script:BenchmarkRunDiagnostics = New-Object System.Collections.Generic.List[object]
$script:BenchmarkRunOrdinal = 0
$script:EvidenceRuns = [Collections.Generic.List[object]]::new()
$productCommit = [string](& git -C $repoRoot rev-parse HEAD)
if ($LASTEXITCODE -ne 0) { throw 'Could not resolve product commit.' }
if ([string]::IsNullOrWhiteSpace($SnapshotOutputPath)) {
    $SnapshotOutputPath = Join-Path $benchDir ('results/perf-snapshots/' + [Guid]::NewGuid().ToString('N'))
}
$SnapshotOutputPath = [IO.Path]::GetFullPath($SnapshotOutputPath)
if (Test-Path -LiteralPath $SnapshotOutputPath) { throw "Snapshot output must be new: $SnapshotOutputPath" }
New-Item -ItemType Directory -Path (Join-Path $SnapshotOutputPath 'logs') -Force | Out-Null
if ($SkipMicroRun) { throw 'Guardrails require fresh execution; SkipMicroRun cannot satisfy evidence integrity.' }
if (-not $NoBuild) {
    Push-Location $benchDir
    try {
        & dotnet build $benchmarkProject -c $Configuration
        if ($LASTEXITCODE -ne 0) { throw 'Benchmark build failed.' }
    }
    finally { Pop-Location }
}
$runtimeIdentity = Get-PerfRuntimeIdentity -Project $benchmarkProject -Configuration $Configuration
$captureStartUtc = [DateTime]::UtcNow

if (Test-Path $runLogsDir)
{
    Remove-Item -Path (Join-Path $runLogsDir "*.log") -Force -ErrorAction SilentlyContinue
}
New-Item -ItemType Directory -Path $runLogsDir -Force | Out-Null

$resolvedThresholdsPath = $ThresholdsPath
if ([string]::IsNullOrWhiteSpace($resolvedThresholdsPath))
{
    $thresholdsFileName =
    if ($Mode -eq "pr")
    {
        "perf-thresholds-pr.json"
    }
    else
    {
        "perf-thresholds.json"
    }

    $resolvedThresholdsPath = Join-Path $benchDir $thresholdsFileName
}
elseif (-not [System.IO.Path]::IsPathRooted($resolvedThresholdsPath))
{
    $resolvedThresholdsPath = Join-Path $repoRoot $resolvedThresholdsPath
}

if (-not (Test-Path $resolvedThresholdsPath))
{
    throw "Threshold file not found: $resolvedThresholdsPath"
}

$thresholdConfig = Get-Content -Path $resolvedThresholdsPath -Raw | ConvertFrom-Json
$microBenchmarks = Get-MicroBenchmarkDefinitions -Checks @($thresholdConfig.checks)
$configuredSuites = Get-ConfiguredSuiteDefinitions -Checks @($thresholdConfig.checks) -RepoRoot $repoRoot
$microArtifactResultsDirs = Resolve-MicroArtifactResultsDirectories -BenchDir $benchDir -RepoRoot $repoRoot

if (Test-Path $microResultsStagingDir)
{
    Get-ChildItem -Path $microResultsStagingDir -File -ErrorAction SilentlyContinue | Remove-Item -Force
}
else
{
    New-Item -ItemType Directory -Path $microResultsStagingDir -Force | Out-Null
}

function Invoke-BenchmarkRun
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Host ""
    Write-Host "=== $Label ==="
    $script:BenchmarkRunOrdinal++
    $safeLabel = ($Label -replace "[^A-Za-z0-9._-]+", "_").Trim("_")
    if ([string]::IsNullOrWhiteSpace($safeLabel))
    {
        $safeLabel = "benchmark-run"
    }
    $runLogPath = Join-Path $runLogsDir ("{0:D2}-{1}.log" -f $script:BenchmarkRunOrdinal, $safeLabel)
    $runStart = [DateTime]::UtcNow

    Push-Location $benchDir
    try
    {
        & dotnet run -c $Configuration --no-build --project $benchmarkProject -- @Arguments 2>&1 |
            Tee-Object -FilePath $runLogPath
        $runExitCode = $LASTEXITCODE
    }
    finally
    {
        Pop-Location
    }
    if ($runExitCode -ne 0)
    {
        throw "Benchmark step failed: $Label"
    }

    if ($Arguments -contains "--micro")
    {
        $completedRuns = @(
            Get-Content -Path $runLogPath |
                Select-String -Pattern 'Global total time:.*executed benchmarks:\s*([1-9]\d*)\s*$'
        )
        if ($completedRuns.Count -ne 1)
        {
            throw "Micro benchmark step did not execute any benchmark cases: $Label. See $runLogPath"
        }
        $executedCases = Get-PerfExecutedMicroCases $runLogPath
    }
    else {
        $freshCsvs = @(Get-ChildItem -LiteralPath $benchResultsDir -File -Filter '*.csv' | Where-Object LastWriteTimeUtc -ge $runStart)
        $executedCases = 0
        foreach ($csv in $freshCsvs) { $executedCases += @(Import-Csv -LiteralPath $csv.FullName).Count }
        if ($executedCases -le 0) { throw "Benchmark step produced zero executed cases: $Label" }
    }
    $evidenceLog = 'logs/' + (Split-Path -Leaf $runLogPath)
    Copy-Item -LiteralPath $runLogPath -Destination (Join-Path $SnapshotOutputPath $evidenceLog)
    $script:EvidenceRuns.Add([pscustomobject]@{ label = $Label; arguments = $Arguments; executedCases = $executedCases; logPath = $evidenceLog; csvPaths = @() })

    $runStatsCount = 0
    foreach ($line in (Get-Content -Path $runLogPath))
    {
        $m = [regex]::Match($line, $script:SelectPlanStatsPattern)
        if (-not $m.Success)
        {
            continue
        }

        $runStatsCount++
        $script:BenchmarkRunDiagnostics.Add([pscustomobject]@{
                RunLabel = $Label
                Sequence = $runStatsCount
                Hits = [long]$m.Groups["hits"].Value
                Misses = [long]$m.Groups["misses"].Value
                Reclassifications = [long]$m.Groups["reclass"].Value
                Stores = [long]$m.Groups["stores"].Value
                Entries = [int]$m.Groups["entries"].Value
            }) | Out-Null
    }

    if ($runStatsCount -gt 0)
    {
        Write-Host "Captured $runStatsCount select-plan cache diagnostic line(s) for '$Label'."
    }
}

if (-not $SkipMicroRun)
{
    if (@($microBenchmarks).Count -eq 0 -and @($configuredSuites).Count -eq 0)
    {
        throw "No micro benchmark filters resolved from $resolvedThresholdsPath."
    }

    foreach ($definition in $microBenchmarks)
    {
        $runStartUtc = (Get-Date).ToUniversalTime()
        Invoke-BenchmarkRun -Label "Micro ($($definition.Filter))" -Arguments (@("--micro", "--filter", $definition.Filter) + $MicroArguments)

        foreach ($csvName in @($definition.Csvs))
        {
            $artifact = Get-LatestGeneratedArtifactFromDirectories `
                -SourceDirs $microArtifactResultsDirs `
                -Pattern $csvName `
                -NotBeforeUtc $runStartUtc

            if ($null -eq $artifact)
            {
                throw "No micro benchmark artifact matching '$csvName' was produced for filter '$($definition.Filter)'."
            }

            $destinationPath = Join-Path $microResultsStagingDir $csvName
            Copy-Item -Path $artifact.FullName -Destination $destinationPath -Force
            $snapshotMicroDir = Join-Path $SnapshotOutputPath 'micro-results'
            New-Item -ItemType Directory -Path $snapshotMicroDir -Force | Out-Null
            Copy-Item -LiteralPath $artifact.FullName -Destination (Join-Path $snapshotMicroDir $csvName)
            $script:EvidenceRuns[-1].csvPaths += 'micro-results/' + $csvName
            Write-Host "Staged $($artifact.Name) -> $destinationPath"
        }
    }
}

foreach ($suite in $configuredSuites)
{
    $suiteStartUtc = (Get-Date).ToUniversalTime()
    Invoke-BenchmarkRun -Label $suite.Label -Arguments $suite.Arguments

    $artifact = Get-LatestGeneratedArtifact `
        -SourceDir $benchResultsDir `
        -Pattern $suite.OutputPattern `
        -NotBeforeUtc $suiteStartUtc

    if ($null -eq $artifact)
    {
        throw "No benchmark artifact matching '$($suite.OutputPattern)' was produced for suite '$($suite.Label)'."
    }

    foreach ($target in $suite.Targets)
    {
        $destinationPath = Join-Path $target.CurrentResultsDir $target.Csv
        Copy-Item -Path $artifact.FullName -Destination $destinationPath -Force
        $check = @($thresholdConfig.checks | Where-Object csv -eq $target.Csv)[0]
        $snapshotSuiteDir = Join-Path $SnapshotOutputPath ([string](Get-OptionalProperty $check 'baselineSubDir' 'macro-stress-scaling'))
        New-Item -ItemType Directory -Path $snapshotSuiteDir -Force | Out-Null
        Copy-Item -LiteralPath $artifact.FullName -Destination (Join-Path $snapshotSuiteDir $target.Csv)
        $script:EvidenceRuns[-1].csvPaths += [IO.Path]::GetRelativePath($SnapshotOutputPath, (Join-Path $snapshotSuiteDir $target.Csv)).Replace('\', '/')
        Write-Host "Staged $($artifact.Name) -> $destinationPath"
    }
}

foreach ($root in @(@{ path = (Join-Path $repoRoot 'BenchmarkDotNet.Artifacts'); label = 'bdn-root' }, @{ path = (Join-Path $benchDir 'BenchmarkDotNet.Artifacts'); label = 'bdn-benchmark' }, @{ path = $benchResultsDir; label = 'raw-suite-results' })) {
    if (Test-Path -LiteralPath $root.path) {
        foreach ($file in @(Get-ChildItem -LiteralPath $root.path -File -Recurse | Where-Object { $_.LastWriteTimeUtc -ge $captureStartUtc -and $_.Extension -in @('.csv', '.json', '.log', '.md', '.html') })) {
            $target = Join-Path $SnapshotOutputPath ($root.label + '/' + [IO.Path]::GetRelativePath($root.path, $file.FullName))
            New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
            Copy-Item -LiteralPath $file.FullName -Destination $target
        }
    }
}
Write-PerfSnapshotManifest -SnapshotDir $SnapshotOutputPath -ProductCommit $productCommit.Trim() -HarnessRoot $benchDir -Runtime $runtimeIdentity -Runs @($script:EvidenceRuns)
if ($CaptureOnly) { return }

$compareParams = @{
    ReportPath = $reportPath
    ThresholdsPath = $resolvedThresholdsPath
    CurrentSnapshot = $SnapshotOutputPath
}

if ($NoFailOnRegression.IsPresent)
{
    $compareParams.NoFailOnRegression = $true
}

if (-not [string]::IsNullOrWhiteSpace($BaselineSnapshot))
{
    $compareParams.BaselineSnapshot = $BaselineSnapshot
}

if (-not $SkipMicroRun -and @($microBenchmarks).Count -gt 0)
{
    $compareParams.CurrentMicroResultsDir = $microResultsStagingDir
}

Write-Host ""
Write-Host "=== Compare Baseline ==="
$compareException = $null
$reportWriteTimeBefore = $null
if (Test-Path $reportPath)
{
    $reportWriteTimeBefore = (Get-Item -Path $reportPath).LastWriteTimeUtc
}
try
{
    & $compareScript @compareParams
}
catch
{
    $compareException = $_
}
finally
{
    $reportWasUpdated = $false
    if (Test-Path $reportPath)
    {
        $reportWriteTimeAfter = (Get-Item -Path $reportPath).LastWriteTimeUtc
        $reportWasUpdated =
            ($null -eq $reportWriteTimeBefore) -or
            ($reportWriteTimeAfter -gt $reportWriteTimeBefore)
    }

    if ($reportWasUpdated)
    {
        Add-SelectPlanDiagnosticsToReport -ReportPath $reportPath -Diagnostics $script:BenchmarkRunDiagnostics
    }
    else
    {
        Write-Warning "Skipping diagnostics append because guardrail report was not updated."
    }
}

if ($null -ne $compareException)
{
    throw $compareException
}

Write-Host ""
Write-Host "Perf guardrail report: $reportPath"
