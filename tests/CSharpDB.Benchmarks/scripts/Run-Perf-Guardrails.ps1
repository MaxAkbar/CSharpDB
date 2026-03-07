[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string]$BaselineSnapshot = "",
    [string]$ThresholdsPath = "",
    [switch]$NoFailOnRegression,
    [switch]$SkipMicroRun
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
        return $Object.$Name
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
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path
$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"
$compareScript = Join-Path $scriptDir "Compare-Baseline.ps1"
$reportPath = Join-Path $benchDir "results\\perf-guardrails-last.md"
$runLogsDir = Join-Path $benchDir "results\\perf-guardrails-run-logs"

$script:SelectPlanStatsPattern =
    'Select plan cache stats:\s*hits=(?<hits>\d+),\s*misses=(?<misses>\d+),\s*reclassifications=(?<reclass>\d+),\s*stores=(?<stores>\d+),\s*entries=(?<entries>\d+)'
$script:BenchmarkRunDiagnostics = New-Object System.Collections.Generic.List[object]
$script:BenchmarkRunOrdinal = 0

if (Test-Path $runLogsDir)
{
    Remove-Item -Path (Join-Path $runLogsDir "*.log") -Force -ErrorAction SilentlyContinue
}
New-Item -ItemType Directory -Path $runLogsDir -Force | Out-Null

$resolvedThresholdsPath = $ThresholdsPath
if ([string]::IsNullOrWhiteSpace($resolvedThresholdsPath))
{
    $resolvedThresholdsPath = Join-Path $benchDir "perf-thresholds.json"
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

    & dotnet run -c $Configuration --project $benchmarkProject -- @Arguments 2>&1 |
        Tee-Object -FilePath $runLogPath
    if ($LASTEXITCODE -ne 0)
    {
        throw "Benchmark step failed: $Label"
    }

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
    $filters = @()

    foreach ($check in @($thresholdConfig.checks))
    {
        $filter = Get-BenchmarkFilterFromCheck -Check $check
        if (-not [string]::IsNullOrWhiteSpace($filter) -and -not ($filters -contains $filter))
        {
            $filters += $filter
        }
    }

    if (@($filters).Count -eq 0)
    {
        throw "No benchmark filters resolved from $resolvedThresholdsPath."
    }

    foreach ($filter in ($filters | Sort-Object))
    {
        Invoke-BenchmarkRun -Label "Micro ($filter)" -Arguments @("--micro", "--filter", $filter)
    }
}

$compareParams = @{
    ReportPath = $reportPath
    ThresholdsPath = $resolvedThresholdsPath
}

if ($NoFailOnRegression.IsPresent)
{
    $compareParams.NoFailOnRegression = $true
}

if (-not [string]::IsNullOrWhiteSpace($BaselineSnapshot))
{
    $compareParams.BaselineSnapshot = $BaselineSnapshot
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
