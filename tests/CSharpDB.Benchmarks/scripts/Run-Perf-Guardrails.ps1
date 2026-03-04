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

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path
$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"
$compareScript = Join-Path $scriptDir "Compare-Baseline.ps1"
$reportPath = Join-Path $benchDir "results\\perf-guardrails-last.md"

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
    & dotnet run -c $Configuration --project $benchmarkProject -- @Arguments
    if ($LASTEXITCODE -ne 0)
    {
        throw "Benchmark step failed: $Label"
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
& $compareScript @compareParams

Write-Host ""
Write-Host "Perf guardrail report: $reportPath"
