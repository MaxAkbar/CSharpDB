[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string[]]$MicroFilters = @(
        "*SqlMaterializationBenchmarks*",
        "*CoveringIndexBenchmarks*",
        "*CollectionAccessBenchmarks*",
        "*CollectionFieldExtractionBenchmarks*"
    )
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$benchmarkProject = Join-Path $benchDir "CSharpDB.Benchmarks.csproj"

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

foreach ($filter in $MicroFilters)
{
    Invoke-BenchmarkRun -Label "Phase 1 Micro ($filter)" -Arguments @("--micro", "--filter", $filter)
}
