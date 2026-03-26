[CmdletBinding()]
param(
    [string]$BaselineSnapshot = "",
    [string]$CurrentMicroResultsDir = "",
    [string]$ThresholdsPath = "",
    [switch]$NoFailOnRegression,
    [string]$ReportPath = ""
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

function Convert-TimeToNanoseconds
{
    param([Parameter(Mandatory = $true)][string]$Value)

    $text = $Value.Trim("'").Trim()
    $text = $text -replace ",", ""

    if ($text -match "^([0-9]*\.?[0-9]+)\s*(ns|us|μs|ms|s)$")
    {
        $number = [double]$matches[1]
        $unit = $matches[2]
        switch ($unit)
        {
            "ns" { return $number }
            "us" { return $number * 1000.0 }
            "μs" { return $number * 1000.0 }
            "ms" { return $number * 1000000.0 }
            "s" { return $number * 1000000000.0 }
        }
    }

    if ($text -match "^([0-9]*\.?[0-9]+)$")
    {
        # Macro/stress/scaling diagnostics currently emit raw millisecond values.
        return ([double]$matches[1]) * 1000000.0
    }

    throw "Unable to parse time value '$Value'."
}

function Convert-SizeToBytes
{
    param([Parameter(Mandatory = $true)][string]$Value)

    $text = $Value.Trim("'").Trim()
    $text = $text -replace ",", ""

    if ($text -match "^([0-9]*\.?[0-9]+)\s*(B|KB|MB|GB)$")
    {
        $number = [double]$matches[1]
        $unit = $matches[2]
        switch ($unit)
        {
            "B" { return $number }
            "KB" { return $number * 1024.0 }
            "MB" { return $number * 1024.0 * 1024.0 }
            "GB" { return $number * 1024.0 * 1024.0 * 1024.0 }
        }
    }

    if ($text -match "^([0-9]*\.?[0-9]+)$")
    {
        return [double]$matches[1]
    }

    throw "Unable to parse size value '$Value'."
}

function Test-RowMatch
{
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $false)]$Criteria
    )

    if ($null -eq $Criteria)
    {
        return $true
    }

    foreach ($property in $Criteria.PSObject.Properties)
    {
        $name = $property.Name
        $expected = [string]$property.Value
        $actual = [string](Get-OptionalProperty -Object $Row -Name $name -DefaultValue "")
        if ($actual -ne $expected)
        {
            return $false
        }
    }

    return $true
}

function Build-RowKey
{
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string[]]$KeyColumns
    )

    $parts = foreach ($column in $KeyColumns)
    {
        $value = Get-OptionalProperty -Object $Row -Name $column -DefaultValue ""
        "$column=$value"
    }

    return ($parts -join "; ")
}

function Format-Nanoseconds
{
    param([Parameter(Mandatory = $true)][double]$Nanoseconds)

    if ($Nanoseconds -ge 1000000.0)
    {
        return ("{0:N2} ms" -f ($Nanoseconds / 1000000.0))
    }

    if ($Nanoseconds -ge 1000.0)
    {
        return ("{0:N2} us" -f ($Nanoseconds / 1000.0))
    }

    return ("{0:N2} ns" -f $Nanoseconds)
}

function Format-Bytes
{
    param([Parameter(Mandatory = $true)][double]$Bytes)

    if ($Bytes -ge 1024.0 * 1024.0)
    {
        return ("{0:N2} MB" -f ($Bytes / (1024.0 * 1024.0)))
    }

    if ($Bytes -ge 1024.0)
    {
        return ("{0:N2} KB" -f ($Bytes / 1024.0))
    }

    return ("{0:N0} B" -f $Bytes)
}

function Resolve-CurrentMicroResultsDirectory
{
    param(
        [Parameter(Mandatory = $true)][string]$BenchDir,
        [Parameter(Mandatory = $true)][string]$RepoRoot,
        [Parameter(Mandatory = $false)][string]$ConfiguredPath
    )

    if (-not [string]::IsNullOrWhiteSpace($ConfiguredPath))
    {
        $resolvedConfiguredPath = $ConfiguredPath
        if (-not [System.IO.Path]::IsPathRooted($resolvedConfiguredPath))
        {
            $resolvedConfiguredPath = Join-Path $RepoRoot $resolvedConfiguredPath
        }

        if (-not (Test-Path $resolvedConfiguredPath))
        {
            throw "Current micro-results directory not found: $resolvedConfiguredPath"
        }

        return (Resolve-Path $resolvedConfiguredPath).Path
    }

    $candidateDirs = @(
        (Join-Path $RepoRoot "BenchmarkDotNet.Artifacts\\results"),
        (Join-Path $BenchDir "BenchmarkDotNet.Artifacts\\results")
    )

    $existingCandidates = @(
        $candidateDirs |
        Where-Object { Test-Path $_ } |
        ForEach-Object { (Resolve-Path $_).Path } |
        Select-Object -Unique
    )

    if ($existingCandidates.Count -eq 0)
    {
        throw "Current micro-results directory not found in expected locations: $($candidateDirs -join '; ')"
    }

    if ($existingCandidates.Count -eq 1)
    {
        return $existingCandidates[0]
    }

    $stagingDir = Join-Path $BenchDir "results\\.tmp-current-micro"
    New-Item -ItemType Directory -Path $stagingDir -Force | Out-Null
    Get-ChildItem -Path $stagingDir -File -ErrorAction SilentlyContinue | Remove-Item -Force

    $latestByName = @{}
    foreach ($candidate in $existingCandidates)
    {
        foreach ($file in Get-ChildItem -Path $candidate -File -Filter "*.csv")
        {
            if (-not $latestByName.ContainsKey($file.Name) -or
                $file.LastWriteTimeUtc -gt $latestByName[$file.Name].LastWriteTimeUtc)
            {
                $latestByName[$file.Name] = $file
            }
        }
    }

    if ($latestByName.Count -eq 0)
    {
        throw "No benchmark CSV files found across candidate result directories: $($existingCandidates -join '; ')"
    }

    foreach ($entry in $latestByName.GetEnumerator())
    {
        Copy-Item -Path $entry.Value.FullName -Destination (Join-Path $stagingDir $entry.Key) -Force
    }

    return (Resolve-Path $stagingDir).Path
}

function Resolve-ResultsDirectory
{
    param(
        [Parameter(Mandatory = $true)][string]$RepoRoot,
        [Parameter(Mandatory = $false)][string]$ConfiguredPath
    )

    if ([string]::IsNullOrWhiteSpace($ConfiguredPath))
    {
        return $null
    }

    $resolvedPath = $ConfiguredPath
    if (-not [System.IO.Path]::IsPathRooted($resolvedPath))
    {
        $resolvedPath = Join-Path $RepoRoot $resolvedPath
    }

    if (-not (Test-Path $resolvedPath))
    {
        throw "Results directory not found: $resolvedPath"
    }

    return (Resolve-Path $resolvedPath).Path
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path

if ([string]::IsNullOrWhiteSpace($ThresholdsPath))
{
    $ThresholdsPath = Join-Path $benchDir "perf-thresholds.json"
}
elseif (-not [System.IO.Path]::IsPathRooted($ThresholdsPath))
{
    $ThresholdsPath = Join-Path $repoRoot $ThresholdsPath
}

if (-not (Test-Path $ThresholdsPath))
{
    throw "Threshold file not found: $ThresholdsPath"
}

$config = Get-Content -Path $ThresholdsPath -Raw | ConvertFrom-Json
$baselineRoot = Join-Path $benchDir "baselines"
$hasCheckBaselineOverrides = @(
    @($config.checks) |
    Where-Object { -not [string]::IsNullOrWhiteSpace([string](Get-OptionalProperty -Object $_ -Name "baselineSnapshot" -DefaultValue "")) }
).Count -gt 0

$configuredBaseline = [string](Get-OptionalProperty -Object $config -Name "baselineSnapshot" -DefaultValue "")
$baselineResolved = $false
$baselineSnapshotDir = $null
if ([string]::IsNullOrWhiteSpace($BaselineSnapshot))
{
    if (-not [string]::IsNullOrWhiteSpace($configuredBaseline))
    {
        $BaselineSnapshot = $configuredBaseline
    }
    else
    {
        if (Test-Path $baselineRoot)
        {
            $latest = Get-ChildItem -Path $baselineRoot -Directory |
                Where-Object { $_.Name -match "^\d{8}-\d{6}$" } |
                Sort-Object Name |
                Select-Object -Last 1

            if ($null -ne $latest)
            {
                $BaselineSnapshot = $latest.FullName
            }
        }
    }
}

$reportBaselineLabel = $BaselineSnapshot
if (-not [string]::IsNullOrWhiteSpace($BaselineSnapshot))
{
    if (-not [System.IO.Path]::IsPathRooted($BaselineSnapshot))
    {
        $BaselineSnapshot = Join-Path $baselineRoot $BaselineSnapshot
    }

    if ((Test-Path $BaselineSnapshot))
    {
        $baselineSnapshotDir = (Resolve-Path $BaselineSnapshot).Path
        $baselineResolved = $true
    }
}

if (-not $baselineResolved -and $hasCheckBaselineOverrides)
{
    $reportBaselineLabel = "<per-check overrides>"
    Write-Host "Global baseline snapshot not found. Proceeding with per-check baseline overrides where available."
}
elseif (-not $baselineResolved)
{
    Write-Host "No baseline snapshot found. Skipping comparison and reporting raw benchmark results only."
}

$checksUsingDefaultCurrentResultsDir = @(
    @($config.checks) |
    Where-Object { [string]::IsNullOrWhiteSpace([string](Get-OptionalProperty -Object $_ -Name "currentResultsDir" -DefaultValue "")) }
).Count -gt 0

$resolvedDefaultCurrentResultsDir = $null
if ($checksUsingDefaultCurrentResultsDir)
{
    $resolvedDefaultCurrentResultsDir = Resolve-CurrentMicroResultsDirectory `
        -BenchDir $benchDir `
        -RepoRoot $repoRoot `
        -ConfiguredPath $CurrentMicroResultsDir
}

$reportCurrentLabel =
if ($checksUsingDefaultCurrentResultsDir)
{
    if (@(
            @($config.checks) |
            Where-Object { -not [string]::IsNullOrWhiteSpace([string](Get-OptionalProperty -Object $_ -Name "currentResultsDir" -DefaultValue "")) }
        ).Count -gt 0)
    {
        "$resolvedDefaultCurrentResultsDir (+ per-check overrides)"
    }
    else
    {
        $resolvedDefaultCurrentResultsDir
    }
}
else
{
    "<per-check current results directories>"
}

if (-not $baselineResolved -and -not $hasCheckBaselineOverrides)
{
    $summary = "No baseline snapshot available. Benchmark run completed but no regression comparison performed."
    Write-Host $summary

    if (-not [string]::IsNullOrWhiteSpace($ReportPath))
    {
        if (-not [System.IO.Path]::IsPathRooted($ReportPath))
        {
            $ReportPath = Join-Path $repoRoot $ReportPath
        }

        $reportDir = Split-Path -Parent $ReportPath
        if (-not [string]::IsNullOrWhiteSpace($reportDir))
        {
            New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
        }

        $lines = New-Object System.Collections.Generic.List[string]
        $lines.Add("# Performance Guardrail Report")
        $lines.Add("")
        $lines.Add("- Baseline: **not available** (baselines directory is not present)")
        $lines.Add("- Current: ``$reportCurrentLabel``")
        $lines.Add("- Thresholds: ``$ThresholdsPath``")
        $lines.Add("- Generated (UTC): $((Get-Date).ToUniversalTime().ToString('u'))")
        $lines.Add("")
        $lines.Add($summary)
        $lines.Add("")
        $lines.Add("To enable regression comparison, run ``Capture-Baseline.ps1`` on this machine and commit or make available the resulting snapshot.")
        Set-Content -Path $ReportPath -Value $lines -Encoding UTF8
        Write-Host "Report written to $ReportPath"
    }

    return
}

$defaults = Get-OptionalProperty -Object $config -Name "defaults" -DefaultValue $null
$defaultMeanRegression = [double](Get-OptionalProperty -Object $defaults -Name "maxMeanRegressionPercent" -DefaultValue 8.0)
$defaultAllocRegressionPct = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionPercent" -DefaultValue 10.0)
$defaultAllocRegressionBytes = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionBytes" -DefaultValue 256.0)

$results = New-Object System.Collections.Generic.List[object]
$failureCount = 0

foreach ($check in $config.checks)
{
    $csvName = [string]$check.csv
    $keyColumns = @($check.keyColumns)
    if ($keyColumns.Count -eq 0)
    {
        throw "Check '$csvName' must define at least one key column."
    }

    $checkBaselineSnapshot = [string](Get-OptionalProperty -Object $check -Name "baselineSnapshot" -DefaultValue "")
    $checkBaselineSnapshotDir = $baselineSnapshotDir
    if (-not [string]::IsNullOrWhiteSpace($checkBaselineSnapshot))
    {
        $checkBaselineSnapshotPath = $checkBaselineSnapshot
        if (-not [System.IO.Path]::IsPathRooted($checkBaselineSnapshotPath))
        {
            $checkBaselineSnapshotPath = Join-Path $baselineRoot $checkBaselineSnapshotPath
        }

        if (-not (Test-Path $checkBaselineSnapshotPath))
        {
            $results.Add([pscustomobject]@{
                    Csv = $csvName
                    Key = "<missing baseline snapshot>"
                    BaselineMean = ""
                    CurrentMean = ""
                    MeanDeltaPct = ""
                    BaselineAlloc = ""
                    CurrentAlloc = ""
                    AllocDeltaPct = ""
                    AllocDeltaBytes = ""
                    Status = "FAIL"
                    Notes = "Baseline snapshot missing: $checkBaselineSnapshotPath"
                })
            $failureCount++
            continue
        }

        $checkBaselineSnapshotDir = (Resolve-Path $checkBaselineSnapshotPath).Path
    }
    elseif (-not $baselineResolved)
    {
        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = "<missing global baseline snapshot>"
                BaselineMean = ""
                CurrentMean = ""
                MeanDeltaPct = ""
                BaselineAlloc = ""
                CurrentAlloc = ""
                AllocDeltaPct = ""
                AllocDeltaBytes = ""
                Status = "FAIL"
                Notes = "Global baseline snapshot missing for check without baselineSnapshot override"
            })
        $failureCount++
        continue
    }

    $baselineSubDir = [string](Get-OptionalProperty -Object $check -Name "baselineSubDir" -DefaultValue "micro-results")
    $checkBaselineResultsDir = Join-Path $checkBaselineSnapshotDir $baselineSubDir
    if (-not (Test-Path $checkBaselineResultsDir))
    {
        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = "<missing baseline results directory>"
                BaselineMean = ""
                CurrentMean = ""
                MeanDeltaPct = ""
                BaselineAlloc = ""
                CurrentAlloc = ""
                AllocDeltaPct = ""
                AllocDeltaBytes = ""
                Status = "FAIL"
                Notes = "Baseline results directory missing: $checkBaselineResultsDir"
            })
        $failureCount++
        continue
    }

    $configuredCurrentResultsDir = [string](Get-OptionalProperty -Object $check -Name "currentResultsDir" -DefaultValue "")
    $checkCurrentResultsDir =
    if ([string]::IsNullOrWhiteSpace($configuredCurrentResultsDir))
    {
        $resolvedDefaultCurrentResultsDir
    }
    else
    {
        Resolve-ResultsDirectory -RepoRoot $repoRoot -ConfiguredPath $configuredCurrentResultsDir
    }

    if ([string]::IsNullOrWhiteSpace($checkCurrentResultsDir) -or -not (Test-Path $checkCurrentResultsDir))
    {
        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = "<missing current results directory>"
                BaselineMean = ""
                CurrentMean = ""
                MeanDeltaPct = ""
                BaselineAlloc = ""
                CurrentAlloc = ""
                AllocDeltaPct = ""
                AllocDeltaBytes = ""
                Status = "FAIL"
                Notes = "Current results directory missing: $checkCurrentResultsDir"
            })
        $failureCount++
        continue
    }

    $baselineFile = Join-Path $checkBaselineResultsDir $csvName
    $currentFile = Join-Path $checkCurrentResultsDir $csvName

    if (-not (Test-Path $baselineFile))
    {
        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = "<missing baseline file>"
                BaselineMean = ""
                CurrentMean = ""
                MeanDeltaPct = ""
                BaselineAlloc = ""
                CurrentAlloc = ""
                AllocDeltaPct = ""
                AllocDeltaBytes = ""
                Status = "FAIL"
                Notes = "Baseline CSV missing"
            })
        $failureCount++
        continue
    }

    if (-not (Test-Path $currentFile))
    {
        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = "<missing current file>"
                BaselineMean = ""
                CurrentMean = ""
                MeanDeltaPct = ""
                BaselineAlloc = ""
                CurrentAlloc = ""
                AllocDeltaPct = ""
                AllocDeltaBytes = ""
                Status = "FAIL"
                Notes = "Current CSV missing"
            })
        $failureCount++
        continue
    }

    $baselineRows = Import-Csv -Path $baselineFile
    $currentRows = Import-Csv -Path $currentFile

    $rowsToCompare = New-Object System.Collections.Generic.List[object]
    $requiredRows = Get-OptionalProperty -Object $check -Name "requiredRows" -DefaultValue $null
    if ($null -eq $requiredRows)
    {
        foreach ($row in $baselineRows)
        {
            $rowsToCompare.Add($row)
        }
    }
    else
    {
        foreach ($required in $requiredRows)
        {
            $match = $baselineRows | Where-Object { Test-RowMatch -Row $_ -Criteria $required } | Select-Object -First 1
            if ($null -eq $match)
            {
                $results.Add([pscustomobject]@{
                        Csv = $csvName
                        Key = (Build-RowKey -Row $required -KeyColumns @($required.PSObject.Properties.Name))
                        BaselineMean = ""
                        CurrentMean = ""
                        MeanDeltaPct = ""
                        BaselineAlloc = ""
                        CurrentAlloc = ""
                        AllocDeltaPct = ""
                        AllocDeltaBytes = ""
                        Status = "FAIL"
                        Notes = "Required row missing in baseline"
                    })
                $failureCount++
                continue
            }

            $rowsToCompare.Add($match)
        }
    }

    foreach ($baselineRow in $rowsToCompare)
    {
        $currentRow = $currentRows |
            Where-Object {
                $allKeyMatch = $true
                foreach ($keyColumn in $keyColumns)
                {
                    if ([string](Get-OptionalProperty -Object $_ -Name $keyColumn -DefaultValue "") -ne
                        [string](Get-OptionalProperty -Object $baselineRow -Name $keyColumn -DefaultValue ""))
                    {
                        $allKeyMatch = $false
                        break
                    }
                }
                $allKeyMatch
            } |
            Select-Object -First 1

        $rowKey = Build-RowKey -Row $baselineRow -KeyColumns $keyColumns
        if ($null -eq $currentRow)
        {
            $results.Add([pscustomobject]@{
                    Csv = $csvName
                    Key = $rowKey
                    BaselineMean = $baselineRow.Mean
                    CurrentMean = ""
                    MeanDeltaPct = ""
                    BaselineAlloc = $baselineRow.Allocated
                    CurrentAlloc = ""
                    AllocDeltaPct = ""
                    AllocDeltaBytes = ""
                    Status = "FAIL"
                    Notes = "Row missing in current results"
                })
            $failureCount++
            continue
        }

        $baselineMeanNs = Convert-TimeToNanoseconds -Value $baselineRow.Mean
        $currentMeanNs = Convert-TimeToNanoseconds -Value $currentRow.Mean
        $meanDeltaPct = (($currentMeanNs - $baselineMeanNs) / $baselineMeanNs) * 100.0

        $skipAllocationComparison = [bool](Get-OptionalProperty -Object $check -Name "skipAllocationComparison" -DefaultValue $false)

        $baselineAlloc = [string](Get-OptionalProperty -Object $baselineRow -Name "Allocated" -DefaultValue "")
        $currentAlloc = [string](Get-OptionalProperty -Object $currentRow -Name "Allocated" -DefaultValue "")
        $allocComparisonEnabled = -not $skipAllocationComparison
        if ($allocComparisonEnabled -and ([string]::IsNullOrWhiteSpace($baselineAlloc) -or [string]::IsNullOrWhiteSpace($currentAlloc)))
        {
            $results.Add([pscustomobject]@{
                    Csv = $csvName
                    Key = $rowKey
                    BaselineMean = (Format-Nanoseconds -Nanoseconds $baselineMeanNs)
                    CurrentMean = (Format-Nanoseconds -Nanoseconds $currentMeanNs)
                    MeanDeltaPct = ("{0:N2}" -f $meanDeltaPct)
                    BaselineAlloc = $baselineAlloc
                    CurrentAlloc = $currentAlloc
                    AllocDeltaPct = ""
                    AllocDeltaBytes = ""
                    Status = "FAIL"
                    Notes = "Allocation column missing; set skipAllocationComparison=true for this check if alloc should not be compared"
                })
            $failureCount++
            continue
        }

        if ($allocComparisonEnabled)
        {
            $baselineAllocBytes = Convert-SizeToBytes -Value $baselineAlloc
            $currentAllocBytes = Convert-SizeToBytes -Value $currentAlloc
            $allocDeltaBytes = $currentAllocBytes - $baselineAllocBytes
            $allocDeltaPct =
            if ($baselineAllocBytes -eq 0.0)
            {
                if ($allocDeltaBytes -gt 0.0) { 100.0 } else { 0.0 }
            }
            else
            {
                ($allocDeltaBytes / $baselineAllocBytes) * 100.0
            }
        }
        else
        {
            $baselineAllocBytes = 0.0
            $currentAllocBytes = 0.0
            $allocDeltaBytes = 0.0
            $allocDeltaPct = 0.0
        }

        $maxMeanRegression = [double](Get-OptionalProperty -Object $check -Name "maxMeanRegressionPercent" -DefaultValue $defaultMeanRegression)
        $maxAllocRegressionPct = [double](Get-OptionalProperty -Object $check -Name "maxAllocRegressionPercent" -DefaultValue $defaultAllocRegressionPct)
        $maxAllocRegressionBytes = [double](Get-OptionalProperty -Object $check -Name "maxAllocRegressionBytes" -DefaultValue $defaultAllocRegressionBytes)

        $overrides = Get-OptionalProperty -Object $check -Name "overrides" -DefaultValue @()
        foreach ($override in $overrides)
        {
            if (Test-RowMatch -Row $baselineRow -Criteria (Get-OptionalProperty -Object $override -Name "match" -DefaultValue $null))
            {
                $maxMeanRegression = [double](Get-OptionalProperty -Object $override -Name "maxMeanRegressionPercent" -DefaultValue $maxMeanRegression)
                $maxAllocRegressionPct = [double](Get-OptionalProperty -Object $override -Name "maxAllocRegressionPercent" -DefaultValue $maxAllocRegressionPct)
                $maxAllocRegressionBytes = [double](Get-OptionalProperty -Object $override -Name "maxAllocRegressionBytes" -DefaultValue $maxAllocRegressionBytes)
            }
        }

        $meanRegressed = $meanDeltaPct -gt $maxMeanRegression
        $allocRegressed = $allocComparisonEnabled -and
            ($allocDeltaPct -gt $maxAllocRegressionPct) -and
            ($allocDeltaBytes -gt $maxAllocRegressionBytes)

        $status = if ($meanRegressed -or $allocRegressed) { "FAIL" } else { "PASS" }
        if ($status -eq "FAIL")
        {
            $failureCount++
        }

        $results.Add([pscustomobject]@{
                Csv = $csvName
                Key = $rowKey
                BaselineMean = (Format-Nanoseconds -Nanoseconds $baselineMeanNs)
                CurrentMean = (Format-Nanoseconds -Nanoseconds $currentMeanNs)
                MeanDeltaPct = ("{0:N2}" -f $meanDeltaPct)
                BaselineAlloc = $(if ($allocComparisonEnabled) { Format-Bytes -Bytes $baselineAllocBytes } else { "n/a" })
                CurrentAlloc = $(if ($allocComparisonEnabled) { Format-Bytes -Bytes $currentAllocBytes } else { "n/a" })
                AllocDeltaPct = $(if ($allocComparisonEnabled) { "{0:N2}" -f $allocDeltaPct } else { "n/a" })
                AllocDeltaBytes = $(if ($allocComparisonEnabled) { "{0:N0}" -f $allocDeltaBytes } else { "n/a" })
                Status = $status
                Notes = $(if ($allocComparisonEnabled) {
                        "Mean<=${maxMeanRegression}% ; Alloc<=${maxAllocRegressionPct}% or +${maxAllocRegressionBytes}B"
                    }
                    else
                    {
                        "Mean<=${maxMeanRegression}% ; Alloc skipped"
                    })
            })
    }
}

$passCount = ($results | Where-Object Status -eq "PASS").Count
$rowCount = $results.Count
$summary = "Compared $rowCount rows against baseline. PASS=$passCount, FAIL=$failureCount"
Write-Host $summary
$results | Sort-Object Csv, Key | Format-Table Csv, Key, MeanDeltaPct, AllocDeltaPct, AllocDeltaBytes, Status -AutoSize

if (-not [string]::IsNullOrWhiteSpace($ReportPath))
{
    if (-not [System.IO.Path]::IsPathRooted($ReportPath))
    {
        $ReportPath = Join-Path $repoRoot $ReportPath
    }

    $reportDir = Split-Path -Parent $ReportPath
    if (-not [string]::IsNullOrWhiteSpace($reportDir))
    {
        New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Performance Guardrail Report")
    $lines.Add("")
    $lines.Add("- Baseline: ``$reportBaselineLabel``")
    if ($hasCheckBaselineOverrides)
    {
        $lines.Add("- Note: one or more checks use per-check ``baselineSnapshot`` overrides")
    }
    $lines.Add("- Current: ``$reportCurrentLabel``")
    $lines.Add("- Thresholds: ``$ThresholdsPath``")
    $lines.Add("- Generated (UTC): $((Get-Date).ToUniversalTime().ToString('u'))")
    $lines.Add("")
    $lines.Add($summary)
    $lines.Add("")
    $lines.Add("| CSV | Key | Mean Δ% | Alloc Δ% | Alloc Δ B | Status |")
    $lines.Add("|---|---|---:|---:|---:|---|")
    foreach ($result in ($results | Sort-Object Csv, Key))
    {
        $lines.Add("| $($result.Csv) | $($result.Key) | $($result.MeanDeltaPct) | $($result.AllocDeltaPct) | $($result.AllocDeltaBytes) | $($result.Status) |")
    }

    Set-Content -Path $ReportPath -Value $lines -Encoding UTF8
    Write-Host "Report written to $ReportPath"
}

if ($failureCount -gt 0 -and -not $NoFailOnRegression.IsPresent)
{
    throw "Performance guardrail violations detected: $failureCount"
}
