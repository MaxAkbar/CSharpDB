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

$configuredBaseline = [string](Get-OptionalProperty -Object $config -Name "baselineSnapshot" -DefaultValue "")
if ([string]::IsNullOrWhiteSpace($BaselineSnapshot))
{
    if (-not [string]::IsNullOrWhiteSpace($configuredBaseline))
    {
        $BaselineSnapshot = $configuredBaseline
    }
    else
    {
        $latest = Get-ChildItem -Path $baselineRoot -Directory |
            Where-Object { $_.Name -match "^\d{8}-\d{6}$" } |
            Sort-Object Name |
            Select-Object -Last 1

        if ($null -eq $latest)
        {
            throw "No baseline snapshots found under $baselineRoot."
        }

        $BaselineSnapshot = $latest.FullName
    }
}

if (-not [System.IO.Path]::IsPathRooted($BaselineSnapshot))
{
    $BaselineSnapshot = Join-Path $baselineRoot $BaselineSnapshot
}

if (-not (Test-Path $BaselineSnapshot))
{
    throw "Baseline snapshot not found: $BaselineSnapshot"
}

$baselineMicroDir = Join-Path $BaselineSnapshot "micro-results"
if (-not (Test-Path $baselineMicroDir))
{
    throw "Baseline micro-results directory not found: $baselineMicroDir"
}

if ([string]::IsNullOrWhiteSpace($CurrentMicroResultsDir))
{
    $CurrentMicroResultsDir = Join-Path $repoRoot "BenchmarkDotNet.Artifacts\\results"
}
elseif (-not [System.IO.Path]::IsPathRooted($CurrentMicroResultsDir))
{
    $CurrentMicroResultsDir = Join-Path $repoRoot $CurrentMicroResultsDir
}

if (-not (Test-Path $CurrentMicroResultsDir))
{
    throw "Current micro-results directory not found: $CurrentMicroResultsDir"
}

$defaults = Get-OptionalProperty -Object $config -Name "defaults" -DefaultValue $null
$defaultMeanRegression = [double](Get-OptionalProperty -Object $defaults -Name "maxMeanRegressionPercent" -DefaultValue 8.0)
$defaultAllocRegressionPct = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionPercent" -DefaultValue 10.0)
$defaultAllocRegressionBytes = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionBytes" -DefaultValue 256.0)
$hasCheckBaselineOverrides = @(
    @($config.checks) |
    Where-Object { -not [string]::IsNullOrWhiteSpace([string](Get-OptionalProperty -Object $_ -Name "baselineSnapshot" -DefaultValue "")) }
).Count -gt 0

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
    $checkBaselineMicroDir = $baselineMicroDir
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

        $checkBaselineMicroDir = Join-Path $checkBaselineSnapshotPath "micro-results"
        if (-not (Test-Path $checkBaselineMicroDir))
        {
            $results.Add([pscustomobject]@{
                    Csv = $csvName
                    Key = "<missing baseline micro-results>"
                    BaselineMean = ""
                    CurrentMean = ""
                    MeanDeltaPct = ""
                    BaselineAlloc = ""
                    CurrentAlloc = ""
                    AllocDeltaPct = ""
                    AllocDeltaBytes = ""
                    Status = "FAIL"
                    Notes = "Baseline micro-results directory missing: $checkBaselineMicroDir"
                })
            $failureCount++
            continue
        }
    }

    $baselineFile = Join-Path $checkBaselineMicroDir $csvName
    $currentFile = Join-Path $CurrentMicroResultsDir $csvName

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

        $baselineAllocBytes = Convert-SizeToBytes -Value $baselineRow.Allocated
        $currentAllocBytes = Convert-SizeToBytes -Value $currentRow.Allocated
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
        $allocRegressed = ($allocDeltaPct -gt $maxAllocRegressionPct) -and ($allocDeltaBytes -gt $maxAllocRegressionBytes)

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
                BaselineAlloc = (Format-Bytes -Bytes $baselineAllocBytes)
                CurrentAlloc = (Format-Bytes -Bytes $currentAllocBytes)
                AllocDeltaPct = ("{0:N2}" -f $allocDeltaPct)
                AllocDeltaBytes = ("{0:N0}" -f $allocDeltaBytes)
                Status = $status
                Notes = "Mean<=${maxMeanRegression}% ; Alloc<=${maxAllocRegressionPct}% or +${maxAllocRegressionBytes}B"
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
    $lines.Add("- Baseline: ``$BaselineSnapshot``")
    if ($hasCheckBaselineOverrides)
    {
        $lines.Add("- Note: one or more checks use per-check ``baselineSnapshot`` overrides")
    }
    $lines.Add("- Current: ``$CurrentMicroResultsDir``")
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
