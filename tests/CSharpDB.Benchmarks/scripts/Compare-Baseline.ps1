[CmdletBinding()]
param(
    [string]$BaselineSnapshot = "",
    [string]$CurrentMicroResultsDir = "",
    [string]$CurrentSnapshot = "",
    [string]$ThresholdsPath = "",
    [switch]$NoFailOnRegression,
    [string]$ReportPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-OptionalProperty
{
    param(
        [Parameter(Mandatory = $true)][AllowNull()][object]$Object,
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

function Normalize-ComparableText
{
    param([Parameter(Mandatory = $false)][AllowNull()][AllowEmptyString()][string]$Value)

    if ($null -eq $Value)
    {
        return ""
    }

    return (($Value.Trim().ToLowerInvariant()) -replace "\s+", " ")
}

function Get-SnapshotMachineFingerprint
{
    param([Parameter(Mandatory = $true)][string]$SnapshotDir)

    $machinePath = Join-Path $SnapshotDir "machine.json"
    if (-not (Test-Path $machinePath))
    {
        return $null
    }

    return [pscustomobject]@{
        Path = $machinePath
        Data = (Get-Content -Path $machinePath -Raw | ConvertFrom-Json)
    }
}

function Format-MachineSummary
{
    param([Parameter(Mandatory = $true)]$Fingerprint)

    $runnerId = [string](Get-OptionalProperty -Object $Fingerprint -Name "runnerId" -DefaultValue "")
    $runnerDisplay = if ([string]::IsNullOrWhiteSpace($runnerId)) { "runner=<unset>" } else { "runner=$runnerId" }
    $cpuName = [string](Get-OptionalProperty -Object $Fingerprint -Name "cpuName" -DefaultValue "<unknown cpu>")
    $cores = [string](Get-OptionalProperty -Object $Fingerprint -Name "logicalCoreCount" -DefaultValue "?")
    $os = [string](Get-OptionalProperty -Object $Fingerprint -Name "osDescription" -DefaultValue "<unknown os>")
    $dotnet = [string](Get-OptionalProperty -Object $Fingerprint -Name "dotnetVersion" -DefaultValue "<unknown dotnet>")
    return "$runnerDisplay; cpu=$cpuName; cores=$cores; os=$os; sdk=$dotnet"
}

function Get-MachineCompatibility
{
    param(
        [Parameter(Mandatory = $false)]$BaselineFingerprint,
        [Parameter(Mandatory = $true)]$CurrentFingerprint
    )

    if ($null -eq $BaselineFingerprint)
    {
        throw 'Baseline snapshot has no machine fingerprint.'
    }

    $baselineRunnerId = Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "runnerId" -DefaultValue ""))
    $currentRunnerId = Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "runnerId" -DefaultValue ""))

    $sameRunnerId = (-not [string]::IsNullOrWhiteSpace($baselineRunnerId)) -and
        (-not [string]::IsNullOrWhiteSpace($currentRunnerId)) -and
        ($baselineRunnerId -eq $currentRunnerId)
    $sameMachineName = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "machineName" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "machineName" -DefaultValue "")))
    $sameCpu = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "cpuName" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "cpuName" -DefaultValue "")))
    $sameCoreCount = ([int](Get-OptionalProperty -Object $BaselineFingerprint -Name "logicalCoreCount" -DefaultValue 0)) -eq
        ([int](Get-OptionalProperty -Object $CurrentFingerprint -Name "logicalCoreCount" -DefaultValue -1))
    $sameOsPlatform = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "osPlatform" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "osPlatform" -DefaultValue "")))
    $sameOsArchitecture = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "osArchitecture" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "osArchitecture" -DefaultValue "")))
    $sameProcessArchitecture = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "processArchitecture" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "processArchitecture" -DefaultValue "")))
    $sameOsDescription = (Normalize-ComparableText ([string](Get-OptionalProperty -Object $BaselineFingerprint -Name "osDescription" -DefaultValue ""))) -eq
        (Normalize-ComparableText ([string](Get-OptionalProperty -Object $CurrentFingerprint -Name "osDescription" -DefaultValue "")))

    $sameHardware = $sameCpu -and $sameCoreCount -and $sameOsPlatform -and $sameOsArchitecture -and $sameProcessArchitecture

    if (($sameRunnerId -or ($sameMachineName -and $sameOsDescription)) -and $sameHardware)
    {
        return [pscustomobject]@{
            Classification = "exact"
            Enforcement = "strict"
            Summary = $(if ($sameRunnerId) { "Strict (matching perf runner)" } else { "Strict (same machine fingerprint)" })
            Notes = $(if ($sameRunnerId) { "Current runner matches the baseline perf runner." } else { "Current machine fingerprint matches the baseline snapshot." })
        }
    }

    if ($sameHardware)
    {
        return [pscustomobject]@{
            Classification = "compatible"
            Enforcement = "warn"
            Summary = "Warn-only (compatible hardware/runtime)"
            Notes = "Hardware/runtime match the baseline, but this is not the authoritative perf runner."
        }
    }

    return [pscustomobject]@{
        Classification = "different"
        Enforcement = "skip"
        Summary = "Skipped (different machine fingerprint)"
        Notes = "Machine fingerprint differs materially from the baseline; regression enforcement is skipped."
    }
}

function Convert-TimeToNanoseconds
{
    param([Parameter(Mandatory = $true)][string]$Value)

    $text = $Value.Trim("'").Trim()
    $text = $text -replace ",", ""
    $text = $text.Replace([string][char]0x03BC, "u")

    if ($text -match "^([0-9]*\.?[0-9]+)\s*(ns|us|ms|s)$")
    {
        $number = [double]$matches[1]
        $unit = $matches[2]
        switch ($unit)
        {
            "ns" { return $number }
            "us" { return $number * 1000.0 }
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

function Test-MetricUnavailable
{
    param([Parameter(Mandatory = $true)][string]$Value)

    $text = $Value.Trim("'").Trim()
    return [string]::IsNullOrWhiteSpace($text) -or $text -eq "NA" -or $text -eq "?"
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

function Get-ExtraInfoValue
{
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string]$Key
    )

    $extraInfo = [string](Get-OptionalProperty -Object $Row -Name "ExtraInfo" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($extraInfo))
    {
        return $null
    }

    $escapedKey = [regex]::Escape($Key)
    $match = [regex]::Match($extraInfo, "(?:^|[,;]\s*)$escapedKey=([^,;]+)")
    if (-not $match.Success)
    {
        return $null
    }

    return $match.Groups[1].Value.Trim()
}

function Convert-ComparableNumber
{
    param(
        [Parameter(Mandatory = $true)][string]$Value,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $text = $Value.Trim("'").Trim() -replace ",", ""
    if ($text -match "^([+-]?[0-9]*\.?[0-9]+)$")
    {
        $number = [double]$matches[1]
        if (-not [double]::IsFinite($number)) { throw "Nonfinite numeric $Description value '$Value'." }
        return $number
    }

    throw "Unable to parse numeric $Description value '$Value'."
}

function Test-ExtraInfoChecks
{
    param(
        [Parameter(Mandatory = $true)]$BaselineRow,
        [Parameter(Mandatory = $true)]$CurrentRow,
        [Parameter(Mandatory = $false)]$Checks
    )

    $notes = New-Object System.Collections.Generic.List[string]
    $regressed = $false

    if ($null -eq $Checks)
    {
        return [pscustomobject]@{
            Regressed = $false
            Notes = [string[]]@()
        }
    }

    foreach ($check in @($Checks))
    {
        if ($null -eq $check)
        {
            continue
        }

        $key = [string](Get-OptionalProperty -Object $check -Name "key" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($key))
        {
            throw "extraInfoChecks entries must define a key."
        }

        $baselineRaw = Get-ExtraInfoValue -Row $BaselineRow -Key $key
        $currentRaw = Get-ExtraInfoValue -Row $CurrentRow -Key $key
        if ($null -eq $baselineRaw -or $null -eq $currentRaw)
        {
            throw "ExtraInfo $key missing in baseline or current results."
        }

        $baselineValue = Convert-ComparableNumber -Value $baselineRaw -Description "baseline ExtraInfo '$key'"
        $currentValue = Convert-ComparableNumber -Value $currentRaw -Description "current ExtraInfo '$key'"

        $minRatioText = [string](Get-OptionalProperty -Object $check -Name "minRatio" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($minRatioText))
        {
            $minRatio = [double]$minRatioText
            $required = $baselineValue * $minRatio
            if ($currentValue -lt $required)
            {
                $regressed = $true
                $formattedRatio = "{0:N2}" -f $minRatio
                $notes.Add("$key>=$formattedRatio x baseline (baseline=$baselineRaw, current=$currentRaw)") | Out-Null
                continue
            }

            $notes.Add("$key OK (baseline=$baselineRaw, current=$currentRaw)") | Out-Null
        }

        $maxRatioText = [string](Get-OptionalProperty -Object $check -Name "maxRatio" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($maxRatioText))
        {
            $maxRatio = [double]$maxRatioText
            $allowed = $baselineValue * $maxRatio
            if ($currentValue -gt $allowed)
            {
                $regressed = $true
                $formattedRatio = "{0:N2}" -f $maxRatio
                $notes.Add("$key<=$formattedRatio x baseline (baseline=$baselineRaw, current=$currentRaw)") | Out-Null
                continue
            }

            $notes.Add("$key OK (baseline=$baselineRaw, current=$currentRaw)") | Out-Null
        }

        $minValueText = [string](Get-OptionalProperty -Object $check -Name "minValue" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($minValueText))
        {
            $minValue = [double]$minValueText
            if ($currentValue -lt $minValue)
            {
                $regressed = $true
                $formattedMinValue = "{0:N2}" -f $minValue
                $notes.Add("$key>=$formattedMinValue (current=$currentRaw)") | Out-Null
                continue
            }

            $formattedMinValue = "{0:N2}" -f $minValue
            $notes.Add("$key>=$formattedMinValue OK (current=$currentRaw)") | Out-Null
        }

        $maxValueText = [string](Get-OptionalProperty -Object $check -Name "maxValue" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($maxValueText))
        {
            $maxValue = [double]$maxValueText
            if ($currentValue -gt $maxValue)
            {
                $regressed = $true
                $formattedMaxValue = "{0:N2}" -f $maxValue
                $notes.Add("$key<=$formattedMaxValue (current=$currentRaw)") | Out-Null
                continue
            }

            $formattedMaxValue = "{0:N2}" -f $maxValue
            $notes.Add("$key<=$formattedMaxValue OK (current=$currentRaw)") | Out-Null
        }
    }

    return [pscustomobject]@{
        Regressed = $regressed
        Notes = [string[]]$notes.ToArray()
    }
}

function Test-CurrentExtraInfoChecks
{
    param(
        [Parameter(Mandatory = $true)]$CurrentRow,
        [Parameter(Mandatory = $false)]$Checks
    )

    $notes = New-Object System.Collections.Generic.List[string]
    $regressed = $false

    if ($null -eq $Checks)
    {
        return [pscustomobject]@{
            Regressed = $false
            Notes = [string[]]@()
        }
    }

    foreach ($check in @($Checks))
    {
        if ($null -eq $check)
        {
            continue
        }

        $key = [string](Get-OptionalProperty -Object $check -Name "key" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($key))
        {
            throw "requiredCurrentRows extraInfoChecks entries must define a key."
        }

        $currentRaw = Get-ExtraInfoValue -Row $CurrentRow -Key $key
        if ($null -eq $currentRaw)
        {
            throw "Required current ExtraInfo $key missing."
        }

        $currentValue = Convert-ComparableNumber -Value $currentRaw -Description "current ExtraInfo '$key'"

        $minRatioText = [string](Get-OptionalProperty -Object $check -Name "minRatio" -DefaultValue "")
        $maxRatioText = [string](Get-OptionalProperty -Object $check -Name "maxRatio" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($minRatioText) -or -not [string]::IsNullOrWhiteSpace($maxRatioText))
        {
            throw "$key ratio check requires a baseline row."
        }

        $minValueText = [string](Get-OptionalProperty -Object $check -Name "minValue" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($minValueText))
        {
            $minValue = [double]$minValueText
            if ($currentValue -lt $minValue)
            {
                $regressed = $true
                $formattedMinValue = "{0:N2}" -f $minValue
                $notes.Add("$key>=$formattedMinValue (current=$currentRaw)") | Out-Null
                continue
            }

            $formattedMinValue = "{0:N2}" -f $minValue
            $notes.Add("$key>=$formattedMinValue OK (current=$currentRaw)") | Out-Null
        }

        $maxValueText = [string](Get-OptionalProperty -Object $check -Name "maxValue" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($maxValueText))
        {
            $maxValue = [double]$maxValueText
            if ($currentValue -gt $maxValue)
            {
                $regressed = $true
                $formattedMaxValue = "{0:N2}" -f $maxValue
                $notes.Add("$key<=$formattedMaxValue (current=$currentRaw)") | Out-Null
                continue
            }

            $formattedMaxValue = "{0:N2}" -f $maxValue
            $notes.Add("$key<=$formattedMaxValue OK (current=$currentRaw)") | Out-Null
        }
    }

    return [pscustomobject]@{
        Regressed = $regressed
        Notes = [string[]]$notes.ToArray()
    }
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

function Add-ComparisonResult
{
    param(
        [Parameter(Mandatory = $true)]$Results,
        [Parameter(Mandatory = $true)][string]$Csv,
        [Parameter(Mandatory = $true)][string]$Key,
        [string]$Metric = "Mean",
        [string]$BaselineMean = "",
        [string]$CurrentMean = "",
        [string]$MeanDeltaPct = "",
        [string]$BaselineAlloc = "",
        [string]$CurrentAlloc = "",
        [string]$AllocDeltaPct = "",
        [string]$AllocDeltaBytes = "",
        [Parameter(Mandatory = $true)][string]$Enforcement,
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][string]$Notes
    )

    $Results.Add([pscustomobject]@{
            Csv = $Csv
            Key = $Key
            Metric = $Metric
            BaselineMean = $BaselineMean
            CurrentMean = $CurrentMean
            MeanDeltaPct = $MeanDeltaPct
            BaselineAlloc = $BaselineAlloc
            CurrentAlloc = $CurrentAlloc
            AllocDeltaPct = $AllocDeltaPct
            AllocDeltaBytes = $AllocDeltaBytes
            Enforcement = $Enforcement
            Status = $Status
            Notes = $Notes
        }) | Out-Null
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$benchDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
$repoRoot = (Resolve-Path (Join-Path $benchDir "..\\..")).Path

$reportWritten = $false
if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
    if (-not [IO.Path]::IsPathRooted($ReportPath)) { $ReportPath = Join-Path $repoRoot $ReportPath }
    New-Item -ItemType Directory -Path (Split-Path -Parent $ReportPath) -Force | Out-Null
    @('# Performance Guardrail Report', '', 'Status: INCOMPLETE', '', 'Comparison has not completed.') |
        Set-Content -LiteralPath $ReportPath -Encoding utf8
}
trap {
    if (-not $reportWritten -and -not [string]::IsNullOrWhiteSpace($ReportPath)) {
        @('# Performance Guardrail Report', '', 'Status: INVALID', '',
            "Generated (UTC): $([DateTime]::UtcNow.ToString('u'))", '', $_.Exception.Message) |
            Set-Content -LiteralPath $ReportPath -Encoding utf8
    }
    throw $_
}

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
if (@($config.checks).Count -eq 0) { throw 'Guardrails require at least one configured check.' }
$explicitBaseline = -not [string]::IsNullOrWhiteSpace($BaselineSnapshot)
if ([string]::IsNullOrWhiteSpace($CurrentSnapshot)) { throw 'Current snapshot manifest and captured fingerprint are required, including in report mode. Use -CurrentSnapshot with a fresh capture.' }
$currentManifest = $null
if (-not [string]::IsNullOrWhiteSpace($CurrentSnapshot)) {
    $CurrentSnapshot = (Resolve-Path -LiteralPath $CurrentSnapshot).Path
    $currentManifest = Assert-PerfSnapshot $CurrentSnapshot
}
$baselineRoot = Join-Path $benchDir "baselines"
$hasCheckBaselineOverrides = (-not $explicitBaseline) -and @(
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
    throw 'No baseline snapshot found. Evidence integrity requires a baseline even in report mode.'
}

$checksUsingDefaultCurrentResultsDir = @(
    @($config.checks) |
    Where-Object { [string]::IsNullOrWhiteSpace([string](Get-OptionalProperty -Object $_ -Name "currentResultsDir" -DefaultValue "")) }
).Count -gt 0

$resolvedDefaultCurrentResultsDir = $null
if ($checksUsingDefaultCurrentResultsDir -and $null -eq $currentManifest)
{
    $resolvedDefaultCurrentResultsDir = Resolve-CurrentMicroResultsDirectory `
        -BenchDir $benchDir `
        -RepoRoot $repoRoot `
        -ConfiguredPath $CurrentMicroResultsDir
}

$reportCurrentLabel =
if ($null -ne $currentManifest) { $CurrentSnapshot }
elseif ($checksUsingDefaultCurrentResultsDir)
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

$defaults = Get-OptionalProperty -Object $config -Name "defaults" -DefaultValue $null
$defaultMeanRegression = [double](Get-OptionalProperty -Object $defaults -Name "maxMeanRegressionPercent" -DefaultValue 8.0)
$defaultAllocRegressionPct = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionPercent" -DefaultValue 10.0)
$defaultAllocRegressionBytes = [double](Get-OptionalProperty -Object $defaults -Name "maxAllocRegressionBytes" -DefaultValue 256.0)

$currentMachineFingerprint = $currentManifest.machine
Assert-PerfMachineFingerprint $currentMachineFingerprint
$baselineMachineFingerprintCache = @{}
$snapshotManifestCache = @{}
$machineCompatibilityCache = @{}
$results = New-Object System.Collections.Generic.List[object]
$failureCount = 0
$enforceableCount = 0

foreach ($check in $config.checks)
{
    $csvName = [string]$check.csv
    $checkEnforceableCount = 0
    $minimumEnforceableComparisons = [int](Get-OptionalProperty $check 'minimumEnforceableComparisons' 1)
    if ($minimumEnforceableComparisons -le 0) { throw "Check '$csvName' requires a positive minimumEnforceableComparisons." }
    $keyColumns = @($check.keyColumns)
    if ($keyColumns.Count -eq 0)
    {
        throw "Check '$csvName' must define at least one key column."
    }

    $checkBaselineSnapshot = [string](Get-OptionalProperty -Object $check -Name "baselineSnapshot" -DefaultValue "")
    $checkBaselineSnapshotDir = $baselineSnapshotDir
    if (-not $explicitBaseline -and -not [string]::IsNullOrWhiteSpace($checkBaselineSnapshot))
    {
        $checkBaselineSnapshotPath = $checkBaselineSnapshot
        if (-not [System.IO.Path]::IsPathRooted($checkBaselineSnapshotPath))
        {
            $checkBaselineSnapshotPath = Join-Path $baselineRoot $checkBaselineSnapshotPath
        }

        if (-not (Test-Path $checkBaselineSnapshotPath))
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key "<missing baseline snapshot>" `
                -Enforcement "Strict" `
                -Status "INVALID" `
                -Notes "Baseline snapshot missing: $checkBaselineSnapshotPath"
            $failureCount++
            continue
        }

        $checkBaselineSnapshotDir = (Resolve-Path $checkBaselineSnapshotPath).Path
    }
    elseif (-not $baselineResolved)
    {
        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key "<missing global baseline snapshot>" `
            -Enforcement "Strict" `
            -Status "INVALID" `
            -Notes "Global baseline snapshot missing for check without baselineSnapshot override"
        $failureCount++
        continue
    }

    if (-not $baselineMachineFingerprintCache.ContainsKey($checkBaselineSnapshotDir))
    {
        $snapshotManifestCache[$checkBaselineSnapshotDir] = Assert-PerfSnapshot $checkBaselineSnapshotDir
        $baselineMachineFingerprintCache[$checkBaselineSnapshotDir] = Get-SnapshotMachineFingerprint -SnapshotDir $checkBaselineSnapshotDir
        if ($null -ne $currentManifest -and $snapshotManifestCache[$checkBaselineSnapshotDir].harnessIdentity -cne $currentManifest.harnessIdentity) {
            throw "Base/head harness identity mismatch for $csvName."
        }
    }

    if (-not $machineCompatibilityCache.ContainsKey($checkBaselineSnapshotDir))
    {
        $baselineFingerprintEntry = $baselineMachineFingerprintCache[$checkBaselineSnapshotDir]
        $baselineFingerprintData =
        if ($null -eq $baselineFingerprintEntry)
        {
            $null
        }
        else
        {
            $baselineFingerprintEntry.Data
        }

        $machineCompatibilityCache[$checkBaselineSnapshotDir] = Get-MachineCompatibility `
            -BaselineFingerprint $baselineFingerprintData `
            -CurrentFingerprint $currentMachineFingerprint
    }

    $machineCompatibility = $machineCompatibilityCache[$checkBaselineSnapshotDir]
    if ($null -ne $currentManifest -and
        ($snapshotManifestCache[$checkBaselineSnapshotDir].runtime | ConvertTo-Json -Compress) -cne ($currentManifest.runtime | ConvertTo-Json -Compress)) {
        throw "Base/head runtime identity mismatch for $csvName."
    }

    $baselineSubDir = [string](Get-OptionalProperty -Object $check -Name "baselineSubDir" -DefaultValue "micro-results")
    $checkBaselineResultsDir = Join-Path $checkBaselineSnapshotDir $baselineSubDir
    if (-not (Test-Path $checkBaselineResultsDir))
    {
        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key "<missing baseline results directory>" `
            -Enforcement $machineCompatibility.Summary `
            -Status "INVALID" `
            -Notes "Baseline results directory missing: $checkBaselineResultsDir"
        $failureCount++
        continue
    }

    $configuredCurrentResultsDir = [string](Get-OptionalProperty -Object $check -Name "currentResultsDir" -DefaultValue "")
    $checkCurrentResultsDir =
    if ($null -ne $currentManifest)
    {
        Join-Path $CurrentSnapshot $baselineSubDir
    }
    elseif ([string]::IsNullOrWhiteSpace($configuredCurrentResultsDir))
    {
        $resolvedDefaultCurrentResultsDir
    }
    else
    {
        Resolve-ResultsDirectory -RepoRoot $repoRoot -ConfiguredPath $configuredCurrentResultsDir
    }

    if ([string]::IsNullOrWhiteSpace($checkCurrentResultsDir) -or -not (Test-Path $checkCurrentResultsDir))
    {
        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key "<missing current results directory>" `
            -Enforcement $machineCompatibility.Summary `
            -Status "INVALID" `
            -Notes "Current results directory missing: $checkCurrentResultsDir"
        $failureCount++
        continue
    }

    $baselineFile = Join-Path $checkBaselineResultsDir $csvName
    $currentFile = Join-Path $checkCurrentResultsDir $csvName
    Assert-PerfSnapshotCsv -Manifest $snapshotManifestCache[$checkBaselineSnapshotDir] -SnapshotDir $checkBaselineSnapshotDir -CsvPath $baselineFile
    if ($null -ne $currentManifest) { Assert-PerfSnapshotCsv -Manifest $currentManifest -SnapshotDir $CurrentSnapshot -CsvPath $currentFile }

    if (-not (Test-Path $baselineFile))
    {
        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key "<missing baseline file>" `
            -Enforcement $machineCompatibility.Summary `
            -Status "INVALID" `
            -Notes "Baseline CSV missing"
        $failureCount++
        continue
    }

    if (-not (Test-Path $currentFile))
    {
        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key "<missing current file>" `
            -Enforcement $machineCompatibility.Summary `
            -Status "INVALID" `
            -Notes "Current CSV missing"
        $failureCount++
        continue
    }

    $baselineRows = @(Import-Csv -Path $baselineFile)
    $currentRows = @(Import-Csv -Path $currentFile)
    if ($baselineRows.Count -eq 0 -or $currentRows.Count -eq 0) { throw "Zero rows in baseline or current CSV: $csvName" }
    foreach ($rows in @(@{ label = 'baseline'; rows = $baselineRows }, @{ label = 'current'; rows = $currentRows })) {
        $keys = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($row in $rows.rows) {
            foreach ($column in $keyColumns) {
                if ([string]::IsNullOrWhiteSpace([string](Get-OptionalProperty $row $column ''))) { throw "Missing key column $column in $($rows.label) $csvName." }
            }
            if (-not $keys.Add((Build-RowKey $row $keyColumns))) { throw "Duplicate row in $($rows.label) $csvName." }
        }
    }

    $requiredCurrentRows = Get-OptionalProperty -Object $check -Name "requiredCurrentRows" -DefaultValue @()
    foreach ($requiredCurrent in @($requiredCurrentRows))
    {
        if ($null -eq $requiredCurrent)
        {
            continue
        }

        $criteria = Get-OptionalProperty -Object $requiredCurrent -Name "match" -DefaultValue $requiredCurrent
        $currentMatch = $currentRows |
            Where-Object { Test-RowMatch -Row $_ -Criteria $criteria } |
            Select-Object -First 1
        $requiredCurrentKey = Build-RowKey -Row $criteria -KeyColumns @($criteria.PSObject.Properties.Name)
        if ($null -eq $currentMatch)
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key $requiredCurrentKey `
                -Enforcement $machineCompatibility.Summary `
                -Status "INVALID" `
                -Notes "Required current row missing"
            $failureCount++
            continue
        }

        $currentOnlyExtraInfoChecks = Get-OptionalProperty -Object $requiredCurrent -Name "extraInfoChecks" -DefaultValue @()
        $currentOnlyMetric = [string](Get-OptionalProperty $check 'metricColumn' 'Mean')
        $currentOnlyValue = Convert-TimeToNanoseconds ([string](Get-OptionalProperty $currentMatch $currentOnlyMetric ''))
        if ($currentOnlyValue -le 0 -or -not [double]::IsFinite($currentOnlyValue)) { throw "Invalid numeric $currentOnlyMetric in required current row $requiredCurrentKey." }
        $currentOnlyExtraInfoEvaluation = Test-CurrentExtraInfoChecks -CurrentRow $currentMatch -Checks $currentOnlyExtraInfoChecks
        $currentOnlyStatus =
        if ($machineCompatibility.Classification -eq "different")
        {
            "SKIP"
        }
        elseif ($currentOnlyExtraInfoEvaluation.Regressed)
        {
            $(if ($machineCompatibility.Classification -eq "compatible") { "WARN" } else { "FAIL" })
        }
        else
        {
            "PASS"
        }

        if ($currentOnlyStatus -eq "FAIL")
        {
            $failureCount++
        }

        $currentOnlyNotes =
        if ($currentOnlyExtraInfoEvaluation.Notes.Count -gt 0)
        {
            "Current row present ; $($currentOnlyExtraInfoEvaluation.Notes -join '; ')"
        }
        else
        {
            "Current row present"
        }

        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key $requiredCurrentKey `
            -CurrentMean ([string](Get-OptionalProperty -Object $currentMatch -Name "Mean" -DefaultValue "")) `
            -CurrentAlloc ([string](Get-OptionalProperty -Object $currentMatch -Name "Allocated" -DefaultValue "")) `
            -Enforcement $machineCompatibility.Summary `
            -Status $currentOnlyStatus `
            -Notes $currentOnlyNotes
    }

    $rowsToCompare = New-Object System.Collections.Generic.List[object]
    $requiredRows = Get-OptionalProperty -Object $check -Name "requiredRows" -DefaultValue $null
    if ($null -eq $requiredRows)
    {
        # Full-table checks require identical key sets, including head-only rows.
        $baselineKeys = @($baselineRows | ForEach-Object { Build-RowKey $_ $keyColumns })
        foreach ($row in $currentRows) {
            if ((Build-RowKey $row $keyColumns) -cnotin $baselineKeys) { throw "Row missing in baseline results: $csvName / $(Build-RowKey $row $keyColumns)" }
        }
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
                Add-ComparisonResult -Results $results `
                    -Csv $csvName `
                    -Key (Build-RowKey -Row $required -KeyColumns @($required.PSObject.Properties.Name)) `
                    -Enforcement $machineCompatibility.Summary `
                    -Status "INVALID" `
                    -Notes "Required row missing in baseline"
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

        $maxMeanRegression = [double](Get-OptionalProperty -Object $check -Name "maxMeanRegressionPercent" -DefaultValue $defaultMeanRegression)
        $maxAllocRegressionPct = [double](Get-OptionalProperty -Object $check -Name "maxAllocRegressionPercent" -DefaultValue $defaultAllocRegressionPct)
        $maxAllocRegressionBytes = [double](Get-OptionalProperty -Object $check -Name "maxAllocRegressionBytes" -DefaultValue $defaultAllocRegressionBytes)
        $skipMeanComparison = [bool](Get-OptionalProperty -Object $check -Name "skipMeanComparison" -DefaultValue $false)
        $skipAllocationComparison = [bool](Get-OptionalProperty -Object $check -Name "skipAllocationComparison" -DefaultValue $false)
        $metricColumn = [string](Get-OptionalProperty -Object $check -Name "metricColumn" -DefaultValue "Mean")
        if ([string]::IsNullOrWhiteSpace($metricColumn))
        {
            throw "Check '$csvName' has an empty metricColumn value."
        }

        $extraInfoChecks = Get-OptionalProperty -Object $check -Name "extraInfoChecks" -DefaultValue @()

        $overrides = Get-OptionalProperty -Object $check -Name "overrides" -DefaultValue @()
        foreach ($override in $overrides)
        {
            if (Test-RowMatch -Row $baselineRow -Criteria (Get-OptionalProperty -Object $override -Name "match" -DefaultValue $null))
            {
                $maxMeanRegression = [double](Get-OptionalProperty -Object $override -Name "maxMeanRegressionPercent" -DefaultValue $maxMeanRegression)
                $maxAllocRegressionPct = [double](Get-OptionalProperty -Object $override -Name "maxAllocRegressionPercent" -DefaultValue $maxAllocRegressionPct)
                $maxAllocRegressionBytes = [double](Get-OptionalProperty -Object $override -Name "maxAllocRegressionBytes" -DefaultValue $maxAllocRegressionBytes)
                $skipMeanComparison = [bool](Get-OptionalProperty -Object $override -Name "skipMeanComparison" -DefaultValue $skipMeanComparison)
                $skipAllocationComparison = [bool](Get-OptionalProperty -Object $override -Name "skipAllocationComparison" -DefaultValue $skipAllocationComparison)
                $metricColumn = [string](Get-OptionalProperty -Object $override -Name "metricColumn" -DefaultValue $metricColumn)
                if ([string]::IsNullOrWhiteSpace($metricColumn))
                {
                    throw "Check '$csvName' override for '$rowKey' has an empty metricColumn value."
                }

                $overrideExtraInfoChecks = Get-OptionalProperty -Object $override -Name "extraInfoChecks" -DefaultValue $null
                if ($null -ne $overrideExtraInfoChecks)
                {
                    $extraInfoChecks = $overrideExtraInfoChecks
                }
            }
        }

        if ($null -eq $currentRow)
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key $rowKey `
                -Metric $metricColumn `
                -BaselineMean ([string](Get-OptionalProperty -Object $baselineRow -Name $metricColumn -DefaultValue "")) `
                -BaselineAlloc ([string](Get-OptionalProperty -Object $baselineRow -Name "Allocated" -DefaultValue "")) `
                -Enforcement $machineCompatibility.Summary `
                -Status "INVALID" `
                -Notes "Row missing in current results"
            $failureCount++
            continue
        }

        $baselineMeanRaw = [string](Get-OptionalProperty -Object $baselineRow -Name $metricColumn -DefaultValue "")
        $currentMeanRaw = [string](Get-OptionalProperty -Object $currentRow -Name $metricColumn -DefaultValue "")
        if ((Test-MetricUnavailable -Value $baselineMeanRaw) -or (Test-MetricUnavailable -Value $currentMeanRaw))
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key $rowKey `
                -Metric $metricColumn `
                -BaselineMean $baselineMeanRaw `
                -CurrentMean $currentMeanRaw `
                -BaselineAlloc ([string](Get-OptionalProperty -Object $baselineRow -Name "Allocated" -DefaultValue "")) `
                -CurrentAlloc ([string](Get-OptionalProperty -Object $currentRow -Name "Allocated" -DefaultValue "")) `
                -Enforcement $machineCompatibility.Summary `
                -Status "INVALID" `
                -Notes "Benchmark did not produce a numeric $metricColumn value"
            $failureCount++
            continue
        }

        $baselineMeanNs = Convert-TimeToNanoseconds -Value $baselineMeanRaw
        $currentMeanNs = Convert-TimeToNanoseconds -Value $currentMeanRaw
        if ($baselineMeanNs -le 0 -or $currentMeanNs -le 0 -or -not [double]::IsFinite($baselineMeanNs) -or -not [double]::IsFinite($currentMeanNs)) { throw "Invalid numeric $metricColumn in $csvName / $rowKey." }
        $meanDeltaPct = (($currentMeanNs - $baselineMeanNs) / $baselineMeanNs) * 100.0

        $baselineAlloc = [string](Get-OptionalProperty -Object $baselineRow -Name "Allocated" -DefaultValue "")
        $currentAlloc = [string](Get-OptionalProperty -Object $currentRow -Name "Allocated" -DefaultValue "")
        $allocComparisonEnabled = -not $skipAllocationComparison
        if ($allocComparisonEnabled -and ([string]::IsNullOrWhiteSpace($baselineAlloc) -or [string]::IsNullOrWhiteSpace($currentAlloc)))
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key $rowKey `
                -Metric $metricColumn `
                -BaselineMean (Format-Nanoseconds -Nanoseconds $baselineMeanNs) `
                -CurrentMean (Format-Nanoseconds -Nanoseconds $currentMeanNs) `
                -MeanDeltaPct ("{0:N2}" -f $meanDeltaPct) `
                -BaselineAlloc $baselineAlloc `
                -CurrentAlloc $currentAlloc `
                -Enforcement $machineCompatibility.Summary `
                -Status "INVALID" `
                -Notes "Allocation column missing; set skipAllocationComparison=true for this check if alloc should not be compared"
            $failureCount++
            continue
        }
        if ($allocComparisonEnabled -and ((Test-MetricUnavailable -Value $baselineAlloc) -or (Test-MetricUnavailable -Value $currentAlloc)))
        {
            Add-ComparisonResult -Results $results `
                -Csv $csvName `
                -Key $rowKey `
                -Metric $metricColumn `
                -BaselineMean (Format-Nanoseconds -Nanoseconds $baselineMeanNs) `
                -CurrentMean (Format-Nanoseconds -Nanoseconds $currentMeanNs) `
                -MeanDeltaPct ("{0:N2}" -f $meanDeltaPct) `
                -BaselineAlloc $baselineAlloc `
                -CurrentAlloc $currentAlloc `
                -Enforcement $machineCompatibility.Summary `
                -Status "INVALID" `
                -Notes "Benchmark did not produce a numeric Allocated value; set skipAllocationComparison=true for this check if alloc should not be compared"
            $failureCount++
            continue
        }

        if ($allocComparisonEnabled)
        {
            $baselineAllocBytes = Convert-SizeToBytes -Value $baselineAlloc
            $currentAllocBytes = Convert-SizeToBytes -Value $currentAlloc
            if (-not [double]::IsFinite($baselineAllocBytes) -or -not [double]::IsFinite($currentAllocBytes)) { throw "Invalid numeric allocation in $csvName / $rowKey." }
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

        $extraInfoEvaluation = Test-ExtraInfoChecks -BaselineRow $baselineRow -CurrentRow $currentRow -Checks $extraInfoChecks
        $hasMetric = (-not $skipMeanComparison) -or $allocComparisonEnabled -or @($extraInfoChecks).Count -gt 0
        if (-not $hasMetric) { throw "Check '$csvName' has no comparison metrics for $rowKey." }
        if ($machineCompatibility.Enforcement -eq 'strict') {
            $checkEnforceableCount++
            $enforceableCount++
        }

        $meanRegressed = (-not $skipMeanComparison) -and ($meanDeltaPct -gt $maxMeanRegression)
        $allocRegressed = $allocComparisonEnabled -and
            ($allocDeltaPct -gt $maxAllocRegressionPct) -and
            ($allocDeltaBytes -gt $maxAllocRegressionBytes)

        if ($machineCompatibility.Classification -eq "different")
        {
            $status = "SKIP"
        }
        elseif ($meanRegressed -or $allocRegressed -or $extraInfoEvaluation.Regressed)
        {
            $status = $(if ($machineCompatibility.Classification -eq "compatible") { "WARN" } else { "FAIL" })
        }
        else
        {
            $status = "PASS"
        }

        if ($status -eq "FAIL")
        {
            $failureCount++
        }

        $metricThresholdNote = if ($skipMeanComparison) { "$metricColumn skipped" } else { "$metricColumn<=${maxMeanRegression}%" }
        $thresholdNote =
        if ($allocComparisonEnabled)
        {
            "$metricThresholdNote ; Alloc<=${maxAllocRegressionPct}% or +${maxAllocRegressionBytes}B"
        }
        else
        {
            "$metricThresholdNote ; Alloc skipped"
        }

        if ($extraInfoEvaluation.Notes.Count -gt 0)
        {
            $thresholdNote = "$thresholdNote ; $($extraInfoEvaluation.Notes -join '; ')"
        }

        $machineNote =
        switch ($machineCompatibility.Classification)
        {
            "compatible" { "$thresholdNote ; $($machineCompatibility.Notes)" }
            "different" { "$thresholdNote ; $($machineCompatibility.Notes)" }
            default { $thresholdNote }
        }

        Add-ComparisonResult -Results $results `
            -Csv $csvName `
            -Key $rowKey `
            -Metric $metricColumn `
            -BaselineMean (Format-Nanoseconds -Nanoseconds $baselineMeanNs) `
            -CurrentMean (Format-Nanoseconds -Nanoseconds $currentMeanNs) `
            -MeanDeltaPct ("{0:N2}" -f $meanDeltaPct) `
            -BaselineAlloc $(if ($allocComparisonEnabled) { Format-Bytes -Bytes $baselineAllocBytes } else { "n/a" }) `
            -CurrentAlloc $(if ($allocComparisonEnabled) { Format-Bytes -Bytes $currentAllocBytes } else { "n/a" }) `
            -AllocDeltaPct $(if ($allocComparisonEnabled) { "{0:N2}" -f $allocDeltaPct } else { "n/a" }) `
            -AllocDeltaBytes $(if ($allocComparisonEnabled) { "{0:N0}" -f $allocDeltaBytes } else { "n/a" }) `
            -Enforcement $machineCompatibility.Summary `
            -Status $status `
            -Notes $machineNote
    }
    if ($checkEnforceableCount -lt [Math]::Max($minimumEnforceableComparisons, $rowsToCompare.Count)) {
        Add-ComparisonResult -Results $results -Csv $csvName -Key '<comparison coverage>' -Enforcement 'Strict' -Status 'INVALID' -Notes "Insufficient enforceable comparisons: $checkEnforceableCount. Every selected baseline row and at least $minimumEnforceableComparisons comparison(s) are required."
    }
}

$failureCount = @($results | Where-Object Status -eq "FAIL").Count
$passCount = @($results | Where-Object Status -eq "PASS").Count
$warnCount = @($results | Where-Object Status -eq "WARN").Count
$skipCount = @($results | Where-Object Status -eq "SKIP").Count
$rowCount = $results.Count
$integrityFailureCount = @($results | Where-Object Status -eq 'INVALID').Count
$summary = "Compared $rowCount rows against baseline. PASS=$passCount, WARN=$warnCount, SKIP=$skipCount, FAIL=$failureCount, INVALID=$integrityFailureCount, ENFORCEABLE=$enforceableCount"
Write-Host $summary
$results | Sort-Object Csv, Key | Format-Table Csv, Key, Metric, MeanDeltaPct, AllocDeltaPct, AllocDeltaBytes, Enforcement, Status -AutoSize

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
    $lines.Add("- Current machine: ``$(Format-MachineSummary -Fingerprint $currentMachineFingerprint)``")
    $lines.Add("- Machine policy: identical captured runtimes required; strict on matching perf runner or same-machine fingerprint; compatible/different machines cannot satisfy comparison coverage")
    $lines.Add("")
    $lines.Add($summary)
    $lines.Add("")
    $lines.Add("| CSV | Key | Metric | Metric Delta% | Alloc Delta% | Alloc Delta B | Enforcement | Status | Notes |")
    $lines.Add("|---|---|---|---:|---:|---:|---|---|---|")
    foreach ($result in ($results | Sort-Object Csv, Key))
    {
        $notes = [string]$result.Notes
        $notes = $notes -replace "\|", "\\|"
        $lines.Add("| $($result.Csv) | $($result.Key) | $($result.Metric) | $($result.MeanDeltaPct) | $($result.AllocDeltaPct) | $($result.AllocDeltaBytes) | $($result.Enforcement) | $($result.Status) | $notes |")
    }

    Set-Content -Path $ReportPath -Value $lines -Encoding UTF8
    $reportWritten = $true
    Write-Host "Report written to $ReportPath"
}

if ($integrityFailureCount -gt 0 -or $enforceableCount -eq 0) {
    throw "Performance evidence integrity failed: INVALID=$integrityFailureCount, ENFORCEABLE=$enforceableCount. Report mode suppresses timing regressions only."
}
if ($failureCount -gt 0 -and -not $NoFailOnRegression.IsPresent)
{
    throw "Performance guardrail violations detected: $failureCount"
}
