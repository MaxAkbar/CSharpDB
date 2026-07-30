#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $BaselineResultsPath,

    [Parameter(Mandatory)]
    [string] $CandidateResultsPath,

    [Parameter(Mandatory)]
    [string] $ReportPath,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$invariant = [Globalization.CultureInfo]::InvariantCulture
$baselineRoot = [IO.Path]::GetFullPath($BaselineResultsPath)
$candidateRoot = [IO.Path]::GetFullPath($CandidateResultsPath)
$resolvedReportPath = [IO.Path]::GetFullPath($ReportPath)

foreach ($path in @($baselineRoot, $candidateRoot)) {
    if (-not (Test-Path -LiteralPath $path -PathType Container)) {
        throw "Release-core results directory not found: $path"
    }
}

function Convert-ToMetric {
    param(
        [AllowNull()]
        [AllowEmptyString()]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [double] $metric = 0
    if ([string]::IsNullOrWhiteSpace($Value) -or
        -not [double]::TryParse(
            $Value,
            [Globalization.NumberStyles]::Float,
            $invariant,
            [ref] $metric) -or
        -not [double]::IsFinite($metric) -or
        $metric -lt 0) {
        throw "Release-core metric '$Description' is missing or invalid: '$Value'."
    }

    return $metric
}

function Format-Percent {
    param([double] $Value)
    return $Value.ToString('0.00', $invariant)
}

function Convert-ToMarkdownCell {
    param([AllowNull()][object] $Value)

    return ([string] $Value).
        Replace('\', '\\').
        Replace('|', '\|').
        Replace("`r", ' ').
        Replace("`n", ' ')
}

function New-FileMap {
    param(
        [Parameter(Mandatory)]
        [IO.FileInfo[]] $Files,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $map = [Collections.Generic.Dictionary[string, IO.FileInfo]]::new(
        [StringComparer]::Ordinal)
    foreach ($file in $Files) {
        if (-not $map.TryAdd($file.Name, $file)) {
            throw "$Description contains duplicate CSV filename '$($file.Name)'."
        }
    }

    return ,$map
}

function New-RowMap {
    param(
        [Parameter(Mandatory)]
        [object[]] $Rows,

        [Parameter(Mandatory)]
        [string] $Path
    )

    $map = [Collections.Generic.Dictionary[string, object]]::new(
        [StringComparer]::Ordinal)
    foreach ($row in $Rows) {
        $nameProperty = $row.PSObject.Properties['Name']
        if ($null -eq $nameProperty -or [string]::IsNullOrWhiteSpace([string] $nameProperty.Value)) {
            throw "Release-core result '$Path' contains a row without a Name."
        }

        $name = [string] $nameProperty.Value
        if (-not $map.TryAdd($name, $row)) {
            throw "Release-core result '$Path' contains duplicate row '$name'."
        }
    }

    return ,$map
}

function Get-ColumnNames {
    param(
        [Parameter(Mandatory)]
        [object] $Row
    )

    return @($Row.PSObject.Properties.Name)
}

$baselineFiles = @(
    Get-ChildItem -LiteralPath $baselineRoot -File -Filter '*.csv' |
        Sort-Object Name
)
$candidateFiles = @(
    Get-ChildItem -LiteralPath $candidateRoot -File -Filter '*.csv' |
        Sort-Object Name
)

if ($baselineFiles.Count -eq 0 -or $candidateFiles.Count -eq 0) {
    throw 'Both release-core result directories must contain at least one CSV file.'
}

$baselineFilesByName = New-FileMap -Files @($baselineFiles) -Description 'Baseline results'
$candidateFilesByName = New-FileMap -Files @($candidateFiles) -Description 'Candidate results'
$allFileNames = @(
    $baselineFilesByName.Keys
    $candidateFilesByName.Keys
) | Sort-Object -CaseSensitive -Unique
$results = [Collections.Generic.List[object]]::new()
foreach ($fileName in $allFileNames) {
    if (-not $baselineFilesByName.ContainsKey($fileName)) {
        $results.Add([pscustomobject]@{
            Suite = [IO.Path]::GetFileNameWithoutExtension($fileName)
            Row = '<file>'
            ThroughputRegression = $null
            P99Regression = $null
            Status = 'FAIL'
            Notes = 'Baseline result file is missing.'
        })
        continue
    }

    $baselineFile = $baselineFilesByName[$fileName]
    if (-not $candidateFilesByName.ContainsKey($fileName)) {
        $results.Add([pscustomobject]@{
            Suite = $baselineFile.BaseName
            Row = '<file>'
            ThroughputRegression = $null
            P99Regression = $null
            Status = 'FAIL'
            Notes = 'Candidate result file is missing.'
        })
        continue
    }

    $candidateFile = $candidateFilesByName[$fileName]
    $baselineRows = @(Import-Csv -LiteralPath $baselineFile.FullName)
    $candidateRows = @(Import-Csv -LiteralPath $candidateFile.FullName)
    if ($baselineRows.Count -eq 0 -or $candidateRows.Count -eq 0) {
        $emptyDescriptions = [Collections.Generic.List[string]]::new()
        if ($baselineRows.Count -eq 0) {
            $emptyDescriptions.Add('baseline')
        }
        if ($candidateRows.Count -eq 0) {
            $emptyDescriptions.Add('candidate')
        }
        $results.Add([pscustomobject]@{
            Suite = $baselineFile.BaseName
            Row = '<file>'
            ThroughputRegression = $null
            P99Regression = $null
            Status = 'FAIL'
            Notes = "The $($emptyDescriptions -join ' and ') result file is empty."
        })
        continue
    }

    $baselineColumns = @(Get-ColumnNames -Row $baselineRows[0])
    $candidateColumns = @(Get-ColumnNames -Row $candidateRows[0])
    $requiredColumns = @('Name', 'OpsPerSec', 'P99')
    $missingBaselineColumns = @($requiredColumns | Where-Object { $_ -cnotin $baselineColumns })
    $missingCandidateColumns = @($requiredColumns | Where-Object { $_ -cnotin $candidateColumns })
    $schemaDifference = @(
        Compare-Object -ReferenceObject $baselineColumns -DifferenceObject $candidateColumns -CaseSensitive
    )
    if ($missingBaselineColumns.Count -gt 0 -or
        $missingCandidateColumns.Count -gt 0 -or
        $schemaDifference.Count -gt 0) {
        $schemaNotes = [Collections.Generic.List[string]]::new()
        if ($missingBaselineColumns.Count -gt 0) {
            $schemaNotes.Add("Baseline is missing required columns: $($missingBaselineColumns -join ', ').")
        }
        if ($missingCandidateColumns.Count -gt 0) {
            $schemaNotes.Add("Candidate is missing required columns: $($missingCandidateColumns -join ', ').")
        }
        if ($schemaDifference.Count -gt 0) {
            $schemaNotes.Add('Baseline and candidate schemas do not match.')
        }
        $results.Add([pscustomobject]@{
            Suite = $baselineFile.BaseName
            Row = '<schema>'
            ThroughputRegression = $null
            P99Regression = $null
            Status = 'FAIL'
            Notes = $schemaNotes -join ' '
        })
        continue
    }

    try {
        $baselineByName = New-RowMap -Rows $baselineRows -Path $baselineFile.FullName
        $candidateByName = New-RowMap -Rows $candidateRows -Path $candidateFile.FullName
    }
    catch {
        $results.Add([pscustomobject]@{
            Suite = $baselineFile.BaseName
            Row = '<schema>'
            ThroughputRegression = $null
            P99Regression = $null
            Status = 'FAIL'
            Notes = $_.Exception.Message
        })
        continue
    }

    $allRowNames = @(
        $baselineByName.Keys
        $candidateByName.Keys
    ) | Sort-Object -CaseSensitive -Unique
    foreach ($rowName in $allRowNames) {
        if (-not $baselineByName.ContainsKey($rowName)) {
            $results.Add([pscustomobject]@{
                Suite = $baselineFile.BaseName
                Row = $rowName
                ThroughputRegression = $null
                P99Regression = $null
                Status = 'FAIL'
                Notes = 'Baseline row is missing.'
            })
            continue
        }

        if (-not $candidateByName.ContainsKey($rowName)) {
            $results.Add([pscustomobject]@{
                Suite = $baselineFile.BaseName
                Row = $rowName
                ThroughputRegression = $null
                P99Regression = $null
                Status = 'FAIL'
                Notes = 'Candidate row is missing.'
            })
            continue
        }

        $baselineRow = $baselineByName[$rowName]
        $candidateRow = $candidateByName[$rowName]
        try {
            $baselineThroughput = Convert-ToMetric $baselineRow.OpsPerSec "$fileName/$rowName/baseline OpsPerSec"
            $candidateThroughput = Convert-ToMetric $candidateRow.OpsPerSec "$fileName/$rowName/candidate OpsPerSec"
            $baselineP99 = Convert-ToMetric $baselineRow.P99 "$fileName/$rowName/baseline P99"
            $candidateP99 = Convert-ToMetric $candidateRow.P99 "$fileName/$rowName/candidate P99"
        }
        catch {
            $results.Add([pscustomobject]@{
                Suite = $baselineFile.BaseName
                Row = $rowName
                ThroughputRegression = $null
                P99Regression = $null
                Status = 'FAIL'
                Notes = $_.Exception.Message
            })
            continue
        }

        $throughputRegression = if ($baselineThroughput -le 0) {
            if ($candidateThroughput -le 0) { 0 } else { -100 }
        }
        else {
            (($baselineThroughput - $candidateThroughput) / $baselineThroughput) * 100
        }
        $p99Regression = if ($baselineP99 -le 0) {
            if ($candidateP99 -le 0) { 0 } else { 100 }
        }
        else {
            (($candidateP99 - $baselineP99) / $baselineP99) * 100
        }

        $failed = $throughputRegression -gt $MaxThroughputRegressionPercent -or
            $p99Regression -gt $MaxP99RegressionPercent
        $results.Add([pscustomobject]@{
            Suite = $baselineFile.BaseName
            Row = $baselineRow.Name
            ThroughputRegression = $throughputRegression
            P99Regression = $p99Regression
            Status = if ($failed) { 'FAIL' } else { 'PASS' }
            Notes = ''
        })
    }
}

$reportDirectory = Split-Path -Parent $resolvedReportPath
New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
$failures = @($results | Where-Object Status -eq 'FAIL')
$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Previous-release performance comparison')
$lines.Add('')
$lines.Add("- Baseline results: ``$baselineRoot``")
$lines.Add("- Candidate results: ``$candidateRoot``")
$lines.Add("- Throughput regression limit: $MaxThroughputRegressionPercent%")
$lines.Add("- P99 regression limit: $MaxP99RegressionPercent%")
$lines.Add("- Result: **$(if ($failures.Count -eq 0) { 'PASS' } else { 'FAIL' })**")
$lines.Add('')
$lines.Add('| Suite | Row | Throughput regression | P99 regression | Status | Notes |')
$lines.Add('|---|---|---:|---:|---|---|')
foreach ($result in $results) {
    $throughput = if ($null -eq $result.ThroughputRegression) {
        'n/a'
    }
    else {
        "$(Format-Percent $result.ThroughputRegression)%"
    }
    $p99 = if ($null -eq $result.P99Regression) {
        'n/a'
    }
    else {
        "$(Format-Percent $result.P99Regression)%"
    }
    $suiteName = Convert-ToMarkdownCell $result.Suite
    $rowName = Convert-ToMarkdownCell $result.Row
    $notes = Convert-ToMarkdownCell $result.Notes
    $lines.Add("| $suiteName | $rowName | $throughput | $p99 | $($result.Status) | $notes |")
}

[IO.File]::WriteAllLines($resolvedReportPath, $lines)
Write-Host "Compared $($results.Count) release-core rows; failures=$($failures.Count)."
Write-Host "Report: $resolvedReportPath"

if ($failures.Count -gt 0) {
    throw "Previous-release performance comparison failed for $($failures.Count) row(s)."
}
