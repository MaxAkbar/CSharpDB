#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $BaselineResultsPath,

    [Parameter(Mandatory)]
    [string] $CandidateResultsPath,

    [Parameter(Mandatory)]
    [string] $BaselineRawResultsPath,

    [Parameter(Mandatory)]
    [string] $CandidateRawResultsPath,

    [Parameter(Mandatory)]
    [ValidateSet(3, 5, 7, 9)]
    [int] $RepeatCount,

    [Parameter(Mandatory)]
    [string] $ReportPath,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

    [ValidateRange(0, 1000)]
    [double] $MaxP99RegressionMilliseconds = 0.05,

    [ValidateSet('P95', 'P99')]
    [string] $BlockingLatencyPercentile = 'P99',

    [ValidateNotNullOrEmpty()]
    [string] $PairManifestPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$invariant = [Globalization.CultureInfo]::InvariantCulture
$numberStyles = [Globalization.NumberStyles]::Float
$integerStyles = [Globalization.NumberStyles]::Integer
$maxThroughputRegressionPercentExact = [decimal]::Parse(
    $MaxThroughputRegressionPercent.ToString('R', $invariant),
    $numberStyles,
    $invariant)
$maxLatencyRegressionPercentExact = [decimal]::Parse(
    $MaxP99RegressionPercent.ToString('R', $invariant),
    $numberStyles,
    $invariant)
$maxLatencyRegressionMillisecondsExact = [decimal]::Parse(
    $MaxP99RegressionMilliseconds.ToString('R', $invariant),
    $numberStyles,
    $invariant)
$stabilityThroughputPercent = [decimal] 15
$blockingLatencyLabel = $BlockingLatencyPercentile.ToUpperInvariant()
$stabilityLatencyPercent = [decimal] 25
$stabilityLatencyMilliseconds = [decimal] 0.05
$hybridQualificationSuite = 'hybrid-storage-mode-scenario'
$qualificationElapsedCaptureToleranceMilliseconds = [decimal] 1
$requiredHybridQualificationTokenKeys =
    [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($requiredHybridQualificationTokenKey in @(
        'qualification',
        'unrecorded-warmup-seconds',
        'minimum-measured-seconds',
        'minimum-retained-latency-samples',
        'measurement-cap-seconds',
        'measurement-begin-utc',
        'measurement-end-utc')) {
    [void] $requiredHybridQualificationTokenKeys.Add(
        $requiredHybridQualificationTokenKey)
}
$requiredColumns = @(
    'Name',
    'TotalOps',
    'ElapsedMs',
    'LatencySamples',
    'OpsPerSec',
    $blockingLatencyLabel,
    'P99',
    'ExtraInfo'
) | Select-Object -Unique
$pairedEvidenceColumns = @(
    'Name',
    'TotalOps',
    'LatencySamples',
    'ElapsedMs',
    'OpsPerSec',
    'P50',
    'P90',
    'P95',
    'P99',
    'P999',
    'Min',
    'Max',
    'Mean',
    'StdDev',
    'ExtraInfo'
)

$baselineRoot = [IO.Path]::GetFullPath($BaselineResultsPath)
$candidateRoot = [IO.Path]::GetFullPath($CandidateResultsPath)
$baselineRawRoot = [IO.Path]::GetFullPath($BaselineRawResultsPath)
$candidateRawRoot = [IO.Path]::GetFullPath($CandidateRawResultsPath)
$resolvedReportPath = [IO.Path]::GetFullPath($ReportPath)
$pairedComparison = $PSBoundParameters.ContainsKey('PairManifestPath')
$resolvedPairManifestPath = if ($pairedComparison) {
    [IO.Path]::GetFullPath($PairManifestPath)
}
else {
    $null
}

foreach ($path in @(
        $baselineRoot,
        $candidateRoot,
        $baselineRawRoot,
        $candidateRawRoot
    )) {
    if (-not (Test-Path -LiteralPath $path -PathType Container)) {
        throw "Release-core results directory not found: $path"
    }
}

function Convert-ToPositiveDecimalMetric {
    param(
        [AllowNull()]
        [AllowEmptyString()]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [decimal] $metric = 0
    if ([string]::IsNullOrWhiteSpace($Value) -or
        -not [decimal]::TryParse(
            $Value,
            $numberStyles,
            $invariant,
            [ref] $metric) -or
        $metric -le 0) {
        throw "Release-core metric '$Description' must be a strictly positive finite number: '$Value'."
    }

    return $metric
}

function Convert-ToPositiveIntegerMetric {
    param(
        [AllowNull()]
        [AllowEmptyString()]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [long] $metric = 0
    if ([string]::IsNullOrWhiteSpace($Value) -or
        -not [long]::TryParse(
            $Value,
            $integerStyles,
            $invariant,
            [ref] $metric) -or
        $metric -le 0) {
        throw "Release-core metric '$Description' must be a strictly positive integer: '$Value'."
    }

    return $metric
}

function Get-GateMetrics {
    param(
        [Parameter(Mandatory)]
        [object] $Row,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $totalOps = Convert-ToPositiveIntegerMetric `
        -Value $Row.TotalOps `
        -Description "$Description TotalOps"
    $elapsedMilliseconds = Convert-ToPositiveDecimalMetric `
        -Value $Row.ElapsedMs `
        -Description "$Description ElapsedMs"
    $latencySamples = Convert-ToPositiveIntegerMetric `
        -Value $Row.LatencySamples `
        -Description "$Description LatencySamples"
    $opsPerSecond = Convert-ToPositiveDecimalMetric `
        -Value $Row.OpsPerSec `
        -Description "$Description OpsPerSec"
    $blockingLatency = Convert-ToPositiveDecimalMetric `
        -Value $Row.PSObject.Properties[$blockingLatencyLabel].Value `
        -Description "$Description $blockingLatencyLabel"
    $diagnosticP99 = if ($blockingLatencyLabel -ceq 'P99') {
        $blockingLatency
    }
    else {
        Convert-ToPositiveDecimalMetric `
            -Value $Row.P99 `
            -Description "$Description P99"
    }

    if ($latencySamples -lt 100) {
        throw (
            "Release-core metric '$Description LatencySamples' must be at least 100: " +
                "'$latencySamples'.")
    }

    return [pscustomobject]@{
        TotalOps = $totalOps
        ElapsedMs = $elapsedMilliseconds
        LatencySamples = $latencySamples
        OpsPerSec = $opsPerSecond
        BlockingLatency = $blockingLatency
        DiagnosticP99 = $diagnosticP99
    }
}

function Read-UniqueExtraInfoTokens {
    param(
        [AllowNull()]
        [object] $ExtraInfo,

        [Parameter(Mandatory)]
        [string] $Description
    )

    if ($null -eq $ExtraInfo -or
        [string]::IsNullOrWhiteSpace([string] $ExtraInfo)) {
        throw "Hybrid qualification '$Description ExtraInfo' is empty."
    }

    $tokens = [Collections.Generic.Dictionary[string, string]]::new(
        [StringComparer]::Ordinal)
    foreach ($rawToken in ([string] $ExtraInfo).Split(';')) {
        $token = $rawToken.Trim()
        if ([string]::IsNullOrWhiteSpace($token)) {
            continue
        }

        $separatorIndex = $token.IndexOf('=', [StringComparison]::Ordinal)
        if ($separatorIndex -lt 0) {
            if ($requiredHybridQualificationTokenKeys.Contains($token)) {
                throw (
                    "Hybrid qualification '$Description ExtraInfo' token '$token' must " +
                        'contain a non-empty key and value separated by =.')
            }
            continue
        }

        $key = $token.Substring(0, $separatorIndex).Trim()
        if (-not $requiredHybridQualificationTokenKeys.Contains($key)) {
            continue
        }

        $value = $token.Substring($separatorIndex + 1).Trim()
        if ([string]::IsNullOrWhiteSpace($value)) {
            throw (
                "Hybrid qualification '$Description ExtraInfo' token '$token' must " +
                    'contain a non-empty key and value separated by =.')
        }
        if (-not $tokens.TryAdd($key, $value)) {
            throw (
                "Hybrid qualification '$Description ExtraInfo' contains duplicate " +
                    "token '$key'.")
        }
    }

    return ,$tokens
}

function Get-RequiredQualificationToken {
    param(
        [Parameter(Mandatory)]
        [Collections.Generic.Dictionary[string, string]] $Tokens,

        [Parameter(Mandatory)]
        [string] $Key,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [string] $value = ''
    if (-not $Tokens.TryGetValue($Key, [ref] $value)) {
        throw (
            "Hybrid qualification '$Description ExtraInfo' is missing required " +
                "token '$Key'.")
    }

    return $value
}

function Convert-ToQualificationInteger {
    param(
        [Parameter(Mandatory)]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Key,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [long] $number = 0
    if (-not [long]::TryParse(
            $Value,
            $integerStyles,
            $invariant,
            [ref] $number)) {
        throw (
            "Hybrid qualification '$Description ExtraInfo' token '$Key' must be " +
                "an integer: '$Value'.")
    }

    return $number
}

function Convert-ToRoundTripUtcTimestamp {
    param(
        [Parameter(Mandatory)]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $Key,

        [Parameter(Mandatory)]
        [string] $Description
    )

    [DateTimeOffset] $timestamp = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParseExact(
            $Value,
            'O',
            $invariant,
            [Globalization.DateTimeStyles]::RoundtripKind,
            [ref] $timestamp) -or
        $timestamp.Offset -ne [TimeSpan]::Zero) {
        throw (
            "Hybrid qualification '$Description ExtraInfo' token '$Key' must be " +
                "a round-trip UTC timestamp: '$Value'.")
    }

    return $timestamp
}

function Assert-HybridQualificationEvidence {
    param(
        [Parameter(Mandatory)]
        [object] $Row,

        [Parameter(Mandatory)]
        [object] $Metrics,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $tokens = Read-UniqueExtraInfoTokens `
        -ExtraInfo $Row.ExtraInfo `
        -Description $Description
    $qualification = Get-RequiredQualificationToken `
        -Tokens $tokens `
        -Key 'qualification' `
        -Description $Description
    if ($qualification -cne 'true') {
        throw (
            "Hybrid qualification '$Description ExtraInfo' token 'qualification' " +
                "must be 'true': '$qualification'.")
    }

    $requiredIntegerTokens = [ordered]@{
        'unrecorded-warmup-seconds' = [long] 2
        'minimum-measured-seconds' = [long] 30
        'minimum-retained-latency-samples' = [long] 10000
        'measurement-cap-seconds' = [long] 120
    }
    $integerValues = @{}
    foreach ($key in $requiredIntegerTokens.Keys) {
        $rawValue = Get-RequiredQualificationToken `
            -Tokens $tokens `
            -Key $key `
            -Description $Description
        $integerValue = Convert-ToQualificationInteger `
            -Value $rawValue `
            -Key $key `
            -Description $Description
        $expectedValue = $requiredIntegerTokens[$key]
        if ($integerValue -ne $expectedValue) {
            throw (
                "Hybrid qualification '$Description ExtraInfo' token '$key' must " +
                    "be ${expectedValue}: '$rawValue'.")
        }
        $integerValues[$key] = $integerValue
    }

    $measurementBeginUtc = Convert-ToRoundTripUtcTimestamp `
        -Value (Get-RequiredQualificationToken `
            -Tokens $tokens `
            -Key 'measurement-begin-utc' `
            -Description $Description) `
        -Key 'measurement-begin-utc' `
        -Description $Description
    $measurementEndUtc = Convert-ToRoundTripUtcTimestamp `
        -Value (Get-RequiredQualificationToken `
            -Tokens $tokens `
            -Key 'measurement-end-utc' `
            -Description $Description) `
        -Key 'measurement-end-utc' `
        -Description $Description
    $measurementIntervalMilliseconds =
        [decimal] ($measurementEndUtc - $measurementBeginUtc).Ticks / 10000
    if ($measurementIntervalMilliseconds -le 0) {
        throw (
            "Hybrid qualification '$Description' measurement interval must be " +
                'strictly positive.')
    }

    $elapsedDifference = [decimal]::Abs(
        $Metrics.ElapsedMs - $measurementIntervalMilliseconds)
    if ($elapsedDifference -gt $qualificationElapsedCaptureToleranceMilliseconds) {
        throw (
            "Hybrid qualification '$Description ElapsedMs' must match the UTC " +
                'measurement interval within ' +
                "$(Format-ExactNumber $qualificationElapsedCaptureToleranceMilliseconds) ms: " +
                "elapsed=$(Format-ExactNumber $Metrics.ElapsedMs) ms; " +
                "interval=$(Format-ExactNumber $measurementIntervalMilliseconds) ms.")
    }

    $minimumElapsedMilliseconds =
        [decimal] $integerValues['minimum-measured-seconds'] * 1000
    $maximumElapsedMilliseconds =
        [decimal] $integerValues['measurement-cap-seconds'] * 1000
    if ($Metrics.ElapsedMs -lt $minimumElapsedMilliseconds) {
        throw (
            "Hybrid qualification '$Description ElapsedMs' must be at least " +
                "$(Format-ExactNumber $minimumElapsedMilliseconds) ms: " +
                "'$(Format-ExactNumber $Metrics.ElapsedMs)'.")
    }
    if ($Metrics.ElapsedMs -gt $maximumElapsedMilliseconds) {
        throw (
            "Hybrid qualification '$Description ElapsedMs' must not exceed " +
                "$(Format-ExactNumber $maximumElapsedMilliseconds) ms: " +
                "'$(Format-ExactNumber $Metrics.ElapsedMs)'.")
    }

    $minimumLatencySamples =
        [long] $integerValues['minimum-retained-latency-samples']
    if ($Metrics.LatencySamples -lt $minimumLatencySamples) {
        throw (
            "Hybrid qualification '$Description LatencySamples' must be at least " +
                "the declared minimum of ${minimumLatencySamples}: " +
                "'$($Metrics.LatencySamples)'.")
    }
}

function Format-ExactNumber {
    param([decimal] $Value)
    return $Value.ToString('0.############################', $invariant)
}

function Format-Percent {
    param([decimal] $Value)
    return $Value.ToString('0.00', $invariant)
}

function Format-Milliseconds {
    param([decimal] $Value)
    return $Value.ToString('0.0000', $invariant)
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
        if ($null -eq $nameProperty -or
            [string]::IsNullOrWhiteSpace([string] $nameProperty.Value)) {
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

function Read-EvidenceCsv {
    param(
        [Parameter(Mandatory)]
        [string] $Path,

        [switch] $RequirePairedSchema
    )

    $rows = @(Import-Csv -LiteralPath $Path)
    if ($rows.Count -eq 0) {
        throw "Release-core result '$Path' is empty."
    }

    $columns = @(Get-ColumnNames -Row $rows[0])
    $missingColumns = @(
        $requiredColumns |
            Where-Object { $_ -cnotin $columns }
    )
    if ($missingColumns.Count -gt 0) {
        throw (
            "Release-core result '$Path' is missing required columns: " +
                "$($missingColumns -join ', ').")
    }
    if ($RequirePairedSchema -and
        -not (Test-SameStringSequence `
            -Reference ([string[]] $pairedEvidenceColumns) `
            -Difference ([string[]] $columns))) {
        throw (
            "Release-core paired result '$Path' must contain exactly these columns " +
                "in this order: $($pairedEvidenceColumns -join ', ').")
    }

    $rowsByName = New-RowMap -Rows $rows -Path $Path
    return [pscustomobject]@{
        Path = $Path
        Columns = $columns
        RowsByName = $rowsByName
    }
}

function Test-SameStringSet {
    param(
        [Parameter(Mandatory)]
        [string[]] $Reference,

        [Parameter(Mandatory)]
        [string[]] $Difference
    )

    $setDifference = @(
        Compare-Object `
            -ReferenceObject $Reference `
            -DifferenceObject $Difference `
            -CaseSensitive
    )
    return $setDifference.Count -eq 0
}

function Test-SameStringSequence {
    param(
        [Parameter(Mandatory)]
        [string[]] $Reference,

        [Parameter(Mandatory)]
        [string[]] $Difference
    )

    if ($Reference.Count -ne $Difference.Count) {
        return $false
    }
    for ($index = 0; $index -lt $Reference.Count; $index++) {
        if ($Reference[$index] -cne $Difference[$index]) {
            return $false
        }
    }
    return $true
}

function Get-MedianDecimal {
    param(
        [Parameter(Mandatory)]
        [decimal[]] $Values
    )

    if ($Values.Count -eq 0 -or ($Values.Count -band 1) -eq 0) {
        throw 'A non-empty odd number of values is required to compute a release-core median.'
    }

    $ordered = [decimal[]] @($Values | Sort-Object)
    return $ordered[[int] [Math]::Floor($ordered.Count / 2)]
}

function Test-MedianAggregateTag {
    param(
        [AllowNull()]
        [object] $ExtraInfo,

        [Parameter(Mandatory)]
        [string] $ExpectedTag
    )

    if ($null -eq $ExtraInfo) {
        return $false
    }

    $tokens = @(
        ([string] $ExtraInfo).Split(';') |
            ForEach-Object { $_.Trim() }
    )
    return $tokens -ccontains $ExpectedTag
}

function Get-StabilityAssessment {
    param(
        [Parameter(Mandatory)]
        [string] $Revision,

        [Parameter(Mandatory)]
        [object] $MedianMetrics,

        [Parameter(Mandatory)]
        [object[]] $RawMetrics
    )

    $outliers = [Collections.Generic.List[string]]::new()
    $stableRunCount = 0
    foreach ($raw in $RawMetrics) {
        $runIssues = [Collections.Generic.List[string]]::new()
        $throughputDelta = [decimal]::Abs(
            $raw.Metrics.OpsPerSec - $MedianMetrics.OpsPerSec)
        $throughputDeviation =
            ($throughputDelta / $MedianMetrics.OpsPerSec) * 100
        if ($throughputDeviation -gt $stabilityThroughputPercent) {
            $runIssues.Add(
                'throughput deviates ' +
                    "$(Format-Percent $throughputDeviation)% from its revision median.")
        }

        $latencyDelta = [decimal]::Abs(
            $raw.Metrics.BlockingLatency - $MedianMetrics.BlockingLatency)
        $latencyDeviation =
            ($latencyDelta / $MedianMetrics.BlockingLatency) * 100
        if ($latencyDeviation -gt $stabilityLatencyPercent -and
            $latencyDelta -gt $stabilityLatencyMilliseconds) {
            $runIssues.Add(
                "$blockingLatencyLabel deviates " +
                    "$(Format-Percent $latencyDeviation)% " +
                    "($(Format-Milliseconds $latencyDelta) ms) " +
                    'from its revision median.')
        }

        if ($runIssues.Count -eq 0) {
            $stableRunCount++
        }
        else {
            $outliers.Add(
                "$Revision run $($raw.Run): $($runIssues -join ' ')")
        }
    }

    $requiredStableRunCount = [int] ([Math]::Floor($RawMetrics.Count / 2) + 1)
    return [pscustomobject]@{
        Revision = $Revision
        IsStable = $stableRunCount -ge $requiredStableRunCount
        StableRunCount = $stableRunCount
        RequiredStableRunCount = $requiredStableRunCount
        TotalRunCount = $RawMetrics.Count
        Outliers = [string[]] @($outliers)
    }
}

function New-EvidenceResult {
    param(
        [Parameter(Mandatory)]
        [string] $Suite,

        [Parameter(Mandatory)]
        [string] $Row,

        [Parameter(Mandatory)]
        [ValidateSet('INVALID', 'UNSTABLE', 'ORDER-SENSITIVE')]
        [string] $Status,

        [Parameter(Mandatory)]
        [string] $Notes
    )

    return [pscustomobject]@{
        Suite = $Suite
        Row = $Row
        ThroughputRegression = $null
        LatencyRegression = $null
        LatencyDifference = $null
        DiagnosticP99Regression = $null
        DiagnosticP99Difference = $null
        Status = $Status
        Notes = $Notes
    }
}

function Convert-DoubleToDecimal {
    param(
        [Parameter(Mandatory)]
        [double] $Value,

        [Parameter(Mandatory)]
        [string] $Description
    )

    if ([double]::IsNaN($Value) -or [double]::IsInfinity($Value)) {
        throw "Release-core value '$Description' must be finite."
    }

    try {
        return [decimal]::Parse(
            $Value.ToString('R', $invariant),
            $numberStyles,
            $invariant)
    }
    catch {
        throw "Release-core value '$Description' is outside the supported range: '$Value'."
    }
}

function Get-MedianDouble {
    param(
        [Parameter(Mandatory)]
        [double[]] $Values
    )

    if ($Values.Count -eq 0 -or ($Values.Count -band 1) -eq 0) {
        throw 'A non-empty odd number of values is required to compute a paired median.'
    }

    $ordered = [double[]] @($Values | Sort-Object)
    return $ordered[[int] [Math]::Floor($ordered.Count / 2)]
}

function Resolve-PairEvidencePath {
    param(
        [AllowNull()]
        [AllowEmptyString()]
        [string] $Value,

        [Parameter(Mandatory)]
        [string] $ManifestDirectory,

        [Parameter(Mandatory)]
        [string] $Description
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "Paired evidence '$Description' path is empty."
    }

    $path = $Value.Trim()
    if (-not [IO.Path]::IsPathFullyQualified($path)) {
        $path = [IO.Path]::Combine($ManifestDirectory, $path)
    }

    try {
        return [IO.Path]::GetFullPath($path)
    }
    catch {
        throw "Paired evidence '$Description' path is invalid: '$Value'."
    }
}

function Test-PathWithinRoot {
    param(
        [Parameter(Mandatory)]
        [string] $Path,

        [Parameter(Mandatory)]
        [string] $Root
    )

    $relativePath = [IO.Path]::GetRelativePath($Root, $Path)
    if ([IO.Path]::IsPathFullyQualified($relativePath) -or
        $relativePath -ceq '..') {
        return $false
    }

    $parentPrefix = '..' + [IO.Path]::DirectorySeparatorChar
    $alternateParentPrefix = '..' + [IO.Path]::AltDirectorySeparatorChar
    return -not $relativePath.StartsWith($parentPrefix, [StringComparison]::Ordinal) -and
        -not $relativePath.StartsWith(
            $alternateParentPrefix,
            [StringComparison]::Ordinal)
}

function Read-PairManifest {
    param(
        [Parameter(Mandatory)]
        [string] $Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Pair manifest not found: $Path"
    }

    $manifestRows = @(Import-Csv -LiteralPath $Path)
    if ($manifestRows.Count -eq 0) {
        throw "Pair manifest '$Path' is empty."
    }

    $requiredManifestColumns = @(
        'Suite',
        'PairId',
        'Order',
        'FirstRevision',
        'SecondRevision',
        'BaselineRaw',
        'CandidateRaw'
    )
    $manifestColumns = @(Get-ColumnNames -Row $manifestRows[0])
    if (-not (Test-SameStringSequence `
            -Reference ([string[]] $requiredManifestColumns) `
            -Difference ([string[]] $manifestColumns))) {
        throw (
            "Pair manifest '$Path' must contain exactly these columns: " +
                "$($requiredManifestColumns -join ', ').")
    }

    $manifestDirectory = Split-Path -Parent $Path
    $pathComparer = if ($IsWindows) {
        [StringComparer]::OrdinalIgnoreCase
    }
    else {
        [StringComparer]::Ordinal
    }
    $pairIds = [Collections.Generic.Dictionary[string, string]]::new(
        [StringComparer]::Ordinal)
    $rawFiles = [Collections.Generic.Dictionary[string, string]]::new(
        $pathComparer)
    $pairs = [Collections.Generic.List[object]]::new()

    for ($index = 0; $index -lt $manifestRows.Count; $index++) {
        $row = $manifestRows[$index]
        $rowNumber = $index + 2
        $suite = ([string] $row.Suite).Trim()
        $pairId = ([string] $row.PairId).Trim()
        $order = ([string] $row.Order).Trim()
        $firstRevision = ([string] $row.FirstRevision).Trim()
        $secondRevision = ([string] $row.SecondRevision).Trim()
        if ([string]::IsNullOrWhiteSpace($suite)) {
            throw "Pair manifest '$Path' row $rowNumber has an empty Suite."
        }
        if ([string]::IsNullOrWhiteSpace($pairId)) {
            throw "Pair manifest '$Path' row $rowNumber has an empty PairId."
        }
        if ($order -cnotin @('previous-candidate', 'candidate-previous')) {
            throw (
                "Pair manifest '$Path' row $rowNumber has invalid Order '$order'; " +
                    "expected previous-candidate or candidate-previous.")
        }

        $expectedFirst = if ($order -ceq 'previous-candidate') {
            'previous'
        }
        else {
            'candidate'
        }
        $expectedSecond = if ($order -ceq 'previous-candidate') {
            'candidate'
        }
        else {
            'previous'
        }
        if ($firstRevision -cne $expectedFirst -or
            $secondRevision -cne $expectedSecond) {
            throw (
                "Pair manifest '$Path' row $rowNumber adjacency identity does not match " +
                    "Order '$order': expected $expectedFirst then $expectedSecond, got " +
                    "'$firstRevision' then '$secondRevision'.")
        }

        $pairKey = $suite + [char] 31 + $pairId
        if (-not $pairIds.TryAdd($pairKey, "row $rowNumber")) {
            throw (
                "Pair manifest '$Path' contains duplicate pair '$suite/$pairId' at " +
                    "row $rowNumber.")
        }

        $baselinePath = Resolve-PairEvidencePath `
            -Value ([string] $row.BaselineRaw) `
            -ManifestDirectory $manifestDirectory `
            -Description "$suite/$pairId baseline"
        $candidatePath = Resolve-PairEvidencePath `
            -Value ([string] $row.CandidateRaw) `
            -ManifestDirectory $manifestDirectory `
            -Description "$suite/$pairId candidate"
        foreach ($entry in @(
                [pscustomobject]@{
                    Revision = 'baseline'
                    Path = $baselinePath
                    Root = $baselineRawRoot
                }
                [pscustomobject]@{
                    Revision = 'candidate'
                    Path = $candidatePath
                    Root = $candidateRawRoot
                }
            )) {
            if (-not (Test-PathWithinRoot `
                    -Path $entry.Path `
                    -Root $entry.Root)) {
                throw (
                    "Pair manifest '$Path' row $rowNumber $($entry.Revision) raw " +
                        "path escapes its supplied $($entry.Revision) raw root: " +
                        "$($entry.Path)")
            }
            $expectedRawPath = [IO.Path]::GetFullPath(
                [IO.Path]::Combine(
                    $entry.Root,
                    $suite,
                    "$pairId.csv"))
            if (-not $pathComparer.Equals($entry.Path, $expectedRawPath)) {
                throw (
                    "Pair manifest '$Path' row $rowNumber $($entry.Revision) raw " +
                        "path does not identify pair '$suite/$pairId'; expected " +
                        "'$expectedRawPath', got '$($entry.Path)'.")
            }
            if (-not (Test-Path -LiteralPath $entry.Path -PathType Leaf)) {
                throw (
                    "Pair manifest '$Path' row $rowNumber references a missing " +
                        "$($entry.Revision) raw file: $($entry.Path)")
            }
            if (-not $rawFiles.TryAdd(
                    $entry.Path,
                    "$suite/$pairId/$($entry.Revision)")) {
                throw (
                    "Pair manifest '$Path' reuses raw file '$($entry.Path)' for " +
                        "$suite/$pairId/$($entry.Revision); it is already assigned to " +
                        "$($rawFiles[$entry.Path]).")
            }
        }

        $pairs.Add([pscustomobject]@{
            Suite = $suite
            PairId = $pairId
            Order = $order
            FirstRevision = $firstRevision
            SecondRevision = $secondRevision
            BaselineRaw = $baselinePath
            CandidateRaw = $candidatePath
        })
    }

    $suiteNames = @($pairs.Suite | Sort-Object -CaseSensitive -Unique)
    $referenceSchedule = $null
    foreach ($suite in $suiteNames) {
        $suitePairs = @($pairs | Where-Object Suite -CEQ $suite)
        foreach ($order in @('previous-candidate', 'candidate-previous')) {
            $stratumCount = @(
                $suitePairs | Where-Object Order -CEQ $order
            ).Count
            if ($stratumCount -ne $RepeatCount) {
                throw (
                    "Pair manifest '$Path' suite '$suite' must contain exactly " +
                        "$RepeatCount '$order' pairs; found $stratumCount.")
            }
        }

        for ($pairIndex = 1; $pairIndex -lt $suitePairs.Count; $pairIndex++) {
            if ($suitePairs[$pairIndex].Order -ceq $suitePairs[$pairIndex - 1].Order) {
                throw (
                    "Pair manifest '$Path' suite '$suite' must alternate pair order; " +
                        "pairs '$($suitePairs[$pairIndex - 1].PairId)' and " +
                        "'$($suitePairs[$pairIndex].PairId)' are both " +
                        "'$($suitePairs[$pairIndex].Order)'.")
            }
        }

        $schedule = [string[]] @(
            $suitePairs |
                ForEach-Object {
                    "$($_.PairId)$([char] 31)$($_.Order)$([char] 31)" +
                        "$($_.FirstRevision)$([char] 31)$($_.SecondRevision)"
                }
        )
        if ($null -eq $referenceSchedule) {
            $referenceSchedule = $schedule
        }
        elseif (-not (Test-SameStringSequence `
                -Reference ([string[]] $referenceSchedule) `
                -Difference $schedule)) {
            throw (
                "Pair manifest '$Path' suite '$suite' does not use the same pair " +
                    'identities and order schedule as the other suites.')
        }
    }

    $baselineAggregateNames = [string[]] @(
        Get-ChildItem -LiteralPath $baselineRoot -File -Filter '*.csv' |
            ForEach-Object Name |
            Sort-Object -CaseSensitive
    )
    $candidateAggregateNames = [string[]] @(
        Get-ChildItem -LiteralPath $candidateRoot -File -Filter '*.csv' |
            ForEach-Object Name |
            Sort-Object -CaseSensitive
    )
    $expectedAggregateNames = [string[]] @(
        $suiteNames |
            ForEach-Object { "$($_).csv" } |
            Sort-Object -CaseSensitive
    )
    if (-not (Test-SameStringSequence `
            -Reference $baselineAggregateNames `
            -Difference $candidateAggregateNames)) {
        throw (
            "Paired baseline and candidate aggregate result filenames do not match: " +
                "baseline=[$($baselineAggregateNames -join ', ')]; " +
                "candidate=[$($candidateAggregateNames -join ', ')].")
    }
    if (-not (Test-SameStringSequence `
            -Reference $expectedAggregateNames `
            -Difference $baselineAggregateNames)) {
        throw (
            "Pair manifest '$Path' suites do not match the aggregate result files: " +
                "manifest=[$($expectedAggregateNames -join ', ')]; " +
                "results=[$($baselineAggregateNames -join ', ')].")
    }

    foreach ($revisionEvidence in @(
            [pscustomobject]@{
                Revision = 'baseline'
                Root = $baselineRawRoot
                Expected = [string[]] @($pairs | ForEach-Object BaselineRaw)
            }
            [pscustomobject]@{
                Revision = 'candidate'
                Root = $candidateRawRoot
                Expected = [string[]] @($pairs | ForEach-Object CandidateRaw)
            }
        )) {
        $expectedRawFiles = [Collections.Generic.HashSet[string]]::new($pathComparer)
        foreach ($expectedRawFile in $revisionEvidence.Expected) {
            [void] $expectedRawFiles.Add($expectedRawFile)
        }
        $actualRawFiles = [Collections.Generic.HashSet[string]]::new($pathComparer)
        foreach ($actualRawFile in @(
                Get-ChildItem `
                    -LiteralPath $revisionEvidence.Root `
                    -File `
                    -Recurse `
                    -Filter '*.csv'
            )) {
            [void] $actualRawFiles.Add([IO.Path]::GetFullPath($actualRawFile.FullName))
        }
        if (-not $expectedRawFiles.SetEquals($actualRawFiles)) {
            $unreferenced = @(
                $actualRawFiles |
                    Where-Object { -not $expectedRawFiles.Contains($_) } |
                    Sort-Object
            )
            $missing = @(
                $expectedRawFiles |
                    Where-Object { -not $actualRawFiles.Contains($_) } |
                    Sort-Object
            )
            throw (
                "Paired $($revisionEvidence.Revision) raw evidence must be " +
                    'referenced exactly once by the manifest; ' +
                    "unreferenced=[$($unreferenced -join ', ')]; " +
                    "missing=[$($missing -join ', ')].")
        }
    }

    return [object[]] @($pairs)
}

function Get-PairedStratumAssessment {
    param(
        [Parameter(Mandatory)]
        [string] $Order,

        [Parameter(Mandatory)]
        [object[]] $Effects
    )

    $medianThroughputLog = Get-MedianDouble `
        -Values ([double[]] @($Effects | ForEach-Object ThroughputLog))
    $medianLatencyLog = Get-MedianDouble `
        -Values ([double[]] @($Effects | ForEach-Object LatencyLog))
    $medianDiagnosticP99Log = Get-MedianDouble `
        -Values ([double[]] @($Effects | ForEach-Object DiagnosticP99Log))
    $medianLatencyDifference = Get-MedianDecimal `
        -Values ([decimal[]] @($Effects | ForEach-Object LatencyDifference))
    $medianDiagnosticP99Difference = Get-MedianDecimal `
        -Values ([decimal[]] @(
                $Effects | ForEach-Object DiagnosticP99Difference
            ))
    $medianThroughputRatio = Get-MedianDecimal `
        -Values ([decimal[]] @($Effects | ForEach-Object ThroughputRatio))
    $medianLatencyRatio = Get-MedianDecimal `
        -Values ([decimal[]] @($Effects | ForEach-Object LatencyRatio))
    $throughputOutliers = [Collections.Generic.List[string]]::new()
    $latencyOutliers = [Collections.Generic.List[string]]::new()
    $stableThroughputPairCount = 0
    $stableLatencyPairCount = 0
    $latencyRelativeCrossingPairCount = 0
    $latencyAbsoluteCrossingPairCount = 0
    $latencyBothLimitsPairCount = 0

    foreach ($effect in $Effects) {
        $throughputDeviation = [decimal]::Abs(
            (($effect.ThroughputRatio / $medianThroughputRatio) - 1) * 100)
        if ($throughputDeviation -gt $stabilityThroughputPercent) {
            $throughputOutliers.Add(
                "$Order pair $($effect.PairId): throughput effect deviates " +
                    "$(Format-Percent $throughputDeviation)% from its order-stratum median.")
        }
        else {
            $stableThroughputPairCount++
        }

        $latencyDeviation = [decimal]::Abs(
            (($effect.LatencyRatio / $medianLatencyRatio) - 1) * 100)
        $latencyDifferenceDeviation = [decimal]::Abs(
            $effect.LatencyDifference - $medianLatencyDifference)
        if ($latencyDeviation -gt $stabilityLatencyPercent -and
            $latencyDifferenceDeviation -gt $stabilityLatencyMilliseconds) {
            $latencyOutliers.Add(
                "$Order pair $($effect.PairId): $blockingLatencyLabel effect deviates " +
                    "$(Format-Percent $latencyDeviation)% " +
                    "($(Format-Milliseconds $latencyDifferenceDeviation) ms) " +
                    'from its order-stratum median.')
        }
        else {
            $stableLatencyPairCount++
        }

        $pairLatencyRegression = ($effect.LatencyRatio - 1) * 100
        $pairLatencyRelativeCrossed =
            $pairLatencyRegression -gt $maxLatencyRegressionPercentExact
        $pairLatencyAbsoluteCrossed =
            $effect.LatencyDifference -gt $maxLatencyRegressionMillisecondsExact
        if ($pairLatencyRelativeCrossed) {
            $latencyRelativeCrossingPairCount++
        }
        if ($pairLatencyAbsoluteCrossed) {
            $latencyAbsoluteCrossingPairCount++
        }
        if ($pairLatencyRelativeCrossed -and $pairLatencyAbsoluteCrossed) {
            $latencyBothLimitsPairCount++
        }

    }

    $requiredStablePairCount = [int] ([Math]::Floor($Effects.Count / 2) + 1)
    $throughputRegression = (1 - $medianThroughputRatio) * 100
    $latencyRegression = ($medianLatencyRatio - 1) * 100
    $diagnosticP99Regression =
        (([decimal] [Math]::Exp($medianDiagnosticP99Log)) - 1) * 100
    $throughputFailed =
        $throughputRegression -gt $maxThroughputRegressionPercentExact
    $latencyRelativeLimitExceeded =
        $latencyRelativeCrossingPairCount -ge $requiredStablePairCount
    $latencyAbsoluteLimitExceeded =
        $latencyAbsoluteCrossingPairCount -ge $requiredStablePairCount
    $latencyFailed = $latencyBothLimitsPairCount -ge $requiredStablePairCount

    return [pscustomobject]@{
        Order = $Order
        IsStable =
            $stableThroughputPairCount -ge $requiredStablePairCount -and
            $stableLatencyPairCount -ge $requiredStablePairCount
        StableThroughputPairCount = $stableThroughputPairCount
        StableLatencyPairCount = $stableLatencyPairCount
        RequiredStablePairCount = $requiredStablePairCount
        TotalPairCount = $Effects.Count
        MedianThroughputLog = $medianThroughputLog
        MedianLatencyLog = $medianLatencyLog
        MedianDiagnosticP99Log = $medianDiagnosticP99Log
        MedianThroughputRatio = $medianThroughputRatio
        MedianLatencyRatio = $medianLatencyRatio
        MedianLatencyDifference = $medianLatencyDifference
        MedianDiagnosticP99Difference = $medianDiagnosticP99Difference
        ThroughputRegression = $throughputRegression
        LatencyRegression = $latencyRegression
        DiagnosticP99Regression = $diagnosticP99Regression
        ThroughputFailed = $throughputFailed
        LatencyRelativeLimitExceeded = $latencyRelativeLimitExceeded
        LatencyAbsoluteLimitExceeded = $latencyAbsoluteLimitExceeded
        LatencyRelativeCrossingPairCount = $latencyRelativeCrossingPairCount
        LatencyAbsoluteCrossingPairCount = $latencyAbsoluteCrossingPairCount
        LatencyBothLimitsPairCount = $latencyBothLimitsPairCount
        LatencyFailed = $latencyFailed
        ThroughputOutliers = [string[]] @($throughputOutliers)
        LatencyOutliers = [string[]] @($latencyOutliers)
    }
}

function Write-PairedComparisonReport {
    param(
        [Parameter(Mandatory)]
        [object[]] $Results,

        [Parameter(Mandatory)]
        [string] $ManifestPath,

        [Parameter(Mandatory)]
        [int] $PairCount
    )

    $reportDirectory = Split-Path -Parent $resolvedReportPath
    New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
    $invalidEvidence = @($Results | Where-Object Status -eq 'INVALID')
    $unstableEvidence = @($Results | Where-Object Status -eq 'UNSTABLE')
    $orderSensitiveEvidence = @(
        $Results | Where-Object Status -eq 'ORDER-SENSITIVE'
    )
    $confirmedRegressions = @($Results | Where-Object Status -eq 'REGRESSION')
    $failures = @($Results | Where-Object Status -ne 'PASS')
    $lines = [Collections.Generic.List[string]]::new()
    $lines.Add('# Previous-release paired performance comparison')
    $lines.Add('')
    $lines.Add('- Comparison mode: adjacent revision pairs')
    $lines.Add("- Pair manifest: ``$ManifestPath``")
    $lines.Add("- Validated pairs: $PairCount")
    $lines.Add("- Repeat count per order stratum: $RepeatCount")
    $lines.Add("- Throughput regression limit: $MaxThroughputRegressionPercent%")
    $lines.Add("- $blockingLatencyLabel regression limit: $MaxP99RegressionPercent%")
    $lines.Add(
        "- $blockingLatencyLabel absolute regression allowance: " +
            "$(Format-Milliseconds $maxLatencyRegressionMillisecondsExact) ms")
    $lines.Add(
        "- $blockingLatencyLabel failure rule: a strict majority of the same " +
            'pairs in each order ' +
            'stratum must exceed both the relative and absolute limits.')
    $lines.Add(
        "- Pair stability rule: throughput and $blockingLatencyLabel each require " +
            'their own strict ' +
            'majority of pair effects in each order stratum. Throughput must stay ' +
            "within 15% of its stratum median; $blockingLatencyLabel must stay " +
            'within either 25% or ' +
            '0.0500 ms of its stratum median effect.')
    $lines.Add(
        '- Combined effects equally average the previous-candidate and ' +
            "candidate-previous median log ratios; the $blockingLatencyLabel " +
            'difference equally ' +
            'averages their median millisecond differences.')
    if ($blockingLatencyLabel -cne 'P99') {
        $lines.Add(
            '- Blocking latency percentile: P95. P99 is retained as a ' +
                'non-blocking diagnostic and does not affect stability, order, ' +
                'or regression status.')
    }
    $lines.Add("- Invalid evidence rows: $($invalidEvidence.Count)")
    $lines.Add("- Insufficient stability rows: $($unstableEvidence.Count)")
    $lines.Add("- Order-sensitive rows: $($orderSensitiveEvidence.Count)")
    $lines.Add("- Confirmed regression rows: $($confirmedRegressions.Count)")
    $lines.Add("- Result: **$(if ($failures.Count -eq 0) { 'PASS' } else { 'FAIL' })**")
    $lines.Add('')
    if ($blockingLatencyLabel -ceq 'P99') {
        $lines.Add(
            '| Suite | Row | Throughput regression | P99 regression | ' +
                'P99 difference | Status | Notes |')
        $lines.Add('|---|---|---:|---:|---:|---|---|')
    }
    else {
        $lines.Add(
            '| Suite | Row | Throughput regression | P95 regression | ' +
                'P95 difference | P99 diagnostic regression | ' +
                'P99 diagnostic difference | Status | Notes |')
        $lines.Add('|---|---|---:|---:|---:|---:|---:|---|---|')
    }
    foreach ($result in $Results) {
        $throughput = if ($null -eq $result.ThroughputRegression) {
            'n/a'
        }
        else {
            "$(Format-Percent $result.ThroughputRegression)%"
        }
        $latency = if ($null -eq $result.LatencyRegression) {
            'n/a'
        }
        else {
            "$(Format-Percent $result.LatencyRegression)%"
        }
        $latencyDifference = if ($null -eq $result.LatencyDifference) {
            'n/a'
        }
        else {
            "$(Format-Milliseconds $result.LatencyDifference) ms"
        }
        $suiteName = Convert-ToMarkdownCell $result.Suite
        $rowName = Convert-ToMarkdownCell $result.Row
        $notes = Convert-ToMarkdownCell $result.Notes
        if ($blockingLatencyLabel -ceq 'P99') {
            $lines.Add(
                "| $suiteName | $rowName | $throughput | $latency | " +
                    "$latencyDifference | $($result.Status) | $notes |")
        }
        else {
            $diagnosticP99 = if ($null -eq $result.DiagnosticP99Regression) {
                'n/a'
            }
            else {
                "$(Format-Percent $result.DiagnosticP99Regression)%"
            }
            $diagnosticP99Difference =
                if ($null -eq $result.DiagnosticP99Difference) {
                    'n/a'
                }
                else {
                    "$(Format-Milliseconds $result.DiagnosticP99Difference) ms"
                }
            $lines.Add(
                "| $suiteName | $rowName | $throughput | $latency | " +
                    "$latencyDifference | $diagnosticP99 | " +
                    "$diagnosticP99Difference | $($result.Status) | $notes |")
        }
    }

    [IO.File]::WriteAllLines($resolvedReportPath, $lines)
    Write-Host (
        "Compared $($Results.Count) paired release-core rows; " +
            "invalid=$($invalidEvidence.Count), " +
            "unstable=$($unstableEvidence.Count), " +
            "order-sensitive=$($orderSensitiveEvidence.Count), " +
            "regressions=$($confirmedRegressions.Count).")
    Write-Host "Report: $resolvedReportPath"
}

function Invoke-PairedComparison {
    param(
        [Parameter(Mandatory)]
        [string] $ManifestPath
    )

    $pairedResults = [Collections.Generic.List[object]]::new()
    [object[]] $manifestPairs = @()
    try {
        $manifestPairs = @(Read-PairManifest -Path $ManifestPath)
    }
    catch {
        $pairedResults.Add((New-EvidenceResult `
                    -Suite '<manifest>' `
                    -Row '<manifest>' `
                    -Status 'INVALID' `
                    -Notes "Invalid paired evidence: $($_.Exception.Message)"))
        Write-PairedComparisonReport `
            -Results ([object[]] @($pairedResults)) `
            -ManifestPath $ManifestPath `
            -PairCount 0
        throw 'Previous-release paired performance comparison found invalid manifest evidence.'
    }

    $suiteNames = @($manifestPairs.Suite | Sort-Object -CaseSensitive -Unique)
    foreach ($suiteName in $suiteNames) {
        $suitePairs = @(
            $manifestPairs |
                Where-Object Suite -CEQ $suiteName |
                Sort-Object PairId -CaseSensitive
        )
        $pairEvidence = [Collections.Generic.List[object]]::new()
        $suiteReadIssues = [Collections.Generic.List[string]]::new()
        foreach ($pair in $suitePairs) {
            try {
                $pairEvidence.Add([pscustomobject]@{
                    Pair = $pair
                    Baseline = Read-EvidenceCsv `
                        -Path $pair.BaselineRaw `
                        -RequirePairedSchema
                    Candidate = Read-EvidenceCsv `
                        -Path $pair.CandidateRaw `
                        -RequirePairedSchema
                })
            }
            catch {
                $suiteReadIssues.Add(
                    "$($pair.PairId): $($_.Exception.Message)")
            }
        }

        if ($suiteReadIssues.Count -gt 0) {
            $pairedResults.Add((New-EvidenceResult `
                        -Suite $suiteName `
                        -Row '<evidence>' `
                        -Status 'INVALID' `
                        -Notes (
                            'Invalid paired evidence: ' +
                            "$($suiteReadIssues -join ' ')")))
            continue
        }

        $canonicalEvidence = $pairEvidence[0].Baseline
        $canonicalColumns = [string[]] $canonicalEvidence.Columns
        $canonicalRows = [string[]] @($canonicalEvidence.RowsByName.Keys)
        $schemaIssues = [Collections.Generic.List[string]]::new()
        foreach ($entry in $pairEvidence) {
            foreach ($revisionEvidence in @(
                    [pscustomobject]@{
                        Revision = 'baseline'
                        Evidence = $entry.Baseline
                    }
                    [pscustomobject]@{
                        Revision = 'candidate'
                        Evidence = $entry.Candidate
                    }
                )) {
                if (-not (Test-SameStringSet `
                        -Reference $canonicalColumns `
                        -Difference ([string[]] $revisionEvidence.Evidence.Columns))) {
                    $schemaIssues.Add(
                        "$($entry.Pair.PairId) $($revisionEvidence.Revision) schema " +
                            'does not match the suite schema.')
                }
                if (-not (Test-SameStringSet `
                        -Reference $canonicalRows `
                        -Difference ([string[]] @(
                                $revisionEvidence.Evidence.RowsByName.Keys)))) {
                    $schemaIssues.Add(
                        "$($entry.Pair.PairId) $($revisionEvidence.Revision) row set " +
                            'does not match the suite row set.')
                }
            }
        }

        if ($schemaIssues.Count -gt 0) {
            $pairedResults.Add((New-EvidenceResult `
                        -Suite $suiteName `
                        -Row '<schema>' `
                        -Status 'INVALID' `
                        -Notes (
                            'Invalid paired evidence: ' +
                            "$($schemaIssues -join ' ')")))
            continue
        }

        foreach ($rowName in ($canonicalRows | Sort-Object -CaseSensitive)) {
            $effects = [Collections.Generic.List[object]]::new()
            $rowIssues = [Collections.Generic.List[string]]::new()
            foreach ($entry in $pairEvidence) {
                try {
                    $baselineRow = $entry.Baseline.RowsByName[$rowName]
                    $candidateRow = $entry.Candidate.RowsByName[$rowName]
                    $baselineDescription =
                        "$suiteName/$rowName/$($entry.Pair.PairId)/baseline"
                    $candidateDescription =
                        "$suiteName/$rowName/$($entry.Pair.PairId)/candidate"
                    $baselineMetrics = Get-GateMetrics `
                        -Row $baselineRow `
                        -Description $baselineDescription
                    $candidateMetrics = Get-GateMetrics `
                        -Row $candidateRow `
                        -Description $candidateDescription
                    if ($suiteName -ceq $hybridQualificationSuite) {
                        Assert-HybridQualificationEvidence `
                            -Row $baselineRow `
                            -Metrics $baselineMetrics `
                            -Description $baselineDescription
                        Assert-HybridQualificationEvidence `
                            -Row $candidateRow `
                            -Metrics $candidateMetrics `
                            -Description $candidateDescription
                    }
                    $effects.Add([pscustomobject]@{
                        PairId = $entry.Pair.PairId
                        Order = $entry.Pair.Order
                        ThroughputLog = [Math]::Log(
                            [double] $candidateMetrics.OpsPerSec /
                                [double] $baselineMetrics.OpsPerSec)
                        LatencyLog = [Math]::Log(
                            [double] $candidateMetrics.BlockingLatency /
                                [double] $baselineMetrics.BlockingLatency)
                        DiagnosticP99Log = [Math]::Log(
                            [double] $candidateMetrics.DiagnosticP99 /
                                [double] $baselineMetrics.DiagnosticP99)
                        ThroughputRatio =
                            $candidateMetrics.OpsPerSec / $baselineMetrics.OpsPerSec
                        LatencyRatio =
                            $candidateMetrics.BlockingLatency /
                                $baselineMetrics.BlockingLatency
                        LatencyDifference =
                            $candidateMetrics.BlockingLatency -
                                $baselineMetrics.BlockingLatency
                        DiagnosticP99Difference =
                            $candidateMetrics.DiagnosticP99 -
                                $baselineMetrics.DiagnosticP99
                    })
                }
                catch {
                    $rowIssues.Add($_.Exception.Message)
                }
            }

            if ($rowIssues.Count -gt 0) {
                $pairedResults.Add((New-EvidenceResult `
                            -Suite $suiteName `
                            -Row $rowName `
                            -Status 'INVALID' `
                            -Notes (
                                'Invalid paired evidence: ' +
                                "$($rowIssues -join ' ')")))
                continue
            }

            $strata = @(
                foreach ($order in @(
                        'previous-candidate',
                        'candidate-previous'
                    )) {
                    Get-PairedStratumAssessment `
                        -Order $order `
                        -Effects ([object[]] @(
                                $effects | Where-Object Order -CEQ $order
                            ))
                }
            )
            $unstableStrata = @($strata | Where-Object { -not $_.IsStable })
            if ($unstableStrata.Count -gt 0) {
                $stabilityIssues = [Collections.Generic.List[string]]::new()
                foreach ($stratum in $unstableStrata) {
                    $throughputOutlierDetail =
                        if (@($stratum.ThroughputOutliers).Count -gt 0) {
                            "; outliers: $($stratum.ThroughputOutliers -join ' ')"
                        }
                        else {
                            ''
                        }
                    $latencyOutlierDetail =
                        if (@($stratum.LatencyOutliers).Count -gt 0) {
                            "; outliers: $($stratum.LatencyOutliers -join ' ')"
                        }
                        else {
                            ''
                        }
                    $stabilityIssues.Add(
                        "$($stratum.Order): throughput stability=" +
                            "$($stratum.StableThroughputPairCount)/" +
                            "$($stratum.TotalPairCount) " +
                            "($($stratum.RequiredStablePairCount) required)" +
                            "$throughputOutlierDetail; $blockingLatencyLabel stability=" +
                            "$($stratum.StableLatencyPairCount)/" +
                            "$($stratum.TotalPairCount) " +
                            "($($stratum.RequiredStablePairCount) required)" +
                            "$latencyOutlierDetail.")
                }
                $pairedResults.Add((New-EvidenceResult `
                            -Suite $suiteName `
                            -Row $rowName `
                            -Status 'UNSTABLE' `
                            -Notes (
                                'Insufficient paired stability: ' +
                                "$($stabilityIssues -join ' ')")))
                continue
            }

            $combinedThroughputLog =
                ($strata[0].MedianThroughputLog +
                    $strata[1].MedianThroughputLog) / 2
            $combinedLatencyLog =
                ($strata[0].MedianLatencyLog +
                    $strata[1].MedianLatencyLog) / 2
            $combinedDiagnosticP99Log =
                ($strata[0].MedianDiagnosticP99Log +
                    $strata[1].MedianDiagnosticP99Log) / 2
            $combinedThroughputRegression = Convert-DoubleToDecimal `
                -Value ((1 - [Math]::Exp($combinedThroughputLog)) * 100) `
                -Description "$suiteName/$rowName combined throughput effect"
            $combinedLatencyRegression = Convert-DoubleToDecimal `
                -Value (([Math]::Exp($combinedLatencyLog) - 1) * 100) `
                -Description (
                    "$suiteName/$rowName combined $blockingLatencyLabel effect")
            $combinedLatencyDifference =
                ($strata[0].MedianLatencyDifference +
                    $strata[1].MedianLatencyDifference) / 2
            $combinedDiagnosticP99Regression = Convert-DoubleToDecimal `
                -Value (([Math]::Exp($combinedDiagnosticP99Log) - 1) * 100) `
                -Description "$suiteName/$rowName combined diagnostic P99 effect"
            $combinedDiagnosticP99Difference =
                ($strata[0].MedianDiagnosticP99Difference +
                    $strata[1].MedianDiagnosticP99Difference) / 2

            $throughputDisagrees =
                $strata[0].ThroughputFailed -ne $strata[1].ThroughputFailed
            $latencyDisagrees =
                $strata[0].LatencyFailed -ne $strata[1].LatencyFailed
            $status = if ($throughputDisagrees -or $latencyDisagrees) {
                'ORDER-SENSITIVE'
            }
            elseif ($strata[0].ThroughputFailed -or $strata[0].LatencyFailed) {
                'REGRESSION'
            }
            else {
                'PASS'
            }

            $notes = [Collections.Generic.List[string]]::new()
            $notes.Add(
                'Order strata: ' +
                    "$($strata[0].Order) throughput=" +
                    "$(Format-Percent $strata[0].ThroughputRegression)%, " +
                    "$blockingLatencyLabel=" +
                    "$(Format-Percent $strata[0].LatencyRegression)%, " +
                    "$blockingLatencyLabel difference=" +
                    "$(Format-Milliseconds $strata[0].MedianLatencyDifference) ms, " +
                    "$blockingLatencyLabel both-limit pairs=" +
                    "$($strata[0].LatencyBothLimitsPairCount)/" +
                    "$($strata[0].TotalPairCount), stability throughput=" +
                    "$($strata[0].StableThroughputPairCount)/" +
                    "$($strata[0].TotalPairCount), $blockingLatencyLabel=" +
                    "$($strata[0].StableLatencyPairCount)/" +
                    "$($strata[0].TotalPairCount); " +
                    "$($strata[1].Order) throughput=" +
                    "$(Format-Percent $strata[1].ThroughputRegression)%, " +
                    "$blockingLatencyLabel=" +
                    "$(Format-Percent $strata[1].LatencyRegression)%, " +
                    "$blockingLatencyLabel difference=" +
                    "$(Format-Milliseconds $strata[1].MedianLatencyDifference) ms, " +
                    "$blockingLatencyLabel both-limit pairs=" +
                    "$($strata[1].LatencyBothLimitsPairCount)/" +
                    "$($strata[1].TotalPairCount), stability throughput=" +
                    "$($strata[1].StableThroughputPairCount)/" +
                    "$($strata[1].TotalPairCount), $blockingLatencyLabel=" +
                    "$($strata[1].StableLatencyPairCount)/" +
                    "$($strata[1].TotalPairCount).")
            if ($blockingLatencyLabel -cne 'P99') {
                $diagnosticP99Difference0 = Format-Milliseconds `
                    $strata[0].MedianDiagnosticP99Difference
                $diagnosticP99Difference1 = Format-Milliseconds `
                    $strata[1].MedianDiagnosticP99Difference
                $notes.Add(
                    'Non-blocking P99 diagnostics by order: ' +
                        "$($strata[0].Order) P99=" +
                        "$(Format-Percent $strata[0].DiagnosticP99Regression)%, " +
                        "P99 difference=$diagnosticP99Difference0 ms; " +
                        "$($strata[1].Order) P99=" +
                        "$(Format-Percent $strata[1].DiagnosticP99Regression)%, " +
                        "P99 difference=$diagnosticP99Difference1 ms.")
            }
            foreach ($stratum in $strata) {
                if (@($stratum.ThroughputOutliers).Count -gt 0) {
                    $notes.Add(
                        'Tolerated paired outlier for throughput with a strict ' +
                            "stable majority in $($stratum.Order) " +
                            "($($stratum.StableThroughputPairCount)/" +
                            "$($stratum.TotalPairCount); " +
                            "$($stratum.RequiredStablePairCount) required): " +
                            "$($stratum.ThroughputOutliers -join ' ')")
                }
                if (@($stratum.LatencyOutliers).Count -gt 0) {
                    $notes.Add(
                        "Tolerated paired outlier for $blockingLatencyLabel with a " +
                            'strict stable ' +
                            "majority in $($stratum.Order) " +
                            "($($stratum.StableLatencyPairCount)/" +
                            "$($stratum.TotalPairCount); " +
                            "$($stratum.RequiredStablePairCount) required): " +
                            "$($stratum.LatencyOutliers -join ' ')")
                }
                if ($stratum.LatencyRelativeLimitExceeded -and
                    -not $stratum.LatencyFailed) {
                    if ($stratum.LatencyAbsoluteLimitExceeded) {
                        $notes.Add(
                            "Disjoint $blockingLatencyLabel crossings in " +
                                "$($stratum.Order): relative " +
                                'and absolute limits each have a strict majority, but ' +
                                'a strict majority of the same pairs did not exceed both.')
                    }
                    else {
                        $notes.Add(
                            "$blockingLatencyLabel percentage-only crossing in " +
                                "$($stratum.Order): " +
                                "$($stratum.LatencyRelativeCrossingPairCount)/" +
                                "$($stratum.TotalPairCount) pairs exceeded the relative " +
                                'limit, but a strict majority of the same pairs did not ' +
                                'also exceed the absolute allowance.')
                    }
                }
            }
            if ($status -eq 'ORDER-SENSITIVE') {
                $notes.Add(
                    'Order-sensitive paired effect: the two order strata disagree ' +
                        'on at least one release gate.')
            }
            elseif ($status -eq 'REGRESSION') {
                $confirmedMetrics = [Collections.Generic.List[string]]::new()
                if ($strata[0].ThroughputFailed) {
                    $confirmedMetrics.Add('throughput')
                }
                if ($strata[0].LatencyFailed) {
                    $confirmedMetrics.Add($blockingLatencyLabel)
                }
                $notes.Add(
                    'Confirmed paired candidate regression in both order strata: ' +
                        "$($confirmedMetrics -join ' and ').")
            }

            $pairedResults.Add([pscustomobject]@{
                Suite = $suiteName
                Row = $rowName
                ThroughputRegression = $combinedThroughputRegression
                LatencyRegression = $combinedLatencyRegression
                LatencyDifference = $combinedLatencyDifference
                DiagnosticP99Regression = $combinedDiagnosticP99Regression
                DiagnosticP99Difference = $combinedDiagnosticP99Difference
                Status = $status
                Notes = $notes -join ' '
            })
        }
    }

    Write-PairedComparisonReport `
        -Results ([object[]] @($pairedResults)) `
        -ManifestPath $ManifestPath `
        -PairCount $manifestPairs.Count
    $pairedFailures = @($pairedResults | Where-Object Status -ne 'PASS')
    if ($pairedFailures.Count -gt 0) {
        $invalidCount = @($pairedResults | Where-Object Status -eq 'INVALID').Count
        $unstableCount = @($pairedResults | Where-Object Status -eq 'UNSTABLE').Count
        $orderSensitiveCount = @(
            $pairedResults | Where-Object Status -eq 'ORDER-SENSITIVE'
        ).Count
        $regressionCount = @(
            $pairedResults | Where-Object Status -eq 'REGRESSION'
        ).Count
        throw (
            "Previous-release paired performance comparison failed for " +
                "$($pairedFailures.Count) row(s): invalid=$invalidCount, " +
                "unstable=$unstableCount, order-sensitive=$orderSensitiveCount, " +
                "regressions=$regressionCount.")
    }
}

if ($pairedComparison) {
    Invoke-PairedComparison -ManifestPath $resolvedPairManifestPath
    return
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
    throw 'Both release-core median result directories must contain at least one CSV file.'
}

$baselineFilesByName = New-FileMap -Files @($baselineFiles) -Description 'Baseline results'
$candidateFilesByName = New-FileMap -Files @($candidateFiles) -Description 'Candidate results'
$allFileNames = @(
    $baselineFilesByName.Keys
    $candidateFilesByName.Keys
) | Sort-Object -CaseSensitive -Unique
$results = [Collections.Generic.List[object]]::new()
$expectedAggregateTag = "Aggregate=median-of-$RepeatCount"

foreach ($fileName in $allFileNames) {
    $suiteName = [IO.Path]::GetFileNameWithoutExtension($fileName)
    if (-not $baselineFilesByName.ContainsKey($fileName)) {
        $results.Add((New-EvidenceResult `
                    -Suite $suiteName `
                    -Row '<file>' `
                    -Status 'INVALID' `
                    -Notes 'Invalid evidence: baseline median result file is missing.'))
        continue
    }

    if (-not $candidateFilesByName.ContainsKey($fileName)) {
        $results.Add((New-EvidenceResult `
                    -Suite $suiteName `
                    -Row '<file>' `
                    -Status 'INVALID' `
                    -Notes 'Invalid evidence: candidate median result file is missing.'))
        continue
    }

    $baselineRawPaths = [Collections.Generic.List[string]]::new()
    $candidateRawPaths = [Collections.Generic.List[string]]::new()
    $missingRawPaths = [Collections.Generic.List[string]]::new()
    for ($run = 1; $run -le $RepeatCount; $run++) {
        $baselineRawPath = Join-Path `
            (Join-Path $baselineRawRoot $suiteName) `
            "run-$run.csv"
        $candidateRawPath = Join-Path `
            (Join-Path $candidateRawRoot $suiteName) `
            "run-$run.csv"
        $baselineRawPaths.Add($baselineRawPath)
        $candidateRawPaths.Add($candidateRawPath)
        if (-not (Test-Path -LiteralPath $baselineRawPath -PathType Leaf)) {
            $missingRawPaths.Add("baseline raw run $run")
        }
        if (-not (Test-Path -LiteralPath $candidateRawPath -PathType Leaf)) {
            $missingRawPaths.Add("candidate raw run $run")
        }
    }

    if ($missingRawPaths.Count -gt 0) {
        $results.Add((New-EvidenceResult `
                    -Suite $suiteName `
                    -Row '<evidence>' `
                    -Status 'INVALID' `
                    -Notes (
                        'Invalid evidence: missing required files: ' +
                        "$($missingRawPaths -join ', ').")))
        continue
    }

    try {
        $baselineMedianEvidence = Read-EvidenceCsv `
            -Path $baselineFilesByName[$fileName].FullName
        $candidateMedianEvidence = Read-EvidenceCsv `
            -Path $candidateFilesByName[$fileName].FullName
        $baselineRawEvidence = @(
            foreach ($path in $baselineRawPaths) {
                Read-EvidenceCsv -Path $path
            }
        )
        $candidateRawEvidence = @(
            foreach ($path in $candidateRawPaths) {
                Read-EvidenceCsv -Path $path
            }
        )
    }
    catch {
        $results.Add((New-EvidenceResult `
                    -Suite $suiteName `
                    -Row '<evidence>' `
                    -Status 'INVALID' `
                    -Notes "Invalid evidence: $($_.Exception.Message)"))
        continue
    }

    $schemaIssues = [Collections.Generic.List[string]]::new()
    $canonicalColumns = [string[]] $baselineMedianEvidence.Columns
    $canonicalRows = [string[]] @($baselineMedianEvidence.RowsByName.Keys)
    $allEvidence = @(
        [pscustomobject]@{
            Description = 'candidate median'
            Evidence = $candidateMedianEvidence
        }
        for ($index = 0; $index -lt $RepeatCount; $index++) {
            [pscustomobject]@{
                Description = "baseline raw run $($index + 1)"
                Evidence = $baselineRawEvidence[$index]
            }
            [pscustomobject]@{
                Description = "candidate raw run $($index + 1)"
                Evidence = $candidateRawEvidence[$index]
            }
        }
    )
    foreach ($entry in $allEvidence) {
        if (-not (Test-SameStringSet `
                -Reference $canonicalColumns `
                -Difference ([string[]] $entry.Evidence.Columns))) {
            $schemaIssues.Add(
                "$($entry.Description) schema does not match the baseline median schema.")
        }

        if (-not (Test-SameStringSet `
                -Reference $canonicalRows `
                -Difference ([string[]] @($entry.Evidence.RowsByName.Keys)))) {
            $schemaIssues.Add(
                "$($entry.Description) row set does not match the baseline median row set.")
        }
    }

    if ($schemaIssues.Count -gt 0) {
        $results.Add((New-EvidenceResult `
                    -Suite $suiteName `
                    -Row '<schema>' `
                    -Status 'INVALID' `
                    -Notes "Invalid evidence: $($schemaIssues -join ' ')"))
        continue
    }

    foreach ($rowName in ($canonicalRows | Sort-Object -CaseSensitive)) {
        $baselineMedianRow = $baselineMedianEvidence.RowsByName[$rowName]
        $candidateMedianRow = $candidateMedianEvidence.RowsByName[$rowName]
        $evidenceIssues = [Collections.Generic.List[string]]::new()
        if (-not (Test-MedianAggregateTag `
                -ExtraInfo $baselineMedianRow.ExtraInfo `
                -ExpectedTag $expectedAggregateTag)) {
            $evidenceIssues.Add(
                "Baseline median ExtraInfo is missing '$expectedAggregateTag'.")
        }
        if (-not (Test-MedianAggregateTag `
                -ExtraInfo $candidateMedianRow.ExtraInfo `
                -ExpectedTag $expectedAggregateTag)) {
            $evidenceIssues.Add(
                "Candidate median ExtraInfo is missing '$expectedAggregateTag'.")
        }

        try {
            $baselineMedianMetrics = Get-GateMetrics `
                -Row $baselineMedianRow `
                -Description "$fileName/$rowName/baseline median"
            $candidateMedianMetrics = Get-GateMetrics `
                -Row $candidateMedianRow `
                -Description "$fileName/$rowName/candidate median"
            $baselineRawMetrics = @(
                for ($index = 0; $index -lt $RepeatCount; $index++) {
                    [pscustomobject]@{
                        Run = $index + 1
                        Metrics = Get-GateMetrics `
                            -Row $baselineRawEvidence[$index].RowsByName[$rowName] `
                            -Description (
                                "$fileName/$rowName/baseline raw run $($index + 1)")
                    }
                }
            )
            $candidateRawMetrics = @(
                for ($index = 0; $index -lt $RepeatCount; $index++) {
                    [pscustomobject]@{
                        Run = $index + 1
                        Metrics = Get-GateMetrics `
                            -Row $candidateRawEvidence[$index].RowsByName[$rowName] `
                            -Description (
                                "$fileName/$rowName/candidate raw run $($index + 1)")
                    }
                }
            )
        }
        catch {
            $evidenceIssues.Add($_.Exception.Message)
        }

        if ($evidenceIssues.Count -gt 0) {
            $results.Add((New-EvidenceResult `
                        -Suite $suiteName `
                        -Row $rowName `
                        -Status 'INVALID' `
                        -Notes "Invalid evidence: $($evidenceIssues -join ' ')"))
            continue
        }

        $recomputedBaselineThroughput = Get-MedianDecimal `
            -Values ([decimal[]] @(
                    $baselineRawMetrics |
                        ForEach-Object { $_.Metrics.OpsPerSec }
                ))
        $recomputedCandidateThroughput = Get-MedianDecimal `
            -Values ([decimal[]] @(
                    $candidateRawMetrics |
                        ForEach-Object { $_.Metrics.OpsPerSec }
                ))
        $recomputedBaselineLatency = Get-MedianDecimal `
            -Values ([decimal[]] @(
                    $baselineRawMetrics |
                        ForEach-Object { $_.Metrics.BlockingLatency }
                ))
        $recomputedCandidateLatency = Get-MedianDecimal `
            -Values ([decimal[]] @(
                    $candidateRawMetrics |
                        ForEach-Object { $_.Metrics.BlockingLatency }
                ))

        if ($baselineMedianMetrics.OpsPerSec -ne $recomputedBaselineThroughput) {
            $evidenceIssues.Add(
                'Baseline median OpsPerSec does not match the raw-run median ' +
                    "($(Format-ExactNumber $baselineMedianMetrics.OpsPerSec) versus " +
                    "$(Format-ExactNumber $recomputedBaselineThroughput)).")
        }
        if ($candidateMedianMetrics.OpsPerSec -ne $recomputedCandidateThroughput) {
            $evidenceIssues.Add(
                'Candidate median OpsPerSec does not match the raw-run median ' +
                    "($(Format-ExactNumber $candidateMedianMetrics.OpsPerSec) versus " +
                    "$(Format-ExactNumber $recomputedCandidateThroughput)).")
        }
        if ($baselineMedianMetrics.BlockingLatency -ne
            $recomputedBaselineLatency) {
            $evidenceIssues.Add(
                "Baseline median $blockingLatencyLabel does not match the " +
                    'raw-run median ' +
                    "($(Format-ExactNumber $baselineMedianMetrics.BlockingLatency) " +
                    "versus $(Format-ExactNumber $recomputedBaselineLatency)).")
        }
        if ($candidateMedianMetrics.BlockingLatency -ne
            $recomputedCandidateLatency) {
            $evidenceIssues.Add(
                "Candidate median $blockingLatencyLabel does not match the " +
                    'raw-run median ' +
                    "($(Format-ExactNumber $candidateMedianMetrics.BlockingLatency) " +
                    "versus $(Format-ExactNumber $recomputedCandidateLatency)).")
        }

        if ($evidenceIssues.Count -gt 0) {
            $results.Add((New-EvidenceResult `
                        -Suite $suiteName `
                        -Row $rowName `
                        -Status 'INVALID' `
                        -Notes "Invalid evidence: $($evidenceIssues -join ' ')"))
            continue
        }

        $baselineStability = Get-StabilityAssessment `
            -Revision 'Baseline' `
            -MedianMetrics $baselineMedianMetrics `
            -RawMetrics $baselineRawMetrics
        $candidateStability = Get-StabilityAssessment `
            -Revision 'Candidate' `
            -MedianMetrics $candidateMedianMetrics `
            -RawMetrics $candidateRawMetrics
        $unstableRevisions = @(
            @($baselineStability, $candidateStability) |
                Where-Object { -not $_.IsStable }
        )
        if ($unstableRevisions.Count -gt 0) {
            $stabilityIssues = [Collections.Generic.List[string]]::new()
            foreach ($assessment in @($baselineStability, $candidateStability)) {
                if ($assessment.IsStable) {
                    continue
                }

                $stabilityIssues.Add(
                    "$($assessment.Revision) has $($assessment.StableRunCount)/" +
                        "$($assessment.TotalRunCount) whole runs within both limits; " +
                        "at least $($assessment.RequiredStableRunCount) are required. " +
                        "$($assessment.Outliers -join ' ')")
            }

            $results.Add((New-EvidenceResult `
                        -Suite $suiteName `
                        -Row $rowName `
                        -Status 'UNSTABLE' `
                        -Notes (
                            'Insufficient stability: ' +
                            "$($stabilityIssues -join ' ')")))
            continue
        }

        $throughputRegression =
            (($baselineMedianMetrics.OpsPerSec - $candidateMedianMetrics.OpsPerSec) /
                $baselineMedianMetrics.OpsPerSec) * 100
        $latencyRegression =
            (($candidateMedianMetrics.BlockingLatency -
                    $baselineMedianMetrics.BlockingLatency) /
                $baselineMedianMetrics.BlockingLatency) * 100
        $latencyRegressionMilliseconds =
            $candidateMedianMetrics.BlockingLatency -
                $baselineMedianMetrics.BlockingLatency
        $diagnosticP99Regression =
            (($candidateMedianMetrics.DiagnosticP99 -
                    $baselineMedianMetrics.DiagnosticP99) /
                $baselineMedianMetrics.DiagnosticP99) * 100
        $throughputFailed =
            $throughputRegression -gt $maxThroughputRegressionPercentExact
        $latencyRelativeLimitExceeded =
            $latencyRegression -gt $maxLatencyRegressionPercentExact
        $latencyAbsoluteLimitExceeded =
            $latencyRegressionMilliseconds -gt
                $maxLatencyRegressionMillisecondsExact
        $latencyFailed =
            $latencyRelativeLimitExceeded -and $latencyAbsoluteLimitExceeded
        $notes = [Collections.Generic.List[string]]::new()
        foreach ($assessment in @($baselineStability, $candidateStability)) {
            if (@($assessment.Outliers).Count -eq 0) {
                continue
            }

            $notes.Add(
                'Tolerated raw-run outlier with a strict stable majority ' +
                    "($($assessment.StableRunCount)/$($assessment.TotalRunCount); " +
                    "$($assessment.RequiredStableRunCount) required): " +
                    "$($assessment.Outliers -join ' ')")
        }
        if ($throughputFailed) {
            $notes.Add(
                'Confirmed candidate throughput regression exceeded the ' +
                    "$(Format-Percent $maxThroughputRegressionPercentExact)% limit.")
        }
        if ($latencyRelativeLimitExceeded) {
            $absoluteOutcome = if ($latencyAbsoluteLimitExceeded) {
                'exceeded'
            }
            else {
                'did not exceed'
            }
            $latencyPrefix = if ($latencyFailed) {
                "Confirmed candidate $blockingLatencyLabel regression:"
            }
            else {
                "$blockingLatencyLabel percentage-only crossing:"
            }
            $notes.Add(
                "$latencyPrefix $blockingLatencyLabel increased by " +
                    "$(Format-Milliseconds $latencyRegressionMilliseconds) ms, " +
                    "which $absoluteOutcome the " +
                    "$(Format-Milliseconds $maxLatencyRegressionMillisecondsExact) " +
                    'ms ' +
                    'absolute allowance.')
        }

        $failed = $throughputFailed -or $latencyFailed
        $results.Add([pscustomobject]@{
            Suite = $suiteName
            Row = $rowName
            ThroughputRegression = $throughputRegression
            LatencyRegression = $latencyRegression
            LatencyDifference = $latencyRegressionMilliseconds
            DiagnosticP99Regression = $diagnosticP99Regression
            DiagnosticP99Difference =
                $candidateMedianMetrics.DiagnosticP99 -
                    $baselineMedianMetrics.DiagnosticP99
            Status = if ($failed) { 'REGRESSION' } else { 'PASS' }
            Notes = $notes -join ' '
        })
    }
}

$reportDirectory = Split-Path -Parent $resolvedReportPath
New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
$invalidEvidence = @($results | Where-Object Status -eq 'INVALID')
$unstableEvidence = @($results | Where-Object Status -eq 'UNSTABLE')
$confirmedRegressions = @($results | Where-Object Status -eq 'REGRESSION')
$failures = @($results | Where-Object Status -ne 'PASS')
$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Previous-release performance comparison')
$lines.Add('')
$lines.Add("- Baseline median results: ``$baselineRoot``")
$lines.Add("- Candidate median results: ``$candidateRoot``")
$lines.Add("- Baseline raw results: ``$baselineRawRoot``")
$lines.Add("- Candidate raw results: ``$candidateRawRoot``")
$lines.Add("- Repeat count: $RepeatCount")
$lines.Add("- Throughput regression limit: $MaxThroughputRegressionPercent%")
$lines.Add("- $blockingLatencyLabel regression limit: $MaxP99RegressionPercent%")
$lines.Add(
    "- $blockingLatencyLabel absolute regression allowance: " +
        "$(Format-Milliseconds $maxLatencyRegressionMillisecondsExact) ms")
$lines.Add(
    "- $blockingLatencyLabel failure rule: relative and absolute limits must " +
        'both be exceeded')
$lines.Add(
    '- Stability rule: a strict majority of whole raw runs must keep throughput ' +
        "within 15% of the revision median and $blockingLatencyLabel within " +
        'either 25% or ' +
        '0.0500 ms of the revision median; tolerated outlier runs remain visible ' +
        'in row notes.')
if ($blockingLatencyLabel -cne 'P99') {
    $lines.Add(
        '- Blocking latency percentile: P95. P99 is retained as a non-blocking ' +
            'diagnostic and does not affect stability or regression status.')
}
$lines.Add("- Invalid evidence rows: $($invalidEvidence.Count)")
$lines.Add("- Insufficient stability rows: $($unstableEvidence.Count)")
$lines.Add("- Confirmed regression rows: $($confirmedRegressions.Count)")
$lines.Add("- Result: **$(if ($failures.Count -eq 0) { 'PASS' } else { 'FAIL' })**")
$lines.Add('')
if ($blockingLatencyLabel -ceq 'P99') {
    $lines.Add('| Suite | Row | Throughput regression | P99 regression | Status | Notes |')
    $lines.Add('|---|---|---:|---:|---|---|')
}
else {
    $lines.Add(
        '| Suite | Row | Throughput regression | P95 regression | ' +
            'P99 diagnostic regression | Status | Notes |')
    $lines.Add('|---|---|---:|---:|---:|---|---|')
}
foreach ($result in $results) {
    $throughput = if ($null -eq $result.ThroughputRegression) {
        'n/a'
    }
    else {
        "$(Format-Percent $result.ThroughputRegression)%"
    }
    $latency = if ($null -eq $result.LatencyRegression) {
        'n/a'
    }
    else {
        "$(Format-Percent $result.LatencyRegression)%"
    }
    $suiteName = Convert-ToMarkdownCell $result.Suite
    $rowName = Convert-ToMarkdownCell $result.Row
    $notes = Convert-ToMarkdownCell $result.Notes
    if ($blockingLatencyLabel -ceq 'P99') {
        $lines.Add(
            "| $suiteName | $rowName | $throughput | $latency | " +
                "$($result.Status) | $notes |")
    }
    else {
        $diagnosticP99 = if ($null -eq $result.DiagnosticP99Regression) {
            'n/a'
        }
        else {
            "$(Format-Percent $result.DiagnosticP99Regression)%"
        }
        $lines.Add(
            "| $suiteName | $rowName | $throughput | $latency | " +
                "$diagnosticP99 | $($result.Status) | $notes |")
    }
}

[IO.File]::WriteAllLines($resolvedReportPath, $lines)
Write-Host (
    "Compared $($results.Count) release-core rows; " +
        "invalid=$($invalidEvidence.Count), " +
        "unstable=$($unstableEvidence.Count), " +
        "regressions=$($confirmedRegressions.Count).")
Write-Host "Report: $resolvedReportPath"

if ($failures.Count -gt 0) {
    throw (
        "Previous-release performance comparison failed for $($failures.Count) row(s): " +
            "invalid=$($invalidEvidence.Count), " +
            "unstable=$($unstableEvidence.Count), " +
            "regressions=$($confirmedRegressions.Count).")
}
