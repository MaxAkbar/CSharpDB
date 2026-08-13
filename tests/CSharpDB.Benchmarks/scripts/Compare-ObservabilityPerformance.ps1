#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $EvidencePath,

    [string] $ThresholdsPath = '',

    [string] $OutputJsonPath = '',

    [string] $OutputMarkdownPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchmarkDirectory = [IO.Path]::GetFullPath(
    [IO.Path]::Combine($scriptDirectory, '..'))
if ([string]::IsNullOrWhiteSpace($ThresholdsPath)) {
    $ThresholdsPath = Join-Path `
        $benchmarkDirectory `
        'observability-perf-thresholds.json'
}

$EvidencePath = [IO.Path]::GetFullPath($EvidencePath)
$ThresholdsPath = [IO.Path]::GetFullPath($ThresholdsPath)
if ([string]::IsNullOrWhiteSpace($OutputJsonPath)) {
    $OutputJsonPath = Join-Path `
        (Split-Path -Parent $EvidencePath) `
        'observability-performance-attestation.json'
}
if ([string]::IsNullOrWhiteSpace($OutputMarkdownPath)) {
    $OutputMarkdownPath = Join-Path `
        (Split-Path -Parent $EvidencePath) `
        'observability-performance-report.md'
}
$OutputJsonPath = [IO.Path]::GetFullPath($OutputJsonPath)
$OutputMarkdownPath = [IO.Path]::GetFullPath($OutputMarkdownPath)

$findings = [Collections.Generic.List[object]]::new()
$comparisons = [Collections.Generic.List[object]]::new()
$seriesResults = [Collections.Generic.List[object]]::new()
$decisionRequiredModes = [Collections.Generic.List[string]]::new()
$configurationBlockedModes = [Collections.Generic.List[string]]::new()
$policy = $null
$evidence = $null
$policySha256 = ''
$evidenceSha256 = ''
$policyId = ''
$referenceCommit = ''
$candidateCommit = ''
$structurallyValid = $false

function Get-Sha256 {
    param([Parameter(Mandatory)][string] $Path)

    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-RequiredProperty {
    param(
        [Parameter(Mandatory)][object] $Object,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][string] $Context
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) {
        throw "$Context is missing required property '$Name'."
    }
    return $property.Value
}

function Get-RequiredString {
    param(
        [Parameter(Mandatory)][object] $Object,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][string] $Context
    )

    $value = [string](Get-RequiredProperty -Object $Object -Name $Name -Context $Context)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "$Context property '$Name' must be a non-empty string."
    }
    return $value
}

function Get-RequiredDouble {
    param(
        [Parameter(Mandatory)][object] $Object,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][string] $Context
    )

    $value = Get-RequiredProperty -Object $Object -Name $Name -Context $Context
    try {
        $number = [Convert]::ToDouble(
            $value,
            [Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        throw "$Context property '$Name' is not numeric."
    }
    if ([double]::IsNaN($number) -or [double]::IsInfinity($number)) {
        throw "$Context property '$Name' must be finite."
    }
    return $number
}

function Assert-ExactNumber {
    param(
        [Parameter(Mandatory)][object] $Object,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][double] $Expected,
        [Parameter(Mandatory)][string] $Context
    )

    $actual = Get-RequiredDouble -Object $Object -Name $Name -Context $Context
    if ([Math]::Abs($actual - $Expected) -gt 0.000000001) {
        throw "$Context property '$Name' must be $Expected; found $actual."
    }
}

function Assert-Sha256 {
    param(
        [Parameter(Mandatory)][string] $Value,
        [Parameter(Mandatory)][string] $Context
    )

    if ($Value -cnotmatch '^[0-9a-f]{64}$') {
        throw "$Context must be a lowercase SHA-256 value."
    }
}

function ConvertTo-UtcTimestamp {
    param(
        [Parameter(Mandatory)][string] $Value,
        [Parameter(Mandatory)][string] $Context
    )

    $timestamp = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse(
            $Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::RoundtripKind,
            [ref] $timestamp)) {
        throw "$Context is not a round-trip timestamp."
    }
    return $timestamp.ToUniversalTime()
}

function Get-Median {
    param([Parameter(Mandatory)][double[]] $Values)

    if ($Values.Count -eq 0) {
        throw 'Cannot calculate a median for an empty series.'
    }
    [double[]] $ordered = @($Values | Sort-Object)
    $middle = [int][Math]::Floor($ordered.Count / 2)
    if (($ordered.Count % 2) -eq 1) {
        return $ordered[$middle]
    }
    return ($ordered[$middle - 1] + $ordered[$middle]) / 2.0
}

function Get-SpreadPercent {
    param([Parameter(Mandatory)][double[]] $Values)

    $median = Get-Median -Values $Values
    if ($median -le 0) {
        throw 'A launch-median series must contain positive elapsed values.'
    }
    $minimum = ($Values | Measure-Object -Minimum).Minimum
    $maximum = ($Values | Measure-Object -Maximum).Maximum
    return (($maximum - $minimum) / $median) * 100.0
}

function Add-Finding {
    param(
        [Parameter(Mandatory)][string] $Code,
        [Parameter(Mandatory)][string] $Message,
        [string] $Mode = '',
        [string] $PathId = ''
    )

    $findings.Add([ordered]@{
            severity = 'error'
            code = $Code
            mode = $Mode
            pathId = $PathId
            message = $Message
        })
}

function Test-SequenceEqual {
    param(
        [Parameter(Mandatory)][object[]] $Actual,
        [Parameter(Mandatory)][object[]] $Expected
    )

    if ($Actual.Count -ne $Expected.Count) {
        return $false
    }
    for ($index = 0; $index -lt $Expected.Count; $index++) {
        if ([string] $Actual[$index] -cne [string] $Expected[$index]) {
            return $false
        }
    }
    return $true
}

function Get-FingerprintKey {
    param(
        [Parameter(Mandatory)][object] $Machine,
        [Parameter(Mandatory)][string] $Context
    )

    $parts = foreach ($name in @(
            'runnerId',
            'machineName',
            'cpuName',
            'logicalCoreCount',
            'osDescription',
            'osArchitecture',
            'dotnetSdk',
            'dotnetRuntime',
            'powerProfile')) {
        $value = Get-RequiredString -Object $Machine -Name $name -Context $Context
        if ($value.Contains("`n") -or $value.Contains("`r")) {
            throw "$Context property '$name' must be single-line."
        }
        "$name=$value"
    }
    return $parts -join "`n"
}

function Assert-ArtifactSet {
    param(
        [Parameter(Mandatory)][object[]] $Artifacts,
        [Parameter(Mandatory)][string] $Context,
        [Parameter(Mandatory)][string] $Root
    )

    if ($Artifacts.Count -eq 0) {
        throw "$Context must retain at least one hashed artifact."
    }

    $comparison = if ($IsWindows) {
        [StringComparison]::OrdinalIgnoreCase
    }
    else {
        [StringComparison]::Ordinal
    }
    $rootPrefix = $Root.TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    $seenPaths = [Collections.Generic.HashSet[string]]::new(
        $(if ($IsWindows) {
                [StringComparer]::OrdinalIgnoreCase
            }
            else {
                [StringComparer]::Ordinal
            }))

    foreach ($artifact in $Artifacts) {
        $relativePath = Get-RequiredString `
            -Object $artifact `
            -Name 'relativePath' `
            -Context $Context
        if ([IO.Path]::IsPathRooted($relativePath)) {
            throw "$Context artifact path must be relative: $relativePath"
        }
        $fullPath = [IO.Path]::GetFullPath([IO.Path]::Combine($Root, $relativePath))
        if (-not $fullPath.StartsWith($rootPrefix, $comparison)) {
            throw "$Context artifact escapes the evidence root: $relativePath"
        }
        if (-not $seenPaths.Add($fullPath)) {
            throw "$Context contains duplicate artifact path '$relativePath'."
        }
        if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
            throw "$Context artifact is missing: $relativePath"
        }
        $item = Get-Item -LiteralPath $fullPath
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Context artifact must not be a reparse point: $relativePath"
        }
        $expectedHash = (Get-RequiredString `
                -Object $artifact `
                -Name 'sha256' `
                -Context $Context).ToLowerInvariant()
        Assert-Sha256 -Value $expectedHash -Context "$Context artifact hash"
        $actualHash = Get-Sha256 -Path $fullPath
        if ($actualHash -cne $expectedHash) {
            throw "$Context artifact hash mismatch: $relativePath"
        }
    }
}

function Get-MeasurementMap {
    param(
        [Parameter(Mandatory)][object] $Run,
        [Parameter(Mandatory)][string[]] $ExpectedModes,
        [Parameter(Mandatory)][object[]] $Paths,
        [Parameter(Mandatory)][string] $Role
    )

    $measurements = @(Get-RequiredProperty `
            -Object $Run `
            -Name 'measurements' `
            -Context "pair $($Run.pairNumber) $Role run")
    $expectedCount = if ($Role -ceq 'reference') {
        $Paths.Count
    }
    else {
        $Paths.Count * $ExpectedModes.Count
    }
    if ($measurements.Count -ne $expectedCount) {
        throw (
            "Pair $($Run.pairNumber) $Role run must contain exactly " +
            "$expectedCount normalized measurements; found $($measurements.Count).")
    }

    $pathById = @{}
    foreach ($path in $Paths) {
        $pathById[[string] $path.id] = $path
    }
    $map = @{}
    foreach ($measurement in $measurements) {
        $context = "pair $($Run.pairNumber) $Role measurement"
        $pathId = Get-RequiredString -Object $measurement -Name 'pathId' -Context $context
        $method = Get-RequiredString -Object $measurement -Name 'method' -Context $context
        $suite = Get-RequiredString -Object $measurement -Name 'suite' -Context $context
        $mode = Get-RequiredString -Object $measurement -Name 'mode' -Context $context
        if (-not $pathById.ContainsKey($pathId)) {
            throw "$context contains unknown path '$pathId'."
        }
        $path = $pathById[$pathId]
        if ($method -cne [string] $path.method -or $suite -cne [string] $path.suite) {
            throw "$context identity does not match policy path '$pathId'."
        }
        if ($Role -ceq 'reference') {
            if ($mode -cne 'DetachedReference') {
                throw "$context must use mode 'DetachedReference'."
            }
        }
        elseif ($ExpectedModes -cnotcontains $mode) {
            throw "$context contains unknown candidate mode '$mode'."
        }

        $medianNanoseconds = Get-RequiredDouble `
            -Object $measurement `
            -Name 'medianNanoseconds' `
            -Context $context
        $allocatedBytes = Get-RequiredDouble `
            -Object $measurement `
            -Name 'allocatedBytes' `
            -Context $context
        if ($medianNanoseconds -le 0 -or $allocatedBytes -lt 0) {
            throw "$context must contain positive elapsed time and non-negative allocation."
        }
        $key = "$pathId|$mode"
        if ($map.ContainsKey($key)) {
            throw "$context duplicates '$key'."
        }
        $map[$key] = [ordered]@{
            pathId = $pathId
            method = $method
            suite = $suite
            mode = $mode
            medianNanoseconds = $medianNanoseconds
            allocatedBytes = $allocatedBytes
        }
    }

    foreach ($path in $Paths) {
        $modesToCheck = if ($Role -ceq 'reference') {
            @('DetachedReference')
        }
        else {
            $ExpectedModes
        }
        foreach ($mode in $modesToCheck) {
            $key = "$($path.id)|$mode"
            if (-not $map.ContainsKey($key)) {
                throw "Pair $($Run.pairNumber) $Role run is missing '$key'."
            }
        }
    }
    return $map
}

try {
    if (-not (Test-Path -LiteralPath $ThresholdsPath -PathType Leaf)) {
        throw "Observability threshold policy not found: $ThresholdsPath"
    }
    if (-not (Test-Path -LiteralPath $EvidencePath -PathType Leaf)) {
        throw "Observability evidence manifest not found: $EvidencePath"
    }

    $policySha256 = Get-Sha256 -Path $ThresholdsPath
    $evidenceSha256 = Get-Sha256 -Path $EvidencePath
    $policy = Get-Content -LiteralPath $ThresholdsPath -Raw | ConvertFrom-Json -Depth 100
    $evidence = Get-Content -LiteralPath $EvidencePath -Raw | ConvertFrom-Json -Depth 100
    $evidenceRoot = Split-Path -Parent $EvidencePath

    if ([int](Get-RequiredProperty -Object $policy -Name 'schemaVersion' -Context 'policy') -ne 1) {
        throw 'The observability threshold policy schemaVersion must be 1.'
    }
    $policyId = Get-RequiredString -Object $policy -Name 'policyId' -Context 'policy'
    $referencePolicy = Get-RequiredProperty `
        -Object $policy `
        -Name 'reference' `
        -Context 'policy'
    if ((Get-RequiredString `
            -Object $referencePolicy `
            -Name 'status' `
            -Context 'policy reference') -cne 'approved') {
        throw 'The detached reference must be explicitly approved before qualification.'
    }
    $approvedReferenceCommit = (Get-RequiredString `
            -Object $referencePolicy `
            -Name 'commit' `
            -Context 'policy reference').ToLowerInvariant()
    if ($approvedReferenceCommit -cnotmatch '^[0-9a-f]{40}$') {
        throw 'The approved detached reference must be a full lowercase commit id.'
    }
    $qualification = Get-RequiredProperty `
        -Object $policy `
        -Name 'qualification' `
        -Context 'policy'
    Assert-ExactNumber -Object $qualification -Name 'requiredPairCount' -Expected 3 -Context 'policy qualification'
    Assert-ExactNumber -Object $qualification -Name 'maximumLaunchSpreadPercent' -Expected 5 -Context 'policy qualification'
    Assert-ExactNumber -Object $qualification -Name 'warmupCount' -Expected 3 -Context 'policy qualification'
    Assert-ExactNumber -Object $qualification -Name 'iterationCount' -Expected 10 -Context 'policy qualification'
    Assert-ExactNumber -Object $qualification -Name 'benchmarkLaunchCount' -Expected 1 -Context 'policy qualification'
    $maximumPairGapSeconds = Get-RequiredDouble `
        -Object $qualification `
        -Name 'maximumReferenceToCandidateGapSeconds' `
        -Context 'policy qualification'
    if ($maximumPairGapSeconds -le 0 -or $maximumPairGapSeconds -gt 300) {
        throw 'The reference-to-candidate gap must be greater than zero and no more than 300 seconds.'
    }
    if ((Get-RequiredString `
            -Object $qualification `
            -Name 'referenceMode' `
            -Context 'policy qualification') -cne 'DetachedReference') {
        throw "The policy referenceMode must be 'DetachedReference'."
    }
    [string[]] $expectedModes = @(
        Get-RequiredProperty `
            -Object $qualification `
            -Name 'candidateModeOrder' `
            -Context 'policy qualification' |
            ForEach-Object { [string] $_ })
    [string[]] $contractModes = @(
        'Disabled',
        'HistoryCapture',
        'StructuredLogging',
        'MetricsOnly',
        'SampledTracing')
    if (-not (Test-SequenceEqual -Actual $expectedModes -Expected $contractModes)) {
        throw 'The policy candidateModeOrder does not match the benchmark contract.'
    }

    $modes = Get-RequiredProperty -Object $policy -Name 'modes' -Context 'policy'
    foreach ($modeName in $contractModes) {
        if ($null -eq $modes.PSObject.Properties[$modeName]) {
            throw "The policy is missing mode '$modeName'."
        }
    }
    $disabledPolicy = $modes.Disabled
    $historyPolicy = $modes.HistoryCapture
    $loggingPolicy = $modes.StructuredLogging
    $metricsPolicy = $modes.MetricsOnly
    $tracingPolicy = $modes.SampledTracing
    if ([string] $disabledPolicy.status -cne 'approved' -or
        [string] $disabledPolicy.comparison -cne 'detachedReference' -or
        [string] $disabledPolicy.elapsedAllowance -cne 'relative') {
        throw 'The Disabled policy must remain an approved detached-reference relative gate.'
    }
    Assert-ExactNumber -Object $disabledPolicy -Name 'maxElapsedPercent' -Expected 3 -Context 'Disabled policy'
    Assert-ExactNumber -Object $disabledPolicy -Name 'maxAdditionalAllocatedBytes' -Expected 0 -Context 'Disabled policy'
    if (-not (Test-SequenceEqual -Actual @($disabledPolicy.appliesToSuites) -Expected @('engine', 'pool'))) {
        throw 'The Disabled policy must apply independently to engine and pool paths.'
    }
    if ([string] $historyPolicy.status -cne 'approved' -or
        [string] $historyPolicy.comparison -cne 'sameLaunchDisabled' -or
        [string] $historyPolicy.elapsedAllowance -cne 'maximumOfRelativeAndFixed') {
        throw 'The HistoryCapture policy does not match the documented gate.'
    }
    Assert-ExactNumber -Object $historyPolicy -Name 'maxElapsedPercent' -Expected 20 -Context 'HistoryCapture policy'
    Assert-ExactNumber -Object $historyPolicy -Name 'maxElapsedNanoseconds' -Expected 1500 -Context 'HistoryCapture policy'
    Assert-ExactNumber -Object $historyPolicy -Name 'maxAdditionalAllocatedBytes' -Expected 1024 -Context 'HistoryCapture policy'
    if (-not (Test-SequenceEqual -Actual @($historyPolicy.appliesToSuites) -Expected @('engine'))) {
        throw 'HistoryCapture must gate engine paths and characterize the pool separately.'
    }
    if ([string] $loggingPolicy.status -cne 'characterization' -or
        @($loggingPolicy.appliesToSuites).Count -ne 0) {
        throw 'StructuredLogging must remain characterization-only without an approved ceiling.'
    }
    if ([string] $metricsPolicy.status -cne 'approved' -or
        [string] $metricsPolicy.comparison -cne 'sameLaunchDisabled' -or
        [string] $metricsPolicy.elapsedAllowance -cne 'relative') {
        throw 'The MetricsOnly policy does not match the documented gate.'
    }
    Assert-ExactNumber -Object $metricsPolicy -Name 'maxElapsedPercent' -Expected 10 -Context 'MetricsOnly policy'
    Assert-ExactNumber -Object $metricsPolicy -Name 'maxAdditionalAllocatedBytes' -Expected 64 -Context 'MetricsOnly policy'
    if (-not (Test-SequenceEqual -Actual @($metricsPolicy.appliesToSuites) -Expected @('engine'))) {
        throw 'MetricsOnly must gate engine paths and characterize the pool separately.'
    }
    $metricsConfigurationStatus = Get-RequiredString `
        -Object $metricsPolicy `
        -Name 'configurationStatus' `
        -Context 'MetricsOnly policy'
    if ($metricsConfigurationStatus -cnotin @('confounded', 'resolved') -or
        -not (Test-SequenceEqual `
            -Actual @($metricsPolicy.measurementComposition) `
            -Expected @('metricsRuntime', 'metricsListener'))) {
        throw 'MetricsOnly must explicitly declare its metrics-runtime plus metrics-listener composition.'
    }
    if ($metricsConfigurationStatus -ceq 'confounded') {
        $configurationBlockedModes.Add('MetricsOnly')
        Add-Finding `
            -Code 'confoundedModeConfiguration' `
            -Mode 'MetricsOnly' `
            -Message (
                'MetricsOnly currently enables default bounded runtime history through the ' +
                'master observability switch. Its unchanged +10%/+64 B comparison measures ' +
                'history plus metrics, not pure metrics, and cannot establish a formal mode pass.')
    }
    $tracingStatus = [string] $tracingPolicy.status
    if ($tracingStatus -cnotin @('decisionRequired', 'approved')) {
        throw "SampledTracing status must be 'decisionRequired' or 'approved'."
    }
    if ([string] $tracingPolicy.comparison -cne 'sameLaunchDisabled' -or
        -not (Test-SequenceEqual -Actual @($tracingPolicy.appliesToSuites) -Expected @('engine'))) {
        throw 'SampledTracing must compare engine paths with same-launch Disabled and characterize the pool separately.'
    }
    $tracingConfigurationStatus = Get-RequiredString `
        -Object $tracingPolicy `
        -Name 'configurationStatus' `
        -Context 'SampledTracing policy'
    if ($tracingConfigurationStatus -cnotin @('confounded', 'resolved') -or
        -not (Test-SequenceEqual `
            -Actual @($tracingPolicy.measurementComposition) `
            -Expected @('metricsRuntime', 'sampledTracingListener'))) {
        throw 'SampledTracing must explicitly declare its metrics-runtime plus tracing-listener composition.'
    }
    if ($tracingConfigurationStatus -ceq 'confounded') {
        $configurationBlockedModes.Add('SampledTracing')
        Add-Finding `
            -Code 'confoundedModeConfiguration' `
            -Mode 'SampledTracing' `
            -Message (
                'SampledTracing currently enables default bounded runtime history through the ' +
                'master observability switch. Its rows measure history plus tracing, not pure tracing.')
    }
    if ($tracingStatus -ceq 'approved') {
        $tracingAllowance = Get-RequiredString `
            -Object $tracingPolicy `
            -Name 'elapsedAllowance' `
            -Context 'SampledTracing policy'
        if ($tracingAllowance -cnotin @('relative', 'maximumOfRelativeAndFixed')) {
            throw 'An approved SampledTracing policy must define a supported elapsed allowance.'
        }
        $tracingElapsedPercent = Get-RequiredDouble `
            -Object $tracingPolicy `
            -Name 'maxElapsedPercent' `
            -Context 'SampledTracing policy'
        $tracingAllocatedBytes = Get-RequiredDouble `
            -Object $tracingPolicy `
            -Name 'maxAdditionalAllocatedBytes' `
            -Context 'SampledTracing policy'
        if ($tracingElapsedPercent -lt 0 -or $tracingAllocatedBytes -lt 0) {
            throw 'Approved SampledTracing ceilings must be non-negative.'
        }
        if ($tracingAllowance -ceq 'maximumOfRelativeAndFixed') {
            $tracingFixedNanoseconds = Get-RequiredDouble `
                -Object $tracingPolicy `
                -Name 'maxElapsedNanoseconds' `
                -Context 'SampledTracing policy'
            if ($tracingFixedNanoseconds -lt 0) {
                throw 'Approved SampledTracing fixed elapsed ceiling must be non-negative.'
            }
        }
    }
    else {
        $decisionRequiredModes.Add('SampledTracing')
        Add-Finding `
            -Code 'thresholdDecisionRequired' `
            -Mode 'SampledTracing' `
            -Message 'SampledTracing has no approved elapsed/allocation ceiling; the formal gate fails closed.'
    }

    $paths = @(Get-RequiredProperty -Object $policy -Name 'paths' -Context 'policy')
    if ($paths.Count -ne 7) {
        throw "The observability policy must contain exactly seven paths; found $($paths.Count)."
    }
    $pathIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $methods = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $referenceMethods = [Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal)
    $enginePathCount = 0
    $poolPathCount = 0
    foreach ($path in $paths) {
        $pathId = Get-RequiredString -Object $path -Name 'id' -Context 'policy path'
        $method = Get-RequiredString -Object $path -Name 'method' -Context "policy path '$pathId'"
        $referenceMethod = Get-RequiredString `
            -Object $path `
            -Name 'referenceMethod' `
            -Context "policy path '$pathId'"
        $suite = Get-RequiredString -Object $path -Name 'suite' -Context "policy path '$pathId'"
        if (-not $pathIds.Add($pathId) -or
            -not $methods.Add($method) -or
            -not $referenceMethods.Add($referenceMethod)) {
            throw (
                'Policy path ids, candidate methods, and reference methods must be unique; ' +
                "duplicate at '$pathId'.")
        }
        if ($suite -ceq 'engine') {
            $enginePathCount++
            Assert-ExactNumber -Object $path -Name 'logicalQueriesPerOperation' -Expected 1 -Context "policy path '$pathId'"
        }
        elseif ($suite -ceq 'pool') {
            $poolPathCount++
            Assert-ExactNumber -Object $path -Name 'logicalQueriesPerOperation' -Expected 0 -Context "policy path '$pathId'"
        }
        else {
            throw "Policy path '$pathId' has unsupported suite '$suite'."
        }
    }
    if ($enginePathCount -ne 6 -or $poolPathCount -ne 1) {
        throw 'The policy must contain six engine paths and one separate pool characterization path.'
    }

    if ([int](Get-RequiredProperty -Object $evidence -Name 'schemaVersion' -Context 'evidence') -ne 1 -or
        (Get-RequiredString -Object $evidence -Name 'evidenceKind' -Context 'evidence') -cne
            'csharpdb.observability-performance.paired') {
        throw 'The observability evidence contract must be schemaVersion 1 and the paired evidence kind.'
    }
    if ((Get-RequiredString -Object $evidence -Name 'policyId' -Context 'evidence') -cne $policyId) {
        throw 'Evidence policyId does not match the threshold policy.'
    }
    $declaredPolicyHash = (Get-RequiredString `
            -Object $evidence `
            -Name 'policySha256' `
            -Context 'evidence').ToLowerInvariant()
    Assert-Sha256 -Value $declaredPolicyHash -Context 'Evidence policySha256'
    if ($declaredPolicyHash -cne $policySha256) {
        throw 'Evidence policySha256 does not match the supplied threshold policy.'
    }

    $referenceCommit = (Get-RequiredString `
            -Object $evidence `
            -Name 'referenceCommit' `
            -Context 'evidence').ToLowerInvariant()
    $candidateCommit = (Get-RequiredString `
            -Object $evidence `
            -Name 'candidateCommit' `
            -Context 'evidence').ToLowerInvariant()
    if ($referenceCommit -cnotmatch '^[0-9a-f]{40}$' -or
        $candidateCommit -cnotmatch '^[0-9a-f]{40}$') {
        throw 'Evidence commits must be full lowercase 40-character Git object ids.'
    }
    if ($referenceCommit -ceq $candidateCommit) {
        throw 'Detached reference and candidate commits must differ.'
    }
    if ($referenceCommit -cne $approvedReferenceCommit) {
        throw 'Evidence referenceCommit is not the policy-approved detached reference.'
    }
    if ((Get-RequiredString -Object $evidence -Name 'configuration' -Context 'evidence') -cne 'Release') {
        throw "Formal observability evidence must use configuration 'Release'."
    }

    $producer = Get-RequiredProperty -Object $evidence -Name 'producer' -Context 'evidence'
    $runnerHash = (Get-RequiredString -Object $producer -Name 'runnerSha256' -Context 'evidence producer').ToLowerInvariant()
    $comparatorHash = (Get-RequiredString -Object $producer -Name 'comparatorSha256' -Context 'evidence producer').ToLowerInvariant()
    $monitorHash = (Get-RequiredString -Object $producer -Name 'environmentMonitorSha256' -Context 'evidence producer').ToLowerInvariant()
    foreach ($entry in @(
            @{ value = $runnerHash; label = 'runnerSha256' },
            @{ value = $comparatorHash; label = 'comparatorSha256' },
            @{ value = $monitorHash; label = 'environmentMonitorSha256' },
            @{ value = (Get-RequiredString -Object $producer -Name 'referenceBenchmarkSourceSha256' -Context 'evidence producer').ToLowerInvariant(); label = 'referenceBenchmarkSourceSha256' },
            @{ value = (Get-RequiredString -Object $producer -Name 'candidateBenchmarkSourceSha256' -Context 'evidence producer').ToLowerInvariant(); label = 'candidateBenchmarkSourceSha256' })) {
        Assert-Sha256 -Value $entry.value -Context "Evidence producer $($entry.label)"
    }
    $runnerScript = Join-Path $scriptDirectory 'Test-ObservabilityPerformance.ps1'
    $monitorScript = Join-Path $scriptDirectory 'Watch-LocalPerformanceEnvironment.ps1'
    if (-not (Test-Path -LiteralPath $runnerScript -PathType Leaf) -or
        (Get-Sha256 -Path $runnerScript) -cne $runnerHash) {
        throw 'Evidence runnerSha256 does not match the checked-out formal runner.'
    }
    if ((Get-Sha256 -Path $PSCommandPath) -cne $comparatorHash) {
        throw 'Evidence comparatorSha256 does not match this comparator.'
    }
    if (-not (Test-Path -LiteralPath $monitorScript -PathType Leaf) -or
        (Get-Sha256 -Path $monitorScript) -cne $monitorHash) {
        throw 'Evidence environmentMonitorSha256 does not match the checked-out monitor.'
    }

    $environment = Get-RequiredProperty -Object $evidence -Name 'environment' -Context 'evidence'
    if ((Get-RequiredString -Object $environment -Name 'status' -Context 'evidence environment') -cne 'PASS') {
        throw 'Formal evidence requires a PASS environment-monitor closeout.'
    }
    Assert-ArtifactSet `
        -Artifacts @(Get-RequiredProperty -Object $environment -Name 'artifacts' -Context 'evidence environment') `
        -Context 'evidence environment' `
        -Root $evidenceRoot

    $runs = @(Get-RequiredProperty -Object $evidence -Name 'runs' -Context 'evidence')
    $requiredPairCount = [int] $qualification.requiredPairCount
    if ($runs.Count -ne ($requiredPairCount * 2)) {
        throw "Evidence must contain exactly $($requiredPairCount * 2) runs."
    }
    $pairData = [Collections.Generic.List[object]]::new()
    $canonicalFingerprint = ''
    for ($pairNumber = 1; $pairNumber -le $requiredPairCount; $pairNumber++) {
        $pairRuns = @($runs | Where-Object { [int] $_.pairNumber -eq $pairNumber })
        $referenceRuns = @($pairRuns | Where-Object { [string] $_.role -ceq 'reference' })
        $candidateRuns = @($pairRuns | Where-Object { [string] $_.role -ceq 'candidate' })
        if ($pairRuns.Count -ne 2 -or $referenceRuns.Count -ne 1 -or $candidateRuns.Count -ne 1) {
            throw "Pair $pairNumber must contain exactly one reference and one candidate run."
        }
        $referenceRun = $referenceRuns[0]
        $candidateRun = $candidateRuns[0]
        foreach ($runEntry in @(
                @{ run = $referenceRun; role = 'reference'; commit = $referenceCommit },
                @{ run = $candidateRun; role = 'candidate'; commit = $candidateCommit })) {
            $run = $runEntry.run
            $role = $runEntry.role
            $context = "pair $pairNumber $role run"
            if ((Get-RequiredString -Object $run -Name 'commit' -Context $context).ToLowerInvariant() -cne $runEntry.commit) {
                throw "$context commit does not match the evidence header."
            }
            if ((Get-RequiredString -Object $run -Name 'configuration' -Context $context) -cne 'Release') {
                throw "$context must use Release configuration."
            }
            $benchmark = Get-RequiredProperty -Object $run -Name 'benchmark' -Context $context
            Assert-ExactNumber -Object $benchmark -Name 'warmupCount' -Expected 3 -Context "$context benchmark"
            Assert-ExactNumber -Object $benchmark -Name 'iterationCount' -Expected 10 -Context "$context benchmark"
            Assert-ExactNumber -Object $benchmark -Name 'launchCount' -Expected 1 -Context "$context benchmark"
            $fingerprint = Get-FingerprintKey `
                -Machine (Get-RequiredProperty -Object $run -Name 'machine' -Context $context) `
                -Context "$context machine"
            if ([string]::IsNullOrEmpty($canonicalFingerprint)) {
                $canonicalFingerprint = $fingerprint
            }
            elseif ($fingerprint -cne $canonicalFingerprint) {
                throw "$context machine/runtime/power fingerprint differs from another run."
            }
            Assert-ArtifactSet `
                -Artifacts @(Get-RequiredProperty -Object $run -Name 'artifacts' -Context $context) `
                -Context $context `
                -Root $evidenceRoot
        }

        $referenceStarted = ConvertTo-UtcTimestamp `
            -Value (Get-RequiredString -Object $referenceRun -Name 'startedUtc' -Context "pair $pairNumber reference run") `
            -Context "pair $pairNumber reference startedUtc"
        $referenceCompleted = ConvertTo-UtcTimestamp `
            -Value (Get-RequiredString -Object $referenceRun -Name 'completedUtc' -Context "pair $pairNumber reference run") `
            -Context "pair $pairNumber reference completedUtc"
        $candidateStarted = ConvertTo-UtcTimestamp `
            -Value (Get-RequiredString -Object $candidateRun -Name 'startedUtc' -Context "pair $pairNumber candidate run") `
            -Context "pair $pairNumber candidate startedUtc"
        $candidateCompleted = ConvertTo-UtcTimestamp `
            -Value (Get-RequiredString -Object $candidateRun -Name 'completedUtc' -Context "pair $pairNumber candidate run") `
            -Context "pair $pairNumber candidate completedUtc"
        if ($referenceCompleted -le $referenceStarted -or
            $candidateCompleted -le $candidateStarted -or
            $candidateStarted -lt $referenceCompleted) {
            throw "Pair $pairNumber does not preserve reference-before-candidate ordering."
        }
        $pairGapSeconds = ($candidateStarted - $referenceCompleted).TotalSeconds
        if ($pairGapSeconds -gt $maximumPairGapSeconds) {
            throw (
                "Pair $pairNumber reference-to-candidate gap is " +
                "$pairGapSeconds seconds; maximum is $maximumPairGapSeconds.")
        }

        $pairData.Add([ordered]@{
                pairNumber = $pairNumber
                reference = Get-MeasurementMap `
                    -Run $referenceRun `
                    -ExpectedModes $expectedModes `
                    -Paths $paths `
                    -Role 'reference'
                candidate = Get-MeasurementMap `
                    -Run $candidateRun `
                    -ExpectedModes $expectedModes `
                    -Paths $paths `
                    -Role 'candidate'
            })
    }

    $maximumSpread = [double] $qualification.maximumLaunchSpreadPercent
    foreach ($path in $paths) {
        $pathId = [string] $path.id
        foreach ($seriesDefinition in @(
                [ordered]@{ role = 'reference'; mode = 'DetachedReference'; gate = $true },
                [ordered]@{ role = 'candidate'; mode = 'Disabled'; gate = $true })) {
            [double[]] $values = @($pairData | ForEach-Object {
                    [double] $_.($seriesDefinition.role)["$pathId|$($seriesDefinition.mode)"].medianNanoseconds
                })
            $spread = Get-SpreadPercent -Values $values
            $seriesStatus = if ($spread -le ($maximumSpread + 0.000000001)) { 'PASS' } else { 'FAIL' }
            $seriesResults.Add([ordered]@{
                    pathId = $pathId
                    suite = [string] $path.suite
                    role = $seriesDefinition.role
                    mode = $seriesDefinition.mode
                    medianNanoseconds = Get-Median -Values $values
                    spreadPercent = $spread
                    maximumSpreadPercent = $maximumSpread
                    gate = $true
                    status = $seriesStatus
                })
            if ($seriesStatus -ceq 'FAIL') {
                Add-Finding `
                    -Code 'unstableLaunchSeries' `
                    -Mode $seriesDefinition.mode `
                    -PathId $pathId `
                    -Message "Launch spread $spread% exceeds the 5% stability ceiling."
            }
        }

        foreach ($modeName in $expectedModes | Where-Object { $_ -cne 'Disabled' }) {
            $modePolicy = $modes.PSObject.Properties[$modeName].Value
            $applies = @($modePolicy.appliesToSuites) -ccontains [string] $path.suite
            $seriesIsGate = ([string] $modePolicy.status -ceq 'approved') -and $applies
            [double[]] $values = @($pairData | ForEach-Object {
                    [double] $_.candidate["$pathId|$modeName"].medianNanoseconds
                })
            $spread = Get-SpreadPercent -Values $values
            $seriesStatus = if (-not $seriesIsGate) {
                'CHARACTERIZATION'
            }
            elseif ($spread -le ($maximumSpread + 0.000000001)) {
                'PASS'
            }
            else {
                'FAIL'
            }
            $seriesResults.Add([ordered]@{
                    pathId = $pathId
                    suite = [string] $path.suite
                    role = 'candidate'
                    mode = $modeName
                    medianNanoseconds = Get-Median -Values $values
                    spreadPercent = $spread
                    maximumSpreadPercent = $maximumSpread
                    gate = $seriesIsGate
                    status = $seriesStatus
                })
            if ($seriesStatus -ceq 'FAIL') {
                Add-Finding `
                    -Code 'unstableLaunchSeries' `
                    -Mode $modeName `
                    -PathId $pathId `
                    -Message "Launch spread $spread% exceeds the 5% stability ceiling."
            }
        }
    }

    foreach ($path in $paths) {
        $pathId = [string] $path.id
        foreach ($modeName in $expectedModes) {
            $modePolicy = $modes.PSObject.Properties[$modeName].Value
            $applies = @($modePolicy.appliesToSuites) -ccontains [string] $path.suite
            $policyStatus = [string] $modePolicy.status
            $isGate = $policyStatus -ceq 'approved' -and $applies
            $isDecisionRequired = $policyStatus -ceq 'decisionRequired' -and $applies
            $configurationStatus = if ($null -ne
                $modePolicy.PSObject.Properties['configurationStatus']) {
                [string] $modePolicy.configurationStatus
            }
            else {
                'resolved'
            }
            $isConfigurationBlocked =
                $configurationStatus -ceq 'confounded' -and $applies
            $measurementComposition = if ($null -ne
                $modePolicy.PSObject.Properties['measurementComposition']) {
                @($modePolicy.measurementComposition) -join '+'
            }
            else {
                $modeName
            }
            $baselineRole = if ($modeName -ceq 'Disabled') {
                'reference'
            }
            else {
                'candidate'
            }
            $baselineMode = if ($modeName -ceq 'Disabled') {
                'DetachedReference'
            }
            else {
                'Disabled'
            }

            [double[]] $baselineTimes = @()
            [double[]] $candidateTimes = @()
            [double[]] $baselineAllocations = @()
            [double[]] $candidateAllocations = @()
            [double[]] $elapsedMargins = @()
            [double[]] $elapsedChanges = @()
            [double[]] $allocationDeltas = @()
            foreach ($pair in $pairData) {
                $baseline = $pair.($baselineRole)["$pathId|$baselineMode"]
                $candidate = $pair.candidate["$pathId|$modeName"]
                $baselineTimes += [double] $baseline.medianNanoseconds
                $candidateTimes += [double] $candidate.medianNanoseconds
                $baselineAllocations += [double] $baseline.allocatedBytes
                $candidateAllocations += [double] $candidate.allocatedBytes
                $elapsedChanges += (
                    ([double] $candidate.medianNanoseconds - [double] $baseline.medianNanoseconds) /
                    [double] $baseline.medianNanoseconds) * 100.0
                $allocationDeltas +=
                    [double] $candidate.allocatedBytes - [double] $baseline.allocatedBytes
                if ($isGate) {
                    $relativeAllowance =
                        [double] $baseline.medianNanoseconds *
                        ([double] $modePolicy.maxElapsedPercent / 100.0)
                    $allowance = if ([string] $modePolicy.elapsedAllowance -ceq
                        'maximumOfRelativeAndFixed') {
                        [Math]::Max(
                            $relativeAllowance,
                            [double] $modePolicy.maxElapsedNanoseconds)
                    }
                    else {
                        $relativeAllowance
                    }
                    $elapsedMargins +=
                        [double] $candidate.medianNanoseconds -
                        [double] $baseline.medianNanoseconds -
                        $allowance
                }
            }

            $medianElapsedMargin = if ($isGate) {
                Get-Median -Values $elapsedMargins
            }
            else {
                $null
            }
            $medianAllocationDelta = Get-Median -Values $allocationDeltas
            $elapsedPass = -not $isGate -or $medianElapsedMargin -le 0.000000001
            $allocationPass = -not $isGate -or
                $medianAllocationDelta -le
                    ([double] $modePolicy.maxAdditionalAllocatedBytes + 0.000000001)
            $comparisonStatus = if ($isDecisionRequired) {
                'DECISION_REQUIRED'
            }
            elseif ($isConfigurationBlocked) {
                'CONFIGURATION_CONFOUNDED'
            }
            elseif (-not $isGate) {
                'CHARACTERIZATION'
            }
            elseif ($elapsedPass -and $allocationPass) {
                'PASS'
            }
            else {
                'FAIL'
            }

            $comparisons.Add([ordered]@{
                    pathId = $pathId
                    method = [string] $path.method
                    suite = [string] $path.suite
                    mode = $modeName
                    measurementComposition = $measurementComposition
                    configurationStatus = $configurationStatus
                    comparison = [string] $modePolicy.comparison
                    gate = $isGate
                    status = $comparisonStatus
                    baselineMedianNanoseconds = Get-Median -Values $baselineTimes
                    candidateMedianNanoseconds = Get-Median -Values $candidateTimes
                    medianPairedElapsedChangePercent = Get-Median -Values $elapsedChanges
                    medianPairedElapsedMarginNanoseconds = $medianElapsedMargin
                    baselineMedianAllocatedBytes = Get-Median -Values $baselineAllocations
                    candidateMedianAllocatedBytes = Get-Median -Values $candidateAllocations
                    medianPairedAllocationDeltaBytes = $medianAllocationDelta
                    maxElapsedPercent = $(if ($isGate) { [double] $modePolicy.maxElapsedPercent } else { $null })
                    maxElapsedNanoseconds = $(if ($isGate -and
                            $null -ne $modePolicy.PSObject.Properties['maxElapsedNanoseconds']) {
                            [double] $modePolicy.maxElapsedNanoseconds
                        }
                        else {
                            $null
                        })
                    maxAdditionalAllocatedBytes = $(if ($isGate) {
                            [double] $modePolicy.maxAdditionalAllocatedBytes
                        }
                        else {
                            $null
                        })
                })

            if ($isGate -and -not $elapsedPass) {
                $compositionLabel = if ($isConfigurationBlocked) {
                    "composite $measurementComposition row"
                }
                else {
                    "$modeName row"
                }
                Add-Finding `
                    -Code 'elapsedThresholdExceeded' `
                    -Mode $modeName `
                    -PathId $pathId `
                    -Message (
                        "The $compositionLabel has a median paired elapsed margin " +
                        "$medianElapsedMargin ns above the unchanged approved allowance.")
            }
            if ($isGate -and -not $allocationPass) {
                $compositionLabel = if ($isConfigurationBlocked) {
                    "composite $measurementComposition row"
                }
                else {
                    "$modeName row"
                }
                Add-Finding `
                    -Code 'allocationThresholdExceeded' `
                    -Mode $modeName `
                    -PathId $pathId `
                    -Message (
                        "The $compositionLabel has a median paired allocation delta of " +
                        "$medianAllocationDelta B; " +
                        "maximum is $($modePolicy.maxAdditionalAllocatedBytes) B.")
            }
        }
    }
    $structurallyValid = $true
}
catch {
    Add-Finding `
        -Code 'invalidEvidence' `
        -Message $_.Exception.Message
}

$verdict = if ($structurallyValid -and $findings.Count -eq 0) {
    'PASS'
}
else {
    'FAIL'
}
$attestation = [ordered]@{
    schemaVersion = 1
    attestationKind = 'csharpdb.observability-performance.qualification'
    generatedUtc = [DateTimeOffset]::UtcNow.ToString('O')
    verdict = $verdict
    formalGateEligible = $verdict -ceq 'PASS'
    policy = [ordered]@{
        id = $policyId
        sha256 = $policySha256
    }
    evidence = [ordered]@{
        manifestPath = [IO.Path]::GetFileName($EvidencePath)
        sha256 = $evidenceSha256
    }
    qualifier = [ordered]@{
        script = [IO.Path]::GetFileName($PSCommandPath)
        sha256 = Get-Sha256 -Path $PSCommandPath
    }
    summary = [ordered]@{
        gateComparisons = @($comparisons | Where-Object { $_.gate }).Count
        failedGateComparisons = @($comparisons | Where-Object {
                $_.gate -and $_.status -cne 'PASS'
            }).Count
        characterizationComparisons = @($comparisons | Where-Object { $_.status -ceq 'CHARACTERIZATION' }).Count
        decisionRequiredModes = @($decisionRequiredModes)
        configurationBlockedModes = @($configurationBlockedModes)
        findingCount = $findings.Count
    }
    findings = @($findings)
    launchSeries = @($seriesResults)
    comparisons = @($comparisons)
    subject = [ordered]@{
        referenceCommit = $referenceCommit
        candidateCommit = $candidateCommit
    }
}

foreach ($outputPath in @($OutputJsonPath, $OutputMarkdownPath)) {
    $parent = Split-Path -Parent $outputPath
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
}
$attestation | ConvertTo-Json -Depth 100 | Set-Content `
    -LiteralPath $OutputJsonPath `
    -Encoding utf8NoBOM

$markdown = [Collections.Generic.List[string]]::new()
$markdown.Add('# Observability performance qualification')
$markdown.Add('')
$markdown.Add("- Verdict: **$verdict**")
$markdown.Add("- Policy: ``$policyId`` (``$policySha256``)")
$markdown.Add("- Evidence SHA-256: ``$evidenceSha256``")
$markdown.Add("- Formal gate eligible: **$($verdict -ceq 'PASS')**")
if ($decisionRequiredModes.Count -gt 0) {
    $markdown.Add("- Decision required: ``$($decisionRequiredModes -join ', ')``")
}
if ($configurationBlockedModes.Count -gt 0) {
    $markdown.Add(
        "- Configuration-confounded modes: ``$($configurationBlockedModes -join ', ')``")
    $markdown.Add(
        '- These rows include default bounded runtime history; they are not pure metrics/tracing measurements.')
}
$markdown.Add('')
$markdown.Add('## Findings')
$markdown.Add('')
if ($findings.Count -eq 0) {
    $markdown.Add('- None.')
}
else {
    foreach ($finding in $findings) {
        $scopeParts = @(
            [string] $finding.mode,
            [string] $finding.pathId) | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string] $_)
        }
        $scope = $scopeParts -join '/'
        if (-not [string]::IsNullOrWhiteSpace($scope)) {
            $scope = " ($scope)"
        }
        $markdown.Add("- ``$($finding.code)``${scope}: $($finding.message)")
    }
}
$markdown.Add('')
$markdown.Add('## Per-path results')
$markdown.Add('')
$markdown.Add('| Path | Mode | Composition | Result | Paired elapsed change | Paired allocation delta |')
$markdown.Add('| --- | --- | --- | ---: | ---: | ---: |')
foreach ($comparison in $comparisons) {
    $elapsedChange = [Math]::Round(
        [double] $comparison.medianPairedElapsedChangePercent,
        6)
    $allocationDelta = [Math]::Round(
        [double] $comparison.medianPairedAllocationDeltaBytes,
        3)
    $markdown.Add(
        "| $($comparison.pathId) | $($comparison.mode) | " +
        "$($comparison.measurementComposition) | $($comparison.status) | " +
        "$elapsedChange% | $allocationDelta B |")
}
$markdown.Add('')
$markdown.Add('Pool non-disabled rows are retained as separate connection-lifecycle characterization; they are not query-operation gate passes.')
$markdown | Set-Content -LiteralPath $OutputMarkdownPath -Encoding utf8NoBOM

Write-Host "Observability performance verdict: $verdict"
Write-Host "Attestation: $OutputJsonPath"
Write-Host "Report: $OutputMarkdownPath"
if ($verdict -cne 'PASS') {
    exit 1
}
