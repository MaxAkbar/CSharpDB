#requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory)][string]$OutputPath, [switch]$NoFailOnRegression)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$OutputPath = (Resolve-Path -LiteralPath $OutputPath).Path
$summaryPath = Join-Path $OutputPath 'report.md'
$reportWritten = $false
@('# Paired performance guardrails', '', 'Status: INCOMPLETE', '', 'Comparison has not completed.') |
    Set-Content -LiteralPath $summaryPath -Encoding utf8
# These are generated reports; never retain an earlier successful pair report
# when a recheck stops before that pair is compared.
foreach ($definition in @(Get-PerfPairSchedule)) {
    $oldReport = Join-Path $OutputPath "pair-$($definition.pair)-report.md"
    if (Test-Path -LiteralPath $oldReport) { Remove-Item -LiteralPath $oldReport }
}
trap {
    if (-not $reportWritten) {
        @('# Paired performance guardrails', '', 'Status: INVALID', '',
            "Generated (UTC): $([DateTime]::UtcNow.ToString('u'))", '', $_.Exception.Message) |
            Set-Content -LiteralPath $summaryPath -Encoding utf8
    }
    throw $_
}
$plan = Get-Content -LiteralPath (Join-Path $OutputPath 'paired.json') -Raw | ConvertFrom-Json
$thresholds = Join-Path $OutputPath 'thresholds.json'
if ($plan.schemaVersion -ne 1 -or $plan.baseCommit -notmatch '^[a-f0-9]{40}$' -or $plan.headCommit -notmatch '^[a-f0-9]{40}$') { throw 'Invalid paired product provenance.' }
if ((Get-FileHash -LiteralPath $thresholds -Algorithm SHA256).Hash -ine $plan.thresholdSha256) { throw 'Paired thresholds hash mismatch.' }
$artifactPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
foreach ($artifact in @($plan.artifacts)) {
    if (-not $artifactPaths.Add($artifact.path)) { throw 'Duplicate paired artifact.' }
    $path = Resolve-PerfArtifactPath $OutputPath $artifact.path
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Paired artifact missing: $($artifact.path)" }
    if ((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -ine $artifact.sha256) { throw "Paired artifact hash mismatch: $($artifact.path)" }
}
foreach ($required in @('thresholds.json', 'execution-order.csv', 'base-build.log', 'head-build.log', 'base-build-inputs.sha256', 'head-build-inputs.sha256')) {
    if (-not $artifactPaths.Contains($required)) { throw "Paired artifact absent from manifest: $required" }
}
$schedule = @(Get-PerfPairSchedule)
if (@($plan.pairs).Count -ne $schedule.Count) { throw 'Both balanced execution orders are required.' }
$expectedExecution = @($schedule | ForEach-Object {
    $definition = $_
    foreach ($role in $definition.roles) { "$($definition.pair),$($definition.order),$role,$($plan."${role}Commit")" }
})
if ((@(Get-Content -LiteralPath (Join-Path $OutputPath 'execution-order.csv')) -join "`n") -cne ($expectedExecution -join "`n")) { throw 'Recorded execution order disagrees with balanced pair plan.' }
$paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$reports = [Collections.Generic.List[string]]::new()
$errors = [Collections.Generic.List[string]]::new()
$workloadIdentity = $null
$runtimeIdentity = $null
foreach ($expected in $schedule) {
    $pair = @($plan.pairs | Where-Object pair -eq $expected.pair)
    if ($pair.Count -ne 1 -or $pair[0].order -cne $expected.order) { throw 'Invalid balanced pair schedule.' }
    $pair = $pair[0]
    $snapshots = @{}
    foreach ($role in @('base', 'head')) {
        if (-not $paths.Add($pair.$role)) { throw 'A snapshot cannot be reused for two paired runs.' }
        $snapshots[$role] = Resolve-PerfArtifactPath $OutputPath $pair.$role
        $manifest = Assert-PerfSnapshot $snapshots[$role]
        if ($manifest.productCommit -cne $plan."${role}Commit" -or $manifest.harnessIdentity -cne $plan.harnessIdentity) { throw "Invalid $role paired provenance." }
        $workload = @($manifest.runs | ForEach-Object { [ordered]@{ label = $_.label; arguments = @($_.arguments) } }) | ConvertTo-Json -Compress -Depth 6
        $runtime = $manifest.runtime | ConvertTo-Json -Compress
        if ($null -eq $workloadIdentity) { $workloadIdentity = $workload; $runtimeIdentity = $runtime }
        elseif ($workload -cne $workloadIdentity -or $runtime -cne $runtimeIdentity) { throw 'Paired runs must use identical workloads and runtime identities in both orders.' }
    }
    $report = Join-Path $OutputPath "pair-$($pair.pair)-report.md"
    try {
        & (Join-Path $scriptDir 'Compare-Baseline.ps1') -BaselineSnapshot $snapshots.base -CurrentSnapshot $snapshots.head -ThresholdsPath $thresholds -ReportPath $report -NoFailOnRegression:$NoFailOnRegression
    }
    catch { $errors.Add("$($pair.order): $($_.Exception.Message)") }
    $reports.Add("## $($pair.order)")
    if (Test-Path -LiteralPath $report) { $reports.Add((Get-Content -LiteralPath $report -Raw)) }
}
$timingPolicy = if ($NoFailOnRegression) { 'report regressions; fail invalid evidence' } else { 'fail regressions and invalid evidence' }
$summary = @('# Paired performance guardrails', '', "Base: $($plan.baseCommit)", "Head: $($plan.headCommit)", "Harness: $($plan.harnessIdentity)", '',
    'Coverage: every configured check must compare every selected row with at least one enforceable comparison in each order.',
    "Threshold policy: $timingPolicy", '', @($reports), '', @($errors))
$summary | Set-Content -LiteralPath $summaryPath -Encoding utf8
$reportWritten = $true
if ($errors.Count -gt 0) { throw "Paired guardrails failed: $($errors -join '; ')" }
