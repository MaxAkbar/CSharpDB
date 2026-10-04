#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$BaseRoot,
    [Parameter(Mandatory)][string]$HeadRoot,
    [Parameter(Mandatory)][string]$OutputPath,
    [ValidateSet('pr', 'release')][string]$Mode = 'release',
    [string]$Configuration = 'Release',
    [string]$ThresholdsPath = '',
    [string[]]$MicroArguments = @(),
    [switch]$NoFailOnRegression
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$BaseRoot = (Resolve-Path -LiteralPath $BaseRoot).Path
$HeadRoot = (Resolve-Path -LiteralPath $HeadRoot).Path
$OutputPath = [IO.Path]::GetFullPath($OutputPath)
if ($BaseRoot -eq $HeadRoot) { throw 'Paired runs require separate base and head checkouts.' }
if (Test-Path -LiteralPath $OutputPath) { throw "Paired output must be new: $OutputPath" }
New-Item -ItemType Directory -Path $OutputPath | Out-Null
$roots = @{ base = $BaseRoot; head = $HeadRoot }
$commits = @{}
foreach ($role in @('base', 'head')) {
    $status = @(& git -C $roots[$role] status --porcelain --untracked-files=all)
    if ($LASTEXITCODE -ne 0 -or $status.Count -gt 0) { throw "Paired $role checkout must be clean before harness synchronization." }
    $commits[$role] = ([string](& git -C $roots[$role] rev-parse HEAD)).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve $role product SHA." }
}
$harnessRoot = [IO.Path]::GetFullPath((Join-Path $scriptDir '..'))
$records = @(Get-PerfHarnessRecords $harnessRoot)
$identity = Get-PerfHarnessIdentity $records
if ([string]::IsNullOrWhiteSpace($ThresholdsPath)) {
    $ThresholdsPath = Join-Path $harnessRoot $(if ($Mode -eq 'pr') { 'perf-thresholds-pr.json' } else { 'perf-thresholds.json' })
}
$ThresholdsPath = (Resolve-Path -LiteralPath $ThresholdsPath).Path
Copy-Item -LiteralPath $ThresholdsPath -Destination (Join-Path $OutputPath 'thresholds.json')

foreach ($role in @('base', 'head')) {
    $target = Join-Path $roots[$role] 'tests/CSharpDB.Benchmarks'
    if ([IO.Path]::GetFullPath($target) -eq $harnessRoot) { throw 'Run the paired driver from a third harness checkout; product checkouts are disposable.' }
    # Synchronize the entire source/configuration closure, removing obsolete files
    # only inside the explicitly supplied disposable benchmark checkout.
    foreach ($old in @(Get-PerfHarnessRecords $target)) {
        Remove-Item -LiteralPath (Resolve-PerfArtifactPath $target $old.path) -Force
    }
    foreach ($record in $records) {
        $destination = Resolve-PerfArtifactPath $target $record.path
        New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
        Copy-Item -LiteralPath (Join-Path $harnessRoot $record.path) -Destination $destination
    }
    if ((Get-PerfHarnessIdentity @(Get-PerfHarnessRecords $target)) -cne $identity) { throw "$role harness synchronization failed." }
    $buildInputs = @(& git -C $roots[$role] ls-files '*.props' '*.targets' 'global.json' '*NuGet.config' '*.csproj' '*packages.lock.json')
    if ($LASTEXITCODE -ne 0) { throw 'Could not enumerate product build inputs.' }
    @($buildInputs | ForEach-Object {
        $path = Join-Path $roots[$role] $_
        "$((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()) *$_"
    }) | Set-Content -LiteralPath (Join-Path $OutputPath "$role-build-inputs.sha256")
    Push-Location $roots[$role]
    try {
        & dotnet build (Join-Path $target 'CSharpDB.Benchmarks.csproj') -c $Configuration 2>&1 |
            Tee-Object -FilePath (Join-Path $OutputPath "$role-build.log")
        if ($LASTEXITCODE -ne 0) { throw "$role benchmark build failed." }
    }
    finally { Pop-Location }
}

$pairs = [Collections.Generic.List[object]]::new()
foreach ($definition in @(Get-PerfPairSchedule)) {
    $snapshots = @{}
    foreach ($role in $definition.roles) {
        $relative = "pair-$($definition.pair)/$role"
        $snapshot = Join-Path $OutputPath $relative
        "$($definition.pair),$($definition.order),$role,$($commits[$role])" | Add-Content -LiteralPath (Join-Path $OutputPath 'execution-order.csv')
        $runner = Join-Path $roots[$role] 'tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1'
        & $runner -Configuration $Configuration -Mode $Mode -ThresholdsPath $ThresholdsPath -SnapshotOutputPath $snapshot -CaptureOnly -NoBuild -MicroArguments $MicroArguments
        $manifest = Assert-PerfSnapshot $snapshot
        if ($manifest.productCommit -cne $commits[$role] -or $manifest.harnessIdentity -cne $identity) { throw 'Executed snapshot provenance disagrees with paired plan.' }
        $snapshots[$role] = $relative
    }
    $pairs.Add([pscustomobject]@{ pair = $definition.pair; order = $definition.order; base = $snapshots.base; head = $snapshots.head })
}
[ordered]@{
    schemaVersion = 1
    baseCommit = $commits.base
    headCommit = $commits.head
    harnessIdentity = $identity
    thresholdSha256 = (Get-FileHash -LiteralPath (Join-Path $OutputPath 'thresholds.json') -Algorithm SHA256).Hash.ToLowerInvariant()
    artifacts = @(@('thresholds.json', 'execution-order.csv', 'base-build.log', 'head-build.log', 'base-build-inputs.sha256', 'head-build-inputs.sha256') | ForEach-Object {
        [ordered]@{ path = $_; sha256 = (Get-FileHash -LiteralPath (Join-Path $OutputPath $_) -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
    pairs = @($pairs)
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputPath 'paired.json')
& (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $OutputPath -NoFailOnRegression:$NoFailOnRegression
