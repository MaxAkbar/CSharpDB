#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $ReferenceProjectPath,

    [Parameter(Mandatory)]
    [string] $CandidateProjectPath,

    [Parameter(Mandatory)]
    [string] $OutputPath,

    [string] $ThresholdsPath = '',

    [switch] $ConfirmDedicatedPerformanceRunner
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$configuration = 'Release'
$requiredPairCount = 3
$quietGateSeconds = 300
$postBuildQuiescenceSeconds = 30
$monitorSampleIntervalMilliseconds = 1000
$maximumExternalCpuPercent = 8.0
$maximumExternalCpuCoreEquivalent = 0.5
$maximumExternalIoBytesPerSecond = 4194304L
$requiredConsecutiveBusySamples = 5
$prohibitedExternalProcessNames =
    'devenv;msbuild;vbcscompiler;testhost;vstest.console;msiexec;' +
    'trustedinstaller;tiworker;mousocoreworker;usoclient;winget;nuget'

if (-not $IsWindows) {
    throw 'Formal observability performance qualification requires the canonical dedicated Windows runner.'
}
if (-not $ConfirmDedicatedPerformanceRunner) {
    throw (
        'Rerun with -ConfirmDedicatedPerformanceRunner only after confirming that this is ' +
        'the dedicated fixed-power CSharpDB performance runner.')
}
$runnerId = [string] $env:CSHARPDB_PERF_RUNNER_ID
if ([string]::IsNullOrWhiteSpace($runnerId)) {
    throw 'CSHARPDB_PERF_RUNNER_ID must identify the dedicated performance runner.'
}

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchmarkDirectory = [IO.Path]::GetFullPath(
    [IO.Path]::Combine($scriptDirectory, '..'))
if ([string]::IsNullOrWhiteSpace($ThresholdsPath)) {
    $ThresholdsPath = Join-Path `
        $benchmarkDirectory `
        'observability-perf-thresholds.json'
}
$ThresholdsPath = [IO.Path]::GetFullPath($ThresholdsPath)
$ReferenceProjectPath = [IO.Path]::GetFullPath($ReferenceProjectPath)
$CandidateProjectPath = [IO.Path]::GetFullPath($CandidateProjectPath)
$OutputPath = [IO.Path]::GetFullPath($OutputPath)
$comparatorScript = Join-Path $scriptDirectory 'Compare-ObservabilityPerformance.ps1'
$monitorScript = Join-Path $scriptDirectory 'Watch-LocalPerformanceEnvironment.ps1'

foreach ($requiredFile in @(
        $ThresholdsPath,
        $ReferenceProjectPath,
        $CandidateProjectPath,
        $comparatorScript,
        $monitorScript)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Required qualification input not found: $requiredFile"
    }
}
if (Test-Path -LiteralPath $OutputPath) {
    throw "The immutable evidence output already exists: $OutputPath"
}
New-Item -ItemType Directory -Path $OutputPath | Out-Null
$rawRoot = Join-Path $OutputPath 'raw'
$setupRoot = Join-Path $OutputPath 'setup'
$environmentRoot = Join-Path $OutputPath 'environment'
New-Item -ItemType Directory -Path $rawRoot, $setupRoot, $environmentRoot | Out-Null

$policy = Get-Content -LiteralPath $ThresholdsPath -Raw | ConvertFrom-Json -Depth 100
if ([int] $policy.schemaVersion -ne 1 -or
    [int] $policy.qualification.requiredPairCount -ne $requiredPairCount -or
    [double] $policy.qualification.maximumLaunchSpreadPercent -ne 5.0 -or
    [int] $policy.qualification.warmupCount -ne 3 -or
    [int] $policy.qualification.iterationCount -ne 10 -or
    [int] $policy.qualification.benchmarkLaunchCount -ne 1) {
    throw 'The supplied policy is not the canonical three-pair 3/10 observability qualification policy.'
}
if ([string] $policy.reference.status -cne 'approved' -or
    [string] $policy.reference.commit -cnotmatch '^[0-9a-f]{40}$') {
    throw 'The supplied policy must approve one full immutable detached-reference commit.'
}
$approvedReferenceCommit = ([string] $policy.reference.commit).ToLowerInvariant()

function Get-Sha256 {
    param([Parameter(Mandatory)][string] $Path)

    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Invoke-GitText {
    param(
        [Parameter(Mandatory)][string] $WorkingDirectory,
        [Parameter(Mandatory)][string[]] $Arguments,
        [Parameter(Mandatory)][string] $FailureMessage
    )

    $output = @(& git -C $WorkingDirectory @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "$FailureMessage $($output -join [Environment]::NewLine)"
    }
    return (($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine).Trim()
}

function Get-RepositoryInformation {
    param(
        [Parameter(Mandatory)][string] $ProjectPath,
        [Parameter(Mandatory)][string] $Role
    )

    $projectDirectory = Split-Path -Parent $ProjectPath
    $root = Invoke-GitText `
        -WorkingDirectory $projectDirectory `
        -Arguments @('rev-parse', '--show-toplevel') `
        -FailureMessage "Could not resolve the $Role repository."
    $commit = (Invoke-GitText `
            -WorkingDirectory $root `
            -Arguments @('rev-parse', 'HEAD') `
            -FailureMessage "Could not resolve the $Role commit.").ToLowerInvariant()
    if ($commit -cnotmatch '^[0-9a-f]{40}$') {
        throw "The $Role checkout did not resolve to a full commit id."
    }
    $status = Invoke-GitText `
        -WorkingDirectory $root `
        -Arguments @('status', '--porcelain=v1', '--untracked-files=no') `
        -FailureMessage "Could not inspect the $Role worktree."
    if (-not [string]::IsNullOrWhiteSpace($status)) {
        throw "The $Role tracked worktree must be clean before formal qualification."
    }

    $sourcePath = Join-Path `
        (Split-Path -Parent $ProjectPath) `
        'Micro/ObservabilityNoListenerBaselineBenchmarks.cs'
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        throw "The $Role checkout does not contain the normalized observability benchmark source."
    }

    return [ordered]@{
        root = [IO.Path]::GetFullPath($root)
        commit = $commit
        sourcePath = [IO.Path]::GetFullPath($sourcePath)
        sourceSha256 = Get-Sha256 -Path $sourcePath
    }
}

function Get-ProcessorName {
    try {
        $name = [string](Get-CimInstance Win32_Processor |
            Select-Object -First 1 -ExpandProperty Name)
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            return $name.Trim()
        }
    }
    catch {
    }
    return ([string] $env:PROCESSOR_IDENTIFIER).Trim()
}

function Get-PowerProfile {
    $output = @(& powercfg /getactivescheme 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "Could not capture the active Windows power profile. $($output -join ' ')"
    }
    $profile = (($output | ForEach-Object { [string] $_ }) -join ' ').Trim()
    if ([string]::IsNullOrWhiteSpace($profile)) {
        throw 'The active Windows power profile is empty.'
    }
    return $profile
}

function Get-MachineFingerprint {
    $sdk = [string]((& dotnet --version) | Select-Object -First 1)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($sdk)) {
        throw 'Could not capture the .NET SDK version.'
    }
    $runtimes = @(& dotnet --list-runtimes 2>&1)
    if ($LASTEXITCODE -ne 0 -or $runtimes.Count -eq 0) {
        throw 'Could not capture installed .NET runtimes.'
    }

    return [ordered]@{
        runnerId = $runnerId
        machineName = [Environment]::MachineName
        cpuName = Get-ProcessorName
        logicalCoreCount = [Environment]::ProcessorCount.ToString(
            [Globalization.CultureInfo]::InvariantCulture)
        osDescription = [Runtime.InteropServices.RuntimeInformation]::OSDescription.Trim()
        osArchitecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        dotnetSdk = $sdk.Trim()
        dotnetRuntime = (($runtimes | ForEach-Object { ([string] $_).Trim() }) -join '; ')
        powerProfile = Get-PowerProfile
    }
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory)][string] $Label,
        [Parameter(Mandatory)][string] $LogPath,
        [Parameter(Mandatory)][scriptblock] $Command
    )

    Write-Host "=== $Label ==="
    & $Command 2>&1 | Tee-Object -FilePath $LogPath | Write-Host
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "$Label failed with exit code $exitCode. See $LogPath"
    }
}

function Convert-BenchmarkDurationToNanoseconds {
    param(
        [Parameter(Mandatory)][string] $Value,
        [Parameter(Mandatory)][string] $Context
    )

    $normalized = $Value.Trim().Replace(',', '').Replace([char] 0x03BC, [char] 0x00B5)
    $match = [regex]::Match(
        $normalized,
        '^(?<number>[0-9]+(?:\.[0-9]+)?)\s*(?<unit>ps|ns|us|µs|ms|s)$',
        [Text.RegularExpressions.RegexOptions]::CultureInvariant)
    if (-not $match.Success) {
        throw "$Context has unsupported BenchmarkDotNet duration '$Value'."
    }
    $number = [double]::Parse(
        $match.Groups['number'].Value,
        [Globalization.CultureInfo]::InvariantCulture)
    $factor = switch ($match.Groups['unit'].Value) {
        'ps' { 0.001 }
        'ns' { 1.0 }
        'us' { 1000.0 }
        'µs' { 1000.0 }
        'ms' { 1000000.0 }
        's' { 1000000000.0 }
        default { throw "$Context has an unsupported duration unit." }
    }
    return $number * $factor
}

function Convert-BenchmarkAllocationToBytes {
    param(
        [Parameter(Mandatory)][string] $Value,
        [Parameter(Mandatory)][string] $Context
    )

    $normalized = $Value.Trim().Replace(',', '')
    $match = [regex]::Match(
        $normalized,
        '^(?<number>[0-9]+(?:\.[0-9]+)?)\s*(?<unit>B|KB|MB)$',
        [Text.RegularExpressions.RegexOptions]::CultureInvariant)
    if (-not $match.Success) {
        throw "$Context has unsupported BenchmarkDotNet allocation '$Value'."
    }
    $number = [double]::Parse(
        $match.Groups['number'].Value,
        [Globalization.CultureInfo]::InvariantCulture)
    $factor = switch ($match.Groups['unit'].Value) {
        'B' { 1.0 }
        'KB' { 1024.0 }
        'MB' { 1048576.0 }
        default { throw "$Context has an unsupported allocation unit." }
    }
    return $number * $factor
}

function Assert-BenchmarkJobShape {
    param(
        [Parameter(Mandatory)][object] $Row,
        [Parameter(Mandatory)][string] $Context
    )

    if ([string] $Row.WarmupCount -cne '3' -or
        [string] $Row.IterationCount -cne '10' -or
        [string] $Row.LaunchCount -cnotin @('Default', '1')) {
        throw "$Context is not the required one-launch, 3-warmup, 10-iteration job."
    }
}

function Get-NormalizedMeasurements {
    param(
        [Parameter(Mandatory)][string] $ArtifactRoot,
        [Parameter(Mandatory)][ValidateSet('reference', 'candidate')][string] $Role
    )

    $resultRoot = Join-Path $ArtifactRoot 'results'
    $csvFiles = @(Get-ChildItem `
            -LiteralPath $resultRoot `
            -File `
            -Filter '*ObservabilityNoListener*report.csv')
    if ($csvFiles.Count -ne 2) {
        throw "$Role run must emit exactly two observability CSV reports; found $($csvFiles.Count)."
    }
    $rows = @($csvFiles | ForEach-Object { Import-Csv -LiteralPath $_.FullName })
    $pathByMethod = @{}
    foreach ($path in @($policy.paths)) {
        $sourceMethod = if ($Role -ceq 'reference') {
            [string] $path.referenceMethod
        }
        else {
            [string] $path.method
        }
        if ([string]::IsNullOrWhiteSpace($sourceMethod) -or
            $pathByMethod.ContainsKey($sourceMethod)) {
            throw "$Role method identities in the policy must be non-empty and unique."
        }
        $pathByMethod[$sourceMethod] = $path
    }
    $recognizedRows = @($rows | Where-Object {
            $pathByMethod.ContainsKey(([string] $_.Method).Trim("'"))
        })

    if ($Role -ceq 'reference') {
        $disabledRows = @($recognizedRows | Where-Object {
                $null -ne $_.PSObject.Properties['Mode'] -and
                [string] $_.Mode -ceq 'Disabled'
            })
        if ($disabledRows.Count -gt 0) {
            $recognizedRows = $disabledRows
        }
        if ($recognizedRows.Count -ne @($policy.paths).Count) {
            throw (
                'The reference run must normalize to exactly one detached/no-listener row ' +
                "for each path; found $($recognizedRows.Count).")
        }
    }
    else {
        $expectedCount =
            @($policy.paths).Count *
            @($policy.qualification.candidateModeOrder).Count
        if ($recognizedRows.Count -ne $expectedCount) {
            throw "The candidate run must contain exactly $expectedCount rows; found $($recognizedRows.Count)."
        }
    }

    $measurements = [Collections.Generic.List[object]]::new()
    $keys = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in $recognizedRows) {
        $method = ([string] $row.Method).Trim("'")
        $path = $pathByMethod[$method]
        $mode = if ($Role -ceq 'reference') {
            'DetachedReference'
        }
        else {
            [string] $row.Mode
        }
        if ($Role -ceq 'candidate' -and
            @($policy.qualification.candidateModeOrder) -cnotcontains $mode) {
            throw "Candidate row '$method' has unexpected mode '$mode'."
        }
        $key = "$($path.id)|$mode"
        if (-not $keys.Add($key)) {
            throw "$Role run contains duplicate normalized row '$key'."
        }
        Assert-BenchmarkJobShape -Row $row -Context "$role row '$key'"
        $measurements.Add([ordered]@{
                pathId = [string] $path.id
                method = [string] $path.method
                suite = [string] $path.suite
                mode = $mode
                medianNanoseconds = Convert-BenchmarkDurationToNanoseconds `
                    -Value ([string] $row.Median) `
                    -Context "$role row '$key'"
                allocatedBytes = Convert-BenchmarkAllocationToBytes `
                    -Value ([string] $row.Allocated) `
                    -Context "$role row '$key'"
            })
    }
    return @($measurements | Sort-Object pathId, mode)
}

function Get-ArtifactDescriptors {
    param([Parameter(Mandatory)][string] $Root)

    return @(Get-ChildItem -LiteralPath $Root -File -Recurse |
        Sort-Object FullName |
        ForEach-Object {
            $relativePath = [IO.Path]::GetRelativePath($OutputPath, $_.FullName)
            $relativePath = $relativePath.Replace(
                [IO.Path]::DirectorySeparatorChar,
                '/')
            [ordered]@{
                relativePath = $relativePath
                sha256 = Get-Sha256 -Path $_.FullName
            }
        })
}

function Start-EnvironmentMonitor {
    $script:monitorCsvPath = Join-Path $environmentRoot 'environment-monitor.csv'
    $script:monitorReadyPath = Join-Path $environmentRoot 'environment-monitor.ready'
    $script:monitorStopPath = Join-Path $environmentRoot 'environment-monitor.stop'
    $script:monitorStdoutPath = Join-Path $environmentRoot 'environment-monitor.stdout.log'
    $script:monitorStderrPath = Join-Path $environmentRoot 'environment-monitor.stderr.log'
    $script:monitorSummaryPath = Join-Path $environmentRoot 'environment-monitor-summary.txt'

    $runnerProcess = [Diagnostics.Process]::GetCurrentProcess()
    try {
        $runnerStartedUtc = [DateTimeOffset] $runnerProcess.StartTime.ToUniversalTime()
    }
    finally {
        $runnerProcess.Dispose()
    }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = (Get-Command pwsh -ErrorAction Stop).Source
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            $monitorScript,
            '-OutputPath',
            $monitorCsvPath,
            '-StopSignalPath',
            $monitorStopPath,
            '-ReadySignalPath',
            $monitorReadyPath,
            '-AllowedRootProcessId',
            $PID.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-AllowedRootStartTimeUtc',
            $runnerStartedUtc.ToString('O'),
            '-SampleIntervalMilliseconds',
            $monitorSampleIntervalMilliseconds.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalCpuPercent',
            $maximumExternalCpuPercent.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalCpuCoreEquivalent',
            $maximumExternalCpuCoreEquivalent.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-MaxExternalIoBytesPerSecond',
            $maximumExternalIoBytesPerSecond.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-RequiredConsecutiveBusySamples',
            $requiredConsecutiveBusySamples.ToString([Globalization.CultureInfo]::InvariantCulture),
            '-ProhibitedExternalProcessNames',
            $prohibitedExternalProcessNames)) {
        [void] $startInfo.ArgumentList.Add([string] $argument)
    }

    $script:monitorProcess = [Diagnostics.Process]::new()
    $script:monitorProcess.StartInfo = $startInfo
    if (-not $script:monitorProcess.Start()) {
        throw 'Could not start the performance environment monitor.'
    }
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    while (-not (Test-Path -LiteralPath $monitorReadyPath -PathType Leaf) -and
        -not $monitorProcess.HasExited -and
        [DateTimeOffset]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 100
    }
    if (-not (Test-Path -LiteralPath $monitorReadyPath -PathType Leaf)) {
        if (-not $monitorProcess.HasExited) {
            $monitorProcess.Kill($true)
            $monitorProcess.WaitForExit()
        }
        throw 'The performance environment monitor did not become ready.'
    }
    $script:monitorReadyUtc = [DateTimeOffset]::Parse(
        [IO.File]::ReadAllText($monitorReadyPath).Trim(),
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
}

function Assert-MonitorClean {
    param(
        [Parameter(Mandatory)][string] $Context,
        [switch] $AllowExited
    )

    if ($null -eq $monitorProcess -or
        ($monitorProcess.HasExited -and -not $AllowExited)) {
        throw "The performance environment monitor exited $Context."
    }
    $rows = if (Test-Path -LiteralPath $monitorCsvPath -PathType Leaf) {
        @(Import-Csv -LiteralPath $monitorCsvPath)
    }
    else {
        @()
    }
    $contaminated = @($rows | Where-Object { [string] $_.Contaminated -ceq 'True' })
    if ($contaminated.Count -gt 0) {
        $first = $contaminated[0]
        throw (
            "Performance environment contamination detected ${Context}: " +
            "timestamp=$($first.TimestampUtc); reason=$($first.BusyReason); " +
            "prohibited=$($first.ProhibitedExternalProcesses).")
    }
}

function Wait-ForQuietGate {
    param([Parameter(Mandatory)][int] $PairNumber)

    & dotnet build-server shutdown | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Could not stop .NET build workers before pair $PairNumber."
    }
    Write-Host (
        "Pair ${PairNumber}: enforcing the canonical $quietGateSeconds-second " +
        'low-activity gate before the reference launch.')
    Start-Sleep -Seconds $quietGateSeconds
    Assert-MonitorClean -Context "before pair $PairNumber"
}

function Stop-AndAuditEnvironmentMonitor {
    param(
        [Parameter(Mandatory)][DateTimeOffset] $FirstMeasurementStartedUtc,
        [Parameter(Mandatory)][DateTimeOffset] $LastMeasurementCompletedUtc
    )

    Start-Sleep -Milliseconds ($monitorSampleIntervalMilliseconds + 250)
    [IO.File]::WriteAllText($monitorStopPath, [DateTimeOffset]::UtcNow.ToString('O'))
    if (-not $monitorProcess.WaitForExit(30000)) {
        $monitorProcess.Kill($true)
        $monitorProcess.WaitForExit()
        throw 'The performance environment monitor did not stop within 30 seconds.'
    }
    [IO.File]::WriteAllText($monitorStdoutPath, $monitorProcess.StandardOutput.ReadToEnd())
    [IO.File]::WriteAllText($monitorStderrPath, $monitorProcess.StandardError.ReadToEnd())
    if ($monitorProcess.ExitCode -ne 0) {
        throw "The performance environment monitor failed with exit code $($monitorProcess.ExitCode)."
    }
    Assert-MonitorClean -Context 'during qualification' -AllowExited
    $rows = @(Import-Csv -LiteralPath $monitorCsvPath)
    if ($rows.Count -lt 1) {
        throw 'The performance environment monitor emitted no samples.'
    }
    $timestamps = @($rows | ForEach-Object {
            [DateTimeOffset]::Parse(
                [string] $_.TimestampUtc,
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
        })
    $maximumGapMilliseconds = [Math]::Max(
        5000.0,
        $monitorSampleIntervalMilliseconds * 3.0)
    for ($index = 1; $index -lt $timestamps.Count; $index++) {
        $gap = ($timestamps[$index] - $timestamps[$index - 1]).TotalMilliseconds
        if ($gap -le 0 -or $gap -gt $maximumGapMilliseconds) {
            throw "The environment monitor has a discontinuous $gap ms sample gap."
        }
    }
    if ($monitorReadyUtc -gt $FirstMeasurementStartedUtc -or
        $timestamps[-1] -lt $LastMeasurementCompletedUtc) {
        throw 'The environment monitor did not cover the full declared measurement interval.'
    }
    $summary = @(
        'Schema=csharpdb-observability-performance-environment/v1',
        'Result=PASS',
        "RunnerId=$runnerId",
        "ReadyUtc=$($monitorReadyUtc.ToString('O'))",
        "FirstMeasurementStartedUtc=$($FirstMeasurementStartedUtc.ToString('O'))",
        "LastMeasurementCompletedUtc=$($LastMeasurementCompletedUtc.ToString('O'))",
        "FirstSampleUtc=$($timestamps[0].ToString('O'))",
        "LastSampleUtc=$($timestamps[-1].ToString('O'))",
        "SampleCount=$($rows.Count)",
        "MaximumSampleGapMilliseconds=$maximumGapMilliseconds",
        "MaximumExternalCpuPercent=$maximumExternalCpuPercent",
        "MaximumExternalCpuCoreEquivalent=$maximumExternalCpuCoreEquivalent",
        "MaximumExternalIoBytesPerSecond=$maximumExternalIoBytesPerSecond",
        "RequiredConsecutiveBusySamples=$requiredConsecutiveBusySamples",
        "TelemetrySha256=$(Get-Sha256 -Path $monitorCsvPath)"
    )
    [IO.File]::WriteAllLines($monitorSummaryPath, $summary)
}

function Invoke-BenchmarkLaunch {
    param(
        [Parameter(Mandatory)][string] $Role,
        [Parameter(Mandatory)][int] $PairNumber,
        [Parameter(Mandatory)][string] $ProjectPath,
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][object] $Machine
    )

    $artifactRoot = Join-Path $rawRoot "pair-$PairNumber/$Role"
    New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null
    $consoleLog = Join-Path $artifactRoot 'console.log'
    $startedUtc = [DateTimeOffset]::UtcNow
    Invoke-LoggedCommand `
        -Label "Pair $PairNumber $Role observability benchmark" `
        -LogPath $consoleLog `
        -Command {
            & dotnet run `
                -c $configuration `
                --no-build `
                --project $ProjectPath `
                -- `
                --micro `
                --filter '*ObservabilityNoListener*' `
                --artifacts $artifactRoot
        }
    $completedUtc = [DateTimeOffset]::UtcNow
    return [ordered]@{
        pairNumber = $PairNumber
        role = $Role
        commit = $Commit
        configuration = $configuration
        startedUtc = $startedUtc.ToString('O')
        completedUtc = $completedUtc.ToString('O')
        machine = $Machine
        benchmark = [ordered]@{
            warmupCount = 3
            iterationCount = 10
            launchCount = 1
        }
        artifactRoot = $artifactRoot
    }
}

$monitorProcess = $null
$monitorReadyUtc = [DateTimeOffset]::MinValue
$primaryFailure = $null
$originalLocation = (Get-Location).Path
$locationChanged = $false
try {
    $referenceInfo = Get-RepositoryInformation `
        -ProjectPath $ReferenceProjectPath `
        -Role 'reference'
    $candidateInfo = Get-RepositoryInformation `
        -ProjectPath $CandidateProjectPath `
        -Role 'candidate'
    if ($referenceInfo.commit -ceq $candidateInfo.commit) {
        throw 'The detached reference and candidate commits must differ.'
    }
    if ($referenceInfo.commit -cne $approvedReferenceCommit) {
        throw (
            "Reference commit $($referenceInfo.commit) is not the policy-approved " +
            "$approvedReferenceCommit. No reference code will be built or executed.")
    }

    Set-Location -LiteralPath $candidateInfo.root
    $locationChanged = $true

    Invoke-LoggedCommand `
        -Label 'Build detached reference' `
        -LogPath (Join-Path $setupRoot 'reference-build.log') `
        -Command { & dotnet build $ReferenceProjectPath -c $configuration --nologo }
    Invoke-LoggedCommand `
        -Label 'Build candidate' `
        -LogPath (Join-Path $setupRoot 'candidate-build.log') `
        -Command { & dotnet build $CandidateProjectPath -c $configuration --nologo }
    Invoke-LoggedCommand `
        -Label 'Shut down .NET build servers' `
        -LogPath (Join-Path $setupRoot 'build-server-shutdown.log') `
        -Command { & dotnet build-server shutdown }
    Write-Host "Waiting $postBuildQuiescenceSeconds seconds after build-server shutdown."
    Start-Sleep -Seconds $postBuildQuiescenceSeconds

    Start-EnvironmentMonitor
    $runRecords = [Collections.Generic.List[object]]::new()
    for ($pairNumber = 1; $pairNumber -le $requiredPairCount; $pairNumber++) {
        $pairMachine = Get-MachineFingerprint
        Wait-ForQuietGate -PairNumber $pairNumber
        $referenceRun = Invoke-BenchmarkLaunch `
            -Role 'reference' `
            -PairNumber $pairNumber `
            -ProjectPath $ReferenceProjectPath `
            -Commit $referenceInfo.commit `
            -Machine $pairMachine
        $candidateRun = Invoke-BenchmarkLaunch `
            -Role 'candidate' `
            -PairNumber $pairNumber `
            -ProjectPath $CandidateProjectPath `
            -Commit $candidateInfo.commit `
            -Machine $pairMachine
        $postPairMachine = Get-MachineFingerprint
        if (($pairMachine | ConvertTo-Json -Compress) -cne
            ($postPairMachine | ConvertTo-Json -Compress)) {
            throw "Machine/runtime/power fingerprint changed during pair $pairNumber."
        }
        Assert-MonitorClean -Context "after pair $pairNumber"

        $referenceRun.measurements = Get-NormalizedMeasurements `
            -ArtifactRoot $referenceRun.artifactRoot `
            -Role 'reference'
        $candidateRun.measurements = Get-NormalizedMeasurements `
            -ArtifactRoot $candidateRun.artifactRoot `
            -Role 'candidate'
        $referenceRun.artifacts = Get-ArtifactDescriptors -Root $referenceRun.artifactRoot
        $candidateRun.artifacts = Get-ArtifactDescriptors -Root $candidateRun.artifactRoot
        $referenceRun.Remove('artifactRoot')
        $candidateRun.Remove('artifactRoot')
        $runRecords.Add($referenceRun)
        $runRecords.Add($candidateRun)
    }

    $firstMeasurementStartedUtc = @($runRecords |
        ForEach-Object { [DateTimeOffset]::Parse([string] $_.startedUtc) } |
        Sort-Object |
        Select-Object -First 1)[0]
    $lastMeasurementCompletedUtc = @($runRecords |
        ForEach-Object { [DateTimeOffset]::Parse([string] $_.completedUtc) } |
        Sort-Object |
        Select-Object -Last 1)[0]
    Stop-AndAuditEnvironmentMonitor `
        -FirstMeasurementStartedUtc $firstMeasurementStartedUtc `
        -LastMeasurementCompletedUtc $lastMeasurementCompletedUtc
    $monitorProcess = $null

    $evidence = [ordered]@{
        schemaVersion = 1
        evidenceKind = 'csharpdb.observability-performance.paired'
        generatedUtc = [DateTimeOffset]::UtcNow.ToString('O')
        policyId = [string] $policy.policyId
        policySha256 = Get-Sha256 -Path $ThresholdsPath
        referenceCommit = $referenceInfo.commit
        candidateCommit = $candidateInfo.commit
        configuration = $configuration
        producer = [ordered]@{
            runnerSha256 = Get-Sha256 -Path $PSCommandPath
            comparatorSha256 = Get-Sha256 -Path $comparatorScript
            environmentMonitorSha256 = Get-Sha256 -Path $monitorScript
            referenceBenchmarkSourceSha256 = $referenceInfo.sourceSha256
            candidateBenchmarkSourceSha256 = $candidateInfo.sourceSha256
        }
        environment = [ordered]@{
            status = 'PASS'
            artifacts = Get-ArtifactDescriptors -Root $environmentRoot
        }
        runs = @($runRecords)
    }
    $evidencePath = Join-Path $OutputPath 'observability-performance-evidence.json'
    $attestationPath = Join-Path $OutputPath 'observability-performance-attestation.json'
    $reportPath = Join-Path $OutputPath 'observability-performance-report.md'
    $evidence | ConvertTo-Json -Depth 100 | Set-Content `
        -LiteralPath $evidencePath `
        -Encoding utf8NoBOM

    & pwsh `
        -NoLogo `
        -NoProfile `
        -File $comparatorScript `
        -EvidencePath $evidencePath `
        -ThresholdsPath $ThresholdsPath `
        -OutputJsonPath $attestationPath `
        -OutputMarkdownPath $reportPath
    $comparisonExitCode = $LASTEXITCODE

    $manifestPath = Join-Path $OutputPath 'sha256-manifest.txt'
    $hashLines = @(Get-ChildItem -LiteralPath $OutputPath -File -Recurse |
        Where-Object { $_.FullName -cne $manifestPath } |
        Sort-Object FullName |
        ForEach-Object {
            $relativePath = [IO.Path]::GetRelativePath($OutputPath, $_.FullName)
            $relativePath = $relativePath.Replace(
                [IO.Path]::DirectorySeparatorChar,
                '/')
            "$(Get-Sha256 -Path $_.FullName)  $relativePath"
        })
    [IO.File]::WriteAllLines($manifestPath, $hashLines)
    if ($comparisonExitCode -ne 0) {
        throw (
            'Observability performance qualification did not pass. ' +
            'The failed attestation and all raw evidence were retained.')
    }
}
catch {
    $primaryFailure = $_
}
finally {
    if ($null -ne $monitorProcess) {
        try {
            if (-not $monitorProcess.HasExited) {
                [IO.File]::WriteAllText(
                    (Join-Path $environmentRoot 'environment-monitor.stop'),
                    [DateTimeOffset]::UtcNow.ToString('O'))
                if (-not $monitorProcess.WaitForExit(30000)) {
                    $monitorProcess.Kill($true)
                    $monitorProcess.WaitForExit()
                }
            }
            [IO.File]::WriteAllText(
                (Join-Path $environmentRoot 'environment-monitor.stdout.log'),
                $monitorProcess.StandardOutput.ReadToEnd())
            [IO.File]::WriteAllText(
                (Join-Path $environmentRoot 'environment-monitor.stderr.log'),
                $monitorProcess.StandardError.ReadToEnd())
        }
        catch {
            if ($null -eq $primaryFailure) {
                $primaryFailure = $_
            }
        }
    }
    if ($locationChanged) {
        Set-Location -LiteralPath $originalLocation
    }
}

if ($null -ne $primaryFailure) {
    throw $primaryFailure
}
Write-Host "Formal observability evidence: $OutputPath"
