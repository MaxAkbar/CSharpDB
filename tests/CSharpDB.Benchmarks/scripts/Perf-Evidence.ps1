# Shared evidence contract for fresh captures and guardrail comparisons.
Set-StrictMode -Version Latest

function Get-PerfProperty {
    param($Object, [string]$Name, $Default = $null)
    if ($null -ne $Object -and $Object.PSObject.Properties.Name -contains $Name) { return $Object.$Name }
    return $Default
}

function Get-PerfHarnessRecords {
    param([Parameter(Mandatory)][string]$Root)
    # Use the release comparison's source/configuration identity convention.
    $records = @(Get-ChildItem -LiteralPath $Root -File -Recurse -Force |
        Where-Object {
            $relative = [IO.Path]::GetRelativePath($Root, $_.FullName)
            $relative -notmatch '(^|[\\/])(BenchmarkDotNet.Artifacts|artifacts|baselines|comparison-snapshots|bin|obj|results|run-logs)([\\/]|$)' -and
            $_.Extension -in @('.cs', '.csproj', '.json', '.props', '.targets', '.slnx', '.ps1')
        } | ForEach-Object {
            if ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Harness file is a reparse point: $($_.FullName)" }
            [pscustomobject]@{
                path = [IO.Path]::GetRelativePath($Root, $_.FullName).Replace('\', '/')
                sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            }
        } | Sort-Object -CaseSensitive path)
    foreach ($required in @('Program.cs', 'CSharpDB.Benchmarks.csproj')) {
        if ($required -cnotin @($records.path)) { throw "Harness is missing $required." }
    }
    return $records
}

function Get-PerfHarnessIdentity {
    param([Parameter(Mandatory)][object[]]$Records)
    $payload = (@($Records | ForEach-Object { "$($_.sha256) *$($_.path)" }) -join "`n") + "`n"
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($payload))).ToLowerInvariant()
}

function Get-PerfMachineFingerprint {
    $cpu = [string]$env:PROCESSOR_IDENTIFIER
    if ($IsWindows) { $cpu = [string](Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name) }
    elseif ($IsLinux) {
        $line = Get-Content /proc/cpuinfo | Where-Object { $_ -match '^model name\s*:' } | Select-Object -First 1
        $cpu = ($line -split ':', 2)[1].Trim()
    }
    elseif ($IsMacOS) { $cpu = [string](& sysctl -n machdep.cpu.brand_string) }
    $sdk = [string](& dotnet --version)
    if ($LASTEXITCODE -ne 0) { throw 'Could not identify the .NET SDK.' }
    return [pscustomobject]@{
        fingerprintVersion = 1
        capturedUtc = [DateTime]::UtcNow.ToString('o')
        runnerId = [string]$env:CSHARPDB_PERF_RUNNER_ID
        machineName = [Environment]::MachineName
        cpuName = $cpu.Trim()
        logicalCoreCount = [Environment]::ProcessorCount
        osPlatform = $(if ($IsWindows) { 'Windows' } elseif ($IsLinux) { 'Linux' } else { 'macOS' })
        osDescription = [Runtime.InteropServices.RuntimeInformation]::OSDescription.Trim()
        osArchitecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        processArchitecture = [Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
        dotnetVersion = $sdk.Trim()
    }
}

function Assert-PerfMachineFingerprint {
    param([Parameter(Mandatory)]$Fingerprint)
    foreach ($name in @('machineName', 'cpuName', 'osPlatform', 'osDescription', 'osArchitecture', 'processArchitecture', 'dotnetVersion')) {
        if ([string]::IsNullOrWhiteSpace([string](Get-PerfProperty $Fingerprint $name))) { throw "Machine fingerprint is missing $name." }
    }
    if ([int](Get-PerfProperty $Fingerprint 'logicalCoreCount' 0) -le 0) { throw 'Machine fingerprint has no positive logicalCoreCount.' }
}

function Get-PerfRuntimeIdentity {
    param([string]$Project, [string]$Configuration)
    Push-Location (Split-Path -Parent $Project)
    try {
        $json = @(& dotnet run -c $Configuration --no-build --project $Project -- --perf-environment)
        if ($LASTEXITCODE -ne 0) { throw 'Could not capture benchmark runtime identity.' }
    }
    finally { Pop-Location }
    $runtime = ($json -join "`n") | ConvertFrom-Json
    Assert-PerfRuntimeIdentity $runtime
    return $runtime
}

function Assert-PerfRuntimeIdentity {
    param([Parameter(Mandatory)]$Runtime)
    foreach ($name in @('frameworkDescription', 'runtimeVersion', 'processArchitecture', 'targetFramework')) {
        if ([string]::IsNullOrWhiteSpace([string](Get-PerfProperty $Runtime $name))) { throw "Runtime identity is missing $name." }
    }
}

function Resolve-PerfArtifactPath {
    param([string]$Root, [string]$Relative)
    $prefix = [IO.Path]::GetFullPath($Root).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    $path = [IO.Path]::GetFullPath((Join-Path $Root $Relative))
    if ([IO.Path]::IsPathRooted($Relative) -or -not $path.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Artifact path escapes snapshot: $Relative"
    }
    return $path
}

function Get-PerfExecutedMicroCases {
    param([string]$LogPath)
    $matches = @(Select-String -LiteralPath $LogPath -Pattern 'Global total time:.*executed benchmarks:\s*([1-9]\d*)\s*$')
    if ($matches.Count -ne 1) { throw "Micro benchmark did not execute any benchmark cases: $LogPath" }
    return [int]$matches[0].Matches[0].Groups[1].Value
}

function Write-PerfSnapshotManifest {
    param([string]$SnapshotDir, [string]$ProductCommit, [string]$HarnessRoot, $Runtime, [object[]]$Runs)
    if ($ProductCommit -notmatch '^[a-f0-9]{40}$') { throw 'Snapshot requires a full product commit SHA.' }
    Assert-PerfRuntimeIdentity $Runtime
    Push-Location $HarnessRoot
    try { $machine = Get-PerfMachineFingerprint }
    finally { Pop-Location }
    Assert-PerfMachineFingerprint $machine
    $records = @(Get-PerfHarnessRecords $HarnessRoot)
    $records | ConvertTo-Json -Depth 5 -AsArray | Set-Content -LiteralPath (Join-Path $SnapshotDir 'harness-files.json') -Encoding utf8
    $machine | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $SnapshotDir 'machine.json') -Encoding utf8
    $Runtime | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $SnapshotDir 'runtime.json') -Encoding utf8
    $artifacts = @(Get-ChildItem -LiteralPath $SnapshotDir -File -Recurse | Where-Object Name -ne 'manifest.json' | ForEach-Object {
        [pscustomobject]@{
            path = [IO.Path]::GetRelativePath($SnapshotDir, $_.FullName).Replace('\', '/')
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    } | Sort-Object -CaseSensitive path)
    [ordered]@{
        schemaVersion = 1
        productCommit = $ProductCommit
        harnessIdentity = Get-PerfHarnessIdentity $records
        capturedUtc = [DateTime]::UtcNow.ToString('o')
        machine = $machine
        runtime = $Runtime
        runs = @($Runs)
        artifacts = $artifacts
    } | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $SnapshotDir 'manifest.json') -Encoding utf8
    Assert-PerfSnapshot $SnapshotDir | Out-Null
}

function Assert-PerfSnapshot {
    param([Parameter(Mandatory)][string]$SnapshotDir)
    $manifestPath = Join-Path $SnapshotDir 'manifest.json'
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) { throw "Snapshot manifest missing: $manifestPath. Historical provenance must not be invented." }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    if ($manifest.schemaVersion -ne 1 -or $manifest.productCommit -notmatch '^[a-f0-9]{40}$' -or $manifest.harnessIdentity -notmatch '^[a-f0-9]{64}$') {
        throw "Snapshot has invalid commit or harness provenance: $SnapshotDir"
    }
    Assert-PerfMachineFingerprint $manifest.machine
    Assert-PerfRuntimeIdentity $manifest.runtime
    $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($artifact in @($manifest.artifacts)) {
        if (-not $paths.Add($artifact.path)) { throw "Duplicate manifest artifact: $($artifact.path)" }
        $path = Resolve-PerfArtifactPath $SnapshotDir $artifact.path
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Snapshot artifact missing: $path" }
        if ($artifact.sha256 -notmatch '^[a-f0-9]{64}$' -or (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -ine $artifact.sha256) {
            throw "Snapshot artifact hash mismatch: $path"
        }
    }
    foreach ($required in @('machine.json', 'runtime.json', 'harness-files.json')) {
        if (-not $paths.Contains($required)) { throw "Snapshot manifest does not reference $required." }
    }
    foreach ($name in @('machine', 'runtime')) {
        $data = Get-Content -LiteralPath (Join-Path $SnapshotDir "$name.json") -Raw | ConvertFrom-Json
        if (($data | ConvertTo-Json -Compress -Depth 5) -cne ($manifest.$name | ConvertTo-Json -Compress -Depth 5)) {
            throw "Snapshot $name identity disagrees with manifest."
        }
    }
    $harnessRecords = @(Get-Content -LiteralPath (Join-Path $SnapshotDir 'harness-files.json') -Raw | ConvertFrom-Json)
    foreach ($required in @('Program.cs', 'CSharpDB.Benchmarks.csproj')) {
        if ($required -cnotin @($harnessRecords.path)) { throw "Snapshot harness manifest is missing $required." }
    }
    foreach ($record in $harnessRecords) {
        if ($record.sha256 -notmatch '^[a-f0-9]{64}$') { throw 'Snapshot harness contains an invalid source hash.' }
    }
    if ((Get-PerfHarnessIdentity $harnessRecords) -cne $manifest.harnessIdentity) { throw 'Snapshot harness identity disagrees with file manifest.' }
    $csvs = @(Get-ChildItem -LiteralPath $SnapshotDir -File -Recurse -Filter '*.csv')
    if ($csvs.Count -eq 0) { throw 'Snapshot contains no CSV artifacts.' }
    foreach ($csv in $csvs) {
        if (-not $paths.Contains([IO.Path]::GetRelativePath($SnapshotDir, $csv.FullName).Replace('\', '/'))) { throw "CSV is absent from snapshot manifest: $($csv.Name)" }
        if (@(Import-Csv -LiteralPath $csv.FullName).Count -eq 0) { throw "Snapshot CSV has zero rows: $($csv.Name)" }
    }
    if (@($manifest.runs).Count -eq 0) { throw 'Snapshot has zero executed cases (no runs).' }
    $microCsvPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($run in @($manifest.runs)) {
        if ([int]$run.executedCases -le 0) { throw "Snapshot run has zero executed cases: $($run.label)" }
        if (-not $paths.Contains($run.logPath)) { throw "Snapshot run log is absent from manifest: $($run.logPath)" }
        if (@($run.arguments) -contains '--micro') {
            if ((Get-PerfExecutedMicroCases (Resolve-PerfArtifactPath $SnapshotDir $run.logPath)) -ne [int]$run.executedCases) { throw 'Snapshot executed-case count disagrees with micro log.' }
            $runCsvs = @(Get-PerfProperty $run 'csvPaths' @())
            if ($runCsvs.Count -eq 0) {
                # A capture with one micro invocation can unambiguously map its
                # full micro-results directory to that invocation.
                if (@($manifest.runs | Where-Object { @($_.arguments) -contains '--micro' }).Count -ne 1) { throw 'Snapshot is missing per-run CSV coverage.' }
                $runCsvs = @($csvs | Where-Object { [IO.Path]::GetRelativePath($SnapshotDir, $_.FullName) -match '^micro-results[\\/]' } | ForEach-Object { [IO.Path]::GetRelativePath($SnapshotDir, $_.FullName).Replace('\', '/') })
            }
            $exportedCases = 0
            $runCsvPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
            foreach ($csv in $runCsvs) {
                if (-not $runCsvPaths.Add($csv)) { throw "Duplicate run CSV coverage: $csv" }
                if (-not $microCsvPaths.Add($csv)) { throw "Micro CSV cannot cover two benchmark invocations: $csv" }
                if (-not $paths.Contains($csv)) { throw "Run CSV is absent from manifest: $csv" }
                $exportedCases += @(Import-Csv -LiteralPath (Resolve-PerfArtifactPath $SnapshotDir $csv)).Count
            }
            if ($exportedCases -ne [int]$run.executedCases) { throw "Snapshot micro case coverage mismatch: executed=$($run.executedCases), exported=$exportedCases." }
        }
    }
    return $manifest
}

function Assert-PerfSnapshotCsv {
    param($Manifest, [string]$SnapshotDir, [string]$CsvPath)
    $relative = [IO.Path]::GetRelativePath($SnapshotDir, $CsvPath).Replace('\', '/')
    if ($relative -cnotin @($Manifest.artifacts.path)) { throw "Referenced CSV is absent from snapshot manifest: $relative" }
}

function Get-PerfPairSchedule {
    # Same balanced AB / BA convention as the release comparison runner.
    return @(
        [pscustomobject]@{ pair = 1; order = 'base-head'; roles = @('base', 'head') },
        [pscustomobject]@{ pair = 2; order = 'head-base'; roles = @('head', 'base') }
    )
}
