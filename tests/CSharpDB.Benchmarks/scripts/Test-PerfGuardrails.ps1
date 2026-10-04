#requires -Version 7.0
# Focused contract tests use synthetic evidence, never historical measurements.
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptDir 'Perf-Evidence.ps1')
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ('csharpdb-perf-tests-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
$script:passed = 0
$script:failed = [Collections.Generic.List[string]]::new()
$fixtureRuntime = [pscustomobject]@{ frameworkDescription = '.NET 10.0.12'; runtimeVersion = '10.0.12'; processArchitecture = 'X64'; targetFramework = '.NETCoreApp,Version=v10.0' }
$fixtureHarness = Join-Path $testRoot 'harness'
New-Item -ItemType Directory -Path $fixtureHarness | Out-Null
'// Synthetic harness fixture' | Set-Content (Join-Path $fixtureHarness 'Program.cs')
'<Project />' | Set-Content (Join-Path $fixtureHarness 'CSharpDB.Benchmarks.csproj')
$fixtureCommit = 'a' * 40

function New-Fixture {
    $root = Join-Path $testRoot ([Guid]::NewGuid().ToString('N'))
    foreach ($role in @('base', 'head')) {
        $snapshot = Join-Path $root $role
        New-Item -ItemType Directory -Path (Join-Path $snapshot 'micro-results') -Force | Out-Null
        New-Item -ItemType Directory -Path (Join-Path $snapshot 'logs') -Force | Out-Null
        @('Method,Mean,Allocated,ExtraInfo', 'A,100 ns,100 B,count=10', 'B,100 ns,100 B,count=10') | Set-Content (Join-Path $snapshot 'micro-results/fixture.csv')
        'Global total time: 00:00:01, executed benchmarks: 2' | Set-Content (Join-Path $snapshot 'logs/run.log')
        Write-PerfSnapshotManifest -SnapshotDir $snapshot -ProductCommit $fixtureCommit -HarnessRoot $fixtureHarness -Runtime $fixtureRuntime -Runs @(
            [pscustomobject]@{ label = 'fixture'; arguments = @('--micro'); executedCases = 2; logPath = 'logs/run.log' })
    }
    $policy = [pscustomobject]@{ checks = @([pscustomobject]@{ csv = 'fixture.csv'; keyColumns = @('Method'); minimumEnforceableComparisons = 1 }) }
    $policy | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $root 'thresholds.json')
    return $root
}

function Refresh-FixtureHashes {
    param([string]$Snapshot)
    # Re-sign synthetic fixture inputs so parser/coverage tests reach their actual
    # failure path, rather than stopping at the deliberately separate hash gate.
    $path = Join-Path $Snapshot 'manifest.json'
    $manifest = Get-Content $path -Raw | ConvertFrom-Json
    foreach ($artifact in $manifest.artifacts) {
        $artifact.sha256 = (Get-FileHash -LiteralPath (Join-Path $Snapshot $artifact.path) -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    $manifest.machine = Get-Content (Join-Path $Snapshot 'machine.json') -Raw | ConvertFrom-Json
    $manifest.runtime = Get-Content (Join-Path $Snapshot 'runtime.json') -Raw | ConvertFrom-Json
    $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
}

function Invoke-FixtureComparison {
    param([string]$Root, [bool]$ReportOnly = $true)
    & (Join-Path $scriptDir 'Compare-Baseline.ps1') -BaselineSnapshot (Join-Path $Root 'base') -CurrentSnapshot (Join-Path $Root 'head') -ThresholdsPath (Join-Path $Root 'thresholds.json') -ReportPath (Join-Path $Root 'report.md') -NoFailOnRegression:$ReportOnly | Out-Null
}

function Assert-Fails {
    param([scriptblock]$Action, [string]$Message)
    $errorText = ''
    try { & $Action | Out-Null } catch { $errorText = $_.Exception.Message }
    if ($errorText -notlike "*$Message*") { throw "Expected failure '$Message'; received '$errorText'." }
}

function Test-Case {
    param([string]$Name, [scriptblock]$Action)
    try { & $Action; $script:passed++; Write-Host "PASS $Name" }
    catch { $script:failed.Add("${Name}: $($_.Exception.Message)"); Write-Host "FAIL ${Name}: $($_.Exception.Message)" }
}

try {
    Test-Case 'Valid same-machine comparisons pass' {
        $root = New-Fixture
        Invoke-FixtureComparison $root
    }
    Test-Case 'Timing regressions report but gate fails' {
        $root = New-Fixture
        $path = Join-Path $root 'head/micro-results/fixture.csv'
        (Get-Content $path -Raw).Replace('100 ns', '200 ns') | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Invoke-FixtureComparison $root
        Assert-Fails { Invoke-FixtureComparison $root $false } 'violations detected'
    }
    Test-Case 'Invalid recheck replaces previous successful report' {
        $root = New-Fixture
        Invoke-FixtureComparison $root
        Remove-Item -LiteralPath (Join-Path $root 'head/manifest.json')
        Assert-Fails { Invoke-FixtureComparison $root } 'manifest missing'
        $report = Get-Content -LiteralPath (Join-Path $root 'report.md') -Raw
        if ($report -notmatch 'Status: INVALID' -or $report -match 'PASS=') { throw 'Invalid evidence retained a previous successful report.' }
    }
    Test-Case 'Duplicate CSV references cannot inflate executed case coverage' {
        $root = New-Fixture
        $path = Join-Path $root 'head/manifest.json'
        $manifest = Get-Content $path -Raw | ConvertFrom-Json
        $manifest.runs[0] | Add-Member csvPaths @('micro-results/fixture.csv', 'micro-results/fixture.csv')
        $manifest.runs[0].executedCases = 4
        'Global total time: 00:00:01, executed benchmarks: 4' | Set-Content (Join-Path $root 'head/logs/run.log')
        $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Assert-Fails { Invoke-FixtureComparison $root } 'Duplicate run CSV coverage'
    }
    Test-Case 'One micro export cannot supply coverage for two invocations' {
        $root = New-Fixture
        $path = Join-Path $root 'head/manifest.json'
        $manifest = Get-Content $path -Raw | ConvertFrom-Json
        $manifest.runs[0] | Add-Member csvPaths @('micro-results/fixture.csv')
        $manifest.runs = @($manifest.runs[0], $manifest.runs[0])
        $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'cannot cover two benchmark invocations'
    }
    foreach ($missing in @('manifest.json', 'machine.json', 'runtime.json', 'harness-files.json', 'micro-results/fixture.csv', 'logs/run.log')) {
        Test-Case "Missing $missing fails report mode" {
            $root = New-Fixture
            Remove-Item -LiteralPath (Join-Path $root "base/$missing")
            Assert-Fails { Invoke-FixtureComparison $root } 'missing'
        }
    }
    Test-Case 'Missing baseline fails report mode' {
        $root = New-Fixture
        Assert-Fails {
            & (Join-Path $scriptDir 'Compare-Baseline.ps1') -BaselineSnapshot (Join-Path $root 'absent') -CurrentSnapshot (Join-Path $root 'head') -ThresholdsPath (Join-Path $root 'thresholds.json') -NoFailOnRegression
        } 'No baseline snapshot'
    }
    Test-Case 'Current data without captured provenance fails report mode' {
        $root = New-Fixture
        Assert-Fails {
            & (Join-Path $scriptDir 'Compare-Baseline.ps1') -BaselineSnapshot (Join-Path $root 'base') -CurrentMicroResultsDir (Join-Path $root 'head/micro-results') -ThresholdsPath (Join-Path $root 'thresholds.json') -NoFailOnRegression
        } 'Current snapshot manifest'
    }
    Test-Case 'SDK changes remain enforceable with identical measured runtime' {
        $root = New-Fixture
        $path = Join-Path $root 'head/machine.json'
        $machine = Get-Content $path -Raw | ConvertFrom-Json
        $machine.dotnetVersion = '11.0.100'
        $machine | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Invoke-FixtureComparison $root
    }
    foreach ($value in @('NA', '?', 'garbage', '0 ns', '1e999 ns')) {
        Test-Case "Nonnumeric or zero metric '$value' fails report mode" {
            $root = New-Fixture
            $path = Join-Path $root 'head/micro-results/fixture.csv'
            (Get-Content $path -Raw).Replace('100 ns', $value) | Set-Content $path
            Refresh-FixtureHashes (Join-Path $root 'head')
            Assert-Fails { Invoke-FixtureComparison $root } $(if ($value -in @('NA', '?')) { 'integrity failed' } elseif ($value -eq '0 ns') { 'Invalid numeric' } else { 'Unable to parse' })
        }
    }
    Test-Case 'Nonnumeric allocation fails report mode' {
        $root = New-Fixture
        $path = Join-Path $root 'head/micro-results/fixture.csv'
        (Get-Content $path -Raw).Replace('100 B', 'NA') | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Assert-Fails { Invoke-FixtureComparison $root } 'integrity failed'
    }
    Test-Case 'Allocation overflow fails report mode' {
        $root = New-Fixture
        $path = Join-Path $root 'head/micro-results/fixture.csv'
        (Get-Content $path -Raw).Replace('100 B', (('9' * 308) + ' GB')) | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Assert-Fails { Invoke-FixtureComparison $root } 'Invalid numeric allocation'
    }
    foreach ($role in @('base', 'head')) {
        Test-Case "Missing $role row fails report mode" {
            $root = New-Fixture
            $path = Join-Path $root "$role/micro-results/fixture.csv"
            @(Get-Content $path | Where-Object { $_ -notlike 'B,*' }) | Set-Content $path
            Refresh-FixtureHashes (Join-Path $root $role)
            Assert-Fails { Invoke-FixtureComparison $root } 'micro case coverage mismatch'
        }
    }
    Test-Case 'Duplicate rows fail' {
        $root = New-Fixture
        $path = Join-Path $root 'head/micro-results/fixture.csv'
        (Get-Content $path -Raw).Replace('B,100 ns', 'A,100 ns') | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'head')
        Assert-Fails { Invoke-FixtureComparison $root } 'Duplicate row'
    }
    foreach ($role in @('base', 'head')) {
        Test-Case "Required $role row missing with unchanged executed count fails" {
            $root = New-Fixture
            $path = Join-Path $root "$role/micro-results/fixture.csv"
            (Get-Content $path -Raw).Replace('B,100 ns', 'C,100 ns') | Set-Content $path
            Refresh-FixtureHashes (Join-Path $root $role)
            $path = Join-Path $root 'thresholds.json'
            $policy = Get-Content $path -Raw | ConvertFrom-Json
            $policy.checks[0] | Add-Member requiredRows @([pscustomobject]@{ Method = 'B' })
            $policy | ConvertTo-Json -Depth 8 | Set-Content $path
            Assert-Fails { Invoke-FixtureComparison $root } 'integrity failed'
        }
    }
    Test-Case 'Zero rows fail' {
        $root = New-Fixture
        'Method,Mean,Allocated' | Set-Content (Join-Path $root 'head/micro-results/fixture.csv')
        Refresh-FixtureHashes (Join-Path $root 'head')
        Assert-Fails { Invoke-FixtureComparison $root } 'zero rows'
    }
    Test-Case 'Zero executed cases fail' {
        $root = New-Fixture
        $path = Join-Path $root 'base/manifest.json'
        $manifest = Get-Content $path -Raw | ConvertFrom-Json
        $manifest.runs[0].executedCases = 0
        $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'zero executed cases'
    }
    Test-Case 'Positive declared count cannot hide zero micro execution' {
        $root = New-Fixture
        'Global total time: 00:00:01, executed benchmarks: 0' | Set-Content (Join-Path $root 'base/logs/run.log')
        Refresh-FixtureHashes (Join-Path $root 'base')
        Assert-Fails { Invoke-FixtureComparison $root } 'did not execute any'
    }
    Test-Case 'Missing commit provenance fails' {
        $root = New-Fixture
        $path = Join-Path $root 'base/manifest.json'
        $manifest = Get-Content $path -Raw | ConvertFrom-Json
        $manifest.productCommit = ''
        $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'invalid commit'
    }
    Test-Case 'Empty fingerprint fields fail' {
        $root = New-Fixture
        $path = Join-Path $root 'base/machine.json'
        $machine = Get-Content $path -Raw | ConvertFrom-Json
        $machine.cpuName = ''
        $machine | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'base')
        Assert-Fails { Invoke-FixtureComparison $root } 'fingerprint is missing cpuName'
    }
    Test-Case 'All-skipped results fail report mode' {
        $root = New-Fixture
        $path = Join-Path $root 'base/machine.json'
        $machine = Get-Content $path -Raw | ConvertFrom-Json
        $machine.cpuName = 'different synthetic CPU'
        $machine | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'base')
        Assert-Fails { Invoke-FixtureComparison $root } 'ENFORCEABLE=0'
    }
    Test-Case 'Compatible non-authoritative results cannot satisfy coverage' {
        $root = New-Fixture
        $path = Join-Path $root 'base/machine.json'
        $machine = Get-Content $path -Raw | ConvertFrom-Json
        $machine.machineName = 'different-fixture-machine'
        $machine.runnerId = ''
        $machine | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'base')
        Assert-Fails { Invoke-FixtureComparison $root } 'ENFORCEABLE=0'
    }
    Test-Case 'Runtime mismatch fails report mode' {
        $root = New-Fixture
        $path = Join-Path $root 'base/runtime.json'
        $runtime = Get-Content $path -Raw | ConvertFrom-Json
        $runtime.runtimeVersion = '10.0.11'
        $runtime | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $root 'base')
        Assert-Fails { Invoke-FixtureComparison $root } 'runtime identity mismatch'
    }
    Test-Case 'Required rows and explicit coverage cannot be empty' {
        $root = New-Fixture
        $path = Join-Path $root 'thresholds.json'
        $policy = Get-Content $path -Raw | ConvertFrom-Json
        $policy.checks[0] | Add-Member requiredRows @()
        $policy | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'ENFORCEABLE=0'
    }
    Test-Case 'One passing check cannot conceal an unenforceable check' {
        $root = New-Fixture
        $other = New-Fixture
        $path = Join-Path $other 'base/machine.json'
        $machine = Get-Content $path -Raw | ConvertFrom-Json
        $machine.cpuName = 'different synthetic CPU'
        $machine | ConvertTo-Json | Set-Content $path
        Refresh-FixtureHashes (Join-Path $other 'base')
        $path = Join-Path $root 'thresholds.json'
        [ordered]@{ baselineSnapshot = (Join-Path $root 'base'); checks = @(
            [ordered]@{ csv = 'fixture.csv'; keyColumns = @('Method'); baselineSnapshot = (Join-Path $root 'base') },
            [ordered]@{ csv = 'fixture.csv'; keyColumns = @('Method'); baselineSnapshot = (Join-Path $other 'base') }
        ) } | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails {
            & (Join-Path $scriptDir 'Compare-Baseline.ps1') -CurrentSnapshot (Join-Path $root 'head') -ThresholdsPath $path -NoFailOnRegression
        } 'INVALID=1, ENFORCEABLE=2'
    }
    Test-Case 'Checks without comparison metrics fail' {
        $root = New-Fixture
        $path = Join-Path $root 'thresholds.json'
        $policy = Get-Content $path -Raw | ConvertFrom-Json
        $policy.checks[0] | Add-Member skipMeanComparison $true
        $policy.checks[0] | Add-Member skipAllocationComparison $true
        $policy | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'no comparison metrics'
    }
    Test-Case 'ExtraInfo missing or malformed fails report mode' {
        $root = New-Fixture
        $path = Join-Path $root 'thresholds.json'
        $policy = Get-Content $path -Raw | ConvertFrom-Json
        $policy.checks[0] | Add-Member extraInfoChecks @([pscustomobject]@{ key = 'absent'; minRatio = 0.8 })
        $policy | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'ExtraInfo absent missing'
    }
    Test-Case 'Artifact tampering fails' {
        $root = New-Fixture
        'tampered' | Add-Content (Join-Path $root 'base/logs/run.log')
        Assert-Fails { Invoke-FixtureComparison $root } 'hash mismatch'
    }
    Test-Case 'Referenced CSV absent from manifest fails' {
        $root = New-Fixture
        $path = Join-Path $root 'base/manifest.json'
        $manifest = Get-Content $path -Raw | ConvertFrom-Json
        $manifest.artifacts = @($manifest.artifacts | Where-Object path -ne 'micro-results/fixture.csv')
        $manifest | ConvertTo-Json -Depth 12 | Set-Content $path
        Assert-Fails { Invoke-FixtureComparison $root } 'absent from snapshot manifest'
    }
    Test-Case 'Paired comparison requires both orders and four distinct snapshots' {
        $root = New-Fixture
        $paths = @('pair-1/base', 'pair-1/head', 'pair-2/base', 'pair-2/head')
        foreach ($path in $paths) {
            $source = Join-Path $root ($path.Split('/')[1])
            New-Item -ItemType Directory -Path (Split-Path -Parent (Join-Path $root $path)) -Force | Out-Null
            Copy-Item -LiteralPath $source -Destination (Join-Path $root $path) -Recurse
        }
        $manifest = Get-Content (Join-Path $root 'base/manifest.json') -Raw | ConvertFrom-Json
        $plan = [pscustomobject]@{
            schemaVersion = 1; baseCommit = $fixtureCommit; headCommit = $fixtureCommit; harnessIdentity = $manifest.harnessIdentity
            thresholdSha256 = (Get-FileHash (Join-Path $root 'thresholds.json')).Hash.ToLowerInvariant()
            pairs = @([pscustomobject]@{ pair = 1; order = 'base-head'; base = $paths[0]; head = $paths[1] }, [pscustomobject]@{ pair = 2; order = 'head-base'; base = $paths[2]; head = $paths[3] })
        }
        @("1,base-head,base,$fixtureCommit", "1,base-head,head,$fixtureCommit", "2,head-base,head,$fixtureCommit", "2,head-base,base,$fixtureCommit") | Set-Content (Join-Path $root 'execution-order.csv')
        foreach ($name in @('base-build.log', 'head-build.log', 'base-build-inputs.sha256', 'head-build-inputs.sha256')) { 'Synthetic fixture' | Set-Content (Join-Path $root $name) }
        $artifacts = @(@('thresholds.json', 'execution-order.csv', 'base-build.log', 'head-build.log', 'base-build-inputs.sha256', 'head-build-inputs.sha256') | ForEach-Object {
            [pscustomobject]@{ path = $_; sha256 = (Get-FileHash -LiteralPath (Join-Path $root $_) -Algorithm SHA256).Hash.ToLowerInvariant() }
        })
        $plan | Add-Member artifacts $artifacts
        $path = Join-Path $root 'paired.json'
        $plan | ConvertTo-Json -Depth 8 | Set-Content $path
        & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression
        $snapshotManifestPath = Join-Path $root 'pair-2/head/manifest.json'
        $snapshotManifest = Get-Content $snapshotManifestPath -Raw | ConvertFrom-Json
        $snapshotManifest.runs[0].arguments = @('--different-workload')
        $snapshotManifest | ConvertTo-Json -Depth 12 | Set-Content $snapshotManifestPath
        Assert-Fails { & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression } 'identical workloads'
        $report = Get-Content -LiteralPath (Join-Path $root 'report.md') -Raw
        if ($report -notmatch 'Status: INVALID' -or $report -match 'PASS=') { throw 'Invalid paired evidence retained a successful summary.' }
        if (Test-Path -LiteralPath (Join-Path $root 'pair-2-report.md')) { throw 'An unexecuted recheck retained a successful second-order report.' }
        $snapshotManifest.runs[0].arguments = @('--micro')
        $snapshotManifest | ConvertTo-Json -Depth 12 | Set-Content $snapshotManifestPath
        $buildLog = Join-Path $root 'head-build.log'
        Remove-Item -LiteralPath $buildLog
        Assert-Fails { & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression } 'Paired artifact missing'
        if ((Get-Content -LiteralPath (Join-Path $root 'report.md') -Raw) -notmatch 'Status: INVALID') { throw 'Preflight failure did not invalidate the paired summary.' }
        if (Test-Path -LiteralPath (Join-Path $root 'pair-1-report.md')) { throw 'Preflight failure retained an earlier pair report.' }
        'Synthetic fixture' | Set-Content $buildLog
        $plan.pairs[1].head = $paths[1]
        $plan | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression } 'snapshot cannot be reused'
        $plan.pairs[1].head = $paths[3]
        'Method,Mean,Allocated' | Set-Content (Join-Path $root 'pair-2/head/micro-results/fixture.csv')
        Refresh-FixtureHashes (Join-Path $root 'pair-2/head')
        $plan | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression } 'zero rows'
        $plan.pairs = @($plan.pairs[0])
        $plan | ConvertTo-Json -Depth 8 | Set-Content $path
        Assert-Fails { & (Join-Path $scriptDir 'Compare-PairedPerfGuardrails.ps1') -OutputPath $root -NoFailOnRegression } 'Both balanced execution orders'
    }
    Test-Case 'Real paired driver synchronizes harness, captures ABBA, and rejects zero execution' {
        $root = Join-Path $testRoot 'driver'
        foreach ($role in @('base', 'head')) {
            $repo = Join-Path $root $role
            $bench = Join-Path $repo 'tests/CSharpDB.Benchmarks'
            New-Item -ItemType Directory -Path $bench -Force | Out-Null
            '// obsolete synthetic harness' | Set-Content (Join-Path $bench 'Program.cs')
            '<Project />' | Set-Content (Join-Path $bench 'CSharpDB.Benchmarks.csproj')
            @('**/bin/', '**/obj/', '**/results/', '**/BenchmarkDotNet.Artifacts/') | Set-Content (Join-Path $repo '.gitignore')
            & git -C $repo init --quiet
            & git -C $repo add .
            & git -C $repo -c user.name=PerfFixture -c user.email=fixture@example.invalid commit --quiet -m 'Synthetic test fixture'
            if ($LASTEXITCODE -ne 0) { throw 'Could not initialize synthetic product checkout.' }
        }
        $csvName = 'CSharpDB.Benchmarks.Micro.PointLookupGuardrailBenchmarks-report.csv'
        [ordered]@{ checks = @(
            [ordered]@{ csv = $csvName; keyColumns = @('Method'); minimumEnforceableComparisons = 1 },
            [ordered]@{ csv = 'suite.csv'; keyColumns = @('Method'); suiteKey = 'fixture-suite'; suiteArgs = @('--fixture-suite'); outputPattern = 'suite-*.csv'; currentResultsDir = 'tests/CSharpDB.Benchmarks/results/suite'; baselineSubDir = 'macro-stress-scaling'; minimumEnforceableComparisons = 1 }
        ) } | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $root 'policy.json')
        $global:PerfFixtureZero = $false
        $global:PerfFixtureInvocations = [Collections.Generic.List[string]]::new()
        $global:PerfFixtureBuildDirectories = [Collections.Generic.List[string]]::new()
        $global:PerfFixtureRuntimeDirectories = [Collections.Generic.List[string]]::new()
        $global:PerfFixtureSdkDirectories = [Collections.Generic.List[string]]::new()
        function dotnet {
            $global:LASTEXITCODE = 0
            if ($args -contains '--version') { $global:PerfFixtureSdkDirectories.Add($PWD.Path); return '10.0.401' }
            if ($args -contains '--perf-environment') { $global:PerfFixtureRuntimeDirectories.Add($PWD.Path); return ($fixtureRuntime | ConvertTo-Json -Compress) }
            if ($args[0] -eq 'build') { $global:PerfFixtureBuildDirectories.Add($PWD.Path); return 'Synthetic build accepted' }
            $projectIndex = [Array]::IndexOf($args, '--project')
            $bench = Split-Path -Parent $args[$projectIndex + 1]
            $global:PerfFixtureInvocations.Add($bench)
            if ($args -contains '--micro') {
                $artifacts = Join-Path $bench 'BenchmarkDotNet.Artifacts/results'
                New-Item -ItemType Directory -Path $artifacts -Force | Out-Null
                @('Method,Mean,Allocated', 'A,100 ns,100 B') | Set-Content (Join-Path $artifacts $csvName)
                return "Global total time: 00:00:01, executed benchmarks: $(if ($global:PerfFixtureZero) { 0 } else { 1 })"
            }
            $artifacts = Join-Path $bench 'bin/Release/net10.0/results'
            New-Item -ItemType Directory -Path $artifacts -Force | Out-Null
            @('Method,Mean,Allocated', 'A,100 ns,100 B') | Set-Content (Join-Path $artifacts ('suite-' + [Guid]::NewGuid().ToString('N') + '.csv'))
            return 'Synthetic suite executed'
        }
        try {
            & (Join-Path $scriptDir 'Run-PairedPerfGuardrails.ps1') -BaseRoot (Join-Path $root 'base') -HeadRoot (Join-Path $root 'head') -OutputPath (Join-Path $root 'evidence') -ThresholdsPath (Join-Path $root 'policy.json') -NoFailOnRegression
            $roles = @($global:PerfFixtureInvocations | ForEach-Object { if ($_ -like '*driver\base\*' -or $_ -like '*driver/base/*') { 'base' } else { 'head' } })
            if (($roles -join ',') -ne 'base,base,head,head,head,head,base,base') { throw "Incorrect real driver execution order: $($roles -join ',')" }
            if (($global:PerfFixtureBuildDirectories -join ',') -cne (@((Join-Path $root 'base'), (Join-Path $root 'head')) -join ',')) { throw 'Product builds did not select SDKs from their own directories.' }
            $captureDirectories = @(@('base', 'head', 'head', 'base') | ForEach-Object { Join-Path $root "$_/tests/CSharpDB.Benchmarks" })
            if (($global:PerfFixtureRuntimeDirectories -join ',') -cne ($captureDirectories -join ',')) { throw 'Runtime probes did not use each product SDK context.' }
            if (($global:PerfFixtureSdkDirectories -join ',') -cne ($captureDirectories -join ',')) { throw 'Fingerprint SDK capture did not use each product SDK context.' }
            $plan = Get-Content (Join-Path $root 'evidence/paired.json') -Raw | ConvertFrom-Json
            if ($plan.pairs.Count -ne 2) { throw 'Driver did not persist both orders.' }
            $global:PerfFixtureZero = $true
            $runner = Join-Path $root 'base/tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1'
            Assert-Fails { & $runner -ThresholdsPath (Join-Path $root 'policy.json') -SnapshotOutputPath (Join-Path $root 'zero-evidence') -NoFailOnRegression -CaptureOnly -NoBuild } 'did not execute any benchmark cases'
        }
        finally {
            Remove-Item function:dotnet
            Remove-Variable PerfFixtureZero,PerfFixtureInvocations,PerfFixtureBuildDirectories,PerfFixtureRuntimeDirectories,PerfFixtureSdkDirectories -Scope Global
        }
    }
}
finally {
    $tempPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not [IO.Path]::GetFullPath($testRoot).StartsWith($tempPrefix, [StringComparison]::OrdinalIgnoreCase)) { throw 'Test cleanup escaped temporary directory.' }
    Remove-Item -LiteralPath $testRoot -Recurse -Force
}
Write-Host "Guardrail contract tests: $script:passed passed, $($script:failed.Count) failed."
if ($script:failed.Count -gt 0) { throw ($script:failed -join "`n") }
