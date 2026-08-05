using System.Diagnostics;

namespace CSharpDB.Daemon.Tests;

public sealed class LocalPerformanceEnvironmentMonitorTests
{
    [Fact]
    public void DurableRunner_BindsMonitorEvidenceToExactRowExecution()
    {
        string repoRoot = FindRepoRoot();
        string runner = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-PreviousReleasePerformance.ps1"));
        string wrapper = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-LocalDurablePerformance.ps1"));

        Assert.Contains("[switch] $MonitorLocalEnvironment", runner);
        Assert.Contains("Start-LocalEnvironmentMonitor", runner);
        Assert.Contains("Assert-LocalEnvironmentMonitorClean", runner);
        Assert.Contains("Stop-AndAuditLocalEnvironmentMonitor", runner);
        Assert.Contains("durable-v3-environment-monitor.csv", runner);
        Assert.Contains("ready signal and first sample", runner);
        Assert.Contains("MaxExternalCpuCoreEquivalent", runner);
        Assert.Contains("SystemResidualCpuCoreEquivalent", runner);
        Assert.Contains("ProhibitedExternalProcessNames", runner);
        Assert.Contains("measurement-begin-utc", runner);
        Assert.Contains("measurement-end-utc", runner);
        Assert.Contains("$parameters.MonitorLocalEnvironment = $true", wrapper);
        Assert.Contains("Watch-LocalPerformanceEnvironment.ps1", wrapper);
        Assert.Contains("design=$designToken", wrapper);
    }

    [Fact]
    public async Task WindowsMonitor_ProducesTimestampedCsvAndStopsBySignal()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-local-monitor-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temporaryRoot);
        try
        {
            string outputPath = Path.Combine(temporaryRoot, "monitor.csv");
            string readyPath = Path.Combine(temporaryRoot, "ready.signal");
            string stopPath = Path.Combine(temporaryRoot, "stop.signal");
            string scriptPath = Path.Combine(
                FindRepoRoot(),
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Watch-LocalPerformanceEnvironment.ps1");
            ProcessStartInfo startInfo = new()
            {
                FileName = "pwsh",
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            using Process currentProcess = Process.GetCurrentProcess();
            string currentProcessStartUtc = currentProcess.StartTime
                .ToUniversalTime()
                .ToString("O", System.Globalization.CultureInfo.InvariantCulture);
            foreach (string argument in new[]
            {
                "-NoLogo",
                "-NoProfile",
                "-File",
                scriptPath,
                "-OutputPath",
                outputPath,
                "-StopSignalPath",
                stopPath,
                "-ReadySignalPath",
                readyPath,
                "-AllowedRootProcessId",
                Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "-AllowedRootStartTimeUtc",
                currentProcessStartUtc,
                "-SampleIntervalMilliseconds",
                "250",
            })
            {
                startInfo.ArgumentList.Add(argument);
            }

            using Process process = Process.Start(startInfo) ??
                throw new InvalidOperationException("Could not start monitor smoke test.");
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
            while (!File.Exists(readyPath) && !process.HasExited)
                await Task.Delay(50, timeout.Token);

            if (process.HasExited)
            {
                string earlyError = await process.StandardError.ReadToEndAsync(timeout.Token);
                Assert.Fail($"The monitor exited before its ready signal. {earlyError}");
            }
            await Task.Delay(1_000, timeout.Token);
            DateTimeOffset stopRequestedUtc = DateTimeOffset.UtcNow;
            await File.WriteAllTextAsync(
                stopPath,
                stopRequestedUtc.ToString("O"),
                timeout.Token);
            await process.WaitForExitAsync(timeout.Token);

            string stderr = await process.StandardError.ReadToEndAsync(timeout.Token);
            Assert.True(process.ExitCode == 0, stderr);
            string[] lines = await File.ReadAllLinesAsync(outputPath, timeout.Token);
            Assert.True(lines.Length >= 2, "The monitor must retain at least one sample.");
            Assert.StartsWith("TimestampUtc,IntervalMilliseconds", lines[0], StringComparison.Ordinal);
            Assert.Contains("SystemResidualCpuCoreEquivalent", lines[0], StringComparison.Ordinal);
            string[] fields = lines[1].Split(',');
            Assert.True(fields.Length >= 11);
            Assert.True(DateTimeOffset.TryParse(fields[0].Trim('"'), out _));
            MonitorSample finalSample = ParseMonitorSample(lines[^1]);
            Assert.True(
                finalSample.TimestampUtc >= stopRequestedUtc,
                $"The final sample {finalSample.TimestampUtc:O} must cover the stop " +
                $"request at {stopRequestedUtc:O}.");
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task MonitorGatePolicy_UsesExactThresholdsAndStickyContamination()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string harnessPath = Path.Combine(temporaryRoot, "monitor-gate-harness.ps1");
            File.WriteAllText(harnessPath, MonitorGateHarness);
            ProcessStartInfo startInfo = CreatePwshStartInfo();
            foreach (string argument in new[]
            {
                "-File",
                harnessPath,
                "-MonitorPath",
                Path.Combine(
                    FindRepoRoot(),
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Watch-LocalPerformanceEnvironment.ps1"),
            })
            {
                startInfo.ArgumentList.Add(argument);
            }

            using Process process = Process.Start(startInfo) ??
                throw new InvalidOperationException("Could not start monitor gate harness.");
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await process.WaitForExitAsync(timeout.Token);
            string stdout = await process.StandardOutput.ReadToEndAsync(timeout.Token);
            string stderr = await process.StandardError.ReadToEndAsync(timeout.Token);

            Assert.True(process.ExitCode == 0, stdout + Environment.NewLine + stderr);
            Assert.Contains("Monitor gate policy: PASS", stdout, StringComparison.Ordinal);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData("stale", "The latest environment monitor sample is stale")]
    [InlineData("discontinuous", "Environment monitor coverage is discontinuous")]
    [InlineData("final-coverage", "Environment monitor stopped before the final recorded measurement ended")]
    public async Task DurableRunner_MonitorAuditRejectsInvalidCoverage(
        string scenario,
        string expectedDiagnostic)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string harnessPath = Path.Combine(temporaryRoot, "monitor-audit-harness.ps1");
            File.WriteAllText(harnessPath, MonitorAuditHarness);
            ProcessStartInfo startInfo = new()
            {
                FileName = "pwsh",
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            foreach (string argument in new[]
            {
                "-NoLogo",
                "-NoProfile",
                "-File",
                harnessPath,
                "-RunnerPath",
                Path.Combine(
                    FindRepoRoot(),
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Test-PreviousReleasePerformance.ps1"),
                "-Scenario",
                scenario,
                "-EvidenceRoot",
                Path.Combine(temporaryRoot, "evidence"),
            })
            {
                startInfo.ArgumentList.Add(argument);
            }

            using Process process = Process.Start(startInfo) ??
                throw new InvalidOperationException("Could not start monitor audit harness.");
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await process.WaitForExitAsync(timeout.Token);
            string stdout = await process.StandardOutput.ReadToEndAsync(timeout.Token);
            string stderr = await process.StandardError.ReadToEndAsync(timeout.Token);

            Assert.True(process.ExitCode == 0, stdout + Environment.NewLine + stderr);
            Assert.Contains(expectedDiagnostic, stdout, StringComparison.Ordinal);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task WindowsMonitor_ProhibitedExternalProcessMakesContaminationSticky()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = CreateTemporaryRoot();
        Process? allowedRoot = null;
        Process? prohibitedWorkload = null;
        Process? monitor = null;
        try
        {
            allowedRoot = StartSleepingPwsh();
            string outputPath = Path.Combine(temporaryRoot, "monitor.csv");
            string readyPath = Path.Combine(temporaryRoot, "ready.signal");
            string stopPath = Path.Combine(temporaryRoot, "stop.signal");
            monitor = StartMonitor(
                allowedRoot,
                outputPath,
                readyPath,
                stopPath,
                prohibitedExternalProcessNames: "pwsh");

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
            await WaitForReadyAsync(monitor, readyPath, timeout.Token);
            prohibitedWorkload = StartSleepingPwsh();
            await Task.Delay(750, timeout.Token);
            prohibitedWorkload.Kill(entireProcessTree: true);
            await prohibitedWorkload.WaitForExitAsync(timeout.Token);
            await Task.Delay(750, timeout.Token);
            await File.WriteAllTextAsync(stopPath, DateTimeOffset.UtcNow.ToString("O"), timeout.Token);
            await monitor.WaitForExitAsync(timeout.Token);

            string stderr = await monitor.StandardError.ReadToEndAsync(timeout.Token);
            Assert.True(monitor.ExitCode == 0, stderr);
            string[] samples = (await File.ReadAllLinesAsync(outputPath, timeout.Token))[1..];
            Assert.Contains(samples, static sample => sample.EndsWith(",\"True\"", StringComparison.Ordinal));
            Assert.EndsWith(",\"True\"", samples[^1], StringComparison.Ordinal);
        }
        finally
        {
            KillIfRunning(prohibitedWorkload);
            KillIfRunning(monitor);
            KillIfRunning(allowedRoot);
            prohibitedWorkload?.Dispose();
            monitor?.Dispose();
            allowedRoot?.Dispose();
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task WindowsMonitor_ExitsWhenAllowedRootIdentityDisappears()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = CreateTemporaryRoot();
        Process? allowedRoot = null;
        Process? monitor = null;
        try
        {
            allowedRoot = StartSleepingPwsh();
            string outputPath = Path.Combine(temporaryRoot, "monitor.csv");
            string readyPath = Path.Combine(temporaryRoot, "ready.signal");
            string stopPath = Path.Combine(temporaryRoot, "stop.signal");
            monitor = StartMonitor(
                allowedRoot,
                outputPath,
                readyPath,
                stopPath,
                prohibitedExternalProcessNames: "process-name-that-does-not-exist");

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
            await WaitForReadyAsync(monitor, readyPath, timeout.Token);
            allowedRoot.Kill(entireProcessTree: true);
            await allowedRoot.WaitForExitAsync(timeout.Token);
            await monitor.WaitForExitAsync(timeout.Token);

            string stderr = await monitor.StandardError.ReadToEndAsync(timeout.Token);
            Assert.NotEqual(0, monitor.ExitCode);
            Assert.Contains("allowed root process exited", stderr, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            KillIfRunning(monitor);
            KillIfRunning(allowedRoot);
            monitor?.Dispose();
            allowedRoot?.Dispose();
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-local-monitor-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static Process StartSleepingPwsh()
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = "pwsh",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("-NoLogo");
        startInfo.ArgumentList.Add("-NoProfile");
        startInfo.ArgumentList.Add("-Command");
        startInfo.ArgumentList.Add("Start-Sleep -Seconds 30");
        return Process.Start(startInfo) ??
            throw new InvalidOperationException("Could not start monitor helper process.");
    }

    private static ProcessStartInfo CreatePwshStartInfo()
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = "pwsh",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("-NoLogo");
        startInfo.ArgumentList.Add("-NoProfile");
        return startInfo;
    }

    private static Process StartMonitor(
        Process allowedRoot,
        string outputPath,
        string readyPath,
        string stopPath,
        string prohibitedExternalProcessNames,
        double maxExternalCpuPercent = 8,
        double maxExternalCpuCoreEquivalent = 0.5,
        long maxExternalIoBytesPerSecond = 4_194_304,
        int requiredConsecutiveBusySamples = 5)
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = "pwsh",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (string argument in new[]
        {
            "-NoLogo",
            "-NoProfile",
            "-File",
            Path.Combine(
                FindRepoRoot(),
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Watch-LocalPerformanceEnvironment.ps1"),
            "-OutputPath",
            outputPath,
            "-StopSignalPath",
            stopPath,
            "-ReadySignalPath",
            readyPath,
            "-AllowedRootProcessId",
            allowedRoot.Id.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "-AllowedRootStartTimeUtc",
            allowedRoot.StartTime.ToUniversalTime().ToString(
                "O",
                System.Globalization.CultureInfo.InvariantCulture),
            "-SampleIntervalMilliseconds",
            "250",
            "-ProhibitedExternalProcessNames",
            prohibitedExternalProcessNames,
            "-MaxExternalCpuPercent",
            maxExternalCpuPercent.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "-MaxExternalCpuCoreEquivalent",
            maxExternalCpuCoreEquivalent.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            "-MaxExternalIoBytesPerSecond",
            maxExternalIoBytesPerSecond.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            "-RequiredConsecutiveBusySamples",
            requiredConsecutiveBusySamples.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
        })
        {
            startInfo.ArgumentList.Add(argument);
        }

        return Process.Start(startInfo) ??
            throw new InvalidOperationException("Could not start local environment monitor.");
    }

    private static MonitorSample ParseMonitorSample(string line)
    {
        string[] fields = line[1..^1].Split("\",\"", StringSplitOptions.None);
        if (fields.Length != 16)
            throw new FormatException($"Expected 16 monitor fields, but found {fields.Length}.");
        return new MonitorSample(
            DateTimeOffset.Parse(
                fields[0],
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
            double.Parse(fields[2], System.Globalization.CultureInfo.InvariantCulture),
            double.Parse(fields[3], System.Globalization.CultureInfo.InvariantCulture),
            double.Parse(fields[5], System.Globalization.CultureInfo.InvariantCulture),
            double.Parse(fields[6], System.Globalization.CultureInfo.InvariantCulture),
            fields[13],
            int.Parse(fields[14], System.Globalization.CultureInfo.InvariantCulture),
            bool.Parse(fields[15]));
    }

    private static async Task WaitForReadyAsync(
        Process monitor,
        string readyPath,
        CancellationToken cancellationToken)
    {
        while (!File.Exists(readyPath) && !monitor.HasExited)
            await Task.Delay(50, cancellationToken);
        if (!File.Exists(readyPath))
        {
            string stderr = await monitor.StandardError.ReadToEndAsync(cancellationToken);
            Assert.Fail($"The monitor exited before its ready signal. {stderr}");
        }
    }

    private static void KillIfRunning(Process? process)
    {
        if (process is null)
            return;
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit();
            }
        }
        catch (InvalidOperationException)
        {
            // The exact helper already exited.
        }
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "CSharpDB.slnx")))
                return current.FullName;
            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root.");
    }

    private static void DeleteTemporaryRoot(string path)
    {
        if (!Directory.Exists(path))
            return;

        for (int attempt = 0; attempt < 10; attempt++)
        {
            try
            {
                Directory.Delete(path, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 9)
            {
                Thread.Sleep(100);
            }
            catch (UnauthorizedAccessException) when (attempt < 9)
            {
                Thread.Sleep(100);
            }
        }
    }

    private sealed record MonitorSample(
        DateTimeOffset TimestampUtc,
        double ExternalCpuPercent,
        double ExternalCpuCoreEquivalent,
        double ExternalReadBytesPerSecond,
        double ExternalWriteBytesPerSecond,
        string BusyReason,
        int ConsecutiveBusySamples,
        bool Contaminated);

    private const string MonitorGateHarness = """
        param([Parameter(Mandatory)][string] $MonitorPath)

        $ErrorActionPreference = 'Stop'
        $tokens = $null
        $parseErrors = $null
        $ast = [System.Management.Automation.Language.Parser]::ParseFile(
            $MonitorPath,
            [ref] $tokens,
            [ref] $parseErrors)
        if ($parseErrors.Count -ne 0) {
            throw "Monitor parse failed: $($parseErrors -join '; ')"
        }
        $functionAst = $ast.Find(
            {
                param($node)
                $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                    $node.Name -ceq 'Get-EnvironmentMonitorGateState'
            },
            $true)
        if ($null -eq $functionAst) {
            throw 'Monitor gate-state function was not found.'
        }
        Invoke-Expression $functionAst.Extent.Text

        function Invoke-Gate {
            param(
                [double] $ExternalCpuPercent = 0,
                [double] $ExternalCpuCoreEquivalent = 0,
                [double] $ExternalReadBytesPerSecond = 0,
                [double] $ExternalWriteBytesPerSecond = 0,
                [switch] $HasProhibitedExternalProcess,
                [int] $UnobservableAllowedCpuProcessCount = 0,
                [int] $PreviousConsecutiveBusySamples = 0,
                [switch] $PreviouslyContaminated)

            return Get-EnvironmentMonitorGateState `
                -ExternalCpuPercent $ExternalCpuPercent `
                -ExternalCpuCoreEquivalent $ExternalCpuCoreEquivalent `
                -ExternalReadBytesPerSecond $ExternalReadBytesPerSecond `
                -ExternalWriteBytesPerSecond $ExternalWriteBytesPerSecond `
                -HasProhibitedExternalProcess $HasProhibitedExternalProcess.IsPresent `
                -UnobservableAllowedCpuProcessCount $UnobservableAllowedCpuProcessCount `
                -PreviousConsecutiveBusySamples $PreviousConsecutiveBusySamples `
                -PreviouslyContaminated $PreviouslyContaminated.IsPresent `
                -MaximumExternalCpuPercent 8 `
                -MaximumExternalCpuCoreEquivalent 0.5 `
                -MaximumExternalIoBytesPerSecond 4194304 `
                -ConsecutiveBusySamplesRequired 2
        }

        function Assert-State {
            param(
                [string] $Name,
                $State,
                [string] $ExpectedBusyReason,
                [int] $ExpectedConsecutiveBusySamples,
                [bool] $ExpectedContaminated)

            if ([string] $State.BusyReason -cne $ExpectedBusyReason -or
                [int] $State.ConsecutiveBusySamples -ne
                    $ExpectedConsecutiveBusySamples -or
                [bool] $State.Contaminated -ne $ExpectedContaminated) {
                throw (
                    "$Name produced reason='$($State.BusyReason)', " +
                    "consecutive=$($State.ConsecutiveBusySamples), " +
                    "contaminated=$($State.Contaminated).")
            }
        }

        Assert-State `
            -Name 'exact boundaries' `
            -State (Invoke-Gate `
                -ExternalCpuPercent 8 `
                -ExternalCpuCoreEquivalent 0.5 `
                -ExternalReadBytesPerSecond 2097152 `
                -ExternalWriteBytesPerSecond 2097152) `
            -ExpectedBusyReason '' `
            -ExpectedConsecutiveBusySamples 0 `
            -ExpectedContaminated $false
        Assert-State `
            -Name 'CPU percent' `
            -State (Invoke-Gate -ExternalCpuPercent 8.0001) `
            -ExpectedBusyReason 'external-cpu' `
            -ExpectedConsecutiveBusySamples 1 `
            -ExpectedContaminated $false
        Assert-State `
            -Name 'CPU core first sample' `
            -State (Invoke-Gate -ExternalCpuCoreEquivalent 0.5001) `
            -ExpectedBusyReason 'external-cpu' `
            -ExpectedConsecutiveBusySamples 1 `
            -ExpectedContaminated $false
        Assert-State `
            -Name 'CPU core sustained' `
            -State (Invoke-Gate `
                -ExternalCpuCoreEquivalent 0.5001 `
                -PreviousConsecutiveBusySamples 1) `
            -ExpectedBusyReason 'external-cpu' `
            -ExpectedConsecutiveBusySamples 2 `
            -ExpectedContaminated $true
        Assert-State `
            -Name 'clean reset' `
            -State (Invoke-Gate -PreviousConsecutiveBusySamples 1) `
            -ExpectedBusyReason '' `
            -ExpectedConsecutiveBusySamples 0 `
            -ExpectedContaminated $false
        Assert-State `
            -Name 'sticky contamination' `
            -State (Invoke-Gate `
                -PreviousConsecutiveBusySamples 2 `
                -PreviouslyContaminated) `
            -ExpectedBusyReason '' `
            -ExpectedConsecutiveBusySamples 0 `
            -ExpectedContaminated $true
        Assert-State `
            -Name 'process I/O' `
            -State (Invoke-Gate -ExternalWriteBytesPerSecond 4194305) `
            -ExpectedBusyReason 'external-io' `
            -ExpectedConsecutiveBusySamples 1 `
            -ExpectedContaminated $false
        Assert-State `
            -Name 'prohibited process' `
            -State (Invoke-Gate -HasProhibitedExternalProcess) `
            -ExpectedBusyReason 'prohibited-process' `
            -ExpectedConsecutiveBusySamples 1 `
            -ExpectedContaminated $true
        Assert-State `
            -Name 'unobservable allowed CPU' `
            -State (Invoke-Gate -UnobservableAllowedCpuProcessCount 1) `
            -ExpectedBusyReason 'unobservable-allowed-cpu' `
            -ExpectedConsecutiveBusySamples 1 `
            -ExpectedContaminated $true

        Write-Output 'Monitor gate policy: PASS'
        """;

    private const string MonitorAuditHarness = """
        param(
            [Parameter(Mandatory)][string] $RunnerPath,
            [Parameter(Mandatory)][string] $Scenario,
            [Parameter(Mandatory)][string] $EvidenceRoot)

        $ErrorActionPreference = 'Stop'
        New-Item -ItemType Directory -Path $EvidenceRoot -Force | Out-Null
        $tokens = $null
        $parseErrors = $null
        $ast = [System.Management.Automation.Language.Parser]::ParseFile(
            $RunnerPath,
            [ref] $tokens,
            [ref] $parseErrors)
        if ($parseErrors.Count -ne 0) {
            throw "Runner parse failed: $($parseErrors -join '; ')"
        }
        foreach ($functionName in @(
                'Get-MaxEnvironmentMonitorGapMilliseconds',
                'Get-LatestEnvironmentMonitorRow',
                'Stop-AndAuditLocalEnvironmentMonitor')) {
            $functionAst = $ast.Find(
                {
                    param($node)
                    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                        $node.Name -ceq $functionName
                },
                $true)
            if ($null -eq $functionAst) {
                throw "Runner function not found: $functionName"
            }
            Invoke-Expression $functionAst.Extent.Text
        }

        function Write-LinesAtomically {
            param([string] $Path, [string[]] $Lines)
            [IO.File]::WriteAllLines($Path, $Lines)
        }

        $MonitorLocalEnvironment = $true
        $MonitorSampleIntervalMilliseconds = 1000
        $MaxExternalCpuPercent = 8
        $MaxExternalCpuCoreEquivalent = 0.5
        $MaxExternalIoBytesPerSecond = 4194304
        $RequiredConsecutiveBusySamples = 5
        $ProhibitedExternalProcessNames = 'none'
        $environmentMonitorStopped = $false
        $environmentMonitorSampleCount = 0
        $environmentMonitorSha256 = ''
        $environmentMonitorCsvPath = Join-Path $EvidenceRoot 'monitor.csv'
        $environmentMonitorStopPath = Join-Path $EvidenceRoot 'monitor.stop'
        $environmentMonitorStdoutPath = Join-Path $EvidenceRoot 'monitor.stdout.log'
        $environmentMonitorStderrPath = Join-Path $EvidenceRoot 'monitor.stderr.log'
        $environmentMonitorSummaryPath = Join-Path $EvidenceRoot 'monitor-summary.txt'
        $reportPath = Join-Path $EvidenceRoot 'absent-report.md'
        $baselineRawResults = Join-Path $EvidenceRoot 'baseline-raw'
        $candidateRawResults = Join-Path $EvidenceRoot 'candidate-raw'
        $isExactMasterDurableMode = $false
        $primaryFailure = $null
        $executionPlan = @()

        $reader = [pscustomobject]@{}
        $reader | Add-Member -MemberType ScriptMethod -Name ReadToEnd -Value { '' }
        $environmentMonitorProcess = [pscustomobject]@{
            HasExited = $false
            ExitCode = 0
            StandardOutput = $reader
            StandardError = $reader
        }
        $environmentMonitorProcess | Add-Member `
            -MemberType ScriptMethod `
            -Name WaitForExit `
            -Value { param([int] $Milliseconds) $this.HasExited = $true; return $true }
        $environmentMonitorProcess | Add-Member `
            -MemberType ScriptMethod `
            -Name Kill `
            -Value { param([bool] $EntireProcessTree) $this.HasExited = $true }

        $header =
            'TimestampUtc,IntervalMilliseconds,ExternalCpuPercent,' +
            'ExternalCpuCoreEquivalent,SystemResidualCpuCoreEquivalent,' +
            'ExternalReadBytesPerSecond,ExternalWriteBytesPerSecond,' +
            'ExternalProcessCount,AllowedProcessCount,' +
            'UnobservableAllowedCpuProcessCount,UnobservableExternalCpuProcessCount,' +
            'UnobservableExternalIoProcessCount,ProhibitedExternalProcesses,' +
            'BusyReason,ConsecutiveBusySamples,Contaminated'
        function New-MonitorRow([DateTimeOffset] $Timestamp) {
            $values = @(
                $Timestamp.ToString('O'), '1000', '0', '0', '0', '0', '0',
                '1', '1', '0', '0', '0', '', '', '0', 'False')
            return '"' + (($values | ForEach-Object { ([string] $_).Replace('"', '""') }) -join '","') + '"'
        }

        $environmentMonitorReadyUtc = [DateTimeOffset]::UtcNow.AddSeconds(-20)
        switch ($Scenario) {
            'stale' {
                [IO.File]::WriteAllLines(
                    $environmentMonitorCsvPath,
                    @($header, (New-MonitorRow $environmentMonitorReadyUtc.AddSeconds(1))))
                try {
                    Get-LatestEnvironmentMonitorRow | Out-Null
                    throw 'Expected stale monitor evidence to fail.'
                }
                catch {
                    if ($_.Exception.Message -notlike '*latest environment monitor sample is stale*') {
                        throw
                    }
                    Write-Output $_.Exception.Message
                    exit 0
                }
            }
            'discontinuous' {
                [IO.File]::WriteAllLines(
                    $environmentMonitorCsvPath,
                    @(
                        $header,
                        (New-MonitorRow $environmentMonitorReadyUtc.AddSeconds(1)),
                        (New-MonitorRow $environmentMonitorReadyUtc.AddSeconds(7))))
                $expected = 'Environment monitor coverage is discontinuous'
            }
            'final-coverage' {
                [IO.File]::WriteAllLines(
                    $environmentMonitorCsvPath,
                    @(
                        $header,
                        (New-MonitorRow $environmentMonitorReadyUtc.AddSeconds(1)),
                        (New-MonitorRow $environmentMonitorReadyUtc.AddSeconds(2))))
                New-Item -ItemType Directory -Path $baselineRawResults -Force | Out-Null
                $begin = $environmentMonitorReadyUtc.AddMilliseconds(500).ToString('O')
                $end = $environmentMonitorReadyUtc.AddSeconds(3).ToString('O')
                [IO.File]::WriteAllLines(
                    (Join-Path $baselineRawResults 'measurement.csv'),
                    @(
                        'ExtraInfo',
                        ('"measurement-begin-utc=' + $begin +
                            '; measurement-end-utc=' + $end + '"')))
                $expected = 'Environment monitor stopped before the final recorded measurement ended'
            }
            default { throw "Unknown scenario: $Scenario" }
        }

        try {
            Stop-AndAuditLocalEnvironmentMonitor
            throw "Expected monitor audit failure: $expected"
        }
        catch {
            if ($_.Exception.Message -notlike "*$expected*") {
                throw
            }
            Write-Output $_.Exception.Message
            exit 0
        }
        """;
}
