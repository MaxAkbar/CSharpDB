using System.Diagnostics;

namespace CSharpDB.Daemon.Tests;

public sealed class PreviousReleasePerformanceScriptTests
{
    [Fact]
    public async Task Comparer_AcceptsMatchingResultsWithinLimits()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            File.WriteAllLines(
                Path.Combine(baseline, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,100,10"]);
            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,90,12"]);

            ProcessResult result = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(
                    FindRepoRoot(),
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Compare-ReleaseCore.ps1"),
                "-BaselineResultsPath",
                baseline,
                "-CandidateResultsPath",
                candidate,
                "-ReportPath",
                report);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **PASS**", contents);
            Assert.Contains("| suite | lookup | 10.00% | 20.00% | PASS |", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_AllowsRelativeP99RegressionWithinAbsoluteAllowance()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            File.WriteAllLines(
                Path.Combine(baseline, "suite.csv"),
                ["Name,OpsPerSec,P99", "hot-read,41557.5,0.0328"]);
            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "hot-read,43518.0,0.0503"]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **PASS**", contents);
            Assert.Contains(
                "- P99 absolute regression allowance: 0.0500 ms",
                contents);
            Assert.Contains(
                "| suite | hot-read | -4.72% | 53.35% | PASS | " +
                "P99 increased by 0.0175 ms, which did not exceed the " +
                "0.0500 ms absolute allowance. |",
                contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RequiresRelativeAndAbsoluteP99LimitsToBeExceeded()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string boundaryReport = Path.Combine(temporaryRoot, "boundary.md");
            string failureReport = Path.Combine(temporaryRoot, "failure.md");
            File.WriteAllLines(
                Path.Combine(baseline, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,100,0.0321"]);
            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,100,0.0821"]);

            ProcessResult boundary = await RunComparerAsync(
                baseline,
                candidate,
                boundaryReport);

            Assert.True(boundary.ExitCode == 0, boundary.CombinedOutput);
            Assert.Contains(
                "| suite | lookup | 0.00% | 155.76% | PASS | " +
                "P99 increased by 0.0500 ms, which did not exceed the " +
                "0.0500 ms absolute allowance. |",
                File.ReadAllText(boundaryReport));

            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,100,0.0822"]);
            ProcessResult failure = await RunComparerAsync(
                baseline,
                candidate,
                failureReport);

            Assert.NotEqual(0, failure.ExitCode);
            Assert.Contains(
                "| suite | lookup | 0.00% | 156.07% | FAIL | " +
                "P99 increased by 0.0501 ms, which exceeded the " +
                "0.0500 ms absolute allowance. |",
                File.ReadAllText(failureReport));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_ZeroAbsoluteP99AllowanceRestoresPercentageOnlyGate()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            File.WriteAllLines(
                Path.Combine(baseline, "suite.csv"),
                ["Name,OpsPerSec,P99", "hot-read,41557.5,0.0328"]);
            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "hot-read,43518.0,0.0503"]);

            ProcessResult result = await RunComparerAsync(
                baseline,
                candidate,
                report,
                "-MaxP99RegressionMilliseconds",
                "0");

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(
                "| suite | hot-read | -4.72% | 53.35% | FAIL |",
                File.ReadAllText(report));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RejectsResultSetDriftAndNonFiniteMetrics()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            File.WriteAllLines(
                Path.Combine(baseline, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,100,10"]);
            File.WriteAllLines(
                Path.Combine(candidate, "suite.csv"),
                ["Name,OpsPerSec,P99", "lookup,NaN,10", "candidate-only,100,10"]);

            ProcessResult result = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(
                    FindRepoRoot(),
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Compare-ReleaseCore.ps1"),
                "-BaselineResultsPath",
                baseline,
                "-CandidateResultsPath",
                candidate,
                "-ReportPath",
                report);

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **FAIL**", contents);
            Assert.Contains("Baseline row is missing.", contents);
            Assert.Contains("missing or invalid", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task WrapperPreflight_ValidatesCleanRepositoryAndSecondPassOrder()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string sourceRoot = Path.Combine(temporaryRoot, "repository");
            string benchmarkRoot = Path.Combine(sourceRoot, "tests", "CSharpDB.Benchmarks");
            string scriptRoot = Path.Combine(benchmarkRoot, "scripts");
            Directory.CreateDirectory(scriptRoot);

            string repositoryRoot = FindRepoRoot();
            File.Copy(
                Path.Combine(
                    repositoryRoot,
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Compare-ReleaseCore.ps1"),
                Path.Combine(scriptRoot, "Compare-ReleaseCore.ps1"));
            File.Copy(
                Path.Combine(
                    repositoryRoot,
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Test-PreviousReleasePerformance.ps1"),
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"));
            File.WriteAllText(
                Path.Combine(benchmarkRoot, "CSharpDB.Benchmarks.csproj"),
                "<Project Sdk=\"Microsoft.NET.Sdk\" />");
            string trackedFile = Path.Combine(sourceRoot, "release.txt");
            File.WriteAllText(trackedFile, "previous");

            await AssertProcessSucceeded("git", "-C", sourceRoot, "init");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.email", "test@example.invalid");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.name", "CSharpDB Tests");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "commit.gpgsign", "false");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", ".");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "previous release");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.3.0");
            File.WriteAllText(trackedFile, "candidate");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", "release.txt");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "candidate");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.4.0");

            string evidence = Path.Combine(temporaryRoot, "evidence");
            ProcessResult result = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "2",
                "-PreflightOnly");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string preflight = File.ReadAllText(
                Path.Combine(evidence, "previous-release-performance-preflight.md"));
            Assert.Contains("- Result: **PASS**", preflight);
            Assert.Contains("- Execution strategy: suite-interleaved", preflight);
            Assert.Contains(
                "- Revision order within each suite: candidate then previous",
                preflight);
            Assert.Contains(
                "- Suite order: master-table, durable-sql-batching, " +
                "concurrent-write-diagnostics, hybrid-storage-mode, " +
                "hybrid-hot-set-read, hybrid-cold-open, sqlite-compare",
                preflight);
            Assert.Contains(
                "- Execution order: master-table/candidate, " +
                "master-table/previous, durable-sql-batching/candidate, " +
                "durable-sql-batching/previous",
                preflight);
            Assert.Contains(
                "- P99 absolute regression allowance: 0.0500 ms",
                preflight);
            Assert.Contains(
                "- P99 failure rule: relative and absolute limits must both be exceeded",
                preflight);
            Assert.Contains("- Planned execution log: `", preflight);
            Assert.Contains("- Previous ref: `v4.3.0`", preflight);
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));

            File.AppendAllText(trackedFile, Environment.NewLine + "dirty");
            ProcessResult dirtyResult = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "dirty-evidence"),
                "-PreflightOnly");

            Assert.NotEqual(0, dirtyResult.ExitCode);
            Assert.Contains("requires a clean repository worktree", dirtyResult.CombinedOutput);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Wrapper_ExecutesInterleavedSuitesAndRejectsDuplicateMedians()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string sourceRoot = Path.Combine(temporaryRoot, "repository");
            string benchmarkRoot = Path.Combine(sourceRoot, "tests", "CSharpDB.Benchmarks");
            string scriptRoot = Path.Combine(benchmarkRoot, "scripts");
            Directory.CreateDirectory(scriptRoot);

            string repositoryRoot = FindRepoRoot();
            File.Copy(
                Path.Combine(
                    repositoryRoot,
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Compare-ReleaseCore.ps1"),
                Path.Combine(scriptRoot, "Compare-ReleaseCore.ps1"));
            File.Copy(
                Path.Combine(
                    repositoryRoot,
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    "Test-PreviousReleasePerformance.ps1"),
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"));
            File.WriteAllText(
                Path.Combine(benchmarkRoot, "CSharpDB.Benchmarks.csproj"),
                "<Project Sdk=\"Microsoft.NET.Sdk\" />");
            string trackedFile = Path.Combine(sourceRoot, "release.txt");
            File.WriteAllText(trackedFile, "previous");

            await AssertProcessSucceeded("git", "-C", sourceRoot, "init");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.email", "test@example.invalid");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.name", "CSharpDB Tests");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "commit.gpgsign", "false");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", ".");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "previous release");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.3.0");
            File.WriteAllText(trackedFile, "candidate");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", "release.txt");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "candidate");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.4.0");

            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreateFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
            };

            string evidence = Path.Combine(temporaryRoot, "evidence");
            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "2");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            Assert.Contains(
                "- Result: **PASS**",
                File.ReadAllText(Path.Combine(evidence, "previous-release-performance.md")));
            Assert.Equal(
                7,
                Directory.GetFiles(Path.Combine(evidence, "baseline-results"), "*.csv").Length);
            Assert.Equal(
                7,
                Directory.GetFiles(Path.Combine(evidence, "candidate-results"), "*.csv").Length);
            Assert.True(File.Exists(Path.Combine(evidence, "logs", "previous-release.log")));
            Assert.True(File.Exists(Path.Combine(evidence, "logs", "candidate.log")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));

            string[] invocations = File.ReadAllLines(invocationLog);
            Assert.Equal(2, invocations.Count(line => line.Contains("|build", StringComparison.Ordinal)));
            string[] runs = invocations
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(14, runs.Length);
            Assert.Collection(
                runs.Take(4),
                line => Assert.Contains("candidate-source|run|master-table|repeat=3", line),
                line => Assert.Contains("baseline-source|run|master-table|repeat=3", line),
                line => Assert.Contains("candidate-source|run|durable-sql-batching|repeat=3", line),
                line => Assert.Contains("baseline-source|run|durable-sql-batching|repeat=3", line));

            string[] executionEvents = File.ReadAllLines(
                Path.Combine(evidence, "logs", "execution-order.log"));
            Assert.Equal(29, executionEvents.Length);
            Assert.Equal(14, executionEvents.Count(line => line.Contains("|START|", StringComparison.Ordinal)));
            Assert.Equal(14, executionEvents.Count(line => line.Contains("|PASS|", StringComparison.Ordinal)));
            Assert.DoesNotContain(
                executionEvents,
                line => line.Contains("|FAIL|", StringComparison.Ordinal));

            string duplicateEvidence = Path.Combine(temporaryRoot, "duplicate-evidence");
            environment["FAKE_DOTNET_DUPLICATE_SUITE"] = "master-table";
            ProcessResult duplicate = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                duplicateEvidence,
                "-QualificationPass",
                "2");

            Assert.NotEqual(0, duplicate.ExitCode);
            Assert.Contains(
                "produced 2 median CSV file(s); expected exactly one",
                duplicate.CombinedOutput);
            Assert.False(Directory.Exists(Path.Combine(duplicateEvidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(duplicateEvidence, "candidate-source")));
            Assert.Contains(
                File.ReadAllLines(Path.Combine(duplicateEvidence, "logs", "execution-order.log")),
                line => line.Contains(
                    "|1|master-table|candidate|FAIL|",
                    StringComparison.Ordinal));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    private static async Task AssertProcessSucceeded(string fileName, params string[] arguments)
    {
        ProcessResult result = await RunProcessAsync(fileName, arguments);
        Assert.True(result.ExitCode == 0, result.CombinedOutput);
    }

    private static void CreateFakeDotnetTool(string toolRoot)
    {
        Directory.CreateDirectory(toolRoot);
        string fakeScript = Path.Combine(toolRoot, "fake-dotnet.ps1");
        File.WriteAllText(
            fakeScript,
            """
            $ErrorActionPreference = 'Stop'
            $CommandArgs = @($args)
            if ($CommandArgs.Count -eq 1 -and $CommandArgs[0] -eq '--version') {
                Write-Output '10.0.203'
                exit 0
            }

            $command = $CommandArgs[0]
            $sourceName = Split-Path -Leaf (Get-Location).Path
            if ($command -eq 'build') {
                Add-Content -LiteralPath $env:FAKE_DOTNET_LOG -Value "$sourceName|build"
                Write-Output "Fake build: $sourceName"
                exit 0
            }
            if ($command -ne 'run') {
                Write-Error "Unsupported fake dotnet command: $command"
                exit 1
            }

            $suiteMap = @{
                '--master-table' = 'master-table'
                '--durable-sql-batching' = 'durable-sql-batching'
                '--concurrent-write-diagnostics' = 'concurrent-write-diagnostics'
                '--hybrid-storage-mode' = 'hybrid-storage-mode'
                '--hybrid-hot-set-read' = 'hybrid-hot-set-read'
                '--hybrid-cold-open' = 'hybrid-cold-open'
                '--sqlite-compare' = 'sqlite-compare'
            }
            $suiteArguments = @(
                $CommandArgs | Where-Object { $suiteMap.ContainsKey($_) }
            )
            if ($suiteArguments.Count -ne 1) {
                Write-Error "Expected one release-core suite argument."
                exit 1
            }
            $suiteName = $suiteMap[$suiteArguments[0]]
            $repeatIndex = [Array]::IndexOf[string]($CommandArgs, '--repeat')
            if ($repeatIndex -lt 0 -or $repeatIndex + 1 -ge $CommandArgs.Count) {
                Write-Error 'Missing --repeat value.'
                exit 1
            }
            $repeatCount = $CommandArgs[$repeatIndex + 1]
            $resultRoot = Join-Path `
                (Get-Location).Path `
                'tests/CSharpDB.Benchmarks/bin/Release/net10.0/results'
            New-Item -ItemType Directory -Path $resultRoot -Force | Out-Null
            $resultPath = Join-Path `
                $resultRoot `
                "$suiteName-smoke-median-of-$repeatCount.csv"
            [IO.File]::WriteAllLines(
                $resultPath,
                @(
                    'Name,OpsPerSec,P99',
                    "$suiteName-row,100,1"
                ))
            if ($env:FAKE_DOTNET_DUPLICATE_SUITE -eq $suiteName) {
                $duplicatePath = Join-Path `
                    $resultRoot `
                    "$suiteName-smoke-extra-median-of-$repeatCount.csv"
                [IO.File]::WriteAllLines(
                    $duplicatePath,
                    @(
                        'Name,OpsPerSec,P99',
                        "$suiteName-row,100,1"
                    ))
            }
            Add-Content `
                -LiteralPath $env:FAKE_DOTNET_LOG `
                -Value "$sourceName|run|$suiteName|repeat=$repeatCount"
            Write-Output "Fake run: $sourceName/$suiteName"
            """);

        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(
                Path.Combine(toolRoot, "dotnet.cmd"),
                """
                @echo off
                pwsh -NoLogo -NoProfile -File "%~dp0fake-dotnet.ps1" %*
                exit /b %ERRORLEVEL%
                """);
            return;
        }

        string launcher = Path.Combine(toolRoot, "dotnet");
        File.WriteAllText(
            launcher,
            """
            #!/usr/bin/env sh
            script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
            exec pwsh -NoLogo -NoProfile -File "${script_dir}/fake-dotnet.ps1" "$@"
            """);
        File.SetUnixFileMode(
            launcher,
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.UserExecute |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherExecute);
    }

    private static Task<ProcessResult> RunComparerAsync(
        string baseline,
        string candidate,
        string report,
        params string[] additionalArguments)
    {
        List<string> arguments =
        [
            "-NoLogo",
            "-NoProfile",
            "-File",
            Path.Combine(
                FindRepoRoot(),
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Compare-ReleaseCore.ps1"),
            "-BaselineResultsPath",
            baseline,
            "-CandidateResultsPath",
            candidate,
            "-ReportPath",
            report,
        ];
        arguments.AddRange(additionalArguments);
        return RunProcessAsync("pwsh", [.. arguments]);
    }

    private static Task<ProcessResult> RunProcessAsync(
        string fileName,
        params string[] arguments)
    {
        return RunProcessCoreAsync(
            fileName,
            environment: null,
            TimeSpan.FromSeconds(30),
            arguments);
    }

    private static Task<ProcessResult> RunProcessWithEnvironmentAsync(
        string fileName,
        IReadOnlyDictionary<string, string> environment,
        params string[] arguments)
    {
        return RunProcessCoreAsync(
            fileName,
            environment,
            TimeSpan.FromSeconds(60),
            arguments);
    }

    private static async Task<ProcessResult> RunProcessCoreAsync(
        string fileName,
        IReadOnlyDictionary<string, string>? environment,
        TimeSpan timeoutDuration,
        params string[] arguments)
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = fileName,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);
        if (environment is not null)
        {
            foreach ((string name, string value) in environment)
                startInfo.Environment[name] = value;
        }

        using Process process = new() { StartInfo = startInfo };
        Assert.True(process.Start(), $"Could not start {fileName}.");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = new(timeoutDuration);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"{fileName} did not finish within 30 seconds.");
        }

        return new ProcessResult(
            process.ExitCode,
            await standardOutput,
            await standardError);
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-previous-release-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTemporaryRoot(string path)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                foreach (string entry in Directory.EnumerateFileSystemEntries(
                    path,
                    "*",
                    SearchOption.AllDirectories))
                {
                    File.SetAttributes(entry, FileAttributes.Normal);
                }
                File.SetAttributes(path, FileAttributes.Normal);
                Directory.Delete(path, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 3)
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
            catch (UnauthorizedAccessException) when (attempt < 3)
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
        }
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError)
    {
        public string CombinedOutput =>
            StandardOutput + Environment.NewLine + StandardError;
    }
}
