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
            Assert.Contains("- Run order: candidate then previous", preflight);
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

    private static async Task AssertProcessSucceeded(string fileName, params string[] arguments)
    {
        ProcessResult result = await RunProcessAsync(fileName, arguments);
        Assert.True(result.ExitCode == 0, result.CombinedOutput);
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
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

        using Process process = new() { StartInfo = startInfo };
        Assert.True(process.Start(), $"Could not start {fileName}.");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
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
