using System.Diagnostics;
using System.Globalization;

namespace CSharpDB.Daemon.Tests;

public sealed class PreviousReleasePerformanceScriptTests
{
    private const int ComparisonRepeatCount = 3;

    [Fact]
    public async Task Comparer_AcceptsMatchingResultsWithinLimits()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(candidate, "lookup", [90m, 90m, 90m], [12m, 12m, 12m]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

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
            WriteComparisonEvidence(
                baseline,
                "hot-read",
                [41557.5m, 41557.5m, 41557.5m],
                [0.0328m, 0.0328m, 0.0328m]);
            WriteComparisonEvidence(
                candidate,
                "hot-read",
                [43518.0m, 43518.0m, 43518.0m],
                [0.0503m, 0.0503m, 0.0503m]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **PASS**", contents);
            Assert.Contains(
                "- P99 absolute regression allowance: 0.0500 ms",
                contents);
            Assert.Contains(
                "| suite | hot-read | -4.72% | 53.35% | PASS | " +
                "P99 percentage-only crossing: P99 increased by 0.0175 ms, " +
                "which did not exceed the " +
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
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [0.0321m, 0.0321m, 0.0321m]);
            WriteComparisonEvidence(candidate, "lookup", [100m, 100m, 100m], [0.0821m, 0.0821m, 0.0821m]);

            ProcessResult boundary = await RunComparerAsync(
                baseline,
                candidate,
                boundaryReport);

            Assert.True(boundary.ExitCode == 0, boundary.CombinedOutput);
            Assert.Contains(
                "| suite | lookup | 0.00% | 155.76% | PASS | " +
                "P99 percentage-only crossing: P99 increased by 0.0500 ms, " +
                "which did not exceed the " +
                "0.0500 ms absolute allowance. |",
                File.ReadAllText(boundaryReport));

            WriteComparisonEvidence(candidate, "lookup", [100m, 100m, 100m], [0.0822m, 0.0822m, 0.0822m]);
            ProcessResult failure = await RunComparerAsync(
                baseline,
                candidate,
                failureReport);

            Assert.NotEqual(0, failure.ExitCode);
            Assert.Contains(
                "| suite | lookup | 0.00% | 156.07% | REGRESSION | " +
                "Confirmed candidate P99 regression: P99 increased by 0.0501 ms, which exceeded the " +
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
            WriteComparisonEvidence(
                baseline,
                "hot-read",
                [41557.5m, 41557.5m, 41557.5m],
                [0.0328m, 0.0328m, 0.0328m]);
            WriteComparisonEvidence(
                candidate,
                "hot-read",
                [43518.0m, 43518.0m, 43518.0m],
                [0.0503m, 0.0503m, 0.0503m]);

            ProcessResult result = await RunComparerAsync(
                baseline,
                candidate,
                report,
                "-MaxP99RegressionMilliseconds",
                "0");

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(
                "| suite | hot-read | -4.72% | 53.35% | REGRESSION |",
                File.ReadAllText(report));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RejectsResultSetDriftAsInvalidEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(candidate, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            string candidateMedian = Path.Combine(candidate, "suite.csv");
            File.AppendAllText(
                candidateMedian,
                Environment.NewLine + CreateEvidenceRow("candidate-only", 100m, 10m, aggregate: true));

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **FAIL**", contents);
            Assert.Contains("INVALID", contents);
            Assert.Contains("row set does not match", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RejectsInsufficientLatencySamplesAsInvalidEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [100m, 100m, 100m],
                [10m, 10m, 10m],
                latencySamples: 99);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("| suite | lookup | n/a | n/a | INVALID |", contents);
            Assert.Contains("LatencySamples", contents);
            Assert.Contains("must be at least 100", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_AllowsOneRawOutlierWhenStrictMajorityIsStable()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(candidate, "lookup", [70m, 100m, 100m], [14m, 10m, 10m]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains("| suite | lookup | 0.00% | 0.00% | PASS |", contents);
            Assert.Contains("Tolerated raw-run outlier with a strict stable majority", contents);
            Assert.Contains("Candidate run 1: throughput deviates 30.00%", contents);
            Assert.Contains("P99 deviates 40.00% (4.0000 ms)", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RejectsWhenDifferentMetricOutliersLeaveNoStableMajority()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(candidate, "lookup", [70m, 100m, 100m], [10m, 10m, 14m]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("| suite | lookup | n/a | n/a | UNSTABLE |", contents);
            Assert.Contains("Insufficient stability", contents);
            Assert.Contains("Candidate has 1/3 whole runs within both limits", contents);
            Assert.Contains("at least 2 are required", contents);
            Assert.Contains("Candidate run 1: throughput deviates 30.00%", contents);
            Assert.Contains("Candidate run 3: P99 deviates 40.00% (4.0000 ms)", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RequiresThreeOfFiveWholeRunsForAStableMajority()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string passReport = Path.Combine(temporaryRoot, "pass.md");
            string failureReport = Path.Combine(temporaryRoot, "failure.md");
            WriteComparisonEvidence(
                baseline,
                "lookup",
                [100m, 100m, 100m, 100m, 100m],
                [10m, 10m, 10m, 10m, 10m]);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [70m, 100m, 100m, 100m, 130m],
                [10m, 10m, 10m, 10m, 10m]);

            ProcessResult passing = await RunComparerAsync(
                baseline,
                candidate,
                passReport,
                "-RepeatCount",
                "5");

            Assert.True(passing.ExitCode == 0, passing.CombinedOutput);
            Assert.Contains("(3/5; 3 required)", File.ReadAllText(passReport));

            WriteComparisonEvidence(
                candidate,
                "lookup",
                [70m, 80m, 100m, 100m, 130m],
                [10m, 10m, 10m, 10m, 10m]);
            ProcessResult failing = await RunComparerAsync(
                baseline,
                candidate,
                failureReport,
                "-RepeatCount",
                "5");

            Assert.NotEqual(0, failing.ExitCode);
            Assert.Contains(
                "Candidate has 2/5 whole runs within both limits; at least 3 are required",
                File.ReadAllText(failureReport));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_AllowsRawP99DeviationAtAbsoluteStabilityAllowance()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(
                baseline,
                "lookup",
                [100m, 100m, 100m],
                [0.0321m, 0.0321m, 0.0821m]);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [100m, 100m, 100m],
                [0.0321m, 0.0321m, 0.0821m]);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains("- Result: **PASS**", contents);
            Assert.Contains("| suite | lookup | 0.00% | 0.00% | PASS |", contents);
            Assert.DoesNotContain("| suite | lookup | n/a | n/a | UNSTABLE |", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_RejectsMedianMismatchAsInvalidEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(baseline, "lookup", [100m, 100m, 100m], [10m, 10m, 10m]);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [100m, 100m, 100m],
                [10m, 10m, 10m],
                medianOpsOverride: 101m);

            ProcessResult result = await RunComparerAsync(baseline, candidate, report);

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("| suite | lookup | n/a | n/a | INVALID |", contents);
            Assert.Contains(
                "Candidate median OpsPerSec does not match the raw-run median (101 versus 100).",
                contents);
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
            File.WriteAllText(
                Path.Combine(benchmarkRoot, "Program.cs"),
                "internal static class Program { private static void Main() { } }");
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

            ProcessResult invalidRepeat = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "invalid-repeat-evidence"),
                "-RepeatCount",
                "2",
                "-PreflightOnly");

            Assert.NotEqual(0, invalidRepeat.ExitCode);
            Assert.Contains("does not belong to the set", invalidRepeat.CombinedOutput);
            Assert.Contains("3,5,7,9", invalidRepeat.CombinedOutput);

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
            File.WriteAllText(
                Path.Combine(benchmarkRoot, "Program.cs"),
                "internal static class Program { private static void Main() { } }");
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
            string[] suiteNames =
            [
                "master-table",
                "durable-sql-batching",
                "concurrent-write-diagnostics",
                "hybrid-storage-mode",
                "hybrid-hot-set-read",
                "hybrid-cold-open",
                "sqlite-compare",
            ];
            foreach (string resultDirectory in new[] { "baseline-results", "candidate-results" })
            {
                string rawRoot = Path.Combine(evidence, resultDirectory, "raw");
                Assert.Equal(
                    21,
                    Directory.GetFiles(rawRoot, "*.csv", SearchOption.AllDirectories).Length);
                foreach (string suiteName in suiteNames)
                {
                    string[] rawFileNames = Directory
                        .GetFiles(Path.Combine(rawRoot, suiteName), "*.csv")
                        .Select(path => Path.GetFileName(path)!)
                        .OrderBy(name => name, StringComparer.Ordinal)
                        .ToArray();
                    Assert.Equal(
                        new[] { "run-1.csv", "run-2.csv", "run-3.csv" },
                        rawFileNames);
                }
            }
            Assert.True(File.Exists(Path.Combine(evidence, "logs", "previous-release.log")));
            Assert.True(File.Exists(Path.Combine(evidence, "logs", "candidate.log")));
            string harnessManifest = File.ReadAllText(
                Path.Combine(evidence, "logs", "candidate-benchmark-harness.sha256"));
            Assert.Contains("HarnessSha256=", harnessManifest);
            Assert.Contains("*CSharpDB.Benchmarks.csproj", harnessManifest);
            Assert.Contains("*Program.cs", harnessManifest);
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

    private const string EvidenceHeader =
        "Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999," +
        "Min,Max,Mean,StdDev,ExtraInfo";

    private static void WriteComparisonEvidence(
        string resultsRoot,
        string rowName,
        IReadOnlyList<decimal> opsRuns,
        IReadOnlyList<decimal> p99Runs,
        int latencySamples = 200,
        decimal? medianOpsOverride = null,
        decimal? medianP99Override = null)
    {
        if (opsRuns.Count == 0 || opsRuns.Count != p99Runs.Count)
        {
            throw new ArgumentException(
                "Comparison evidence requires matching non-empty throughput and P99 raw runs.");
        }

        int repeatCount = opsRuns.Count;
        Directory.CreateDirectory(resultsRoot);
        string rawSuiteRoot = Directory
            .CreateDirectory(Path.Combine(resultsRoot, "raw", "suite"))
            .FullName;
        decimal medianOps = medianOpsOverride ?? GetMedian(opsRuns);
        decimal medianP99 = medianP99Override ?? GetMedian(p99Runs);
        File.WriteAllLines(
            Path.Combine(resultsRoot, "suite.csv"),
            [
                EvidenceHeader,
                CreateEvidenceRow(
                    rowName,
                    medianOps,
                    medianP99,
                    aggregate: true,
                    latencySamples,
                    aggregateRepeatCount: repeatCount),
            ]);

        for (int index = 0; index < repeatCount; index++)
        {
            File.WriteAllLines(
                Path.Combine(rawSuiteRoot, $"run-{index + 1}.csv"),
                [
                    EvidenceHeader,
                    CreateEvidenceRow(
                        rowName,
                        opsRuns[index],
                        p99Runs[index],
                        aggregate: false,
                        latencySamples,
                        runNumber: index + 1),
                ]);
        }
    }

    private static decimal GetMedian(IReadOnlyList<decimal> values)
    {
        return values
            .OrderBy(value => value)
            .ElementAt(values.Count / 2);
    }

    private static string CreateEvidenceRow(
        string name,
        decimal opsPerSecond,
        decimal p99,
        bool aggregate,
        int latencySamples = 200,
        int? runNumber = null,
        int aggregateRepeatCount = ComparisonRepeatCount)
    {
        string extraInfo = aggregate
            ? $"Aggregate=median-of-{aggregateRepeatCount}"
            : $"Run={runNumber}";
        return string.Join(
            ',',
            [
                name,
                "1000",
                latencySamples.ToString(CultureInfo.InvariantCulture),
                "10000",
                FormatEvidenceNumber(opsPerSecond),
                "1",
                "2",
                "3",
                FormatEvidenceNumber(p99),
                FormatEvidenceNumber(p99),
                "0.1",
                "100",
                "1",
                "0.1",
                extraInfo,
            ]);
    }

    private static string FormatEvidenceNumber(decimal value)
    {
        return value.ToString(CultureInfo.InvariantCulture);
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
            $runStamp = '20260731-120000'
            $header = 'Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo'
            $resultPath = Join-Path `
                $resultRoot `
                "$suiteName-$runStamp-median-of-$repeatCount.csv"
            [IO.File]::WriteAllLines(
                $resultPath,
                @(
                    $header,
                    "$suiteName-row,1000,1000,10000,100,1,1,1,1,1,1,1,1,1,Aggregate=median-of-$repeatCount"
                ))
            for ($runIndex = 1; $runIndex -le [int] $repeatCount; $runIndex++) {
                $rawPath = Join-Path `
                    $resultRoot `
                    "$suiteName-$runStamp-run$runIndex.csv"
                [IO.File]::WriteAllLines(
                    $rawPath,
                    @(
                        $header,
                        "$suiteName-row,1000,1000,10000,100,1,1,1,1,1,1,1,1,1,Run=$runIndex"
                    ))
            }
            if ($env:FAKE_DOTNET_DUPLICATE_SUITE -eq $suiteName) {
                $duplicatePath = Join-Path `
                    $resultRoot `
                    "$suiteName-$runStamp-extra-median-of-$repeatCount.csv"
                [IO.File]::WriteAllLines(
                    $duplicatePath,
                    @(
                        $header,
                        "$suiteName-row,1000,1000,10000,100,1,1,1,1,1,1,1,1,1,Aggregate=median-of-$repeatCount"
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
            "-BaselineRawResultsPath",
            Path.Combine(baseline, "raw"),
            "-CandidateRawResultsPath",
            Path.Combine(candidate, "raw"),
            "-ReportPath",
            report,
        ];
        if (!additionalArguments.Contains("-RepeatCount", StringComparer.OrdinalIgnoreCase))
        {
            arguments.Add("-RepeatCount");
            arguments.Add(ComparisonRepeatCount.ToString(CultureInfo.InvariantCulture));
        }
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
