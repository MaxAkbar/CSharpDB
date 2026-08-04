using System.Diagnostics;
using System.Globalization;

namespace CSharpDB.Daemon.Tests;

public sealed class PreviousReleasePerformanceScriptTests
{
    private const int ComparisonRepeatCount = 3;

    [Fact]
    public void DiagnosticNormalization_RejoinsPowerShellConciseViewContinuationLines()
    {
        const string diagnostic =
            "\u001b[31;1mException: benchmark validation failed\u001b[0m\n" +
            "\u001b[36;1mLine |\u001b[0m\n" +
            "  42 |  throw $message\n" +
            "     \u001b[36;1m|\u001b[0m  ~~~~~~~~~~~~~~\n" +
            "     \u001b[36;1m|\u001b[0m produced 2 median CSV file(s); expected\n" +
            "     \u001b[36;1m|\u001b[0m exactly one";

        AssertDiagnosticContains(
            "produced 2 median CSV file(s); expected exactly one",
            diagnostic);
        Assert.Equal("| column-zero content", NormalizeDiagnostic("| column-zero content"));
    }

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
    public async Task Comparer_P95SelectionIgnoresP99ForStatusButReportsIt()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(
                Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(
                Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(
                baseline,
                "lookup",
                [100m, 100m, 100m],
                [10m, 10m, 10m],
                p95Runs: [5m, 5m, 5m]);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [100m, 100m, 100m],
                [20m, 20m, 20m],
                p95Runs: [5.5m, 5.5m, 5.5m]);

            ProcessResult result = await RunComparerAsync(
                baseline,
                candidate,
                report,
                "-BlockingLatencyPercentile",
                "P95");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string contents = File.ReadAllText(report);
            Assert.Contains(
                "- Blocking latency percentile: P95. P99 is retained as a " +
                "non-blocking diagnostic",
                contents);
            Assert.Contains(
                "| Suite | Row | Throughput regression | P95 regression | " +
                "P99 diagnostic regression |",
                contents);
            Assert.Contains(
                "| suite | lookup | 0.00% | 10.00% | 100.00% | PASS |",
                contents);
            Assert.DoesNotContain("P99 failure rule", contents);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Comparer_P95SelectionRecomputesP95Median()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string baseline = Directory.CreateDirectory(
                Path.Combine(temporaryRoot, "baseline")).FullName;
            string candidate = Directory.CreateDirectory(
                Path.Combine(temporaryRoot, "candidate")).FullName;
            string report = Path.Combine(temporaryRoot, "comparison.md");
            WriteComparisonEvidence(
                baseline,
                "lookup",
                [100m, 100m, 100m],
                [10m, 10m, 10m],
                p95Runs: [5m, 5m, 5m],
                medianP95Override: 6m);
            WriteComparisonEvidence(
                candidate,
                "lookup",
                [100m, 100m, 100m],
                [10m, 10m, 10m],
                p95Runs: [5m, 5m, 5m]);

            ProcessResult result = await RunComparerAsync(
                baseline,
                candidate,
                report,
                "-BlockingLatencyPercentile",
                "P95");

            Assert.NotEqual(0, result.ExitCode);
            string contents = File.ReadAllText(report);
            Assert.Contains("INVALID", contents);
            Assert.Contains(
                "Baseline median P95 does not match the raw-run median",
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

            string selectedSuiteEvidence = Path.Combine(
                temporaryRoot,
                "selected-suite-evidence");
            ProcessResult selectedSuiteResult = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                selectedSuiteEvidence,
                "-QualificationPass",
                "1",
                "-SuiteName",
                "hybrid-cold-open",
                "-PreflightOnly");

            Assert.True(
                selectedSuiteResult.ExitCode == 0,
                selectedSuiteResult.CombinedOutput);
            string selectedSuitePreflight = File.ReadAllText(Path.Combine(
                selectedSuiteEvidence,
                "previous-release-performance-preflight.md"));
            Assert.Contains(
                "- Revision order within each suite: previous then candidate",
                selectedSuitePreflight);
            Assert.Contains(
                "- Suite order: hybrid-cold-open",
                selectedSuitePreflight);
            Assert.Contains(
                "- Execution order: hybrid-cold-open/previous, " +
                "hybrid-cold-open/candidate",
                selectedSuitePreflight);
            Assert.DoesNotContain("master-table/", selectedSuitePreflight);

            string durableEvidence = Path.Combine(
                temporaryRoot,
                "durable-suite-evidence");
            ProcessResult durableResult = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                durableEvidence,
                "-QualificationPass",
                "1",
                "-Paired",
                "-SuiteName",
                "master-table-durable-writes",
                "-PreflightOnly");

            Assert.True(durableResult.ExitCode == 0, durableResult.CombinedOutput);
            string durablePreflight = File.ReadAllText(Path.Combine(
                durableEvidence,
                "previous-release-performance-preflight.md"));
            Assert.Contains("- Suite order: master-table-durable-writes", durablePreflight);
            Assert.Contains(
                "- Paired repeats per order: 3 (total pairs per suite: 6; " +
                "recorded samples per revision: 6)",
                durablePreflight);
            Assert.Contains(
                "master-table-durable-writes/pair-01/previous, " +
                "master-table-durable-writes/pair-01/candidate",
                durablePreflight);
            Assert.DoesNotContain("master-table/pair-", durablePreflight);

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
            AssertDiagnosticContains(
                "requires a clean repository worktree",
                dirtyResult.CombinedOutput);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData("-PreviousRef", "v4.3.0")]
    [InlineData("-RepeatCount", "5")]
    [InlineData("-PostBuildQuiescenceSeconds", "0")]
    [InlineData("-MaxThroughputRegressionPercent", "100")]
    [InlineData("-MaxP99RegressionPercent", "500")]
    [InlineData("-MaxP99RegressionMilliseconds", "1000")]
    [InlineData("-BlockingLatencyPercentile", "P99")]
    public async Task LocalDurableWrapper_NonCanonicalSettingsCannotPublishOfficialStatus(
        string settingName,
        string settingValue)
    {
        if (!OperatingSystem.IsWindows())
            return;

        string script = Path.Combine(
            FindRepoRoot(),
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-LocalDurablePerformance.ps1");
        ProcessResult result = await RunProcessAsync(
            "pwsh",
            "-NoLogo",
            "-NoProfile",
            "-File",
            script,
            "-ConfirmDedicatedFixedSsd",
            "-GitHubRepository",
            "example/csharpdb",
            settingName,
            settingValue);

        Assert.NotEqual(0, result.ExitCode);
        AssertDiagnosticContains("requires canonical policy 'durable-v2'", result.CombinedOutput);
        AssertDiagnosticContains("Use -NoGitHubStatus for diagnostic overrides", result.CombinedOutput);
    }

    [Fact]
    public async Task LocalDurableWrapper_PinsCommitsRunsBothPassesAndPropagatesFailure()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string sourceRoot = Path.Combine(temporaryRoot, "repository");
            string scriptRoot = Path.Combine(
                sourceRoot,
                "tests",
                "CSharpDB.Benchmarks",
                "scripts");
            Directory.CreateDirectory(scriptRoot);

            string repositoryRoot = FindRepoRoot();
            string localDurableWrapper = File.ReadAllText(Path.Combine(
                repositoryRoot,
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Test-LocalDurablePerformance.ps1"));
            const string environmentGuardInsertionPoint = "$status = Invoke-Git `";
            Assert.Contains(environmentGuardInsertionPoint, localDurableWrapper);
            localDurableWrapper = localDurableWrapper.Replace(
                environmentGuardInsertionPoint,
                """
                # Test-only environment probes keep this wrapper test independent of the host.
                function Get-PendingRestartReasons {
                    if ($env:FAKE_PENDING_RESTART -eq '1') {
                        return @('Simulated pending restart')
                    }
                    return @()
                }
                function Get-ActiveInstallerReasons { return @() }
                function Get-InstallerActivityReasons {
                    param([DateTimeOffset] $SinceUtc)
                    if ($env:FAKE_INSTALLER_ACTIVITY -eq '1') {
                        return @('Simulated MsiInstaller event 1040')
                    }
                    return @()
                }

                $status = Invoke-Git `
                """,
                StringComparison.Ordinal);
            File.WriteAllText(
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                localDurableWrapper);
            File.WriteAllText(
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                """
                #requires -Version 7.0
                [CmdletBinding()]
                param(
                    [string] $PreviousRef = '',
                    [string] $CandidateRef = 'HEAD',
                    [string] $OutputPath = '',
                    [int] $QualificationPass = 1,
                    [switch] $Paired,
                    [string[]] $SuiteName = @(),
                    [int] $RepeatCount = 3,
                    [int] $PostBuildQuiescenceSeconds = 0,
                    [double] $MaxThroughputRegressionPercent = 15,
                    [double] $MaxP99RegressionPercent = 25,
                    [double] $MaxP99RegressionMilliseconds = 0.05,
                    [string] $BlockingLatencyPercentile = 'P99')

                $ErrorActionPreference = 'Stop'
                New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
                $resolvedPrevious = if ([string]::IsNullOrWhiteSpace($PreviousRef)) {
                    $env:FAKE_PREVIOUS_COMMIT
                }
                else {
                    $PreviousRef
                }
                [IO.File]::WriteAllLines(
                    (Join-Path $OutputPath 'previous-release-performance-preflight.md'),
                    @(
                        '# Previous-release performance preflight',
                        '',
                        "- Previous ref: ``v4.3.0`` (``$resolvedPrevious``)"))
                [IO.File]::WriteAllLines(
                    (Join-Path $OutputPath 'previous-release-performance.md'),
                    @('# Previous-release performance', '', '- Result: **PASS**'))
                Add-Content -LiteralPath $env:FAKE_LOCAL_DURABLE_LOG -Value (
                    "$QualificationPass|$PreviousRef|$CandidateRef|" +
                    "$env:CSHARPDB_BENCH_DURABILITY|$($SuiteName -join ',')|" +
                    "$BlockingLatencyPercentile")
                if ($QualificationPass -eq 1 -and $env:FAKE_FAIL_PASS_ONE -eq '1') {
                    throw 'Simulated pass-one failure.'
                }
                """);
            File.WriteAllText(Path.Combine(sourceRoot, "release.txt"), "previous");

            await AssertProcessSucceeded("git", "-C", sourceRoot, "init");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.email", "test@example.invalid");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.name", "CSharpDB Tests");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "commit.gpgsign", "false");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", ".");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "previous release");
            string previousCommit = (await RunProcessAsync(
                "git",
                "-C",
                sourceRoot,
                "rev-parse",
                "HEAD")).StandardOutput.Trim();
            await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.3.0");
            File.WriteAllText(Path.Combine(sourceRoot, "release.txt"), "candidate");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "add", "release.txt");
            await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "candidate");
            string candidateCommit = (await RunProcessAsync(
                "git",
                "-C",
                sourceRoot,
                "rev-parse",
                "HEAD")).StandardOutput.Trim();

            string fakeGitHubRoot = Path.Combine(temporaryRoot, "fake-github");
            Directory.CreateDirectory(fakeGitHubRoot);
            File.WriteAllText(
                Path.Combine(fakeGitHubRoot, "fake-gh.ps1"),
                """
                param(
                    [Parameter(ValueFromRemainingArguments = $true)]
                    [string[]] $Arguments)

                Add-Content -LiteralPath $env:FAKE_GH_LOG -Value ($Arguments -join '|')
                if ($Arguments.Count -ge 2 -and
                    $Arguments[0] -eq 'auth' -and
                    $Arguments[1] -eq 'status') {
                    exit 0
                }
                if ($Arguments.Count -ge 1 -and $Arguments[0] -eq 'api') {
                    if (-not [string]::IsNullOrWhiteSpace($env:FAKE_GH_FAIL_STATE) -and
                        $Arguments -contains "state=$env:FAKE_GH_FAIL_STATE") {
                        Write-Error "Simulated GitHub $env:FAKE_GH_FAIL_STATE status failure."
                        exit 1
                    }
                    exit 0
                }
                Write-Error "Unexpected fake gh command: $($Arguments -join ' ')"
                exit 1
                """);
            File.WriteAllText(
                Path.Combine(fakeGitHubRoot, "gh.cmd"),
                """
                @echo off
                pwsh -NoLogo -NoProfile -File "%~dp0fake-gh.ps1" %*
                exit /b %ERRORLEVEL%
                """);

            string successLog = Path.Combine(temporaryRoot, "success.log");
            string githubLog = Path.Combine(temporaryRoot, "github.log");
            string successEvidence = Path.Combine(temporaryRoot, "success-evidence");
            ProcessResult success = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = successLog,
                    ["FAKE_GH_LOG"] = githubLog,
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                    ["CSHARPDB_BENCH_DURABILITY"] = "Buffered",
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                successEvidence,
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.True(success.ExitCode == 0, success.CombinedOutput);
            string[] successLines = File.ReadAllLines(successLog);
            Assert.Equal(2, successLines.Length);
            Assert.Equal(
                $"1||{candidateCommit}|Durable|master-table-durable-writes|P95",
                successLines[0]);
            Assert.Equal(
                $"2|{previousCommit}|{candidateCommit}|Durable|master-table-durable-writes|P95",
                successLines[1]);
            string successSummary = File.ReadAllText(Path.Combine(
                successEvidence,
                "local-durable-performance.md"));
            Assert.Contains("- Result: **PASS**", successSummary);
            Assert.Contains("- Blocking latency percentile: `P95`", successSummary);
            Assert.Contains("- P99 latency: diagnostic only", successSummary);
            Assert.Contains(
                "GitHub release status: `csharpdb/local-durable-performance` " +
                "in `example/csharpdb`",
                successSummary);
            string[] githubCalls = File.ReadAllLines(githubLog);
            Assert.Equal(3, githubCalls.Length);
            Assert.Equal("auth|status", githubCalls[0]);
            Assert.Contains($"repos/example/csharpdb/statuses/{candidateCommit}", githubCalls[1]);
            Assert.Contains("state=pending", githubCalls[1]);
            Assert.Contains("context=csharpdb/local-durable-performance", githubCalls[1]);
            Assert.Contains($"repos/example/csharpdb/statuses/{candidateCommit}", githubCalls[2]);
            Assert.Contains("state=success", githubCalls[2]);
            Assert.Contains("context=csharpdb/local-durable-performance", githubCalls[2]);
            Assert.Contains("description=policy=durable-v2", githubCalls[2]);
            Assert.Contains($"baseline={previousCommit}", githubCalls[2]);

            string pendingRestartLog = Path.Combine(
                temporaryRoot,
                "pending-restart.log");
            string pendingRestartGitHubLog = Path.Combine(
                temporaryRoot,
                "pending-restart-github.log");
            ProcessResult pendingRestart = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = pendingRestartLog,
                    ["FAKE_GH_LOG"] = pendingRestartGitHubLog,
                    ["FAKE_PENDING_RESTART"] = "1",
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "pending-restart-evidence"),
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.NotEqual(0, pendingRestart.ExitCode);
            AssertDiagnosticContains("Simulated pending restart", pendingRestart.CombinedOutput);
            AssertDiagnosticContains(
                "Restart the machine, allow installers and updates to finish, then retry",
                pendingRestart.CombinedOutput);
            Assert.False(File.Exists(pendingRestartLog));
            Assert.False(File.Exists(pendingRestartGitHubLog));

            string installerActivityLog = Path.Combine(
                temporaryRoot,
                "installer-activity.log");
            string installerActivityGitHubLog = Path.Combine(
                temporaryRoot,
                "installer-activity-github.log");
            string installerActivityEvidence = Path.Combine(
                temporaryRoot,
                "installer-activity-evidence");
            ProcessResult installerActivity = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = installerActivityLog,
                    ["FAKE_GH_LOG"] = installerActivityGitHubLog,
                    ["FAKE_INSTALLER_ACTIVITY"] = "1",
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                installerActivityEvidence,
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.NotEqual(0, installerActivity.ExitCode);
            Assert.Single(File.ReadAllLines(installerActivityLog));
            AssertDiagnosticContains(
                "Pass 1 detected installer or pending-restart activity; remaining passes will not run",
                installerActivity.CombinedOutput);
            string installerActivitySummary = File.ReadAllText(Path.Combine(
                installerActivityEvidence,
                "local-durable-performance.md"));
            Assert.Contains("- Result: **FAIL**", installerActivitySummary);
            Assert.Contains("Simulated MsiInstaller event 1040", installerActivitySummary);
            string[] installerActivityGitHubCalls = File.ReadAllLines(
                installerActivityGitHubLog);
            Assert.Equal(3, installerActivityGitHubCalls.Length);
            Assert.Contains("state=pending", installerActivityGitHubCalls[1]);
            Assert.Contains("state=failure", installerActivityGitHubCalls[2]);

            string pendingStatusFailureLog = Path.Combine(
                temporaryRoot,
                "pending-status-failure.log");
            string pendingStatusFailureGitHubLog = Path.Combine(
                temporaryRoot,
                "pending-status-failure-github.log");
            ProcessResult pendingStatusFailure = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = pendingStatusFailureLog,
                    ["FAKE_GH_LOG"] = pendingStatusFailureGitHubLog,
                    ["FAKE_GH_FAIL_STATE"] = "pending",
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "pending-status-failure-evidence"),
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.NotEqual(0, pendingStatusFailure.ExitCode);
            AssertDiagnosticContains(
                "Could not publish GitHub status 'csharpdb/local-durable-performance'",
                pendingStatusFailure.CombinedOutput);
            Assert.False(File.Exists(pendingStatusFailureLog));
            string[] pendingStatusFailureCalls = File.ReadAllLines(
                pendingStatusFailureGitHubLog);
            Assert.Equal(2, pendingStatusFailureCalls.Length);
            Assert.Contains("state=pending", pendingStatusFailureCalls[1]);

            string successStatusFailureLog = Path.Combine(
                temporaryRoot,
                "success-status-failure.log");
            string successStatusFailureGitHubLog = Path.Combine(
                temporaryRoot,
                "success-status-failure-github.log");
            string successStatusFailureEvidence = Path.Combine(
                temporaryRoot,
                "success-status-failure-evidence");
            ProcessResult successStatusFailure = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = successStatusFailureLog,
                    ["FAKE_GH_LOG"] = successStatusFailureGitHubLog,
                    ["FAKE_GH_FAIL_STATE"] = "success",
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                successStatusFailureEvidence,
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.NotEqual(0, successStatusFailure.ExitCode);
            Assert.Equal(2, File.ReadAllLines(successStatusFailureLog).Length);
            string successStatusFailureSummary = File.ReadAllText(Path.Combine(
                successStatusFailureEvidence,
                "local-durable-performance.md"));
            Assert.Contains("- Result: **FAIL**", successStatusFailureSummary);
            Assert.Contains(
                "Could not publish GitHub status",
                successStatusFailureSummary);
            string[] successStatusFailureCalls = File.ReadAllLines(
                successStatusFailureGitHubLog);
            Assert.Equal(3, successStatusFailureCalls.Length);
            Assert.Contains("state=pending", successStatusFailureCalls[1]);
            Assert.Contains("state=success", successStatusFailureCalls[2]);

            string failureLog = Path.Combine(temporaryRoot, "failure.log");
            string failureGitHubLog = Path.Combine(temporaryRoot, "failure-github.log");
            string failureEvidence = Path.Combine(temporaryRoot, "failure-evidence");
            ProcessResult failure = await RunProcessWithEnvironmentAsync(
                "pwsh",
                new Dictionary<string, string>
                {
                    ["FAKE_PREVIOUS_COMMIT"] = previousCommit,
                    ["FAKE_LOCAL_DURABLE_LOG"] = failureLog,
                    ["FAKE_GH_LOG"] = failureGitHubLog,
                    ["PATH"] = fakeGitHubRoot + Path.PathSeparator +
                        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                    ["FAKE_FAIL_PASS_ONE"] = "1",
                },
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-LocalDurablePerformance.ps1"),
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                failureEvidence,
                "-ConfirmDedicatedFixedSsd",
                "-GitHubRepository",
                "example/csharpdb");

            Assert.NotEqual(0, failure.ExitCode);
            Assert.Equal(2, File.ReadAllLines(failureLog).Length);
            string failureSummary = File.ReadAllText(Path.Combine(
                failureEvidence,
                "local-durable-performance.md"));
            Assert.Contains("- Result: **FAIL**", failureSummary);
            Assert.Contains("Simulated pass-one failure", failureSummary);
            string[] failureGitHubCalls = File.ReadAllLines(failureGitHubLog);
            Assert.Equal(3, failureGitHubCalls.Length);
            Assert.Contains("state=pending", failureGitHubCalls[1]);
            Assert.Contains("state=failure", failureGitHubCalls[2]);
            Assert.Contains("context=csharpdb/local-durable-performance", failureGitHubCalls[2]);
            Assert.Contains("description=policy=durable-v2", failureGitHubCalls[2]);
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

            string selectedSuiteEvidence = Path.Combine(
                temporaryRoot,
                "selected-suite-run-evidence");
            string selectedSuiteInvocationLog = Path.Combine(
                temporaryRoot,
                "selected-suite-fake-dotnet.log");
            var selectedSuiteEnvironment = new Dictionary<string, string>(environment)
            {
                ["FAKE_DOTNET_LOG"] = selectedSuiteInvocationLog,
            };
            ProcessResult selectedSuite = await RunProcessWithEnvironmentAsync(
                "pwsh",
                selectedSuiteEnvironment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"),
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                selectedSuiteEvidence,
                "-QualificationPass",
                "1",
                "-SuiteName",
                "hybrid-cold-open");

            Assert.True(selectedSuite.ExitCode == 0, selectedSuite.CombinedOutput);
            Assert.Contains(
                "- Result: **PASS**",
                File.ReadAllText(Path.Combine(
                    selectedSuiteEvidence,
                    "previous-release-performance.md")));
            Assert.Equal(
                new[] { "hybrid-cold-open.csv" },
                Directory.GetFiles(
                        Path.Combine(selectedSuiteEvidence, "baseline-results"),
                        "*.csv")
                    .Select(path => Path.GetFileName(path)!)
                    .ToArray());
            Assert.Equal(
                new[] { "hybrid-cold-open.csv" },
                Directory.GetFiles(
                        Path.Combine(selectedSuiteEvidence, "candidate-results"),
                        "*.csv")
                    .Select(path => Path.GetFileName(path)!)
                    .ToArray());
            string[] selectedSuiteRuns = File.ReadAllLines(selectedSuiteInvocationLog)
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Collection(
                selectedSuiteRuns,
                line => Assert.Contains(
                    "baseline-source|run|hybrid-cold-open|repeat=3",
                    line),
                line => Assert.Contains(
                    "candidate-source|run|hybrid-cold-open|repeat=3",
                    line));

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
            AssertDiagnosticContains(
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
        decimal? medianP99Override = null,
        IReadOnlyList<decimal>? p95Runs = null,
        decimal? medianP95Override = null)
    {
        if (opsRuns.Count == 0 || opsRuns.Count != p99Runs.Count)
        {
            throw new ArgumentException(
                "Comparison evidence requires matching non-empty throughput and P99 raw runs.");
        }
        p95Runs ??= p99Runs;
        if (opsRuns.Count != p95Runs.Count)
        {
            throw new ArgumentException(
                "Comparison evidence requires matching P95 raw runs.");
        }

        int repeatCount = opsRuns.Count;
        Directory.CreateDirectory(resultsRoot);
        string rawSuiteRoot = Directory
            .CreateDirectory(Path.Combine(resultsRoot, "raw", "suite"))
            .FullName;
        decimal medianOps = medianOpsOverride ?? GetMedian(opsRuns);
        decimal medianP99 = medianP99Override ?? GetMedian(p99Runs);
        decimal medianP95 = medianP95Override ?? GetMedian(p95Runs);
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
                    aggregateRepeatCount: repeatCount,
                    p95: medianP95),
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
                        runNumber: index + 1,
                        p95: p95Runs[index]),
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
        int aggregateRepeatCount = ComparisonRepeatCount,
        decimal? p95 = null)
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
                FormatEvidenceNumber(p95 ?? p99),
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
                '--master-table-durable-writes' = 'master-table-durable-writes'
                '--master-table-hosted-stable' = 'master-table-hosted-stable'
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

    private static void AssertDiagnosticContains(string expected, string actual)
    {
        Assert.Contains(
            NormalizeDiagnostic(expected),
            NormalizeDiagnostic(actual),
            StringComparison.Ordinal);
    }

    private static string NormalizeDiagnostic(string value)
    {
        string withoutAnsi = System.Text.RegularExpressions.Regex.Replace(
            value,
            "\u001b\\[[0-?]*[ -/]*[@-~]",
            string.Empty);
        string withoutPowerShellGutters = System.Text.RegularExpressions.Regex.Replace(
            withoutAnsi,
            "^[\\t ]+\\|[\\t ]?",
            string.Empty,
            System.Text.RegularExpressions.RegexOptions.Multiline);
        return System.Text.RegularExpressions.Regex.Replace(
            withoutPowerShellGutters,
            "\\s+",
            " ").Trim();
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
