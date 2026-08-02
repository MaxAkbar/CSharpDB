using System.Globalization;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using BenchmarkProgram = CSharpDB.Benchmarks.Program;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ReleaseCoreSingleSampleWarmupTests
{
    [Theory]
    [InlineData("--master-table")]
    [InlineData("--durable-sql-batching")]
    [InlineData("--concurrent-write-diagnostics")]
    [InlineData("--hybrid-storage-mode")]
    [InlineData("--hybrid-hot-set-read")]
    [InlineData("--hybrid-cold-open")]
    [InlineData("--sqlite-compare")]
    public void OptionValidation_AllowsEachDirectReleaseCoreMode(string mode)
    {
        BenchmarkProgram.ValidateWarmupSingleSampleOption(
            mode,
            repeatCount: 1,
            warmupSingleSample: true);
    }

    [Fact]
    public void OptionValidation_RejectsRepeatCountsOtherThanOne()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => BenchmarkProgram.ValidateWarmupSingleSampleOption(
                "--master-table",
                repeatCount: 3,
                warmupSingleSample: true));

        Assert.Contains("--repeat 1", exception.Message);
    }

    [Theory]
    [InlineData("--release-core")]
    [InlineData("--macro")]
    public void OptionValidation_RejectsModesThatAreNotDirectReleaseCoreSuites(string mode)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => BenchmarkProgram.ValidateWarmupSingleSampleOption(
                mode,
                repeatCount: 1,
                warmupSingleSample: true));

        Assert.Contains("direct release-core suite modes", exception.Message);
    }

    [Fact]
    public async Task SuiteRunner_WarmsOnceRecordsOnceAndWritesExactlyOneCsv()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        int invocationCount = 0;
        try
        {
            await BenchmarkProgram.RunSuiteWithRepeatsAsync(
                "single-sample-test",
                () => Task.FromResult(new List<BenchmarkResult>
                {
                    CreateResult(++invocationCount),
                }),
                repeatCount: 1,
                warmupSingleSample: true,
                outputDirectory: temporaryRoot);

            Assert.Equal(2, invocationCount);
            string csvPath = Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
            Assert.DoesNotContain("median", Path.GetFileName(csvPath), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(2, ReadCsvInt(csvPath, "TotalOps"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task SuiteRunner_WithoutOptionPreservesSingleInvocationBehavior()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        int invocationCount = 0;
        try
        {
            await BenchmarkProgram.RunSuiteWithRepeatsAsync(
                "single-sample-default-test",
                () => Task.FromResult(new List<BenchmarkResult>
                {
                    CreateResult(++invocationCount),
                }),
                repeatCount: 1,
                outputDirectory: temporaryRoot);

            Assert.Equal(1, invocationCount);
            Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task SuiteRunner_ReleaseCoreRowBelowSampleFloorFailsBeforeCsvEmission()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => BenchmarkProgram.RunSuiteWithRepeatsAsync(
                    "master-table",
                    () => Task.FromResult(new List<BenchmarkResult>
                    {
                        CreateResult(invocation: 99),
                    }),
                    repeatCount: 1,
                    outputDirectory: temporaryRoot));

            Assert.Contains("master-table", exception.Message);
            Assert.Contains("single-sample-row", exception.Message);
            Assert.Contains("99 retained latency samples", exception.Message);
            Assert.Contains("100", exception.Message);
            Assert.Empty(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task SuiteRunner_EmptyReleaseCoreResultFailsBeforeCsvEmission()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => BenchmarkProgram.RunSuiteWithRepeatsAsync(
                    "master-table",
                    static () => Task.FromResult(new List<BenchmarkResult>()),
                    repeatCount: 1,
                    outputDirectory: temporaryRoot));

            Assert.Contains("master-table", exception.Message);
            Assert.Contains("no benchmark rows", exception.Message);
            Assert.Empty(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Theory]
    [InlineData("TotalOps")]
    [InlineData("ElapsedMs")]
    [InlineData("OpsPerSec")]
    [InlineData("P99")]
    public async Task SuiteRunner_InvalidComparatorMetricFailsBeforeCsvEmission(
        string invalidMetric)
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            BenchmarkResult invalidResult = new()
            {
                Name = "invalid-release-row",
                TotalOps = invalidMetric switch
                {
                    "TotalOps" => 0,
                    "OpsPerSec" => 1,
                    _ => int.MaxValue,
                },
                LatencySamples = 100,
                ElapsedMs = invalidMetric switch
                {
                    "ElapsedMs" => 0.004,
                    "OpsPerSec" => 100_000,
                    _ => 1_000,
                },
                P99Ms = invalidMetric == "P99" ? 0.00004 : 1,
            };

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => BenchmarkProgram.RunSuiteWithRepeatsAsync(
                    "master-table",
                    () => Task.FromResult(new List<BenchmarkResult> { invalidResult }),
                    repeatCount: 1,
                    outputDirectory: temporaryRoot));

            Assert.Contains("master-table", exception.Message);
            Assert.Contains("invalid-release-row", exception.Message);
            Assert.Contains(invalidMetric, exception.Message);
            Assert.Contains("positive finite value", exception.Message);
            Assert.Empty(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("undersampled")]
    [InlineData("invalid-metric")]
    public void ColdOpenWriter_InvalidReleaseEvidenceFailsBeforeCsvEmission(
        string evidenceKind)
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            IReadOnlyList<BenchmarkResult> rows = evidenceKind switch
            {
                "empty" => [],
                "undersampled" => [CreateResult(99)],
                "invalid-metric" =>
                [
                    new BenchmarkResult
                    {
                        Name = "invalid-cold-open-row",
                        TotalOps = 100,
                        LatencySamples = 100,
                        ElapsedMs = 1_000,
                        P99Ms = 0.00004,
                    },
                ],
                _ => throw new ArgumentOutOfRangeException(nameof(evidenceKind)),
            };

            InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
                () => BenchmarkProgram.WriteSuiteResults(
                    "hybrid-cold-open",
                    [rows],
                    outputDirectory: temporaryRoot));

            Assert.Contains("hybrid-cold-open", exception.Message);
            Assert.Empty(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task SuiteRunner_NonReleaseSuiteDoesNotApplyReleaseSampleFloor()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            await BenchmarkProgram.RunSuiteWithRepeatsAsync(
                "diagnostic-only-test",
                () => Task.FromResult(new List<BenchmarkResult>
                {
                    CreateResult(invocation: 1),
                }),
                repeatCount: 1,
                outputDirectory: temporaryRoot);

            Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task ScenarioMajorRunner_RepeatOneWarmupIsNotRecorded()
    {
        int invocationCount = 0;
        IReadOnlyList<Func<Task<BenchmarkResult>>> scenarios =
        [
            () => Task.FromResult(CreateResult(++invocationCount)),
        ];

        List<IReadOnlyList<BenchmarkResult>> runs = await ScenarioMajorBenchmarkRunner.RunAsync(
            scenarios,
            repeatCount: 1,
            warmUpEachScenario: true);

        Assert.Equal(2, invocationCount);
        IReadOnlyList<BenchmarkResult> recordedRun = Assert.Single(runs);
        Assert.Equal(2, Assert.Single(recordedRun).TotalOps);
    }

    [Fact]
    public void HybridColdOpen_EnablesPerScenarioWarmupForOneSampleOnlyWhenRequested()
    {
        Assert.False(HybridColdOpenBenchmark.ShouldWarmUpEachScenario(
            repeatCount: 1,
            warmupSingleSample: false));
        Assert.True(HybridColdOpenBenchmark.ShouldWarmUpEachScenario(
            repeatCount: 1,
            warmupSingleSample: true));
        Assert.True(HybridColdOpenBenchmark.ShouldWarmUpEachScenario(
            repeatCount: 3,
            warmupSingleSample: false));

        Assert.Throws<ArgumentException>(
            () => HybridColdOpenBenchmark.ShouldWarmUpEachScenario(
                repeatCount: 3,
                warmupSingleSample: true));
    }

    [Fact]
    public void SingleColdOpenRun_WritesExactlyOneCsv()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            BenchmarkProgram.WriteSuiteResults(
                "hybrid-cold-open-single-sample-test",
                [[CreateResult(1)]],
                outputDirectory: temporaryRoot);

            Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    private static BenchmarkResult CreateResult(int invocation)
    {
        return new BenchmarkResult
        {
            Name = "single-sample-row",
            TotalOps = invocation,
            LatencySamples = invocation,
            ElapsedMs = 1_000,
            P99Ms = invocation,
        };
    }

    private static string CreateTemporaryDirectory()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-release-core-single-sample-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static int ReadCsvInt(string path, string columnName)
    {
        string[] lines = File.ReadAllLines(path);
        Assert.Equal(2, lines.Length);
        string[] columns = lines[0].Split(',');
        string[] values = lines[1].Split(',');
        int columnIndex = Array.IndexOf(columns, columnName);
        Assert.True(columnIndex >= 0, $"CSV column '{columnName}' was not found.");
        return int.Parse(values[columnIndex], CultureInfo.InvariantCulture);
    }
}
