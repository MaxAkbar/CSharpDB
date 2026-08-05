using System.Globalization;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using BenchmarkProgram = CSharpDB.Benchmarks.Program;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ReleaseCoreSingleSampleWarmupTests
{
    [Theory]
    [InlineData("--master-table")]
    [InlineData("--master-table-durable-writes")]
    [InlineData("--master-table-durable-write-scenario")]
    [InlineData("--master-table-hosted-stable")]
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

        Assert.Contains("direct release-evidence suite modes", exception.Message);
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
    public async Task DurableWriteScenarioCliPath_UsesInternalWarmupAndWritesOneMasterNamedRow()
    {
        const string rowName = "MasterComparison_Sql_FileBacked_SingleInsert";
        string temporaryRoot = CreateTemporaryDirectory();
        int invocationCount = 0;
        try
        {
            await BenchmarkProgram.RunMasterComparisonDurableWriteScenarioWithRepeatsAsync(
                rowName,
                repeatCount: 1,
                warmupSingleSample: true,
                outputDirectory: temporaryRoot,
                runScenarioAsync: requestedRowName =>
                {
                    invocationCount++;
                    return Task.FromResult(new BenchmarkResult
                    {
                        Name = requestedRowName,
                        TotalOps = 10_000,
                        LatencySamples = 10_000,
                        ElapsedMs = 30_000,
                        P99Ms = 5,
                        ExtraInfo = CreateDurableQualificationExtraInfo(
                            TimeSpan.FromSeconds(30)),
                    });
                });

            Assert.Equal(1, invocationCount);
            string csvPath = Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
            Assert.StartsWith(
                BenchmarkProgram.DurableMasterWriteScenarioSuiteKey + "-",
                Path.GetFileName(csvPath),
                StringComparison.Ordinal);
            string[] lines = File.ReadAllLines(csvPath);
            Assert.Equal(2, lines.Length);
            Assert.StartsWith(rowName + ",", lines[1], StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    [Fact]
    public async Task DurableWriteScenarioCliPath_RejectsUnknownRowBeforeInvocation()
    {
        int invocationCount = 0;

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
            () => BenchmarkProgram.RunMasterComparisonDurableWriteScenarioWithRepeatsAsync(
                "MasterComparison_Sql_FileBacked_PointLookup",
                repeatCount: 1,
                warmupSingleSample: true,
                runScenarioAsync: _ =>
                {
                    invocationCount++;
                    return Task.FromResult(new BenchmarkResult
                    {
                        Name = "not-invoked",
                        LatencySamples = 0,
                    });
                }));

        Assert.Equal(0, invocationCount);
        Assert.Contains("Unknown master durable-write qualification row", exception.Message);
    }

    [Fact]
    public async Task DurableWriteScenarioCliPath_RejectsMismatchedReturnedRow()
    {
        const string requestedRow = "MasterComparison_Sql_FileBacked_SingleInsert";

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => BenchmarkProgram.RunMasterComparisonDurableWriteScenarioWithRepeatsAsync(
                requestedRow,
                repeatCount: 1,
                warmupSingleSample: false,
                runScenarioAsync: _ => Task.FromResult(CreateDurableQualificationResult(
                    "MasterComparison_Sql_FileBacked_BatchInsertRows"))));

        Assert.Contains(requestedRow, exception.Message);
        Assert.Contains("MasterComparison_Sql_FileBacked_BatchInsertRows", exception.Message);
    }

    [Fact]
    public void DurableWriteScenarioWriter_RejectsMalformedQualificationEvidence()
    {
        const string validRow = "MasterComparison_Sql_FileBacked_SingleInsert";
        BenchmarkResult valid = CreateDurableQualificationResult(validRow);
        var invalidCases = new Dictionary<string, IReadOnlyList<BenchmarkResult>>
        {
            ["multiple rows"] = [valid, CreateDurableQualificationResult(validRow)],
            ["unknown row"] = [CreateDurableQualificationResult("unknown-row")],
            ["sample floor"] = [CreateDurableQualificationResult(validRow, latencySamples: 9_999)],
            ["duration floor"] = [CreateDurableQualificationResult(
                validRow,
                elapsed: TimeSpan.FromSeconds(29.999))],
            ["duration cap"] = [CreateDurableQualificationResult(
                validRow,
                elapsed: TimeSpan.FromSeconds(120.001))],
            ["missing metadata"] = [CreateDurableQualificationResult(validRow, extraInfo: string.Empty)],
            ["mismatched interval"] = [CreateDurableQualificationResult(
                validRow,
                elapsed: TimeSpan.FromSeconds(30),
                extraInfo: CreateDurableQualificationExtraInfo(TimeSpan.FromSeconds(31)))],
        };

        foreach ((string description, IReadOnlyList<BenchmarkResult> rows) in invalidCases)
        {
            InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
                () => BenchmarkProgram.ValidateReleaseCoreResults(
                    BenchmarkProgram.DurableMasterWriteScenarioSuiteKey,
                    rows));
            Assert.False(string.IsNullOrWhiteSpace(exception.Message), description);
        }
    }

    [Theory]
    [InlineData("master-table")]
    [InlineData("master-table-durable-writes")]
    [InlineData("master-table-hosted-stable")]
    public async Task SuiteRunner_ReleaseEvidenceRowBelowSampleFloorFailsBeforeCsvEmission(
        string suiteName)
    {
        string temporaryRoot = CreateTemporaryDirectory();
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => BenchmarkProgram.RunSuiteWithRepeatsAsync(
                    suiteName,
                    () => Task.FromResult(new List<BenchmarkResult>
                    {
                        CreateResult(invocation: 99),
                    }),
                    repeatCount: 1,
                    outputDirectory: temporaryRoot));

            Assert.Contains(suiteName, exception.Message);
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

    private static BenchmarkResult CreateDurableQualificationResult(
        string rowName,
        int latencySamples = 10_000,
        TimeSpan? elapsed = null,
        string? extraInfo = null)
    {
        TimeSpan measured = elapsed ?? TimeSpan.FromSeconds(30);
        return new BenchmarkResult
        {
            Name = rowName,
            TotalOps = latencySamples,
            LatencySamples = latencySamples,
            ElapsedMs = measured.TotalMilliseconds,
            P99Ms = 5,
            ExtraInfo = extraInfo ?? CreateDurableQualificationExtraInfo(measured),
        };
    }

    private static string CreateDurableQualificationExtraInfo(TimeSpan measured)
    {
        DateTimeOffset beginUtc = new(2026, 8, 4, 20, 0, 0, TimeSpan.Zero);
        return ReleaseQualificationSettings.DurableWrite.CreateExtraInfo(
            beginUtc,
            beginUtc + measured);
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
