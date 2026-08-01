using System.Globalization;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Tests;

public sealed class BenchmarkEvidenceAggregationTests
{
    [Fact]
    public async Task ScenarioMajorRunner_KeepsRepeatedMeasurementsAdjacentAndGroupsRawRuns()
    {
        var executionOrder = new List<string>();
        int firstInvocation = 0;
        int secondInvocation = 0;
        IReadOnlyList<Func<Task<BenchmarkResult>>> scenarios =
        [
            () => CreateScenarioResultAsync("first", ++firstInvocation, executionOrder),
            () => CreateScenarioResultAsync("second", ++secondInvocation, executionOrder),
        ];

        List<IReadOnlyList<BenchmarkResult>> runs = await ScenarioMajorBenchmarkRunner.RunAsync(
            scenarios,
            repeatCount: 3,
            warmUpEachScenario: true);

        Assert.Equal(
            ["first-1", "first-2", "first-3", "first-4", "second-1", "second-2", "second-3", "second-4"],
            executionOrder);
        Assert.Equal(3, runs.Count);
        Assert.All(runs, run => Assert.Equal(["first", "second"], run.Select(result => result.Name)));
        Assert.Equal([2, 3, 4], runs.Select(run => run[0].TotalOps));
        Assert.Equal([2, 3, 4], runs.Select(run => run[1].TotalOps));
    }

    [Fact]
    public async Task ScenarioMajorRunner_RejectsNamesThatChangeAcrossRepeats()
    {
        int invocation = 0;
        IReadOnlyList<Func<Task<BenchmarkResult>>> scenarios =
        [
            () => CreateScenarioResultAsync(
                ++invocation == 1 ? "first" : "changed",
                invocation,
                []),
        ];

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ScenarioMajorBenchmarkRunner.RunAsync(
                scenarios,
                repeatCount: 2,
                warmUpEachScenario: false));

        Assert.Contains("changed benchmark name", exception.Message);
    }

    [Fact]
    public async Task ScenarioMajorRunner_RejectsDuplicateRows()
    {
        IReadOnlyList<Func<Task<BenchmarkResult>>> scenarios =
        [
            () => CreateScenarioResultAsync("duplicate", 1, []),
            () => CreateScenarioResultAsync("duplicate", 1, []),
        ];

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ScenarioMajorBenchmarkRunner.RunAsync(
                scenarios,
                repeatCount: 1,
                warmUpEachScenario: false));

        Assert.Contains("duplicate row", exception.Message);
    }

    [Fact]
    public void MedianAcrossRuns_PreservesMedianThroughputEvidenceInCsv()
    {
        IReadOnlyList<IReadOnlyList<BenchmarkResult>> runs =
        [
            [CreateResult(totalOps: 1_000, elapsedMs: 100, latencySamples: 101, p99Ms: 3, "Run=1")],
            [CreateResult(totalOps: 2_000, elapsedMs: 100, latencySamples: 202, p99Ms: 1, "Run=2")],
            [CreateResult(totalOps: 3_000, elapsedMs: 200, latencySamples: 303, p99Ms: 2, "Run=3")],
        ];

        BenchmarkResult aggregate = Assert.Single(BenchmarkResultAggregator.MedianAcrossRuns(runs));

        Assert.Equal(3_000, aggregate.TotalOps);
        Assert.Equal(200, aggregate.ElapsedMs);
        Assert.Equal(15_000, aggregate.OpsPerSecond);
        Assert.Equal(202, aggregate.LatencySamples);
        Assert.Equal(2, aggregate.P99Ms);
        Assert.Equal("Run=3; Aggregate=median-of-3", aggregate.ExtraInfo);

        string temporaryRoot = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-benchmark-evidence-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temporaryRoot);
        try
        {
            decimal[] rawThroughputs = new decimal[runs.Count];
            for (int index = 0; index < runs.Count; index++)
            {
                string rawPath = Path.Combine(temporaryRoot, $"run-{index + 1}.csv");
                CsvReporter.WriteResults(rawPath, runs[index]);
                rawThroughputs[index] = ReadCsvMetric(rawPath, "OpsPerSec");
            }

            string aggregatePath = Path.Combine(temporaryRoot, "median.csv");
            CsvReporter.WriteResults(aggregatePath, [aggregate]);

            Assert.Contains(
                "Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec",
                File.ReadLines(aggregatePath).First());
            Assert.Equal(3_000m, ReadCsvMetric(aggregatePath, "TotalOps"));
            Assert.Equal(200m, ReadCsvMetric(aggregatePath, "ElapsedMs"));
            Assert.Equal(
                rawThroughputs.Order().ElementAt(rawThroughputs.Length / 2),
                ReadCsvMetric(aggregatePath, "OpsPerSec"));
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    private static BenchmarkResult CreateResult(
        int totalOps,
        double elapsedMs,
        int latencySamples,
        double p99Ms,
        string extraInfo)
    {
        return new BenchmarkResult
        {
            Name = "variable-throughput",
            TotalOps = totalOps,
            LatencySamples = latencySamples,
            ElapsedMs = elapsedMs,
            P99Ms = p99Ms,
            ExtraInfo = extraInfo,
        };
    }

    private static Task<BenchmarkResult> CreateScenarioResultAsync(
        string name,
        int invocation,
        List<string> executionOrder)
    {
        executionOrder.Add($"{name}-{invocation}");
        return Task.FromResult(new BenchmarkResult
        {
            Name = name,
            TotalOps = invocation,
            LatencySamples = 100,
            ElapsedMs = 1,
            P99Ms = 1,
        });
    }

    private static decimal ReadCsvMetric(string path, string columnName)
    {
        string[] lines = File.ReadAllLines(path);
        Assert.Equal(2, lines.Length);
        string[] columns = lines[0].Split(',');
        string[] values = lines[1].Split(',');
        int columnIndex = Array.IndexOf(columns, columnName);
        Assert.True(columnIndex >= 0, $"CSV column '{columnName}' was not found.");
        return decimal.Parse(values[columnIndex], CultureInfo.InvariantCulture);
    }
}
