namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Collects repeated benchmark evidence with each scenario's measurements kept
/// adjacent. This avoids turning long-suite position drift into apparent
/// within-revision instability.
/// </summary>
public static class ScenarioMajorBenchmarkRunner
{
    public static async Task<List<IReadOnlyList<BenchmarkResult>>> RunAsync(
        IReadOnlyList<Func<Task<BenchmarkResult>>> scenarios,
        int repeatCount,
        bool warmUpEachScenario)
    {
        ArgumentNullException.ThrowIfNull(scenarios);
        if (repeatCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(repeatCount), "Repeat count must be positive.");

        var runs = Enumerable.Range(0, repeatCount)
            .Select(static _ => new List<BenchmarkResult>())
            .ToArray();
        var benchmarkNames = new HashSet<string>(StringComparer.Ordinal);

        for (int scenarioIndex = 0; scenarioIndex < scenarios.Count; scenarioIndex++)
        {
            Func<Task<BenchmarkResult>> scenario = scenarios[scenarioIndex]
                ?? throw new ArgumentException(
                    $"Scenario {scenarioIndex + 1} is null.",
                    nameof(scenarios));

            if (warmUpEachScenario)
            {
                Console.WriteLine(
                    $"=== Scenario {scenarioIndex + 1}/{scenarios.Count} warmup (not recorded) ===");
                await scenario();
                MacroBenchmarkRunner.StabilizeAfterWarmup();
            }

            string? benchmarkName = null;
            for (int runIndex = 0; runIndex < repeatCount; runIndex++)
            {
                if (repeatCount > 1)
                {
                    Console.WriteLine(
                        $"=== Scenario {scenarioIndex + 1}/{scenarios.Count}, " +
                        $"run {runIndex + 1}/{repeatCount} ===");
                }

                BenchmarkResult result = await scenario();
                benchmarkName ??= result.Name;
                if (!string.Equals(result.Name, benchmarkName, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Scenario {scenarioIndex + 1} changed benchmark name from " +
                        $"'{benchmarkName}' to '{result.Name}' across repeated runs.");
                }

                runs[runIndex].Add(result);
            }

            if (!benchmarkNames.Add(benchmarkName!))
            {
                throw new InvalidOperationException(
                    $"Scenario-major benchmark collection produced duplicate row '{benchmarkName}'.");
            }
        }

        return runs
            .Select(static run => (IReadOnlyList<BenchmarkResult>)run)
            .ToList();
    }
}
