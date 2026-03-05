namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Aggregates repeated benchmark runs into a robust median summary.
/// </summary>
public static class BenchmarkResultAggregator
{
    public static List<BenchmarkResult> MedianAcrossRuns(IReadOnlyList<IReadOnlyList<BenchmarkResult>> runs)
    {
        if (runs.Count == 0)
            return new List<BenchmarkResult>();

        var runLookups = new Dictionary<string, BenchmarkResult>[runs.Count];
        for (int i = 0; i < runs.Count; i++)
        {
            runLookups[i] = runs[i].ToDictionary(static r => r.Name, StringComparer.Ordinal);
        }

        var aggregated = new List<BenchmarkResult>(runs[0].Count);
        foreach (var seed in runs[0])
        {
            var samples = new BenchmarkResult[runs.Count];
            for (int i = 0; i < runLookups.Length; i++)
            {
                if (!runLookups[i].TryGetValue(seed.Name, out var sample))
                    throw new InvalidOperationException($"Missing benchmark '{seed.Name}' in repeated run {i + 1}.");
                samples[i] = sample;
            }

            string aggregateTag = $"Aggregate=median-of-{runs.Count}";
            var medianOpsSample = samples.OrderBy(static s => s.OpsPerSecond).ToArray()[samples.Length / 2];
            string extraInfo = string.IsNullOrWhiteSpace(medianOpsSample.ExtraInfo)
                ? aggregateTag
                : $"{medianOpsSample.ExtraInfo}; {aggregateTag}";

            aggregated.Add(new BenchmarkResult
            {
                Name = seed.Name,
                TotalOps = (int)Math.Round(Median(samples.Select(static s => (double)s.TotalOps))),
                ElapsedMs = Median(samples.Select(static s => s.ElapsedMs)),
                P50Ms = Median(samples.Select(static s => s.P50Ms)),
                P90Ms = Median(samples.Select(static s => s.P90Ms)),
                P95Ms = Median(samples.Select(static s => s.P95Ms)),
                P99Ms = Median(samples.Select(static s => s.P99Ms)),
                P999Ms = Median(samples.Select(static s => s.P999Ms)),
                MinMs = Median(samples.Select(static s => s.MinMs)),
                MaxMs = Median(samples.Select(static s => s.MaxMs)),
                MeanMs = Median(samples.Select(static s => s.MeanMs)),
                StdDevMs = Median(samples.Select(static s => s.StdDevMs)),
                ExtraInfo = extraInfo,
            });
        }

        return aggregated;
    }

    private static double Median(IEnumerable<double> values)
    {
        var ordered = values.OrderBy(static value => value).ToArray();
        if (ordered.Length == 0)
            return 0;

        int middle = ordered.Length / 2;
        if ((ordered.Length & 1) == 1)
            return ordered[middle];

        return (ordered[middle - 1] + ordered[middle]) / 2.0;
    }
}
