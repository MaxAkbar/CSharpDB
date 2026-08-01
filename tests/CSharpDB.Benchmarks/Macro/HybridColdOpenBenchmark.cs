using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridColdOpenBenchmark
{
    private const int SeedCount = 200_000;
    private const int MinimumMeasuredIterations = 500;
    private const int SqlLookupId = 175_321;
    private const int CollectionLookupId = 175_321;
    private static readonly TimeSpan MinimumMeasuredDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan MaximumMeasuredDuration = TimeSpan.FromSeconds(90);

    private sealed record BenchDoc(string Name, int Value, string Category);

    private enum StorageMode
    {
        FileBacked,
        InMemory,
        HybridIncrementalDurable,
        HybridHotSetIncrementalDurable,
    }

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        List<IReadOnlyList<BenchmarkResult>> runs = await RunRepeatedAsync(1);
        return runs[0].ToList();
    }

    public static async Task<List<IReadOnlyList<BenchmarkResult>>> RunRepeatedAsync(
        int repeatCount,
        bool warmupSingleSample = false)
    {
        bool warmUpEachScenario = ShouldWarmUpEachScenario(repeatCount, warmupSingleSample);

        await using var inputs = await SeededInputs.CreateAsync();
        await PrimeCodePathsAsync();

        var scenarios = new List<Func<Task<BenchmarkResult>>>();
        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            StorageMode scenarioMode = mode;
            scenarios.Add(() => RunSqlOpenOnlyAsync(scenarioMode, inputs.SqlFilePath));
            scenarios.Add(() => RunSqlOpenAndFirstLookupAsync(scenarioMode, inputs.SqlFilePath));
            scenarios.Add(() => RunCollectionOpenOnlyAsync(scenarioMode, inputs.CollectionFilePath));
            scenarios.Add(() => RunCollectionOpenAndFirstGetAsync(scenarioMode, inputs.CollectionFilePath));
        }

        return await ScenarioMajorBenchmarkRunner.RunAsync(
            scenarios,
            repeatCount,
            warmUpEachScenario);
    }

    internal static bool ShouldWarmUpEachScenario(
        int repeatCount,
        bool warmupSingleSample)
    {
        if (repeatCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(repeatCount), "Repeat count must be positive.");
        if (warmupSingleSample && repeatCount != 1)
        {
            throw new ArgumentException(
                "Single-sample warmup requires exactly one recorded repeat.",
                nameof(warmupSingleSample));
        }

        return repeatCount > 1 || warmupSingleSample;
    }

    private static Task<BenchmarkResult> RunSqlOpenOnlyAsync(StorageMode mode, string filePath)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Sql_OpenOnly_{SeedCount}",
            async () =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath);
            });
    }

    private static Task<BenchmarkResult> RunSqlOpenAndFirstLookupAsync(StorageMode mode, string filePath)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Sql_OpenAndFirstLookup_{SeedCount}",
            async () =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath);
                await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {SqlLookupId};");
                if (!await result.MoveNextAsync() || result.Current[0].AsInteger != SqlLookupId * 10L)
                    throw new InvalidOperationException($"Lookup for id={SqlLookupId} returned an unexpected result.");
            });
    }

    private static Task<BenchmarkResult> RunCollectionOpenOnlyAsync(StorageMode mode, string filePath)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Collection_OpenOnly_{SeedCount}",
            async () =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath);
            });
    }

    private static Task<BenchmarkResult> RunCollectionOpenAndFirstGetAsync(StorageMode mode, string filePath)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Collection_OpenAndFirstGet_{SeedCount}",
            async () =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath);
                var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                BenchDoc? document = await collection.GetAsync($"doc:{CollectionLookupId}");
                if (document is null || document.Value != CollectionLookupId)
                    throw new InvalidOperationException($"Document 'doc:{CollectionLookupId}' was not found or was invalid.");
            });
    }

    private static async Task<BenchmarkResult> RunColdScenarioAsync(
        string name,
        Func<Task> operation)
    {
        MacroBenchmarkRunner.StabilizeAfterWarmup();

        var histogram = new LatencyHistogram();
        var totalSw = Stopwatch.StartNew();
        while (histogram.SampleCount < MinimumMeasuredIterations ||
               totalSw.Elapsed < MinimumMeasuredDuration)
        {
            if (totalSw.Elapsed >= MaximumMeasuredDuration)
            {
                throw new InvalidOperationException(
                    $"Cold-open scenario '{name}' retained only " +
                    $"{histogram.SampleCount} latency samples within " +
                    $"{MaximumMeasuredDuration.TotalSeconds:F0} seconds; at least " +
                    $"{MinimumMeasuredIterations} samples and " +
                    $"{MinimumMeasuredDuration.TotalSeconds:F0} seconds are required.");
            }

            var sw = Stopwatch.StartNew();
            await operation();
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);

            if (totalSw.Elapsed > MaximumMeasuredDuration)
            {
                throw new InvalidOperationException(
                    $"Cold-open scenario '{name}' exceeded the " +
                    $"{MaximumMeasuredDuration.TotalSeconds:F0}-second measurement cap.");
            }
        }
        totalSw.Stop();

        BenchmarkResult result = BenchmarkResult.FromHistogram(
            name,
            histogram,
            totalSw.Elapsed.TotalMilliseconds);
        Console.WriteLine(
            $"  {name}: {result.OpsPerSecond:N0} ops/sec, " +
            $"P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, " +
            $"P999={result.P999Ms:F3}ms");
        return result;
    }

    private static async Task PrimeCodePathsAsync()
    {
        string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
            "hybrid-cold-open-prime-sql",
            rowCount: 32);
        string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
            "hybrid-cold-open-prime-col",
            rowCount: 32);

        try
        {
            foreach (StorageMode mode in Enum.GetValues<StorageMode>())
            {
                await using (var db = await OpenSqlDatabaseAsync(mode, sqlFilePath))
                {
                    await using var result = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 7;");
                    _ = await result.MoveNextAsync();
                }

                await using (var db = await OpenCollectionDatabaseAsync(mode, collectionFilePath))
                {
                    var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                    _ = await collection.GetAsync("doc:7");
                }
            }
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(sqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(collectionFilePath);
        }
    }

    private static ValueTask<Database> OpenSqlDatabaseAsync(StorageMode mode, string filePath)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotTableNames = new[] { "bench" },
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static ValueTask<Database> OpenCollectionDatabaseAsync(StorageMode mode, string filePath)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotCollectionNames = new[] { "bench_docs" },
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "ColdOpen_FileBacked",
            StorageMode.InMemory => "ColdOpen_InMemory",
            StorageMode.HybridIncrementalDurable => "ColdOpen_HybridIncrementalDurable",
            StorageMode.HybridHotSetIncrementalDurable => "ColdOpen_HybridHotSetIncrementalDurable",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    private sealed class SeededInputs : IAsyncDisposable
    {
        private SeededInputs(string sqlFilePath, string collectionFilePath)
        {
            SqlFilePath = sqlFilePath;
            CollectionFilePath = collectionFilePath;
        }

        public string SqlFilePath { get; }
        public string CollectionFilePath { get; }

        public static async Task<SeededInputs> CreateAsync()
        {
            string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
                "hybrid-cold-open-sql",
                SeedCount);
            string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
                "hybrid-cold-open-col",
                SeedCount);
            return new SeededInputs(sqlFilePath, collectionFilePath);
        }

        public ValueTask DisposeAsync()
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(SqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(CollectionFilePath);
            return ValueTask.CompletedTask;
        }
    }
}
