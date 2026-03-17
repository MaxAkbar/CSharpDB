using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridHotSetReadBenchmark
{
    private const int SeedCount = 200_000;
    private const int BurstSize = 256;
    private const int WarmupIterations = 1;
    private const int MeasuredIterations = 15;

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
        await using var inputs = await SeededInputs.CreateAsync();
        await PrimeCodePathsAsync();

        int[] hotIds = CreateHotIds();
        var results = new List<BenchmarkResult>();

        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            results.Add(await RunSqlPostOpenHotBurstAsync(mode, inputs.SqlFilePath, hotIds));
            results.Add(await RunCollectionPostOpenHotBurstAsync(mode, inputs.CollectionFilePath, hotIds));
        }

        return results;
    }

    private static Task<BenchmarkResult> RunSqlPostOpenHotBurstAsync(
        StorageMode mode,
        string filePath,
        IReadOnlyList<int> hotIds)
    {
        return RunBurstIterationsAsync(
            $"{GetPrefix(mode)}_Sql_PostOpenHotBurst{hotIds.Count}_{SeedCount}",
            hotIds.Count,
            WarmupIterations,
            MeasuredIterations,
            async ct =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath, ct);
                var sw = Stopwatch.StartNew();
                foreach (int id in hotIds)
                {
                    await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};", ct);
                    if (!await result.MoveNextAsync(ct) || result.Current[0].AsInteger != id * 10L)
                        throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result.");
                }

                sw.Stop();
                return sw.Elapsed;
            });
    }

    private static Task<BenchmarkResult> RunCollectionPostOpenHotBurstAsync(
        StorageMode mode,
        string filePath,
        IReadOnlyList<int> hotIds)
    {
        return RunBurstIterationsAsync(
            $"{GetPrefix(mode)}_Collection_PostOpenHotBurst{hotIds.Count}_{SeedCount}",
            hotIds.Count,
            WarmupIterations,
            MeasuredIterations,
            async ct =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath, ct);
                var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs", ct);
                var sw = Stopwatch.StartNew();
                foreach (int id in hotIds)
                {
                    BenchDoc? document = await collection.GetAsync($"doc:{id}", ct);
                    if (document is null || document.Value != id)
                        throw new InvalidOperationException($"Document 'doc:{id}' was not found or was invalid.");
                }

                sw.Stop();
                return sw.Elapsed;
            });
    }

    private static async Task<BenchmarkResult> RunBurstIterationsAsync(
        string name,
        int operationsPerBurst,
        int warmupIterations,
        int measuredIterations,
        Func<CancellationToken, Task<TimeSpan>> executeScenarioAsync,
        CancellationToken ct = default)
    {
        for (int i = 0; i < warmupIterations && !ct.IsCancellationRequested; i++)
            _ = await executeScenarioAsync(ct);

        var histogram = new LatencyHistogram();
        double measuredElapsedMs = 0;
        for (int i = 0; i < measuredIterations && !ct.IsCancellationRequested; i++)
        {
            TimeSpan burstElapsed = await executeScenarioAsync(ct);
            measuredElapsedMs += burstElapsed.TotalMilliseconds;
            double perOperationMs = burstElapsed.TotalMilliseconds / operationsPerBurst;
            for (int op = 0; op < operationsPerBurst; op++)
                histogram.Record(perOperationMs);
        }

        var baseResult = BenchmarkResult.FromHistogram(name, histogram, measuredElapsedMs);
        var result = new BenchmarkResult
        {
            Name = baseResult.Name,
            TotalOps = baseResult.TotalOps,
            ElapsedMs = baseResult.ElapsedMs,
            P50Ms = baseResult.P50Ms,
            P90Ms = baseResult.P90Ms,
            P95Ms = baseResult.P95Ms,
            P99Ms = baseResult.P99Ms,
            P999Ms = baseResult.P999Ms,
            MinMs = baseResult.MinMs,
            MaxMs = baseResult.MaxMs,
            MeanMs = baseResult.MeanMs,
            StdDevMs = baseResult.StdDevMs,
            ExtraInfo = $"BurstSize={operationsPerBurst}; Open cost excluded.",
        };

        Console.WriteLine($"  {name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, P999={result.P999Ms:F3}ms");
        return result;
    }

    private static async Task PrimeCodePathsAsync()
    {
        string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
            "hybrid-hot-set-prime-sql",
            rowCount: 32);
        string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
            "hybrid-hot-set-prime-col",
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

    private static ValueTask<Database> OpenSqlDatabaseAsync(
        StorageMode mode,
        string filePath,
        CancellationToken ct = default)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, ct),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, ct),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
                ct),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotTableNames = new[] { "bench" },
                },
                ct),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static ValueTask<Database> OpenCollectionDatabaseAsync(
        StorageMode mode,
        string filePath,
        CancellationToken ct = default)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, ct),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, ct),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
                ct),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotCollectionNames = new[] { "bench_docs" },
                },
                ct),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static int[] CreateHotIds()
    {
        var rng = new Random(20260316);
        var ids = new HashSet<int>();
        while (ids.Count < BurstSize)
            ids.Add(rng.Next(1, SeedCount + 1));
        return ids.ToArray();
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "HotSetRead_FileBacked",
            StorageMode.InMemory => "HotSetRead_InMemory",
            StorageMode.HybridIncrementalDurable => "HotSetRead_HybridIncrementalDurable",
            StorageMode.HybridHotSetIncrementalDurable => "HotSetRead_HybridHotSetIncrementalDurable",
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
                "hybrid-hot-set-read-sql",
                SeedCount);
            string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
                "hybrid-hot-set-read-col",
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
