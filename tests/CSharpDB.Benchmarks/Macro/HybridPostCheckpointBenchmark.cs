using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridPostCheckpointBenchmark
{
    private const int SeedCount = 20_000;
    private const int HotSetSize = 256;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

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

        int[] hotIds = CreateHotIds();
        var results = new List<BenchmarkResult>();

        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            results.Add(await RunSqlPostCheckpointHotBurstAsync(mode, inputs.SqlFilePath, hotIds));
            results.Add(await RunCollectionPostCheckpointHotBurstAsync(mode, inputs.CollectionFilePath, hotIds));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSqlPostCheckpointHotBurstAsync(
        StorageMode mode,
        string filePath,
        IReadOnlyList<int> hotIds)
    {
        string workingFilePath = CloneDatabaseFiles(filePath, "hybrid-post-checkpoint-sql-work");
        try
        {
            await using var db = await OpenSqlDatabaseAsync(mode, workingFilePath);
            await WarmSqlLookupsAsync(db, hotIds);
            int nextId = SeedCount + 1_000_000;

            return await RunPostCheckpointBurstAsync(
                $"{GetPrefix(mode)}_Sql_PostCheckpointHotBurst{hotIds.Count}",
                hotIds.Count,
                WarmupDuration,
                MeasuredDuration,
                async ct =>
                {
                    int id = nextId++;
                    await using var insert = await db.ExecuteAsync(
                        $"INSERT INTO bench VALUES ({id}, {id * 10L}, 'hot_{id}', '{GetCategory(id)}');",
                        ct);
                    if (insert.RowsAffected != 1)
                        throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {insert.RowsAffected}.");

                    return await MeasureSqlHotBurstAsync(db, hotIds, ct);
                });
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(workingFilePath);
        }
    }

    private static async Task<BenchmarkResult> RunCollectionPostCheckpointHotBurstAsync(
        StorageMode mode,
        string filePath,
        IReadOnlyList<int> hotIds)
    {
        string workingFilePath = CloneDatabaseFiles(filePath, "hybrid-post-checkpoint-col-work");
        try
        {
            await using var db = await OpenCollectionDatabaseAsync(mode, workingFilePath);
            var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
            await WarmCollectionGetsAsync(collection, hotIds);
            int nextId = SeedCount + 2_000_000;

            return await RunPostCheckpointBurstAsync(
                $"{GetPrefix(mode)}_Collection_PostCheckpointHotBurst{hotIds.Count}",
                hotIds.Count,
                WarmupDuration,
                MeasuredDuration,
                async ct =>
                {
                    int id = nextId++;
                    await collection.PutAsync($"doc:{id}", new BenchDoc($"User_{id}", id, GetCategory(id)), ct);
                    return await MeasureCollectionHotBurstAsync(collection, hotIds, ct);
                });
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(workingFilePath);
        }
    }

    private static async Task<BenchmarkResult> RunPostCheckpointBurstAsync(
        string name,
        int operationsPerBurst,
        TimeSpan warmupDuration,
        TimeSpan measuredDuration,
        Func<CancellationToken, Task<TimeSpan>> executeScenarioAsync,
        CancellationToken ct = default)
    {
        var warmupEnd = DateTime.UtcNow + warmupDuration;
        while (DateTime.UtcNow < warmupEnd && !ct.IsCancellationRequested)
        {
            _ = await executeScenarioAsync(ct);
        }

        var histogram = new LatencyHistogram();
        int totalOps = 0;
        var wallClock = Stopwatch.StartNew();
        var measuredEnd = DateTime.UtcNow + measuredDuration;

        while (DateTime.UtcNow < measuredEnd && !ct.IsCancellationRequested)
        {
            TimeSpan burstElapsed = await executeScenarioAsync(ct);
            double perOperationMs = burstElapsed.TotalMilliseconds / operationsPerBurst;
            for (int i = 0; i < operationsPerBurst; i++)
                histogram.Record(perOperationMs);
            totalOps += operationsPerBurst;
        }

        wallClock.Stop();

        var baseResult = BenchmarkResult.FromHistogram(name, histogram, wallClock.Elapsed.TotalMilliseconds);
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
            ExtraInfo = $"BurstSize={operationsPerBurst}; After one auto-checkpointed write per burst.",
        };

        Console.WriteLine($"  {name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, P999={result.P999Ms:F3}ms");
        return result;
    }

    private static async Task<TimeSpan> MeasureSqlHotBurstAsync(Database db, IReadOnlyList<int> hotIds, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        foreach (int id in hotIds)
        {
            await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};", ct);
            if (!await result.MoveNextAsync(ct) || result.Current[0].AsInteger != id * 10L)
                throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result.");
        }

        sw.Stop();
        return sw.Elapsed;
    }

    private static async Task<TimeSpan> MeasureCollectionHotBurstAsync(
        Collection<BenchDoc> collection,
        IReadOnlyList<int> hotIds,
        CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        foreach (int id in hotIds)
        {
            BenchDoc? document = await collection.GetAsync($"doc:{id}", ct);
            if (document is null || document.Value != id)
                throw new InvalidOperationException($"Document 'doc:{id}' was not found or was invalid.");
        }

        sw.Stop();
        return sw.Elapsed;
    }

    private static async Task WarmSqlLookupsAsync(Database db, IReadOnlyList<int> hotIds)
    {
        foreach (int id in hotIds)
        {
            await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};");
            _ = await result.MoveNextAsync();
        }
    }

    private static async Task WarmCollectionGetsAsync(Collection<BenchDoc> collection, IReadOnlyList<int> hotIds)
    {
        foreach (int id in hotIds)
            _ = await collection.GetAsync($"doc:{id}");
    }

    private static ValueTask<Database> OpenSqlDatabaseAsync(StorageMode mode, string filePath)
    {
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                }
            }
        };

        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, options),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, options),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                options,
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                options,
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
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                }
            }
        };

        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, options),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, options),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                options,
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                options,
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotCollectionNames = new[] { "bench_docs" },
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static int[] CreateHotIds()
    {
        var rng = new Random(20260317);
        var ids = new HashSet<int>();
        while (ids.Count < HotSetSize)
            ids.Add(rng.Next(1, SeedCount + 1));
        return ids.ToArray();
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "CheckpointHot_FileBacked",
            StorageMode.InMemory => "CheckpointHot_InMemory",
            StorageMode.HybridIncrementalDurable => "CheckpointHot_HybridIncrementalDurable",
            StorageMode.HybridHotSetIncrementalDurable => "CheckpointHot_HybridHotSetIncrementalDurable",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static string CloneDatabaseFiles(string sourceFilePath, string prefix)
    {
        string destinationFilePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
        File.Copy(sourceFilePath, destinationFilePath, overwrite: true);

        string sourceWalPath = sourceFilePath + ".wal";
        if (File.Exists(sourceWalPath))
            File.Copy(sourceWalPath, destinationFilePath + ".wal", overwrite: true);

        return destinationFilePath;
    }

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
                "hybrid-post-checkpoint-sql",
                SeedCount);
            string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
                "hybrid-post-checkpoint-col",
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
