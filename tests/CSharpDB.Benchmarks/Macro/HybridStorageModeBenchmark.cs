using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridStorageModeBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int ConcurrentReaderCount = 8;
    private const int ReusedSessionBurstReads = 32;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

    private sealed record BenchDoc(string Name, int Value, string Category);

    private enum StorageMode
    {
        FileBacked,
        InMemory,
        HybridIncrementalDurable,
    }

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            results.Add(await RunSqlSingleInsertAsync(mode));
            results.Add(await RunSqlBatchInsertAsync(mode));
            results.Add(await RunSqlPointLookupAsync(mode));
            results.Add(await RunSqlConcurrentReadsAsync(mode, reuseSessionBurstReads: false));
            results.Add(await RunSqlConcurrentReadsAsync(mode, reuseSessionBurstReads: true));
            results.Add(await RunCollectionPutAsync(mode));
            results.Add(await RunCollectionBatchInsertAsync(mode));
            results.Add(await RunCollectionGetAsync(mode));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateSqlWriteAsync(mode);
        var db = context.Database;
        int nextId = SeedCount + 1_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Sql_SingleInsert_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                await using var result = await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');");
                if (result.RowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {result.RowsAffected}.");
            });
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateSqlWriteAsync(mode);
        var db = context.Database;
        int nextId = SeedCount + 2_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Sql_Batch{BatchSize}_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                await db.BeginTransactionAsync();
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        await using var result = await db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');");
                        if (result.RowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {result.RowsAffected}.");
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            });
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateSqlReadAsync(mode);
        var db = context.Database;
        var rng = new Random(42);

        await WarmSqlLookupsAsync(db, rng, 128);

        rng = new Random(42);
        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Sql_PointLookup_{SeedCount}",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};");
                if (!await result.MoveNextAsync() || result.Current[0].AsInteger != id * 10L)
                    throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result.");
            });
    }

    private static async Task<BenchmarkResult> RunCollectionPutAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateCollectionWriteAsync(mode);
        var collection = context.Collection!;
        int nextId = SeedCount + 3_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Collection_Put_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                await collection.PutAsync(
                    $"doc:{id}",
                    new BenchDoc($"User_{id}", id, GetCategory(id)));
            });
    }

    private static async Task<BenchmarkResult> RunCollectionBatchInsertAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateCollectionWriteAsync(mode);
        var db = context.Database;
        var collection = context.Collection!;
        int nextId = SeedCount + 4_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Collection_Batch{BatchSize}_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                await db.BeginTransactionAsync();
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        await collection.PutAsync(
                            $"doc:{id}",
                            new BenchDoc($"User_{id}", id, GetCategory(id)));
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            });
    }

    private static async Task<BenchmarkResult> RunCollectionGetAsync(StorageMode mode)
    {
        await using var context = await BenchmarkContext.CreateCollectionReadAsync(mode);
        var collection = context.Collection!;
        var rng = new Random(84);

        await WarmCollectionGetsAsync(collection, rng, 128);

        rng = new Random(84);
        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(mode)}_Collection_Get_{SeedCount}",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                BenchDoc? document = await collection.GetAsync($"doc:{id}");
                if (document is null || document.Value != id)
                    throw new InvalidOperationException($"Document 'doc:{id}' was not found or was invalid.");
            });
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(StorageMode mode, bool reuseSessionBurstReads)
    {
        await using var context = await BenchmarkContext.CreateSqlReadAsync(mode);
        var db = context.Database;
        var histograms = new LatencyHistogram[ConcurrentReaderCount];

        for (int i = 0; i < ConcurrentReaderCount; i++)
            histograms[i] = new LatencyHistogram();

        using var cts = new CancellationTokenSource(MeasuredDuration);
        var readerTasks = new Task[ConcurrentReaderCount];
        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            var histogram = histograms[readerIndex];
            readerTasks[readerIndex] = Task.Run(
                () => reuseSessionBurstReads
                    ? RunReusedReaderLoopAsync(db, histogram, cts.Token)
                    : RunPerQueryReaderLoopAsync(db, histogram, cts.Token),
                cts.Token);
        }

        await Task.WhenAll(readerTasks);

        return new BenchmarkResult
        {
            Name = reuseSessionBurstReads
                ? $"{GetPrefix(mode)}_Sql_ConcurrentReadsBurst{ReusedSessionBurstReads}_{ConcurrentReaderCount}readers"
                : $"{GetPrefix(mode)}_Sql_ConcurrentReads_{ConcurrentReaderCount}readers",
            TotalOps = histograms.Sum(static histogram => histogram.Count),
            ElapsedMs = MeasuredDuration.TotalMilliseconds,
            P50Ms = histograms.Average(static histogram => histogram.Percentile(0.50)),
            P90Ms = histograms.Average(static histogram => histogram.Percentile(0.90)),
            P95Ms = histograms.Average(static histogram => histogram.Percentile(0.95)),
            P99Ms = histograms.Average(static histogram => histogram.Percentile(0.99)),
            P999Ms = histograms.Average(static histogram => histogram.Percentile(0.999)),
            MinMs = histograms.Min(static histogram => histogram.Min),
            MaxMs = histograms.Max(static histogram => histogram.Max),
            MeanMs = histograms.Average(static histogram => histogram.Mean),
            StdDevMs = histograms.Average(static histogram => histogram.StdDev),
        };
    }

    private static async Task RunPerQueryReaderLoopAsync(Database db, LatencyHistogram histogram, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                using var reader = db.CreateReaderSession();
                await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", ct);
                _ = await result.MoveNextAsync(ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }
    }

    private static async Task RunReusedReaderLoopAsync(Database db, LatencyHistogram histogram, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            using var reader = db.CreateReaderSession();
            for (int i = 0; i < ReusedSessionBurstReads && !ct.IsCancellationRequested; i++)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", ct);
                    _ = await result.MoveNextAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                sw.Stop();
                histogram.Record(sw.Elapsed.TotalMilliseconds);
            }
        }
    }

    private static async Task WarmSqlLookupsAsync(Database db, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};");
            _ = await result.MoveNextAsync();
        }
    }

    private static async Task WarmCollectionGetsAsync(Collection<BenchDoc> collection, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            _ = await collection.GetAsync($"doc:{id}");
        }
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "Storage_FileBacked",
            StorageMode.InMemory => "Storage_InMemory",
            StorageMode.HybridIncrementalDurable => "Storage_HybridIncrementalDurable",
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

    private sealed class BenchmarkContext : IAsyncDisposable
    {
        private readonly string? _filePath;

        private BenchmarkContext(Database database, Collection<BenchDoc>? collection, string? filePath)
        {
            Database = database;
            Collection = collection;
            _filePath = filePath;
        }

        internal Database Database { get; }
        internal Collection<BenchDoc>? Collection { get; }

        internal static async Task<BenchmarkContext> CreateSqlWriteAsync(StorageMode mode)
        {
            var (database, filePath) = await OpenDatabaseAsync(mode);
            await database.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");
            return new BenchmarkContext(database, collection: null, filePath);
        }

        internal static async Task<BenchmarkContext> CreateSqlReadAsync(StorageMode mode)
        {
            string? seededFilePath = null;
            Database database;

            switch (mode)
            {
                case StorageMode.FileBacked:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await Database.OpenAsync(seededFilePath, BenchmarkDurability.Apply());
                    break;
                case StorageMode.InMemory:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await Database.LoadIntoMemoryAsync(seededFilePath);
                    break;
                case StorageMode.HybridIncrementalDurable:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await OpenHybridAsync(seededFilePath);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            return new BenchmarkContext(database, collection: null, seededFilePath);
        }

        internal static async Task<BenchmarkContext> CreateCollectionWriteAsync(StorageMode mode)
        {
            var (database, filePath) = await OpenDatabaseAsync(mode);
            var collection = await database.GetCollectionAsync<BenchDoc>("bench_docs");
            return new BenchmarkContext(database, collection, filePath);
        }

        internal static async Task<BenchmarkContext> CreateCollectionReadAsync(StorageMode mode)
        {
            string? seededFilePath = null;
            Database database;

            switch (mode)
            {
                case StorageMode.FileBacked:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await Database.OpenAsync(seededFilePath, BenchmarkDurability.Apply());
                    break;
                case StorageMode.InMemory:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await Database.LoadIntoMemoryAsync(seededFilePath);
                    break;
                case StorageMode.HybridIncrementalDurable:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await OpenHybridAsync(seededFilePath);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            var collection = await database.GetCollectionAsync<BenchDoc>("bench_docs");
            return new BenchmarkContext(database, collection, seededFilePath);
        }

        public async ValueTask DisposeAsync()
        {
            await Database.DisposeAsync();
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_filePath);
        }

        private static async Task<(Database Database, string? FilePath)> OpenDatabaseAsync(StorageMode mode)
        {
            return mode switch
            {
                StorageMode.FileBacked => await OpenFileBackedAsync(),
                StorageMode.InMemory => (await Database.OpenInMemoryAsync(), null),
                StorageMode.HybridIncrementalDurable => await OpenHybridModeAsync(),
                _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
            };
        }

        private static async Task<(Database Database, string FilePath)> OpenFileBackedAsync()
        {
            string filePath = NewTempDbPath("storage-file");
            return (await Database.OpenAsync(filePath, BenchmarkDurability.Apply()), filePath);
        }

        private static async Task<(Database Database, string FilePath)> OpenHybridModeAsync()
        {
            string filePath = NewTempDbPath("storage-hybrid");
            return (await OpenHybridAsync(filePath), filePath);
        }

        private static async Task<Database> OpenHybridAsync(string filePath)
        {
            return await Database.OpenHybridAsync(
                filePath,
                BenchmarkDurability.Apply(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                });
        }

        private static async Task<string> CreateSeededSqlDatabaseAsync()
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"storage-hybrid-sql_{Guid.NewGuid():N}.db");
            await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
            await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");

            const int seedBatchSize = 500;
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += seedBatchSize)
            {
                await db.BeginTransactionAsync();
                try
                {
                    int batchEnd = Math.Min(batchStart + seedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        await db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');");
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            }

            return filePath;
        }

        private static async Task<string> CreateSeededCollectionDatabaseAsync()
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"storage-hybrid-col_{Guid.NewGuid():N}.db");
            await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
            var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");

            const int seedBatchSize = 500;
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += seedBatchSize)
            {
                await db.BeginTransactionAsync();
                try
                {
                    int batchEnd = Math.Min(batchStart + seedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        await collection.PutAsync(
                            $"doc:{id}",
                            new BenchDoc($"User_{id}", id, GetCategory(id)));
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            }

            return filePath;
        }

        private static string NewTempDbPath(string prefix)
            => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
    }
}
