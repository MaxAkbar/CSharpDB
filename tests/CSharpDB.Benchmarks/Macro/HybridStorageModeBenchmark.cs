using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridStorageModeBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int InsertTradeoffRowsPerCommit = 1_000;
    private const int InsertTradeoffSeedRows = 20_000;
    private const int ConcurrentReaderCount = 8;
    private const int ReusedSessionBurstReads = 32;
    private const int HighThroughputLatencySampleEvery = 128;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan InsertTradeoffMeasuredDuration = TimeSpan.FromSeconds(10);
    private static readonly InsertTradeoffScenario[] s_insertTradeoffScenarios = CreateInsertTradeoffScenarios();

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

        foreach (InsertTradeoffScenario scenario in s_insertTradeoffScenarios)
            results.Add(await RunInsertTradeoffScenarioAsync(scenario));

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

    private static async Task<BenchmarkResult> RunInsertTradeoffScenarioAsync(InsertTradeoffScenario scenario)
    {
        await using var context = await InsertTradeoffContext.CreateAsync(scenario);
        var db = context.Database;
        var batch = db.PrepareInsertBatch("bench", initialCapacity: InsertTradeoffRowsPerCommit);
        var rowBuffer = new DbValue[4];
        DbValue textValue = DbValue.FromText("durable_batch");
        DbValue categoryValue = DbValue.FromText("Alpha");
        int nextSequence = scenario.SeedRows;

        async Task OperationAsync()
        {
            nextSequence = await ExecuteInsertBatchCommitAsync(
                db,
                batch,
                rowBuffer,
                nextSequence,
                InsertTradeoffRowsPerCommit,
                textValue,
                categoryValue);
        }

        string benchmarkName =
            $"StoragePlan2_{scenario.Name}_InsertBatch_B{InsertTradeoffRowsPerCommit}_Seed{scenario.SeedRows}_10s";
        BenchmarkResult rawResult = await MacroBenchmarkRunner.RunForDurationAsync(
            benchmarkName,
            WarmupDuration,
            InsertTradeoffMeasuredDuration,
            OperationAsync);

        double rowsPerSecond = rawResult.OpsPerSecond * InsertTradeoffRowsPerCommit;
        string? extraInfo = AppendExtraInfo(
            rawResult.ExtraInfo,
            $"throughput-unit=commits/sec; rowsPerSec={rowsPerSecond:F1}",
            $"rowsPerCommit={InsertTradeoffRowsPerCommit}",
            $"seedRows={scenario.SeedRows}",
            "schema=bench(id,value,text_col,category)",
            "keyPattern=monotonic",
            $"preset={scenario.PresetLabel}",
            $"durability={scenario.DurabilitySemantics}",
            $"residency={scenario.ResidencySemantics}");

        Console.WriteLine(
            $"    rows/sec={rowsPerSecond:N0}, P50={rawResult.P50Ms:F3}ms, P95={rawResult.P95Ms:F3}ms, P99={rawResult.P99Ms:F3}ms");
        Console.WriteLine($"    durability={scenario.DurabilitySemantics}");
        Console.WriteLine($"    residency={scenario.ResidencySemantics}");

        return CloneResult(rawResult, extraInfo);
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(StorageMode mode, bool reuseSessionBurstReads)
    {
        await using var context = await BenchmarkContext.CreateSqlReadAsync(mode);
        var db = context.Database;
        var histograms = new LatencyHistogram[ConcurrentReaderCount];
        int latencySampleEvery = reuseSessionBurstReads ? HighThroughputLatencySampleEvery : 1;

        for (int i = 0; i < ConcurrentReaderCount; i++)
            histograms[i] = new LatencyHistogram(latencySampleEvery);

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
            ExtraInfo = reuseSessionBurstReads
                ? $"session-mode=reused reader session; burst-reads={ReusedSessionBurstReads}; readers={ConcurrentReaderCount}; latency-sampling=1/{latencySampleEvery}"
                : $"session-mode=per-query reader session; readers={ConcurrentReaderCount}",
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
                Stopwatch? sw = histogram.ShouldSampleNext() ? Stopwatch.StartNew() : null;
                try
                {
                    await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", ct);
                    _ = await result.MoveNextAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                if (sw is null)
                {
                    histogram.RecordUnsampled();
                }
                else
                {
                    sw.Stop();
                    histogram.Record(sw.Elapsed.TotalMilliseconds);
                }
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

    private static InsertTradeoffScenario[] CreateInsertTradeoffScenarios()
    {
        return
        [
            new(
                "FileBackedDurableWriteOptimized",
                InsertTradeoffMode.FileBacked,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "full durable file-backed commits; each acknowledged commit forces durable backing-file visibility",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "FileBackedDurableLowLatency",
                InsertTradeoffMode.FileBacked,
                InsertTradeoffSeedRows,
                StoragePreset.LowLatency,
                "full durable file-backed commits; same durability as the baseline with deferred planner-stat persistence",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "FileBackedBufferedWriteOptimized",
                InsertTradeoffMode.FileBackedBuffered,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "buffered file-backed commits; managed buffers are flushed but recent commits remain more exposed on OS crash or power loss",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "InMemoryFresh",
                InsertTradeoffMode.InMemoryFresh,
                InsertTradeoffSeedRows,
                StoragePreset.NotApplicable,
                "no crash durability; the database exists only in private process memory",
                "new private in-memory database with no backing file"),
            new(
                "LoadIntoMemory",
                InsertTradeoffMode.LoadIntoMemory,
                InsertTradeoffSeedRows,
                StoragePreset.NotApplicable,
                "no crash durability after load; persistence requires an explicit later save back to disk",
                "an existing file plus committed WAL state are loaded once, then inserts run entirely in memory"),
            new(
                "HybridIncrementalDurable",
                InsertTradeoffMode.HybridIncrementalDurable,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "full durable commits through the hybrid WAL and checkpoint path",
                "the durable backing file remains authoritative while touched pages stay resident by cache policy"),
        ];
    }

    private static async Task InitializeInsertTradeoffDatabaseAsync(Database db, int seedRows)
    {
        await using var _ = await db.ExecuteAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);");

        if (seedRows <= 0)
            return;

        var batch = db.PrepareInsertBatch("bench", initialCapacity: InsertTradeoffRowsPerCommit);
        var rowBuffer = new DbValue[4];
        DbValue textValue = DbValue.FromText("durable_batch");
        DbValue categoryValue = DbValue.FromText("Alpha");
        int nextSequence = 0;

        while (nextSequence < seedRows)
        {
            int remaining = seedRows - nextSequence;
            int rowsThisCommit = Math.Min(InsertTradeoffRowsPerCommit, remaining);
            nextSequence = await ExecuteInsertBatchCommitAsync(
                db,
                batch,
                rowBuffer,
                nextSequence,
                rowsThisCommit,
                textValue,
                categoryValue);
        }
    }

    private static async Task<int> ExecuteInsertBatchCommitAsync(
        Database db,
        InsertBatch batch,
        DbValue[] rowBuffer,
        int nextSequence,
        int rowsToInsert,
        DbValue textValue,
        DbValue categoryValue)
    {
        batch.Clear();
        await db.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < rowsToInsert; i++)
            {
                nextSequence++;
                PopulateInsertTradeoffRow(rowBuffer, nextSequence, textValue, categoryValue);
                batch.AddRow(rowBuffer);
            }

            int rowsAffected = await batch.ExecuteAsync();
            if (rowsAffected != rowsToInsert)
            {
                throw new InvalidOperationException(
                    $"Expected {rowsToInsert} inserted rows, observed {rowsAffected}.");
            }

            await db.CommitAsync();
            return nextSequence;
        }
        catch
        {
            await RollbackQuietlyAsync(db);
            throw;
        }
    }

    private static void PopulateInsertTradeoffRow(
        DbValue[] row,
        int sequence,
        DbValue textValue,
        DbValue categoryValue)
    {
        row[0] = DbValue.FromInteger(sequence);
        row[1] = DbValue.FromInteger(sequence);
        row[2] = textValue;
        row[3] = categoryValue;
    }

    private static async Task RollbackQuietlyAsync(Database db)
    {
        try
        {
            await db.RollbackAsync();
        }
        catch
        {
            // Preserve the original benchmark failure.
        }
    }

    private static DatabaseOptions CreateInsertTradeoffOptions(StoragePreset preset, DurabilityMode durabilityMode)
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UseDurabilityMode(durabilityMode);

            if (preset == StoragePreset.LowLatency)
            {
                builder.UseLowLatencyDurableWritePreset();
            }
            else
            {
                builder.UseWriteOptimizedPreset();
            }
        });
    }

    private static BenchmarkResult CloneResult(BenchmarkResult source, string? extraInfo)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = source.TotalOps,
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo,
        };
    }

    private static string? AppendExtraInfo(params string?[] values)
    {
        var parts = new List<string>(values.Length);
        foreach (string? value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                parts.Add(value);
        }

        return parts.Count == 0 ? null : string.Join("; ", parts);
    }

    private static string NewInsertTradeoffDbPath(string prefix)
        => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

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

    private sealed class InsertTradeoffContext : IAsyncDisposable
    {
        private readonly string[] _cleanupPaths;

        private InsertTradeoffContext(Database database, params string?[] cleanupPaths)
        {
            Database = database;
            _cleanupPaths = cleanupPaths
                .Where(static path => !string.IsNullOrWhiteSpace(path))
                .Select(static path => path!)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        internal Database Database { get; }

        internal static async Task<InsertTradeoffContext> CreateAsync(InsertTradeoffScenario scenario)
        {
            switch (scenario.Mode)
            {
                case InsertTradeoffMode.FileBacked:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-file");
                    var database = await Database.OpenAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Durable));
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                case InsertTradeoffMode.FileBackedBuffered:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-buffered");
                    var database = await Database.OpenAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Buffered));
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                case InsertTradeoffMode.InMemoryFresh:
                {
                    var database = await Database.OpenInMemoryAsync();
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database);
                }

                case InsertTradeoffMode.LoadIntoMemory:
                {
                    string sourcePath = NewInsertTradeoffDbPath("storage-plan2-load");
                    await using (var source = await Database.OpenAsync(
                                     sourcePath,
                                     CreateInsertTradeoffOptions(StoragePreset.WriteOptimized, DurabilityMode.Durable)))
                    {
                        await InitializeInsertTradeoffDatabaseAsync(source, scenario.SeedRows);
                    }

                    var database = await Database.LoadIntoMemoryAsync(sourcePath);
                    return new InsertTradeoffContext(database, sourcePath);
                }

                case InsertTradeoffMode.HybridIncrementalDurable:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-hybrid");
                    var database = await Database.OpenHybridAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Durable),
                        new HybridDatabaseOptions
                        {
                            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                        });
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(scenario.Mode), scenario.Mode, null);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Database.DisposeAsync();
            foreach (string path in _cleanupPaths)
                InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(path);
        }
    }

    private sealed record InsertTradeoffScenario(
        string Name,
        InsertTradeoffMode Mode,
        int SeedRows,
        StoragePreset Preset,
        string DurabilitySemantics,
        string ResidencySemantics)
    {
        public string PresetLabel => Preset switch
        {
            StoragePreset.WriteOptimized => "write-optimized",
            StoragePreset.LowLatency => "low-latency durable",
            StoragePreset.NotApplicable => "n/a",
            _ => throw new ArgumentOutOfRangeException(nameof(Preset), Preset, null),
        };
    }

    private enum InsertTradeoffMode
    {
        FileBacked,
        FileBackedBuffered,
        InMemoryFresh,
        LoadIntoMemory,
        HybridIncrementalDurable,
    }

    private enum StoragePreset
    {
        WriteOptimized,
        LowLatency,
        NotApplicable,
    }
}
