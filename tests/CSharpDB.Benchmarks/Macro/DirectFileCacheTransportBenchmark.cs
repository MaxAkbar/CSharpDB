using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Execution;
using CSharpDB.Engine;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Macro;

public static class DirectFileCacheTransportBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int WarmupCount = 128;
    private const int ConcurrentReaderCount = 8;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(10);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        results.Add(await RunSqlSingleInsertAsync(tunedFileCache: true));
        results.Add(await RunSqlBatchInsertAsync(tunedFileCache: true));
        results.Add(await RunSqlPointLookupAsync(tunedFileCache: false));
        results.Add(await RunSqlPointLookupAsync(tunedFileCache: true));
        results.Add(await RunSqlConcurrentReadsAsync(tunedFileCache: false));
        results.Add(await RunSqlConcurrentReadsAsync(tunedFileCache: true));
        results.Add(await RunCollectionGetAsync(tunedFileCache: false));
        results.Add(await RunCollectionGetAsync(tunedFileCache: true));

        return results;
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        await using var client = context.CreateClient(tunedFileCache);
        int nextId = SeedCount + 5_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_SingleInsert_10s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                SqlExecutionResult result = await client.ExecuteSqlAsync(sql, CancellationToken.None);
                EnsureWriteSucceeded(result, 1, sql);
            });
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        await using var client = context.CreateClient(tunedFileCache);
        int nextId = SeedCount + 6_000_000;

        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_Batch{BatchSize}_10s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                TransactionSessionInfo tx = await client.BeginTransactionAsync(CancellationToken.None);
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                        SqlExecutionResult result = await client.ExecuteInTransactionAsync(
                            tx.TransactionId,
                            sql,
                            CancellationToken.None);
                        EnsureWriteSucceeded(result, 1, sql);
                    }

                    await client.CommitTransactionAsync(tx.TransactionId, CancellationToken.None);
                }
                catch
                {
                    try
                    {
                        await client.RollbackTransactionAsync(tx.TransactionId, CancellationToken.None);
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
            });
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        await using var client = context.CreateClient(tunedFileCache);
        var rng = new Random(42);

        await WarmSqlLookupsAsync(client, rng, WarmupCount);

        rng = new Random(42);
        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_PointLookup_{SeedCount}",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                SqlExecutionResult result = await client.ExecuteSqlAsync(
                    $"SELECT value FROM bench WHERE id = {id};",
                    CancellationToken.None);
                EnsureSingleRow(result, id);
            });
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        await using var writer = context.CreateClient(tunedFileCache);
        Database database = await GetSharedDatabaseAsync(writer);
        var readerHistograms = new LatencyHistogram[ConcurrentReaderCount];

        for (int i = 0; i < ConcurrentReaderCount; i++)
        {
            readerHistograms[i] = new LatencyHistogram();
        }

        await WarmConcurrentReadersAsync(writer, database);

        using var cts = new CancellationTokenSource(MeasuredDuration);
        int nextId = SeedCount + 7_000_000;
        var writerTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                int id = nextId++;
                try
                {
                    _ = await writer.ExecuteSqlAsync(
                        $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                        cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // Ignore transient contention during the concurrent-read run.
                }
            }
        });

        var readerTasks = new Task[ConcurrentReaderCount];
        for (int i = 0; i < ConcurrentReaderCount; i++)
        {
            LatencyHistogram histogram = readerHistograms[i];
            readerTasks[i] = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        using var reader = database.CreateReaderSession();
                        await using QueryResult result = await reader.ExecuteReadAsync(
                            "SELECT COUNT(*) FROM bench;",
                            cts.Token);
                        if (!await result.MoveNextAsync(cts.Token))
                            continue;
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch
                    {
                        continue;
                    }

                    sw.Stop();
                    histogram.Record(sw.Elapsed.TotalMilliseconds);
                }
            });
        }

        await Task.WhenAll(new[] { writerTask }.Concat(readerTasks));

        int totalReaderOps = readerHistograms.Sum(static h => h.Count);
        return new BenchmarkResult
        {
            Name = $"{GetPrefix(tunedFileCache)}_Sql_ConcurrentReads_{ConcurrentReaderCount}readers",
            TotalOps = totalReaderOps,
            ElapsedMs = MeasuredDuration.TotalMilliseconds,
            P50Ms = readerHistograms.Average(static h => h.Percentile(0.50)),
            P90Ms = readerHistograms.Average(static h => h.Percentile(0.90)),
            P95Ms = readerHistograms.Average(static h => h.Percentile(0.95)),
            P99Ms = readerHistograms.Average(static h => h.Percentile(0.99)),
            P999Ms = readerHistograms.Average(static h => h.Percentile(0.999)),
            MinMs = readerHistograms.Min(static h => h.Min),
            MaxMs = readerHistograms.Max(static h => h.Max),
            MeanMs = readerHistograms.Average(static h => h.Mean),
            StdDevMs = readerHistograms.Average(static h => h.StdDev),
        };
    }

    private static async Task<BenchmarkResult> RunCollectionGetAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        await using var client = context.CreateClient(tunedFileCache);
        var rng = new Random(84);

        await WarmCollectionGetsAsync(client, rng, WarmupCount);

        rng = new Random(84);
        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(tunedFileCache)}_Collection_Get_{SeedCount}",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                JsonElement? document = await client.GetDocumentAsync("bench_docs", $"doc:{id}", CancellationToken.None);
                if (document is null)
                    throw new InvalidOperationException($"Document 'doc:{id}' was not found in the direct hybrid benchmark dataset.");
            });
    }

    private static async Task WarmSqlLookupsAsync(ICSharpDbClient client, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            SqlExecutionResult result = await client.ExecuteSqlAsync(
                $"SELECT value FROM bench WHERE id = {id};",
                CancellationToken.None);
            EnsureSingleRow(result, id);
        }
    }

    private static async Task WarmCollectionGetsAsync(ICSharpDbClient client, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            JsonElement? document = await client.GetDocumentAsync("bench_docs", $"doc:{id}", CancellationToken.None);
            if (document is null)
                throw new InvalidOperationException($"Document 'doc:{id}' was not found during direct benchmark warmup.");
        }
    }

    private static async Task WarmConcurrentReadersAsync(ICSharpDbClient writer, Database database)
    {
        for (int i = 0; i < 16; i++)
        {
            _ = await writer.ExecuteSqlAsync(
                $"INSERT INTO bench VALUES ({SeedCount + 8_000_000 + i}, {(SeedCount + i) * 10L}, 'warmup');",
                CancellationToken.None);
        }

        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            for (int i = 0; i < 8; i++)
            {
                using var reader = database.CreateReaderSession();
                await using QueryResult result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", CancellationToken.None);
                _ = await result.MoveNextAsync(CancellationToken.None);
            }
        }
    }

    private static async Task<Database> GetSharedDatabaseAsync(CSharpDbClient writer)
    {
        Database? database = await writer.TryGetDatabaseAsync(CancellationToken.None);
        if (database is null)
            throw new InvalidOperationException("The direct hybrid benchmark requires an engine-backed client.");

        return database;
    }

    private static bool IsSuccessfulSingleRow(SqlExecutionResult result)
        => string.IsNullOrWhiteSpace(result.Error)
            && result.IsQuery
            && result.Rows is { Count: 1 };

    private static void EnsureSingleRow(SqlExecutionResult result, int id)
    {
        if (!IsSuccessfulSingleRow(result))
        {
            throw new InvalidOperationException(
                $"Expected one row for SQL lookup '{id}', but received error='{result.Error}', isQuery={result.IsQuery}, rowCount={result.Rows?.Count ?? 0}.");
        }
    }

    private static void EnsureWriteSucceeded(SqlExecutionResult result, int expectedRowsAffected, string sql)
    {
        if (!string.IsNullOrWhiteSpace(result.Error) || result.IsQuery || result.RowsAffected != expectedRowsAffected)
        {
            throw new InvalidOperationException(
                $"Expected write success for SQL '{sql}', but received error='{result.Error}', isQuery={result.IsQuery}, rowsAffected={result.RowsAffected}.");
        }
    }

    private static string GetPrefix(bool tunedFileCache)
        => tunedFileCache ? "Direct_DirectLookupPreset" : "Direct_Default";

    private static async Task<string> CreateSeededDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_direct_hybrid_{Guid.NewGuid():N}.db");

        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");
        var collection = await db.GetCollectionAsync<JsonElement>("bench_docs");
        var batch = db.PrepareInsertBatch("bench", initialCapacity: 512);

        const int batchSize = 512;
        for (int batchStart = 1; batchStart <= SeedCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            try
            {
                int batchEnd = Math.Min(batchStart + batchSize - 1, SeedCount);
                for (int id = batchStart; id <= batchEnd; id++)
                {
                    batch.AddRow(
                        CSharpDB.Primitives.DbValue.FromInteger(id),
                        CSharpDB.Primitives.DbValue.FromInteger(id * 10L),
                        CSharpDB.Primitives.DbValue.FromText(GetCategory(id)));
                    await collection.PutAsync($"doc:{id}", CreateBenchDocument(id));
                }

                int batchCount = batchEnd - batchStart + 1;
                AssertBatchCount(batchCount, await batch.ExecuteAsync(CancellationToken.None));
                await db.CommitAsync();
            }
            catch
            {
                await db.RollbackAsync();
                throw;
            }
        }

        await db.CheckpointAsync();
        return filePath;
    }

    private static DatabaseOptions CreateDirectDatabaseOptions(bool tunedFileCache)
    {
        if (!tunedFileCache)
        {
            return BenchmarkDurability.Apply();
        }

        return BenchmarkDurability.Apply(new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions().Configure(static builder => builder.UseDirectLookupOptimizedPreset()),
        });
    }

    private static void AssertBatchCount(int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException(
                $"Failed to seed the direct hybrid benchmark dataset. Expected {expected} batched inserts but observed {actual}.");
        }
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static JsonElement CreateBenchDocument(int id)
    {
        using var document = JsonDocument.Parse(
            $$"""{"name":"User_{{id}}","value":{{id}},"category":"{{GetCategory(id)}}" }""");
        return document.RootElement.Clone();
    }

    private sealed class DirectBenchmarkContext : IAsyncDisposable
    {
        private readonly string _dbPath;

        private DirectBenchmarkContext(string dbPath)
        {
            _dbPath = dbPath;
        }

        internal static async Task<DirectBenchmarkContext> CreateAsync()
            => new(await CreateSeededDatabaseAsync());

        internal CSharpDbClient CreateClient(bool tunedFileCache)
        {
            return (CSharpDbClient)CSharpDbClient.Create(new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Direct,
                DataSource = _dbPath,
                DirectDatabaseOptions = CreateDirectDatabaseOptions(tunedFileCache),
            });
        }

        public ValueTask DisposeAsync()
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_dbPath);
            return ValueTask.CompletedTask;
        }
    }
}
