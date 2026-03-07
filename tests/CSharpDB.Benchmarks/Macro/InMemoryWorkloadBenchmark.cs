using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class InMemoryWorkloadBenchmark
{
    private static int _sqlIdCounter = 1_000_000;
    private static int _collectionIdCounter = 1_000_000;

    private sealed record BenchDoc(string Name, int Value, string Category);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        results.AddRange(await RunSqlMixedAsync(inMemory: false));
        results.AddRange(await RunSqlMixedAsync(inMemory: true));
        results.AddRange(await RunCollectionMixedAsync(inMemory: false));
        results.AddRange(await RunCollectionMixedAsync(inMemory: true));

        return results;
    }

    private static async Task<List<BenchmarkResult>> RunSqlMixedAsync(bool inMemory)
    {
        const int seedCount = 10_000;
        var results = new List<BenchmarkResult>();
        await using var handle = await CreateSqlDatabaseAsync(inMemory, seedCount);
        var db = handle.Db;

        var rng = new Random(42);
        var readHistogram = new LatencyHistogram();
        var writeHistogram = new LatencyHistogram();
        var duration = TimeSpan.FromSeconds(10);
        var deadline = DateTime.UtcNow + duration;

        while (DateTime.UtcNow < deadline)
        {
            var sw = Stopwatch.StartNew();

            if (rng.NextDouble() < 0.8)
            {
                int id = rng.Next(0, seedCount);
                await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id}");
                await result.MoveNextAsync();
                sw.Stop();
                readHistogram.Record(sw.Elapsed.TotalMilliseconds);
            }
            else
            {
                int id = Interlocked.Increment(ref _sqlIdCounter);
                await using var result = await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, 'mixed_{id}', 'Alpha')");
                sw.Stop();
                writeHistogram.Record(sw.Elapsed.TotalMilliseconds);
            }
        }

        string prefix = inMemory ? "InMemory" : "FileBacked";
        results.Add(BenchmarkResult.FromHistogram($"{prefix}_Sql_Mixed_Reads_80pct", readHistogram, duration.TotalMilliseconds));
        results.Add(BenchmarkResult.FromHistogram($"{prefix}_Sql_Mixed_Writes_20pct", writeHistogram, duration.TotalMilliseconds));
        return results;
    }

    private static async Task<List<BenchmarkResult>> RunCollectionMixedAsync(bool inMemory)
    {
        const int seedCount = 10_000;
        var results = new List<BenchmarkResult>();
        await using var handle = await CreateCollectionDatabaseAsync(inMemory, seedCount);
        var db = handle.Db;
        var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");

        var rng = new Random(42);
        var readHistogram = new LatencyHistogram();
        var writeHistogram = new LatencyHistogram();
        var duration = TimeSpan.FromSeconds(10);
        var deadline = DateTime.UtcNow + duration;

        while (DateTime.UtcNow < deadline)
        {
            var sw = Stopwatch.StartNew();

            if (rng.NextDouble() < 0.8)
            {
                int id = rng.Next(0, seedCount);
                await collection.GetAsync($"doc:{id}");
                sw.Stop();
                readHistogram.Record(sw.Elapsed.TotalMilliseconds);
            }
            else
            {
                int id = Interlocked.Increment(ref _collectionIdCounter);
                await collection.PutAsync($"doc:new:{id}", new BenchDoc($"User_{id}", id, "Beta"));
                sw.Stop();
                writeHistogram.Record(sw.Elapsed.TotalMilliseconds);
            }
        }

        string prefix = inMemory ? "InMemory" : "FileBacked";
        results.Add(BenchmarkResult.FromHistogram($"{prefix}_Collection_Mixed_Reads_80pct", readHistogram, duration.TotalMilliseconds));
        results.Add(BenchmarkResult.FromHistogram($"{prefix}_Collection_Mixed_Writes_20pct", writeHistogram, duration.TotalMilliseconds));
        return results;
    }

    private static async Task<WorkloadDatabaseHandle> CreateSqlDatabaseAsync(bool inMemory, int seedCount)
    {
        if (inMemory)
        {
            var db = await Database.OpenInMemoryAsync();
            await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");
            await SeedSqlAsync(db, seedCount);
            return new WorkloadDatabaseHandle(db);
        }

        string filePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("macro-memory-sql", seedCount);
        return new WorkloadDatabaseHandle(await Database.OpenAsync(filePath), filePath);
    }

    private static async Task<WorkloadDatabaseHandle> CreateCollectionDatabaseAsync(bool inMemory, int seedCount)
    {
        if (inMemory)
        {
            var db = await Database.OpenInMemoryAsync();
            await SeedCollectionAsync(db, seedCount);
            return new WorkloadDatabaseHandle(db);
        }

        string filePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("macro-memory-collection", seedCount);
        return new WorkloadDatabaseHandle(await Database.OpenAsync(filePath), filePath);
    }

    private static async Task SeedSqlAsync(Database db, int seedCount)
    {
        const int batchSize = 500;
        for (int batchStart = 0; batchStart < seedCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            int batchEnd = Math.Min(batchStart + batchSize, seedCount);
            for (int i = batchStart; i < batchEnd; i++)
            {
                await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({i}, {i * 10L}, 'seed_{i}', 'Alpha')");
            }
            await db.CommitAsync();
        }
    }

    private static async Task SeedCollectionAsync(Database db, int seedCount)
    {
        var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
        const int batchSize = 500;
        for (int batchStart = 0; batchStart < seedCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            int batchEnd = Math.Min(batchStart + batchSize, seedCount);
            for (int i = batchStart; i < batchEnd; i++)
            {
                await collection.PutAsync($"doc:{i}", new BenchDoc($"User_{i}", i, "Alpha"));
            }
            await db.CommitAsync();
        }
    }

    private sealed class WorkloadDatabaseHandle : IAsyncDisposable
    {
        private readonly string? _filePath;

        internal WorkloadDatabaseHandle(Database db, string? filePath = null)
        {
            Db = db;
            _filePath = filePath;
        }

        internal Database Db { get; }

        public async ValueTask DisposeAsync()
        {
            await Db.DisposeAsync();
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_filePath);
        }
    }
}
