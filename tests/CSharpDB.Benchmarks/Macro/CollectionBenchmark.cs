using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Benchmarks the NoSQL Document Collection API (Put, Get, Scan, Find, mixed workloads).
/// Measures raw B+tree-backed document operations that bypass the SQL parser/planner.
/// </summary>
public static class CollectionBenchmark
{
    private static int _idCounter;

    private record BenchDoc(string Name, int Value, string Category);

    private static readonly string[] Categories = { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        // ── 1. Sustained single-document Put (15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");

            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Put_Single_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int id = Interlocked.Increment(ref _idCounter);
                    await col.PutAsync($"doc:{id}", new BenchDoc($"User_{id}", id, Categories[id % 5]));
                });
            results.Add(result);
        }

        // ── 2. Sustained batch Put (100 per tx, 15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");

            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Put_Batch100_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    await bench.Db.BeginTransactionAsync();
                    for (int i = 0; i < 100; i++)
                    {
                        int id = Interlocked.Increment(ref _idCounter);
                        await col.PutAsync($"doc:{id}", new BenchDoc($"User_{id}", id, Categories[id % 5]));
                    }
                    await bench.Db.CommitAsync();
                });
            results.Add(result);
        }

        // ── 3. Point Get on pre-seeded collection (15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");

            // Seed 10,000 documents
            const int seedCount = 10_000;
            await SeedCollectionAsync(bench.Db, col, seedCount);

            var rng = new Random(42);
            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Get_10k_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int id = rng.Next(0, seedCount);
                    await col.GetAsync($"doc:{id}");
                });
            results.Add(result);
        }

        // ── 4. Mixed 80/20 Read/Write (15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");

            const int seedCount = 10_000;
            await SeedCollectionAsync(bench.Db, col, seedCount);

            var rng = new Random(42);
            var readHistogram = new LatencyHistogram();
            var writeHistogram = new LatencyHistogram();

            // Warmup
            for (int i = 0; i < 200; i++)
            {
                if (rng.NextDouble() < 0.8)
                    await col.GetAsync($"doc:{rng.Next(0, seedCount)}");
                else
                {
                    int id = Interlocked.Increment(ref _idCounter);
                    await col.PutAsync($"doc:new:{id}", new BenchDoc($"User_{id}", id, "Alpha"));
                }
            }

            // Measured run
            var sw = Stopwatch.StartNew();
            var end = DateTime.UtcNow + TimeSpan.FromSeconds(15);

            while (DateTime.UtcNow < end)
            {
                var opSw = Stopwatch.StartNew();

                if (rng.NextDouble() < 0.8)
                {
                    int id = rng.Next(0, seedCount);
                    await col.GetAsync($"doc:{id}");
                    opSw.Stop();
                    readHistogram.Record(opSw.Elapsed.TotalMilliseconds);
                }
                else
                {
                    int id = Interlocked.Increment(ref _idCounter);
                    await col.PutAsync($"doc:new:{id}", new BenchDoc($"User_{id}", id, "Beta"));
                    opSw.Stop();
                    writeHistogram.Record(opSw.Elapsed.TotalMilliseconds);
                }
            }

            sw.Stop();

            if (readHistogram.Count > 0)
            {
                var readResult = BenchmarkResult.FromHistogram(
                    "Collection_Mixed_Reads_80pct", readHistogram, sw.Elapsed.TotalMilliseconds);
                results.Add(readResult);
                Console.WriteLine($"  Collection_Mixed_Reads_80pct: {readResult.OpsPerSecond:N0} ops/sec, P50={readResult.P50Ms:F3}ms, P99={readResult.P99Ms:F3}ms");
            }

            if (writeHistogram.Count > 0)
            {
                var writeResult = BenchmarkResult.FromHistogram(
                    "Collection_Mixed_Writes_20pct", writeHistogram, sw.Elapsed.TotalMilliseconds);
                results.Add(writeResult);
                Console.WriteLine($"  Collection_Mixed_Writes_20pct: {writeResult.OpsPerSecond:N0} ops/sec, P50={writeResult.P50Ms:F3}ms, P99={writeResult.P99Ms:F3}ms");
            }
        }

        // ── 5. Full Scan throughput (1,000 docs) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");
            await SeedCollectionAsync(bench.Db, col, 1_000);

            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Scan_1k_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int count = 0;
                    await foreach (var kvp in col.ScanAsync())
                        count++;
                });
            results.Add(result);
        }

        // ── 6. Filtered Find (predicate scan, 1,000 docs, ~20% match rate) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");
            await SeedCollectionAsync(bench.Db, col, 1_000);

            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Find_1k_20pctMatch_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int count = 0;
                    await foreach (var kvp in col.FindAsync(d => d.Category == "Alpha"))
                        count++;
                });
            results.Add(result);
        }

        // ── 7. Indexed equality lookup on pre-seeded collection (10,000 docs, 15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");
            const int seedCount = 10_000;
            await SeedCollectionAsync(bench.Db, col, seedCount);
            await col.EnsureIndexAsync(d => d.Value);

            var rng = new Random(42);
            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_FindByIndex_Value_10k_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int id = rng.Next(0, seedCount);
                    int count = 0;
                    await foreach (var _ in col.FindByIndexAsync(d => d.Value, id))
                        count++;
                });
            results.Add(result);
        }

        // ── 8. Sustained single-document Put with secondary index (15s) ──
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");
            await col.EnsureIndexAsync(d => d.Value);

            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "Collection_Put_Single_WithIndex_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int id = Interlocked.Increment(ref _idCounter);
                    await col.PutAsync($"doc:indexed:{id}", new BenchDoc($"User_{id}", id, Categories[id % 5]));
                });
            results.Add(result);
        }

        // ── 9. SQL vs Collection comparison: point read on same data size ──
        await using (var bench = await BenchmarkDatabase.CreateAsync(10_000))
        {
            var col = await bench.Db.GetCollectionAsync<BenchDoc>("bench_docs");
            await SeedCollectionAsync(bench.Db, col, 10_000);

            var rng2 = new Random(42);

            // SQL point lookup
            var sqlResult = await MacroBenchmarkRunner.RunForDurationAsync(
                "Comparison_SQL_PointLookup_10k",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(10),
                async () =>
                {
                    int id = rng2.Next(0, 10_000);
                    await using var r = await bench.Db.ExecuteAsync($"SELECT * FROM bench WHERE id = {id}");
                    await r.ToListAsync();
                });
            results.Add(sqlResult);

            // Collection point lookup (same DB, same duration)
            rng2 = new Random(42); // reset to same sequence
            var colResult = await MacroBenchmarkRunner.RunForDurationAsync(
                "Comparison_Collection_PointLookup_10k",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(10),
                async () =>
                {
                    int id = rng2.Next(0, 10_000);
                    await col.GetAsync($"doc:{id}");
                });
            results.Add(colResult);

            double speedup = colResult.OpsPerSecond / Math.Max(1, sqlResult.OpsPerSecond);
            Console.WriteLine($"  Collection vs SQL speedup: {speedup:F2}x");
        }

        return results;
    }

    /// <summary>
    /// Seed a collection with deterministic documents in batches of 500.
    /// </summary>
    private static async Task SeedCollectionAsync(Database db, Collection<BenchDoc> col, int count)
    {
        const int batchSize = 500;
        for (int i = 0; i < count; i += batchSize)
        {
            await db.BeginTransactionAsync();
            int end = Math.Min(i + batchSize, count);
            for (int j = i; j < end; j++)
            {
                await col.PutAsync($"doc:{j}", new BenchDoc($"User_{j}", j, Categories[j % 5]));
            }
            await db.CommitAsync();
        }
    }
}
