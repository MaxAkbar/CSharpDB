using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Stress;

/// <summary>
/// Measures reopen/recovery timing after clean process shutdown with an
/// abandoned uncommitted transaction. True process-crash durability coverage
/// now lives in the test suite's process-level crash harness tests.
/// </summary>
public static class CrashRecoveryBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        const int cycles = 50;
        const int rowsPerCycle = 100;
        int successCount = 0;
        var recoveryHist = new LatencyHistogram();

        var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_crash_bench_{Guid.NewGuid():N}.db");
        var walPath = filePath + ".wal";

        try
        {
            // Initial setup
            await using (var db = await Database.OpenAsync(filePath))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
            }

            int totalCommittedRows = 0;

            for (int cycle = 0; cycle < cycles; cycle++)
            {
                // Phase 1: Insert committed rows
                {
                    var db = await Database.OpenAsync(filePath);
                    try
                    {
                        await db.BeginTransactionAsync();
                        int startId = totalCommittedRows;
                        for (int i = 0; i < rowsPerCycle; i++)
                        {
                            await db.ExecuteAsync(
                                $"INSERT INTO t VALUES ({startId + i}, {(startId + i) * 10})");
                        }
                        await db.CommitAsync();
                        totalCommittedRows += rowsPerCycle;

                        // Simulate crash: begin another transaction but don't commit
                        await db.BeginTransactionAsync();
                        for (int i = 0; i < 50; i++)
                        {
                            await db.ExecuteAsync(
                                $"INSERT INTO t VALUES ({totalCommittedRows + 999_000 + i}, 0)");
                        }
                        // "Crash" — dispose without commit (rollback happens implicitly)
                    }
                    finally
                    {
                        await db.DisposeAsync();
                    }
                }

                // Phase 2: Recover and verify
                {
                    var recoverySw = Stopwatch.StartNew();
                    await using var db = await Database.OpenAsync(filePath);
                    recoverySw.Stop();
                    recoveryHist.Record(recoverySw.Elapsed.TotalMilliseconds);

                    // Verify committed rows exist
                    await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM t");
                    var rows = await result.ToListAsync();
                    long count = rows[0][0].AsInteger;

                    if (count == totalCommittedRows)
                    {
                        successCount++;
                    }
                    else
                    {
                        Console.WriteLine(
                            $"  WARNING: Cycle {cycle}: expected {totalCommittedRows} rows, got {count}");
                    }
                }
            }

            var totalSw = recoveryHist.Count > 0 ? recoveryHist.Mean * recoveryHist.Count : 0;

            var recoveryResult = BenchmarkResult.FromHistogram(
                $"CrashRecovery_{cycles}cycles", recoveryHist, totalSw);
            recoveryResult = new BenchmarkResult
            {
                Name = recoveryResult.Name,
                TotalOps = recoveryResult.TotalOps,
                ElapsedMs = recoveryResult.ElapsedMs,
                P50Ms = recoveryResult.P50Ms,
                P90Ms = recoveryResult.P90Ms,
                P95Ms = recoveryResult.P95Ms,
                P99Ms = recoveryResult.P99Ms,
                P999Ms = recoveryResult.P999Ms,
                MinMs = recoveryResult.MinMs,
                MaxMs = recoveryResult.MaxMs,
                MeanMs = recoveryResult.MeanMs,
                StdDevMs = recoveryResult.StdDevMs,
                ExtraInfo = $"Success={successCount}/{cycles}, FinalRows={totalCommittedRows}"
            };
            results.Add(recoveryResult);

            Console.WriteLine($"  Crash recovery: {successCount}/{cycles} successful, " +
                              $"recovery P50={recoveryResult.P50Ms:F2}ms, P99={recoveryResult.P99Ms:F2}ms");
        }
        finally
        {
            try { File.Delete(filePath); } catch { }
            try { File.Delete(walPath); } catch { }
        }

        return results;
    }
}
