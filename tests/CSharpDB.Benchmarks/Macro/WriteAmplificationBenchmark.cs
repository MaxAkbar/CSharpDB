using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures storage efficiency by comparing logical data size vs actual I/O:
/// WAL file size, DB file size, and write amplification ratio.
/// </summary>
public static class WriteAmplificationBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (int rowCount in new[] { 1_000, 10_000, 50_000 })
        {
            var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_wa_bench_{Guid.NewGuid():N}.db");
            var walPath = filePath + ".wal";

            try
            {
                long logicalBytes = 0;

                await using (var db = await Database.OpenAsync(filePath))
                {
                    await db.ExecuteAsync(
                        "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, data TEXT)");

                    var rng = new Random(42);
                    const int batchSize = 500;

                    for (int i = 0; i < rowCount; i += batchSize)
                    {
                        await db.BeginTransactionAsync();
                        int end = Math.Min(i + batchSize, rowCount);
                        for (int j = i; j < end; j++)
                        {
                            var text = DataGenerator.RandomString(rng, 100);
                            var sql = $"INSERT INTO t VALUES ({j}, {rng.Next()}, '{text}')";
                            logicalBytes += 8 + 4 + text.Length; // rough estimate of payload
                            await db.ExecuteAsync(sql);
                        }
                        await db.CommitAsync();
                    }

                    // Measure WAL size before checkpoint
                    long walSizeBefore = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;
                    long dbSizeBefore = new FileInfo(filePath).Length;

                    // Checkpoint
                    await db.CheckpointAsync();

                    long walSizeAfter = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;
                    long dbSizeAfter = new FileInfo(filePath).Length;

                    double amplification = logicalBytes > 0
                        ? (double)(dbSizeAfter + walSizeBefore) / logicalBytes
                        : 0;

                    var result = new BenchmarkResult
                    {
                        Name = $"WriteAmplification_{rowCount}rows",
                        TotalOps = rowCount,
                        ElapsedMs = 0,
                        P50Ms = 0, P90Ms = 0, P95Ms = 0, P99Ms = 0, P999Ms = 0,
                        MinMs = 0, MaxMs = 0, MeanMs = 0, StdDevMs = 0,
                        ExtraInfo = $"Logical={logicalBytes:N0}B, WAL(pre-ckpt)={walSizeBefore:N0}B, " +
                                    $"DB(post-ckpt)={dbSizeAfter:N0}B, Amplification={amplification:F2}x"
                    };
                    results.Add(result);

                    Console.WriteLine($"  {rowCount} rows: logical={logicalBytes:N0}B, " +
                                      $"WAL={walSizeBefore:N0}B, DB={dbSizeAfter:N0}B, " +
                                      $"amplification={amplification:F2}x");
                }
            }
            finally
            {
                try { File.Delete(filePath); } catch { }
                try { File.Delete(walPath); } catch { }
            }
        }

        return results;
    }
}
