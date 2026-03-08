using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class InMemoryPersistenceBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        string sqlSourcePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("macro-persist-sql", 10_000);
        string collectionSourcePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("macro-persist-col", 10_000);
        string sqlSavePath = Path.Combine(Path.GetTempPath(), $"macro-persist-save-sql_{Guid.NewGuid():N}.db");
        string collectionSavePath = Path.Combine(Path.GetTempPath(), $"macro-persist-save-col_{Guid.NewGuid():N}.db");

        try
        {
            results.Add(await MeasureLoadAsync("InMemory_LoadFromDisk_Sql", sqlSourcePath));
            results.Add(await MeasureLoadAsync("InMemory_LoadFromDisk_Collections", collectionSourcePath));

            await using var sqlDb = await Database.LoadIntoMemoryAsync(sqlSourcePath);
            await using var collectionDb = await Database.LoadIntoMemoryAsync(collectionSourcePath);

            results.Add(await MeasureSaveAsync("InMemory_SaveToFile_Sql", sqlDb, sqlSavePath));
            results.Add(await MeasureSaveAsync("InMemory_SaveToFile_Collections", collectionDb, collectionSavePath));
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(sqlSourcePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(collectionSourcePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(sqlSavePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(collectionSavePath);
        }

        return results;
    }

    private static async Task<BenchmarkResult> MeasureLoadAsync(string name, string filePath)
    {
        const int iterations = 20;
        var histogram = new LatencyHistogram();
        long sizeBytes = new FileInfo(filePath).Length;

        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            await using var db = await Database.LoadIntoMemoryAsync(filePath);
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        return CreateResultWithExtra(name, histogram, $"Bytes={sizeBytes}");
    }

    private static async Task<BenchmarkResult> MeasureSaveAsync(string name, Database db, string savePath)
    {
        const int iterations = 20;
        var histogram = new LatencyHistogram();

        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            await db.SaveToFileAsync(savePath);
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        long sizeBytes = File.Exists(savePath) ? new FileInfo(savePath).Length : 0;
        return CreateResultWithExtra(name, histogram, $"Bytes={sizeBytes}");
    }

    private static BenchmarkResult CreateResultWithExtra(string name, LatencyHistogram histogram, string extraInfo)
    {
        var result = BenchmarkResult.FromHistogram(
            name,
            histogram,
            histogram.Count > 0 ? histogram.Count * histogram.Mean : 0);

        return new BenchmarkResult
        {
            Name = result.Name,
            TotalOps = result.TotalOps,
            ElapsedMs = result.ElapsedMs,
            P50Ms = result.P50Ms,
            P90Ms = result.P90Ms,
            P95Ms = result.P95Ms,
            P99Ms = result.P99Ms,
            P999Ms = result.P999Ms,
            MinMs = result.MinMs,
            MaxMs = result.MaxMs,
            MeanMs = result.MeanMs,
            StdDevMs = result.StdDevMs,
            ExtraInfo = extraInfo,
        };
    }
}
