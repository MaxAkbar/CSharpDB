using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 8)]
public class WideRowSortBenchmarks
{
    private const string LateMaterializationOverrideEnvVar = "CSHARPDB_SORT_LATE_MATERIALIZATION";
    private const int RowCount = 50_000;
    private const int PayloadLength = 256;
    private const string Query = "SELECT * FROM bench_wide ORDER BY value ASC";

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench_wide (" +
            "id INTEGER PRIMARY KEY, " +
            "value INTEGER NOT NULL, " +
            "payload_a TEXT NOT NULL, " +
            "payload_b TEXT NOT NULL, " +
            "payload_c TEXT NOT NULL, " +
            "payload_d TEXT NOT NULL)");

        await SeedAsync(_bench);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Environment.SetEnvironmentVariable(LateMaterializationOverrideEnvVar, null);
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "wide ORDER BY value (50k, lazy off)")]
    public async Task OrderedWide_ForceRowMaterialized()
    {
        Environment.SetEnvironmentVariable(LateMaterializationOverrideEnvVar, "off");
        await ExecuteQueryAsync();
    }

    [Benchmark(Description = "wide ORDER BY value (50k, lazy on)")]
    public async Task OrderedWide_ForceLateMaterialized()
    {
        Environment.SetEnvironmentVariable(LateMaterializationOverrideEnvVar, "on");
        await ExecuteQueryAsync();
    }

    private async Task ExecuteQueryAsync()
    {
        await using var result = await _bench.Db.ExecuteAsync(Query);
        await result.ToListAsync();
    }

    private static Task SeedAsync(BenchmarkDatabase bench)
    {
        return bench.SeedAsync("bench_wide", RowCount, id =>
        {
            int value = (id * 7919) % 1_000_000;
            string payloadA = BuildPayload('A', id);
            string payloadB = BuildPayload('B', id);
            string payloadC = BuildPayload('C', id);
            string payloadD = BuildPayload('D', id);
            return $"INSERT INTO bench_wide VALUES ({id}, {value}, '{payloadA}', '{payloadB}', '{payloadC}', '{payloadD}')";
        });
    }

    private static string BuildPayload(char prefix, int rowId)
    {
        string rowText = rowId.ToString("D6");
        int fillerLength = PayloadLength - rowText.Length - 2;
        if (fillerLength < 0)
            fillerLength = 0;

        return $"{prefix}_{rowText}_{new string(prefix, fillerLength)}";
    }
}
