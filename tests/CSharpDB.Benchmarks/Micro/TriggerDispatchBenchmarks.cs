using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures trigger dispatch overhead when many unrelated triggers exist.
/// This isolates lookup cost for "no matching trigger" write paths.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class TriggerDispatchBenchmarks
{
    [Params(0, 64, 256)]
    public int UnrelatedTriggerCount { get; set; }

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync("CREATE TABLE target (value INTEGER)");
        await _bench.Db.ExecuteAsync("CREATE TABLE other (value INTEGER)");
        await _bench.Db.ExecuteAsync("CREATE TABLE sink (value INTEGER)");

        for (int i = 0; i < UnrelatedTriggerCount; i++)
        {
            await _bench.Db.ExecuteAsync(
                $"CREATE TRIGGER trg_other_{i} AFTER INSERT ON other " +
                "BEGIN INSERT INTO sink VALUES (NEW.value); END");
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "INSERT with no matching triggers")]
    public async Task Insert_NoMatchingTriggers()
    {
        await _bench.Db.ExecuteAsync("INSERT INTO target VALUES (1)");
    }
}
