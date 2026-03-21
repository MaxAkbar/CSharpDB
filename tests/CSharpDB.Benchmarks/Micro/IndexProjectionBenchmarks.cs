using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures secondary-index equality lookups where the projection can now be
/// answered from index payloads without fetching base table rows.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class IndexProjectionBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_idxproj (id INTEGER PRIMARY KEY, group_id INTEGER, payload TEXT)")
            .GetAwaiter().GetResult();

        _bench.SeedAsync(
            "t_idxproj",
            RowCount,
            i => $"INSERT INTO t_idxproj VALUES ({i}, {i % 256}, 'payload_{i}')")
            .GetAwaiter().GetResult();

        _bench.Db.ExecuteAsync("CREATE INDEX idx_t_idxproj_group_id ON t_idxproj (group_id)")
            .AsTask().GetAwaiter().GetResult();

        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Indexed lookup SELECT * (non-unique)")]
    public async Task SelectStar_ByIndexedColumn()
    {
        int groupId = _rng.Next(0, 256);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM t_idxproj WHERE group_id = {groupId}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Indexed lookup SELECT id (non-unique)")]
    public async Task SelectPrimaryKey_ByIndexedColumn()
    {
        int groupId = _rng.Next(0, 256);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id FROM t_idxproj WHERE group_id = {groupId}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Indexed lookup SELECT group_id (non-unique)")]
    public async Task SelectIndexedValue_ByIndexedColumn()
    {
        int groupId = _rng.Next(0, 256);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT group_id FROM t_idxproj WHERE group_id = {groupId}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Indexed lookup SELECT payload (non-unique)")]
    public async Task SelectPayload_ByIndexedColumn()
    {
        int groupId = _rng.Next(0, 256);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT payload FROM t_idxproj WHERE group_id = {groupId}");
        await result.ToListAsync();
    }
}
