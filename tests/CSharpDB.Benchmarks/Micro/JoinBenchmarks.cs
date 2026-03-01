using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures JOIN performance across different table sizes and join types.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JoinBenchmarks
{
    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_join_bench_{Guid.NewGuid():N}.db");
        _bench = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE left_t (id INTEGER PRIMARY KEY, value INTEGER, label TEXT)")
            .GetAwaiter().GetResult();

        var db = _bench.Db;

        // Create right table
        db.ExecuteAsync("CREATE TABLE right_t (id INTEGER PRIMARY KEY, left_id INTEGER, amount INTEGER)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE TABLE right_big_t (id INTEGER PRIMARY KEY, left_id INTEGER, amount INTEGER)")
            .AsTask().GetAwaiter().GetResult();

        // Small table for cross join
        db.ExecuteAsync("CREATE TABLE small_t (id INTEGER PRIMARY KEY, name TEXT)")
            .AsTask().GetAwaiter().GetResult();

        // Seed left table (1000 rows)
        _bench.SeedAsync("left_t", 1000, i =>
            $"INSERT INTO left_t VALUES ({i}, {i * 10}, 'item_{i}')")
            .GetAwaiter().GetResult();

        // Seed right table (1000 rows, referencing left_t)
        _bench.SeedAsync("right_t", 1000, i =>
            $"INSERT INTO right_t VALUES ({i}, {i % 1000}, {i * 5})")
            .GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE INDEX idx_right_t_left_id ON right_t(left_id)")
            .AsTask().GetAwaiter().GetResult();

        // Skewed table (20K rows) to stress hash build-side choice.
        _bench.SeedAsync("right_big_t", 20000, i =>
            $"INSERT INTO right_big_t VALUES ({i}, {i % 1000}, {i * 7})")
            .GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE VIEW right_big_v AS SELECT * FROM right_big_t")
            .AsTask().GetAwaiter().GetResult();

        // Seed small table (100 rows for cross join)
        _bench.SeedAsync("small_t", 100, i =>
            $"INSERT INTO small_t VALUES ({i}, 'name_{i}')")
            .GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K")]
    public async Task InnerJoin_1Kx1K()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K LIMIT 1")]
    public async Task InnerJoin_1Kx1K_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx20K (planner swap build side)")]
    public async Task InnerJoin_Skewed_1Kx20K_PlannerSwap()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_big_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 20Kx1K (natural build side)")]
    public async Task InnerJoin_Skewed_20Kx1K_Natural()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM right_big_t r INNER JOIN left_t l ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx20K (no swap via view)")]
    public async Task InnerJoin_Skewed_1Kx20K_NoSwapViaView()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_big_v r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (forced nested-loop)")]
    public async Task InnerJoin_1Kx1K_ForcedNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id + 0 = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN on right PK (index nested-loop)")]
    public async Task InnerJoin_OnRightPk_IndexNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN on right PK (forced hash)")]
    public async Task InnerJoin_OnRightPk_ForcedHash()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.id AND l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "RIGHT JOIN on left PK (rewritten index nested-loop)")]
    public async Task RightJoin_OnLeftPk_RewrittenIndexNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l RIGHT JOIN right_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "RIGHT JOIN on left PK (forced hash)")]
    public async Task RightJoin_OnLeftPk_ForcedHash()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l RIGHT JOIN right_t r ON l.id = r.left_id AND l.id = r.id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "LEFT JOIN 1Kx1K")]
    public async Task LeftJoin_1Kx1K()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l LEFT JOIN right_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "CROSS JOIN 100x100")]
    public async Task CrossJoin_100x100()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT a.name, b.name FROM small_t a CROSS JOIN small_t b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN with filter")]
    public async Task InnerJoinWithFilter()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id WHERE r.amount > 2500");
        await result.ToListAsync();
    }
}
