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
        db.ExecuteAsync(
            "CREATE TABLE left_wide_t (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE right_wide_t (id INTEGER PRIMARY KEY, left_id INTEGER, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE left_comp_t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, label TEXT)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE right_comp_t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, amount INTEGER, left_id INTEGER)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE mid_left_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, label TEXT)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE mid_right_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE reorder_big_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL, nullable_tag INTEGER)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE reorder_mid_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, marker INTEGER NOT NULL)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE reorder_small_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, flag INTEGER NOT NULL)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE stats_join_left_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync(
            "CREATE TABLE stats_join_right_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)")
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

        _bench.SeedAsync("left_wide_t", 1000, i =>
            $"INSERT INTO left_wide_t VALUES ({i}, 'l1_{i}', 'l2_{i}', 'l3_{i}', 'l4_{i}', 'l5_{i}', 'l6_{i}', 'ltail_{i}')")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("right_wide_t", 1000, i =>
            $"INSERT INTO right_wide_t VALUES ({i}, {i % 1000}, 'r1_{i}', 'r2_{i}', 'r3_{i}', 'r4_{i}', 'r5_{i}', 'r6_{i}', 'rtail_{i}')")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("left_comp_t", 1000, i =>
            $"INSERT INTO left_comp_t VALUES ({i}, {i % 100}, 'code_{i / 100}', 'left_comp_{i}')")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("right_comp_t", 1000, i =>
            $"INSERT INTO right_comp_t VALUES ({i}, {i % 100}, 'code_{i / 100}', {i * 3}, {i})")
            .GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_comp_t_ab ON right_comp_t(a, b)")
            .AsTask().GetAwaiter().GetResult();
        _bench.SeedAsync("mid_left_t", 800, i =>
            $"INSERT INTO mid_left_t VALUES ({i}, {i}, 'mid_left_{i}')")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("mid_right_t", 1000, i =>
            $"INSERT INTO mid_right_t VALUES ({i}, {i}, {i * 13})")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("reorder_big_t", 5000, i =>
        {
            int code = ((i - 1) % 200) + 1;
            string nullableTag = i <= 5 ? "NULL" : i.ToString();
            return $"INSERT INTO reorder_big_t VALUES ({i}, {code}, {i * 17}, {nullableTag})";
        }).GetAwaiter().GetResult();
        _bench.SeedAsync("reorder_mid_t", 200, i =>
            $"INSERT INTO reorder_mid_t VALUES ({i}, {i}, {i * 19})")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("reorder_small_t", 10, i =>
            $"INSERT INTO reorder_small_t VALUES ({i}, {i}, {i * 23})")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("stats_join_left_t", 3000, i =>
            $"INSERT INTO stats_join_left_t VALUES ({i}, {i})")
            .GetAwaiter().GetResult();
        _bench.SeedAsync("stats_join_right_t", 10000, i =>
        {
            int code = ((i - 1) % 5000) + 1;
            return $"INSERT INTO stats_join_right_t VALUES ({i}, {code}, {i * 11})";
        }).GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE INDEX idx_stats_join_right_t_code ON stats_join_right_t(code)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("ANALYZE stats_join_left_t")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("ANALYZE stats_join_right_t")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("ANALYZE reorder_big_t")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("ANALYZE reorder_mid_t")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("ANALYZE reorder_small_t")
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

    [Benchmark(Description = "INNER JOIN 800x1K (planner builds smaller side)")]
    public async Task InnerJoin_800x1K_BuildSmallerSide()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.payload FROM mid_left_t l INNER JOIN mid_right_t r ON l.code = r.code");
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

    [Benchmark(Description = "INNER JOIN 1Kx1K (forced nested-loop LIMIT 1)")]
    public async Task InnerJoin_1Kx1K_ForcedNestedLoop_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id + 0 = r.left_id LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (forced nested-loop expression projection)")]
    public async Task InnerJoin_1Kx1K_ForcedNestedLoop_ExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_t l INNER JOIN right_t r ON l.id + 0 = r.left_id");
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

    [Benchmark(Description = "INNER JOIN 1Kx1K (wide late projection hash)")]
    public async Task InnerJoin_WideLateProjection_HashJoin()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.tail, r.tail FROM left_wide_t l INNER JOIN right_wide_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (wide late projection forced nested-loop)")]
    public async Task InnerJoin_WideLateProjection_ForcedNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.tail, r.tail FROM left_wide_t l INNER JOIN right_wide_t r ON l.id + 0 = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite index nested-loop)")]
    public async Task InnerJoin_CompositeIndexLookup()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite forced hash)")]
    public async Task InnerJoin_CompositeIndex_ForcedHash()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a AND l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite covered lookup)")]
    public async Task InnerJoin_CompositeIndex_CoveredLookup()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.id, r.a, r.b FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite covered forced hash)")]
    public async Task InnerJoin_CompositeIndex_CoveredForcedHash()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.id, r.a, r.b FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a AND l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 3Kx10K (stats-driven non-unique lookup)")]
    public async Task InnerJoin_StatsDrivenNonUniqueLookup()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.payload FROM stats_join_left_t l INNER JOIN stats_join_right_t r ON l.code = r.code");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (planner reorder chain)")]
    public async Task InnerJoin_ReorderedThreeWayChain()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_big_t b INNER JOIN reorder_mid_t m ON b.code = m.code INNER JOIN reorder_small_t s ON m.code = s.code");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with selective leaf)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveLeaf()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code AND b.id = 42");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer WHERE filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterWhere()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id = 42");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer range filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterRange()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id BETWEEN 1 AND 5");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer IN filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterIn()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id IN (1, 2, 3)");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer IS NULL filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterIsNull()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.nullable_tag IS NULL");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer OR filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterOr()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id = 1 OR b.id = 2 OR b.id = 3");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer OR range filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterOrRange()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id BETWEEN 1 AND 2 OR b.id BETWEEN 10 AND 11");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer mixed union filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterMixedUnion()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.id IN (1, 2, 3) OR b.id BETWEEN 10 AND 11");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (reorder chain with outer NULL OR filter)")]
    public async Task InnerJoin_ReorderedThreeWayChain_SelectiveOuterNullOr()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_small_t s INNER JOIN reorder_mid_t m ON m.code = s.code INNER JOIN reorder_big_t b ON b.code = m.code WHERE b.nullable_tag IS NULL OR b.nullable_tag = 42");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite index lookup LIMIT 1)")]
    public async Task InnerJoin_CompositeIndexLookup_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite index lookup expression projection)")]
    public async Task InnerJoin_CompositeIndexLookup_ExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a");
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

    [Benchmark(Description = "LEFT JOIN 1Kx1K (forced nested-loop LIMIT 1)")]
    public async Task LeftJoin_1Kx1K_ForcedNestedLoop_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l LEFT JOIN right_t r ON l.id + 0 = r.left_id LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "RIGHT JOIN 1Kx1K (forced nested-loop LIMIT 1)")]
    public async Task RightJoin_1Kx1K_ForcedNestedLoop_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l RIGHT JOIN right_t r ON l.id + 0 = r.left_id LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "CROSS JOIN 100x100")]
    public async Task CrossJoin_100x100()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT a.name, b.name FROM small_t a CROSS JOIN small_t b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "CROSS JOIN 100x100 LIMIT 1")]
    public async Task CrossJoin_100x100_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT a.name, b.name FROM small_t a CROSS JOIN small_t b LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN with filter")]
    public async Task InnerJoinWithFilter()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id WHERE r.amount > 2500");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN with expression projection")]
    public async Task InnerJoinWithExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_t l INNER JOIN right_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN with filter + expression projection")]
    public async Task InnerJoinWithFilterAndExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_t l INNER JOIN right_t r ON l.id = r.left_id WHERE r.amount > 2500");
        await result.ToListAsync();
    }
}
