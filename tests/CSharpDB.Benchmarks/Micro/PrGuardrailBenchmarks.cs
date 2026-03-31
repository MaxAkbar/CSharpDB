using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using System.Text;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PointLookupGuardrailBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
        _bench.Db.ExecuteAsync("CREATE INDEX idx_value ON bench (value)")
            .AsTask().GetAwaiter().GetResult();
        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "SELECT by primary key")]
    public async Task SelectByPrimaryKey()
    {
        int id = _rng.Next(0, RowCount);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM bench WHERE id = {id}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT by PK with residual conjunct")]
    public async Task SelectByPrimaryKeyWithResidualConjunct()
    {
        int id = _rng.Next(0, RowCount);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id FROM bench WHERE id = {id} AND category = 'Alpha'");
        await result.ToListAsync();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JoinGuardrailBenchmarks
{
    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "INNER JOIN 1Kx1K")]
    public async Task InnerJoin_1Kx1K()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN on right PK (index nested-loop)")]
    public async Task InnerJoin_OnRightPk_IndexNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (forced nested-loop)")]
    public async Task InnerJoin_1Kx1K_ForcedNestedLoop()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id + 0 = r.left_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite covered lookup)")]
    public async Task InnerJoin_CompositeIndex_CoveredLookup()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.id, r.a, r.b FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K (composite index lookup expression projection)")]
    public async Task InnerJoin_CompositeIndexLookup_ExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_comp_t l INNER JOIN right_comp_t r ON l.b = r.b AND l.a = r.a");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 1Kx1K LIMIT 1")]
    public async Task InnerJoin_1Kx1K_Limit1()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.label, r.amount FROM left_t l INNER JOIN right_t r ON l.id = r.left_id LIMIT 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN 5Kx200x10 (planner reorder chain)")]
    public async Task InnerJoin_ReorderedThreeWayChain()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT b.payload, s.flag FROM reorder_big_t b INNER JOIN reorder_mid_t m ON b.code = m.code INNER JOIN reorder_small_t s ON m.code = s.code");
        await result.ToListAsync();
    }

    [Benchmark(Description = "INNER JOIN with filter + expression projection")]
    public async Task InnerJoinWithFilterAndExpressionProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT l.id, r.amount + l.id FROM left_t l INNER JOIN right_t r ON l.id = r.left_id WHERE r.amount > 2500");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE left_t (id INTEGER PRIMARY KEY, value INTEGER, label TEXT)");

        var db = _bench.Db;
        await db.ExecuteAsync("CREATE TABLE right_t (id INTEGER PRIMARY KEY, left_id INTEGER, amount INTEGER)");
        await db.ExecuteAsync("CREATE TABLE left_comp_t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, label TEXT)");
        await db.ExecuteAsync("CREATE TABLE right_comp_t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, amount INTEGER, left_id INTEGER)");
        await db.ExecuteAsync("CREATE TABLE reorder_big_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL, nullable_tag INTEGER)");
        await db.ExecuteAsync("CREATE TABLE reorder_mid_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, marker INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE reorder_small_t (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, flag INTEGER NOT NULL)");

        await _bench.SeedAsync("left_t", 1000, i =>
            $"INSERT INTO left_t VALUES ({i}, {i * 10}, 'item_{i}')");
        await _bench.SeedAsync("right_t", 1000, i =>
            $"INSERT INTO right_t VALUES ({i}, {i % 1000}, {i * 5})");
        await db.ExecuteAsync("CREATE INDEX idx_right_t_left_id ON right_t(left_id)");

        await _bench.SeedAsync("left_comp_t", 1000, i =>
            $"INSERT INTO left_comp_t VALUES ({i}, {i % 100}, 'code_{i / 100}', 'left_comp_{i}')");
        await _bench.SeedAsync("right_comp_t", 1000, i =>
            $"INSERT INTO right_comp_t VALUES ({i}, {i % 100}, 'code_{i / 100}', {i * 3}, {i})");
        await db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_comp_t_ab ON right_comp_t(a, b)");

        await _bench.SeedAsync("reorder_big_t", 5000, i =>
        {
            int code = ((i - 1) % 200) + 1;
            string nullableTag = i <= 5 ? "NULL" : i.ToString();
            return $"INSERT INTO reorder_big_t VALUES ({i}, {code}, {i * 17}, {nullableTag})";
        });
        await _bench.SeedAsync("reorder_mid_t", 200, i =>
            $"INSERT INTO reorder_mid_t VALUES ({i}, {i}, {i * 19})");
        await _bench.SeedAsync("reorder_small_t", 10, i =>
            $"INSERT INTO reorder_small_t VALUES ({i}, {i}, {i * 23})");

        await db.ExecuteAsync("ANALYZE reorder_big_t");
        await db.ExecuteAsync("ANALYZE reorder_mid_t");
        await db.ExecuteAsync("ANALYZE reorder_small_t");
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InsertGuardrailBenchmarks
{
    private const int PreSeededRows = 1_000;

    private BenchmarkDatabase _bench = null!;
    private InsertBatch _preparedBatch1000 = null!;
    private int _nextId;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(PreSeededRows).GetAwaiter().GetResult();
        _preparedBatch1000 = _bench.Db.PrepareInsertBatch("bench", 1000);
        _nextId = PreSeededRows + 1_000_000;
        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Single INSERT (auto-commit)")]
    public async Task SingleInsert()
    {
        int id = Interlocked.Increment(ref _nextId);
        var text = DataGenerator.RandomString(_rng, 50);
        await _bench.Db.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, '{text}', 'Alpha')");
    }

    [Benchmark(Description = "Single INSERT in explicit transaction")]
    public async Task SingleInsertInTransaction()
    {
        int id = Interlocked.Increment(ref _nextId);
        var text = DataGenerator.RandomString(_rng, 50);
        await _bench.Db.BeginTransactionAsync();
        await _bench.Db.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, '{text}', 'Beta')");
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x100 in transaction")]
    public async Task BatchInsert_100()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync(
                $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, 'batch100', 'Gamma')");
        }
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x1000 in transaction")]
    public async Task BatchInsert_1000()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 1000; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync(
                $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, 'batch1000', 'Delta')");
        }
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x1000 in one statement")]
    public async Task BatchInsert_1000_MultiRow()
    {
        await _bench.Db.ExecuteAsync(BuildMultiRowInsertSql(1000, "batch1000_multi", "Delta"));
    }

    [Benchmark(Description = "Batch INSERT x1000 prepared batch")]
    public async Task BatchInsert_1000_PreparedBatch()
    {
        _preparedBatch1000.Clear();
        var row = new DbValue[4];
        for (int i = 0; i < 1000; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(_rng.Next());
            row[2] = DbValue.FromText("batch1000_prepared");
            row[3] = DbValue.FromText("Delta");
            _preparedBatch1000.AddRow(row);
        }

        await _bench.Db.BeginTransactionAsync();
        await _preparedBatch1000.ExecuteAsync();
        await _bench.Db.CommitAsync();
    }

    private string BuildMultiRowInsertSql(int rowCount, string text, string category)
    {
        var builder = new StringBuilder(rowCount * 40);
        builder.Append("INSERT INTO bench VALUES ");

        for (int i = 0; i < rowCount; i++)
        {
            if (i > 0)
                builder.Append(", ");

            int id = Interlocked.Increment(ref _nextId);
            builder
                .Append('(')
                .Append(id)
                .Append(", ")
                .Append(_rng.Next())
                .Append(", '")
                .Append(text)
                .Append("', '")
                .Append(category)
                .Append("')");
        }

        return builder.ToString();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionIndexGuardrailBenchmarks
{
    private const int SeedCount = 10_000;
    private const int UpdateWorkingSetSize = 1_024;

    private static readonly string[] s_categories = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];

    private Database _lookupDb = null!;
    private Database _writeDb = null!;
    private Collection<BenchDoc> _lookupCollection = null!;
    private Collection<BenchDoc> _writeCollection = null!;
    private Random _lookupRandom = null!;
    private int _nextInsertId;
    private int _nextUpdateSlot = -1;
    private int _nextDeleteSlot = -1;
    private long _sink;

    private sealed record BenchDoc(string Name, int Value, string Category, string Tag);

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        GlobalCleanupAsync().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true, Description = "Collection FindByIndex int equality (1 match)")]
    public async Task FindByIndex_Integer()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        await foreach (var match in _lookupCollection.FindByIndexAsync(d => d.Value, id))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByIndex text equality (1 match)")]
    public async Task FindByIndex_Text()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string tag = $"tag:{id}";
        await foreach (var match in _lookupCollection.FindByIndexAsync(d => d.Tag, tag))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection Put with secondary indexes (insert, tx rollback)")]
    public async Task PutWithIndexes_Insert()
    {
        int id = Interlocked.Increment(ref _nextInsertId);
        var document = CreateDoc(id, $"insert:{id}");
        await _writeDb.BeginTransactionAsync();
        await _writeCollection.PutAsync($"bench:new:{id}", document);
        await _writeDb.RollbackAsync();
        _sink ^= document.Value;
    }

    [Benchmark(Description = "Collection Put with secondary indexes (update, tx rollback)")]
    public async Task PutWithIndexes_Update()
    {
        int slot = (Interlocked.Increment(ref _nextUpdateSlot) & int.MaxValue) % UpdateWorkingSetSize;
        int version = Interlocked.Increment(ref _nextInsertId);
        var document = CreateDoc(version, $"update:{version}");
        await _writeDb.BeginTransactionAsync();
        await _writeCollection.PutAsync($"bench:slot:{slot}", document);
        await _writeDb.RollbackAsync();
        _sink ^= document.Value;
    }

    [Benchmark(Description = "Collection Delete with secondary indexes (tx rollback)")]
    public async Task DeleteWithIndexes_Restore()
    {
        int slot = (Interlocked.Increment(ref _nextDeleteSlot) & int.MaxValue) % UpdateWorkingSetSize;
        string key = $"bench:slot:{slot}";
        await _writeDb.BeginTransactionAsync();
        bool deleted = await _writeCollection.DeleteAsync(key);
        await _writeDb.RollbackAsync();
        if (deleted)
            _sink ^= slot;
    }

    private async Task GlobalSetupAsync()
    {
        _lookupRandom = new Random(42);
        var options = CreateInMemoryOptions();

        _lookupDb = await Database.OpenInMemoryAsync(options);
        _writeDb = await Database.OpenInMemoryAsync(options);

        _lookupCollection = await _lookupDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _writeCollection = await _writeDb.GetCollectionAsync<BenchDoc>("bench_docs");

        await SeedLookupCollectionAsync();
        await SeedWriteCollectionAsync();

        await _lookupCollection.EnsureIndexAsync(d => d.Value);
        await _lookupCollection.EnsureIndexAsync(d => d.Tag);
        await _writeCollection.EnsureIndexAsync(d => d.Value);
        await _writeCollection.EnsureIndexAsync(d => d.Tag);

        _nextInsertId = SeedCount;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_lookupDb != null)
            await _lookupDb.DisposeAsync();
        if (_writeDb != null)
            await _writeDb.DisposeAsync();
    }

    private async Task SeedLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
                await _lookupCollection.PutAsync($"doc:{i}", CreateDoc(i, $"tag:{i}"));

            await _lookupDb.CommitAsync();
        }
        catch
        {
            await _lookupDb.RollbackAsync();
            throw;
        }
    }

    private async Task SeedWriteCollectionAsync()
    {
        await _writeDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < UpdateWorkingSetSize; i++)
                await _writeCollection.PutAsync($"bench:slot:{i}", CreateDoc(i, $"seed:{i}"));

            await _writeDb.CommitAsync();
        }
        catch
        {
            await _writeDb.RollbackAsync();
            throw;
        }
    }

    private static BenchDoc CreateDoc(int value, string tag)
        => new(
            $"User_{value}",
            value,
            s_categories[value % s_categories.Length],
            tag);

    private static DatabaseOptions CreateInMemoryOptions()
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
            }));
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CompositeIndexGuardrailBenchmarks
{
    private const int TargetA = 123;
    private const int TargetB = 10;
    private static readonly string PayloadPadding = new('p', 256);

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchSingleIndex = null!;
    private BenchmarkDatabase _benchCompositeIndex = null!;
    private string _lookupSql = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchNoIndex.Dispose();
        _benchSingleIndex.Dispose();
        _benchCompositeIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "WHERE a+b (no index)")]
    public async Task LookupNoIndex()
    {
        await ExecuteLookupAsync(_benchNoIndex);
    }

    [Benchmark(Description = "WHERE a+b (single-column index)")]
    public async Task LookupSingleColumnIndex()
    {
        await ExecuteLookupAsync(_benchSingleIndex);
    }

    [Benchmark(Description = "WHERE a+b (composite index)")]
    public async Task LookupCompositeIndex()
    {
        await ExecuteLookupAsync(_benchCompositeIndex);
    }

    private async Task ExecuteLookupAsync(BenchmarkDatabase bench)
    {
        await using var result = await bench.Db.ExecuteAsync(_lookupSql);
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        const string createTableSql =
            "CREATE TABLE bench_comp (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, payload TEXT)";

        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchSingleIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchCompositeIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);

        await SeedBenchAsync(_benchNoIndex, RowCount);
        await SeedBenchAsync(_benchSingleIndex, RowCount);
        await SeedBenchAsync(_benchCompositeIndex, RowCount);

        await _benchSingleIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_comp_a ON bench_comp(a)");
        await _benchCompositeIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_comp_ab ON bench_comp(a, b)");

        _lookupSql = $"SELECT * FROM bench_comp WHERE a = {TargetA} AND b = {TargetB}";
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        await bench.SeedAsync("bench_comp", rowCount, id =>
        {
            int a = id % 500;
            int b = (id / 500) % 500;
            return $"INSERT INTO bench_comp VALUES ({id}, {a}, {b}, 'payload_{id}_{PayloadPadding}')";
        });
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class IndexAggregateGuardrailBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchWithIndex = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchNoIndex.Dispose();
        _benchWithIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "COUNT(text_col) WHERE value BETWEEN ... (no index)")]
    public async Task CountTextRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(text_col) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task CountTextRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _benchNoIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        _benchWithIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)");
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class QueryPlanCacheGuardrailBenchmarks
{
    private const int RowCount = 10_000;

    private BenchmarkDatabase _bench = null!;
    private string _stableSql = null!;
    private Statement _preParsedStable = null!;
    private string[] _varyingSql = null!;
    private int _varyingSqlIndex;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
        _bench.Db.PreferSyncPointLookups = false;
        _bench.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)").GetAwaiter().GetResult();
        _bench.Db.ResetSelectPlanCacheDiagnostics();

        _stableSql = "SELECT id, value FROM bench WHERE value >= 1000 ORDER BY value LIMIT 128";
        _preParsedStable = Parser.Parse(_stableSql);
        _varyingSql = BuildVaryingSqlSet(1024);
        _varyingSqlIndex = 0;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        var stats = _bench.Db.GetSelectPlanCacheDiagnostics();
        Console.WriteLine(
            $"Select plan cache stats: hits={stats.HitCount}, misses={stats.MissCount}, " +
            $"reclassifications={stats.ReclassificationCount}, stores={stats.StoreCount}, entries={stats.EntryCount}");
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Stable SQL text (statement+plan cache hits)")]
    public async Task ExecuteStableSqlText()
    {
        await using var result = await _bench.Db.ExecuteAsync(_stableSql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Pre-parsed statement (plan cache hit)")]
    public async Task ExecutePreParsedStatement()
    {
        await using var result = await _bench.Db.ExecuteAsync(_preParsedStable);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Varying SQL text (limited plan reuse)")]
    public async Task ExecuteVaryingSqlText()
    {
        string sql = _varyingSql[_varyingSqlIndex];
        _varyingSqlIndex++;
        if (_varyingSqlIndex == _varyingSql.Length)
            _varyingSqlIndex = 0;

        await using var result = await _bench.Db.ExecuteAsync(sql);
        await result.ToListAsync();
    }

    private static string[] BuildVaryingSqlSet(int count)
    {
        var sql = new string[count];
        for (int i = 0; i < count; i++)
        {
            string sp1 = new(' ', 1 + (i & 0x3));
            string sp2 = new(' ', 1 + ((i >> 2) & 0x3));
            string sp3 = new(' ', 1 + ((i >> 4) & 0x3));
            string sp4 = new(' ', 1 + ((i >> 6) & 0x3));
            string sp5 = new(' ', 1 + ((i >> 8) & 0x3));
            string sp6 = new(' ', 1 + ((i >> 10) & 0x3));
            string sp7 = new(' ', 1 + ((i >> 12) & 0x3));

            sql[i] =
                $"SELECT{sp1}id,{sp2}value FROM{sp3}bench WHERE{sp4}value >={sp5}1000 ORDER BY{sp6}value LIMIT{sp7}128";
        }

        return sql;
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class WalGuardrailBenchmarks
{
    private BenchmarkDatabase _bench = null!;
    private int _nextId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync().GetAwaiter().GetResult();
        _nextId = 1_000_000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Single-row commit (WAL flush)")]
    public async Task CommitSingleRow()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _bench.Db.BeginTransactionAsync();
        await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'wal_test', 'Alpha')");
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "100-row batch commit")]
    public async Task CommitBatch_100()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'batch', 'Beta')");
        }
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Manual checkpoint after 100 writes")]
    public Task ManualCheckpoint_100()
    {
        return ManualCheckpointAsync(100, "ckpt100");
    }

    [Benchmark(Description = "Manual checkpoint after 1000 writes")]
    public Task ManualCheckpoint_1000()
    {
        return ManualCheckpointAsync(1000, "ckpt1000");
    }

    private async Task ManualCheckpointAsync(int writeCount, string text)
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < writeCount; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, '{text}', 'Gamma')");
        }
        await _bench.Db.CommitAsync();
        await _bench.Db.CheckpointAsync();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ScanProjectionGuardrailBenchmarks
{
    private const int RowCount = 10_000;

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Compact scan batch plan: residual column projection")]
    public async Task CompactResidualColumnProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, category FROM bench WHERE category IS NOT NULL");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compact scan batch plan: expression projection (20% selectivity)")]
    public async Task CompactExpressionProjection_20Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, value + id FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: residual column projection + LIMIT")]
    public async Task GenericResidualColumnProjection_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, category FROM bench WHERE category IS NOT NULL LIMIT {RowCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: IN expression projection + LIMIT")]
    public async Task GenericInExpressionProjection_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, value + id FROM bench WHERE category IN ('Beta', 'Gamma') LIMIT {RowCount}");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, text_col TEXT, category TEXT)");

        await SeedBenchAsync(_bench, RowCount);
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        string[] categories = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];
        var rng = new Random(42);

        await bench.SeedAsync("bench", rowCount, id =>
        {
            string categoryLiteral = (id % 5) == 0
                ? "NULL"
                : $"'{categories[id % categories.Length]}'";
            string text = DataGenerator.RandomString(rng, 50);
            return $"INSERT INTO bench VALUES ({id}, {rng.Next(0, 1_000_000)}, '{text}', {categoryLiteral})";
        });
    }
}
