using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class PreparedSelectPlanTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static PreparedSelectTemplate Prepare(string sql, params string[] names)
        => Assert.IsType<PreparedSelectTemplate>(PreparedSelectTemplate.TryCreate(Parser.Parse(sql), names));

    private static async Task<Database> CreateAsync(int count = 40)
    {
        var db = await Database.OpenInMemoryAsync(Ct);
        db.PreferSyncPointLookups = false;
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.BeginTransactionAsync(Ct);
        var batch = db.PrepareInsertBatch("t", count);
        for (int i = 0; i < count; i++) batch.AddRow([DbValue.FromInteger(i), DbValue.FromInteger(i)]);
        await batch.ExecuteAsync(Ct);
        await db.CommitAsync(Ct);
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        db.ResetSelectPlanCacheDiagnostics();
        return db;
    }

    private static async Task<long[]> ReadAsync(Database db, Statement statement)
    {
        await using var result = await db.ExecuteAsync(statement, Ct);
        return (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger).ToArray();
    }

    [Fact]
    public async Task PrimaryKeyRangeObservability_ReportsScanAndExecutionLocalEquality()
    {
        await using var db = await CreateAsync();
        var planner = Assert.IsType<QueryPlanner>(typeof(Database)
            .GetField("_planner", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(db));
        var observer = new PreparedRangeObserver();
        planner.PlanRuntimeObserver = observer;
        var template = Prepare("SELECT id FROM t WHERE id >= @lower AND id <= @upper ORDER BY id", "lower", "upper");

        Assert.Equal(new long[] { 2, 3, 4, 5 }, await ReadAsync(db, template.Bind([2, 5])));
        Assert.Equal(new long[] { 7 }, await ReadAsync(db, template.Bind([7, 7])));
        Assert.Equal(new long[] { 10, 11, 12 }, await ReadAsync(db, template.Bind([10, 12])));
        Assert.Equal(new[] { false, true, true }, observer.Hits);
        Assert.Equal(QueryPlanAccessPathCategory.IndexScan, observer.Selections[0].AccessPath);
        Assert.Null(observer.Selections[0].EstimatedRows);
        Assert.Equal(QueryPlanAccessPathCategory.PrimaryKeyLookup, observer.Selections[1].AccessPath);
        Assert.Equal(1, observer.Selections[1].EstimatedRows);
        Assert.Equal(QueryPlanAccessPathCategory.IndexScan, observer.Selections[2].AccessPath);
        Assert.Null(observer.Selections[2].EstimatedRows);
    }

    private sealed class PreparedRangeObserver : IQueryPlanRuntimeObserver
    {
        internal List<bool> Hits { get; } = [];
        internal List<QueryPlanRuntimeSelection> Selections { get; } = [];
        public void OnPlanCacheLookup(bool hit) => Hits.Add(hit);
        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection) => Selections.Add(selection);
        public void OnPlanChanged(QueryPlanChangeKind change) { }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(256)]
    [InlineData(2048)]
    public async Task ChangingValues_ReusesOnePlan(int cardinality)
    {
        await using var db = await CreateAsync(cardinality);
        var template = Prepare("SELECT id, value FROM t WHERE value = @p", "p");
        int executions = 2 * cardinality + 3;
        for (int i = 0; i < executions; i++)
            Assert.Equal(new[] { (long)(i % cardinality) }, await ReadAsync(db, template.Bind([i % cardinality])));
        var stats = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(1, stats.MissCount);
        Assert.Equal(executions - 1, stats.HitCount);
        Assert.Equal(1, stats.EntryCount);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(65, false)]
    [InlineData(65, true)]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    public async Task PrimaryKeyChangingValues_ReusesOnePlan(int cardinality, bool preferSync)
    {
        await using var db = await CreateAsync(cardinality);
        db.PreferSyncPointLookups = preferSync;
        var template = Prepare("SELECT id FROM t WHERE id = @p", "p");
        int executions = 2 * cardinality + 3;
        for (int i = 0; i < executions; i++)
            Assert.Equal(new[] { (long)(i % cardinality) }, await ReadAsync(db, template.Bind([i % cardinality])));
        var stats = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(1, stats.MissCount);
        Assert.Equal(executions - 1, stats.HitCount);
        Assert.Equal(1, stats.EntryCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PrimaryKeyProjections_PreserveValuesMetadataAndLookupPaths(bool preferSync)
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("UPDATE t SET value = 1005 WHERE id = 5", Ct);
        // Warm the table page before checking the optional cache-only execution path.
        await using (var warm = await db.ExecuteAsync("SELECT * FROM t WHERE id = 5", Ct))
            _ = await warm.ToListAsync(Ct);
        db.PreferSyncPointLookups = preferSync;

        var star = Prepare("SELECT * FROM t WHERE id = @p", "p");
        await using (var result = await db.ExecuteAsync(star.Bind([5]), Ct))
        {
            if (preferSync) Assert.Null(result.PhysicalRootOperator);
            else Assert.IsType<PrimaryKeyLookupOperator>(result.PhysicalRootOperator);
            var row = Assert.Single(await result.ToListAsync(Ct));
            Assert.Equal(new long[] { 5, 1005 }, row.Select(value => value.AsInteger));
            Assert.Equal(new[] { "id", "value" }, result.Schema.Select(column => column.Name));
        }

        var keyOnly = Prepare("SELECT x.id AS first_key, x.id AS second_key FROM t x WHERE x.id = @p", "p");
        await using (var result = await db.ExecuteAsync(keyOnly.Bind([5]), Ct))
        {
            if (preferSync) Assert.Null(result.PhysicalRootOperator);
            else Assert.IsType<PrimaryKeyProjectionLookupOperator>(result.PhysicalRootOperator);
            var row = Assert.Single(await result.ToListAsync(Ct));
            Assert.Equal(new long[] { 5, 5 }, row.Select(value => value.AsInteger));
            Assert.Equal(new[] { "first_key", "second_key" }, result.Schema.Select(column => column.Name));
        }

        var columns = Prepare("SELECT x.value AS payload, x.id AS identifier FROM t x WHERE @p = x.id", "p");
        await using (var result = await db.ExecuteAsync(columns.Bind([5]), Ct))
        {
            var row = Assert.Single(await result.ToListAsync(Ct));
            Assert.Equal(new long[] { 1005, 5 }, row.Select(value => value.AsInteger));
            Assert.Equal(new[] { "payload", "identifier" }, result.Schema.Select(column => column.Name));
        }

        Assert.Empty(await ReadAsync(db, star.Bind([500])));
        Assert.Empty(await ReadAsync(db, keyOnly.Bind([500])));
        Assert.Empty(await ReadAsync(db, columns.Bind([500])));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PrimaryKeyEquality_IntersectsBoundsAndAppliesRowWindows(bool preferSync)
    {
        await using var db = await CreateAsync();
        db.PreferSyncPointLookups = preferSync;
        var template = Prepare("SELECT id FROM t WHERE id = @p AND @lower <= id AND id < @upper ORDER BY id LIMIT 1 OFFSET 0", "p", "lower", "upper");
        Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5, 5, 6])));
        Assert.Empty(await ReadAsync(db, template.Bind([5, 6, 10])));
        Assert.Empty(await ReadAsync(db, template.Bind([5, 0, 5])));
        Assert.Equal(new long[] { 6 }, await ReadAsync(db, template.Bind([6, long.MinValue, long.MaxValue])));

        var limited = Prepare("SELECT * FROM t WHERE id = @p ORDER BY id LIMIT 0", "p");
        var offset = Prepare("SELECT id AS identifier FROM t WHERE id = @p ORDER BY id LIMIT 1 OFFSET 1", "p");
        Assert.Empty(await ReadAsync(db, limited.Bind([5])));
        Assert.Empty(await ReadAsync(db, offset.Bind([5])));
    }

    [Fact]
    public async Task PrimaryKeyLongExtremesAndOverlappingResults_RetainEachExecutionValue()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id BIGINT PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync($"INSERT INTO t VALUES ({long.MinValue}, 10), ({long.MaxValue}, 20)", Ct);
        db.PreferSyncPointLookups = false;
        var template = Prepare("SELECT id, value FROM t WHERE id = @p", "p");
        long[] values = [long.MinValue];
        var first = template.Bind(values);
        values[0] = long.MaxValue;
        await using var firstResult = await db.ExecuteAsync(first, Ct);
        await using var secondResult = await db.ExecuteAsync(template.Bind(values), Ct);
        var secondRow = Assert.Single(await secondResult.ToListAsync(Ct));
        var firstRow = Assert.Single(await firstResult.ToListAsync(Ct));
        Assert.Equal(new long[] { long.MinValue, 10 }, firstRow.Select(value => value.AsInteger));
        Assert.Equal(new long[] { long.MaxValue, 20 }, secondRow.Select(value => value.AsInteger));
        Assert.Empty(await ReadAsync(db, template.Bind([0])));
    }

    [Fact]
    public async Task PrimaryKeySchemaReplacementAndRollback_RefreshMetadataAndTableRoots()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id, value FROM t WHERE id = @p", "p");
        Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5])));
        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("UPDATE t SET id = 105 WHERE id = 5", Ct);
        Assert.Equal(new long[] { 105 }, await ReadAsync(db, template.Bind([105])));
        Assert.Empty(await ReadAsync(db, template.Bind([5])));
        await db.RollbackAsync(Ct);
        Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5])));
        Assert.Empty(await ReadAsync(db, template.Bind([105])));

        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("DROP TABLE t", Ct);
        await db.ExecuteAsync("CREATE TABLE t (value BIGINT, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (500, 5)", Ct);
        await using (var changed = await db.ExecuteAsync(template.Bind([5]), Ct))
            Assert.Equal(new long[] { 5, 500 }, Assert.Single(await changed.ToListAsync(Ct)).Select(value => value.AsInteger));
        await db.RollbackAsync(Ct);
        await using var restored = await db.ExecuteAsync(template.Bind([5]), Ct);
        Assert.Equal(new long[] { 5, 5 }, Assert.Single(await restored.ToListAsync(Ct)).Select(value => value.AsInteger));
        Assert.Equal(1, db.GetSelectPlanCacheDiagnostics().EntryCount);
    }

    [Theory]
    [InlineData("INTEGER", true)]
    [InlineData("SMALLINT", false)]
    [InlineData("TINYINT", false)]
    public async Task BackedPrimaryKeys_DoNotTreatDeclaredKeyValuesAsRowIds(string keyType, bool composite)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        string definition = composite
            ? $"id {keyType}, value INTEGER, PRIMARY KEY (id, value)"
            : $"id {keyType} PRIMARY KEY, value INTEGER";
        await db.ExecuteAsync($"CREATE TABLE t ({definition})", Ct);
        await db.ExecuteAsync(composite
            ? "INSERT INTO t VALUES (11, 101), (11, 102), (22, 201)"
            : "INSERT INTO t VALUES (11, 101), (22, 201)", Ct);
        Assert.NotNull(Assert.Single(db.GetTableSchema("t")!.KeyConstraints).BackingIndexName);

        var template = Prepare("SELECT value FROM t WHERE id = @p", "p");
        long[] firstExpected = composite ? [101, 102] : [101];
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(firstExpected, (await ReadAsync(db, template.Bind([11]))).Order());
            Assert.Equal(new long[] { 201 }, await ReadAsync(db, template.Bind([22])));
        }
        Assert.Empty(await ReadAsync(db, template.Bind([1])));
    }

    [Fact]
    public async Task MonotonicValues_DoNotAccumulateBoundPlans()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        for (int i = 0; i < 4096; i++)
            Assert.Equal(i < 40 ? new[] { (long)i } : [], await ReadAsync(db, template.Bind([i])));
        Assert.Equal(1, db.GetSelectPlanCacheDiagnostics().EntryCount);
        Assert.Equal(4095, db.GetSelectPlanCacheDiagnostics().HitCount);
    }

    [Theory]
    [InlineData(3, 20)]
    [InlineData(20, 3)]
    [InlineData(8, 8)]
    [InlineData(long.MinValue, long.MaxValue)]
    public async Task RangeBounds_OrderAndOffset_MatchPredicate(long lower, long upper)
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT x.id AS identifier, x.value FROM t x WHERE @lower <= x.value AND x.value < @upper ORDER BY x.value LIMIT 4 OFFSET 1", "lower", "upper");
        long[] expected = Enumerable.Range(0, 40).Select(i => (long)i).Where(i => i >= lower && i < upper).Skip(1).Take(4).ToArray();
        Assert.Equal(expected, await ReadAsync(db, template.Bind([lower, upper])));
    }

    [Fact]
    public async Task MultipleBounds_EqualityAndLongExtremes_AreIntersectedWithoutOverflow()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync($"INSERT INTO t VALUES (100, {long.MinValue}), (101, {long.MaxValue}), (102, NULL)", Ct);
        var equality = Prepare("SELECT id FROM t WHERE value = @p AND value >= @lower AND @upper >= value", "p", "lower", "upper");
        Assert.Equal(new long[] { 100 }, await ReadAsync(db, equality.Bind([long.MinValue, long.MinValue, long.MaxValue])));
        Assert.Equal(new long[] { 101 }, await ReadAsync(db, equality.Bind([long.MaxValue, long.MinValue, long.MaxValue])));
        Assert.Empty(await ReadAsync(db, equality.Bind([10, 11, 12])));
        var range = Prepare("SELECT id FROM t WHERE value > @p ORDER BY value", "p");
        Assert.Empty(await ReadAsync(db, range.Bind([long.MaxValue])));
        Assert.DoesNotContain(102, await ReadAsync(db, range.Bind([long.MinValue])));
    }

    [Fact]
    public async Task ExecutionValues_AndOverlappingReaders_AreIndependent()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE value >= @p ORDER BY value LIMIT 3", "p");
        long[] values = [3];
        var first = template.Bind(values);
        values[0] = 20;
        await using var firstResult = await db.ExecuteAsync(first, Ct);
        await using var secondResult = await db.ExecuteAsync(template.Bind(values), Ct);
        Assert.Equal(new long[] { 20, 21, 22 }, (await secondResult.ToListAsync(Ct)).Select(r => r[0].AsInteger));
        Assert.Equal(new long[] { 3, 4, 5 }, (await firstResult.ToListAsync(Ct)).Select(r => r[0].AsInteger));
    }

    [Fact]
    public async Task IndexAndTableSchemaChanges_InvalidateCachedMetadata()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5])));
        await db.ExecuteAsync("DROP INDEX ix_value", Ct);
        Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5])));
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        Assert.Equal(new long[] { 6 }, await ReadAsync(db, template.Bind([6])));
        await db.ExecuteAsync("DROP TABLE t", Ct);
        await db.ExecuteAsync("CREATE TABLE t (value INTEGER, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (5, 99)", Ct);
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        Assert.Equal(new long[] { 99 }, await ReadAsync(db, template.Bind([5])));
        Assert.Equal(1, db.GetSelectPlanCacheDiagnostics().EntryCount);
    }

    [Fact]
    public async Task TransactionRollback_DoesNotRetainIndexContentsOrUncommittedSchema()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([1])));
        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("UPDATE t SET value = 700 WHERE id = 1", Ct);
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([700])));
        await db.RollbackAsync(Ct);
        Assert.Empty(await ReadAsync(db, template.Bind([700])));
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([1])));
        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("DROP INDEX ix_value", Ct);
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([1])));
        await db.RollbackAsync(Ct);
        Assert.Equal(new long[] { 2 }, await ReadAsync(db, template.Bind([2])));
    }

    [Fact]
    public async Task TemporaryTableShadow_DoesNotReusePersistentIndexMetadata()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([1])));
        await db.ExecuteAsync("CREATE TEMP TABLE t (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (90, 1)", Ct);
        Assert.Equal(new long[] { 90 }, await ReadAsync(db, template.Bind([1])));
        await db.ExecuteAsync("DROP TEMP TABLE t", Ct);
        Assert.Equal(new long[] { 1 }, await ReadAsync(db, template.Bind([1])));
    }

    [Fact]
    public async Task NonUniqueKeysAndStarProjection_ReturnEveryMatchingRow()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("UPDATE t SET value = 10 WHERE id < 4", Ct);
        var equality = Prepare("SELECT * FROM t WHERE value = @p", "p");
        var range = Prepare("SELECT * FROM t WHERE value >= @lower AND value < @upper ORDER BY value", "lower", "upper");
        Assert.Equal(new long[] { 0, 1, 2, 3, 10 }, (await ReadAsync(db, equality.Bind([10]))).Order());
        Assert.Equal(new long[] { 0, 1, 2, 3, 10 }, (await ReadAsync(db, range.Bind([10, 11]))).Order());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task SmallOrderedRangeLimits_PreserveDuplicateRowsAndChangingBounds(int limit)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT, payload BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES " +
            "(1, 10, 1001), (2, 10, 1002), (3, 10, 1003), " +
            "(4, 20, 1004), (5, 20, 1005), (6, 20, 1006), " +
            "(7, 30, 1007), (8, 30, 1008), (9, 30, 1009)", Ct);
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);

        foreach (string projection in new[] { "id, value", "*" })
        {
            var template = Prepare($"SELECT {projection} FROM t WHERE value >= @p ORDER BY value LIMIT {limit}", "p");
            foreach (long lower in new long[] { 10, 20, 30, 99, 10 })
            {
                long[] expectedIds = Enumerable.Range(1, 9).Select(id => (long)id)
                    .Where(id => ((id - 1) / 3 + 1) * 10 >= lower).Take(limit).ToArray();
                await using var result = await db.ExecuteAsync(template.Bind([lower]), Ct);
                var rows = await result.ToListAsync(Ct);
                Assert.Equal(expectedIds.Length, rows.Count);
                Assert.Equal(expectedIds, rows.Select(row => row[0].AsInteger));
                Assert.Equal(expectedIds.Select(id => ((id - 1) / 3 + 1) * 10), rows.Select(row => row[1].AsInteger));
                if (projection == "*")
                    Assert.Equal(expectedIds.Select(id => 1000 + id), rows.Select(row => row[2].AsInteger));
            }
        }
    }

    [Fact]
    public async Task UniqueIndexAndIndexReplacement_PreserveLookupResults()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE UNIQUE INDEX ux_value ON t(value)", Ct);
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        Assert.Equal(new long[] { 7 }, await ReadAsync(db, template.Bind([7])));
        Assert.Empty(await ReadAsync(db, template.Bind([700])));
        await db.ExecuteAsync("DROP INDEX ux_value", Ct);
        Assert.Equal(new long[] { 8 }, await ReadAsync(db, template.Bind([8])));
    }

    [Fact]
    public async Task AnalyzedFrequentValues_StillUseTheOrdinarySelectivityDecision()
    {
        await using var db = await CreateAsync(200);
        await db.ExecuteAsync("UPDATE t SET value = 0 WHERE id < 199", Ct);
        await db.ExecuteAsync("ANALYZE t", Ct);
        var template = Prepare("SELECT id FROM t WHERE value = @p", "p");
        Assert.Equal(199, (await ReadAsync(db, template.Bind([0]))).Length);
        // The frequent value fell back to an ordinary bound plan in addition to the template metadata.
        Assert.Equal(2, db.GetSelectPlanCacheDiagnostics().EntryCount);
        Assert.Equal(new long[] { 199 }, await ReadAsync(db, template.Bind([199])));
        Assert.Equal(2, db.GetSelectPlanCacheDiagnostics().EntryCount);
    }

    [Fact]
    public async Task TemplateCache_IsBoundedAndEvictedTemplatesCanExecuteAgain()
    {
        await using var db = await CreateAsync();
        var templates = Enumerable.Range(0, 300).Select(_ => Prepare("SELECT id FROM t WHERE value = @p", "p")).ToArray();
        foreach (var template in templates) Assert.Equal(new long[] { 5 }, await ReadAsync(db, template.Bind([5])));
        Assert.Equal(256, db.GetSelectPlanCacheDiagnostics().EntryCount);
        long misses = db.GetSelectPlanCacheDiagnostics().MissCount;
        Assert.Equal(new long[] { 6 }, await ReadAsync(db, templates[0].Bind([6])));
        Assert.Equal(misses + 1, db.GetSelectPlanCacheDiagnostics().MissCount);
        Assert.Equal(256, db.GetSelectPlanCacheDiagnostics().EntryCount);
    }
}
