using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class PreparedPrimaryKeyRangeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static PreparedSelectTemplate Prepare(string sql, params string[] names)
        => Assert.IsType<PreparedSelectTemplate>(PreparedSelectTemplate.TryCreate(Parser.Parse(sql), names));

    private static async Task<Database> CreateAsync(int count = 40)
    {
        var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id BIGINT PRIMARY KEY, value BIGINT)", Ct);
        await InsertAsync(db, 0, count);
        db.ResetSelectPlanCacheDiagnostics();
        return db;
    }

    private static async Task InsertAsync(Database db, int start, int count)
    {
        if (count == 0) return;
        await db.BeginTransactionAsync(Ct);
        var batch = db.PrepareInsertBatch("t", count);
        for (int i = start; i < start + count; i++)
            batch.AddRow([DbValue.FromInteger(i), DbValue.FromInteger(i * 10L)]);
        await batch.ExecuteAsync(Ct);
        await db.CommitAsync(Ct);
    }

    private static async Task<long[]> ReadAsync(Database db, Statement statement)
    {
        await using var result = await db.ExecuteAsync(statement, Ct);
        return (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger).ToArray();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(65)]
    [InlineData(2048)]
    public async Task ChangingRangeBounds_ReusesOnePlan(int cardinality)
    {
        await using var db = await CreateAsync(cardinality + 2);
        var template = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id", "lower", "upper");
        int executions = cardinality * 2 + 3;
        for (int i = 0; i < executions; i++)
        {
            long lower = i % cardinality;
            await using var result = await db.ExecuteAsync(template.Bind([lower, lower + 3]), Ct);
            if (i == 0) Assert.IsType<PrimaryKeyRangeScanOperator>(result.PhysicalRootOperator);
            var rows = await result.ToListAsync(Ct);
            Assert.Equal(new[] { lower, lower + 1, lower + 2 }, rows.Select(row => row[0].AsInteger));
            Assert.All(rows, row => Assert.Equal(row[0].AsInteger * 10, row[1].AsInteger));
        }
        var stats = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(1, stats.MissCount);
        Assert.Equal(executions - 1, stats.HitCount);
        Assert.Equal(1, stats.EntryCount);
    }

    [Fact]
    public async Task MonotonicRangeBounds_DoNotRetainValueBoundPlans()
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT id FROM t WHERE id >= @lower AND id < @upper ORDER BY id", "lower", "upper");
        for (int lower = 0; lower < 4096; lower++)
        {
            long[] expected = Enumerable.Range(0, 40).Select(i => (long)i).Where(i => i >= lower && i < lower + 2).ToArray();
            Assert.Equal(expected, await ReadAsync(db, template.Bind([lower, lower + 2])));
        }
        var stats = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(1, stats.EntryCount);
        Assert.Equal(1, stats.MissCount);
        Assert.Equal(4095, stats.HitCount);
    }

    [Theory]
    [InlineData(5, 20)]
    [InlineData(20, 5)]
    [InlineData(8, 8)]
    [InlineData(long.MinValue, long.MaxValue)]
    public async Task ReversedBoundsAndRowWindows_PreserveInclusiveExclusiveSemantics(long lower, long upper)
    {
        await using var db = await CreateAsync();
        var template = Prepare("SELECT x.id AS identifier, x.value FROM t x WHERE @lower < x.id AND x.id <= @upper AND x.id >= 1 ORDER BY x.id LIMIT 4 OFFSET 1", "lower", "upper");
        long[] expected = Enumerable.Range(0, 40).Select(i => (long)i)
            .Where(i => lower < i && i <= upper && i >= 1).Skip(1).Take(4).ToArray();
        Assert.Equal(expected, await ReadAsync(db, template.Bind([lower, upper])));
        var zero = Prepare("SELECT * FROM t WHERE id > @lower ORDER BY id LIMIT 0", "lower");
        Assert.Empty(await ReadAsync(db, zero.Bind([lower])));
    }

    [Fact]
    public async Task RangeProjections_WithChangingBoundsAndRowWindows()
    {
        await using var db = await CreateAsync(160);
        foreach (int limit in new[] { 1, 4, 5, 128 })
        foreach (int offset in new[] { 0, 3, 200 })
        {
            var template = Prepare($"SELECT value AS payload, id AS first_key, id AS second_key FROM t WHERE id >= @lower AND id < @upper ORDER BY id LIMIT {limit} OFFSET {offset}", "lower", "upper");
            foreach (int lower in new[] { 10, 20, 180 })
            {
                await using var result = await db.ExecuteAsync(template.Bind([lower, lower + 140]), Ct);
                Assert.Equal(new[] { "payload", "first_key", "second_key" }, result.Schema.Select(column => column.Name));
                long[] expected = Enumerable.Range(0, 160).Select(i => (long)i)
                    .Where(i => i >= lower && i < lower + 140).Skip(offset).Take(limit).ToArray();
                var rows = await result.ToListAsync(Ct);
                Assert.Equal(expected, rows.Select(row => row[1].AsInteger));
                Assert.All(rows, row =>
                {
                    Assert.Equal(row[1].AsInteger * 10, row[0].AsInteger);
                    Assert.Equal(row[1], row[2]);
                });
            }
        }
    }

    [Fact]
    public async Task ExtremeBoundsAndOverlappingResults_KeepExecutionValuesIndependent()
    {
        await using var db = await CreateAsync(0);
        await db.ExecuteAsync($"INSERT INTO t VALUES ({long.MinValue}, 1), (-1, 2), (0, 3), ({long.MaxValue}, 4)", Ct);
        var template = Prepare("SELECT id, value FROM t WHERE id >= @lower AND id <= @upper ORDER BY id", "lower", "upper");
        long[] values = [long.MinValue, -1];
        Statement first = template.Bind(values);
        values[0] = 0; values[1] = long.MaxValue;
        await using var firstResult = await db.ExecuteAsync(first, Ct);
        await using var secondResult = await db.ExecuteAsync(template.Bind(values), Ct);
        Assert.Equal(new long[] { 0, long.MaxValue }, (await secondResult.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        Assert.Equal(new long[] { long.MinValue, -1 }, (await firstResult.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        Assert.Equal(new[] { long.MaxValue }, await ReadAsync(db, template.Bind([long.MaxValue, long.MaxValue])));
        var exclusive = Prepare("SELECT id FROM t WHERE id > @lower ORDER BY id", "lower");
        Assert.Empty(await ReadAsync(db, exclusive.Bind([long.MaxValue])));
    }

    [Fact]
    public async Task NullableColumnAndRolledBackSchemaChanges_RefreshRangeMetadata()
    {
        await using var db = await CreateAsync(3);
        var template = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id", "lower", "upper");
        Assert.Equal(new long[] { 0, 1 }, await ReadAsync(db, template.Bind([0, 2])));
        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN added TEXT", Ct);
        await using (var expanded = await db.ExecuteAsync(template.Bind([0, 2]), Ct))
        {
            Assert.Equal(new[] { "id", "value", "added" }, expanded.Schema.Select(column => column.Name));
            Assert.All(await expanded.ToListAsync(Ct), row => { Assert.Equal(3, row.Length); Assert.True(row[2].IsNull); });
        }
        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("DROP TABLE t", Ct);
        await db.ExecuteAsync("CREATE TABLE t (value BIGINT, id BIGINT PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (900, 1)", Ct);
        await using (var changed = await db.ExecuteAsync(template.Bind([0, 2]), Ct))
        {
            Assert.Equal(new[] { "value", "id" }, changed.Schema.Select(column => column.Name));
            Assert.Equal(new long[] { 900, 1 }, Assert.Single(await changed.ToListAsync(Ct)).Select(value => value.AsInteger));
        }
        await db.RollbackAsync(Ct);
        Assert.Equal(new long[] { 0, 1 }, await ReadAsync(db, template.Bind([0, 2])));
    }

    [Fact]
    public async Task CachedRangePlan_UsesCurrentRootsWhileOpenReaderKeepsItsSnapshot()
    {
        await using var db = await CreateAsync(16);
        var template = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id", "lower", "upper");
        Assert.Equal(16, (await ReadAsync(db, template.Bind([0, 1024]))).Length);
        using var snapshot = db.CreateReaderSession();
        // ReaderSession retains its range fallback. New executions are covered by snapshot-catalog tests.
        await using var old = await snapshot.ExecuteReadAsync(template.Bind([0, 1024]), Ct);
        Assert.IsNotType<PrimaryKeyRangeScanOperator>(old.PhysicalRootOperator);
        // Grow beyond a leaf page so the cached plan cannot retain a table root from preparation.
        await InsertAsync(db, 16, 1008);
        Assert.Equal(Enumerable.Range(0, 1024).Select(i => (long)i), await ReadAsync(db, template.Bind([0, 1024])));
        Assert.Equal(Enumerable.Range(0, 16).Select(i => (long)i), (await old.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        using var latest = db.CreateReaderSession();
        await using var current = await latest.ExecuteReadAsync(template.Bind([1000, 1024]), Ct);
        Assert.Equal(Enumerable.Range(1000, 24).Select(i => (long)i), (await current.ToListAsync(Ct)).Select(row => row[0].AsInteger));
    }

    [Theory]
    [InlineData(9, false, false)]
    [InlineData(10, true, false)]
    [InlineData(19, true, false)]
    [InlineData(20, false, false)]
    [InlineData(12, true, true)]
    [InlineData(20, false, true)]
    public async Task SerializableRangeRead_TracksOnlyIncludedTableKeys(int insertedKey, bool conflicts, bool emptyRead)
    {
        await using var db = await CreateAsync(0);
        await db.ExecuteAsync(emptyRead ? "INSERT INTO t VALUES (30, 300)" : "INSERT INTO t VALUES (15, 150), (30, 300)", Ct);
        var template = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id", "lower", "upper");
        await using var transaction = await db.BeginWriteTransactionAsync(Ct);
        await using (var result = await transaction.ExecuteAsync(template.Bind([10, 20]), Ct))
        {
            Assert.IsType<PrimaryKeyRangeScanOperator>(result.PhysicalRootOperator);
            Assert.Equal(emptyRead ? Array.Empty<long>() : [15L], (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        }
        await db.ExecuteAsync($"INSERT INTO t VALUES ({insertedKey}, 1)", Ct);
        if (conflicts)
            await Assert.ThrowsAsync<CSharpDbConflictException>(() => transaction.CommitAsync(Ct).AsTask());
        else
            await transaction.CommitAsync(Ct);
    }

    [Fact]
    public async Task ReaderSession_RetainsRangeFallbackAndExistingLookupAndIndexPaths()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        using var reader = db.CreateReaderSession();
        var range = Prepare("SELECT * FROM t WHERE id >= @lower AND id <= @upper ORDER BY id", "lower", "upper");
        foreach (long[] bounds in new long[][] { [2, 4], [7, 7], [10, 12] })
        {
            await using var result = await reader.ExecuteReadAsync(range.Bind(bounds), Ct);
            Assert.IsNotType<PrimaryKeyRangeScanOperator>(result.PhysicalRootOperator);
            Assert.IsNotType<PrimaryKeyLookupOperator>(result.PhysicalRootOperator);
            Assert.Equal(Enumerable.Range((int)bounds[0], (int)(bounds[1] - bounds[0] + 1)).Select(i => (long)i),
                (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        }

        var equality = Prepare("SELECT id FROM t WHERE id = @p", "p");
        await using (var result = await reader.ExecuteReadAsync(equality.Bind([5]), Ct))
            Assert.Equal(5L, Assert.Single(await result.ToListAsync(Ct))[0].AsInteger);

        var indexed = Prepare("SELECT * FROM t WHERE value >= @lower AND value < @upper ORDER BY value", "lower", "upper");
        await using var indexedResult = await reader.ExecuteReadAsync(indexed.Bind([20, 50]), Ct);
        Assert.IsType<IndexOrderedScanOperator>(indexedResult.PhysicalRootOperator);
        Assert.Equal(new long[] { 2, 3, 4 }, (await indexedResult.ToListAsync(Ct)).Select(row => row[0].AsInteger));
    }
}
