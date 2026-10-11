using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class PrimaryKeyRangeMutationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static IEnumerable<object[]> Predicates()
    {
        (string Sql, long[] Ids, bool Indexed)[] cases =
        [
            ("id >= 40 AND id < 44", [40, 41, 42, 43], true),
            ("40 <= id AND 44 > id", [40, 41, 42, 43], true),
            ("id BETWEEN 40 AND 43", [40, 41, 42, 43], true),
            ("id >= 38 AND id >= 40 AND id <= 45 AND id < 44", [40, 41, 42, 43], true),
            ("id > 124", [125, 126, 127], true),
            ("id < 3", [0, 1, 2], true),
            ("id >= 44 AND id < 40", [], true),
            ("id > 40 AND id <= 40", [], true),
            ("id >= 200 AND id < 204", [], true),
            ("id >= 40 AND id < 44 AND (value = 410 OR value = 430)", [41, 43], true),
            ("id >= NULL AND id < 4", [], true),
            ("(id >= 40 AND id < 44) OR id = 1", [1, 40, 41, 42, 43], false),
        ];
        foreach (bool delete in new[] { false, true })
            foreach (var item in cases)
                yield return [delete, item.Sql, item.Ids, item.Indexed];
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public async Task RangeMutation_PreservesBoundsResidualsAndNeighborRows(
        bool delete, string predicate, long[] targets, bool indexed)
    {
        await using var db = await CreateAsync();
        db.ResetMutationTargetCollectionDiagnostics();
        await using var result = await db.ExecuteAsync(
            (delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") + $" WHERE {predicate}", Ct);
        Assert.Equal(targets.Length, result.RowsAffected);
        List<DbValue[]> rows = await RowsAsync(db);
        Assert.Equal(128 - (delete ? targets.Length : 0), rows.Count);
        Assert.Equal(Enumerable.Range(0, 128).Select(i => (long)i).Where(i => !delete || !targets.Contains(i)),
            rows.Select(row => row[0].AsInteger));
        foreach (DbValue[] row in rows)
        {
            long id = row[0].AsInteger;
            Assert.Equal(id * 10 + (!delete && targets.Contains(id) ? 1 : 0), row[1].AsInteger);
            Assert.Equal($"payload-{id}", row[2].AsText);
        }
        var diagnostics = db.GetMutationTargetCollectionDiagnosticsSnapshot();
        Assert.Equal(indexed ? 1 : 0, diagnostics.IndexedCollectionCount);
        Assert.Equal(indexed ? 0 : 1, diagnostics.ScannedCollectionCount);
    }

    [Fact]
    public async Task PrimaryKeyChange_CollectsStableTargetsAndMaintainsSecondaryIndex()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        await using var result = await db.ExecuteAsync(
            "UPDATE t SET id = id + 1000, value = value + 1 WHERE id >= 40 AND id < 44", Ct);
        Assert.Equal(4, result.RowsAffected);
        Assert.Equal(128, (await RowsAsync(db)).Count);
        for (int id = 40; id < 44; id++)
        {
            await using var old = await db.ExecuteAsync($"SELECT id FROM t WHERE id = {id} OR value = {id * 10}", Ct);
            Assert.Empty(await old.ToListAsync(Ct));
            await using var current = await db.ExecuteAsync($"SELECT id, payload FROM t WHERE value = {id * 10 + 1}", Ct);
            DbValue[] row = Assert.Single(await current.ToListAsync(Ct));
            Assert.Equal(id + 1000, row[0].AsInteger);
            Assert.Equal($"payload-{id}", row[1].AsText);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rollback_RestoresRowsAndSecondaryIndex(bool delete)
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);
        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync((delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") +
            " WHERE id >= 40 AND id < 44", Ct);
        await db.RollbackAsync(Ct);
        Assert.Equal(128, (await RowsAsync(db)).Count);
        await using var result = await db.ExecuteAsync("SELECT id, value FROM t WHERE value BETWEEN 400 AND 430 ORDER BY id", Ct);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(new long[] { 40, 41, 42, 43 }, rows.Select(row => row[0].AsInteger));
        Assert.All(rows, row => Assert.Equal(row[0].AsInteger * 10, row[1].AsInteger));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExtremeBounds_DoNotOverflow(bool delete)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id BIGINT PRIMARY KEY, value INTEGER)", Ct);
        await db.ExecuteAsync($"INSERT INTO t VALUES ({long.MinValue}, 1), (-1, 2), (0, 3), ({long.MaxValue}, 4)", Ct);
        string mutation = delete ? "DELETE FROM t" : "UPDATE t SET value = value + 10";
        foreach (string empty in new[] { $"id > {long.MaxValue}", $"id < {long.MinValue}" })
        {
            await using var zero = await db.ExecuteAsync($"{mutation} WHERE {empty}", Ct);
            Assert.Equal(0, zero.RowsAffected);
        }
        await using var changed = await db.ExecuteAsync($"{mutation} WHERE id > {long.MinValue} AND id < {long.MaxValue}", Ct);
        Assert.Equal(2, changed.RowsAffected);
        await using var result = await db.ExecuteAsync("SELECT id, value FROM t ORDER BY id", Ct);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(delete ? new long[] { long.MinValue, long.MaxValue } : [long.MinValue, -1, 0, long.MaxValue],
            rows.Select(row => row[0].AsInteger));
        Assert.Equal(delete ? new long[] { 1, 4 } : [1, 12, 13, 4], rows.Select(row => row[1].AsInteger));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BackedPrimaryKey_UsesLogicalKeyInsteadOfHiddenRowId(bool delete, bool composite)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync(composite
            ? "CREATE TABLE t (id INTEGER, value INTEGER, PRIMARY KEY (id, value))"
            : "CREATE TABLE t (id SMALLINT PRIMARY KEY, value INTEGER)", Ct);
        await db.ExecuteAsync(composite ? "INSERT INTO t VALUES (11, 100), (11, 200), (22, 300)"
            : "INSERT INTO t VALUES (11, 100), (22, 300)", Ct);
        await using var changed = await db.ExecuteAsync((delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") +
            " WHERE id >= 10 AND id < 20", Ct);
        Assert.Equal(composite ? 2 : 1, changed.RowsAffected);
        await using var result = await db.ExecuteAsync("SELECT id, value FROM t ORDER BY id, value", Ct);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(delete ? new long[] { 22 } : composite ? [11, 11, 22] : [11, 22], rows.Select(row => row[0].AsInteger));
        Assert.Equal(delete ? new long[] { 300 } : composite ? [101, 201, 300] : [101, 300], rows.Select(row => row[1].AsInteger));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SerializableMutation_EmptyRangeStillDetectsPhantom(bool delete)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (30, 300)", Ct);
        await using var transaction = await db.BeginWriteTransactionAsync(Ct);
        await using var result = await transaction.ExecuteAsync(
            (delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") + " WHERE id >= 10 AND id < 20", Ct);
        Assert.Equal(0, result.RowsAffected);
        await db.ExecuteAsync("INSERT INTO t VALUES (12, 120)", Ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => transaction.CommitAsync(Ct).AsTask());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Explain_ReportsBoundedMutationAndAnalyzePreservesRowIdentity(bool delete, bool analyze)
    {
        await using var db = await CreateAsync();
        string mutation = (delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") +
            " WHERE id >= 40 AND id < 44";
        await using var result = await db.ExecuteAsync($"EXPLAIN {(analyze ? "ANALYZE " : "")}{mutation}", Ct);
        List<DbValue[]> plan = await result.ToListAsync(Ct);
        DbValue[] range = Assert.Single(plan, row => !row[8].IsNull && row[8].AsText == "ordered_index");
        Assert.Equal("t", range[9].AsText);
        Assert.DoesNotContain(plan, row => row[2].AsText == "table_scan");
        if (analyze)
            Assert.Equal(4, range[5].AsInteger);
        else
            Assert.True(range[5].IsNull);
        List<DbValue[]> rows = await RowsAsync(db);
        Assert.Equal(analyze && delete ? 124 : 128, rows.Count);
        foreach (DbValue[] row in rows)
        {
            long id = row[0].AsInteger;
            Assert.Equal(id * 10 + (analyze && !delete && id >= 40 && id < 44 ? 1 : 0), row[1].AsInteger);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NonLeadingPrimaryKeyAndAlteredSchema_PreserveFullRowShape(bool delete)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (value INTEGER, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (100, 11), (200, 12), (300, 22)", Ct);
        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN extra TEXT", Ct);
        await using var changed = await db.ExecuteAsync((delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") +
            " WHERE id >= 10 AND id < 20", Ct);
        Assert.Equal(2, changed.RowsAffected);
        await using var result = await db.ExecuteAsync("SELECT * FROM t ORDER BY id", Ct);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(delete ? new long[] { 22 } : [11, 12, 22], rows.Select(row => row[1].AsInteger));
        Assert.Equal(delete ? new long[] { 300 } : [101, 201, 300], rows.Select(row => row[0].AsInteger));
        Assert.All(rows, row => Assert.True(row[2].IsNull));
    }

    private static async Task<Database> CreateAsync()
    {
        var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER, payload TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES " + string.Join(',', Enumerable.Range(0, 128)
            .Select(i => $"({i}, {i * 10}, 'payload-{i}')")), Ct);
        return db;
    }

    private static async Task<List<DbValue[]>> RowsAsync(Database db)
    {
        await using var result = await db.ExecuteAsync("SELECT id, value, payload FROM t ORDER BY id", Ct);
        return await result.ToListAsync(Ct);
    }
}
