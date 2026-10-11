using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class OrdinaryPrimaryKeyRangeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("100 <= x.id AND x.id < 104", "100,101,102,103")]
    [InlineData("x.id BETWEEN 100 AND 103", "100,101,102,103")]
    [InlineData("x.id >= 98 AND x.id >= 100 AND x.id <= 105 AND x.id < 104", "100,101,102,103")]
    [InlineData("x.id >= 252", "252,253,254,255")]
    [InlineData("x.id < 3", "0,1,2")]
    [InlineData("x.id >= 104 AND x.id < 100", "")]
    public async Task ArithmeticProjection_UsesBoundedOrdinaryRange(string predicate, string expected)
    {
        await using var db = await CreateAsync();
        long[] ids = ParseIds(expected);
        List<DbValue[]> rows = await ReadAsync(db,
            $"SELECT x.id + 1 AS next_id, x.value * 2 AS twice_value FROM t x WHERE {predicate} ORDER BY x.id LIMIT 4",
            requireRange: ids.Length > 0);
        Assert.Equal(ids.Select(id => id + 1), rows.Select(row => row[0].AsInteger));
        Assert.Equal(ids.Select(id => id * 20), rows.Select(row => row[1].AsInteger));
    }

    [Theory]
    [InlineData("x.id", "104,105")]
    [InlineData("x.id DESC", "105,104")]
    [InlineData("x.value", "105,104")]
    public async Task ResidualPredicate_IsAppliedBeforeOrderingOffsetAndLimit(string order, string expected)
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("UPDATE t SET value = 1000 - id WHERE id >= 100 AND id < 107", Ct);
        List<DbValue[]> rows = await ReadAsync(db,
            "SELECT x.id + 1 AS next_id FROM t x " +
            $"WHERE x.id >= 100 AND x.id < 107 AND x.value < 898 ORDER BY {order} LIMIT 2 OFFSET 1",
            requireRange: true);
        Assert.Equal(ParseIds(expected).Select(id => id + 1), rows.Select(row => row[0].AsInteger));
    }

    [Theory]
    [InlineData("(x.id >= 100 AND x.id < 104) OR x.id = 1", "1,100,101,102")]
    [InlineData("x.id >= 100 AND x.id < 106 AND (x.value = 1010 OR x.value = 1040)", "101,104")]
    [InlineData("x.id >= NULL AND x.id < 7", "")]
    public async Task UnconsumedPredicates_PreserveOrAndNullSemantics(string predicate, string expected)
    {
        await using var db = await CreateAsync();
        List<DbValue[]> rows = await ReadAsync(db,
            $"SELECT x.id + 1 FROM t x WHERE {predicate} ORDER BY x.id LIMIT 4");
        Assert.Equal(ParseIds(expected).Select(id => id + 1), rows.Select(row => row[0].AsInteger));
    }

    [Theory]
    [InlineData("x.id >= 2.5 AND x.id < 7")]
    [InlineData("x.id > 2 AND x.id < 4.5")]
    public async Task FractionalBounds_PreserveExactIntegerCoercionErrors(string predicate)
    {
        await using var db = await CreateAsync();
        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await ReadAsync(db, $"SELECT x.id + 1 FROM t x WHERE {predicate} ORDER BY x.id LIMIT 4"));
        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Equal(
            "Value of type Real is not valid for column '<expression>' declared as INTEGER: An exact integral value is required.",
            error.Message);
    }

    [Theory]
    [InlineData("SMALLINT", false)]
    [InlineData("INTEGER", true)]
    public async Task BackedPrimaryKeys_DoNotUsePhysicalRowIds(string type, bool composite)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync(composite
            ? $"CREATE TABLE t (id {type}, value INTEGER, PRIMARY KEY (id, value))"
            : $"CREATE TABLE t (id {type} PRIMARY KEY, value INTEGER)", Ct);
        await db.ExecuteAsync(composite
            ? "INSERT INTO t VALUES (11, 100), (11, 200), (22, 300)"
            : "INSERT INTO t VALUES (11, 100), (22, 300)", Ct);
        await using QueryResult result = await db.ExecuteAsync(
            "SELECT id + 1, value FROM t WHERE id >= 10 AND id < 20 ORDER BY value", Ct);
        Assert.DoesNotContain(Operators(result.PhysicalRootOperator), op => op is PrimaryKeyRangeScanOperator);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(composite ? new long[] { 12, 12 } : [12L], rows.Select(row => row[0].AsInteger));
        Assert.Equal(composite ? new long[] { 100, 200 } : [100L], rows.Select(row => row[1].AsInteger));
    }

    [Fact]
    public async Task ExtremeExclusiveBounds_DoNotOverflowOrIncludeEndpoints()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id BIGINT PRIMARY KEY, value INTEGER)", Ct);
        await db.ExecuteAsync($"INSERT INTO t VALUES ({long.MinValue}, 1), (-1, 2), (0, 3), ({long.MaxValue}, 4)", Ct);
        List<DbValue[]> rows = await ReadAsync(db,
            $"SELECT id, value + 1 FROM t WHERE id > {long.MinValue} AND id < {long.MaxValue} ORDER BY id");
        Assert.Equal(new long[] { -1, 0 }, rows.Select(row => row[0].AsInteger));
        Assert.Empty(await ReadAsync(db, $"SELECT id, value + 1 FROM t WHERE id > {long.MaxValue} ORDER BY id"));
        Assert.Empty(await ReadAsync(db, $"SELECT id, value + 1 FROM t WHERE id < {long.MinValue} ORDER BY id"));
        rows = await ReadAsync(db, $"SELECT id, value + 1 FROM t WHERE id >= {long.MaxValue} ORDER BY id");
        Assert.Equal(long.MaxValue, Assert.Single(rows)[0].AsInteger);
    }

    [Fact]
    public async Task JoinLocalRanges_PreserveAliasesDuplicateMatchesAndResidualPredicates()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE TABLE u (id INTEGER PRIMARY KEY, owner INTEGER, payload INTEGER)", Ct);
        await db.ExecuteAsync("INSERT INTO u VALUES (1, 100, 10), (2, 100, 20), (3, 101, 30), (4, 102, 40), (5, 103, 50)", Ct);
        List<DbValue[]> rows = await ReadAsync(db,
            "SELECT l.id + 1, r.payload * 2 FROM t l JOIN u r ON l.id = r.owner " +
            "WHERE 100 <= l.id AND l.id < 103 AND r.id >= 1 AND r.id < 4 AND r.payload < 40 ORDER BY l.id, r.id",
            requireRange: true);
        Assert.Equal(new long[] { 101, 101, 102 }, rows.Select(row => row[0].AsInteger));
        Assert.Equal(new long[] { 20, 40, 60 }, rows.Select(row => row[1].AsInteger));

        rows = await ReadAsync(db,
            "SELECT l.id + 1, r.payload * 2 FROM t l LEFT JOIN u r ON l.id = r.owner " +
            "WHERE l.id >= 103 AND l.id < 105 ORDER BY l.id", requireRange: true);
        Assert.Equal(new long[] { 104, 105 }, rows.Select(row => row[0].AsInteger));
        Assert.Equal(100, rows[0][1].AsInteger);
        Assert.True(rows[1][1].IsNull);
    }

    [Fact]
    public async Task SameNamedCte_UsesMaterializedRowsInsteadOfPersistentTableRange()
    {
        await using var db = await CreateAsync();
        await db.ExecuteAsync("CREATE TABLE u (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO u VALUES (1000, 7), (1001, 8)", Ct);
        await using QueryResult result = await db.ExecuteAsync(
            "WITH t AS (SELECT id, value FROM u) " +
            "SELECT id + 1, value * 2 FROM t WHERE id >= 1000 AND id < 1002 ORDER BY id", Ct);
        Assert.DoesNotContain(Operators(result.PhysicalRootOperator), op => op is PrimaryKeyRangeScanOperator);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(new long[] { 1001, 1002 }, rows.Select(row => row[0].AsInteger));
        Assert.Equal(new long[] { 14, 16 }, rows.Select(row => row[1].AsInteger));
    }

    [Fact]
    public async Task NullableCustomFunctionResidual_FiltersBeforeSmallLimit()
    {
        DatabaseOptions options = new DatabaseOptions().ConfigureFunctions(functions => functions.AddScalar(
            "KeepValue", 1,
            new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
            static (_, args) => DbValue.FromInteger(args[0].AsInteger >= 40 ? 1 : 0)));
        await using var db = await Database.OpenInMemoryAsync(options, Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (100, NULL), (101, 10), (102, 20), (103, NULL), (104, 40), (105, 50)", Ct);
        List<DbValue[]> rows = await ReadAsync(db,
            "SELECT id + 1, value * 2 FROM t WHERE id >= 100 AND id < 106 " +
            "AND value IS NOT NULL AND KeepValue(value) = 1 ORDER BY id LIMIT 1", requireRange: true);
        Assert.Equal(new long[] { 105, 80 }, Assert.Single(rows).Select(value => value.AsInteger));
    }

    [Theory]
    [InlineData(12, true)]
    [InlineData(20, false)]
    public async Task OrdinarySerializableRange_TracksIncludedKeysAndExcludesUpperEndpoint(int insertedKey, bool conflicts)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (15, 150), (30, 300)", Ct);
        await using var transaction = await db.BeginWriteTransactionAsync(Ct);
        await using (QueryResult result = await transaction.ExecuteAsync(
            "SELECT id + 1, value * 2 FROM t WHERE id >= 10 AND id < 20 ORDER BY id", Ct))
        {
            Assert.Contains(Operators(result.PhysicalRootOperator), op => op is PrimaryKeyRangeScanOperator);
            Assert.Equal(new long[] { 16, 300 }, Assert.Single(await result.ToListAsync(Ct)).Select(value => value.AsInteger));
        }
        await db.ExecuteAsync($"INSERT INTO t VALUES ({insertedKey}, 1)", Ct);
        if (conflicts)
            await Assert.ThrowsAsync<CSharpDbConflictException>(() => transaction.CommitAsync(Ct).AsTask());
        else
            await transaction.CommitAsync(Ct);
    }

    [Fact]
    public async Task SnapshotRange_AfterRootGrowthAndTableReplacement_KeepsOriginalSchemaAndRows()
    {
        await using var db = await CreateAsync();
        using var reader = db.CreateReaderSession();
        const string sql = "SELECT id + 1, value * 2 FROM t WHERE id >= 250 AND id < 260 ORDER BY id";
        await using (QueryResult warm = await reader.ExecuteReadAsync(sql, Ct))
            Assert.Equal(6, (await warm.ToListAsync(Ct)).Count);
        await InsertAsync(db, 256, 1024);
        await db.ExecuteAsync("DROP TABLE t", Ct);
        await db.ExecuteAsync("CREATE TABLE t (value BIGINT, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (9, 250)", Ct);

        await using (QueryResult old = await reader.ExecuteReadAsync(sql, Ct))
        {
            Assert.Contains(Operators(old.PhysicalRootOperator), op => op is PrimaryKeyRangeScanOperator);
            List<DbValue[]> rows = await old.ToListAsync(Ct);
            Assert.Equal(Enumerable.Range(251, 6).Select(i => (long)i), rows.Select(row => row[0].AsInteger));
            Assert.Equal(Enumerable.Range(250, 6).Select(i => i * 20L), rows.Select(row => row[1].AsInteger));
        }
        List<DbValue[]> latest = await ReadAsync(db, sql, requireRange: true);
        Assert.Equal(new long[] { 251, 18 }, Assert.Single(latest).Select(value => value.AsInteger));
    }

    private static async Task<Database> CreateAsync()
    {
        Database db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await InsertAsync(db, 0, 256);
        return db;
    }

    private static async Task InsertAsync(Database db, int start, int count)
    {
        await db.BeginTransactionAsync(Ct);
        var batch = db.PrepareInsertBatch("t", count);
        for (int id = start; id < start + count; id++)
            batch.AddRow([DbValue.FromInteger(id), DbValue.FromInteger(id * 10L)]);
        await batch.ExecuteAsync(Ct);
        await db.CommitAsync(Ct);
    }

    private static async Task<List<DbValue[]>> ReadAsync(Database db, string sql, bool requireRange = false)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        if (requireRange)
            Assert.Contains(Operators(result.PhysicalRootOperator), op => op is PrimaryKeyRangeScanOperator);
        return await result.ToListAsync(Ct);
    }

    private static IEnumerable<IOperator> Operators(IOperator? op)
    {
        if (op is null) yield break;
        yield return op;
        if (op is IPhysicalOperatorChildren children)
            foreach (IOperator child in children.PhysicalChildren)
                foreach (IOperator descendant in Operators(child)) yield return descendant;
        else if (op is IUnaryOperatorSource unary)
            foreach (IOperator descendant in Operators(unary.Source)) yield return descendant;
        else if (op is BatchToRowOperatorAdapter adapter && adapter.BatchSource is IOperator source)
            foreach (IOperator descendant in Operators(source)) yield return descendant;
    }

    private static long[] ParseIds(string value)
        => value.Length == 0 ? [] : value.Split(',').Select(long.Parse).ToArray();
}
