using System.Reflection;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class IntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public IntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task CreateTable_And_Query()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM users", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task Select_WithWhere()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT, price REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 9.99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 29.99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 4.99)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM items WHERE price > 5.0", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Insert_SimpleFastPath_WithEscapedStringAndSemicolon()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE fast_insert (id INTEGER PRIMARY KEY, name TEXT, price REAL, note TEXT)",
            TestContext.Current.CancellationToken);
        await _db.ExecuteAsync(
            "INSERT INTO fast_insert VALUES (1, 'O''Reilly', 9.5, NULL);",
            TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM fast_insert WHERE id = 1",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("O'Reilly", rows[0][1].AsText);
        Assert.Equal(9.5, rows[0][2].AsReal);
        Assert.True(rows[0][3].IsNull);
    }

    [Fact]
    public async Task Insert_SimpleFastPath_WithMultipleValueRows()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE fast_insert_many (id INTEGER PRIMARY KEY, name TEXT, price REAL, note TEXT)",
            TestContext.Current.CancellationToken);

        var result = await _db.ExecuteAsync(
            "INSERT INTO fast_insert_many VALUES (1, 'Alice', 1.5, NULL), (2, 'O''Reilly', -2.5, 'note')",
            TestContext.Current.CancellationToken);

        Assert.Equal(2, result.RowsAffected);

        await using var query = await _db.ExecuteAsync(
            "SELECT * FROM fast_insert_many ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await query.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal(1.5, rows[0][2].AsReal);
        Assert.True(rows[0][3].IsNull);
        Assert.Equal(2, rows[1][0].AsInteger);
        Assert.Equal("O'Reilly", rows[1][1].AsText);
        Assert.Equal(-2.5, rows[1][2].AsReal);
        Assert.Equal("note", rows[1][3].AsText);
    }

    [Fact]
    public async Task Select_WithLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (val INTEGER)", TestContext.Current.CancellationToken);
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i})", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums LIMIT 3", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task Update_Rows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'old')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'old')", TestContext.Current.CancellationToken);

        var updateResult = await _db.ExecuteAsync("UPDATE t SET val = 'new' WHERE id = 1", TestContext.Current.CancellationToken);
        Assert.Equal(1, updateResult.RowsAffected);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 'new'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Delete_Rows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Charlie')", TestContext.Current.CancellationToken);

        var deleteResult = await _db.ExecuteAsync("DELETE FROM t WHERE id = 2", TestContext.Current.CancellationToken);
        Assert.Equal(1, deleteResult.RowsAffected);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task DropTable()
    {
        await _db.ExecuteAsync("CREATE TABLE temp (x INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DROP TABLE temp", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CSharpDbException>(async () => await _db.ExecuteAsync("SELECT * FROM temp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Persistence_AcrossReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE persist (id INTEGER, data TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO persist VALUES (1, 'hello')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO persist VALUES (2, 'world')", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM persist", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }

    [Fact]
    public async Task Persistence_PrimaryKeyLookupManyRows_AcrossReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE persist_big (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        for (int i = 0; i < 5000; i++)
            await _db.ExecuteAsync($"INSERT INTO persist_big VALUES ({i}, {i * 10})", TestContext.Current.CancellationToken);
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT val FROM persist_big WHERE id = 4999", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(49990, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Persistence_ColumnListInsertManyRows_AcrossReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE persist_cols (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        for (int i = 0; i < 5000; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO persist_cols (id, val) VALUES ({i}, {i * 10})",
                TestContext.Current.CancellationToken);
        }
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM persist_cols WHERE id = 4999",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(49990, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task PreparedInsertBatch_ReusesBuffersAcrossExecutions()
    {
        await _db.ExecuteAsync("CREATE TABLE batch_rows (id INTEGER PRIMARY KEY, val INTEGER, text_col TEXT)", TestContext.Current.CancellationToken);

        var batch = _db.PrepareInsertBatch("batch_rows", initialCapacity: 2);
        batch.AddRow(DbValue.FromInteger(1), DbValue.FromInteger(10), DbValue.FromText("one"));
        batch.AddRow(DbValue.FromInteger(2), DbValue.FromInteger(20), DbValue.FromText("two"));
        Assert.Equal(2, await batch.ExecuteAsync(TestContext.Current.CancellationToken));

        batch.AddRow(DbValue.FromInteger(3), DbValue.FromInteger(30), DbValue.FromText("three"));
        batch.AddRow(DbValue.FromInteger(4), DbValue.FromInteger(40), DbValue.FromText("four"));
        Assert.Equal(2, await batch.ExecuteAsync(TestContext.Current.CancellationToken));

        await using var result = await _db.ExecuteAsync(
            "SELECT text_col FROM batch_rows WHERE id >= 3 ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("three", rows[0][0].AsText);
        Assert.Equal("four", rows[1][0].AsText);
    }

    [Fact]
    public async Task PreparedInsertBatch_AllowsIdentityPrimaryKeyGeneration()
    {
        await _db.ExecuteAsync("CREATE TABLE batch_identity (id INTEGER PRIMARY KEY IDENTITY, name TEXT)", TestContext.Current.CancellationToken);

        var batch = _db.PrepareInsertBatch("batch_identity", initialCapacity: 2);
        batch.AddRow(DbValue.Null, DbValue.FromText("alice"));
        batch.AddRow(DbValue.Null, DbValue.FromText("bob"));

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, await batch.ExecuteAsync(TestContext.Current.CancellationToken));
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT id, name FROM batch_identity ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("alice", rows[0][1].AsText);
        Assert.Equal(2, rows[1][0].AsInteger);
        Assert.Equal("bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task PreparedInsertBatch_InvalidatedBySchemaChange()
    {
        await _db.ExecuteAsync("CREATE TABLE batch_schema (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);

        var batch = _db.PrepareInsertBatch("batch_schema", initialCapacity: 1);
        await _db.ExecuteAsync("ALTER TABLE batch_schema ADD COLUMN text_col TEXT", TestContext.Current.CancellationToken);

        var ex = Assert.Throws<InvalidOperationException>(
            () => batch.AddRow(DbValue.FromInteger(1), DbValue.FromInteger(2)));
        Assert.Contains("schema changed", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Transaction_Rollback()
    {
        await _db.ExecuteAsync("CREATE TABLE tx (id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tx VALUES (1)", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tx VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tx VALUES (3)", TestContext.Current.CancellationToken);
        await _db.RollbackAsync(TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM tx", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows); // only row 1 should remain
    }

    [Fact]
    public async Task Select_OrderBy()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (3, 'Charlie')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (2, 'Bob')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM sorted ORDER BY name ASC", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
        Assert.Equal("Charlie", rows[2][1].AsText);
    }

    [Fact]
    public async Task Select_OrderByIndexedColumn_WithLimitAndOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_indexed (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_indexed_val ON sorted_indexed(val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (1, 50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (2, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (3, 40)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (4, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (5, 60)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (6, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_indexed ORDER BY val ASC LIMIT 3 OFFSET 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(30L, rows[0][0].AsInteger);
        Assert.Equal(40L, rows[1][0].AsInteger);
        Assert.Equal(50L, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderByIndexedColumn_WithBetweenFilter()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_between_idx (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_between_idx_val ON sorted_between_idx(val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (1, 50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (2, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (3, 40)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (4, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (5, 60)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_between_idx VALUES (6, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_between_idx WHERE val BETWEEN 20 AND 50 ORDER BY val ASC LIMIT 10",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Count);
        Assert.Equal(20L, rows[0][0].AsInteger);
        Assert.Equal(30L, rows[1][0].AsInteger);
        Assert.Equal(40L, rows[2][0].AsInteger);
        Assert.Equal(50L, rows[3][0].AsInteger);
    }

    [Fact]
    public async Task Select_IndexedColumn_NotBetween_PreservesSemantics()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_not_between_idx (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_not_between_idx_val ON sorted_not_between_idx(val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (1, 50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (2, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (3, 40)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (4, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (5, 60)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_not_between_idx VALUES (6, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_not_between_idx WHERE val NOT BETWEEN 20 AND 40 ORDER BY val ASC",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.Equal(50L, rows[1][0].AsInteger);
        Assert.Equal(60L, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderByIndexedNullableColumn_IncludesNullRows()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_nullable_idx (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_nullable_idx_val ON sorted_nullable_idx(val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (1, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (2, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (3, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (4, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_nullable_idx ORDER BY val ASC", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(4, rows.Count);
        Assert.True(rows[0][0].IsNull);
        Assert.Equal(10L, rows[1][0].AsInteger);
        Assert.Equal(20L, rows[2][0].AsInteger);
        Assert.Equal(30L, rows[3][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderBy_WithLimitAndOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_nums (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (1, 50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (2, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (3, 40)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (4, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (5, 60)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (6, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_nums ORDER BY val ASC LIMIT 3 OFFSET 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(30L, rows[0][0].AsInteger);
        Assert.Equal(40L, rows[1][0].AsInteger);
        Assert.Equal(50L, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderBy_WithLimit_SelectStarPreservesAllColumns()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE sorted_star (id INTEGER PRIMARY KEY, val INTEGER, name TEXT, category TEXT)",
            TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_star VALUES (1, 30, 'thirty', 'A')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_star VALUES (2, 10, 'ten', 'B')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_star VALUES (3, 20, 'twenty', 'C')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM sorted_star ORDER BY val ASC LIMIT 2",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(4, rows[0].Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal("ten", rows[0][2].AsText);
        Assert.Equal("B", rows[0][3].AsText);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(20L, rows[1][1].AsInteger);
        Assert.Equal("twenty", rows[1][2].AsText);
        Assert.Equal("C", rows[1][3].AsText);
    }

    [Fact]
    public async Task Select_OrderByExpression_WithLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_expr (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (2, 40)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (3, 15)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (4, 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (5, 1)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM sorted_expr ORDER BY val + id DESC LIMIT 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(4L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task NullValues()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (1, NULL)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.True(rows[0][1].IsNull);
    }

    [Fact]
    public async Task Select_WithOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (val INTEGER)", TestContext.Current.CancellationToken);
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i})", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums LIMIT 3 OFFSET 5", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(6, rows[0][0].AsInteger);
        Assert.Equal(7, rows[1][0].AsInteger);
        Assert.Equal(8, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_WithOffset_BeyondRows()
    {
        await _db.ExecuteAsync("CREATE TABLE small (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO small VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO small VALUES (2)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM small LIMIT 10 OFFSET 5", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Select_OffsetWithoutLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE nums2 (val INTEGER)", TestContext.Current.CancellationToken);
        for (int i = 1; i <= 5; i++)
            await _db.ExecuteAsync($"INSERT INTO nums2 VALUES ({i})", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums2 OFFSET 3", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(4, rows[0][0].AsInteger);
        Assert.Equal(5, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task MultipleInserts_ManyRows()
    {
        await _db.ExecuteAsync("CREATE TABLE big (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        for (int i = 0; i < 100; i++)
            await _db.ExecuteAsync($"INSERT INTO big VALUES ({i}, {i * 10})", TestContext.Current.CancellationToken);
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM big", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(100, rows.Count);
    }

    [Fact]
    public async Task PrimaryKeyLookup_ManyRows()
    {
        await _db.ExecuteAsync("CREATE TABLE bigpk (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        for (int i = 0; i < 5000; i++)
            await _db.ExecuteAsync($"INSERT INTO bigpk VALUES ({i}, {i * 10})", TestContext.Current.CancellationToken);
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT val FROM bigpk WHERE id = 4999", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(49990, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task PrimaryKeyLookup_ProjectOnlyPrimaryKeyColumns_RemainsCorrect()
    {
        await _db.ExecuteAsync("CREATE TABLE pk_projection (id INTEGER PRIMARY KEY, payload TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO pk_projection VALUES (7, 'x')", TestContext.Current.CancellationToken);

        await using var hitResult = await _db.ExecuteAsync(
            "SELECT id, id AS id_alias FROM pk_projection WHERE id = 7", TestContext.Current.CancellationToken);
        var hitRows = await hitResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(hitRows);
        Assert.Equal(7L, hitRows[0][0].AsInteger);
        Assert.Equal(7L, hitRows[0][1].AsInteger);

        await using var missResult = await _db.ExecuteAsync(
            "SELECT id, id AS id_alias FROM pk_projection WHERE id = 8", TestContext.Current.CancellationToken);
        var missRows = await missResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(missRows);
    }

    [Fact]
    public async Task LookupConjunct_WithResidualFilter_RemainsCorrect()
    {
        await _db.ExecuteAsync("CREATE TABLE lookup_residual (id INTEGER PRIMARY KEY, code INTEGER, state TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_lookup_residual_code ON lookup_residual(code)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO lookup_residual VALUES (1, 100, 'active')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO lookup_residual VALUES (2, 200, 'inactive')", TestContext.Current.CancellationToken);

        await using var hitResult = await _db.ExecuteAsync(
            "SELECT id FROM lookup_residual WHERE code = 100 AND state = 'active'", TestContext.Current.CancellationToken);
        var hitRows = await hitResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(hitRows);
        Assert.Equal(1L, hitRows[0][0].AsInteger);

        await using var missResult = await _db.ExecuteAsync(
            "SELECT id FROM lookup_residual WHERE code = 100 AND state = 'inactive'", TestContext.Current.CancellationToken);
        var missRows = await missResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(missRows);
    }

    [Fact]
    public async Task PrimaryKeyLookup_WithResidualFilter_RemainsCorrect()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE pk_lookup_residual (id INTEGER PRIMARY KEY, code INTEGER, state TEXT)",
            TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO pk_lookup_residual VALUES (1, 100, 'active')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO pk_lookup_residual VALUES (2, 200, 'inactive')", TestContext.Current.CancellationToken);

        await using var hitResult = await _db.ExecuteAsync(
            "SELECT id FROM pk_lookup_residual WHERE id = 1 AND state = 'active'",
            TestContext.Current.CancellationToken);
        var hitRows = await hitResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(hitRows);
        Assert.Equal(1L, hitRows[0][0].AsInteger);

        await using var starResult = await _db.ExecuteAsync(
            "SELECT * FROM pk_lookup_residual WHERE id = 1 AND state = 'active'",
            TestContext.Current.CancellationToken);
        var starRows = await starResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(starRows);
        Assert.Equal(1L, starRows[0][0].AsInteger);
        Assert.Equal(100L, starRows[0][1].AsInteger);
        Assert.Equal("active", starRows[0][2].AsText);

        await using var missResult = await _db.ExecuteAsync(
            "SELECT id FROM pk_lookup_residual WHERE id = 1 AND state = 'inactive'",
            TestContext.Current.CancellationToken);
        var missRows = await missResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(missRows);
    }

    [Fact]
    public async Task SelectProjection_UsesIndexOrder_WithCorrectRows()
    {
        await _db.ExecuteAsync("CREATE TABLE ordered_lookup (id INTEGER PRIMARY KEY, sort_key INTEGER, payload TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_ordered_lookup_sort_key ON ordered_lookup(sort_key)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (1, 30, 'c')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (2, 10, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (3, 20, 'b')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM ordered_lookup ORDER BY sort_key LIMIT 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task SelectProjection_UsesCoveredIndexOrder_WithoutLimit()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE ordered_cover (id INTEGER PRIMARY KEY, sort_key INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_ordered_cover_sort_key ON ordered_cover(sort_key)", ct);
        await _db.ExecuteAsync("INSERT INTO ordered_cover VALUES (1, 30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO ordered_cover VALUES (2, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO ordered_cover VALUES (3, 20, 'b')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT id, sort_key FROM ordered_cover ORDER BY sort_key") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexOrderedProjectionScanOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var pairs = rows.Select(row => (Id: row[0].AsInteger, SortKey: row[1].AsInteger)).ToArray();
        Assert.Equal([(2L, 10L), (3L, 20L), (1L, 30L)], pairs);
    }

    [Fact]
    public async Task SelectProjection_UsesCoveredIntegerRangeScan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE range_cover (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_range_cover_score ON range_cover(score)", ct);
        await _db.ExecuteAsync("INSERT INTO range_cover VALUES (1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO range_cover VALUES (2, 20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO range_cover VALUES (3, 30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO range_cover VALUES (4, 40, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO range_cover VALUES (5, 50, 'e')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT id, score FROM range_cover WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexOrderedProjectionScanOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var pairs = rows
            .Select(row => (Id: row[0].AsInteger, Score: row[1].AsInteger))
            .OrderBy(pair => pair.Score)
            .ToArray();
        Assert.Equal([(2L, 20L), (3L, 30L), (4L, 40L)], pairs);
    }

    #region LIKE / IN / BETWEEN / IS NULL

    [Fact]
    public async Task Like_PercentWildcard()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'Apple')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (2, 'Banana')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (3, 'Apricot')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM products WHERE name LIKE 'Ap%'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Like_UnderscoreWildcard()
    {
        await _db.ExecuteAsync("CREATE TABLE codes (id INTEGER, code TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO codes VALUES (1, 'A1')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO codes VALUES (2, 'A2')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO codes VALUES (3, 'AB')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO codes VALUES (4, 'B1')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM codes WHERE code LIKE 'A_'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count); // A1, A2, AB
    }

    [Fact]
    public async Task Like_CaseInsensitive()
    {
        await _db.ExecuteAsync("CREATE TABLE names (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (2, 'ALICE')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (3, 'Bob')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM names WHERE name LIKE 'alice'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task NotLike()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'foo')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'bar')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'foobar')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM items WHERE name NOT LIKE 'foo%'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("bar", rows[0][1].AsText);
    }

    [Fact]
    public async Task Like_WithEscape()
    {
        await _db.ExecuteAsync("CREATE TABLE paths (id INTEGER, path TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO paths VALUES (1, '100%')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO paths VALUES (2, '100 items')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO paths VALUES (3, '50%')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM paths WHERE path LIKE '%!%' ESCAPE '!'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // 100% and 50%
    }

    [Fact]
    public async Task In_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);
        for (int i = 1; i <= 5; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i}, {i * 10})", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums WHERE id IN (1, 3, 5)", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task In_Strings()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER, status TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'active')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'inactive')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'pending')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM users WHERE status IN ('active', 'pending')", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task NotIn()
    {
        await _db.ExecuteAsync("CREATE TABLE tags (id INTEGER, tag TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (2, 'b')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (3, 'c')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM tags WHERE tag NOT IN ('a', 'b')", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("c", rows[0][1].AsText);
    }

    [Fact]
    public async Task Between_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE ages (id INTEGER, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ages VALUES (1, 15)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ages VALUES (2, 18)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ages VALUES (3, 25)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ages VALUES (4, 65)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO ages VALUES (5, 70)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM ages WHERE age BETWEEN 18 AND 65", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count); // 18, 25, 65 (inclusive)
    }

    [Fact]
    public async Task Between_Reals()
    {
        await _db.ExecuteAsync("CREATE TABLE prices (id INTEGER, price REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (1, 5.99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (2, 10.00)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (3, 15.50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (4, 20.00)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM prices WHERE price BETWEEN 10.0 AND 20.0", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count); // 10.00, 15.50, 20.00
    }

    [Fact]
    public async Task NotBetween()
    {
        await _db.ExecuteAsync("CREATE TABLE vals (id INTEGER, v INTEGER)", TestContext.Current.CancellationToken);
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO vals VALUES ({i}, {i})", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM vals WHERE v NOT BETWEEN 3 AND 7", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(5, rows.Count); // 1,2,8,9,10
    }

    [Fact]
    public async Task IsNull()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (1, 'hello')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (2, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (3, NULL)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable WHERE val IS NULL", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task IsNotNull()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable2 (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (1, 'hello')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (2, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (3, 'world')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable2 WHERE val IS NOT NULL", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Like_NullOperand_ReturnsNoRows()
    {
        await _db.ExecuteAsync("CREATE TABLE nulllike (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nulllike VALUES (1, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nulllike VALUES (2, 'abc')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM nulllike WHERE val LIKE '%a%'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(2, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task TextFunction_AllowsLikeFilteringAcrossValueTypes()
    {
        await _db.ExecuteAsync("CREATE TABLE mixed_filter (id INTEGER, code INTEGER, note TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO mixed_filter VALUES (1, 5, 'alpha')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO mixed_filter VALUES (2, 15, 'bravo')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO mixed_filter VALUES (3, NULL, 'charlie')", TestContext.Current.CancellationToken);

        await using var numericResult = await _db.ExecuteAsync(
            "SELECT id FROM mixed_filter WHERE TEXT(code) LIKE '%15%' ORDER BY id",
            TestContext.Current.CancellationToken);
        var numericRows = await numericResult.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(numericRows);
        Assert.Equal(2L, numericRows[0][0].AsInteger);

        await using var nullResult = await _db.ExecuteAsync(
            "SELECT id FROM mixed_filter WHERE TEXT(code) LIKE '%NULL%' ORDER BY id",
            TestContext.Current.CancellationToken);
        var nullRows = await nullResult.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(nullRows);
        Assert.Equal(3L, nullRows[0][0].AsInteger);
    }

    [Fact]
    public async Task In_WithNull()
    {
        await _db.ExecuteAsync("CREATE TABLE innull (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO innull VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO innull VALUES (2, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO innull VALUES (3, 20)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM innull WHERE val IN (10, 20)", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // NULL row excluded
    }

    #endregion

    #region Aggregate Functions + GROUP BY + HAVING

    [Fact]
    public async Task CountStar()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'b')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'c')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM items", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(3, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task GenericScalarAggregate_UsesScalarAggregateOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_scalar_agg (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scalar_agg VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scalar_agg VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scalar_agg VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT AVG(score) FROM batch_scalar_agg") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        // The planner now uses the specialized ScalarAggregateTableOperator fast path
        // for single-aggregate queries over a simple table scan.
        Assert.IsType<ScalarAggregateTableOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(20d, rows[0][0].AsReal);
    }

    [Fact]
    public async Task TextFunction_CanWrapAggregateOutput()
    {
        await _db.ExecuteAsync("CREATE TABLE agg_text (id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO agg_text VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO agg_text VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO agg_text VALUES (3)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT TEXT(COUNT(*)) FROM agg_text",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);
        Assert.Equal("3", rows[0][0].AsText);
    }

    [Fact]
    public async Task CountStar_RemainsCorrectAfterMutations()
    {
        await _db.ExecuteAsync("CREATE TABLE count_mut (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (2, 'b')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (3, 'c')", TestContext.Current.CancellationToken);

        async Task<long> CountAsync()
        {
            await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM count_mut");
            var rows = await result.ToListAsync();
            return rows[0][0].AsInteger;
        }

        Assert.Equal(3, await CountAsync());

        await _db.ExecuteAsync("DELETE FROM count_mut WHERE id = 2", TestContext.Current.CancellationToken);
        Assert.Equal(2, await CountAsync());

        await _db.ExecuteAsync("UPDATE count_mut SET id = 30 WHERE id = 3", TestContext.Current.CancellationToken);
        Assert.Equal(2, await CountAsync());

        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (40, 'd')", TestContext.Current.CancellationToken);
        Assert.Equal(3, await CountAsync());
    }

    [Fact]
    public async Task CountColumn_IgnoresNull()
    {
        await _db.ExecuteAsync("CREATE TABLE vals (id INTEGER, v TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO vals VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO vals VALUES (2, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO vals VALUES (3, 'b')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(v) FROM vals", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task GenericGroupedAggregate_UsesHashAggregateOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_agg (id INTEGER PRIMARY KEY, category TEXT NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_agg VALUES (1, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_agg VALUES (2, 'A', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_agg VALUES (3, 'B', 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, COUNT(*), AVG(score) FROM batch_hash_agg GROUP BY category") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][1].AsInteger);
        Assert.Equal(15d, rows[0][2].AsReal);
        Assert.Equal(1L, rows[1][1].AsInteger);
        Assert.Equal(30d, rows[1][2].AsReal);
    }

    [Fact]
    public async Task GenericGroupedDistinctAggregate_UsesHashAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_distinct_group (id INTEGER PRIMARY KEY, category TEXT NOT NULL, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group VALUES (1, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group VALUES (2, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group VALUES (3, 'A', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group VALUES (4, 'B', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group VALUES (5, 'B', NULL)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, COUNT(DISTINCT score), AVG(DISTINCT score) FROM batch_hash_distinct_group GROUP BY category") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("A", rows[0][0].AsText);
        Assert.Equal(2L, rows[0][1].AsInteger);
        Assert.Equal(15d, rows[0][2].AsReal);
        Assert.Equal("B", rows[1][0].AsText);
        Assert.Equal(1L, rows[1][1].AsInteger);
        Assert.Equal(30d, rows[1][2].AsReal);
    }

    [Fact]
    public async Task FilteredGroupedDistinctAggregate_UsesHashAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_distinct_group_filter (id INTEGER PRIMARY KEY, category TEXT NOT NULL, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (1, 'A', 5)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (2, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (3, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (4, 'A', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (5, 'B', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (6, 'B', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_distinct_group_filter VALUES (7, 'B', 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, COUNT(DISTINCT score), AVG(DISTINCT score) " +
            "FROM batch_hash_distinct_group_filter " +
            "WHERE score BETWEEN 10 AND 30 " +
            "GROUP BY category") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("A", rows[0][0].AsText);
        Assert.Equal(2L, rows[0][1].AsInteger);
        Assert.Equal(15d, rows[0][2].AsReal);
        Assert.Equal("B", rows[1][0].AsText);
        Assert.Equal(2L, rows[1][1].AsInteger);
        Assert.Equal(20d, rows[1][2].AsReal);
    }

    [Fact]
    public async Task CompositeGroupedAggregate_UsesHashAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_composite_group (id INTEGER PRIMARY KEY, category TEXT NOT NULL, bucket INTEGER NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group VALUES (1, 'A', 1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group VALUES (2, 'A', 1, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group VALUES (3, 'A', 2, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group VALUES (4, 'B', 1, 40)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group VALUES (5, 'B', 1, 50)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, bucket, COUNT(*), SUM(score), AVG(score) " +
            "FROM batch_hash_composite_group GROUP BY category, bucket") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct))
            .OrderBy(row => row[0].AsText)
            .ThenBy(row => row[1].AsInteger)
            .ToArray();

        Assert.Equal(3, rows.Length);

        Assert.Equal("A", rows[0][0].AsText);
        Assert.Equal(1L, rows[0][1].AsInteger);
        Assert.Equal(2L, rows[0][2].AsInteger);
        Assert.Equal(30L, rows[0][3].AsInteger);
        Assert.Equal(15d, rows[0][4].AsReal);

        Assert.Equal("A", rows[1][0].AsText);
        Assert.Equal(2L, rows[1][1].AsInteger);
        Assert.Equal(1L, rows[1][2].AsInteger);
        Assert.Equal(30L, rows[1][3].AsInteger);
        Assert.Equal(30d, rows[1][4].AsReal);

        Assert.Equal("B", rows[2][0].AsText);
        Assert.Equal(1L, rows[2][1].AsInteger);
        Assert.Equal(2L, rows[2][2].AsInteger);
        Assert.Equal(90L, rows[2][3].AsInteger);
        Assert.Equal(45d, rows[2][4].AsReal);
    }

    [Fact]
    public async Task FilteredCompositeGroupedAggregate_UsesHashAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_composite_group_filter (id INTEGER PRIMARY KEY, category TEXT NOT NULL, bucket INTEGER NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (1, 'A', 1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (2, 'A', 1, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (3, 'A', 2, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (4, 'B', 1, 40)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (5, 'B', 2, 50)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_composite_group_filter VALUES (6, 'B', 3, 60)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, bucket, COUNT(*), SUM(score), AVG(score) " +
            "FROM batch_hash_composite_group_filter " +
            "WHERE bucket BETWEEN 1 AND 2 " +
            "GROUP BY category, bucket") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct))
            .OrderBy(row => row[0].AsText)
            .ThenBy(row => row[1].AsInteger)
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal("A", rows[0][0].AsText);
        Assert.Equal(1L, rows[0][1].AsInteger);
        Assert.Equal(2L, rows[0][2].AsInteger);
        Assert.Equal(30L, rows[0][3].AsInteger);
        Assert.Equal(15d, rows[0][4].AsReal);

        Assert.Equal("A", rows[1][0].AsText);
        Assert.Equal(2L, rows[1][1].AsInteger);
        Assert.Equal(1L, rows[1][2].AsInteger);
        Assert.Equal(30L, rows[1][3].AsInteger);
        Assert.Equal(30d, rows[1][4].AsReal);

        Assert.Equal("B", rows[2][0].AsText);
        Assert.Equal(1L, rows[2][1].AsInteger);
        Assert.Equal(1L, rows[2][2].AsInteger);
        Assert.Equal(40L, rows[2][3].AsInteger);
        Assert.Equal(40d, rows[2][4].AsReal);

        Assert.Equal("B", rows[3][0].AsText);
        Assert.Equal(2L, rows[3][1].AsInteger);
        Assert.Equal(1L, rows[3][2].AsInteger);
        Assert.Equal(50L, rows[3][3].AsInteger);
        Assert.Equal(50d, rows[3][4].AsReal);
    }

    [Fact]
    public async Task GroupedAggregateWithHaving_UsesHashAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hash_having_group (id INTEGER PRIMARY KEY, category TEXT NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_having_group VALUES (1, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_having_group VALUES (2, 'A', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_having_group VALUES (3, 'B', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_having_group VALUES (4, 'B', 40)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hash_having_group VALUES (5, 'C', 5)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT category, COUNT(*), SUM(score), AVG(score) " +
            "FROM batch_hash_having_group " +
            "GROUP BY category HAVING AVG(score) >= 25") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Single(rows);
        Assert.Equal("B", rows[0][0].AsText);
        Assert.Equal(2L, rows[0][1].AsInteger);
        Assert.Equal(70L, rows[0][2].AsInteger);
        Assert.Equal(35d, rows[0][3].AsReal);
    }

    [Fact]
    public async Task Sum_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT SUM(val) FROM nums", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(60, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Sum_Reals()
    {
        await _db.ExecuteAsync("CREATE TABLE prices (id INTEGER, price REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (1, 9.99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO prices VALUES (2, 19.99)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT SUM(price) FROM prices", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.True(Math.Abs(rows[0][0].AsReal - 29.98) < 0.001);
    }

    [Fact]
    public async Task Avg()
    {
        await _db.ExecuteAsync("CREATE TABLE scores (id INTEGER, score INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scores VALUES (1, 80)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scores VALUES (2, 90)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scores VALUES (3, 100)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT AVG(score) FROM scores", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(90.0, rows[0][0].AsReal);
    }

    [Fact]
    public async Task Min_Max()
    {
        await _db.ExecuteAsync("CREATE TABLE temps (id INTEGER, temp REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO temps VALUES (1, 22.5)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO temps VALUES (2, 18.0)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO temps VALUES (3, 31.2)", TestContext.Current.CancellationToken);

        await using var minResult = await _db.ExecuteAsync("SELECT MIN(temp) FROM temps", TestContext.Current.CancellationToken);
        var minRows = await minResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.True(Math.Abs(minRows[0][0].AsReal - 18.0) < 0.001);

        await using var maxResult = await _db.ExecuteAsync("SELECT MAX(temp) FROM temps", TestContext.Current.CancellationToken);
        var maxRows = await maxResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.True(Math.Abs(maxRows[0][0].AsReal - 31.2) < 0.001);
    }

    [Fact]
    public async Task IndexedIntegerMin_UsesIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE temps_idx (id INTEGER PRIMARY KEY, temp INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO temps_idx VALUES (1, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO temps_idx VALUES (2, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO temps_idx VALUES (3, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO temps_idx VALUES (4, 20)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_temps_idx_temp ON temps_idx(temp)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT MIN(temp) FROM temps_idx") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(10L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedIntegerMaxRange_UsesIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE scores_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO scores_idx VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO scores_idx VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO scores_idx VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO scores_idx VALUES (4, 40)", ct);
        await _db.ExecuteAsync("INSERT INTO scores_idx VALUES (5, 50)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_scores_idx_score ON scores_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT MAX(score) FROM scores_idx WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(40L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedIntegerMaxExclusiveUpperBound_UsesIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE scores_idx_exclusive (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        for (int i = 1; i <= 2000; i++)
            await _db.ExecuteAsync($"INSERT INTO scores_idx_exclusive VALUES ({i}, {i})", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_scores_idx_exclusive_score ON scores_idx_exclusive(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT MAX(score) FROM scores_idx_exclusive WHERE score < 1501") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(1500L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedIntegerCountColumn_UsesIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE counts_idx (id INTEGER PRIMARY KEY, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO counts_idx VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO counts_idx VALUES (2, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO counts_idx VALUES (3, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO counts_idx VALUES (4, NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_counts_idx_score ON counts_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(score) FROM counts_idx") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedIntegerCountStarRange_UsesIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE count_range_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_idx VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_idx VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_idx VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_idx VALUES (4, 40)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_idx VALUES (5, 50)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_count_range_idx_score ON count_range_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM count_range_idx WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task FilteredScalarAggregateWithoutIndex_UsesFilteredTableAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE count_range_scan (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_scan VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_scan VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_scan VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_scan VALUES (4, 40)", ct);
        await _db.ExecuteAsync("INSERT INTO count_range_scan VALUES (5, 50)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM count_range_scan WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task FilteredScalarAggregateWithoutIndex_UsesCompactBatchPlanLayout()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE sum_range_scan (id INTEGER PRIMARY KEY, category TEXT, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_range_scan VALUES (1, 'A', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_range_scan VALUES (2, 'B', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_range_scan VALUES (3, 'C', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_range_scan VALUES (4, 'D', 40)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_range_scan VALUES (5, 'E', 50)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(score) FROM sum_range_scan WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 2 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(rootOperator, "_columnIndex"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(90L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task FilteredScalarExpressionAggregateWithoutIndex_UsesBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE sum_expr_scan (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_expr_scan VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_expr_scan VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_expr_scan VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO sum_expr_scan VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id + score) FROM sum_expr_scan WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0, 1 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(-1, GetPrivateField<int>(rootOperator, "_columnIndex"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(99L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task OrFilteredCountStarOnScan_UsesSpecializedBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE count_or_scan (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO count_or_scan VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO count_or_scan VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO count_or_scan VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO count_or_scan VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM count_or_scan WHERE score = 10 OR score = 30") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task NotOrFilteredCountStarOnScan_UsesSpecializedBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE count_not_or_scan (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO count_not_or_scan VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO count_not_or_scan VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO count_not_or_scan VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO count_not_or_scan VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM count_not_or_scan WHERE NOT (score = 10 OR score = 30)") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedTextCountStar_UsesFilteredPayloadAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_count_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO payload_count_idx VALUES (1, 10, 'Alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_count_idx VALUES (2, 20, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_count_idx VALUES (3, 30, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_count_idx VALUES (4, 40, 'Gamma')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_count_idx_category ON payload_count_idx(category)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM payload_count_idx WHERE category = 'Beta'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedRangeScalarAggregateOtherColumn_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_sum_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 25_000; i++)
        {
            char category = (char)('A' + (i % 5));
            await _db.ExecuteAsync($"INSERT INTO payload_sum_idx VALUES ({i}, {i}, '{category}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_sum_idx_score ON payload_sum_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id) FROM payload_sum_idx WHERE score BETWEEN 20000 AND 20010 AND id >= 20005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(120045L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedRangeAvgAggregateOtherColumn_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_avg_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 25_000; i++)
        {
            char category = (char)('A' + (i % 5));
            await _db.ExecuteAsync($"INSERT INTO payload_avg_idx VALUES ({i}, {i}, '{category}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_avg_idx_score ON payload_avg_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT AVG(id) FROM payload_avg_idx WHERE score BETWEEN 20000 AND 20010 AND id >= 20005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(20_007.5d, rows[0][0].AsReal);
    }

    [Fact]
    public async Task IndexedRangeScalarExpressionAggregate_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_sum_expr_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 25_000; i++)
        {
            char category = (char)('A' + (i % 5));
            await _db.ExecuteAsync($"INSERT INTO payload_sum_expr_idx VALUES ({i}, {i}, '{category}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_sum_expr_idx_score ON payload_sum_expr_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id + 1) FROM payload_sum_expr_idx WHERE score BETWEEN 20000 AND 20010 AND id >= 20005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(-1, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(120_051L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task SmallIndexedRangeScalarAggregateOtherColumn_FallsBackToFilteredTableAggregate()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_sum_small_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO payload_sum_small_idx VALUES (1, 10, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_sum_small_idx VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_sum_small_idx VALUES (3, 30, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_sum_small_idx VALUES (4, 40, 'D')", ct);
        await _db.ExecuteAsync("INSERT INTO payload_sum_small_idx VALUES (5, 50, 'E')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_sum_small_idx_score ON payload_sum_small_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id) FROM payload_sum_small_idx WHERE score BETWEEN 20 AND 50 AND id >= 3") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(12L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task AnalyzedSelectiveSmallIndexedRangeScalarAggregateOtherColumn_UsesFilteredPayloadAggregate()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_sum_selective_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 10_000; i++)
        {
            await _db.ExecuteAsync($"INSERT INTO payload_sum_selective_idx VALUES ({i}, {i}, 'cat_{i:D5}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_sum_selective_idx_score ON payload_sum_selective_idx(score)", ct);
        await _db.ExecuteAsync("ANALYZE payload_sum_selective_idx", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id) FROM payload_sum_selective_idx WHERE score BETWEEN 5000 AND 5005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.Null(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(30_015L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task AnalyzedSelectiveSmallIndexedRangeAvgAggregateOtherColumn_UsesFilteredPayloadAggregate()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_avg_selective_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 10_000; i++)
        {
            await _db.ExecuteAsync($"INSERT INTO payload_avg_selective_idx VALUES ({i}, {i}, 'cat_{i:D5}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_avg_selective_idx_score ON payload_avg_selective_idx(score)", ct);
        await _db.ExecuteAsync("ANALYZE payload_avg_selective_idx", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT AVG(id) FROM payload_avg_selective_idx WHERE score BETWEEN 5000 AND 5005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.Null(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(5_002.5d, rows[0][0].AsReal);
    }

    [Fact]
    public async Task AnalyzedSelectiveSmallIndexedRangeDistinctPayloadAggregate_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_distinct_selective_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 10_000; i++)
        {
            char category = (char)('A' + (i % 3));
            await _db.ExecuteAsync($"INSERT INTO payload_distinct_selective_idx VALUES ({i}, {i}, '{category}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_distinct_selective_idx_score ON payload_distinct_selective_idx(score)", ct);
        await _db.ExecuteAsync("ANALYZE payload_distinct_selective_idx", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(DISTINCT category) FROM payload_distinct_selective_idx WHERE score BETWEEN 5000 AND 5005 AND id >= 5002") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0, 2 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(1, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task AnalyzedSelectiveSmallIndexedRangeDistinctAvgPayloadAggregate_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_distinct_avg_selective_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, metric INTEGER)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 10_000; i++)
        {
            int metric = (i % 4) * 10;
            await _db.ExecuteAsync($"INSERT INTO payload_distinct_avg_selective_idx VALUES ({i}, {i}, {metric})", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_distinct_avg_selective_idx_score ON payload_distinct_avg_selective_idx(score)", ct);
        await _db.ExecuteAsync("ANALYZE payload_distinct_avg_selective_idx", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT AVG(DISTINCT metric) FROM payload_distinct_avg_selective_idx WHERE score BETWEEN 5000 AND 5005 AND id >= 5002") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0, 2 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(1, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(15d, rows[0][0].AsReal);
    }

    [Fact]
    public async Task FilteredDistinctScalarAggregate_UsesBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE distinct_category_scan (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_category_scan VALUES (1, 10, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_category_scan VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_category_scan VALUES (3, 30, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_category_scan VALUES (4, 35, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_category_scan VALUES (5, 50, 'C')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(DISTINCT category) FROM distinct_category_scan WHERE score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregateTableOperator>(GetRootOperator(result));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 1, 2 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(1, GetPrivateField<int>(rootOperator, "_columnIndex"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedRangeDistinctPayloadAggregate_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_distinct_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 25_000; i++)
        {
            char category = (char)('A' + (i % 3));
            await _db.ExecuteAsync($"INSERT INTO payload_distinct_idx VALUES ({i}, {i}, '{category}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_distinct_idx_score ON payload_distinct_idx(score)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(DISTINCT category) FROM payload_distinct_idx WHERE score BETWEEN 20000 AND 20010 AND id >= 20005") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(result));
        var source = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));
        Assert.Equal(new[] { 0, 2 }, GetPrivateField<int[]>(rootOperator, "_decodedColumnIndices"));
        Assert.Equal(1, GetPrivateField<int>(rootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(source, "_decodedColumnIndices"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedRangeTextMinMaxAggregate_UsesFilteredPayloadAggregateBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE payload_minmax_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 25_000; i++)
        {
            await _db.ExecuteAsync($"INSERT INTO payload_minmax_idx VALUES ({i}, {i}, 'cat_{i:D5}')", ct);
        }
        await _db.CommitAsync(ct);
        await _db.ExecuteAsync("CREATE INDEX idx_payload_minmax_idx_score ON payload_minmax_idx(score)", ct);

        var planner = GetPlanner();

        var minStatement = Parser.Parse("SELECT MIN(category) FROM payload_minmax_idx WHERE score BETWEEN 20000 AND 20010 AND category >= 'cat_20003'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var minResult = await planner.ExecuteAsync(minStatement, ct);
        var minRootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(minResult));
        var minSource = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(minRootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(minRootOperator, "_batchPlan"));
        Assert.Equal(new[] { 2 }, GetPrivateField<int[]>(minRootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(minRootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(minSource, "_decodedColumnIndices"));

        var minRows = await minResult.ToListAsync(ct);
        Assert.Single(minRows);
        Assert.Equal("cat_20003", minRows[0][0].AsText);

        var maxStatement = Parser.Parse("SELECT MAX(category) FROM payload_minmax_idx WHERE score BETWEEN 20000 AND 20010 AND category >= 'cat_20003'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var maxResult = await planner.ExecuteAsync(maxStatement, ct);
        var maxRootOperator = Assert.IsType<FilteredScalarAggregatePayloadOperator>(GetRootOperator(maxResult));
        var maxSource = Assert.IsType<IndexOrderedScanOperator>(GetPrivateField<IOperator>(maxRootOperator, "_source"));

        Assert.IsType<SpecializedScalarAggregateBatchPlan>(GetPrivateField<object>(maxRootOperator, "_batchPlan"));
        Assert.Equal(new[] { 2 }, GetPrivateField<int[]>(maxRootOperator, "_decodedColumnIndices"));
        Assert.Equal(0, GetPrivateField<int>(maxRootOperator, "_columnIndex"));
        Assert.Equal(Array.Empty<int>(), GetPrivateField<int[]>(maxSource, "_decodedColumnIndices"));

        var maxRows = await maxResult.ToListAsync(ct);
        Assert.Single(maxRows);
        Assert.Equal("cat_20010", maxRows[0][0].AsText);
    }

    [Fact]
    public async Task SimpleFilteredColumnProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_cols (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_cols VALUES (1, 10, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_cols VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_cols VALUES (3, 30, 'C')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.category FROM compact_scan_cols s WHERE s.category IS NOT NULL") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal("B", rows[0][1].AsText);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal("C", rows[1][1].AsText);
    }

    [Fact]
    public async Task SimpleFilteredExpressionProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_expr (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_expr VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_expr VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_expr VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.score + 5 FROM compact_scan_expr s WHERE s.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(25L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(35L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task RealBetweenExpressionProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_real_expr (id INTEGER PRIMARY KEY, price REAL NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_real_expr VALUES (1, 1.5)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_real_expr VALUES (2, 2.0)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_real_expr VALUES (3, 3.0)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_real_expr VALUES (4, 4.0)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.price + 0.5 FROM compact_scan_real_expr s WHERE s.price BETWEEN 2.0 AND 3.5") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(2.5d, rows[0][1].AsReal);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(3.5d, rows[1][1].AsReal);
    }

    [Fact]
    public async Task InPredicateColumnProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_in_cols (id INTEGER PRIMARY KEY, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_in_cols VALUES (1, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_in_cols VALUES (2, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_in_cols VALUES (3, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_in_cols VALUES (4, 'D')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.category FROM compact_scan_in_cols s WHERE s.category IN ('B', 'C')") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal("B", rows[0][1].AsText);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal("C", rows[1][1].AsText);
    }

    [Fact]
    public async Task NotBetweenExpressionProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_not_between_expr (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_not_between_expr VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_not_between_expr VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_not_between_expr VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_not_between_expr VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.score + 5 FROM compact_scan_not_between_expr s WHERE s.score NOT BETWEEN 20 AND 30") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(15L, rows[0][1].AsInteger);
        Assert.Equal(4L, rows[1][0].AsInteger);
        Assert.Equal(45L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task LikePredicateColumnProjection_UsesCompactScanProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_scan_like_cols (id INTEGER PRIMARY KEY, category TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_like_cols VALUES (1, 'Alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_like_cols VALUES (2, '100%')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_like_cols VALUES (3, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_scan_like_cols VALUES (4, 'x%y')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.category FROM compact_scan_like_cols s WHERE s.category LIKE '%!%%' ESCAPE '!'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactTableScanProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal("100%", rows[0][1].AsText);
        Assert.Equal(4L, rows[1][0].AsInteger);
        Assert.Equal("x%y", rows[1][1].AsText);
    }

    [Fact]
    public async Task SimpleSelectStar_UsesBatchTableScanOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_scan_star (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_star VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_star VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_star VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_scan_star") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<TableScanOperator>(GetStoredOperator(result));
        Assert.IsType<TableScanOperator>(GetRootOperator(result));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(20L, rows[1][1].AsInteger);
        Assert.Equal(30L, rows[2][1].AsInteger);
    }

    [Fact]
    public async Task SimpleFilteredSelectStar_UsesBatchFilterOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_scan_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_scan_filter WHERE score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        // The planner now pushes the filter into the TableScanOperator as a pre-decode
        // filter, so the root operator is TableScanOperator rather than a separate FilterOperator.
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<TableScanOperator>(GetStoredOperator(result));
        Assert.IsType<TableScanOperator>(GetRootOperator(result));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task GenericFunctionFilteredSelectStar_UsesBatchFilterOperatorWithoutPredicateRowBuffer()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_generic_filter WHERE TEXT(id) = '2'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task GenericFunctionExpressionFilteredSelectStar_UsesBatchFilterOperatorWithoutPredicateRowBuffer()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_expr_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_generic_expr_filter WHERE TEXT(score + 5) = '25'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task GenericFunctionLikeFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_like_filter (id INTEGER PRIMARY KEY, code INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_like_filter VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_like_filter VALUES (2, 15)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_like_filter VALUES (3, NULL)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_generic_like_filter WHERE TEXT(code) LIKE '%NULL%'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
        Assert.True(rows[0][1].IsNull);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task GenericFunctionLikeExpressionFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_expr_like_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_like_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_like_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_like_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_generic_expr_like_filter WHERE TEXT(score + 5) LIKE '2%'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task ArithmeticFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_arithmetic_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_arithmetic_filter VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_arithmetic_filter VALUES (2, 2)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_arithmetic_filter VALUES (3, 3)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_arithmetic_filter VALUES (4, 4)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_arithmetic_filter WHERE score / 2 = 1") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(2L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(3L, rows[1][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task NumericExpressionInFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_numeric_expr_in_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_in_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_in_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_in_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_numeric_expr_in_filter WHERE score + 5 IN (15, 35)") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task NumericExpressionRangeFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_numeric_expr_range_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_range_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_range_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_numeric_expr_range_filter VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_numeric_expr_range_filter WHERE score + 5 BETWEEN 20 AND 30") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task OrFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_or_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_or_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_or_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_or_filter VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_or_filter VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_or_filter WHERE score = 10 OR score = 30") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task NotOrFilteredSelectStar_UsesBatchFilterOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_not_or_filter (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_not_or_filter VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_not_or_filter VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_not_or_filter VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_not_or_filter VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_not_or_filter WHERE NOT (score = 10 OR score = 30)") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(4L, rows[1][0].AsInteger);
        Assert.Equal(40L, rows[1][1].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_predicateRowBuffer"));
    }

    [Fact]
    public async Task SimpleLimitedSelectStar_UsesBatchLimitOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_scan_limit (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_limit VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_limit VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_scan_limit VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_scan_limit LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<LimitOperator>(GetStoredOperator(result));
        Assert.IsType<LimitOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task SimpleOrderBy_UsesBatchSortOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_sort_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_sort_root VALUES (1, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_sort_root VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_sort_root VALUES (3, 20)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_sort_root ORDER BY score ASC") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<SortOperator>(GetStoredOperator(result));
        Assert.IsType<SortOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(new long[] { 10, 20, 30 }, rows.Select(r => r[1].AsInteger).ToArray());
    }

    [Fact]
    public async Task SimpleOrderByWithLimit_UsesBatchTopNSortOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_topn_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_root VALUES (1, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_root VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_root VALUES (3, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_root VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_topn_root ORDER BY score ASC LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        Assert.IsType<LimitOperator>(GetStoredOperator(result));
        Assert.IsType<TopNSortOperator>(FindOperatorInUnaryChain<TopNSortOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal(new long[] { 10, 20 }, rows.Select(r => r[1].AsInteger).ToArray());
    }

    [Fact]
    public async Task ExpressionOrderByWithLimit_UsesBatchTopNSortOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_topn_expr_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_expr_root VALUES (1, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_expr_root VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_expr_root VALUES (3, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_topn_expr_root VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT id FROM batch_topn_expr_root ORDER BY score + id DESC LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        Assert.IsType<LimitOperator>(GetStoredOperator(result));
        Assert.IsType<TopNSortOperator>(FindOperatorInUnaryChain<TopNSortOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(new long[] { 4, 1 }, rows.Select(r => r[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task SimpleDistinct_UsesBatchDistinctOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_distinct_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_root VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_root VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_root VALUES (3, 20)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT DISTINCT score FROM batch_distinct_root") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<DistinctOperator>(GetStoredOperator(result));
        Assert.IsType<DistinctOperator>(GetRootOperator(result));

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(new long[] { 10, 20 }, rows.Select(r => r[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task DistinctOrderByWithLimit_UsesBatchDistinctPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_distinct_topn_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_topn_root VALUES (1, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_topn_root VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_topn_root VALUES (3, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_topn_root VALUES (4, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_distinct_topn_root VALUES (5, 20)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT DISTINCT score FROM batch_distinct_topn_root ORDER BY score ASC LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        Assert.IsType<LimitOperator>(GetStoredOperator(result));

        var source = GetPrivateField<IOperator>(rootOperator, "_source");
        var distinct = Assert.IsType<DistinctOperator>(FindOperatorInUnaryChain<DistinctOperator>(source));
        Assert.True(GetPrivateField<bool>(distinct, "_orderedSingleColumnFastPath"));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(new long[] { 10, 20 }, rows.Select(r => r[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task IndexedEqualitySelectStar_UsesIndexScanOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_index_eq_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, label TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_batch_index_eq_root_score ON batch_index_eq_root(score)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_eq_root VALUES (1, 10, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_eq_root VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_eq_root VALUES (3, 20, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_eq_root VALUES (4, 30, 'D')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_index_eq_root WHERE score = 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<IndexScanOperator>(GetRootOperator(result));

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(new long[] { 2, 3 }, rows.Select(r => r[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task IndexedTextEqualitySelectStar_UsesIndexScanOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_text_eq_root (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_batch_text_eq_root_name ON batch_text_eq_root(name)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_eq_root VALUES (1, 'Alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_eq_root VALUES (2, 'beta')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_eq_root VALUES (3, 'BETA')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_eq_root VALUES (4, 'carrot')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_text_eq_root WHERE name = 'BeTa'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<IndexScanOperator>(GetRootOperator(result));

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal([2L, 3L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task IndexedTextRangeSelectStar_UsesIndexOrderedScanOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_text_range_root (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_batch_text_range_root_name ON batch_text_range_root(name)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_range_root VALUES (1, 'aardvark')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_range_root VALUES (2, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_range_root VALUES (3, 'carrot')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_range_root VALUES (4, 'delta')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_text_range_root VALUES (5, 'echo')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_text_range_root WHERE name >= 'b' COLLATE NOCASE AND name < 'e' COLLATE NOCASE") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<IndexOrderedScanOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Equal(["Beta", "carrot", "delta"], rows.Select(static row => row[1].AsText).ToArray());
    }

    [Fact]
    public async Task IndexedOrderedSelectStar_UsesIndexOrderedScanOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_index_order_root (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, label TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_batch_index_order_root_score ON batch_index_order_root(score)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_order_root VALUES (1, 30, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_order_root VALUES (2, 10, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_order_root VALUES (3, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_index_order_root VALUES (4, 40, 'D')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_index_order_root WHERE score BETWEEN 10 AND 30 ORDER BY score ASC") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        // For uncovered SELECT * without LIMIT, the planner currently prefers a sort over
        // an index-ordered lookup path that would require random table fetches.
        var rootOp = GetRootOperator(result);
        Assert.IsType<SortOperator>(rootOp);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(new long[] { 10, 20, 30 }, rows.Select(r => r[1].AsInteger).ToArray());
    }

    [Fact]
    public async Task SimpleHashJoin_UsesHashJoinOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_join_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_join_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_right VALUES (1, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_right VALUES (2, 3, 300)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_right VALUES (3, 4, 400)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_join_left l INNER JOIN batch_join_right r ON l.id = r.left_id") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<HashJoinOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<HashJoinOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(200L, rows[0][4].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(300L, rows[1][4].AsInteger);
    }

    [Fact]
    public async Task SimpleIndexNestedLoopJoin_UsesBatchCapableIndexNestedLoopJoinOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_right (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_right VALUES (2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_right VALUES (3, 300)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_right VALUES (4, 400)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_inlj_left l INNER JOIN batch_inlj_right r ON l.id = r.id") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<IndexNestedLoopJoinOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<IndexNestedLoopJoinOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(200L, rows[0][3].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(300L, rows[1][3].AsInteger);
    }

    [Fact]
    public async Task SimpleIndexNestedLoopJoinWithLimit_UsesBatchLimitOverIndexNestedLoopJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_limit_right (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_right VALUES (1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_right VALUES (2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_limit_right VALUES (3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_inlj_limit_left l INNER JOIN batch_inlj_limit_right r ON l.id = r.id LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<IndexNestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task IndexNestedLoopJoinedColumnProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_proj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_inlj_proj_right (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_right VALUES (1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_right VALUES (2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_inlj_proj_right VALUES (3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount FROM batch_inlj_proj_left l INNER JOIN batch_inlj_proj_right r ON l.id = r.id WHERE l.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterProjectionOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<IndexNestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(200L, rows[0][1].AsInteger);
        Assert.Equal(300L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task SimpleHashedIndexNestedLoopJoin_UsesBatchCapableHashedLookupOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_left (id INTEGER PRIMARY KEY, code TEXT NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_right (id INTEGER PRIMARY KEY, code TEXT NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_batch_hinlj_right_code ON batch_hinlj_right(code)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_left VALUES (1, 'A-10', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_left VALUES (2, 'B-20', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_left VALUES (3, 'C-30', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_right VALUES (10, 'B-20', 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_right VALUES (11, 'C-30', 300)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_right VALUES (12, 'D-40', 400)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_hinlj_left l INNER JOIN batch_hinlj_right r ON l.code = r.code") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<HashedIndexNestedLoopJoinOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<HashedIndexNestedLoopJoinOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(200L, rows[0][5].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(300L, rows[1][5].AsInteger);
    }

    [Fact]
    public async Task SimpleHashedIndexNestedLoopJoinWithLimit_UsesBatchLimitOverHashedLookupJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_limit_left (id INTEGER PRIMARY KEY, code TEXT NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_limit_right (id INTEGER PRIMARY KEY, code TEXT NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_batch_hinlj_limit_right_code ON batch_hinlj_limit_right(code)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_left VALUES (1, 'A-10', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_left VALUES (2, 'B-20', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_left VALUES (3, 'C-30', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_right VALUES (10, 'A-10', 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_right VALUES (11, 'B-20', 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_limit_right VALUES (12, 'C-30', 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_hinlj_limit_left l INNER JOIN batch_hinlj_limit_right r ON l.code = r.code LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<HashedIndexNestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task HashedIndexNestedLoopJoinedColumnProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_proj_left (id INTEGER PRIMARY KEY, code TEXT NOT NULL, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_hinlj_proj_right (id INTEGER PRIMARY KEY, code TEXT NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_batch_hinlj_proj_right_code ON batch_hinlj_proj_right(code)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_left VALUES (1, 'A-10', 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_left VALUES (2, 'B-20', 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_left VALUES (3, 'C-30', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_right VALUES (10, 'A-10', 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_right VALUES (11, 'B-20', 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_hinlj_proj_right VALUES (12, 'C-30', 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount FROM batch_hinlj_proj_left l INNER JOIN batch_hinlj_proj_right r ON l.code = r.code WHERE l.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterProjectionOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<HashedIndexNestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(200L, rows[0][1].AsInteger);
        Assert.Equal(300L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task SimpleNestedLoopJoin_UsesBatchCapableNestedLoopJoinOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_right VALUES (10, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_right VALUES (11, 3, 300)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_right VALUES (12, 4, 400)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_nlj_left l INNER JOIN batch_nlj_right r ON l.id + 0 = r.left_id") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<NestedLoopJoinOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<NestedLoopJoinOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(200L, rows[0][4].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(300L, rows[1][4].AsInteger);
    }

    [Fact]
    public async Task SimpleNestedLoopJoinWithLimit_UsesBatchLimitOverNestedLoopJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_right VALUES (10, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_right VALUES (11, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_limit_right VALUES (12, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_nlj_limit_left l INNER JOIN batch_nlj_limit_right r ON l.id + 0 = r.left_id LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<NestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task NestedLoopJoinedColumnProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_proj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_nlj_proj_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_right VALUES (10, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_right VALUES (11, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nlj_proj_right VALUES (12, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount FROM batch_nlj_proj_left l INNER JOIN batch_nlj_proj_right r ON l.id + 0 = r.left_id WHERE l.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterProjectionOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<NestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(200L, rows[0][1].AsInteger);
        Assert.Equal(300L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task SimpleLeftNestedLoopJoinWithLimit_UsesBatchLimitOverNestedLoopJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_left_nlj_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_left_nlj_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_right VALUES (2, 1, 150)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_nlj_limit_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_left_nlj_limit_left l LEFT JOIN batch_left_nlj_limit_right r ON l.id + 0 = r.left_id LIMIT 4") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = Assert.IsType<NestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ThenBy(r => r[4].IsNull ? long.MaxValue : r[4].AsInteger).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(100L, rows[0][4].AsInteger);
        Assert.Equal(1L, rows[1][0].AsInteger);
        Assert.Equal(150L, rows[1][4].AsInteger);
        Assert.Equal(2L, rows[2][0].AsInteger);
        Assert.True(rows[2][4].IsNull);
        Assert.Equal(3L, rows[3][0].AsInteger);
        Assert.Equal(300L, rows[3][4].AsInteger);
    }

    [Fact]
    public async Task SimpleRightNestedLoopJoinWithLimit_UsesBatchLimitOverNestedLoopJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_right_nlj_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_right_nlj_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_right VALUES (2, 1, 150)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_right VALUES (3, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_nlj_limit_right VALUES (4, 99, 900)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_right_nlj_limit_left l RIGHT JOIN batch_right_nlj_limit_right r ON l.id + 0 = r.left_id LIMIT 4") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<NestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[2].AsInteger).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(100L, rows[0][4].AsInteger);
        Assert.Equal(1L, rows[1][0].AsInteger);
        Assert.Equal(10L, rows[1][1].AsInteger);
        Assert.Equal(150L, rows[1][4].AsInteger);
        Assert.Equal(2L, rows[2][0].AsInteger);
        Assert.Equal(20L, rows[2][1].AsInteger);
        Assert.Equal(200L, rows[2][4].AsInteger);
        Assert.True(rows[3][0].IsNull);
        Assert.True(rows[3][1].IsNull);
        Assert.Equal(4L, rows[3][2].AsInteger);
        Assert.Equal(900L, rows[3][4].AsInteger);
    }

    [Fact]
    public async Task CrossJoinWithLimit_UsesBatchLimitOverNestedLoopJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_cross_left (id INTEGER PRIMARY KEY, name TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_cross_right (id INTEGER PRIMARY KEY, size TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_cross_left VALUES (1, 'red')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_cross_left VALUES (2, 'blue')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_cross_right VALUES (10, 'S')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_cross_right VALUES (11, 'M')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_cross_right VALUES (12, 'L')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_cross_left a CROSS JOIN batch_cross_right b LIMIT 4") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = Assert.IsType<NestedLoopJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(4, rows.Count);
        Assert.Equal("red", rows[0][1].AsText);
        Assert.Equal("S", rows[0][3].AsText);
        Assert.Equal("red", rows[1][1].AsText);
        Assert.Equal("M", rows[1][3].AsText);
    }

    [Fact]
    public async Task SimpleHashJoinWithLimit_UsesBatchLimitOverHashJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_join_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_join_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_limit_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_join_limit_left l INNER JOIN batch_join_limit_right r ON l.id = r.left_id LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task SimpleHashJoinWithResidualLimit_UsesBatchLimitOverHashJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_join_residual_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_join_residual_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_join_residual_limit_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_join_residual_limit_left l INNER JOIN batch_join_residual_limit_right r ON l.id = r.left_id AND r.amount >= 200 LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(200L, rows[0][4].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(300L, rows[1][4].AsInteger);
    }

    [Fact]
    public async Task SimpleLeftHashJoinWithLimit_UsesBatchLimitOverHashJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_left_join_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_left_join_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_right VALUES (2, 1, 150)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_left_join_limit_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_left_join_limit_left l LEFT JOIN batch_left_join_limit_right r ON l.id = r.left_id LIMIT 4") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = Assert.IsType<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[0].AsInteger).ThenBy(r => r[4].IsNull ? long.MaxValue : r[4].AsInteger).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(100L, rows[0][4].AsInteger);
        Assert.Equal(1L, rows[1][0].AsInteger);
        Assert.Equal(150L, rows[1][4].AsInteger);
        Assert.Equal(2L, rows[2][0].AsInteger);
        Assert.True(rows[2][4].IsNull);
        Assert.Equal(3L, rows[3][0].AsInteger);
        Assert.Equal(300L, rows[3][4].AsInteger);
    }

    [Fact]
    public async Task SimpleRightHashJoinWithLimit_UsesBatchLimitOverHashJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_right_join_limit_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_right_join_limit_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_right VALUES (2, 1, 150)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_right VALUES (3, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_join_limit_right VALUES (4, 99, 900)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT * FROM batch_right_join_limit_left l RIGHT JOIN batch_right_join_limit_right r ON l.id = r.left_id AND l.id = r.id LIMIT 4") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = Assert.IsType<LimitOperator>(GetRootOperator(result));
        var joinOperator = FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsAssignableFrom<IBatchOperator>(joinOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(r => r[2].AsInteger).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(100L, rows[0][4].AsInteger);
        Assert.True(rows[1][0].IsNull);
        Assert.True(rows[1][1].IsNull);
        Assert.Equal(150L, rows[1][4].AsInteger);
        Assert.True(rows[2][0].IsNull);
        Assert.True(rows[2][1].IsNull);
        Assert.Equal(200L, rows[2][4].AsInteger);
        Assert.True(rows[3][0].IsNull);
        Assert.True(rows[3][1].IsNull);
        Assert.Equal(900L, rows[3][4].AsInteger);
    }

    [Fact]
    public async Task JoinedFilteredColumnProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_proj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_proj_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_proj_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount FROM batch_proj_left l INNER JOIN batch_proj_right r ON l.id = r.left_id WHERE l.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<FilterProjectionOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_batchPlan"));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(200L, rows[0][1].AsInteger);
        Assert.Equal(300L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task RightJoinedColumnProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_right_proj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_right_proj_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_right VALUES (2, 1, 150)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_right VALUES (3, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_right_proj_right VALUES (4, 99, 900)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.score, r.amount FROM batch_right_proj_left l RIGHT JOIN batch_right_proj_right r ON l.id = r.left_id AND l.id = r.id") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<ProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<ProjectionOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[1].AsInteger).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.Equal(100L, rows[0][1].AsInteger);
        Assert.True(rows[1][0].IsNull);
        Assert.Equal(150L, rows[1][1].AsInteger);
        Assert.True(rows[2][0].IsNull);
        Assert.Equal(200L, rows[2][1].AsInteger);
        Assert.True(rows[3][0].IsNull);
        Assert.Equal(900L, rows[3][1].AsInteger);
    }

    [Fact]
    public async Task JoinedExpressionProjectionWithResidual_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_expr_proj_residual_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_expr_proj_residual_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_residual_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount + l.score FROM batch_expr_proj_residual_left l INNER JOIN batch_expr_proj_residual_right r ON l.id = r.left_id AND r.amount >= 200") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<ProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<ProjectionOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(FindOperatorInUnaryChain<HashJoinOperator>(GetPrivateField<IOperator>(rootOperator, "_source")));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(220L, rows[0][1].AsInteger);
        Assert.Equal(330L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task JoinedExpressionProjection_UsesBatchProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_expr_proj_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_expr_proj_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_proj_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount + l.score FROM batch_expr_proj_left l INNER JOIN batch_expr_proj_right r ON l.id = r.left_id") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<ProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<ProjectionOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(110L, rows[0][1].AsInteger);
        Assert.Equal(220L, rows[1][1].AsInteger);
        Assert.Equal(330L, rows[2][1].AsInteger);
    }

    [Fact]
    public async Task GenericFunctionProjection_UsesBatchProjectionOperatorWithoutSourceRowBuffer()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_projection (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_projection VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_projection VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_projection VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT TEXT(id) FROM batch_generic_projection") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<CompactTableScanProjectionOperator>(GetRootOperator(result));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("1", rows[0][0].AsText);
        Assert.Equal("2", rows[1][0].AsText);
        Assert.Equal("3", rows[2][0].AsText);
    }

    [Fact]
    public async Task GenericFunctionExpressionProjection_UsesBatchProjectionOperatorWithoutSourceRowBuffer()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_expr_projection (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_projection VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_projection VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_expr_projection VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT TEXT(score + 5) FROM batch_generic_expr_projection") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<CompactTableScanProjectionOperator>(GetRootOperator(result));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("15", rows[0][0].AsText);
        Assert.Equal("25", rows[1][0].AsText);
        Assert.Equal("35", rows[2][0].AsText);
    }

    [Fact]
    public async Task JoinedFilteredExpressionProjection_UsesBatchFilterProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_expr_left (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_expr_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_left VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_left VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_left VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_right VALUES (1, 1, 100)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_right VALUES (2, 2, 200)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_expr_right VALUES (3, 3, 300)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.amount + l.score FROM batch_expr_left l INNER JOIN batch_expr_right r ON l.id = r.left_id WHERE l.score >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<FilterProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<FilterProjectionOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(220L, rows[0][1].AsInteger);
        Assert.Equal(330L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task NestedArithmeticFilteredProjection_UsesBatchFilterProjectionOperatorBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_nested_expr (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nested_expr VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nested_expr VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_nested_expr VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT s.id, (s.score + 5) * 2 FROM batch_nested_expr s WHERE (s.score + 5) * 2 >= 50") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<CompactTableScanProjectionOperator>(GetRootOperator(result));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(50L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(70L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task GenericFunctionFilteredProjection_UsesBatchFilterProjectionOperatorWithoutSourceRowBuffer()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_generic_filter_projection (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter_projection VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter_projection VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_generic_filter_projection VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT TEXT(id) FROM batch_generic_filter_projection WHERE TEXT(id) <> '1'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactTableScanProjectionOperator>(GetStoredOperator(result));
        var rootOperator = Assert.IsType<CompactTableScanProjectionOperator>(GetRootOperator(result));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("2", rows[0][0].AsText);
        Assert.Equal("3", rows[1][0].AsText);
    }

    [Fact]
    public async Task IndexedRangeColumnProjection_UsesCompactPayloadProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_index_cols (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_compact_index_cols_score ON compact_index_cols(score)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_cols VALUES (1, 10, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_cols VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_cols VALUES (3, 30, 'C')", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_cols VALUES (4, 40, 'D')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT s.id, s.category FROM compact_index_cols s WHERE s.score BETWEEN 20 AND 40 AND s.category IS NOT NULL") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactPayloadProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactPayloadProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("B", rows[0][1].AsText);
        Assert.Equal("C", rows[1][1].AsText);
        Assert.Equal("D", rows[2][1].AsText);
    }

    [Fact]
    public async Task IndexedRangeExpressionProjection_UsesCompactPayloadProjectionOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE compact_index_expr (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_compact_index_expr_score ON compact_index_expr(score)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_expr VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_expr VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_expr VALUES (3, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO compact_index_expr VALUES (4, 40)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT s.id, s.score + 5 FROM compact_index_expr s WHERE s.score BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));
        Assert.IsType<CompactPayloadProjectionOperator>(GetStoredOperator(result));
        var rootOperator = GetRootOperator(result);
        Assert.IsType<CompactPayloadProjectionOperator>(rootOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(rootOperator, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(25L, rows[0][1].AsInteger);
        Assert.Equal(35L, rows[1][1].AsInteger);
        Assert.Equal(45L, rows[2][1].AsInteger);
    }

    [Fact]
    public async Task FilteredColumnProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_cols (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_cols VALUES (1, 10, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_cols VALUES (2, 20, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_cols VALUES (3, 30, 'C')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.category FROM generic_batch_cols s WHERE s.category IS NOT NULL LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = Assert.IsType<FilterProjectionOperator>(GetPrivateField<IOperator>(rootOperator, "_source"));
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator, "_batchPlan"));
        Assert.IsAssignableFrom<IBatchOperator>(GetPrivateField<IOperator>(projectionOperator, "_source"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal("B", rows[0][1].AsText);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal("C", rows[1][1].AsText);
    }

    [Fact]
    public async Task ExpressionProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_expr (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_expr VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_expr VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_expr VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.score + 5 FROM generic_batch_expr s LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<ProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(15L, rows[0][1].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal(25L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task RealBetweenExpressionProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_real_expr (id INTEGER PRIMARY KEY, price REAL NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_real_expr VALUES (1, 1.5)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_real_expr VALUES (2, 2.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_real_expr VALUES (3, 3.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_real_expr VALUES (4, 4.0)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.price * 2.0 FROM generic_batch_real_expr s WHERE s.price BETWEEN 2.0 AND 3.5 LIMIT 10") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<FilterProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(4.0d, rows[0][1].AsReal);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(6.0d, rows[1][1].AsReal);
    }

    [Fact]
    public async Task InPredicateExpressionProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_in_expr (id INTEGER PRIMARY KEY, price REAL NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_expr VALUES (1, 1.5)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_expr VALUES (2, 2.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_expr VALUES (3, 4.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_expr VALUES (4, 5.0)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.price + 0.25 FROM generic_batch_in_expr s WHERE s.price IN (2.0, 4, NULL) LIMIT 10") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<FilterProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(2.25d, rows[0][1].AsReal);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(4.25d, rows[1][1].AsReal);
    }

    [Fact]
    public async Task InPredicateTextExpressionProjection_WithLimit_UsesGenericBatchPlanAndPreDecodeFilter()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_in_text_expr (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, category TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_text_expr VALUES (1, 10, 'Alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_text_expr VALUES (2, 20, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_text_expr VALUES (3, 30, 'Gamma')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_in_text_expr VALUES (4, 40, NULL)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.value + s.id FROM generic_batch_in_text_expr s WHERE s.category IN ('Beta', 'Gamma') LIMIT 10") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<FilterProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));
        var scanOperator = Assert.IsType<TableScanOperator>(GetPrivateField<IOperator>(projectionOperator!, "_source"));
        Assert.True(GetPrivateField<bool>(scanOperator, "_hasPreDecodeFilter"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(22L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(33L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task NotInPredicateExpressionProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_not_in_expr (id INTEGER PRIMARY KEY, price REAL NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_in_expr VALUES (1, 1.5)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_in_expr VALUES (2, 2.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_in_expr VALUES (3, 4.0)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_in_expr VALUES (4, 5.0)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.price + 0.25 FROM generic_batch_not_in_expr s WHERE s.price NOT IN (2.0, 4) LIMIT 10") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<FilterProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(1.75d, rows[0][1].AsReal);
        Assert.Equal(4L, rows[1][0].AsInteger);
        Assert.Equal(5.25d, rows[1][1].AsReal);
    }

    [Fact]
    public async Task NotLikeExpressionProjection_WithLimit_UsesGenericBatchPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE generic_batch_not_like_expr (id INTEGER PRIMARY KEY, category TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_like_expr VALUES (1, 'Alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_like_expr VALUES (2, 'Beta')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_like_expr VALUES (3, 'Gamma')", ct);
        await _db.ExecuteAsync("INSERT INTO generic_batch_not_like_expr VALUES (4, 'Bravo')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT s.id, s.id + 10 FROM generic_batch_not_like_expr s WHERE s.category NOT LIKE 'B%' LIMIT 10") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        var rootOperator = GetRootOperator(result);
        Assert.IsType<LimitOperator>(rootOperator);

        var projectionOperator = GetPrivateField<IOperator>(rootOperator, "_source");
        Assert.IsType<FilterProjectionOperator>(projectionOperator);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(GetPrivateField<object>(projectionOperator!, "_batchPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(11L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(13L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task DirectBatchQueryResult_ToListAsync_ContinuesAfterMoveNextAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_result_resume (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_result_resume VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_result_resume VALUES (2, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_result_resume VALUES (3, 30)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT * FROM batch_result_resume") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesDirectBatchStorage(result));

        Assert.True(await result.MoveNextAsync(ct));
        Assert.Equal(1L, result.Current[0].AsInteger);
        Assert.Equal(10L, result.Current[1].AsInteger);

        var remainingRows = await result.ToListAsync(ct);
        Assert.Equal(2, remainingRows.Count);
        Assert.Equal(new long[] { 2, 3 }, remainingRows.Select(row => row[0].AsInteger).ToArray());
        Assert.False(await result.MoveNextAsync(ct));
    }

    [Fact]
    public async Task IntegerPrimaryKeyMin_UsesTableKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_min (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_min VALUES (30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_min VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_min VALUES (20, 'b')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT MIN(id) FROM pk_min") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(10L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKeySumRange_UsesTableKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_sum_range (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_sum_range VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_sum_range VALUES (20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_sum_range VALUES (30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_sum_range VALUES (40, 'd')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT SUM(id) FROM pk_sum_range WHERE id BETWEEN 20 AND 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(90L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKeyCountStarRange_UsesTableKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_count_range (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_range VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_range VALUES (20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_range VALUES (30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_range VALUES (40, 'd')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(*) FROM pk_count_range WHERE id >= 20 AND id < 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKeyMaxExclusiveUpperBound_UsesTableKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_max_exclusive (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        for (int i = 1; i <= 2000; i++)
            await _db.ExecuteAsync($"INSERT INTO pk_max_exclusive VALUES ({i}, 'v{i}')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT MAX(id) FROM pk_max_exclusive WHERE id < 1501") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(1500L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKeyCount_UsesCachedRowCountFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_count_fast (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_fast VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_fast VALUES (20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_fast VALUES (30, 'c')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(id) FROM pk_count_fast") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesSyncLookupResult(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKeyCountDistinct_UsesCachedRowCountFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_count_distinct_fast (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_distinct_fast VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_distinct_fast VALUES (20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_count_distinct_fast VALUES (30, 'c')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT COUNT(DISTINCT id) FROM pk_count_distinct_fast") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.True(UsesSyncLookupResult(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task IndexedIntegerDistinctAggregates_UseIndexKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE distinct_idx_aggs (id INTEGER PRIMARY KEY, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (2, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (3, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (4, 30)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (5, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO distinct_idx_aggs VALUES (6, 30)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_distinct_idx_aggs_score ON distinct_idx_aggs(score)", ct);

        var planner = GetPlanner();

        var countStatement = Parser.Parse("SELECT COUNT(DISTINCT score) FROM distinct_idx_aggs") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var countResult = await planner.ExecuteAsync(countStatement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(countResult));
        var countRows = await countResult.ToListAsync(ct);
        Assert.Single(countRows);
        Assert.Equal(3L, countRows[0][0].AsInteger);

        var sumStatement = Parser.Parse("SELECT SUM(DISTINCT score) FROM distinct_idx_aggs") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var sumResult = await planner.ExecuteAsync(sumStatement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(sumResult));
        var sumRows = await sumResult.ToListAsync(ct);
        Assert.Single(sumRows);
        Assert.Equal(60L, sumRows[0][0].AsInteger);

        var avgStatement = Parser.Parse("SELECT AVG(DISTINCT score) FROM distinct_idx_aggs") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var avgResult = await planner.ExecuteAsync(avgStatement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(avgResult));
        var avgRows = await avgResult.ToListAsync(ct);
        Assert.Single(avgRows);
        Assert.Equal(20d, avgRows[0][0].AsReal);

        var maxStatement = Parser.Parse("SELECT MAX(DISTINCT score) FROM distinct_idx_aggs WHERE score < 30") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var maxResult = await planner.ExecuteAsync(maxStatement, ct);
        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(maxResult));
        var maxRows = await maxResult.ToListAsync(ct);
        Assert.Single(maxRows);
        Assert.Equal(20L, maxRows[0][0].AsInteger);
    }

    [Fact]
    public async Task GroupBy_IndexedIntegerColumn_UsesGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_fast (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_fast_score ON grouped_fast(score)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_fast VALUES (1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_fast VALUES (2, 10, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_fast VALUES (3, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_fast VALUES (4, 20, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_fast VALUES (5, 20, 'e')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT score, COUNT(*), SUM(score), AVG(score), MIN(score), MAX(score) " +
            "FROM grouped_fast GROUP BY score") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexGroupedAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (
                Score: row[0].AsInteger,
                Count: row[1].AsInteger,
                Sum: row[2].AsInteger,
                Avg: row[3].AsReal,
                Min: row[4].AsInteger,
                Max: row[5].AsInteger))
            .OrderBy(row => row.Score)
            .ToArray();

        Assert.Equal(2, actual.Length);
        Assert.Equal((10L, 2L, 20L, 10d, 10L, 10L), actual[0]);
        Assert.Equal((20L, 3L, 60L, 20d, 20L, 20L), actual[1]);
    }

    [Fact]
    public async Task GroupBy_IndexedIntegerColumn_WithRangeFilter_UsesGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_range_fast (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_range_fast_score ON grouped_range_fast(score)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (2, 10, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (3, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (4, 20, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (5, 30, 'e')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_range_fast VALUES (6, 40, 'f')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT score, COUNT(*), SUM(score) " +
            "FROM grouped_range_fast " +
            "WHERE score BETWEEN 20 AND 30 " +
            "GROUP BY score") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexGroupedAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (Score: row[0].AsInteger, Count: row[1].AsInteger, Sum: row[2].AsInteger))
            .OrderBy(row => row.Score)
            .ToArray();

        Assert.Equal([(20L, 2L, 40L), (30L, 1L, 30L)], actual);
    }

    [Fact]
    public async Task GroupBy_IndexedIntegerColumn_WithOrderAndLimit_UsesGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_order_fast (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_order_fast_score ON grouped_order_fast(score)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_order_fast VALUES (1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_order_fast VALUES (2, 10, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_order_fast VALUES (3, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_order_fast VALUES (4, 30, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_order_fast VALUES (5, 30, 'e')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT score, COUNT(*) " +
            "FROM grouped_order_fast " +
            "GROUP BY score " +
            "ORDER BY score " +
            "LIMIT 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        var limit = Assert.IsType<LimitOperator>(rootOperator);
        var grouped = Assert.IsType<IndexGroupedAggregateOperator>(GetPrivateField<IOperator>(limit, "_source"));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (Score: row[0].AsInteger, Count: row[1].AsInteger))
            .ToArray();

        Assert.NotNull(grouped);
        Assert.Equal([(10L, 2L), (20L, 1L)], actual);
    }

    [Fact]
    public async Task GroupBy_IndexedIntegerColumn_WithEqualityFilterAndHavingCount_UsesGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_having_fast (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_having_fast_score ON grouped_having_fast(score)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_having_fast VALUES (1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_having_fast VALUES (2, 20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_having_fast VALUES (3, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_having_fast VALUES (4, 30, 'd')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT score, COUNT(*) " +
            "FROM grouped_having_fast " +
            "WHERE score = 20 " +
            "GROUP BY score " +
            "HAVING COUNT(*) = 2") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<IndexGroupedAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (Score: row[0].AsInteger, Count: row[1].AsInteger))
            .ToArray();

        Assert.Equal([(20L, 2L)], actual);
    }

    [Fact]
    public async Task GroupBy_CompositeIndexedColumns_UsesCompositeGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_comp_fast (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_comp_fast_ab ON grouped_comp_fast(a, b)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_fast VALUES (1, 1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_fast VALUES (2, 1, 10, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_fast VALUES (3, 1, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_fast VALUES (4, 2, 10, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_fast VALUES (5, 2, 10, 'e')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT a, b, COUNT(*) " +
            "FROM grouped_comp_fast " +
            "GROUP BY a, b") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<CompositeIndexGroupedAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (A: row[0].AsInteger, B: row[1].AsInteger, Count: row[2].AsInteger))
            .OrderBy(row => row.A)
            .ThenBy(row => row.B)
            .ToArray();

        Assert.Equal([(1L, 10L, 2L), (1L, 20L, 1L), (2L, 10L, 2L)], actual);
    }

    [Fact]
    public async Task GroupBy_CompositeIndexLeftmostPrefix_UsesCompositeGroupedIndexAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE grouped_comp_prefix (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_grouped_comp_prefix_ab ON grouped_comp_prefix(a, b)", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_prefix VALUES (1, 1, 10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_prefix VALUES (2, 1, 20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_prefix VALUES (3, 1, 20, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_prefix VALUES (4, 2, 10, 'd')", ct);
        await _db.ExecuteAsync("INSERT INTO grouped_comp_prefix VALUES (5, 2, 30, 'e')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT a, COUNT(*) " +
            "FROM grouped_comp_prefix " +
            "GROUP BY a") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<CompositeIndexGroupedAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var actual = rows
            .Select(row => (A: row[0].AsInteger, Count: row[1].AsInteger))
            .OrderBy(row => row.A)
            .ToArray();

        Assert.Equal([(1L, 3L), (2L, 2L)], actual);
    }

    [Fact]
    public async Task IntegerPrimaryKeyDistinctAggregates_UseTableKeyAggregateFastPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE pk_distinct_aggs (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO pk_distinct_aggs VALUES (10, 'a')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_distinct_aggs VALUES (20, 'b')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_distinct_aggs VALUES (30, 'c')", ct);
        await _db.ExecuteAsync("INSERT INTO pk_distinct_aggs VALUES (40, 'd')", ct);

        var planner = GetPlanner();

        var sumStatement = Parser.Parse("SELECT SUM(DISTINCT id) FROM pk_distinct_aggs") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var sumResult = await planner.ExecuteAsync(sumStatement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(sumResult));
        var sumRows = await sumResult.ToListAsync(ct);
        Assert.Single(sumRows);
        Assert.Equal(100L, sumRows[0][0].AsInteger);

        var avgStatement = Parser.Parse("SELECT AVG(DISTINCT id) FROM pk_distinct_aggs WHERE id >= 20") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var avgResult = await planner.ExecuteAsync(avgStatement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(avgResult));
        var avgRows = await avgResult.ToListAsync(ct);
        Assert.Single(avgRows);
        Assert.Equal(30d, avgRows[0][0].AsReal);

        var maxStatement = Parser.Parse("SELECT MAX(DISTINCT id) FROM pk_distinct_aggs WHERE id < 40") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");
        await using var maxResult = await planner.ExecuteAsync(maxStatement, ct);
        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(maxResult));
        var maxRows = await maxResult.ToListAsync(ct);
        Assert.Single(maxRows);
        Assert.Equal(30L, maxRows[0][0].AsInteger);
    }

    [Fact]
    public async Task GroupBy_Single()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER, category TEXT, amount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 'A', 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 'B', 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 'A', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 'B', 40)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT category, SUM(amount) FROM orders GROUP BY category", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);

        // Find group A and B (order not guaranteed)
        var groupA = rows.FirstOrDefault(r => r[0].AsText == "A");
        var groupB = rows.FirstOrDefault(r => r[0].AsText == "B");
        Assert.NotNull(groupA);
        Assert.NotNull(groupB);
        Assert.Equal(40, groupA[1].AsInteger);
        Assert.Equal(60, groupB[1].AsInteger);
    }

    [Fact]
    public async Task GroupBy_MultipleColumns()
    {
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER, region TEXT, product TEXT, qty INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 'East', 'Widget', 5)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 'East', 'Widget', 3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 'East', 'Gadget', 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (4, 'West', 'Widget', 7)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT region, product, SUM(qty) FROM sales GROUP BY region, product", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count); // East/Widget, East/Gadget, West/Widget
    }

    [Fact]
    public async Task Having()
    {
        await _db.ExecuteAsync("CREATE TABLE data (id INTEGER, grp TEXT, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO data VALUES (1, 'A', 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO data VALUES (2, 'A', 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO data VALUES (3, 'B', 5)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO data VALUES (4, 'C', 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO data VALUES (5, 'C', 200)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT grp, SUM(val) FROM data GROUP BY grp HAVING SUM(val) > 20", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // A(30), C(300) — B(5) excluded
    }

    [Fact]
    public async Task CountStar_EmptyTable()
    {
        await _db.ExecuteAsync("CREATE TABLE empty (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM empty", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(0, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Avg_EmptyTable_ReturnsNull()
    {
        await _db.ExecuteAsync("CREATE TABLE empty2 (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT AVG(val) FROM empty2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.True(rows[0][0].IsNull);
    }

    [Fact]
    public async Task CountDistinct()
    {
        await _db.ExecuteAsync("CREATE TABLE tags (id INTEGER, tag TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (2, 'b')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (3, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (4, 'c')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags VALUES (5, 'b')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(DISTINCT tag) FROM tags", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows[0][0].AsInteger); // a, b, c
    }

    [Fact]
    public async Task SelectDistinct_WithNullsAndOrderBy()
    {
        await _db.ExecuteAsync("CREATE TABLE tags_distinct (id INTEGER, tag TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (1, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (2, 'b')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (3, 'a')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (4, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (5, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO tags_distinct VALUES (6, 'c')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT DISTINCT tag FROM tags_distinct ORDER BY tag",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(4, rows.Count);
        Assert.True(rows[0][0].IsNull);
        Assert.Equal("a", rows[1][0].AsText);
        Assert.Equal("b", rows[2][0].AsText);
        Assert.Equal("c", rows[3][0].AsText);
    }

    [Fact]
    public async Task SelectDistinct_AppliesBeforeLimitAndOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE nums_distinct (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums_distinct VALUES (4)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT DISTINCT val FROM nums_distinct ORDER BY val LIMIT 2 OFFSET 1",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0][0].AsInteger);
        Assert.Equal(3, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task DistinctAggregates_IntegerColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (4, 3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (5, NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (6, 3)", TestContext.Current.CancellationToken);

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(DISTINCT val) FROM nums", TestContext.Current.CancellationToken);
        var countRows = await countResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(countRows);
        Assert.Equal(3, countRows[0][0].AsInteger);

        await using var sumResult = await _db.ExecuteAsync("SELECT SUM(DISTINCT val) FROM nums", TestContext.Current.CancellationToken);
        var sumRows = await sumResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(sumRows);
        Assert.Equal(6, sumRows[0][0].AsInteger);

        await using var avgResult = await _db.ExecuteAsync("SELECT AVG(DISTINCT val) FROM nums", TestContext.Current.CancellationToken);
        var avgRows = await avgResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(avgRows);
        Assert.Equal(2d, avgRows[0][0].AsReal);
    }

    [Fact]
    public async Task DistinctAggregates_EmptyTable()
    {
        await _db.ExecuteAsync("CREATE TABLE nums2 (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(DISTINCT val) FROM nums2", TestContext.Current.CancellationToken);
        var countRows = await countResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(countRows);
        Assert.Equal(0, countRows[0][0].AsInteger);

        await using var sumResult = await _db.ExecuteAsync("SELECT SUM(DISTINCT val) FROM nums2", TestContext.Current.CancellationToken);
        var sumRows = await sumResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(sumRows);
        Assert.Equal(0, sumRows[0][0].AsInteger);

        await using var avgResult = await _db.ExecuteAsync("SELECT AVG(DISTINCT val) FROM nums2", TestContext.Current.CancellationToken);
        var avgRows = await avgResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(avgRows);
        Assert.True(avgRows[0][0].IsNull);
    }

    [Fact]
    public async Task Union_RemovesDuplicates_AndAppliesOuterOrderBy()
    {
        await _db.ExecuteAsync("CREATE TABLE set_left (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE set_right (val INTEGER)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO set_left VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO set_left VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO set_left VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO set_right VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO set_right VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO set_right VALUES (4)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM set_left UNION SELECT val FROM set_right ORDER BY val",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(4, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(2, rows[1][0].AsInteger);
        Assert.Equal(3, rows[2][0].AsInteger);
        Assert.Equal(4, rows[3][0].AsInteger);
    }

    [Fact]
    public async Task Intersect_ReturnsDistinctSharedRows()
    {
        await _db.ExecuteAsync("CREATE TABLE intersect_left (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE intersect_right (val INTEGER)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO intersect_left VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_left VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_left VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_left VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_right VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_right VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO intersect_right VALUES (4)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM intersect_left INTERSECT SELECT val FROM intersect_right",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);
        Assert.Equal(2, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Except_ReturnsDistinctRowsOnlyFromLeft()
    {
        await _db.ExecuteAsync("CREATE TABLE except_left (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE except_right (val INTEGER)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO except_left VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO except_left VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO except_left VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO except_left VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO except_right VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO except_right VALUES (4)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM except_left EXCEPT SELECT val FROM except_right ORDER BY val",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(3, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task CompoundSelect_CanBackViewDefinition()
    {
        await _db.ExecuteAsync("CREATE TABLE view_left (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE view_right (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO view_left VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO view_right VALUES (2)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE VIEW combined_vals AS SELECT val FROM view_left UNION SELECT val FROM view_right",
            TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM combined_vals ORDER BY val",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(2, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task ScalarSubquery_CanFilterRows()
    {
        await _db.ExecuteAsync("CREATE TABLE scalar_users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE scalar_cfg (selected_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_users VALUES (1, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_users VALUES (2, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_cfg VALUES (2)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM scalar_users WHERE id = (SELECT selected_id FROM scalar_cfg)",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);
        Assert.Equal("Grace", rows[0][0].AsText);
    }

    [Fact]
    public async Task InSubquery_FiltersRows()
    {
        await _db.ExecuteAsync("CREATE TABLE in_users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE featured_users (user_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO in_users VALUES (1, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO in_users VALUES (2, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO in_users VALUES (3, 'Linus')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO featured_users VALUES (3)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO featured_users VALUES (2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO featured_users VALUES (2)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM in_users WHERE id IN (SELECT user_id FROM featured_users) ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Grace", rows[0][0].AsText);
        Assert.Equal("Linus", rows[1][0].AsText);
    }

    [Fact]
    public async Task ExistsSubquery_CanGateResultSet()
    {
        await _db.ExecuteAsync("CREATE TABLE exists_users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE feature_flags (enabled INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO exists_users VALUES (1, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO exists_users VALUES (2, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO exists_users VALUES (3, 'Linus')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO feature_flags VALUES (1)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM exists_users WHERE id > 1 AND EXISTS (SELECT enabled FROM feature_flags WHERE enabled = 1) ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Grace", rows[0][0].AsText);
        Assert.Equal("Linus", rows[1][0].AsText);
    }

    [Fact]
    public async Task ScalarSubquery_CanDriveUpdateExpressions()
    {
        await _db.ExecuteAsync("CREATE TABLE subquery_products (id INTEGER PRIMARY KEY, discount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE subquery_rules (discount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_products VALUES (1, 0)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_products VALUES (2, 0)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_rules VALUES (15)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "UPDATE subquery_products SET discount = (SELECT discount FROM subquery_rules) WHERE id = 2",
            TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT discount FROM subquery_products WHERE id = 2",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);
        Assert.Equal(15L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task View_CanContainScalarSubquery()
    {
        await _db.ExecuteAsync("CREATE TABLE subquery_view_items (id INTEGER PRIMARY KEY, label TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE subquery_view_cfg (mode TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_view_items VALUES (1, 'A')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_view_items VALUES (2, 'B')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO subquery_view_cfg VALUES ('live')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE VIEW item_modes AS SELECT id, (SELECT mode FROM subquery_view_cfg) AS mode FROM subquery_view_items",
            TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT mode FROM item_modes ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("live", rows[0][0].AsText);
        Assert.Equal("live", rows[1][0].AsText);
    }

    [Fact]
    public async Task ScalarSubquery_ThrowsWhenMultipleRowsReturned()
    {
        await _db.ExecuteAsync("CREATE TABLE scalar_outer (id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE scalar_many (val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_outer VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_many VALUES (10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO scalar_many VALUES (20)", TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync(
                "SELECT (SELECT val FROM scalar_many) AS picked FROM scalar_outer",
                TestContext.Current.CancellationToken);
            _ = await result.ToListAsync(TestContext.Current.CancellationToken);
        });

        Assert.Equal(ErrorCode.SyntaxError, ex.Code);
        Assert.Contains("more than one row", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CorrelatedScalarSubquery_CanFilterRows()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_scalar_users (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_scalar_cfg (dept_id INTEGER, selected_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_scalar_users VALUES (1, 10, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_scalar_users VALUES (2, 10, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_scalar_users VALUES (3, 20, 'Linus')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_scalar_cfg VALUES (10, 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_scalar_cfg VALUES (20, 3)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM corr_scalar_users u WHERE id = (SELECT selected_id FROM corr_scalar_cfg c WHERE c.dept_id = u.dept_id) ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Grace", rows[0][0].AsText);
        Assert.Equal("Linus", rows[1][0].AsText);
    }

    [Fact]
    public async Task CorrelatedInSubquery_FiltersRows()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_in_users (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_in_featured (region_id INTEGER, user_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_in_users VALUES (1, 10, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_in_users VALUES (2, 10, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_in_users VALUES (3, 20, 'Linus')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_in_featured VALUES (10, 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_in_featured VALUES (20, 3)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM corr_in_users u WHERE id IN (SELECT user_id FROM corr_in_featured f WHERE f.region_id = u.region_id) ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Grace", rows[0][0].AsText);
        Assert.Equal("Linus", rows[1][0].AsText);
    }

    [Fact]
    public async Task CorrelatedNotInSubquery_FiltersRows_AndPreservesNullSemantics()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_not_in_users (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_not_in_excluded (region_id INTEGER, user_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_users VALUES (1, 10, 'Ada')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_users VALUES (2, 10, 'Grace')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_users VALUES (3, 20, 'Linus')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_users VALUES (4, 30, 'Ken')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_excluded VALUES (10, 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_not_in_excluded VALUES (20, NULL)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM corr_not_in_users u WHERE id NOT IN (SELECT user_id FROM corr_not_in_excluded e WHERE e.region_id = u.region_id) ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Ada", rows[0][0].AsText);
        Assert.Equal("Ken", rows[1][0].AsText);
    }

    [Fact]
    public async Task CorrelatedExistsSubquery_FiltersRows()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_exists_projects (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_exists_members (project_id INTEGER, role TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_projects VALUES (1, 'Apollo')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_projects VALUES (2, 'Gemini')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_projects VALUES (3, 'Mercury')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_members VALUES (1, 'lead')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_members VALUES (1, 'reviewer')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_exists_members VALUES (3, 'lead')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT name FROM corr_exists_projects p WHERE EXISTS (SELECT project_id FROM corr_exists_members m WHERE m.project_id = p.id AND m.role = 'lead') ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Apollo", rows[0][0].AsText);
        Assert.Equal("Mercury", rows[1][0].AsText);
    }

    [Fact]
    public async Task CorrelatedScalarSubquery_CanDriveUpdateExpressions()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_update_staff (id INTEGER PRIMARY KEY, dept_code INTEGER, bonus INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_update_rules (target_code INTEGER, bonus INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_update_staff VALUES (1, 10, 0)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_update_staff VALUES (2, 20, 0)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_update_rules VALUES (10, 5)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_update_rules VALUES (20, 9)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "UPDATE corr_update_staff SET bonus = (SELECT bonus FROM corr_update_rules WHERE target_code = dept_code)",
            TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT bonus FROM corr_update_staff ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal(5L, rows[0][0].AsInteger);
        Assert.Equal(9L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task CorrelatedOrderBySubquery_IsRejected()
    {
        await _db.ExecuteAsync("CREATE TABLE corr_order_items (id INTEGER PRIMARY KEY, score INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE corr_order_weights (item_id INTEGER, weight INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_order_items VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_order_items VALUES (2, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_order_weights VALUES (1, 5)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO corr_order_weights VALUES (2, 1)", TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync(
                "SELECT id FROM corr_order_items o ORDER BY (SELECT weight FROM corr_order_weights w WHERE w.item_id = o.id)",
                TestContext.Current.CancellationToken);
            _ = await result.ToListAsync(TestContext.Current.CancellationToken);
        });

        Assert.Equal(ErrorCode.SyntaxError, ex.Code);
        Assert.Contains("ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Aggregate_WithAlias()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(2, rows[0][0].AsInteger);
        Assert.Equal(30, rows[0][1].AsInteger);
        Assert.Equal("cnt", result.Schema[0].Name);
        Assert.Equal("total", result.Schema[1].Name);
    }

    [Fact]
    public async Task GroupBy_CountPerGroup()
    {
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER, level TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO log VALUES (1, 'INFO')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO log VALUES (2, 'ERROR')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO log VALUES (3, 'INFO')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO log VALUES (4, 'INFO')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO log VALUES (5, 'ERROR')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT level, COUNT(*) FROM log GROUP BY level", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);

        var infoGroup = rows.FirstOrDefault(r => r[0].AsText == "INFO");
        var errorGroup = rows.FirstOrDefault(r => r[0].AsText == "ERROR");
        Assert.NotNull(infoGroup);
        Assert.NotNull(errorGroup);
        Assert.Equal(3, infoGroup[1].AsInteger);
        Assert.Equal(2, errorGroup[1].AsInteger);
    }

    [Fact]
    public async Task Min_Max_Text()
    {
        await _db.ExecuteAsync("CREATE TABLE names (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (1, 'Charlie')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (2, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO names VALUES (3, 'Bob')", TestContext.Current.CancellationToken);

        await using var minResult = await _db.ExecuteAsync("SELECT MIN(name) FROM names", TestContext.Current.CancellationToken);
        var minRows = await minResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal("Alice", minRows[0][0].AsText);

        await using var maxResult = await _db.ExecuteAsync("SELECT MAX(name) FROM names", TestContext.Current.CancellationToken);
        var maxRows = await maxResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal("Charlie", maxRows[0][0].AsText);
    }

    [Fact]
    public async Task Having_CountFilter()
    {
        await _db.ExecuteAsync("CREATE TABLE visits (id INTEGER, page TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (1, 'home')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (2, 'about')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (3, 'home')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (4, 'home')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (5, 'about')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO visits VALUES (6, 'contact')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT page, COUNT(*) FROM visits GROUP BY page HAVING COUNT(*) >= 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // home(3), about(2) — contact(1) excluded
    }

    #endregion

    #region JOINs

    private async Task SetupJoinTables()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 'Widget')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 1, 'Gadget')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 2, 'Widget')", TestContext.Current.CancellationToken);
        // Note: Charlie (id=3) has no orders, order user_id=4 doesn't exist
    }

    [Fact]
    public async Task InnerJoin()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        // users has 2 cols (id, name), orders has 3 cols (id, user_id, product)
        // Each row should have 5 values
        Assert.Equal(5, rows[0].Length);
    }

    [Fact]
    public async Task InnerJoin_WithQualifiedColumns()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        // Alice has 2 orders, Bob has 1
        var names = rows.Select(r => r[0].AsText).ToList();
        Assert.Equal(2, names.Count(n => n == "Alice"));
        Assert.Single(names, n => n == "Bob");
    }

    [Fact]
    public async Task InnerJoin_WithAliases()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task InnerJoin_WithAsAliases()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT u.name, o.product FROM users AS u JOIN orders AS o ON u.id = o.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task LeftJoin_NullPadding()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Count); // Alice(2) + Bob(1) + Charlie(NULL)

        // Charlie should have NULL product
        var charlie = rows.FirstOrDefault(r => r[0].AsText == "Charlie");
        Assert.NotNull(charlie);
        Assert.True(charlie[1].IsNull);
    }

    [Fact]
    public async Task LeftOuterJoin()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Count);
    }

    [Fact]
    public async Task RightJoin()
    {
        await SetupJoinTables();
        // Add an order with no matching user
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 99, 'Orphan')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        // 3 matched rows + 1 unmatched right row (Orphan with NULL name)
        Assert.Equal(4, rows.Count);

        var orphan = rows.FirstOrDefault(r => r[1].AsText == "Orphan");
        Assert.NotNull(orphan);
        Assert.True(orphan[0].IsNull);
    }

    [Fact]
    public async Task RightJoin_SelectStar_PreservesColumnOrder()
    {
        await SetupJoinTables();
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 99, 'Orphan')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.Equal(5, r.Length)); // users(2) + orders(3)

        // Unmatched right row should have NULL users columns and intact orders columns.
        var orphan = rows.Single(r => r[4].AsText == "Orphan");
        Assert.True(orphan[0].IsNull); // users.id
        Assert.True(orphan[1].IsNull); // users.name
        Assert.Equal(4L, orphan[2].AsInteger); // orders.id
        Assert.Equal(99L, orphan[3].AsInteger); // orders.user_id
    }

    [Fact]
    public async Task RightJoin_WithResidualOnPredicate_NullPadsFilteredRightRows()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id AND orders.product = 'Widget' ORDER BY orders.id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);

        // Order #2 has product=Gadget; ON predicate rejects it, so it is emitted as unmatched right row.
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Widget", rows[0][1].AsText);
        Assert.True(rows[1][0].IsNull);
        Assert.Equal("Gadget", rows[1][1].AsText);
        Assert.Equal("Bob", rows[2][0].AsText);
        Assert.Equal("Widget", rows[2][1].AsText);
    }

    [Fact]
    public async Task CrossJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE colors (id INTEGER, color TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO colors VALUES (1, 'Red')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO colors VALUES (2, 'Blue')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE sizes (id INTEGER, size TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (1, 'S')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (2, 'M')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (3, 'L')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM colors CROSS JOIN sizes", TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<NestedLoopJoinOperator>(GetRootOperator(result));
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(6, rows.Count); // 2 * 3 = 6
    }

    [Fact]
    public async Task Join_WithWhere()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.product = 'Widget'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // Alice+Widget, Bob+Widget
    }

    [Fact]
    public async Task Join_WithResidualOnPredicate()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id AND orders.product = 'Widget'", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("Widget", r[1].AsText));
    }

    [Fact]
    public async Task Join_NonEquiCondition_UsesFallbackPath()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id > orders.user_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Join_OnRightPrimaryKey_UsesLookupPath()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.id ORDER BY users.name", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Bob", rows[1][0].AsText);
        Assert.Equal("Charlie", rows[2][0].AsText);
    }

    [Fact]
    public async Task LeftJoin_OnRightUniqueIndex_UsesLookupPath()
    {
        await _db.ExecuteAsync("CREATE TABLE left_lookup (id INTEGER PRIMARY KEY, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (1, 'L1')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (2, 'L2')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (3, 'L3')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE right_lookup (id INTEGER PRIMARY KEY, code INTEGER, payload TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_lookup_code ON right_lookup(code)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO right_lookup VALUES (10, 1, 'R1')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO right_lookup VALUES (11, 2, 'R2')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM left_lookup l LEFT JOIN right_lookup r ON l.id = r.code ORDER BY l.id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("R1", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("R2", rows[1][1].AsText);
        Assert.Equal(3L, rows[2][0].AsInteger);
        Assert.True(rows[2][1].IsNull);
    }

    [Fact]
    public async Task InnerJoin_OnRightCompositeIndex_UsesHashedLookupPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_comp_lookup (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, label TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO left_comp_lookup VALUES (1, 10, 'alpha', 'L1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_comp_lookup VALUES (2, 20, 'beta', 'L2')", ct);
        await _db.ExecuteAsync("INSERT INTO left_comp_lookup VALUES (3, 30, 'gamma', 'L3')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_comp_lookup (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, amount INTEGER, left_id INTEGER)",
            ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_comp_lookup_ab ON right_comp_lookup(a, b)", ct);
        await _db.ExecuteAsync("INSERT INTO right_comp_lookup VALUES (10, 10, 'alpha', 100, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO right_comp_lookup VALUES (11, 20, 'beta', 200, 2)", ct);
        await _db.ExecuteAsync("INSERT INTO right_comp_lookup VALUES (12, 30, 'gamma', 300, 3)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.label, r.amount FROM left_comp_lookup l JOIN right_comp_lookup r ON l.b = r.b AND l.a = r.a") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<HashedIndexNestedLoopJoinOperator>(rootOperator);

        var orderedOuterKeys = GetPrivateField<int[]>(rootOperator, "_outerKeyIndices");
        var orderedRightKeys = GetPrivateField<int[]>(rootOperator, "_rightKeyColumnIndices");
        var decodedRightColumns = GetPrivateField<int[]>(rootOperator, "_decodedRightColumnIndices");
        Assert.NotNull(orderedOuterKeys);
        Assert.NotNull(orderedRightKeys);
        Assert.Equal([1, 2], orderedOuterKeys!);
        Assert.Equal([1, 2], orderedRightKeys!);
        Assert.NotNull(decodedRightColumns);
        Assert.Equal([3], decodedRightColumns!);

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("L1", rows[0][0].AsText);
        Assert.Equal(100L, rows[0][1].AsInteger);
        Assert.Equal("L2", rows[1][0].AsText);
        Assert.Equal(200L, rows[1][1].AsInteger);
        Assert.Equal("L3", rows[2][0].AsText);
        Assert.Equal(300L, rows[2][1].AsInteger);
    }

    [Fact]
    public async Task LeftJoin_OnRightTextIndex_UsesHashedLookupPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_text_lookup (id INTEGER PRIMARY KEY, code TEXT, label TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO left_text_lookup VALUES (1, 'A-10', 'L1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_text_lookup VALUES (2, 'B-20', 'L2')", ct);
        await _db.ExecuteAsync("INSERT INTO left_text_lookup VALUES (3, 'C-30', 'L3')", ct);

        await _db.ExecuteAsync("CREATE TABLE right_text_lookup (id INTEGER PRIMARY KEY, code TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_text_lookup_code ON right_text_lookup(code)", ct);
        await _db.ExecuteAsync("INSERT INTO right_text_lookup VALUES (10, 'A-10', 'R1')", ct);
        await _db.ExecuteAsync("INSERT INTO right_text_lookup VALUES (11, 'B-20', 'R2')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.id, r.payload FROM left_text_lookup l LEFT JOIN right_text_lookup r ON l.code = r.code") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<HashedIndexNestedLoopJoinOperator>(rootOperator);

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("R1", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("R2", rows[1][1].AsText);
        Assert.Equal(3L, rows[2][0].AsInteger);
        Assert.True(rows[2][1].IsNull);
    }

    [Fact]
    public async Task InnerJoin_OnRightCompositeIndex_CoveredProjection_UsesIndexPayload()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_comp_cov (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, label TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO left_comp_cov VALUES (1, 10, 'alpha', 'L1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_comp_cov VALUES (2, 20, 'beta', 'L2')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_comp_cov (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, amount INTEGER)",
            ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_comp_cov_ab ON right_comp_cov(a, b)", ct);
        await _db.ExecuteAsync("INSERT INTO right_comp_cov VALUES (10, 10, 'alpha', 100)", ct);
        await _db.ExecuteAsync("INSERT INTO right_comp_cov VALUES (11, 20, 'beta', 200)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.label, r.id, r.a, r.b FROM left_comp_cov l JOIN right_comp_cov r ON l.b = r.b AND l.a = r.a") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<HashedIndexNestedLoopJoinOperator>(rootOperator);

        var coveredProjection = GetPrivateField<bool>(rootOperator, "_canProjectRightFromIndexPayload");
        Assert.True(coveredProjection);

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("L1", rows[0][0].AsText);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(10L, rows[0][2].AsInteger);
        Assert.Equal("alpha", rows[0][3].AsText);
        Assert.Equal("L2", rows[1][0].AsText);
        Assert.Equal(11L, rows[1][1].AsInteger);
        Assert.Equal(20L, rows[1][2].AsInteger);
        Assert.Equal("beta", rows[1][3].AsText);
    }

    [Fact]
    public async Task Scan_WithCompoundSimpleWhere_UsesMultiplePreDecodeFilters()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE pushdown_scan (id INTEGER PRIMARY KEY, value INTEGER, category TEXT, note TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO pushdown_scan VALUES (1, 5, 'A', 'skip-low')", ct);
        await _db.ExecuteAsync("INSERT INTO pushdown_scan VALUES (2, 12, 'A', 'match-1')", ct);
        await _db.ExecuteAsync("INSERT INTO pushdown_scan VALUES (3, 15, 'B', 'skip-category')", ct);
        await _db.ExecuteAsync("INSERT INTO pushdown_scan VALUES (4, 19, 'A', 'match-2')", ct);
        await _db.ExecuteAsync("INSERT INTO pushdown_scan VALUES (5, 25, 'A', 'skip-high')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT id FROM pushdown_scan WHERE value >= 10 AND value < 20 AND category = 'A'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.True(
            rootOperator is TableScanOperator or CompactTableScanProjectionOperator,
            $"Expected TableScanOperator or CompactTableScanProjectionOperator, got {rootOperator.GetType().Name}.");
        var scanOperator = rootOperator;

        var extraFilters = GetPrivateField<Array>(scanOperator!, "_additionalPreDecodeFilters");
        Assert.NotNull(extraFilters);
        Assert.Equal(2, extraFilters!.Length);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal([2L, 4L], rows.Select(row => row[0].AsInteger).OrderBy(id => id).ToArray());
    }

    [Fact]
    public async Task InnerJoin_OnRightUniqueIndex_UsesSparseRightProjectionTrim()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_trim (id INTEGER PRIMARY KEY, label TEXT, status TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO left_trim VALUES (1, 'L1', 'open')", ct);
        await _db.ExecuteAsync("INSERT INTO left_trim VALUES (2, 'L2', 'open')", ct);
        await _db.ExecuteAsync("INSERT INTO left_trim VALUES (3, 'L3', 'closed')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_trim (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_trim_code ON right_trim(code)", ct);
        await _db.ExecuteAsync("INSERT INTO right_trim VALUES (10, 1, 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO right_trim VALUES (11, 2, 'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO right_trim VALUES (12, 3, 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'tail-3')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.label, r.tail FROM left_trim l JOIN right_trim r ON l.id = r.code") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<IndexNestedLoopJoinOperator>(rootOperator);
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var decodedRightColumns = GetPrivateField<int[]>(rootOperator, "_decodedRightColumnIndices");
        Assert.NotNull(decodedRightColumns);
        Assert.Equal([8], decodedRightColumns!);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(3, rows.Count);
        var ordered = rows.OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal("L1", ordered[0][0].AsText);
        Assert.Equal("tail-1", ordered[0][1].AsText);
        Assert.Equal("L2", ordered[1][0].AsText);
        Assert.Equal("tail-2", ordered[1][1].AsText);
        Assert.Equal("L3", ordered[2][0].AsText);
        Assert.Equal("tail-3", ordered[2][1].AsText);
    }

    [Fact]
    public async Task InnerJoin_HashJoin_UsesSparseProjectionTrimOnBothSides()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE left_hash_trim (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim VALUES (1, 'l11', 'l12', 'l13', 'l14', 'l15', 'l16', 'left-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim VALUES (2, 'l21', 'l22', 'l23', 'l24', 'l25', 'l26', 'left-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim VALUES (3, 'l31', 'l32', 'l33', 'l34', 'l35', 'l36', 'left-tail-3')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_hash_trim (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim VALUES (10, 1, 'r11', 'r12', 'r13', 'r14', 'r15', 'r16', 'right-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim VALUES (11, 2, 'r21', 'r22', 'r23', 'r24', 'r25', 'r26', 'right-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim VALUES (12, 3, 'r31', 'r32', 'r33', 'r34', 'r35', 'r36', 'right-tail-3')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.tail, r.tail FROM left_hash_trim l JOIN right_hash_trim r ON l.id = r.code") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<HashJoinOperator>(rootOperator);

        var leftSource = GetPrivateField<IOperator>(rootOperator, "_left");
        var rightSource = GetPrivateField<IOperator>(rootOperator, "_right");
        Assert.IsType<TableScanOperator>(leftSource);
        Assert.IsType<TableScanOperator>(rightSource);

        var leftDecodedColumns = GetPrivateField<int[]>(leftSource!, "_decodedColumnIndices");
        var rightDecodedColumns = GetPrivateField<int[]>(rightSource!, "_decodedColumnIndices");

        Assert.NotNull(leftDecodedColumns);
        Assert.NotNull(rightDecodedColumns);
        Assert.Equal([0, 7], leftDecodedColumns!);
        Assert.Equal([1, 8], rightDecodedColumns!);

        var rows = await result.ToListAsync(ct);
        var buildRequiredColumns = GetPrivateField<int[]>(rootOperator, "_buildRequiredColumnIndices");
        Assert.NotNull(buildRequiredColumns);
        Assert.Equal([1, 8], buildRequiredColumns!);

        Assert.Equal(3, rows.Count);
        var ordered = rows.OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal("left-tail-1", ordered[0][0].AsText);
        Assert.Equal("right-tail-1", ordered[0][1].AsText);
        Assert.Equal("left-tail-2", ordered[1][0].AsText);
        Assert.Equal("right-tail-2", ordered[1][1].AsText);
        Assert.Equal("left-tail-3", ordered[2][0].AsText);
        Assert.Equal("right-tail-3", ordered[2][1].AsText);
    }

    [Fact]
    public async Task InnerJoin_HashJoinWithResidual_UsesSparseProjectionTrimAndCompactedResidualPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE left_hash_trim_residual (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim_residual VALUES (1, 'l11', 'l12', 'l13', 'l14', 'l15', 'l16', 'left-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim_residual VALUES (2, 'l21', 'l22', 'l23', 'l24', 'l25', 'l26', 'left-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO left_hash_trim_residual VALUES (3, 'l31', 'l32', 'l33', 'l34', 'l35', 'l36', 'left-tail-3')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_hash_trim_residual (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim_residual VALUES (10, 1, 'r11', 'r12', 'r13', 'r14', 'r15', 'r16', 'right-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim_residual VALUES (11, 2, 'r21', 'r22', 'r23', 'r24', 'r25', 'r26', 'right-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO right_hash_trim_residual VALUES (12, 3, 'r31', 'r32', 'r33', 'r34', 'r35', 'r36', 'right-tail-3')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.tail, r.tail FROM left_hash_trim_residual l JOIN right_hash_trim_residual r ON l.id = r.code AND r.c5 >= 'r25'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<HashJoinOperator>(rootOperator);

        var leftSource = GetPrivateField<IOperator>(rootOperator, "_left");
        var rightSource = GetPrivateField<IOperator>(rootOperator, "_right");
        Assert.IsType<TableScanOperator>(leftSource);
        Assert.IsType<TableScanOperator>(rightSource);

        var leftDecodedColumns = GetPrivateField<int[]?>(leftSource!, "_decodedColumnIndices");
        var rightDecodedColumns = GetPrivateField<int[]?>(rightSource!, "_decodedColumnIndices");
        var buildRequiredColumns = GetPrivateField<int[]?>(rootOperator, "_buildRequiredColumnIndices");
        var buildCompactionEnabled = GetPrivateField<bool>(rootOperator, "_buildRowCompactionEnabled");
        var compactionConfigured =
            leftDecodedColumns is not null ||
            rightDecodedColumns is not null ||
            buildRequiredColumns is not null ||
            buildCompactionEnabled;
        if (compactionConfigured)
        {
            Assert.NotNull(leftDecodedColumns);
            Assert.NotNull(rightDecodedColumns);
            Assert.NotNull(buildRequiredColumns);
            Assert.True(buildCompactionEnabled);
            Assert.Equal([0, 7], leftDecodedColumns!);
            Assert.Equal([1, 6, 8], rightDecodedColumns!);
            Assert.Equal([1, 6, 8], buildRequiredColumns!);
        }

        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        var ordered = rows.OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal("left-tail-2", ordered[0][0].AsText);
        Assert.Equal("right-tail-2", ordered[0][1].AsText);
        Assert.Equal("left-tail-3", ordered[1][0].AsText);
        Assert.Equal("right-tail-3", ordered[1][1].AsText);
    }

    [Fact]
    public async Task InnerJoin_NestedLoop_UsesSparseProjectionTrimOnBothSides()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE left_nested_trim (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO left_nested_trim VALUES (1, 'l11', 'l12', 'l13', 'l14', 'l15', 'l16', 'left-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO left_nested_trim VALUES (2, 'l21', 'l22', 'l23', 'l24', 'l25', 'l26', 'left-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO left_nested_trim VALUES (3, 'l31', 'l32', 'l33', 'l34', 'l35', 'l36', 'left-tail-3')", ct);

        await _db.ExecuteAsync(
            "CREATE TABLE right_nested_trim (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, tail TEXT)",
            ct);
        await _db.ExecuteAsync("INSERT INTO right_nested_trim VALUES (10, 1, 'r11', 'r12', 'r13', 'r14', 'r15', 'r16', 'right-tail-1')", ct);
        await _db.ExecuteAsync("INSERT INTO right_nested_trim VALUES (11, 2, 'r21', 'r22', 'r23', 'r24', 'r25', 'r26', 'right-tail-2')", ct);
        await _db.ExecuteAsync("INSERT INTO right_nested_trim VALUES (12, 3, 'r31', 'r32', 'r33', 'r34', 'r35', 'r36', 'right-tail-3')", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse(
            "SELECT l.tail, r.tail FROM left_nested_trim l JOIN right_nested_trim r ON l.id + 0 = r.code") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        var rootOperator = GetRootOperator(result);
        Assert.IsType<NestedLoopJoinOperator>(rootOperator);
        Assert.IsAssignableFrom<IBatchOperator>(rootOperator);

        var leftSource = GetPrivateField<IOperator>(rootOperator, "_left");
        var rightSource = GetPrivateField<IOperator>(rootOperator, "_right");
        Assert.IsType<TableScanOperator>(leftSource);
        Assert.IsType<TableScanOperator>(rightSource);

        var leftDecodedColumns = GetPrivateField<int[]>(leftSource!, "_decodedColumnIndices");
        var rightDecodedColumns = GetPrivateField<int[]>(rightSource!, "_decodedColumnIndices");
        Assert.NotNull(leftDecodedColumns);
        Assert.NotNull(rightDecodedColumns);
        Assert.Equal([0, 7], leftDecodedColumns!);
        Assert.Equal([1, 8], rightDecodedColumns!);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(3, rows.Count);
        var ordered = rows.OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal("left-tail-1", ordered[0][0].AsText);
        Assert.Equal("right-tail-1", ordered[0][1].AsText);
        Assert.Equal("left-tail-2", ordered[1][0].AsText);
        Assert.Equal("right-tail-2", ordered[1][1].AsText);
        Assert.Equal("left-tail-3", ordered[2][0].AsText);
        Assert.Equal("right-tail-3", ordered[2][1].AsText);
    }

    [Fact]
    public async Task Join_OnRightNonUniqueIndex_ReturnsAllMatches()
    {
        await _db.ExecuteAsync("CREATE TABLE left_many (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO left_many VALUES (1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO left_many VALUES (2)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE right_many (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_right_many_code ON right_many(code)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (10, 1, 'A')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (11, 1, 'B')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (12, 2, 'C')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM left_many l JOIN right_many r ON l.id = r.code ORDER BY l.id, r.payload", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("A", rows[0][1].AsText);
        Assert.Equal(1L, rows[1][0].AsInteger);
        Assert.Equal("B", rows[1][1].AsText);
        Assert.Equal(2L, rows[2][0].AsInteger);
        Assert.Equal("C", rows[2][1].AsText);
    }

    [Fact]
    public async Task Join_WithOrderBy()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Alice", rows[1][0].AsText);
        Assert.Equal("Bob", rows[2][0].AsText);
    }

    [Fact]
    public async Task MultiWayJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO departments VALUES (1, 'Engineering')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO departments VALUES (2, 'Sales')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO employees VALUES (1, 'Alice', 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO employees VALUES (2, 'Bob', 2)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE projects (id INTEGER PRIMARY KEY, emp_id INTEGER, title TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO projects VALUES (1, 1, 'Project X')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO projects VALUES (2, 1, 'Project Y')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO projects VALUES (3, 2, 'Project Z')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT departments.name, employees.name, projects.title " +
            "FROM departments " +
            "JOIN employees ON departments.id = employees.dept_id " +
            "JOIN projects ON employees.id = projects.emp_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task SelfJoin_WithAliases()
    {
        await _db.ExecuteAsync("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO emp VALUES (1, 'Alice', NULL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO emp VALUES (2, 'Bob', 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO emp VALUES (3, 'Charlie', 1)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT e.name, m.name FROM emp e JOIN emp m ON e.manager_id = m.id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // Bob->Alice, Charlie->Alice
        Assert.All(rows, r => Assert.Equal("Alice", r[1].AsText));
    }

    [Fact]
    public async Task Join_WithAggregate()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name", TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));

        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // Alice(2), Bob(1)

        var alice = rows.FirstOrDefault(r => r[0].AsText == "Alice");
        Assert.NotNull(alice);
        Assert.Equal(2, alice[1].AsInteger);
    }

    [Fact]
    public async Task Join_WithScalarAggregate_UsesBatchSourceWithoutRowMaterializationBuffer()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.user_id",
            TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<ScalarAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(3L, rows[0][0].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_batchRowBuffer"));
    }

    [Fact]
    public async Task Join_WithExpressionScalarAggregate_UsesBatchSourceWithoutRowMaterializationBuffer()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT SUM(users.id + orders.id) FROM users JOIN orders ON users.id = orders.user_id",
            TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<ScalarAggregateOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.Null(GetPrivateField<object?>(rootOperator, "_batchRowBuffer"));
    }

    [Fact]
    public async Task Join_WithGroupedFunctionKey_UsesBatchKeyPlan()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT TEXT(users.id), COUNT(*) " +
            "FROM users JOIN orders ON users.id = orders.user_id " +
            "GROUP BY TEXT(users.id)",
            TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.Null(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchKeyPlan"));

        var rows = (await result.ToListAsync(TestContext.Current.CancellationToken))
            .OrderBy(row => row[0].AsText)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(("1", 2L), (rows[0][0].AsText, rows[0][1].AsInteger));
        Assert.Equal(("2", 1L), (rows[1][0].AsText, rows[1][1].AsInteger));
        Assert.Null(GetPrivateField<object?>(rootOperator, "_batchRowBuffer"));
    }

    [Fact]
    public async Task Join_WithGroupedExpressionAggregate_UsesBatchKeyPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE batch_group_key_join_left (id INTEGER PRIMARY KEY, category TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE batch_group_key_join_right (id INTEGER PRIMARY KEY, left_id INTEGER NOT NULL, amount INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_left VALUES (1, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_left VALUES (2, 'A')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_left VALUES (3, 'B')", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_right VALUES (10, 1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_right VALUES (11, 1, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO batch_group_key_join_right VALUES (12, 3, 30)", ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT l.category, SUM(r.amount + 1) " +
            "FROM batch_group_key_join_left l " +
            "JOIN batch_group_key_join_right r ON l.id = r.left_id " +
            "GROUP BY l.category",
            ct);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.Null(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchKeyPlan"));

        var rows = (await result.ToListAsync(ct)).OrderBy(row => row[0].AsText).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("A", rows[0][0].AsText);
        Assert.Equal(32L, rows[0][1].AsInteger);
        Assert.Equal("B", rows[1][0].AsText);
        Assert.Equal(31L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task Join_WithGroupedKeyExpression_UsesBatchKeyPlan()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.id + orders.id, COUNT(*) " +
            "FROM users JOIN orders ON users.id = orders.user_id " +
            "GROUP BY users.id + orders.id",
            TestContext.Current.CancellationToken);
        var rootOperator = Assert.IsType<HashAggregateOperator>(GetRootOperator(result));
        Assert.Null(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchPlan"));
        Assert.NotNull(GetPrivateField<object?>(rootOperator, "_simpleGroupedBatchKeyPlan"));

        var rows = (await result.ToListAsync(TestContext.Current.CancellationToken))
            .OrderBy(row => row[0].AsInteger)
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal((2L, 1L), (rows[0][0].AsInteger, rows[0][1].AsInteger));
        Assert.Equal((3L, 1L), (rows[1][0].AsInteger, rows[1][1].AsInteger));
        Assert.Equal((5L, 1L), (rows[2][0].AsInteger, rows[2][1].AsInteger));
    }

    [Fact]
    public async Task LeftJoin_EmptyRight()
    {
        await _db.ExecuteAsync("CREATE TABLE a (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO a VALUES (1, 'x')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO a VALUES (2, 'y')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE b (id INTEGER, a_id INTEGER, data TEXT)", TestContext.Current.CancellationToken);
        // b is empty

        await using var result = await _db.ExecuteAsync(
            "SELECT a.val, b.data FROM a LEFT JOIN b ON a.id = b.a_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.True(r[1].IsNull));
    }

    #endregion

    #region ALTER TABLE

    [Fact]
    public async Task AlterTable_AddColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN age INTEGER", TestContext.Current.CancellationToken);

        // Old rows should have NULL for the new column
        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(3, rows[0].Length); // id, name, age
        Assert.True(rows[0][2].IsNull);
        Assert.True(rows[1][2].IsNull);
    }

    [Fact]
    public async Task AlterTable_AddColumn_InsertAfter()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t ADD email TEXT", TestContext.Current.CancellationToken);

        // Insert with new column
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'bob@test.com')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.True(rows[0][2].IsNull); // Alice's email is NULL
        Assert.Equal("bob@test.com", rows[1][2].AsText); // Bob's email set
    }

    [Fact]
    public async Task AlterTable_AddColumn_WithNamedInsert()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN score INTEGER", TestContext.Current.CancellationToken);

        // Insert using named columns
        await _db.ExecuteAsync("INSERT INTO t (id, name, score) VALUES (2, 'Bob', 95)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE score = 95", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("Bob", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN age", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // only id and name
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropColumn_MiddleColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, temp TEXT, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'x', 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'y', 'Bob')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t DROP temp", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // id and name
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("Alice", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropPrimaryKey_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN id", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AlterTable_DropLastColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER)", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN id", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AlterTable_RenameTable()
    {
        await _db.ExecuteAsync("CREATE TABLE old_name (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO old_name VALUES (1, 'hello')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE old_name RENAME TO new_name", TestContext.Current.CancellationToken);

        // Old name should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("SELECT * FROM old_name", TestContext.Current.CancellationToken));

        // New name should work
        await using var result = await _db.ExecuteAsync("SELECT * FROM new_name", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("hello", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_RenameColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, old_name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t RENAME COLUMN old_name TO new_name", TestContext.Current.CancellationToken);

        // Query using new column name
        await using var result = await _db.ExecuteAsync("SELECT new_name FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][0].AsText);
    }

    [Fact]
    public async Task AlterTable_RenameColumn_OldNameFails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, old_col TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'test')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("ALTER TABLE t RENAME old_col TO new_col", TestContext.Current.CancellationToken);

        // Old column name should fail when iterating
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync("SELECT old_col FROM t", TestContext.Current.CancellationToken);
            await result.ToListAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task AlterTable_AddDuplicateColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN name TEXT", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AlterTable_DropNonexistentColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN doesntexist", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AlterTable_RenameTable_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE orig (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orig VALUES (1, 'data')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("ALTER TABLE orig RENAME TO renamed", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM renamed", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("data", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_AddColumn_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN age INTEGER", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Length);
        Assert.True(rows[0][2].IsNull); // old row still has NULL for added column
    }

    #endregion

    #region INDEXES

    [Fact]
    public async Task CreateIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 100)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // The index should be used for this equality lookup
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 100", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(100, r[1].AsInteger));
    }

    [Fact]
    public async Task CreateIndex_NonUniqueIntegerBackfill_WithManyDuplicates()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)", ct);

        const int rowCount = 100_000;
        const int batchSize = 500;
        for (int start = 0; start < rowCount; start += batchSize)
        {
            await _db.BeginTransactionAsync(ct);
            try
            {
                int end = Math.Min(start + batchSize, rowCount);
                for (int i = start; i < end; i++)
                    await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, {i % 256}, 'payload_{i}')", ct);

                await _db.CommitAsync(ct);
            }
            catch
            {
                await _db.RollbackAsync(ct);
                throw;
            }
        }

        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM t WHERE val = 5 ORDER BY id",
            ct);
        var rows = await result.ToListAsync(ct);
        Assert.NotEmpty(rows);
        Assert.All(rows, row => Assert.Equal(5, row[0].AsInteger % 256));
    }

    [Fact]
    public async Task CreateIndex_IfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Should not throw
        await _db.ExecuteAsync("CREATE INDEX IF NOT EXISTS idx_val ON t (val)", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CreateIndex_Duplicate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DropIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DROP INDEX idx_val", TestContext.Current.CancellationToken);

        // Queries should still work (full scan)
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 100", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task DropIndex_IfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP INDEX IF EXISTS nonexistent", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DropIndex_Nonexistent_Fails()
    {
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP INDEX nonexistent", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, email_hash INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 1001)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 1002)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_email ON t (email_hash)", TestContext.Current.CancellationToken);

        // Inserting a duplicate should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (3, 1001)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_CreateOnDuplicateData_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 100)", TestContext.Current.CancellationToken); // duplicate val

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Index_MaintainedOnInsert()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Insert after index creation
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 42)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Index_MaintainedOnInsert_WithManyDuplicateKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", ct);

        const int rowCount = 100_000;
        const int batchSize = 500;
        for (int start = 0; start < rowCount; start += batchSize)
        {
            await _db.BeginTransactionAsync(ct);
            try
            {
                int end = Math.Min(start + batchSize, rowCount);
                for (int i = start; i < end; i++)
                    await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, {i % 256}, 'payload_{i}')", ct);

                await _db.CommitAsync(ct);
            }
            catch
            {
                await _db.RollbackAsync(ct);
                throw;
            }
        }

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM t WHERE val = 5 ORDER BY id",
            ct);
        var rows = await result.ToListAsync(ct);
        Assert.NotEmpty(rows);
        Assert.All(rows, row => Assert.Equal(5, row[0].AsInteger % 256));
    }

    [Fact]
    public async Task Index_MaintainedOnDelete()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("DELETE FROM t WHERE id = 1", TestContext.Current.CancellationToken);

        // val=42 should no longer be found
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Index_MaintainedOnUpdate()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("UPDATE t SET val = 50 WHERE id = 1", TestContext.Current.CancellationToken);

        // Old value gone
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42", TestContext.Current.CancellationToken);
            var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Empty(rows);
        }

        // New value present
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 50", TestContext.Current.CancellationToken);
            var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Single(rows);
            Assert.Equal(1, rows[0][0].AsInteger);
        }
    }

    [Fact]
    public async Task Index_MaintainedOnUpdate_WithManyDuplicateKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", ct);

        const int rowCount = 20_000;
        const int batchSize = 500;
        for (int start = 0; start < rowCount; start += batchSize)
        {
            await _db.BeginTransactionAsync(ct);
            try
            {
                int end = Math.Min(start + batchSize, rowCount);
                for (int i = start; i < end; i++)
                    await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, {i % 256}, 'payload_{i}')", ct);

                await _db.CommitAsync(ct);
            }
            catch
            {
                await _db.RollbackAsync(ct);
                throw;
            }
        }

        await _db.ExecuteAsync("UPDATE t SET val = 6 WHERE val = 5", ct);

        static long CountMatches(int totalRows, int moduloValue)
        {
            if (totalRows <= moduloValue)
                return 0;

            return ((totalRows - 1 - moduloValue) / 256) + 1;
        }

        await using var missingResult = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM t WHERE val = 5",
            ct);
        var missingRows = await missingResult.ToListAsync(ct);
        Assert.Single(missingRows);
        Assert.Equal(0, missingRows[0][0].AsInteger);

        await using var expandedResult = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM t WHERE val = 6",
            ct);
        var expandedRows = await expandedResult.ToListAsync(ct);
        Assert.Single(expandedRows);
        long expectedSixCount = CountMatches(rowCount, 6) + CountMatches(rowCount, 5);
        Assert.Equal(expectedSixCount, expandedRows[0][0].AsInteger);
    }

    [Fact]
    public async Task UniqueIndex_ViolationOnUpdate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Updating to a value that already exists should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("UPDATE t SET val = 100 WHERE id = 2", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Index_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        // Index should still work for lookups
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task UniqueIndex_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        // Unique constraint should persist
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (2, 100)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Index_DropTableDropsIndexes()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DROP TABLE t", TestContext.Current.CancellationToken);

        // Re-creating the same index name should work since the old one was dropped
        await _db.ExecuteAsync("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t2 (val)", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Index_NullValuesNotIndexed()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t (id) VALUES (2)", TestContext.Current.CancellationToken); // val is NULL
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)", TestContext.Current.CancellationToken);

        // Should only find the non-null row
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Index_OnTextColumn_Works()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Alice')", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE INDEX idx_name ON t (name)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM t WHERE name = 'Alice' ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(3, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task SimpleLookupPlanner_TextIndexLookup_SelectsPrimaryKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'p2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Alice', 'p3')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_name ON t (name)", ct);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT id FROM t WHERE name = 'Alice'",
            out var lookup));

        var planner = GetPlanner();
        Assert.True(planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult));

        await using var result = fastResult;
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal([1L, 3L], rows.Select(row => row[0].AsInteger).OrderBy(id => id).ToArray());
    }

    [Fact]
    public async Task SimpleLookupPlanner_TextIndexLookup_SelectsIndexedTextColumn()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'p2')", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_name ON t (name)", ct);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT name FROM t WHERE name = 'Alice'",
            out var lookup));

        var planner = GetPlanner();
        Assert.True(planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult));

        await using var result = fastResult;
        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][0].AsText);
    }

    [Fact]
    public async Task SimpleLookupPlanner_PrimaryKeyLookup_SelectsMultipleColumns()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 30)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 31)", ct);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT id, name FROM t WHERE id = 1",
            out var lookup));

        var planner = GetPlanner();
        Assert.True(planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult));

        await using var result = fastResult;
        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("Alice", rows[0][1].AsText);
    }

    [Fact]
    public async Task SimpleLookupPlanner_IntegerIndexLookup_SelectsCoveredColumns()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, group_id INTEGER, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 5, 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 7, 'p2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 5, 'p3')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_group_id ON t (group_id)", ct);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT id, group_id FROM t WHERE group_id = 5",
            out var lookup));

        var planner = GetPlanner();
        Assert.True(planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult));

        await using var result = fastResult;
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        var pairs = rows
            .Select(row => (Id: row[0].AsInteger, GroupId: row[1].AsInteger))
            .OrderBy(pair => pair.Id)
            .ToArray();
        Assert.Equal([(1L, 5L), (3L, 5L)], pairs);
    }

    [Fact]
    public async Task SimpleLookupPlanner_TextIndexLookup_SelectsMultipleColumns()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'p2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Alice', 'p3')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_name ON t (name)", ct);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT id, name FROM t WHERE name = 'Alice'",
            out var lookup));

        var planner = GetPlanner();
        Assert.True(planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult));

        await using var result = fastResult;
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        var pairs = rows
            .Select(row => (Id: row[0].AsInteger, Name: row[1].AsText))
            .OrderBy(pair => pair.Id)
            .ToArray();
        Assert.Equal([(1L, "Alice"), (3L, "Alice")], pairs);
    }

    [Fact]
    public async Task DatabaseApi_SimpleLookup_IntegerIndexLookup_SelectsCoveredColumns()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, group_id INTEGER, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 5, 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 7, 'p2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 5, 'p3')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_group_id ON t (group_id)", ct);

        await using var result = await _db.ExecuteAsync("SELECT id, group_id FROM t WHERE group_id = 5", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        var pairs = rows
            .Select(row => (Id: row[0].AsInteger, GroupId: row[1].AsInteger))
            .OrderBy(pair => pair.Id)
            .ToArray();
        Assert.Equal([(1L, 5L), (3L, 5L)], pairs);
    }

    [Fact]
    public async Task DatabaseApi_SimpleLookup_TextIndexLookup_SelectsMultipleColumns()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 'p1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'p2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Alice', 'p3')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_name ON t (name)", ct);

        await using var result = await _db.ExecuteAsync("SELECT id, name FROM t WHERE name = 'Alice'", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        var pairs = rows
            .Select(row => (Id: row[0].AsInteger, Name: row[1].AsText))
            .OrderBy(pair => pair.Id)
            .ToArray();
        Assert.Equal([(1L, "Alice"), (3L, "Alice")], pairs);
    }

    [Fact]
    public async Task Index_OnUnsupportedColumnType_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, score REAL)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE INDEX idx_score ON t (score)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Index_MultiColumn_CanCreateAndQuery()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, payload INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 20, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 21, 101)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 11, 20, 102)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (4, 10, 20, 103)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE INDEX idx_ab ON t (a, b)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "SELECT id, payload FROM t WHERE a = 10 AND b = 20 ORDER BY id",
            TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(4, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task Index_MultiColumn_CoveredProjection_UsesHashedIndexProjectionLookup()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 'x', 'keep-1')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 'x', 'keep-2')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 10, 'y', 'skip')", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_ab ON t (a, b)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT id, a, b FROM t WHERE a = 10 AND b = 'x'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<HashedIndexProjectionLookupOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        var projected = rows
            .Select(row => (Id: row[0].AsInteger, A: row[1].AsInteger, B: row[2].AsText))
            .OrderBy(row => row.Id)
            .ToArray();
        Assert.Equal([(1L, 10L, "x"), (2L, 10L, "x")], projected);
    }

    [Fact]
    public async Task UniqueIndex_MultiColumn_CoveredProjection_UsesHashedIndexProjectionLookup()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 'x', 'keep')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 'y', 'skip')", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_ab ON t (a, b)", ct);

        var planner = GetPlanner();
        var statement = Parser.Parse("SELECT a, b, id FROM t WHERE a = 10 AND b = 'x'") as SelectStatement
            ?? throw new InvalidOperationException("Expected SELECT statement.");

        await using var result = await planner.ExecuteAsync(statement, ct);
        Assert.IsType<HashedIndexProjectionLookupOperator>(GetRootOperator(result));

        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.Equal("x", rows[0][1].AsText);
        Assert.Equal(1L, rows[0][2].AsInteger);
    }

    [Fact]
    public async Task UniqueIndex_MultiColumn_EnforcesTupleUniqueness()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_ab ON t (a, b)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 21)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (3, 10, 20)", TestContext.Current.CancellationToken));

        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 11, 20)", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task UniqueIndex_MultiColumn_CreateOnDuplicateData_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 20)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_ab ON t (a, b)", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_MultiColumn_ViolationOnUpdate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10, 21)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_ab ON t (a, b)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("UPDATE t SET b = 20 WHERE id = 2", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_Text_EnforcesUniqueness()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_email ON t (email)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'a@x.com')", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'a@x.com')", TestContext.Current.CancellationToken));

        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'b@x.com')", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task UniqueIndex_Text_ViolationOnUpdate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'a@x.com')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'b@x.com')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_email ON t (email)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("UPDATE t SET email = 'a@x.com' WHERE id = 2", TestContext.Current.CancellationToken));
    }

    #endregion

    #region VIEWS

    [Fact]
    public async Task CreateView_And_Query()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie', 35)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM adults", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r[1].AsText == "Alice");
        Assert.Contains(rows, r => r[1].AsText == "Charlie");
    }

    [Fact]
    public async Task CreateView_IfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t", TestContext.Current.CancellationToken);

        // Should not throw
        await _db.ExecuteAsync("CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CreateView_Duplicate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateView_ConflictsWithTable_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE VIEW t AS SELECT * FROM t", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DropView_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DROP VIEW v", TestContext.Current.CancellationToken);

        // Querying the dropped view should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM v", TestContext.Current.CancellationToken);
            await result.ToListAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task DropView_IfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP VIEW IF EXISTS nonexistent", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DropView_Nonexistent_Fails()
    {
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP VIEW nonexistent", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task View_WithProjection()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW names AS SELECT name FROM users", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM names", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Single(rows[0]); // Only the name column
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Bob", rows[1][0].AsText);
    }

    [Fact]
    public async Task View_WithWhereOnView()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 50)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW cheap AS SELECT * FROM items WHERE price < 40", TestContext.Current.CancellationToken);

        // Query the view with an additional WHERE
        await using var result = await _db.ExecuteAsync("SELECT * FROM cheap WHERE price > 15", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("Gadget", rows[0][1].AsText);
    }

    [Fact]
    public async Task View_ReflectsDataChanges()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t", TestContext.Current.CancellationToken);

        // Insert more data
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM v", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count); // View should reflect the new row
    }

    [Fact]
    public async Task View_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW seniors AS SELECT * FROM users WHERE age >= 30", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM seniors", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][1].AsText);
    }

    [Fact]
    public async Task View_WithJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 2, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 1, 150)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW order_details AS SELECT orders.id, users.name, orders.amount FROM orders JOIN users ON orders.user_id = users.id", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM order_details", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task View_WithAggregate()
    {
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 1, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 2, 300)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE VIEW category_totals AS SELECT category, SUM(amount) AS total FROM sales GROUP BY category", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM category_totals", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    #endregion

    #region CTEs (Common Table Expressions)

    [Fact]
    public async Task Cte_SingleBasic()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie', 35)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH adults AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM adults", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Charlie", rows[1][1].AsText);
    }

    [Fact]
    public async Task Cte_WithProjection()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 50)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 30)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH expensive AS (SELECT name, price FROM items WHERE price > 20) SELECT * FROM expensive", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // name and price only
    }

    [Fact]
    public async Task Cte_WithWhereOnMainQuery()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (4, 40)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH big AS (SELECT * FROM t WHERE val > 15) SELECT * FROM big WHERE val < 35", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task Cte_MultipleCtes()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'A', 10, 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (2, 'B', 20, 1)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (3, 'C', 30, 2)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO products VALUES (4, 'D', 40, 2)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH cheap AS (SELECT * FROM products WHERE price <= 20), " +
            "expensive AS (SELECT * FROM products WHERE price > 20) " +
            "SELECT * FROM cheap", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("A", rows[0][1].AsText);
        Assert.Equal("B", rows[1][1].AsText);
    }

    [Fact]
    public async Task Cte_ReferencedInJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 2, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 1, 150)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH big_orders AS (SELECT * FROM orders WHERE amount >= 150) " +
            "SELECT users.name, big_orders.amount FROM big_orders JOIN users ON big_orders.user_id = users.id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Cte_ReferencedMultipleTimes()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 30)", TestContext.Current.CancellationToken);

        // Use the CTE twice in a CROSS JOIN
        await using var result = await _db.ExecuteAsync(
            "WITH small AS (SELECT * FROM nums WHERE val <= 20) " +
            "SELECT a.val, b.val FROM small a CROSS JOIN small b", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Count); // 2 x 2 = 4
    }

    [Fact]
    public async Task Cte_WithExplicitColumnNames()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 90)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 80)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH renamed(student_id, student_name, student_score) AS (SELECT * FROM t) " +
            "SELECT student_name FROM renamed WHERE student_score > 85", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][0].AsText);
    }

    [Fact]
    public async Task Cte_WithAggregate()
    {
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 1, 100)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 1, 200)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 2, 300)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO sales VALUES (4, 2, 400)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH totals AS (SELECT category, SUM(amount) AS total FROM sales GROUP BY category) " +
            "SELECT * FROM totals WHERE total > 500", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(700L, rows[0][1].AsInteger);
    }

    [Fact]
    public async Task Cte_EmptyResult()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH empty AS (SELECT * FROM t WHERE val > 100) SELECT * FROM empty", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Cte_WithOrderByAndLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 20)", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync(
            "WITH all_rows AS (SELECT * FROM t) SELECT * FROM all_rows ORDER BY val LIMIT 2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(20L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task Cte_ChainedCteReferencingPrior()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 30)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO t VALUES (4, 40)", TestContext.Current.CancellationToken);

        // Second CTE references the first
        await using var result = await _db.ExecuteAsync(
            "WITH step1 AS (SELECT * FROM t WHERE val >= 20), " +
            "step2 AS (SELECT * FROM step1 WHERE val <= 30) " +
            "SELECT * FROM step2", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
    }

    #endregion

    #region Triggers

    [Fact]
    public async Task Trigger_AfterInsert_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, item_name TEXT)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_insert AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO audit_log VALUES (NEW.id, 'INSERT', NEW.name); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM audit_log ORDER BY id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("INSERT", rows[0][1].AsText);
        Assert.Equal("Widget", rows[0][2].AsText);
        Assert.Equal("INSERT", rows[1][1].AsText);
        Assert.Equal("Gadget", rows[1][2].AsText);
    }

    [Fact]
    public async Task Trigger_BeforeInsert_Validation()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE rejected (id INTEGER PRIMARY KEY, reason TEXT)", TestContext.Current.CancellationToken);

        // This trigger fires before insert; it inserts into rejected log
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_products_validate BEFORE INSERT ON products " +
            "BEGIN " +
            "INSERT INTO rejected VALUES (NEW.id, 'checked'); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'Laptop', 999.99)", TestContext.Current.CancellationToken);

        // Both tables should have data
        await using var prodResult = await _db.ExecuteAsync("SELECT * FROM products", TestContext.Current.CancellationToken);
        var prodRows = await prodResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(prodRows);

        await using var rejResult = await _db.ExecuteAsync("SELECT * FROM rejected", TestContext.Current.CancellationToken);
        var rejRows = await rejResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rejRows);
        Assert.Equal("checked", rejRows[0][1].AsText);
    }

    [Fact]
    public async Task Trigger_AfterDelete_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE delete_log (id INTEGER PRIMARY KEY, deleted_name TEXT)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_delete AFTER DELETE ON items " +
            "BEGIN " +
            "INSERT INTO delete_log VALUES (OLD.id, OLD.name); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DELETE FROM items WHERE id = 1", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM delete_log", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("Widget", rows[0][1].AsText);
    }

    [Fact]
    public async Task Trigger_BeforeDelete()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE pre_delete_log (item_id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_before_delete BEFORE DELETE ON items " +
            "BEGIN " +
            "INSERT INTO pre_delete_log VALUES (OLD.id); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'A')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("DELETE FROM items WHERE id = 2", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM pre_delete_log", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Trigger_AfterUpdate_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE update_log (item_id INTEGER PRIMARY KEY, old_qty INTEGER, new_qty INTEGER)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_update AFTER UPDATE ON items " +
            "BEGIN " +
            "INSERT INTO update_log VALUES (NEW.id, OLD.qty, NEW.qty); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("UPDATE items SET qty = 20 WHERE id = 1", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM update_log", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(20L, rows[0][2].AsInteger);
    }

    [Fact]
    public async Task Trigger_BeforeUpdate()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE price_changes (item_id INTEGER PRIMARY KEY, old_price REAL)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_before_update BEFORE UPDATE ON items " +
            "BEGIN " +
            "INSERT INTO price_changes VALUES (OLD.id, OLD.price); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 9.99)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("UPDATE items SET price = 14.99 WHERE id = 1", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM price_changes", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(9.99, rows[0][1].AsReal);
    }

    [Fact]
    public async Task Trigger_DropTrigger()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_log AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO log VALUES (NEW.id, 'inserted'); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'A')", TestContext.Current.CancellationToken);

        // Verify trigger fired
        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log", TestContext.Current.CancellationToken);
        var rows1 = await r1.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows1);

        // Drop trigger
        await _db.ExecuteAsync("DROP TRIGGER trg_log", TestContext.Current.CancellationToken);

        // Insert again — trigger should NOT fire
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')", TestContext.Current.CancellationToken);

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log", TestContext.Current.CancellationToken);
        var rows2 = await r2.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows2); // Still 1 row
    }

    [Fact]
    public async Task Trigger_DropTriggerIfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS nonexistent_trigger", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS trg1", TestContext.Current.CancellationToken);

        // Should not throw again
        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS trg1", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Trigger_CreateIfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END", TestContext.Current.CancellationToken);

        // Should not throw
        await _db.ExecuteAsync(
            "CREATE TRIGGER IF NOT EXISTS trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Trigger_MultipleTriggersSameEvent()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log1 (item_id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log2 (item_id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON items BEGIN INSERT INTO log1 VALUES (NEW.id); END", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg2 AFTER INSERT ON items BEGIN INSERT INTO log2 VALUES (NEW.id); END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')", TestContext.Current.CancellationToken);

        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log1", TestContext.Current.CancellationToken);
        var rows1 = await r1.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows1);

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log2", TestContext.Current.CancellationToken);
        var rows2 = await r2.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows2);
    }

    [Fact]
    public async Task Trigger_Persistence_CloseAndReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log (item_id INTEGER PRIMARY KEY, action TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_insert AFTER INSERT ON items " +
            "BEGIN INSERT INTO log VALUES (NEW.id, 'created'); END", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')", TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')", TestContext.Current.CancellationToken);

        await using var result = await _db.ExecuteAsync("SELECT * FROM log ORDER BY item_id", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("created", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("created", rows[1][1].AsText);
    }

    [Fact]
    public async Task Trigger_MultipleBodyStatements()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log1 (item_id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log2 (item_name TEXT)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_multi AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO log1 VALUES (NEW.id); " +
            "INSERT INTO log2 VALUES (NEW.name); " +
            "END", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')", TestContext.Current.CancellationToken);

        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log1", TestContext.Current.CancellationToken);
        var rows1 = await r1.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows1);
        Assert.Equal(1L, rows1[0][0].AsInteger);

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log2", TestContext.Current.CancellationToken);
        var rows2 = await r2.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows2);
        Assert.Equal("Widget", rows2[0][0].AsText);
    }

    [Fact]
    public async Task Trigger_DuplicateThrows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)", TestContext.Current.CancellationToken);

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END", TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync(
                "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END", TestContext.Current.CancellationToken));
        Assert.Equal(ErrorCode.TriggerAlreadyExists, ex.Code);
    }

    [Fact]
    public async Task Trigger_DropNonexistentThrows()
    {
        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP TRIGGER nonexistent", TestContext.Current.CancellationToken));
        Assert.Equal(ErrorCode.TriggerNotFound, ex.Code);
    }

    private QueryPlanner GetPlanner()
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Database planner field not found.");
        return (QueryPlanner?)plannerField.GetValue(_db)
            ?? throw new InvalidOperationException("Database planner was not initialized.");
    }

    private static IOperator GetStoredOperator(QueryResult result)
    {
        var operatorField = typeof(QueryResult).GetField("_operator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field not found.");
        var storedOperator = (IOperator?)operatorField.GetValue(result);
        if (storedOperator != null)
            return storedOperator;

        var batchOperatorField = typeof(QueryResult).GetField("_batchOperator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
        return (IOperator?)batchOperatorField.GetValue(result)
            ?? throw new InvalidOperationException("QueryResult did not contain an operator.");
    }

    private static IOperator GetRootOperator(QueryResult result)
    {
        var storedOperator = GetStoredOperator(result);
        return storedOperator is BatchToRowOperatorAdapter batchAdapter
            ? batchAdapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : storedOperator;
    }

    private static bool UsesSyncLookupResult(QueryResult result)
    {
        var hasSyncLookupField = typeof(QueryResult).GetField("_hasSyncLookupResult", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult sync lookup field not found.");
        return (bool)hasSyncLookupField.GetValue(result)!;
    }

    private static bool UsesDirectBatchStorage(QueryResult result)
    {
        var batchOperatorField = typeof(QueryResult).GetField("_batchOperator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
        return batchOperatorField.GetValue(result) is not null;
    }

    private static T? GetPrivateField<T>(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' not found on {target.GetType().Name}.");
        return (T?)field.GetValue(target);
    }

    private static TOperator FindOperatorInUnaryChain<TOperator>(IOperator? start)
        where TOperator : class, IOperator
    {
        for (var current = start; current != null; current = current is IUnaryOperatorSource unary ? unary.Source : null)
        {
            if (current is TOperator typed)
                return typed;
        }

        throw new Xunit.Sdk.XunitException($"Expected to find {typeof(TOperator).Name} in unary operator chain.");
    }

    #endregion
}
