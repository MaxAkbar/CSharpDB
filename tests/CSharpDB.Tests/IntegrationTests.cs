using CSharpDB.Core;
using CSharpDB.Engine;

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
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM users");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task Select_WithWhere()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT, price REAL)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 9.99)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 29.99)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 4.99)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM items WHERE price > 5.0");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Select_WithLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (val INTEGER)");
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i})");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums LIMIT 3");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task Update_Rows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'old')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'old')");

        var updateResult = await _db.ExecuteAsync("UPDATE t SET val = 'new' WHERE id = 1");
        Assert.Equal(1, updateResult.RowsAffected);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 'new'");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Delete_Rows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'Charlie')");

        var deleteResult = await _db.ExecuteAsync("DELETE FROM t WHERE id = 2");
        Assert.Equal(1, deleteResult.RowsAffected);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task DropTable()
    {
        await _db.ExecuteAsync("CREATE TABLE temp (x INTEGER)");
        await _db.ExecuteAsync("DROP TABLE temp");
        await Assert.ThrowsAsync<CSharpDbException>(async () => await _db.ExecuteAsync("SELECT * FROM temp"));
    }

    [Fact]
    public async Task Persistence_AcrossReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE persist (id INTEGER, data TEXT)");
        await _db.ExecuteAsync("INSERT INTO persist VALUES (1, 'hello')");
        await _db.ExecuteAsync("INSERT INTO persist VALUES (2, 'world')");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM persist");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }

    [Fact]
    public async Task Persistence_PrimaryKeyLookupManyRows_AcrossReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE persist_big (id INTEGER PRIMARY KEY, val INTEGER)");

        await _db.BeginTransactionAsync();
        for (int i = 0; i < 5000; i++)
            await _db.ExecuteAsync($"INSERT INTO persist_big VALUES ({i}, {i * 10})");
        await _db.CommitAsync();

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT val FROM persist_big WHERE id = 4999");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(49990, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Transaction_Rollback()
    {
        await _db.ExecuteAsync("CREATE TABLE tx (id INTEGER)");
        await _db.ExecuteAsync("INSERT INTO tx VALUES (1)");

        await _db.BeginTransactionAsync();
        await _db.ExecuteAsync("INSERT INTO tx VALUES (2)");
        await _db.ExecuteAsync("INSERT INTO tx VALUES (3)");
        await _db.RollbackAsync();

        await using var result = await _db.ExecuteAsync("SELECT * FROM tx");
        var rows = await result.ToListAsync();
        Assert.Single(rows); // only row 1 should remain
    }

    [Fact]
    public async Task Select_OrderBy()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (3, 'Charlie')");
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO sorted VALUES (2, 'Bob')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM sorted ORDER BY name ASC");
        var rows = await result.ToListAsync();
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
        Assert.Equal("Charlie", rows[2][1].AsText);
    }

    [Fact]
    public async Task Select_OrderByIndexedColumn_WithLimitAndOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_indexed (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_indexed_val ON sorted_indexed(val)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (1, 50)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (2, 10)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (3, 40)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (4, 20)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (5, 60)");
        await _db.ExecuteAsync("INSERT INTO sorted_indexed VALUES (6, 30)");

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_indexed ORDER BY val ASC LIMIT 3 OFFSET 2");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(30L, rows[0][0].AsInteger);
        Assert.Equal(40L, rows[1][0].AsInteger);
        Assert.Equal(50L, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderByIndexedNullableColumn_IncludesNullRows()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_nullable_idx (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_sorted_nullable_idx_val ON sorted_nullable_idx(val)");
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (1, 20)");
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (2, NULL)");
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (3, 10)");
        await _db.ExecuteAsync("INSERT INTO sorted_nullable_idx VALUES (4, 30)");

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_nullable_idx ORDER BY val ASC");
        var rows = await result.ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.True(rows[0][0].IsNull);
        Assert.Equal(10L, rows[1][0].AsInteger);
        Assert.Equal(20L, rows[2][0].AsInteger);
        Assert.Equal(30L, rows[3][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderBy_WithLimitAndOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_nums (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (1, 50)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (2, 10)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (3, 40)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (4, 20)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (5, 60)");
        await _db.ExecuteAsync("INSERT INTO sorted_nums VALUES (6, 30)");

        await using var result = await _db.ExecuteAsync(
            "SELECT val FROM sorted_nums ORDER BY val ASC LIMIT 3 OFFSET 2");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(30L, rows[0][0].AsInteger);
        Assert.Equal(40L, rows[1][0].AsInteger);
        Assert.Equal(50L, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_OrderByExpression_WithLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE sorted_expr (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (2, 40)");
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (3, 15)");
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (4, 30)");
        await _db.ExecuteAsync("INSERT INTO sorted_expr VALUES (5, 1)");

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM sorted_expr ORDER BY val + id DESC LIMIT 2");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(4L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task NullValues()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (1, NULL)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable");
        var rows = await result.ToListAsync();
        Assert.True(rows[0][1].IsNull);
    }

    [Fact]
    public async Task Select_WithOffset()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (val INTEGER)");
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i})");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums LIMIT 3 OFFSET 5");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(6, rows[0][0].AsInteger);
        Assert.Equal(7, rows[1][0].AsInteger);
        Assert.Equal(8, rows[2][0].AsInteger);
    }

    [Fact]
    public async Task Select_WithOffset_BeyondRows()
    {
        await _db.ExecuteAsync("CREATE TABLE small (val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO small VALUES (1)");
        await _db.ExecuteAsync("INSERT INTO small VALUES (2)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM small LIMIT 10 OFFSET 5");
        var rows = await result.ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Select_OffsetWithoutLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE nums2 (val INTEGER)");
        for (int i = 1; i <= 5; i++)
            await _db.ExecuteAsync($"INSERT INTO nums2 VALUES ({i})");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums2 OFFSET 3");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(4, rows[0][0].AsInteger);
        Assert.Equal(5, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task MultipleInserts_ManyRows()
    {
        await _db.ExecuteAsync("CREATE TABLE big (id INTEGER, val INTEGER)");

        await _db.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
            await _db.ExecuteAsync($"INSERT INTO big VALUES ({i}, {i * 10})");
        await _db.CommitAsync();

        await using var result = await _db.ExecuteAsync("SELECT * FROM big");
        var rows = await result.ToListAsync();
        Assert.Equal(100, rows.Count);
    }

    [Fact]
    public async Task PrimaryKeyLookup_ManyRows()
    {
        await _db.ExecuteAsync("CREATE TABLE bigpk (id INTEGER PRIMARY KEY, val INTEGER)");

        await _db.BeginTransactionAsync();
        for (int i = 0; i < 5000; i++)
            await _db.ExecuteAsync($"INSERT INTO bigpk VALUES ({i}, {i * 10})");
        await _db.CommitAsync();

        await using var result = await _db.ExecuteAsync("SELECT val FROM bigpk WHERE id = 4999");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(49990, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task PrimaryKeyLookup_ProjectOnlyPrimaryKeyColumns_RemainsCorrect()
    {
        await _db.ExecuteAsync("CREATE TABLE pk_projection (id INTEGER PRIMARY KEY, payload TEXT)");
        await _db.ExecuteAsync("INSERT INTO pk_projection VALUES (7, 'x')");

        await using var hitResult = await _db.ExecuteAsync(
            "SELECT id, id AS id_alias FROM pk_projection WHERE id = 7");
        var hitRows = await hitResult.ToListAsync();
        Assert.Single(hitRows);
        Assert.Equal(7L, hitRows[0][0].AsInteger);
        Assert.Equal(7L, hitRows[0][1].AsInteger);

        await using var missResult = await _db.ExecuteAsync(
            "SELECT id, id AS id_alias FROM pk_projection WHERE id = 8");
        var missRows = await missResult.ToListAsync();
        Assert.Empty(missRows);
    }

    [Fact]
    public async Task LookupConjunct_WithResidualFilter_RemainsCorrect()
    {
        await _db.ExecuteAsync("CREATE TABLE lookup_residual (id INTEGER PRIMARY KEY, code INTEGER, state TEXT)");
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_lookup_residual_code ON lookup_residual(code)");
        await _db.ExecuteAsync("INSERT INTO lookup_residual VALUES (1, 100, 'active')");
        await _db.ExecuteAsync("INSERT INTO lookup_residual VALUES (2, 200, 'inactive')");

        await using var hitResult = await _db.ExecuteAsync(
            "SELECT id FROM lookup_residual WHERE code = 100 AND state = 'active'");
        var hitRows = await hitResult.ToListAsync();
        Assert.Single(hitRows);
        Assert.Equal(1L, hitRows[0][0].AsInteger);

        await using var missResult = await _db.ExecuteAsync(
            "SELECT id FROM lookup_residual WHERE code = 100 AND state = 'inactive'");
        var missRows = await missResult.ToListAsync();
        Assert.Empty(missRows);
    }

    [Fact]
    public async Task SelectProjection_UsesIndexOrder_WithCorrectRows()
    {
        await _db.ExecuteAsync("CREATE TABLE ordered_lookup (id INTEGER PRIMARY KEY, sort_key INTEGER, payload TEXT)");
        await _db.ExecuteAsync("CREATE INDEX idx_ordered_lookup_sort_key ON ordered_lookup(sort_key)");
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (1, 30, 'c')");
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (2, 10, 'a')");
        await _db.ExecuteAsync("INSERT INTO ordered_lookup VALUES (3, 20, 'b')");

        await using var result = await _db.ExecuteAsync(
            "SELECT id FROM ordered_lookup ORDER BY sort_key LIMIT 2");
        var rows = await result.ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(3L, rows[1][0].AsInteger);
    }

    #region LIKE / IN / BETWEEN / IS NULL

    [Fact]
    public async Task Like_PercentWildcard()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'Apple')");
        await _db.ExecuteAsync("INSERT INTO products VALUES (2, 'Banana')");
        await _db.ExecuteAsync("INSERT INTO products VALUES (3, 'Apricot')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM products WHERE name LIKE 'Ap%'");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Like_UnderscoreWildcard()
    {
        await _db.ExecuteAsync("CREATE TABLE codes (id INTEGER, code TEXT)");
        await _db.ExecuteAsync("INSERT INTO codes VALUES (1, 'A1')");
        await _db.ExecuteAsync("INSERT INTO codes VALUES (2, 'A2')");
        await _db.ExecuteAsync("INSERT INTO codes VALUES (3, 'AB')");
        await _db.ExecuteAsync("INSERT INTO codes VALUES (4, 'B1')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM codes WHERE code LIKE 'A_'");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count); // A1, A2, AB
    }

    [Fact]
    public async Task Like_CaseInsensitive()
    {
        await _db.ExecuteAsync("CREATE TABLE names (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO names VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO names VALUES (2, 'ALICE')");
        await _db.ExecuteAsync("INSERT INTO names VALUES (3, 'Bob')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM names WHERE name LIKE 'alice'");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task NotLike()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'foo')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'bar')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'foobar')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM items WHERE name NOT LIKE 'foo%'");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("bar", rows[0][1].AsText);
    }

    [Fact]
    public async Task Like_WithEscape()
    {
        await _db.ExecuteAsync("CREATE TABLE paths (id INTEGER, path TEXT)");
        await _db.ExecuteAsync("INSERT INTO paths VALUES (1, '100%')");
        await _db.ExecuteAsync("INSERT INTO paths VALUES (2, '100 items')");
        await _db.ExecuteAsync("INSERT INTO paths VALUES (3, '50%')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM paths WHERE path LIKE '%!%' ESCAPE '!'");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // 100% and 50%
    }

    [Fact]
    public async Task In_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)");
        for (int i = 1; i <= 5; i++)
            await _db.ExecuteAsync($"INSERT INTO nums VALUES ({i}, {i * 10})");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nums WHERE id IN (1, 3, 5)");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task In_Strings()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER, status TEXT)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'active')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'inactive')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'pending')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM users WHERE status IN ('active', 'pending')");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task NotIn()
    {
        await _db.ExecuteAsync("CREATE TABLE tags (id INTEGER, tag TEXT)");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (1, 'a')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (2, 'b')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (3, 'c')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM tags WHERE tag NOT IN ('a', 'b')");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("c", rows[0][1].AsText);
    }

    [Fact]
    public async Task Between_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE ages (id INTEGER, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO ages VALUES (1, 15)");
        await _db.ExecuteAsync("INSERT INTO ages VALUES (2, 18)");
        await _db.ExecuteAsync("INSERT INTO ages VALUES (3, 25)");
        await _db.ExecuteAsync("INSERT INTO ages VALUES (4, 65)");
        await _db.ExecuteAsync("INSERT INTO ages VALUES (5, 70)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM ages WHERE age BETWEEN 18 AND 65");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count); // 18, 25, 65 (inclusive)
    }

    [Fact]
    public async Task Between_Reals()
    {
        await _db.ExecuteAsync("CREATE TABLE prices (id INTEGER, price REAL)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (1, 5.99)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (2, 10.00)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (3, 15.50)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (4, 20.00)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM prices WHERE price BETWEEN 10.0 AND 20.0");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count); // 10.00, 15.50, 20.00
    }

    [Fact]
    public async Task NotBetween()
    {
        await _db.ExecuteAsync("CREATE TABLE vals (id INTEGER, v INTEGER)");
        for (int i = 1; i <= 10; i++)
            await _db.ExecuteAsync($"INSERT INTO vals VALUES ({i}, {i})");

        await using var result = await _db.ExecuteAsync("SELECT * FROM vals WHERE v NOT BETWEEN 3 AND 7");
        var rows = await result.ToListAsync();
        Assert.Equal(5, rows.Count); // 1,2,8,9,10
    }

    [Fact]
    public async Task IsNull()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (1, 'hello')");
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (2, NULL)");
        await _db.ExecuteAsync("INSERT INTO nullable VALUES (3, NULL)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable WHERE val IS NULL");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task IsNotNull()
    {
        await _db.ExecuteAsync("CREATE TABLE nullable2 (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (1, 'hello')");
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (2, NULL)");
        await _db.ExecuteAsync("INSERT INTO nullable2 VALUES (3, 'world')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nullable2 WHERE val IS NOT NULL");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Like_NullOperand_ReturnsNoRows()
    {
        await _db.ExecuteAsync("CREATE TABLE nulllike (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO nulllike VALUES (1, NULL)");
        await _db.ExecuteAsync("INSERT INTO nulllike VALUES (2, 'abc')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM nulllike WHERE val LIKE '%a%'");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task In_WithNull()
    {
        await _db.ExecuteAsync("CREATE TABLE innull (id INTEGER, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO innull VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO innull VALUES (2, NULL)");
        await _db.ExecuteAsync("INSERT INTO innull VALUES (3, 20)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM innull WHERE val IN (10, 20)");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // NULL row excluded
    }

    #endregion

    #region Aggregate Functions + GROUP BY + HAVING

    [Fact]
    public async Task CountStar()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'b')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'c')");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM items");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task CountStar_RemainsCorrectAfterMutations()
    {
        await _db.ExecuteAsync("CREATE TABLE count_mut (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (1, 'a')");
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (2, 'b')");
        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (3, 'c')");

        async Task<long> CountAsync()
        {
            await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM count_mut");
            var rows = await result.ToListAsync();
            return rows[0][0].AsInteger;
        }

        Assert.Equal(3, await CountAsync());

        await _db.ExecuteAsync("DELETE FROM count_mut WHERE id = 2");
        Assert.Equal(2, await CountAsync());

        await _db.ExecuteAsync("UPDATE count_mut SET id = 30 WHERE id = 3");
        Assert.Equal(2, await CountAsync());

        await _db.ExecuteAsync("INSERT INTO count_mut VALUES (40, 'd')");
        Assert.Equal(3, await CountAsync());
    }

    [Fact]
    public async Task CountColumn_IgnoresNull()
    {
        await _db.ExecuteAsync("CREATE TABLE vals (id INTEGER, v TEXT)");
        await _db.ExecuteAsync("INSERT INTO vals VALUES (1, 'a')");
        await _db.ExecuteAsync("INSERT INTO vals VALUES (2, NULL)");
        await _db.ExecuteAsync("INSERT INTO vals VALUES (3, 'b')");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(v) FROM vals");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Sum_Integers()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 20)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 30)");

        await using var result = await _db.ExecuteAsync("SELECT SUM(val) FROM nums");
        var rows = await result.ToListAsync();
        Assert.Equal(60, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Sum_Reals()
    {
        await _db.ExecuteAsync("CREATE TABLE prices (id INTEGER, price REAL)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (1, 9.99)");
        await _db.ExecuteAsync("INSERT INTO prices VALUES (2, 19.99)");

        await using var result = await _db.ExecuteAsync("SELECT SUM(price) FROM prices");
        var rows = await result.ToListAsync();
        Assert.True(Math.Abs(rows[0][0].AsReal - 29.98) < 0.001);
    }

    [Fact]
    public async Task Avg()
    {
        await _db.ExecuteAsync("CREATE TABLE scores (id INTEGER, score INTEGER)");
        await _db.ExecuteAsync("INSERT INTO scores VALUES (1, 80)");
        await _db.ExecuteAsync("INSERT INTO scores VALUES (2, 90)");
        await _db.ExecuteAsync("INSERT INTO scores VALUES (3, 100)");

        await using var result = await _db.ExecuteAsync("SELECT AVG(score) FROM scores");
        var rows = await result.ToListAsync();
        Assert.Equal(90.0, rows[0][0].AsReal);
    }

    [Fact]
    public async Task Min_Max()
    {
        await _db.ExecuteAsync("CREATE TABLE temps (id INTEGER, temp REAL)");
        await _db.ExecuteAsync("INSERT INTO temps VALUES (1, 22.5)");
        await _db.ExecuteAsync("INSERT INTO temps VALUES (2, 18.0)");
        await _db.ExecuteAsync("INSERT INTO temps VALUES (3, 31.2)");

        await using var minResult = await _db.ExecuteAsync("SELECT MIN(temp) FROM temps");
        var minRows = await minResult.ToListAsync();
        Assert.True(Math.Abs(minRows[0][0].AsReal - 18.0) < 0.001);

        await using var maxResult = await _db.ExecuteAsync("SELECT MAX(temp) FROM temps");
        var maxRows = await maxResult.ToListAsync();
        Assert.True(Math.Abs(maxRows[0][0].AsReal - 31.2) < 0.001);
    }

    [Fact]
    public async Task GroupBy_Single()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER, category TEXT, amount INTEGER)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 'A', 10)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 'B', 20)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 'A', 30)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 'B', 40)");

        await using var result = await _db.ExecuteAsync("SELECT category, SUM(amount) FROM orders GROUP BY category");
        var rows = await result.ToListAsync();
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
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER, region TEXT, product TEXT, qty INTEGER)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 'East', 'Widget', 5)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 'East', 'Widget', 3)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 'East', 'Gadget', 2)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (4, 'West', 'Widget', 7)");

        await using var result = await _db.ExecuteAsync(
            "SELECT region, product, SUM(qty) FROM sales GROUP BY region, product");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count); // East/Widget, East/Gadget, West/Widget
    }

    [Fact]
    public async Task Having()
    {
        await _db.ExecuteAsync("CREATE TABLE data (id INTEGER, grp TEXT, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO data VALUES (1, 'A', 10)");
        await _db.ExecuteAsync("INSERT INTO data VALUES (2, 'A', 20)");
        await _db.ExecuteAsync("INSERT INTO data VALUES (3, 'B', 5)");
        await _db.ExecuteAsync("INSERT INTO data VALUES (4, 'C', 100)");
        await _db.ExecuteAsync("INSERT INTO data VALUES (5, 'C', 200)");

        await using var result = await _db.ExecuteAsync(
            "SELECT grp, SUM(val) FROM data GROUP BY grp HAVING SUM(val) > 20");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // A(30), C(300) — B(5) excluded
    }

    [Fact]
    public async Task CountStar_EmptyTable()
    {
        await _db.ExecuteAsync("CREATE TABLE empty (id INTEGER, val TEXT)");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM empty");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(0, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Avg_EmptyTable_ReturnsNull()
    {
        await _db.ExecuteAsync("CREATE TABLE empty2 (id INTEGER, val INTEGER)");

        await using var result = await _db.ExecuteAsync("SELECT AVG(val) FROM empty2");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.True(rows[0][0].IsNull);
    }

    [Fact]
    public async Task CountDistinct()
    {
        await _db.ExecuteAsync("CREATE TABLE tags (id INTEGER, tag TEXT)");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (1, 'a')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (2, 'b')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (3, 'a')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (4, 'c')");
        await _db.ExecuteAsync("INSERT INTO tags VALUES (5, 'b')");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(DISTINCT tag) FROM tags");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows[0][0].AsInteger); // a, b, c
    }

    [Fact]
    public async Task DistinctAggregates_IntegerColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 1)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 1)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 2)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (4, 3)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (5, NULL)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (6, 3)");

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(DISTINCT val) FROM nums");
        var countRows = await countResult.ToListAsync();
        Assert.Single(countRows);
        Assert.Equal(3, countRows[0][0].AsInteger);

        await using var sumResult = await _db.ExecuteAsync("SELECT SUM(DISTINCT val) FROM nums");
        var sumRows = await sumResult.ToListAsync();
        Assert.Single(sumRows);
        Assert.Equal(6, sumRows[0][0].AsInteger);

        await using var avgResult = await _db.ExecuteAsync("SELECT AVG(DISTINCT val) FROM nums");
        var avgRows = await avgResult.ToListAsync();
        Assert.Single(avgRows);
        Assert.Equal(2d, avgRows[0][0].AsReal);
    }

    [Fact]
    public async Task DistinctAggregates_EmptyTable()
    {
        await _db.ExecuteAsync("CREATE TABLE nums2 (id INTEGER, val INTEGER)");

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(DISTINCT val) FROM nums2");
        var countRows = await countResult.ToListAsync();
        Assert.Single(countRows);
        Assert.Equal(0, countRows[0][0].AsInteger);

        await using var sumResult = await _db.ExecuteAsync("SELECT SUM(DISTINCT val) FROM nums2");
        var sumRows = await sumResult.ToListAsync();
        Assert.Single(sumRows);
        Assert.Equal(0, sumRows[0][0].AsInteger);

        await using var avgResult = await _db.ExecuteAsync("SELECT AVG(DISTINCT val) FROM nums2");
        var avgRows = await avgResult.ToListAsync();
        Assert.Single(avgRows);
        Assert.True(avgRows[0][0].IsNull);
    }

    [Fact]
    public async Task Aggregate_WithAlias()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2, rows[0][0].AsInteger);
        Assert.Equal(30, rows[0][1].AsInteger);
        Assert.Equal("cnt", result.Schema[0].Name);
        Assert.Equal("total", result.Schema[1].Name);
    }

    [Fact]
    public async Task GroupBy_CountPerGroup()
    {
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER, level TEXT)");
        await _db.ExecuteAsync("INSERT INTO log VALUES (1, 'INFO')");
        await _db.ExecuteAsync("INSERT INTO log VALUES (2, 'ERROR')");
        await _db.ExecuteAsync("INSERT INTO log VALUES (3, 'INFO')");
        await _db.ExecuteAsync("INSERT INTO log VALUES (4, 'INFO')");
        await _db.ExecuteAsync("INSERT INTO log VALUES (5, 'ERROR')");

        await using var result = await _db.ExecuteAsync("SELECT level, COUNT(*) FROM log GROUP BY level");
        var rows = await result.ToListAsync();
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
        await _db.ExecuteAsync("CREATE TABLE names (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO names VALUES (1, 'Charlie')");
        await _db.ExecuteAsync("INSERT INTO names VALUES (2, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO names VALUES (3, 'Bob')");

        await using var minResult = await _db.ExecuteAsync("SELECT MIN(name) FROM names");
        var minRows = await minResult.ToListAsync();
        Assert.Equal("Alice", minRows[0][0].AsText);

        await using var maxResult = await _db.ExecuteAsync("SELECT MAX(name) FROM names");
        var maxRows = await maxResult.ToListAsync();
        Assert.Equal("Charlie", maxRows[0][0].AsText);
    }

    [Fact]
    public async Task Having_CountFilter()
    {
        await _db.ExecuteAsync("CREATE TABLE visits (id INTEGER, page TEXT)");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (1, 'home')");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (2, 'about')");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (3, 'home')");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (4, 'home')");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (5, 'about')");
        await _db.ExecuteAsync("INSERT INTO visits VALUES (6, 'contact')");

        await using var result = await _db.ExecuteAsync(
            "SELECT page, COUNT(*) FROM visits GROUP BY page HAVING COUNT(*) >= 2");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // home(3), about(2) — contact(1) excluded
    }

    #endregion

    #region JOINs

    private async Task SetupJoinTables()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie')");

        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 'Widget')");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 1, 'Gadget')");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 2, 'Widget')");
        // Note: Charlie (id=3) has no orders, order user_id=4 doesn't exist
    }

    [Fact]
    public async Task InnerJoin()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        var rows = await result.ToListAsync();
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
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        // Alice has 2 orders, Bob has 1
        var names = rows.Select(r => r[0].AsText).ToList();
        Assert.Equal(2, names.Count(n => n == "Alice"));
        Assert.Single(names.Where(n => n == "Bob"));
    }

    [Fact]
    public async Task InnerJoin_WithAliases()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task InnerJoin_WithAsAliases()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT u.name, o.product FROM users AS u JOIN orders AS o ON u.id = o.user_id");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task LeftJoin_NullPadding()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id");
        var rows = await result.ToListAsync();
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
            "SELECT users.name, orders.product FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id");
        var rows = await result.ToListAsync();
        Assert.Equal(4, rows.Count);
    }

    [Fact]
    public async Task RightJoin()
    {
        await SetupJoinTables();
        // Add an order with no matching user
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 99, 'Orphan')");

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id");
        var rows = await result.ToListAsync();
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
        await _db.ExecuteAsync("INSERT INTO orders VALUES (4, 99, 'Orphan')");

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id");
        var rows = await result.ToListAsync();
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
            "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id AND orders.product = 'Widget' ORDER BY orders.id");
        var rows = await result.ToListAsync();
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
        await _db.ExecuteAsync("CREATE TABLE colors (id INTEGER, color TEXT)");
        await _db.ExecuteAsync("INSERT INTO colors VALUES (1, 'Red')");
        await _db.ExecuteAsync("INSERT INTO colors VALUES (2, 'Blue')");

        await _db.ExecuteAsync("CREATE TABLE sizes (id INTEGER, size TEXT)");
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (1, 'S')");
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (2, 'M')");
        await _db.ExecuteAsync("INSERT INTO sizes VALUES (3, 'L')");

        await using var result = await _db.ExecuteAsync(
            "SELECT * FROM colors CROSS JOIN sizes");
        var rows = await result.ToListAsync();
        Assert.Equal(6, rows.Count); // 2 * 3 = 6
    }

    [Fact]
    public async Task Join_WithWhere()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.product = 'Widget'");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // Alice+Widget, Bob+Widget
    }

    [Fact]
    public async Task Join_WithResidualOnPredicate()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id AND orders.product = 'Widget'");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("Widget", r[1].AsText));
    }

    [Fact]
    public async Task Join_NonEquiCondition_UsesFallbackPath()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id > orders.user_id");
        var rows = await result.ToListAsync();
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Join_OnRightPrimaryKey_UsesLookupPath()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.id ORDER BY users.name");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Bob", rows[1][0].AsText);
        Assert.Equal("Charlie", rows[2][0].AsText);
    }

    [Fact]
    public async Task LeftJoin_OnRightUniqueIndex_UsesLookupPath()
    {
        await _db.ExecuteAsync("CREATE TABLE left_lookup (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (1, 'L1')");
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (2, 'L2')");
        await _db.ExecuteAsync("INSERT INTO left_lookup VALUES (3, 'L3')");

        await _db.ExecuteAsync("CREATE TABLE right_lookup (id INTEGER PRIMARY KEY, code INTEGER, payload TEXT)");
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_right_lookup_code ON right_lookup(code)");
        await _db.ExecuteAsync("INSERT INTO right_lookup VALUES (10, 1, 'R1')");
        await _db.ExecuteAsync("INSERT INTO right_lookup VALUES (11, 2, 'R2')");

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM left_lookup l LEFT JOIN right_lookup r ON l.id = r.code ORDER BY l.id");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("R1", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("R2", rows[1][1].AsText);
        Assert.Equal(3L, rows[2][0].AsInteger);
        Assert.True(rows[2][1].IsNull);
    }

    [Fact]
    public async Task Join_OnRightNonUniqueIndex_ReturnsAllMatches()
    {
        await _db.ExecuteAsync("CREATE TABLE left_many (id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync("INSERT INTO left_many VALUES (1)");
        await _db.ExecuteAsync("INSERT INTO left_many VALUES (2)");

        await _db.ExecuteAsync("CREATE TABLE right_many (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload TEXT)");
        await _db.ExecuteAsync("CREATE INDEX idx_right_many_code ON right_many(code)");
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (10, 1, 'A')");
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (11, 1, 'B')");
        await _db.ExecuteAsync("INSERT INTO right_many VALUES (12, 2, 'C')");

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM left_many l JOIN right_many r ON l.id = r.code ORDER BY l.id, r.payload");
        var rows = await result.ToListAsync();
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
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Alice", rows[1][0].AsText);
        Assert.Equal("Bob", rows[2][0].AsText);
    }

    [Fact]
    public async Task MultiWayJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO departments VALUES (1, 'Engineering')");
        await _db.ExecuteAsync("INSERT INTO departments VALUES (2, 'Sales')");

        await _db.ExecuteAsync("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)");
        await _db.ExecuteAsync("INSERT INTO employees VALUES (1, 'Alice', 1)");
        await _db.ExecuteAsync("INSERT INTO employees VALUES (2, 'Bob', 2)");

        await _db.ExecuteAsync("CREATE TABLE projects (id INTEGER PRIMARY KEY, emp_id INTEGER, title TEXT)");
        await _db.ExecuteAsync("INSERT INTO projects VALUES (1, 1, 'Project X')");
        await _db.ExecuteAsync("INSERT INTO projects VALUES (2, 1, 'Project Y')");
        await _db.ExecuteAsync("INSERT INTO projects VALUES (3, 2, 'Project Z')");

        await using var result = await _db.ExecuteAsync(
            "SELECT departments.name, employees.name, projects.title " +
            "FROM departments " +
            "JOIN employees ON departments.id = employees.dept_id " +
            "JOIN projects ON employees.id = projects.emp_id");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task SelfJoin_WithAliases()
    {
        await _db.ExecuteAsync("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)");
        await _db.ExecuteAsync("INSERT INTO emp VALUES (1, 'Alice', NULL)");
        await _db.ExecuteAsync("INSERT INTO emp VALUES (2, 'Bob', 1)");
        await _db.ExecuteAsync("INSERT INTO emp VALUES (3, 'Charlie', 1)");

        await using var result = await _db.ExecuteAsync(
            "SELECT e.name, m.name FROM emp e JOIN emp m ON e.manager_id = m.id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // Bob->Alice, Charlie->Alice
        Assert.All(rows, r => Assert.Equal("Alice", r[1].AsText));
    }

    [Fact]
    public async Task Join_WithAggregate()
    {
        await SetupJoinTables();

        await using var result = await _db.ExecuteAsync(
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // Alice(2), Bob(1)

        var alice = rows.FirstOrDefault(r => r[0].AsText == "Alice");
        Assert.NotNull(alice);
        Assert.Equal(2, alice[1].AsInteger);
    }

    [Fact]
    public async Task LeftJoin_EmptyRight()
    {
        await _db.ExecuteAsync("CREATE TABLE a (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO a VALUES (1, 'x')");
        await _db.ExecuteAsync("INSERT INTO a VALUES (2, 'y')");

        await _db.ExecuteAsync("CREATE TABLE b (id INTEGER, a_id INTEGER, data TEXT)");
        // b is empty

        await using var result = await _db.ExecuteAsync(
            "SELECT a.val, b.data FROM a LEFT JOIN b ON a.id = b.a_id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.True(r[1].IsNull));
    }

    #endregion

    #region ALTER TABLE

    [Fact]
    public async Task AlterTable_AddColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob')");

        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN age INTEGER");

        // Old rows should have NULL for the new column
        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(3, rows[0].Length); // id, name, age
        Assert.True(rows[0][2].IsNull);
        Assert.True(rows[1][2].IsNull);
    }

    [Fact]
    public async Task AlterTable_AddColumn_InsertAfter()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");

        await _db.ExecuteAsync("ALTER TABLE t ADD email TEXT");

        // Insert with new column
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 'bob@test.com')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.True(rows[0][2].IsNull); // Alice's email is NULL
        Assert.Equal("bob@test.com", rows[1][2].AsText); // Bob's email set
    }

    [Fact]
    public async Task AlterTable_AddColumn_WithNamedInsert()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");

        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN score INTEGER");

        // Insert using named columns
        await _db.ExecuteAsync("INSERT INTO t (id, name, score) VALUES (2, 'Bob', 95)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE score = 95");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Bob", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 25)");

        await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN age");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // only id and name
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropColumn_MiddleColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, temp TEXT, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'x', 'Alice')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'y', 'Bob')");

        await _db.ExecuteAsync("ALTER TABLE t DROP temp");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // id and name
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal("Alice", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_DropPrimaryKey_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN id"));
    }

    [Fact]
    public async Task AlterTable_DropLastColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER)");
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN id"));
    }

    [Fact]
    public async Task AlterTable_RenameTable()
    {
        await _db.ExecuteAsync("CREATE TABLE old_name (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO old_name VALUES (1, 'hello')");

        await _db.ExecuteAsync("ALTER TABLE old_name RENAME TO new_name");

        // Old name should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("SELECT * FROM old_name"));

        // New name should work
        await using var result = await _db.ExecuteAsync("SELECT * FROM new_name");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("hello", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_RenameColumn()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, old_name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");

        await _db.ExecuteAsync("ALTER TABLE t RENAME COLUMN old_name TO new_name");

        // Query using new column name
        await using var result = await _db.ExecuteAsync("SELECT new_name FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][0].AsText);
    }

    [Fact]
    public async Task AlterTable_RenameColumn_OldNameFails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, old_col TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'test')");

        await _db.ExecuteAsync("ALTER TABLE t RENAME old_col TO new_col");

        // Old column name should fail when iterating
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync("SELECT old_col FROM t");
            await result.ToListAsync();
        });
    }

    [Fact]
    public async Task AlterTable_AddDuplicateColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN name TEXT"));
    }

    [Fact]
    public async Task AlterTable_DropNonexistentColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("ALTER TABLE t DROP COLUMN doesntexist"));
    }

    [Fact]
    public async Task AlterTable_RenameTable_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE orig (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO orig VALUES (1, 'data')");
        await _db.ExecuteAsync("ALTER TABLE orig RENAME TO renamed");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM renamed");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("data", rows[0][1].AsText);
    }

    [Fact]
    public async Task AlterTable_AddColumn_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')");
        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN age INTEGER");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Length);
        Assert.True(rows[0][2].IsNull); // old row still has NULL for added column
    }

    #endregion

    #region INDEXES

    [Fact]
    public async Task CreateIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 200)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 100)");

        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        // The index should be used for this equality lookup
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 100");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(100, r[1].AsInteger));
    }

    [Fact]
    public async Task CreateIndex_IfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        // Should not throw
        await _db.ExecuteAsync("CREATE INDEX IF NOT EXISTS idx_val ON t (val)");
    }

    [Fact]
    public async Task CreateIndex_Duplicate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)"));
    }

    [Fact]
    public async Task DropIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");
        await _db.ExecuteAsync("DROP INDEX idx_val");

        // Queries should still work (full scan)
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 100");
        var rows = await result.ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task DropIndex_IfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP INDEX IF EXISTS nonexistent");
    }

    [Fact]
    public async Task DropIndex_Nonexistent_Fails()
    {
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP INDEX nonexistent"));
    }

    [Fact]
    public async Task UniqueIndex_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, email_hash INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 1001)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 1002)");

        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_email ON t (email_hash)");

        // Inserting a duplicate should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (3, 1001)"));
    }

    [Fact]
    public async Task UniqueIndex_CreateOnDuplicateData_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 100)"); // duplicate val

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)"));
    }

    [Fact]
    public async Task Index_MaintainedOnInsert()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        // Insert after index creation
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 42)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Index_MaintainedOnDelete()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        await _db.ExecuteAsync("DELETE FROM t WHERE id = 1");

        // val=42 should no longer be found
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42");
        var rows = await result.ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Index_MaintainedOnUpdate()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        await _db.ExecuteAsync("UPDATE t SET val = 50 WHERE id = 1");

        // Old value gone
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42");
            var rows = await result.ToListAsync();
            Assert.Empty(rows);
        }

        // New value present
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 50");
            var rows = await result.ToListAsync();
            Assert.Single(rows);
            Assert.Equal(1, rows[0][0].AsInteger);
        }
    }

    [Fact]
    public async Task UniqueIndex_ViolationOnUpdate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 200)");
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)");

        // Updating to a value that already exists should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("UPDATE t SET val = 100 WHERE id = 2"));
    }

    [Fact]
    public async Task Index_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 99)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        // Index should still work for lookups
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task UniqueIndex_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 100)");
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_val ON t (val)");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        // Unique constraint should persist
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("INSERT INTO t VALUES (2, 100)"));
    }

    [Fact]
    public async Task Index_DropTableDropsIndexes()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");
        await _db.ExecuteAsync("DROP TABLE t");

        // Re-creating the same index name should work since the old one was dropped
        await _db.ExecuteAsync("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t2 (val)");
    }

    [Fact]
    public async Task Index_NullValuesNotIndexed()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 42)");
        await _db.ExecuteAsync("INSERT INTO t (id) VALUES (2)"); // val is NULL
        await _db.ExecuteAsync("CREATE INDEX idx_val ON t (val)");

        // Should only find the non-null row
        await using var result = await _db.ExecuteAsync("SELECT * FROM t WHERE val = 42");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Index_OnNonIntegerColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE INDEX idx_name ON t (name)"));
    }

    [Fact]
    public async Task Index_MultiColumn_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE INDEX idx_ab ON t (a, b)"));
    }

    #endregion

    #region VIEWS

    [Fact]
    public async Task CreateView_And_Query()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie', 35)");

        await _db.ExecuteAsync("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30");

        await using var result = await _db.ExecuteAsync("SELECT * FROM adults");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r[1].AsText == "Alice");
        Assert.Contains(rows, r => r[1].AsText == "Charlie");
    }

    [Fact]
    public async Task CreateView_IfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t");

        // Should not throw
        await _db.ExecuteAsync("CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t");
    }

    [Fact]
    public async Task CreateView_Duplicate_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t");

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t"));
    }

    [Fact]
    public async Task CreateView_ConflictsWithTable_Fails()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)");

        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("CREATE VIEW t AS SELECT * FROM t"));
    }

    [Fact]
    public async Task DropView_Basic()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER, val TEXT)");
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t");
        await _db.ExecuteAsync("DROP VIEW v");

        // Querying the dropped view should fail
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using var result = await _db.ExecuteAsync("SELECT * FROM v");
            await result.ToListAsync();
        });
    }

    [Fact]
    public async Task DropView_IfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP VIEW IF EXISTS nonexistent");
    }

    [Fact]
    public async Task DropView_Nonexistent_Fails()
    {
        await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP VIEW nonexistent"));
    }

    [Fact]
    public async Task View_WithProjection()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");

        await _db.ExecuteAsync("CREATE VIEW names AS SELECT name FROM users");

        await using var result = await _db.ExecuteAsync("SELECT * FROM names");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].Length); // Only the name column
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Bob", rows[1][0].AsText);
    }

    [Fact]
    public async Task View_WithWhereOnView()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 30)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 50)");

        await _db.ExecuteAsync("CREATE VIEW cheap AS SELECT * FROM items WHERE price < 40");

        // Query the view with an additional WHERE
        await using var result = await _db.ExecuteAsync("SELECT * FROM cheap WHERE price > 15");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Gadget", rows[0][1].AsText);
    }

    [Fact]
    public async Task View_ReflectsDataChanges()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)");
        await _db.ExecuteAsync("CREATE VIEW v AS SELECT * FROM t");

        // Insert more data
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)");

        await using var result = await _db.ExecuteAsync("SELECT * FROM v");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count); // View should reflect the new row
    }

    [Fact]
    public async Task View_Persistence()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");

        await _db.ExecuteAsync("CREATE VIEW seniors AS SELECT * FROM users WHERE age >= 30");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM seniors");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][1].AsText);
    }

    [Fact]
    public async Task View_WithJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)");
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 100)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 2, 200)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 1, 150)");

        await _db.ExecuteAsync("CREATE VIEW order_details AS SELECT orders.id, users.name, orders.amount FROM orders JOIN users ON orders.user_id = users.id");

        await using var result = await _db.ExecuteAsync("SELECT * FROM order_details");
        var rows = await result.ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task View_WithAggregate()
    {
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 1, 100)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 1, 200)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 2, 300)");

        await _db.ExecuteAsync("CREATE VIEW category_totals AS SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        await using var result = await _db.ExecuteAsync("SELECT * FROM category_totals");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    #endregion

    #region CTEs (Common Table Expressions)

    [Fact]
    public async Task Cte_SingleBasic()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (3, 'Charlie', 35)");

        await using var result = await _db.ExecuteAsync(
            "WITH adults AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM adults");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][1].AsText);
        Assert.Equal("Charlie", rows[1][1].AsText);
    }

    [Fact]
    public async Task Cte_WithProjection()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget', 50)");
        await _db.ExecuteAsync("INSERT INTO items VALUES (3, 'Doohickey', 30)");

        await using var result = await _db.ExecuteAsync(
            "WITH expensive AS (SELECT name, price FROM items WHERE price > 20) SELECT * FROM expensive");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Length); // name and price only
    }

    [Fact]
    public async Task Cte_WithWhereOnMainQuery()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 30)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (4, 40)");

        await using var result = await _db.ExecuteAsync(
            "WITH big AS (SELECT * FROM t WHERE val > 15) SELECT * FROM big WHERE val < 35");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task Cte_MultipleCtes()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category INTEGER)");
        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'A', 10, 1)");
        await _db.ExecuteAsync("INSERT INTO products VALUES (2, 'B', 20, 1)");
        await _db.ExecuteAsync("INSERT INTO products VALUES (3, 'C', 30, 2)");
        await _db.ExecuteAsync("INSERT INTO products VALUES (4, 'D', 40, 2)");

        await using var result = await _db.ExecuteAsync(
            "WITH cheap AS (SELECT * FROM products WHERE price <= 20), " +
            "expensive AS (SELECT * FROM products WHERE price > 20) " +
            "SELECT * FROM cheap");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("A", rows[0][1].AsText);
        Assert.Equal("B", rows[1][1].AsText);
    }

    [Fact]
    public async Task Cte_ReferencedInJoin()
    {
        await _db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)");
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')");
        await _db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 100)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (2, 2, 200)");
        await _db.ExecuteAsync("INSERT INTO orders VALUES (3, 1, 150)");

        await using var result = await _db.ExecuteAsync(
            "WITH big_orders AS (SELECT * FROM orders WHERE amount >= 150) " +
            "SELECT users.name, big_orders.amount FROM big_orders JOIN users ON big_orders.user_id = users.id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Cte_ReferencedMultipleTimes()
    {
        await _db.ExecuteAsync("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (2, 20)");
        await _db.ExecuteAsync("INSERT INTO nums VALUES (3, 30)");

        // Use the CTE twice in a CROSS JOIN
        await using var result = await _db.ExecuteAsync(
            "WITH small AS (SELECT * FROM nums WHERE val <= 20) " +
            "SELECT a.val, b.val FROM small a CROSS JOIN small b");
        var rows = await result.ToListAsync();
        Assert.Equal(4, rows.Count); // 2 x 2 = 4
    }

    [Fact]
    public async Task Cte_WithExplicitColumnNames()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice', 90)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'Bob', 80)");

        await using var result = await _db.ExecuteAsync(
            "WITH renamed(student_id, student_name, student_score) AS (SELECT * FROM t) " +
            "SELECT student_name FROM renamed WHERE student_score > 85");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][0].AsText);
    }

    [Fact]
    public async Task Cte_WithAggregate()
    {
        await _db.ExecuteAsync("CREATE TABLE sales (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (1, 1, 100)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (2, 1, 200)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (3, 2, 300)");
        await _db.ExecuteAsync("INSERT INTO sales VALUES (4, 2, 400)");

        await using var result = await _db.ExecuteAsync(
            "WITH totals AS (SELECT category, SUM(amount) AS total FROM sales GROUP BY category) " +
            "SELECT * FROM totals WHERE total > 500");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.Equal(700L, rows[0][1].AsInteger);
    }

    [Fact]
    public async Task Cte_EmptyResult()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)");

        await using var result = await _db.ExecuteAsync(
            "WITH empty AS (SELECT * FROM t WHERE val > 100) SELECT * FROM empty");
        var rows = await result.ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Cte_WithOrderByAndLimit()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 30)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 10)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 20)");

        await using var result = await _db.ExecuteAsync(
            "WITH all_rows AS (SELECT * FROM t) SELECT * FROM all_rows ORDER BY val LIMIT 2");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(20L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task Cte_ChainedCteReferencingPrior()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 20)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 30)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (4, 40)");

        // Second CTE references the first
        await using var result = await _db.ExecuteAsync(
            "WITH step1 AS (SELECT * FROM t WHERE val >= 20), " +
            "step2 AS (SELECT * FROM step1 WHERE val <= 30) " +
            "SELECT * FROM step2");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(20L, rows[0][1].AsInteger);
        Assert.Equal(30L, rows[1][1].AsInteger);
    }

    #endregion

    #region Triggers

    [Fact]
    public async Task Trigger_AfterInsert_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, item_name TEXT)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_insert AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO audit_log VALUES (NEW.id, 'INSERT', NEW.name); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM audit_log ORDER BY id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("INSERT", rows[0][1].AsText);
        Assert.Equal("Widget", rows[0][2].AsText);
        Assert.Equal("INSERT", rows[1][1].AsText);
        Assert.Equal("Gadget", rows[1][2].AsText);
    }

    [Fact]
    public async Task Trigger_BeforeInsert_Validation()
    {
        await _db.ExecuteAsync("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)");
        await _db.ExecuteAsync("CREATE TABLE rejected (id INTEGER PRIMARY KEY, reason TEXT)");

        // This trigger fires before insert; it inserts into rejected log
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_products_validate BEFORE INSERT ON products " +
            "BEGIN " +
            "INSERT INTO rejected VALUES (NEW.id, 'checked'); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO products VALUES (1, 'Laptop', 999.99)");

        // Both tables should have data
        await using var prodResult = await _db.ExecuteAsync("SELECT * FROM products");
        var prodRows = await prodResult.ToListAsync();
        Assert.Single(prodRows);

        await using var rejResult = await _db.ExecuteAsync("SELECT * FROM rejected");
        var rejRows = await rejResult.ToListAsync();
        Assert.Single(rejRows);
        Assert.Equal("checked", rejRows[0][1].AsText);
    }

    [Fact]
    public async Task Trigger_AfterDelete_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE delete_log (id INTEGER PRIMARY KEY, deleted_name TEXT)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_delete AFTER DELETE ON items " +
            "BEGIN " +
            "INSERT INTO delete_log VALUES (OLD.id, OLD.name); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')");
        await _db.ExecuteAsync("DELETE FROM items WHERE id = 1");

        await using var result = await _db.ExecuteAsync("SELECT * FROM delete_log");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("Widget", rows[0][1].AsText);
    }

    [Fact]
    public async Task Trigger_BeforeDelete()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE pre_delete_log (item_id INTEGER PRIMARY KEY)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_before_delete BEFORE DELETE ON items " +
            "BEGIN " +
            "INSERT INTO pre_delete_log VALUES (OLD.id); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'A')");
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')");
        await _db.ExecuteAsync("DELETE FROM items WHERE id = 2");

        await using var result = await _db.ExecuteAsync("SELECT * FROM pre_delete_log");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Trigger_AfterUpdate_AuditTrail()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
        await _db.ExecuteAsync("CREATE TABLE update_log (item_id INTEGER PRIMARY KEY, old_qty INTEGER, new_qty INTEGER)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_items_update AFTER UPDATE ON items " +
            "BEGIN " +
            "INSERT INTO update_log VALUES (NEW.id, OLD.qty, NEW.qty); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget', 10)");
        await _db.ExecuteAsync("UPDATE items SET qty = 20 WHERE id = 1");

        await using var result = await _db.ExecuteAsync("SELECT * FROM update_log");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(20L, rows[0][2].AsInteger);
    }

    [Fact]
    public async Task Trigger_BeforeUpdate()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL)");
        await _db.ExecuteAsync("CREATE TABLE price_changes (item_id INTEGER PRIMARY KEY, old_price REAL)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_before_update BEFORE UPDATE ON items " +
            "BEGIN " +
            "INSERT INTO price_changes VALUES (OLD.id, OLD.price); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 9.99)");
        await _db.ExecuteAsync("UPDATE items SET price = 14.99 WHERE id = 1");

        await using var result = await _db.ExecuteAsync("SELECT * FROM price_changes");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(9.99, rows[0][1].AsReal);
    }

    [Fact]
    public async Task Trigger_DropTrigger()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_log AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO log VALUES (NEW.id, 'inserted'); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'A')");

        // Verify trigger fired
        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log");
        var rows1 = await r1.ToListAsync();
        Assert.Single(rows1);

        // Drop trigger
        await _db.ExecuteAsync("DROP TRIGGER trg_log");

        // Insert again — trigger should NOT fire
        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')");

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log");
        var rows2 = await r2.ToListAsync();
        Assert.Single(rows2); // Still 1 row
    }

    [Fact]
    public async Task Trigger_DropTriggerIfExists()
    {
        // Should not throw
        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS nonexistent_trigger");

        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END");

        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS trg1");

        // Should not throw again
        await _db.ExecuteAsync("DROP TRIGGER IF EXISTS trg1");
    }

    [Fact]
    public async Task Trigger_CreateIfNotExists()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END");

        // Should not throw
        await _db.ExecuteAsync(
            "CREATE TRIGGER IF NOT EXISTS trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END");
    }

    [Fact]
    public async Task Trigger_MultipleTriggersSameEvent()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE log1 (item_id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync("CREATE TABLE log2 (item_id INTEGER PRIMARY KEY)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON items BEGIN INSERT INTO log1 VALUES (NEW.id); END");
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg2 AFTER INSERT ON items BEGIN INSERT INTO log2 VALUES (NEW.id); END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')");

        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log1");
        var rows1 = await r1.ToListAsync();
        Assert.Single(rows1);

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log2");
        var rows2 = await r2.ToListAsync();
        Assert.Single(rows2);
    }

    [Fact]
    public async Task Trigger_Persistence_CloseAndReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE log (item_id INTEGER PRIMARY KEY, action TEXT)");
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_insert AFTER INSERT ON items " +
            "BEGIN INSERT INTO log VALUES (NEW.id, 'created'); END");
        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await _db.ExecuteAsync("INSERT INTO items VALUES (2, 'Gadget')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM log ORDER BY item_id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("created", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("created", rows[1][1].AsText);
    }

    [Fact]
    public async Task Trigger_MultipleBodyStatements()
    {
        await _db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE log1 (item_id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync("CREATE TABLE log2 (item_name TEXT)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_multi AFTER INSERT ON items " +
            "BEGIN " +
            "INSERT INTO log1 VALUES (NEW.id); " +
            "INSERT INTO log2 VALUES (NEW.name); " +
            "END");

        await _db.ExecuteAsync("INSERT INTO items VALUES (1, 'Widget')");

        await using var r1 = await _db.ExecuteAsync("SELECT * FROM log1");
        var rows1 = await r1.ToListAsync();
        Assert.Single(rows1);
        Assert.Equal(1L, rows1[0][0].AsInteger);

        await using var r2 = await _db.ExecuteAsync("SELECT * FROM log2");
        var rows2 = await r2.ToListAsync();
        Assert.Single(rows2);
        Assert.Equal("Widget", rows2[0][0].AsText);
    }

    [Fact]
    public async Task Trigger_DuplicateThrows()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        await _db.ExecuteAsync("CREATE TABLE log (id INTEGER PRIMARY KEY)");

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END");

        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync(
                "CREATE TRIGGER trg1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END"));
        Assert.Equal(ErrorCode.TriggerAlreadyExists, ex.Code);
    }

    [Fact]
    public async Task Trigger_DropNonexistentThrows()
    {
        var ex = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await _db.ExecuteAsync("DROP TRIGGER nonexistent"));
        Assert.Equal(ErrorCode.TriggerNotFound, ex.Code);
    }

    #endregion
}
