using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class SystemCatalogTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public SystemCatalogTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_syscat_test_{Guid.NewGuid():N}.db");
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
    public async Task SystemCatalog_ExposesTablesColumnsAndIndexes()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)");
        await _db.ExecuteAsync("CREATE INDEX idx_users_age ON users(age)");

        await using var tables = await _db.ExecuteAsync(
            "SELECT table_name, column_count, primary_key_column FROM sys.tables WHERE table_name = 'users'");
        var tableRows = await tables.ToListAsync();
        var tableRow = Assert.Single(tableRows);
        Assert.Equal("users", tableRow[0].AsText);
        Assert.Equal(3L, tableRow[1].AsInteger);
        Assert.Equal("id", tableRow[2].AsText);

        await using var columns = await _db.ExecuteAsync(
            "SELECT column_name, ordinal_position, data_type, is_nullable, is_primary_key " +
            "FROM sys.columns WHERE table_name = 'users' ORDER BY ordinal_position");
        var columnRows = await columns.ToListAsync();
        Assert.Equal(3, columnRows.Count);
        Assert.Equal("id", columnRows[0][0].AsText);
        Assert.Equal(1L, columnRows[0][1].AsInteger);
        Assert.Equal("INTEGER", columnRows[0][2].AsText);
        Assert.Equal(1L, columnRows[0][4].AsInteger);
        Assert.Equal("name", columnRows[1][0].AsText);
        Assert.Equal("TEXT", columnRows[1][2].AsText);
        Assert.Equal(0L, columnRows[1][3].AsInteger);
        Assert.Equal(0L, columnRows[1][4].AsInteger);

        await using var indexes = await _db.ExecuteAsync(
            "SELECT index_name, table_name, column_name, ordinal_position, is_unique " +
            "FROM sys.indexes WHERE index_name = 'idx_users_age'");
        var indexRows = await indexes.ToListAsync();
        var indexRow = Assert.Single(indexRows);
        Assert.Equal("idx_users_age", indexRow[0].AsText);
        Assert.Equal("users", indexRow[1].AsText);
        Assert.Equal("age", indexRow[2].AsText);
        Assert.Equal(1L, indexRow[3].AsInteger);
        Assert.Equal(0L, indexRow[4].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_ExposesViewsAndTriggers()
    {
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("CREATE TABLE audit (user_id INTEGER)");
        await _db.ExecuteAsync("CREATE VIEW user_names AS SELECT id, name FROM users");
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER trg_users_audit AFTER INSERT ON users
            BEGIN
                INSERT INTO audit VALUES (NEW.id);
            END;
            """);

        await using var views = await _db.ExecuteAsync(
            "SELECT view_name, sql FROM sys.views WHERE view_name = 'user_names'");
        var viewRows = await views.ToListAsync();
        var viewRow = Assert.Single(viewRows);
        Assert.Equal("user_names", viewRow[0].AsText);
        Assert.Contains("SELECT", viewRow[1].AsText, StringComparison.OrdinalIgnoreCase);

        await using var triggers = await _db.ExecuteAsync(
            "SELECT trigger_name, table_name, timing, event, body_sql " +
            "FROM sys.triggers WHERE trigger_name = 'trg_users_audit'");
        var triggerRows = await triggers.ToListAsync();
        var triggerRow = Assert.Single(triggerRows);
        Assert.Equal("trg_users_audit", triggerRow[0].AsText);
        Assert.Equal("users", triggerRow[1].AsText);
        Assert.Equal("AFTER", triggerRow[2].AsText);
        Assert.Equal("INSERT", triggerRow[3].AsText);
        Assert.Contains("INSERT INTO audit", triggerRow[4].AsText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SystemCatalog_AllowsUnderscoredAliases()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

        await using var tableCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_tables WHERE table_name = 't'");
        var tableCountRows = await tableCount.ToListAsync();
        Assert.Equal(1L, Assert.Single(tableCountRows)[0].AsInteger);

        await using var columnCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_columns WHERE table_name = 't'");
        var columnCountRows = await columnCount.ToListAsync();
        Assert.Equal(2L, Assert.Single(columnCountRows)[0].AsInteger);
    }
}
