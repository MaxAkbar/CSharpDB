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
        var ct = TestContext.Current.CancellationToken;
        _db = await Database.OpenAsync(_dbPath, ct);
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
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_users_age ON users(age)", ct);

        await using var tables = await _db.ExecuteAsync(
            "SELECT table_name, column_count, primary_key_column FROM sys.tables WHERE table_name = 'users'", ct);
        var tableRows = await tables.ToListAsync(ct);
        var tableRow = Assert.Single(tableRows);
        Assert.Equal("users", tableRow[0].AsText);
        Assert.Equal(3L, tableRow[1].AsInteger);
        Assert.Equal("id", tableRow[2].AsText);

        await using var columns = await _db.ExecuteAsync(
            "SELECT column_name, ordinal_position, data_type, is_nullable, is_primary_key, is_identity " +
            "FROM sys.columns WHERE table_name = 'users' ORDER BY ordinal_position", ct);
        var columnRows = await columns.ToListAsync(ct);
        Assert.Equal(3, columnRows.Count);
        Assert.Equal("id", columnRows[0][0].AsText);
        Assert.Equal(1L, columnRows[0][1].AsInteger);
        Assert.Equal("INTEGER", columnRows[0][2].AsText);
        Assert.Equal(1L, columnRows[0][4].AsInteger);
        Assert.Equal(1L, columnRows[0][5].AsInteger);
        Assert.Equal("name", columnRows[1][0].AsText);
        Assert.Equal("TEXT", columnRows[1][2].AsText);
        Assert.Equal(0L, columnRows[1][3].AsInteger);
        Assert.Equal(0L, columnRows[1][4].AsInteger);
        Assert.Equal(0L, columnRows[1][5].AsInteger);

        await using var indexes = await _db.ExecuteAsync(
            "SELECT index_name, table_name, column_name, ordinal_position, is_unique " +
            "FROM sys.indexes WHERE index_name = 'idx_users_age'", ct);
        var indexRows = await indexes.ToListAsync(ct);
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
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ct);
        await _db.ExecuteAsync("CREATE TABLE audit (user_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE VIEW user_names AS SELECT id, name FROM users", ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER trg_users_audit AFTER INSERT ON users
            BEGIN
                INSERT INTO audit VALUES (NEW.id);
            END;
            """, ct);

        await using var views = await _db.ExecuteAsync(
            "SELECT view_name, sql FROM sys.views WHERE view_name = 'user_names'", ct);
        var viewRows = await views.ToListAsync(ct);
        var viewRow = Assert.Single(viewRows);
        Assert.Equal("user_names", viewRow[0].AsText);
        Assert.Contains("SELECT", viewRow[1].AsText, StringComparison.OrdinalIgnoreCase);

        await using var triggers = await _db.ExecuteAsync(
            "SELECT trigger_name, table_name, timing, event, body_sql " +
            "FROM sys.triggers WHERE trigger_name = 'trg_users_audit'", ct);
        var triggerRows = await triggers.ToListAsync(ct);
        var triggerRow = Assert.Single(triggerRows);
        Assert.Equal("trg_users_audit", triggerRow[0].AsText);
        Assert.Equal("users", triggerRow[1].AsText);
        Assert.Equal("AFTER", triggerRow[2].AsText);
        Assert.Equal("INSERT", triggerRow[3].AsText);
        Assert.Contains("INSERT INTO audit", triggerRow[4].AsText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SystemCatalog_ExposesObjects()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", ct);
        await _db.ExecuteAsync("CREATE TABLE audit (user_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_users_age ON users(age)", ct);
        await _db.ExecuteAsync("CREATE VIEW v_users AS SELECT id, name, age FROM users", ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER trg_users_audit AFTER INSERT ON users
            BEGIN
                INSERT INTO audit VALUES (NEW.id);
            END;
            """, ct);

        await using var objects = await _db.ExecuteAsync(
            "SELECT object_name, object_type, parent_table_name FROM sys.objects ORDER BY object_type, object_name", ct);
        var objectRows = await objects.ToListAsync(ct);
        Assert.Equal(5, objectRows.Count);

        Assert.Collection(
            objectRows,
            row =>
            {
                Assert.Equal("idx_users_age", row[0].AsText);
                Assert.Equal("INDEX", row[1].AsText);
                Assert.Equal("users", row[2].AsText);
            },
            row =>
            {
                Assert.Equal("audit", row[0].AsText);
                Assert.Equal("TABLE", row[1].AsText);
                Assert.True(row[2].IsNull);
            },
            row =>
            {
                Assert.Equal("users", row[0].AsText);
                Assert.Equal("TABLE", row[1].AsText);
                Assert.True(row[2].IsNull);
            },
            row =>
            {
                Assert.Equal("trg_users_audit", row[0].AsText);
                Assert.Equal("TRIGGER", row[1].AsText);
                Assert.Equal("users", row[2].AsText);
            },
            row =>
            {
                Assert.Equal("v_users", row[0].AsText);
                Assert.Equal("VIEW", row[1].AsText);
                Assert.True(row[2].IsNull);
            });
    }

    [Fact]
    public async Task SystemCatalog_AllowsUnderscoredAliases()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);

        await using var tableCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_tables WHERE table_name = 't'", ct);
        var tableCountRows = await tableCount.ToListAsync(ct);
        Assert.Equal(1L, Assert.Single(tableCountRows)[0].AsInteger);

        await using var columnCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_columns WHERE table_name = 't'", ct);
        var columnCountRows = await columnCount.ToListAsync(ct);
        Assert.Equal(2L, Assert.Single(columnCountRows)[0].AsInteger);

        await using var objectCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_objects WHERE object_name = 't' AND object_type = 'TABLE'", ct);
        var objectCountRows = await objectCount.ToListAsync(ct);
        Assert.Equal(1L, Assert.Single(objectCountRows)[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_SchemaVersion_AdvancesOnDdlOnly()
    {
        var ct = TestContext.Current.CancellationToken;

        long v0 = _db.SchemaVersion;

        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        long v1 = _db.SchemaVersion;
        Assert.True(v1 > v0);

        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
        long afterInsert = _db.SchemaVersion;
        Assert.Equal(v1, afterInsert);

        await _db.ExecuteAsync("ALTER TABLE t ADD COLUMN extra TEXT", ct);
        long v2 = _db.SchemaVersion;
        Assert.True(v2 > v1);

        await _db.ExecuteAsync("CREATE INDEX idx_t_extra ON t(id)", ct);
        long v3 = _db.SchemaVersion;
        Assert.True(v3 > v2);

        await _db.ExecuteAsync("DROP INDEX idx_t_extra", ct);
        long v4 = _db.SchemaVersion;
        Assert.True(v4 > v3);

        await _db.ExecuteAsync("CREATE VIEW t_view AS SELECT id FROM t", ct);
        long v5 = _db.SchemaVersion;
        Assert.True(v5 > v4);

        await _db.ExecuteAsync("DROP VIEW t_view", ct);
        long v6 = _db.SchemaVersion;
        Assert.True(v6 > v5);
    }

    [Fact]
    public async Task SystemCatalog_MetadataCollections_ReflectDdlUpdates()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("CREATE TABLE audit (id INTEGER)", ct);

        Assert.Empty(_db.GetIndexes());
        Assert.DoesNotContain(_db.GetViewNames(), n => string.Equals(n, "v_t", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(_db.GetTriggers(), t => string.Equals(t.TriggerName, "trg_t", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("CREATE INDEX idx_t_val ON t(val)", ct);
        Assert.Contains(_db.GetIndexes(), i => string.Equals(i.IndexName, "idx_t_val", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("DROP INDEX idx_t_val", ct);
        Assert.DoesNotContain(_db.GetIndexes(), i => string.Equals(i.IndexName, "idx_t_val", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("CREATE VIEW v_t AS SELECT id FROM t", ct);
        Assert.Contains(_db.GetViewNames(), n => string.Equals(n, "v_t", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("DROP VIEW v_t", ct);
        Assert.DoesNotContain(_db.GetViewNames(), n => string.Equals(n, "v_t", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END",
            ct);
        Assert.Contains(_db.GetTriggers(), t => string.Equals(t.TriggerName, "trg_t", StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("DROP TRIGGER trg_t", ct);
        Assert.DoesNotContain(_db.GetTriggers(), t => string.Equals(t.TriggerName, "trg_t", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task SystemCatalog_CountStar_UsesCurrentMetadata()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("CREATE TABLE audit (id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_t_val ON t(val)", ct);
        await _db.ExecuteAsync("CREATE VIEW v_t AS SELECT id FROM t", ct);
        await _db.ExecuteAsync(
            "CREATE TRIGGER trg_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END",
            ct);

        await using var tableCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.tables", ct);
        Assert.Equal(2L, Assert.Single(await tableCountResult.ToListAsync(ct))[0].AsInteger);

        await using var columnCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys_columns", ct);
        Assert.Equal(3L, Assert.Single(await columnCountResult.ToListAsync(ct))[0].AsInteger);

        await using var indexCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.indexes", ct);
        Assert.Equal(1L, Assert.Single(await indexCountResult.ToListAsync(ct))[0].AsInteger);

        await using var viewCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.views", ct);
        Assert.Equal(1L, Assert.Single(await viewCountResult.ToListAsync(ct))[0].AsInteger);

        await using var triggerCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys_triggers", ct);
        Assert.Equal(1L, Assert.Single(await triggerCountResult.ToListAsync(ct))[0].AsInteger);

        await using var objectCountResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.objects", ct);
        Assert.Equal(5L, Assert.Single(await objectCountResult.ToListAsync(ct))[0].AsInteger);
    }
}
