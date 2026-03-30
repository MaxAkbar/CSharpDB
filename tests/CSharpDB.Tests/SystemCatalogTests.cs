using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

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
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE NOT NULL, age INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_users_name_binary ON users(name COLLATE BINARY)", ct);

        await using var tables = await _db.ExecuteAsync(
            "SELECT table_name, column_count, primary_key_column FROM sys.tables WHERE table_name = 'users'", ct);
        var tableRows = await tables.ToListAsync(ct);
        var tableRow = Assert.Single(tableRows);
        Assert.Equal("users", tableRow[0].AsText);
        Assert.Equal(3L, tableRow[1].AsInteger);
        Assert.Equal("id", tableRow[2].AsText);

        await using var columns = await _db.ExecuteAsync(
            "SELECT column_name, ordinal_position, data_type, is_nullable, is_primary_key, is_identity, collation " +
            "FROM sys.columns WHERE table_name = 'users' ORDER BY ordinal_position", ct);
        var columnRows = await columns.ToListAsync(ct);
        Assert.Equal(3, columnRows.Count);
        Assert.Equal("id", columnRows[0][0].AsText);
        Assert.Equal(1L, columnRows[0][1].AsInteger);
        Assert.Equal("INTEGER", columnRows[0][2].AsText);
        Assert.Equal(1L, columnRows[0][4].AsInteger);
        Assert.Equal(1L, columnRows[0][5].AsInteger);
        Assert.True(columnRows[0][6].IsNull);
        Assert.Equal("name", columnRows[1][0].AsText);
        Assert.Equal("TEXT", columnRows[1][2].AsText);
        Assert.Equal(0L, columnRows[1][3].AsInteger);
        Assert.Equal(0L, columnRows[1][4].AsInteger);
        Assert.Equal(0L, columnRows[1][5].AsInteger);
        Assert.Equal("NOCASE", columnRows[1][6].AsText);

        await using var indexes = await _db.ExecuteAsync(
            "SELECT index_name, table_name, column_name, ordinal_position, is_unique, collation " +
            "FROM sys.indexes WHERE index_name = 'idx_users_name_binary'", ct);
        var indexRows = await indexes.ToListAsync(ct);
        var indexRow = Assert.Single(indexRows);
        Assert.Equal("idx_users_name_binary", indexRow[0].AsText);
        Assert.Equal("users", indexRow[1].AsText);
        Assert.Equal("name", indexRow[2].AsText);
        Assert.Equal(1L, indexRow[3].AsInteger);
        Assert.Equal(0L, indexRow[4].AsInteger);
        Assert.Equal("BINARY", indexRow[5].AsText);
    }

    [Fact]
    public async Task SystemCatalog_IndexCollation_UsesEffectiveColumnCollationWhenInherited()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_users_name ON users(name)", ct);

        await using var indexes = await _db.ExecuteAsync(
            "SELECT collation FROM sys.indexes WHERE index_name = 'idx_users_name'",
            ct);
        var indexRow = Assert.Single(await indexes.ToListAsync(ct));

        Assert.Equal("NOCASE", indexRow[0].AsText);
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
    public async Task SystemCatalog_ExposesForeignKeys_AndHidesSupportIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)",
            ct);

        await using var foreignKeys = await _db.ExecuteAsync(
            "SELECT constraint_name, table_name, column_name, referenced_table_name, referenced_column_name, on_delete, supporting_index_name FROM sys.foreign_keys",
            ct);
        var foreignKeyRow = Assert.Single(await foreignKeys.ToListAsync(ct));
        string supportingIndexName = foreignKeyRow[6].AsText;

        Assert.Equal("children", foreignKeyRow[1].AsText);
        Assert.Equal("parent_id", foreignKeyRow[2].AsText);
        Assert.Equal("parents", foreignKeyRow[3].AsText);
        Assert.Equal("id", foreignKeyRow[4].AsText);
        Assert.Equal("CASCADE", foreignKeyRow[5].AsText);

        await using var systemIndexes = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.indexes WHERE index_name = '" + supportingIndexName + "'",
            ct);
        Assert.Equal(0L, Assert.Single(await systemIndexes.ToListAsync(ct))[0].AsInteger);

        await using var objects = await _db.ExecuteAsync(
            "SELECT object_name, object_type, parent_table_name FROM sys.objects WHERE object_type = 'FOREIGN KEY'",
            ct);
        var objectRow = Assert.Single(await objects.ToListAsync(ct));
        Assert.Equal(foreignKeyRow[0].AsText, objectRow[0].AsText);
        Assert.Equal("children", objectRow[2].AsText);
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
    public async Task SystemCatalog_SavedQueriesVirtualTable_EmptyWhenBackingTableMissing()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var dotted = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.saved_queries", ct);
        Assert.Equal(0L, Assert.Single(await dotted.ToListAsync(ct))[0].AsInteger);

        await using var underscored = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys_saved_queries", ct);
        Assert.Equal(0L, Assert.Single(await underscored.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_SavedQueriesVirtualTable_ProjectsBackingRows()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync(
            """
            CREATE TABLE __saved_queries (
                id INTEGER PRIMARY KEY IDENTITY,
                name TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx___saved_queries_name ON __saved_queries(name)", ct);
        await _db.ExecuteAsync(
            """
            INSERT INTO __saved_queries (name, sql_text, created_utc, updated_utc)
            VALUES ('recent_users', 'SELECT * FROM users ORDER BY id DESC LIMIT 25;', '2026-03-06T00:00:00Z', '2026-03-06T00:00:00Z')
            """,
            ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT id, name, sql_text, created_utc, updated_utc FROM sys.saved_queries", ct);
        var rows = await result.ToListAsync(ct);
        var row = Assert.Single(rows);
        Assert.Equal(1L, row[0].AsInteger);
        Assert.Equal("recent_users", row[1].AsText);
        Assert.Contains("SELECT * FROM users", row[2].AsText, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("2026-03-06T00:00:00Z", row[3].AsText);
        Assert.Equal("2026-03-06T00:00:00Z", row[4].AsText);
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

    [Fact]
    public async Task SystemCatalog_TableStats_TracksExactRowCounts()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE stats_users (id INTEGER PRIMARY KEY, val INTEGER)", ct);

        await using var initialStats = await _db.ExecuteAsync(
            "SELECT row_count, has_stale_columns FROM sys.table_stats WHERE table_name = 'stats_users'",
            ct);
        var initialRow = Assert.Single(await initialStats.ToListAsync(ct));
        Assert.Equal(0L, initialRow[0].AsInteger);
        Assert.Equal(0L, initialRow[1].AsInteger);

        await _db.ExecuteAsync("INSERT INTO stats_users VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO stats_users VALUES (2, 20)", ct);
        await _db.ExecuteAsync("DELETE FROM stats_users WHERE id = 1", ct);
        await _db.ExecuteAsync("ANALYZE stats_users", ct);

        await using var refreshedStats = await _db.ExecuteAsync(
            "SELECT row_count, has_stale_columns FROM sys.table_stats WHERE table_name = 'stats_users'",
            ct);
        var refreshedRow = Assert.Single(await refreshedStats.ToListAsync(ct));
        Assert.Equal(1L, refreshedRow[0].AsInteger);
        Assert.Equal(0L, refreshedRow[1].AsInteger);

        await using var tableStatsCount = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.table_stats", ct);
        Assert.Equal(1L, Assert.Single(await tableStatsCount.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_TableStats_PersistAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE persisted_stats (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO persisted_stats VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO persisted_stats VALUES (2, 20)", ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var statsResult = await _db.ExecuteAsync(
            "SELECT row_count FROM sys.table_stats WHERE table_name = 'persisted_stats'",
            ct);
        Assert.Equal(2L, Assert.Single(await statsResult.ToListAsync(ct))[0].AsInteger);

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM persisted_stats", ct);
        Assert.Equal(2L, Assert.Single(await countResult.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_TableStats_RollbackRestoresCounts()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE rollback_stats (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO rollback_stats VALUES (1, 10)", ct);

        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("INSERT INTO rollback_stats VALUES (2, 20)", ct);

        await using var inTxStats = await _db.ExecuteAsync(
            "SELECT row_count FROM sys.table_stats WHERE table_name = 'rollback_stats'",
            ct);
        Assert.Equal(2L, Assert.Single(await inTxStats.ToListAsync(ct))[0].AsInteger);

        await _db.RollbackAsync(ct);

        await using var afterRollbackStats = await _db.ExecuteAsync(
            "SELECT row_count FROM sys.table_stats WHERE table_name = 'rollback_stats'",
            ct);
        Assert.Equal(1L, Assert.Single(await afterRollbackStats.ToListAsync(ct))[0].AsInteger);

        await using var countResult = await _db.ExecuteAsync("SELECT COUNT(*) FROM rollback_stats", ct);
        Assert.Equal(1L, Assert.Single(await countResult.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_ColumnStats_ExposeAnalyzeResultsAndUnderscoredAlias()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE column_stats_users (id INTEGER PRIMARY KEY, age INTEGER, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO column_stats_users VALUES (1, 20, 'Alice')", ct);
        await _db.ExecuteAsync("INSERT INTO column_stats_users VALUES (2, NULL, 'Bob')", ct);
        await _db.ExecuteAsync("INSERT INTO column_stats_users VALUES (3, 40, 'Bob')", ct);

        await using var beforeAnalyze = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys_column_stats WHERE table_name = 'column_stats_users'",
            ct);
        Assert.Equal(0L, Assert.Single(await beforeAnalyze.ToListAsync(ct))[0].AsInteger);

        await _db.ExecuteAsync("ANALYZE column_stats_users", ct);

        await using var result = await _db.ExecuteAsync(
            """
            SELECT column_name, ordinal_position, distinct_count, non_null_count, min_value, max_value, is_stale
            FROM sys.column_stats
            WHERE table_name = 'column_stats_users'
            ORDER BY ordinal_position
            """,
            ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(3, rows.Count);

        Assert.Equal("id", rows[0][0].AsText);
        Assert.Equal(1L, rows[0][1].AsInteger);
        Assert.Equal(3L, rows[0][2].AsInteger);
        Assert.Equal(3L, rows[0][3].AsInteger);
        Assert.Equal(1L, rows[0][4].AsInteger);
        Assert.Equal(3L, rows[0][5].AsInteger);
        Assert.Equal(0L, rows[0][6].AsInteger);

        Assert.Equal("age", rows[1][0].AsText);
        Assert.Equal(2L, rows[1][1].AsInteger);
        Assert.Equal(2L, rows[1][2].AsInteger);
        Assert.Equal(2L, rows[1][3].AsInteger);
        Assert.Equal(20L, rows[1][4].AsInteger);
        Assert.Equal(40L, rows[1][5].AsInteger);
        Assert.Equal(0L, rows[1][6].AsInteger);

        Assert.Equal("name", rows[2][0].AsText);
        Assert.Equal(3L, rows[2][1].AsInteger);
        Assert.Equal(2L, rows[2][2].AsInteger);
        Assert.Equal(3L, rows[2][3].AsInteger);
        Assert.Equal("Alice", rows[2][4].AsText);
        Assert.Equal("Bob", rows[2][5].AsText);
        Assert.Equal(0L, rows[2][6].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_ColumnStats_BecomeStaleOnWriteAndRollbackRestoresFreshState()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE stale_column_stats (id INTEGER PRIMARY KEY, age INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO stale_column_stats VALUES (1, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO stale_column_stats VALUES (2, 40)", ct);
        await _db.ExecuteAsync("ANALYZE stale_column_stats", ct);

        await _db.ExecuteAsync("INSERT INTO stale_column_stats VALUES (3, 60)", ct);

        await using var staleTableStats = await _db.ExecuteAsync(
            "SELECT has_stale_columns FROM sys.table_stats WHERE table_name = 'stale_column_stats'",
            ct);
        Assert.Equal(1L, Assert.Single(await staleTableStats.ToListAsync(ct))[0].AsInteger);

        await using var staleColumnStats = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.column_stats WHERE table_name = 'stale_column_stats' AND is_stale = 1",
            ct);
        Assert.Equal(2L, Assert.Single(await staleColumnStats.ToListAsync(ct))[0].AsInteger);

        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("ANALYZE stale_column_stats", ct);
        await using var freshInTx = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.column_stats WHERE table_name = 'stale_column_stats' AND is_stale = 1",
            ct);
        Assert.Equal(0L, Assert.Single(await freshInTx.ToListAsync(ct))[0].AsInteger);

        await _db.ExecuteAsync("INSERT INTO stale_column_stats VALUES (4, 80)", ct);
        await using var staleInTx = await _db.ExecuteAsync(
            "SELECT has_stale_columns FROM sys.table_stats WHERE table_name = 'stale_column_stats'",
            ct);
        Assert.Equal(1L, Assert.Single(await staleInTx.ToListAsync(ct))[0].AsInteger);

        await _db.RollbackAsync(ct);

        await using var afterRollbackTableStats = await _db.ExecuteAsync(
            "SELECT has_stale_columns FROM sys.table_stats WHERE table_name = 'stale_column_stats'",
            ct);
        Assert.Equal(1L, Assert.Single(await afterRollbackTableStats.ToListAsync(ct))[0].AsInteger);

        await using var afterRollbackColumnStats = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.column_stats WHERE table_name = 'stale_column_stats' AND is_stale = 1",
            ct);
        Assert.Equal(2L, Assert.Single(await afterRollbackColumnStats.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_ColumnStats_PersistAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE persisted_column_stats (id INTEGER PRIMARY KEY, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO persisted_column_stats VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO persisted_column_stats VALUES (2, 30)", ct);
        await _db.ExecuteAsync("ANALYZE persisted_column_stats", ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var result = await _db.ExecuteAsync(
            """
            SELECT distinct_count, non_null_count, min_value, max_value, is_stale
            FROM sys.column_stats
            WHERE table_name = 'persisted_column_stats' AND column_name = 'score'
            """,
            ct);
        var row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(2L, row[0].AsInteger);
        Assert.Equal(2L, row[1].AsInteger);
        Assert.Equal(10L, row[2].AsInteger);
        Assert.Equal(30L, row[3].AsInteger);
        Assert.Equal(0L, row[4].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_ImmediateStats_RemainFreshAcrossReopen_AfterUnrelatedWrites()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.ExecuteAsync("CREATE TABLE primary_stats (id INTEGER PRIMARY KEY, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO primary_stats VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO primary_stats VALUES (2, 30)", ct);
        await _db.ExecuteAsync("ANALYZE primary_stats", ct);

        await _db.ExecuteAsync("CREATE TABLE unrelated_stats_churn (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO unrelated_stats_churn VALUES (1, 'noise')", ct);

        await using (var inSessionStats = await _db.ExecuteAsync(
            "SELECT is_stale FROM sys.column_stats WHERE table_name = 'primary_stats' AND column_name = 'score'",
            ct))
        {
            Assert.Equal(0L, Assert.Single(await inSessionStats.ToListAsync(ct))[0].AsInteger);
        }

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var tableStats = await _db.ExecuteAsync(
            "SELECT row_count, has_stale_columns FROM sys.table_stats WHERE table_name = 'primary_stats'",
            ct);
        var tableStatsRow = Assert.Single(await tableStats.ToListAsync(ct));
        Assert.Equal(2L, tableStatsRow[0].AsInteger);
        Assert.Equal(0L, tableStatsRow[1].AsInteger);

        await using var columnStats = await _db.ExecuteAsync(
            """
            SELECT distinct_count, non_null_count, min_value, max_value, is_stale
            FROM sys.column_stats
            WHERE table_name = 'primary_stats' AND column_name = 'score'
            """,
            ct);
        var columnStatsRow = Assert.Single(await columnStats.ToListAsync(ct));
        Assert.Equal(2L, columnStatsRow[0].AsInteger);
        Assert.Equal(2L, columnStatsRow[1].AsInteger);
        Assert.Equal(10L, columnStatsRow[2].AsInteger);
        Assert.Equal(30L, columnStatsRow[3].AsInteger);
        Assert.Equal(0L, columnStatsRow[4].AsInteger);
    }

    [Fact]
    public async Task SystemCatalog_DeferredAdvisoryStatistics_ArePersistedOnCleanDispose()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_syscat_deferred_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UseLowLatencyDurableWritePreset());

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, options, ct))
            {
                await db.ExecuteAsync("CREATE TABLE deferred_stats (id INTEGER PRIMARY KEY, age INTEGER)", ct);
                await db.ExecuteAsync("INSERT INTO deferred_stats VALUES (1, 20)", ct);
                await db.ExecuteAsync("INSERT INTO deferred_stats VALUES (2, 40)", ct);
                await db.ExecuteAsync("ANALYZE deferred_stats", ct);
                await db.ExecuteAsync("INSERT INTO deferred_stats VALUES (3, 60)", ct);
            }

            await using var reopened = await Database.OpenAsync(dbPath, options, ct);

            await using var statsResult = await reopened.ExecuteAsync(
                "SELECT row_count, has_stale_columns FROM sys.table_stats WHERE table_name = 'deferred_stats'",
                ct);
            var statsRow = Assert.Single(await statsResult.ToListAsync(ct));
            Assert.Equal(3L, statsRow[0].AsInteger);
            Assert.Equal(1L, statsRow[1].AsInteger);

            await using var staleColumnStats = await reopened.ExecuteAsync(
                "SELECT COUNT(*) FROM sys.column_stats WHERE table_name = 'deferred_stats' AND is_stale = 1",
                ct);
            Assert.Equal(2L, Assert.Single(await staleColumnStats.ToListAsync(ct))[0].AsInteger);

            await using var countResult = await reopened.ExecuteAsync("SELECT COUNT(*) FROM deferred_stats", ct);
            Assert.Equal(3L, Assert.Single(await countResult.ToListAsync(ct))[0].AsInteger);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task AutoCommitSchemaFailure_ReloadsCatalogBeforeFurtherReads()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(
            _dbPath,
            new DatabaseOptions
            {
                StorageEngineFactory = new FailFirstCommitStorageEngineFactory(new FailFirstCommitWalFlushPolicy()),
            },
            ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("CREATE TABLE failed_create (id INTEGER PRIMARY KEY)", ct).AsTask());
        Assert.Equal(ErrorCode.WalError, error.Code);
        Assert.DoesNotContain("failed_create", _db.GetTableNames());
    }

    [Fact]
    public async Task AutoCommitCheckpointFailure_DoesNotFaultCommittedIndexCreate()
    {
        var ct = TestContext.Current.CancellationToken;
        var factory = new CheckpointFailureStorageEngineFactory();

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(
            _dbPath,
            new DatabaseOptions
            {
                StorageEngineFactory = factory,
                StorageEngineOptions = new StorageEngineOptions
                {
                    PagerOptions = new PagerOptions
                    {
                        CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                        AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Foreground,
                    },
                },
            },
            ct);

        await _db.ExecuteAsync("CREATE TABLE checkpointed_index (id INTEGER PRIMARY KEY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO checkpointed_index VALUES (1, 'alpha')", ct);
        await _db.ExecuteAsync("INSERT INTO checkpointed_index VALUES (2, 'beta')", ct);

        factory.Device!.ArmFlushFailure();
        await _db.ExecuteAsync("CREATE INDEX idx_checkpointed_index_name ON checkpointed_index(name)", ct);

        Assert.Contains(
            _db.GetIndexes(),
            static index => string.Equals(index.IndexName, "idx_checkpointed_index_name", StringComparison.OrdinalIgnoreCase));

        factory.Device.DisarmFlushFailure();

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        Assert.Contains(
            _db.GetIndexes(),
            static index => string.Equals(index.IndexName, "idx_checkpointed_index_name", StringComparison.OrdinalIgnoreCase));

        await using var reopenedCount = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.indexes WHERE index_name = 'idx_checkpointed_index_name'",
            ct);
        var row = Assert.Single(await reopenedCount.ToListAsync(ct));
        Assert.Equal(1L, row[0].AsInteger);
    }

    private sealed class FailFirstCommitStorageEngineFactory : IStorageEngineFactory
    {
        private readonly IWalFlushPolicy _flushPolicy;

        public FailFirstCommitStorageEngineFactory(IWalFlushPolicy flushPolicy)
        {
            _flushPolicy = flushPolicy ?? throw new ArgumentNullException(nameof(flushPolicy));
        }

        public async ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            bool isNew = !File.Exists(filePath);
            FileStorageDevice? device = null;
            Pager? pager = null;

            try
            {
                device = new FileStorageDevice(filePath);
                var walIndex = new WalIndex();
                var wal = new WriteAheadLog(
                    filePath,
                    walIndex,
                    options.ChecksumProvider,
                    _flushPolicy,
                    options.DurableCommitBatchWindow,
                    options.WalPreallocationChunkBytes);
                pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

                if (isNew)
                {
                    await pager.InitializeNewDatabaseAsync(ct);
                    await wal.OpenAsync(pager.PageCount, ct);
                }
                else
                {
                    await pager.RecoverAsync(ct);
                }

                var schemaSerializer = options.SerializerProvider.SchemaSerializer;
                return new StorageEngineContext
                {
                    Pager = pager,
                    Catalog = await SchemaCatalog.CreateAsync(
                        pager,
                        schemaSerializer,
                        options.IndexProvider,
                        options.CatalogStore,
                        options.AdvisoryStatisticsPersistenceMode,
                        ct),
                    RecordSerializer = options.SerializerProvider.RecordSerializer,
                    SchemaSerializer = schemaSerializer,
                    IndexProvider = options.IndexProvider,
                    ChecksumProvider = options.ChecksumProvider,
                    AdvisoryStatisticsPersistenceMode = options.AdvisoryStatisticsPersistenceMode,
                };
            }
            catch
            {
                if (pager != null)
                    await pager.DisposeAsync();
                if (device != null)
                    await device.DisposeAsync();

                throw;
            }
        }
    }

    private sealed class CheckpointFailureStorageEngineFactory : IStorageEngineFactory
    {
        public ArmableCheckpointFailingStorageDevice? Device { get; private set; }

        public async ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            bool isNew = !File.Exists(filePath);
            ArmableCheckpointFailingStorageDevice? device = null;
            Pager? pager = null;

            try
            {
                device = new ArmableCheckpointFailingStorageDevice(new FileStorageDevice(filePath));
                Device = device;

                var walIndex = new WalIndex();
                var wal = new WriteAheadLog(
                    filePath,
                    walIndex,
                    options.ChecksumProvider,
                    options.DurabilityMode,
                    options.DurableCommitBatchWindow,
                    options.WalPreallocationChunkBytes);
                pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

                if (isNew)
                {
                    await pager.InitializeNewDatabaseAsync(ct);
                    await wal.OpenAsync(pager.PageCount, ct);
                }
                else
                {
                    await pager.RecoverAsync(ct);
                }

                var schemaSerializer = options.SerializerProvider.SchemaSerializer;
                return new StorageEngineContext
                {
                    Pager = pager,
                    Catalog = await SchemaCatalog.CreateAsync(
                        pager,
                        schemaSerializer,
                        options.IndexProvider,
                        options.CatalogStore,
                        options.AdvisoryStatisticsPersistenceMode,
                        ct),
                    RecordSerializer = options.SerializerProvider.RecordSerializer,
                    SchemaSerializer = schemaSerializer,
                    IndexProvider = options.IndexProvider,
                    ChecksumProvider = options.ChecksumProvider,
                    AdvisoryStatisticsPersistenceMode = options.AdvisoryStatisticsPersistenceMode,
                };
            }
            catch
            {
                if (pager != null)
                    await pager.DisposeAsync();
                if (device != null)
                    await device.DisposeAsync();

                throw;
            }
        }
    }

    private sealed class ArmableCheckpointFailingStorageDevice : IStorageDevice
    {
        private readonly IStorageDevice _inner;
        private int _failFlush;

        public ArmableCheckpointFailingStorageDevice(IStorageDevice inner)
        {
            _inner = inner;
        }

        public long Length => _inner.Length;

        public void ArmFlushFailure()
        {
            Interlocked.Exchange(ref _failFlush, 1);
        }

        public void DisarmFlushFailure()
        {
            Interlocked.Exchange(ref _failFlush, 0);
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) =>
            _inner.ReadAsync(offset, buffer, ct);

        public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default) =>
            _inner.WriteAsync(offset, buffer, ct);

        public ValueTask FlushAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (Volatile.Read(ref _failFlush) != 0)
                return ValueTask.FromException(new IOException("Injected checkpoint device flush failure."));

            return _inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) =>
            _inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();

        public void Dispose() => _inner.Dispose();
    }

    private sealed class FailFirstCommitWalFlushPolicy : IWalFlushPolicy
    {
        private int _flushCount;

        public bool AllowsWriteConcurrencyDuringCommitFlush => true;

        public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (stream.Position <= PageConstants.WalHeaderSize)
                return ValueTask.CompletedTask;

            if (Interlocked.Increment(ref _flushCount) == 1)
                return ValueTask.FromException(new IOException("Injected first commit flush failure."));

            return ValueTask.CompletedTask;
        }
    }
}
