using System.Text;
using CSharpDB.Client;
using ClientModels = CSharpDB.Client.Models;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class DatabaseMaintenanceTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private ICSharpDbClient _client = null!;

    public DatabaseMaintenanceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_maintenance_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _client = CreateClient();
        _ = await _client.GetInfoAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task GetMaintenanceReportAsync_ReturnsCurrentSpaceMetrics()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE metrics (id INTEGER PRIMARY KEY, value TEXT);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO metrics VALUES (1, 'alpha');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO metrics VALUES (2, 'beta');", Ct);

        var report = await _client.GetMaintenanceReportAsync(Ct);

        Assert.Equal(Path.GetFullPath(_dbPath), report.DatabasePath);
        Assert.True(report.SpaceUsage.DatabaseFileBytes > 0);
        Assert.True(report.SpaceUsage.PageSizeBytes > 0);
        Assert.True(report.SpaceUsage.PhysicalPageCount > 0);
        Assert.NotEmpty(report.PageTypeHistogram);
        Assert.True(report.Fragmentation.BTreeFreeBytes >= 0);
        Assert.True(report.Fragmentation.PagesWithFreeSpace >= 0);
    }

    [Fact]
    public async Task ReindexAsync_RebuildsNamedIndexTableScopeAndFullScope()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE people (id INTEGER PRIMARY KEY, age INTEGER, name TEXT);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (1, 30, 'Alice');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (2, 30, 'Bob');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (3, 25, 'Cara');", Ct);
        await _client.CreateIndexAsync("idx_people_age", "people", "age", isUnique: false, Ct);
        await _client.CreateIndexAsync("idx_people_name", "people", "name", isUnique: false, Ct);

        var single = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Index,
            Name = "idx_people_age",
        }, Ct);

        var table = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Table,
            Name = "people",
        }, Ct);

        var all = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.All,
        }, Ct);

        var indexReport = await _client.CheckIndexesAsync(ct: Ct);
        var query = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM people WHERE age = 30;", Ct);

        Assert.Equal(1, single.RebuiltIndexCount);
        Assert.Equal(2, table.RebuiltIndexCount);
        Assert.True(all.RebuiltIndexCount >= 2);
        Assert.DoesNotContain(indexReport.Issues, issue => issue.Severity == CSharpDB.Storage.Diagnostics.InspectSeverity.Error);
        Assert.Null(query.Error);
        Assert.True(query.IsQuery);
        Assert.NotNull(query.Rows);
        Assert.Equal(2L, Convert.ToInt64(Assert.Single(query.Rows)[0]));
    }

    [Fact]
    public async Task ReindexAsync_RebuildsCollectionIndexes()
    {
        await _client.DisposeAsync();

        string collectionIndexName;
        await using (var db = await Database.OpenAsync(_dbPath, Ct))
        {
            var users = await db.GetCollectionAsync<UserDoc>("users", Ct);
            await users.PutAsync("u:1", new UserDoc("Alice", 30), Ct);
            await users.PutAsync("u:2", new UserDoc("Bob", 30), Ct);
            await users.PutAsync("u:3", new UserDoc("Cara", 25), Ct);
            await users.EnsureIndexAsync(x => x.Age, Ct);
            collectionIndexName = db.GetIndexes().Single(index => string.Equals(index.TableName, "_col_users", StringComparison.OrdinalIgnoreCase)).IndexName;
        }

        _client = CreateClient();

        var result = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Index,
            Name = collectionIndexName,
        }, Ct);

        await using var reopened = await Database.OpenAsync(_dbPath, Ct);
        var reopenedUsers = await reopened.GetCollectionAsync<UserDoc>("users", Ct);
        var matches = await CollectAsync(reopenedUsers.FindByIndexAsync(x => x.Age, 30, Ct), Ct);

        Assert.Equal(1, result.RebuiltIndexCount);
        Assert.Equal(["u:1", "u:2"], matches.Select(item => item.Key).OrderBy(key => key).ToArray());
    }

    [Fact]
    public async Task VacuumAsync_RewritesDatabaseAndPreservesCatalogObjects()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL);", Ct);
        await _client.ExecuteSqlAsync("CREATE TABLE order_audit (order_id INTEGER NOT NULL);", Ct);
        await _client.CreateIndexAsync("idx_orders_total", "orders", "total", isUnique: false, Ct);
        await _client.CreateViewAsync("orders_view", "SELECT id, total FROM orders", Ct);
        await _client.CreateTriggerAsync("trg_orders_audit", "orders", ClientModels.TriggerTiming.After, ClientModels.TriggerEvent.Insert, "INSERT INTO order_audit VALUES (NEW.id)", Ct);
        await _client.UpsertSavedQueryAsync("orders_query", "SELECT * FROM orders ORDER BY id;", Ct);
        await _client.CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "orders_proc",
            BodySql = "SELECT COUNT(*) FROM orders;",
            Parameters = [],
            Description = "Maintenance test procedure",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        }, Ct);

        for (int i = 1; i <= 50; i++)
            await _client.ExecuteSqlAsync($"INSERT INTO orders VALUES ({i}, {i * 10});", Ct);
        await _client.ExecuteSqlAsync("ANALYZE orders;", Ct);

        await _client.ExecuteSqlAsync("CREATE TABLE vacuum_junk (id INTEGER PRIMARY KEY, payload TEXT);", Ct);
        for (int i = 1; i <= 75; i++)
            await _client.ExecuteSqlAsync($"INSERT INTO vacuum_junk VALUES ({i}, '{new string('x', 128)}');", Ct);
        await _client.DropTableAsync("vacuum_junk", Ct);

        await _client.DisposeAsync();
        _client = CreateClient();
        var beforeVacuumSavedQueryRows = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM __saved_queries WHERE name = 'orders_query';", Ct);
        var beforeVacuumProcedureRows = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM __procedures WHERE name = 'orders_proc';", Ct);

        await _client.DisposeAsync();
        _client = CreateClient();
        await _client.DisposeAsync();

        var result = await DatabaseMaintenanceCoordinator.VacuumAsync(_dbPath, Ct);

        await using var rawDb = await Database.OpenAsync(_dbPath, Ct);
        var tables = rawDb.GetTableNames().Where(name => !name.StartsWith("__", StringComparison.Ordinal)).ToArray();
        var views = rawDb.GetViewNames().ToArray();
        var triggers = rawDb.GetTriggers().ToArray();
        await using var auditResult = await rawDb.ExecuteAsync("SELECT COUNT(*) FROM order_audit;", Ct);
        await using var savedQueryResult = await rawDb.ExecuteAsync("SELECT COUNT(*) FROM __saved_queries WHERE name = 'orders_query';", Ct);
        await using var procedureResult = await rawDb.ExecuteAsync("SELECT COUNT(*) FROM __procedures WHERE name = 'orders_proc';", Ct);
        await using var columnStatsResult = await rawDb.ExecuteAsync(
            "SELECT distinct_count, min_value, max_value, is_stale FROM sys.column_stats WHERE table_name = 'orders' AND column_name = 'total';",
            Ct);
        var queryRows = await auditResult.ToListAsync(Ct);
        var savedQueryRows = await savedQueryResult.ToListAsync(Ct);
        var procedureRows = await procedureResult.ToListAsync(Ct);
        var columnStatsRows = await columnStatsResult.ToListAsync(Ct);
        _client = CreateClient();

        Assert.True(result.DatabaseFileBytesAfter <= result.DatabaseFileBytesBefore);
        Assert.Contains("orders", tables);
        Assert.Contains("order_audit", tables);
        Assert.Contains("orders_view", views);
        Assert.Contains(triggers, trigger => string.Equals(trigger.TriggerName, "trg_orders_audit", StringComparison.OrdinalIgnoreCase));
        Assert.Null(beforeVacuumSavedQueryRows.Error);
        Assert.NotNull(beforeVacuumSavedQueryRows.Rows);
        Assert.Null(beforeVacuumProcedureRows.Error);
        Assert.NotNull(beforeVacuumProcedureRows.Rows);
        Assert.Equal(1L, Convert.ToInt64(Assert.Single(beforeVacuumSavedQueryRows.Rows)[0]));
        Assert.Equal(1L, Convert.ToInt64(Assert.Single(beforeVacuumProcedureRows.Rows)[0]));
        Assert.Equal(50L, Assert.Single(queryRows)[0].AsInteger);
        Assert.Equal(1L, Assert.Single(savedQueryRows)[0].AsInteger);
        Assert.Equal(1L, Assert.Single(procedureRows)[0].AsInteger);
        var totalStats = Assert.Single(columnStatsRows);
        Assert.Equal(50L, totalStats[0].AsInteger);
        Assert.Equal(10L, totalStats[1].AsInteger);
        Assert.Equal(500L, totalStats[2].AsInteger);
        Assert.Equal(0L, totalStats[3].AsInteger);
    }

    [Fact]
    public async Task VacuumAsync_PreservesLegacyUnknownNextRowId()
    {
        await _client.DisposeAsync();

        await using (var db = await Database.OpenAsync(_dbPath, Ct))
        {
            await db.ExecuteAsync("CREATE TABLE legacy_t (id INTEGER PRIMARY KEY, name TEXT);", Ct);
            await db.ExecuteAsync("INSERT INTO legacy_t (id, name) VALUES (1, 'one');", Ct);
            await db.ExecuteAsync("INSERT INTO legacy_t (id, name) VALUES (3, 'three');", Ct);
        }

        await RewriteTableSchemaAsLegacyAsync(_dbPath, "legacy_t", Ct);

        _client = CreateClient();
        await _client.VacuumAsync(Ct);
        await _client.DisposeAsync();

        await using var reopened = await Database.OpenAsync(_dbPath, Ct);
        await reopened.ExecuteAsync("INSERT INTO legacy_t (name) VALUES ('next');", Ct);
        await using var result = await reopened.ExecuteAsync("SELECT id FROM legacy_t WHERE name = 'next';", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(4L, Assert.Single(rows)[0].AsInteger);

        _client = CreateClient();
    }

    [Fact]
    public async Task VacuumAsync_WhenReplacementFailsAfterBackupMove_PreservesBackupCopy()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE backup_guard (id INTEGER PRIMARY KEY, value TEXT);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO backup_guard VALUES (1, 'alpha');", Ct);
        await _client.DisposeAsync();

        string tempPath = _dbPath + ".vacuum-test.tmp";
        string backupPath = _dbPath + ".vacuum-backup-test.tmp";

        try
        {
            var ex = await Assert.ThrowsAsync<IOException>(() =>
                DatabaseMaintenanceCoordinator.VacuumAsync(
                    _dbPath,
                    Ct,
                    static (fullPath, _, backupFilePath, cancellationToken) =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        File.Move(fullPath, backupFilePath, overwrite: true);
                        throw new IOException("Simulated replacement failure.");
                    },
                    tempPath,
                    backupPath).AsTask());

            Assert.Contains("Simulated replacement failure", ex.Message, StringComparison.Ordinal);
            Assert.False(File.Exists(tempPath));
            Assert.False(File.Exists(tempPath + ".wal"));
            Assert.True(File.Exists(backupPath));

            await using var backupDb = await Database.OpenAsync(backupPath, Ct);
            await using var result = await backupDb.ExecuteAsync("SELECT COUNT(*) FROM backup_guard;", Ct);
            var rows = await result.ToListAsync(Ct);
            Assert.Equal(1L, Assert.Single(rows)[0].AsInteger);
        }
        finally
        {
            if (File.Exists(backupPath) && !File.Exists(_dbPath))
                File.Move(backupPath, _dbPath, overwrite: true);

            if (File.Exists(tempPath))
                File.Delete(tempPath);
            if (File.Exists(tempPath + ".wal"))
                File.Delete(tempPath + ".wal");

            _client = CreateClient();
            _ = await _client.GetInfoAsync(Ct);
        }
    }

    [Fact]
    public async Task ReindexAsync_RejectsActiveClientManagedTransactions()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE tx_guard (id INTEGER PRIMARY KEY, value TEXT);", Ct);
        await _client.DisposeAsync();
        _client = CreateClient();
        var transaction = await _client.BeginTransactionAsync(Ct);

        try
        {
            var ex = await Assert.ThrowsAsync<CSharpDbClientException>(() => _client.ReindexAsync(new ClientModels.ReindexRequest(), Ct));
            Assert.Contains("exclusive access", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await _client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    private ICSharpDbClient CreateClient()
        => CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _dbPath });

    private static async Task<List<KeyValuePair<string, TDocument>>> CollectAsync<TDocument>(
        IAsyncEnumerable<KeyValuePair<string, TDocument>> source,
        CancellationToken ct)
    {
        var items = new List<KeyValuePair<string, TDocument>>();
        await foreach (var item in source.WithCancellation(ct))
            items.Add(item);
        return items;
    }

    private static async ValueTask RewriteTableSchemaAsLegacyAsync(string dbPath, string tableName, CancellationToken ct)
    {
        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(dbPath, new StorageEngineOptions(), ct);
        try
        {
            var schema = context.Catalog.GetTable(tableName);
            Assert.NotNull(schema);

            uint tableRootPage = context.Catalog.GetTableRootPage(tableName);
            byte[] legacySchemaBytes = BuildLegacyTableSchemaPayload(tableName, schema!.Columns);
            byte[] payload = new CatalogStore().WriteRootPayload(tableRootPage, legacySchemaBytes);

            var catalogTree = new BTree(context.Pager, context.Pager.SchemaRootPage);
            long key = context.SchemaSerializer.TableNameToKey(tableName);

            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                Assert.True(await catalogTree.DeleteAsync(key, ct));
                await catalogTree.InsertAsync(key, payload, ct);
                await context.Pager.CommitAsync(ct);
            }
            catch
            {
                await context.Pager.RollbackAsync(ct);
                throw;
            }
        }
        finally
        {
            await context.Pager.DisposeAsync();
        }
    }

    private static byte[] BuildLegacyTableSchemaPayload(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (var column in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(column.Name));
            ms.Write(Encoding.UTF8.GetBytes(column.Name));
            ms.WriteByte((byte)column.Type);

            byte flags = 0;
            if (column.Nullable)
                flags |= 0x01;
            if (column.IsPrimaryKey)
                flags |= 0x02;
            ms.WriteByte(flags);
        }

        return ms.ToArray();
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int length = Varint.Write(buffer, value);
        stream.Write(buffer[..length]);
    }

    private sealed record UserDoc(string Name, int Age);
}
