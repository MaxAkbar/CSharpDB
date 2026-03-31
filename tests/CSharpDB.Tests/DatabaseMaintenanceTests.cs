using System.Text;
using System.Globalization;
using System.Buffers.Binary;
using CSharpDB.Client;
using ClientModels = CSharpDB.Client.Models;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;
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
    public async Task BackupAndRestoreAsync_RoundTripThroughClientContract()
    {
        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_maintenance_backup_{Guid.NewGuid():N}.db");
        string manifestPath = backupPath + ".manifest.json";

        try
        {
            await _client.ExecuteSqlAsync("CREATE TABLE restore_test (id INTEGER PRIMARY KEY, value TEXT);", Ct);
            await _client.ExecuteSqlAsync("INSERT INTO restore_test VALUES (1, 'before');", Ct);

            var backup = await _client.BackupAsync(new ClientModels.BackupRequest
            {
                DestinationPath = backupPath,
                WithManifest = true,
            }, Ct);

            Assert.Equal(Path.GetFullPath(_dbPath), backup.SourcePath);
            Assert.Equal(Path.GetFullPath(backupPath), backup.DestinationPath);
            Assert.Equal(manifestPath, backup.ManifestPath);
            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(manifestPath));

            await _client.ExecuteSqlAsync("INSERT INTO restore_test VALUES (2, 'after');", Ct);

            var validate = await _client.RestoreAsync(new ClientModels.RestoreRequest
            {
                SourcePath = backupPath,
                ValidateOnly = true,
            }, Ct);

            Assert.True(validate.ValidateOnly);
            Assert.Null(validate.DestinationPath);

            var restore = await _client.RestoreAsync(new ClientModels.RestoreRequest
            {
                SourcePath = backupPath,
            }, Ct);

            Assert.False(restore.ValidateOnly);
            Assert.Equal(Path.GetFullPath(_dbPath), restore.DestinationPath);

            var restoredRows = await _client.ExecuteSqlAsync("SELECT id, value FROM restore_test ORDER BY id;", Ct);
            Assert.Null(restoredRows.Error);
            Assert.NotNull(restoredRows.Rows);
            var row = Assert.Single(restoredRows.Rows);
            Assert.Equal(1L, row[0]);
            Assert.Equal("before", row[1]);
        }
        finally
        {
            if (File.Exists(backupPath))
                File.Delete(backupPath);
            if (File.Exists(backupPath + ".wal"))
                File.Delete(backupPath + ".wal");
            if (File.Exists(manifestPath))
                File.Delete(manifestPath);
        }
    }

    [Fact]
    public async Task MigrateForeignKeysAsync_ValidateOnly_LegacyDatabaseWithValidDataReportsSuccessWithoutMutation()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);", Ct);
        await _client.ExecuteSqlAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO customers VALUES (1, 'Ada');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (10, 1, 25);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (11, NULL, 50);", Ct);

        await _client.DisposeAsync();
        await RewriteTableSchemaAsLegacyAsync(_dbPath, "orders", Ct);
        _client = CreateClient();

        var result = await _client.MigrateForeignKeysAsync(
            new ClientModels.ForeignKeyMigrationRequest
            {
                ValidateOnly = true,
                Constraints =
                [
                    new ClientModels.ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "orders",
                        ColumnName = "customer_id",
                        ReferencedTableName = "customers",
                        ReferencedColumnName = "id",
                    },
                ],
            },
            Ct);

        Assert.True(result.ValidateOnly);
        Assert.True(result.Succeeded);
        Assert.Equal(1, result.AffectedTables);
        Assert.Equal(1, result.AppliedForeignKeys);
        Assert.Equal(0, result.CopiedRows);
        Assert.Equal(0, result.ViolationCount);
        var applied = Assert.Single(result.AppliedConstraints);
        Assert.Equal("orders", applied.TableName);
        Assert.Equal("customer_id", applied.ColumnName);
        Assert.Equal("customers", applied.ReferencedTableName);
        Assert.Equal("id", applied.ReferencedColumnName);
        Assert.StartsWith("__fk_orders_customer_id_", applied.SupportingIndexName, StringComparison.Ordinal);

        var schema = await _client.GetTableSchemaAsync("orders", Ct);
        Assert.NotNull(schema);
        Assert.Empty(schema!.ForeignKeys);

        var fkRows = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM sys.foreign_keys WHERE table_name = 'orders';", Ct);
        Assert.Null(fkRows.Error);
        Assert.NotNull(fkRows.Rows);
        Assert.Equal(0L, Convert.ToInt64(Assert.Single(fkRows.Rows)[0]));
    }

    [Fact]
    public async Task MigrateForeignKeysAsync_ValidateOnly_ReportsSampledOrphans()
    {
        await _client.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);", Ct);
        await _client.ExecuteSqlAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO customers VALUES (1, 'Ada');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (10, 1, 25);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (11, 99, 50);", Ct);

        await _client.DisposeAsync();
        await RewriteTableSchemaAsLegacyAsync(_dbPath, "orders", Ct);
        _client = CreateClient();

        var result = await _client.MigrateForeignKeysAsync(
            new ClientModels.ForeignKeyMigrationRequest
            {
                ValidateOnly = true,
                ViolationSampleLimit = 10,
                Constraints =
                [
                    new ClientModels.ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "orders",
                        ColumnName = "customer_id",
                        ReferencedTableName = "customers",
                        ReferencedColumnName = "id",
                    },
                ],
            },
            Ct);

        Assert.True(result.ValidateOnly);
        Assert.False(result.Succeeded);
        Assert.Equal(1, result.AffectedTables);
        Assert.Equal(1, result.AppliedForeignKeys);
        Assert.Equal(1, result.ViolationCount);
        var violation = Assert.Single(result.Violations);
        Assert.Equal("orders", violation.TableName);
        Assert.Equal("customer_id", violation.ColumnName);
        Assert.Equal("customers", violation.ReferencedTableName);
        Assert.Equal("id", violation.ReferencedColumnName);
        Assert.Equal("id", violation.ChildKeyColumnName);
        Assert.Equal(11L, Assert.IsType<long>(violation.ChildKeyValue));
        Assert.Equal(99L, Assert.IsType<long>(violation.ChildValue));
        Assert.Equal("MissingReferencedParent", violation.Reason);

        var schema = await _client.GetTableSchemaAsync("orders", Ct);
        Assert.NotNull(schema);
        Assert.Empty(schema!.ForeignKeys);
    }

    [Fact]
    public async Task MigrateForeignKeysAsync_Apply_PersistsMetadataBackupEnforcementAndDropConstraint()
    {
        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_fk_migration_backup_{Guid.NewGuid():N}.db");

        try
        {
            await _client.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);", Ct);
            await _client.ExecuteSqlAsync("CREATE TABLE child_audit (child_id INTEGER NOT NULL);", Ct);
            await _client.ExecuteSqlAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER);", Ct);
            await _client.ExecuteSqlAsync("CREATE INDEX idx_orders_total ON orders(total);", Ct);
            await _client.ExecuteSqlAsync(
                """
                CREATE TRIGGER trg_orders_audit AFTER INSERT ON orders BEGIN
                    INSERT INTO child_audit VALUES (NEW.id);
                END;
                """,
                Ct);
            await _client.ExecuteSqlAsync("INSERT INTO customers VALUES (1, 'Ada');", Ct);
            await _client.ExecuteSqlAsync("INSERT INTO customers VALUES (2, 'Grace');", Ct);
            await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (10, 1, 25);", Ct);

            long auditCountBefore = await QueryScalarCountAsync(_client, "SELECT COUNT(*) FROM child_audit;", Ct);
            Assert.Equal(1L, auditCountBefore);

            await _client.DisposeAsync();
            await RewriteTableSchemaAsLegacyAsync(_dbPath, "orders", Ct);
            _client = CreateClient();

            var result = await _client.MigrateForeignKeysAsync(
                new ClientModels.ForeignKeyMigrationRequest
                {
                    BackupDestinationPath = backupPath,
                    Constraints =
                    [
                        new ClientModels.ForeignKeyMigrationConstraintSpec
                        {
                            TableName = "orders",
                            ColumnName = "customer_id",
                            ReferencedTableName = "customers",
                            ReferencedColumnName = "id",
                            OnDelete = ClientModels.ForeignKeyOnDeleteAction.Cascade,
                        },
                    ],
                },
                Ct);

            Assert.False(result.ValidateOnly);
            Assert.True(result.Succeeded);
            Assert.Equal(Path.GetFullPath(backupPath), result.BackupDestinationPath);
            Assert.Equal(1, result.AffectedTables);
            Assert.Equal(1, result.AppliedForeignKeys);
            Assert.Equal(1, result.CopiedRows);
            Assert.True(File.Exists(backupPath));

            var schema = await _client.GetTableSchemaAsync("orders", Ct);
            Assert.NotNull(schema);
            var foreignKey = Assert.Single(schema!.ForeignKeys);
            Assert.Equal("customer_id", foreignKey.ColumnName);
            Assert.Equal("customers", foreignKey.ReferencedTableName);
            Assert.Equal("id", foreignKey.ReferencedColumnName);
            Assert.Equal(ClientModels.ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);

            long auditCountAfterMigration = await QueryScalarCountAsync(_client, "SELECT COUNT(*) FROM child_audit;", Ct);
            Assert.Equal(auditCountBefore, auditCountAfterMigration);

            var foreignKeyRows = await _client.ExecuteSqlAsync(
                "SELECT constraint_name, supporting_index_name, on_delete FROM sys.foreign_keys WHERE table_name = 'orders';",
                Ct);
            Assert.Null(foreignKeyRows.Error);
            Assert.NotNull(foreignKeyRows.Rows);
            var foreignKeyRow = Assert.Single(foreignKeyRows.Rows);
            string constraintName = Assert.IsType<string>(foreignKeyRow[0]);
            string supportingIndexName = Assert.IsType<string>(foreignKeyRow[1]);
            Assert.Equal("CASCADE", Assert.IsType<string>(foreignKeyRow[2]));
            Assert.StartsWith("__fk_orders_customer_id_", supportingIndexName, StringComparison.Ordinal);

            var invalidInsert = await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (20, 999, 30);", Ct);
            Assert.NotNull(invalidInsert.Error);

            var validInsert = await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (21, 2, 30);", Ct);
            Assert.Null(validInsert.Error);
            long auditCountAfterInsert = await QueryScalarCountAsync(_client, "SELECT COUNT(*) FROM child_audit;", Ct);
            Assert.Equal(2L, auditCountAfterInsert);

            var deleteParent = await _client.ExecuteSqlAsync("DELETE FROM customers WHERE id = 2;", Ct);
            Assert.Null(deleteParent.Error);
            long remainingChildren = await QueryScalarCountAsync(_client, "SELECT COUNT(*) FROM orders WHERE customer_id = 2;", Ct);
            Assert.Equal(0L, remainingChildren);

            var dropConstraint = await _client.ExecuteSqlAsync($"ALTER TABLE orders DROP CONSTRAINT {constraintName};", Ct);
            Assert.Null(dropConstraint.Error);

            var postDropSchema = await _client.GetTableSchemaAsync("orders", Ct);
            Assert.NotNull(postDropSchema);
            Assert.Empty(postDropSchema!.ForeignKeys);

            var insertAfterDrop = await _client.ExecuteSqlAsync("INSERT INTO orders VALUES (30, 999, 35);", Ct);
            Assert.Null(insertAfterDrop.Error);

            await _client.DisposeAsync();
            _client = CreateClient();

            var reopenedSchema = await _client.GetTableSchemaAsync("orders", Ct);
            Assert.NotNull(reopenedSchema);
            Assert.Empty(reopenedSchema!.ForeignKeys);
        }
        finally
        {
            if (File.Exists(backupPath))
                File.Delete(backupPath);
            if (File.Exists(backupPath + ".wal"))
                File.Delete(backupPath + ".wal");
        }
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
    public async Task ReindexAsync_RebuildsCollatedSqlIndexes()
    {
        const string icuCollation = "ICU:en-US";

        await _client.ExecuteSqlAsync("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (1, 'résumé');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (2, 'Resume');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (3, 'résumé');", Ct);
        await _client.ExecuteSqlAsync("INSERT INTO people VALUES (4, 'zebra');", Ct);
        await _client.ExecuteSqlAsync("CREATE INDEX idx_people_name_ai ON people (name COLLATE NOCASE_AI);", Ct);
        await _client.ExecuteSqlAsync($"CREATE INDEX idx_people_name_icu ON people (name COLLATE {icuCollation});", Ct);

        var beforeAi = await _client.ExecuteSqlAsync(
            "SELECT id FROM people WHERE name COLLATE NOCASE_AI = 'RESUME' ORDER BY id;",
            Ct);
        var beforeIcu = await _client.ExecuteSqlAsync(
            $"SELECT id FROM people WHERE name COLLATE {icuCollation} = 'résumé' ORDER BY id;",
            Ct);

        var result = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Table,
            Name = "people",
        }, Ct);

        var afterAi = await _client.ExecuteSqlAsync(
            "SELECT id FROM people WHERE name COLLATE NOCASE_AI = 'RESUME' ORDER BY id;",
            Ct);
        var afterIcu = await _client.ExecuteSqlAsync(
            $"SELECT id FROM people WHERE name COLLATE {icuCollation} = 'résumé' ORDER BY id;",
            Ct);
        var indexReport = await _client.CheckIndexesAsync(ct: Ct);

        Assert.Equal(2, result.RebuiltIndexCount);
        Assert.DoesNotContain(indexReport.Issues, issue => issue.Severity == CSharpDB.Storage.Diagnostics.InspectSeverity.Error);
        Assert.Null(beforeAi.Error);
        Assert.Null(beforeIcu.Error);
        Assert.Null(afterAi.Error);
        Assert.Null(afterIcu.Error);
        Assert.NotNull(beforeAi.Rows);
        Assert.NotNull(beforeIcu.Rows);
        Assert.NotNull(afterAi.Rows);
        Assert.NotNull(afterIcu.Rows);
        Assert.Equal([1L, 2L, 3L], beforeAi.Rows.Select(static row => Convert.ToInt64(row[0], CultureInfo.InvariantCulture)).ToArray());
        Assert.Equal([1L, 3L], beforeIcu.Rows.Select(static row => Convert.ToInt64(row[0], CultureInfo.InvariantCulture)).ToArray());
        Assert.Equal([1L, 2L, 3L], afterAi.Rows.Select(static row => Convert.ToInt64(row[0], CultureInfo.InvariantCulture)).ToArray());
        Assert.Equal([1L, 3L], afterIcu.Rows.Select(static row => Convert.ToInt64(row[0], CultureInfo.InvariantCulture)).ToArray());
    }

    [Fact]
    public async Task ReindexAsync_RebuildsCollatedCollectionIndexes()
    {
        await _client.DisposeAsync();

        const string icuCollation = "ICU:en-US";
        string collectionIndexName;

        await using (var db = await Database.OpenAsync(_dbPath, Ct))
        {
            var users = await db.GetCollectionAsync<UserDoc>("users_icu_collation", Ct);
            await users.PutAsync("u:1", new UserDoc("résumé", 30), Ct);
            await users.PutAsync("u:2", new UserDoc("résumé", 31), Ct);
            await users.PutAsync("u:3", new UserDoc("Resume", 32), Ct);
            await users.EnsureIndexAsync(x => x.Name, icuCollation, Ct);
            collectionIndexName = db.GetIndexes()
                .Single(index => string.Equals(index.TableName, "_col_users_icu_collation", StringComparison.OrdinalIgnoreCase))
                .IndexName;
        }

        _client = CreateClient();

        var result = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Index,
            Name = collectionIndexName,
        }, Ct);

        await using var reopened = await Database.OpenAsync(_dbPath, Ct);
        var reopenedUsers = await reopened.GetCollectionAsync<UserDoc>("users_icu_collation", Ct);
        var matches = await CollectAsync(reopenedUsers.FindByIndexAsync(x => x.Name, "résumé", Ct), Ct);

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

    [Fact]
    public async Task ReindexAsync_AllowCorruptIndexRecovery_RebuildsBrokenIndexWithoutReclaim()
    {
        const string indexName = "idx_customers_customer_id";

        await _client.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, name TEXT NOT NULL);", Ct);
        for (int i = 1; i <= 5000; i++)
            await _client.ExecuteSqlAsync($"INSERT INTO customers VALUES ({i}, {i}, 'Customer {i}');", Ct);
        await _client.CreateIndexAsync(indexName, "customers", "customer_id", isUnique: false, Ct);
        await _client.ExecuteSqlAsync("CREATE TABLE junk (id INTEGER PRIMARY KEY, payload TEXT);", Ct);
        for (int i = 1; i <= 200; i++)
            await _client.ExecuteSqlAsync($"INSERT INTO junk VALUES ({i}, '{new string('x', 64)}');", Ct);
        await _client.DropTableAsync("junk", Ct);

        await _client.DisposeAsync();
        await CorruptIndexChildReferenceAsync(_dbPath, indexName, Ct);

        _client = CreateClient();
        var brokenReport = await _client.CheckIndexesAsync(indexName: indexName, ct: Ct);
        Assert.Contains(brokenReport.Issues, issue => issue.Severity == CSharpDB.Storage.Diagnostics.InspectSeverity.Error);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
            _client.ReindexAsync(new ClientModels.ReindexRequest
            {
                Scope = ClientModels.ReindexScope.Index,
                Name = indexName,
            }, Ct));
        Assert.Contains("Cannot reclaim B+tree page", ex.Message, StringComparison.OrdinalIgnoreCase);

        var repaired = await _client.ReindexAsync(new ClientModels.ReindexRequest
        {
            Scope = ClientModels.ReindexScope.Index,
            Name = indexName,
            AllowCorruptIndexRecovery = true,
        }, Ct);

        var indexReport = await _client.CheckIndexesAsync(indexName: indexName, ct: Ct);
        var query = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM customers WHERE customer_id = 1405;", Ct);

        Assert.Equal(1, repaired.RebuiltIndexCount);
        Assert.Equal(1, repaired.RecoveredCorruptIndexCount);
        Assert.DoesNotContain(indexReport.Issues, issue => issue.Severity == CSharpDB.Storage.Diagnostics.InspectSeverity.Error);
        Assert.Null(query.Error);
        Assert.True(query.IsQuery);
        Assert.NotNull(query.Rows);
        Assert.Equal(1L, Convert.ToInt64(Assert.Single(query.Rows)[0]));
    }

    private ICSharpDbClient CreateClient()
        => CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _dbPath });

    private static async Task<long> QueryScalarCountAsync(ICSharpDbClient client, string sql, CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(sql, ct);
        Assert.Null(result.Error);
        Assert.NotNull(result.Rows);
        return Convert.ToInt64(Assert.Single(result.Rows)[0]);
    }

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

    private static async ValueTask CorruptIndexChildReferenceAsync(string dbPath, string indexName, CancellationToken ct)
    {
        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(dbPath, new StorageEngineOptions(), ct);
        try
        {
            var store = context.Catalog.GetIndexStore(indexName);
            uint rootPageId = store.RootPageId;

            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                byte[] rootPage = await context.Pager.GetPageAsync(rootPageId, ct);
                int baseOffset = rootPageId == 0 ? PageConstants.FileHeaderSize : 0;
                Assert.Equal(PageConstants.PageTypeInterior, rootPage[baseOffset]);
                Assert.NotEqual(PageConstants.NullPageId, context.Pager.FreelistHead);

                BinaryPrimitives.WriteUInt32LittleEndian(
                    rootPage.AsSpan(baseOffset + PageConstants.RightChildOffset, sizeof(uint)),
                    context.Pager.FreelistHead);
                await context.Pager.MarkDirtyAsync(rootPageId, ct);

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

    private sealed record UserDoc(string Name, int Age);
}
