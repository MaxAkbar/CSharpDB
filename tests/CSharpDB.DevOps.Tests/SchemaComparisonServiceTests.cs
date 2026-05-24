using CSharpDB.Client.Models;
using CSharpDB.Client;
using CSharpDB.DevOps;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using PrimitiveColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveTableSchema = CSharpDB.Primitives.TableSchema;

namespace CSharpDB.DevOps.Tests;

public sealed class SchemaComparisonServiceTests
{
    [Fact]
    public void Compare_ReportsTableColumnIndexAndViewDifferences()
    {
        var source = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables =
            [
                Table(
                    "customers",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                    Column("name", ClientDbType.Text, nullable: false, collation: "NOCASE"),
                    Column("email", ClientDbType.Text)),
            ],
            Indexes =
            [
                new ClientIndexSchema
                {
                    IndexName = "idx_customers_name",
                    TableName = "customers",
                    Columns = ["name"],
                    ColumnCollations = ["NOCASE"],
                },
            ],
            Views =
            [
                new ViewDefinition { Name = "customer_names", Sql = "SELECT name FROM customers" },
            ],
        };

        var target = new SchemaSnapshot
        {
            Target = Target("target"),
            Tables =
            [
                Table(
                    "customers",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                    Column("name", ClientDbType.Text),
                    Column("legacy", ClientDbType.Text)),
                Table("legacy_table", Column("id", ClientDbType.Integer, nullable: false)),
            ],
            Views =
            [
                new ViewDefinition { Name = "customer_names", Sql = "SELECT id FROM customers" },
            ],
        };

        SchemaDiffReport report = new SchemaComparisonService().Compare(source, target);

        Assert.Equal(6, report.Summary.TotalChanges);
        Assert.Equal(3, report.Summary.DestructiveChanges);
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.Column && c.ChangeKind == SchemaChangeKind.Changed && c.Name == "customers.name");
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.Column && c.ChangeKind == SchemaChangeKind.Added && c.Name == "customers.email");
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.Column && c.ChangeKind == SchemaChangeKind.Removed && c.Name == "customers.legacy");
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.Table && c.ChangeKind == SchemaChangeKind.Removed && c.Name == "legacy_table");
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.Index && c.ChangeKind == SchemaChangeKind.Added && c.Name == "idx_customers_name");
        Assert.Contains(report.Changes, c => c.ObjectKind == SchemaObjectKind.View && c.ChangeKind == SchemaChangeKind.Changed && c.Name == "customer_names");
    }

    [Fact]
    public async Task TableArchiveTarget_LoadsSchemaForCompare()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_devops_archive_{Guid.NewGuid():N}.csdbtable");

        try
        {
            var schema = new PrimitiveTableSchema
            {
                TableName = "archive_customers",
                Columns =
                [
                    new PrimitiveColumnDefinition
                    {
                        Name = "id",
                        Type = PrimitiveDbType.Integer,
                        Nullable = false,
                        IsPrimaryKey = true,
                    },
                    new PrimitiveColumnDefinition
                    {
                        Name = "name",
                        Type = PrimitiveDbType.Text,
                        Nullable = false,
                        Collation = "NOCASE",
                    },
                ],
            };

            await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows([], ct), ct);

            var archive = new TableArchiveSchemaCompareTarget(path);
            SchemaSnapshot snapshot = await archive.LoadSchemaAsync(ct);

            ClientTableSchema table = Assert.Single(snapshot.Tables);
            Assert.Equal("archive_customers", table.TableName);
            Assert.Equal("NOCASE", table.Columns[1].Collation);

            var empty = new StaticSchemaCompareTarget("empty", []);
            SchemaDiffReport report = await new SchemaComparisonService().CompareAsync(archive, empty, ct);

            SchemaDiffChange change = Assert.Single(report.Changes);
            Assert.Equal(SchemaObjectKind.Table, change.ObjectKind);
            Assert.Equal(SchemaChangeKind.Added, change.ChangeKind);
            Assert.Equal("archive_customers", change.Name);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public void RenderDeployScript_EmitsCreateStatementsAndCommentsDestructiveChanges()
    {
        var report = new SchemaComparisonService().Compare(
            new SchemaSnapshot
            {
                Target = Target("source"),
                Tables = [Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true))],
            },
            new SchemaSnapshot
            {
                Target = Target("target"),
                Tables = [Table("old_customers", Column("id", ClientDbType.Integer, nullable: false))],
            });

        string script = SchemaScriptRenderer.RenderDeployScript(report);

        Assert.Contains("CREATE TABLE customers", script, StringComparison.Ordinal);
        Assert.Contains("-- DROP TABLE old_customers;", script, StringComparison.Ordinal);
        Assert.Contains("-- WARNING:", script, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderDeployScript_EmitsAlterTableForAddedColumns()
    {
        var report = new SchemaComparisonService().Compare(
            new SchemaSnapshot
            {
                Target = Target("source"),
                Tables =
                [
                    Table(
                        "customers",
                        Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                        Column("email", ClientDbType.Text)),
                ],
            },
            new SchemaSnapshot
            {
                Target = Target("target"),
                Tables = [Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true))],
            });

        string script = SchemaScriptRenderer.RenderDeployScript(report);

        Assert.Contains("ALTER TABLE customers ADD COLUMN email TEXT;", script, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderSnapshotScript_EmitsUserObjectsForWholeDatabase()
    {
        var snapshot = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables =
            [
                Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true)),
                Table("orders", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true)),
            ],
            Indexes =
            [
                new ClientIndexSchema
                {
                    IndexName = "idx_orders_id",
                    TableName = "orders",
                    Columns = ["id"],
                },
            ],
            Views = [new ViewDefinition { Name = "customer_ids", Sql = "SELECT id FROM customers" }],
        };

        string script = SchemaScriptRenderer.RenderSnapshotScript(
            snapshot,
            new SchemaScriptOptions { Scope = SchemaScriptScope.WholeDatabase });

        Assert.Contains("CREATE TABLE customers", script, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE orders", script, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX idx_orders_id", script, StringComparison.Ordinal);
        Assert.Contains("CREATE VIEW customer_ids", script, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderSnapshotScript_CanFilterToOneTableAndItsIndexes()
    {
        var snapshot = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables =
            [
                Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true)),
                Table("orders", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true)),
            ],
            Indexes =
            [
                new ClientIndexSchema
                {
                    IndexName = "idx_orders_id",
                    TableName = "orders",
                    Columns = ["id"],
                },
            ],
        };

        string script = SchemaScriptRenderer.RenderSnapshotScript(
            snapshot,
            new SchemaScriptOptions { Scope = SchemaScriptScope.UserObjects, ObjectName = "orders" });

        Assert.DoesNotContain("CREATE TABLE customers", script, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE orders", script, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX idx_orders_id", script, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderSnapshotScript_HonorsTableRelatedObjectOptions()
    {
        var snapshot = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables = [Table("orders", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true))],
            Indexes =
            [
                new ClientIndexSchema
                {
                    IndexName = "idx_orders_id",
                    TableName = "orders",
                    Columns = ["id"],
                },
            ],
            Views = [new ViewDefinition { Name = "order_ids", Sql = "SELECT id FROM orders" }],
        };

        string script = SchemaScriptRenderer.RenderSnapshotScript(
            snapshot,
            new SchemaScriptOptions
            {
                ObjectKind = SchemaObjectKind.Table,
                ObjectName = "orders",
                IncludeIndexes = false,
                IncludeRelatedViews = true,
            });

        Assert.Contains("CREATE TABLE orders", script, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX idx_orders_id", script, StringComparison.Ordinal);
        Assert.Contains("CREATE VIEW order_ids", script, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ClientTarget_LoadSchema_DoesNotCreateProcedureCatalog()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_devops_readonly_{Guid.NewGuid():N}.db");

        try
        {
            await using (var db = await Database.OpenAsync(path, ct))
            {
                await db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY);", ct);
            }

            await using (var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path }))
            {
                var target = new ClientSchemaCompareTarget(client);
                SchemaSnapshot snapshot = await target.LoadSchemaAsync(ct);

                Assert.Single(snapshot.Tables);
                Assert.Empty(snapshot.Procedures);
            }

            await using (var reopened = await Database.OpenAsync(path, ct))
            {
                Assert.DoesNotContain("__procedures", reopened.GetTableNames(), StringComparer.OrdinalIgnoreCase);
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task DataCompare_ReportsKeyedDifferencesAndRendersSyncScript()
    {
        var schema = Table(
            "customers",
            Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
            Column("name", ClientDbType.Text, nullable: false),
            Column("balance", ClientDbType.Real));

        var source = new StaticDataCompareTarget(
            "source",
            schema,
            [
                Row(("id", 1L), ("name", "Ada"), ("balance", 10.0)),
                Row(("id", 2L), ("name", "Grace"), ("balance", 20.0)),
                Row(("id", 3L), ("name", "Linus"), ("balance", 30.0)),
            ]);

        var target = new StaticDataCompareTarget(
            "target",
            schema,
            [
                Row(("id", 1L), ("name", "Ada"), ("balance", 10.0)),
                Row(("id", 2L), ("name", "Grace Hopper"), ("balance", 20.0)),
                Row(("id", 4L), ("name", "Target Only"), ("balance", 40.0)),
            ]);

        var service = new DataComparisonService();
        DataDiffReport report = await service.CompareAsync(
            source,
            target,
            new DataCompareOptions { TableName = "customers" },
            TestContext.Current.CancellationToken);

        Assert.Equal(["id"], report.KeyColumns);
        Assert.Equal(1, report.Summary.SourceOnlyRows);
        Assert.Equal(1, report.Summary.TargetOnlyRows);
        Assert.Equal(1, report.Summary.ChangedRows);
        Assert.Equal(3, report.Rows.Count);

        string script = service.RenderSyncScript(report);
        Assert.Contains("INSERT INTO customers", script, StringComparison.Ordinal);
        Assert.Contains("UPDATE customers SET name = 'Grace'", script, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM customers WHERE id = 4;", script, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DriftReport_IncludesSchemaAndOptionalDataDifferences()
    {
        var baselineSchema = new StaticSchemaCompareTarget(
            "baseline",
            [Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true), Column("name", ClientDbType.Text))]);
        var currentSchema = new StaticSchemaCompareTarget(
            "current",
            [Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true), Column("name", ClientDbType.Text), Column("email", ClientDbType.Text))]);

        var dataSchema = Table("customers", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true), Column("name", ClientDbType.Text));
        var baselineData = new StaticDataCompareTarget("baseline", dataSchema, [Row(("id", 1L), ("name", "before"))]);
        var currentData = new StaticDataCompareTarget("current", dataSchema, [Row(("id", 1L), ("name", "after"))]);

        DriftReport report = await new DriftReportService().CreateAsync(
            baselineSchema,
            currentSchema,
            baselineData,
            currentData,
            new DriftReportOptions
            {
                DataTables = [new DataCompareOptions { TableName = "customers" }],
            },
            TestContext.Current.CancellationToken);

        Assert.True(report.Summary.HasDrift);
        Assert.Equal(1, report.Summary.SchemaChanges);
        Assert.Equal(1, report.Summary.DataTablesCompared);
        Assert.Equal(1, report.Summary.DataRowsDifferent);
    }

    private static SchemaTargetDescriptor Target(string name)
        => new() { Kind = DevOpsTargetKind.Database, DisplayName = name };

    private static ClientTableSchema Table(string name, params ClientColumnDefinition[] columns)
        => new() { TableName = name, Columns = columns };

    private static ClientColumnDefinition Column(
        string name,
        ClientDbType type,
        bool nullable = true,
        bool isPrimaryKey = false,
        bool isIdentity = false,
        string? collation = null)
        => new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
            IsPrimaryKey = isPrimaryKey,
            IsIdentity = isIdentity,
            Collation = collation,
        };

    private sealed class StaticSchemaCompareTarget : ISchemaCompareTarget
    {
        private readonly SchemaSnapshot _snapshot;

        public StaticSchemaCompareTarget(string name, IReadOnlyList<ClientTableSchema> tables)
        {
            Descriptor = Target(name);
            _snapshot = new SchemaSnapshot { Target = Descriptor, Tables = tables };
        }

        public SchemaTargetDescriptor Descriptor { get; }

        public Task<SchemaSnapshot> LoadSchemaAsync(CancellationToken ct = default)
            => Task.FromResult(_snapshot);
    }

    private sealed class StaticDataCompareTarget : IDataCompareTarget
    {
        private readonly ClientTableSchema _schema;
        private readonly IReadOnlyList<IReadOnlyDictionary<string, object?>> _rows;

        public StaticDataCompareTarget(
            string name,
            ClientTableSchema schema,
            IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
        {
            Descriptor = Target(name);
            _schema = schema;
            _rows = rows;
        }

        public SchemaTargetDescriptor Descriptor { get; }

        public Task<ClientTableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
            => Task.FromResult(
                _schema.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                    ? _schema
                    : null);

        public async IAsyncEnumerable<IReadOnlyDictionary<string, object?>> ReadRowsAsync(
            ClientTableSchema schema,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (IReadOnlyDictionary<string, object?> row in _rows)
            {
                ct.ThrowIfCancellationRequested();
                yield return row;
                await Task.Yield();
            }
        }
    }

    private static IReadOnlyDictionary<string, object?> Row(params (string Column, object? Value)[] values)
        => values.ToDictionary(item => item.Column, item => item.Value, StringComparer.OrdinalIgnoreCase);
}
