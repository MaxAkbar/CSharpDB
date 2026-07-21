using CSharpDB.Client.Models;
using CSharpDB.Client;
using CSharpDB.DevOps;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientCheckConstraintDefinition = CSharpDB.Client.Models.CheckConstraintDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientKeyConstraintDefinition = CSharpDB.Client.Models.KeyConstraintDefinition;
using ClientKeyConstraintKind = CSharpDB.Client.Models.KeyConstraintKind;
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
                        DefaultSql = "'anonymous'",
                    },
                    new PrimitiveColumnDefinition
                    {
                        Name = "version",
                        Type = PrimitiveDbType.Blob,
                        Nullable = false,
                        IsRowVersion = true,
                    },
                ],
                CheckConstraints =
                [
                    new CSharpDB.Primitives.CheckConstraintDefinition
                    {
                        ConstraintName = "ck_archive_customers_name",
                        ExpressionSql = "name <> ''",
                        ColumnName = "name",
                    },
                ],
                ForeignKeys =
                [
                    new CSharpDB.Primitives.ForeignKeyDefinition
                    {
                        ConstraintName = "fk_archive_customers_tenant",
                        ColumnName = "id",
                        ReferencedTableName = "archive_tenants",
                        ReferencedColumnName = "tenant_id",
                        ColumnNames = ["id", "name"],
                        ReferencedColumnNames = ["tenant_id", "customer_name"],
                        OnDelete = CSharpDB.Primitives.ForeignKeyOnDeleteAction.Cascade,
                        SupportingIndexName = "__fk_archive_customers_tenant",
                    },
                ],
                KeyConstraints =
                [
                    new CSharpDB.Primitives.KeyConstraintDefinition
                    {
                        ConstraintName = "pk_archive_customers",
                        Kind = CSharpDB.Primitives.KeyConstraintKind.PrimaryKey,
                        Columns = ["id"],
                    },
                    new CSharpDB.Primitives.KeyConstraintDefinition
                    {
                        ConstraintName = "uq_archive_customers_name",
                        Kind = CSharpDB.Primitives.KeyConstraintKind.Unique,
                        Columns = ["name"],
                        BackingIndexName = "__constraint_archive_customers_name",
                    },
                ],
            };

            await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows([], ct), ct);

            var archive = new TableArchiveSchemaCompareTarget(path);
            SchemaSnapshot snapshot = await archive.LoadSchemaAsync(ct);

            ClientTableSchema table = Assert.Single(snapshot.Tables);
            Assert.Equal("archive_customers", table.TableName);
            Assert.Equal("NOCASE", table.Columns[1].Collation);
            Assert.Equal("'anonymous'", table.Columns[1].DefaultSql);
            Assert.True(table.Columns[2].IsRowVersion);
            Assert.Equal("ck_archive_customers_name", Assert.Single(table.CheckConstraints).ConstraintName);
            ClientForeignKeyDefinition tableForeignKey = Assert.Single(table.ForeignKeys);
            Assert.Equal(["id", "name"], tableForeignKey.ColumnNames);
            Assert.Equal(["tenant_id", "customer_name"], tableForeignKey.ReferencedColumnNames);
            Assert.Collection(
                table.KeyConstraints,
                primary => Assert.Equal(ClientKeyConstraintKind.PrimaryKey, primary.Kind),
                unique => Assert.Equal(ClientKeyConstraintKind.Unique, unique.Kind));

            var dataTarget = new TableArchiveDataCompareTarget(path);
            ClientTableSchema? dataSchema = await dataTarget.GetTableSchemaAsync("archive_customers", ct);
            Assert.NotNull(dataSchema);
            Assert.Equal("'anonymous'", dataSchema!.Columns[1].DefaultSql);
            Assert.True(dataSchema.Columns[2].IsRowVersion);
            Assert.Single(dataSchema.CheckConstraints);
            Assert.Equal(2, dataSchema.KeyConstraints.Count);
            ClientForeignKeyDefinition dataForeignKey = Assert.Single(dataSchema.ForeignKeys);
            Assert.Equal(["id", "name"], dataForeignKey.ColumnNames);
            Assert.Equal(["tenant_id", "customer_name"], dataForeignKey.ReferencedColumnNames);

            var empty = new StaticSchemaCompareTarget("empty", []);
            SchemaDiffReport report = await new SchemaComparisonService().CompareAsync(archive, empty, ct);

            SchemaDiffChange change = Assert.Single(report.Changes);
            Assert.Equal(SchemaObjectKind.Table, change.ObjectKind);
            Assert.Equal(SchemaChangeKind.Added, change.ChangeKind);
            Assert.Equal("archive_customers", change.Name);
            Assert.Contains("DEFAULT 'anonymous'", change.SourceDefinition, StringComparison.Ordinal);
            Assert.Contains("version BLOB ROWVERSION NOT NULL", change.SourceDefinition, StringComparison.Ordinal);
            Assert.Contains("CONSTRAINT ck_archive_customers_name CHECK (name <> '')", change.SourceDefinition, StringComparison.Ordinal);
            Assert.Contains("CONSTRAINT uq_archive_customers_name UNIQUE (name)", change.SourceDefinition, StringComparison.Ordinal);
            Assert.Contains(
                "CONSTRAINT fk_archive_customers_tenant FOREIGN KEY (id, name) REFERENCES archive_tenants (tenant_id, customer_name) ON DELETE CASCADE",
                change.SourceDefinition,
                StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public void CompareAndRender_RowVersionChangesRequireTableRebuild()
    {
        var source = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables =
            [
                Table(
                    "items",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                    Column("version", ClientDbType.Blob, nullable: false, isRowVersion: true)),
            ],
        };
        var target = new SchemaSnapshot
        {
            Target = Target("target"),
            Tables =
            [
                Table(
                    "items",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                    Column("version", ClientDbType.Blob, nullable: false)),
            ],
        };

        SchemaDiffReport report = new SchemaComparisonService().Compare(source, target);

        SchemaDiffChange change = Assert.Single(report.Changes);
        Assert.True(change.IsDestructive);
        Assert.Equal("False -> True", change.Details["rowVersion"]);
        Assert.Contains("version BLOB ROWVERSION NOT NULL", change.SourceDefinition, StringComparison.Ordinal);
        Assert.Contains(
            "version BLOB ROWVERSION NOT NULL",
            SchemaScriptRenderer.RenderCreateTable(source.Tables[0]),
            StringComparison.Ordinal);

        var addedReport = new SchemaComparisonService().Compare(
            new SchemaSnapshot
            {
                Target = Target("source"),
                Tables =
                [
                    Table(
                        "items",
                        Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                        Column("version", ClientDbType.Blob, nullable: false, isRowVersion: true)),
                ],
            },
            new SchemaSnapshot
            {
                Target = Target("target"),
                Tables = [Table("items", Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true))],
            });
        SchemaDiffChange added = Assert.Single(addedReport.Changes);
        Assert.True(added.IsDestructive);

        string deployScript = SchemaScriptRenderer.RenderDeployScript(addedReport);
        Assert.Contains("rebuild the table", deployScript, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ALTER TABLE items ADD COLUMN version", deployScript, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RenderDeployScript_DoesNotMistakeDefaultTextForRowVersionMetadata()
    {
        var source = new SchemaSnapshot
        {
            Target = Target("source"),
            Tables =
            [
                Table(
                    "items",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                    Column(
                        "note",
                        ClientDbType.Text,
                        defaultSql: "' ROWVERSION'")),
            ],
        };
        var target = new SchemaSnapshot
        {
            Target = Target("target"),
            Tables =
            [
                Table(
                    "items",
                    Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true)),
            ],
        };

        SchemaDiffReport report =
            new SchemaComparisonService().Compare(source, target);
        SchemaDiffChange change = Assert.Single(report.Changes);
        Assert.Equal("False", change.Details["rowVersion"]);

        string deployScript =
            SchemaScriptRenderer.RenderDeployScript(report);
        Assert.Contains(
            "ALTER TABLE items ADD COLUMN note TEXT DEFAULT ' ROWVERSION';",
            deployScript,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "Standalone ROWVERSION",
            deployScript,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Compare_ReportsDefaultCheckAndLogicalKeyDifferences()
    {
        var sourceTable = new ClientTableSchema
        {
            TableName = "orders",
            Columns =
            [
                Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true, isIdentity: true),
                Column("tenant", ClientDbType.Text, nullable: false),
                Column("code", ClientDbType.Text, defaultSql: "'new'"),
                Column("score", ClientDbType.Integer),
            ],
            KeyConstraints =
            [
                new ClientKeyConstraintDefinition
                {
                    ConstraintName = "pk_orders",
                    Kind = ClientKeyConstraintKind.PrimaryKey,
                    Columns = ["id"],
                },
                new ClientKeyConstraintDefinition
                {
                    ConstraintName = "uq_orders_tenant_code",
                    Kind = ClientKeyConstraintKind.Unique,
                    Columns = ["tenant", "code"],
                },
            ],
            CheckConstraints =
            [
                new ClientCheckConstraintDefinition
                {
                    ConstraintName = "ck_orders_score",
                    ExpressionSql = "score >= 0",
                    ColumnName = "score",
                },
            ],
        };
        var targetTable = new ClientTableSchema
        {
            TableName = "orders",
            Columns =
            [
                Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true, isIdentity: true),
                Column("tenant", ClientDbType.Text, nullable: false),
                Column("code", ClientDbType.Text),
                Column("score", ClientDbType.Integer),
            ],
        };

        SchemaDiffReport report = new SchemaComparisonService().Compare(
            new SchemaSnapshot { Target = Target("source"), Tables = [sourceTable] },
            new SchemaSnapshot { Target = Target("target"), Tables = [targetTable] });

        SchemaDiffChange defaultChange = Assert.Single(
            report.Changes,
            change => change.ObjectKind == SchemaObjectKind.Column && change.Name == "orders.code");
        Assert.Equal("'new'", defaultChange.Details["defaultSql"].Split(" -> ", StringSplitOptions.None)[1]);

        SchemaDiffChange constraintChange = Assert.Single(
            report.Changes,
            change => change.ObjectKind == SchemaObjectKind.Table && change.Name == "orders");
        Assert.Contains("id INTEGER NOT NULL", constraintChange.SourceDefinition, StringComparison.Ordinal);
        Assert.DoesNotContain("id INTEGER IDENTITY", constraintChange.SourceDefinition, StringComparison.Ordinal);
        Assert.Contains("code TEXT DEFAULT 'new'", constraintChange.SourceDefinition, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT pk_orders PRIMARY KEY (id)", constraintChange.SourceDefinition, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT uq_orders_tenant_code UNIQUE (tenant, code)", constraintChange.SourceDefinition, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT ck_orders_score CHECK (score >= 0)", constraintChange.SourceDefinition, StringComparison.Ordinal);
    }

    [Fact]
    public void CompareAndRender_PreserveOrderedCompositeForeignKeyColumns()
    {
        var source = new ClientTableSchema
        {
            TableName = "children",
            Columns =
            [
                Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                Column("tenant_id", ClientDbType.Integer),
                Column("parent_code", ClientDbType.Text),
            ],
            ForeignKeys =
            [
                new ClientForeignKeyDefinition
                {
                    ConstraintName = "fk_children_parent",
                    ColumnName = "tenant_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "tenant_id",
                    ColumnNames = ["tenant_id", "parent_code"],
                    ReferencedColumnNames = ["tenant_id", "code"],
                    OnDelete = ClientForeignKeyOnDeleteAction.Cascade,
                    SupportingIndexName = "__fk_children_parent",
                },
            ],
        };
        var target = new ClientTableSchema
        {
            TableName = "children",
            Columns = source.Columns,
            ForeignKeys =
            [
                new ClientForeignKeyDefinition
                {
                    ConstraintName = "fk_children_parent",
                    ColumnName = "tenant_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "tenant_id",
                    ColumnNames = ["tenant_id", "legacy_code"],
                    ReferencedColumnNames = ["tenant_id", "legacy_code"],
                    OnDelete = ClientForeignKeyOnDeleteAction.Cascade,
                    SupportingIndexName = "__fk_children_parent",
                },
            ],
        };

        SchemaDiffReport report = new SchemaComparisonService().Compare(
            new SchemaSnapshot { Target = Target("source"), Tables = [source] },
            new SchemaSnapshot { Target = Target("target"), Tables = [target] });

        SchemaDiffChange change = Assert.Single(report.Changes);
        Assert.Equal(SchemaObjectKind.ForeignKey, change.ObjectKind);
        Assert.Equal(SchemaChangeKind.Changed, change.ChangeKind);
        Assert.Contains(
            "(tenant_id, parent_code) -> parents.(tenant_id, code)",
            change.SourceDefinition,
            StringComparison.Ordinal);
        Assert.Contains(
            "FOREIGN KEY (tenant_id, parent_code) REFERENCES parents (tenant_id, code) ON DELETE CASCADE",
            SchemaScriptRenderer.RenderCreateTable(source),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task RenderCreateTable_RoundTripsDefaultsChecksLogicalKeysAndIdentity()
    {
        var ct = TestContext.Current.CancellationToken;
        string sourcePath = Path.Combine(Path.GetTempPath(), $"csharpdb_devops_script_source_{Guid.NewGuid():N}.db");
        string targetPath = Path.Combine(Path.GetTempPath(), $"csharpdb_devops_script_target_{Guid.NewGuid():N}.db");

        try
        {
            await using var source = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = sourcePath });
            SqlExecutionResult sourceCreate = await source.ExecuteSqlAsync(
                """
                CREATE TABLE scripted_orders (
                    id INTEGER PRIMARY KEY IDENTITY,
                    tenant TEXT NOT NULL,
                    code TEXT DEFAULT 'new',
                    score INTEGER CONSTRAINT ck_scripted_orders_score CHECK (score >= 0),
                    CONSTRAINT uq_scripted_orders_tenant_code UNIQUE (tenant, code)
                );
                """,
                ct);
            Assert.Null(sourceCreate.Error);

            ClientTableSchema sourceSchema = Assert.IsType<ClientTableSchema>(
                await source.GetTableSchemaAsync("scripted_orders", ct));
            string script = SchemaScriptRenderer.RenderCreateTable(sourceSchema);

            await using var target = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = targetPath });
            SqlExecutionResult targetCreate = await target.ExecuteSqlAsync(script, ct);
            Assert.Null(targetCreate.Error);

            ClientTableSchema targetSchema = Assert.IsType<ClientTableSchema>(
                await target.GetTableSchemaAsync("scripted_orders", ct));
            Assert.True(Assert.Single(targetSchema.Columns, column => column.Name == "id").IsIdentity);
            Assert.Equal("'new'", Assert.Single(targetSchema.Columns, column => column.Name == "code").DefaultSql);
            Assert.Equal("ck_scripted_orders_score", Assert.Single(targetSchema.CheckConstraints).ConstraintName);
            Assert.Collection(
                targetSchema.KeyConstraints,
                primary => Assert.Equal(ClientKeyConstraintKind.PrimaryKey, primary.Kind),
                unique =>
                {
                    Assert.Equal(ClientKeyConstraintKind.Unique, unique.Kind);
                    Assert.Equal(["tenant", "code"], unique.Columns);
                });
        }
        finally
        {
            foreach (string path in new[] { sourcePath, sourcePath + ".wal", targetPath, targetPath + ".wal" })
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task RenderCreateTable_LogicalIntegerPrimaryKey_RoundTripsWithoutDuplicatePrimaryKey()
    {
        var schema = new ClientTableSchema
        {
            TableName = "orders",
            Columns =
            [
                Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true, isIdentity: true),
                Column("tenant", ClientDbType.Text, nullable: false),
            ],
            KeyConstraints =
            [
                new ClientKeyConstraintDefinition
                {
                    ConstraintName = "pk_orders",
                    Kind = ClientKeyConstraintKind.PrimaryKey,
                    Columns = ["id"],
                },
                new ClientKeyConstraintDefinition
                {
                    ConstraintName = "uq_orders_tenant",
                    Kind = ClientKeyConstraintKind.Unique,
                    Columns = ["tenant"],
                },
            ],
        };

        string sql = SchemaScriptRenderer.RenderCreateTable(schema);
        Assert.DoesNotContain("IDENTITY", sql, StringComparison.Ordinal);

        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_devops_keys_{Guid.NewGuid():N}.db");
        try
        {
            await using var db = await Database.OpenAsync(path, TestContext.Current.CancellationToken);
            await db.ExecuteAsync(sql, TestContext.Current.CancellationToken);

            PrimitiveTableSchema restored = db.GetTableSchema("orders")!;
            Assert.True(restored.Columns[0].IsIdentity);
            Assert.Collection(
                restored.KeyConstraints,
                primary => Assert.Equal(CSharpDB.Primitives.KeyConstraintKind.PrimaryKey, primary.Kind),
                unique => Assert.Equal(CSharpDB.Primitives.KeyConstraintKind.Unique, unique.Kind));
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
    public async Task DataCompare_ExcludesRowVersionValuesAndAssignments()
    {
        var schema = Table(
            "items",
            Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
            Column("name", ClientDbType.Text, nullable: false),
            Column("version", ClientDbType.Blob, nullable: false, isRowVersion: true));
        var source = new StaticDataCompareTarget(
            "source",
            schema,
            [
                Row(("id", 1L), ("name", "same"), ("version", new byte[] { 1 })),
                Row(("id", 2L), ("name", "source"), ("version", new byte[] { 2 })),
                Row(("id", 3L), ("name", "insert"), ("version", new byte[] { 3 })),
            ]);
        var target = new StaticDataCompareTarget(
            "target",
            schema,
            [
                Row(("id", 1L), ("name", "same"), ("version", new byte[] { 9 })),
                Row(("id", 2L), ("name", "target"), ("version", new byte[] { 8 })),
            ]);

        var service = new DataComparisonService();
        DataDiffReport report = await service.CompareAsync(
            source,
            target,
            new DataCompareOptions { TableName = "items" },
            TestContext.Current.CancellationToken);

        Assert.Equal(1, report.Summary.SourceOnlyRows);
        Assert.Equal(1, report.Summary.ChangedRows);
        Assert.Contains(report.Warnings, warning => warning.Contains("regenerated", StringComparison.OrdinalIgnoreCase));
        Assert.All(
            report.Rows,
            row =>
            {
                Assert.DoesNotContain("version", row.SourceValues?.Keys ?? [], StringComparer.OrdinalIgnoreCase);
                Assert.DoesNotContain("version", row.TargetValues?.Keys ?? [], StringComparer.OrdinalIgnoreCase);
                Assert.DoesNotContain("version", row.ChangedColumns, StringComparer.OrdinalIgnoreCase);
            });

        string script = service.RenderSyncScript(report);
        Assert.Contains("INSERT INTO items (id, name) VALUES (3, 'insert');", script, StringComparison.Ordinal);
        Assert.Contains("UPDATE items SET name = 'source' WHERE id = 2;", script, StringComparison.Ordinal);
        Assert.DoesNotContain("version =", script, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("(id, name, version)", script, StringComparison.OrdinalIgnoreCase);

        InvalidOperationException keyError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.CompareAsync(
                source,
                target,
                new DataCompareOptions { TableName = "items", KeyColumns = ["version"] },
                TestContext.Current.CancellationToken));
        Assert.Contains("cannot be used", keyError.Message, StringComparison.OrdinalIgnoreCase);

        var mismatchedTarget = new StaticDataCompareTarget(
            "target",
            Table(
                "items",
                Column("id", ClientDbType.Integer, nullable: false, isPrimaryKey: true),
                Column("name", ClientDbType.Text, nullable: false),
                Column("version", ClientDbType.Blob, nullable: false)),
            []);
        InvalidOperationException markerError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.CompareAsync(
                source,
                mismatchedTarget,
                new DataCompareOptions { TableName = "items" },
                TestContext.Current.CancellationToken));
        Assert.Contains("ROWVERSION", markerError.Message, StringComparison.OrdinalIgnoreCase);
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
        bool isRowVersion = false,
        string? collation = null,
        string? defaultSql = null)
        => new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
            IsPrimaryKey = isPrimaryKey,
            IsIdentity = isIdentity,
            IsRowVersion = isRowVersion,
            Collation = collation,
            DefaultSql = defaultSql,
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
