using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Client.Internal;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public class TableArchiveTests
{
    [Fact]
    public async Task ExactArchiveRestore_PreservesStableSchemaIdentities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string TableName = " exact_identity_items ";
        string quotedTableName = SqlIdentifierRules.Quote(TableName);
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"exact_identity_restore_{Guid.NewGuid():N}.db");
        string archivePath = Path.Combine(
            Path.GetTempPath(),
            $"exact_identity_restore_{Guid.NewGuid():N}.csdbtable");

        try
        {
            CSharpDB.Client.Models.TableSchema before;
            await using (var client =
                         new EngineTransportClient(databasePath))
            {
                CSharpDB.Client.Models.SqlExecutionResult create =
                    await client.ExecuteSqlAsync(
                        $"""
                        CREATE TABLE {quotedTableName} (
                            id INTEGER PRIMARY KEY,
                            parent_id INTEGER,
                            CONSTRAINT ck_exact_identity_id CHECK (id > 0),
                            CONSTRAINT fk_exact_identity_parent
                                FOREIGN KEY (parent_id)
                                REFERENCES {quotedTableName} (id)
                                ON DELETE RESTRICT
                        );
                        """,
                        ct);
                Assert.Null(create.Error);
                Assert.Null((await client.ExecuteSqlAsync(
                    $"INSERT INTO {quotedTableName} (id, parent_id) VALUES (1, NULL), (2, 1);",
                    ct)).Error);

                before =
                    Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                        await client.GetTableSchemaAsync(TableName, ct));
                var service = new TableImportExportService(
                    client,
                    new TableArchiveDownloadStore());
                await service.ExportTableAsync(
                    new TableExportRequest
                    {
                        TableName = TableName,
                        Destination =
                            TableExportDestination.ServerPath,
                        ServerPath = archivePath,
                    },
                    ct: ct);

                Assert.Null((await client.ExecuteSqlAsync(
                    $"DROP TABLE {quotedTableName};",
                    ct)).Error);
                RestoreTableResult result =
                    await service.RestoreTableAsync(
                        new RestoreTableRequest
                        {
                            ArchivePath = archivePath,
                            TargetTableName = TableName,
                        },
                        ct);
                Assert.Equal(TableName, result.TableName);
            }

            await using var reopened =
                new EngineTransportClient(databasePath);
            CSharpDB.Client.Models.TableSchema after =
                Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                    await reopened.GetTableSchemaAsync(TableName, ct));
            Assert.Equal(before.SchemaId, after.SchemaId);
            Assert.Equal(
                before.Columns.Select(static column => column.SchemaId),
                after.Columns.Select(static column => column.SchemaId));
            Assert.Equal(
                before.KeyConstraints.Select(static key => key.SchemaId),
                after.KeyConstraints.Select(static key => key.SchemaId));
            Assert.Equal(
                before.CheckConstraints.Select(static check => check.SchemaId),
                after.CheckConstraints.Select(static check => check.SchemaId));

            CSharpDB.Client.Models.ForeignKeyDefinition beforeForeignKey =
                Assert.Single(before.ForeignKeys);
            CSharpDB.Client.Models.ForeignKeyDefinition afterForeignKey =
                Assert.Single(after.ForeignKeys);
            Assert.Equal(beforeForeignKey.SchemaId, afterForeignKey.SchemaId);
            Assert.Equal(
                beforeForeignKey.ColumnSchemaIds,
                afterForeignKey.ColumnSchemaIds);
            Assert.Equal(
                beforeForeignKey.ReferencedTableSchemaId,
                afterForeignKey.ReferencedTableSchemaId);
            Assert.Equal(
                beforeForeignKey.ReferencedColumnSchemaIds,
                afterForeignKey.ReferencedColumnSchemaIds);
            Assert.Equal(
                beforeForeignKey.ReferencedKeySchemaId,
                afterForeignKey.ReferencedKeySchemaId);
        }
        finally
        {
            if (File.Exists(archivePath))
                File.Delete(archivePath);
            if (File.Exists(databasePath))
                File.Delete(databasePath);
            if (File.Exists(databasePath + ".wal"))
                File.Delete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task ExactArchiveRestore_RebindsExternalForeignKeyToLiveParent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"external_identity_restore_{Guid.NewGuid():N}.db");
        string archivePath = Path.Combine(
            Path.GetTempPath(),
            $"external_identity_restore_{Guid.NewGuid():N}.csdbtable");

        try
        {
            CSharpDB.Client.Models.TableSchema childBefore;
            CSharpDB.Client.Models.TableSchema replacementParent;
            await using (var client =
                         new EngineTransportClient(databasePath))
            {
                Assert.Null((await client.ExecuteSqlAsync(
                    "CREATE TABLE restore_parent (id INTEGER PRIMARY KEY);",
                    ct)).Error);
                Assert.Null((await client.ExecuteSqlAsync(
                    """
                    CREATE TABLE restore_child (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER,
                        CONSTRAINT fk_restore_child_parent
                            FOREIGN KEY (parent_id)
                            REFERENCES restore_parent(id)
                    );
                    """,
                    ct)).Error);
                Assert.Null((await client.ExecuteSqlAsync(
                    "INSERT INTO restore_parent VALUES (1); " +
                    "INSERT INTO restore_child VALUES (1, 1);",
                    ct)).Error);

                CSharpDB.Client.Models.TableSchema originalParent =
                    Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                        await client.GetTableSchemaAsync(
                            "restore_parent",
                            ct));
                childBefore =
                    Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                        await client.GetTableSchemaAsync(
                            "restore_child",
                            ct));
                var service = new TableImportExportService(
                    client,
                    new TableArchiveDownloadStore());
                await service.ExportTableAsync(
                    new TableExportRequest
                    {
                        TableName = "restore_child",
                        Destination =
                            TableExportDestination.ServerPath,
                        ServerPath = archivePath,
                    },
                    ct: ct);

                Assert.Null((await client.ExecuteSqlAsync(
                    "DROP TABLE restore_child; " +
                    "DROP TABLE restore_parent; " +
                    "CREATE TABLE restore_parent (id INTEGER PRIMARY KEY); " +
                    "INSERT INTO restore_parent VALUES (1);",
                    ct)).Error);
                replacementParent =
                    Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                        await client.GetTableSchemaAsync(
                            "restore_parent",
                            ct));
                Assert.NotEqual(
                    originalParent.SchemaId,
                    replacementParent.SchemaId);

                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restore_child",
                    },
                    ct);
            }

            await using var reopened =
                new EngineTransportClient(databasePath);
            CSharpDB.Client.Models.TableSchema childAfter =
                Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                    await reopened.GetTableSchemaAsync(
                        "restore_child",
                        ct));
            Assert.Equal(childBefore.SchemaId, childAfter.SchemaId);
            Assert.Equal(
                childBefore.Columns.Select(column =>
                    (column.Name, column.SchemaId)),
                childAfter.Columns.Select(column =>
                    (column.Name, column.SchemaId)));
            Assert.Equal(
                childBefore.ForeignKeys.Select(foreignKey =>
                    foreignKey.SchemaId),
                childAfter.ForeignKeys.Select(foreignKey =>
                    foreignKey.SchemaId));

            CSharpDB.Client.Models.ForeignKeyDefinition rebound =
                Assert.Single(childAfter.ForeignKeys);
            Assert.Equal(
                replacementParent.SchemaId,
                rebound.ReferencedTableSchemaId);
            Assert.Equal(
                [
                    replacementParent.Columns.Single(column =>
                        column.Name == "id").SchemaId,
                ],
                rebound.ReferencedColumnSchemaIds);
            Assert.Equal(
                Assert.Single(replacementParent.KeyConstraints)
                    .SchemaId,
                rebound.ReferencedKeySchemaId);
        }
        finally
        {
            if (File.Exists(archivePath))
                File.Delete(archivePath);
            if (File.Exists(databasePath))
                File.Delete(databasePath);
            if (File.Exists(databasePath + ".wal"))
                File.Delete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task Archive_RoundTripsRowVersionMetadataAndValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"rowversion_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new ColumnDefinition
                {
                    Name = "version",
                    Type = DbType.Blob,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
        };
        byte[] token = [0, 0, 0, 0, 0, 0, 0, 7];
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromBlob(token)],
        ];

        try
        {
            TableArchiveManifest manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(rows, ct),
                ct);

            Assert.Equal(TableArchiveManifest.LatestFormatVersion, manifest.FormatVersion);
            byte[] header = File.ReadAllBytes(path);
            Assert.Equal(
                TableArchiveManifest.LatestFormatVersion,
                BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, sizeof(int))));
            Assert.DoesNotContain("\"secondaryIndexes\"", ReadSchemaJson(header));
            TableSchema restoredSchema =
                await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);
            Assert.True(restoredSchema.Columns[1].IsRowVersion);
            Assert.Equal(DbType.Blob, restoredSchema.Columns[1].Type);
            Assert.False(restoredSchema.Columns[1].Nullable);

            var restoredRows = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                restoredRows.Add(row);

            Assert.Equal(token, Assert.Single(restoredRows)[1].AsBlob);

            string databasePath = Path.Combine(Path.GetTempPath(), $"rowversion_restore_{Guid.NewGuid():N}.db");
            try
            {
                await using var client = new EngineTransportClient(databasePath);
                var service = new TableImportExportService(client, new TableArchiveDownloadStore());

                RestoreTableResult restore = await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = path,
                        TargetTableName = "restored_items",
                    },
                    ct);

                Assert.Equal(1, restore.RowsInserted);
                Assert.True(restore.RowVersionTokensRegenerated);
                CSharpDB.Client.Models.TableSchema restoredTable = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                    await client.GetTableSchemaAsync("restored_items", ct));
                Assert.True(Assert.Single(restoredTable.Columns, column => column.Name == "version").IsRowVersion);

                CSharpDB.Client.Models.SqlExecutionResult query =
                    await client.ExecuteSqlAsync("SELECT id, version FROM restored_items;", ct);
                Assert.Null(query.Error);
                object?[] restoredRow = Assert.Single(query.Rows!);
                byte[] regeneratedToken = Assert.IsType<byte[]>(restoredRow[1]);
                Assert.Equal([0, 0, 0, 0, 0, 0, 0, 1], regeneratedToken);
                Assert.NotEqual(token, regeneratedToken);
            }
            finally
            {
                if (File.Exists(databasePath))
                    File.Delete(databasePath);
                if (File.Exists(databasePath + ".wal"))
                    File.Delete(databasePath + ".wal");
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RowVersionHeaderAndManifestVersionsMustMatchOnEveryReadPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"rowversion_version_mismatch_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "version",
                    Type = DbType.Blob,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [DbValue.FromBlob([0, 0, 0, 0, 0, 0, 0, 1])],
                    ],
                    ct),
                ct);

            byte[] archive = await File.ReadAllBytesAsync(path, ct);
            byte[] versionSix =
                Encoding.UTF8.GetBytes("\"formatVersion\": 6");
            int versionOffset = archive.AsSpan().IndexOf(versionSix);
            Assert.True(versionOffset >= 0);
            archive[versionOffset + versionSix.Length - 1] = (byte)'5';
            await File.WriteAllBytesAsync(path, archive, ct);

            await Assert.ThrowsAsync<InvalidDataException>(
                async () =>
                    await TableArchiveReader.ReadArchiveSchemaAsync(
                        path,
                        ct));
            await Assert.ThrowsAsync<InvalidDataException>(
                async () =>
                {
                    await foreach (DbValue[] _ in
                        TableArchiveReader.ReadRowsAsync(path, ct))
                    {
                    }
                });
            await Assert.ThrowsAsync<InvalidDataException>(
                async () =>
                    await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(
                        path,
                        ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RoundtripsSchemaAndRows()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"customers_{Guid.NewGuid():N}.csdbtable");
        Guid tableId = Guid.NewGuid();
        Guid idColumnId = Guid.NewGuid();
        Guid nameColumnId = Guid.NewGuid();
        Guid balanceColumnId = Guid.NewGuid();
        Guid payloadColumnId = Guid.NewGuid();
        Guid checkId = Guid.NewGuid();
        Guid foreignKeyId = Guid.NewGuid();
        Guid referencedTableId = Guid.NewGuid();
        Guid referencedColumnId = Guid.NewGuid();
        Guid referencedNameColumnId = Guid.NewGuid();
        Guid referencedKeyId = Guid.NewGuid();
        Guid primaryKeyId = Guid.NewGuid();
        Guid uniqueKeyId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = tableId,
            TableName = "customers",
            Columns =
            [
                new ColumnDefinition { SchemaId = idColumnId, Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true, IsIdentity = true },
                new ColumnDefinition { SchemaId = nameColumnId, Name = "name", Type = DbType.Text, Nullable = false, Collation = "NOCASE", DefaultSql = "'anonymous'" },
                new ColumnDefinition { SchemaId = balanceColumnId, Name = "balance", Type = DbType.Real, Nullable = true },
                new ColumnDefinition { SchemaId = payloadColumnId, Name = "payload", Type = DbType.Blob, Nullable = true },
            ],
            CheckConstraints =
            [
                new CheckConstraintDefinition
                {
                    SchemaId = checkId,
                    ConstraintName = "ck_customers_balance",
                    ExpressionSql = "balance >= 0",
                    ColumnName = "balance",
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = foreignKeyId,
                    ColumnSchemaIds = [idColumnId, nameColumnId],
                    ReferencedTableSchemaId = referencedTableId,
                    ReferencedColumnSchemaIds = [referencedColumnId, referencedNameColumnId],
                    ReferencedKeySchemaId = referencedKeyId,
                    ConstraintName = "fk_customers_tenant",
                    ColumnName = "id",
                    ReferencedTableName = "tenants",
                    ReferencedColumnName = "tenant_id",
                    ColumnNames = ["id", "name"],
                    ReferencedColumnNames = ["tenant_id", "customer_name"],
                    OnDelete = ForeignKeyOnDeleteAction.Cascade,
                    OnUpdate = ForeignKeyOnDeleteAction.NoAction,
                    SupportingIndexName = "__fk_customers_tenant",
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    SchemaId = primaryKeyId,
                    ConstraintName = "pk_customers",
                    Kind = KeyConstraintKind.PrimaryKey,
                    Columns = ["id"],
                },
                new KeyConstraintDefinition
                {
                    SchemaId = uniqueKeyId,
                    ConstraintName = "uq_customers_name",
                    Kind = KeyConstraintKind.Unique,
                    Columns = ["name"],
                    BackingIndexName = "__constraint_customers_name",
                },
            ],
            NextRowId = 12,
        };
        var rows = new List<DbValue[]>
        {
            new[] { DbValue.FromInteger(1), DbValue.FromText("O'Reilly"), DbValue.FromReal(10.5), DbValue.FromBlob(new byte[] { 0x01, 0x02, 0xff }) },
            new[] { DbValue.FromInteger(2), DbValue.FromText("Nulls"), DbValue.Null, DbValue.Null },
        };

        try
        {
            var manifest = await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(rows, ct), ct);
            Assert.Equal(2, manifest.RowCount);
            Assert.Equal("customers", manifest.SourceTableName);
            Assert.Equal(TableArchiveManifest.LatestFormatVersion, manifest.FormatVersion);
            Assert.Equal("CSDBTBL3"u8.ToArray(), File.ReadAllBytes(path).Take(8).ToArray());
            Assert.DoesNotContain("\"secondaryIndexes\"", ReadSchemaJson(File.ReadAllBytes(path)));
            var index = Assert.Single(manifest.Indexes);
            Assert.Equal("primary-key", index.Kind);
            Assert.Equal("id", index.ColumnName);
            Assert.Equal(2, index.EntryCount);

            TableSchema restoredSchema = await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);
            Assert.Equal(tableId, restoredSchema.SchemaId);
            Assert.Equal("customers", restoredSchema.TableName);
            Assert.Equal(4, restoredSchema.Columns.Count);
            Assert.True(restoredSchema.Columns[0].IsPrimaryKey);
            Assert.True(restoredSchema.Columns[0].IsIdentity);
            Assert.Equal(idColumnId, restoredSchema.Columns[0].SchemaId);
            Assert.Equal("NOCASE", restoredSchema.Columns[1].Collation);
            Assert.Equal("'anonymous'", restoredSchema.Columns[1].DefaultSql);
            CheckConstraintDefinition check = Assert.Single(restoredSchema.CheckConstraints);
            Assert.Equal(checkId, check.SchemaId);
            Assert.Equal("ck_customers_balance", check.ConstraintName);
            Assert.Equal("balance >= 0", check.ExpressionSql);
            Assert.Equal("balance", check.ColumnName);
            ForeignKeyDefinition foreignKey = Assert.Single(restoredSchema.ForeignKeys);
            Assert.Equal(foreignKeyId, foreignKey.SchemaId);
            Assert.Equal([idColumnId, nameColumnId], foreignKey.ColumnSchemaIds);
            Assert.Equal(referencedTableId, foreignKey.ReferencedTableSchemaId);
            Assert.Equal([referencedColumnId, referencedNameColumnId], foreignKey.ReferencedColumnSchemaIds);
            Assert.Equal(referencedKeyId, foreignKey.ReferencedKeySchemaId);
            Assert.Equal("id", foreignKey.ColumnName);
            Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);
            Assert.Equal(["id", "name"], foreignKey.ColumnNames);
            Assert.Equal(["tenant_id", "customer_name"], foreignKey.ReferencedColumnNames);
            Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
            Assert.Equal(ForeignKeyOnDeleteAction.NoAction, foreignKey.OnUpdate);
            Assert.Collection(
                restoredSchema.KeyConstraints,
                primary =>
                {
                    Assert.Equal(primaryKeyId, primary.SchemaId);
                    Assert.Equal(KeyConstraintKind.PrimaryKey, primary.Kind);
                    Assert.Equal(["id"], primary.Columns);
                },
                unique =>
                {
                    Assert.Equal(uniqueKeyId, unique.SchemaId);
                    Assert.Equal(KeyConstraintKind.Unique, unique.Kind);
                    Assert.Equal(["name"], unique.Columns);
                    Assert.Equal("__constraint_customers_name", unique.BackingIndexName);
                });
            Assert.Equal(12, restoredSchema.NextRowId);

            var restoredRows = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                restoredRows.Add(row);

            Assert.Equal(rows.Count, restoredRows.Count);
            Assert.Equal("O'Reilly", restoredRows[0][1].AsText);
            Assert.Equal(10.5, restoredRows[0][2].AsReal);
            Assert.Equal(new byte[] { 0x01, 0x02, 0xff }, restoredRows[0][3].AsBlob);
            Assert.True(restoredRows[1][2].IsNull);
            Assert.True(restoredRows[1][3].IsNull);

            Assert.True(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(path, ct));
            var lookup = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 2, ct);
            Assert.True(lookup.IsIndexed);
            Assert.NotNull(lookup.Row);
            Assert.Equal("Nulls", lookup.Row![1].AsText);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_V6_RestoresNamedOrderedConstraintsSecondaryIndexesAndPhase3Actions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"schema_fidelity_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"schema_fidelity_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "archive_items",
            Columns =
            [
                new ColumnDefinition { Name = "tenant_id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "code", Type = DbType.Text, Nullable = false, IsPrimaryKey = true, Collation = "NOCASE" },
                new ColumnDefinition { Name = "parent_tenant_id", Type = DbType.Integer, Nullable = true },
                new ColumnDefinition { Name = "parent_code", Type = DbType.Text, Nullable = true, Collation = "NOCASE" },
                new ColumnDefinition { Name = "status", Type = DbType.Text, Nullable = false, Collation = "NOCASE", DefaultSql = "'new'" },
                new ColumnDefinition { Name = "quantity", Type = DbType.Integer, Nullable = false },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    ConstraintName = "pk_archive_items",
                    Kind = KeyConstraintKind.PrimaryKey,
                    Columns = ["tenant_id", "code"],
                    BackingIndexName = "__constraint_archive_items_pk",
                },
                new KeyConstraintDefinition
                {
                    ConstraintName = "uq_archive_items_code_tenant",
                    Kind = KeyConstraintKind.Unique,
                    Columns = ["code", "tenant_id"],
                    BackingIndexName = "__constraint_archive_items_uq",
                },
            ],
            CheckConstraints =
            [
                new CheckConstraintDefinition
                {
                    ConstraintName = "ck_archive_items_quantity",
                    ExpressionSql = "(\"quantity\" >= 0)",
                    ColumnName = "quantity",
                },
                new CheckConstraintDefinition
                {
                    ConstraintName = "ck_archive_items_status",
                    ExpressionSql = "(\"status\" <> '')",
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_archive_items_parent",
                    ColumnName = "parent_tenant_id",
                    ReferencedTableName = "archive_items",
                    ReferencedColumnName = "tenant_id",
                    ColumnNames = ["parent_tenant_id", "parent_code"],
                    ReferencedColumnNames = ["tenant_id", "code"],
                    OnDelete = ForeignKeyOnDeleteAction.SetDefault,
                    OnUpdate = ForeignKeyOnDeleteAction.Cascade,
                    SupportingIndexName = "__fk_archive_items_parent",
                },
            ],
            NextRowId = 41,
        };
        IndexSchema[] secondaryIndexes =
        [
            new IndexSchema
            {
                IndexName = "ix_archive_items_status_quantity",
                TableName = "archive_items",
                Columns = ["status", "quantity"],
                ColumnCollations = ["NOCASE", null],
                IsUnique = false,
            },
            new IndexSchema
            {
                IndexName = "ux_archive_items_status",
                TableName = "archive_items",
                Columns = ["status"],
                ColumnCollations = ["NOCASE"],
                IsUnique = true,
            },
        ];
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("P"), DbValue.Null, DbValue.Null, DbValue.FromText("new"), DbValue.FromInteger(1)],
            [DbValue.FromInteger(1), DbValue.FromText("C"), DbValue.FromInteger(1), DbValue.FromText("P"), DbValue.FromText("ready"), DbValue.FromInteger(2)],
        ];

        try
        {
            TableArchiveManifest manifest = await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                secondaryIndexes,
                TableArchiveWriter.ToAsyncRows(rows, ct),
                ct);

            Assert.Equal(TableArchiveManifest.LatestFormatVersion, manifest.FormatVersion);
            byte[] archive = await File.ReadAllBytesAsync(archivePath, ct);
            Assert.Equal(
                TableArchiveManifest.LatestFormatVersion,
                BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(8, sizeof(int))));
            Assert.Contains("\"secondaryIndexes\"", ReadSchemaJson(archive));

            (TableArchiveSchema archivedSchema, TableArchiveManifest archivedManifest) =
                await TableArchiveReader.ReadMetadataAsync(archivePath, ct);
            Assert.Equal(TableArchiveManifest.LatestFormatVersion, archivedManifest.FormatVersion);
            TableArchiveForeignKey archivedForeignKey =
                Assert.Single(archivedSchema.ForeignKeys);
            Assert.Equal(
                ForeignKeyOnDeleteAction.SetDefault,
                archivedForeignKey.OnDelete);
            Assert.Equal(
                ForeignKeyOnDeleteAction.Cascade,
                archivedForeignKey.OnUpdate);
            Assert.Collection(
                Assert.IsAssignableFrom<IReadOnlyList<TableArchiveSecondaryIndex>>(archivedSchema.SecondaryIndexes),
                index =>
                {
                    Assert.Equal("ix_archive_items_status_quantity", index.Name);
                    Assert.Equal(["status", "quantity"], index.Columns);
                    Assert.Equal(["NOCASE", null], index.ColumnCollations);
                    Assert.False(index.IsUnique);
                },
                index =>
                {
                    Assert.Equal("ux_archive_items_status", index.Name);
                    Assert.Equal(["status"], index.Columns);
                    Assert.True(index.IsUnique);
                });

            await using var client = new EngineTransportClient(databasePath);
            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            RestoreTableResult restored = await service.RestoreTableAsync(
                new RestoreTableRequest
                {
                    ArchivePath = archivePath,
                    TargetTableName = "restored_items",
                },
                ct);

            Assert.Equal(2, restored.RowsInserted);
            CSharpDB.Client.Models.TableSchema restoredSchema = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                await client.GetTableSchemaAsync("restored_items", ct));
            Assert.Equal(
                ["tenant_id", "code", "parent_tenant_id", "parent_code", "status", "quantity"],
                restoredSchema.Columns.Select(column => column.Name));
            Assert.Equal("'new'", restoredSchema.Columns.Single(column => column.Name == "status").DefaultSql);
            Assert.Collection(
                restoredSchema.KeyConstraints,
                primary =>
                {
                    Assert.Equal("pk_archive_items", primary.ConstraintName);
                    Assert.Equal(CSharpDB.Client.Models.KeyConstraintKind.PrimaryKey, primary.Kind);
                    Assert.Equal(["tenant_id", "code"], primary.Columns);
                },
                unique =>
                {
                    Assert.Equal("uq_archive_items_code_tenant", unique.ConstraintName);
                    Assert.Equal(CSharpDB.Client.Models.KeyConstraintKind.Unique, unique.Kind);
                    Assert.Equal(["code", "tenant_id"], unique.Columns);
                });
            Assert.Collection(
                restoredSchema.CheckConstraints,
                quantity =>
                {
                    Assert.Equal("ck_archive_items_quantity", quantity.ConstraintName);
                    Assert.Equal("quantity", quantity.ColumnName);
                    Assert.Equal("(\"quantity\" >= 0)", quantity.ExpressionSql);
                },
                status =>
                {
                    Assert.Equal("ck_archive_items_status", status.ConstraintName);
                    Assert.Null(status.ColumnName);
                    Assert.Equal("(\"status\" <> '')", status.ExpressionSql);
                });
            CSharpDB.Client.Models.ForeignKeyDefinition restoredForeignKey = Assert.Single(restoredSchema.ForeignKeys);
            Assert.Equal("fk_archive_items_parent", restoredForeignKey.ConstraintName);
            Assert.Equal(["parent_tenant_id", "parent_code"], restoredForeignKey.ColumnNames);
            Assert.Equal("restored_items", restoredForeignKey.ReferencedTableName);
            Assert.Equal(["tenant_id", "code"], restoredForeignKey.ReferencedColumnNames);
            Assert.Equal(CSharpDB.Client.Models.ForeignKeyOnDeleteAction.SetDefault, restoredForeignKey.OnDelete);
            Assert.Equal(CSharpDB.Client.Models.ForeignKeyOnDeleteAction.Cascade, restoredForeignKey.OnUpdate);

            CSharpDB.Client.Models.IndexSchema[] restoredIndexes = (await client.GetIndexesAsync(ct))
                .Where(index => string.Equals(index.TableName, "restored_items", StringComparison.Ordinal))
                .ToArray();
            Assert.Collection(
                restoredIndexes,
                index =>
                {
                    Assert.Equal("ix_archive_items_status_quantity", index.IndexName);
                    Assert.Equal(["status", "quantity"], index.Columns);
                    Assert.Equal(["NOCASE", null], index.ColumnCollations);
                    Assert.False(index.IsUnique);
                },
                index =>
                {
                    Assert.Equal("ux_archive_items_status", index.IndexName);
                    Assert.Equal(["status"], index.Columns);
                    Assert.Equal(["NOCASE"], index.ColumnCollations);
                    Assert.True(index.IsUnique);
                });
        }
        finally
        {
            if (File.Exists(archivePath))
                File.Delete(archivePath);
            if (File.Exists(databasePath))
                File.Delete(databasePath);
            if (File.Exists(databasePath + ".wal"))
                File.Delete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task Archive_PathWriterFailure_PreservesExistingDestinationAtomically()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"atomic_archive_{Guid.NewGuid():N}.csdbtable");
        byte[] original = "existing archive sentinel"u8.ToArray();
        var schema = new TableSchema
        {
            TableName = "atomic_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
            ],
        };

        try
        {
            await File.WriteAllBytesAsync(path, original, ct);
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await TableArchiveWriter.WriteAsync(
                    path,
                    schema,
                    TableArchiveWriter.ToAsyncRows(
                        [
                            [DbValue.FromInteger(1)],
                            [DbValue.FromInteger(2), DbValue.FromInteger(3)],
                        ],
                        ct),
                    ct));

            Assert.Equal(original, await File.ReadAllBytesAsync(path, ct));
            Assert.Empty(Directory.GetFiles(
                Path.GetDirectoryName(path)!,
                $".{Path.GetFileName(path)}.*.tmp"));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsPartialStableForeignKeyBindings()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"partial_bindings_{Guid.NewGuid():N}.csdbtable");
        Guid firstColumnId = Guid.NewGuid();
        Guid secondColumnId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = Guid.NewGuid(),
            TableName = "partial_bindings",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = firstColumnId,
                    Name = "first_id",
                    Type = DbType.Integer,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    SchemaId = secondColumnId,
                    Name = "second_id",
                    Type = DbType.Integer,
                    Nullable = false,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ColumnSchemaIds = [firstColumnId],
                    ReferencedTableSchemaId = Guid.NewGuid(),
                    ReferencedColumnSchemaIds = [Guid.NewGuid()],
                    ReferencedKeySchemaId = Guid.NewGuid(),
                    ConstraintName = "fk_partial_bindings",
                    ColumnName = "first_id",
                    ReferencedTableName = "parent_bindings",
                    ReferencedColumnName = "first_id",
                    ColumnNames = ["first_id", "second_id"],
                    ReferencedColumnNames = ["first_id", "second_id"],
                    SupportingIndexName = "__fk_partial_bindings",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct);

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsExternalBindingThatReusesLaterForeignKeyIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"later_fk_identity_{Guid.NewGuid():N}.csdbtable");
        Guid firstColumnId = Guid.NewGuid();
        Guid secondColumnId = Guid.NewGuid();
        Guid firstForeignKeyId = Guid.NewGuid();
        Guid secondForeignKeyId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = Guid.NewGuid(),
            TableName = "later_fk_identity",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = firstColumnId,
                    Name = "first_parent_id",
                    Type = DbType.Integer,
                },
                new ColumnDefinition
                {
                    SchemaId = secondColumnId,
                    Name = "second_parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = firstForeignKeyId,
                    ColumnSchemaIds = [firstColumnId],
                    ReferencedTableSchemaId = secondForeignKeyId,
                    ReferencedColumnSchemaIds = [Guid.NewGuid()],
                    ConstraintName = "fk_later_identity_first",
                    ColumnName = "first_parent_id",
                    ColumnNames = ["first_parent_id"],
                    ReferencedTableName = "external_first",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName = "__fk_later_identity_first",
                },
                new ForeignKeyDefinition
                {
                    SchemaId = secondForeignKeyId,
                    ColumnSchemaIds = [secondColumnId],
                    ReferencedTableSchemaId = Guid.NewGuid(),
                    ReferencedColumnSchemaIds = [Guid.NewGuid()],
                    ConstraintName = "fk_later_identity_second",
                    ColumnName = "second_parent_id",
                    ColumnNames = ["second_parent_id"],
                    ReferencedTableName = "external_second",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName = "__fk_later_identity_second",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "reuses an identity",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsExternalBindingIdentityRoleCollision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"fk_role_collision_{Guid.NewGuid():N}.csdbtable");
        Guid firstChildColumnId = Guid.NewGuid();
        Guid secondChildColumnId = Guid.NewGuid();
        Guid aliasedReferencedId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = Guid.NewGuid(),
            TableName = "fk_role_collision",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = firstChildColumnId,
                    Name = "first_parent_id",
                    Type = DbType.Integer,
                },
                new ColumnDefinition
                {
                    SchemaId = secondChildColumnId,
                    Name = "second_parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ColumnSchemaIds = [firstChildColumnId],
                    ReferencedTableSchemaId = aliasedReferencedId,
                    ReferencedColumnSchemaIds =
                        [Guid.NewGuid()],
                    ConstraintName = "fk_role_collision_first",
                    ColumnName = "first_parent_id",
                    ColumnNames = ["first_parent_id"],
                    ReferencedTableName = "external_parent_first",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName =
                        "__fk_role_collision_first",
                },
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ColumnSchemaIds = [secondChildColumnId],
                    ReferencedTableSchemaId = Guid.NewGuid(),
                    ReferencedColumnSchemaIds =
                        [aliasedReferencedId],
                    ConstraintName = "fk_role_collision_second",
                    ColumnName = "second_parent_id",
                    ColumnNames = ["second_parent_id"],
                    ReferencedTableName = "external_parent_second",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName =
                        "__fk_role_collision_second",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "referenced object roles",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsScalarOrderedForeignKeyMismatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"fk_scalar_mismatch_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "fk_scalar_mismatch",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "first_parent_id",
                    Type = DbType.Integer,
                },
                new ColumnDefinition
                {
                    Name = "second_parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_scalar_mismatch",
                    ColumnName = "first_parent_id",
                    ColumnNames = ["second_parent_id"],
                    ReferencedTableName = "external_parent",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName = "__fk_scalar_mismatch",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "scalar and ordered columns",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsRepeatedExternalReferencedColumn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"fk_repeated_reference_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "fk_repeated_reference",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "first_parent_id",
                    Type = DbType.Integer,
                },
                new ColumnDefinition
                {
                    Name = "second_parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_repeated_reference",
                    ColumnName = "first_parent_id",
                    ColumnNames =
                        ["first_parent_id", "second_parent_id"],
                    ReferencedTableName = "external_parent",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id", "id"],
                    SupportingIndexName =
                        "__fk_repeated_reference",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "repeats referenced column",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Archive_RejectsMissingSelfReferencedColumn(
        bool stableIdentities)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"missing_self_column_{Guid.NewGuid():N}.csdbtable");
        Guid tableId = stableIdentities ? Guid.NewGuid() : Guid.Empty;
        Guid childColumnId =
            stableIdentities ? Guid.NewGuid() : Guid.Empty;
        var schema = new TableSchema
        {
            SchemaId = tableId,
            TableName = "missing_self_column",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = childColumnId,
                    Name = "parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId =
                        stableIdentities ? Guid.NewGuid() : Guid.Empty,
                    ColumnSchemaIds = stableIdentities
                        ? [childColumnId]
                        : [],
                    ReferencedTableSchemaId = tableId,
                    ReferencedColumnSchemaIds = stableIdentities
                        ? [Guid.NewGuid()]
                        : [],
                    ConstraintName = "fk_missing_self_column",
                    ColumnName = "parent_id",
                    ColumnNames = ["parent_id"],
                    ReferencedTableName = "missing_self_column",
                    ReferencedColumnName = "missing_id",
                    ReferencedColumnNames = ["missing_id"],
                    SupportingIndexName =
                        "__fk_missing_self_column",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "missing column",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsNullLegacyForeignKeyIdentityLists()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"null_fk_bindings_{Guid.NewGuid():N}.csdbtable");

        try
        {
            await WriteLegacyArchiveAsync(path, ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);
            byte[] mutated = RewriteEmptyUnindexedArchiveJson(
                original,
                rewriteSchema: true,
                json =>
                {
                    const string Existing =
                        "\"supportingIndexName\": \"__fk_legacy_items_parent\"";
                    const string Replacement =
                        "\"supportingIndexName\": \"__fk_legacy_items_parent\",\n" +
                        "      \"columnSchemaIds\": null,\n" +
                        "      \"referencedColumnSchemaIds\": null";
                    Assert.Contains(
                        Existing,
                        json,
                        StringComparison.Ordinal);
                    return json.Replace(
                        Existing,
                        Replacement,
                        StringComparison.Ordinal);
                });
            await File.WriteAllBytesAsync(path, mutated, ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "null stable identity bindings",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsWrongSelfReferencedKeyIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"wrong_self_key_{Guid.NewGuid():N}.csdbtable");
        Guid tableId = Guid.NewGuid();
        Guid idColumnId = Guid.NewGuid();
        Guid parentColumnId = Guid.NewGuid();
        Guid keyId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = tableId,
            TableName = "self_key_items",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = idColumnId,
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                },
                new ColumnDefinition
                {
                    SchemaId = parentColumnId,
                    Name = "parent_id",
                    Type = DbType.Integer,
                    Nullable = true,
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    SchemaId = keyId,
                    Kind = KeyConstraintKind.PrimaryKey,
                    Columns = ["id"],
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ColumnSchemaIds = [parentColumnId],
                    ReferencedTableSchemaId = tableId,
                    ReferencedColumnSchemaIds = [idColumnId],
                    ReferencedKeySchemaId = Guid.NewGuid(),
                    ConstraintName = "fk_self_key_parent",
                    ColumnName = "parent_id",
                    ColumnNames = ["parent_id"],
                    ReferencedTableName = "self_key_items",
                    ReferencedColumnName = "id",
                    ReferencedColumnNames = ["id"],
                    SupportingIndexName =
                        "__fk_self_key_parent",
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    Array.Empty<DbValue[]>(),
                    ct),
                ct);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    async () =>
                        await TableArchiveReader.ReadTableSchemaAsync(
                            path,
                            ct: ct));
            Assert.Contains(
                "referenced-key identity",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_LegacySchemaWithoutAdditiveMetadata_UsesSafeDefaults()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"legacy_schema_{Guid.NewGuid():N}.csdbtable");

        try
        {
            await WriteLegacyArchiveAsync(path, ct);

            TableSchema restored = await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);

            Assert.Equal("legacy_items", restored.TableName);
            ColumnDefinition column = Assert.Single(restored.Columns);
            Assert.Equal("id", column.Name);
            Assert.Null(column.DefaultSql);
            Assert.Empty(restored.CheckConstraints);
            Assert.Empty(restored.KeyConstraints);
            ForeignKeyDefinition foreignKey = Assert.Single(restored.ForeignKeys);
            Assert.Equal(["id"], foreignKey.ColumnNames);
            Assert.Equal(["id"], foreignKey.ReferencedColumnNames);
            Assert.Equal(ForeignKeyOnDeleteAction.Restrict, foreignKey.OnDelete);
            Assert.Equal(ForeignKeyOnDeleteAction.Restrict, foreignKey.OnUpdate);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_VersionFiveForeignKeyWithoutOnUpdateDefaultsToRestrict()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"version_five_fk_{Guid.NewGuid():N}.csdbtable");

        try
        {
            await WriteVersionFiveArchiveWithoutOnUpdateAsync(path, ct);

            TableSchema restored =
                await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);
            ForeignKeyDefinition foreignKey =
                Assert.Single(restored.ForeignKeys);

            Assert.Equal(
                ForeignKeyOnDeleteAction.Restrict,
                foreignKey.OnDelete);
            Assert.Equal(
                ForeignKeyOnDeleteAction.Restrict,
                foreignKey.OnUpdate);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_PrimaryKeyIndexSupportsMultipleBTreeLevels()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"indexed_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "large_customers",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
            NextRowId = 65001,
        };

        try
        {
            await TableArchiveWriter.WriteAsync(path, schema, GenerateRows(65000, ct), ct);

            Assert.True(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(path, ct));

            var first = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 1, ct);
            var middle = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 32768, ct);
            var last = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 65000, ct);
            var missing = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 70000, ct);

            Assert.Equal("Customer 1", first.Row![1].AsText);
            Assert.Equal("Customer 32768", middle.Row![1].AsText);
            Assert.Equal("Customer 65000", last.Row![1].AsText);
            Assert.True(missing.IsIndexed);
            Assert.Null(missing.Row);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_SupportsEmptyTables()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"empty_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "empty_table",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
            ],
            NextRowId = 1,
        };

        try
        {
            var manifest = await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct), ct);
            Assert.Equal(0, manifest.RowCount);

            var rows = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                rows.Add(row);

            Assert.Empty(rows);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_RejectsOversizedNonCanonicalAndTrailingSections()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"invalid_layout_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "layout_items",
            Columns =
            [
                new ColumnDefinition { Name = "value", Type = DbType.Text, Nullable = false },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);

            byte[] oversized = original.ToArray();
            BinaryPrimitives.WriteInt32LittleEndian(oversized.AsSpan(20), int.MaxValue);
            await File.WriteAllBytesAsync(path, oversized, ct);
            InvalidDataException sizeError = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
            Assert.Contains("schema section exceeds", sizeError.Message, StringComparison.Ordinal);

            byte[] nonCanonical = original.ToArray();
            long rowsOffset = BinaryPrimitives.ReadInt64LittleEndian(nonCanonical.AsSpan(36));
            BinaryPrimitives.WriteInt64LittleEndian(nonCanonical.AsSpan(36), rowsOffset + 1);
            await File.WriteAllBytesAsync(path, nonCanonical, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));

            byte[] trailing = new byte[original.Length + 1];
            original.CopyTo(trailing, 0);
            trailing[^1] = 0x7f;
            await File.WriteAllBytesAsync(path, trailing, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_ExplicitNullCollectionsProduceControlledValidationErrors()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"null_metadata_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "null_metadata_items",
            Columns =
            [
                new ColumnDefinition { Name = "value", Type = DbType.Text, Nullable = false },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);

            (bool IsSchema, string Property)[] nullCollections =
            [
                (true, "foreignKeys"),
                (true, "checkConstraints"),
                (true, "keyConstraints"),
                (false, "indexes"),
            ];
            foreach ((bool isSchema, string property) in nullCollections)
            {
                byte[] mutated = RewriteEmptyUnindexedArchiveJson(
                    original,
                    isSchema,
                    json => ReplaceEmptyJsonCollectionWithNull(json, property));
                await File.WriteAllBytesAsync(path, mutated, ct);

                Exception? error = await Record.ExceptionAsync(
                    async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
                Assert.IsType<InvalidDataException>(error);
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_PhysicalIndexManifestAndHeaderMustMatchSchema()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"index_metadata_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "index_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("one")],
            [DbValue.FromInteger(2), DbValue.FromText("two")],
        ];

        try
        {
            await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(rows, ct), ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);

            byte[] manifestMismatch = original.ToArray();
            byte[] columnOrdinal = "\"columnIndex\": 0"u8.ToArray();
            int columnOrdinalOffset = manifestMismatch.AsSpan().IndexOf(columnOrdinal);
            Assert.True(columnOrdinalOffset >= 0);
            manifestMismatch[columnOrdinalOffset + columnOrdinal.Length - 1] = (byte)'1';
            await File.WriteAllBytesAsync(path, manifestMismatch, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));

            byte[] nativeHeaderMismatch = original.ToArray();
            long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(nativeHeaderMismatch.AsSpan(60));
            BinaryPrimitives.WriteInt32LittleEndian(
                nativeHeaderMismatch.AsSpan(checked((int)indexOffset + 12)),
                1);
            await File.WriteAllBytesAsync(path, nativeHeaderMismatch, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_IndexedLookupRejectsWrongAndMalformedRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"index_row_validation_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "lookup_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("one")],
            [DbValue.FromInteger(2), DbValue.FromText("two")],
        ];

        try
        {
            await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(rows, ct), ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);
            long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(original.AsSpan(60));
            long rootPageOffset = BinaryPrimitives.ReadInt64LittleEndian(
                original.AsSpan(checked((int)indexOffset + 24)));
            int firstEntryOffset = checked((int)(indexOffset + rootPageOffset + 24));
            long firstRowOffset = BinaryPrimitives.ReadInt64LittleEndian(
                original.AsSpan(firstEntryOffset + sizeof(long)));
            long secondRowOffset = BinaryPrimitives.ReadInt64LittleEndian(
                original.AsSpan(firstEntryOffset + 16 + sizeof(long)));

            byte[] wrongRow = original.ToArray();
            BinaryPrimitives.WriteInt64LittleEndian(
                wrongRow.AsSpan(firstEntryOffset + sizeof(long)),
                secondRowOffset);
            await File.WriteAllBytesAsync(path, wrongRow, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 1, ct));

            byte[] malformedRow = original.ToArray();
            malformedRow[checked((int)firstRowOffset + sizeof(int))] = 1;
            await File.WriteAllBytesAsync(path, malformedRow, ct);
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 1, ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_StreamWriterRewindsTruncatesAndRequiresWritableDestination()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var schema = new TableSchema
        {
            TableName = "stream_items",
            Columns =
            [
                new ColumnDefinition { Name = "value", Type = DbType.Text, Nullable = false },
            ],
        };
        byte[] oldContents = Enumerable.Repeat((byte)0x5a, 1024 * 1024).ToArray();
        await using var destination = new MemoryStream(oldContents, writable: true);
        destination.Position = 4096;

        await TableArchiveWriter.WriteAsync(
            destination,
            schema,
            TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
            ct);

        Assert.Equal(destination.Length, destination.Position);
        Assert.True(destination.Length < oldContents.Length);
        Assert.Equal("CSDBTBL3"u8.ToArray(), destination.ToArray().Take(8).ToArray());

        await using var readOnly = new MemoryStream(new byte[128], writable: false);
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await TableArchiveWriter.WriteAsync(
                readOnly,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct));
    }

    private static async IAsyncEnumerable<DbValue[]> GenerateRows(
        int count,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        for (int i = 1; i <= count; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return [DbValue.FromInteger(i), DbValue.FromText($"Customer {i}")];
            if ((i & 1023) == 0)
                await Task.Yield();
        }
    }

    private static string ReadSchemaJson(byte[] archive)
    {
        long schemaOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12, sizeof(long)));
        int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20, sizeof(int)));
        return Encoding.UTF8.GetString(archive, checked((int)schemaOffset), schemaLength);
    }

    private static string ReplaceEmptyJsonCollectionWithNull(string json, string property)
    {
        string oldValue = $"\"{property}\": []";
        string newValue = $"\"{property}\": null";
        Assert.Contains(oldValue, json, StringComparison.Ordinal);
        return json.Replace(oldValue, newValue, StringComparison.Ordinal);
    }

    private static byte[] RewriteEmptyUnindexedArchiveJson(
        byte[] archive,
        bool rewriteSchema,
        Func<string, string> rewrite)
    {
        const int headerSize = 76;
        long schemaOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12));
        int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20));
        long manifestOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(24));
        int manifestLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(32));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(44)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(52)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(68)));

        string schemaJson = Encoding.UTF8.GetString(archive, checked((int)schemaOffset), schemaLength);
        string manifestJson = Encoding.UTF8.GetString(archive, checked((int)manifestOffset), manifestLength);
        if (rewriteSchema)
            schemaJson = rewrite(schemaJson);
        else
            manifestJson = rewrite(manifestJson);

        byte[] schemaBytes = Encoding.UTF8.GetBytes(schemaJson);
        byte[] manifestBytes = Encoding.UTF8.GetBytes(manifestJson);
        long rowsOffset = headerSize + schemaBytes.Length;
        long rewrittenManifestOffset = rowsOffset;
        var result = new byte[checked(headerSize + schemaBytes.Length + manifestBytes.Length)];
        archive.AsSpan(0, headerSize).CopyTo(result);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(12), headerSize);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(20), schemaBytes.Length);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(24), rewrittenManifestOffset);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(32), manifestBytes.Length);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(36), rowsOffset);
        schemaBytes.CopyTo(result.AsSpan(headerSize));
        manifestBytes.CopyTo(result.AsSpan(checked((int)rewrittenManifestOffset)));
        return result;
    }

    private static async Task WriteLegacyArchiveAsync(string path, CancellationToken ct)
    {
        const int headerSize = 76;
        byte[] schema = Encoding.UTF8.GetBytes(
            """
            {
              "tableName": "legacy_items",
              "columns": [
                {
                  "name": "id",
                  "type": "integer",
                  "nullable": false,
                  "isPrimaryKey": true,
                  "isIdentity": false,
                  "collation": null
                }
              ],
              "foreignKeys": [
                {
                  "constraintName": "fk_legacy_items_parent",
                  "columnName": "id",
                  "referencedTableName": "legacy_parents",
                  "referencedColumnName": "id",
                  "onDelete": "restrict",
                  "supportingIndexName": "__fk_legacy_items_parent"
                }
              ],
              "nextRowId": 2
            }
            """);
        byte[] manifest = Encoding.UTF8.GetBytes(
            """
            {
              "formatVersion": 3,
              "sourceTableName": "legacy_items",
              "createdUtc": "2025-01-01T00:00:00+00:00",
              "rowCount": 0,
              "schemaEntry": "native:schema",
              "rowsEntry": "native:rows",
              "indexes": []
            }
            """);

        long schemaOffset = headerSize;
        long rowsOffset = schemaOffset + schema.Length;
        long manifestOffset = rowsOffset;
        var header = new byte[headerSize];
        "CSDBTBL3"u8.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), 3);
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(12), schemaOffset);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(20), schema.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(24), manifestOffset);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(32), manifest.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(36), rowsOffset);

        await using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        await stream.WriteAsync(header, ct);
        await stream.WriteAsync(schema, ct);
        await stream.WriteAsync(manifest, ct);
    }

    private static async Task WriteVersionFiveArchiveWithoutOnUpdateAsync(
        string path,
        CancellationToken ct)
    {
        const int headerSize = 76;
        byte[] schema = Encoding.UTF8.GetBytes(
            """
            {
              "tableName": "version_five_items",
              "columns": [
                {
                  "name": "id",
                  "type": "integer",
                  "nullable": false,
                  "isPrimaryKey": true,
                  "isIdentity": false,
                  "collation": null
                }
              ],
              "foreignKeys": [
                {
                  "constraintName": "fk_version_five_items_parent",
                  "columnName": "id",
                  "referencedTableName": "version_five_parents",
                  "referencedColumnName": "id",
                  "onDelete": "restrict",
                  "supportingIndexName": "__fk_version_five_items_parent"
                }
              ],
              "nextRowId": 2
            }
            """);
        string schemaDigest = Convert.ToHexString(
            SHA256.HashData(schema)).ToLowerInvariant();
        string emptyDigest = Convert.ToHexString(
            SHA256.HashData(Array.Empty<byte>())).ToLowerInvariant();
        byte[] manifest = Encoding.UTF8.GetBytes(
            $$"""
            {
              "formatVersion": 5,
              "sourceTableName": "version_five_items",
              "createdUtc": "2025-01-01T00:00:00+00:00",
              "rowCount": 0,
              "schemaEntry": "native:schema",
              "rowsEntry": "native:rows",
              "indexes": [],
              "digests": {
                "algorithm": "sha256",
                "encoding": "lowercase-hex",
                "schema": "{{schemaDigest}}",
                "rows": "{{emptyDigest}}",
                "physicalIndex": "{{emptyDigest}}"
              }
            }
            """);

        long schemaOffset = headerSize;
        long rowsOffset = schemaOffset + schema.Length;
        long manifestOffset = rowsOffset;
        var header = new byte[headerSize];
        "CSDBTBL3"u8.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), 5);
        BinaryPrimitives.WriteInt64LittleEndian(
            header.AsSpan(12),
            schemaOffset);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(20),
            schema.Length);
        BinaryPrimitives.WriteInt64LittleEndian(
            header.AsSpan(24),
            manifestOffset);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(32),
            manifest.Length);
        BinaryPrimitives.WriteInt64LittleEndian(
            header.AsSpan(36),
            rowsOffset);

        await using var stream = new FileStream(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None);
        await stream.WriteAsync(header, ct);
        await stream.WriteAsync(schema, ct);
        await stream.WriteAsync(manifest, ct);
    }
}
