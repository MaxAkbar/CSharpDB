using System.Buffers.Binary;
using System.Text;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public class TableArchiveTests
{
    [Fact]
    public async Task Archive_RoundtripsSchemaAndRows()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"customers_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "customers",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true, IsIdentity = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false, Collation = "NOCASE", DefaultSql = "'anonymous'" },
                new ColumnDefinition { Name = "balance", Type = DbType.Real, Nullable = true },
                new ColumnDefinition { Name = "payload", Type = DbType.Blob, Nullable = true },
            ],
            CheckConstraints =
            [
                new CheckConstraintDefinition
                {
                    ConstraintName = "ck_customers_balance",
                    ExpressionSql = "balance >= 0",
                    ColumnName = "balance",
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_customers_tenant",
                    ColumnName = "id",
                    ReferencedTableName = "tenants",
                    ReferencedColumnName = "tenant_id",
                    ColumnNames = ["id", "name"],
                    ReferencedColumnNames = ["tenant_id", "customer_name"],
                    OnDelete = ForeignKeyOnDeleteAction.Cascade,
                    SupportingIndexName = "__fk_customers_tenant",
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    ConstraintName = "pk_customers",
                    Kind = KeyConstraintKind.PrimaryKey,
                    Columns = ["id"],
                },
                new KeyConstraintDefinition
                {
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
            Assert.Equal(TableArchiveManifest.CurrentFormatVersion, manifest.FormatVersion);
            Assert.Equal("CSDBTBL3"u8.ToArray(), File.ReadAllBytes(path).Take(8).ToArray());
            var index = Assert.Single(manifest.Indexes);
            Assert.Equal("primary-key", index.Kind);
            Assert.Equal("id", index.ColumnName);
            Assert.Equal(2, index.EntryCount);

            TableSchema restoredSchema = await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);
            Assert.Equal("customers", restoredSchema.TableName);
            Assert.Equal(4, restoredSchema.Columns.Count);
            Assert.True(restoredSchema.Columns[0].IsPrimaryKey);
            Assert.True(restoredSchema.Columns[0].IsIdentity);
            Assert.Equal("NOCASE", restoredSchema.Columns[1].Collation);
            Assert.Equal("'anonymous'", restoredSchema.Columns[1].DefaultSql);
            CheckConstraintDefinition check = Assert.Single(restoredSchema.CheckConstraints);
            Assert.Equal("ck_customers_balance", check.ConstraintName);
            Assert.Equal("balance >= 0", check.ExpressionSql);
            Assert.Equal("balance", check.ColumnName);
            ForeignKeyDefinition foreignKey = Assert.Single(restoredSchema.ForeignKeys);
            Assert.Equal("id", foreignKey.ColumnName);
            Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);
            Assert.Equal(["id", "name"], foreignKey.ColumnNames);
            Assert.Equal(["tenant_id", "customer_name"], foreignKey.ReferencedColumnNames);
            Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
            Assert.Collection(
                restoredSchema.KeyConstraints,
                primary =>
                {
                    Assert.Equal(KeyConstraintKind.PrimaryKey, primary.Kind);
                    Assert.Equal(["id"], primary.Columns);
                },
                unique =>
                {
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
}
