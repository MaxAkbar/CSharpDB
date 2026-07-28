using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class SchemaSerializerCompatibilityTests
{
    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsStableIdentities()
    {
        Guid tableId = Guid.NewGuid();
        Guid columnId = Guid.NewGuid();
        Guid foreignKeyId = Guid.NewGuid();
        Guid referencedTableId = Guid.NewGuid();
        Guid referencedColumnId = Guid.NewGuid();
        Guid referencedKeyId = Guid.NewGuid();
        Guid checkId = Guid.NewGuid();
        Guid keyId = Guid.NewGuid();
        var schema = new TableSchema
        {
            SchemaId = tableId,
            TableName = "children",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = columnId,
                    Name = "parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = foreignKeyId,
                    ColumnSchemaIds = [columnId],
                    ReferencedTableSchemaId = referencedTableId,
                    ReferencedColumnSchemaIds = [referencedColumnId],
                    ReferencedKeySchemaId = referencedKeyId,
                    ConstraintName = "fk_children_parent",
                    ColumnName = "parent_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "id",
                    SupportingIndexName = "__fk_children_parent",
                },
            ],
            CheckConstraints =
            [
                new CheckConstraintDefinition
                {
                    SchemaId = checkId,
                    ConstraintName = "ck_parent_positive",
                    ExpressionSql = "parent_id > 0",
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    SchemaId = keyId,
                    ConstraintName = "uq_parent",
                    Kind = KeyConstraintKind.Unique,
                    Columns = ["parent_id"],
                },
            ],
        };

        TableSchema decoded =
            SchemaSerializer.Deserialize(SchemaSerializer.Serialize(schema));

        Assert.Equal(tableId, decoded.SchemaId);
        Assert.Equal(columnId, Assert.Single(decoded.Columns).SchemaId);
        Assert.Equal(foreignKeyId, Assert.Single(decoded.ForeignKeys).SchemaId);
        ForeignKeyDefinition foreignKey = Assert.Single(decoded.ForeignKeys);
        Assert.Equal([columnId], foreignKey.ColumnSchemaIds);
        Assert.Equal(referencedTableId, foreignKey.ReferencedTableSchemaId);
        Assert.Equal([referencedColumnId], foreignKey.ReferencedColumnSchemaIds);
        Assert.Equal(referencedKeyId, foreignKey.ReferencedKeySchemaId);
        Assert.Equal(checkId, Assert.Single(decoded.CheckConstraints).SchemaId);
        Assert.Equal(keyId, Assert.Single(decoded.KeyConstraints).SchemaId);
    }

    [Fact]
    public void Deserialize_LegacySchema_DerivesRepeatableNonEmptyIdentities()
    {
        byte[] legacy = BuildLegacyTableSchemaPayload(
            tableName: "legacy_identity",
            columns:
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    IsPrimaryKey = true,
                    Nullable = false,
                },
            ]);

        TableSchema first = SchemaSerializer.Deserialize(legacy);
        TableSchema second = SchemaSerializer.Deserialize(legacy);

        Assert.NotEqual(Guid.Empty, first.SchemaId);
        Assert.Equal(first.SchemaId, second.SchemaId);
        Assert.NotEqual(Guid.Empty, Assert.Single(first.Columns).SchemaId);
        Assert.Equal(
            Assert.Single(first.Columns).SchemaId,
            Assert.Single(second.Columns).SchemaId);
    }

    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsRowVersionMetadata()
    {
        var schema = new TableSchema
        {
            TableName = "items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    IsPrimaryKey = true,
                    Nullable = false,
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

        TableSchema decoded = SchemaSerializer.Deserialize(SchemaSerializer.Serialize(schema));

        Assert.False(decoded.Columns[0].IsRowVersion);
        Assert.True(decoded.Columns[1].IsRowVersion);
        Assert.Equal(DbType.Blob, decoded.Columns[1].Type);
        Assert.False(decoded.Columns[1].Nullable);
    }

    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsNextRowId()
    {
        var schema = new TableSchema
        {
            TableName = "users",
            Columns = new[]
            {
                new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, IsIdentity = true, Nullable = false },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = true, Collation = "NOCASE" },
            },
            NextRowId = 1234,
        };

        byte[] encoded = SchemaSerializer.Serialize(schema);
        var decoded = SchemaSerializer.Deserialize(encoded);

        Assert.Equal("users", decoded.TableName);
        Assert.Equal(2, decoded.Columns.Count);
        Assert.Equal(1234L, decoded.NextRowId);
        Assert.True(decoded.Columns[0].IsIdentity);
        Assert.Null(decoded.Columns[0].Collation);
        Assert.Equal("NOCASE", decoded.Columns[1].Collation);
    }

    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsForeignKeys()
    {
        var schema = new TableSchema
        {
            TableName = "children",
            Columns = new[]
            {
                new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, IsIdentity = true, Nullable = false },
                new ColumnDefinition { Name = "parent_id", Type = DbType.Integer, Nullable = true },
            },
            ForeignKeys = new[]
            {
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_children_parent_id_a1b2c3d4",
                    ColumnName = "parent_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "id",
                    OnDelete = ForeignKeyOnDeleteAction.Cascade,
                    SupportingIndexName = "__fk_children_parent_id_a1b2",
                },
            },
            NextRowId = 7,
        };

        byte[] encoded = SchemaSerializer.Serialize(schema);
        var decoded = SchemaSerializer.Deserialize(encoded);

        ForeignKeyDefinition foreignKey = Assert.Single(decoded.ForeignKeys);
        Assert.Equal("fk_children_parent_id_a1b2c3d4", foreignKey.ConstraintName);
        Assert.Equal("parent_id", foreignKey.ColumnName);
        Assert.Equal("parents", foreignKey.ReferencedTableName);
        Assert.Equal("id", foreignKey.ReferencedColumnName);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
        Assert.Equal("__fk_children_parent_id_a1b2", foreignKey.SupportingIndexName);
    }

    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsDefaultsAndChecks()
    {
        var schema = new TableSchema
        {
            TableName = "orders",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    Name = "quantity",
                    Type = DbType.Integer,
                    Nullable = false,
                    DefaultSql = "1",
                },
                new ColumnDefinition
                {
                    Name = "status",
                    Type = DbType.Text,
                    Nullable = true,
                    DefaultSql = "'new'",
                },
            ],
            CheckConstraints =
            [
                new CheckConstraintDefinition
                {
                    ConstraintName = "ck_orders_quantity",
                    ColumnName = "quantity",
                    ExpressionSql = "(quantity > 0)",
                },
                new CheckConstraintDefinition
                {
                    ConstraintName = null,
                    ColumnName = null,
                    ExpressionSql = "status IN ('new', 'paid')",
                },
            ],
            NextRowId = 12,
        };

        TableSchema decoded = SchemaSerializer.Deserialize(SchemaSerializer.Serialize(schema));

        Assert.Equal("1", decoded.Columns[1].DefaultSql);
        Assert.Equal("'new'", decoded.Columns[2].DefaultSql);
        Assert.Collection(
            decoded.CheckConstraints,
            check =>
            {
                Assert.Equal("ck_orders_quantity", check.ConstraintName);
                Assert.Equal("quantity", check.ColumnName);
                Assert.Equal("(quantity > 0)", check.ExpressionSql);
            },
            check =>
            {
                Assert.Null(check.ConstraintName);
                Assert.Null(check.ColumnName);
                Assert.Equal("status IN ('new', 'paid')", check.ExpressionSql);
            });
    }

    [Fact]
    public void Deserialize_LegacyPayloadWithoutNextRowId_DefaultsToUnknown()
    {
        byte[] legacy = BuildLegacyTableSchemaPayload(
            tableName: "legacy_users",
            columns: new[]
            {
                new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = true },
            });

        var decoded = SchemaSerializer.Deserialize(legacy);

        Assert.Equal("legacy_users", decoded.TableName);
        Assert.Equal(2, decoded.Columns.Count);
        Assert.Equal(0L, decoded.NextRowId);
        Assert.True(decoded.Columns[0].IsIdentity);
        Assert.Null(decoded.Columns[0].Collation);
        Assert.Null(decoded.Columns[1].Collation);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    public void Deserialize_PreviousVersionedTableMetadata_DefaultsNewConstraintFields(
        ulong metadataVersion)
    {
        byte[] payload = BuildVersionedTableSchemaPayload(metadataVersion);

        TableSchema decoded = SchemaSerializer.Deserialize(payload);

        Assert.Equal(42L, decoded.NextRowId);
        Assert.All(decoded.Columns, column => Assert.Null(column.DefaultSql));
        Assert.Empty(decoded.CheckConstraints);
    }

    [Fact]
    public void SerializeDeserialize_IndexSchema_RoundTripsFullTextMetadata()
    {
        var schema = new IndexSchema
        {
            IndexName = "fts_docs",
            TableName = "docs",
            Columns = ["title", "body"],
            ColumnCollations = ["NOCASE", null],
            IsUnique = false,
            Kind = IndexKind.FullText,
            State = IndexState.Building,
            OwnerIndexName = "fts_docs_owner",
            OptionsJson = "{\"lowercaseInvariant\":true}",
        };

        byte[] encoded = SchemaSerializer.SerializeIndex(schema);
        var decoded = SchemaSerializer.DeserializeIndex(encoded);

        Assert.Equal("fts_docs", decoded.IndexName);
        Assert.Equal("docs", decoded.TableName);
        Assert.Equal(["title", "body"], decoded.Columns);
        Assert.Equal(["NOCASE", null], decoded.ColumnCollations);
        Assert.False(decoded.IsUnique);
        Assert.Equal(IndexKind.FullText, decoded.Kind);
        Assert.Equal(IndexState.Building, decoded.State);
        Assert.Equal("fts_docs_owner", decoded.OwnerIndexName);
        Assert.Equal("{\"lowercaseInvariant\":true}", decoded.OptionsJson);
    }

    [Fact]
    public void Deserialize_LegacyIndexPayloadWithoutMetadata_DefaultsToSqlReady()
    {
        byte[] legacy = BuildLegacyIndexSchemaPayload(
            indexName: "idx_users_name",
            tableName: "users",
            columns: ["name"],
            isUnique: true);

        var decoded = SchemaSerializer.DeserializeIndex(legacy);

        Assert.Equal("idx_users_name", decoded.IndexName);
        Assert.Equal("users", decoded.TableName);
        Assert.Equal(["name"], decoded.Columns);
        Assert.True(decoded.IsUnique);
        Assert.Equal(IndexKind.Sql, decoded.Kind);
        Assert.Equal(IndexState.Ready, decoded.State);
        Assert.Empty(decoded.ColumnCollations);
        Assert.Null(decoded.OwnerIndexName);
        Assert.Null(decoded.OptionsJson);
    }

    private static byte[] BuildLegacyTableSchemaPayload(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (var col in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(col.Name));
            ms.Write(Encoding.UTF8.GetBytes(col.Name));
            ms.WriteByte((byte)col.Type);

            byte flags = 0;
            if (col.Nullable)
                flags |= 0x01;
            if (col.IsPrimaryKey)
                flags |= 0x02;
            ms.WriteByte(flags);
        }

        return ms.ToArray();
    }

    private static byte[] BuildLegacyIndexSchemaPayload(
        string indexName,
        string tableName,
        IReadOnlyList<string> columns,
        bool isUnique)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(indexName));
        ms.Write(Encoding.UTF8.GetBytes(indexName));
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (string column in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(column));
            ms.Write(Encoding.UTF8.GetBytes(column));
        }

        ms.WriteByte(isUnique ? (byte)1 : (byte)0);
        return ms.ToArray();
    }

    private static byte[] BuildVersionedTableSchemaPayload(ulong metadataVersion)
    {
        ColumnDefinition[] columns =
        [
            new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
            new ColumnDefinition { Name = "value", Type = DbType.Text, Nullable = true },
        ];
        using var ms = new MemoryStream();
        ms.Write(BuildLegacyTableSchemaPayload("versioned", columns));
        WriteVarint(ms, 42);
        WriteVarint(ms, metadataVersion);
        WriteVarint(ms, (ulong)columns.Length);
        for (int i = 0; i < columns.Length; i++)
            WriteVarint(ms, 0); // null collation
        if (metadataVersion >= 2)
            WriteVarint(ms, 0); // foreign-key count
        return ms.ToArray();
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int len = Varint.Write(buffer, value);
        stream.Write(buffer[..len]);
    }
}
