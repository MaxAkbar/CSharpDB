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
    public void LegacyIdentityDerivation_FollowsOrdinalIgnoreCaseWithoutInvariantOverFolding()
    {
        Assert.Equal(
            SchemaIdentity.ForLegacyTable("Orders"),
            SchemaIdentity.ForLegacyTable("orders"));
        Assert.Equal(
            SchemaIdentity.ForLegacyTable("\u00E4"),
            SchemaIdentity.ForLegacyTable("\u00C4"));

        const string LongS = "\u017F";
        Assert.False(
            string.Equals(
                "s",
                LongS,
                StringComparison.OrdinalIgnoreCase));
        Assert.Equal("S", LongS.ToUpperInvariant());
        Assert.NotEqual(
            SchemaIdentity.ForLegacyTable("s"),
            SchemaIdentity.ForLegacyTable(LongS));

        Guid tableId = SchemaIdentity.ForLegacyTable("identity_owner");
        Assert.NotEqual(
            SchemaIdentity.ForLegacyColumn(tableId, "s", 0),
            SchemaIdentity.ForLegacyColumn(tableId, LongS, 0));
        Assert.NotEqual(
            SchemaIdentity.ForLegacyConstraint(tableId, "check", "s", 0),
            SchemaIdentity.ForLegacyConstraint(tableId, "check", LongS, 0));
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
                    OnUpdate = ForeignKeyOnDeleteAction.SetNull,
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
        Assert.Equal(ForeignKeyOnDeleteAction.SetNull, foreignKey.OnUpdate);
        Assert.Equal("__fk_children_parent_id_a1b2", foreignKey.SupportingIndexName);
    }

    [Fact]
    public void Deserialize_PreVersionNineForeignKey_DefaultsOnUpdateToRestrict()
    {
        byte[] payload = BuildVersionTwoForeignKeyPayload(
            (ulong)ForeignKeyOnDeleteAction.Cascade);

        ForeignKeyDefinition foreignKey = Assert.Single(
            SchemaSerializer.Deserialize(payload).ForeignKeys);

        Assert.Equal(
            ForeignKeyOnDeleteAction.Cascade,
            foreignKey.OnDelete);
        Assert.Equal(
            ForeignKeyOnDeleteAction.Restrict,
            foreignKey.OnUpdate);
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
    [InlineData(3UL)]
    [InlineData(4UL)]
    [InlineData(5UL)]
    [InlineData(6UL)]
    [InlineData(7UL)]
    [InlineData(8UL)]
    [InlineData(9UL)]
    public void Deserialize_PreviousVersionedTableMetadata_DefaultsNewConstraintFields(
        ulong metadataVersion)
    {
        byte[] payload = BuildVersionedTableSchemaPayload(metadataVersion);

        TableSchema decoded = SchemaSerializer.Deserialize(payload);

        Assert.Equal(42L, decoded.NextRowId);
        Assert.All(decoded.Columns, column => Assert.Null(column.DefaultSql));
        Assert.Empty(decoded.CheckConstraints);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [InlineData(4UL)]
    [InlineData(5UL)]
    [InlineData(6UL)]
    [InlineData(7UL)]
    [InlineData(8UL)]
    [InlineData(9UL)]
    public void Deserialize_DeclaredMetadataVersion_RequiresCompletePayload(
        ulong metadataVersion)
    {
        byte[] payload = BuildVersionedTableSchemaPayload(metadataVersion);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload.AsSpan(0, payload.Length - 1)));

        Assert.Contains(
            "trunc",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [InlineData(4UL)]
    [InlineData(5UL)]
    [InlineData(6UL)]
    [InlineData(7UL)]
    [InlineData(8UL)]
    [InlineData(9UL)]
    public void Deserialize_VersionedMetadata_RejectsTrailingBytes(
        ulong metadataVersion)
    {
        byte[] payload = BuildVersionedTableSchemaPayload(metadataVersion);
        byte[] withTrailingByte = [.. payload, 0x7F];

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(withTrailingByte));

        Assert.Contains("trailing", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_ExplicitlyRejectsUnknownVersionAfterVersionNine()
    {
        byte[] payload = BuildVersionedTableSchemaPayload(10);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains("version '10'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Serialize_StableForeignKeyBindings_RejectsMismatchedOrderedArity()
    {
        TableSchema schema = BuildCompositeForeignKeySchema(
            childBindingIds: [Guid.NewGuid()]);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains("arity '2'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_StableForeignKeyBindings_RejectsMismatchedOrderedArity()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [Guid.NewGuid(), Guid.NewGuid()]));
        int childBindingCountOffset = GetCompositeChildBindingCountOffset(payload);
        byte[] corrupt = ReplaceSingleByte(
            payload,
            childBindingCountOffset,
            [1]);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(corrupt));

        Assert.Contains("arity '2'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Serialize_StableForeignKeyBindings_RejectsNullChildIdentityList()
    {
        TableSchema schema = BuildCompositeForeignKeySchema(
            childBindingIds: null!);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains("cannot be null", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_StableForeignKeyBindings_RejectsNullReferencedIdentityList()
    {
        TableSchema schema = BuildCompositeForeignKeySchema(
            childBindingIds: [Guid.NewGuid(), Guid.NewGuid()],
            nullReferencedBindings: true);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains("cannot be null", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_StableForeignKeyBindings_BoundsOversizedCountBeforeAllocation()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [Guid.NewGuid(), Guid.NewGuid()]));
        int childBindingCountOffset = GetCompositeChildBindingCountOffset(payload);
        byte[] oversizedCount = EncodeVarint(ulong.MaxValue);
        byte[] corrupt = ReplaceSingleByte(
            payload,
            childBindingCountOffset,
            oversizedCount);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(corrupt));

        Assert.Contains(
            "exceeds the supported maximum",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_BoundsBaseColumnCountBeforeAllocation()
    {
        using var payload = new MemoryStream();
        WriteVarint(payload, 1);
        payload.WriteByte((byte)'t');
        WriteVarint(payload, int.MaxValue);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload.ToArray()));

        Assert.Contains(
            "exceeds the supported maximum",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_BoundsBaseColumnCountBeforeWriting()
    {
        const int MaximumCollectionCount = 65_536;
        var column = new ColumnDefinition
        {
            Name = "id",
            Type = DbType.Integer,
        };
        var schema = new TableSchema
        {
            TableName = "oversized",
            Columns = Enumerable.Repeat(
                column,
                MaximumCollectionCount + 1).ToArray(),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains(
            "exceeds the supported maximum",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_BoundsNestedKeyColumnCountBeforeWriting()
    {
        const int MaximumCollectionCount = 65_536;
        var schema = new TableSchema
        {
            TableName = "oversized_key",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    Kind = KeyConstraintKind.Unique,
                    Columns = Enumerable.Repeat(
                        "id",
                        MaximumCollectionCount + 1).ToArray(),
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains(
            "key constraint column count",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_RejectsPayloadLargerThanDeserializeLimit()
    {
        const int MaximumPayloadBytes = 64 * 1024 * 1024;
        string oversizedTableName = new(
            '\u0800',
            (MaximumPayloadBytes / 3) + 1);
        var schema = new TableSchema
        {
            TableName = oversizedTableName,
            Columns = Array.Empty<ColumnDefinition>(),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains(
            "payload length",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "exceeds the supported maximum",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_RejectsUndefinedColumnType()
    {
        var schema = new TableSchema
        {
            TableName = "invalid_column_type",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "value",
                    Type = (DbType)byte.MaxValue,
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains("column type", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_RejectsUndefinedForeignKeyOnDeleteAction()
    {
        var schema = new TableSchema
        {
            TableName = "invalid_foreign_key",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_invalid",
                    ColumnName = "parent_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "id",
                    OnDelete = (ForeignKeyOnDeleteAction)5,
                    SupportingIndexName = "ix_invalid",
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains("ON DELETE", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_RejectsUndefinedForeignKeyOnUpdateAction()
    {
        var schema = new TableSchema
        {
            TableName = "invalid_foreign_key",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "parent_id",
                    Type = DbType.Integer,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    ConstraintName = "fk_invalid",
                    ColumnName = "parent_id",
                    ReferencedTableName = "parents",
                    ReferencedColumnName = "id",
                    OnUpdate = (ForeignKeyOnDeleteAction)5,
                    SupportingIndexName = "ix_invalid",
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains(
            "ON UPDATE",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Serialize_RejectsUndefinedKeyConstraintKind()
    {
        var schema = new TableSchema
        {
            TableName = "invalid_key",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    Kind = (KeyConstraintKind)2,
                    Columns = ["id"],
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));

        Assert.Contains(
            "key constraint kind",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_RejectsUndefinedColumnType()
    {
        byte[] payload = BuildLegacyTableSchemaPayload(
            "invalid_column_type",
            [
                new ColumnDefinition
                {
                    Name = "value",
                    Type = (DbType)byte.MaxValue,
                },
            ]);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains("column type", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [InlineData(4UL)]
    [InlineData(5UL)]
    [InlineData(4_294_967_296UL)]
    public void Deserialize_PreVersionNineRejectsExpandedOrUndefinedForeignKeyOnDeleteAction(
        ulong rawValue)
    {
        byte[] payload =
            BuildVersionTwoForeignKeyPayload(rawValue);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains("ON DELETE", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(5UL)]
    [InlineData(4_294_967_296UL)]
    public void Deserialize_RejectsUndefinedForeignKeyOnUpdateAction(
        ulong rawValue)
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [Guid.NewGuid(), Guid.NewGuid()]));
        byte[] corrupt = ReplaceSingleByte(
            payload,
            payload.Length - 1,
            EncodeVarint(rawValue));

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(corrupt));

        Assert.Contains(
            "ON UPDATE",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(2UL)]
    [InlineData(4_294_967_296UL)]
    public void Deserialize_RejectsUndefinedKeyConstraintKind(
        ulong rawValue)
    {
        byte[] payload =
            BuildVersionFourKeyConstraintPayload(rawValue);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains(
            "key constraint kind",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_StableForeignKeyBindings_RejectsTruncatedGuidList()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [Guid.NewGuid(), Guid.NewGuid()]));
        int childBindingCountOffset = GetCompositeChildBindingCountOffset(payload);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(
                payload.AsSpan(0, childBindingCountOffset + 1)));

        Assert.Contains("truncated", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_VersionEightLegacyUnresolvedBinding_NormalizesToAbsent()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [],
                bindChildColumnsBySchemaIdentity: true));
        int childBindingCountOffset =
            GetCompositeChildBindingCountOffset(payload);
        const int GuidLength = 16;
        const int CompositeArity = 2;
        int referencedTableIdOffset =
            childBindingCountOffset - GuidLength;
        int referencedBindingIdsOffset =
            childBindingCountOffset +
            1 +
            (CompositeArity * GuidLength) +
            1;
        int referencedKeyIdOffset =
            referencedBindingIdsOffset +
            (CompositeArity * GuidLength);
        Array.Clear(payload, referencedTableIdOffset, GuidLength);
        Array.Clear(
            payload,
            referencedBindingIdsOffset,
            CompositeArity * GuidLength);
        Array.Clear(payload, referencedKeyIdOffset, GuidLength);

        TableSchema decoded = SchemaSerializer.Deserialize(payload);
        ForeignKeyDefinition foreignKey = Assert.Single(
            decoded.ForeignKeys);

        Assert.Equal(Guid.Empty, foreignKey.ReferencedTableSchemaId);
        Assert.Empty(foreignKey.ColumnSchemaIds);
        Assert.Empty(foreignKey.ReferencedColumnSchemaIds);
        Assert.Equal(Guid.Empty, foreignKey.ReferencedKeySchemaId);
        _ = SchemaSerializer.Serialize(decoded);
    }

    [Fact]
    public void Deserialize_VersionEightLegacyUnresolvedBinding_RejectsWrongChildIdentity()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [],
                bindChildColumnsBySchemaIdentity: true));
        int childBindingCountOffset =
            GetCompositeChildBindingCountOffset(payload);
        const int GuidLength = 16;
        const int CompositeArity = 2;
        int referencedTableIdOffset =
            childBindingCountOffset - GuidLength;
        int referencedBindingIdsOffset =
            childBindingCountOffset +
            1 +
            (CompositeArity * GuidLength) +
            1;
        int referencedKeyIdOffset =
            referencedBindingIdsOffset +
            (CompositeArity * GuidLength);
        Array.Clear(payload, referencedTableIdOffset, GuidLength);
        Array.Clear(
            payload,
            referencedBindingIdsOffset,
            CompositeArity * GuidLength);
        Array.Clear(payload, referencedKeyIdOffset, GuidLength);
        payload[childBindingCountOffset + 1] ^= 0xFF;

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains(
            "do not match its named columns",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_VersionEightLegacyUnresolvedBinding_RejectsZeroChildIdentities()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [],
                bindChildColumnsBySchemaIdentity: true));
        int childBindingCountOffset =
            GetCompositeChildBindingCountOffset(payload);
        const int GuidLength = 16;
        const int CompositeArity = 2;
        int referencedTableIdOffset =
            childBindingCountOffset - GuidLength;
        int childBindingIdsOffset = childBindingCountOffset + 1;
        int referencedBindingIdsOffset =
            childBindingIdsOffset +
            (CompositeArity * GuidLength) +
            1;
        int referencedKeyIdOffset =
            referencedBindingIdsOffset +
            (CompositeArity * GuidLength);
        Array.Clear(payload, referencedTableIdOffset, GuidLength);
        Array.Clear(
            payload,
            childBindingIdsOffset,
            CompositeArity * GuidLength);
        Array.Clear(
            payload,
            referencedBindingIdsOffset,
            CompositeArity * GuidLength);
        Array.Clear(payload, referencedKeyIdOffset, GuidLength);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains(
            "do not match its named columns",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_StableForeignKeyBindings_RejectsMissingTargetIdentity()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [],
                bindChildColumnsBySchemaIdentity: true));
        int childBindingCountOffset =
            GetCompositeChildBindingCountOffset(payload);
        const int GuidLength = 16;
        int referencedTableIdOffset =
            childBindingCountOffset - GuidLength;
        Array.Clear(payload, referencedTableIdOffset, GuidLength);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains(
            "either complete or absent",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_StableForeignKeyBindings_RejectsZeroColumnIdentity()
    {
        byte[] payload = SchemaSerializer.Serialize(
            BuildCompositeForeignKeySchema(
                childBindingIds: [],
                bindChildColumnsBySchemaIdentity: true));
        int childBindingCountOffset =
            GetCompositeChildBindingCountOffset(payload);
        const int GuidLength = 16;
        int firstChildBindingOffset = childBindingCountOffset + 1;
        Array.Clear(payload, firstChildBindingOffset, GuidLength);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Deserialize(payload));

        Assert.Contains(
            "cannot be empty",
            error.Message,
            StringComparison.Ordinal);
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
        if (metadataVersion >= 3)
        {
            WriteVarint(ms, (ulong)columns.Length); // default column count
            for (int i = 0; i < columns.Length; i++)
                WriteVarint(ms, 0); // null default
            WriteVarint(ms, 0); // check-constraint count
        }
        if (metadataVersion >= 4)
            WriteVarint(ms, 0); // key-constraint count
        if (metadataVersion >= 5)
            WriteVarint(ms, 0); // ordered foreign-key count
        if (metadataVersion >= 7)
        {
            WriteGuid(ms, new Guid("01010101-0101-0101-0101-010101010101"));
            WriteVarint(ms, (ulong)columns.Length);
            WriteGuid(ms, new Guid("02020202-0202-0202-0202-020202020202"));
            WriteGuid(ms, new Guid("03030303-0303-0303-0303-030303030303"));
            WriteVarint(ms, 0); // foreign-key identity count
            WriteVarint(ms, 0); // check-constraint identity count
            WriteVarint(ms, 0); // key-constraint identity count
        }
        if (metadataVersion >= 8)
            WriteVarint(ms, 0); // stable foreign-key binding count
        if (metadataVersion >= 9)
            WriteVarint(ms, 0); // foreign-key ON UPDATE action count
        return ms.ToArray();
    }

    private static byte[] BuildVersionTwoForeignKeyPayload(
        ulong onDeleteRaw)
    {
        ColumnDefinition[] columns =
        [
            new ColumnDefinition
            {
                Name = "parent_id",
                Type = DbType.Integer,
            },
        ];
        using var ms = new MemoryStream();
        ms.Write(BuildLegacyTableSchemaPayload("children", columns));
        WriteVarint(ms, 0); // unknown next row identity
        WriteVarint(ms, 2); // metadata version
        WriteVarint(ms, 1); // metadata column count
        WriteVarint(ms, 0); // null collation
        WriteVarint(ms, 1); // foreign-key count
        WriteString(ms, "fk_children_parent");
        WriteString(ms, "parent_id");
        WriteString(ms, "parents");
        WriteString(ms, "id");
        WriteVarint(ms, onDeleteRaw);
        WriteString(ms, "ix_children_parent");
        return ms.ToArray();
    }

    private static byte[] BuildVersionFourKeyConstraintPayload(
        ulong kindRaw)
    {
        ColumnDefinition[] columns =
        [
            new ColumnDefinition
            {
                Name = "id",
                Type = DbType.Integer,
            },
        ];
        using var ms = new MemoryStream();
        ms.Write(BuildLegacyTableSchemaPayload("keyed", columns));
        WriteVarint(ms, 0); // unknown next row identity
        WriteVarint(ms, 4); // metadata version
        WriteVarint(ms, 1); // metadata column count
        WriteVarint(ms, 0); // null collation
        WriteVarint(ms, 0); // foreign-key count
        WriteVarint(ms, 1); // default column count
        WriteVarint(ms, 0); // null default
        WriteVarint(ms, 0); // check-constraint count
        WriteVarint(ms, 1); // key-constraint count
        WriteVarint(ms, 0); // null constraint name
        WriteVarint(ms, kindRaw);
        WriteVarint(ms, 1); // key column count
        WriteString(ms, "id");
        WriteVarint(ms, 0); // null backing index name
        return ms.ToArray();
    }

    private static TableSchema BuildCompositeForeignKeySchema(
        IReadOnlyList<Guid> childBindingIds,
        bool nullReferencedBindings = false,
        bool bindChildColumnsBySchemaIdentity = false)
    {
        Guid firstColumnId = Guid.NewGuid();
        Guid secondColumnId = Guid.NewGuid();
        return new TableSchema
        {
            SchemaId = Guid.NewGuid(),
            TableName = "composite_children",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = firstColumnId,
                    Name = "tenant_id",
                    Type = DbType.Integer,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    SchemaId = secondColumnId,
                    Name = "parent_id",
                    Type = DbType.Integer,
                    Nullable = false,
                },
            ],
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ConstraintName = "fk_composite_children_parent",
                    ColumnName = "tenant_id",
                    ColumnNames = ["tenant_id", "parent_id"],
                    ColumnSchemaIds = bindChildColumnsBySchemaIdentity
                        ? [firstColumnId, secondColumnId]
                        : childBindingIds,
                    ReferencedTableName = "composite_parents",
                    ReferencedTableSchemaId = Guid.NewGuid(),
                    ReferencedColumnName = "tenant_id",
                    ReferencedColumnNames = ["tenant_id", "id"],
                    ReferencedColumnSchemaIds = nullReferencedBindings
                        ? null!
                        : [Guid.NewGuid(), Guid.NewGuid()],
                    ReferencedKeySchemaId = Guid.NewGuid(),
                    SupportingIndexName = "__fk_composite_children_parent",
                },
            ],
        };
    }

    private static int GetCompositeChildBindingCountOffset(byte[] payload)
    {
        const int GuidLength = 16;
        const int OneByteCountLength = 1;
        const int CompositeArity = 2;
        const int ForeignKeyUpdateActionSectionLength = 2;
        int bindingSectionLength =
            OneByteCountLength +
            GuidLength +
            OneByteCountLength +
            (CompositeArity * GuidLength) +
            OneByteCountLength +
            (CompositeArity * GuidLength) +
            GuidLength;
        int bindingSectionOffset =
            payload.Length -
            ForeignKeyUpdateActionSectionLength -
            bindingSectionLength;
        return bindingSectionOffset + OneByteCountLength + GuidLength;
    }

    private static byte[] ReplaceSingleByte(
        byte[] source,
        int offset,
        byte[] replacement)
    {
        var result = new byte[source.Length - 1 + replacement.Length];
        source.AsSpan(0, offset).CopyTo(result);
        replacement.CopyTo(result, offset);
        source.AsSpan(offset + 1).CopyTo(result.AsSpan(offset + replacement.Length));
        return result;
    }

    private static byte[] EncodeVarint(ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int length = Varint.Write(buffer, value);
        return buffer[..length].ToArray();
    }

    private static void WriteGuid(Stream stream, Guid value) =>
        stream.Write(value.ToByteArray());

    private static void WriteString(Stream stream, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(stream, (ulong)bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int len = Varint.Write(buffer, value);
        stream.Write(buffer[..len]);
    }
}
