using System.Numerics;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CanonicalRowProjectorTests
{
    [Fact]
    public void CSharpDbTableContractMapsNativeTypesExcludesRowVersionAndPreservesKeyOrder()
    {
        TableSchema schema = NativeSchema("archived_items");

        CanonicalRowContract contract = CanonicalRowProjector.CreateCSharpDbTableContract(schema);

        Assert.True(contract.IsKeyed);
        Assert.Equal([1, 0], contract.KeyFieldOrdinals);
        Assert.Equal(
            [CanonicalType.Int64, CanonicalType.Text, CanonicalType.Binary64, CanonicalType.Blob, CanonicalType.Blob],
            contract.Fields.Select(field => field.CanonicalType));
        Assert.Equal(
            CanonicalExclusionReason.RegeneratedRowVersion,
            contract.Fields[4].ExclusionReason);

        CanonicalValue[] first = CanonicalRowProjector.ProjectRow(
            contract,
            [
                DbValue.FromInteger(7),
                DbValue.FromText("A-1"),
                DbValue.FromReal(1.5),
                DbValue.FromBlob([0x10, 0x20]),
                DbValue.FromBlob([0x01]),
            ]);
        CanonicalValue[] second = CanonicalRowProjector.ProjectRow(
            contract,
            [
                DbValue.FromInteger(7),
                DbValue.FromText("A-1"),
                DbValue.FromReal(1.5),
                DbValue.FromBlob([0x10, 0x20]),
                DbValue.FromBlob([0xff, 0xee]),
            ]);

        Assert.Equal(CanonicalFieldState.Excluded, first[4].State);
        Assert.Equal(
            CanonicalRowCodec.ComputeRowHash(first),
            CanonicalRowCodec.ComputeRowHash(second));
        Assert.Equal(
            CanonicalRowCodec.ComputeKeyHash([CanonicalValue.Text("A-1"), CanonicalValue.Int64(7)]),
            CanonicalRowCodec.ComputeKeyHash(CanonicalRowProjector.ProjectKey(contract, first)));
    }

    [Fact]
    public void CSharpDbTableContractDigestIgnoresTableIdentityButBindsRowLayout()
    {
        CanonicalRowContract archived = CanonicalRowProjector.CreateCSharpDbTableContract(
            NativeSchema("archived_items"));
        CanonicalRowContract staging = CanonicalRowProjector.CreateCSharpDbTableContract(
            NativeSchema("__csharpdb_restore_stage_v1_deadbeef"));
        TableSchema changedSchema = NativeSchema("archived_items", payloadColumnName: "content");
        CanonicalRowContract changed = CanonicalRowProjector.CreateCSharpDbTableContract(changedSchema);

        Assert.Equal(archived.ObjectContractDigest, staging.ObjectContractDigest);
        Assert.NotEqual(archived.ObjectContractDigest, changed.ObjectContractDigest);
        Assert.Equal(64, archived.ObjectContractDigest.Length);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void CSharpDbTableContractRejectsMissingTableIdentity(string? tableName)
    {
        TableSchema schema = NativeSchema(tableName!);

        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(schema));
    }

    [Fact]
    public void CSharpDbTableContractUsesLegacyPrimaryKeyFlagsWithoutLogicalConstraint()
    {
        var schema = new TableSchema
        {
            TableName = "legacy_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "tenant_id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new ColumnDefinition { Name = "value", Type = DbType.Text },
                new ColumnDefinition
                {
                    Name = "code",
                    Type = DbType.Text,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
            ],
        };

        CanonicalRowContract contract = CanonicalRowProjector.CreateCSharpDbTableContract(schema);

        Assert.Equal([0, 2], contract.KeyFieldOrdinals);
    }

    [Fact]
    public void CSharpDbTableContractRejectsNonPersistentAndInvalidRowVersionColumns()
    {
        var nullType = new TableSchema
        {
            TableName = "invalid",
            Columns = [new ColumnDefinition { Name = "value", Type = DbType.Null }],
        };
        var textRowVersion = new TableSchema
        {
            TableName = "invalid",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "version",
                    Type = DbType.Text,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
        };
        TableSchema nullableRowVersion = NativeSchema("invalid", rowVersionNullable: true);
        TableSchema identityRowVersion = NativeSchema("invalid", rowVersionIdentity: true);
        TableSchema validRowVersion = NativeSchema("invalid");
        var multipleRowVersions = new TableSchema
        {
            TableName = validRowVersion.TableName,
            Columns =
            [
                .. validRowVersion.Columns,
                new ColumnDefinition
                {
                    Name = "second_version",
                    Type = DbType.Blob,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
            KeyConstraints = validRowVersion.KeyConstraints,
        };

        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(nullType));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(textRowVersion));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(nullableRowVersion));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(identityRowVersion));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(multipleRowVersions));
    }

    [Fact]
    public void CSharpDbTableContractRejectsMalformedKeyConstraints()
    {
        TableSchema baseline = NativeSchema("invalid");
        TableSchema WithKeys(IReadOnlyList<KeyConstraintDefinition> keys) => new()
        {
            TableName = baseline.TableName,
            Columns = baseline.Columns,
            KeyConstraints = keys,
        };

        TableSchema nullCollection = WithKeys(null!);
        TableSchema nullEntry = WithKeys([null!]);
        TableSchema unknownKind = WithKeys(
        [
            new KeyConstraintDefinition
            {
                Kind = (KeyConstraintKind)int.MaxValue,
                Columns = ["code"],
            },
        ]);
        TableSchema emptyUnique = WithKeys(
        [
            new KeyConstraintDefinition
            {
                Kind = KeyConstraintKind.Unique,
                Columns = [],
            },
        ]);
        TableSchema missingUniqueColumn = WithKeys(
        [
            new KeyConstraintDefinition
            {
                Kind = KeyConstraintKind.Unique,
                Columns = ["missing"],
            },
        ]);

        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(nullCollection));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(nullEntry));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(unknownKind));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(emptyUnique));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(missingUniqueColumn));
    }

    [Fact]
    public void CSharpDbTableContractRejectsNullablePrimaryKey()
    {
        var schema = new TableSchema
        {
            TableName = "invalid",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    IsPrimaryKey = true,
                },
            ],
        };

        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.CreateCSharpDbTableContract(schema));
    }

    [Fact]
    public async Task SyntheticContractUsesLogicalTypesAndPrimaryKeyOrder()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();

        CanonicalRowContract contract = CanonicalRowProjector.CreateContract(
            plan,
            catalog,
            "syn:table:customers-upper");

        Assert.True(contract.IsKeyed);
        Assert.Single(contract.KeyFieldOrdinals);
        Assert.Equal("syn:column:customers-upper:id", contract.Fields[contract.KeyFieldOrdinals[0]].SourceColumnObjectId);
        Assert.Equal(64, contract.ObjectContractDigest.Length);
        Assert.Equal(
            [CanonicalType.Boolean, CanonicalType.Guid, CanonicalType.Int64, CanonicalType.Text, CanonicalType.Blob],
            contract.Fields.Select(field => field.CanonicalType));

        CanonicalValue[] row = CanonicalRowProjector.ProjectRow(
            contract,
            [
                DbValue.FromInteger(1),
                DbValue.FromText("00112233-4455-6677-8899-aabbccddeeff"),
                DbValue.FromInteger(42),
                DbValue.FromText("A\u030A"),
                DbValue.FromBlob([0x01, 0x02]),
            ]);
        CanonicalValue[] key = CanonicalRowProjector.ProjectKey(contract, row);

        Assert.Equal(CanonicalRowCodec.ComputeKeyHash([CanonicalValue.Int64(42)]), CanonicalRowCodec.ComputeKeyHash(key));
        Assert.Equal(CanonicalFieldState.Value, row[0].State);
        Assert.Equal(CanonicalFieldState.Value, row[4].State);
    }

    [Fact]
    public void ProjectionRestoresDecimalLogicalValueFromScaledIntegerAndText()
    {
        CanonicalFieldContract scaled = Field(
            DbType.Integer,
            CanonicalType.Decimal,
            "decimal-scaled-int64",
            new MigrationCatalogFacet { Name = "scale", Value = "2" });
        CanonicalFieldContract text = Field(DbType.Text, CanonicalType.Decimal, "decimal-text");

        CanonicalValue scaledValue = CanonicalRowProjector.ProjectValue(scaled, DbValue.FromInteger(12_345));
        CanonicalValue textValue = CanonicalRowProjector.ProjectValue(text, DbValue.FromText("123.4500"));
        string expected = CanonicalRowCodec.ComputeRowHash(
            [CanonicalValue.Decimal(new BigInteger(12_345), 2)]);

        Assert.Equal(expected, CanonicalRowCodec.ComputeRowHash([scaledValue]));
        Assert.Equal(expected, CanonicalRowCodec.ComputeRowHash([textValue]));
    }

    [Fact]
    public void ProjectionPreservesTypedNullAndRegisteredExclusion()
    {
        CanonicalValue typedNull = CanonicalRowProjector.ProjectValue(
            Field(DbType.Text, CanonicalType.Guid, "guid-text"),
            DbValue.Null);
        CanonicalValue excluded = CanonicalRowProjector.ProjectValue(
            Field(
                DbType.Blob,
                CanonicalType.Blob,
                conversionId: null,
                exclusion: CanonicalExclusionReason.RegeneratedRowVersion),
            DbValue.FromBlob([0x01, 0x02, 0x03]));

        Assert.Equal(CanonicalFieldState.Null, typedNull.State);
        Assert.Equal(CanonicalType.Guid, typedNull.Type);
        Assert.Equal(CanonicalFieldState.Excluded, excluded.State);
        Assert.Equal(CanonicalType.Blob, excluded.Type);
    }

    [Fact]
    public void ProjectionRejectsInvalidBooleanDomainAndStoredTag()
    {
        CanonicalFieldContract boolean = Field(DbType.Integer, CanonicalType.Boolean, "boolean-integer");

        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.ProjectValue(boolean, DbValue.FromInteger(2)));
        Assert.Throws<InvalidDataException>(
            () => CanonicalRowProjector.ProjectValue(boolean, DbValue.FromText("true")));
    }

    private static CanonicalFieldContract Field(
        DbType storedType,
        CanonicalType canonicalType,
        string? conversionId,
        MigrationCatalogFacet? parameter = null,
        CanonicalExclusionReason? exclusion = null) => new()
    {
        SourceColumnObjectId = "column:value",
        TargetColumnName = "value",
        StoredType = storedType,
        CanonicalType = canonicalType,
        ConversionId = conversionId,
        ConversionParameters = parameter is null ? [] : [parameter],
        ExclusionReason = exclusion,
    };

    private static TableSchema NativeSchema(
        string tableName,
        string payloadColumnName = "payload",
        bool rowVersionNullable = false,
        bool rowVersionIdentity = false) => new()
    {
        TableName = tableName,
        Columns =
        [
            new ColumnDefinition
            {
                Name = "tenant_id",
                Type = DbType.Integer,
                Nullable = false,
                IsPrimaryKey = true,
            },
            new ColumnDefinition
            {
                Name = "code",
                Type = DbType.Text,
                Nullable = false,
                IsPrimaryKey = true,
            },
            new ColumnDefinition { Name = "score", Type = DbType.Real },
            new ColumnDefinition { Name = payloadColumnName, Type = DbType.Blob },
            new ColumnDefinition
            {
                Name = "row_version",
                Type = DbType.Blob,
                Nullable = rowVersionNullable,
                IsIdentity = rowVersionIdentity,
                IsRowVersion = true,
            },
        ],
        KeyConstraints =
        [
            new KeyConstraintDefinition
            {
                ConstraintName = "pk_items",
                Kind = KeyConstraintKind.PrimaryKey,
                Columns = ["code", "tenant_id"],
                BackingIndexName = "__constraint_items_pk",
            },
        ],
    };

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> ReadyPlanAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });
        MigrationPlan planned = new MigrationPlanner().CreatePlan(catalog);
        return (catalog, planned with
        {
            AcceptedExclusionObjectIds = planned.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
        });
    }
}
