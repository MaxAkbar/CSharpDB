using System.Numerics;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CanonicalRowProjectorTests
{
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
