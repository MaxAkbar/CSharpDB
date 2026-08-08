using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationNormalizedSchemaTests
{
    [Fact]
    public void SchemaDigestIsIndependentOfInputOrdering()
    {
        MigrationNormalizedSchemaObject table = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject column = Object(
            "column:orders:id",
            MigrationObjectKind.Column,
            "id",
            "table:orders",
            [
                new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Integer" },
                new MigrationNormalizedSchemaAttribute { Name = "nullable", Value = "false" },
            ]);

        MigrationNormalizedSchema forward = MigrationNormalizedSchemaContract.Create([table, column]);
        MigrationNormalizedSchema reverse = MigrationNormalizedSchemaContract.Create([column, table]);

        Assert.Equal(forward.Digest, reverse.Digest);
        Assert.Equal(
            ["column:orders:id", "table:orders"],
            forward.Objects.Select(item => item.ObjectId));
    }

    [Fact]
    public void CompareLocalizesMissingAndChangedDefinitions()
    {
        MigrationNormalizedSchema source = MigrationNormalizedSchemaContract.Create(
        [
            Object("table:orders", MigrationObjectKind.Table, "orders"),
            Object(
                "column:orders:id",
                MigrationObjectKind.Column,
                "id",
                "table:orders",
                [new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Integer" }]),
        ]);
        MigrationNormalizedSchema target = MigrationNormalizedSchemaContract.Create(
        [
            Object(
                "column:orders:id",
                MigrationObjectKind.Column,
                "id",
                "table:orders",
                [new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Text" }]),
        ]);

        MigrationNormalizedSchemaDifference[] differences =
            MigrationNormalizedSchemaContract.Compare(source, target).ToArray();

        Assert.Equal(2, differences.Length);
        Assert.Equal("column:orders:id", differences[0].ObjectId);
        Assert.NotEqual(differences[0].SourceDefinitionDigest, differences[0].TargetDefinitionDigest);
        Assert.Equal("table:orders", differences[1].ObjectId);
        Assert.NotNull(differences[1].SourceDefinitionDigest);
        Assert.Null(differences[1].TargetDefinitionDigest);
    }

    [Fact]
    public void CreateRejectsTamperedDefinitionDigest()
    {
        MigrationNormalizedSchemaObject valid = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject tampered = valid with
        {
            DefinitionDigest = new string('0', 64),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationNormalizedSchemaContract.Create([tampered]));

        Assert.Contains("does not match", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDuplicateObjectIdentity()
    {
        MigrationNormalizedSchemaObject first = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject second = Object("table:orders", MigrationObjectKind.Table, "orders_copy");

        Assert.Throws<InvalidDataException>(
            () => MigrationNormalizedSchemaContract.Create([first, second]));
    }

    [Fact]
    public void ExpectedSchemaRetainsThePlannedLogicalSqlType()
    {
        var source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:normalized-logical-type",
            Fingerprint = new string('1', 64),
            ProviderVersion = "fixture-v1",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable normalized-schema fixture.",
            },
        };
        var column = new MigrationCatalogObject
        {
            ObjectId = "column:flag",
            Kind = MigrationObjectKind.Column,
            SourceName = "flag",
            NativeType = "sys.bit",
            Facets =
            [
                new MigrationCatalogFacet { Name = "nullable", Value = "false" },
            ],
        };
        var mapping = new MigrationTypeMapping
        {
            SourceObjectId = column.ObjectId,
            SourceNativeType = column.NativeType!,
            TargetType = DbType.Integer,
            TargetSqlType = "BOOLEAN",
            Classification = MigrationMappingClassification.Exact,
            Profile = MigrationMappingProfile.Preserve,
            Coverage = new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.Full,
                ValuesExamined = 0,
                TotalValues = 0,
            },
        };
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = "4.5.0",
            Source = source,
            Objects = [column],
        };
        var plan = new MigrationPlan
        {
            TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
            Source = source,
            CatalogDigest = new string('2', 64),
            CapabilityDigest = new string('3', 64),
            NamingAlgorithmVersion = "fixture-v1",
            MappingPolicyId = "fixture",
            MappingPolicyVersion = StandardDataTypeMappingProvider.StandardPolicyVersion,
            MappingProfile = MigrationMappingProfile.Preserve,
            Objects =
            [
                new MigrationPlanObject
                {
                    SourceObjectId = column.ObjectId,
                    TargetName = "flag",
                    TypeMappings = [mapping],
                },
            ],
        };

        MigrationNormalizedSchema normalized =
            MigrationNormalizedSchemaContract.CreateExpected(plan, catalog);
        MigrationNormalizedSchemaObject normalizedColumn = Assert.Single(normalized.Objects);

        Assert.Contains(
            normalizedColumn.Attributes,
            attribute => attribute.Name == "targetSqlType" && attribute.Value == "BOOLEAN");
    }

    private static MigrationNormalizedSchemaObject Object(
        string objectId,
        MigrationObjectKind kind,
        string targetName,
        string? parentObjectId = null,
        IReadOnlyList<MigrationNormalizedSchemaAttribute>? attributes = null) =>
        MigrationNormalizedSchemaContract.CreateObject(
            objectId,
            kind,
            parentObjectId,
            targetName,
            attributes);
}
