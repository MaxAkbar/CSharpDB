using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class DeterministicMigrationNameMapperTests
{
    [Fact]
    public void Map_TreatsIndexNamesAsGlobalAcrossTables()
    {
        MigrationCatalog catalog = CreateCatalog(
            Object("table:left", MigrationObjectKind.Table, "Left"),
            Object("table:right", MigrationObjectKind.Table, "Right"),
            Object("index:left:value", MigrationObjectKind.Index, "IX_Value", "table:left"),
            Object("index:right:value", MigrationObjectKind.Index, "ix_value", "table:right"));

        IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(catalog);

        Assert.False(string.Equals(
            names["index:left:value"],
            names["index:right:value"],
            StringComparison.OrdinalIgnoreCase));
        Assert.Matches("^IX_Value__[0-9a-f]{16}$", names["index:left:value"]);
        Assert.Matches("^ix_value__[0-9a-f]{16}$", names["index:right:value"]);
    }

    [Fact]
    public void Map_TreatsTablesAndViewsAsOneGlobalRelationNamespace()
    {
        MigrationCatalog catalog = CreateCatalog(
            new MigrationCatalogObject
            {
                ObjectId = "namespace:main",
                Kind = MigrationObjectKind.Namespace,
                SourceName = "main",
                Facets = [new MigrationCatalogFacet { Name = "isDefault", Value = "true" }],
            },
            Object("table:report", MigrationObjectKind.Table, "Report", "namespace:main"),
            Object("view:report", MigrationObjectKind.View, "report", "namespace:main"));

        IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(catalog);

        Assert.False(string.Equals(
            names["table:report"],
            names["view:report"],
            StringComparison.OrdinalIgnoreCase));
        Assert.Matches("^Report__[0-9a-f]{16}$", names["table:report"]);
        Assert.Matches("^report__[0-9a-f]{16}$", names["view:report"]);
    }

    [Fact]
    public void Map_ResolvesCollisionsIntroducedByReservedNameFinalization()
    {
        MigrationCatalogObject reserved = Object(
            "table:reserved",
            MigrationObjectKind.Table,
            "sys_tables");
        string firstFinalName = DeterministicMigrationNameMapper.Map(CreateCatalog(reserved))[reserved.ObjectId];
        MigrationCatalogObject craftedCollision = Object(
            "table:crafted",
            MigrationObjectKind.Table,
            firstFinalName);

        IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(
            CreateCatalog(reserved, craftedCollision));

        Assert.False(string.Equals(
            names[reserved.ObjectId],
            names[craftedCollision.ObjectId],
            StringComparison.OrdinalIgnoreCase));
        Assert.NotEqual(firstFinalName, names[craftedCollision.ObjectId]);
        Assert.All(names.Values, name => Assert.True(name.Length <= CSharpDB.Primitives.SqlIdentifierRules.MaxLength));
    }

    private static MigrationCatalog CreateCatalog(params MigrationCatalogObject[] objects) => new()
    {
        TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "fixture:naming-v1",
            Fingerprint = "sha256:5f2091f29b02b5c9c788abe498515214f69330c15fbdcf7f9820b3b77a2e2909",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Versioned immutable naming fixture.",
            },
        },
        Objects = objects,
    };

    private static MigrationCatalogObject Object(
        string objectId,
        MigrationObjectKind kind,
        string sourceName,
        string? parentObjectId = null) => new()
    {
        ObjectId = objectId,
        Kind = kind,
        ParentObjectId = parentObjectId,
        SourceName = sourceName,
    };
}
