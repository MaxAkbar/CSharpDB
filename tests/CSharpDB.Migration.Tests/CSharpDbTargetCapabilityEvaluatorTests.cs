using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbTargetCapabilityEvaluatorTests
{
    [Fact]
    public async Task Planner_UsesMappedDependencyTypesForKeysForeignKeysAndIndexes()
    {
        MigrationCatalog catalog = await InspectAsync();

        MigrationPlan preserve = new MigrationPlanner().CreatePlan(catalog);
        Assert.True(Object(preserve, "syn:key:orders:pk").Included);
        Assert.True(Object(preserve, "syn:fk:orders:customer").Included);
        Assert.True(Object(preserve, "syn:index:orders:amount").Included);

        MigrationPlan queryable = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { MappingProfile = MigrationMappingProfile.Queryable });
        MigrationPlanObject realIndex = Object(queryable, "syn:index:orders:amount");
        Assert.False(realIndex.Included);
        Assert.Contains("CSDB-INDEX-001", realIndex.ExclusionReason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Planner_ProvesSupportedConditionalColumnFeaturesAndRejectsUnsafeDefaults()
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalog catalog = source with
        {
            Objects =
            [
                .. source.Objects,
                CatalogObject("cap:table:system", MigrationObjectKind.Table, "System Rows", "syn:ns:main"),
                Column(
                    "cap:column:system:id",
                    "Id",
                    "INT64",
                    "signedInteger",
                    "cap:table:system",
                    Facet("nullable", "false"),
                    Facet("identity", "true")),
                Column(
                    "cap:column:system:stamp",
                    "Stamp",
                    "BLOB",
                    "binary",
                    "cap:table:system",
                    Facet("nullable", "false"),
                    Facet("rowVersion", "true")),
                CatalogObject(
                    "cap:key:system:pk",
                    MigrationObjectKind.Key,
                    "PK System Rows",
                    "cap:table:system",
                    [Facet("kind", "primary")],
                    ["cap:column:system:id"]),
                CatalogObject("cap:table:unsafe-default", MigrationObjectKind.Table, "Unsafe Defaults", "syn:ns:main"),
                Column(
                    "cap:column:unsafe-default:value",
                    "Value",
                    "TEXT",
                    "text",
                    "cap:table:unsafe-default",
                    Facet("nullable", "true"),
                    Facet("defaultKind", "expression"),
                    Facet("defaultExpression", "current_user()")),
            ],
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        Assert.True(Object(plan, "cap:table:system").Included);
        Assert.True(Object(plan, "cap:column:system:id").Included);
        Assert.True(Object(plan, "cap:column:system:stamp").Included);
        Assert.True(Object(plan, "cap:key:system:pk").Included);

        MigrationPlanObject unsafeDefault = Object(plan, "cap:column:unsafe-default:value");
        Assert.False(unsafeDefault.Included);
        Assert.Contains("CSDB-COLUMN-DEFAULT-001", unsafeDefault.ExclusionReason, StringComparison.Ordinal);
        Assert.False(Object(plan, "cap:table:unsafe-default").Included);
    }

    [Fact]
    public async Task Planner_ConservativelyExcludesUnprovenOrIncompatibleConditionalConstraints()
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalog catalog = source with
        {
            Objects =
            [
                .. source.Objects,
                CatalogObject(
                    "cap:key:customers-lower:unique-code",
                    MigrationObjectKind.Key,
                    "UQ Customer Codes",
                    "syn:table:customers-lower",
                    [Facet("kind", "unique")],
                    ["syn:column:customers-lower:code-upper", "syn:column:customers-lower:code-lower"]),
                CatalogObject(
                    "cap:key:customers-upper:blob",
                    MigrationObjectKind.Key,
                    "PK Customer Payload",
                    "syn:table:customers-upper",
                    [Facet("kind", "primary")],
                    ["syn:column:customers-upper:payload"]),
                CatalogObject(
                    "cap:check:customers-upper:proven",
                    MigrationObjectKind.CheckConstraint,
                    "CK Customer Id",
                    "syn:table:customers-upper",
                    [Facet("deterministic", "true"), Facet("rowLocal", "true")],
                    ["syn:column:customers-upper:id"]),
                CatalogObject(
                    "cap:check:customers-upper:unproven",
                    MigrationObjectKind.CheckConstraint,
                    "CK Customer Name",
                    "syn:table:customers-upper",
                    dependsOn: ["syn:column:customers-upper:name"]),
                CatalogObject(
                    "cap:index:customers-upper:blob",
                    MigrationObjectKind.Index,
                    "IX Customer Payload",
                    "syn:table:customers-upper",
                    dependsOn: ["syn:column:customers-upper:payload"]),
                CatalogObject(
                    "cap:fk:customers-upper:mismatched",
                    MigrationObjectKind.ForeignKey,
                    "FK Customer External Id",
                    "syn:table:customers-upper",
                    dependsOn:
                    [
                        "syn:column:customers-upper:external-id",
                        "syn:key:customers-upper:pk",
                    ]),
            ],
        };

        var planner = new MigrationPlanner();
        MigrationPlan first = planner.CreatePlan(catalog);
        MigrationPlan second = planner.CreatePlan(catalog);

        Assert.Equal(
            MigrationArtifactSerializer.SerializePlan(first, catalog),
            MigrationArtifactSerializer.SerializePlan(second, catalog));
        Assert.True(Object(first, "cap:key:customers-lower:unique-code").Included);
        Assert.True(Object(first, "cap:check:customers-upper:proven").Included);

        AssertExcludedBy(first, "cap:key:customers-upper:blob", "CSDB-KEY-PRIMARY-001");
        AssertExcludedBy(first, "cap:check:customers-upper:unproven", "CSDB-CHECK-001");
        AssertExcludedBy(first, "cap:index:customers-upper:blob", "CSDB-INDEX-001");
        AssertExcludedBy(first, "cap:fk:customers-upper:mismatched", "CSDB-FOREIGNKEY-001");
    }

    [Fact]
    public async Task Planner_ExcludesInventoriedTablesWithoutRetainedData()
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalogObject table = source.Objects.Single(
            item => item.ObjectId == "syn:table:customers-upper");
        MigrationCatalog catalog = source with
        {
            Objects = source.Objects
                .Select(item => item.ObjectId == table.ObjectId
                    ? item with
                    {
                        Facets =
                        [
                            .. item.Facets,
                            Facet(
                                MigrationDataAvailabilityContract
                                    .AvailableFacet,
                                "false"),
                            Facet(
                                MigrationDataAvailabilityContract
                                    .UnavailableReasonFacet,
                                "no deterministic source key"),
                        ],
                    }
                    : item)
                .ToArray(),
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        MigrationPlanObject excluded = Object(plan, table.ObjectId);
        Assert.False(excluded.Included);
        Assert.Contains(
            "no deterministic source key",
            excluded.ExclusionReason,
            StringComparison.Ordinal);
        Assert.All(
            plan.Objects.Where(item =>
                source.Objects.Any(sourceObject =>
                    sourceObject.ParentObjectId == table.ObjectId &&
                    sourceObject.ObjectId == item.SourceObjectId)),
            item => Assert.False(item.Included));
    }

    [Fact]
    public async Task Planner_RejectsMalformedRetainedDataAvailability()
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalogObject table = source.Objects.First(
            item => item.Kind == MigrationObjectKind.Table);
        MigrationCatalog catalog = source with
        {
            Objects = source.Objects
                .Select(item => item.ObjectId == table.ObjectId
                    ? item with
                    {
                        Facets =
                        [
                            .. item.Facets,
                            Facet(
                                MigrationDataAvailabilityContract
                                    .AvailableFacet,
                                "sometimes"),
                        ],
                    }
                    : item)
                .ToArray(),
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        Assert.False(Object(plan, table.ObjectId).Included);
        Assert.Contains(
            "availability",
            Object(plan, table.ObjectId).ExclusionReason,
            StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);

    private static MigrationPlanObject Object(MigrationPlan plan, string objectId) =>
        plan.Objects.Single(item => item.SourceObjectId == objectId);

    private static void AssertExcludedBy(MigrationPlan plan, string objectId, string ruleId)
    {
        MigrationPlanObject item = Object(plan, objectId);
        Assert.False(item.Included);
        Assert.Contains(ruleId, item.ExclusionReason, StringComparison.Ordinal);
    }

    private static MigrationCatalogObject CatalogObject(
        string objectId,
        MigrationObjectKind kind,
        string name,
        string? parentObjectId = null,
        IReadOnlyList<MigrationCatalogFacet>? facets = null,
        IReadOnlyList<string>? dependsOn = null) => new()
    {
        ObjectId = objectId,
        Kind = kind,
        ParentObjectId = parentObjectId,
        SourceName = name,
        Facets = facets ?? [],
        DependsOn = dependsOn ?? [],
    };

    private static MigrationCatalogObject Column(
        string objectId,
        string name,
        string nativeType,
        string logicalType,
        string parentObjectId,
        params MigrationCatalogFacet[] facets) => new()
    {
        ObjectId = objectId,
        Kind = MigrationObjectKind.Column,
        ParentObjectId = parentObjectId,
        SourceName = name,
        NativeType = nativeType,
        Facets = [Facet("logicalType", logicalType), .. facets],
    };

    private static MigrationCatalogFacet Facet(string name, string? value) => new()
    {
        Name = name,
        Value = value,
    };
}
