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
        Assert.True(realIndex.Included);
        Assert.Null(realIndex.ExclusionReason);
    }

    [Theory]
    [InlineData("ordered")]
    [InlineData("range")]
    public async Task Planner_RealIndexesRemainEqualityOnly(
        string unsupportedAccessFacet)
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalogObject amountIndex = source.Objects.Single(
            item => item.ObjectId == "syn:index:orders:amount");
        MigrationCatalog catalog = source with
        {
            Objects =
            [
                .. source.Objects.Select(item =>
                    item.ObjectId == amountIndex.ObjectId
                        ? item with
                        {
                            Facets =
                            [
                                Facet("kind", "standard"),
                                Facet(unsupportedAccessFacet, "true"),
                            ],
                        }
                        : item),
            ],
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                MappingProfile = MigrationMappingProfile.Queryable,
            });

        MigrationPlanObject realIndex = Object(
            plan,
            amountIndex.ObjectId);
        Assert.False(realIndex.Included);
        Assert.Contains(
            "CSDB-INDEX-001",
            realIndex.ExclusionReason,
            StringComparison.Ordinal);
        Assert.Contains(
            "only for equality access",
            realIndex.ExclusionReason,
            StringComparison.Ordinal);
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
                CatalogObject("cap:table:safe-default", MigrationObjectKind.Table, "Safe Defaults", "syn:ns:main"),
                Column(
                    "cap:column:safe-default:value",
                    "SafeValue",
                    "INT64",
                    "signedInteger",
                    "cap:table:safe-default",
                    Facet("nullable", "false"),
                    Facet("hasDefault", "true"),
                    Facet("defaultKind", "typed-literal"),
                    Facet("defaultType", "integer"),
                    Facet("defaultValue", "-7"),
                    Facet("defaultExpression", "-7")),
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
                Column(
                    "cap:column:unsafe-default:malformed",
                    "MalformedValue",
                    "INT64",
                    "signedInteger",
                    "cap:table:unsafe-default",
                    Facet("nullable", "true"),
                    Facet("hasDefault", "true"),
                    Facet("defaultKind", "typed-literal"),
                    Facet("defaultType", "integer"),
                    Facet("defaultValue", "seven"),
                    Facet("defaultExpression", "seven")),
                Column(
                    "cap:column:unsafe-default:tampered",
                    "TamperedValue",
                    "INT64",
                    "signedInteger",
                    "cap:table:unsafe-default",
                    Facet("nullable", "true"),
                    Facet("hasDefault", "true"),
                    Facet("defaultKind", "typed-literal"),
                    Facet("defaultType", "integer"),
                    Facet("defaultValue", "7"),
                    Facet(
                        "defaultExpression",
                        "7); DROP TABLE private_data; --")),
            ],
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        Assert.True(Object(plan, "cap:table:system").Included);
        Assert.True(Object(plan, "cap:column:system:id").Included);
        Assert.True(Object(plan, "cap:column:system:stamp").Included);
        Assert.True(Object(plan, "cap:key:system:pk").Included);
        Assert.True(Object(
            plan,
            "cap:column:safe-default:value").Included);

        MigrationPlanObject unsafeDefault = Object(plan, "cap:column:unsafe-default:value");
        Assert.False(unsafeDefault.Included);
        Assert.Contains("CSDB-COLUMN-DEFAULT-001", unsafeDefault.ExclusionReason, StringComparison.Ordinal);
        AssertExcludedBy(
            plan,
            "cap:column:unsafe-default:malformed",
            "CSDB-COLUMN-DEFAULT-001");
        AssertExcludedBy(
            plan,
            "cap:column:unsafe-default:tampered",
            "CSDB-COLUMN-DEFAULT-001");
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
    public async Task Planner_AdmitsPhase3ActionsAndProvesMutatingChildEligibility()
    {
        MigrationCatalog source = await InspectAsync();
        MigrationCatalogObject childColumn = source.Objects.Single(
            item => item.ObjectId == "syn:column:orders:customer-id");
        MigrationCatalogObject foreignKey = source.Objects.Single(
            item => item.ObjectId == "syn:fk:orders:customer");
        MigrationCatalog catalog = source with
        {
            Objects =
            [
                .. source.Objects.Select(item =>
                    item.ObjectId == childColumn.ObjectId
                        ? item with
                        {
                            Facets =
                            [
                                .. item.Facets.Where(facet =>
                                    !string.Equals(
                                        facet.Name,
                                        "nullable",
                                        StringComparison.Ordinal) &&
                                    !facet.Name.StartsWith(
                                        "default",
                                        StringComparison.Ordinal) &&
                                    !string.Equals(
                                        facet.Name,
                                        "hasDefault",
                                        StringComparison.Ordinal)),
                                Facet("nullable", "true"),
                                Facet("hasDefault", "true"),
                                Facet(
                                    "defaultKind",
                                    "typed-literal"),
                                Facet("defaultType", "integer"),
                                Facet("defaultValue", "1"),
                                Facet("defaultExpression", "1"),
                            ],
                        }
                        : item),
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:set-null",
                    Facets =
                    [
                        Facet("onDelete", "set-null"),
                        Facet("onUpdate", "no-action"),
                    ],
                },
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:update-set-null",
                    Facets =
                    [
                        Facet("onDelete", "restrict"),
                        Facet("onUpdate", "set-null"),
                    ],
                },
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:delete-set-default",
                    Facets =
                    [
                        Facet("onDelete", "set-default"),
                        Facet("onUpdate", "restrict"),
                    ],
                },
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:update-set-default",
                    Facets =
                    [
                        Facet("onDelete", "restrict"),
                        Facet("onUpdate", "set-default"),
                    ],
                },
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:update-cascade",
                    Facets =
                    [
                        Facet("onDelete", "restrict"),
                        Facet("onUpdate", "cascade"),
                    ],
                },
                foreignKey with
                {
                    ObjectId = "cap:fk:orders:no-action",
                    Facets =
                    [
                        Facet("onDelete", "no-action"),
                        Facet("onUpdate", "restrict"),
                    ],
                },
            ],
        };

        MigrationPlan supported = new MigrationPlanner().CreatePlan(catalog);

        Assert.True(Object(supported, "cap:fk:orders:set-null").Included);
        Assert.True(Object(
            supported,
            "cap:fk:orders:update-set-null").Included);
        Assert.True(Object(
            supported,
            "cap:fk:orders:delete-set-default").Included);
        Assert.True(Object(
            supported,
            "cap:fk:orders:update-set-default").Included);
        Assert.True(Object(
            supported,
            "cap:fk:orders:update-cascade").Included);
        Assert.True(Object(supported, "cap:fk:orders:no-action").Included);

        MigrationCatalogObject supportedChild =
            catalog.Objects.Single(
                item => item.ObjectId == childColumn.ObjectId);
        MigrationCatalog nonNullableCatalog = catalog with
        {
            Objects = catalog.Objects
                .Select(item =>
                    item.ObjectId == childColumn.ObjectId
                        ? supportedChild with
                        {
                            Facets =
                            [
                                .. supportedChild.Facets.Where(
                                    facet =>
                                        !string.Equals(
                                            facet.Name,
                                            "nullable",
                                            StringComparison.Ordinal)),
                                Facet("nullable", "false"),
                            ],
                        }
                        : item)
                .ToArray(),
        };
        MigrationPlan nonNullable = new MigrationPlanner().CreatePlan(nonNullableCatalog);

        AssertExcludedBy(
            nonNullable,
            "cap:fk:orders:set-null",
            "CSDB-FOREIGNKEY-001");
        Assert.Contains(
            "nullable=true",
            Object(nonNullable, "cap:fk:orders:set-null").ExclusionReason,
            StringComparison.Ordinal);
        Assert.True(Object(nonNullable, "cap:fk:orders:no-action").Included);

        MigrationCatalog primaryKeyCatalog = catalog with
        {
            Objects =
            [
                .. catalog.Objects,
                CatalogObject(
                    "cap:key:orders:customer-id",
                    MigrationObjectKind.Key,
                    "PK Orders Customer",
                    "syn:table:orders",
                    [Facet("kind", "primary")],
                    [childColumn.ObjectId]),
            ],
        };
        MigrationPlan primaryKey = new MigrationPlanner().CreatePlan(
            primaryKeyCatalog);

        AssertExcludedBy(
            primaryKey,
            "cap:fk:orders:set-null",
            "CSDB-FOREIGNKEY-001");
        AssertExcludedBy(
            primaryKey,
            "cap:fk:orders:update-set-null",
            "CSDB-FOREIGNKEY-001");
        Assert.Contains(
            "primary key",
            Object(primaryKey, "cap:fk:orders:set-null").ExclusionReason,
            StringComparison.OrdinalIgnoreCase);
        Assert.True(Object(primaryKey, "cap:fk:orders:no-action").Included);

        MigrationCatalog implicitNullDefaultCatalog =
            catalog with
            {
                Objects = catalog.Objects
                    .Select(item =>
                        item.ObjectId == childColumn.ObjectId
                            ? supportedChild with
                            {
                                Facets =
                                [
                                    .. supportedChild.Facets.Where(
                                        facet =>
                                            !facet.Name.StartsWith(
                                                "default",
                                                StringComparison.Ordinal) &&
                                            !string.Equals(
                                                facet.Name,
                                                "hasDefault",
                                                StringComparison.Ordinal)),
                                    Facet("hasDefault", "false"),
                                ],
                            }
                            : item)
                    .ToArray(),
            };
        MigrationPlan implicitNullDefault =
            new MigrationPlanner().CreatePlan(
                implicitNullDefaultCatalog);
        Assert.True(Object(
            implicitNullDefault,
            "cap:fk:orders:delete-set-default").Included);
        Assert.True(Object(
            implicitNullDefault,
            "cap:fk:orders:update-set-default").Included);

        MigrationCatalog missingDefaultProofCatalog =
            implicitNullDefaultCatalog with
            {
                Objects = implicitNullDefaultCatalog.Objects
                    .Select(item =>
                        item.ObjectId == childColumn.ObjectId
                            ? item with
                            {
                                Facets =
                                [
                                    .. item.Facets.Where(facet =>
                                        !string.Equals(
                                            facet.Name,
                                            "hasDefault",
                                            StringComparison.Ordinal)),
                                ],
                            }
                            : item)
                    .ToArray(),
            };
        MigrationPlan missingDefaultProof =
            new MigrationPlanner().CreatePlan(
                missingDefaultProofCatalog);
        AssertExcludedBy(
            missingDefaultProof,
            "cap:fk:orders:delete-set-default",
            "CSDB-FOREIGNKEY-001");
        Assert.Contains(
            "explicitly prove",
            Object(
                missingDefaultProof,
                "cap:fk:orders:delete-set-default")
                .ExclusionReason,
            StringComparison.OrdinalIgnoreCase);

        MigrationCatalog nullOnNonNullableCatalog =
            catalog with
            {
                Objects = catalog.Objects
                    .Select(item =>
                        item.ObjectId == childColumn.ObjectId
                            ? supportedChild with
                            {
                                Facets =
                                [
                                    .. supportedChild.Facets.Where(
                                        facet =>
                                            !string.Equals(
                                                facet.Name,
                                                "nullable",
                                                StringComparison.Ordinal) &&
                                            !facet.Name.StartsWith(
                                                "default",
                                                StringComparison.Ordinal) &&
                                            !string.Equals(
                                                facet.Name,
                                                "hasDefault",
                                                StringComparison.Ordinal)),
                                    Facet("nullable", "false"),
                                    Facet("hasDefault", "true"),
                                    Facet("defaultKind", "null"),
                                    Facet(
                                        "defaultExpression",
                                        "NULL"),
                                ],
                            }
                            : item)
                    .ToArray(),
            };
        MigrationPlan nullOnNonNullable =
            new MigrationPlanner().CreatePlan(
                nullOnNonNullableCatalog);
        AssertExcludedBy(
            nullOnNonNullable,
            "cap:fk:orders:update-set-default",
            "CSDB-FOREIGNKEY-001");
        Assert.Contains(
            "nullable",
            Object(
                nullOnNonNullable,
                "cap:fk:orders:update-set-default")
                .ExclusionReason,
            StringComparison.OrdinalIgnoreCase);
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
