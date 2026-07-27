using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationDocumentCollectionContractTests
{
    [Fact]
    public void Contract_FormatsOrdinalKeysAndBindsOnlyTheExactV1Shape()
    {
        MigrationCatalog catalog = CreateCatalog();
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        MigrationCatalogObject collection = objectsById[CollectionId];

        Assert.Equal(
            "json-ordinal-v1:00000000000000000000",
            MigrationDocumentCollectionContract.FormatOrdinalKey(0));
        Assert.Equal(
            "json-ordinal-v1:00000000000000000042",
            MigrationDocumentCollectionContract.FormatOrdinalKey(42));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => MigrationDocumentCollectionContract.FormatOrdinalKey(-1));

        Assert.True(
            MigrationDocumentCollectionContract.TryBindExactV1Collection(
                collection,
                objectsById,
                out MigrationCatalogObject? keyColumn,
                out MigrationCatalogObject? documentColumn,
                out string? reason),
            reason);
        Assert.Equal(KeyColumnId, keyColumn!.ObjectId);
        Assert.Equal(DocumentColumnId, documentColumn!.ObjectId);
    }

    [Fact]
    public void Planner_AdmitsExactV1CollectionWithCanonicalDocumentMappingForFailFast()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        Assert.True(PlanObject(plan, CollectionId).Included);
        Assert.True(PlanObject(plan, KeyColumnId).Included);
        MigrationPlanObject document = PlanObject(plan, DocumentColumnId);
        Assert.True(document.Included);

        MigrationTypeMapping mapping = Assert.Single(document.TypeMappings);
        Assert.Equal(DbType.Text, mapping.TargetType);
        Assert.Equal(MigrationMappingClassification.LosslessReencoded, mapping.Classification);
        Assert.Equal("canonical-text", mapping.Conversion!.ConversionId);
        Assert.Equal(1, mapping.Conversion.Version);
        Assert.Equal(
            MigrationDocumentCollectionContract.JsonLogicalType,
            Assert.Single(mapping.Conversion.Parameters).Value);

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);
        Assert.True(PlanObject(restored, CollectionId).Included);
        Assert.Equal(MigrationRejectMode.FailFast, restored.Load.RejectMode);
    }

    [Fact]
    public void MappingPolicy_LeavesGenericJsonUnsupportedOutsideTheVersionedDocumentMarker()
    {
        var genericJson = new MigrationCatalogObject
        {
            ObjectId = "generic:json",
            Kind = MigrationObjectKind.Column,
            SourceName = "payload",
            NativeType = "JSON_CANONICAL",
            Facets =
            [
                Facet(
                    MigrationDocumentCollectionContract.LogicalTypeFacet,
                    MigrationDocumentCollectionContract.JsonLogicalType),
            ],
        };

        MigrationTypeMappingDecision decision = new StandardDataTypeMappingProvider().Map(
            new MigrationTypeMappingRequest
            {
                SourceObject = genericJson,
                Profile = MigrationMappingProfile.Preserve,
                Coverage = new MigrationProfileCoverage
                {
                    Kind = MigrationCoverageKind.None,
                    RequiresFullStreamValidation = true,
                },
            });

        Assert.Equal(MigrationMappingClassification.Unsupported, decision.Mapping.Classification);
    }

    [Fact]
    public void Planner_AndArtifactValidationRejectCollectionInDeterministicRejectMode()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationLoadPolicy load = DeterministicRejectLoad();
        var planner = new MigrationPlanner();

        MigrationPlan deterministic = planner.CreatePlan(
            catalog,
            new MigrationPlanningOptions { Load = load });

        MigrationPlanObject collection = PlanObject(deterministic, CollectionId);
        Assert.False(collection.Included);
        Assert.Contains("requires fail-fast", collection.ExclusionReason, StringComparison.Ordinal);
        Assert.False(PlanObject(deterministic, KeyColumnId).Included);
        Assert.False(PlanObject(deterministic, DocumentColumnId).Included);
        MigrationArtifactSerializer.SerializePlan(deterministic, catalog);
        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(
            deterministic,
            catalog);
        Assert.Equal(MigrationPlanReadinessStatus.RequiresApproval, readiness.Status);
        Assert.Contains(CollectionId, readiness.PendingExclusionObjectIds);
        Assert.Throws<InvalidDataException>(
            () => MigrationPlanReadinessValidator.ValidateForApply(deterministic, catalog));

        MigrationPlan included = planner.CreatePlan(catalog) with { Load = load };
        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(included, catalog));
        Assert.Contains("requires fail-fast", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ArtifactValidation_RejectsIncludedCollectionWithExcludedBridgeColumn()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan included = new MigrationPlanner().CreatePlan(catalog);

        foreach (string bridgeColumnId in new[] { KeyColumnId, DocumentColumnId })
        {
            MigrationPlan tampered = included with
            {
                Objects = included.Objects
                    .Select(item => item.SourceObjectId == bridgeColumnId
                        ? item with
                        {
                            Included = false,
                            TargetName = null,
                            ExclusionReason = "tampered bridge exclusion",
                        }
                        : item)
                    .ToArray(),
            };

            InvalidDataException error = Assert.Throws<InvalidDataException>(
                () => MigrationArtifactSerializer.SerializePlan(tampered, catalog));
            Assert.Contains(
                "requires included key and document bridge columns",
                error.Message,
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Planner_RejectsLookalikesExtraChildrenAndDependenciesByCollectionRule()
    {
        MigrationCatalog baseline = CreateCatalog();
        MigrationCatalogObject collection = CatalogObject(baseline, CollectionId);
        MigrationCatalogObject key = CatalogObject(baseline, KeyColumnId);
        MigrationCatalogObject document = CatalogObject(baseline, DocumentColumnId);

        MigrationCatalog wrongVersion = Replace(
            baseline,
            collection with
            {
                Facets =
                [
                    .. collection.Facets.Where(facet =>
                        facet.Name != MigrationDocumentCollectionContract.ProjectionFacet),
                    Facet(
                        MigrationDocumentCollectionContract.ProjectionFacet,
                        "csharpdb-json-collection-projection/v2"),
                ],
            });
        MigrationCatalog wrongRole = Replace(
            baseline,
            document with
            {
                Facets =
                [
                    .. document.Facets.Where(facet =>
                        facet.Name != MigrationDocumentCollectionContract.FieldRoleFacet),
                    Facet(MigrationDocumentCollectionContract.FieldRoleFacet, "payload"),
                ],
            });
        MigrationCatalog wrongKeyNativeType = Replace(
            baseline,
            key with { NativeType = "JSON_COLLECTION_KEY_LOOKALIKE" });
        MigrationCatalog wrongDocumentNativeType = Replace(
            baseline,
            document with { NativeType = "JSON_ORDERED_DOCUMENT_LOOKALIKE" });
        MigrationCatalog missingKeyVersion = Replace(
            baseline,
            key with
            {
                Facets = key.Facets
                    .Where(facet =>
                        facet.Name != MigrationDocumentCollectionContract.KeyContractFacet)
                    .ToArray(),
            });
        MigrationCatalog wrongDocumentEncoding = Replace(
            baseline,
            document with
            {
                Facets =
                [
                    .. document.Facets.Where(facet =>
                        facet.Name !=
                        MigrationDocumentCollectionContract.DocumentEncodingFacet),
                    Facet(
                        MigrationDocumentCollectionContract.DocumentEncodingFacet,
                        "csharpdb-json-ordered-value/v2"),
                ],
            });
        MigrationCatalog scalarCollection = Replace(
            baseline,
            collection with
            {
                NativeType = MigrationDocumentCollectionContract.DocumentNativeType,
                Facets =
                [
                    .. collection.Facets,
                    Facet(
                        MigrationDocumentCollectionContract.LogicalTypeFacet,
                        MigrationDocumentCollectionContract.JsonLogicalType),
                    Facet(MigrationDocumentCollectionContract.NullableFacet, "false"),
                    Facet(
                        MigrationDocumentCollectionContract.FieldRoleFacet,
                        MigrationDocumentCollectionContract.DocumentRole),
                ],
            });
        MigrationCatalog extraChild = baseline with
        {
            Objects =
            [
                .. baseline.Objects,
                Column("json:collection:extra", "_extra", "TEXT", "text", "key"),
            ],
        };
        var dependency = new MigrationCatalogObject
        {
            ObjectId = "table:dependency",
            Kind = MigrationObjectKind.Table,
            SourceName = "dependency",
        };
        MigrationCatalog withDependency = Replace(
            baseline with { Objects = [.. baseline.Objects, dependency] },
            collection with { DependsOn = [dependency.ObjectId] });
        MigrationCatalog nestedCollection = Replace(
            baseline with { Objects = [.. baseline.Objects, dependency] },
            collection with { ParentObjectId = dependency.ObjectId });
        var nestedBridgeChild = new MigrationCatalogObject
        {
            ObjectId = "json:collection:document:index",
            Kind = MigrationObjectKind.Index,
            ParentObjectId = DocumentColumnId,
            SourceName = "nested-index",
        };
        MigrationCatalog withNestedBridgeChild = baseline with
        {
            Objects = [.. baseline.Objects, nestedBridgeChild],
        };

        foreach (MigrationCatalog malformed in new[]
                 {
                     wrongVersion,
                     wrongRole,
                     wrongKeyNativeType,
                     wrongDocumentNativeType,
                     missingKeyVersion,
                     wrongDocumentEncoding,
                     scalarCollection,
                     extraChild,
                     withDependency,
                     nestedCollection,
                     withNestedBridgeChild,
                 })
        {
            MigrationPlan plan = new MigrationPlanner().CreatePlan(malformed);
            MigrationPlanObject rejected = PlanObject(plan, CollectionId);

            Assert.False(rejected.Included);
            Assert.Contains(
                "CSDB-OBJ-COLLECTION-001",
                rejected.ExclusionReason,
                StringComparison.Ordinal);
            Assert.False(PlanObject(plan, KeyColumnId).Included);
            Assert.False(PlanObject(plan, DocumentColumnId).Included);
        }
    }

    [Fact]
    public void Planner_DoesNotAdmitOtherConditionalObjectKinds()
    {
        MigrationCatalog baseline = CreateCatalog();
        MigrationCatalog catalog = baseline with
        {
            Objects =
            [
                .. baseline.Objects,
                new MigrationCatalogObject
                {
                    ObjectId = "trigger:lookalike",
                    Kind = MigrationObjectKind.Trigger,
                    SourceName = "lookalike",
                    Facets = CollectionFacets(),
                },
            ],
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        MigrationPlanObject trigger = PlanObject(plan, "trigger:lookalike");
        Assert.False(trigger.Included);
        Assert.Contains("Conditional", trigger.ExclusionReason, StringComparison.Ordinal);
    }

    [Fact]
    public void Planner_FailsClosedForCollectionPhysicalNameLengthAndTableCollision()
    {
        string maximum = new(
            'c',
            MigrationDocumentCollectionContract.MaximumLogicalCollectionNameLength);
        MigrationPlan boundary = new MigrationPlanner().CreatePlan(CreateCatalog(maximum));
        Assert.True(PlanObject(boundary, CollectionId).Included);
        Assert.Equal(
            SqlIdentifierRules.MaxLength,
            MigrationDocumentCollectionContract.GetPhysicalCollectionName(maximum).Length);

        string tooLong = maximum + "c";
        MigrationPlan overflow = new MigrationPlanner().CreatePlan(CreateCatalog(tooLong));
        MigrationPlanObject excluded = PlanObject(overflow, CollectionId);
        Assert.False(excluded.Included);
        Assert.Contains("physical '_col_' prefix", excluded.ExclusionReason, StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => MigrationDocumentCollectionContract.GetPhysicalCollectionName(tooLong));

        MigrationCatalogObject table = new()
        {
            ObjectId = "table:collision",
            Kind = MigrationObjectKind.Table,
            SourceName = "table",
        };
        MigrationCatalogObject collection = new()
        {
            ObjectId = CollectionId,
            Kind = MigrationObjectKind.Collection,
            SourceName = "docs",
        };
        MigrationCatalogObject[] objects = [table, collection];
        var targetNames = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [table.ObjectId] = "_COL_docs",
            [collection.ObjectId] = "docs",
        };
        var exclusions = new Dictionary<string, string>(StringComparer.Ordinal);

        MigrationPlanner.ExcludeCollectionPhysicalNameConflicts(
            objects,
            targetNames,
            exclusions);

        Assert.DoesNotContain(table.ObjectId, exclusions.Keys);
        Assert.Contains(collection.ObjectId, exclusions.Keys);
        Assert.Contains("collides case-insensitively", exclusions[collection.ObjectId]);
    }

    private const string CollectionId = "json:collection";
    private const string KeyColumnId = "json:collection:key";
    private const string DocumentColumnId = "json:collection:document";

    private static MigrationCatalog CreateCatalog(string collectionName = "documents") => new()
    {
        TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Json,
            Identity = "json:collection-contract-tests",
            Fingerprint = "sha256:8c7845f67a63606730b9a21f5c1135104457d27fdff2fd4fe839ccb5eade88ee",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Versioned immutable collection contract fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = CollectionId,
                Kind = MigrationObjectKind.Collection,
                SourceName = collectionName,
                Facets = CollectionFacets(),
            },
            Column(
                KeyColumnId,
                MigrationDocumentCollectionContract.KeyColumnName,
                MigrationDocumentCollectionContract.KeyNativeType,
                MigrationDocumentCollectionContract.TextLogicalType,
                MigrationDocumentCollectionContract.KeyRole),
            Column(
                DocumentColumnId,
                MigrationDocumentCollectionContract.DocumentColumnName,
                MigrationDocumentCollectionContract.DocumentNativeType,
                MigrationDocumentCollectionContract.JsonLogicalType,
                MigrationDocumentCollectionContract.DocumentRole),
        ],
    };

    private static MigrationLoadPolicy DeterministicRejectLoad() => new()
    {
        RejectMode = MigrationRejectMode.DeterministicRejects,
        RejectPolicy = new MigrationDeterministicRejectPolicy
        {
            ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            AllowedRuleIds = ["MIG-JSON-ROW-001"],
            MaxRejectedRowsPerBatch = 4,
            MaxRejectedRowsPerRun = 10,
            MaxRawValueBytes = 1_024,
            MaxRawValueBytesPerBatch = 4_096,
            MaxRawValueBytesPerRun = 8_192,
            MaxArtifactBytes = 131_072,
        },
    };

    private static IReadOnlyList<MigrationCatalogFacet> CollectionFacets() =>
    [
        Facet(
            MigrationDocumentCollectionContract.ProjectionFacet,
            MigrationDocumentCollectionContract.ProjectionContract),
        Facet(
            MigrationDocumentCollectionContract.RowContractFacet,
            MigrationDocumentCollectionContract.RowContract),
        Facet(
            MigrationDocumentCollectionContract.KeyContractFacet,
            MigrationDocumentCollectionContract.KeyContract),
        Facet(
            MigrationDocumentCollectionContract.CursorContractFacet,
            MigrationDocumentCollectionContract.CursorContract),
        Facet(
            MigrationDocumentCollectionContract.SchemaContractFacet,
            MigrationDocumentCollectionContract.SchemaContract),
        Facet(
            MigrationDocumentCollectionContract.DocumentEncodingFacet,
            MigrationDocumentCollectionContract.DocumentEncoding),
    ];

    private static MigrationCatalogObject Column(
        string objectId,
        string name,
        string nativeType,
        string logicalType,
        string role) => new()
        {
            ObjectId = objectId,
            Kind = MigrationObjectKind.Column,
            ParentObjectId = CollectionId,
            SourceName = name,
            NativeType = nativeType,
            Facets =
        [
            Facet(MigrationDocumentCollectionContract.LogicalTypeFacet, logicalType),
            Facet(MigrationDocumentCollectionContract.NullableFacet, "false"),
            Facet(MigrationDocumentCollectionContract.FieldRoleFacet, role),
            Facet(
                role == MigrationDocumentCollectionContract.KeyRole
                    ? MigrationDocumentCollectionContract.KeyContractFacet
                    : MigrationDocumentCollectionContract.DocumentEncodingFacet,
                role == MigrationDocumentCollectionContract.KeyRole
                    ? MigrationDocumentCollectionContract.KeyContract
                    : MigrationDocumentCollectionContract.DocumentEncoding),
        ],
        };

    private static MigrationCatalog Replace(
        MigrationCatalog catalog,
        MigrationCatalogObject replacement) =>
        catalog with
        {
            Objects = catalog.Objects
                .Select(item => item.ObjectId == replacement.ObjectId ? replacement : item)
                .ToArray(),
        };

    private static MigrationCatalogObject CatalogObject(
        MigrationCatalog catalog,
        string objectId) =>
        catalog.Objects.Single(item => item.ObjectId == objectId);

    private static MigrationPlanObject PlanObject(MigrationPlan plan, string objectId) =>
        plan.Objects.Single(item => item.SourceObjectId == objectId);

    private static MigrationCatalogFacet Facet(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };
}
