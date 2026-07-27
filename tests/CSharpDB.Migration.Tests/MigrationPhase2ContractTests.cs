using System.Text.Json;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationPhase2ContractTests
{
    [Fact]
    public void Catalog_MemberOrderSurvivesRoundTripAndAffectsDigest()
    {
        MigrationCatalog catalog = CreateCatalog(
            memberObjectIds: ["column:b", "column:a"],
            dependencies: ["column:b", "column:a"]);

        string json = MigrationArtifactSerializer.SerializeCatalog(catalog);
        MigrationCatalog restored = MigrationArtifactSerializer.DeserializeCatalog(json);
        MigrationCatalogObject restoredKey = Assert.Single(
            restored.Objects,
            item => item.ObjectId == "key:composite");

        Assert.Equal(
            ["column:b", "column:a"],
            restoredKey.Members.Select(member => member.ObjectId));
        Assert.Equal([0, 1], restoredKey.Members.Select(member => member.Ordinal));
        Assert.Equal(json, MigrationArtifactSerializer.SerializeCatalog(restored));

        MigrationCatalog reorderedMembers = CreateCatalog(
            memberObjectIds: ["column:a", "column:b"],
            dependencies: ["column:b", "column:a"]);

        Assert.NotEqual(
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            MigrationArtifactSerializer.ComputeCatalogDigest(reorderedMembers));
    }

    [Fact]
    public void Catalog_DependencyOrderIsSetLikeAndDoesNotAffectDigest()
    {
        MigrationCatalog first = CreateCatalog(
            memberObjectIds: ["column:b", "column:a"],
            dependencies: ["column:b", "column:a"]);
        MigrationCatalog second = CreateCatalog(
            memberObjectIds: ["column:b", "column:a"],
            dependencies: ["column:a", "column:b"]);

        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(first),
            MigrationArtifactSerializer.SerializeCatalog(second));
    }

    [Fact]
    public async Task Plan_AcceptedExclusionsAreNormalizedAndMakeExcludedUnsupportedObjectsNonblocking()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        string[] excludedObjectIds = plan.Objects
            .Where(item => !item.Included)
            .Select(item => item.SourceObjectId)
            .OrderByDescending(item => item, StringComparer.Ordinal)
            .ToArray();
        Assert.NotEmpty(excludedObjectIds);

        MigrationPlan accepted = plan with
        {
            AcceptedExclusionObjectIds = excludedObjectIds,
        };
        string json = MigrationArtifactSerializer.SerializePlan(accepted, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);
        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(restored, catalog);

        Assert.Equal(
            excludedObjectIds.OrderBy(item => item, StringComparer.Ordinal),
            restored.AcceptedExclusionObjectIds);
        Assert.Equal(MigrationPlanReadinessStatus.Ready, readiness.Status);
        Assert.Empty(readiness.PendingExclusionObjectIds);
        Assert.Empty(readiness.BlockingDiagnosticIds);

        MigrationPlan oppositeOrder = accepted with
        {
            AcceptedExclusionObjectIds = excludedObjectIds.Reverse().ToArray(),
        };
        Assert.Equal(
            json,
            MigrationArtifactSerializer.SerializePlan(oppositeOrder, catalog));
    }

    [Fact]
    public async Task Plan_UnacceptedExclusionsRequireApprovalInsteadOfBlocking()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        string[] excludedObjectIds = plan.Objects
            .Where(item => !item.Included)
            .Select(item => item.SourceObjectId)
            .OrderBy(item => item, StringComparer.Ordinal)
            .ToArray();

        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(plan, catalog);

        Assert.NotEmpty(excludedObjectIds);
        Assert.Equal(MigrationPlanReadinessStatus.RequiresApproval, readiness.Status);
        Assert.Equal(excludedObjectIds, readiness.PendingExclusionObjectIds);
        Assert.Empty(readiness.BlockingDiagnosticIds);
    }

    [Theory]
    [InlineData("missing:object")]
    [InlineData("syn:table:orders")]
    public async Task Plan_RejectsAcceptedExclusionThatIsNotAnExcludedPlanObject(string objectId)
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog) with
        {
            AcceptedExclusionObjectIds = [objectId],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("exclusion", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PlanDigest_IsDeterministicMatchesTheArtifactAndDetectsTampering()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        string first = MigrationArtifactSerializer.ComputePlanDigest(plan);
        string second = MigrationArtifactSerializer.ComputePlanDigest(plan);
        using JsonDocument document = JsonDocument.Parse(
            MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Equal(first, second);
        Assert.Equal(document.RootElement.GetProperty("digest").GetString(), first);
        Assert.Equal(64, first.Length);

        MigrationPlan tampered = plan with
        {
            Load = plan.Load with { BatchSize = plan.Load.BatchSize + 1 },
        };
        Assert.NotEqual(first, MigrationArtifactSerializer.ComputePlanDigest(tampered));
    }

    [Fact]
    public async Task SyntheticUnsignedProfile_UsesTheTwelveRowFixtureTotal()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationCatalogObject unsignedColumn = Assert.Single(
            catalog.Objects,
            item => item.ObjectId == "syn:column:orders:source-counter");
        MigrationCatalogFacet total = Assert.Single(
            unsignedColumn.Facets,
            facet => facet.Name == "profileTotalValues");

        Assert.Equal("12", total.Value);

        MigrationTypeMapping mapping = Assert.Single(
            new MigrationPlanner()
                .CreatePlan(catalog)
                .Objects
                .Single(item => item.SourceObjectId == unsignedColumn.ObjectId)
                .TypeMappings);
        Assert.Equal(12, mapping.Coverage.TotalValues);
    }

    private static async Task<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });

    private static MigrationCatalog CreateCatalog(
        IReadOnlyList<string> memberObjectIds,
        IReadOnlyList<string> dependencies)
    {
        var table = new MigrationCatalogObject
        {
            ObjectId = "table:sample",
            Kind = MigrationObjectKind.Table,
            SourceName = "sample",
        };
        var columnA = new MigrationCatalogObject
        {
            ObjectId = "column:a",
            Kind = MigrationObjectKind.Column,
            ParentObjectId = table.ObjectId,
            SourceName = "a",
            NativeType = "INT64",
        };
        var columnB = new MigrationCatalogObject
        {
            ObjectId = "column:b",
            Kind = MigrationObjectKind.Column,
            ParentObjectId = table.ObjectId,
            SourceName = "b",
            NativeType = "INT64",
        };
        var key = new MigrationCatalogObject
        {
            ObjectId = "key:composite",
            Kind = MigrationObjectKind.Key,
            ParentObjectId = table.ObjectId,
            SourceName = "PK sample",
            Facets = [new MigrationCatalogFacet { Name = "kind", Value = "primary" }],
            DependsOn = dependencies,
            Members = memberObjectIds
                .Select((objectId, ordinal) => new MigrationObjectReference
                {
                    ObjectId = objectId,
                    Role = "column",
                    Ordinal = ordinal,
                })
                .ToArray(),
        };

        return new MigrationCatalog
        {
            TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.Synthetic,
                Identity = "synthetic:phase2-member-order",
                Fingerprint = "sha256:phase2-member-order",
                SourceVersion = "1",
                ProviderVersion = "1",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description = "Immutable contract-test catalog.",
                },
            },
            Objects = [key, columnB, table, columnA],
        };
    }
}
