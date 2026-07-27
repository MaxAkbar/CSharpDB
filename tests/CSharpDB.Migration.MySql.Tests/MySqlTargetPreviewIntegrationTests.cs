using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlTargetPreviewIntegrationTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task SupportedRelationalSubsetProducesScratchValidatedTargetDdl()
    {
        var inspector = new MySqlMigrationSourceInspector(
            new SnapshotReader(MySqlTestSnapshot.CreateSupportedRelational()),
            MySqlInspectionLimits.Default);
        MigrationCatalog catalog = await inspector.InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            },
            Ct);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        CSharpDbDdlPreview preview = CSharpDbDdlPreviewBuilder.Build(
            plan,
            catalog,
            cancellationToken: Ct);
        MigrationPlan attachedPlan =
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);
        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                attachedPlan,
                catalog,
                preview,
                cancellationToken: Ct);

        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            preview.Readiness.Status);
        Assert.Equal(
            CSharpDbDdlScratchValidationStatus.Passed,
            report.Status);
        Assert.Equal(MigrationEvidenceLevel.ScratchExecuted, report.HighestEvidence);
        Assert.Equal(report.ExpectedSchemaDigest, report.ActualSchemaDigest);
        Assert.Empty(report.Differences);
        Assert.NotEmpty(preview.Stages.SelectMany(static stage => stage.Actions));

        AssertIncluded(catalog, plan, MigrationObjectKind.Key, "PRIMARY");
        AssertIncluded(
            catalog,
            plan,
            MigrationObjectKind.Index,
            "IX_Parent_Code");
        AssertIncluded(
            catalog,
            plan,
            MigrationObjectKind.ForeignKey,
            "FK_Child_Parent");
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-MYSQL-INVENTORY-PARTIAL-001" &&
                !item.CanOverride);
    }

    private static void AssertIncluded(
        MigrationCatalog catalog,
        MigrationPlan plan,
        MigrationObjectKind kind,
        string sourceName)
    {
        MigrationCatalogObject[] candidates = catalog.Objects
            .Where(item =>
                item.Kind == kind &&
                item.SourceName == sourceName)
            .ToArray();
        Assert.NotEmpty(candidates);
        Assert.Contains(
            candidates,
            candidate => plan.Objects.Single(item =>
                item.SourceObjectId == candidate.ObjectId).Included);
    }

    private sealed class SnapshotReader : IMySqlCatalogReader
    {
        private readonly MySqlCatalogSnapshot snapshot;

        public SnapshotReader(MySqlCatalogSnapshot snapshot)
        {
            this.snapshot = snapshot;
        }

        public ValueTask<MySqlCatalogSnapshot> ReadAsync(
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(snapshot);
        }
    }
}
