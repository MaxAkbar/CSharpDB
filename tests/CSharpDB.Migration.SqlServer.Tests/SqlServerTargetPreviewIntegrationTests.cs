using System.Text.Json;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerTargetPreviewIntegrationTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task SupportedRelationalSubsetProducesDeterministicScratchProof()
    {
        SqlServerCatalogSnapshot ordered =
            SqlServerTestSnapshot.CreateSupportedRelational();
        SqlServerCatalogSnapshot reversed = Reverse(ordered);

        Assessment first = await AssessAsync(ordered, Ct);
        Assessment repeated = await AssessAsync(ordered, Ct);
        Assessment reordered = await AssessAsync(reversed, Ct);

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(first.Catalog),
            MigrationArtifactSerializer.SerializeCatalog(reordered.Catalog));
        Assert.Equal(
            MigrationArtifactSerializer.SerializePlan(
                first.Plan,
                first.Catalog),
            MigrationArtifactSerializer.SerializePlan(
                reordered.Plan,
                reordered.Catalog));
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(first.AttachedPlan),
            MigrationArtifactSerializer.ComputePlanDigest(
                reordered.AttachedPlan));
        Assert.Equal(
            JsonSerializer.Serialize(first.Preview),
            JsonSerializer.Serialize(repeated.Preview));
        Assert.Equal(
            JsonSerializer.Serialize(first.Preview),
            JsonSerializer.Serialize(reordered.Preview));
        Assert.Equal(
            JsonSerializer.Serialize(first.Report),
            JsonSerializer.Serialize(repeated.Report));
        Assert.Equal(
            JsonSerializer.Serialize(first.Report),
            JsonSerializer.Serialize(reordered.Report));

        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            first.Preview.Readiness.Status);
        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            first.Report.ReadinessStatus);
        Assert.Equal(
            CSharpDbDdlScratchValidationStatus.Passed,
            first.Report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            first.Report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlScratchValidator.SchemaEqualRuleId,
            first.Report.RuleId);
        Assert.Equal(
            first.Report.ExpectedSchemaDigest,
            first.Report.ActualSchemaDigest);
        AssertLowerSha256(first.Preview.CatalogDigest);
        AssertLowerSha256(first.Preview.PlanContractDigest);
        AssertLowerSha256(first.Preview.GeneratedDdlDigest);
        AssertLowerSha256(first.Report.AttachedPlanDigest);
        AssertLowerSha256(first.Report.ExpectedSchemaDigest);
        Assert.Equal(
            first.Preview.GeneratedDdlDigest,
            first.AttachedPlan.GeneratedDdlDigest);
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(
                first.AttachedPlan),
            first.Report.AttachedPlanDigest);

        Assert.Equal(
            [
                MigrationSchemaStage.LoadEssential,
                MigrationSchemaStage.SecondaryIndexes,
                MigrationSchemaStage.Constraints,
                MigrationSchemaStage.Views,
                MigrationSchemaStage.Triggers,
            ],
            first.Preview.Stages.Select(stage => stage.Stage));
        Assert.Equal(
            [2, 1, 2, 0, 0],
            first.Preview.Stages.Select(stage => stage.Actions.Count));
        Assert.All(
            first.Preview.Stages.SelectMany(stage => stage.Actions),
            action => Assert.Equal(
                CSharpDbDdlPreviewActionKind.Sql,
                action.Kind));
        Assert.Equal(5, first.Report.ParsedActionCount);
        Assert.Equal(5, first.Report.ExecutedActionCount);
        Assert.Empty(first.Report.Differences);

        IReadOnlyDictionary<string, MigrationObjectKind> kindsById =
            first.Catalog.Objects.ToDictionary(
                item => item.ObjectId,
                item => item.Kind,
                StringComparer.Ordinal);
        MigrationObjectKind[] includedKinds = first.Plan.Objects
            .Where(item => item.Included)
            .Select(item => kindsById[item.SourceObjectId])
            .ToArray();
        Assert.Equal(2, includedKinds.Count(
            kind => kind == MigrationObjectKind.Table));
        Assert.Equal(4, includedKinds.Count(
            kind => kind == MigrationObjectKind.Column));
        Assert.Equal(1, includedKinds.Count(
            kind => kind == MigrationObjectKind.Key));
        Assert.Equal(1, includedKinds.Count(
            kind => kind == MigrationObjectKind.Index));
        Assert.Equal(1, includedKinds.Count(
            kind => kind == MigrationObjectKind.ForeignKey));

        MigrationPlanReadiness planReadiness =
            MigrationPlanReadinessValidator.Evaluate(
                first.AttachedPlan,
                first.Catalog);
        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            planReadiness.Status);
        Assert.Contains(
            first.Catalog.Diagnostics.Single(diagnostic =>
                diagnostic.RuleId ==
                    "MIG-SQLSERVER-INVENTORY-PARTIAL-001")
                .DiagnosticId,
            planReadiness.BlockingDiagnosticIds);
    }

    [Fact]
    public async Task OfflineIntegrationPropagatesCancellationAfterAttachment()
    {
        using var cancellation = new CancellationTokenSource();
        bool attachmentCompleted = false;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await AssessAsync(
                SqlServerTestSnapshot.CreateSupportedRelational(),
                cancellation.Token,
                beforeScratch: () =>
                {
                    attachmentCompleted = true;
                    cancellation.Cancel();
                }));
        Assert.True(attachmentCompleted);
    }

    [Fact]
    public async Task FullInventoryKeepsUnloweredObjectsOutOfTargetActions()
    {
        Assessment assessment = await AssessAsync(
            SqlServerTestSnapshot.Create(),
            Ct);

        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            assessment.Preview.Readiness.Status);
        Assert.Equal(
            CSharpDbDdlScratchValidationStatus.Passed,
            assessment.Report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            assessment.Report.HighestEvidence);
        Assert.Empty(assessment.Preview.Stages.Single(stage =>
            stage.Stage == MigrationSchemaStage.Views).Actions);
        Assert.Empty(assessment.Preview.Stages.Single(stage =>
            stage.Stage == MigrationSchemaStage.Triggers).Actions);

        AssertExcluded(
            assessment,
            MigrationObjectKind.CheckConstraint,
            "CK_Orders_Amount");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Sequence,
            "OrderSequence");
        AssertExcluded(
            assessment,
            MigrationObjectKind.View,
            "OrderSummary");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Trigger,
            "TR_Orders_Audit");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Routine,
            "usp_CycleA");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Index,
            "CUX_OrderSummary_Id");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Index,
            "$fulltext");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Other,
            "MigrationSearch");
        AssertExcluded(
            assessment,
            MigrationObjectKind.Other,
            "PF_Orders_Customer");
        AssertIncluded(
            assessment,
            MigrationObjectKind.Table,
            "Archive");
        AssertIncluded(
            assessment,
            MigrationObjectKind.Column,
            "ArchiveId");
        Assert.NotEmpty(assessment.Preview.Stages.Single(stage =>
            stage.Stage == MigrationSchemaStage.LoadEssential).Actions);
        Assert.True(assessment.Report.ExecutedActionCount > 0);

        string publishedPreview =
            JsonSerializer.Serialize(assessment.Preview);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretDefaultDefinition,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretCheckDefinition,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretFilterDefinition,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretModuleDefinition,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretPartitionBoundary,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretPartitionBoundaryHex,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "CUX_OrderSummary_Id",
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "MigrationSearch",
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "PF_Orders_Customer",
            publishedPreview,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpecializedIndexInventoryNeverBecomesOrdinaryTargetDdl()
    {
        Assessment assessment = await AssessAsync(
            SqlServerTestSnapshot.CreateSpecializedIndexes(),
            Ct);

        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            assessment.Preview.Readiness.Status);
        Assert.Equal(
            CSharpDbDdlScratchValidationStatus.Passed,
            assessment.Report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            assessment.Report.HighestEvidence);

        string[] indexNames =
        [
            "PXML_XmlDocuments",
            "SXML_XmlDocuments_Path",
            "SXI_XmlDocuments",
            "SSXI_XmlDocuments_Path",
            "SIX_SpatialDocuments",
            "HIX_MemoryDocuments",
            "CCI_ColumnStoreFacts",
            "NCCI_ColumnStoreProjection",
            "JIX_JsonDocuments",
        ];
        foreach (string indexName in indexNames)
        {
            AssertExcluded(
                assessment,
                MigrationObjectKind.Index,
                indexName);
        }

        string[] configurationClasses =
        [
            "xml-index-config",
            "selective-xml-index-path",
            "spatial-index-config",
            "spatial-index-tessellation",
            "hash-index-config",
            "columnstore-index-config",
            "columnstore-index-column",
            "json-index-config",
            "json-index-path",
        ];
        MigrationCatalogObject[] configurationObjects =
            assessment.Catalog.Objects
                .Where(item =>
                    item.Kind == MigrationObjectKind.Other &&
                    CatalogFacet(
                        item,
                        "sqlServerObjectClass") is string objectClass &&
                    configurationClasses.Contains(
                        objectClass,
                        StringComparer.Ordinal))
                .ToArray();
        Assert.NotEmpty(configurationObjects);
        Assert.All(
            configurationObjects,
            configuration =>
            {
                MigrationPlanObject planned = Assert.Single(
                    assessment.Plan.Objects,
                    item => item.SourceObjectId ==
                        configuration.ObjectId);
                Assert.False(planned.Included);
            });

        CSharpDbDdlPreviewStage secondaryIndexes =
            assessment.Preview.Stages.Single(stage =>
                stage.Stage == MigrationSchemaStage.SecondaryIndexes);
        string publishedPreview =
            JsonSerializer.Serialize(assessment.Preview);
        foreach (string indexName in indexNames)
        {
            Assert.DoesNotContain(
                indexName,
                publishedPreview,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                secondaryIndexes.Actions,
                action => action.Sql?.Contains(
                    indexName,
                    StringComparison.Ordinal) == true);
        }
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretSelectiveXmlPath,
            publishedPreview,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretJsonIndexPath,
            publishedPreview,
            StringComparison.Ordinal);

        MigrationPlan approvedExclusions =
            new MigrationPlanner().CreatePlan(
                assessment.Catalog,
                new MigrationPlanningOptions
                {
                    AcceptAllExclusions = true,
                });
        CSharpDbDdlPreview approvedPreview =
            CSharpDbDdlPreviewBuilder.Build(
                approvedExclusions,
                assessment.Catalog,
                cancellationToken: Ct);
        Assert.Equal(
            MigrationPlanReadinessStatus.Blocked,
            approvedPreview.Readiness.Status);
        Assert.Contains(
            assessment.Catalog.Diagnostics.Single(diagnostic =>
                diagnostic.RuleId ==
                    "MIG-SQLSERVER-INVENTORY-PARTIAL-001")
                .DiagnosticId,
            approvedPreview.Readiness.BlockingDiagnosticIds);
    }

    private static async ValueTask<Assessment> AssessAsync(
        SqlServerCatalogSnapshot snapshot,
        CancellationToken cancellationToken,
        Action? beforeScratch = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var inspector = new SqlServerMigrationSourceInspector(
            new SnapshotReader(snapshot),
            SqlServerInspectionLimits.Default);
        MigrationCatalog catalog = await inspector.InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = false,
            },
            cancellationToken);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        CSharpDbDdlPreview preview = CSharpDbDdlPreviewBuilder.Build(
            plan,
            catalog,
            cancellationToken: cancellationToken);
        MigrationPlan attachedPlan =
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                plan,
                catalog,
                preview,
                cancellationToken: cancellationToken);
        beforeScratch?.Invoke();
        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                attachedPlan,
                catalog,
                preview,
                cancellationToken: cancellationToken);
        return new Assessment(
            catalog,
            plan,
            preview,
            attachedPlan,
            report);
    }

    private static SqlServerCatalogSnapshot Reverse(
        SqlServerCatalogSnapshot source) =>
        new(
            source.EndpointDigest,
            source.ProviderVersion,
            source.Instance,
            source.Database,
            source.Schemas.Reverse(),
            source.Tables.Reverse(),
            source.Columns.Reverse(),
            source.Keys.Reverse(),
            source.Indexes.Reverse(),
            source.IndexColumns.Reverse(),
            source.ForeignKeys.Reverse(),
            source.ForeignKeyColumns.Reverse(),
            source.Checks.Reverse(),
            source.Sequences.Reverse(),
            Reverse(source.PermissionAuditBefore),
            Reverse(source.PermissionAuditAfter),
            source.Views.Reverse(),
            source.ViewColumns.Reverse(),
            source.Triggers.Reverse(),
            source.TriggerEvents.Reverse(),
            source.Routines.Reverse(),
            source.Modules.Reverse(),
            source.Parameters.Reverse(),
            Reverse(source.ExpressionDependencyAudit),
            source.FullTextCatalogs.Reverse(),
            source.FullTextStoplists.Reverse(),
            source.SearchPropertyLists.Reverse(),
            source.FullTextIndexes.Reverse(),
            source.FullTextIndexColumns.Reverse(),
            source.DataSpaces.Reverse(),
            source.PartitionSchemes.Reverse(),
            source.PartitionSchemeDestinations.Reverse(),
            source.PartitionFunctions.Reverse(),
            source.PartitionParameters.Reverse(),
            source.PartitionRangeValues.Reverse(),
            source.IndexPartitions.Reverse(),
            source.XmlIndexes.Reverse(),
            source.SelectiveXmlIndexPaths.Reverse(),
            source.SpatialIndexes.Reverse(),
            source.SpatialIndexTessellations.Reverse(),
            source.HashIndexes.Reverse(),
            source.JsonIndexes.Reverse(),
            source.JsonIndexPaths.Reverse());

    private static SqlServerPermissionAuditMetadata Reverse(
        SqlServerPermissionAuditMetadata source) =>
        new(
            source.Tokens.Reverse().ToArray(),
            source.Denials.Reverse().ToArray(),
            source.Attempted);

    private static SqlServerExpressionDependencyAuditMetadata Reverse(
        SqlServerExpressionDependencyAuditMetadata source) =>
        new(
            source.Dependencies.Reverse().ToArray(),
            source.Attempted);

    private static void AssertLowerSha256(string? value)
    {
        Assert.NotNull(value);
        Assert.Equal(64, value.Length);
        Assert.All(
            value,
            character => Assert.True(
                character is >= '0' and <= '9' or >= 'a' and <= 'f'));
    }

    private static string? CatalogFacet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static void AssertExcluded(
        Assessment assessment,
        MigrationObjectKind kind,
        string sourceName)
    {
        MigrationCatalogObject catalogObject = Assert.Single(
            assessment.Catalog.Objects,
            item => item.Kind == kind &&
                string.Equals(
                    item.SourceName,
                    sourceName,
                    StringComparison.Ordinal));
        MigrationPlanObject planObject = Assert.Single(
            assessment.Plan.Objects,
            item => string.Equals(
                item.SourceObjectId,
                catalogObject.ObjectId,
                StringComparison.Ordinal));
        Assert.False(planObject.Included);
    }

    private static void AssertIncluded(
        Assessment assessment,
        MigrationObjectKind kind,
        string sourceName)
    {
        MigrationCatalogObject catalogObject = Assert.Single(
            assessment.Catalog.Objects,
            item => item.Kind == kind &&
                string.Equals(
                    item.SourceName,
                    sourceName,
                    StringComparison.Ordinal));
        MigrationPlanObject planObject = Assert.Single(
            assessment.Plan.Objects,
            item => string.Equals(
                item.SourceObjectId,
                catalogObject.ObjectId,
                StringComparison.Ordinal));
        Assert.True(planObject.Included);
    }

    private sealed record Assessment(
        MigrationCatalog Catalog,
        MigrationPlan Plan,
        CSharpDbDdlPreview Preview,
        MigrationPlan AttachedPlan,
        CSharpDbDdlScratchValidationReport Report);

    private sealed class SnapshotReader(SqlServerCatalogSnapshot snapshot)
        : ISqlServerCatalogReader
    {
        public ValueTask<SqlServerCatalogSnapshot> ReadAsync(
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(snapshot);
        }
    }
}
