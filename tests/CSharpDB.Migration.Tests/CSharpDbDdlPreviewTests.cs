using System.Reflection;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbDdlPreviewTests
{
    private const string CollectionActionPrefix =
        "csharpdb-migration-json-collection-action/v1:";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Build_UsesRendererActionsInFixedStageOrderWithTypedClassification()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);

        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);

        MigrationSchemaStage[] expectedStages =
        [
            MigrationSchemaStage.LoadEssential,
            MigrationSchemaStage.SecondaryIndexes,
            MigrationSchemaStage.Constraints,
            MigrationSchemaStage.Views,
            MigrationSchemaStage.Triggers,
        ];
        Assert.Equal(expectedStages, preview.Stages.Select(stage => stage.Stage));
        Assert.Equal(
            Enumerable.Range(0, expectedStages.Length),
            preview.Stages.Select(stage => stage.Ordinal));

        foreach (CSharpDbDdlPreviewStage stage in preview.Stages)
        {
            IReadOnlyList<string> rendered = RenderStageActions(
                plan,
                catalog,
                stage.Stage);
            Assert.Equal(rendered.Count, stage.Actions.Count);
            Assert.Equal(
                Enumerable.Range(0, rendered.Count),
                stage.Actions.Select(action => action.Ordinal));

            for (int index = 0; index < rendered.Count; index++)
            {
                string raw = rendered[index];
                CSharpDbDdlPreviewAction action = stage.Actions[index];
                if (raw.StartsWith(CollectionActionPrefix, StringComparison.Ordinal))
                {
                    Assert.Equal(
                        CSharpDbDdlPreviewActionKind.EnsureJsonDocumentCollection,
                        action.Kind);
                    Assert.Equal(raw[CollectionActionPrefix.Length..], action.TargetName);
                    Assert.Null(action.Sql);
                }
                else
                {
                    Assert.Equal(CSharpDbDdlPreviewActionKind.Sql, action.Kind);
                    Assert.Equal(raw, action.Sql);
                    Assert.Null(action.TargetName);
                }
            }
        }
    }

    [Fact]
    public async Task Build_IsDeterministicForRepeatedAndReorderedInputs()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        MigrationCatalog reorderedCatalog = catalog with
        {
            Objects = catalog.Objects.Reverse().ToArray(),
            Diagnostics = catalog.Diagnostics.Reverse().ToArray(),
        };
        MigrationPlan reorderedPlan = plan with
        {
            Objects = plan.Objects.Reverse().ToArray(),
            Diagnostics = plan.Diagnostics.Reverse().ToArray(),
            AcceptedDiagnosticIds = plan.AcceptedDiagnosticIds.Reverse().ToArray(),
            AcceptedExclusionObjectIds =
                plan.AcceptedExclusionObjectIds.Reverse().ToArray(),
        };

        CSharpDbDdlPreview first =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        CSharpDbDdlPreview repeated =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        CSharpDbDdlPreview reordered = CSharpDbDdlPreviewBuilder.Build(
            reorderedPlan,
            reorderedCatalog,
            cancellationToken: Ct);

        Assert.Equal(first.GeneratedDdlDigest, repeated.GeneratedDdlDigest);
        Assert.Equal(first.GeneratedDdlDigest, reordered.GeneratedDdlDigest);
        Assert.Equal(first.PlanContractDigest, reordered.PlanContractDigest);
        Assert.Equal(Project(first), Project(repeated));
        Assert.Equal(Project(first), Project(reordered));
        Assert.Equal(first.Readiness.Status, reordered.Readiness.Status);
        Assert.Equal(
            first.Readiness.PendingDiagnosticIds,
            reordered.Readiness.PendingDiagnosticIds);
        Assert.Equal(
            first.Readiness.PendingExclusionObjectIds,
            reordered.Readiness.PendingExclusionObjectIds);
        Assert.Equal(
            first.Readiness.BlockingDiagnosticIds,
            reordered.Readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public async Task Attach_SealsAValidatedPlanWithoutChangingThePreviewDigest()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);

        MigrationPlan attached = CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
            plan,
            catalog,
            preview,
            cancellationToken: Ct);
        CSharpDbDdlPreview rebuilt = CSharpDbDdlPreviewBuilder.Build(
            attached,
            catalog,
            cancellationToken: Ct);
        string serialized =
            MigrationArtifactSerializer.SerializePlan(attached, catalog, writeIndented: false);
        MigrationPlan restored =
            MigrationArtifactSerializer.DeserializePlan(serialized, catalog);
        CSharpDbDdlPreview restoredPreview = CSharpDbDdlPreviewBuilder.Build(
            restored,
            catalog,
            cancellationToken: Ct);
        MigrationPlan attachedAgain =
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                attached,
                catalog,
                rebuilt,
                cancellationToken: Ct);

        Assert.Equal(preview.GeneratedDdlDigest, attached.GeneratedDdlDigest);
        Assert.Equal(preview.GeneratedDdlDigest, rebuilt.GeneratedDdlDigest);
        Assert.Equal(preview.GeneratedDdlDigest, restoredPreview.GeneratedDdlDigest);
        Assert.Equal(preview.PlanContractDigest, rebuilt.PlanContractDigest);
        Assert.Equal(preview.PlanContractDigest, restoredPreview.PlanContractDigest);
        Assert.Equal(attached.GeneratedDdlDigest, attachedAgain.GeneratedDdlDigest);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            MigrationArtifactSerializer.ComputePlanDigest(attached));
        MigrationPlanReadinessValidator.ValidateForApply(restored, catalog);
    }

    [Fact]
    public async Task Attach_RejectsTamperedPreviewAndPreviewFromChangedPlan()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        CSharpDbDdlPreviewStage firstNonEmpty =
            preview.Stages.First(stage => stage.Actions.Count > 0);
        CSharpDbDdlPreviewAction firstAction = firstNonEmpty.Actions[0];
        Assert.Equal(CSharpDbDdlPreviewActionKind.Sql, firstAction.Kind);
        CSharpDbDdlPreview tampered = preview with
        {
            Stages = preview.Stages
                .Select(stage => stage.Ordinal == firstNonEmpty.Ordinal
                    ? stage with
                    {
                        Actions = stage.Actions
                            .Select(action => action.Ordinal == firstAction.Ordinal
                                ? action with { Sql = action.Sql + " -- tampered" }
                                : action)
                            .ToArray(),
                    }
                    : stage)
                .ToArray(),
        };
        MigrationPlan changed = plan with
        {
            Load = plan.Load with { BatchSize = plan.Load.BatchSize + 1 },
        };
        CSharpDbDdlPreview tamperedReadiness = preview with
        {
            Readiness = preview.Readiness with
            {
                Status = MigrationPlanReadinessStatus.Blocked,
            },
        };
        MigrationPlan wrongAttachedDigest = plan with
        {
            GeneratedDdlDigest = new string('0', 64),
        };

        Assert.Throws<InvalidDataException>(() =>
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                plan,
                catalog,
                tampered,
                cancellationToken: Ct));
        Assert.Throws<InvalidDataException>(() =>
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                changed,
                catalog,
                preview,
                cancellationToken: Ct));
        Assert.Throws<InvalidDataException>(() =>
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                plan,
                catalog,
                tamperedReadiness,
                cancellationToken: Ct));
        Assert.Throws<InvalidDataException>(() =>
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                wrongAttachedDigest,
                catalog,
                preview,
                cancellationToken: Ct));
    }

    [Fact]
    public async Task PlanContract_RejectsUppercaseGeneratedDdlDigest()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog) with
        {
            GeneratedDdlDigest = new string('A', 64),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            MigrationArtifactSerializer.SerializePlan(
                plan,
                catalog,
                writeIndented: false));
        Assert.Contains("lowercase", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StagedTargetFactory_RejectsWrongAttachedDigestBeforeWritingFiles()
    {
        using var files = new TemporaryPreviewDirectory();
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog) with
        {
            GeneratedDdlDigest = new string('0', 64),
        };

        async Task CreateTargetAsync()
        {
            await using CSharpDbStagedMigrationTarget target =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    files.TargetPath,
                    plan,
                    catalog,
                    SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                    cancellationToken: Ct);
        }

        InvalidDataException error =
            await Assert.ThrowsAsync<InvalidDataException>(CreateTargetAsync);
        Assert.Contains("generated DDL digest", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(Directory.EnumerateFileSystemEntries(files.DirectoryPath));
    }

    [Fact]
    public async Task Build_ReportsUnresolvedReadinessWithoutWritingTargetFiles()
    {
        using var files = new TemporaryPreviewDirectory();
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan requiresApproval = new MigrationPlanner().CreatePlan(catalog);
        MigrationDiagnostic blocker = new()
        {
            DiagnosticId = "preview:blocking:customers",
            RuleId = "PREVIEW-BLOCK-001",
            Severity = MigrationDiagnosticSeverity.Error,
            Status = MigrationCompatibilityStatus.Unsupported,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = "Synthetic preview blocker.",
            Explanation = "Exercises no-write preview of a structurally valid blocked plan.",
            ObjectId = "syn:table:customers-upper",
            Remediation = "Resolve before apply.",
            CanOverride = false,
        };
        MigrationCatalog blockedCatalog = catalog with
        {
            Diagnostics = catalog.Diagnostics.Append(blocker).ToArray(),
        };
        MigrationPlan blockedPlan = ReadyPlan(blockedCatalog);
        Assert.True(blockedPlan.Objects.Single(
            item => item.SourceObjectId == blocker.ObjectId).Included);

        CSharpDbDdlPreview approvalPreview = CSharpDbDdlPreviewBuilder.Build(
            requiresApproval,
            catalog,
            cancellationToken: Ct);
        CSharpDbDdlPreview blockedPreview = CSharpDbDdlPreviewBuilder.Build(
            blockedPlan,
            blockedCatalog,
            cancellationToken: Ct);

        Assert.Equal(
            MigrationPlanReadinessStatus.RequiresApproval,
            approvalPreview.Readiness.Status);
        Assert.NotEmpty(approvalPreview.Readiness.PendingExclusionObjectIds);
        Assert.Equal(MigrationPlanReadinessStatus.Blocked, blockedPreview.Readiness.Status);
        Assert.Contains(blocker.DiagnosticId, blockedPreview.Readiness.BlockingDiagnosticIds);
        Assert.Empty(Directory.EnumerateFileSystemEntries(files.DirectoryPath));
    }

    [Fact]
    public void Build_ClassifiesCollectionActionWithoutExposingInternalSentinelAsSql()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);

        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        CSharpDbDdlPreviewStage load = Assert.Single(
            preview.Stages,
            stage => stage.Stage == MigrationSchemaStage.LoadEssential);
        CSharpDbDdlPreviewAction action = Assert.Single(load.Actions);
        string raw = Assert.Single(RenderStageActions(
            plan,
            catalog,
            MigrationSchemaStage.LoadEssential));

        Assert.StartsWith(CollectionActionPrefix, raw, StringComparison.Ordinal);
        Assert.Equal(
            CSharpDbDdlPreviewActionKind.EnsureJsonDocumentCollection,
            action.Kind);
        Assert.Equal(raw[CollectionActionPrefix.Length..], action.TargetName);
        Assert.Null(action.Sql);
        Assert.DoesNotContain(
            preview.Stages.SelectMany(stage => stage.Actions),
            candidate =>
                candidate.Sql?.Contains(
                    CollectionActionPrefix,
                    StringComparison.Ordinal) == true);
    }

    [Fact]
    public void Build_PinsVersionedSchemaActionDigest()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);

        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);

        Assert.Equal(
            "caae142bc142a608b64e6f0f6662cca151534c87ad567983f3380cd0d4f88fc7",
            preview.GeneratedDdlDigest);
    }

    [Fact]
    public void AttachGeneratedDdlDigest_HonorsPreCanceledValidation()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(() =>
            CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
                plan,
                catalog,
                preview,
                cancellationToken: cancellation.Token));
    }

    private static async Task<MigrationCatalog> InspectSyntheticAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            Ct);

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog) =>
        new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });

    private static IReadOnlyList<string> RenderStageActions(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage)
    {
        Type renderer = typeof(CSharpDbDdlPreviewBuilder).Assembly.GetType(
            "CSharpDB.Migration.CSharpDb.CSharpDbMigrationSql",
            throwOnError: true)!;
        MethodInfo method = renderer.GetMethod(
            "BuildStageActions",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        return Assert.IsAssignableFrom<IReadOnlyList<string>>(
            method.Invoke(null, [plan, catalog, stage]));
    }

    private static string[] Project(CSharpDbDdlPreview preview) =>
        preview.Stages
            .SelectMany(stage => stage.Actions.Select(action =>
                $"{stage.Ordinal}:{(int)stage.Stage}:{action.Ordinal}:{(int)action.Kind}:" +
                (action.Sql ?? action.TargetName)))
            .ToArray();

    private static MigrationCatalog CollectionCatalog() => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Json,
            Identity = "json:ddl-preview-tests",
            Fingerprint =
                "46a175c26b28c507a23e4cfc7fe110587fb98634b85eafb45808aa8c8d1af339",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Versioned immutable DDL preview fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "json:collection",
                Kind = MigrationObjectKind.Collection,
                SourceName = "documents",
                Facets =
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
                ],
            },
            CollectionColumn(
                "json:collection:key",
                MigrationDocumentCollectionContract.KeyColumnName,
                MigrationDocumentCollectionContract.KeyNativeType,
                MigrationDocumentCollectionContract.TextLogicalType,
                MigrationDocumentCollectionContract.KeyRole),
            CollectionColumn(
                "json:collection:document",
                MigrationDocumentCollectionContract.DocumentColumnName,
                MigrationDocumentCollectionContract.DocumentNativeType,
                MigrationDocumentCollectionContract.JsonLogicalType,
                MigrationDocumentCollectionContract.DocumentRole),
        ],
    };

    private static MigrationCatalogObject CollectionColumn(
        string objectId,
        string name,
        string nativeType,
        string logicalType,
        string role) => new()
        {
            ObjectId = objectId,
            Kind = MigrationObjectKind.Column,
            ParentObjectId = "json:collection",
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

    private static MigrationCatalogFacet Facet(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };

    private sealed class TemporaryPreviewDirectory : IDisposable
    {
        public TemporaryPreviewDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-ddl-preview-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "target.csharpdb");
        }

        public string DirectoryPath { get; }

        public string TargetPath { get; }

        public void Dispose()
        {
            if (Directory.Exists(DirectoryPath))
                Directory.Delete(DirectoryPath, recursive: true);
        }
    }
}
