using System.Reflection;
using System.Text;
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
    public async Task BuildBounded_DefaultLimitsMatchExistingBuildContract()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);

        CSharpDbDdlPreview existing =
            CSharpDbDdlPreviewBuilder.Build(
                plan,
                catalog,
                cancellationToken: Ct);
        CSharpDbDdlPreview bounded =
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                cancellationToken: Ct);

        Assert.Equal(existing.Format, bounded.Format);
        Assert.Equal(
            existing.TargetCSharpDbVersion,
            bounded.TargetCSharpDbVersion);
        Assert.Equal(existing.CatalogDigest, bounded.CatalogDigest);
        Assert.Equal(
            existing.PlanContractDigest,
            bounded.PlanContractDigest);
        Assert.Equal(
            existing.GeneratedDdlDigest,
            bounded.GeneratedDdlDigest);
        Assert.Equal(existing.Readiness.Status, bounded.Readiness.Status);
        Assert.Equal(Project(existing), Project(bounded));
        Assert.Single(
            typeof(CSharpDbDdlPreviewBuilder)
                .GetMethods(BindingFlags.Static | BindingFlags.Public),
            method => method.Name == nameof(
                CSharpDbDdlPreviewBuilder.Build));
        Assert.Single(
            typeof(CSharpDbDdlPreviewBuilder)
                .GetMethods(BindingFlags.Static | BindingFlags.Public),
            method => method.Name == nameof(
                CSharpDbDdlPreviewBuilder.BuildBounded));
    }

    [Fact]
    public async Task BuildBounded_ActionAndAggregateLimitsAreDeterministic()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview unbounded =
            CSharpDbDdlPreviewBuilder.Build(
                plan,
                catalog,
                cancellationToken: Ct);
        int actionCount = unbounded.Stages.Sum(stage => stage.Actions.Count);
        long sqlUtf8Bytes = unbounded.Stages
            .SelectMany(stage => stage.Actions)
            .Where(action => action.Kind == CSharpDbDdlPreviewActionKind.Sql)
            .Sum(action => (long)Encoding.UTF8.GetByteCount(action.Sql!));
        Assert.True(actionCount > 1);
        Assert.True(sqlUtf8Bytes > 1);

        var actionOptions = CSharpDbDdlPreviewBuildOptions.Default with
        {
            MaxActionCount = actionCount - 1,
        };
        var aggregateOptions = CSharpDbDdlPreviewBuildOptions.Default with
        {
            MaxAggregateSqlUtf8Bytes = sqlUtf8Bytes - 1,
        };

        CSharpDbDdlPreviewLimitException firstAction =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    plan,
                    catalog,
                    actionOptions,
                    cancellationToken: Ct));
        CSharpDbDdlPreviewLimitException repeatedAction =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    plan,
                    catalog,
                    actionOptions,
                    cancellationToken: Ct));
        CSharpDbDdlPreviewLimitException aggregate =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    plan,
                    catalog,
                    aggregateOptions,
                    cancellationToken: Ct));

        Assert.Equal(
            CSharpDbDdlPreviewLimitKind.ActionCount,
            firstAction.Kind);
        Assert.Equal(firstAction.Kind, repeatedAction.Kind);
        Assert.Equal(firstAction.Message, repeatedAction.Message);
        Assert.Equal(
            CSharpDbDdlPreviewLimitKind.AggregateSqlUtf8Bytes,
            aggregate.Kind);
    }

    [Fact]
    public void BuildBounded_RejectsOversizedTargetSqlWithSanitizedLimit()
    {
        string privateTargetSql = new('x', 64 * 1024);
        MigrationCatalog catalog =
            TargetSqlCheckCatalog(privateTargetSql);
        MigrationPlan plan = ReadyPlan(catalog);
        var options = CSharpDbDdlPreviewBuildOptions.Default with
        {
            MaxSqlCharactersPerAction = 1024,
        };

        CSharpDbDdlPreviewLimitException error =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    plan,
                    catalog,
                    options,
                    cancellationToken: Ct));

        Assert.Equal(
            CSharpDbDdlPreviewLimitKind.SqlActionSize,
            error.Kind);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(
            privateTargetSql,
            error.ToString(),
            StringComparison.Ordinal);

        string privateUtf8TargetSql = new('\u00e9', 2048);
        MigrationCatalog utf8Catalog =
            TargetSqlCheckCatalog(privateUtf8TargetSql);
        MigrationPlan utf8Plan = ReadyPlan(utf8Catalog);
        CSharpDbDdlPreviewLimitException utf8Error =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    utf8Plan,
                    utf8Catalog,
                    CSharpDbDdlPreviewBuildOptions.Default with
                    {
                        MaxSqlCharactersPerAction = 4096,
                        MaxSqlUtf8BytesPerAction = 3000,
                    },
                    cancellationToken: Ct));

        Assert.Equal(
            CSharpDbDdlPreviewLimitKind.SqlActionSize,
            utf8Error.Kind);
        Assert.DoesNotContain(
            privateUtf8TargetSql,
            utf8Error.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public void BuildBounded_ValidatesHardCeilingsAndPreCancellation()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                CSharpDbDdlPreviewBuildOptions.Default with
                {
                    MaxActionCount =
                        CSharpDbDdlPreviewBuildOptions
                            .HardMaxActionCount + 1,
                },
                cancellationToken: Ct));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                CSharpDbDdlPreviewBuildOptions.Default with
                {
                    MaxSqlCharactersPerAction =
                        CSharpDbDdlPreviewBuildOptions
                            .HardMaxSqlCharactersPerAction + 1,
                },
                cancellationToken: Ct));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                CSharpDbDdlPreviewBuildOptions.Default with
                {
                    MaxSqlUtf8BytesPerAction =
                        CSharpDbDdlPreviewBuildOptions
                            .HardMaxSqlUtf8BytesPerAction + 1,
                },
                cancellationToken: Ct));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                CSharpDbDdlPreviewBuildOptions.Default with
                {
                    MaxAggregateSqlUtf8Bytes =
                        CSharpDbDdlPreviewBuildOptions
                            .HardMaxAggregateSqlUtf8Bytes + 1,
                },
                cancellationToken: Ct));

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                cancellationToken: cancellation.Token));
    }

    [Fact]
    public async Task AttachBounded_BuildsAndAttachesAuthoritativeDigest()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);

        MigrationPlan attached =
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: Ct);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.BuildBounded(
                attached,
                catalog,
                cancellationToken: Ct);

        Assert.NotNull(attached.GeneratedDdlDigest);
        Assert.Equal(
            preview.GeneratedDdlDigest,
            attached.GeneratedDdlDigest);
        Assert.Null(plan.GeneratedDdlDigest);
        Assert.Single(
            typeof(CSharpDbDdlPreviewBuilder)
                .GetMethods(BindingFlags.Static | BindingFlags.Public),
            method => method.Name == nameof(
                CSharpDbDdlPreviewBuilder
                    .BuildAndAttachGeneratedDdlDigestBounded));
        MigrationPlanReadinessValidator.ValidateForApply(attached, catalog);
    }

    [Fact]
    public async Task AttachBounded_IsDeterministicAndPreservesMatchingDigest()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);

        MigrationPlan first =
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: Ct);
        MigrationPlan repeated =
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: Ct);
        MigrationPlan preserved =
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                first,
                catalog,
                cancellationToken: Ct);

        Assert.Equal(first.GeneratedDdlDigest, repeated.GeneratedDdlDigest);
        Assert.Same(first, preserved);
    }

    [Fact]
    public async Task AttachBounded_RejectsConflictingExistingDigest()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog) with
        {
            GeneratedDdlDigest = new string('0', 64),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: Ct));

        Assert.Contains(
            "different generated DDL digest",
            error.Message,
            StringComparison.Ordinal);
        Assert.Equal(new string('0', 64), plan.GeneratedDdlDigest);
    }

    [Fact]
    public async Task AttachBounded_HonorsRenderLimits()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);

        CSharpDbDdlPreviewLimitException error =
            Assert.Throws<CSharpDbDdlPreviewLimitException>(() =>
                CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                    plan,
                    catalog,
                    CSharpDbDdlPreviewBuildOptions.Default with
                    {
                        MaxActionCount = 1,
                    },
                    cancellationToken: Ct));

        Assert.Equal(
            CSharpDbDdlPreviewLimitKind.ActionCount,
            error.Kind);
        Assert.Null(plan.GeneratedDdlDigest);
    }

    [Fact]
    public void AttachBounded_HonorsPreCancellation()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(() =>
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: cancellation.Token));
        Assert.Null(plan.GeneratedDdlDigest);
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

    private static MigrationCatalog TargetSqlCheckCatalog(string targetSql) => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:ddl-preview-target-sql-limit",
            Fingerprint =
                "26bac50e3e9ff88f8f719f8021e3dc651c47ba142471098701ea4fd45f8d2afa",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description =
                    "Immutable bounded DDL preview target-SQL fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "limit:table",
                Kind = MigrationObjectKind.Table,
                SourceName = "bounded_preview_table",
            },
            new MigrationCatalogObject
            {
                ObjectId = "limit:column",
                Kind = MigrationObjectKind.Column,
                ParentObjectId = "limit:table",
                SourceName = "bounded_preview_column",
                NativeType = "INT64",
                Facets =
                [
                    Facet("logicalType", "signedInteger"),
                    Facet("nullable", "false"),
                ],
            },
            new MigrationCatalogObject
            {
                ObjectId = "limit:check",
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = "limit:table",
                SourceName = "bounded_preview_check",
                DependsOn = ["limit:column"],
                Facets =
                [
                    Facet("deterministic", "true"),
                    Facet("rowLocal", "true"),
                    Facet("targetSql", targetSql),
                ],
            },
        ],
    };

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
