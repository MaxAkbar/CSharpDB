using System.Text.Json;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbDdlScratchValidationTests
{
    private const string CollectionActionPrefix =
        "csharpdb-migration-json-collection-action/v1:";
    private const string SchemaEqualRule = "csharpdb.scratch.schema.equal";
    private const string PreviewBindingRule = "csharpdb.scratch.preview-binding";
    private const string ActionLimitRule = "csharpdb.scratch.limit.action-count";
    private const string SqlByteLimitRule = "csharpdb.scratch.limit.sql-bytes";
    private const string StatementCountRule =
        "csharpdb.scratch.sql.statement-count";
    private const string ExecuteRule = "csharpdb.scratch.sql.execute";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ValidateAsync_SupportedPlanPassesDeterministicallyWithoutFileWrites()
    {
        using var files = new TemporaryScratchDirectory();
        MigrationCatalog inspected = await InspectSyntheticAsync();
        MigrationCatalog catalog = inspected with
        {
            Source = inspected.Source with
            {
                Identity = Path.Combine(
                    files.DirectoryPath,
                    "private-source-identity.csharpdb"),
            },
        };
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
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
        CSharpDbDdlPreview reorderedPreview = CSharpDbDdlPreviewBuilder.Build(
            reorderedPlan,
            reorderedCatalog,
            cancellationToken: Ct);

        CSharpDbDdlScratchValidationReport first =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);
        CSharpDbDdlScratchValidationReport repeated =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);
        CSharpDbDdlScratchValidationReport reordered =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                reorderedPlan,
                reorderedCatalog,
                reorderedPreview,
                cancellationToken: Ct);

        AssertPass(first, plan, catalog, preview);
        Assert.Equal(Serialize(first), Serialize(repeated));
        Assert.Equal(Serialize(first), Serialize(reordered));
        Assert.Empty(Directory.EnumerateFileSystemEntries(files.DirectoryPath));
    }

    [Fact]
    public async Task ValidateAsync_TypedJsonCollectionPassesWithoutExposingSentinel()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);
        CSharpDbDdlPreviewAction action = Assert.Single(
            preview.Stages.SelectMany(stage => stage.Actions));

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);

        Assert.Equal(
            CSharpDbDdlPreviewActionKind.EnsureJsonDocumentCollection,
            action.Kind);
        Assert.Null(action.Sql);
        AssertPass(report, plan, catalog, preview);
        Assert.DoesNotContain(
            CollectionActionPrefix,
            Serialize(report) + report,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateAsync_TamperedPreviewIsRejectedBeforeParsingOrExecution()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);
        CSharpDbDdlPreviewStage stage =
            preview.Stages.First(candidate => candidate.Actions.Count > 0);
        CSharpDbDdlPreviewAction action =
            stage.Actions.First(candidate => candidate.Kind == CSharpDbDdlPreviewActionKind.Sql);
        const string privateSql = " -- private-tampered-preview-sql";
        CSharpDbDdlPreview tampered = preview with
        {
            Stages = preview.Stages
                .Select(candidate => candidate.Ordinal == stage.Ordinal
                    ? candidate with
                    {
                        Actions = candidate.Actions
                            .Select(candidateAction =>
                                candidateAction.Ordinal == action.Ordinal
                                    ? candidateAction with
                                    {
                                        Sql = candidateAction.Sql + privateSql,
                                    }
                                    : candidateAction)
                            .ToArray(),
                    }
                    : candidate)
                .ToArray(),
        };

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                tampered,
                cancellationToken: Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(PreviewBindingRule, report.RuleId);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(0, report.ParsedActionCount);
        Assert.Equal(0, report.ExecutedActionCount);
        Assert.Null(report.ExpectedSchemaDigest);
        Assert.Null(report.ActualSchemaDigest);
        Assert.Null(report.ReadinessStatus);
        Assert.Null(report.StageId);
        Assert.Null(report.ActionId);
        Assert.DoesNotContain(
            privateSql,
            Serialize(report) + report,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateAsync_MaliciousTargetVersionIsSanitizedOnBindingRejection()
    {
        const string privateVersion =
            "private-malicious-target-version-content-5931";
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        CSharpDbDdlPreview malicious = preview with
        {
            TargetCSharpDbVersion = privateVersion,
        };

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                malicious,
                cancellationToken: Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(PreviewBindingRule, report.RuleId);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(0, report.ParsedActionCount);
        Assert.Equal(0, report.ExecutedActionCount);
        Assert.Null(report.ReadinessStatus);
        Assert.DoesNotContain(
            privateVersion,
            Serialize(report) + report,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateAsync_PropagatesPreCancellation()
    {
        MigrationCatalog catalog = CollectionCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: cancellation.Token));
    }

    [Fact]
    public async Task ValidateAsync_ActionLimitReturnsStableSanitizedRejection()
    {
        MigrationCatalog catalog = await InspectSyntheticAsync();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);
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
        CSharpDbDdlPreview reorderedPreview = CSharpDbDdlPreviewBuilder.Build(
            reorderedPlan,
            reorderedCatalog,
            cancellationToken: Ct);
        var options = CSharpDbDdlScratchValidationOptions.Default with
        {
            MaxActionCount = 1,
        };

        CSharpDbDdlScratchValidationReport first =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                options,
                Ct);
        CSharpDbDdlScratchValidationReport repeated =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                reorderedPlan,
                reorderedCatalog,
                reorderedPreview,
                options,
                Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, first.Status);
        Assert.Equal(ActionLimitRule, first.RuleId);
        Assert.Null(first.HighestEvidence);
        Assert.Equal(0, first.ParsedActionCount);
        Assert.Equal(0, first.ExecutedActionCount);
        Assert.Null(first.ExpectedSchemaDigest);
        Assert.Null(first.ActualSchemaDigest);
        Assert.Empty(first.Differences);
        Assert.Equal(Serialize(first), Serialize(repeated));
        AssertSanitized(first, plan, catalog, preview);
    }

    [Fact]
    public async Task ValidateAsync_SmallMismatchedPreviewCannotBypassAuthoritativeActionLimit()
    {
        MigrationCatalog authoritativeCatalog = await InspectSyntheticAsync();
        MigrationPlan authoritativePlan = ReadyPlan(authoritativeCatalog);
        MigrationCatalog smallCatalog = CollectionCatalog();
        MigrationPlan smallPlan = ReadyPlan(smallCatalog);
        CSharpDbDdlPreview smallMismatchedPreview =
            CSharpDbDdlPreviewBuilder.Build(
                smallPlan,
                smallCatalog,
                cancellationToken: Ct);
        Assert.Single(
            smallMismatchedPreview.Stages.SelectMany(stage => stage.Actions));
        var options = CSharpDbDdlScratchValidationOptions.Default with
        {
            MaxActionCount = 1,
        };

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                authoritativePlan,
                authoritativeCatalog,
                smallMismatchedPreview,
                options,
                Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(ActionLimitRule, report.RuleId);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(0, report.ParsedActionCount);
        Assert.Equal(0, report.ExecutedActionCount);
        Assert.Empty(report.Differences);
    }

    [Fact]
    public async Task ValidateAsync_SmallMismatchedPreviewCannotBypassAuthoritativeSqlByteLimit()
    {
        MigrationCatalog authoritativeCatalog =
            EngineRejectCatalog(new string('x', 256));
        MigrationPlan authoritativePlan = ReadyPlan(authoritativeCatalog);
        MigrationCatalog smallCatalog = CollectionCatalog();
        MigrationPlan smallPlan = ReadyPlan(smallCatalog);
        CSharpDbDdlPreview smallMismatchedPreview =
            CSharpDbDdlPreviewBuilder.Build(
                smallPlan,
                smallCatalog,
                cancellationToken: Ct);
        var options = CSharpDbDdlScratchValidationOptions.Default with
        {
            MaxSqlUtf8Bytes = 1,
        };

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                authoritativePlan,
                authoritativeCatalog,
                smallMismatchedPreview,
                options,
                Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(SqlByteLimitRule, report.RuleId);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(0, report.ParsedActionCount);
        Assert.Equal(0, report.ExecutedActionCount);
        Assert.Null(report.ReadinessStatus);
        Assert.Empty(report.Differences);
    }

    [Fact]
    public async Task ValidateAsync_MultipleStatementsAreRejectedBeforeExecution()
    {
        const string privateInjectedSql =
            "1 = 1); SELECT (1";
        MigrationCatalog catalog = EngineRejectCatalog(privateInjectedSql);
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(StatementCountRule, report.RuleId);
        Assert.Equal(
            MigrationEvidenceLevel.CapabilityMatched,
            report.HighestEvidence);
        Assert.Equal(1, report.ParsedActionCount);
        Assert.Equal(0, report.ExecutedActionCount);
        Assert.Equal("constraints", report.StageId);
        Assert.Equal("constraints/action/0", report.ActionId);
        Assert.DoesNotContain(
            privateInjectedSql,
            Serialize(report) + report,
            StringComparison.Ordinal);
        AssertSanitized(report, plan, catalog, preview);
    }

    [Fact]
    public async Task ValidateAsync_EngineRejectionDoesNotPublishPrivateContent()
    {
        const string privateEngineText = "private_engine_message_content_9387";
        MigrationCatalog catalog = EngineRejectCatalog(privateEngineText);
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);
        plan = ReviewPlan(plan, catalog, preview);

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Rejected, report.Status);
        Assert.Equal(ExecuteRule, report.RuleId);
        Assert.Equal(MigrationEvidenceLevel.Parsed, report.HighestEvidence);
        Assert.Equal(2, report.ParsedActionCount);
        Assert.Equal(1, report.ExecutedActionCount);
        Assert.Equal("constraints", report.StageId);
        Assert.Equal("constraints/action/0", report.ActionId);
        Assert.DoesNotContain(
            privateEngineText,
            Serialize(report) + report,
            StringComparison.Ordinal);
        AssertSanitized(report, plan, catalog, preview);
    }

    [Fact]
    public async Task ValidateAsync_SchemaDifferencePublishesOnlyHashedIdentity()
    {
        MigrationCatalog catalog = SchemaDifferenceCatalog();
        MigrationPlan plan = ReadyPlan(catalog);
        CSharpDbDdlPreview preview =
            CSharpDbDdlPreviewBuilder.Build(plan, catalog, cancellationToken: Ct);

        CSharpDbDdlScratchValidationReport report =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                preview,
                cancellationToken: Ct);

        Assert.Equal(CSharpDbDdlScratchValidationStatus.Different, report.Status);
        Assert.Equal(MigrationEvidenceLevel.ScratchExecuted, report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlScratchValidator.SchemaDifferentRuleId,
            report.RuleId);
        CSharpDbDdlScratchValidationDifference difference =
            Assert.Single(report.Differences);
        Assert.Equal(0, difference.Ordinal);
        Assert.Equal(MigrationObjectKind.CheckConstraint, difference.Kind);
        AssertLowerSha256(difference.ObjectIdentityDigest);
        AssertLowerSha256(difference.ExpectedDefinitionDigest);
        AssertLowerSha256(difference.ActualDefinitionDigest);
        Assert.NotEqual(
            difference.ExpectedDefinitionDigest,
            difference.ActualDefinitionDigest);
        AssertSanitized(report, plan, catalog, preview);
    }

    private static void AssertPass(
        CSharpDbDdlScratchValidationReport report,
        MigrationPlan plan,
        MigrationCatalog catalog,
        CSharpDbDdlPreview preview)
    {
        MigrationPlan attached = ReviewPlan(plan, catalog, preview);
        Assert.Equal(CSharpDbDdlScratchValidationReport.CurrentFormat, report.Format);
        Assert.Equal(CSharpDbDdlScratchValidationStatus.Passed, report.Status);
        Assert.Equal(MigrationEvidenceLevel.ScratchExecuted, report.HighestEvidence);
        Assert.Equal(SchemaEqualRule, report.RuleId);
        Assert.Equal(preview.TargetCSharpDbVersion, report.TargetCSharpDbVersion);
        Assert.Equal(preview.CatalogDigest, report.CatalogDigest);
        Assert.Equal(preview.PlanContractDigest, report.PlanContractDigest);
        Assert.Equal(preview.GeneratedDdlDigest, report.GeneratedDdlDigest);
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(attached),
            report.AttachedPlanDigest);
        Assert.Equal(preview.Readiness.Status, report.ReadinessStatus);
        Assert.NotNull(report.ExpectedSchemaDigest);
        Assert.Equal(report.ExpectedSchemaDigest, report.ActualSchemaDigest);
        Assert.Null(report.StageId);
        Assert.Null(report.ActionId);
        Assert.Empty(report.Differences);
        Assert.Equal(
            preview.Stages
                .SelectMany(stage => stage.Actions)
                .Count(action => action.Kind == CSharpDbDdlPreviewActionKind.Sql),
            report.ParsedActionCount);
        Assert.Equal(
            preview.Stages.Sum(stage => stage.Actions.Count),
            report.ExecutedActionCount);
    }

    private static void AssertLowerSha256(string? value)
    {
        Assert.NotNull(value);
        Assert.Equal(64, value.Length);
        Assert.All(
            value,
            character => Assert.True(
                character is >= '0' and <= '9' or >= 'a' and <= 'f'));
    }

    private static void AssertSanitized(
        CSharpDbDdlScratchValidationReport report,
        MigrationPlan plan,
        MigrationCatalog catalog,
        CSharpDbDdlPreview preview)
    {
        string published = Serialize(report) + report;
        Assert.DoesNotContain(CollectionActionPrefix, published, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", published, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ALTER TABLE", published, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(catalog.Source.Identity, published, StringComparison.Ordinal);

        foreach (string name in catalog.Objects
                     .Select(item => item.SourceName)
                     .Where(name => name.Length >= 8))
        {
            Assert.DoesNotContain(name, published, StringComparison.Ordinal);
        }

        foreach (string name in plan.Objects
                     .Select(item => item.TargetName)
                     .OfType<string>()
                     .Where(name => name.Length >= 8))
        {
            Assert.DoesNotContain(name, published, StringComparison.Ordinal);
        }

        foreach (string sql in preview.Stages
                     .SelectMany(stage => stage.Actions)
                     .Select(action => action.Sql)
                     .OfType<string>())
        {
            Assert.DoesNotContain(sql, published, StringComparison.Ordinal);
        }
    }

    private static string Serialize(CSharpDbDdlScratchValidationReport report) =>
        JsonSerializer.Serialize(report);

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

    private static MigrationPlan ReviewPlan(
        MigrationPlan plan,
        MigrationCatalog catalog,
        CSharpDbDdlPreview preview) =>
        CSharpDbDdlPreviewBuilder.AttachGeneratedDdlDigest(
            plan,
            catalog,
            preview,
            cancellationToken: Ct);

    private static MigrationCatalog EngineRejectCatalog(string privateEngineText) => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:private-engine-reject-source",
            Fingerprint =
                "ee3a7dc882aff45db33eab031a6c7f76f42af41a52828ba96994040f5c966fd3",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable private engine rejection fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "private:table",
                Kind = MigrationObjectKind.Table,
                SourceName = "private source table 8273",
            },
            new MigrationCatalogObject
            {
                ObjectId = "private:column",
                Kind = MigrationObjectKind.Column,
                ParentObjectId = "private:table",
                SourceName = "private source column 8273",
                NativeType = "INT64",
                Facets =
                [
                    Facet("logicalType", "signedInteger"),
                    Facet("nullable", "false"),
                ],
            },
            new MigrationCatalogObject
            {
                ObjectId = "private:check",
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = "private:table",
                SourceName = "private source check 8273",
                DependsOn = ["private:column"],
                Facets =
                [
                    Facet("deterministic", "true"),
                    Facet("rowLocal", "true"),
                    Facet("targetSql", $"{privateEngineText} = 1"),
                ],
            },
        ],
    };

    private static MigrationCatalog SchemaDifferenceCatalog() => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:private-schema-difference-source",
            Fingerprint =
                "c39993f70a2ff7cfa426cdd38e9936ecfb33b0f5a2b0a68821f87b31ba369853",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable private schema difference fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "difference:table",
                Kind = MigrationObjectKind.Table,
                SourceName = "private difference table 7412",
            },
            new MigrationCatalogObject
            {
                ObjectId = "difference:column",
                Kind = MigrationObjectKind.Column,
                ParentObjectId = "difference:table",
                SourceName = "value",
                NativeType = "INT64",
                Facets =
                [
                    Facet("logicalType", "signedInteger"),
                    Facet("nullable", "false"),
                ],
            },
            new MigrationCatalogObject
            {
                ObjectId = "difference:check",
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = "difference:table",
                SourceName = "private difference check 7412",
                DependsOn = ["difference:column"],
                Facets =
                [
                    Facet("deterministic", "true"),
                    Facet("rowLocal", "true"),
                    Facet("targetSql", "value = 1 "),
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
            Identity = "json:ddl-scratch-validation-tests",
            Fingerprint =
                "66d8368c9f16ee299cd96f69350bc9f12fceda44fc09658a1c7b06bd21ca2342",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Versioned immutable scratch validation fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "json:collection",
                Kind = MigrationObjectKind.Collection,
                SourceName = "private documents collection",
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
                Facet(
                    MigrationDocumentCollectionContract.LogicalTypeFacet,
                    logicalType),
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

    private sealed class TemporaryScratchDirectory : IDisposable
    {
        public TemporaryScratchDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-ddl-scratch-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
        }

        public string DirectoryPath { get; }

        public void Dispose()
        {
            if (Directory.Exists(DirectoryPath))
                Directory.Delete(DirectoryPath, recursive: true);
        }
    }
}
