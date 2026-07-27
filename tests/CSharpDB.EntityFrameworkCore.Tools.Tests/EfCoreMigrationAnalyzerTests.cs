using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.EntityFrameworkCore.Tools.Fixtures;
using CSharpDB.Migration;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreMigrationAnalyzerTests
{
    [Fact]
    public async Task AnalyzeScratchAsync_ExecutesEveryFixturePrefix()
    {
        EfCoreMigrationScratchAnalysisReport report =
            await AnalyzeScratchAsync<FixtureContext>();

        Assert.True(
            report.Outcome ==
                EfCoreMigrationScratchAnalysisOutcome.Passed,
            JsonSerializer.Serialize(report));
        Assert.Equal(
            MigrationCompatibilityStatus.Compatible,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(2, report.ScratchChain.PrefixCount);
        Assert.Equal(2, report.ScratchChain.AppliedPrefixCount);
        Assert.Equal(
            2,
            report.ScratchChain.SchemaVerifiedPrefixCount);
        Assert.Equal(
            2,
            report.ScratchChain.RoundTripVerifiedPrefixCount);
        Assert.Equal(2, report.ScratchChain.IdempotentApplyCount);
        Assert.Equal(2, report.ScratchChain.Prefixes.Count);
        Assert.True(report.ScratchChain.ResourcesDisposed);
        Assert.False(report.ScratchChain.DataPreflightCompleted);

        Assert.True(EfCoreScratchReportSanitizer.TrySanitize(
            report,
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out EfCoreMigrationScratchAnalysisReport? sanitized));
        Assert.NotNull(sanitized);

        Assert.False(EfCoreScratchReportSanitizer.TrySanitize(
            report with
            {
                Outcome =
                    EfCoreMigrationScratchAnalysisOutcome.Blocked,
            },
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));
        Assert.False(EfCoreScratchReportSanitizer.TrySanitize(
            report with
            {
                ScratchChain = report.ScratchChain with
                {
                    SecondIdempotentSchemaDigest =
                        new string('a', 64),
                },
            },
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));
        Assert.False(EfCoreScratchReportSanitizer.TrySanitize(
            report with
            {
                ScratchChain = report.ScratchChain with
                {
                    Algorithm = "forged",
                },
            },
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));
        Assert.False(EfCoreScratchReportSanitizer.TrySanitize(
            report with
            {
                ScratchChain = report.ScratchChain with
                {
                    DataPreflightCompleted = true,
                },
            },
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));

        EfCoreMigrationScratchAnalysisReport partialIdempotentFailure =
            report with
            {
                Outcome =
                    EfCoreMigrationScratchAnalysisOutcome.Failed,
                Status = MigrationCompatibilityStatus.Unknown,
                RuleId =
                    EfCoreMigrationScratchAnalysisRules
                        .IdempotenceFailed,
                ScratchChain = report.ScratchChain with
                {
                    Outcome =
                        EfCoreMigrationScratchAnalysisOutcome.Failed,
                    IdempotentApplyCount = 1,
                    IdempotentCommandCount =
                        report.ScratchChain
                            .IdempotentCommandCount / 2,
                    FirstIdempotentSchemaDigest = null,
                    FirstIdempotentHistoryDigest = null,
                    SecondIdempotentSchemaDigest = null,
                    SecondIdempotentHistoryDigest = null,
                },
            };
        Assert.True(EfCoreScratchReportSanitizer.TrySanitize(
            partialIdempotentFailure,
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));
        Assert.False(EfCoreScratchReportSanitizer.TrySanitize(
            partialIdempotentFailure with
            {
                ScratchChain =
                    partialIdempotentFailure.ScratchChain with
                    {
                        FirstIdempotentSchemaDigest =
                            "TOP-SECRET",
                        FirstIdempotentHistoryDigest =
                            "TOP-SECRET",
                    },
            },
            report.GenerationPreflight.AssemblyDigest,
            typeof(FixtureContext).FullName!,
            out _));
    }

    [Fact]
    public async Task AnalyzeScratchAsync_RejectsSqlGenerationServiceOverrides()
    {
        EfCoreAnalysisException generatorException =
            await Assert.ThrowsAsync<EfCoreAnalysisException>(
                async () =>
                    await AnalyzeScratchAsync<
                        ScratchSqlGeneratorOverrideFixtureContext>());
        EfCoreAnalysisException dependencyException =
            await Assert.ThrowsAsync<EfCoreAnalysisException>(
                async () =>
                    await AnalyzeScratchAsync<
                        ScratchSqlGenerationDependencyOverrideFixtureContext>());
        EfCoreAnalysisException customExtensionException =
            await Assert.ThrowsAsync<EfCoreAnalysisException>(
                async () =>
                    await AnalyzeScratchAsync<
                        ScratchCustomOptionsExtensionFixtureContext>());

        Assert.Equal(
            EfCoreAnalysisFailureKind.AnalysisFailed,
            generatorException.Kind);
        Assert.Equal(
            EfCoreAnalysisFailureKind.AnalysisFailed,
            dependencyException.Kind);
        Assert.Equal(
            EfCoreAnalysisFailureKind.AnalysisFailed,
            customExtensionException.Kind);
    }

    [Fact]
    public async Task AnalyzeAsync_LoadsFactorylessGenericHostWithTargetOnlyHosting()
    {
        EfCoreMigrationAnalysisReport report =
            await AnalyzeAsync<HostedFixtureContext>();

        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.Bound,
            report.HighestEvidence);
        Assert.Equal(
            typeof(HostedFixtureContext).FullName,
            report.Context);
        EfCoreMigrationAnalysisMigration migration =
            Assert.Single(report.Migrations);
        Assert.Equal(
            "202607250004_HostedInitialCreate",
            migration.MigrationId);
        Assert.Collection(
            migration.Operations,
            operation =>
            {
                Assert.Equal(
                    EfCoreMigrationDirection.Up,
                    operation.Direction);
                Assert.Equal(
                    EfCoreMigrationOperationKind.CreateTable,
                    operation.Kind);
            },
            operation =>
            {
                Assert.Equal(
                    EfCoreMigrationDirection.Down,
                    operation.Direction);
                Assert.Equal(
                    EfCoreMigrationOperationKind.DropTable,
                    operation.Kind);
            });
    }

    [Fact]
    public async Task AnalyzeAsync_ProvesRawSqlThroughBoundedDdlAnalyzer()
    {
        EfCoreMigrationAnalysisReport report =
            await AnalyzeAsync<RawSqlFixtureContext>();

        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            report.Status);
        EfCoreMigrationAnalysisMigration migration =
            Assert.Single(report.Migrations);
        Assert.Equal("202607250005_RawSql", migration.MigrationId);
        AssertRawSqlOperation(Assert.Single(migration.Operations));
    }

    [Fact]
    public async Task AnalyzeAsync_UsesScaffoldedTargetModelsForBothDirections()
    {
        EfCoreMigrationAnalysisReport report =
            await AnalyzeAsync<TargetModelFixtureContext>();

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Equal(2, report.Migrations.Count);
        Assert.Equal(
            "202607250006_TargetModelInitial",
            report.Migrations[0].MigrationId);
        Assert.Equal(
            "202607250007_ReplaceLegacyPrimaryKey",
            report.Migrations[1].MigrationId);

        EfCoreMigrationOperationFinding currentModelOperation =
            Assert.Single(
                report.Migrations[0].Operations,
                operation =>
                    operation.Direction ==
                        EfCoreMigrationDirection.Up &&
                    operation.Kind ==
                        EfCoreMigrationOperationKind
                            .AddPrimaryKey);
        AssertModelBoundPrimaryKeyRejection(
            currentModelOperation);

        EfCoreMigrationOperationFinding previousModelOperation =
            Assert.Single(
                report.Migrations[1].Operations,
                operation =>
                    operation.Direction ==
                        EfCoreMigrationDirection.Down &&
                    operation.Kind ==
                        EfCoreMigrationOperationKind
                            .AddPrimaryKey);
        AssertModelBoundPrimaryKeyRejection(
            previousModelOperation);

        EfCoreMigrationOperationFinding newPrimaryKey =
            Assert.Single(
                report.Migrations[1].Operations,
                operation =>
                    operation.Direction ==
                        EfCoreMigrationDirection.Up &&
                    operation.Kind ==
                        EfCoreMigrationOperationKind
                            .AddPrimaryKey);
        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            newPrimaryKey.Status);
        Assert.Equal(1, newPrimaryKey.CommandCount);
        Assert.NotNull(newPrimaryKey.GeneratedSqlDigest);
    }

    private static void AssertModelBoundPrimaryKeyRejection(
        EfCoreMigrationOperationFinding operation)
    {
        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            operation.Status);
        Assert.Equal(
            EfCoreMigrationAnalysisRules.GenerationUnsupported,
            operation.RuleId);
        Assert.Equal(MigrationEvidenceLevel.Bound, operation.Evidence);
        Assert.Equal(0, operation.CommandCount);
        Assert.Null(operation.GeneratedSqlDigest);
    }

    private static void AssertRawSqlOperation(
        EfCoreMigrationOperationFinding operation)
    {
        Assert.Equal(
            EfCoreMigrationOperationKind.RawSql,
            operation.Kind);
        Assert.Equal(
            EfCoreMigrationAnalysisRules.RawSqlBound,
            operation.RuleId);
        Assert.Equal(1, operation.CommandCount);
        Assert.NotNull(operation.GeneratedSqlDigest);
    }

    private static async ValueTask<EfCoreMigrationAnalysisReport>
        AnalyzeAsync<TContext>()
    {
        EfCoreMigrationAnalysisRequest request =
            await CreateRequestAsync<TContext>();
        return await EfCoreMigrationAnalyzer.AnalyzeAsync(
            request,
            TestContext.Current.CancellationToken);
    }

    private static async ValueTask<
        EfCoreMigrationScratchAnalysisReport>
        AnalyzeScratchAsync<TContext>()
    {
        EfCoreMigrationAnalysisRequest request =
            await CreateRequestAsync<TContext>();
        return await EfCoreMigrationAnalyzer.AnalyzeScratchAsync(
            request,
            TestContext.Current.CancellationToken);
    }

    private static async ValueTask<EfCoreMigrationAnalysisRequest>
        CreateRequestAsync<TContext>()
    {
        string assemblyPath = typeof(TContext).Assembly.Location;
        byte[] assemblyBytes =
            await File.ReadAllBytesAsync(
                assemblyPath,
                TestContext.Current.CancellationToken);
        string assemblyDigest = Convert.ToHexString(
                SHA256.HashData(assemblyBytes))
            .ToLowerInvariant();

        return new EfCoreMigrationAnalysisRequest
        {
            AssemblyPath = assemblyPath,
            AssemblyDigest = assemblyDigest,
            Context = typeof(TContext).FullName!,
        };
    }
}
