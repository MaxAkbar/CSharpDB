using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.EntityFrameworkCore.Tools.Fixtures;
using CSharpDB.Migration;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreScratchChainIsolationTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnalyzeScratchAsync_TargetModelMismatchReportsSchemaDifferent()
    {
        EfCoreMigrationScratchAnalysisReport report =
            await AnalyzeScratchAsync<
                ScratchSchemaMismatchFixtureContext>();

        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            report.GenerationPreflight.Status);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            report.Outcome);
        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules.SchemaDifferent,
            report.RuleId);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            report.ScratchChain.Outcome);
        Assert.Equal(1, report.ScratchChain.PrefixCount);
        Assert.Equal(1, report.ScratchChain.AppliedPrefixCount);
        Assert.Equal(
            0,
            report.ScratchChain.SchemaVerifiedPrefixCount);
        Assert.Equal(0, report.ScratchChain.DownPrefixCount);
        Assert.Equal(
            0,
            report.ScratchChain.RoundTripVerifiedPrefixCount);
        Assert.True(report.ScratchChain.ExecutedCommandCount > 0);
        Assert.NotNull(report.ScratchChain.ExecutedSqlDigest);
        Assert.True(report.ScratchChain.ResourcesDisposed);
        AssertSanitizes<
            ScratchSchemaMismatchFixtureContext>(report);
    }

    [Fact]
    public async Task AnalyzeScratchAsync_IncorrectDownReportsRoundTripDifferent()
    {
        EfCoreMigrationScratchAnalysisReport report =
            await AnalyzeScratchAsync<
                ScratchRoundTripMismatchFixtureContext>();

        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            report.GenerationPreflight.Status);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            report.Outcome);
        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules.RoundTripDifferent,
            report.RuleId);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            report.ScratchChain.Outcome);
        Assert.Equal(1, report.ScratchChain.PrefixCount);
        Assert.Equal(1, report.ScratchChain.AppliedPrefixCount);
        Assert.Equal(
            1,
            report.ScratchChain.SchemaVerifiedPrefixCount);
        Assert.Equal(1, report.ScratchChain.DownPrefixCount);
        Assert.Equal(0, report.ScratchChain.ReappliedPrefixCount);
        Assert.Equal(
            0,
            report.ScratchChain.RoundTripVerifiedPrefixCount);
        Assert.True(report.ScratchChain.ExecutedCommandCount > 0);
        Assert.NotNull(report.ScratchChain.ExecutedSqlDigest);
        Assert.True(report.ScratchChain.ResourcesDisposed);
        AssertSanitizes<
            ScratchRoundTripMismatchFixtureContext>(report);
    }

    [Fact]
    public async Task AnalyzeScratchAsync_NeverOpensConfiguredSentinelDatabase()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-ef-scratch-isolation",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string sentinelPath = Path.Combine(
            directory,
            "configured-target.db");
        string variable =
            ScratchSentinelIsolationFixtureContext
                .SentinelPathEnvironmentVariable;
        string? previousValue =
            Environment.GetEnvironmentVariable(variable);

        try
        {
            Environment.SetEnvironmentVariable(
                variable,
                sentinelPath);
            Assert.False(File.Exists(sentinelPath));
            Assert.Empty(
                Directory.EnumerateFileSystemEntries(directory));

            EfCoreMigrationScratchAnalysisReport report =
                await AnalyzeScratchAsync<
                    ScratchSentinelIsolationFixtureContext>();

            Assert.Equal(
                MigrationCompatibilityStatus.Conditional,
                report.GenerationPreflight.Status);
            Assert.Equal(
                EfCoreMigrationScratchAnalysisOutcome.Passed,
                report.Outcome);
            Assert.Equal(
                MigrationCompatibilityStatus.Compatible,
                report.Status);
            Assert.Equal(
                MigrationEvidenceLevel.ScratchExecuted,
                report.HighestEvidence);
            Assert.Equal(
                EfCoreMigrationScratchAnalysisRules.ScratchPassed,
                report.RuleId);
            Assert.Equal(1, report.ScratchChain.PrefixCount);
            Assert.Equal(1, report.ScratchChain.AppliedPrefixCount);
            Assert.Equal(
                1,
                report.ScratchChain.SchemaVerifiedPrefixCount);
            Assert.Equal(1, report.ScratchChain.DownPrefixCount);
            Assert.Equal(1, report.ScratchChain.ReappliedPrefixCount);
            Assert.Equal(
                1,
                report.ScratchChain.RoundTripVerifiedPrefixCount);
            Assert.Equal(2, report.ScratchChain.IdempotentApplyCount);
            Assert.True(report.ScratchChain.ResourcesDisposed);
            AssertSanitizes<
                ScratchSentinelIsolationFixtureContext>(report);

            EfCoreMigrationScratchPrefixEvidence prefix =
                Assert.Single(report.ScratchChain.Prefixes);
            Assert.Equal(
                prefix.ExpectedSchemaDigest,
                prefix.AppliedSchemaDigest);
            Assert.Equal(
                prefix.ExpectedSchemaDigest,
                prefix.ReappliedSchemaDigest);
            Assert.Equal(
                prefix.ExpectedHistoryDigest,
                prefix.AppliedHistoryDigest);
            Assert.Equal(
                prefix.ExpectedHistoryDigest,
                prefix.ReappliedHistoryDigest);

            Assert.False(File.Exists(sentinelPath));
            Assert.Empty(
                Directory.EnumerateFileSystemEntries(directory));
            string serialized = JsonSerializer.Serialize(report);
            Assert.False(serialized.Contains(
                sentinelPath,
                StringComparison.OrdinalIgnoreCase));
            Assert.False(serialized.Contains(
                Path.GetFileName(sentinelPath),
                StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                variable,
                previousValue);
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static async ValueTask<
        EfCoreMigrationScratchAnalysisReport>
        AnalyzeScratchAsync<TContext>()
    {
        EfCoreMigrationAnalysisRequest request =
            await CreateRequestAsync<TContext>();
        return await EfCoreMigrationAnalyzer.AnalyzeScratchAsync(
            request,
            Ct);
    }

    private static async ValueTask<EfCoreMigrationAnalysisRequest>
        CreateRequestAsync<TContext>()
    {
        string assemblyPath = typeof(TContext).Assembly.Location;
        byte[] assemblyBytes = await File.ReadAllBytesAsync(
            assemblyPath,
            Ct);
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

    private static void AssertSanitizes<TContext>(
        EfCoreMigrationScratchAnalysisReport report)
    {
        Assert.True(EfCoreScratchReportSanitizer.TrySanitize(
            report,
            report.GenerationPreflight.AssemblyDigest,
            typeof(TContext).FullName!,
            out EfCoreMigrationScratchAnalysisReport? sanitized));
        Assert.NotNull(sanitized);
    }
}
