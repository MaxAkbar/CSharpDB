using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Tests;

public sealed class EfCoreMigrationScratchAnalysisContractTests
{
    [Fact]
    public void FormatsKeepGenerationAndScratchEvidenceDistinct()
    {
        EfCoreMigrationScratchAnalysisReport report = CreateReport();

        Assert.Equal(
            "csharpdb-ef-migration-analysis/v1",
            EfCoreMigrationAnalysisReport.CurrentFormat);
        Assert.Equal(
            "csharpdb-ef-migration-scratch-analysis/v1",
            EfCoreMigrationScratchAnalysisReport.CurrentFormat);
        Assert.Equal(
            "csharpdb-ef-scratch-chain/v1",
            EfCoreMigrationScratchChainProof.CurrentFormat);
        Assert.Equal(
            EfCoreMigrationAnalysisReport.CurrentFormat,
            report.GenerationPreflight.Format);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisReport.CurrentFormat,
            report.Format);
        Assert.Equal(
            EfCoreMigrationScratchChainProof.CurrentFormat,
            report.ScratchChain.Format);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Passed,
            report.Outcome);
        Assert.Equal(report.ScratchChain.Outcome, report.Outcome);
    }

    [Fact]
    public void ScratchChainMakesEmptyDatabaseScopeAndNoDataProofExplicit()
    {
        EfCoreMigrationScratchChainProof scratchChain = CreateScratchChain();

        Assert.Equal(
            EfCoreMigrationScratchProofScope.EmptyDatabase,
            scratchChain.ProofScope);
        Assert.False(scratchChain.DataPreflightCompleted);
        Assert.Equal(
            "csharpdb-ef-empty-chain/v1",
            scratchChain.Algorithm);
        Assert.True(scratchChain.ResourcesDisposed);
        Assert.Equal(1, scratchChain.PrefixCount);
        Assert.Equal(1, scratchChain.AppliedPrefixCount);
        Assert.Equal(1, scratchChain.SchemaVerifiedPrefixCount);
        Assert.Equal(1, scratchChain.DownPrefixCount);
        Assert.Equal(1, scratchChain.ReappliedPrefixCount);
        Assert.Equal(1, scratchChain.RoundTripVerifiedPrefixCount);
        Assert.Equal(2, scratchChain.IdempotentApplyCount);
        Assert.Equal(7, scratchChain.ExecutedCommandCount);
        Assert.Equal(2, scratchChain.IdempotentCommandCount);
        Assert.Equal("executed-sql-digest", scratchChain.ExecutedSqlDigest);
        Assert.Equal("idempotent-sql-digest", scratchChain.IdempotentSqlDigest);
        Assert.Equal(
            "first-idempotent-schema",
            scratchChain.FirstIdempotentSchemaDigest);
        Assert.Equal(
            "first-idempotent-history",
            scratchChain.FirstIdempotentHistoryDigest);
        Assert.Equal(
            "second-idempotent-schema",
            scratchChain.SecondIdempotentSchemaDigest);
        Assert.Equal(
            "second-idempotent-history",
            scratchChain.SecondIdempotentHistoryDigest);
    }

    [Fact]
    public void ScratchPrefixEvidenceCarriesEverySchemaAndHistoryDigest()
    {
        EfCoreMigrationScratchPrefixEvidence prefix =
            Assert.Single(CreateScratchChain().Prefixes);

        Assert.Equal("expected-schema", prefix.ExpectedSchemaDigest);
        Assert.Equal("expected-history", prefix.ExpectedHistoryDigest);
        Assert.Equal("applied-schema", prefix.AppliedSchemaDigest);
        Assert.Equal("applied-history", prefix.AppliedHistoryDigest);
        Assert.Equal("down-schema", prefix.DownSchemaDigest);
        Assert.Equal("down-history", prefix.DownHistoryDigest);
        Assert.Equal("reapplied-schema", prefix.ReappliedSchemaDigest);
        Assert.Equal("reapplied-history", prefix.ReappliedHistoryDigest);
    }

    [Theory]
    [InlineData(EfCoreMigrationScratchAnalysisOutcome.Blocked)]
    [InlineData(EfCoreMigrationScratchAnalysisOutcome.Failed)]
    public void IncompleteOutcomeAllowsMissingIdempotentEndStateEvidence(
        EfCoreMigrationScratchAnalysisOutcome outcome)
    {
        var scratchChain = new EfCoreMigrationScratchChainProof
        {
            Outcome = outcome,
            ResourcesDisposed = true,
        };
        EfCoreMigrationScratchAnalysisReport report = CreateReport() with
        {
            Outcome = outcome,
            ScratchChain = scratchChain,
        };

        Assert.Equal(outcome, report.Outcome);
        Assert.Null(scratchChain.FirstIdempotentSchemaDigest);
        Assert.Null(scratchChain.FirstIdempotentHistoryDigest);
        Assert.Null(scratchChain.SecondIdempotentSchemaDigest);
        Assert.Null(scratchChain.SecondIdempotentHistoryDigest);
    }

    [Fact]
    public void WireRoundTripPreservesIndependentOutcomesForSanitization()
    {
        EfCoreMigrationScratchAnalysisReport mismatched = CreateReport() with
        {
            Outcome = EfCoreMigrationScratchAnalysisOutcome.Passed,
            ScratchChain = CreateScratchChain() with
            {
                Outcome = EfCoreMigrationScratchAnalysisOutcome.Failed,
            },
        };
        JsonSerializerOptions options = CreateJsonOptions();

        string json = JsonSerializer.Serialize(mismatched, options);
        EfCoreMigrationScratchAnalysisReport restored =
            Assert.IsType<EfCoreMigrationScratchAnalysisReport>(
                JsonSerializer
                    .Deserialize<EfCoreMigrationScratchAnalysisReport>(
                        json,
                        options));

        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Passed,
            restored.Outcome);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            restored.ScratchChain.Outcome);
        Assert.NotEqual(restored.Outcome, restored.ScratchChain.Outcome);
    }

    [Fact]
    public void ScratchChainDefensivelyCopiesPrefixes()
    {
        EfCoreMigrationScratchPrefixEvidence original = CreatePrefix();
        var source = new List<EfCoreMigrationScratchPrefixEvidence>
        {
            original,
        };
        var scratchChain = CreateScratchChain() with
        {
            Prefixes = source,
        };

        source[0] = CreatePrefix() with { Ordinal = 99 };
        source.Add(CreatePrefix() with { Ordinal = 100 });

        EfCoreMigrationScratchPrefixEvidence actual =
            Assert.Single(scratchChain.Prefixes);
        Assert.Same(original, actual);
        IList<EfCoreMigrationScratchPrefixEvidence> readOnly =
            Assert.IsAssignableFrom<
                IList<EfCoreMigrationScratchPrefixEvidence>>(
                scratchChain.Prefixes);
        Assert.True(readOnly.IsReadOnly);
        Assert.Throws<NotSupportedException>(
            () => readOnly.Add(CreatePrefix()));
    }

    [Fact]
    public void ScratchReportDefensivelyCopiesDiagnostics()
    {
        EfCoreMigrationAnalysisDiagnostic original = CreateDiagnostic();
        var source = new List<EfCoreMigrationAnalysisDiagnostic>
        {
            original,
        };
        var report = CreateReport() with
        {
            Diagnostics = source,
        };

        source[0] = CreateDiagnostic() with { Ordinal = 99 };
        source.Add(CreateDiagnostic() with { Ordinal = 100 });

        EfCoreMigrationAnalysisDiagnostic actual =
            Assert.Single(report.Diagnostics);
        Assert.Same(original, actual);
        IList<EfCoreMigrationAnalysisDiagnostic> readOnly =
            Assert.IsAssignableFrom<
                IList<EfCoreMigrationAnalysisDiagnostic>>(
                report.Diagnostics);
        Assert.True(readOnly.IsReadOnly);
        Assert.Throws<NotSupportedException>(
            () => readOnly.Clear());
    }

    [Fact]
    public void ScratchWireContractUsesFixedEnumTokensAndFalseDataPreflight()
    {
        JsonSerializerOptions options = CreateJsonOptions();

        string json = JsonSerializer.Serialize(CreateReport(), options);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        JsonElement chain = root.GetProperty("scratchChain");
        JsonElement prefix = chain.GetProperty("prefixes")[0];

        Assert.Equal(
            "csharpdb-ef-migration-scratch-analysis/v1",
            root.GetProperty("format").GetString());
        Assert.Equal("passed", root.GetProperty("outcome").GetString());
        Assert.Equal(
            "csharpdb-ef-migration-analysis/v1",
            root.GetProperty("generationPreflight")
                .GetProperty("format")
                .GetString());
        Assert.Equal(
            "csharpdb-ef-scratch-chain/v1",
            chain.GetProperty("format").GetString());
        Assert.Equal(
            "csharpdb-ef-empty-chain/v1",
            chain.GetProperty("algorithm").GetString());
        Assert.Equal(
            "emptyDatabase",
            chain.GetProperty("proofScope").GetString());
        Assert.False(
            chain.GetProperty("dataPreflightCompleted").GetBoolean());
        Assert.Equal(
            "executed-sql-digest",
            chain.GetProperty("executedSqlDigest").GetString());
        Assert.Equal(
            "idempotent-sql-digest",
            chain.GetProperty("idempotentSqlDigest").GetString());
        Assert.Equal(
            "first-idempotent-schema",
            chain.GetProperty("firstIdempotentSchemaDigest").GetString());
        Assert.Equal(
            "first-idempotent-history",
            chain.GetProperty("firstIdempotentHistoryDigest").GetString());
        Assert.Equal(
            "second-idempotent-schema",
            chain.GetProperty("secondIdempotentSchemaDigest").GetString());
        Assert.Equal(
            "second-idempotent-history",
            chain.GetProperty("secondIdempotentHistoryDigest").GetString());
        Assert.Equal(
            "expected-schema",
            prefix.GetProperty("expectedSchemaDigest").GetString());
        Assert.Equal(
            "reapplied-history",
            prefix.GetProperty("reappliedHistoryDigest").GetString());
    }

    [Fact]
    public void OutcomesAndRuleIdentifiersAreFixed()
    {
        Assert.Equal(
            [
                EfCoreMigrationScratchAnalysisOutcome.Passed,
                EfCoreMigrationScratchAnalysisOutcome.Blocked,
                EfCoreMigrationScratchAnalysisOutcome.Failed,
            ],
            Enum.GetValues<EfCoreMigrationScratchAnalysisOutcome>());
        Assert.Equal(
            "csharpdb.ef.scratch.passed",
            EfCoreMigrationScratchAnalysisRules.ScratchPassed);
        Assert.Equal(
            "csharpdb.ef.scratch.generation-preflight-blocked",
            EfCoreMigrationScratchAnalysisRules.GenerationPreflightBlocked);
        Assert.Equal(
            "csharpdb.ef.scratch.execution-failed",
            EfCoreMigrationScratchAnalysisRules.ScratchExecutionFailed);
        Assert.Equal(
            "csharpdb.ef.scratch.schema-different",
            EfCoreMigrationScratchAnalysisRules.SchemaDifferent);
        Assert.Equal(
            "csharpdb.ef.scratch.round-trip-different",
            EfCoreMigrationScratchAnalysisRules.RoundTripDifferent);
        Assert.Equal(
            "csharpdb.ef.scratch.idempotence-failed",
            EfCoreMigrationScratchAnalysisRules.IdempotenceFailed);
        Assert.Equal(
            "csharpdb.ef.scratch.analysis-limit",
            EfCoreMigrationScratchAnalysisRules.AnalysisLimit);
        Assert.Equal(
            "csharpdb.ef.scratch.resource-disposal-failed",
            EfCoreMigrationScratchAnalysisRules.ResourceDisposalFailed);
    }

    private static EfCoreMigrationScratchAnalysisReport CreateReport() =>
        new()
        {
            Outcome = EfCoreMigrationScratchAnalysisOutcome.Passed,
            Status = MigrationCompatibilityStatus.Compatible,
            HighestEvidence = MigrationEvidenceLevel.ScratchExecuted,
            RuleId = EfCoreMigrationScratchAnalysisRules.ScratchPassed,
            GenerationPreflight = CreateGenerationPreflight(),
            ScratchChain = CreateScratchChain(),
        };

    private static EfCoreMigrationAnalysisReport CreateGenerationPreflight() =>
        new()
        {
            TargetCSharpDbVersion = "4.3.0",
            CapabilityDigest = "capability-digest",
            AssemblyDigest = "assembly-digest",
            QualifiedEfCoreVersion = "10.0.0",
            Context = "Fixtures.SampleContext",
            Status = MigrationCompatibilityStatus.Conditional,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            MigrationCount = 1,
            OperationCount = 1,
            CommandCount = 1,
            GeneratedSqlDigest = "generated-sql-digest",
        };

    private static EfCoreMigrationScratchChainProof CreateScratchChain() =>
        new()
        {
            Outcome = EfCoreMigrationScratchAnalysisOutcome.Passed,
            PrefixCount = 1,
            AppliedPrefixCount = 1,
            SchemaVerifiedPrefixCount = 1,
            DownPrefixCount = 1,
            ReappliedPrefixCount = 1,
            RoundTripVerifiedPrefixCount = 1,
            IdempotentApplyCount = 2,
            ExecutedCommandCount = 7,
            IdempotentCommandCount = 2,
            ExecutedSqlDigest = "executed-sql-digest",
            IdempotentSqlDigest = "idempotent-sql-digest",
            FirstIdempotentSchemaDigest = "first-idempotent-schema",
            FirstIdempotentHistoryDigest = "first-idempotent-history",
            SecondIdempotentSchemaDigest = "second-idempotent-schema",
            SecondIdempotentHistoryDigest = "second-idempotent-history",
            ResourcesDisposed = true,
            Prefixes = [CreatePrefix()],
        };

    private static EfCoreMigrationScratchPrefixEvidence CreatePrefix() =>
        new()
        {
            Ordinal = 0,
            MigrationOrdinal = 0,
            Status = MigrationCompatibilityStatus.Compatible,
            Evidence = MigrationEvidenceLevel.ScratchExecuted,
            RuleId = EfCoreMigrationScratchAnalysisRules.ScratchPassed,
            ExpectedSchemaDigest = "expected-schema",
            ExpectedHistoryDigest = "expected-history",
            AppliedSchemaDigest = "applied-schema",
            AppliedHistoryDigest = "applied-history",
            DownSchemaDigest = "down-schema",
            DownHistoryDigest = "down-history",
            ReappliedSchemaDigest = "reapplied-schema",
            ReappliedHistoryDigest = "reapplied-history",
        };

    private static EfCoreMigrationAnalysisDiagnostic CreateDiagnostic() =>
        new()
        {
            Ordinal = 0,
            DiagnosticId = "EF-SCRATCH-0001",
            RuleId = EfCoreMigrationScratchAnalysisRules.ScratchPassed,
            Severity = MigrationDiagnosticSeverity.Information,
            Status = MigrationCompatibilityStatus.Compatible,
            Evidence = MigrationEvidenceLevel.ScratchExecuted,
            MigrationOrdinal = 0,
            Summary = "Scratch migration evidence was collected.",
        };

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }
}
