using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreWorkerRunnerTests
{
    [Fact]
    public async Task RunAsync_ValidRequestWritesOnlyFramedReport()
    {
        EfCoreMigrationAnalysisReport expected = CreateCoherentReport();
        var dependencies = new EfCoreWorkerDependencies
        {
            AnalyzeAsync = (request, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                Assert.Equal(
                    expected.AssemblyDigest,
                    request.AssemblyDigest);
                Assert.Equal(expected.Context, request.Context);
                return ValueTask.FromResult(expected);
            },
        };
        using var input = RequestStream(
            expected.AssemblyDigest,
            expected.Context);
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreWorkerRunner.RunAsync(
            ValidArguments,
            input,
            output,
            error,
            dependencies,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreWorkerRunner.ExitSuccess, exitCode);
        Assert.Empty(error.ToString());
        string framed = output.ToString();
        Assert.StartsWith(
            EfCoreWorkerRunner.SuccessHeader,
            framed,
            StringComparison.Ordinal);
        EfCoreMigrationAnalysisReport? actual =
            JsonSerializer.Deserialize<EfCoreMigrationAnalysisReport>(
                framed[EfCoreWorkerRunner.SuccessHeader.Length..],
                EfCoreWorkerRunner.JsonOptions);
        Assert.NotNull(actual);
        Assert.Equal(expected.AssemblyDigest, actual.AssemblyDigest);
        Assert.Equal(expected.Context, actual.Context);
    }

    [Fact]
    public async Task RunAsync_ScratchRequestWritesOnlyScratchEnvelope()
    {
        EfCoreMigrationAnalysisReport generation =
            CreateCoherentReport();
        var expected =
            new EfCoreMigrationScratchAnalysisReport
            {
                Outcome =
                    EfCoreMigrationScratchAnalysisOutcome.Blocked,
                Status = generation.Status,
                HighestEvidence = MigrationEvidenceLevel.Bound,
                RuleId = EfCoreMigrationScratchAnalysisRules
                    .GenerationPreflightBlocked,
                GenerationPreflight = generation,
                ScratchChain =
                    new EfCoreMigrationScratchChainProof
                    {
                        Outcome =
                            EfCoreMigrationScratchAnalysisOutcome
                                .Blocked,
                        PrefixCount = generation.MigrationCount,
                        ResourcesDisposed = true,
                    },
            };
        var dependencies = new EfCoreWorkerDependencies
        {
            AnalyzeScratchAsync = (request, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                Assert.Equal(
                    generation.AssemblyDigest,
                    request.AssemblyDigest);
                return ValueTask.FromResult(expected);
            },
        };
        using var input = RequestStream(
            generation.AssemblyDigest,
            generation.Context,
            EfCoreAnalysisMode.Scratch);
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreWorkerRunner.RunAsync(
            ValidArguments,
            input,
            output,
            error,
            dependencies,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreWorkerRunner.ExitSuccess, exitCode);
        Assert.Empty(error.ToString());
        string framed = output.ToString();
        EfCoreMigrationScratchAnalysisReport? actual =
            JsonSerializer
                .Deserialize<EfCoreMigrationScratchAnalysisReport>(
                    framed[EfCoreWorkerRunner.SuccessHeader.Length..],
                    EfCoreWorkerRunner.JsonOptions);
        Assert.NotNull(actual);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Blocked,
            actual.Outcome);
        Assert.Equal(
            generation.AssemblyDigest,
            actual.GenerationPreflight.AssemblyDigest);
    }

    [Fact]
    public async Task RunAsync_MalformedInputUsesFixedError()
    {
        const string secret = "TOP-SECRET-WORKER-INPUT";
        using var input = new MemoryStream(
            Encoding.UTF8.GetBytes(
                $$"""{"format":"{{secret}}"}"""));
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreWorkerRunner.RunAsync(
            ValidArguments,
            input,
            output,
            error,
            EfCoreWorkerDependencies.Default,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreWorkerRunner.ExitIncompatible, exitCode);
        Assert.Empty(output.ToString());
        AssertFixedError(
            error.ToString(),
            EfCoreWorkerErrorCode.Incompatible,
            secret);
    }

    [Fact]
    public async Task RunAsync_OversizedInputUsesFixedLimitError()
    {
        byte[] payload =
            new byte[EfCoreWorkerRunner.MaxInputBytes + 1];
        Array.Fill(payload, (byte)'X');
        using var input = new MemoryStream(payload, writable: false);
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreWorkerRunner.RunAsync(
            ValidArguments,
            input,
            output,
            error,
            EfCoreWorkerDependencies.Default,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreWorkerRunner.ExitInputLimit, exitCode);
        Assert.Empty(output.ToString());
        AssertFixedError(
            error.ToString(),
            EfCoreWorkerErrorCode.InputLimitExceeded,
            new string('X', 32));
    }

    [Fact]
    public async Task RunAsync_DependencyExceptionDoesNotLeak()
    {
        const string secret = "TOP-SECRET-WORKER-EXCEPTION";
        EfCoreMigrationAnalysisReport expected = CreateCoherentReport();
        var dependencies = new EfCoreWorkerDependencies
        {
            AnalyzeAsync = static (_, _) =>
                throw new InvalidOperationException(secret),
        };
        using var input = RequestStream(
            expected.AssemblyDigest,
            expected.Context);
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreWorkerRunner.RunAsync(
            ValidArguments,
            input,
            output,
            error,
            dependencies,
            TestContext.Current.CancellationToken);

        Assert.Equal(
            EfCoreWorkerRunner.ExitInternalFailure,
            exitCode);
        Assert.Empty(output.ToString());
        AssertFixedError(
            error.ToString(),
            EfCoreWorkerErrorCode.InternalFailure,
            secret);
    }

    [Fact]
    public void ReportSanitizer_ReconstructsHostOwnedProse()
    {
        const string secret = "TOP-SECRET-REPORT-PROSE";
        EfCoreMigrationAnalysisReport report =
            CreateCoherentReport() with
            {
                Diagnostics =
                [
                    CreateCoherentReport().Diagnostics[0] with
                    {
                        Summary = secret,
                        Remediation = secret,
                    },
                ],
            };

        bool accepted = EfCoreReportSanitizer.TrySanitize(
            report,
            report.AssemblyDigest,
            "FixtureContext",
            out EfCoreMigrationAnalysisReport? sanitized);

        Assert.True(accepted);
        Assert.NotNull(sanitized);
        Assert.Equal(
            "Example.Tools.FixtureContext",
            sanitized.Context);
        Assert.Equal(
            "Migration SQL generation succeeded, but the chain was not executed.",
            Assert.Single(sanitized.Diagnostics).Summary);
        string json = JsonSerializer.Serialize(
            sanitized,
            EfCoreWorkerRunner.JsonOptions);
        Assert.DoesNotContain(secret, json, StringComparison.Ordinal);
    }

    [Fact]
    public void ReportSanitizer_RejectsIncoherentMetadata()
    {
        EfCoreMigrationAnalysisReport report =
            CreateCoherentReport();

        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with { GeneratedSqlDigest = null },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with
            {
                Status =
                    MigrationCompatibilityStatus.Compatible,
            },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report,
            report.AssemblyDigest,
            "DifferentContext",
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with { OperationCount = 3 },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with
            {
                Migrations =
                [
                    report.Migrations[0] with
                    {
                        RuleId =
                            EfCoreMigrationAnalysisRules
                                .EmptyDownMigration,
                    },
                ],
            },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with
            {
                Migrations =
                [
                    report.Migrations[0] with
                    {
                        Operations =
                        [
                            report.Migrations[0].Operations[0] with
                            {
                                Kind =
                                    EfCoreMigrationOperationKind
                                        .EnsureSchema,
                            },
                            report.Migrations[0].Operations[1],
                        ],
                    },
                ],
            },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with { Diagnostics = [] },
            report.AssemblyDigest,
            report.Context,
            out _));
        Assert.False(EfCoreReportSanitizer.TrySanitize(
            report with
            {
                Diagnostics =
                [
                    report.Diagnostics[0] with
                    {
                        MigrationOrdinal = 0,
                        OperationOrdinal = 0,
                    },
                ],
            },
            report.AssemblyDigest,
            report.Context,
            out _));
    }

    private static string[] ValidArguments =>
    [
        "--worker",
        "--protocol",
        EfCoreWorkerRunner.Protocol,
        "--target-version",
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
    ];

    private static MemoryStream RequestStream(
        string assemblyDigest,
        string context,
        EfCoreAnalysisMode mode =
            EfCoreAnalysisMode.Generation)
    {
        string json = JsonSerializer.Serialize(
            new
            {
                format = EfCoreWorkerRunner.RequestFormat,
                mode,
                assemblyPath = Path.GetFullPath("fixture.dll"),
                assemblyDigest,
                context,
            },
            EfCoreWorkerRunner.JsonOptions);
        return new MemoryStream(
            Encoding.UTF8.GetBytes(json),
            writable: false);
    }

    private static void AssertFixedError(
        string value,
        EfCoreWorkerErrorCode expectedCode,
        string secret)
    {
        EfCoreWorkerErrorEnvelope? envelope =
            JsonSerializer.Deserialize<EfCoreWorkerErrorEnvelope>(
                value,
                EfCoreWorkerRunner.JsonOptions);
        Assert.NotNull(envelope);
        Assert.Equal(
            EfCoreWorkerErrorEnvelope.CurrentFormat,
            envelope.Format);
        Assert.Equal(EfCoreWorkerRunner.Protocol, envelope.Protocol);
        Assert.Equal(expectedCode, envelope.Code);
        Assert.DoesNotContain(
            secret,
            value,
            StringComparison.Ordinal);
    }

    private static EfCoreMigrationAnalysisReport
        CreateCoherentReport()
    {
        const string assemblyDigest =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const string upDigest =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        const string downDigest =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        const string migrationDigest =
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        const string chainDigest =
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

        var up = new EfCoreMigrationOperationFinding
        {
            Ordinal = 0,
            Direction = EfCoreMigrationDirection.Up,
            DirectionOrdinal = 0,
            Kind = EfCoreMigrationOperationKind.CreateTable,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            AnnotationCount = 0,
            CommandCount = 1,
            GeneratedSqlUtf8Bytes = 32,
            GeneratedSqlDigest = upDigest,
        };
        var down = new EfCoreMigrationOperationFinding
        {
            Ordinal = 1,
            Direction = EfCoreMigrationDirection.Down,
            DirectionOrdinal = 0,
            Kind = EfCoreMigrationOperationKind.DropTable,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            AnnotationCount = 0,
            CommandCount = 1,
            GeneratedSqlUtf8Bytes = 24,
            GeneratedSqlDigest = downDigest,
        };
        var migration = new EfCoreMigrationAnalysisMigration
        {
            Ordinal = 0,
            MigrationId = "202607250001_InitialCreate",
            Status = MigrationCompatibilityStatus.Conditional,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            UpOperationCount = 1,
            DownOperationCount = 1,
            OperationCount = 2,
            DestructiveOperationCount = 0,
            CommandCount = 2,
            GeneratedSqlDigest = migrationDigest,
            Operations = [up, down],
        };
        var diagnostic = new EfCoreMigrationAnalysisDiagnostic
        {
            Ordinal = 0,
            DiagnosticId = "ef.diagnostic.000000",
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            Severity = MigrationDiagnosticSeverity.Warning,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            Summary =
                "Migration SQL generation succeeded, but the chain was not executed.",
            Remediation =
                "Validate every migration prefix in an isolated scratch database before production use.",
        };
        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        return new EfCoreMigrationAnalysisReport
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            CapabilityDigest = capabilities.Digest,
            AssemblyDigest = assemblyDigest,
            QualifiedEfCoreVersion = ProductInfo.GetVersion(),
            Context = "Example.Tools.FixtureContext",
            Status = MigrationCompatibilityStatus.Conditional,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationAnalysisRules.GenerationBound,
            MigrationCount = 1,
            OperationCount = 2,
            DestructiveOperationCount = 0,
            CommandCount = 2,
            GeneratedSqlDigest = chainDigest,
            Migrations = [migration],
            Diagnostics = [diagnostic],
        };
    }
}
