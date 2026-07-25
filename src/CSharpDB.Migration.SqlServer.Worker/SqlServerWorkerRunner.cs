using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.SqlServer.Worker;

internal sealed record SqlServerWorkerDependencies
{
    internal static SqlServerWorkerDependencies Default { get; } = new();

    internal Func<string, string?> ReadEnvironmentVariable { get; init; } =
        Environment.GetEnvironmentVariable;

    internal Func<string, IMigrationSourceInspector> CreateInspector { get; init; } =
        static connectionString =>
            new SqlServerMigrationSourceInspector(connectionString);

    internal Func<MigrationCatalog, string> SerializeCatalog { get; init; } =
        static catalog =>
            MigrationArtifactSerializer.SerializeCatalog(
                catalog,
                writeIndented: false);

    internal Func<string, long> MeasureUtf8Bytes { get; init; } =
        static value => new UTF8Encoding(
                encoderShouldEmitUTF8Identifier: false,
                throwOnInvalidBytes: true)
            .GetByteCount(value);

    internal Func<
        string,
        CancellationToken,
        ValueTask<CSharpDbDdlCompatibilityReport>> AnalyzeDdlAsync
    {
        get;
        init;
    } = static (script, cancellationToken) =>
        SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
            script,
            cancellationToken: cancellationToken);

    internal Func<CSharpDbDdlCompatibilityReport, string>
        SerializeDdlReport
    {
        get;
        init;
    } = SqlServerWorkerRunner.SerializeDdlReport;
}

internal static class SqlServerWorkerRunner
{
    internal const string Protocol = "csharpdb-sqlserver-worker/v1";
    internal const string SuccessHeader = Protocol + "\n";
    internal const string DdlProtocol =
        "csharpdb-sqlserver-ddl-worker/v1";
    internal const string DdlSuccessHeader = DdlProtocol + "\n";
    internal const int ExitSuccess = 0;
    internal const int ExitIncompatible = 10;
    internal const int ExitConnectionUnavailable = 11;
    internal const int ExitInspectionFailure = 12;
    internal const int ExitInternalFailure = 13;
    internal const long MaxCatalogBytes = 64L * 1024 * 1024;
    internal const int MaxDdlInputBytes =
        SqlServerTsqlDdlCompatibilityOptions.HardMaxScriptUtf8Bytes;
    internal const int MaxDdlInputCharacters =
        SqlServerTsqlDdlCompatibilityOptions.HardMaxScriptCharacters;
    internal const long MaxDdlReportBytes = 8L * 1024 * 1024;

    private const string IncompatibleError =
        Protocol + ":error:incompatible";
    private const string ConnectionUnavailableError =
        Protocol + ":error:connection-unavailable";
    private const string InspectionFailureError =
        Protocol + ":error:inspection-failed";
    private const string InternalFailureError =
        Protocol + ":error:internal-failure";
    private const string DdlIncompatibleError =
        DdlProtocol + ":error:incompatible";
    private const string DdlAnalysisFailureError =
        DdlProtocol + ":error:analysis-failed";
    private const string DdlInternalFailureError =
        DdlProtocol + ":error:internal-failure";
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);
    private static ReadOnlySpan<byte> Utf8Bom =>
        [0xEF, 0xBB, 0xBF];
    private static readonly JsonSerializerOptions DdlJsonOptions =
        CreateDdlJsonOptions();

    internal static ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default) =>
        RunAsync(
            args,
            Stream.Null,
            output,
            error,
            dependencies,
            cancellationToken);

    internal static async ValueTask<int> RunAsync(
        string[] args,
        Stream input,
        TextWriter output,
        TextWriter error,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);

        if (IsDdlProtocol(args))
        {
            return await RunDdlAsync(
                args,
                input,
                output,
                error,
                dependencies,
                cancellationToken);
        }

        if (!TryParseInvocation(
                args,
                out string? environmentVariableName,
                out string? targetVersion))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                IncompatibleError);
        }

        if (!string.Equals(
                targetVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                IncompatibleError);
        }

        if (dependencies.ReadEnvironmentVariable is null ||
            dependencies.CreateInspector is null ||
            dependencies.SerializeCatalog is null ||
            dependencies.MeasureUtf8Bytes is null)
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                InternalFailureError);
        }

        string? connectionString;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            connectionString =
                dependencies.ReadEnvironmentVariable(environmentVariableName!);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                InspectionFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitConnectionUnavailable,
                ConnectionUnavailableError);
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return await FailAsync(
                error,
                ExitConnectionUnavailable,
                ConnectionUnavailableError);
        }

        MigrationCatalog catalog;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            IMigrationSourceInspector inspector =
                dependencies.CreateInspector(connectionString);
            if (inspector is null ||
                inspector.SourceKind != MigrationSourceKind.SqlServer)
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    InternalFailureError);
            }

            catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = targetVersion!,
                    IncludeProfile = false,
                },
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                InspectionFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                InspectionFailureError);
        }

        string serialized;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (catalog is null ||
                catalog.Source.Kind != MigrationSourceKind.SqlServer ||
                !string.Equals(
                    catalog.TargetCSharpDbVersion,
                    targetVersion,
                    StringComparison.Ordinal))
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    InternalFailureError);
            }

            serialized = dependencies.SerializeCatalog(catalog);
            if (string.IsNullOrEmpty(serialized) ||
                dependencies.MeasureUtf8Bytes(serialized) > MaxCatalogBytes)
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    InternalFailureError);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                InspectionFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                InternalFailureError);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteAsync(SuccessHeader);
            await output.WriteAsync(serialized);
            await output.FlushAsync(cancellationToken);
            return ExitSuccess;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                InspectionFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                InternalFailureError);
        }
    }

    private static bool IsDdlProtocol(IReadOnlyList<string> args) =>
        args.Count >= 2 &&
        string.Equals(
            args[0],
            "--protocol",
            StringComparison.Ordinal) &&
        string.Equals(
            args[1],
            DdlProtocol,
            StringComparison.Ordinal);

    private static async ValueTask<int> RunDdlAsync(
        IReadOnlyList<string> args,
        Stream input,
        TextWriter output,
        TextWriter error,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken)
    {
        if (!TryParseDdlInvocation(args, out string? targetVersion) ||
            !string.Equals(
                targetVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                DdlIncompatibleError);
        }

        if (dependencies.AnalyzeDdlAsync is null ||
            dependencies.SerializeDdlReport is null ||
            dependencies.MeasureUtf8Bytes is null)
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                DdlInternalFailureError);
        }

        DdlInput ddlInput;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ddlInput = await ReadDdlInputAsync(
                input,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or
                DdlInputLimitException)
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                DdlIncompatibleError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }

        CSharpDbDdlCompatibilityReport report;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            report = await dependencies.AnalyzeDdlAsync(
                ddlInput.Script,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }

        string serialized;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TrySanitizeDdlReport(
                    report,
                    targetVersion!,
                    ddlInput.ScriptDigest,
                    ddlInput.Script,
                    out CSharpDbDdlCompatibilityReport? sanitizedReport))
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    DdlInternalFailureError);
            }

            serialized = dependencies.SerializeDdlReport(
                sanitizedReport!);
            if (string.IsNullOrEmpty(serialized) ||
                dependencies.MeasureUtf8Bytes(serialized) >
                    MaxDdlReportBytes)
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    DdlInternalFailureError);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                DdlInternalFailureError);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteAsync(
                DdlSuccessHeader.AsMemory(),
                cancellationToken);
            await output.WriteAsync(
                serialized.AsMemory(),
                cancellationToken);
            await output.FlushAsync(cancellationToken);
            return ExitSuccess;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitInspectionFailure,
                DdlAnalysisFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                DdlInternalFailureError);
        }
    }

    private static bool TryParseDdlInvocation(
        IReadOnlyList<string> args,
        out string? targetVersion)
    {
        targetVersion = null;
        if (args.Count != 4 ||
            !string.Equals(
                args[0],
                "--protocol",
                StringComparison.Ordinal) ||
            !string.Equals(
                args[1],
                DdlProtocol,
                StringComparison.Ordinal) ||
            !string.Equals(
                args[2],
                "--target-version",
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[3]))
        {
            return false;
        }

        targetVersion = args[3];
        return true;
    }

    private static async ValueTask<DdlInput> ReadDdlInputAsync(
        Stream input,
        CancellationToken cancellationToken)
    {
        byte[] readBuffer =
            ArrayPool<byte>.Shared.Rent(64 * 1024);
        byte[]? payload = null;
        int payloadLength = 0;
        try
        {
            payload =
                ArrayPool<byte>.Shared.Rent(MaxDdlInputBytes);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int read = await input.ReadAsync(
                    readBuffer.AsMemory(0, readBuffer.Length),
                    cancellationToken);
                if (read == 0)
                    break;
                if (payloadLength > MaxDdlInputBytes - read)
                    throw new DdlInputLimitException();

                readBuffer
                    .AsSpan(0, read)
                    .CopyTo(payload.AsSpan(payloadLength));
                payloadLength += read;
            }

            ReadOnlySpan<byte> source =
                payload.AsSpan(0, payloadLength);
            if (source.StartsWith(Utf8Bom))
                source = source[Utf8Bom.Length..];

            cancellationToken.ThrowIfCancellationRequested();
            int characterCount = StrictUtf8.GetCharCount(source);
            if (characterCount > MaxDdlInputCharacters)
                throw new DdlInputLimitException();

            cancellationToken.ThrowIfCancellationRequested();
            string script = StrictUtf8.GetString(source);
            cancellationToken.ThrowIfCancellationRequested();
            return new DdlInput(
                script,
                ComputeDdlSourceDigest(
                    source,
                    cancellationToken));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                readBuffer,
                clearArray: true);
            if (payload is not null)
            {
                ArrayPool<byte>.Shared.Return(
                    payload,
                    clearArray: true);
            }
        }
    }

    private static bool TrySanitizeDdlReport(
        CSharpDbDdlCompatibilityReport? report,
        string targetVersion,
        string expectedScriptDigest,
        string script,
        out CSharpDbDdlCompatibilityReport? sanitized)
    {
        sanitized = null;
        int scriptCharacterCount = script.Length;
        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        if (report is null ||
            !string.Equals(
                report.Format,
                CSharpDbDdlCompatibilityReport.CurrentFormat,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.Dialect,
                "tsql",
                StringComparison.Ordinal) ||
            !string.Equals(
                report.SourceGrammar,
                SqlServerTsqlDdlCompatibilityAnalyzer.SourceGrammar,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.TargetCSharpDbVersion,
                targetVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.TargetCSharpDbVersion,
                capabilities.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.CapabilityDigest,
                capabilities.Digest,
                StringComparison.Ordinal) ||
            !IsLowerHexSha256(report.CapabilityDigest) ||
            !string.Equals(
                report.ScriptDigest,
                expectedScriptDigest,
                StringComparison.Ordinal) ||
            !IsLowerHexSha256(report.ScriptDigest) ||
            !IsAllowedDdlRule(report.RuleId) ||
            report.StatementCount < 0 ||
            report.StatementCount >
                SqlServerTsqlDdlCompatibilityOptions
                    .HardMaxStatementCount ||
            report.ProvenStatementCount < 0 ||
            report.ProvenStatementCount > report.StatementCount ||
            report.CandidateActionCount < 0 ||
            report.CandidateActionCount >
                CSharpDbDdlScratchValidationOptions
                    .HardMaxActionCount ||
            report.Statements is null ||
            report.Diagnostics is null ||
            report.Differences is null ||
            report.Statements.Count != report.StatementCount ||
            !IsValidDdlRoot(report))
        {
            return false;
        }

        var statements =
            new CSharpDbDdlCompatibilityStatement[
                report.StatementCount];
        int previousEnd = 0;
        int sourceCursor = 0;
        int sourceLine = 1;
        int sourceColumn = 1;
        for (int index = 0; index < statements.Length; index++)
        {
            CSharpDbDdlCompatibilityStatement? statement =
                report.Statements[index];
            if (statement is null ||
                statement.Index != index ||
                !IsAllowedDdlStatementKind(statement.Kind) ||
                !IsAllowedDdlRule(statement.RuleId) ||
                !IsValidDdlStatementSpan(
                    statement.Span,
                    scriptCharacterCount) ||
                statement.Span.Start!.Value < previousEnd ||
                !HasExactDdlLocation(
                    statement.Span,
                    script,
                    ref sourceCursor,
                    ref sourceLine,
                    ref sourceColumn))
            {
                return false;
            }

            previousEnd = checked(
                statement.Span.Start.Value +
                statement.Span.Length!.Value);
            statements[index] = new CSharpDbDdlCompatibilityStatement
            {
                Index = index,
                Kind = statement.Kind,
                Span = CopyDdlSpan(statement.Span),
                Status = statement.Status,
                Evidence = statement.Evidence,
                RuleId = statement.RuleId,
            };
        }
        if (!AreDdlStatementsCoherent(report, statements))
            return false;

        int maximumDiagnostics = report.StatementCount == 0
            ? 1
            : checked(report.StatementCount + 2);
        if (report.Diagnostics.Count == 0 ||
            report.Diagnostics.Count > maximumDiagnostics)
        {
            return false;
        }
        var diagnostics =
            new CSharpDbDdlCompatibilityDiagnostic[
                report.Diagnostics.Count];
        for (int ordinal = 0;
             ordinal < diagnostics.Length;
             ordinal++)
        {
            CSharpDbDdlCompatibilityDiagnostic? diagnostic =
                report.Diagnostics[ordinal];
            if (diagnostic is null ||
                !TrySanitizeDdlDiagnostic(
                    diagnostic,
                    ordinal,
                    report,
                    statements,
                    script,
                    out CSharpDbDdlCompatibilityDiagnostic?
                        sanitizedDiagnostic))
            {
                return false;
            }
            diagnostics[ordinal] = sanitizedDiagnostic!;
        }
        if (!AreDdlDiagnosticsCoherent(
                report,
                statements,
                diagnostics))
        {
            return false;
        }

        int differenceLimit =
            2 * SqlServerTsqlDdlCompatibilityOptions
                .HardMaxCatalogObjectCount;
        if (report.Differences.Count > differenceLimit)
            return false;
        var differences =
            new CSharpDbDdlScratchValidationDifference[
                report.Differences.Count];
        var differenceIdentities =
            new HashSet<string>(StringComparer.Ordinal);
        for (int ordinal = 0;
             ordinal < differences.Length;
             ordinal++)
        {
            CSharpDbDdlScratchValidationDifference? difference =
                report.Differences[ordinal];
            if (difference is null ||
                difference.Ordinal != ordinal ||
                !IsLowerHexSha256(
                    difference.ObjectIdentityDigest) ||
                !differenceIdentities.Add(
                    difference.ObjectIdentityDigest) ||
                !IsAllowedDdlDifferenceKind(difference.Kind) ||
                !IsOptionalLowerHexSha256(
                    difference.ExpectedDefinitionDigest) ||
                !IsOptionalLowerHexSha256(
                    difference.ActualDefinitionDigest) ||
                difference.ExpectedDefinitionDigest is null &&
                difference.ActualDefinitionDigest is null ||
                difference.ExpectedDefinitionDigest is not null &&
                string.Equals(
                    difference.ExpectedDefinitionDigest,
                    difference.ActualDefinitionDigest,
                    StringComparison.Ordinal))
            {
                return false;
            }
            differences[ordinal] =
                new CSharpDbDdlScratchValidationDifference
                {
                    Ordinal = ordinal,
                    ObjectIdentityDigest =
                        difference.ObjectIdentityDigest,
                    Kind = difference.Kind,
                    ExpectedDefinitionDigest =
                        difference.ExpectedDefinitionDigest,
                    ActualDefinitionDigest =
                        difference.ActualDefinitionDigest,
                };
        }

        if (!HasValidDdlDigestShape(report, differences.Length))
            return false;

        sanitized = new CSharpDbDdlCompatibilityReport
        {
            Format = CSharpDbDdlCompatibilityReport.CurrentFormat,
            Dialect = "tsql",
            SourceGrammar =
                SqlServerTsqlDdlCompatibilityAnalyzer.SourceGrammar,
            TargetCSharpDbVersion = targetVersion,
            CapabilityDigest = capabilities.Digest,
            ScriptDigest = expectedScriptDigest,
            Status = report.Status,
            HighestEvidence = report.HighestEvidence,
            RuleId = report.RuleId,
            StatementCount = report.StatementCount,
            ProvenStatementCount = report.ProvenStatementCount,
            CandidateActionCount = report.CandidateActionCount,
            CatalogDigest = report.CatalogDigest,
            PlanContractDigest = report.PlanContractDigest,
            GeneratedDdlDigest = report.GeneratedDdlDigest,
            ExpectedSchemaDigest = report.ExpectedSchemaDigest,
            ActualSchemaDigest = report.ActualSchemaDigest,
            Statements = statements,
            Diagnostics = diagnostics,
            Differences = differences,
        };
        return true;
    }

    private static bool IsValidDdlRoot(
        CSharpDbDdlCompatibilityReport report)
    {
        bool noStatements = report.StatementCount == 0;
        bool hasStatements = report.StatementCount > 0;
        bool validRoot = report.RuleId switch
        {
            SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId =>
                noStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence is null,
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId =>
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                (noStatements &&
                 report.HighestEvidence is null ||
                 hasStatements &&
                 report.HighestEvidence ==
                     MigrationEvidenceLevel.Parsed),
            SqlServerTsqlDdlCompatibilityAnalyzer.EmptyRuleId =>
                noStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed,
            SqlServerTsqlDdlCompatibilityAnalyzer.InternalRuleId =>
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                (report.HighestEvidence is null && noStatements ||
                 report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed),
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.CapabilityMatched,
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.CapabilityMatched,
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed,
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                report.HighestEvidence is
                    MigrationEvidenceLevel.CapabilityMatched or
                    MigrationEvidenceLevel.Bound or
                    MigrationEvidenceLevel.ScratchExecuted,
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.ScratchExecuted,
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus
                        .CompatibleWithRewrite &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.ScratchExecuted,
            SqlServerTsqlDdlCompatibilityAnalyzer.TextCollationRuleId =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Conditional &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.ScratchExecuted,
            _ when IsTsqlLoweringRule(report.RuleId) =>
                hasStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed,
            _ => false,
        };
        if (!validRoot)
            return false;

        int expectedProven = report.Status ==
                MigrationCompatibilityStatus.CompatibleWithRewrite
            ? report.StatementCount
            : 0;
        return report.ProvenStatementCount == expectedProven;
    }

    private static bool AreDdlStatementsCoherent(
        CSharpDbDdlCompatibilityReport report,
        IReadOnlyList<CSharpDbDdlCompatibilityStatement> statements)
    {
        if (statements.Count == 0)
            return report.StatementCount == 0;

        if (IsTsqlLoweringRule(report.RuleId))
        {
            int unsupportedCount = 0;
            foreach (CSharpDbDdlCompatibilityStatement statement
                     in statements)
            {
                if (statement.Evidence !=
                    MigrationEvidenceLevel.Parsed ||
                    statement.Kind == "unproven")
                {
                    return false;
                }
                if (statement.Status ==
                    MigrationCompatibilityStatus.Unsupported)
                {
                    unsupportedCount++;
                    if (!IsTsqlLoweringRule(statement.RuleId) ||
                        statement.Kind == "unsupported" &&
                        !string.Equals(
                            statement.RuleId,
                            SqlServerTsqlDdlCompatibilityAnalyzer
                                .UnsupportedStatementRuleId,
                            StringComparison.Ordinal) ||
                        statement.Kind != "unsupported" &&
                        string.Equals(
                            statement.RuleId,
                            SqlServerTsqlDdlCompatibilityAnalyzer
                                .UnsupportedStatementRuleId,
                            StringComparison.Ordinal))
                    {
                        return false;
                    }
                }
                else if (statement.Status !=
                             MigrationCompatibilityStatus.Conditional ||
                         !string.Equals(
                             statement.RuleId,
                             CSharpDbDdlCompatibilityAnalyzer
                                 .CapabilityRuleId,
                             StringComparison.Ordinal) ||
                         statement.Kind == "unsupported")
                {
                    return false;
                }
            }
            return unsupportedCount > 0;
        }

        if (report.HighestEvidence is not { } evidence)
            return false;
        foreach (CSharpDbDdlCompatibilityStatement statement
                 in statements)
        {
            if (statement.Status != report.Status ||
                statement.Evidence != evidence ||
                !string.Equals(
                    statement.RuleId,
                    report.RuleId,
                    StringComparison.Ordinal))
            {
                return false;
            }
            if (string.Equals(
                    report.RuleId,
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .InternalRuleId,
                    StringComparison.Ordinal))
            {
                if (statement.Kind != "unproven")
                    return false;
            }
            else if (statement.Kind is not (
                         "create-table" or "create-index"))
            {
                return false;
            }
        }
        return true;
    }

    private static bool TrySanitizeDdlDiagnostic(
        CSharpDbDdlCompatibilityDiagnostic diagnostic,
        int ordinal,
        CSharpDbDdlCompatibilityReport report,
        IReadOnlyList<CSharpDbDdlCompatibilityStatement> statements,
        string script,
        out CSharpDbDdlCompatibilityDiagnostic? sanitized)
    {
        sanitized = null;
        if (diagnostic.Ordinal != ordinal ||
            !IsAllowedDdlRule(diagnostic.RuleId) ||
            !TryGetDdlDiagnosticProse(
                diagnostic.RuleId,
                out string? summary,
                out string? remediation))
        {
            return false;
        }

        string prefix = IsTsqlRule(diagnostic.RuleId)
            ? "tsql-ddl/"
            : "csharpdb-ddl/";
        string expectedId = string.Concat(
            prefix,
            ordinal.ToString("D6", CultureInfo.InvariantCulture),
            "/",
            diagnostic.RuleId);
        if (!string.Equals(
                diagnostic.DiagnosticId,
                expectedId,
                StringComparison.Ordinal) ||
            !IsDdlDiagnosticRuleCoherent(
                diagnostic,
                report))
        {
            return false;
        }

        MigrationSourceSpan? sourceSpan = diagnostic.SourceSpan;
        if (diagnostic.StatementIndex is int statementIndex)
        {
            if (statementIndex < 0 ||
                statementIndex >= statements.Count ||
                sourceSpan is null ||
                !DdlSpansEqual(
                    sourceSpan,
                    statements[statementIndex].Span))
            {
                return false;
            }
        }
        else if (string.Equals(
                     diagnostic.RuleId,
                     SqlServerTsqlDdlCompatibilityAnalyzer
                         .ParseRuleId,
                     StringComparison.Ordinal))
        {
            if (!IsValidDdlLocationSpan(
                    sourceSpan,
                    script.Length) ||
                sourceSpan!.Length is not null ||
                !HasExactDdlLocation(
                    sourceSpan,
                    script))
            {
                return false;
            }
        }
        else if (sourceSpan is not null)
        {
            return false;
        }

        sanitized = new CSharpDbDdlCompatibilityDiagnostic
        {
            Ordinal = ordinal,
            DiagnosticId = expectedId,
            RuleId = diagnostic.RuleId,
            Severity = diagnostic.Severity,
            Status = diagnostic.Status,
            Evidence = diagnostic.Evidence,
            StatementIndex = diagnostic.StatementIndex,
            SourceSpan = sourceSpan is null
                ? null
                : CopyDdlSpan(sourceSpan),
            Summary = summary!,
            Remediation = remediation,
        };
        return true;
    }

    private static bool IsDdlDiagnosticRuleCoherent(
        CSharpDbDdlCompatibilityDiagnostic diagnostic,
        CSharpDbDdlCompatibilityReport report)
    {
        bool noStatement =
            diagnostic.StatementIndex is null;
        bool noSpan = diagnostic.SourceSpan is null;
        if (IsTsqlLoweringRule(diagnostic.RuleId))
        {
            return IsTsqlLoweringRule(report.RuleId) &&
                diagnostic.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                diagnostic.Evidence ==
                    MigrationEvidenceLevel.Parsed &&
                diagnostic.Severity ==
                    MigrationDiagnosticSeverity.Error &&
                !noStatement &&
                !noSpan;
        }

        return diagnostic.RuleId switch
        {
            SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId =>
                string.Equals(
                    report.RuleId,
                    diagnostic.RuleId,
                    StringComparison.Ordinal) &&
                diagnostic.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                diagnostic.Evidence is null &&
                diagnostic.Severity ==
                    MigrationDiagnosticSeverity.Error &&
                noStatement &&
                !noSpan,
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId =>
                DdlDiagnosticMatchesRoot(
                    diagnostic,
                    report,
                    requireNoLocation: true),
            SqlServerTsqlDdlCompatibilityAnalyzer.EmptyRuleId =>
                DdlDiagnosticMatchesRoot(
                    diagnostic,
                    report,
                    requireNoLocation: true),
            SqlServerTsqlDdlCompatibilityAnalyzer.InternalRuleId =>
                DdlDiagnosticMatchesRoot(
                    diagnostic,
                    report,
                    requireNoLocation: true),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .TextCollationRuleId =>
                report.StatementCount > 0 &&
                (string.Equals(
                     report.RuleId,
                     diagnostic.RuleId,
                     StringComparison.Ordinal) ||
                 IsSharedPostLoweringRule(report.RuleId)) &&
                diagnostic.Status ==
                    MigrationCompatibilityStatus.Conditional &&
                diagnostic.Evidence ==
                    MigrationEvidenceLevel.Parsed &&
                diagnostic.Severity ==
                    MigrationDiagnosticSeverity.Warning &&
                noStatement &&
                noSpan,
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId =>
                report.RuleId is
                    CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId or
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .TextCollationRuleId &&
                diagnostic.Status == report.Status &&
                diagnostic.Evidence ==
                    MigrationEvidenceLevel.ScratchExecuted &&
                diagnostic.Severity ==
                    MigrationDiagnosticSeverity.Warning &&
                noStatement &&
                noSpan,
            _ when IsSharedPostLoweringRule(
                diagnostic.RuleId) =>
                DdlDiagnosticMatchesRoot(
                    diagnostic,
                    report,
                    requireNoLocation: true),
            _ => false,
        };
    }

    private static bool DdlDiagnosticMatchesRoot(
        CSharpDbDdlCompatibilityDiagnostic diagnostic,
        CSharpDbDdlCompatibilityReport report,
        bool requireNoLocation)
    {
        MigrationDiagnosticSeverity expectedSeverity =
            report.Status ==
                MigrationCompatibilityStatus.Conditional
                ? MigrationDiagnosticSeverity.Warning
                : MigrationDiagnosticSeverity.Error;
        return string.Equals(
                report.RuleId,
                diagnostic.RuleId,
                StringComparison.Ordinal) &&
            diagnostic.Status == report.Status &&
            diagnostic.Evidence == report.HighestEvidence &&
            diagnostic.Severity == expectedSeverity &&
            (!requireNoLocation ||
             diagnostic.StatementIndex is null &&
             diagnostic.SourceSpan is null);
    }

    private static bool AreDdlDiagnosticsCoherent(
        CSharpDbDdlCompatibilityReport report,
        IReadOnlyList<CSharpDbDdlCompatibilityStatement> statements,
        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics)
    {
        if (IsTsqlLoweringRule(report.RuleId))
        {
            CSharpDbDdlCompatibilityStatement[] unsupported =
                statements.Where(static statement =>
                        statement.Status ==
                            MigrationCompatibilityStatus.Unsupported)
                    .ToArray();
            if (diagnostics.Count != unsupported.Length)
                return false;
            int previousStatementIndex = -1;
            foreach (CSharpDbDdlCompatibilityDiagnostic diagnostic
                     in diagnostics)
            {
                if (diagnostic.StatementIndex is not int index ||
                    index <= previousStatementIndex ||
                    statements[index].Status !=
                        MigrationCompatibilityStatus.Unsupported ||
                    !string.Equals(
                        statements[index].RuleId,
                        diagnostic.RuleId,
                        StringComparison.Ordinal))
                {
                    return false;
                }
                previousStatementIndex = index;
            }
            return string.Equals(
                diagnostics[0].RuleId,
                report.RuleId,
                StringComparison.Ordinal);
        }

        int rootCount = diagnostics.Count(diagnostic =>
            string.Equals(
                diagnostic.RuleId,
                report.RuleId,
                StringComparison.Ordinal));
        int textCount = diagnostics.Count(diagnostic =>
            string.Equals(
                diagnostic.RuleId,
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .TextCollationRuleId,
                StringComparison.Ordinal));
        int rewriteCount = diagnostics.Count(diagnostic =>
            string.Equals(
                diagnostic.RuleId,
                CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
                StringComparison.Ordinal));
        if (rootCount != 1 || textCount > 1 || rewriteCount > 1)
            return false;

        if (report.Status ==
            MigrationCompatibilityStatus.CompatibleWithRewrite)
        {
            return diagnostics.Count == 1 &&
                rewriteCount == 1 &&
                textCount == 0 &&
                string.Equals(
                    diagnostics[0].RuleId,
                    report.RuleId,
                    StringComparison.Ordinal);
        }
        if (report.Status ==
            MigrationCompatibilityStatus.Conditional)
        {
            return diagnostics.Count == 2 &&
                textCount == 1 &&
                rewriteCount == 1 &&
                string.Equals(
                    diagnostics[0].RuleId,
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .TextCollationRuleId,
                    StringComparison.Ordinal) &&
                string.Equals(
                    diagnostics[1].RuleId,
                    CSharpDbDdlCompatibilityAnalyzer
                        .RewriteRuleId,
                    StringComparison.Ordinal);
        }
        if (report.StatementCount == 0)
        {
            return diagnostics.Count == 1 &&
                string.Equals(
                    diagnostics[0].RuleId,
                    report.RuleId,
                    StringComparison.Ordinal);
        }
        return diagnostics.Count == 1 &&
                string.Equals(
                    diagnostics[0].RuleId,
                    report.RuleId,
                    StringComparison.Ordinal) ||
            diagnostics.Count == 2 &&
                string.Equals(
                    diagnostics[0].RuleId,
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .TextCollationRuleId,
                    StringComparison.Ordinal) &&
                string.Equals(
                    diagnostics[1].RuleId,
                    report.RuleId,
                    StringComparison.Ordinal);
    }

    private static bool HasValidDdlDigestShape(
        CSharpDbDdlCompatibilityReport report,
        int differenceCount)
    {
        if (!IsOptionalLowerHexSha256(report.CatalogDigest) ||
            !IsOptionalLowerHexSha256(report.PlanContractDigest) ||
            !IsOptionalLowerHexSha256(report.GeneratedDdlDigest) ||
            !IsOptionalLowerHexSha256(report.ExpectedSchemaDigest) ||
            !IsOptionalLowerHexSha256(report.ActualSchemaDigest))
        {
            return false;
        }

        bool catalog = report.CatalogDigest is not null;
        bool plan = report.PlanContractDigest is not null;
        bool generated = report.GeneratedDdlDigest is not null;
        bool expected = report.ExpectedSchemaDigest is not null;
        bool actual = report.ActualSchemaDigest is not null;
        return report.RuleId switch
        {
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .TextCollationRuleId =>
                catalog &&
                plan &&
                generated &&
                expected &&
                actual &&
                report.CandidateActionCount > 0 &&
                differenceCount == 0 &&
                string.Equals(
                    report.ExpectedSchemaDigest,
                    report.ActualSchemaDigest,
                    StringComparison.Ordinal),
            CSharpDbDdlCompatibilityAnalyzer
                .ScratchDifferentRuleId =>
                catalog &&
                plan &&
                generated &&
                expected &&
                actual &&
                report.CandidateActionCount > 0 &&
                differenceCount > 0 &&
                !string.Equals(
                    report.ExpectedSchemaDigest,
                    report.ActualSchemaDigest,
                    StringComparison.Ordinal),
            CSharpDbDdlCompatibilityAnalyzer
                .ScratchRejectedRuleId =>
                catalog &&
                plan &&
                generated &&
                (!actual || expected) &&
                report.CandidateActionCount > 0 &&
                differenceCount == 0,
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId =>
                catalog &&
                !plan &&
                !generated &&
                !expected &&
                !actual &&
                report.CandidateActionCount == 0 &&
                differenceCount == 0,
            _ =>
                !catalog &&
                !plan &&
                !generated &&
                !expected &&
                !actual &&
                report.CandidateActionCount == 0 &&
                differenceCount == 0,
        };
    }

    private static bool TryGetDdlDiagnosticProse(
        string ruleId,
        out string? summary,
        out string? remediation)
    {
        (summary, remediation) = ruleId switch
        {
            SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId =>
                (
                    "The T-SQL script could not be parsed completely.",
                    "Correct the script and retry."),
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId =>
                (
                    "The T-SQL script exceeded a qualified analysis limit.",
                    "Reduce the script or split it into bounded scripts."),
            SqlServerTsqlDdlCompatibilityAnalyzer.EmptyRuleId =>
                (
                    "The T-SQL script contains no schema statements.",
                    "Provide a bounded additive DDL script."),
            SqlServerTsqlDdlCompatibilityAnalyzer.InternalRuleId =>
                (
                    "The T-SQL DDL proof could not be completed safely.",
                    "Treat the script as unproven and retry."),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedStatementRuleId =>
                (
                    "A statement kind is outside the qualified T-SQL DDL subset.",
                    "Remove or rewrite the unsupported statement."),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedFeatureRuleId =>
                (
                    "A schema feature is outside the qualified T-SQL DDL subset.",
                    "Remove or rewrite the unsupported schema feature."),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .DuplicateObjectRuleId =>
                (
                    "The script declares a duplicate schema identity.",
                    "Use distinct bounded schema identities."),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .InvalidReferenceRuleId =>
                (
                    "A schema reference could not be resolved safely.",
                    "Correct the source-order reference and retry."),
            SqlServerTsqlDdlCompatibilityAnalyzer
                .TextCollationRuleId =>
                (
                    "SQL Server text collation semantics remain unresolved.",
                    "Review source and target text comparison semantics."),
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId =>
                (
                    "At least one schema object is not proven by the target capability catalog.",
                    "Remove or rewrite the unproven schema feature."),
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId =>
                (
                    "The candidate DDL exceeded a production rendering limit.",
                    "Reduce the script or split it into bounded scripts."),
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId =>
                (
                    "The candidate schema could not be bound safely.",
                    "Treat the script as unproven and retry."),
            CSharpDbDdlCompatibilityAnalyzer
                .ScratchRejectedRuleId =>
                (
                    "The isolated scratch proof rejected the candidate schema.",
                    "Treat the script as unproven and review the reported evidence."),
            CSharpDbDdlCompatibilityAnalyzer
                .ScratchDifferentRuleId =>
                (
                    "The isolated scratch schema differs from the intended normalized schema.",
                    "Treat the script as unproven and review the reported evidence."),
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId =>
                (
                    "The proven candidate requires a deterministic canonical rewrite.",
                    "Review the generated migration plan before any apply workflow."),
            _ => (null, null),
        };
        return summary is not null;
    }

    private static bool IsAllowedDdlRule(string? ruleId) =>
        ruleId is not null &&
        (IsTsqlRule(ruleId) ||
         ruleId is
             CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId or
             CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId or
             CSharpDbDdlCompatibilityAnalyzer.InternalRuleId or
             CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId or
             CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId or
             CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId);

    private static bool IsTsqlRule(string ruleId) =>
        ruleId is
            SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer.EmptyRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer.InternalRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedStatementRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedFeatureRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .DuplicateObjectRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .InvalidReferenceRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .TextCollationRuleId;

    private static bool IsTsqlLoweringRule(string ruleId) =>
        ruleId is
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedStatementRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .UnsupportedFeatureRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .DuplicateObjectRuleId or
            SqlServerTsqlDdlCompatibilityAnalyzer
                .InvalidReferenceRuleId;

    private static bool IsSharedPostLoweringRule(string ruleId) =>
        ruleId is
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId or
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId or
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId;

    private static bool IsAllowedDdlStatementKind(string? kind) =>
        kind is "create-table" or "create-index" or
            "unsupported" or "unproven";

    private static bool IsAllowedDdlDifferenceKind(
        MigrationObjectKind kind) =>
        kind is MigrationObjectKind.Table or
            MigrationObjectKind.Column or
            MigrationObjectKind.Key or
            MigrationObjectKind.ForeignKey or
            MigrationObjectKind.Index;

    private static bool IsValidDdlStatementSpan(
        MigrationSourceSpan? span,
        int scriptCharacterCount) =>
        span is
        {
            SourceId: "input",
            Start: >= 0,
            Length: > 0,
            Line: >= 1,
            Column: >= 1,
        } &&
        span.Start.Value <=
            scriptCharacterCount - span.Length.Value;

    private static bool IsValidDdlLocationSpan(
        MigrationSourceSpan? span,
        int scriptCharacterCount) =>
        span is
        {
            SourceId: "input",
            Start: >= 0,
            Line: >= 1,
            Column: >= 1,
        } &&
        (span.Length is null || span.Length >= 0) &&
        span.Start.Value <= scriptCharacterCount &&
        (span.Length is null ||
         span.Start.Value <=
            scriptCharacterCount - span.Length.Value);

    private static bool HasExactDdlLocation(
        MigrationSourceSpan span,
        string script)
    {
        int sourceCursor = 0;
        int sourceLine = 1;
        int sourceColumn = 1;
        return HasExactDdlLocation(
            span,
            script,
            ref sourceCursor,
            ref sourceLine,
            ref sourceColumn);
    }

    private static bool HasExactDdlLocation(
        MigrationSourceSpan span,
        string script,
        ref int sourceCursor,
        ref int sourceLine,
        ref int sourceColumn)
    {
        int target = span.Start!.Value;
        if (target < sourceCursor)
            return false;
        while (sourceCursor < target)
        {
            char character = script[sourceCursor++];
            if (character == '\r')
            {
                if (sourceCursor < target &&
                    script[sourceCursor] == '\n')
                {
                    sourceCursor++;
                }
                sourceLine++;
                sourceColumn = 1;
            }
            else if (character == '\n')
            {
                sourceLine++;
                sourceColumn = 1;
            }
            else
            {
                sourceColumn++;
            }
        }

        return span.Line == sourceLine &&
            span.Column == sourceColumn;
    }

    private static bool DdlSpansEqual(
        MigrationSourceSpan left,
        MigrationSourceSpan right) =>
        string.Equals(
            left.SourceId,
            right.SourceId,
            StringComparison.Ordinal) &&
        left.Start == right.Start &&
        left.Length == right.Length &&
        left.Line == right.Line &&
        left.Column == right.Column;

    private static MigrationSourceSpan CopyDdlSpan(
        MigrationSourceSpan span) =>
        new()
        {
            SourceId = span.SourceId,
            Start = span.Start,
            Length = span.Length,
            Line = span.Line,
            Column = span.Column,
        };

    private static bool IsOptionalLowerHexSha256(string? value) =>
        value is null || IsLowerHexSha256(value);

    private static bool IsLowerHexSha256(string? value)
    {
        if (value is null || value.Length != 64)
            return false;
        foreach (char character in value)
        {
            if (character is not (
                    >= '0' and <= '9' or
                    >= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
    }

    private static string ComputeDdlSourceDigest(
        ReadOnlySpan<byte> scriptBytes,
        CancellationToken cancellationToken)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(
            StrictUtf8.GetBytes(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .InputDigestDomain));
        hash.AppendData([0]);
        const int chunkSize = 64 * 1024;
        for (int offset = 0;
             offset < scriptBytes.Length;
             offset += chunkSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int length = Math.Min(
                chunkSize,
                scriptBytes.Length - offset);
            hash.AppendData(
                scriptBytes.Slice(offset, length));
        }

        return Convert.ToHexString(hash.GetHashAndReset())
            .ToLowerInvariant();
    }

    private static JsonSerializerOptions CreateDdlJsonOptions()
    {
        var options = new JsonSerializerOptions(
            JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    internal static string SerializeDdlReport(
        CSharpDbDdlCompatibilityReport report) =>
        JsonSerializer.Serialize(
            report,
            DdlJsonOptions);

    private static bool TryParseInvocation(
        IReadOnlyList<string> args,
        out string? environmentVariableName,
        out string? targetVersion)
    {
        environmentVariableName = null;
        targetVersion = null;
        if (args.Count != 6 ||
            !string.Equals(args[0], "--protocol", StringComparison.Ordinal) ||
            !string.Equals(args[1], Protocol, StringComparison.Ordinal) ||
            !string.Equals(
                args[2],
                "--connection-env",
                StringComparison.Ordinal) ||
            !IsSafeEnvironmentVariableName(args[3]) ||
            !string.Equals(
                args[4],
                "--target-version",
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[5]))
        {
            return false;
        }

        environmentVariableName = args[3];
        targetVersion = args[5];
        return true;
    }

    private static bool IsSafeEnvironmentVariableName(string? value)
    {
        if (value is not { Length: > 0 and <= 128 } ||
            !IsAsciiLetter(value[0]) &&
            value[0] != '_')
        {
            return false;
        }

        foreach (char character in value.AsSpan(1))
        {
            if (!IsAsciiLetter(character) &&
                character is not (>= '0' and <= '9') and not '_')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsAsciiLetter(char value) =>
        value is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z');

    private readonly record struct DdlInput(
        string Script,
        string ScriptDigest);

    private sealed class DdlInputLimitException : Exception
    {
    }

    private static async ValueTask<int> FailAsync(
        TextWriter error,
        int exitCode,
        string message)
    {
        try
        {
            await error.WriteAsync(message + "\n");
        }
        catch
        {
            // The exit code remains the only available protocol signal when
            // the inherited standard-error stream cannot be written.
        }

        return exitCode;
    }
}
