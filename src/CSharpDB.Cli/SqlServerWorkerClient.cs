using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Sql;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Cli;

internal enum SqlServerWorkerStatus
{
    Success,
    Missing,
    Incompatible,
    ConnectionUnavailable,
    InspectionFailed,
}

internal sealed record SqlServerWorkerResult
{
    internal required SqlServerWorkerStatus Status { get; init; }

    internal MigrationCatalog? Catalog { get; init; }

    internal static SqlServerWorkerResult Success(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        return new SqlServerWorkerResult
        {
            Status = SqlServerWorkerStatus.Success,
            Catalog = catalog,
        };
    }

    internal static SqlServerWorkerResult Failure(SqlServerWorkerStatus status)
    {
        if (status == SqlServerWorkerStatus.Success)
            throw new ArgumentOutOfRangeException(nameof(status));

        return new SqlServerWorkerResult { Status = status };
    }
}

internal enum SqlServerDdlWorkerStatus
{
    Success,
    Missing,
    Incompatible,
    AnalysisFailed,
}

internal sealed record SqlServerDdlWorkerResult
{
    internal required SqlServerDdlWorkerStatus Status { get; init; }

    internal CSharpDbDdlCompatibilityReport? Report { get; init; }

    internal static SqlServerDdlWorkerResult Success(
        CSharpDbDdlCompatibilityReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new SqlServerDdlWorkerResult
        {
            Status = SqlServerDdlWorkerStatus.Success,
            Report = report,
        };
    }

    internal static SqlServerDdlWorkerResult Failure(
        SqlServerDdlWorkerStatus status)
    {
        if (status == SqlServerDdlWorkerStatus.Success)
            throw new ArgumentOutOfRangeException(nameof(status));

        return new SqlServerDdlWorkerResult { Status = status };
    }
}

internal static class SqlServerWorkerClient
{
    internal const string ProtocolV1 = "csharpdb-sqlserver-worker/v1";
    internal const string DdlProtocolV1 =
        "csharpdb-sqlserver-ddl-worker/v1";
    internal const long MaxCatalogBytes = 64L * 1024 * 1024;
    internal const long MaxDdlReportBytes = 8L * 1024 * 1024;
    internal const long MaxStderrBytes = 64L * 1024;
    internal const int MaxDdlDifferenceCount = 200_000;

    private const int ExitIncompatible = 10;
    private const int ExitConnectionUnavailable = 11;
    private const int ExitInspectionFailed = 12;
    private const int ExitInternalFailure = 13;
    private const string TsqlParseRuleId =
        "tsql.ddl.script.parse";
    private const string TsqlLimitRuleId =
        "tsql.ddl.script.limit";
    private const string TsqlEmptyRuleId =
        "tsql.ddl.script.empty";
    private const string TsqlInternalRuleId =
        "tsql.ddl.proof.unavailable";
    private const string TsqlUnsupportedStatementRuleId =
        "tsql.ddl.statement.unsupported";
    private const string TsqlUnsupportedFeatureRuleId =
        "tsql.ddl.feature.unsupported";
    private const string TsqlDuplicateObjectRuleId =
        "tsql.ddl.object.duplicate";
    private const string TsqlInvalidReferenceRuleId =
        "tsql.ddl.reference.invalid";
    private const string TsqlTextCollationRuleId =
        "tsql.ddl.collation.unresolved";
    private static readonly byte[] HeaderBytes =
        Encoding.ASCII.GetBytes(ProtocolV1 + "\n");
    private static readonly byte[] DdlHeaderBytes =
        Encoding.ASCII.GetBytes(DdlProtocolV1 + "\n");
    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions DdlJsonOptions =
        CreateDdlJsonOptions();

    internal static async ValueTask<SqlServerDdlWorkerResult>
        AnalyzeDdlAsync(
            string script,
            string targetCSharpDbVersion,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(script);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetCSharpDbVersion);
        cancellationToken.ThrowIfCancellationRequested();
        if (script.Length >
            SqlScriptParserOptions.HardMaxScriptCharacters)
        {
            return SqlServerDdlWorkerResult.Failure(
                SqlServerDdlWorkerStatus.Incompatible);
        }

        byte[] scriptBytes;
        try
        {
            scriptBytes = StrictUtf8.GetBytes(script);
        }
        catch (EncoderFallbackException)
        {
            return SqlServerDdlWorkerResult.Failure(
                SqlServerDdlWorkerStatus.Incompatible);
        }
        if (scriptBytes.LongLength >
            SqlScriptParserOptions.HardMaxScriptUtf8Bytes)
        {
            CryptographicOperations.ZeroMemory(scriptBytes);
            return SqlServerDdlWorkerResult.Failure(
                SqlServerDdlWorkerStatus.Incompatible);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            string expectedSourceDigest =
                ComputeDdlSourceDigest(scriptBytes);
            cancellationToken.ThrowIfCancellationRequested();
            string workerDirectory = Path.Combine(
                AppContext.BaseDirectory,
                "adapters",
                "sqlserver");
            string workerPath = Path.Combine(
                workerDirectory,
                OperatingSystem.IsWindows()
                    ? "csharpdb-migration-sqlserver-worker.exe"
                    : "csharpdb-migration-sqlserver-worker");
            if (!File.Exists(workerPath))
            {
                return SqlServerDdlWorkerResult.Failure(
                    SqlServerDdlWorkerStatus.Missing);
            }

            var startInfo = new ProcessStartInfo
            {
                FileName = workerPath,
                WorkingDirectory = workerDirectory,
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add("--protocol");
            startInfo.ArgumentList.Add(DdlProtocolV1);
            startInfo.ArgumentList.Add("--target-version");
            startInfo.ArgumentList.Add(targetCSharpDbVersion);

            using var process = new Process { StartInfo = startInfo };
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!process.Start())
                {
                    return SqlServerDdlWorkerResult.Failure(
                        SqlServerDdlWorkerStatus.Missing);
                }
            }
            catch (Exception exception) when (
                exception is Win32Exception or IOException or
                    InvalidOperationException or UnauthorizedAccessException)
            {
                return SqlServerDdlWorkerResult.Failure(
                    SqlServerDdlWorkerStatus.Missing);
            }

            WorkerProcessContainment containment;
            try
            {
                containment = WorkerProcessContainment.Attach(process);
            }
            catch (Exception exception) when (
                exception is Win32Exception or IOException or
                    InvalidOperationException or NotSupportedException)
            {
                if (!await KillAndWaitAsync(process).ConfigureAwait(false))
                    throw new WorkerTerminationException();
                return SqlServerDdlWorkerResult.Failure(
                    SqlServerDdlWorkerStatus.Incompatible);
            }
            using WorkerProcessContainment containmentScope = containment;

            byte[] stdout;
            using var processCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            Task inputTask = WriteAndCloseAsync(
                process.StandardInput.BaseStream,
                scriptBytes,
                processCancellation.Token);
            Task<byte[]> stdoutTask = ReadBoundedAsync(
                process.StandardOutput.BaseStream,
                checked(
                    MaxDdlReportBytes +
                    DdlHeaderBytes.LongLength),
                processCancellation.Token);
            Task stderrTask = DrainBoundedAsync(
                process.StandardError.BaseStream,
                MaxStderrBytes,
                processCancellation.Token);
            Task exitTask =
                process.WaitForExitAsync(processCancellation.Token);

            try
            {
                await ObserveAllAsync(
                        [inputTask, stdoutTask, stderrTask, exitTask])
                    .ConfigureAwait(false);
                stdout = await stdoutTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (
                cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await TerminateAsync(
                            process,
                            containmentScope,
                            processCancellation,
                            inputTask,
                            stdoutTask,
                            stderrTask,
                            exitTask)
                        .ConfigureAwait(false);
                }
                finally
                {
                    ClearCompletedOutput(stdoutTask);
                }
                throw;
            }
            catch (Exception exception) when (
                exception is WorkerOutputLimitException or IOException or
                    InvalidOperationException)
            {
                try
                {
                    await TerminateAsync(
                            process,
                            containmentScope,
                            processCancellation,
                            inputTask,
                            stdoutTask,
                            stderrTask,
                            exitTask)
                        .ConfigureAwait(false);
                }
                finally
                {
                    ClearCompletedOutput(stdoutTask);
                }
                return SqlServerDdlWorkerResult.Failure(
                    SqlServerDdlWorkerStatus.Incompatible);
            }

            try
            {
                return process.ExitCode switch
                {
                    0 => ParseDdlSuccess(
                        stdout,
                        targetCSharpDbVersion,
                        expectedSourceDigest,
                        script),
                    ExitInspectionFailed =>
                        SqlServerDdlWorkerResult.Failure(
                            SqlServerDdlWorkerStatus.AnalysisFailed),
                    ExitIncompatible or ExitConnectionUnavailable or
                    ExitInternalFailure =>
                        SqlServerDdlWorkerResult.Failure(
                            SqlServerDdlWorkerStatus.Incompatible),
                    _ => SqlServerDdlWorkerResult.Failure(
                            SqlServerDdlWorkerStatus.Incompatible),
                };
            }
            finally
            {
                CryptographicOperations.ZeroMemory(stdout);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(scriptBytes);
        }
    }

    internal static async ValueTask<SqlServerWorkerResult> InspectAsync(
        string connectionEnvironmentVariableName,
        string targetCSharpDbVersion,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionEnvironmentVariableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetCSharpDbVersion);
        cancellationToken.ThrowIfCancellationRequested();

        string workerDirectory = Path.Combine(
            AppContext.BaseDirectory,
            "adapters",
            "sqlserver");
        string workerPath = Path.Combine(
            workerDirectory,
            OperatingSystem.IsWindows()
                ? "csharpdb-migration-sqlserver-worker.exe"
                : "csharpdb-migration-sqlserver-worker");
        if (!File.Exists(workerPath))
        {
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Missing);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = workerPath,
            WorkingDirectory = workerDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("--protocol");
        startInfo.ArgumentList.Add(ProtocolV1);
        startInfo.ArgumentList.Add("--connection-env");
        startInfo.ArgumentList.Add(connectionEnvironmentVariableName);
        startInfo.ArgumentList.Add("--target-version");
        startInfo.ArgumentList.Add(targetCSharpDbVersion);

        using var process = new Process { StartInfo = startInfo };
        try
        {
            if (!process.Start())
            {
                return SqlServerWorkerResult.Failure(
                    SqlServerWorkerStatus.Missing);
            }
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or UnauthorizedAccessException)
        {
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Missing);
        }

        WorkerProcessContainment containment;
        try
        {
            containment = WorkerProcessContainment.Attach(process);
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or NotSupportedException)
        {
            if (!await KillAndWaitAsync(process).ConfigureAwait(false))
                throw new WorkerTerminationException();
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Incompatible);
        }
        using WorkerProcessContainment containmentScope = containment;

        byte[] stdout;
        using var processCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        Task<byte[]> stdoutTask = ReadBoundedAsync(
            process.StandardOutput.BaseStream,
            checked(MaxCatalogBytes + HeaderBytes.LongLength),
            processCancellation.Token);
        Task stderrTask = DrainBoundedAsync(
            process.StandardError.BaseStream,
            MaxStderrBytes,
            processCancellation.Token);
        Task exitTask = process.WaitForExitAsync(processCancellation.Token);

        try
        {
            await ObserveAllAsync(
                    [stdoutTask, stderrTask, exitTask])
                .ConfigureAwait(false);
            stdout = await stdoutTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            cancellationToken.IsCancellationRequested)
        {
            try
            {
                await TerminateAsync(
                        process,
                        containmentScope,
                        processCancellation,
                        stdoutTask,
                        stderrTask,
                        exitTask)
                    .ConfigureAwait(false);
            }
            finally
            {
                ClearCompletedOutput(stdoutTask);
            }
            throw;
        }
        catch (Exception exception) when (
            exception is WorkerOutputLimitException or IOException or
                InvalidOperationException)
        {
            try
            {
                await TerminateAsync(
                        process,
                        containmentScope,
                        processCancellation,
                        stdoutTask,
                        stderrTask,
                        exitTask)
                    .ConfigureAwait(false);
            }
            finally
            {
                ClearCompletedOutput(stdoutTask);
            }
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Incompatible);
        }

        try
        {
            return process.ExitCode switch
            {
                0 => ParseSuccess(stdout, targetCSharpDbVersion),
                ExitConnectionUnavailable =>
                    SqlServerWorkerResult.Failure(
                        SqlServerWorkerStatus.ConnectionUnavailable),
                ExitInspectionFailed =>
                    SqlServerWorkerResult.Failure(
                        SqlServerWorkerStatus.InspectionFailed),
                ExitIncompatible or ExitInternalFailure =>
                    SqlServerWorkerResult.Failure(
                        SqlServerWorkerStatus.Incompatible),
                _ => SqlServerWorkerResult.Failure(
                    SqlServerWorkerStatus.Incompatible),
            };
        }
        finally
        {
            CryptographicOperations.ZeroMemory(stdout);
        }
    }

    private static SqlServerWorkerResult ParseSuccess(
        byte[] stdout,
        string targetCSharpDbVersion)
    {
        if (stdout.Length <= HeaderBytes.Length ||
            !stdout.AsSpan(0, HeaderBytes.Length).SequenceEqual(HeaderBytes))
        {
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Incompatible);
        }

        try
        {
            string json = StrictUtf8.GetString(
                stdout,
                HeaderBytes.Length,
                stdout.Length - HeaderBytes.Length);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(json);
            if (catalog.Source.Kind != MigrationSourceKind.SqlServer ||
                !string.Equals(
                    catalog.TargetCSharpDbVersion,
                    targetCSharpDbVersion,
                    StringComparison.Ordinal))
            {
                return SqlServerWorkerResult.Failure(
                    SqlServerWorkerStatus.Incompatible);
            }

            return SqlServerWorkerResult.Success(catalog);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or InvalidDataException or
                JsonException or ArgumentException or FormatException or
                OverflowException)
        {
            return SqlServerWorkerResult.Failure(
                SqlServerWorkerStatus.Incompatible);
        }
    }

    private static SqlServerDdlWorkerResult ParseDdlSuccess(
        byte[] stdout,
        string targetCSharpDbVersion,
        string expectedSourceDigest,
        string sourceScript)
    {
        if (stdout.Length <= DdlHeaderBytes.Length ||
            !stdout
                .AsSpan(0, DdlHeaderBytes.Length)
                .SequenceEqual(DdlHeaderBytes))
        {
            return SqlServerDdlWorkerResult.Failure(
                SqlServerDdlWorkerStatus.Incompatible);
        }

        try
        {
            string json = StrictUtf8.GetString(
                stdout,
                DdlHeaderBytes.Length,
                stdout.Length - DdlHeaderBytes.Length);
            CSharpDbDdlCompatibilityReport? report =
                JsonSerializer.Deserialize<
                    CSharpDbDdlCompatibilityReport>(
                    json,
                    DdlJsonOptions);
            if (!TrySanitizeDdlReport(
                    report,
                    targetCSharpDbVersion,
                    expectedSourceDigest,
                    sourceScript,
                    out CSharpDbDdlCompatibilityReport? sanitized))
            {
                return SqlServerDdlWorkerResult.Failure(
                    SqlServerDdlWorkerStatus.Incompatible);
            }

            return SqlServerDdlWorkerResult.Success(sanitized!);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or InvalidDataException or
                JsonException or ArgumentException or FormatException or
                OverflowException or NotSupportedException)
        {
            return SqlServerDdlWorkerResult.Failure(
                SqlServerDdlWorkerStatus.Incompatible);
        }
    }

    internal static bool TrySanitizeDdlReport(
        CSharpDbDdlCompatibilityReport? report,
        string targetCSharpDbVersion,
        string script,
        out CSharpDbDdlCompatibilityReport? sanitized)
    {
        sanitized = null;
        byte[] scriptBytes;
        try
        {
            scriptBytes = StrictUtf8.GetBytes(script);
        }
        catch (EncoderFallbackException)
        {
            return false;
        }

        try
        {
            return TrySanitizeDdlReport(
                report,
                targetCSharpDbVersion,
                ComputeDdlSourceDigest(scriptBytes),
                script,
                out sanitized);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(scriptBytes);
        }
    }

    private static bool TrySanitizeDdlReport(
        CSharpDbDdlCompatibilityReport? report,
        string targetCSharpDbVersion,
        string expectedSourceDigest,
        string sourceScript,
        out CSharpDbDdlCompatibilityReport? sanitized)
    {
        sanitized = null;
        int sourceCharacterLength = sourceScript.Length;
        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        if (report is null ||
            sourceCharacterLength < 0 ||
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
                "tsql160",
                StringComparison.Ordinal) ||
            !string.Equals(
                report.TargetCSharpDbVersion,
                targetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.TargetCSharpDbVersion,
                capabilities.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.CapabilityDigest,
                capabilities.Digest,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.ScriptDigest,
                expectedSourceDigest,
                StringComparison.Ordinal) ||
            !IsLowerSha256(report.CapabilityDigest) ||
            !IsLowerSha256(report.ScriptDigest) ||
            !IsAllowedDdlRuleId(report.RuleId) ||
            !Enum.IsDefined(report.Status) ||
            report.Status ==
                MigrationCompatibilityStatus.Compatible ||
            !IsValidDdlEvidence(report.HighestEvidence) ||
            report.StatementCount < 0 ||
            report.StatementCount >
                SqlScriptParserOptions.HardMaxStatementCount ||
            report.ProvenStatementCount < 0 ||
            report.ProvenStatementCount > report.StatementCount ||
            report.CandidateActionCount < 0 ||
            report.CandidateActionCount >
                CSharpDbDdlScratchValidationOptions
                    .HardMaxActionCount ||
            !IsOptionalLowerSha256(report.CatalogDigest) ||
            !IsOptionalLowerSha256(report.PlanContractDigest) ||
            !IsOptionalLowerSha256(report.GeneratedDdlDigest) ||
            !IsOptionalLowerSha256(report.ExpectedSchemaDigest) ||
            !IsOptionalLowerSha256(report.ActualSchemaDigest) ||
            report.Statements is null ||
            report.Diagnostics is null ||
            report.Differences is null)
        {
            return false;
        }

        int maximumDiagnostics = report.StatementCount == 0
            ? 1
            : checked(report.StatementCount + 2);
        if (report.Statements.Count != report.StatementCount ||
            report.Diagnostics.Count == 0 ||
            report.Diagnostics.Count > maximumDiagnostics ||
            report.Differences.Count > MaxDdlDifferenceCount ||
            !IsValidDdlRoot(report))
        {
            return false;
        }

        int previousEnd = 0;
        int sourceCursor = 0;
        int sourceLine = 1;
        int sourceColumn = 1;
        for (int index = 0;
             index < report.Statements.Count;
             index++)
        {
            CSharpDbDdlCompatibilityStatement? statement =
                report.Statements[index];
            if (statement is null ||
                statement.Index != index ||
                !IsAllowedDdlStatementKind(statement.Kind) ||
                !IsValidDdlStatementSpan(
                    statement.Span,
                    sourceCharacterLength) ||
                statement.Span.Start!.Value < previousEnd ||
                !HasExactDdlLocation(
                    statement.Span,
                    sourceScript,
                    ref sourceCursor,
                    ref sourceLine,
                    ref sourceColumn) ||
                !IsAllowedDdlRuleId(statement.RuleId) ||
                !Enum.IsDefined(statement.Status) ||
                statement.Status ==
                    MigrationCompatibilityStatus.Compatible ||
                !IsValidDdlEvidence(statement.Evidence) ||
                report.HighestEvidence is not { } highestEvidence ||
                statement.Evidence > highestEvidence)
            {
                return false;
            }

            previousEnd = checked(
                statement.Span.Start.Value +
                statement.Span.Length!.Value);
        }
        if (!AreDdlStatementsCoherent(
                report,
                report.Statements))
        {
            return false;
        }

        var sanitizedDiagnostics =
            new CSharpDbDdlCompatibilityDiagnostic[
                report.Diagnostics.Count];
        for (int index = 0;
             index < report.Diagnostics.Count;
             index++)
        {
            CSharpDbDdlCompatibilityDiagnostic? diagnostic =
                report.Diagnostics[index];
            if (diagnostic is null ||
                diagnostic.Ordinal != index ||
                !IsAllowedDdlRuleId(diagnostic.RuleId) ||
                !string.Equals(
                    diagnostic.DiagnosticId,
                    ExpectedDdlDiagnosticId(
                        index,
                        diagnostic.RuleId),
                    StringComparison.Ordinal) ||
                diagnostic.Severity is not (
                    MigrationDiagnosticSeverity.Warning or
                    MigrationDiagnosticSeverity.Error) ||
                !Enum.IsDefined(diagnostic.Status) ||
                diagnostic.Status ==
                    MigrationCompatibilityStatus.Compatible ||
                !IsValidDdlEvidence(diagnostic.Evidence) ||
                diagnostic.Evidence is { } diagnosticEvidence &&
                (report.HighestEvidence is not { } highestEvidence ||
                 diagnosticEvidence > highestEvidence) ||
                diagnostic.StatementIndex is { } boundedStatementIndex &&
                (boundedStatementIndex < 0 ||
                 boundedStatementIndex >= report.StatementCount) ||
                !IsDdlDiagnosticRuleCoherent(
                    diagnostic,
                    report))
            {
                return false;
            }

            if (diagnostic.StatementIndex is { } diagnosticStatementIndex)
            {
                if (diagnostic.SourceSpan is null ||
                    !DdlSpansEqual(
                        diagnostic.SourceSpan,
                        report.Statements[
                            diagnosticStatementIndex].Span))
                {
                    return false;
                }
            }
            else if (string.Equals(
                         diagnostic.RuleId,
                         "tsql.ddl.script.parse",
                         StringComparison.Ordinal))
            {
                if (!IsValidDdlLocationSpan(
                        diagnostic.SourceSpan,
                        sourceCharacterLength) ||
                    diagnostic.SourceSpan!.Length is not null ||
                    !HasExactDdlLocation(
                        diagnostic.SourceSpan,
                        sourceScript))
                {
                    return false;
                }
            }
            else if (diagnostic.SourceSpan is not null)
            {
                return false;
            }

            sanitizedDiagnostics[index] = diagnostic with
            {
                Summary = SafeDdlSummary(diagnostic.RuleId),
                Remediation =
                    SafeDdlRemediation(diagnostic.RuleId),
            };
        }
        if (!AreDdlDiagnosticsCoherent(
                report,
                report.Statements,
                sanitizedDiagnostics))
        {
            return false;
        }

        var differenceIdentities =
            new HashSet<string>(StringComparer.Ordinal);
        for (int index = 0;
             index < report.Differences.Count;
             index++)
        {
            CSharpDbDdlScratchValidationDifference? difference =
                report.Differences[index];
            if (difference is null ||
                difference.Ordinal != index ||
                !IsLowerSha256(
                    difference.ObjectIdentityDigest) ||
                !differenceIdentities.Add(
                    difference.ObjectIdentityDigest) ||
                !IsAllowedDdlDifferenceKind(
                    difference.Kind) ||
                !IsOptionalLowerSha256(
                    difference.ExpectedDefinitionDigest) ||
                !IsOptionalLowerSha256(
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
        }

        if (!HasValidDdlDigestShape(
                report,
                report.Differences.Count))
        {
            return false;
        }

        sanitized = report with
        {
            Diagnostics = sanitizedDiagnostics,
        };
        return true;
    }

    private static bool IsValidDdlStatementSpan(
        MigrationSourceSpan? span,
        int sourceCharacterLength) =>
        span is
        {
            SourceId: "input",
            Start: >= 0,
            Length: > 0,
            Line: >= 1,
            Column: >= 1,
        } &&
        span.Start.Value <=
            sourceCharacterLength - span.Length.Value;

    private static bool IsValidDdlLocationSpan(
        MigrationSourceSpan? span,
        int sourceCharacterLength) =>
        span is
        {
            SourceId: "input",
            Start: >= 0,
            Line: >= 1,
            Column: >= 1,
        } &&
        (span.Length is null || span.Length >= 0) &&
        span.Start.Value <= sourceCharacterLength &&
        (span.Length is null ||
         span.Start.Value <=
            sourceCharacterLength - span.Length.Value);

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

    private static bool IsValidDdlEvidence(
        MigrationEvidenceLevel? evidence) =>
        evidence is null ||
        Enum.IsDefined(evidence.Value) &&
        evidence.Value <= MigrationEvidenceLevel.ScratchExecuted;

    private static bool IsAllowedDdlStatementKind(string? kind) =>
        kind is "create-table" or "create-index" or
            "unsupported" or "unproven";

    private static bool IsAllowedDdlRuleId(string? ruleId) =>
        ruleId is
            TsqlParseRuleId or
            TsqlLimitRuleId or
            TsqlEmptyRuleId or
            TsqlInternalRuleId or
            TsqlUnsupportedStatementRuleId or
            TsqlUnsupportedFeatureRuleId or
            TsqlDuplicateObjectRuleId or
            TsqlInvalidReferenceRuleId or
            TsqlTextCollationRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId or
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId or
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId or
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId or
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId;

    private static bool IsValidDdlRoot(
        CSharpDbDdlCompatibilityReport report)
    {
        bool noStatements = report.StatementCount == 0;
        bool hasStatements = report.StatementCount > 0;
        bool validRoot = report.RuleId switch
        {
            TsqlParseRuleId =>
                noStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence is null,
            TsqlLimitRuleId =>
                report.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                (noStatements &&
                 report.HighestEvidence is null ||
                 hasStatements &&
                 report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed),
            TsqlEmptyRuleId =>
                noStatements &&
                report.Status ==
                    MigrationCompatibilityStatus.Unsupported &&
                report.HighestEvidence ==
                    MigrationEvidenceLevel.Parsed,
            TsqlInternalRuleId =>
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
            TsqlTextCollationRuleId =>
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
                            TsqlUnsupportedStatementRuleId,
                            StringComparison.Ordinal) ||
                        statement.Kind != "unsupported" &&
                        string.Equals(
                            statement.RuleId,
                            TsqlUnsupportedStatementRuleId,
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
                    TsqlInternalRuleId,
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

    private static bool IsDdlDiagnosticRuleCoherent(
        CSharpDbDdlCompatibilityDiagnostic diagnostic,
        CSharpDbDdlCompatibilityReport report)
    {
        bool noStatement = diagnostic.StatementIndex is null;
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
            TsqlParseRuleId =>
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
            TsqlLimitRuleId or
            TsqlEmptyRuleId or
            TsqlInternalRuleId =>
                DdlDiagnosticMatchesRoot(
                    diagnostic,
                    report,
                    requireNoLocation: true),
            TsqlTextCollationRuleId =>
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
                    TsqlTextCollationRuleId &&
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
                TsqlTextCollationRuleId,
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
                    TsqlTextCollationRuleId,
                    StringComparison.Ordinal) &&
                string.Equals(
                    diagnostics[1].RuleId,
                    CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
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
                    TsqlTextCollationRuleId,
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
        bool catalog = report.CatalogDigest is not null;
        bool plan = report.PlanContractDigest is not null;
        bool generated = report.GeneratedDdlDigest is not null;
        bool expected = report.ExpectedSchemaDigest is not null;
        bool actual = report.ActualSchemaDigest is not null;
        return report.RuleId switch
        {
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId or
            TsqlTextCollationRuleId =>
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

    private static bool IsTsqlLoweringRule(string ruleId) =>
        ruleId is
            TsqlUnsupportedStatementRuleId or
            TsqlUnsupportedFeatureRuleId or
            TsqlDuplicateObjectRuleId or
            TsqlInvalidReferenceRuleId;

    private static bool IsSharedPostLoweringRule(string ruleId) =>
        ruleId is
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId or
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId or
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId;

    private static bool IsAllowedDdlDifferenceKind(
        MigrationObjectKind kind) =>
        kind is MigrationObjectKind.Table or
            MigrationObjectKind.Column or
            MigrationObjectKind.Key or
            MigrationObjectKind.ForeignKey or
            MigrationObjectKind.Index;

    private static string ExpectedDdlDiagnosticId(
        int ordinal,
        string ruleId) =>
        string.Concat(
            ruleId.StartsWith(
                "tsql.",
                StringComparison.Ordinal)
                ? "tsql-ddl/"
                : "csharpdb-ddl/",
            ordinal.ToString(
                "D6",
                System.Globalization.CultureInfo.InvariantCulture),
            "/",
            ruleId);

    private static string SafeDdlSummary(string ruleId) =>
        ruleId switch
        {
            "tsql.ddl.script.parse" =>
                "The T-SQL script could not be parsed completely.",
            "tsql.ddl.script.limit" =>
                "The T-SQL script exceeded a production analysis limit.",
            "tsql.ddl.script.empty" =>
                "The T-SQL script contains no schema statements.",
            "tsql.ddl.proof.unavailable" =>
                "The T-SQL DDL proof could not be completed safely.",
            "tsql.ddl.statement.unsupported" =>
                "A statement kind is outside the T-SQL DDL allowlist.",
            "tsql.ddl.feature.unsupported" =>
                "A T-SQL DDL feature is outside the bounded allowlist.",
            "tsql.ddl.object.duplicate" =>
                "The script declares a duplicate schema object.",
            "tsql.ddl.reference.invalid" =>
                "The script contains an unresolved or unsupported schema reference.",
            "tsql.ddl.collation.unresolved" =>
                "SQL Server text collation semantics remain unresolved.",
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId =>
                "The isolated scratch proof rejected the candidate schema.",
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId =>
                "The isolated scratch schema differs from the intended normalized schema.",
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId =>
                "The candidate DDL exceeded a production rendering limit.",
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId =>
                "At least one schema object is not proven by the target capability catalog.",
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId =>
                "The candidate schema could not be proven safely.",
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId =>
                "The proven candidate requires a deterministic canonical rewrite.",
            _ => throw new UnreachableException(),
        };

    private static string? SafeDdlRemediation(string ruleId) =>
        ruleId switch
        {
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId or
            CSharpDbDdlCompatibilityAnalyzer.ScratchDifferentRuleId =>
                "Treat the script as unproven and review the reported evidence.",
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId =>
                "Reduce the script or split it into independently reviewed bounded scripts.",
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId =>
                "Remove or rewrite the unproven schema feature.",
            CSharpDbDdlCompatibilityAnalyzer.InternalRuleId =>
                "Review the reported supported subset and retry.",
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId =>
                "Review the generated migration plan before any apply workflow.",
            _ => null,
        };

    private static bool IsOptionalLowerSha256(string? value) =>
        value is null || IsLowerSha256(value);

    private static bool IsLowerSha256(string? value)
    {
        if (value is not { Length: 64 })
            return false;
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
    }

    private static string ComputeDdlSourceDigest(
        ReadOnlySpan<byte> scriptBytes)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData("tsql-ddl-input/v1"u8);
        hash.AppendData([0]);
        hash.AppendData(scriptBytes);
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

    private static async Task ObserveAllAsync(IReadOnlyList<Task> tasks)
    {
        var remaining = new List<Task>(tasks);
        while (remaining.Count > 0)
        {
            Task completed = await Task.WhenAny(remaining)
                .ConfigureAwait(false);
            await completed.ConfigureAwait(false);
            remaining.Remove(completed);
        }
    }

    private static async Task WriteAndCloseAsync(
        Stream stream,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        try
        {
            await stream.WriteAsync(payload, cancellationToken)
                .ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            try
            {
                stream.Close();
            }
            catch (Exception exception) when (
                exception is IOException or ObjectDisposedException)
            {
            }
        }
    }

    private static async Task<byte[]> ReadBoundedAsync(
        Stream stream,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        if (maxBytes is <= 0 or > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(maxBytes));

        int maximum = checked((int)maxBytes);
        byte[] payload = ArrayPool<byte>.Shared.Rent(maximum);
        byte[] overflowProbe = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            int total = 0;
            while (total < maximum)
            {
                int read = await stream.ReadAsync(
                        payload.AsMemory(
                            total,
                            Math.Min(64 * 1024, maximum - total)),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    return payload.AsSpan(0, total).ToArray();
                total += read;
            }

            int extra = await stream.ReadAsync(
                    overflowProbe.AsMemory(0, 1),
                    cancellationToken)
                .ConfigureAwait(false);
            if (extra != 0)
            {
                throw new WorkerOutputLimitException();
            }

            return payload.AsSpan(0, total).ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload, clearArray: true);
            ArrayPool<byte>.Shared.Return(
                overflowProbe,
                clearArray: true);
        }
    }

    private static async Task DrainBoundedAsync(
        Stream stream,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(8 * 1024);
        try
        {
            long total = 0;
            while (true)
            {
                int read = await stream.ReadAsync(
                        buffer.AsMemory(0, buffer.Length),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    return;
                if (total > maxBytes - read)
                    throw new WorkerOutputLimitException();
                total += read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static async Task TerminateAsync(
        Process process,
        WorkerProcessContainment containment,
        CancellationTokenSource processCancellation,
        params Task[] tasks)
    {
        containment.Terminate();
        bool processStopped =
            await KillAndWaitAsync(process).ConfigureAwait(false);

        processCancellation.Cancel();
        try
        {
            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
            await Task.WhenAll(tasks)
                .WaitAsync(timeout.Token)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (
            exception is OperationCanceledException or IOException or
                InvalidOperationException or WorkerOutputLimitException)
        {
        }

        if (!processStopped || tasks.Any(static task => !task.IsCompleted))
            throw new WorkerTerminationException();
    }

    private static void ClearCompletedOutput(Task<byte[]> stdoutTask)
    {
        if (stdoutTask.Status == TaskStatus.RanToCompletion)
            CryptographicOperations.ZeroMemory(stdoutTask.Result);
    }

    private static async Task<bool> KillAndWaitAsync(Process process)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (process.HasExited)
                    return true;
                process.Kill(entireProcessTree: true);
            }
            catch (Exception exception) when (
                exception is Win32Exception or InvalidOperationException or
                    NotSupportedException)
            {
            }

            try
            {
                using var exitTimeout = new CancellationTokenSource(
                    TimeSpan.FromSeconds(2));
                await process.WaitForExitAsync(exitTimeout.Token)
                    .ConfigureAwait(false);
                return true;
            }
            catch (Exception exception) when (
                exception is OperationCanceledException or
                    InvalidOperationException)
            {
            }
        }

        try
        {
            return process.HasExited;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private sealed class WorkerOutputLimitException : IOException;

    private sealed class WorkerTerminationException : IOException;

    private sealed class WorkerProcessContainment : IDisposable
    {
        private const uint KillOnJobClose = 0x00002000;
        private const uint ProcessMemoryLimit = 0x00000100;
        private const ulong WorkerMemoryLimitBytes =
            512UL * 1024UL * 1024UL;
        private SafeFileHandle? job;

        private WorkerProcessContainment(SafeFileHandle? job)
        {
            this.job = job;
        }

        internal static WorkerProcessContainment Attach(Process process)
        {
            if (!OperatingSystem.IsWindows())
                return new WorkerProcessContainment(job: null);

            SafeFileHandle job = CreateJobObject(
                IntPtr.Zero,
                lpName: null);
            if (job.IsInvalid)
                throw new Win32Exception(Marshal.GetLastPInvokeError());

            try
            {
                var limits = new JobObjectExtendedLimitInformation
                {
                    BasicLimitInformation = new JobObjectBasicLimitInformation
                    {
                        LimitFlags = KillOnJobClose | ProcessMemoryLimit,
                    },
                    ProcessMemoryLimit =
                        new UIntPtr(WorkerMemoryLimitBytes),
                };
                if (!SetInformationJobObject(
                        job,
                        JobObjectInformationClass.ExtendedLimitInformation,
                        ref limits,
                        (uint)Marshal.SizeOf<JobObjectExtendedLimitInformation>()))
                {
                    throw new Win32Exception(Marshal.GetLastPInvokeError());
                }
                if (!AssignProcessToJobObject(job, process.Handle))
                    throw new Win32Exception(Marshal.GetLastPInvokeError());

                return new WorkerProcessContainment(job);
            }
            catch
            {
                job.Dispose();
                try
                {
                    if (!process.HasExited)
                        process.Kill(entireProcessTree: true);
                }
                catch (Exception exception) when (
                    exception is Win32Exception or InvalidOperationException or
                        NotSupportedException)
                {
                }

                throw;
            }
        }

        internal void Terminate()
        {
            SafeFileHandle? handle = Interlocked.Exchange(ref job, null);
            handle?.Dispose();
        }

        public void Dispose() => Terminate();

        [DllImport(
            "kernel32.dll",
            EntryPoint = "CreateJobObjectW",
            CharSet = CharSet.Unicode,
            SetLastError = true)]
        private static extern SafeFileHandle CreateJobObject(
            IntPtr lpJobAttributes,
            string? lpName);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetInformationJobObject(
            SafeFileHandle hJob,
            JobObjectInformationClass jobObjectInformationClass,
            ref JobObjectExtendedLimitInformation lpJobObjectInformation,
            uint cbJobObjectInformationLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool AssignProcessToJobObject(
            SafeFileHandle hJob,
            IntPtr hProcess);

        private enum JobObjectInformationClass
        {
            ExtendedLimitInformation = 9,
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JobObjectBasicLimitInformation
        {
            internal long PerProcessUserTimeLimit;
            internal long PerJobUserTimeLimit;
            internal uint LimitFlags;
            internal UIntPtr MinimumWorkingSetSize;
            internal UIntPtr MaximumWorkingSetSize;
            internal uint ActiveProcessLimit;
            internal UIntPtr Affinity;
            internal uint PriorityClass;
            internal uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IoCounters
        {
            internal ulong ReadOperationCount;
            internal ulong WriteOperationCount;
            internal ulong OtherOperationCount;
            internal ulong ReadTransferCount;
            internal ulong WriteTransferCount;
            internal ulong OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JobObjectExtendedLimitInformation
        {
            internal JobObjectBasicLimitInformation BasicLimitInformation;
            internal IoCounters IoInfo;
            internal UIntPtr ProcessMemoryLimit;
            internal UIntPtr JobMemoryLimit;
            internal UIntPtr PeakProcessMemoryUsed;
            internal UIntPtr PeakJobMemoryUsed;
        }
    }
}
