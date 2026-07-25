using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal static class EfCoreAnalyzeCommandRunner
{
    internal const int ExitConditional = 1;
    internal const int ExitError = 2;
    internal const int ExitUsage = 64;
    internal const int ExitCanceled = 130;

    internal const string Usage =
        "Usage:\n" +
        "  dotnet csharpdb-ef analyze --project <project.csproj> " +
        "--context <fully-qualified-or-unique-simple-name> " +
        "[--scratch] [--format text|json]";

    internal const string ExecutionWarning =
        "Warning: Building the selected project and creating its EF Core " +
        "design-time context can execute application code. The tooling " +
        "does not ask the provider to open the configured database.";

    internal const string ScratchExecutionWarning =
        "Scratch mode executes retained migration SQL only against " +
        "tool-owned private-memory CSharpDB databases. It does not validate " +
        "existing application data or file-backed persistence.";

    private static readonly JsonSerializerOptions JsonOptions =
        CreateJsonOptions();

    internal static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        cancellationToken.ThrowIfCancellationRequested();

        if (!TryParse(
                args,
                out AnalyzeOptions? options,
                out string? optionError))
        {
            await WriteFixedErrorAsync(
                error,
                "CSHARPDB-EF-USAGE-001",
                optionError ??
                    "The EF Core migration analysis command is invalid.");
            await error.WriteLineAsync(Usage);
            return ExitUsage;
        }

        if (!EfCoreWorkerClient.TryResolveProjectPath(
                options!.Project,
                out string? projectPath))
        {
            await WriteFixedErrorAsync(
                error,
                "CSHARPDB-EF-PROJECT-001",
                "The selected project must be an existing C# project file.");
            return ExitError;
        }

        await error.WriteLineAsync(ExecutionWarning);
        if (options.Scratch)
            await error.WriteLineAsync(ScratchExecutionWarning);
        cancellationToken.ThrowIfCancellationRequested();

        EfCoreWorkerClientResult result =
            await EfCoreWorkerClient.AnalyzeProjectAsync(
                    projectPath!,
                    options.Context,
                    options.Scratch
                        ? EfCoreAnalysisMode.Scratch
                        : EfCoreAnalysisMode.Generation,
                    cancellationToken)
                .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (result.Status != EfCoreWorkerClientStatus.Success ||
            !options.Scratch && result.Report is null ||
            options.Scratch && result.ScratchReport is null)
        {
            await WriteWorkerFailureAsync(error, result.Status);
            return ExitError;
        }

        if (string.Equals(
                options.Format,
                "json",
                StringComparison.OrdinalIgnoreCase))
        {
            string json = options.Scratch
                ? JsonSerializer.Serialize(
                    result.ScratchReport!,
                    JsonOptions)
                : JsonSerializer.Serialize(
                    result.Report!,
                    JsonOptions);
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteLineAsync(json);
        }
        else
        {
            if (options.Scratch)
            {
                await WriteScratchTextReportAsync(
                    output,
                    result.ScratchReport!,
                    cancellationToken);
            }
            else
            {
                await WriteTextReportAsync(
                    output,
                    result.Report!,
                    cancellationToken);
            }
        }
        cancellationToken.ThrowIfCancellationRequested();

        MigrationCompatibilityStatus status = options.Scratch
            ? result.ScratchReport!.Status
            : result.Report!.Status;
        return status switch
        {
            MigrationCompatibilityStatus.Compatible
                when options.Scratch => 0,
            MigrationCompatibilityStatus.Conditional =>
                ExitConditional,
            _ => ExitError,
        };
    }

    private static bool TryParse(
        IReadOnlyList<string> args,
        out AnalyzeOptions? options,
        out string? error)
    {
        options = null;
        error = null;
        if (args.Count == 0 ||
            !string.Equals(
                args[0],
                "analyze",
                StringComparison.OrdinalIgnoreCase))
        {
            error = "The only supported command is analyze.";
            return false;
        }

        var values =
            new Dictionary<string, string>(
                StringComparer.OrdinalIgnoreCase);
        bool scratch = false;
        for (int i = 1; i < args.Count; i++)
        {
            string option = args[i];
            if (!option.StartsWith("--", StringComparison.Ordinal))
            {
                error =
                    "The command contains an unexpected positional argument.";
                return false;
            }
            if (string.Equals(
                    option,
                    "--scratch",
                    StringComparison.OrdinalIgnoreCase))
            {
                if (scratch)
                {
                    error = "The command contains a duplicate option.";
                    return false;
                }
                scratch = true;
                continue;
            }
            if (option is not ("--project" or "--context" or "--format"))
            {
                error = "The command contains an unsupported option.";
                return false;
            }
            if (!values.TryAdd(option, string.Empty))
            {
                error = "The command contains a duplicate option.";
                return false;
            }
            if (i + 1 >= args.Count ||
                args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                error = "A command option is missing its value.";
                return false;
            }

            values[option] = args[++i];
        }

        if (!values.TryGetValue("--project", out string? project) ||
            string.IsNullOrWhiteSpace(project))
        {
            error = "Missing required option --project.";
            return false;
        }
        if (!values.TryGetValue("--context", out string? context) ||
            !IsSafeContextSelector(context))
        {
            error =
                "The --context value must be a valid fully qualified or " +
                "simple .NET type name.";
            return false;
        }

        string format = values.GetValueOrDefault(
            "--format",
            "text");
        if (!string.Equals(
                format,
                "text",
                StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(
                format,
                "json",
                StringComparison.OrdinalIgnoreCase))
        {
            error = "The output format must be text or json.";
            return false;
        }

        options = new AnalyzeOptions(
            project,
            context,
            format.ToLowerInvariant(),
            scratch);
        return true;
    }

    internal static bool IsSafeContextSelector(string value)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 512)
        {
            return false;
        }

        bool atSegmentStart = true;
        foreach (char character in value)
        {
            if (character is '.' or '+')
            {
                if (atSegmentStart)
                    return false;
                atSegmentStart = true;
                continue;
            }

            if (atSegmentStart)
            {
                if (character is not (>= 'A' and <= 'Z') and
                    not (>= 'a' and <= 'z') and not '_')
                {
                    return false;
                }

                atSegmentStart = false;
                continue;
            }

            if (character is not (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not (>= '0' and <= '9') and not '_')
            {
                return false;
            }
        }

        return !atSegmentStart;
    }

    private static async ValueTask WriteTextReportAsync(
        TextWriter output,
        EfCoreMigrationAnalysisReport report,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await output.WriteLineAsync("CSharpDB EF Core migration analysis");
        await output.WriteLineAsync(
            $"Status: {CliToken(report.Status)}");
        await output.WriteLineAsync(
            $"Evidence: {CliToken(report.HighestEvidence)}");
        await output.WriteLineAsync(
            $"Rule: {report.RuleId}");
        await output.WriteLineAsync(
            $"Target CSharpDB version: {report.TargetCSharpDbVersion}");
        await output.WriteLineAsync(
            $"Capability digest: {report.CapabilityDigest}");
        await output.WriteLineAsync(
            $"Assembly digest: {report.AssemblyDigest}");
        await output.WriteLineAsync(
            $"Generated SQL digest: {report.GeneratedSqlDigest ?? "none"}");
        await output.WriteLineAsync(
            $"Qualified EF Core version: {report.QualifiedEfCoreVersion}");
        await output.WriteLineAsync(
            $"Context: {report.Context}");
        await output.WriteLineAsync(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Counts: migrations={report.MigrationCount} | operations={report.OperationCount} | destructive={report.DestructiveOperationCount} | commands={report.CommandCount}"));

        foreach (EfCoreMigrationAnalysisMigration migration in
                 report.Migrations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Migration: ordinal={migration.Ordinal} | id={migration.MigrationId} | status={CliToken(migration.Status)} | evidence={CliToken(migration.HighestEvidence)} | rule={migration.RuleId} | up={migration.UpOperationCount} | down={migration.DownOperationCount} | operations={migration.OperationCount} | destructive={migration.DestructiveOperationCount} | commands={migration.CommandCount} | generated-sql-digest={migration.GeneratedSqlDigest ?? "none"}"));
            foreach (EfCoreMigrationOperationFinding operation in
                     migration.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await output.WriteLineAsync(
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"Operation: ordinal={operation.Ordinal} | direction={CliToken(operation.Direction)} | direction-ordinal={operation.DirectionOrdinal} | kind={CliToken(operation.Kind)} | status={CliToken(operation.Status)} | evidence={CliToken(operation.Evidence)} | rule={operation.RuleId} | destructive={BoolToken(operation.IsDestructive)} | annotations={operation.AnnotationCount} | commands={operation.CommandCount} | generated-sql-bytes={operation.GeneratedSqlUtf8Bytes} | generated-sql-digest={operation.GeneratedSqlDigest ?? "none"}"));
            }
        }

        foreach (EfCoreMigrationAnalysisDiagnostic diagnostic in
                 report.Diagnostics)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Diagnostic: ordinal={diagnostic.Ordinal} | id={diagnostic.DiagnosticId} | rule={diagnostic.RuleId} | severity={CliToken(diagnostic.Severity)} | status={CliToken(diagnostic.Status)} | evidence={CliToken(diagnostic.Evidence)} | migration={NullableIntToken(diagnostic.MigrationOrdinal)} | operation={NullableIntToken(diagnostic.OperationOrdinal)} | summary={diagnostic.Summary} | remediation={diagnostic.Remediation ?? "none"}"));
        }
    }

    private static async ValueTask WriteScratchTextReportAsync(
        TextWriter output,
        EfCoreMigrationScratchAnalysisReport report,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EfCoreMigrationScratchChainProof proof =
            report.ScratchChain;
        await output.WriteLineAsync(
            "CSharpDB EF Core scratch-chain analysis");
        await output.WriteLineAsync(
            $"Outcome: {CliToken(report.Outcome)}");
        await output.WriteLineAsync(
            $"Status: {CliToken(report.Status)}");
        await output.WriteLineAsync(
            $"Evidence: {CliToken(report.HighestEvidence)}");
        await output.WriteLineAsync($"Rule: {report.RuleId}");
        await output.WriteLineAsync(
            $"Scope: {CliToken(proof.ProofScope)}");
        await output.WriteLineAsync(
            $"Algorithm: {proof.Algorithm}");
        await output.WriteLineAsync(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Scratch counts: prefixes={proof.PrefixCount} | applied={proof.AppliedPrefixCount} | schema-verified={proof.SchemaVerifiedPrefixCount} | down={proof.DownPrefixCount} | reapplied={proof.ReappliedPrefixCount} | round-trip={proof.RoundTripVerifiedPrefixCount} | idempotent-applies={proof.IdempotentApplyCount} | commands={proof.ExecutedCommandCount}"));
        await output.WriteLineAsync(
            $"Executed SQL digest: {proof.ExecutedSqlDigest ?? "none"}");
        await output.WriteLineAsync(
            $"Idempotent SQL digest: {proof.IdempotentSqlDigest ?? "none"}");
        await output.WriteLineAsync(
            $"Data preflight completed: {BoolToken(proof.DataPreflightCompleted)}");
        await output.WriteLineAsync(
            $"Resources disposed: {BoolToken(proof.ResourcesDisposed)}");

        foreach (EfCoreMigrationScratchPrefixEvidence prefix in
                 proof.Prefixes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Scratch prefix: ordinal={prefix.Ordinal} | migration={prefix.MigrationOrdinal} | status={CliToken(prefix.Status)} | evidence={CliToken(prefix.Evidence)} | rule={prefix.RuleId} | expected-schema={prefix.ExpectedSchemaDigest} | applied-schema={prefix.AppliedSchemaDigest ?? "none"} | down-schema={prefix.DownSchemaDigest ?? "none"} | reapplied-schema={prefix.ReappliedSchemaDigest ?? "none"}"));
        }

        await output.WriteLineAsync("Generation preflight:");
        await WriteTextReportAsync(
            output,
            report.GenerationPreflight,
            cancellationToken);
    }

    private static async ValueTask WriteWorkerFailureAsync(
        TextWriter error,
        EfCoreWorkerClientStatus status)
    {
        (string code, string message) = status switch
        {
            EfCoreWorkerClientStatus.ToolUnavailable =>
                (
                    "CSHARPDB-EF-HOST-001",
                    "The required .NET host is unavailable."),
            EfCoreWorkerClientStatus.ProjectQueryFailed =>
                (
                    "CSHARPDB-EF-PROJECT-002",
                    "The project properties could not be inspected safely."),
            EfCoreWorkerClientStatus.ProjectIncompatible =>
                (
                    "CSHARPDB-EF-PROJECT-003",
                    "The project must target only net10.0."),
            EfCoreWorkerClientStatus.BuildFailed =>
                (
                    "CSHARPDB-EF-BUILD-001",
                    "The restored project could not be built safely."),
            EfCoreWorkerClientStatus.AssemblyInvalid =>
                (
                    "CSHARPDB-EF-ASSEMBLY-001",
                    "The project did not produce a supported managed assembly."),
            EfCoreWorkerClientStatus.WorkerUnavailable =>
                (
                    "CSHARPDB-EF-WORKER-001",
                    "The isolated analyzer worker is unavailable."),
            EfCoreWorkerClientStatus.WorkerTimedOut =>
                (
                    "CSHARPDB-EF-WORKER-002",
                    "The isolated analyzer worker timed out."),
            EfCoreWorkerClientStatus.AnalysisFailed =>
                (
                    "CSHARPDB-EF-ANALYSIS-001",
                    "The compiled migration analysis could not be completed."),
            _ =>
                (
                    "CSHARPDB-EF-WORKER-003",
                    "The isolated analyzer worker returned an invalid result."),
        };
        await WriteFixedErrorAsync(error, code, message);
    }

    private static async ValueTask WriteFixedErrorAsync(
        TextWriter error,
        string code,
        string message) =>
        await error.WriteLineAsync($"{code}: {message}");

    private static string CliToken<T>(T value)
        where T : struct, Enum =>
        value.ToString().ToUpperInvariant();

    private static string BoolToken(bool value) =>
        value ? "yes" : "no";

    private static string NullableIntToken(int? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? "none";

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    private sealed record AnalyzeOptions(
        string Project,
        string Context,
        string Format,
        bool Scratch);
}
