using System.Text;
using CSharpDB.Migration;

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
}

internal static class SqlServerWorkerRunner
{
    internal const string Protocol = "csharpdb-sqlserver-worker/v1";
    internal const string SuccessHeader = Protocol + "\n";
    internal const int ExitSuccess = 0;
    internal const int ExitIncompatible = 10;
    internal const int ExitConnectionUnavailable = 11;
    internal const int ExitInspectionFailure = 12;
    internal const int ExitInternalFailure = 13;
    internal const long MaxCatalogBytes = 64L * 1024 * 1024;

    private const string IncompatibleError =
        Protocol + ":error:incompatible";
    private const string ConnectionUnavailableError =
        Protocol + ":error:connection-unavailable";
    private const string InspectionFailureError =
        Protocol + ":error:inspection-failed";
    private const string InternalFailureError =
        Protocol + ":error:internal-failure";

    internal static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);

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
