using System.Globalization;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.MySql.Worker;

internal static class MySqlCaptureWorkerRunner
{
    internal const string Protocol =
        "csharpdb-mysql-capture-worker/v1";
    internal const string SuccessHeader = Protocol + "\n";
    internal const string ResultFormat =
        "csharpdb-mysql-capture-result/v1";
    internal const string OutputFileName =
        "capture.csdbmysql";
    internal const string WorkspacePrefix =
        ".csharpdb-mysql-capture-";
    internal const int ExitSuccess = 0;
    internal const int ExitIncompatible = 10;
    internal const int ExitConnectionUnavailable = 11;
    internal const int ExitCaptureFailure = 12;
    internal const int ExitInternalFailure = 13;
    internal const int ExitLimitExceeded = 14;
    internal const long MaxResultBytes = 64L * 1024;
    internal const long HardMaxPackageBytes =
        256L * 1024 * 1024 * 1024;
    internal const int DefaultTableTimeoutSeconds =
        1_800;
    internal const int MaxTableTimeoutSeconds =
        86_400;
    private const string SnapshotIdentityPrefix =
        "mysql-retained:";

    private const string IncompatibleError =
        Protocol + ":error:incompatible";
    private const string ConnectionUnavailableError =
        Protocol + ":error:connection-unavailable";
    private const string CaptureFailureError =
        Protocol + ":error:capture-failed";
    private const string InternalFailureError =
        Protocol + ":error:internal-failure";
    private const string LimitExceededError =
        Protocol + ":error:limit-exceeded";

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static bool IsCaptureProtocol(
        IReadOnlyList<string> args) =>
        args.Count >= 2 &&
        string.Equals(
            args[0],
            "--protocol",
            StringComparison.Ordinal) &&
        string.Equals(
            args[1],
            Protocol,
            StringComparison.Ordinal);

    internal static async ValueTask<int> RunAsync(
        IReadOnlyList<string> args,
        TextWriter output,
        TextWriter error,
        MySqlWorkerDependencies dependencies,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);

        if (!TryParseInvocation(
                args,
                out string? environmentVariableName,
                out string? targetVersion,
                out string? outputPath,
                out long maxPackageBytes,
                out int tableTimeoutSeconds))
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
            dependencies.ClearEnvironmentVariable is null ||
            dependencies.CaptureRetainedAsync is null)
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
            try
            {
                connectionString =
                    dependencies.ReadEnvironmentVariable(
                        environmentVariableName!);
            }
            finally
            {
                dependencies.ClearEnvironmentVariable(
                    environmentVariableName!);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
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

        RetainedMigrationPackageWriteResult result;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            result = await dependencies.CaptureRetainedAsync(
                connectionString,
                outputPath!,
                maxPackageBytes,
                tableTimeoutSeconds,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch (MySqlRetainedCaptureLimitException)
        {
            return await FailAsync(
                error,
                ExitLimitExceeded,
                LimitExceededError);
        }
        catch (RetainedMigrationPackageException)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch (MySqlMigrationException)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }

        CaptureResult response;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TryCreateResult(
                    result,
                    outputPath!,
                    maxPackageBytes,
                    out response))
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
                ExitCaptureFailure,
                CaptureFailureError);
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
            string json = JsonSerializer.Serialize(
                response,
                JsonOptions);
            if (StrictUtf8.GetByteCount(json) >
                MaxResultBytes)
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    InternalFailureError);
            }

            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteAsync(SuccessHeader);
            await output.WriteAsync(json);
            await output.FlushAsync(cancellationToken);
            return ExitSuccess;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                InternalFailureError);
        }
    }

    private static bool TryCreateResult(
        RetainedMigrationPackageWriteResult? result,
        string outputPath,
        long maxPackageBytes,
        out CaptureResult response)
    {
        response = null!;
        if (result?.Manifest is null ||
            result.ContentSummary is null ||
            result.Manifest.Tables is null ||
            !string.Equals(
                result.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            result.Manifest.SourceKind !=
                MigrationSourceKind.MySql ||
            !IsPackageDigest(result.PackageDigest) ||
            !IsLowerSha256(
                result.Manifest.CatalogDigest) ||
            !IsSnapshotIdentity(
                result.Manifest.SnapshotIdentity))
        {
            return false;
        }

        var package = new FileInfo(outputPath);
        package.Refresh();
        if (!package.Exists ||
            package.Length <= 0 ||
            package.Length > maxPackageBytes ||
            (package.Attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            return false;
        }

        long rowCount = 0;
        foreach (RetainedMigrationPackageTableManifest table
                 in result.Manifest.Tables)
        {
            if (table is null || table.RowCount < 0)
                return false;
            rowCount = checked(
                rowCount + table.RowCount);
        }

        response = new CaptureResult
        {
            Format = ResultFormat,
            PackageDigest = result.PackageDigest,
            CatalogDigest =
                result.Manifest.CatalogDigest,
            SnapshotIdentity =
                result.Manifest.SnapshotIdentity,
            PackageBytes = package.Length,
            TableCount =
                result.Manifest.Tables.Count,
            RowCount = rowCount,
        };
        return true;
    }

    private static bool TryParseInvocation(
        IReadOnlyList<string> args,
        out string? environmentVariableName,
        out string? targetVersion,
        out string? outputPath,
        out long maxPackageBytes,
        out int tableTimeoutSeconds)
    {
        environmentVariableName = null;
        targetVersion = null;
        outputPath = null;
        maxPackageBytes = 0;
        tableTimeoutSeconds = 0;
        if (args.Count != 12 ||
            !string.Equals(
                args[0],
                "--protocol",
                StringComparison.Ordinal) ||
            !string.Equals(
                args[1],
                Protocol,
                StringComparison.Ordinal) ||
            !string.Equals(
                args[2],
                "--connection-env",
                StringComparison.Ordinal) ||
            !IsSafeEnvironmentVariableName(
                args[3]) ||
            !string.Equals(
                args[4],
                "--target-version",
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[5]) ||
            args[5].Length > 128 ||
            !string.Equals(
                args[6],
                "--output",
                StringComparison.Ordinal) ||
            !TryValidateOutputPath(
                args[7],
                out string? safeOutputPath) ||
            !string.Equals(
                args[8],
                "--max-source-bytes",
                StringComparison.Ordinal) ||
            !long.TryParse(
                args[9],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long parsedMaximum) ||
            parsedMaximum <= 0 ||
            parsedMaximum >
                HardMaxPackageBytes ||
            !string.Equals(
                args[10],
                "--table-timeout-seconds",
                StringComparison.Ordinal) ||
            !int.TryParse(
                args[11],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsedTableTimeout) ||
            parsedTableTimeout <= 0 ||
            parsedTableTimeout >
                MaxTableTimeoutSeconds)
        {
            return false;
        }

        environmentVariableName = args[3];
        targetVersion = args[5];
        outputPath = safeOutputPath;
        maxPackageBytes = parsedMaximum;
        tableTimeoutSeconds =
            parsedTableTimeout;
        return true;
    }

    private static bool TryValidateOutputPath(
        string value,
        out string? outputPath)
    {
        outputPath = null;
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 32_767 ||
            !Path.IsPathFullyQualified(value))
        {
            return false;
        }

        try
        {
            string fullPath =
                Path.GetFullPath(value);
            StringComparison pathComparison =
                OperatingSystem.IsWindows() ||
                OperatingSystem.IsMacOS()
                    ? StringComparison.OrdinalIgnoreCase
                    : StringComparison.Ordinal;
            if (!string.Equals(
                    value,
                    fullPath,
                    pathComparison) ||
                !string.Equals(
                    Path.GetFileName(fullPath),
                    OutputFileName,
                    StringComparison.Ordinal) ||
                File.Exists(fullPath) ||
                Directory.Exists(fullPath))
            {
                return false;
            }

            string? parent =
                Path.GetDirectoryName(fullPath);
            if (string.IsNullOrEmpty(parent) ||
                !Directory.Exists(parent) ||
                !TryValidateWorkspaceName(
                    Path.GetFileName(parent)))
            {
                return false;
            }

            FileAttributes attributes =
                File.GetAttributes(parent);
            if ((attributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
            {
                return false;
            }

            outputPath = fullPath;
            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or
                IOException or
                UnauthorizedAccessException or
                NotSupportedException or
                PathTooLongException)
        {
            return false;
        }
    }

    private static bool TryValidateWorkspaceName(
        string name)
    {
        if (!name.StartsWith(
                WorkspacePrefix,
                StringComparison.Ordinal))
        {
            return false;
        }
        string suffix =
            name[WorkspacePrefix.Length..];
        return suffix.Length == 32 &&
            suffix.All(static character =>
                character is
                    (>= '0' and <= '9') or
                    (>= 'a' and <= 'f'));
    }

    private static bool
        IsSafeEnvironmentVariableName(
        string value)
    {
        if (value is not
            { Length: > 0 and <= 128 } ||
            value[0] is not
                (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not '_')
        {
            return false;
        }

        foreach (char character
                 in value.AsSpan(1))
        {
            if (character is not
                    (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not (>= '0' and <= '9') and
                not '_')
            {
                return false;
            }
        }
        return true;
    }

    private static bool IsPackageDigest(
        string? value) =>
        value is { Length: 71 } &&
        value.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        IsLowerSha256(value[7..]);

    private static bool IsLowerSha256(
        string? value) =>
        value is { Length: 64 } &&
        value.All(static character =>
            character is
                (>= '0' and <= '9') or
                (>= 'a' and <= 'f'));

    private static bool IsSnapshotIdentity(
        string? value) =>
        value is not null &&
        value.StartsWith(
            SnapshotIdentityPrefix,
            StringComparison.Ordinal) &&
        IsPackageDigest(
            value[SnapshotIdentityPrefix.Length..]);

    private static async ValueTask<int>
        FailAsync(
        TextWriter error,
        int exitCode,
        string message)
    {
        try
        {
            await error.WriteAsync(
                message + "\n");
        }
        catch
        {
        }
        return exitCode;
    }

    private sealed record CaptureResult
    {
        public required string Format { get; init; }

        public required string PackageDigest { get; init; }

        public required string CatalogDigest { get; init; }

        public required string SnapshotIdentity { get; init; }

        public long PackageBytes { get; init; }

        public int TableCount { get; init; }

        public long RowCount { get; init; }
    }
}
