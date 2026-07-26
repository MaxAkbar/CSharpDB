using System.Globalization;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access.Worker;

internal sealed record AccessWorkerDependencies
{
    internal static AccessWorkerDependencies Default { get; } =
        new();

    internal Func<
        string,
        AccessRetainedCaptureOptions,
        string,
        CancellationToken,
        ValueTask<RetainedMigrationPackageWriteResult>>
        CaptureRetainedAsync
    {
        get;
        init;
    } = static (
        sourcePath,
        options,
        outputPath,
        cancellationToken) =>
        AccessRetainedCapture.CaptureAsync(
            sourcePath,
            outputPath,
            options,
            cancellationToken);
}

internal static class AccessWorkerRunner
{
    internal const string Protocol =
        "csharpdb-access-capture-worker/v1";
    internal const string SuccessHeader = Protocol + "\n";
    internal const string ResultFormat =
        "csharpdb-access-capture-result/v1";
    internal const int ExitSuccess = 0;
    internal const int ExitIncompatible = 10;
    internal const int ExitProviderUnavailable = 11;
    internal const int ExitCaptureFailure = 12;
    internal const int ExitInternalFailure = 13;
    internal const int ExitLimitExceeded = 14;
    internal const int ExitUnsupportedPlatform = 15;
    internal const long MaxResultBytes = 64L * 1024;

    private const string IncompatibleError =
        Protocol + ":error:incompatible";
    private const string ProviderUnavailableError =
        Protocol + ":error:provider-unavailable";
    private const string CaptureFailureError =
        Protocol + ":error:capture-failed";
    private const string InternalFailureError =
        Protocol + ":error:internal-failure";
    private const string LimitExceededError =
        Protocol + ":error:limit-exceeded";
    private const string UnsupportedPlatformError =
        Protocol + ":error:unsupported-platform";

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    internal static async ValueTask<int> RunAsync(
        IReadOnlyList<string> args,
        TextWriter output,
        TextWriter error,
        AccessWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);

        if (!TryParseInvocation(
                args,
                out Invocation? invocation))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                IncompatibleError);
        }

        if (!OperatingSystem.IsWindows())
        {
            return await FailAsync(
                error,
                ExitUnsupportedPlatform,
                UnsupportedPlatformError);
        }
        if (!string.Equals(
                invocation!.TargetVersion,
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                IncompatibleError);
        }
        if (dependencies.CaptureRetainedAsync is null)
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                InternalFailureError);
        }

        RetainedMigrationPackageWriteResult result;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            result =
                await dependencies.CaptureRetainedAsync(
                    invocation.SourcePath,
                    new AccessRetainedCaptureOptions
                    {
                        Source = new AccessSourceOptions
                        {
                            Provider =
                                invocation.Provider,
                            AllowAce12Fallback =
                                invocation
                                    .AllowAce12Fallback,
                            CommandTimeoutSeconds =
                                invocation
                                    .CommandTimeoutSeconds,
                            MaxSourceBytes =
                                invocation.MaxSourceBytes,
                        },
                        MaxPackageBytes =
                            invocation.MaxPackageBytes,
                    },
                    invocation.OutputPath,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch (AccessRetainedCaptureLimitException)
        {
            return await FailAsync(
                error,
                ExitLimitExceeded,
                LimitExceededError);
        }
        catch (AccessMigrationException exception)
            when (exception.ErrorCode ==
                AccessMigrationErrorCode
                    .ProviderUnavailable)
        {
            return await FailAsync(
                error,
                ExitProviderUnavailable,
                ProviderUnavailableError);
        }
        catch (AccessMigrationException exception)
            when (exception.ErrorCode ==
                AccessMigrationErrorCode
                    .UnsupportedPlatform)
        {
            return await FailAsync(
                error,
                ExitUnsupportedPlatform,
                UnsupportedPlatformError);
        }
        catch (AccessMigrationException)
        {
            return await FailAsync(
                error,
                ExitCaptureFailure,
                CaptureFailureError);
        }
        catch (RetainedMigrationPackageException)
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
            if (!TryCreateResult(
                    result,
                    invocation.OutputPath,
                    invocation.MaxPackageBytes,
                    out response))
            {
                return await FailAsync(
                    error,
                    ExitInternalFailure,
                    InternalFailureError);
            }
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
                MigrationSourceKind.Access ||
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
        foreach (
            RetainedMigrationPackageTableManifest table
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
        out Invocation? invocation)
    {
        invocation = null;
        if (args.Count != 18 ||
            !Pair(args, 0, "--protocol", Protocol) ||
            !string.Equals(
                args[2],
                "--input",
                StringComparison.Ordinal) ||
            !TryInputPath(args[3], out string? input) ||
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
            !TryOutputPath(
                args[7],
                out string? output) ||
            !string.Equals(
                args[8],
                "--provider",
                StringComparison.Ordinal) ||
            !TryProvider(
                args[9],
                out AccessOleDbProvider provider) ||
            !string.Equals(
                args[10],
                "--allow-ace12-fallback",
                StringComparison.Ordinal) ||
            !bool.TryParse(
                args[11],
                out bool allowFallback) ||
            provider == AccessOleDbProvider.Ace12 &&
                allowFallback ||
            !string.Equals(
                args[12],
                "--command-timeout-seconds",
                StringComparison.Ordinal) ||
            !int.TryParse(
                args[13],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int timeout) ||
            timeout is < 1 or >
                AccessSourceOptions
                    .MaximumCommandTimeoutSeconds ||
            !string.Equals(
                args[14],
                "--max-input-bytes",
                StringComparison.Ordinal) ||
            !long.TryParse(
                args[15],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long maxInput) ||
            maxInput is < 1 or >
                AccessSourceOptions.MaximumSourceBytes ||
            !string.Equals(
                args[16],
                "--max-package-bytes",
                StringComparison.Ordinal) ||
            !long.TryParse(
                args[17],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long maxPackage) ||
            maxPackage <
                AccessRetainedCaptureOptions
                    .MinimumPackageBytes ||
            maxPackage >
                AccessRetainedCaptureOptions
                    .MaximumPackageBytes)
        {
            return false;
        }

        invocation = new Invocation
        {
            SourcePath = input!,
            TargetVersion = args[5],
            OutputPath = output!,
            Provider = provider,
            AllowAce12Fallback = allowFallback,
            CommandTimeoutSeconds = timeout,
            MaxSourceBytes = maxInput,
            MaxPackageBytes = maxPackage,
        };
        return true;
    }

    private static bool Pair(
        IReadOnlyList<string> args,
        int index,
        string name,
        string value) =>
        string.Equals(
            args[index],
            name,
            StringComparison.Ordinal) &&
        string.Equals(
            args[index + 1],
            value,
            StringComparison.Ordinal);

    private static bool TryProvider(
        string value,
        out AccessOleDbProvider provider)
    {
        provider = value switch
        {
            "ace16" => AccessOleDbProvider.Ace16,
            "ace12" => AccessOleDbProvider.Ace12,
            _ => (AccessOleDbProvider)(-1),
        };
        return Enum.IsDefined(provider);
    }

    private static bool TryInputPath(
        string value,
        out string? fullPath)
    {
        fullPath = null;
        if (!TryCanonicalFullPath(
                value,
                mustExist: true,
                out string? candidate))
        {
            return false;
        }
        string extension = Path.GetExtension(candidate!);
        if (!string.Equals(
                extension,
                ".mdb",
                StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(
                extension,
                ".accdb",
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        fullPath = candidate;
        return true;
    }

    private static bool TryOutputPath(
        string value,
        out string? fullPath) =>
        TryCanonicalFullPath(
            value,
            mustExist: false,
            out fullPath);

    private static bool TryCanonicalFullPath(
        string value,
        bool mustExist,
        out string? fullPath)
    {
        fullPath = null;
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 32_767 ||
            !Path.IsPathFullyQualified(value))
        {
            return false;
        }
        try
        {
            string candidate =
                Path.GetFullPath(value);
            StringComparison comparison =
                OperatingSystem.IsWindows() ||
                OperatingSystem.IsMacOS()
                    ? StringComparison
                        .OrdinalIgnoreCase
                    : StringComparison.Ordinal;
            if (!string.Equals(
                    value,
                    candidate,
                    comparison) ||
                Directory.Exists(candidate) ||
                mustExist != File.Exists(candidate))
            {
                return false;
            }
            if (mustExist)
            {
                FileAttributes attributes =
                    File.GetAttributes(candidate);
                if ((attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
                {
                    return false;
                }
            }
            else
            {
                string? parent =
                    Path.GetDirectoryName(candidate);
                if (string.IsNullOrEmpty(parent) ||
                    !Directory.Exists(parent))
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
            }
            fullPath = candidate;
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
            AccessRetainedDataContract
                .SnapshotIdentityPrefix,
            StringComparison.Ordinal) &&
        IsLowerSha256(
            value[
                AccessRetainedDataContract
                    .SnapshotIdentityPrefix.Length..]);

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
        }
        return exitCode;
    }

    private sealed record Invocation
    {
        internal required string SourcePath { get; init; }

        internal required string TargetVersion { get; init; }

        internal required string OutputPath { get; init; }

        internal AccessOleDbProvider Provider { get; init; }

        internal bool AllowAce12Fallback { get; init; }

        internal int CommandTimeoutSeconds { get; init; }

        internal long MaxSourceBytes { get; init; }

        internal long MaxPackageBytes { get; init; }
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
