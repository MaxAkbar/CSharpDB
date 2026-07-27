using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Cli;

internal enum AccessCaptureWorkerStatus
{
    Success,
    Missing,
    Incompatible,
    UnsupportedPlatform,
    ProviderUnavailable,
    LimitExceeded,
    CaptureFailed,
}

internal sealed record AccessCaptureReceipt
{
    internal const string CurrentFormat =
        "csharpdb-access-capture-result/v1";

    public required string Format { get; init; }

    public required string PackageDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SnapshotIdentity { get; init; }

    public long PackageBytes { get; init; }

    public int TableCount { get; init; }

    public long RowCount { get; init; }
}

internal sealed record AccessCaptureWorkerResult
{
    internal required AccessCaptureWorkerStatus Status
    {
        get;
        init;
    }

    internal AccessCaptureReceipt? Receipt { get; init; }

    internal static AccessCaptureWorkerResult Success(
        AccessCaptureReceipt receipt)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        return new AccessCaptureWorkerResult
        {
            Status = AccessCaptureWorkerStatus.Success,
            Receipt = receipt,
        };
    }

    internal static AccessCaptureWorkerResult Failure(
        AccessCaptureWorkerStatus status)
    {
        if (status == AccessCaptureWorkerStatus.Success)
        {
            throw new ArgumentOutOfRangeException(
                nameof(status));
        }
        return new AccessCaptureWorkerResult
        {
            Status = status,
        };
    }
}

internal static class AccessWorkerClient
{
    internal const string CaptureProtocolV1 =
        "csharpdb-access-capture-worker/v1";
    internal const string CaptureOutputFileName =
        "capture.csdbaccess";
    internal const string CaptureWorkspacePrefix =
        ".csharpdb-access-capture-";
    internal const long DefaultMaxSourceBytes =
        64L * 1024 * 1024 * 1024;
    internal const long HardMaxSourceBytes =
        64L * 1024 * 1024 * 1024;
    internal const long DefaultMaxPackageBytes =
        256L * 1024 * 1024 * 1024;
    internal const long HardMaxPackageBytes =
        256L * 1024 * 1024 * 1024;
    internal const int DefaultCommandTimeoutSeconds =
        30;
    internal const int MaxCommandTimeoutSeconds =
        3_600;
    internal const long MaxCaptureResultBytes =
        64L * 1024;
    internal const long MaxStderrBytes =
        64L * 1024;

    private const int ExitIncompatible = 10;
    private const int ExitProviderUnavailable = 11;
    private const int ExitCaptureFailed = 12;
    private const int ExitInternalFailure = 13;
    private const int ExitLimitExceeded = 14;
    private const int ExitUnsupportedPlatform = 15;
    private const string SnapshotIdentityPrefix =
        "access-retained:";

    private static readonly byte[] HeaderBytes =
        Encoding.ASCII.GetBytes(
            CaptureProtocolV1 + "\n");
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions JsonOptions =
        CreateJsonOptions();

    internal static async ValueTask<
        AccessCaptureWorkerResult> CaptureAsync(
        string sourcePath,
        string targetCSharpDbVersion,
        string temporaryOutputPath,
        string provider,
        bool allowAce12Fallback,
        int commandTimeoutSeconds,
        long maxSourceBytes,
        long maxPackageBytes,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            targetCSharpDbVersion);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            temporaryOutputPath);
        if (provider is not ("ace16" or "ace12") ||
            provider == "ace12" &&
                allowAce12Fallback ||
            commandTimeoutSeconds is < 1 or >
                MaxCommandTimeoutSeconds ||
            maxSourceBytes is < 1 or >
                HardMaxSourceBytes ||
            maxPackageBytes is < 13 or >
                HardMaxPackageBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(provider));
        }
        cancellationToken.ThrowIfCancellationRequested();

        if (!OperatingSystem.IsWindows())
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .UnsupportedPlatform);
        }

        string fullSourcePath =
            Path.GetFullPath(sourcePath);
        string fullOutputPath =
            Path.GetFullPath(temporaryOutputPath);
        if (!string.Equals(
                Path.GetFileName(fullOutputPath),
                CaptureOutputFileName,
                StringComparison.Ordinal) ||
            File.Exists(fullOutputPath) ||
            Directory.Exists(fullOutputPath))
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .Incompatible);
        }
        string? captureDirectory =
            Path.GetDirectoryName(fullOutputPath);
        if (string.IsNullOrEmpty(captureDirectory) ||
            !Directory.Exists(captureDirectory) ||
            !IsCaptureWorkspaceName(
                Path.GetFileName(captureDirectory)))
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .Incompatible);
        }

        string workerDirectory = Path.Combine(
            AppContext.BaseDirectory,
            "adapters",
            "access");
        string workerPath = Path.Combine(
            workerDirectory,
            "csharpdb-migration-access-worker.exe");
        if (!File.Exists(workerPath))
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus.Missing);
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
        AddArgument(
            startInfo,
            "--protocol",
            CaptureProtocolV1);
        AddArgument(
            startInfo,
            "--input",
            fullSourcePath);
        AddArgument(
            startInfo,
            "--target-version",
            targetCSharpDbVersion);
        AddArgument(
            startInfo,
            "--output",
            fullOutputPath);
        AddArgument(
            startInfo,
            "--provider",
            provider);
        AddArgument(
            startInfo,
            "--allow-ace12-fallback",
            allowAce12Fallback.ToString(
                System.Globalization.CultureInfo
                    .InvariantCulture));
        AddArgument(
            startInfo,
            "--command-timeout-seconds",
            commandTimeoutSeconds.ToString(
                System.Globalization.CultureInfo
                    .InvariantCulture));
        AddArgument(
            startInfo,
            "--max-input-bytes",
            maxSourceBytes.ToString(
                System.Globalization.CultureInfo
                    .InvariantCulture));
        AddArgument(
            startInfo,
            "--max-package-bytes",
            maxPackageBytes.ToString(
                System.Globalization.CultureInfo
                    .InvariantCulture));

        using var process = new Process
        {
            StartInfo = startInfo,
        };
        try
        {
            if (!process.Start())
            {
                return AccessCaptureWorkerResult
                    .Failure(
                        AccessCaptureWorkerStatus
                            .Missing);
            }
        }
        catch (Exception exception) when (
            exception is Win32Exception or
                IOException or
                InvalidOperationException or
                UnauthorizedAccessException)
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus.Missing);
        }

        byte[] stdout;
        using var linkedCancellation =
            CancellationTokenSource
                .CreateLinkedTokenSource(
                    cancellationToken);
        Task<byte[]> stdoutTask =
            ReadBoundedAsync(
                process.StandardOutput.BaseStream,
                checked(
                    MaxCaptureResultBytes +
                    HeaderBytes.LongLength),
                linkedCancellation.Token);
        Task stderrTask =
            DrainBoundedAsync(
                process.StandardError.BaseStream,
                MaxStderrBytes,
                linkedCancellation.Token);
        Task exitTask =
            process.WaitForExitAsync(
                linkedCancellation.Token);
        try
        {
            await Task.WhenAll(
                    stdoutTask,
                    stderrTask,
                    exitTask)
                .ConfigureAwait(false);
            stdout = await stdoutTask
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            await TerminateAsync(
                    process,
                    linkedCancellation)
                .ConfigureAwait(false);
            throw;
        }
        catch (Exception exception) when (
            exception is IOException or
                InvalidOperationException or
                WorkerOutputLimitException)
        {
            await TerminateAsync(
                    process,
                    linkedCancellation)
                .ConfigureAwait(false);
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .Incompatible);
        }

        try
        {
            return process.ExitCode switch
            {
                0 => ParseSuccess(
                    stdout,
                    fullOutputPath,
                    maxPackageBytes),
                ExitProviderUnavailable =>
                    AccessCaptureWorkerResult
                        .Failure(
                            AccessCaptureWorkerStatus
                                .ProviderUnavailable),
                ExitCaptureFailed =>
                    AccessCaptureWorkerResult
                        .Failure(
                            AccessCaptureWorkerStatus
                                .CaptureFailed),
                ExitLimitExceeded =>
                    AccessCaptureWorkerResult
                        .Failure(
                            AccessCaptureWorkerStatus
                                .LimitExceeded),
                ExitUnsupportedPlatform =>
                    AccessCaptureWorkerResult
                        .Failure(
                            AccessCaptureWorkerStatus
                                .UnsupportedPlatform),
                ExitIncompatible or
                    ExitInternalFailure =>
                    AccessCaptureWorkerResult
                        .Failure(
                            AccessCaptureWorkerStatus
                                .Incompatible),
                _ => AccessCaptureWorkerResult
                    .Failure(
                        AccessCaptureWorkerStatus
                            .Incompatible),
            };
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                stdout);
        }
    }

    private static AccessCaptureWorkerResult
        ParseSuccess(
        byte[] stdout,
        string outputPath,
        long maxPackageBytes)
    {
        if (stdout.Length <= HeaderBytes.Length ||
            !stdout.AsSpan(
                    0,
                    HeaderBytes.Length)
                .SequenceEqual(HeaderBytes))
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .Incompatible);
        }
        try
        {
            string json = StrictUtf8.GetString(
                stdout,
                HeaderBytes.Length,
                stdout.Length -
                    HeaderBytes.Length);
            AccessCaptureReceipt? receipt =
                JsonSerializer.Deserialize<
                    AccessCaptureReceipt>(
                    json,
                    JsonOptions);
            if (receipt is null ||
                !string.Equals(
                    receipt.Format,
                    AccessCaptureReceipt
                        .CurrentFormat,
                    StringComparison.Ordinal) ||
                !IsPackageDigest(
                    receipt.PackageDigest) ||
                !IsLowerSha256(
                    receipt.CatalogDigest) ||
                !IsSnapshotIdentity(
                    receipt.SnapshotIdentity) ||
                receipt.PackageBytes <= 0 ||
                receipt.PackageBytes >
                    maxPackageBytes ||
                receipt.TableCount < 0 ||
                receipt.RowCount < 0)
            {
                return AccessCaptureWorkerResult
                    .Failure(
                        AccessCaptureWorkerStatus
                            .Incompatible);
            }

            var package = new FileInfo(outputPath);
            package.Refresh();
            if (!package.Exists ||
                package.Length !=
                    receipt.PackageBytes ||
                (package.Attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                return AccessCaptureWorkerResult
                    .Failure(
                        AccessCaptureWorkerStatus
                            .Incompatible);
            }
            return AccessCaptureWorkerResult.Success(
                receipt);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or
                JsonException or
                IOException or
                UnauthorizedAccessException or
                ArgumentException or
                FormatException or
                OverflowException or
                NotSupportedException)
        {
            return AccessCaptureWorkerResult.Failure(
                AccessCaptureWorkerStatus
                    .Incompatible);
        }
    }

    private static void AddArgument(
        ProcessStartInfo startInfo,
        string name,
        string value)
    {
        startInfo.ArgumentList.Add(name);
        startInfo.ArgumentList.Add(value);
    }

    private static bool IsCaptureWorkspaceName(
        string name)
    {
        if (!name.StartsWith(
                CaptureWorkspacePrefix,
                StringComparison.Ordinal))
        {
            return false;
        }
        string suffix =
            name[CaptureWorkspacePrefix.Length..];
        return suffix.Length == 32 &&
            suffix.All(static character =>
                character is
                    (>= '0' and <= '9') or
                    (>= 'a' and <= 'f'));
    }

    private static async Task<byte[]>
        ReadBoundedAsync(
        Stream stream,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        using var destination =
            new MemoryStream();
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                16 * 1024);
        try
        {
            while (true)
            {
                int read = await stream.ReadAsync(
                        buffer.AsMemory(
                            0,
                            buffer.Length),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                if (destination.Length >
                    maximumBytes - read)
                {
                    throw new
                        WorkerOutputLimitException();
                }
                await destination.WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            return destination.ToArray();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                buffer.AsSpan());
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task DrainBoundedAsync(
        Stream stream,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        byte[] bytes = await ReadBoundedAsync(
                stream,
                maximumBytes,
                cancellationToken)
            .ConfigureAwait(false);
        CryptographicOperations.ZeroMemory(bytes);
    }

    private static async Task TerminateAsync(
        Process process,
        CancellationTokenSource cancellation)
    {
        cancellation.Cancel();
        try
        {
            if (!process.HasExited)
            {
                process.Kill(
                    entireProcessTree: true);
            }
        }
        catch (Exception exception) when (
            exception is Win32Exception or
                InvalidOperationException or
                NotSupportedException)
        {
        }
        try
        {
            await process.WaitForExitAsync()
                .ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
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
            SnapshotIdentityPrefix,
            StringComparison.Ordinal) &&
        IsLowerSha256(
            value[SnapshotIdentityPrefix.Length..]);

    private static JsonSerializerOptions
        CreateJsonOptions() =>
        new(JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
            NumberHandling =
                JsonNumberHandling.Strict,
        };

    private sealed class WorkerOutputLimitException
        : IOException
    {
    }
}
