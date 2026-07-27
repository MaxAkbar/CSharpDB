using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal enum EfCoreWorkerClientStatus
{
    Success,
    ToolUnavailable,
    ProjectQueryFailed,
    ProjectIncompatible,
    BuildFailed,
    AssemblyInvalid,
    WorkerUnavailable,
    WorkerIncompatible,
    WorkerTimedOut,
    AnalysisFailed,
}

internal sealed record EfCoreWorkerClientResult
{
    internal required EfCoreWorkerClientStatus Status { get; init; }

    internal EfCoreMigrationAnalysisReport? Report { get; init; }

    internal EfCoreMigrationScratchAnalysisReport? ScratchReport
    {
        get;
        init;
    }

    internal static EfCoreWorkerClientResult Success(
        EfCoreMigrationAnalysisReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new EfCoreWorkerClientResult
        {
            Status = EfCoreWorkerClientStatus.Success,
            Report = report,
        };
    }

    internal static EfCoreWorkerClientResult Success(
        EfCoreMigrationScratchAnalysisReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new EfCoreWorkerClientResult
        {
            Status = EfCoreWorkerClientStatus.Success,
            ScratchReport = report,
        };
    }

    internal static EfCoreWorkerClientResult Failure(
        EfCoreWorkerClientStatus status)
    {
        if (status == EfCoreWorkerClientStatus.Success)
            throw new ArgumentOutOfRangeException(nameof(status));

        return new EfCoreWorkerClientResult { Status = status };
    }
}

internal static class EfCoreWorkerClient
{
    internal const string ProtocolV2 = "csharpdb-ef-worker/v2";
    internal const string RequestFormatV2 =
        EfCoreWorkerRunner.RequestFormat;
    internal const long MaxAssemblyBytes = 128L * 1024 * 1024;
    internal const long MaxWorkerReportBytes = 8L * 1024 * 1024;

    private const long MaxPropertyOutputBytes = 64L * 1024;
    private const long MaxBuildOutputBytes = 1024L * 1024;
    private const long MaxStderrBytes = 64L * 1024;
    private const int MaxRequestBytes = 64 * 1024;

    private static readonly TimeSpan PropertyQueryTimeout =
        TimeSpan.FromSeconds(30);
    private static readonly TimeSpan BuildTimeout =
        TimeSpan.FromMinutes(2);
    private static readonly TimeSpan WorkerTimeout =
        TimeSpan.FromMinutes(1);
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly byte[] SuccessHeaderBytes =
        StrictUtf8.GetBytes(ProtocolV2 + "\n");
    private static readonly JsonSerializerOptions WorkerJsonOptions =
        CreateWorkerJsonOptions();

    private static readonly string[] EnvironmentAllowlist =
    [
        "APPDATA",
        "HOME",
        "HOMEDRIVE",
        "HOMEPATH",
        "LANG",
        "LC_ALL",
        "LOCALAPPDATA",
        "NUGET_PACKAGES",
        "OS",
        "PATH",
        "PATHEXT",
        "PROCESSOR_ARCHITECTURE",
        "PROCESSOR_IDENTIFIER",
        "PROCESSOR_LEVEL",
        "PROCESSOR_REVISION",
        "ProgramData",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "SystemDrive",
        "SystemRoot",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "windir",
    ];

    internal static bool TryResolveProjectPath(
        string value,
        out string? projectPath)
    {
        projectPath = null;
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 32_767)
        {
            return false;
        }

        try
        {
            string rooted = Path.GetFullPath(value);
            if (!Path.IsPathFullyQualified(rooted) ||
                !string.Equals(
                    Path.GetExtension(rooted),
                    ".csproj",
                    StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var file = new FileInfo(rooted);
            if (!file.Exists)
                return false;
            if ((file.Attributes & FileAttributes.Directory) != 0)
                return false;

            if ((file.Attributes & FileAttributes.ReparsePoint) != 0)
            {
                FileSystemInfo? target =
                    file.ResolveLinkTarget(returnFinalTarget: true);
                if (target is not FileInfo targetFile ||
                    !targetFile.Exists)
                {
                    return false;
                }

                file = targetFile;
            }

            if (!Path.IsPathFullyQualified(file.FullName) ||
                !string.Equals(
                    file.Extension,
                    ".csproj",
                    StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            projectPath = file.FullName;
            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or IOException or
                NotSupportedException or PathTooLongException or
                UnauthorizedAccessException)
        {
            return false;
        }
    }

    internal static async ValueTask<EfCoreWorkerClientResult>
        AnalyzeProjectAsync(
            string projectPath,
            string contextName,
            EfCoreAnalysisMode mode,
            CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(contextName);
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode));
        cancellationToken.ThrowIfCancellationRequested();

        string? dotnetPath = ResolveDotnetHostPath();
        if (dotnetPath is null)
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.ToolUnavailable);
        }

        string projectDirectory = Path.GetDirectoryName(projectPath)!;
        ProjectProperties? beforeBuild;
        ProcessRunStatus queryStatus;
        (queryStatus, beforeBuild) =
            await QueryProjectPropertiesAsync(
                    dotnetPath,
                    projectPath,
                    projectDirectory,
                    cancellationToken)
                .ConfigureAwait(false);
        if (queryStatus != ProcessRunStatus.Success)
        {
            return EfCoreWorkerClientResult.Failure(
                queryStatus == ProcessRunStatus.TimedOut
                    ? EfCoreWorkerClientStatus.ProjectQueryFailed
                    : EfCoreWorkerClientStatus.ProjectQueryFailed);
        }
        if (beforeBuild is null ||
            !IsSupportedFramework(beforeBuild))
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.ProjectIncompatible);
        }

        ProcessRunResult build = await RunDotnetAsync(
                dotnetPath,
                projectDirectory,
                [
                    "build",
                    projectPath,
                    "-c",
                    "Debug",
                    "--framework",
                    "net10.0",
                    "--no-restore",
                    "--nologo",
                    "--verbosity",
                    "quiet",
                ],
                standardInput: null,
                MaxBuildOutputBytes,
                MaxStderrBytes,
                BuildTimeout,
                cancellationToken)
            .ConfigureAwait(false);
        try
        {
            if (build.Status != ProcessRunStatus.Success ||
                build.ExitCode != 0)
            {
                return EfCoreWorkerClientResult.Failure(
                    EfCoreWorkerClientStatus.BuildFailed);
            }
        }
        finally
        {
            build.ClearOutput();
        }

        ProjectProperties? afterBuild;
        (queryStatus, afterBuild) =
            await QueryProjectPropertiesAsync(
                    dotnetPath,
                    projectPath,
                    projectDirectory,
                    cancellationToken)
                .ConfigureAwait(false);
        if (queryStatus != ProcessRunStatus.Success ||
            afterBuild is null)
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.ProjectQueryFailed);
        }
        if (!IsSupportedFramework(afterBuild))
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.ProjectIncompatible);
        }

        if (!TryResolveManagedAssembly(
                afterBuild.TargetPath,
                projectDirectory,
                out string? assemblyPath))
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.AssemblyInvalid);
        }

        string assemblyDigest;
        try
        {
            assemblyDigest = await ComputeAssemblyDigestAsync(
                    assemblyPath!,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException or
                CryptographicException)
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.AssemblyInvalid);
        }

        return await RunWorkerAsync(
                dotnetPath,
                projectDirectory,
                assemblyPath!,
                assemblyDigest,
                contextName,
                mode,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async ValueTask<(
        ProcessRunStatus Status,
        ProjectProperties? Properties)> QueryProjectPropertiesAsync(
        string dotnetPath,
        string projectPath,
        string projectDirectory,
        CancellationToken cancellationToken)
    {
        ProcessRunResult result = await RunDotnetAsync(
                dotnetPath,
                projectDirectory,
                [
                    "msbuild",
                    projectPath,
                    "-nologo",
                    "-getProperty:TargetFrameworks,TargetFramework,TargetPath",
                    "-property:Configuration=Debug",
                ],
                standardInput: null,
                MaxPropertyOutputBytes,
                MaxStderrBytes,
                PropertyQueryTimeout,
                cancellationToken)
            .ConfigureAwait(false);
        try
        {
            if (result.Status != ProcessRunStatus.Success ||
                result.ExitCode != 0 ||
                !TryParseProjectProperties(
                    result.StandardOutput,
                    out ProjectProperties? properties))
            {
                return (result.Status, null);
            }

            return (ProcessRunStatus.Success, properties);
        }
        finally
        {
            result.ClearOutput();
        }
    }

    private static async ValueTask<EfCoreWorkerClientResult>
        RunWorkerAsync(
            string dotnetPath,
            string workingDirectory,
            string assemblyPath,
            string assemblyDigest,
            string contextName,
            EfCoreAnalysisMode mode,
            CancellationToken cancellationToken)
    {
        string? entryAssemblyPath = GetEntryAssemblyPath();
        if (entryAssemblyPath is null)
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.WorkerUnavailable);
        }

        byte[] request;
        try
        {
            request = JsonSerializer.SerializeToUtf8Bytes(
                new WorkerClientRequest
                {
                    Format = RequestFormatV2,
                    Mode = mode,
                    AssemblyPath = assemblyPath,
                    AssemblyDigest = assemblyDigest,
                    Context = contextName,
                },
                WorkerJsonOptions);
        }
        catch (Exception exception) when (
            exception is ArgumentException or JsonException or
                NotSupportedException)
        {
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.WorkerIncompatible);
        }
        if (request.Length > MaxRequestBytes)
        {
            CryptographicOperations.ZeroMemory(request);
            return EfCoreWorkerClientResult.Failure(
                EfCoreWorkerClientStatus.WorkerIncompatible);
        }

        try
        {
            ProcessRunResult result = await RunDotnetAsync(
                    dotnetPath,
                    workingDirectory,
                    [
                        entryAssemblyPath,
                        "--worker",
                        "--protocol",
                        ProtocolV2,
                        "--target-version",
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion,
                    ],
                    request,
                    MaxWorkerReportBytes,
                    MaxStderrBytes,
                    WorkerTimeout,
                    cancellationToken)
                .ConfigureAwait(false);
            try
            {
                if (result.Status == ProcessRunStatus.TimedOut)
                {
                    return EfCoreWorkerClientResult.Failure(
                        EfCoreWorkerClientStatus.WorkerTimedOut);
                }
                if (result.Status != ProcessRunStatus.Success)
                {
                    return EfCoreWorkerClientResult.Failure(
                        result.Status == ProcessRunStatus.StartFailed
                            ? EfCoreWorkerClientStatus.WorkerUnavailable
                            : EfCoreWorkerClientStatus.WorkerIncompatible);
                }

                if (result.ExitCode == 0)
                {
                    if (!TryParseAndSanitizeReport(
                            result.StandardOutput,
                            assemblyDigest,
                            contextName,
                            mode,
                            out EfCoreMigrationAnalysisReport?
                                sanitized,
                            out EfCoreMigrationScratchAnalysisReport?
                                sanitizedScratch))
                    {
                        return EfCoreWorkerClientResult.Failure(
                            EfCoreWorkerClientStatus.WorkerIncompatible);
                    }

                    return mode == EfCoreAnalysisMode.Generation
                        ? EfCoreWorkerClientResult.Success(sanitized!)
                        : EfCoreWorkerClientResult.Success(
                            sanitizedScratch!);
                }

                return EfCoreWorkerClientResult.Failure(
                    result.ExitCode is
                        EfCoreWorkerRunner.ExitInputLimit or
                        EfCoreWorkerRunner.ExitAssemblyUnavailable or
                        EfCoreWorkerRunner.ExitAssemblyDigestMismatch or
                        EfCoreWorkerRunner.ExitContextUnavailable or
                        EfCoreWorkerRunner.ExitAnalysisFailed
                        ? EfCoreWorkerClientStatus.AnalysisFailed
                        : EfCoreWorkerClientStatus.WorkerIncompatible);
            }
            finally
            {
                result.ClearOutput();
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(request);
        }
    }

    private static bool TryParseAndSanitizeReport(
        byte[] payload,
        string expectedAssemblyDigest,
        string expectedContextName,
        EfCoreAnalysisMode mode,
        out EfCoreMigrationAnalysisReport? sanitized,
        out EfCoreMigrationScratchAnalysisReport? sanitizedScratch)
    {
        // The worker contract is validated and reconstructed here rather than
        // being rendered directly. The exact reconstruction follows the
        // shared report records in the analyzer implementation.
        sanitized = null;
        sanitizedScratch = null;
        try
        {
            if (payload.Length <= SuccessHeaderBytes.Length ||
                !payload
                    .AsSpan(0, SuccessHeaderBytes.Length)
                    .SequenceEqual(SuccessHeaderBytes))
            {
                return false;
            }

            ReadOnlySpan<byte> body =
                payload.AsSpan(SuccessHeaderBytes.Length);
            if (mode == EfCoreAnalysisMode.Generation)
            {
                EfCoreMigrationAnalysisReport? report =
                    JsonSerializer
                        .Deserialize<EfCoreMigrationAnalysisReport>(
                            body,
                            WorkerJsonOptions);
                return report is not null &&
                    EfCoreReportSanitizer.TrySanitize(
                        report,
                        expectedAssemblyDigest,
                        expectedContextName,
                        out sanitized);
            }
            if (mode != EfCoreAnalysisMode.Scratch)
                return false;

            EfCoreMigrationScratchAnalysisReport? scratchReport =
                JsonSerializer
                    .Deserialize<EfCoreMigrationScratchAnalysisReport>(
                        body,
                        WorkerJsonOptions);
            return scratchReport is not null &&
                EfCoreScratchReportSanitizer.TrySanitize(
                    scratchReport,
                    expectedAssemblyDigest,
                    expectedContextName,
                    out sanitizedScratch);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or JsonException or
                ArgumentException or FormatException or
                InvalidOperationException or NotSupportedException or
                OverflowException)
        {
            return false;
        }
    }

    private static bool TryParseProjectProperties(
        byte[] payload,
        out ProjectProperties? properties)
    {
        properties = null;
        try
        {
            using JsonDocument document = JsonDocument.Parse(
                payload,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 8,
                });
            JsonElement root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;

            JsonElement propertyObject = default;
            int rootPropertyCount = 0;
            foreach (JsonProperty property in root.EnumerateObject())
            {
                rootPropertyCount++;
                if (!string.Equals(
                        property.Name,
                        "Properties",
                        StringComparison.Ordinal) ||
                    property.Value.ValueKind != JsonValueKind.Object)
                {
                    return false;
                }

                propertyObject = property.Value;
            }
            if (rootPropertyCount != 1)
                return false;

            string? targetFrameworks = null;
            string? targetFramework = null;
            string? targetPath = null;
            int propertyCount = 0;
            foreach (JsonProperty property in
                     propertyObject.EnumerateObject())
            {
                propertyCount++;
                if (property.Value.ValueKind != JsonValueKind.String)
                    return false;
                string value = property.Value.GetString()!;
                if (value.Length > 32_767 ||
                    ContainsControlCharacter(value))
                {
                    return false;
                }

                switch (property.Name)
                {
                    case "TargetFrameworks"
                        when targetFrameworks is null:
                        targetFrameworks = value;
                        break;
                    case "TargetFramework"
                        when targetFramework is null:
                        targetFramework = value;
                        break;
                    case "TargetPath" when targetPath is null:
                        targetPath = value;
                        break;
                    default:
                        return false;
                }
            }
            if (propertyCount != 3 ||
                targetFrameworks is null ||
                targetFramework is null ||
                targetPath is null)
            {
                return false;
            }

            properties = new ProjectProperties(
                targetFrameworks,
                targetFramework,
                targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is JsonException or InvalidOperationException)
        {
            return false;
        }
    }

    private static bool IsSupportedFramework(
        ProjectProperties properties)
    {
        if (string.IsNullOrEmpty(properties.TargetFrameworks))
        {
            return string.Equals(
                properties.TargetFramework,
                "net10.0",
                StringComparison.Ordinal);
        }

        string[] frameworks = properties.TargetFrameworks.Split(
            ';',
            StringSplitOptions.RemoveEmptyEntries |
                StringSplitOptions.TrimEntries);
        return frameworks.Length == 1 &&
            string.Equals(
                frameworks[0],
                "net10.0",
                StringComparison.Ordinal) &&
            string.Equals(
                properties.TargetFramework,
                "net10.0",
                StringComparison.Ordinal);
    }

    private static bool TryResolveManagedAssembly(
        string targetPath,
        string projectDirectory,
        out string? assemblyPath)
    {
        assemblyPath = null;
        if (string.IsNullOrWhiteSpace(targetPath) ||
            ContainsControlCharacter(targetPath))
        {
            return false;
        }

        try
        {
            string rooted = Path.IsPathFullyQualified(targetPath)
                ? Path.GetFullPath(targetPath)
                : Path.GetFullPath(targetPath, projectDirectory);
            if (!Path.IsPathFullyQualified(rooted) ||
                !string.Equals(
                    Path.GetExtension(rooted),
                    ".dll",
                    StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var file = new FileInfo(rooted);
            if (!file.Exists ||
                file.Length is <= 0 or > MaxAssemblyBytes)
            {
                return false;
            }
            if ((file.Attributes & FileAttributes.ReparsePoint) != 0)
            {
                FileSystemInfo? target =
                    file.ResolveLinkTarget(returnFinalTarget: true);
                if (target is not FileInfo targetFile ||
                    !targetFile.Exists)
                {
                    return false;
                }

                file = targetFile;
            }
            if (!Path.IsPathFullyQualified(file.FullName) ||
                !string.Equals(
                    file.Extension,
                    ".dll",
                    StringComparison.OrdinalIgnoreCase) ||
                file.Length is <= 0 or > MaxAssemblyBytes)
            {
                return false;
            }

            using var stream = new FileStream(
                file.FullName,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 64 * 1024,
                FileOptions.SequentialScan);
            using var peReader = new PEReader(
                stream,
                PEStreamOptions.LeaveOpen);
            if (!peReader.HasMetadata ||
                peReader.PEHeaders.CorHeader is null)
            {
                return false;
            }
            MetadataReader metadata = peReader.GetMetadataReader();
            if (!metadata.IsAssembly)
                return false;

            _ = AssemblyName.GetAssemblyName(file.FullName);
            assemblyPath = file.FullName;
            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or BadImageFormatException or
                FileLoadException or IOException or NotSupportedException or
                PathTooLongException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static async ValueTask<string> ComputeAssemblyDigestAsync(
        string assemblyPath,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            assemblyPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        long expectedLength = stream.Length;
        if (expectedLength is <= 0 or > MaxAssemblyBytes)
            throw new IOException("The analysis assembly is invalid.");

        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        using var hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[]? digest = null;
        try
        {
            long remaining = expectedLength;
            while (remaining > 0)
            {
                int read = await stream.ReadAsync(
                        buffer.AsMemory(
                            0,
                            (int)Math.Min(
                                buffer.Length,
                                remaining)),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    throw new IOException(
                        "The analysis assembly changed while it was read.");
                hash.AppendData(buffer, 0, read);
                remaining -= read;
            }

            int extra = await stream.ReadAsync(
                    buffer.AsMemory(0, 1),
                    cancellationToken)
                .ConfigureAwait(false);
            if (extra != 0 ||
                stream.Length != expectedLength)
            {
                throw new IOException(
                    "The analysis assembly changed while it was read.");
            }

            digest = hash.GetHashAndReset();
            return Convert.ToHexString(digest).ToLowerInvariant();
        }
        finally
        {
            if (digest is not null)
                CryptographicOperations.ZeroMemory(digest);
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static async ValueTask<ProcessRunResult> RunDotnetAsync(
        string dotnetPath,
        string workingDirectory,
        IReadOnlyList<string> arguments,
        byte[]? standardInput,
        long maxStandardOutputBytes,
        long maxStandardErrorBytes,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var startInfo = new ProcessStartInfo
        {
            FileName = dotnetPath,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            RedirectStandardInput = standardInput is not null,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);
        ConfigureScrubbedEnvironment(startInfo);
        if (arguments.Contains(
                "--worker",
                StringComparer.Ordinal))
        {
            // 0x18000000 = 384 MiB. This applies a managed-heap ceiling on
            // every supported OS in addition to the Windows job-object cap.
            startInfo.Environment["DOTNET_GCHeapHardLimit"] =
                "18000000";
        }

        using var process = new Process { StartInfo = startInfo };
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!process.Start())
                return ProcessRunResult.Failure(
                    ProcessRunStatus.StartFailed);
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or UnauthorizedAccessException)
        {
            return ProcessRunResult.Failure(
                ProcessRunStatus.StartFailed);
        }

        EfCoreProcessContainment containment;
        try
        {
            containment = EfCoreProcessContainment.Attach(process);
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or NotSupportedException)
        {
            await KillAndWaitAsync(process).ConfigureAwait(false);
            return ProcessRunResult.Failure(
                ProcessRunStatus.ContainmentFailed);
        }
        using EfCoreProcessContainment containmentScope = containment;

        using var timeoutCancellation =
            new CancellationTokenSource(timeout);
        using var processCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                timeoutCancellation.Token);

        Task inputTask = standardInput is null
            ? Task.CompletedTask
            : WriteAndCloseAsync(
                process.StandardInput.BaseStream,
                standardInput,
                processCancellation.Token);
        Task<byte[]> stdoutTask = ReadBoundedAsync(
            process.StandardOutput.BaseStream,
            maxStandardOutputBytes,
            processCancellation.Token);
        Task stderrTask = DrainBoundedAsync(
            process.StandardError.BaseStream,
            maxStandardErrorBytes,
            processCancellation.Token);
        Task exitTask =
            process.WaitForExitAsync(processCancellation.Token);
        Task[] tasks =
            [inputTask, stdoutTask, stderrTask, exitTask];

        try
        {
            await ObserveAllAsync(tasks).ConfigureAwait(false);
            byte[] stdout = await stdoutTask.ConfigureAwait(false);
            return ProcessRunResult.Success(
                process.ExitCode,
                stdout);
        }
        catch (OperationCanceledException) when (
            cancellationToken.IsCancellationRequested)
        {
            await TerminateAsync(
                    process,
                    containmentScope,
                    processCancellation,
                    tasks)
                .ConfigureAwait(false);
            ClearCompletedOutput(stdoutTask);
            throw;
        }
        catch (OperationCanceledException) when (
            timeoutCancellation.IsCancellationRequested)
        {
            await TerminateAsync(
                    process,
                    containmentScope,
                    processCancellation,
                    tasks)
                .ConfigureAwait(false);
            ClearCompletedOutput(stdoutTask);
            return ProcessRunResult.Failure(
                ProcessRunStatus.TimedOut);
        }
        catch (Exception exception) when (
            exception is IOException or InvalidOperationException or
                ProcessOutputLimitException)
        {
            await TerminateAsync(
                    process,
                    containmentScope,
                    processCancellation,
                    tasks)
                .ConfigureAwait(false);
            ClearCompletedOutput(stdoutTask);
            return ProcessRunResult.Failure(
                exception is ProcessOutputLimitException
                    ? ProcessRunStatus.OutputLimitExceeded
                    : ProcessRunStatus.Failed);
        }
    }

    private static void ConfigureScrubbedEnvironment(
        ProcessStartInfo startInfo)
    {
        var retained =
            new Dictionary<string, string>(
                OperatingSystem.IsWindows()
                    ? StringComparer.OrdinalIgnoreCase
                    : StringComparer.Ordinal);
        foreach (string name in EnvironmentAllowlist)
        {
            string? value = Environment.GetEnvironmentVariable(name);
            if (!string.IsNullOrEmpty(value))
                retained[name] = value;
        }

        startInfo.Environment.Clear();
        foreach ((string name, string value) in retained)
            startInfo.Environment[name] = value;

        startInfo.Environment["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1";
        startInfo.Environment["DOTNET_NOLOGO"] = "1";
        startInfo.Environment["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1";
        startInfo.Environment["MSBUILDDISABLENODEREUSE"] = "1";
        startInfo.Environment["NO_COLOR"] = "1";
    }

    private static string? ResolveDotnetHostPath()
    {
        var candidates = new List<string>();
        AddCandidate(
            candidates,
            Environment.GetEnvironmentVariable("DOTNET_HOST_PATH"));

        string? processPath = Environment.ProcessPath;
        if (processPath is not null &&
            string.Equals(
                Path.GetFileNameWithoutExtension(processPath),
                "dotnet",
                StringComparison.OrdinalIgnoreCase))
        {
            AddCandidate(candidates, processPath);
        }

        string? dotnetRoot =
            Environment.GetEnvironmentVariable("DOTNET_ROOT");
        if (!string.IsNullOrWhiteSpace(dotnetRoot))
        {
            AddCandidate(
                candidates,
                Path.Combine(
                    dotnetRoot,
                    OperatingSystem.IsWindows()
                        ? "dotnet.exe"
                        : "dotnet"));
        }

        string? programFiles =
            Environment.GetFolderPath(
                Environment.SpecialFolder.ProgramFiles);
        if (!string.IsNullOrWhiteSpace(programFiles))
        {
            AddCandidate(
                candidates,
                Path.Combine(
                    programFiles,
                    "dotnet",
                    OperatingSystem.IsWindows()
                        ? "dotnet.exe"
                        : "dotnet"));
        }

        string? path = Environment.GetEnvironmentVariable("PATH");
        if (!string.IsNullOrEmpty(path))
        {
            foreach (string directory in path.Split(
                         Path.PathSeparator,
                         StringSplitOptions.RemoveEmptyEntries |
                             StringSplitOptions.TrimEntries))
            {
                try
                {
                    AddCandidate(
                        candidates,
                        Path.Combine(
                            directory,
                            OperatingSystem.IsWindows()
                                ? "dotnet.exe"
                                : "dotnet"));
                }
                catch (Exception exception) when (
                    exception is ArgumentException or
                        NotSupportedException or PathTooLongException)
                {
                }
            }
        }

        foreach (string candidate in candidates)
        {
            try
            {
                string rooted = Path.GetFullPath(candidate);
                if (Path.IsPathFullyQualified(rooted) &&
                    File.Exists(rooted))
                {
                    return rooted;
                }
            }
            catch (Exception exception) when (
                exception is ArgumentException or IOException or
                    NotSupportedException or PathTooLongException or
                    UnauthorizedAccessException)
            {
            }
        }

        return null;
    }

    private static void AddCandidate(
        ICollection<string> candidates,
        string? candidate)
    {
        if (!string.IsNullOrWhiteSpace(candidate))
            candidates.Add(candidate);
    }

    private static string? GetEntryAssemblyPath()
    {
        try
        {
            string? path = Assembly.GetEntryAssembly()?.Location;
            if (string.IsNullOrWhiteSpace(path))
                return null;
            string rooted = Path.GetFullPath(path);
            return Path.IsPathFullyQualified(rooted) &&
                File.Exists(rooted)
                ? rooted
                : null;
        }
        catch (Exception exception) when (
            exception is ArgumentException or IOException or
                NotSupportedException or PathTooLongException or
                UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static async Task ObserveAllAsync(
        IReadOnlyList<Task> tasks)
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
                            Math.Min(
                                64 * 1024,
                                maximum - total)),
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
                throw new ProcessOutputLimitException();

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
                    throw new ProcessOutputLimitException();
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
        EfCoreProcessContainment containment,
        CancellationTokenSource processCancellation,
        params Task[] tasks)
    {
        containment.Terminate();
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
                InvalidOperationException or ProcessOutputLimitException)
        {
        }
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
                using var timeout = new CancellationTokenSource(
                    TimeSpan.FromSeconds(2));
                await process.WaitForExitAsync(timeout.Token)
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

    private static void ClearCompletedOutput(
        Task<byte[]> stdoutTask)
    {
        if (stdoutTask.Status == TaskStatus.RanToCompletion)
            CryptographicOperations.ZeroMemory(stdoutTask.Result);
    }

    private static bool ContainsControlCharacter(string value)
    {
        foreach (char character in value)
        {
            if (char.IsControl(character))
                return true;
        }

        return false;
    }

    private static JsonSerializerOptions CreateWorkerJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            WriteIndented = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    private sealed record WorkerClientRequest
    {
        public required string Format { get; init; }

        public required EfCoreAnalysisMode Mode { get; init; }

        public required string AssemblyPath { get; init; }

        public required string AssemblyDigest { get; init; }

        public string? Context { get; init; }
    }

    private sealed record ProjectProperties(
        string TargetFrameworks,
        string TargetFramework,
        string TargetPath);

    private enum ProcessRunStatus
    {
        Success,
        StartFailed,
        ContainmentFailed,
        TimedOut,
        OutputLimitExceeded,
        Failed,
    }

    private sealed record ProcessRunResult
    {
        internal required ProcessRunStatus Status { get; init; }

        internal int ExitCode { get; init; }

        internal byte[] StandardOutput { get; init; } = [];

        internal static ProcessRunResult Success(
            int exitCode,
            byte[] standardOutput) =>
            new()
            {
                Status = ProcessRunStatus.Success,
                ExitCode = exitCode,
                StandardOutput = standardOutput,
            };

        internal static ProcessRunResult Failure(
            ProcessRunStatus status)
        {
            if (status == ProcessRunStatus.Success)
                throw new ArgumentOutOfRangeException(nameof(status));

            return new ProcessRunResult { Status = status };
        }

        internal void ClearOutput()
        {
            if (StandardOutput.Length != 0)
                CryptographicOperations.ZeroMemory(StandardOutput);
        }
    }

    private sealed class ProcessOutputLimitException : IOException;
}

internal static class EfCoreReportSanitizer
{
    private const int MaxMigrationCount =
        EfCoreMigrationAnalyzer.MaxMigrations;
    private const int MaxOperationCount =
        EfCoreMigrationAnalyzer.MaxOperations;
    private const int MaxDiagnosticCount =
        EfCoreMigrationAnalyzer.MaxOperations +
        EfCoreMigrationAnalyzer.MaxMigrations + 1;
    private const int MaxAnnotationCount =
        EfCoreMigrationAnalyzer.MaxAnnotationsPerOperation;
    private const int MaxCommandCount =
        EfCoreMigrationAnalyzer.MaxCommands;
    private const long MaxGeneratedSqlUtf8Bytes =
        EfCoreMigrationAnalyzer.MaxGeneratedSqlUtf8Bytes;
    private static readonly string QualifiedEfCoreVersion =
        ProductInfo.GetVersion();

    internal static bool TrySanitize(
        EfCoreMigrationAnalysisReport report,
        string expectedAssemblyDigest,
        string expectedContextSelector,
        out EfCoreMigrationAnalysisReport? sanitized)
    {
        sanitized = null;
        ArgumentNullException.ThrowIfNull(report);
        if (!IsCanonicalSha256(expectedAssemblyDigest) ||
            !EfCoreAnalyzeCommandRunner.IsSafeContextSelector(
                expectedContextSelector))
        {
            return false;
        }

        CSharpDbCapabilityCatalog capabilities;
        try
        {
            capabilities =
                CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        }
        catch (Exception exception) when (
            exception is InvalidDataException or InvalidOperationException or
                JsonException)
        {
            return false;
        }

        if (!string.Equals(
                report.Format,
                EfCoreMigrationAnalysisReport.CurrentFormat,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.Provider,
                EfCoreMigrationAnalysisReport.CSharpDbProvider,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.CapabilityDigest,
                capabilities.Digest,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.AssemblyDigest,
                expectedAssemblyDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.QualifiedEfCoreVersion,
                QualifiedEfCoreVersion,
                StringComparison.Ordinal) ||
            !IsExpectedContext(
                report.Context,
                expectedContextSelector) ||
            !IsAnalysisStatus(report.Status) ||
            report.HighestEvidence != MigrationEvidenceLevel.Bound ||
            !IsRuleStatusValid(report.RuleId, report.Status) ||
            report.MigrationCount is < 0 or > MaxMigrationCount ||
            report.OperationCount is < 0 or > MaxOperationCount ||
            report.DestructiveOperationCount is < 0 or
                > MaxOperationCount ||
            report.CommandCount is < 0 or > MaxCommandCount ||
            !IsDigestPresentExactlyForCommands(
                report.GeneratedSqlDigest,
                report.CommandCount) ||
            report.Migrations is null ||
            report.Diagnostics is null ||
            report.Migrations.Count != report.MigrationCount ||
            report.Diagnostics.Count > MaxDiagnosticCount)
        {
            return false;
        }

        var migrations =
            new List<EfCoreMigrationAnalysisMigration>(
                report.MigrationCount);
        var expectedDiagnostics =
            new List<ExpectedDiagnostic>(
                Math.Min(
                    report.OperationCount +
                        report.MigrationCount + 1,
                    MaxDiagnosticCount));
        long operationCount = 0;
        long destructiveCount = 0;
        long commandCount = 0;
        long generatedSqlBytes = 0;
        string? previousMigrationId = null;
        MigrationCompatibilityStatus aggregateStatus =
            MigrationCompatibilityStatus.Conditional;
        string aggregateRule =
            EfCoreMigrationAnalysisRules.GenerationBound;
        for (int migrationIndex = 0;
             migrationIndex < report.Migrations.Count;
             migrationIndex++)
        {
            EfCoreMigrationAnalysisMigration? migration =
                report.Migrations[migrationIndex];
            if (migration is null ||
                migration.Ordinal != migrationIndex ||
                !IsSafeMigrationId(migration.MigrationId) ||
                previousMigrationId is not null &&
                StringComparer.Ordinal.Compare(
                    previousMigrationId,
                    migration.MigrationId) >= 0 ||
                !IsAnalysisStatus(migration.Status) ||
                migration.HighestEvidence !=
                    MigrationEvidenceLevel.Bound ||
                !IsRuleStatusValid(
                    migration.RuleId,
                    migration.Status) ||
                migration.UpOperationCount is < 0 or > MaxOperationCount ||
                migration.DownOperationCount is < 0 or
                    > MaxOperationCount ||
                migration.OperationCount is < 0 or > MaxOperationCount ||
                migration.DestructiveOperationCount is < 0 or
                    > MaxOperationCount ||
                migration.CommandCount is < 0 or > MaxCommandCount ||
                migration.Operations is null ||
                migration.Operations.Count !=
                    migration.OperationCount ||
                (long)migration.UpOperationCount +
                    migration.DownOperationCount !=
                    migration.OperationCount ||
                !IsDigestPresentExactlyForCommands(
                    migration.GeneratedSqlDigest,
                    migration.CommandCount))
            {
                return false;
            }

            var operations =
                new List<EfCoreMigrationOperationFinding>(
                    migration.OperationCount);
            int migrationDestructiveCount = 0;
            long migrationCommandCount = 0;
            int nextUpOrdinal = 0;
            int nextDownOrdinal = 0;
            MigrationCompatibilityStatus migrationAggregateStatus =
                MigrationCompatibilityStatus.Conditional;
            string migrationAggregateRule =
                EfCoreMigrationAnalysisRules.GenerationBound;
            for (int operationIndex = 0;
                 operationIndex < migration.Operations.Count;
                 operationIndex++)
            {
                EfCoreMigrationOperationFinding? operation =
                    migration.Operations[operationIndex];
                if (operation is null ||
                    operation.Ordinal != operationIndex ||
                    !Enum.IsDefined(operation.Direction) ||
                    !Enum.IsDefined(operation.Kind) ||
                    !IsAnalysisStatus(operation.Status) ||
                    operation.Evidence != MigrationEvidenceLevel.Bound ||
                    !IsRuleStatusValid(
                        operation.RuleId,
                        operation.Status) ||
                    !IsRuleKindValid(
                        operation.RuleId,
                        operation.Kind) ||
                    operation.AnnotationCount is < 0 or
                        > MaxAnnotationCount ||
                    operation.CommandCount is < 0 or > MaxCommandCount ||
                    operation.GeneratedSqlUtf8Bytes is < 0 ||
                    !IsDigestPresentExactlyForCommands(
                        operation.GeneratedSqlDigest,
                        operation.CommandCount))
                {
                    return false;
                }

                if (operation.Direction ==
                    EfCoreMigrationDirection.Up)
                {
                    if (operationIndex >=
                            migration.UpOperationCount ||
                        operation.DirectionOrdinal !=
                            nextUpOrdinal++)
                    {
                        return false;
                    }
                }
                else
                {
                    if (operationIndex <
                            migration.UpOperationCount ||
                        operation.DirectionOrdinal !=
                            nextDownOrdinal++)
                    {
                        return false;
                    }
                }

                if (operation.CommandCount == 0 &&
                    operation.GeneratedSqlUtf8Bytes != 0)
                {
                    return false;
                }
                if (operation.CommandCount > 0 &&
                    operation.GeneratedSqlUtf8Bytes <= 0)
                {
                    return false;
                }

                migrationCommandCount += operation.CommandCount;
                generatedSqlBytes += operation.GeneratedSqlUtf8Bytes;
                if (migrationCommandCount > MaxCommandCount ||
                    generatedSqlBytes > MaxGeneratedSqlUtf8Bytes)
                {
                    return false;
                }
                if (operation.IsDestructive)
                    migrationDestructiveCount++;
                UpdateAggregate(
                    operation.Status,
                    operation.RuleId,
                    ref migrationAggregateStatus,
                    ref migrationAggregateRule);

                if (operation.Status !=
                    MigrationCompatibilityStatus.Conditional)
                {
                    expectedDiagnostics.Add(
                        new ExpectedDiagnostic(
                            operation.RuleId,
                            operation.Status,
                            migrationIndex,
                            operationIndex));
                }

                operations.Add(new EfCoreMigrationOperationFinding
                {
                    Ordinal = operation.Ordinal,
                    Direction = operation.Direction,
                    DirectionOrdinal = operation.DirectionOrdinal,
                    Kind = operation.Kind,
                    Status = operation.Status,
                    Evidence = operation.Evidence,
                    RuleId = operation.RuleId,
                    IsDestructive = operation.IsDestructive,
                    AnnotationCount = operation.AnnotationCount,
                    CommandCount = operation.CommandCount,
                    GeneratedSqlUtf8Bytes =
                        operation.GeneratedSqlUtf8Bytes,
                    GeneratedSqlDigest = operation.GeneratedSqlDigest,
                });
            }

            if (migration.OperationCount == 0)
            {
                migrationAggregateRule =
                    EfCoreMigrationAnalysisRules.EmptyMigration;
                expectedDiagnostics.Add(
                    new ExpectedDiagnostic(
                        migrationAggregateRule,
                        MigrationCompatibilityStatus.Conditional,
                        migrationIndex,
                        OperationOrdinal: null));
            }
            else if (migration.DownOperationCount == 0 &&
                     migrationAggregateStatus ==
                        MigrationCompatibilityStatus.Conditional)
            {
                migrationAggregateRule =
                    EfCoreMigrationAnalysisRules.EmptyDownMigration;
                expectedDiagnostics.Add(
                    new ExpectedDiagnostic(
                        migrationAggregateRule,
                        MigrationCompatibilityStatus.Conditional,
                        migrationIndex,
                        OperationOrdinal: null));
            }

            if (nextUpOrdinal != migration.UpOperationCount ||
                nextDownOrdinal != migration.DownOperationCount ||
                migrationDestructiveCount !=
                    migration.DestructiveOperationCount ||
                migrationCommandCount != migration.CommandCount ||
                migrationAggregateStatus != migration.Status ||
                !string.Equals(
                    migrationAggregateRule,
                    migration.RuleId,
                    StringComparison.Ordinal))
            {
                return false;
            }

            operationCount += migration.OperationCount;
            destructiveCount += migration.DestructiveOperationCount;
            commandCount += migration.CommandCount;
            if (operationCount > MaxOperationCount ||
                destructiveCount > MaxOperationCount ||
                commandCount > MaxCommandCount)
            {
                return false;
            }
            UpdateAggregate(
                migration.Status,
                migration.RuleId,
                ref aggregateStatus,
                ref aggregateRule);
            previousMigrationId = migration.MigrationId;
            migrations.Add(new EfCoreMigrationAnalysisMigration
            {
                Ordinal = migration.Ordinal,
                MigrationId = migration.MigrationId,
                Status = migration.Status,
                HighestEvidence = migration.HighestEvidence,
                RuleId = migration.RuleId,
                UpOperationCount = migration.UpOperationCount,
                DownOperationCount = migration.DownOperationCount,
                OperationCount = migration.OperationCount,
                DestructiveOperationCount =
                    migration.DestructiveOperationCount,
                CommandCount = migration.CommandCount,
                GeneratedSqlDigest = migration.GeneratedSqlDigest,
                Operations = operations,
            });
        }

        if (aggregateStatus ==
            MigrationCompatibilityStatus.Conditional)
        {
            aggregateRule =
                EfCoreMigrationAnalysisRules.GenerationBound;
            expectedDiagnostics.Add(
                new ExpectedDiagnostic(
                    aggregateRule,
                    aggregateStatus,
                    MigrationOrdinal: null,
                    OperationOrdinal: null));
        }

        if (operationCount != report.OperationCount ||
            destructiveCount != report.DestructiveOperationCount ||
            commandCount != report.CommandCount ||
            aggregateStatus != report.Status ||
            !string.Equals(
                aggregateRule,
                report.RuleId,
                StringComparison.Ordinal) ||
            report.Diagnostics.Count != expectedDiagnostics.Count)
        {
            return false;
        }

        var diagnostics =
            new List<EfCoreMigrationAnalysisDiagnostic>(
                report.Diagnostics.Count);
        for (int diagnosticIndex = 0;
             diagnosticIndex < report.Diagnostics.Count;
             diagnosticIndex++)
        {
            EfCoreMigrationAnalysisDiagnostic? diagnostic =
                report.Diagnostics[diagnosticIndex];
            ExpectedDiagnostic expected =
                expectedDiagnostics[diagnosticIndex];
            if (diagnostic is null ||
                diagnostic.Ordinal != diagnosticIndex ||
                !string.Equals(
                    diagnostic.DiagnosticId,
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"ef.diagnostic.{diagnosticIndex:D6}"),
                    StringComparison.Ordinal) ||
                diagnostic.Status != expected.Status ||
                !string.Equals(
                    diagnostic.RuleId,
                    expected.RuleId,
                    StringComparison.Ordinal) ||
                diagnostic.Evidence != MigrationEvidenceLevel.Bound ||
                diagnostic.Severity !=
                    ExpectedSeverity(diagnostic.Status) ||
                diagnostic.MigrationOrdinal !=
                    expected.MigrationOrdinal ||
                diagnostic.OperationOrdinal !=
                    expected.OperationOrdinal ||
                !TryGetFixedText(
                    diagnostic.RuleId,
                    out FixedDiagnosticText text) ||
                !TryValidateDiagnosticLocation(
                    diagnostic,
                    migrations))
            {
                return false;
            }

            diagnostics.Add(new EfCoreMigrationAnalysisDiagnostic
            {
                Ordinal = diagnostic.Ordinal,
                DiagnosticId = diagnostic.DiagnosticId,
                RuleId = diagnostic.RuleId,
                Severity = diagnostic.Severity,
                Status = diagnostic.Status,
                Evidence = diagnostic.Evidence,
                MigrationOrdinal = diagnostic.MigrationOrdinal,
                OperationOrdinal = diagnostic.OperationOrdinal,
                Summary = text.Summary,
                Remediation = text.Remediation,
            });
        }

        sanitized = new EfCoreMigrationAnalysisReport
        {
            Format = EfCoreMigrationAnalysisReport.CurrentFormat,
            Provider = EfCoreMigrationAnalysisReport.CSharpDbProvider,
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            CapabilityDigest = capabilities.Digest,
            AssemblyDigest = expectedAssemblyDigest,
            QualifiedEfCoreVersion = QualifiedEfCoreVersion,
            Context = report.Context,
            Status = report.Status,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = report.RuleId,
            MigrationCount = report.MigrationCount,
            OperationCount = report.OperationCount,
            DestructiveOperationCount =
                report.DestructiveOperationCount,
            CommandCount = report.CommandCount,
            GeneratedSqlDigest = report.GeneratedSqlDigest,
            Migrations = migrations,
            Diagnostics = diagnostics,
        };
        return true;
    }

    private static bool TryValidateDiagnosticLocation(
        EfCoreMigrationAnalysisDiagnostic diagnostic,
        IReadOnlyList<EfCoreMigrationAnalysisMigration> migrations)
    {
        if (diagnostic.MigrationOrdinal is null)
            return diagnostic.OperationOrdinal is null;
        if (diagnostic.MigrationOrdinal < 0 ||
            diagnostic.MigrationOrdinal >= migrations.Count)
        {
            return false;
        }

        if (diagnostic.OperationOrdinal is null)
            return true;

        EfCoreMigrationAnalysisMigration migration =
            migrations[diagnostic.MigrationOrdinal.Value];
        return diagnostic.OperationOrdinal >= 0 &&
            diagnostic.OperationOrdinal < migration.OperationCount;
    }

    private static bool IsExpectedContext(
        string? context,
        string selector)
    {
        if (context is null ||
            !EfCoreAnalyzeCommandRunner.IsSafeContextSelector(context))
        {
            return false;
        }

        if (selector.Contains('.') || selector.Contains('+'))
        {
            return string.Equals(
                context,
                selector,
                StringComparison.Ordinal);
        }

        return string.Equals(
                context,
                selector,
                StringComparison.Ordinal) ||
            context.EndsWith(
                "." + selector,
                StringComparison.Ordinal) ||
            context.EndsWith(
                "+" + selector,
                StringComparison.Ordinal);
    }

    private static bool IsSafeMigrationId(string? value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > 256)
            return false;

        foreach (char character in value)
        {
            if (character is not (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not (>= '0' and <= '9') and not '_' and not '-' and
                not '.')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsCanonicalSha256(string? value)
    {
        if (value is null || value.Length != 64)
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

    private static bool IsDigestPresentExactlyForCommands(
        string? digest,
        int commandCount) =>
        commandCount == 0
            ? digest is null
            : IsCanonicalSha256(digest);

    private static bool IsAnalysisStatus(
        MigrationCompatibilityStatus status) =>
        status is MigrationCompatibilityStatus.Conditional or
            MigrationCompatibilityStatus.Unsupported or
            MigrationCompatibilityStatus.Unknown;

    private static void UpdateAggregate(
        MigrationCompatibilityStatus candidateStatus,
        string candidateRule,
        ref MigrationCompatibilityStatus aggregateStatus,
        ref string aggregateRule)
    {
        if (StatusRank(candidateStatus) >
            StatusRank(aggregateStatus))
        {
            aggregateStatus = candidateStatus;
            aggregateRule = candidateRule;
        }
    }

    private static int StatusRank(
        MigrationCompatibilityStatus status) =>
        status switch
        {
            MigrationCompatibilityStatus.Conditional => 0,
            MigrationCompatibilityStatus.Unsupported => 1,
            MigrationCompatibilityStatus.Unknown => 2,
            _ => -1,
        };

    private static bool IsRuleStatusValid(
        string? ruleId,
        MigrationCompatibilityStatus status)
    {
        if (ruleId is null)
            return false;

        return ruleId switch
        {
            EfCoreMigrationAnalysisRules.GenerationBound or
            EfCoreMigrationAnalysisRules.RawSqlBound or
            EfCoreMigrationAnalysisRules.EmptyMigration or
            EfCoreMigrationAnalysisRules.EmptyDownMigration =>
                status == MigrationCompatibilityStatus.Conditional,
            EfCoreMigrationAnalysisRules.SchemaUnsupported or
            EfCoreMigrationAnalysisRules.SequenceUnsupported or
            EfCoreMigrationAnalysisRules.RawSqlUnsupported or
            EfCoreMigrationAnalysisRules.GenerationUnsupported or
            EfCoreMigrationAnalysisRules.TransactionSuppressed =>
                status == MigrationCompatibilityStatus.Unsupported,
            EfCoreMigrationAnalysisRules.DataUnknown or
            EfCoreMigrationAnalysisRules.OperationUnknown or
            EfCoreMigrationAnalysisRules.RawSqlUnknown or
            EfCoreMigrationAnalysisRules.GenerationFailed or
            EfCoreMigrationAnalysisRules.AnalysisLimit =>
                status == MigrationCompatibilityStatus.Unknown,
            _ => false,
        };
    }

    private static bool IsRuleKindValid(
        string ruleId,
        EfCoreMigrationOperationKind kind) =>
        ruleId switch
        {
            EfCoreMigrationAnalysisRules.GenerationBound =>
                IsStructuralGeneratorKind(kind),
            EfCoreMigrationAnalysisRules.SchemaUnsupported =>
                kind is EfCoreMigrationOperationKind.EnsureSchema or
                    EfCoreMigrationOperationKind.DropSchema,
            EfCoreMigrationAnalysisRules.SequenceUnsupported =>
                kind is EfCoreMigrationOperationKind.CreateSequence or
                    EfCoreMigrationOperationKind.AlterSequence or
                    EfCoreMigrationOperationKind.RenameSequence or
                    EfCoreMigrationOperationKind.DropSequence or
                    EfCoreMigrationOperationKind.RestartSequence,
            EfCoreMigrationAnalysisRules.DataUnknown =>
                kind is EfCoreMigrationOperationKind.InsertData or
                    EfCoreMigrationOperationKind.UpdateData or
                    EfCoreMigrationOperationKind.DeleteData,
            EfCoreMigrationAnalysisRules.OperationUnknown =>
                kind is EfCoreMigrationOperationKind.AlterDatabase or
                    EfCoreMigrationOperationKind.AlterTable or
                    EfCoreMigrationOperationKind.Unknown,
            EfCoreMigrationAnalysisRules.RawSqlBound or
            EfCoreMigrationAnalysisRules.RawSqlUnsupported or
            EfCoreMigrationAnalysisRules.RawSqlUnknown =>
                kind == EfCoreMigrationOperationKind.RawSql,
            EfCoreMigrationAnalysisRules.GenerationUnsupported or
            EfCoreMigrationAnalysisRules.GenerationFailed or
            EfCoreMigrationAnalysisRules.TransactionSuppressed =>
                IsStructuralGeneratorKind(kind) ||
                kind == EfCoreMigrationOperationKind.RawSql,
            EfCoreMigrationAnalysisRules.EmptyMigration or
            EfCoreMigrationAnalysisRules.EmptyDownMigration or
            EfCoreMigrationAnalysisRules.AnalysisLimit => false,
            _ => false,
        };

    private static bool IsStructuralGeneratorKind(
        EfCoreMigrationOperationKind kind) =>
        kind is EfCoreMigrationOperationKind.CreateTable or
            EfCoreMigrationOperationKind.DropTable or
            EfCoreMigrationOperationKind.RenameTable or
            EfCoreMigrationOperationKind.AddColumn or
            EfCoreMigrationOperationKind.AlterColumn or
            EfCoreMigrationOperationKind.DropColumn or
            EfCoreMigrationOperationKind.RenameColumn or
            EfCoreMigrationOperationKind.CreateIndex or
            EfCoreMigrationOperationKind.DropIndex or
            EfCoreMigrationOperationKind.RenameIndex or
            EfCoreMigrationOperationKind.AddPrimaryKey or
            EfCoreMigrationOperationKind.DropPrimaryKey or
            EfCoreMigrationOperationKind.AddUniqueConstraint or
            EfCoreMigrationOperationKind.DropUniqueConstraint or
            EfCoreMigrationOperationKind.AddForeignKey or
            EfCoreMigrationOperationKind.DropForeignKey or
            EfCoreMigrationOperationKind.AddCheckConstraint or
            EfCoreMigrationOperationKind.DropCheckConstraint;

    private static MigrationDiagnosticSeverity ExpectedSeverity(
        MigrationCompatibilityStatus status) =>
        status == MigrationCompatibilityStatus.Conditional
            ? MigrationDiagnosticSeverity.Warning
            : MigrationDiagnosticSeverity.Error;

    private static bool TryGetFixedText(
        string ruleId,
        out FixedDiagnosticText text)
    {
        text = ruleId switch
        {
            EfCoreMigrationAnalysisRules.GenerationBound =>
                new FixedDiagnosticText(
                    "Migration SQL generation succeeded, but the chain was not executed.",
                    "Validate every migration prefix in an isolated scratch database before production use."),
            EfCoreMigrationAnalysisRules.SchemaUnsupported =>
                new FixedDiagnosticText(
                    "The migration contains a schema operation.",
                    "Remove schema usage before targeting CSharpDB."),
            EfCoreMigrationAnalysisRules.SequenceUnsupported =>
                new FixedDiagnosticText(
                    "The migration contains a sequence operation.",
                    "Replace sequence-backed value generation before targeting CSharpDB."),
            EfCoreMigrationAnalysisRules.DataUnknown =>
                new FixedDiagnosticText(
                    "The migration contains a data operation that was not proven.",
                    "Review and migrate the affected data separately."),
            EfCoreMigrationAnalysisRules.OperationUnknown =>
                new FixedDiagnosticText(
                    "The migration contains an operation type that was not recognized.",
                    "Replace the operation with the bounded CSharpDB schema subset."),
            EfCoreMigrationAnalysisRules.RawSqlBound =>
                new FixedDiagnosticText(
                    "Raw SQL passed bounded DDL analysis and SQL generation, but was not chain-executed.",
                    "Validate every migration prefix in an isolated scratch database before production use."),
            EfCoreMigrationAnalysisRules.RawSqlUnsupported =>
                new FixedDiagnosticText(
                    "Raw SQL contains DDL that is unsupported by CSharpDB.",
                    "Replace the raw SQL with supported migration operations."),
            EfCoreMigrationAnalysisRules.RawSqlUnknown =>
                new FixedDiagnosticText(
                    "Raw SQL could not be proven by the bounded DDL analyzer.",
                    "Replace the raw SQL with supported migration operations."),
            EfCoreMigrationAnalysisRules.GenerationUnsupported =>
                new FixedDiagnosticText(
                    "The CSharpDB SQL generator rejected the migration operation.",
                    "Rewrite the operation using the supported CSharpDB migration subset."),
            EfCoreMigrationAnalysisRules.GenerationFailed =>
                new FixedDiagnosticText(
                    "Migration SQL generation could not be completed.",
                    "Review the compiled migration and provider configuration."),
            EfCoreMigrationAnalysisRules.TransactionSuppressed =>
                new FixedDiagnosticText(
                    "The generated command suppresses the migration transaction.",
                    "Rewrite the operation so every generated command remains transactional."),
            EfCoreMigrationAnalysisRules.AnalysisLimit =>
                new FixedDiagnosticText(
                    "The migration analysis exceeded a fixed safety limit.",
                    "Reduce the compiled migration input before retrying."),
            EfCoreMigrationAnalysisRules.EmptyMigration =>
                new FixedDiagnosticText(
                    "The compiled migration contains no Up or Down operations.",
                    "Review whether the empty migration should remain in the chain."),
            EfCoreMigrationAnalysisRules.EmptyDownMigration =>
                new FixedDiagnosticText(
                    "The compiled migration contains no Down operations.",
                    "Add a bounded rollback path or document the irreversible migration."),
            _ => default,
        };
        return text.Summary is not null;
    }

    private readonly record struct FixedDiagnosticText(
        string Summary,
        string Remediation);

    private readonly record struct ExpectedDiagnostic(
        string RuleId,
        MigrationCompatibilityStatus Status,
        int? MigrationOrdinal,
        int? OperationOrdinal);
}

internal static class EfCoreScratchReportSanitizer
{
    internal const int MaxScratchMigrations = 128;
    private const int MaxScratchCommands =
        EfCoreMigrationAnalyzer.MaxCommands * 4;

    internal static bool TrySanitize(
        EfCoreMigrationScratchAnalysisReport report,
        string expectedAssemblyDigest,
        string expectedContextSelector,
        out EfCoreMigrationScratchAnalysisReport? sanitized)
    {
        sanitized = null;
        ArgumentNullException.ThrowIfNull(report);

        if (!EfCoreReportSanitizer.TrySanitize(
                report.GenerationPreflight,
                expectedAssemblyDigest,
                expectedContextSelector,
                out EfCoreMigrationAnalysisReport? generation) ||
            generation is null ||
            report.ScratchChain is null ||
            report.Diagnostics is null ||
            report.Diagnostics.Count != 0)
        {
            return false;
        }

        EfCoreMigrationScratchChainProof proof =
            report.ScratchChain;
        if (!string.Equals(
                report.Format,
                EfCoreMigrationScratchAnalysisReport.CurrentFormat,
                StringComparison.Ordinal) ||
            !string.Equals(
                proof.Format,
                EfCoreMigrationScratchChainProof.CurrentFormat,
                StringComparison.Ordinal) ||
            !string.Equals(
                proof.Algorithm,
                EfCoreMigrationScratchChainProof.EmptyChainAlgorithm,
                StringComparison.Ordinal) ||
            proof.ProofScope !=
                EfCoreMigrationScratchProofScope.EmptyDatabase ||
            proof.DataPreflightCompleted ||
            report.Outcome != proof.Outcome ||
            proof.PrefixCount != generation.MigrationCount ||
            proof.PrefixCount is < 0 or >
                EfCoreMigrationAnalyzer.MaxMigrations ||
            proof.AppliedPrefixCount is < 0 ||
            proof.AppliedPrefixCount > proof.PrefixCount ||
            proof.SchemaVerifiedPrefixCount is < 0 ||
            proof.SchemaVerifiedPrefixCount >
                proof.AppliedPrefixCount ||
            proof.DownPrefixCount is < 0 ||
            proof.DownPrefixCount > proof.AppliedPrefixCount ||
            proof.ReappliedPrefixCount is < 0 ||
            proof.ReappliedPrefixCount > proof.DownPrefixCount ||
            proof.RoundTripVerifiedPrefixCount is < 0 ||
            proof.RoundTripVerifiedPrefixCount >
                proof.ReappliedPrefixCount ||
            proof.IdempotentApplyCount is < 0 or > 2 ||
            proof.ExecutedCommandCount is < 0 or >
                MaxScratchCommands ||
            proof.IdempotentCommandCount is < 0 or >
                MaxScratchCommands ||
            proof.Prefixes is null ||
            proof.Prefixes.Count >
                proof.SchemaVerifiedPrefixCount ||
            !proof.ResourcesDisposed ||
            !IsDigestPresentExactlyForCount(
                proof.ExecutedSqlDigest,
                proof.ExecutedCommandCount) ||
            !IsDigestPresentExactlyForCount(
                proof.IdempotentSqlDigest,
                proof.IdempotentCommandCount))
        {
            return false;
        }

        return proof.Outcome switch
        {
            EfCoreMigrationScratchAnalysisOutcome.Passed =>
                TrySanitizePassed(
                    report,
                    generation,
                    proof,
                    out sanitized),
            EfCoreMigrationScratchAnalysisOutcome.Blocked =>
                TrySanitizeBlocked(
                    report,
                    generation,
                    proof,
                    out sanitized),
            EfCoreMigrationScratchAnalysisOutcome.Failed =>
                TrySanitizeFailed(
                    report,
                    generation,
                    proof,
                    out sanitized),
            _ => false,
        };
    }

    private static bool TrySanitizePassed(
        EfCoreMigrationScratchAnalysisReport report,
        EfCoreMigrationAnalysisReport generation,
        EfCoreMigrationScratchChainProof proof,
        out EfCoreMigrationScratchAnalysisReport? sanitized)
    {
        sanitized = null;
        if (proof.PrefixCount is <= 0 or > MaxScratchMigrations ||
            report.Status != MigrationCompatibilityStatus.Compatible ||
            report.HighestEvidence !=
                MigrationEvidenceLevel.ScratchExecuted ||
            !string.Equals(
                report.RuleId,
                EfCoreMigrationScratchAnalysisRules.ScratchPassed,
                StringComparison.Ordinal) ||
            generation.Status !=
                MigrationCompatibilityStatus.Conditional ||
            proof.AppliedPrefixCount != proof.PrefixCount ||
            proof.SchemaVerifiedPrefixCount != proof.PrefixCount ||
            proof.DownPrefixCount != proof.PrefixCount ||
            proof.ReappliedPrefixCount != proof.PrefixCount ||
            proof.RoundTripVerifiedPrefixCount != proof.PrefixCount ||
            proof.IdempotentApplyCount != 2 ||
            proof.ExecutedCommandCount <= 0 ||
            proof.IdempotentCommandCount <= 0 ||
            proof.Prefixes.Count != proof.PrefixCount ||
            !AllIdempotentDigestsPresent(proof))
        {
            return false;
        }

        if (!TryCopyVerifiedPrefixes(
                generation,
                proof,
                out IReadOnlyList<
                    EfCoreMigrationScratchPrefixEvidence> prefixes))
        {
            return false;
        }

        EfCoreMigrationScratchPrefixEvidence last =
            prefixes[^1];
        if (!string.Equals(
                proof.FirstIdempotentSchemaDigest,
                last.ExpectedSchemaDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                proof.SecondIdempotentSchemaDigest,
                last.ExpectedSchemaDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                proof.FirstIdempotentHistoryDigest,
                last.ExpectedHistoryDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                proof.SecondIdempotentHistoryDigest,
                last.ExpectedHistoryDigest,
                StringComparison.Ordinal))
        {
            return false;
        }

        sanitized = Rebuild(
            report,
            generation,
            proof,
            prefixes);
        return true;
    }

    private static bool TrySanitizeBlocked(
        EfCoreMigrationScratchAnalysisReport report,
        EfCoreMigrationAnalysisReport generation,
        EfCoreMigrationScratchChainProof proof,
        out EfCoreMigrationScratchAnalysisReport? sanitized)
    {
        sanitized = null;
        if (report.Status != generation.Status ||
            report.HighestEvidence != MigrationEvidenceLevel.Bound ||
            !string.Equals(
                report.RuleId,
                EfCoreMigrationScratchAnalysisRules
                    .GenerationPreflightBlocked,
                StringComparison.Ordinal) ||
            proof.AppliedPrefixCount != 0 ||
            proof.SchemaVerifiedPrefixCount != 0 ||
            proof.DownPrefixCount != 0 ||
            proof.ReappliedPrefixCount != 0 ||
            proof.RoundTripVerifiedPrefixCount != 0 ||
            proof.IdempotentApplyCount != 0 ||
            proof.ExecutedCommandCount != 0 ||
            proof.IdempotentCommandCount != 0 ||
            proof.ExecutedSqlDigest is not null ||
            proof.IdempotentSqlDigest is not null ||
            proof.Prefixes.Count != 0 ||
            AnyIdempotentDigestPresent(proof))
        {
            return false;
        }

        sanitized = Rebuild(
            report,
            generation,
            proof,
            []);
        return true;
    }

    private static bool TrySanitizeFailed(
        EfCoreMigrationScratchAnalysisReport report,
        EfCoreMigrationAnalysisReport generation,
        EfCoreMigrationScratchChainProof proof,
        out EfCoreMigrationScratchAnalysisReport? sanitized)
    {
        sanitized = null;
        if (report.Status != MigrationCompatibilityStatus.Unknown ||
            report.HighestEvidence !=
                (proof.ExecutedCommandCount == 0
                    ? MigrationEvidenceLevel.Bound
                    : MigrationEvidenceLevel.ScratchExecuted) ||
            !IsFailureRule(report.RuleId) ||
            generation.Status !=
                MigrationCompatibilityStatus.Conditional ||
            proof.Prefixes.Count !=
                proof.RoundTripVerifiedPrefixCount ||
            !AreOptionalIdempotentDigestsCoherent(proof))
        {
            return false;
        }

        if (!IsFailureStageCoherent(report.RuleId, proof) ||
            !TryCopyVerifiedPrefixes(
                generation,
                proof,
                out IReadOnlyList<
                    EfCoreMigrationScratchPrefixEvidence> prefixes))
        {
            return false;
        }

        sanitized = Rebuild(
            report,
            generation,
            proof,
            prefixes);
        return true;
    }

    private static bool TryCopyVerifiedPrefixes(
        EfCoreMigrationAnalysisReport generation,
        EfCoreMigrationScratchChainProof proof,
        out IReadOnlyList<
            EfCoreMigrationScratchPrefixEvidence> sanitized)
    {
        sanitized = [];
        IReadOnlyList<string> migrationIds =
            generation.Migrations
                .Select(static migration => migration.MigrationId)
                .ToArray();
        string emptySchemaDigest =
            MigrationNormalizedSchemaContract.Create([]).Digest;
        var prefixes =
            new List<EfCoreMigrationScratchPrefixEvidence>(
                proof.Prefixes.Count);
        for (int index = 0;
             index < proof.Prefixes.Count;
             index++)
        {
            EfCoreMigrationScratchPrefixEvidence? prefix =
                proof.Prefixes[index];
            string expectedHistory =
                EfCoreScratchEvidenceDigest.History(
                    migrationIds,
                    index + 1);
            string expectedDownHistory =
                EfCoreScratchEvidenceDigest.History(
                    migrationIds,
                    index);
            string expectedDownSchema = index == 0
                ? emptySchemaDigest
                : proof.Prefixes[index - 1]
                    .ExpectedSchemaDigest;
            if (prefix is null ||
                prefix.Ordinal != index ||
                prefix.MigrationOrdinal != index ||
                prefix.Status !=
                    MigrationCompatibilityStatus.Compatible ||
                prefix.Evidence !=
                    MigrationEvidenceLevel.ScratchExecuted ||
                !string.Equals(
                    prefix.RuleId,
                    EfCoreMigrationScratchAnalysisRules
                        .ScratchPassed,
                    StringComparison.Ordinal) ||
                !IsCanonicalSha256(prefix.ExpectedSchemaDigest) ||
                !string.Equals(
                    prefix.ExpectedHistoryDigest,
                    expectedHistory,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.AppliedSchemaDigest,
                    prefix.ExpectedSchemaDigest,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.AppliedHistoryDigest,
                    expectedHistory,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.DownSchemaDigest,
                    expectedDownSchema,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.DownHistoryDigest,
                    expectedDownHistory,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.ReappliedSchemaDigest,
                    prefix.ExpectedSchemaDigest,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    prefix.ReappliedHistoryDigest,
                    expectedHistory,
                    StringComparison.Ordinal))
            {
                return false;
            }
            prefixes.Add(CopyPrefix(prefix));
        }

        sanitized = prefixes;
        return true;
    }

    private static bool IsFailureStageCoherent(
        string ruleId,
        EfCoreMigrationScratchChainProof proof)
    {
        if (proof.IdempotentApplyCount == 0 &&
            proof.IdempotentCommandCount != 0)
        {
            return false;
        }
        if (proof.IdempotentApplyCount > 0 &&
            proof.IdempotentCommandCount == 0)
        {
            return false;
        }

        return ruleId switch
        {
            EfCoreMigrationScratchAnalysisRules.SchemaDifferent =>
                (proof.AppliedPrefixCount == 0 &&
                 proof.SchemaVerifiedPrefixCount == 0 &&
                 proof.DownPrefixCount == 0 &&
                 proof.ReappliedPrefixCount == 0) ||
                (proof.AppliedPrefixCount ==
                     proof.SchemaVerifiedPrefixCount + 1 &&
                 proof.DownPrefixCount ==
                     proof.RoundTripVerifiedPrefixCount &&
                 proof.ReappliedPrefixCount ==
                     proof.RoundTripVerifiedPrefixCount),
            EfCoreMigrationScratchAnalysisRules
                .RoundTripDifferent =>
                proof.SchemaVerifiedPrefixCount ==
                    proof.AppliedPrefixCount &&
                proof.DownPrefixCount ==
                    proof.RoundTripVerifiedPrefixCount + 1 &&
                (proof.ReappliedPrefixCount ==
                    proof.RoundTripVerifiedPrefixCount ||
                 proof.ReappliedPrefixCount ==
                    proof.RoundTripVerifiedPrefixCount + 1),
            EfCoreMigrationScratchAnalysisRules
                .IdempotenceFailed =>
                proof.AppliedPrefixCount == proof.PrefixCount &&
                proof.SchemaVerifiedPrefixCount ==
                    proof.PrefixCount &&
                proof.DownPrefixCount == proof.PrefixCount &&
                proof.ReappliedPrefixCount ==
                    proof.PrefixCount &&
                proof.RoundTripVerifiedPrefixCount ==
                    proof.PrefixCount,
            EfCoreMigrationScratchAnalysisRules
                .ScratchExecutionFailed or
            EfCoreMigrationScratchAnalysisRules.AnalysisLimit =>
                true,
            _ => false,
        };
    }

    private static EfCoreMigrationScratchAnalysisReport Rebuild(
        EfCoreMigrationScratchAnalysisReport report,
        EfCoreMigrationAnalysisReport generation,
        EfCoreMigrationScratchChainProof proof,
        IReadOnlyList<EfCoreMigrationScratchPrefixEvidence> prefixes) =>
        new()
        {
            Outcome = proof.Outcome,
            Status = report.Status,
            HighestEvidence = report.HighestEvidence,
            RuleId = report.RuleId,
            GenerationPreflight = generation,
            ScratchChain = new EfCoreMigrationScratchChainProof
            {
                Outcome = proof.Outcome,
                PrefixCount = proof.PrefixCount,
                AppliedPrefixCount = proof.AppliedPrefixCount,
                SchemaVerifiedPrefixCount =
                    proof.SchemaVerifiedPrefixCount,
                DownPrefixCount = proof.DownPrefixCount,
                ReappliedPrefixCount =
                    proof.ReappliedPrefixCount,
                RoundTripVerifiedPrefixCount =
                    proof.RoundTripVerifiedPrefixCount,
                IdempotentApplyCount =
                    proof.IdempotentApplyCount,
                ExecutedCommandCount =
                    proof.ExecutedCommandCount,
                IdempotentCommandCount =
                    proof.IdempotentCommandCount,
                ExecutedSqlDigest = proof.ExecutedSqlDigest,
                IdempotentSqlDigest = proof.IdempotentSqlDigest,
                FirstIdempotentSchemaDigest =
                    proof.FirstIdempotentSchemaDigest,
                FirstIdempotentHistoryDigest =
                    proof.FirstIdempotentHistoryDigest,
                SecondIdempotentSchemaDigest =
                    proof.SecondIdempotentSchemaDigest,
                SecondIdempotentHistoryDigest =
                    proof.SecondIdempotentHistoryDigest,
                ResourcesDisposed = true,
                Prefixes = prefixes,
            },
            Diagnostics = [],
        };

    private static EfCoreMigrationScratchPrefixEvidence CopyPrefix(
        EfCoreMigrationScratchPrefixEvidence prefix) =>
        new()
        {
            Ordinal = prefix.Ordinal,
            MigrationOrdinal = prefix.MigrationOrdinal,
            Status = prefix.Status,
            Evidence = prefix.Evidence,
            RuleId = prefix.RuleId,
            ExpectedSchemaDigest = prefix.ExpectedSchemaDigest,
            ExpectedHistoryDigest = prefix.ExpectedHistoryDigest,
            AppliedSchemaDigest = prefix.AppliedSchemaDigest,
            AppliedHistoryDigest = prefix.AppliedHistoryDigest,
            DownSchemaDigest = prefix.DownSchemaDigest,
            DownHistoryDigest = prefix.DownHistoryDigest,
            ReappliedSchemaDigest =
                prefix.ReappliedSchemaDigest,
            ReappliedHistoryDigest =
                prefix.ReappliedHistoryDigest,
        };

    private static bool IsFailureRule(string? ruleId) =>
        ruleId is
            EfCoreMigrationScratchAnalysisRules
                .ScratchExecutionFailed or
            EfCoreMigrationScratchAnalysisRules.SchemaDifferent or
            EfCoreMigrationScratchAnalysisRules
                .RoundTripDifferent or
            EfCoreMigrationScratchAnalysisRules
                .IdempotenceFailed or
            EfCoreMigrationScratchAnalysisRules.AnalysisLimit;

    private static bool AllIdempotentDigestsPresent(
        EfCoreMigrationScratchChainProof proof) =>
        IsCanonicalSha256(proof.FirstIdempotentSchemaDigest) &&
        IsCanonicalSha256(proof.FirstIdempotentHistoryDigest) &&
        IsCanonicalSha256(proof.SecondIdempotentSchemaDigest) &&
        IsCanonicalSha256(proof.SecondIdempotentHistoryDigest);

    private static bool AnyIdempotentDigestPresent(
        EfCoreMigrationScratchChainProof proof) =>
        proof.FirstIdempotentSchemaDigest is not null ||
        proof.FirstIdempotentHistoryDigest is not null ||
        proof.SecondIdempotentSchemaDigest is not null ||
        proof.SecondIdempotentHistoryDigest is not null;

    private static bool AreOptionalIdempotentDigestsCoherent(
        EfCoreMigrationScratchChainProof proof)
    {
        bool firstSchema =
            proof.FirstIdempotentSchemaDigest is not null;
        bool firstHistory =
            proof.FirstIdempotentHistoryDigest is not null;
        bool secondSchema =
            proof.SecondIdempotentSchemaDigest is not null;
        bool secondHistory =
            proof.SecondIdempotentHistoryDigest is not null;
        if (firstSchema != firstHistory ||
            secondSchema != secondHistory ||
            firstSchema &&
                (!IsCanonicalSha256(
                    proof.FirstIdempotentSchemaDigest) ||
                 !IsCanonicalSha256(
                    proof.FirstIdempotentHistoryDigest)) ||
            secondSchema &&
                (!IsCanonicalSha256(
                    proof.SecondIdempotentSchemaDigest) ||
                 !IsCanonicalSha256(
                    proof.SecondIdempotentHistoryDigest)) ||
            firstSchema && proof.IdempotentApplyCount < 1 ||
            secondSchema &&
                (proof.IdempotentApplyCount < 2 || !firstSchema) ||
            proof.IdempotentApplyCount == 0 &&
                AnyIdempotentDigestPresent(proof))
        {
            return false;
        }

        return true;
    }

    private static bool IsDigestPresentExactlyForCount(
        string? digest,
        int count) =>
        count == 0
            ? digest is null
            : IsCanonicalSha256(digest);

    private static bool IsCanonicalSha256(string? value)
    {
        if (value is null || value.Length != 64)
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
}
