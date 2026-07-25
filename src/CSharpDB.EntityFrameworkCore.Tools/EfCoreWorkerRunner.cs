using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal enum EfCoreWorkerErrorCode
{
    Incompatible,
    InputLimitExceeded,
    AssemblyUnavailable,
    AssemblyDigestMismatch,
    ContextUnavailable,
    AnalysisFailed,
    OutputLimitExceeded,
    InternalFailure,
}

internal sealed record EfCoreWorkerErrorEnvelope
{
    internal const string CurrentFormat =
        "csharpdb-ef-worker-error/v1";

    public string Format { get; init; } = CurrentFormat;

    public string Protocol { get; init; } =
        EfCoreWorkerRunner.Protocol;

    public required EfCoreWorkerErrorCode Code { get; init; }
}

internal sealed record EfCoreWorkerDependencies
{
    internal static EfCoreWorkerDependencies Default { get; } =
        new();

    internal Func<
        EfCoreMigrationAnalysisRequest,
        CancellationToken,
        ValueTask<EfCoreMigrationAnalysisReport>> AnalyzeAsync
    {
        get;
        init;
    } = EfCoreMigrationAnalyzer.AnalyzeAsync;

    internal Func<EfCoreMigrationAnalysisReport, string>
        SerializeReport
    {
        get;
        init;
    } = static report => JsonSerializer.Serialize(
        report,
        EfCoreWorkerRunner.JsonOptions);

    internal Func<EfCoreWorkerErrorEnvelope, string>
        SerializeError
    {
        get;
        init;
    } = static envelope => JsonSerializer.Serialize(
        envelope,
        EfCoreWorkerRunner.JsonOptions);
}

internal static class EfCoreWorkerRunner
{
    internal const string Protocol =
        "csharpdb-ef-worker/v1";
    internal const string SuccessHeader = Protocol + "\n";
    internal const string RequestFormat =
        EfCoreMigrationAnalysisRequest.CurrentFormat;
    internal const int ExitSuccess = 0;
    internal const int ExitIncompatible = 10;
    internal const int ExitInputLimit = 12;
    internal const int ExitAssemblyUnavailable = 12;
    internal const int ExitAssemblyDigestMismatch = 12;
    internal const int ExitContextUnavailable = 12;
    internal const int ExitAnalysisFailed = 12;
    internal const int ExitOutputLimit = 13;
    internal const int ExitInternalFailure = 13;
    internal const int MaxInputBytes = 64 * 1024;
    // The protocol hard ceiling is 16 MiB. The v1 host accepts a stricter
    // 8 MiB report, so the worker applies that bound before writing.
    internal const int MaxOutputBytes = 8 * 1024 * 1024;

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static JsonSerializerOptions JsonOptions { get; } =
        CreateJsonOptions();

    internal static async ValueTask<int> RunAsync(
        string[] args,
        Stream input,
        TextWriter output,
        TextWriter error,
        EfCoreWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);

        if (!TryParseInvocation(args))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                EfCoreWorkerErrorCode.Incompatible);
        }
        if (dependencies.AnalyzeAsync is null ||
            dependencies.SerializeReport is null ||
            dependencies.SerializeError is null)
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                EfCoreWorkerErrorCode.InternalFailure);
        }

        WorkerRequest request;
        try
        {
            byte[] payload = await ReadInputAsync(
                input,
                cancellationToken);
            request = ParseRequest(payload);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitAnalysisFailed,
                EfCoreWorkerErrorCode.AnalysisFailed);
        }
        catch (WorkerInputLimitException)
        {
            return await FailAsync(
                error,
                ExitInputLimit,
                EfCoreWorkerErrorCode.InputLimitExceeded);
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer
                .IsRecoverable(exception))
        {
            return await FailAsync(
                error,
                ExitIncompatible,
                EfCoreWorkerErrorCode.Incompatible);
        }

        EfCoreMigrationAnalysisReport report;
        try
        {
            report = await dependencies.AnalyzeAsync(
                new EfCoreMigrationAnalysisRequest
                {
                    AssemblyPath = request.AssemblyPath,
                    AssemblyDigest =
                        request.AssemblyDigest,
                    Context = request.Context,
                },
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return await FailAsync(
                error,
                ExitAnalysisFailed,
                EfCoreWorkerErrorCode.AnalysisFailed);
        }
        catch (EfCoreAnalysisException exception)
        {
            (int exitCode, EfCoreWorkerErrorCode code) =
                MapFailure(exception.Kind);
            return await FailAsync(
                error,
                exitCode,
                code);
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer
                .IsRecoverable(exception))
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                EfCoreWorkerErrorCode.InternalFailure);
        }

        string serialized;
        try
        {
            serialized =
                dependencies.SerializeReport(report);
            if (string.IsNullOrEmpty(serialized) ||
                StrictUtf8.GetByteCount(SuccessHeader) >
                    MaxOutputBytes -
                    StrictUtf8.GetByteCount(serialized))
            {
                return await FailAsync(
                    error,
                    ExitOutputLimit,
                    EfCoreWorkerErrorCode
                        .OutputLimitExceeded);
            }
        }
        catch (EncoderFallbackException)
        {
            return await FailAsync(
                error,
                ExitOutputLimit,
                EfCoreWorkerErrorCode.OutputLimitExceeded);
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer
                .IsRecoverable(exception))
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                EfCoreWorkerErrorCode.InternalFailure);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.WriteAsync(
                SuccessHeader.AsMemory(),
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
                ExitAnalysisFailed,
                EfCoreWorkerErrorCode.AnalysisFailed);
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer
                .IsRecoverable(exception))
        {
            return await FailAsync(
                error,
                ExitInternalFailure,
                EfCoreWorkerErrorCode.InternalFailure);
        }
    }

    private static bool TryParseInvocation(
        IReadOnlyList<string> args) =>
        args.Count == 5 &&
        string.Equals(
            args[0],
            "--worker",
            StringComparison.Ordinal) &&
        string.Equals(
            args[1],
            "--protocol",
            StringComparison.Ordinal) &&
        string.Equals(
            args[2],
            Protocol,
            StringComparison.Ordinal) &&
        string.Equals(
            args[3],
            "--target-version",
            StringComparison.Ordinal) &&
        string.Equals(
            args[4],
            CSharpDbCapabilityCatalogLoader
                .CurrentTargetVersion,
            StringComparison.Ordinal);

    private static async ValueTask<byte[]> ReadInputAsync(
        Stream input,
        CancellationToken cancellationToken)
    {
        byte[] readBuffer =
            ArrayPool<byte>.Shared.Rent(16 * 1024);
        try
        {
            using var payload = new MemoryStream(
                capacity: MaxInputBytes);
            while (true)
            {
                int read = await input.ReadAsync(
                    readBuffer.AsMemory(
                        0,
                        readBuffer.Length),
                    cancellationToken);
                if (read == 0)
                    break;
                if (payload.Length >
                    MaxInputBytes - read)
                {
                    throw new WorkerInputLimitException();
                }
                payload.Write(readBuffer, 0, read);
            }

            if (payload.Length == 0)
                throw new JsonException();
            return payload.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                readBuffer,
                clearArray: true);
        }
    }

    private static WorkerRequest ParseRequest(
        byte[] payload)
    {
        ReadOnlySpan<byte> utf8Bom =
            [0xEF, 0xBB, 0xBF];
        if (payload.AsSpan().StartsWith(utf8Bom))
        {
            throw new JsonException();
        }
        _ = StrictUtf8.GetCharCount(payload);

        using JsonDocument document = JsonDocument.Parse(
            payload,
            new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling =
                    JsonCommentHandling.Disallow,
                MaxDepth = 8,
            });
        if (document.RootElement.ValueKind !=
            JsonValueKind.Object)
        {
            throw new JsonException();
        }

        var seen = new HashSet<string>(
            StringComparer.Ordinal);
        foreach (JsonProperty property in
            document.RootElement.EnumerateObject())
        {
            if (!seen.Add(property.Name) ||
                property.Name is not (
                    "format" or
                    "assemblyPath" or
                    "assemblyDigest" or
                    "context"))
            {
                throw new JsonException();
            }
        }
        if (seen.Count != 4)
            throw new JsonException();

        WorkerRequest? request =
            document.RootElement.Deserialize<WorkerRequest>(
                JsonOptions);
        if (request is null ||
            !string.Equals(
                request.Format,
                RequestFormat,
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(
                request.AssemblyPath) ||
            string.IsNullOrWhiteSpace(
                request.AssemblyDigest))
        {
            throw new JsonException();
        }
        return request;
    }

    private static (int ExitCode, EfCoreWorkerErrorCode Code)
        MapFailure(EfCoreAnalysisFailureKind kind) =>
        kind switch
        {
            EfCoreAnalysisFailureKind.InvalidRequest =>
                (ExitIncompatible,
                    EfCoreWorkerErrorCode.Incompatible),
            EfCoreAnalysisFailureKind.AssemblyUnavailable =>
                (ExitAssemblyUnavailable,
                    EfCoreWorkerErrorCode
                        .AssemblyUnavailable),
            EfCoreAnalysisFailureKind
                .AssemblyDigestMismatch =>
                (ExitAssemblyDigestMismatch,
                    EfCoreWorkerErrorCode
                        .AssemblyDigestMismatch),
            EfCoreAnalysisFailureKind.AnalysisLimit =>
                (ExitInputLimit,
                    EfCoreWorkerErrorCode
                        .InputLimitExceeded),
            EfCoreAnalysisFailureKind.ContextUnavailable =>
                (ExitContextUnavailable,
                    EfCoreWorkerErrorCode
                        .ContextUnavailable),
            EfCoreAnalysisFailureKind.AnalysisFailed =>
                (ExitAnalysisFailed,
                    EfCoreWorkerErrorCode.AnalysisFailed),
            _ => (ExitInternalFailure,
                EfCoreWorkerErrorCode.InternalFailure),
        };

    private static async ValueTask<int> FailAsync(
        TextWriter error,
        int exitCode,
        EfCoreWorkerErrorCode code)
    {
        try
        {
            string serialized = JsonSerializer.Serialize(
                new EfCoreWorkerErrorEnvelope
                {
                    Code = code,
                },
                JsonOptions);
            if (StrictUtf8.GetByteCount(serialized) <=
                MaxOutputBytes)
            {
                await error.WriteLineAsync(serialized);
                await error.FlushAsync();
            }
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer
                .IsRecoverable(exception))
        {
            // Preserve the fixed exit code even when the diagnostic stream
            // itself is unavailable.
        }
        return exitCode;
    }

    private static JsonSerializerOptions
        CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy =
                JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
            AllowTrailingCommas = false,
            ReadCommentHandling =
                JsonCommentHandling.Disallow,
            MaxDepth = 32,
            WriteIndented = false,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    private sealed record WorkerRequest
    {
        public required string Format { get; init; }

        public required string AssemblyPath { get; init; }

        public required string AssemblyDigest { get; init; }

        public string? Context { get; init; }
    }

    private sealed class WorkerInputLimitException
        : Exception;
}
