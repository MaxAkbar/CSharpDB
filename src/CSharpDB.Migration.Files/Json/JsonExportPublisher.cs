using System.Buffers;
using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Complete restart-only request for one manifest-last JSON file publication.
/// The publisher owns private sibling staging and runs the export before any
/// final path becomes visible.
/// </summary>
public sealed record JsonExportPublicationRequest
{
    public required string DestinationPath { get; init; }

    public required string ManifestPath { get; init; }

    public required JsonStreamingExportRequest Export { get; init; }
}

/// <summary>Exact final pair returned by manifest-last publication.</summary>
public sealed record JsonExportPublicationResult
{
    public required string DestinationPath { get; init; }

    public required string ManifestPath { get; init; }

    public required JsonExportManifest Manifest { get; init; }

    public required byte[] CanonicalManifestBytes { get; init; }

    public required string ManifestDigest { get; init; }

    public required bool ReusedData { get; init; }

    public required bool ReusedManifest { get; init; }
}

/// <summary>
/// Completes a restart-only JSON export in private sibling files, durably
/// flushes them as supported, and publishes data before its canonical
/// manifest without overwriting an existing path.
/// </summary>
public sealed class JsonExportPublisher
{
    private const int BufferSize = 64 * 1024;

    private readonly IJsonExportPublicationFaultInjector?
        faultInjector;

    public JsonExportPublisher()
    {
    }

    internal JsonExportPublisher(
        IJsonExportPublicationFaultInjector faultInjector)
    {
        this.faultInjector =
            faultInjector ??
            throw new ArgumentNullException(
                nameof(faultInjector));
    }

    /// <summary>
    /// Validates Windows-local, absolute, normalized, distinct sibling final
    /// paths and safely opens their parent without creating an export file.
    /// </summary>
    public static void ValidatePaths(
        string destinationPath,
        string manifestPath) =>
        JsonExportPublicationFileSystem
            .ValidatePathsForPreflight(
                destinationPath,
                manifestPath);

    public async ValueTask<JsonExportPublicationResult>
        PublishAsync(
        JsonExportPublicationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(
            request.Export);
        cancellationToken
            .ThrowIfCancellationRequested();

        JsonExportPublicationFileSystem?
            fileSystem = null;
        FileStream? dataStaging = null;
        FileStream? manifestStaging = null;
        FileStream? stableData = null;
        FileStream? stableManifest = null;
        bool committedPair = false;
        bool newlyPublishedData = false;
        bool freshDataRollbackRequired = false;
        Exception? bodyFailure = null;
        List<Exception> cleanupFailures = [];
        try
        {
            fileSystem =
                JsonExportPublicationFileSystem.Open(
                    request.DestinationPath,
                    request.ManifestPath);
            cancellationToken
                .ThrowIfCancellationRequested();
            JsonExportPublicationFileSystem
                .PublicationPaths paths =
                fileSystem.Paths;

            dataStaging =
                fileSystem.CreatePrivateStagingFile(
                    paths.DataStagingPath);
            JsonStreamingExportResult export =
                await new JsonStreamingExporter()
                    .WriteAsync(
                        dataStaging,
                        request.Export,
                        cancellationToken)
                    .ConfigureAwait(false);
            await FlushDurablyAsync(
                    dataStaging,
                    cancellationToken)
                .ConfigureAwait(false);

            manifestStaging =
                fileSystem.CreatePrivateStagingFile(
                    paths.ManifestStagingPath);
            await manifestStaging
                .WriteAsync(
                    export.CanonicalManifestBytes,
                    cancellationToken)
                .ConfigureAwait(false);
            await FlushDurablyAsync(
                    manifestStaging,
                    cancellationToken)
                .ConfigureAwait(false);

            await VerifyStagingFilesAsync(
                    dataStaging,
                    manifestStaging,
                    export,
                    cancellationToken)
                .ConfigureAwait(false);
            fileSystem.RequireParentIdentity();

            await using FinalState initial =
                await InspectFinalStateAsync(
                        fileSystem,
                        export,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (initial.Kind ==
                FinalStateKind.ExactPair)
            {
                committedPair = true;
                return CreateResult(
                    paths,
                    export,
                    reusedData: true,
                    reusedManifest: true);
            }

            bool reusedData;
            if (initial.Kind ==
                FinalStateKind.ExactDataOnly)
            {
                stableData =
                    initial.TakeData();
                reusedData = true;
            }
            else
            {
                await InjectFaultAsync(
                        JsonExportPublicationFaultPoint
                            .BeforeDataNamespaceCommit,
                        cancellationToken)
                    .ConfigureAwait(false);
                cancellationToken
                    .ThrowIfCancellationRequested();
                fileSystem.RequireAbsent(
                    paths.ManifestPath);
                await InjectFaultAsync(
                        JsonExportPublicationFaultPoint
                            .AfterManifestAbsenceCheckBeforeDataCommit,
                        cancellationToken)
                    .ConfigureAwait(false);
                cancellationToken
                    .ThrowIfCancellationRequested();

                JsonExportPublicationFileSystem
                    .NoReplaceRenameStatus dataStatus =
                    fileSystem.RenameNoReplace(
                        dataStaging,
                        paths.DestinationPath);
                if (dataStatus ==
                    JsonExportPublicationFileSystem
                        .NoReplaceRenameStatus.Published)
                {
                    stableData =
                        dataStaging;
                    dataStaging = null;
                    reusedData = false;
                    newlyPublishedData = true;
                    await RequireStableExactDataAsync(
                            fileSystem,
                            stableData,
                            export)
                        .ConfigureAwait(false);
                    RejectManifestAfterFreshData(
                        fileSystem,
                        paths.ManifestPath,
                        ref stableData,
                        ref newlyPublishedData,
                        ref freshDataRollbackRequired);
                }
                else
                {
                    RemoveAndDispose(
                        fileSystem,
                        ref dataStaging);
                    stableData =
                        fileSystem.OpenExistingRequired(
                            paths.DestinationPath);
                    await RequireStableExactDataAsync(
                            fileSystem,
                            stableData,
                            export)
                        .ConfigureAwait(false);
                    reusedData = true;
                }

                await InjectFaultAsync(
                        JsonExportPublicationFaultPoint
                            .AfterDataNamespaceCommitBeforeManifest,
                        CancellationToken.None)
                    .ConfigureAwait(false);
                if (newlyPublishedData)
                {
                    RejectManifestAfterFreshData(
                        fileSystem,
                        paths.ManifestPath,
                        ref stableData,
                        ref newlyPublishedData,
                        ref freshDataRollbackRequired);
                }
            }

            // Exact final data is now the recovery authority. Its open handle
            // denies write/delete sharing through the manifest decision, and
            // cancellation is deliberately no longer observed.
            FileStream exactData =
                stableData ??
                throw new InvalidOperationException(
                    "The exact final JSON export data handle is unavailable.");
            await RequireStableExactDataAsync(
                    fileSystem,
                    exactData,
                    export)
                .ConfigureAwait(false);
            await InjectFaultAsync(
                    JsonExportPublicationFaultPoint
                        .BeforeManifestNamespaceCommit,
                    CancellationToken.None)
                .ConfigureAwait(false);
            fileSystem.RequireParentIdentity();

            bool reusedManifest;
            JsonExportPublicationFileSystem
                .NoReplaceRenameStatus manifestStatus =
                fileSystem.RenameNoReplace(
                    manifestStaging,
                    paths.ManifestPath);
            if (manifestStatus ==
                JsonExportPublicationFileSystem
                    .NoReplaceRenameStatus.Published)
            {
                stableManifest =
                    manifestStaging;
                manifestStaging = null;
                reusedManifest = false;
            }
            else
            {
                RemoveAndDispose(
                    fileSystem,
                    ref manifestStaging);
                try
                {
                    stableManifest =
                        fileSystem.OpenExistingRequired(
                            paths.ManifestPath);
                    await RequireExactManifestAsync(
                            stableManifest,
                            export.CanonicalManifestBytes,
                            CancellationToken.None)
                        .ConfigureAwait(false);
                    reusedManifest = true;
                }
                catch
                {
                    if (newlyPublishedData)
                    {
                        RollBackFreshData(
                            fileSystem,
                            ref stableData,
                            ref newlyPublishedData,
                            ref freshDataRollbackRequired);
                    }
                    throw;
                }
            }

            FileStream exactManifest =
                stableManifest ??
                throw new InvalidOperationException(
                    "The exact final JSON export manifest handle is unavailable.");
            await RequireExactManifestAsync(
                    exactManifest,
                    export.CanonicalManifestBytes,
                    CancellationToken.None)
                .ConfigureAwait(false);
            JsonExportPublicationFileSystem
                .RequireDistinctFiles(
                    exactData,
                    exactManifest);
            await RequireStableExactDataAsync(
                    fileSystem,
                    exactData,
                    export)
                .ConfigureAwait(false);

            committedPair = true;
            await InjectFaultAsync(
                    JsonExportPublicationFaultPoint
                        .AfterManifestNamespaceCommitBeforeResult,
                    CancellationToken.None)
                .ConfigureAwait(false);
            return CreateResult(
                paths,
                export,
                reusedData,
                reusedManifest);
        }
        catch (Exception exception)
        {
            bodyFailure = exception;
            throw;
        }
        finally
        {
            CaptureCleanupFailure(
                cleanupFailures,
                committedPair,
                () => DisposeStream(
                    ref stableManifest));
            if (fileSystem is not null &&
                freshDataRollbackRequired)
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    () => RollBackFreshData(
                        fileSystem,
                        ref stableData,
                        ref newlyPublishedData,
                        ref freshDataRollbackRequired));
            }
            CaptureCleanupFailure(
                cleanupFailures,
                committedPair,
                () => DisposeStream(
                    ref stableData));
            if (fileSystem is not null)
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref dataStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    fileSystem.Dispose);
            }
            else
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    () => DisposeStream(
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair,
                    () => DisposeStream(
                        ref dataStaging));
            }

            if (!committedPair &&
                cleanupFailures.Count != 0)
            {
                if (bodyFailure is not null)
                {
                    cleanupFailures.Insert(
                        0,
                        bodyFailure);
                }
                throw new AggregateException(
                    "JSON export publication and bound-handle cleanup did not both complete.",
                    cleanupFailures);
            }
        }
    }

    private static JsonExportPublicationResult
        CreateResult(
        JsonExportPublicationFileSystem
            .PublicationPaths paths,
        JsonStreamingExportResult export,
        bool reusedData,
        bool reusedManifest) =>
        new()
        {
            DestinationPath =
                paths.DestinationPath,
            ManifestPath =
                paths.ManifestPath,
            Manifest = export.Manifest,
            CanonicalManifestBytes =
                export.CanonicalManifestBytes,
            ManifestDigest =
                export.ManifestDigest,
            ReusedData = reusedData,
            ReusedManifest = reusedManifest,
        };

    private async ValueTask InjectFaultAsync(
        JsonExportPublicationFaultPoint point,
        CancellationToken cancellationToken)
    {
        if (faultInjector is not null)
        {
            await faultInjector
                .InjectAsync(
                    point,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async ValueTask
        VerifyStagingFilesAsync(
        FileStream data,
        FileStream manifest,
        JsonStreamingExportResult export,
        CancellationToken cancellationToken)
    {
        await RequireExactDataAsync(
                data,
                export,
                cancellationToken)
            .ConfigureAwait(false);
        await RequireExactManifestAsync(
                manifest,
                export.CanonicalManifestBytes,
                cancellationToken)
            .ConfigureAwait(false);
        JsonExportPublicationFileSystem
            .RequireDistinctFiles(
                data,
                manifest);
    }

    private static async ValueTask<FinalState>
        InspectFinalStateAsync(
        JsonExportPublicationFileSystem fileSystem,
        JsonStreamingExportResult export,
        CancellationToken cancellationToken)
    {
        JsonExportPublicationFileSystem
            .PublicationPaths paths =
            fileSystem.Paths;
        FileStream? data = null;
        FileStream? manifest = null;
        try
        {
            data =
                fileSystem.OpenExisting(
                    paths.DestinationPath,
                    allowMissing: true);
            manifest =
                fileSystem.OpenExisting(
                    paths.ManifestPath,
                    allowMissing: true);
            if (data is not null &&
                manifest is not null)
            {
                JsonExportPublicationFileSystem
                    .RequireDistinctFiles(
                        data,
                        manifest);
            }

            if (manifest is not null)
            {
                if (data is null)
                {
                    throw new InvalidDataException(
                        "A JSON export manifest exists without its final data file.");
                }

                bool dataMatches =
                    await DataMatchesAsync(
                            data,
                            export,
                            cancellationToken)
                        .ConfigureAwait(false);
                bool manifestMatches =
                    await ManifestMatchesAsync(
                            manifest,
                            export.CanonicalManifestBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                if (!manifestMatches)
                {
                    throw new IOException(
                        "The JSON export manifest destination already contains a different file.");
                }
                if (!dataMatches)
                {
                    throw new InvalidDataException(
                        "The JSON export manifest does not accompany its exact data file.");
                }

                var pair =
                    new FinalState(
                        FinalStateKind.ExactPair,
                        data,
                        manifest);
                data = null;
                manifest = null;
                return pair;
            }

            if (data is null)
            {
                return new FinalState(
                    FinalStateKind.Absent,
                    null,
                    null);
            }
            if (!await DataMatchesAsync(
                    data,
                    export,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                throw new IOException(
                    "The JSON export destination already contains a different file.");
            }

            var dataOnly =
                new FinalState(
                    FinalStateKind.ExactDataOnly,
                    data,
                    null);
            data = null;
            return dataOnly;
        }
        finally
        {
            if (manifest is not null)
            {
                await manifest
                    .DisposeAsync()
                    .ConfigureAwait(false);
            }
            if (data is not null)
            {
                await data
                    .DisposeAsync()
                    .ConfigureAwait(false);
            }
        }
    }

    private static void
        RejectManifestAfterFreshData(
        JsonExportPublicationFileSystem fileSystem,
        string manifestPath,
        ref FileStream? stableData,
        ref bool newlyPublishedData,
        ref bool rollbackRequired)
    {
        try
        {
            using FileStream? appeared =
                fileSystem.OpenExisting(
                    manifestPath,
                    allowMissing: true);
            if (appeared is null)
                return;
        }
        catch
        {
            RollBackFreshData(
                fileSystem,
                ref stableData,
                ref newlyPublishedData,
                ref rollbackRequired);
            throw;
        }

        RollBackFreshData(
            fileSystem,
            ref stableData,
            ref newlyPublishedData,
            ref rollbackRequired);
        throw new InvalidDataException(
            "The JSON export manifest appeared after the fresh data commit.");
    }

    private static void RollBackFreshData(
        JsonExportPublicationFileSystem fileSystem,
        ref FileStream? stableData,
        ref bool newlyPublishedData,
        ref bool rollbackRequired)
    {
        if (!newlyPublishedData)
            return;

        FileStream data =
            stableData ??
            throw new InvalidOperationException(
                "The newly published JSON data handle is unavailable for rollback.");
        rollbackRequired = true;
        fileSystem.RemoveByHandle(data);
        stableData = null;
        newlyPublishedData = false;
        rollbackRequired = false;
        data.Dispose();
    }

    private static async ValueTask
        RequireStableExactDataAsync(
        JsonExportPublicationFileSystem fileSystem,
        FileStream stableData,
        JsonStreamingExportResult export)
    {
        fileSystem.RequireParentIdentity();
        _ =
            JsonExportPublicationFileSystem
                .GetIdentity(stableData);
        await RequireExactDataAsync(
                stableData,
                export,
                CancellationToken.None)
            .ConfigureAwait(false);
        fileSystem.RequireParentIdentity();
    }

    private static async ValueTask RequireExactDataAsync(
        FileStream stream,
        JsonStreamingExportResult export,
        CancellationToken cancellationToken)
    {
        if (!await DataMatchesAsync(
                stream,
                export,
                cancellationToken)
            .ConfigureAwait(false))
        {
            throw new InvalidDataException(
                "The JSON export data file does not match its completed export evidence.");
        }
    }

    private static async ValueTask<bool> DataMatchesAsync(
        FileStream stream,
        JsonStreamingExportResult export,
        CancellationToken cancellationToken)
    {
        if (stream.Length !=
            export.Manifest.Content
                .DataByteLength)
        {
            return false;
        }

        long position = stream.Position;
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                BufferSize);
        byte[] expected =
            Convert.FromHexString(
                export.Manifest.Content
                    .DataDigest.Value);
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
            stream.Position = 0;
            long remaining = stream.Length;
            while (remaining > 0)
            {
                int requested =
                    (int)Math.Min(
                        remaining,
                        buffer.Length);
                int read =
                    await stream.ReadAsync(
                            buffer.AsMemory(
                                0,
                                requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0)
                    return false;
                hash.AppendData(
                    buffer,
                    0,
                    read);
                remaining -= read;
            }

            byte[] actual =
                hash.GetHashAndReset();
            try
            {
                return CryptographicOperations
                    .FixedTimeEquals(
                        actual,
                        expected);
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(actual);
            }
        }
        finally
        {
            stream.Position = position;
            CryptographicOperations
                .ZeroMemory(expected);
            ArrayPool<byte>.Shared.Return(
                buffer,
                clearArray: true);
        }
    }

    private static async ValueTask
        RequireExactManifestAsync(
        FileStream stream,
        ReadOnlyMemory<byte> expected,
        CancellationToken cancellationToken)
    {
        if (!await ManifestMatchesAsync(
                stream,
                expected,
                cancellationToken)
            .ConfigureAwait(false))
        {
            throw new IOException(
                "The JSON export manifest destination already contains a different file.");
        }
    }

    private static async ValueTask<bool>
        ManifestMatchesAsync(
        FileStream stream,
        ReadOnlyMemory<byte> expected,
        CancellationToken cancellationToken)
    {
        if (stream.Length != expected.Length)
            return false;

        long position = stream.Position;
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                BufferSize);
        try
        {
            stream.Position = 0;
            int offset = 0;
            while (offset < expected.Length)
            {
                int requested =
                    Math.Min(
                        buffer.Length,
                        expected.Length -
                        offset);
                int read =
                    await stream.ReadAsync(
                            buffer.AsMemory(
                                0,
                                requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0 ||
                    !CryptographicOperations
                        .FixedTimeEquals(
                            buffer.AsSpan(
                                0,
                                read),
                            expected.Span.Slice(
                                offset,
                                read)))
                {
                    return false;
                }
                offset += read;
            }

            return true;
        }
        finally
        {
            stream.Position = position;
            ArrayPool<byte>.Shared.Return(
                buffer,
                clearArray: true);
        }
    }

    private static async ValueTask FlushDurablyAsync(
        FileStream stream,
        CancellationToken cancellationToken)
    {
        await stream.FlushAsync(
                cancellationToken)
            .ConfigureAwait(false);
        cancellationToken
            .ThrowIfCancellationRequested();
        stream.Flush(
            flushToDisk: true);
    }

    private static void RemoveAndDispose(
        JsonExportPublicationFileSystem fileSystem,
        ref FileStream? stream)
    {
        if (stream is null)
            return;

        FileStream owned = stream;
        stream = null;
        try
        {
            fileSystem.RemoveByHandle(
                owned);
        }
        finally
        {
            owned.Dispose();
        }
    }

    private static void DisposeStream(
        ref FileStream? stream)
    {
        FileStream? owned = stream;
        stream = null;
        owned?.Dispose();
    }

    private static void CaptureCleanupFailure(
        List<Exception> failures,
        bool committedPair,
        Action cleanup)
    {
        try
        {
            cleanup();
        }
        catch (Exception exception)
        {
            if (!committedPair)
            {
                failures.Add(exception);
            }
        }
    }

    private enum FinalStateKind
    {
        Absent,
        ExactDataOnly,
        ExactPair,
    }

    private sealed class FinalState :
        IAsyncDisposable
    {
        private FileStream? data;
        private FileStream? manifest;

        internal FinalState(
            FinalStateKind kind,
            FileStream? data,
            FileStream? manifest)
        {
            Kind = kind;
            this.data = data;
            this.manifest = manifest;
        }

        internal FinalStateKind Kind { get; }

        internal FileStream TakeData()
        {
            FileStream result =
                data ??
                throw new InvalidOperationException(
                    "The exact final data handle is unavailable.");
            data = null;
            return result;
        }

        public async ValueTask DisposeAsync()
        {
            if (manifest is not null)
            {
                await manifest
                    .DisposeAsync()
                    .ConfigureAwait(false);
                manifest = null;
            }
            if (data is not null)
            {
                await data
                    .DisposeAsync()
                    .ConfigureAwait(false);
                data = null;
            }
        }
    }
}

internal enum JsonExportPublicationFaultPoint
{
    BeforeDataNamespaceCommit,
    AfterManifestAbsenceCheckBeforeDataCommit,
    AfterDataNamespaceCommitBeforeManifest,
    BeforeManifestNamespaceCommit,
    AfterManifestNamespaceCommitBeforeResult,
}

internal interface IJsonExportPublicationFaultInjector
{
    ValueTask InjectAsync(
        JsonExportPublicationFaultPoint point,
        CancellationToken cancellationToken);
}
