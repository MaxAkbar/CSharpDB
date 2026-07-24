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

/// <summary>
/// One completed prepared JSON or NDJSON output to publish manifest-last.
/// The expected manifest digest independently pins the terminal checkpoint
/// evidence supplied by the prepared-output lease.
/// </summary>
public sealed record JsonPreparedExportPublicationRequest
{
    public required string DestinationPath { get; init; }

    public required string ManifestPath { get; init; }

    public required string ExpectedManifestDigest { get; init; }
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
/// Completes a restart-only JSON export in deterministic private sibling
/// files, safely reclaims qualified crash leftovers, durably flushes them as
/// supported, and publishes data before its canonical manifest without
/// overwriting an existing path.
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

    internal static void
        ValidatePreparedPublicationPaths(
        string destinationPath,
        string manifestPath)
    {
        ValidatePaths(
            destinationPath,
            manifestPath);
        (
            string preparedDestination,
            JsonExportPreparedOutputPaths
                preparedPaths
        ) = JsonExportPreparedOutputLease
            .BindPathsAllowingCompletedDestination(
                destinationPath);
        PreflightPreparedPublicationPaths(
            destinationPath,
            manifestPath,
            preparedDestination,
            preparedPaths);
    }

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
        FinalState? initial = null;
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

            initial =
                await InspectFinalStateAsync(
                        fileSystem,
                        export,
                        cancellationToken)
                    .ConfigureAwait(false);
            JsonExportPublicationFileSystem
                ownedFileSystem = fileSystem;
            fileSystem = null;
            FileStream ownedDataStaging =
                dataStaging;
            dataStaging = null;
            FileStream ownedManifestStaging =
                manifestStaging;
            manifestStaging = null;
            FinalState ownedInitial =
                initial;
            initial = null;
            return await CompleteVerifiedPublicationAsync(
                    ownedFileSystem,
                    export,
                    ownedInitial,
                    ownedDataStaging,
                    ownedManifestStaging,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            bodyFailure = exception;
            throw;
        }
        finally
        {
            if (initial is not null)
            {
                try
                {
                    await initial
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    cleanupFailures.Add(
                        exception);
                }
            }
            if (fileSystem is not null)
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref dataStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    fileSystem.Dispose);
            }
            else
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => DisposeStream(
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => DisposeStream(
                        ref dataStaging));
            }

            if (cleanupFailures.Count != 0)
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

    /// <summary>
    /// Reopens and publishes one terminal prepared output without deleting its
    /// prepared data or checkpoint journal.
    /// </summary>
    public async ValueTask<JsonExportPublicationResult>
        PublishCompletedAsync(
        JsonPreparedExportPublicationRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidatePreparedPublicationRequest(
            request);
        ValidatePreparedPublicationPaths(
            request.DestinationPath,
            request.ManifestPath);

        JsonExportPreparedOutputLease? lease =
            null;
        bool committedPair = false;
        Exception? bodyFailure = null;
        try
        {
            lease =
                await JsonExportPreparedOutputLease
                    .OpenForPublicationAsync(
                        request.DestinationPath,
                        request.ExpectedManifestDigest,
                        cancellationToken)
                    .ConfigureAwait(false);
            JsonExportPublicationResult result =
                await PublishCompletedAsync(
                        request,
                        lease,
                        cancellationToken)
                    .ConfigureAwait(false);
            committedPair = true;
            return result;
        }
        catch (Exception exception)
        {
            bodyFailure = exception;
            throw;
        }
        finally
        {
            if (lease is not null)
            {
                try
                {
                    await lease
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception cleanupFailure)
                    when (!committedPair &&
                          bodyFailure is not null)
                {
                    throw new AggregateException(
                        "Prepared JSON export publication and lease cleanup did not both complete.",
                        bodyFailure,
                        cleanupFailure);
                }
                catch when (committedPair)
                {
                    // A qualified final pair is already the commit.
                }
            }
        }
    }

    internal async ValueTask<JsonExportPublicationResult>
        PublishCompletedAsync(
        JsonPreparedExportPublicationRequest request,
        JsonExportPreparedOutputLease liveLease,
        CancellationToken cancellationToken = default)
    {
        ValidatePreparedPublicationRequest(
            request);
        ArgumentNullException.ThrowIfNull(
            liveLease);
        cancellationToken
            .ThrowIfCancellationRequested();
        PreflightPreparedPublicationPaths(
            request.DestinationPath,
            request.ManifestPath,
            liveLease.DestinationPath,
            liveLease.Paths);

        await using
            JsonExportPreparedOutputPublicationQualification
                qualification =
                    await liveLease
                        .QualifyForPublicationAsync(
                            request
                                .ExpectedManifestDigest,
                            cancellationToken)
                        .ConfigureAwait(false);
        return await PublishQualifiedCompletedAsync(
                request,
                qualification,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<JsonExportPublicationResult>
        PublishQualifiedCompletedAsync(
        JsonPreparedExportPublicationRequest request,
        JsonExportPreparedOutputPublicationQualification
            qualification,
        CancellationToken cancellationToken)
    {
        JsonExportPublicationFileSystem?
            fileSystem = null;
        FileStream? dataStaging = null;
        FileStream? manifestStaging = null;
        FinalState? initial = null;
        Exception? bodyFailure = null;
        List<Exception> cleanupFailures = [];
        try
        {
            if (!string.Equals(
                    request.DestinationPath,
                    qualification.DestinationPath,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The prepared JSON publication destination does not match its live lease.");
            }

            JsonStreamingExportResult export =
                CreateCompletedExportEvidence(
                    qualification.Checkpoint,
                    request
                        .ExpectedManifestDigest);
            fileSystem =
                JsonExportPublicationFileSystem.Open(
                    request.DestinationPath,
                    request.ManifestPath);
            RequireDistinctPublicationPaths(
                fileSystem.Paths,
                qualification.Paths);

            initial =
                await InspectFinalStateAsync(
                        fileSystem,
                        export,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (initial.Kind !=
                FinalStateKind.ExactPair)
            {
                if (initial.Kind ==
                    FinalStateKind.Absent)
                {
                    dataStaging =
                        fileSystem
                            .CreatePrivateStagingFile(
                                fileSystem.Paths
                                    .DataStagingPath);
                    await CopyPreparedDataAsync(
                            qualification.DataStream,
                            dataStaging,
                            export.Manifest.Content,
                            cancellationToken)
                        .ConfigureAwait(false);
                    await FlushDurablyAsync(
                            dataStaging,
                            cancellationToken)
                        .ConfigureAwait(false);
                }

                manifestStaging =
                    fileSystem
                        .CreatePrivateStagingFile(
                            fileSystem.Paths
                                .ManifestStagingPath);
                await manifestStaging
                    .WriteAsync(
                        export.CanonicalManifestBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                await FlushDurablyAsync(
                        manifestStaging,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (dataStaging is not null)
                {
                    await VerifyStagingFilesAsync(
                            dataStaging,
                            manifestStaging,
                            export,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await RequireExactManifestAsync(
                            manifestStaging,
                            export.CanonicalManifestBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                fileSystem.RequireParentIdentity();
            }

            JsonExportPublicationFileSystem
                ownedFileSystem = fileSystem;
            fileSystem = null;
            FileStream? ownedDataStaging =
                dataStaging;
            dataStaging = null;
            FileStream? ownedManifestStaging =
                manifestStaging;
            manifestStaging = null;
            FinalState ownedInitial =
                initial;
            initial = null;
            return await CompleteVerifiedPublicationAsync(
                    ownedFileSystem,
                    export,
                    ownedInitial,
                    ownedDataStaging,
                    ownedManifestStaging,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            bodyFailure = exception;
            throw;
        }
        finally
        {
            if (initial is not null)
            {
                try
                {
                    await initial
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    cleanupFailures.Add(
                        exception);
                }
            }
            if (fileSystem is not null)
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => RemoveAndDispose(
                        fileSystem,
                        ref dataStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    fileSystem.Dispose);
            }
            else
            {
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => DisposeStream(
                        ref manifestStaging));
                CaptureCleanupFailure(
                    cleanupFailures,
                    committedPair: false,
                    () => DisposeStream(
                        ref dataStaging));
            }

            if (cleanupFailures.Count != 0)
            {
                if (bodyFailure is not null)
                {
                    cleanupFailures.Insert(
                        0,
                        bodyFailure);
                }
                throw new AggregateException(
                    "Prepared JSON export publication and bound-handle cleanup did not both complete.",
                    cleanupFailures);
            }
        }
    }

    private async ValueTask<JsonExportPublicationResult>
        CompleteVerifiedPublicationAsync(
        JsonExportPublicationFileSystem fileSystem,
        JsonStreamingExportResult export,
        FinalState initial,
        FileStream? dataStaging,
        FileStream? manifestStaging,
        CancellationToken cancellationToken)
    {
        JsonExportPublicationFileSystem
            .PublicationPaths paths =
            fileSystem.Paths;
        FileStream? stableData = null;
        FileStream? stableManifest = null;
        bool committedPair = false;
        bool newlyPublishedData = false;
        bool freshDataRollbackRequired = false;
        Exception? bodyFailure = null;
        List<Exception> cleanupFailures = [];
        try
        {
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
                FileStream unpublishedData =
                    dataStaging ??
                    throw new InvalidOperationException(
                        "Verified JSON data staging is required before a fresh publication.");
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
                        unpublishedData,
                        paths.DestinationPath);
                if (dataStatus ==
                    JsonExportPublicationFileSystem
                        .NoReplaceRenameStatus.Published)
                {
                    stableData =
                        unpublishedData;
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
                        fileSystem
                            .OpenExistingRequired(
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

            // Exact final data is now the recovery authority. Its open
            // handle denies write/delete sharing through the manifest
            // decision, and cancellation is deliberately no longer
            // observed.
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

            FileStream unpublishedManifest =
                manifestStaging ??
                throw new InvalidOperationException(
                    "Verified JSON manifest staging is required before publication.");
            bool reusedManifest;
            JsonExportPublicationFileSystem
                .NoReplaceRenameStatus manifestStatus =
                fileSystem.RenameNoReplace(
                    unpublishedManifest,
                    paths.ManifestPath);
            if (manifestStatus ==
                JsonExportPublicationFileSystem
                    .NoReplaceRenameStatus.Published)
            {
                stableManifest =
                    unpublishedManifest;
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
                        fileSystem
                            .OpenExistingRequired(
                                paths.ManifestPath);
                    await RequireExactManifestAsync(
                            stableManifest,
                            export
                                .CanonicalManifestBytes,
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
            try
            {
                await initial
                    .DisposeAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                if (!committedPair)
                {
                    cleanupFailures.Add(
                        exception);
                }
            }
            CaptureCleanupFailure(
                cleanupFailures,
                committedPair,
                () => DisposeStream(
                    ref stableManifest));
            if (freshDataRollbackRequired)
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

    private static void
        ValidatePreparedPublicationRequest(
        JsonPreparedExportPublicationRequest request)
    {
        ArgumentNullException.ThrowIfNull(
            request);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.DestinationPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.ManifestPath);
        ValidateCanonicalSha256(
            request.ExpectedManifestDigest,
            nameof(
                request.ExpectedManifestDigest));
    }

    private static JsonStreamingExportResult
        CreateCompletedExportEvidence(
        JsonExportCheckpoint checkpoint,
        string expectedManifestDigest)
    {
        JsonExportManifest manifest =
            JsonExportCheckpointSerializer
                .CreateCompletedManifest(
                    checkpoint);
        byte[] canonicalManifestBytes =
            JsonExportManifestSerializer
                .Serialize(manifest);
        try
        {
            string manifestDigest =
                JsonExportManifestSerializer
                    .ComputeManifestDigest(
                        manifest);
            RequireManifestDigestEquals(
                manifestDigest,
                checkpoint.Completion!
                    .ManifestDigest,
                "The terminal JSON checkpoint does not match its reconstructed manifest.");
            RequireManifestDigestEquals(
                manifestDigest,
                expectedManifestDigest,
                "The terminal JSON checkpoint does not match the expected manifest digest.");
            return new JsonStreamingExportResult
            {
                Manifest = manifest,
                CanonicalManifestBytes =
                    canonicalManifestBytes,
                ManifestDigest =
                    manifestDigest,
            };
        }
        catch
        {
            CryptographicOperations
                .ZeroMemory(
                    canonicalManifestBytes);
            throw;
        }
    }

    private static void
        RequireDistinctPublicationPaths(
        JsonExportPublicationFileSystem
            .PublicationPaths publication,
        JsonExportPreparedOutputPaths prepared)
    {
        var paths =
            new HashSet<string>(
                StringComparer.OrdinalIgnoreCase);
        AddDistinctPublicationPath(
            paths,
            publication.DestinationPath);
        AddDistinctPublicationPath(
            paths,
            publication.ManifestPath);
        AddDistinctPublicationPath(
            paths,
            prepared.PreparedDataPath);
        AddDistinctPublicationPath(
            paths,
            prepared.CheckpointPath);
        AddDistinctPublicationPath(
            paths,
            prepared.PendingCheckpointPath);
        AddDistinctPublicationPath(
            paths,
            publication.DataStagingPath);
        AddDistinctPublicationPath(
            paths,
            publication.ManifestStagingPath);
    }

    private static void
        PreflightPreparedPublicationPaths(
        string destinationPath,
        string manifestPath,
        string preparedDestination,
        JsonExportPreparedOutputPaths
            preparedPaths)
    {
        if (!string.Equals(
                destinationPath,
                preparedDestination,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The prepared JSON publication destination does not match its path binding.");
        }
        JsonExportPublicationFileSystem
            .PublicationPaths publication =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    destinationPath,
                    manifestPath);
        RequireDistinctPublicationPaths(
            publication,
            preparedPaths);
    }

    private static void AddDistinctPublicationPath(
        HashSet<string> paths,
        string path)
    {
        if (!paths.Add(path))
        {
            throw new ArgumentException(
                "Prepared JSON final, journal, and publication-staging paths must all be distinct.");
        }
    }

    private static async ValueTask
        CopyPreparedDataAsync(
        FileStream prepared,
        FileStream staging,
        JsonExportContentManifest content,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            prepared);
        ArgumentNullException.ThrowIfNull(
            staging);
        ArgumentNullException.ThrowIfNull(
            content);
        if (prepared.Length !=
            content.DataByteLength)
        {
            throw new InvalidDataException(
                "The prepared JSON data length changed before publication.");
        }

        long preparedPosition =
            prepared.Position;
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                BufferSize);
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
            prepared.Position = 0;
            long remaining =
                content.DataByteLength;
            while (remaining > 0)
            {
                int requested =
                    (int)Math.Min(
                        remaining,
                        buffer.Length);
                int read =
                    await prepared
                        .ReadAsync(
                            buffer.AsMemory(
                                0,
                                requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0)
                {
                    throw new InvalidDataException(
                        "The prepared JSON data ended before its terminal checkpoint boundary.");
                }

                hash.AppendData(
                    buffer,
                    0,
                    read);
                await staging
                    .WriteAsync(
                        buffer.AsMemory(
                            0,
                            read),
                        cancellationToken)
                    .ConfigureAwait(false);
                remaining -= read;
            }

            if (prepared.Length !=
                    content.DataByteLength ||
                staging.Length !=
                    content.DataByteLength)
            {
                throw new InvalidDataException(
                    "The prepared JSON data changed while it was staged for publication.");
            }

            byte[] actual =
                hash.GetHashAndReset();
            try
            {
                RequireHashEquals(
                    actual,
                    content.DataDigest,
                    "The prepared JSON data digest changed while it was staged for publication.");
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(actual);
            }
        }
        finally
        {
            prepared.Position =
                preparedPosition;
            CryptographicOperations
                .ZeroMemory(
                    buffer.AsSpan(
                        0,
                        buffer.Length));
            ArrayPool<byte>.Shared.Return(
                buffer);
        }
    }

    private static void RequireHashEquals(
        ReadOnlySpan<byte> actual,
        JsonExportHashManifest expected,
        string message)
    {
        if (expected is null ||
            !string.Equals(
                expected.Algorithm,
                JsonExportHashManifest
                    .Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                message);
        }

        byte[] expectedBytes;
        try
        {
            expectedBytes =
                Convert.FromHexString(
                    expected.Value);
        }
        catch (Exception exception) when (
            exception is
                FormatException or
                ArgumentNullException)
        {
            throw new InvalidDataException(
                message);
        }

        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        actual,
                        expectedBytes))
            {
                throw new InvalidDataException(
                    message);
            }
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(
                    expectedBytes);
        }
    }

    private static void
        RequireManifestDigestEquals(
        string actual,
        string expected,
        string message)
    {
        ValidateCanonicalSha256(
            actual,
            nameof(actual));
        ValidateCanonicalSha256(
            expected,
            nameof(expected));
        byte[] actualBytes =
            Convert.FromHexString(actual);
        byte[] expectedBytes =
            Convert.FromHexString(expected);
        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        actualBytes,
                        expectedBytes))
            {
                throw new InvalidDataException(
                    message);
            }
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(
                    actualBytes);
            CryptographicOperations
                .ZeroMemory(
                    expectedBytes);
        }
    }

    private static void ValidateCanonicalSha256(
        string value,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            value,
            parameterName);
        if (value.Length !=
                SHA256.HashSizeInBytes *
                2 ||
            value.Any(
                character =>
                    character is not
                        (>= '0' and <= '9') and not
                        (>= 'a' and <= 'f')))
        {
            throw new ArgumentException(
                "The expected JSON manifest digest must be a lowercase SHA-256 value.",
                parameterName);
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
