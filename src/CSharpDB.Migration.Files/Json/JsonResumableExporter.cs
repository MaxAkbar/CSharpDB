using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Complete platform-neutral input for one durable, resumable JSON or NDJSON
/// prepared output. Every sequence returned by <see cref="OpenRows"/> must
/// read the same immutable source snapshot described by <see cref="Source"/>
/// and <see cref="SourceSnapshotIdentity"/>.
/// </summary>
public sealed record JsonResumableExportRequest
{
    /// <summary>
    /// The fully qualified future destination. Path qualification and session
    /// opening belong to the platform-specific lease layer.
    /// </summary>
    public required string DestinationPath { get; init; }

    public required JsonExportProfile Profile { get; init; }

    public required JsonExportFraming Framing { get; init; }

    public required JsonExportSourceManifest Source { get; init; }

    /// <summary>
    /// Canonical retained-snapshot identity bound into every checkpoint.
    /// </summary>
    public required string SourceSnapshotIdentity { get; init; }

    public required TableSchema Table { get; init; }

    /// <summary>
    /// Opens rows from the immutable source. A null boundary means from the
    /// beginning; a non-null signed row ID means strictly after that row.
    /// </summary>
    public required Func<
        long?,
        CancellationToken,
        IAsyncEnumerable<JsonExportRow>> OpenRows
    { get; init; }

    /// <summary>Maximum number of JSON data bytes that may be prepared.</summary>
    public long MaxDataBytes { get; init; } = 1L << 40;

    /// <summary>
    /// Per-value decoded BLOB ceiling recorded in every BLOB column contract.
    /// </summary>
    public int MaximumDecodedBlobBytes { get; init; } =
        JsonExportContracts.MaximumSupportedDecodedBlobBytes;

    /// <summary>
    /// Number of newly completed objects between durable writing checkpoints.
    /// </summary>
    public long CheckpointRowInterval { get; init; } = 10_000;
}

/// <summary>
/// Opening disposition of an already qualified private prepared-output
/// session. It is a snapshot: the coordinator never re-reads it after a
/// successful checkpoint persistence.
/// </summary>
internal enum JsonExportPreparedOutputState
{
    New,
    Recovered,
    UncheckpointedData,
}

/// <summary>
/// Platform-specific durable prepared-output authority consumed by the
/// platform-neutral coordinator. A recovered stream has already had any
/// non-authoritative tail removed by the lease and is positioned exactly at
/// its active checkpoint boundary.
/// </summary>
internal interface IJsonExportPreparedOutputSession :
    IAsyncDisposable
{
    JsonExportPreparedOutputState State { get; }

    JsonExportCheckpoint? CurrentCheckpoint { get; }

    /// <summary>
    /// Exact private data stream. PersistCheckpointAsync owns durable data
    /// flush, pending-checkpoint durability, and the cancellation cutoff for
    /// activating that checkpoint.
    /// </summary>
    Stream DataStream { get; }

    /// <summary>
    /// Durably discards private bytes that have no active generation-zero
    /// authority. Successful return exposes the same stream with both length
    /// and position exactly zero.
    /// </summary>
    ValueTask ResetUncheckpointedAsync(
        CancellationToken cancellationToken);

    /// <summary>
    /// Durably commits the supplied checkpoint for the session-bound expected
    /// binding. The implementation validates canonical generation zero,
    /// idempotent or exact +1 transitions, terminality, and the stream's exact
    /// length, position, physical digest, and framing boundary. Cancellation
    /// is honored until the pending checkpoint is durable; activation after
    /// that cutoff must complete without observing cancellation. Successful
    /// return leaves the same stream positioned at the checkpoint boundary.
    /// </summary>
    ValueTask PersistCheckpointAsync(
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken);
}

public sealed partial class JsonStreamingExporter
{
    private const int ResumeHashBufferBytes =
        64 * 1024;

    /// <summary>
    /// Creates or resumes an already opened private prepared output. Session
    /// opening, path qualification, link defense, durable replacement, and
    /// final publication are deliberately outside this coordinator.
    /// </summary>
    internal async ValueTask<JsonStreamingExportResult>
        WriteResumableCoreAsync(
        JsonResumableExportRequest request,
        IJsonExportPreparedOutputSession session,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(session);
        if (string.IsNullOrWhiteSpace(
                request.DestinationPath))
        {
            throw new ArgumentException(
                "A resumable JSON export destination is required.",
                nameof(request));
        }
        if (request.OpenRows is null)
        {
            throw new ArgumentException(
                "A resumable JSON export row source is required.",
                nameof(request));
        }
        if (request.CheckpointRowInterval <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The JSON export checkpoint row interval must be positive.");
        }

        PreparedRequest prepared =
            PrepareRequest(
                new JsonStreamingExportRequest
                {
                    Profile = request.Profile,
                    Framing = request.Framing,
                    Source = request.Source,
                    Table = request.Table,
                    Rows = ResumeEmptyRows(),
                    MaxDataBytes =
                        request.MaxDataBytes,
                    MaximumDecodedBlobBytes =
                        request
                            .MaximumDecodedBlobBytes,
                },
                cancellationToken);
        var binding =
            new JsonExportCheckpointBinding
            {
                Profile = prepared.Profile,
                Source = prepared.Source,
                SourceSnapshotIdentity =
                    request.SourceSnapshotIdentity,
                Table = prepared.Table,
                Json = prepared.Format,
            };
        JsonExportHashManifest bindingDigest =
            JsonExportCheckpointSerializer
                .ComputeBindingDigest(binding);

        cancellationToken
            .ThrowIfCancellationRequested();

        JsonExportPreparedOutputState openingState =
            session.State;
        JsonExportCheckpoint? openingCheckpoint =
            session.CurrentCheckpoint;
        if (openingState ==
            JsonExportPreparedOutputState
                .UncheckpointedData)
        {
            if (openingCheckpoint is not null)
            {
                throw new InvalidDataException(
                    "An uncheckpointed JSON prepared output cannot expose an active checkpoint.");
            }
            await session
                .ResetUncheckpointedAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            openingState =
                JsonExportPreparedOutputState.New;
        }

        Stream data = session.DataStream
            ?? throw new InvalidDataException(
                "The JSON prepared-output session did not expose its data stream.");
        ValidateResumeStreamCapabilities(data);

        using var sourceDigest =
            new JsonExportOrderedContentDigest();
        using var exportedDigest =
            new JsonExportOrderedContentDigest();

        long rowCount;
        long? lastCompletedRowId;
        JsonExportCheckpoint activeCheckpoint;
        ExportByteSink sink;

        switch (openingState)
        {
            case JsonExportPreparedOutputState.New:
                if (openingCheckpoint is not null)
                {
                    throw new InvalidDataException(
                        "A new JSON prepared output cannot expose an active checkpoint.");
                }
                RequireResumeStreamPosition(
                    data,
                    expectedLength: 0,
                    expectedPosition: 0,
                    "A new JSON prepared output must be empty and positioned at byte zero.");

                sink = new ExportByteSink(
                    data,
                    request.MaxDataBytes);
                try
                {
                    await WriteFramingStartAsync(
                            sink,
                            prepared.Framing,
                            cancellationToken)
                        .ConfigureAwait(false);
                    activeCheckpoint =
                        CreateResumeCheckpoint(
                            binding,
                            bindingDigest,
                            generation: 0,
                            JsonExportCheckpointPhase
                                .Writing,
                            rowCount: 0,
                            lastCompletedRowId: null,
                            sink,
                            sourceDigest
                                .GetCurrentPrefixDigest(),
                            exportedDigest
                                .GetCurrentPrefixDigest(),
                            completion: null);
                    await PersistResumeCheckpointAsync(
                            session,
                            data,
                            previous: null,
                            activeCheckpoint,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    sink.Dispose();
                    throw;
                }

                rowCount = 0;
                lastCompletedRowId = null;
                break;

            case JsonExportPreparedOutputState.Recovered:
                activeCheckpoint =
                    openingCheckpoint
                    ?? throw new InvalidDataException(
                        "A recovered JSON prepared output has no active checkpoint.");
                _ = JsonExportCheckpointSerializer
                    .ComputeCheckpointDigest(
                        activeCheckpoint);
                ResumeRequireHashEquals(
                    activeCheckpoint.BindingDigest,
                    bindingDigest,
                    "The recovered JSON export checkpoint does not match the requested export binding.");
                RequireResumeStreamPosition(
                    data,
                    activeCheckpoint.Progress
                        .DataPrefixByteLength,
                    activeCheckpoint.Progress
                        .DataPrefixByteLength,
                    "The recovered JSON prepared output is not the exact active checkpoint prefix.");

                ReplayState replay =
                    await ReplayAndVerifyJsonAsync(
                            request,
                            prepared,
                            activeCheckpoint,
                            sourceDigest,
                            exportedDigest,
                            cancellationToken)
                        .ConfigureAwait(false);
                sink = await CreateSeededJsonSinkAsync(
                        data,
                        request.MaxDataBytes,
                        activeCheckpoint,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (activeCheckpoint.Phase ==
                    JsonExportCheckpointPhase
                        .DataComplete)
                {
                    sink.Dispose();
                    return CreateCompletedJsonResult(
                        activeCheckpoint);
                }

                rowCount = replay.RowCount;
                lastCompletedRowId =
                    replay.LastCompletedRowId;
                break;

            case JsonExportPreparedOutputState
                .UncheckpointedData:
            default:
                throw new InvalidDataException(
                    "The JSON prepared-output session state is unsupported.");
        }

        using (sink)
        {
            long rowsSinceCheckpoint = 0;
            IAsyncEnumerable<JsonExportRow>
                continuation =
                    OpenResumeRows(
                        request,
                        lastCompletedRowId,
                        cancellationToken);
            await foreach (
                JsonExportRow row in
                continuation
                    .WithCancellation(
                        cancellationToken)
                    .ConfigureAwait(false))
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                if (lastCompletedRowId is
                        long previousRowId &&
                    row.RowId <= previousRowId)
                {
                    throw new InvalidDataException(
                        "JSON export continuation rows must be strictly after the durable checkpoint boundary.");
                }
                if (rowCount == long.MaxValue)
                {
                    throw new OverflowException(
                        "JSON export row count exceeds the signed 64-bit contract.");
                }

                bool followsRow = rowCount != 0;
                PreparedRow preparedRow =
                    await PrepareRowAsync(
                            row,
                            prepared,
                            sink.BytesWritten,
                            followsRow,
                            request.MaxDataBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                try
                {
                    await WriteVerifiedRowAsync(
                            sink,
                            prepared,
                            preparedRow,
                            followsRow,
                            cancellationToken)
                        .ConfigureAwait(false);
                    sourceDigest.AppendRowHash(
                        preparedRow
                            .SourceCanonicalRowHash);
                    exportedDigest.AppendRowHash(
                        preparedRow
                            .ExportedCanonicalRowHash);
                    rowCount =
                        checked(rowCount + 1);
                    rowsSinceCheckpoint =
                        checked(
                            rowsSinceCheckpoint +
                            1);
                    lastCompletedRowId =
                        row.RowId;
                }
                finally
                {
                    preparedRow
                        .ClearSensitiveBuffers();
                }

                if (rowsSinceCheckpoint <
                    request.CheckpointRowInterval)
                {
                    continue;
                }

                JsonExportCheckpoint next =
                    CreateResumeCheckpoint(
                        binding,
                        bindingDigest,
                        NextJsonGeneration(
                            activeCheckpoint
                                .Generation),
                        JsonExportCheckpointPhase
                            .Writing,
                        rowCount,
                        lastCompletedRowId,
                        sink,
                        sourceDigest
                            .GetCurrentPrefixDigest(),
                        exportedDigest
                            .GetCurrentPrefixDigest(),
                        completion: null);
                await PersistResumeCheckpointAsync(
                        session,
                        data,
                        activeCheckpoint,
                        next,
                        cancellationToken)
                    .ConfigureAwait(false);
                activeCheckpoint = next;
                rowsSinceCheckpoint = 0;
            }

            if (sourceDigest.RowCount !=
                    rowCount ||
                exportedDigest.RowCount !=
                    rowCount)
            {
                throw new InvalidOperationException(
                    "JSON export logical row counts diverged.");
            }

            await WriteFramingEndAsync(
                    sink,
                    prepared.Framing,
                    cancellationToken)
                .ConfigureAwait(false);

            JsonExportHashManifest
                sourcePrefixDigest =
                    sourceDigest
                        .GetCurrentPrefixDigest();
            JsonExportHashManifest
                exportedPrefixDigest =
                    exportedDigest
                        .GetCurrentPrefixDigest();
            JsonExportHashManifest
                sourceLogicalDigest =
                    sourceDigest.Complete();
            JsonExportHashManifest
                exportedLogicalDigest =
                    exportedDigest.Complete();
            ResumeRequireHashEquals(
                sourceLogicalDigest,
                exportedLogicalDigest,
                "Lossless JSON source and exported logical digests diverged.");

            JsonExportHashManifest dataDigest =
                sink.GetCurrentHash();
            JsonExportManifest manifest =
                CreateManifest(
                    prepared,
                    rowCount,
                    sink.BytesWritten,
                    dataDigest,
                    sourceLogicalDigest,
                    exportedLogicalDigest);
            byte[] canonicalManifestBytes =
                JsonExportManifestSerializer
                    .Serialize(manifest);
            try
            {
                string manifestDigest =
                    JsonExportManifestSerializer
                        .ComputeManifestDigest(
                            manifest);
                var completion =
                    new JsonExportCheckpointCompletion
                    {
                        SourceLogicalDigest =
                            sourceLogicalDigest,
                        ExportedLogicalDigest =
                            exportedLogicalDigest,
                        ManifestDigest =
                            manifestDigest,
                    };
                JsonExportCheckpoint completed =
                    CreateResumeCheckpoint(
                        binding,
                        bindingDigest,
                        NextJsonGeneration(
                            activeCheckpoint
                                .Generation),
                        JsonExportCheckpointPhase
                            .DataComplete,
                        rowCount,
                        lastCompletedRowId,
                        sink,
                        sourcePrefixDigest,
                        exportedPrefixDigest,
                        completion);
                await PersistResumeCheckpointAsync(
                        session,
                        data,
                        activeCheckpoint,
                        completed,
                        cancellationToken)
                    .ConfigureAwait(false);

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
    }

    private static async ValueTask<ReplayState>
        ReplayAndVerifyJsonAsync(
        JsonResumableExportRequest request,
        PreparedRequest prepared,
        JsonExportCheckpoint checkpoint,
        JsonExportOrderedContentDigest sourceDigest,
        JsonExportOrderedContentDigest exportedDigest,
        CancellationToken cancellationToken)
    {
        JsonExportCheckpointProgress progress =
            checkpoint.Progress;
        using var replaySink =
            new ExportByteSink(
                Stream.Null,
                request.MaxDataBytes);
        await WriteFramingStartAsync(
                replaySink,
                prepared.Framing,
                cancellationToken)
            .ConfigureAwait(false);

        long rowCount = 0;
        long? previousRowId = null;
        IAsyncEnumerable<JsonExportRow> replayRows =
            OpenResumeRows(
                request,
                boundary: null,
                cancellationToken);
        await using IAsyncEnumerator<JsonExportRow>
            enumerator =
                replayRows.GetAsyncEnumerator(
                    cancellationToken);

        while (rowCount <
               progress.CompletedRowCount)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            if (!await enumerator
                    .MoveNextAsync()
                    .ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "The JSON export source ended before the durable checkpoint row boundary.");
            }

            JsonExportRow row =
                enumerator.Current;
            if (previousRowId is long previous &&
                row.RowId <= previous)
            {
                throw new InvalidDataException(
                    "JSON export replay rows must have strictly increasing physical row IDs.");
            }

            bool followsRow = rowCount != 0;
            PreparedRow preparedRow =
                await PrepareRowAsync(
                        row,
                        prepared,
                        replaySink.BytesWritten,
                        followsRow,
                        request.MaxDataBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
            try
            {
                await WriteVerifiedRowAsync(
                        replaySink,
                        prepared,
                        preparedRow,
                        followsRow,
                        cancellationToken)
                    .ConfigureAwait(false);
                sourceDigest.AppendRowHash(
                    preparedRow
                        .SourceCanonicalRowHash);
                exportedDigest.AppendRowHash(
                    preparedRow
                        .ExportedCanonicalRowHash);
                rowCount =
                    checked(rowCount + 1);
                previousRowId = row.RowId;
            }
            finally
            {
                preparedRow
                    .ClearSensitiveBuffers();
            }
        }

        if (checkpoint.Phase ==
            JsonExportCheckpointPhase.DataComplete)
        {
            await WriteFramingEndAsync(
                    replaySink,
                    prepared.Framing,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (previousRowId !=
            progress.LastCompletedRowId)
        {
            throw new InvalidDataException(
                "The JSON export replay row boundary does not match the durable checkpoint.");
        }
        if (replaySink.BytesWritten !=
            progress.DataPrefixByteLength)
        {
            throw new InvalidDataException(
                "The JSON export replay byte length does not match the durable checkpoint.");
        }
        ResumeRequireHashEquals(
            replaySink.GetCurrentHash(),
            progress.DataPrefixDigest,
            "The JSON export replay bytes do not match the durable checkpoint.");
        ResumeRequireHashEquals(
            sourceDigest
                .GetCurrentPrefixDigest(),
            progress
                .SourceLogicalRowHashPrefixDigest,
            "The JSON export replay source values do not match the durable checkpoint.");
        ResumeRequireHashEquals(
            exportedDigest
                .GetCurrentPrefixDigest(),
            progress
                .ExportedLogicalRowHashPrefixDigest,
            "The JSON export replay output values do not match the durable checkpoint.");
        if (sourceDigest.RowCount !=
                progress.CompletedRowCount ||
            exportedDigest.RowCount !=
                progress.CompletedRowCount)
        {
            throw new InvalidDataException(
                "The JSON export replay counters do not match the durable checkpoint.");
        }

        if (checkpoint.Phase ==
            JsonExportCheckpointPhase.DataComplete)
        {
            if (await enumerator
                    .MoveNextAsync()
                    .ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "The JSON export source contains rows beyond its data-complete checkpoint.");
            }

            JsonExportCheckpointCompletion
                completion =
                    checkpoint.Completion
                    ?? throw new
                        InvalidDataException(
                            "The data-complete JSON export checkpoint has no completion evidence.");
            ResumeRequireHashEquals(
                sourceDigest.Complete(),
                completion
                    .SourceLogicalDigest,
                "The completed JSON export source digest does not match replay.");
            ResumeRequireHashEquals(
                exportedDigest.Complete(),
                completion
                    .ExportedLogicalDigest,
                "The completed JSON export output digest does not match replay.");
            _ = JsonExportCheckpointSerializer
                .CreateCompletedManifest(
                    checkpoint);
        }

        return new ReplayState(
            rowCount,
            previousRowId);
    }

    private static async ValueTask<ExportByteSink>
        CreateSeededJsonSinkAsync(
        Stream data,
        long maximumBytes,
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        JsonExportCheckpointProgress progress =
            checkpoint.Progress;
        var hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                ResumeHashBufferBytes);
        try
        {
            data.Position = 0;
            long remaining =
                progress.DataPrefixByteLength;
            while (remaining > 0)
            {
                int requested =
                    (int)Math.Min(
                        remaining,
                        buffer.Length);
                int read =
                    await data.ReadAsync(
                            buffer.AsMemory(
                                0,
                                requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0)
                {
                    throw new InvalidDataException(
                        "The prepared JSON data ended before its checkpoint boundary.");
                }

                hash.AppendData(
                    buffer,
                    0,
                    read);
                remaining -= read;
            }

            byte[] current =
                hash.GetCurrentHash();
            try
            {
                ResumeRequireHashEquals(
                    CreateResumeHashManifest(
                        current),
                    progress.DataPrefixDigest,
                    "The prepared JSON prefix changed while resume state was reconstructed.");
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(current);
            }

            await ValidateObservedJsonBoundaryAsync(
                    data,
                    checkpoint,
                    cancellationToken)
                .ConfigureAwait(false);
            data.Position =
                progress.DataPrefixByteLength;
            return new ExportByteSink(
                data,
                maximumBytes,
                hash,
                progress.DataPrefixByteLength);
        }
        catch
        {
            hash.Dispose();
            throw;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                buffer.AsSpan(
                    0,
                    buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask
        ValidateObservedJsonBoundaryAsync(
        Stream data,
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        JsonExportCheckpointProgress progress =
            checkpoint.Progress;
        long length =
            progress.DataPrefixByteLength;
        int trailingLength =
            (checkpoint.Binding.Json.Framing,
             checkpoint.Phase,
             progress.CompletedRowCount > 0)
            switch
            {
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase
                        .Writing,
                    false) => 1,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase
                        .Writing,
                    true) => 1,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase
                        .DataComplete,
                    _) => 3,
                (
                    JsonExportFraming.Ndjson,
                    _,
                    false) => 0,
                (
                    JsonExportFraming.Ndjson,
                    _,
                    true) => 2,
                _ => throw new InvalidDataException(
                    "The recovered JSON export framing is unsupported."),
            };

        byte? firstByte = null;
        if (length > 0)
        {
            data.Position = 0;
            byte[] first = new byte[1];
            await ReadResumeExactlyAsync(
                    data,
                    first,
                    cancellationToken)
                .ConfigureAwait(false);
            firstByte = first[0];
        }

        byte[] trailing =
            new byte[trailingLength];
        if (trailingLength != 0)
        {
            data.Position =
                checked(length -
                    trailingLength);
            await ReadResumeExactlyAsync(
                    data,
                    trailing,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        _ = JsonExportCheckpointFraming
            .ValidateObservedBoundary(
                checkpoint.Binding,
                checkpoint.Phase,
                progress,
                firstByte,
                trailing);
    }

    private static async ValueTask
        ReadResumeExactlyAsync(
        Stream data,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            int read =
                await data.ReadAsync(
                        destination[offset..],
                        cancellationToken)
                    .ConfigureAwait(false);
            if (read == 0)
            {
                throw new InvalidDataException(
                    "The prepared JSON data ended before its checkpoint boundary.");
            }
            offset += read;
        }
    }

    private static JsonExportCheckpoint
        CreateResumeCheckpoint(
        JsonExportCheckpointBinding binding,
        JsonExportHashManifest bindingDigest,
        long generation,
        JsonExportCheckpointPhase phase,
        long rowCount,
        long? lastCompletedRowId,
        ExportByteSink sink,
        JsonExportHashManifest sourcePrefixDigest,
        JsonExportHashManifest exportedPrefixDigest,
        JsonExportCheckpointCompletion?
            completion) =>
        new()
        {
            Generation = generation,
            Phase = phase,
            Binding = binding,
            BindingDigest = bindingDigest,
            Progress =
                new JsonExportCheckpointProgress
                {
                    CompletedRowCount =
                        rowCount,
                    LastCompletedRowId =
                        lastCompletedRowId,
                    DataPrefixByteLength =
                        sink.BytesWritten,
                    DataPrefixDigest =
                        sink.GetCurrentHash(),
                    LogicalPrefixAggregation =
                        JsonExportCheckpointContracts
                            .LogicalPrefixAggregation,
                    SourceLogicalRowHashPrefixDigest =
                        sourcePrefixDigest,
                    ExportedLogicalRowHashPrefixDigest =
                        exportedPrefixDigest,
                },
            Completion = completion,
        };

    private static async ValueTask
        PersistResumeCheckpointAsync(
        IJsonExportPreparedOutputSession session,
        Stream expectedData,
        JsonExportCheckpoint? previous,
        JsonExportCheckpoint next,
        CancellationToken cancellationToken)
    {
        if (previous is null)
        {
            if (next.Generation != 0 ||
                next.Phase !=
                    JsonExportCheckpointPhase
                        .Writing)
            {
                throw new InvalidOperationException(
                    "The first JSON export checkpoint must be writing generation zero.");
            }
        }
        else
        {
            JsonExportCheckpointFraming
                .ValidateTransition(
                    previous,
                    next);
        }

        _ = JsonExportCheckpointSerializer
            .ComputeCheckpointDigest(next);
        await session
            .PersistCheckpointAsync(
                next,
                cancellationToken)
            .ConfigureAwait(false);

        Stream persistedData =
            session.DataStream
            ?? throw new InvalidDataException(
                "The JSON prepared-output session did not retain its data stream after checkpoint persistence.");
        if (!ReferenceEquals(
                expectedData,
                persistedData))
        {
            throw new InvalidDataException(
                "The JSON prepared-output session replaced its bound data stream during checkpoint persistence.");
        }
        ValidateResumeStreamCapabilities(
            persistedData);
        RequireResumeStreamPosition(
            persistedData,
            next.Progress.DataPrefixByteLength,
            next.Progress.DataPrefixByteLength,
            "The JSON prepared-output session did not retain the exact persisted checkpoint boundary.");
    }

    private static JsonStreamingExportResult
        CreateCompletedJsonResult(
        JsonExportCheckpoint checkpoint)
    {
        JsonExportManifest manifest =
            JsonExportCheckpointSerializer
                .CreateCompletedManifest(
                    checkpoint);
        return new JsonStreamingExportResult
        {
            Manifest = manifest,
            CanonicalManifestBytes =
                JsonExportManifestSerializer
                    .Serialize(manifest),
            ManifestDigest =
                JsonExportManifestSerializer
                    .ComputeManifestDigest(
                        manifest),
        };
    }

    private static IAsyncEnumerable<JsonExportRow>
        OpenResumeRows(
        JsonResumableExportRequest request,
        long? boundary,
        CancellationToken cancellationToken)
    {
        IAsyncEnumerable<JsonExportRow>? rows =
            request.OpenRows(
                boundary,
                cancellationToken);
        return rows
            ?? throw new InvalidDataException(
                "The resumable JSON export row source returned no sequence.");
    }

    private static long NextJsonGeneration(
        long generation)
    {
        if (generation == long.MaxValue)
        {
            throw new InvalidOperationException(
                "JSON export checkpoint generation is exhausted.");
        }
        return generation + 1;
    }

    private static void ValidateResumeStreamCapabilities(
        Stream data)
    {
        if (!data.CanRead ||
            !data.CanWrite ||
            !data.CanSeek)
        {
            throw new InvalidDataException(
                "The JSON prepared-output stream must be readable, writable, and seekable.");
        }
    }

    private static void RequireResumeStreamPosition(
        Stream data,
        long expectedLength,
        long expectedPosition,
        string message)
    {
        long length;
        long position;
        try
        {
            length = data.Length;
            position = data.Position;
        }
        catch (Exception exception) when (
            exception is
                IOException or
                NotSupportedException or
                ObjectDisposedException)
        {
            throw new InvalidDataException(
                "The JSON prepared-output stream did not expose its length and position.");
        }

        if (length != expectedLength ||
            position != expectedPosition)
        {
            throw new InvalidDataException(
                message);
        }
    }

    private static void ResumeRequireHashEquals(
        JsonExportHashManifest actual,
        JsonExportHashManifest expected,
        string message)
    {
        if (actual is null ||
            expected is null ||
            !string.Equals(
                actual.Algorithm,
                expected.Algorithm,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                message);
        }

        byte[] actualBytes;
        byte[] expectedBytes;
        try
        {
            actualBytes =
                Convert.FromHexString(
                    actual.Value);
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
                        actualBytes,
                        expectedBytes))
            {
                throw new InvalidDataException(
                    message);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                actualBytes);
            CryptographicOperations.ZeroMemory(
                expectedBytes);
        }
    }

    private static JsonExportHashManifest
        CreateResumeHashManifest(
        ReadOnlySpan<byte> hash) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = Convert.ToHexString(hash)
                .ToLowerInvariant(),
        };

    private static async IAsyncEnumerable<
        JsonExportRow> ResumeEmptyRows(
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        cancellationToken
            .ThrowIfCancellationRequested();
        await Task.CompletedTask
            .ConfigureAwait(false);
        yield break;
    }

    private sealed record ReplayState(
        long RowCount,
        long? LastCompletedRowId);
}
