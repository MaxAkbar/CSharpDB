using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Complete input for one durable, resumable CSV prepared output.
/// Every sequence returned by <see cref="OpenRows"/> must read the same
/// immutable source snapshot described by <see cref="Source"/> and
/// <see cref="SourceSnapshotIdentity"/>.
/// </summary>
public sealed record CsvResumableExportRequest
{
    /// <summary>The fully qualified, normalized future CSV destination.</summary>
    public required string DestinationPath { get; init; }

    public required CsvExportProfile Profile { get; init; }

    public required CsvExportSourceManifest Source { get; init; }

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
        IAsyncEnumerable<CsvExportRow>> OpenRows
    { get; init; }

    /// <summary>Maximum number of CSV bytes that may be prepared.</summary>
    public long MaxDataBytes { get; init; } = 1L << 40;

    /// <summary>
    /// Per-value decoded BLOB ceiling recorded in every BLOB column contract.
    /// </summary>
    public int MaximumDecodedBlobBytes { get; init; } =
        CsvExportContracts.MaximumSupportedDecodedBlobBytes;

    /// <summary>
    /// Number of newly completed rows between durable writing checkpoints.
    /// </summary>
    public long CheckpointRowInterval { get; init; } = 10_000;
}

public sealed partial class CsvStreamingExporter
{
    private const int ResumeHashBufferBytes = 64 * 1024;

    /// <summary>
    /// Creates or resumes a durable private CSV prepared output. The final CSV
    /// and manifest destinations are not published by this method. Private
    /// bytes without an active checkpoint are explicitly reset because they
    /// carry no authoritative progress.
    /// </summary>
    public async ValueTask<CsvStreamingExportResult> WriteResumableAsync(
        CsvResumableExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.OpenRows is null)
        {
            throw new ArgumentException(
                "A resumable CSV export row source is required.",
                nameof(request));
        }
        if (request.CheckpointRowInterval <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The CSV export checkpoint row interval must be positive.");
        }

        PreparedRequest prepared = PrepareRequest(new CsvStreamingExportRequest
        {
            Profile = request.Profile,
            Source = request.Source,
            Table = request.Table,
            Rows = EmptyRows(),
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes = request.MaximumDecodedBlobBytes,
        });
        var binding = new CsvExportCheckpointBinding
        {
            Profile = prepared.Profile,
            Source = prepared.Source,
            SourceSnapshotIdentity = request.SourceSnapshotIdentity,
            Table = prepared.Table,
            Csv = prepared.Format,
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes = request.MaximumDecodedBlobBytes,
        };
        CsvExportHashManifest bindingDigest =
            CsvExportCheckpointSerializer.ComputeBindingDigest(binding);

        cancellationToken.ThrowIfCancellationRequested();
        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                    request.DestinationPath,
                    binding,
                    cancellationToken)
                .ConfigureAwait(false);

        if (lease.State == CsvExportPreparedOutputState.UncheckpointedData)
        {
            // No bytes are authoritative without generation zero. Reset is
            // explicit at the lease boundary and affects only its private file.
            await lease.ResetUncheckpointedAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        using var sourceDigest = new CsvExportOrderedContentDigest();
        using var exportedDigest = new CsvExportOrderedContentDigest();

        long rowCount;
        long? lastCompletedRowId;
        long transformedRows;
        long transformedCells;
        long generation;
        ExportByteSink sink;

        if (lease.State == CsvExportPreparedOutputState.Recovered)
        {
            CsvExportCheckpoint checkpoint = lease.CurrentCheckpoint
                ?? throw new InvalidOperationException(
                    "A recovered CSV prepared output has no active checkpoint.");
            ReplayState replay = await ReplayAndVerifyAsync(
                    request,
                    prepared,
                    checkpoint,
                    sourceDigest,
                    exportedDigest,
                    cancellationToken)
                .ConfigureAwait(false);

            if (checkpoint.Phase == CsvExportCheckpointPhase.DataComplete)
                return CreateCompletedResult(checkpoint);

            sink = await CreateSeededSinkAsync(
                    lease.DataStream,
                    request.MaxDataBytes,
                    checkpoint.Progress,
                    cancellationToken)
                .ConfigureAwait(false);
            rowCount = replay.RowCount;
            lastCompletedRowId = replay.LastCompletedRowId;
            transformedRows = replay.TransformedRowCount;
            transformedCells = replay.TransformedCellCount;
            generation = checkpoint.Generation;
        }
        else
        {
            sink = new ExportByteSink(lease.DataStream, request.MaxDataBytes);
            try
            {
                await WriteHeaderAsync(sink, prepared, cancellationToken)
                    .ConfigureAwait(false);

                CsvExportCheckpoint headerCheckpoint = CreateCheckpoint(
                    binding,
                    bindingDigest,
                    generation: 0,
                    CsvExportCheckpointPhase.Writing,
                    rowCount: 0,
                    lastCompletedRowId: null,
                    sink,
                    sourceDigest.GetCurrentPrefixDigest(),
                    exportedDigest.GetCurrentPrefixDigest(),
                    transformedRows: 0,
                    transformedCells: 0,
                    completion: null);
                await lease.PersistCheckpointAsync(
                        headerCheckpoint,
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
            transformedRows = 0;
            transformedCells = 0;
            generation = 0;
        }

        using (sink)
        {
            long rowsSinceCheckpoint = 0;
            IAsyncEnumerable<CsvExportRow> continuation =
                OpenRows(request, lastCompletedRowId, cancellationToken);
            await foreach (CsvExportRow row in continuation
                               .WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                if (lastCompletedRowId is long previousRowId &&
                    row.RowId <= previousRowId)
                {
                    throw new InvalidDataException(
                        "CSV export rows must have strictly increasing physical row IDs.");
                }

                PreparedRow preparedRow = PrepareRow(
                    row,
                    prepared,
                    sink.BytesWritten,
                    request.MaxDataBytes);
                try
                {
                    await WriteRowAsync(sink, preparedRow, cancellationToken)
                        .ConfigureAwait(false);
                    sourceDigest.AppendRow(preparedRow.SourceLogicalValues);
                    exportedDigest.AppendRow(preparedRow.ExportedLogicalValues);
                    if (preparedRow.TransformedCellCount != 0)
                    {
                        transformedRows = checked(transformedRows + 1);
                        transformedCells = checked(
                            transformedCells +
                            preparedRow.TransformedCellCount);
                    }

                    rowCount = checked(rowCount + 1);
                    rowsSinceCheckpoint = checked(rowsSinceCheckpoint + 1);
                    lastCompletedRowId = row.RowId;
                }
                finally
                {
                    preparedRow.ClearSensitiveBuffers();
                }

                if (rowsSinceCheckpoint < request.CheckpointRowInterval)
                    continue;

                generation = NextGeneration(generation);
                CsvExportCheckpoint writingCheckpoint = CreateCheckpoint(
                    binding,
                    bindingDigest,
                    generation,
                    CsvExportCheckpointPhase.Writing,
                    rowCount,
                    lastCompletedRowId,
                    sink,
                    sourceDigest.GetCurrentPrefixDigest(),
                    exportedDigest.GetCurrentPrefixDigest(),
                    transformedRows,
                    transformedCells,
                    completion: null);
                await lease.PersistCheckpointAsync(
                        writingCheckpoint,
                        cancellationToken)
                    .ConfigureAwait(false);
                rowsSinceCheckpoint = 0;
            }

            if (sourceDigest.RowCount != rowCount ||
                exportedDigest.RowCount != rowCount)
            {
                throw new InvalidOperationException(
                    "CSV export logical row counts diverged.");
            }

            CsvExportHashManifest dataDigest = sink.GetCurrentHash();
            CsvExportHashManifest sourcePrefixDigest =
                sourceDigest.GetCurrentPrefixDigest();
            CsvExportHashManifest exportedPrefixDigest =
                exportedDigest.GetCurrentPrefixDigest();
            CsvExportHashManifest sourceLogicalDigest = sourceDigest.Complete();
            CsvExportHashManifest exportedLogicalDigest =
                exportedDigest.Complete();
            CsvExportManifest manifest = CreateManifest(
                prepared,
                rowCount,
                sink.BytesWritten,
                dataDigest,
                sourceLogicalDigest,
                exportedLogicalDigest,
                transformedRows,
                transformedCells);
            byte[] canonicalManifestBytes =
                CsvExportManifestSerializer.Serialize(manifest);
            string manifestDigest =
                CsvExportManifestSerializer.ComputeManifestDigest(manifest);

            var completion = new CsvExportCheckpointCompletion
            {
                SourceLogicalDigest = sourceLogicalDigest,
                ExportedLogicalDigest = exportedLogicalDigest,
                ManifestDigest = manifestDigest,
            };
            generation = NextGeneration(generation);
            CsvExportCheckpoint completedCheckpoint = CreateCheckpoint(
                binding,
                bindingDigest,
                generation,
                CsvExportCheckpointPhase.DataComplete,
                rowCount,
                lastCompletedRowId,
                sink,
                sourcePrefixDigest,
                exportedPrefixDigest,
                transformedRows,
                transformedCells,
                completion);
            await lease.PersistCheckpointAsync(
                    completedCheckpoint,
                    cancellationToken)
                .ConfigureAwait(false);

            return new CsvStreamingExportResult
            {
                Manifest = manifest,
                CanonicalManifestBytes = canonicalManifestBytes,
                ManifestDigest = manifestDigest,
            };
        }
    }

    private static async ValueTask<ReplayState> ReplayAndVerifyAsync(
        CsvResumableExportRequest request,
        PreparedRequest prepared,
        CsvExportCheckpoint checkpoint,
        CsvExportOrderedContentDigest sourceDigest,
        CsvExportOrderedContentDigest exportedDigest,
        CancellationToken cancellationToken)
    {
        CsvExportCheckpointProgress progress = checkpoint.Progress;
        using var replaySink =
            new ExportByteSink(Stream.Null, request.MaxDataBytes);
        await WriteHeaderAsync(replaySink, prepared, cancellationToken)
            .ConfigureAwait(false);

        long rowCount = 0;
        long? previousRowId = null;
        long transformedRows = 0;
        long transformedCells = 0;
        IAsyncEnumerable<CsvExportRow> replayRows =
            OpenRows(request, boundary: null, cancellationToken);
        await using IAsyncEnumerator<CsvExportRow> enumerator =
            replayRows.GetAsyncEnumerator(cancellationToken);

        while (rowCount < progress.CompletedRowCount)
        {
            if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "The CSV export source ended before the durable checkpoint row boundary.");
            }

            CsvExportRow row = enumerator.Current;
            if (previousRowId is long previous && row.RowId <= previous)
            {
                throw new InvalidDataException(
                    "CSV export replay rows must have strictly increasing physical row IDs.");
            }

            PreparedRow preparedRow = PrepareRow(
                row,
                prepared,
                replaySink.BytesWritten,
                request.MaxDataBytes);
            try
            {
                await WriteRowAsync(
                        replaySink,
                        preparedRow,
                        cancellationToken)
                    .ConfigureAwait(false);
                sourceDigest.AppendRow(preparedRow.SourceLogicalValues);
                exportedDigest.AppendRow(preparedRow.ExportedLogicalValues);
                if (preparedRow.TransformedCellCount != 0)
                {
                    transformedRows = checked(transformedRows + 1);
                    transformedCells = checked(
                        transformedCells +
                        preparedRow.TransformedCellCount);
                }

                rowCount = checked(rowCount + 1);
                previousRowId = row.RowId;
            }
            finally
            {
                preparedRow.ClearSensitiveBuffers();
            }
        }

        if (previousRowId != progress.LastCompletedRowId)
        {
            throw new InvalidDataException(
                "The CSV export replay row boundary does not match the durable checkpoint.");
        }
        if (replaySink.BytesWritten != progress.DataPrefixByteLength)
        {
            throw new InvalidDataException(
                "The CSV export replay byte length does not match the durable checkpoint.");
        }
        RequireHashEquals(
            replaySink.GetCurrentHash(),
            progress.DataPrefixDigest,
            "The CSV export replay bytes do not match the durable checkpoint.");
        RequireHashEquals(
            sourceDigest.GetCurrentPrefixDigest(),
            progress.SourceLogicalRowHashPrefixDigest,
            "The CSV export replay source values do not match the durable checkpoint.");
        RequireHashEquals(
            exportedDigest.GetCurrentPrefixDigest(),
            progress.ExportedLogicalRowHashPrefixDigest,
            "The CSV export replay output values do not match the durable checkpoint.");
        if (sourceDigest.RowCount != progress.CompletedRowCount ||
            exportedDigest.RowCount != progress.CompletedRowCount ||
            transformedRows != progress.TransformedRowCount ||
            transformedCells != progress.TransformedCellCount)
        {
            throw new InvalidDataException(
                "The CSV export replay counters do not match the durable checkpoint.");
        }

        if (checkpoint.Phase == CsvExportCheckpointPhase.DataComplete)
        {
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "The CSV export source contains rows beyond its data-complete checkpoint.");
            }

            CsvExportCheckpointCompletion completion = checkpoint.Completion
                ?? throw new InvalidDataException(
                    "The data-complete CSV export checkpoint has no completion evidence.");
            RequireHashEquals(
                sourceDigest.Complete(),
                completion.SourceLogicalDigest,
                "The completed CSV export source digest does not match replay.");
            RequireHashEquals(
                exportedDigest.Complete(),
                completion.ExportedLogicalDigest,
                "The completed CSV export output digest does not match replay.");
        }

        return new ReplayState(
            rowCount,
            previousRowId,
            transformedRows,
            transformedCells);
    }

    private static async ValueTask<ExportByteSink> CreateSeededSinkAsync(
        Stream data,
        long maximumBytes,
        CsvExportCheckpointProgress progress,
        CancellationToken cancellationToken)
    {
        var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ResumeHashBufferBytes);
        try
        {
            data.Position = 0;
            long remaining = progress.DataPrefixByteLength;
            while (remaining > 0)
            {
                int requested = (int)Math.Min(remaining, buffer.Length);
                int read = await data.ReadAsync(
                        buffer.AsMemory(0, requested),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                {
                    throw new InvalidDataException(
                        "The prepared CSV data ended before its checkpoint boundary.");
                }

                hash.AppendData(buffer, 0, read);
                remaining -= read;
            }

            RequireHashEquals(
                CreateHashManifest(hash.GetCurrentHash()),
                progress.DataPrefixDigest,
                "The prepared CSV prefix changed while resume state was reconstructed.");
            data.Position = progress.DataPrefixByteLength;
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
                buffer.AsSpan(0, buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static CsvExportCheckpoint CreateCheckpoint(
        CsvExportCheckpointBinding binding,
        CsvExportHashManifest bindingDigest,
        long generation,
        CsvExportCheckpointPhase phase,
        long rowCount,
        long? lastCompletedRowId,
        ExportByteSink sink,
        CsvExportHashManifest sourcePrefixDigest,
        CsvExportHashManifest exportedPrefixDigest,
        long transformedRows,
        long transformedCells,
        CsvExportCheckpointCompletion? completion) => new()
        {
            Generation = generation,
            Phase = phase,
            Binding = binding,
            BindingDigest = bindingDigest,
            Progress = new CsvExportCheckpointProgress
            {
                CompletedRowCount = rowCount,
                LastCompletedRowId = lastCompletedRowId,
                DataPrefixByteLength = sink.BytesWritten,
                DataPrefixDigest = sink.GetCurrentHash(),
                LogicalPrefixAggregation =
                    CsvExportCheckpointContracts.LogicalPrefixAggregation,
                SourceLogicalRowHashPrefixDigest = sourcePrefixDigest,
                ExportedLogicalRowHashPrefixDigest = exportedPrefixDigest,
                TransformedRowCount = transformedRows,
                TransformedCellCount = transformedCells,
            },
            Completion = completion,
        };

    private static CsvStreamingExportResult CreateCompletedResult(
        CsvExportCheckpoint checkpoint)
    {
        CsvExportManifest manifest =
            CsvExportCheckpointSerializer.CreateCompletedManifest(checkpoint);
        return new CsvStreamingExportResult
        {
            Manifest = manifest,
            CanonicalManifestBytes =
                CsvExportManifestSerializer.Serialize(manifest),
            ManifestDigest =
                CsvExportManifestSerializer.ComputeManifestDigest(manifest),
        };
    }

    private static IAsyncEnumerable<CsvExportRow> OpenRows(
        CsvResumableExportRequest request,
        long? boundary,
        CancellationToken cancellationToken)
    {
        IAsyncEnumerable<CsvExportRow>? rows =
            request.OpenRows(boundary, cancellationToken);
        return rows ?? throw new InvalidDataException(
            "The resumable CSV export row source returned no sequence.");
    }

    private static long NextGeneration(long generation)
    {
        if (generation == long.MaxValue)
        {
            throw new InvalidOperationException(
                "CSV export checkpoint generation is exhausted.");
        }
        return generation + 1;
    }

    private static void RequireHashEquals(
        CsvExportHashManifest actual,
        CsvExportHashManifest expected,
        string message)
    {
        byte[] actualBytes = Convert.FromHexString(actual.Value);
        byte[] expectedBytes = Convert.FromHexString(expected.Value);
        try
        {
            if (!string.Equals(
                    actual.Algorithm,
                    expected.Algorithm,
                    StringComparison.Ordinal) ||
                !CryptographicOperations.FixedTimeEquals(
                    actualBytes,
                    expectedBytes))
            {
                throw new InvalidDataException(message);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(actualBytes);
            CryptographicOperations.ZeroMemory(expectedBytes);
        }
    }

    private static CsvExportHashManifest CreateHashManifest(
        ReadOnlySpan<byte> hash) => new()
        {
            Algorithm = CsvExportHashManifest.Sha256Algorithm,
            Value = Convert.ToHexString(hash).ToLowerInvariant(),
        };

    private static async IAsyncEnumerable<CsvExportRow> EmptyRows(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    private sealed record ReplayState(
        long RowCount,
        long? LastCompletedRowId,
        long TransformedRowCount,
        long TransformedCellCount);
}
