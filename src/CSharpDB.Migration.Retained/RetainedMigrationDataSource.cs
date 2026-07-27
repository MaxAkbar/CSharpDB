using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using CSharpDB.Migration;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Migration.Retained;

/// <summary>
/// Reads verified provider-neutral retained table sections in captured order.
/// Resume cursors are integrity-checked and bound to the package, projection,
/// and read policy, but are not authentication tokens. Persist them only in
/// trusted checkpoint storage.
/// </summary>
public sealed class RetainedMigrationDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource
{
    private const int MaximumBufferedRows = 65_536;
    private const int MaximumBufferedScalarValues =
        1_048_576;
    private const long MaximumBufferedCanonicalBytes =
        64L * 1024 * 1024;

    private readonly SafeFileHandle packageHandle;
    private readonly long packageLength;
    private readonly long bodyOffset;
    private readonly IReadOnlyDictionary<
        string,
        RetainedPackageTableBinding> tables;
    private readonly RetainedMigrationPackageOpenOptions
        packageLimits;
    private readonly object gate = new();
    private int activeReaders;
    private int disposed;
    private Task? disposeTask;
    private TaskCompletionSource? readersDrained;

    internal RetainedMigrationDataSource(
        SafeFileHandle packageHandle,
        long packageLength,
        long bodyOffset,
        MigrationCatalog catalog,
        string snapshotIdentity,
        string catalogDigest,
        string packageDigest,
        IReadOnlyList<RetainedPackageTableBinding>
            tables,
        RetainedMigrationPackageOpenOptions
            packageLimits)
    {
        this.packageHandle =
            packageHandle ??
            throw new ArgumentNullException(
                nameof(packageHandle));
        this.packageLength = packageLength;
        this.bodyOffset = bodyOffset;
        this.packageLimits = packageLimits;
        Source = catalog.Source;
        SnapshotIdentity = snapshotIdentity;
        CatalogDigest = catalogDigest;
        PackageDigest = packageDigest;
        this.tables = tables.ToDictionary(
            static item =>
                item.Descriptor.SourceObjectId,
            StringComparer.Ordinal);
    }

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity { get; }

    public string CatalogDigest { get; }

    public string PackageDigest { get; }

    public IAsyncEnumerable<MigrationDataBatch> ReadAsync(
        MigrationReadRequest request,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
        ValidatedRead validated =
            Validate(request);
        return ReadCoreAsync(
            validated,
            cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        lock (gate)
        {
            disposeTask ??=
                DisposeCoreAsync();
            return new ValueTask(disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        Task readersCompleted;
        lock (gate)
        {
            Volatile.Write(
                ref disposed,
                1);
            readersCompleted =
                activeReaders == 0
                    ? Task.CompletedTask
                    : (readersDrained ??=
                        new TaskCompletionSource(
                            TaskCreationOptions
                                .RunContinuationsAsynchronously))
                        .Task;
        }
        await readersCompleted
            .ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void AcquireReader()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(
                disposed != 0,
                this);
            activeReaders++;
        }
    }

    private void ReleaseReader()
    {
        TaskCompletionSource? completed = null;
        lock (gate)
        {
            if (activeReaders <= 0)
            {
                throw new InvalidOperationException(
                    "The retained source reader lease is not active.");
            }
            activeReaders--;
            if (disposed != 0 &&
                activeReaders == 0)
            {
                completed = readersDrained;
            }
        }
        completed?.TrySetResult();
    }

    private async IAsyncEnumerable<MigrationDataBatch>
        ReadCoreAsync(
        ValidatedRead request,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        AcquireReader();
        try
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            await using Stream stream =
                OpenPackageReader();
            if (request.Resume is not null)
            {
                ValidateResumeBoundary(
                    stream,
                    request,
                    cancellationToken);
            }

            long sectionStart = checked(
                bodyOffset +
                request.Table.RelativeOffset);
            long relativeOffset =
                request.Resume?
                    .RelativeOffset ?? 0;
            long rowOrdinal =
                request.Resume?.RowOrdinal ?? 0;
            long batchOrdinal =
                request.Resume?.BatchOrdinal ?? 0;
            string? startCursor =
                request.Resume?.Original;
            stream.Position = checked(
                sectionStart +
                relativeOffset);

            var rows = NewBuffer(
                request.EffectiveMaximumRows);
            long batchBytes = 0;
            while (relativeOffset <
                request.Table.SectionLength)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                long recordStart =
                    relativeOffset;
                DecodedRetainedRow decoded =
                    RetainedMigrationBinaryCodec
                        .ReadRow(
                            stream,
                            rowOrdinal,
                            request.Table
                                .Descriptor
                                .ColumnObjectIds
                                .Count,
                            packageLimits
                                .MaxValueBytes,
                            packageLimits
                                .MaxStableKeyBytes,
                            packageLimits
                                .MaxRowBytes);
                relativeOffset = checked(
                    relativeOffset +
                    decoded.EncodedBytes);
                if (relativeOffset >
                    request.Table.SectionLength)
                {
                    throw new RetainedMigrationPackageException(
                        "A retained row extends beyond its table section.");
                }

                ProjectedRow projected =
                    Project(
                        decoded.Row,
                        request);
                if (rows.Count > 0 &&
                    (rows.Count >=
                         request
                             .EffectiveMaximumRows ||
                     checked(
                         batchBytes +
                         projected.CanonicalBytes) >
                     request
                         .EffectiveMaximumBatchBytes))
                {
                    string nextCursor =
                        RetainedMigrationCursorCodec
                            .Encode(
                                rowOrdinal,
                                recordStart,
                                checked(
                                    batchOrdinal +
                                    1),
                                request.ScopeDigest);
                    yield return CreateBatch(
                        request,
                        rows,
                        batchOrdinal,
                        startCursor,
                        nextCursor);

                    rows = NewBuffer(
                        request
                            .EffectiveMaximumRows);
                    batchBytes = 0;
                    batchOrdinal++;
                    startCursor = nextCursor;
                }

                rows.Add(projected.Row);
                batchBytes = checked(
                    batchBytes +
                    projected.CanonicalBytes);
                rowOrdinal++;
            }

            if (rowOrdinal !=
                request.Table.RowCount)
            {
                throw new RetainedMigrationPackageException(
                    "The retained table row count changed after package verification.");
            }
            if (rows.Count > 0)
            {
                yield return CreateBatch(
                    request,
                    rows,
                    batchOrdinal,
                    startCursor,
                    nextCursor: null);
            }
        }
        finally
        {
            ReleaseReader();
        }
    }

    private void ValidateResumeBoundary(
        Stream stream,
        ValidatedRead request,
        CancellationToken cancellationToken)
    {
        RetainedMigrationCursorCodec.Position
            resume =
            request.Resume ??
            throw new InvalidOperationException(
                "A retained resume boundary cannot be validated without a cursor.");
        if (resume.RowOrdinal >=
                request.Table.RowCount ||
            resume.RelativeOffset >=
                request.Table.SectionLength)
        {
            throw InvalidResumeBoundary();
        }

        long sectionStart = checked(
            bodyOffset +
            request.Table.RelativeOffset);
        stream.Position = sectionStart;
        long relativeOffset = 0;
        long rowOrdinal = 0;
        long batchOrdinal = 0;
        int rowsInBatch = 0;
        long batchBytes = 0;
        while (relativeOffset <
            request.Table.SectionLength)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            long recordStart =
                relativeOffset;
            DecodedRetainedRow decoded =
                RetainedMigrationBinaryCodec
                    .ReadRow(
                        stream,
                        rowOrdinal,
                        request.Table.Descriptor
                            .ColumnObjectIds.Count,
                        packageLimits.MaxValueBytes,
                        packageLimits
                            .MaxStableKeyBytes,
                        packageLimits.MaxRowBytes);
            relativeOffset = checked(
                relativeOffset +
                decoded.EncodedBytes);
            ProjectedRow projected =
                Project(
                    decoded.Row,
                    request);

            if (rowsInBatch > 0 &&
                (rowsInBatch >=
                     request.EffectiveMaximumRows ||
                 checked(
                     batchBytes +
                     projected.CanonicalBytes) >
                 request
                     .EffectiveMaximumBatchBytes))
            {
                batchOrdinal++;
                if (recordStart ==
                        resume.RelativeOffset &&
                    rowOrdinal ==
                        resume.RowOrdinal &&
                    batchOrdinal ==
                        resume.BatchOrdinal)
                {
                    return;
                }
                rowsInBatch = 0;
                batchBytes = 0;
            }
            rowsInBatch++;
            batchBytes = checked(
                batchBytes +
                projected.CanonicalBytes);
            rowOrdinal++;
            if (recordStart >=
                resume.RelativeOffset)
            {
                break;
            }
        }
        throw InvalidResumeBoundary();
    }

    private ValidatedRead Validate(
        MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The batch size must be positive.");
        }
        if (request.MaxBatchBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum batch bytes must be positive.");
        }
        if (request.MaxValueBytes <= 0 ||
            request.MaxValueBytes >
                request.MaxBatchBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum value bytes must be positive and no greater than the batch bound.");
        }
        MigrationRejectReadPolicyValidator
            .Validate(request);
        if (!string.Equals(
                request.RejectContractVersion,
                MigrationRejectContract
                    .DeterministicFailFastV1,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                "Retained packages support deterministic fail-fast replay only.");
        }
        if (request.SnapshotToken is not null &&
            !string.Equals(
                request.SnapshotToken,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained read request snapshot token does not match this package.");
        }
        if (!tables.TryGetValue(
                request.SourceObjectId,
                out RetainedPackageTableBinding?
                    table))
        {
            throw new ArgumentException(
                "The retained source object is not present in this package.",
                nameof(request));
        }
        if (request.ColumnObjectIds is null ||
            request.ColumnObjectIds.Count == 0)
        {
            throw new ArgumentException(
                "At least one retained column must be requested.",
                nameof(request));
        }

        var available =
            table.Descriptor.ColumnObjectIds
                .Select(
                    static (id, index) =>
                        (id, index))
                .ToDictionary(
                    static item => item.id,
                    static item => item.index,
                    StringComparer.Ordinal);
        var seen =
            new HashSet<string>(
                StringComparer.Ordinal);
        var ids =
            new string[
                request.ColumnObjectIds.Count];
        var indexes =
            new int[ids.Length];
        for (int index = 0;
             index < ids.Length;
             index++)
        {
            string? id =
                request.ColumnObjectIds[index];
            if (string.IsNullOrWhiteSpace(id) ||
                !seen.Add(id) ||
                !available.TryGetValue(
                    id,
                    out int storedIndex))
            {
                throw new ArgumentException(
                    "The retained column projection contains an unknown or duplicate identifier.",
                    nameof(request));
            }
            ids[index] = id;
            indexes[index] =
                storedIndex;
        }

        int maximumRowsByScalarCount =
            Math.Max(
                1,
                MaximumBufferedScalarValues /
                indexes.Length);
        int effectiveMaximumRows =
            Math.Min(
                request.BatchSize,
                Math.Min(
                    MaximumBufferedRows,
                    maximumRowsByScalarCount));
        long effectiveMaximumBatchBytes =
            Math.Min(
                request.MaxBatchBytes,
                MaximumBufferedCanonicalBytes);
        int maximumValueBytes =
            checked((int)Math.Min(
                request.MaxValueBytes,
                effectiveMaximumBatchBytes));
        ReadOnlyCollection<string> frozenIds =
            Array.AsReadOnly(ids);
        string scopeDigest =
            RetainedMigrationCursorCodec
                .ComputeScope(
                    PackageDigest,
                    CatalogDigest,
                    Source.Identity,
                    Source.Fingerprint,
                    SnapshotIdentity,
                    table.Descriptor
                        .SourceObjectId,
                    frozenIds,
                    request.BatchSize,
                    request.MaxBatchBytes,
                    request.MaxValueBytes,
                    request
                        .RejectContractVersion);

        RetainedMigrationCursorCodec.Position?
            resume = null;
        if (request.ResumeCursor is not null)
        {
            if (!string.Equals(
                    request.SnapshotToken,
                    SnapshotIdentity,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A retained resume cursor requires the exact package snapshot token.");
            }
            resume =
                RetainedMigrationCursorCodec
                    .Parse(
                        request.ResumeCursor,
                        scopeDigest);
        }
        return new ValidatedRead(
            table,
            frozenIds,
            indexes,
            effectiveMaximumRows,
            effectiveMaximumBatchBytes,
            maximumValueBytes,
            scopeDigest,
            resume);
    }

    private static ProjectedRow Project(
        MigrationDataRow stored,
        ValidatedRead request)
    {
        var values =
            new MigrationSourceValue[
                request.StoredColumnIndexes
                    .Length];
        long canonicalBytes =
            1 + sizeof(int);
        if (stored.StableKey is not null)
        {
            canonicalBytes = checked(
                canonicalBytes +
                sizeof(int) +
                RetainedMigrationBinaryCodec
                    .GetUtf8ByteCount(
                        stored.StableKey,
                        "stable key"));
        }
        for (int index = 0;
             index < values.Length;
             index++)
        {
            MigrationSourceValue value =
                stored.Values[
                    request
                        .StoredColumnIndexes[
                            index]];
            int payloadBytes =
                value.Kind switch
                {
                    MigrationSourceValueKind.Null =>
                        0,
                    MigrationSourceValueKind.Binary =>
                        value.BinaryValue.Length,
                    _ =>
                        RetainedMigrationBinaryCodec
                            .GetUtf8ByteCount(
                                value.CanonicalText ??
                                throw new RetainedMigrationPackageException(
                                    "A retained scalar lost its canonical text."),
                                "canonical scalar"),
                };
            if (payloadBytes >
                request.MaximumValueBytes)
            {
                throw new InvalidDataException(
                    "A retained scalar exceeds the read request value bound.");
            }
            canonicalBytes = checked(
                canonicalBytes +
                1 + sizeof(int) +
                payloadBytes);
            values[index] = value;
        }
        if (canonicalBytes >
            request.EffectiveMaximumBatchBytes)
        {
            throw new InvalidDataException(
                "A retained row exceeds the read request batch-byte bound.");
        }
        return new ProjectedRow(
            new MigrationDataRow
            {
                StableKey =
                    stored.StableKey,
                Values =
                    Array.AsReadOnly(values),
            },
            canonicalBytes);
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        long batchOrdinal,
        string? startCursor,
        string? nextCursor) =>
        new()
        {
            SourceObjectId =
                request.Table.Descriptor
                    .SourceObjectId,
            SnapshotIdentity =
                SnapshotIdentity,
            ColumnObjectIds =
                request.ColumnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = startCursor,
            NextCursor = nextCursor,
            Rows = rows.AsReadOnly(),
        };

    private Stream OpenPackageReader() =>
        new RetainedPackageReadStream(
            packageHandle,
            packageLength);

    private static List<MigrationDataRow>
        NewBuffer(int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private static InvalidDataException
        InvalidResumeBoundary() =>
        new(
            "The retained resume cursor does not identify an emitted batch boundary.");

    private sealed record ValidatedRead(
        RetainedPackageTableBinding Table,
        ReadOnlyCollection<string> ColumnObjectIds,
        int[] StoredColumnIndexes,
        int EffectiveMaximumRows,
        long EffectiveMaximumBatchBytes,
        int MaximumValueBytes,
        string ScopeDigest,
        RetainedMigrationCursorCodec.Position? Resume);

    private sealed record ProjectedRow(
        MigrationDataRow Row,
        long CanonicalBytes);

    private sealed class RetainedPackageReadStream(
        SafeFileHandle handle,
        long length) : Stream
    {
        private long position;

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => length;

        public override long Position
        {
            get => position;
            set
            {
                if (value < 0 ||
                    value > length)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(value));
                }
                position = value;
            }
        }

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            Read(
                buffer.AsSpan(
                    offset,
                    count));

        public override int Read(
            Span<byte> buffer)
        {
            if (position >= length)
                return 0;
            int boundedCount =
                checked((int)Math.Min(
                    buffer.Length,
                    length - position));
            int read =
                RandomAccess.Read(
                    handle,
                    buffer[..boundedCount],
                    position);
            position = checked(
                position + read);
            return read;
        }

        public override long Seek(
            long offset,
            SeekOrigin origin)
        {
            long target = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current =>
                    checked(position + offset),
                SeekOrigin.End =>
                    checked(length + offset),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(origin)),
            };
            Position = target;
            return target;
        }

        public override void Flush()
        {
        }

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();
    }
}
