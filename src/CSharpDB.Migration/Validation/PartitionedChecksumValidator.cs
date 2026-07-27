using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Validation;

public sealed record PartitionedChecksumValidatorOptions
{
    public required string SpillRootDirectory { get; init; }

    public long SortMemoryBudgetBytes { get; init; } = 8L * 1024 * 1024;

    public long MaxSpillBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    public int MergeFanIn { get; init; } = 16;

    public int MaxOpenFiles { get; init; } = 32;

    public int MaxOpenPartitionWriters { get; init; } = 16;

    public int MaxMismatchDetailsPerPartition { get; init; } = 100;
}

public sealed record PartitionedChecksumValidationResult
{
    public required MigrationValidationStatus Status { get; init; }

    public long SourceRowCount { get; init; }

    public long TargetRowCount { get; init; }

    public required string SourceChecksum { get; init; }

    public required string TargetChecksum { get; init; }

    public long PeakSpillBytes { get; init; }

    public IReadOnlyList<MigrationValidationPartitionEvidence> Partitions { get; init; } = [];
}

/// <summary>
/// Computes duplicate-preserving, order-independent object checksums using 256
/// deterministic SHA-256 partitions and a bounded external merge sort.
/// </summary>
public sealed class PartitionedChecksumValidator
{
    private const int PartitionCount = 256;
    private static readonly byte[] s_objectDigestDomain =
        Encoding.ASCII.GetBytes("csharpdb-validation-object/v1");
    private static readonly byte[] s_partitionDigestDomain =
        Encoding.ASCII.GetBytes("csharpdb-validation-partition/v1");

    public async ValueTask<PartitionedChecksumValidationResult> ValidateAsync(
        CanonicalRowContract contract,
        IAsyncEnumerable<MigrationValidationRow> sourceRows,
        IAsyncEnumerable<MigrationValidationRow> targetRows,
        PartitionedChecksumValidatorOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(sourceRows);
        ArgumentNullException.ThrowIfNull(targetRows);
        ValidateOptions(options);

        await using var workspace = new ValidationSpillWorkspace(
            options.SpillRootDirectory,
            options.MaxSpillBytes);
        var sourceSpools = new PartitionSpoolSet(
            workspace,
            "source",
            options.MaxOpenPartitionWriters,
            options.MaxSpillBytes);
        var targetSpools = new PartitionSpoolSet(
            workspace,
            "target",
            options.MaxOpenPartitionWriters,
            options.MaxSpillBytes);

        try
        {
            await SpoolRowsAsync(contract, sourceRows, sourceSpools, cancellationToken).ConfigureAwait(false);
            PartitionSpool[] source = await sourceSpools.CompleteAsync(cancellationToken).ConfigureAwait(false);
            long remainingSpillBytes = checked(options.MaxSpillBytes - workspace.LiveSpillBytes);
            targetSpools.SetRemainingBudget(remainingSpillBytes);
            await SpoolRowsAsync(contract, targetRows, targetSpools, cancellationToken).ConfigureAwait(false);
            PartitionSpool[] target = await targetSpools.CompleteAsync(cancellationToken).ConfigureAwait(false);

            var sorter = new ExternalHashRecordSorter(
                workspace,
                new ExternalHashRecordSorterOptions
                {
                    MemoryBudgetBytes = options.SortMemoryBudgetBytes,
                    MergeFanIn = options.MergeFanIn,
                    MaxOpenFiles = options.MaxOpenFiles,
                });

            using IncrementalHash sourceObjectHash = CreateDigest(
                s_objectDigestDomain,
                contract,
                partitionId: null,
                sourceSpools.RowCount);
            using IncrementalHash targetObjectHash = CreateDigest(
                s_objectDigestDomain,
                contract,
                partitionId: null,
                targetSpools.RowCount);
            var partitions = new MigrationValidationPartitionEvidence[PartitionCount];

            for (int partitionId = 0; partitionId < PartitionCount; partitionId++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ExternalHashRecordSortResult? sourceSorted = null;
                ExternalHashRecordSortResult? targetSorted = null;
                try
                {
                    if (source[partitionId].RecordCount > 0)
                    {
                        sourceSorted = await sorter.SortAsync(
                            ReadSpoolAsync(source[partitionId], cancellationToken),
                            cancellationToken).ConfigureAwait(false);
                        workspace.DeleteFile(source[partitionId].Path!);
                    }
                    if (target[partitionId].RecordCount > 0)
                    {
                        targetSorted = await sorter.SortAsync(
                            ReadSpoolAsync(target[partitionId], cancellationToken),
                            cancellationToken).ConfigureAwait(false);
                        workspace.DeleteFile(target[partitionId].Path!);
                    }

                    partitions[partitionId] = await ComparePartitionAsync(
                        contract,
                        partitionId,
                        source[partitionId].RecordCount,
                        target[partitionId].RecordCount,
                        sourceSorted,
                        targetSorted,
                        sourceObjectHash,
                        targetObjectHash,
                        options.MaxMismatchDetailsPerPartition,
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    if (sourceSorted is not null && File.Exists(sourceSorted.SpillFilePath))
                        workspace.DeleteFile(sourceSorted.SpillFilePath);
                    if (targetSorted is not null && File.Exists(targetSorted.SpillFilePath))
                        workspace.DeleteFile(targetSorted.SpillFilePath);
                }
            }

            string sourceChecksum = Hex(sourceObjectHash.GetHashAndReset());
            string targetChecksum = Hex(targetObjectHash.GetHashAndReset());
            MigrationValidationStatus status =
                sourceSpools.RowCount == targetSpools.RowCount &&
                string.Equals(sourceChecksum, targetChecksum, StringComparison.Ordinal)
                    ? MigrationValidationStatus.Passed
                    : MigrationValidationStatus.Different;

            return new PartitionedChecksumValidationResult
            {
                Status = status,
                SourceRowCount = sourceSpools.RowCount,
                TargetRowCount = targetSpools.RowCount,
                SourceChecksum = sourceChecksum,
                TargetChecksum = targetChecksum,
                PeakSpillBytes = workspace.MaximumSpillBytes,
                Partitions = partitions,
            };
        }
        finally
        {
            await sourceSpools.DisposeAsync().ConfigureAwait(false);
            await targetSpools.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask SpoolRowsAsync(
        CanonicalRowContract contract,
        IAsyncEnumerable<MigrationValidationRow> rows,
        PartitionSpoolSet spools,
        CancellationToken cancellationToken)
    {
        await foreach (MigrationValidationRow row in rows
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (row is null || row.Values is null)
                throw new InvalidDataException("Validation snapshot emitted a null row or value collection.");

            CanonicalValue[] projected = CanonicalRowProjector.ProjectRow(contract, row.Values);
            byte[] rowHash = CanonicalRowCodec.ComputeRowHashBytes(projected);
            byte[] firstHash = contract.IsKeyed
                ? CanonicalRowCodec.ComputeKeyHashBytes(CanonicalRowProjector.ProjectKey(contract, projected))
                : rowHash;
            var record = new ValidationHashRecord(firstHash, rowHash);
            await spools.AppendAsync(firstHash[0], record.ToArray(), cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask<MigrationValidationPartitionEvidence> ComparePartitionAsync(
        CanonicalRowContract contract,
        int partitionId,
        long sourceCount,
        long targetCount,
        ExternalHashRecordSortResult? sourceSorted,
        ExternalHashRecordSortResult? targetSorted,
        IncrementalHash sourceObjectHash,
        IncrementalHash targetObjectHash,
        int maximumDetails,
        CancellationToken cancellationToken)
    {
        using IncrementalHash sourcePartitionHash = CreateDigest(
            s_partitionDigestDomain,
            contract,
            partitionId,
            sourceCount);
        using IncrementalHash targetPartitionHash = CreateDigest(
            s_partitionDigestDomain,
            contract,
            partitionId,
            targetCount);
        await using var source = new GroupCursor(
            sourceSorted,
            contract.IsKeyed,
            sourcePartitionHash,
            sourceObjectHash,
            cancellationToken);
        await using var target = new GroupCursor(
            targetSorted,
            contract.IsKeyed,
            targetPartitionHash,
            targetObjectHash,
            cancellationToken);
        await source.InitializeAsync().ConfigureAwait(false);
        await target.InitializeAsync().ConfigureAwait(false);

        var mismatches = new List<MigrationValidationMismatchEvidence>();
        while (source.HasCurrent || target.HasCurrent)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!source.HasCurrent)
            {
                AddTargetOnly(mismatches, target.Current, maximumDetails, contract.IsKeyed);
                await target.MoveNextAsync().ConfigureAwait(false);
                continue;
            }
            if (!target.HasCurrent)
            {
                AddSourceOnly(mismatches, source.Current, maximumDetails, contract.IsKeyed);
                await source.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            HashGroup sourceGroup = source.Current;
            HashGroup targetGroup = target.Current;
            int comparison = contract.IsKeyed
                ? sourceGroup.FirstHash.AsSpan().SequenceCompareTo(targetGroup.FirstHash)
                : CompareTuple(sourceGroup, targetGroup);
            if (comparison < 0)
            {
                AddSourceOnly(mismatches, sourceGroup, maximumDetails, contract.IsKeyed);
                await source.MoveNextAsync().ConfigureAwait(false);
                continue;
            }
            if (comparison > 0)
            {
                AddTargetOnly(mismatches, targetGroup, maximumDetails, contract.IsKeyed);
                await target.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            if (contract.IsKeyed)
            {
                if (!sourceGroup.SecondHash.AsSpan().SequenceEqual(targetGroup.SecondHash))
                    AddChanged(mismatches, sourceGroup, targetGroup, maximumDetails);
            }
            else if (sourceGroup.Multiplicity != targetGroup.Multiplicity)
            {
                if (sourceGroup.Multiplicity > targetGroup.Multiplicity)
                {
                    AddSourceOnly(
                        mismatches,
                        sourceGroup with
                        {
                            Multiplicity = sourceGroup.Multiplicity - targetGroup.Multiplicity,
                        },
                        maximumDetails,
                        keyed: false);
                }
                else
                {
                    AddTargetOnly(
                        mismatches,
                        targetGroup with
                        {
                            Multiplicity = targetGroup.Multiplicity - sourceGroup.Multiplicity,
                        },
                        maximumDetails,
                        keyed: false);
                }
            }

            await source.MoveNextAsync().ConfigureAwait(false);
            await target.MoveNextAsync().ConfigureAwait(false);
        }

        string sourceDigest = Hex(sourcePartitionHash.GetHashAndReset());
        string targetDigest = Hex(targetPartitionHash.GetHashAndReset());
        return new MigrationValidationPartitionEvidence
        {
            PartitionId = partitionId,
            Status = sourceCount == targetCount &&
                string.Equals(sourceDigest, targetDigest, StringComparison.Ordinal)
                    ? MigrationValidationStatus.Passed
                    : MigrationValidationStatus.Different,
            SourceRowCount = sourceCount,
            TargetRowCount = targetCount,
            SourceDigest = sourceDigest,
            TargetDigest = targetDigest,
            Mismatches = mismatches,
        };
    }

    private static void AddSourceOnly(
        List<MigrationValidationMismatchEvidence> output,
        HashGroup group,
        int maximum,
        bool keyed)
    {
        if (output.Count >= maximum)
            return;
        output.Add(new MigrationValidationMismatchEvidence
        {
            Kind = MigrationValidationMismatchKind.SourceOnly,
            KeyHash = keyed ? Hex(group.FirstHash) : null,
            SourceRowHash = Hex(group.SecondHash),
            SourceMultiplicity = group.Multiplicity,
            TargetMultiplicity = 0,
        });
    }

    private static void AddTargetOnly(
        List<MigrationValidationMismatchEvidence> output,
        HashGroup group,
        int maximum,
        bool keyed)
    {
        if (output.Count >= maximum)
            return;
        output.Add(new MigrationValidationMismatchEvidence
        {
            Kind = MigrationValidationMismatchKind.TargetOnly,
            KeyHash = keyed ? Hex(group.FirstHash) : null,
            TargetRowHash = Hex(group.SecondHash),
            SourceMultiplicity = 0,
            TargetMultiplicity = group.Multiplicity,
        });
    }

    private static void AddChanged(
        List<MigrationValidationMismatchEvidence> output,
        HashGroup source,
        HashGroup target,
        int maximum)
    {
        if (output.Count >= maximum)
            return;
        output.Add(new MigrationValidationMismatchEvidence
        {
            Kind = MigrationValidationMismatchKind.Changed,
            KeyHash = Hex(source.FirstHash),
            SourceRowHash = Hex(source.SecondHash),
            TargetRowHash = Hex(target.SecondHash),
            SourceMultiplicity = source.Multiplicity,
            TargetMultiplicity = target.Multiplicity,
        });
    }

    private static int CompareTuple(HashGroup left, HashGroup right)
    {
        int first = left.FirstHash.AsSpan().SequenceCompareTo(right.FirstHash);
        return first != 0 ? first : left.SecondHash.AsSpan().SequenceCompareTo(right.SecondHash);
    }

    private static IncrementalHash CreateDigest(
        ReadOnlySpan<byte> domain,
        CanonicalRowContract contract,
        int? partitionId,
        long rowCount)
    {
        var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(domain);
        hash.AppendData(Convert.FromHexString(CanonicalRowCodec.ContractHashHex));
        hash.AppendData(Convert.FromHexString(contract.ObjectContractDigest));
        hash.AppendData(contract.IsKeyed ? [1] : [0]);
        if (partitionId is int id)
            hash.AppendData([checked((byte)id)]);
        AppendUInt64(hash, checked((ulong)rowCount));
        return hash;
    }

    private static void AppendGroup(IncrementalHash hash, HashGroup group)
    {
        hash.AppendData(group.FirstHash);
        hash.AppendData(group.SecondHash);
        AppendUInt64(hash, checked((ulong)group.Multiplicity));
    }

    private static void AppendUInt64(IncrementalHash hash, ulong value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static string Hex(ReadOnlySpan<byte> value) =>
        Convert.ToHexString(value).ToLowerInvariant();

    private static void ValidateOptions(PartitionedChecksumValidatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SpillRootDirectory);
        if (options.MaxSpillBytes <= PartitionSpoolFile.HeaderLength)
            throw new ArgumentOutOfRangeException(nameof(options), "Maximum spill bytes is too small.");
        if (options.MaxOpenPartitionWriters <= 0 || options.MaxOpenPartitionWriters > PartitionCount)
            throw new ArgumentOutOfRangeException(nameof(options), "Open partition writers must be between 1 and 256.");
        if (options.MaxMismatchDetailsPerPartition < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "Mismatch detail limit cannot be negative.");
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadSpoolAsync(
        PartitionSpool spool,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (spool.Path is null || spool.RecordCount <= 0)
            yield break;

        await using var stream = new FileStream(
            spool.Path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
                BufferSize = 64 * 1024,
            });
        long count = await PartitionSpoolFile.ReadHeaderAsync(
            stream,
            spool.PartitionId,
            cancellationToken).ConfigureAwait(false);
        if (count != spool.RecordCount)
            throw new InvalidDataException("Validation spool record count changed after publication.");

        for (long index = 0; index < count; index++)
        {
            byte[] record = GC.AllocateUninitializedArray<byte>(ValidationHashRecord.SerializedLength);
            try
            {
                await stream.ReadExactlyAsync(record, cancellationToken).ConfigureAwait(false);
            }
            catch (EndOfStreamException error)
            {
                throw new InvalidDataException("Validation spool record data is truncated.", error);
            }
            yield return record;
        }
    }

    private sealed class GroupCursor : IAsyncDisposable
    {
        private readonly IAsyncEnumerator<ValidationHashRecord>? _records;
        private readonly bool _keyed;
        private readonly IncrementalHash _partitionHash;
        private readonly IncrementalHash _objectHash;
        private readonly CancellationToken _cancellationToken;
        private ValidationHashRecord? _lookahead;
        private byte[]? _previousKey;

        internal GroupCursor(
            ExternalHashRecordSortResult? sorted,
            bool keyed,
            IncrementalHash partitionHash,
            IncrementalHash objectHash,
            CancellationToken cancellationToken)
        {
            _records = sorted?.ReadRecordsAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
            _keyed = keyed;
            _partitionHash = partitionHash;
            _objectHash = objectHash;
            _cancellationToken = cancellationToken;
        }

        internal bool HasCurrent { get; private set; }

        internal HashGroup Current { get; private set; } = null!;

        internal ValueTask InitializeAsync() => MoveNextAsync();

        internal async ValueTask MoveNextAsync()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            if (_records is null)
            {
                HasCurrent = false;
                return;
            }

            ValidationHashRecord first;
            if (_lookahead is not null)
            {
                first = _lookahead;
                _lookahead = null;
            }
            else if (await _records.MoveNextAsync().ConfigureAwait(false))
            {
                first = _records.Current;
            }
            else
            {
                HasCurrent = false;
                return;
            }

            long multiplicity = 1;
            while (await _records.MoveNextAsync().ConfigureAwait(false))
            {
                ValidationHashRecord candidate = _records.Current;
                if (!candidate.Equals(first))
                {
                    _lookahead = candidate;
                    break;
                }
                multiplicity = checked(multiplicity + 1);
            }

            var group = new HashGroup(
                first.FirstHash.ToArray(),
                first.SecondHash.ToArray(),
                multiplicity);
            if (_keyed)
            {
                if (multiplicity != 1 ||
                    (_previousKey is not null && _previousKey.AsSpan().SequenceEqual(group.FirstHash)))
                {
                    throw new InvalidDataException(
                        "A canonical primary key occurred more than once in a validation snapshot.");
                }
                _previousKey = group.FirstHash;
            }

            AppendGroup(_partitionHash, group);
            AppendGroup(_objectHash, group);
            Current = group;
            HasCurrent = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_records is not null)
                await _records.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed record HashGroup(byte[] FirstHash, byte[] SecondHash, long Multiplicity);

    private sealed class PartitionSpoolSet : IAsyncDisposable
    {
        private const int PartitionBufferBytes = 4 * 1024;

        private readonly ValidationSpillWorkspace _workspace;
        private readonly string _side;
        private readonly int _maximumOpenWriters;
        private readonly PartitionSpool[] _spools;
        private readonly Dictionary<int, Writer> _writers = [];
        private readonly LinkedList<int> _leastRecentlyUsed = [];
        private readonly byte[]?[] _partitionBuffers = new byte[PartitionCount][];
        private readonly int[] _bufferedBytes = new int[PartitionCount];
        private long _maximumBytes;
        private long _writtenBytes;
        private bool _completed;

        internal PartitionSpoolSet(
            ValidationSpillWorkspace workspace,
            string side,
            int maximumOpenWriters,
            long maximumBytes)
        {
            _workspace = workspace;
            _side = side;
            _maximumOpenWriters = maximumOpenWriters;
            _maximumBytes = maximumBytes;
            _spools = Enumerable.Range(0, PartitionCount)
                .Select(id => new PartitionSpool(id, null, 0))
                .ToArray();
        }

        internal long RowCount { get; private set; }

        internal void SetRemainingBudget(long bytes)
        {
            if (_writtenBytes != 0)
                throw new InvalidOperationException("Spill budget cannot change after writes begin.");
            if (bytes <= PartitionSpoolFile.HeaderLength)
                throw new IOException("Validation spill budget was exhausted by source hashes.");
            _maximumBytes = bytes;
        }

        internal async ValueTask AppendAsync(
            int partitionId,
            ReadOnlyMemory<byte> record,
            CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_completed, this);
            if ((uint)partitionId >= PartitionCount || record.Length != ValidationHashRecord.SerializedLength)
                throw new InvalidDataException("Validation partition record is invalid.");

            PartitionSpool spool = _spools[partitionId];
            if (spool.Path is null)
            {
                EnsureCapacity(PartitionSpoolFile.HeaderLength);
                string path = _workspace.GetImmediateChildPath($"{_side}-partition-{partitionId:D3}.spool");
                await using (FileStream created = _workspace.CreateNewFile(Path.GetFileName(path)))
                {
                    await PartitionSpoolFile.WriteHeaderAsync(
                        created,
                        partitionId,
                        recordCount: 0,
                        cancellationToken).ConfigureAwait(false);
                }
                _writtenBytes += PartitionSpoolFile.HeaderLength;
                spool = spool with { Path = path };
                _spools[partitionId] = spool;
            }

            EnsureCapacity(ValidationHashRecord.SerializedLength);
            byte[] buffer = _partitionBuffers[partitionId] ??=
                GC.AllocateUninitializedArray<byte>(PartitionBufferBytes);
            int bufferedBytes = _bufferedBytes[partitionId];
            record.Span.CopyTo(buffer.AsSpan(bufferedBytes, record.Length));
            bufferedBytes += record.Length;
            _bufferedBytes[partitionId] = bufferedBytes;
            if (bufferedBytes == PartitionBufferBytes)
            {
                await FlushPartitionBufferAsync(partitionId, spool.Path!, cancellationToken)
                    .ConfigureAwait(false);
            }
            _writtenBytes += ValidationHashRecord.SerializedLength;
            RowCount = checked(RowCount + 1);
            _spools[partitionId] = spool with { RecordCount = checked(spool.RecordCount + 1) };
        }

        internal async ValueTask<PartitionSpool[]> CompleteAsync(CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_completed, this);
            foreach (PartitionSpool spool in _spools.Where(item => item.Path is not null))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await FlushPartitionBufferAsync(spool.PartitionId, spool.Path!, cancellationToken)
                    .ConfigureAwait(false);
                _partitionBuffers[spool.PartitionId] = null;
            }
            await CloseWritersAsync().ConfigureAwait(false);
            foreach (PartitionSpool spool in _spools.Where(item => item.Path is not null))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await using (var stream = new FileStream(
                    spool.Path!,
                    FileMode.Open,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    4096,
                    FileOptions.Asynchronous))
                {
                    await PartitionSpoolFile.UpdateCountAsync(
                        stream,
                        spool.RecordCount,
                        cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                _workspace.RegisterClosedFile(spool.Path!);
            }
            _completed = true;
            return _spools;
        }

        public async ValueTask DisposeAsync()
        {
            await CloseWritersAsync().ConfigureAwait(false);
            _completed = true;
        }

        private async ValueTask<Writer> GetWriterAsync(
            int partitionId,
            string path,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_writers.TryGetValue(partitionId, out Writer? existing))
                return existing;

            if (_writers.Count >= _maximumOpenWriters)
            {
                int evictedId = _leastRecentlyUsed.First!.Value;
                Writer evicted = _writers[evictedId];
                _leastRecentlyUsed.RemoveFirst();
                _writers.Remove(evictedId);
                await evicted.Stream.DisposeAsync().ConfigureAwait(false);
            }

            var stream = new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.Append,
                    Access = FileAccess.Write,
                    Share = FileShare.Read,
                    Options = FileOptions.Asynchronous,
                    BufferSize = 4 * 1024,
                });
            LinkedListNode<int> node = _leastRecentlyUsed.AddLast(partitionId);
            var writer = new Writer(stream, node);
            _writers.Add(partitionId, writer);
            return writer;
        }

        private async ValueTask FlushPartitionBufferAsync(
            int partitionId,
            string path,
            CancellationToken cancellationToken)
        {
            int bufferedBytes = _bufferedBytes[partitionId];
            if (bufferedBytes == 0)
                return;

            Writer writer = await GetWriterAsync(partitionId, path, cancellationToken)
                .ConfigureAwait(false);
            await writer.Stream.WriteAsync(
                    _partitionBuffers[partitionId]!.AsMemory(0, bufferedBytes),
                    cancellationToken)
                .ConfigureAwait(false);
            Touch(writer);
            _bufferedBytes[partitionId] = 0;
        }

        private void Touch(Writer writer)
        {
            _leastRecentlyUsed.Remove(writer.Node);
            _leastRecentlyUsed.AddLast(writer.Node);
        }

        private async ValueTask CloseWritersAsync()
        {
            var failures = new List<Exception>();
            foreach (Writer writer in _writers.Values)
            {
                try
                {
                    await writer.Stream.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception failure)
                {
                    failures.Add(failure);
                }
            }
            _writers.Clear();
            _leastRecentlyUsed.Clear();
            if (failures.Count != 0)
            {
                throw new AggregateException(
                    "One or more validation partition writers failed to close.",
                    failures);
            }
        }

        private void EnsureCapacity(int additionalBytes)
        {
            if (_writtenBytes > _maximumBytes - additionalBytes)
            {
                throw new IOException(
                    $"Validation spill exceeds the configured {_maximumBytes}-byte limit.");
            }
        }

        private sealed record Writer(FileStream Stream, LinkedListNode<int> Node);
    }

    private sealed record PartitionSpool(int PartitionId, string? Path, long RecordCount);

    private static class PartitionSpoolFile
    {
        internal const int HeaderLength = 32;
        private const uint Version = 1;
        private static ReadOnlySpan<byte> Magic => "CSVSPV1\0"u8;

        internal static async ValueTask WriteHeaderAsync(
            Stream stream,
            int partitionId,
            long recordCount,
            CancellationToken cancellationToken)
        {
            byte[] header = new byte[HeaderLength];
            Magic.CopyTo(header);
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8), Version);
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(12), HeaderLength);
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(16), ValidationHashRecord.SerializedLength);
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(20), checked((uint)partitionId));
            BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(24), checked((ulong)recordCount));
            await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        }

        internal static async ValueTask UpdateCountAsync(
            FileStream stream,
            long recordCount,
            CancellationToken cancellationToken)
        {
            stream.Position = 24;
            byte[] count = new byte[sizeof(ulong)];
            BinaryPrimitives.WriteUInt64BigEndian(count, checked((ulong)recordCount));
            await stream.WriteAsync(count, cancellationToken).ConfigureAwait(false);
        }

        internal static async ValueTask<long> ReadHeaderAsync(
            FileStream stream,
            int expectedPartitionId,
            CancellationToken cancellationToken)
        {
            byte[] header = new byte[HeaderLength];
            try
            {
                await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
            }
            catch (EndOfStreamException error)
            {
                throw new InvalidDataException("Validation spool header is truncated.", error);
            }
            if (!header.AsSpan(0, 8).SequenceEqual(Magic) ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(8)) != Version ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(12)) != HeaderLength ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(16)) != ValidationHashRecord.SerializedLength ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(20)) != expectedPartitionId)
            {
                throw new InvalidDataException("Validation spool header is invalid.");
            }

            ulong count = BinaryPrimitives.ReadUInt64BigEndian(header.AsSpan(24));
            if (count > long.MaxValue)
                throw new InvalidDataException("Validation spool record count exceeds the supported range.");
            long expectedLength = checked(HeaderLength + ((long)count * ValidationHashRecord.SerializedLength));
            if (stream.Length != expectedLength)
                throw new InvalidDataException("Validation spool length does not match its record count.");
            return (long)count;
        }
    }
}
