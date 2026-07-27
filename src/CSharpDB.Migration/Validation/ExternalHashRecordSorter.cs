using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace CSharpDB.Migration.Validation;

public sealed record ExternalHashRecordSorterOptions
{
    /// <summary>
    /// Maximum combined size of the two fixed-width in-memory sort buffers.
    /// Merge passes additionally retain one 64-byte record and one bounded
    /// FileStream buffer per open input. Their count is capped by MaxOpenFiles.
    /// </summary>
    public long MemoryBudgetBytes { get; init; } = 8L * 1024 * 1024;

    public int MergeFanIn { get; init; } = 16;

    /// <summary>
    /// Includes the merge output file, so at most <c>MaxOpenFiles - 1</c>
    /// input runs are opened at once.
    /// </summary>
    public int MaxOpenFiles { get; init; } = 32;
}

/// <summary>
/// Generates sorted fixed-width runs within a caller-supplied memory budget,
/// then performs bounded-fan-in merge passes. Duplicate records are retained.
/// </summary>
public sealed class ExternalHashRecordSorter
{
    private const int SortBufferBytesPerRecord = ValidationHashRecord.SerializedLength * 2;
    private const int MaximumMergeLevels = 64;

    private readonly ValidationSpillWorkspace _workspace;
    private readonly int _recordsPerRun;
    private readonly int _effectiveMergeFanIn;
    private long _fileSequence;

    public ExternalHashRecordSorter(
        ValidationSpillWorkspace workspace,
        ExternalHashRecordSorterOptions? options = null)
    {
        _workspace = workspace ?? throw new ArgumentNullException(nameof(workspace));
        options ??= new ExternalHashRecordSorterOptions();

        if (options.MemoryBudgetBytes < SortBufferBytesPerRecord)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The memory budget must be at least {SortBufferBytesPerRecord} bytes.");
        }

        if (options.MergeFanIn < 2)
            throw new ArgumentOutOfRangeException(nameof(options), "Merge fan-in must be at least two.");
        if (options.MaxOpenFiles < 3)
            throw new ArgumentOutOfRangeException(nameof(options), "At least three open files are required for merging.");

        long maximumArrayRecords = Array.MaxLength / ValidationHashRecord.SerializedLength;
        long requestedRecords = options.MemoryBudgetBytes / SortBufferBytesPerRecord;
        _recordsPerRun = checked((int)Math.Min(requestedRecords, maximumArrayRecords));
        _effectiveMergeFanIn = Math.Min(options.MergeFanIn, options.MaxOpenFiles - 1);
    }

    public async Task<ExternalHashRecordSortResult> SortAsync(
        IAsyncEnumerable<ReadOnlyMemory<byte>> records,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);

        var liveRuns = new HashSet<string>(PathComparer);
        try
        {
            List<RunInfo> runs = await GenerateRunsAsync(records, liveRuns, cancellationToken)
                .ConfigureAwait(false);

            if (runs.Count == 0)
            {
                RunInfo empty = await WriteRunAsync(
                        ReadOnlyMemory<byte>.Empty,
                        recordCount: 0,
                        cancellationToken)
                    .ConfigureAwait(false);
                runs.Add(empty);
                liveRuns.Add(empty.Path);
            }

            while (runs.Count > 1)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var nextPass = new List<RunInfo>((runs.Count + _effectiveMergeFanIn - 1) / _effectiveMergeFanIn);

                for (int start = 0; start < runs.Count; start += _effectiveMergeFanIn)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int count = Math.Min(_effectiveMergeFanIn, runs.Count - start);
                    if (count == 1)
                    {
                        nextPass.Add(runs[start]);
                        continue;
                    }

                    List<RunInfo> inputs = runs.GetRange(start, count);
                    RunInfo output = await MergeRunsAsync(inputs, cancellationToken).ConfigureAwait(false);
                    liveRuns.Add(output.Path);

                    // MergeRunsAsync closes every input and the output before it
                    // returns. Only then is it safe to remove intermediate runs.
                    foreach (RunInfo input in inputs)
                    {
                        _workspace.DeleteFile(input.Path);
                        liveRuns.Remove(input.Path);
                    }

                    nextPass.Add(output);
                }

                runs = nextPass;
            }

            RunInfo finalRun = runs[0];
            return new ExternalHashRecordSortResult(finalRun.Path, finalRun.RecordCount);
        }
        catch (Exception failure)
        {
            var cleanupFailures = new List<Exception>();
            foreach (string path in liveRuns)
            {
                try
                {
                    _workspace.DeleteFile(path);
                }
                catch (Exception cleanupFailure)
                {
                    cleanupFailures.Add(cleanupFailure);
                }
            }

            if (cleanupFailures.Count > 0)
            {
                cleanupFailures.Insert(0, failure);
                throw new AggregateException("Validation sorting and spill cleanup both failed.", cleanupFailures);
            }

            throw;
        }
    }

    private async Task<List<RunInfo>> GenerateRunsAsync(
        IAsyncEnumerable<ReadOnlyMemory<byte>> records,
        HashSet<string> liveRuns,
        CancellationToken cancellationToken)
    {
        // Compact runs online in fixed merge levels. Retaining one RunInfo for
        // every input run would otherwise make metadata memory proportional to
        // the row count when the sort buffer is deliberately small.
        var levels = new List<RunInfo>?[MaximumMergeLevels];
        byte[] buffer = GC.AllocateUninitializedArray<byte>(
            checked(_recordsPerRun * ValidationHashRecord.SerializedLength));
        byte[] scratch = GC.AllocateUninitializedArray<byte>(buffer.Length);
        int bufferedRecords = 0;

        await foreach (ReadOnlyMemory<byte> record in records
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (record.Length != ValidationHashRecord.SerializedLength)
            {
                throw new InvalidDataException(
                    $"A validation hash record must contain exactly {ValidationHashRecord.SerializedLength} bytes; " +
                    $"received {record.Length} bytes.");
            }

            record.Span.CopyTo(buffer.AsSpan(
                bufferedRecords * ValidationHashRecord.SerializedLength,
                ValidationHashRecord.SerializedLength));
            bufferedRecords++;

            if (bufferedRecords == _recordsPerRun)
            {
                RunInfo run = await SortAndWriteRunAsync(
                        buffer,
                        scratch,
                        bufferedRecords,
                        cancellationToken)
                    .ConfigureAwait(false);
                liveRuns.Add(run.Path);
                await AddRunWithCompactionAsync(
                        levels,
                        run,
                        liveRuns,
                        cancellationToken)
                    .ConfigureAwait(false);
                bufferedRecords = 0;
            }
        }

        if (bufferedRecords > 0)
        {
            RunInfo run = await SortAndWriteRunAsync(
                    buffer,
                    scratch,
                    bufferedRecords,
                    cancellationToken)
                .ConfigureAwait(false);
            liveRuns.Add(run.Path);
            await AddRunWithCompactionAsync(
                    levels,
                    run,
                    liveRuns,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return levels
            .Where(level => level is not null)
            .SelectMany(level => level!)
            .ToList();
    }

    private async ValueTask AddRunWithCompactionAsync(
        List<RunInfo>?[] levels,
        RunInfo run,
        HashSet<string> liveRuns,
        CancellationToken cancellationToken)
    {
        for (int levelIndex = 0; ; levelIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (levelIndex >= levels.Length)
            {
                throw new IOException(
                    "Validation sorting exceeded the supported 64 merge levels.");
            }

            List<RunInfo> level = levels[levelIndex] ??=
                new List<RunInfo>(_effectiveMergeFanIn);
            level.Add(run);
            if (level.Count < _effectiveMergeFanIn)
                return;

            RunInfo[] inputs = level.ToArray();
            level.Clear();
            RunInfo output = await MergeRunsAsync(inputs, cancellationToken).ConfigureAwait(false);
            liveRuns.Add(output.Path);
            foreach (RunInfo input in inputs)
            {
                _workspace.DeleteFile(input.Path);
                liveRuns.Remove(input.Path);
            }
            run = output;
        }
    }

    private Task<RunInfo> SortAndWriteRunAsync(
        byte[] buffer,
        byte[] scratch,
        int recordCount,
        CancellationToken cancellationToken)
    {
        byte[] sorted = SortInMemory(buffer, scratch, recordCount, cancellationToken);
        return WriteRunAsync(
            sorted.AsMemory(0, recordCount * ValidationHashRecord.SerializedLength),
            recordCount,
            cancellationToken);
    }

    private async Task<RunInfo> WriteRunAsync(
        ReadOnlyMemory<byte> records,
        long recordCount,
        CancellationToken cancellationToken)
    {
        long expectedBytes = checked(recordCount * ValidationHashRecord.SerializedLength);
        if (records.Length != expectedBytes)
            throw new InvalidDataException("The run payload length does not match its record count.");

        string path = NewRunPath();
        bool fileCreated = false;
        try
        {
            FileStream stream = _workspace.CreateNewFile(Path.GetFileName(path));
            fileCreated = true;
            await using (stream)
            {
                await ValidationHashRunFile.WriteHeaderAsync(stream, recordCount, cancellationToken)
                    .ConfigureAwait(false);
                await stream.WriteAsync(records, cancellationToken).ConfigureAwait(false);
            }

            _workspace.RegisterClosedFile(path);
            return new RunInfo(path, recordCount);
        }
        catch (Exception failure)
        {
            if (fileCreated)
            {
                try
                {
                    _workspace.DeleteFile(path);
                }
                catch (Exception cleanupFailure)
                {
                    throw new AggregateException(failure, cleanupFailure);
                }
            }

            throw;
        }
    }

    private async Task<RunInfo> MergeRunsAsync(
        IReadOnlyList<RunInfo> inputs,
        CancellationToken cancellationToken)
    {
        long outputRecordCount = 0;
        foreach (RunInfo input in inputs)
            outputRecordCount = checked(outputRecordCount + input.RecordCount);

        string outputPath = NewRunPath();
        var readers = new List<ValidationHashRunReader>(inputs.Count);
        FileStream? output = null;
        bool outputCreated = false;
        Exception? operationFailure = null;

        try
        {
            foreach (RunInfo input in inputs)
            {
                readers.Add(await ValidationHashRunReader.OpenAsync(input.Path, cancellationToken)
                    .ConfigureAwait(false));
            }

            output = _workspace.CreateNewFile(Path.GetFileName(outputPath));
            outputCreated = true;
            await ValidationHashRunFile.WriteHeaderAsync(output, outputRecordCount, cancellationToken)
                .ConfigureAwait(false);

            foreach (ValidationHashRunReader reader in readers)
                await reader.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            long written = 0;
            while (written < outputRecordCount)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int bestReader = FindLowestReader(readers);
                if (bestReader < 0)
                    throw new InvalidDataException("A sorted run ended before its declared record count.");

                ValidationHashRunReader selected = readers[bestReader];
                await output.WriteAsync(selected.Current, cancellationToken).ConfigureAwait(false);
                written++;
                await selected.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            }

            if (FindLowestReader(readers) >= 0)
                throw new InvalidDataException("A sorted run contains more records than its header declares.");
        }
        catch (Exception failure)
        {
            operationFailure = failure;
        }

        List<Exception> closeFailures = await DisposeMergeStreamsAsync(output, readers).ConfigureAwait(false);
        if (operationFailure is not null || closeFailures.Count > 0)
        {
            if (outputCreated)
            {
                try
                {
                    _workspace.DeleteFile(outputPath);
                }
                catch (Exception cleanupFailure)
                {
                    closeFailures.Add(cleanupFailure);
                }
            }

            if (operationFailure is not null && closeFailures.Count == 0)
            {
                ExceptionDispatchInfo.Capture(operationFailure).Throw();
                throw new InvalidOperationException("Unreachable exception propagation path.");
            }

            if (operationFailure is not null)
                closeFailures.Insert(0, operationFailure);
            throw new AggregateException("Validation run merging or stream cleanup failed.", closeFailures);
        }

        try
        {
            _workspace.RegisterClosedFile(outputPath);
            return new RunInfo(outputPath, outputRecordCount);
        }
        catch (Exception failure)
        {
            try
            {
                _workspace.DeleteFile(outputPath);
            }
            catch (Exception cleanupFailure)
            {
                throw new AggregateException(failure, cleanupFailure);
            }

            throw;
        }
    }

    private static async ValueTask<List<Exception>> DisposeMergeStreamsAsync(
        FileStream? output,
        IReadOnlyList<ValidationHashRunReader> readers)
    {
        var failures = new List<Exception>();
        if (output is not null)
        {
            try
            {
                await output.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                failures.Add(failure);
            }
        }

        foreach (ValidationHashRunReader reader in readers)
        {
            try
            {
                await reader.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                failures.Add(failure);
            }
        }

        return failures;
    }

    private static int FindLowestReader(IReadOnlyList<ValidationHashRunReader> readers)
    {
        int best = -1;
        for (int index = 0; index < readers.Count; index++)
        {
            if (!readers[index].HasCurrent)
                continue;

            if (best < 0 || ValidationHashRecord.CompareSerialized(
                    readers[index].Current.Span,
                    readers[best].Current.Span) < 0)
            {
                best = index;
            }
        }

        return best;
    }

    private static byte[] SortInMemory(
        byte[] buffer,
        byte[] scratch,
        int recordCount,
        CancellationToken cancellationToken)
    {
        if (recordCount <= 1)
            return buffer;

        byte[] source = buffer;
        byte[] destination = scratch;
        int width = 1;

        while (width < recordCount)
        {
            cancellationToken.ThrowIfCancellationRequested();
            for (int left = 0; left < recordCount; left += width * 2)
            {
                int middle = Math.Min(left + width, recordCount);
                int right = Math.Min(left + (width * 2), recordCount);
                MergeMemoryRanges(source, destination, left, middle, right, cancellationToken);
            }

            (source, destination) = (destination, source);
            width = width > recordCount / 2 ? recordCount : width * 2;
        }

        return source;
    }

    private static void MergeMemoryRanges(
        byte[] source,
        byte[] destination,
        int left,
        int middle,
        int right,
        CancellationToken cancellationToken)
    {
        int leftCursor = left;
        int rightCursor = middle;
        int outputCursor = left;

        while (leftCursor < middle && rightCursor < right)
        {
            if ((outputCursor & 0xFFF) == 0)
                cancellationToken.ThrowIfCancellationRequested();

            ReadOnlySpan<byte> leftRecord = RecordSpan(source, leftCursor);
            ReadOnlySpan<byte> rightRecord = RecordSpan(source, rightCursor);
            int selected = leftRecord.SequenceCompareTo(rightRecord) <= 0
                ? leftCursor++
                : rightCursor++;
            RecordSpan(source, selected).CopyTo(RecordSpan(destination, outputCursor++));
        }

        while (leftCursor < middle)
        {
            if ((outputCursor & 0xFFF) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            RecordSpan(source, leftCursor++).CopyTo(RecordSpan(destination, outputCursor++));
        }

        while (rightCursor < right)
        {
            if ((outputCursor & 0xFFF) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            RecordSpan(source, rightCursor++).CopyTo(RecordSpan(destination, outputCursor++));
        }
    }

    private static Span<byte> RecordSpan(byte[] buffer, int recordIndex)
        => buffer.AsSpan(
            recordIndex * ValidationHashRecord.SerializedLength,
            ValidationHashRecord.SerializedLength);

    private string NewRunPath()
    {
        long sequence = Interlocked.Increment(ref _fileSequence);
        return _workspace.GetImmediateChildPath($"run-{sequence:D12}-{Guid.NewGuid():N}.bin");
    }

    private sealed record RunInfo(string Path, long RecordCount);

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
}

public sealed class ExternalHashRecordSortResult
{
    internal ExternalHashRecordSortResult(string spillFilePath, long recordCount)
    {
        SpillFilePath = spillFilePath;
        RecordCount = recordCount;
    }

    public string SpillFilePath { get; }

    public long RecordCount { get; }

    public async IAsyncEnumerable<ValidationHashRecord> ReadRecordsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using ValidationHashRunReader reader =
            await ValidationHashRunReader.OpenAsync(SpillFilePath, cancellationToken).ConfigureAwait(false);
        while (await reader.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            yield return ValidationHashRecord.FromBytes(reader.Current.Span);
    }
}

internal static class ValidationHashRunFile
{
    internal const int HeaderLength = 32;
    internal const uint Version = 1;

    private static ReadOnlySpan<byte> Magic =>
        [0x43, 0x53, 0x48, 0x52, 0x53, 0x4F, 0x52, 0x54]; // CSHRSORT

    internal static async ValueTask WriteHeaderAsync(
        Stream stream,
        long recordCount,
        CancellationToken cancellationToken)
    {
        if (recordCount < 0)
            throw new ArgumentOutOfRangeException(nameof(recordCount));

        byte[] header = GC.AllocateUninitializedArray<byte>(HeaderLength);
        header.AsSpan().Clear();
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8, 4), Version);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(12, 4), HeaderLength);
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(16, 4),
            ValidationHashRecord.SerializedLength);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(20, 4), 0); // Reserved.
        BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(24, 8), checked((ulong)recordCount));
        await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
    }

    internal static async ValueTask<long> ReadAndValidateHeaderAsync(
        FileStream stream,
        CancellationToken cancellationToken)
    {
        byte[] header = GC.AllocateUninitializedArray<byte>(HeaderLength);
        try
        {
            await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        }
        catch (EndOfStreamException exception)
        {
            throw new InvalidDataException("The validation run header is truncated.", exception);
        }

        if (!header.AsSpan(0, 8).SequenceEqual(Magic))
            throw new InvalidDataException("The validation run magic is invalid.");
        if (BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(8, 4)) != Version)
            throw new InvalidDataException("The validation run version is unsupported.");
        if (BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(12, 4)) != HeaderLength)
            throw new InvalidDataException("The validation run header length is invalid.");
        if (BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(16, 4)) != ValidationHashRecord.SerializedLength)
            throw new InvalidDataException("The validation run record length is invalid.");
        if (BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(20, 4)) != 0)
            throw new InvalidDataException("The validation run reserved header field must be zero.");

        ulong encodedCount = BinaryPrimitives.ReadUInt64BigEndian(header.AsSpan(24, 8));
        if (encodedCount > long.MaxValue)
            throw new InvalidDataException("The validation run record count exceeds the supported range.");

        long recordCount = (long)encodedCount;
        long expectedLength;
        try
        {
            expectedLength = checked(HeaderLength + (recordCount * ValidationHashRecord.SerializedLength));
        }
        catch (OverflowException exception)
        {
            throw new InvalidDataException("The validation run length exceeds the supported range.", exception);
        }

        if (stream.Length != expectedLength)
        {
            throw new InvalidDataException(
                $"The validation run length is {stream.Length} bytes; expected {expectedLength} bytes.");
        }

        return recordCount;
    }
}

internal sealed class ValidationHashRunReader : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly byte[] _current;
    private long _remaining;

    private ValidationHashRunReader(FileStream stream, long recordCount)
    {
        _stream = stream;
        _remaining = recordCount;
        _current = new byte[ValidationHashRecord.SerializedLength];
    }

    internal bool HasCurrent { get; private set; }

    internal ReadOnlyMemory<byte> Current => _current;

    internal static async ValueTask<ValidationHashRunReader> OpenAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
                BufferSize = 64 * 1024,
            });

        try
        {
            long count = await ValidationHashRunFile.ReadAndValidateHeaderAsync(stream, cancellationToken)
                .ConfigureAwait(false);
            return new ValidationHashRunReader(stream, count);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        if (_remaining == 0)
        {
            HasCurrent = false;
            return false;
        }

        try
        {
            await _stream.ReadExactlyAsync(_current, cancellationToken).ConfigureAwait(false);
        }
        catch (EndOfStreamException exception)
        {
            throw new InvalidDataException("The validation run record data is truncated.", exception);
        }

        _remaining--;
        HasCurrent = true;
        return true;
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
