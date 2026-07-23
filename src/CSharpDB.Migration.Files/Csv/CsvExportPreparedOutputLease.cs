using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Describes whether a prepared CSV output was created, recovered from a
/// durable checkpoint, or contains bytes that were never checkpointed.
/// </summary>
public enum CsvExportPreparedOutputState
{
    New,
    Recovered,
    UncheckpointedData,
}

/// <summary>Deterministic private sibling paths for one CSV destination.</summary>
public sealed record CsvExportPreparedOutputPaths
{
    public required string PreparedDataPath { get; init; }

    public required string CheckpointPath { get; init; }

    public required string PendingCheckpointPath { get; init; }
}

/// <summary>
/// Owns the exclusive prepared-output lease and the durable physical checkpoint
/// journal for one future CSV destination. Final CSV and manifest paths are
/// never opened or published by this type.
/// </summary>
public sealed class CsvExportPreparedOutputLease : IAsyncDisposable
{
    private const int HashBufferSize = 64 * 1024;
    private const string PathBindingContract =
        "csharpdb-csv-export-prepared-output-path/v1";

    private readonly CsvExportPreparedOutputFileSystem fileSystem;
    private readonly CsvExportHashManifest expectedBindingDigest;
    private readonly SemaphoreSlim operationGate = new(1, 1);
    private byte[]? currentCheckpointBytes;
    private bool disposed;

    private CsvExportPreparedOutputLease(
        string destinationPath,
        CsvExportPreparedOutputPaths paths,
        CsvExportPreparedOutputFileSystem fileSystem,
        CsvExportHashManifest expectedBindingDigest,
        CsvExportPreparedOutputState state,
        CsvExportCheckpoint? currentCheckpoint,
        byte[]? currentCheckpointBytes)
    {
        DestinationPath = destinationPath;
        Paths = paths;
        this.fileSystem = fileSystem;
        this.expectedBindingDigest = expectedBindingDigest;
        State = state;
        CurrentCheckpoint = currentCheckpoint;
        this.currentCheckpointBytes = currentCheckpointBytes;
    }

    /// <summary>The normalized future CSV destination.</summary>
    public string DestinationPath { get; }

    /// <summary>The deterministic private files owned by this lease.</summary>
    public CsvExportPreparedOutputPaths Paths { get; }

    /// <summary>The state observed and qualified when the lease was opened.</summary>
    public CsvExportPreparedOutputState State { get; private set; }

    /// <summary>The active durable checkpoint, when one exists.</summary>
    public CsvExportCheckpoint? CurrentCheckpoint { get; private set; }

    /// <summary>
    /// The exclusively leased prepared data stream. Uncheckpointed bytes must
    /// be explicitly reset before the stream can be reused.
    /// </summary>
    public Stream DataStream
    {
        get
        {
            ThrowIfDisposed();
            if (State == CsvExportPreparedOutputState.UncheckpointedData)
            {
                throw new InvalidOperationException(
                    "The prepared CSV output contains uncheckpointed bytes and must be explicitly reset.");
            }

            return fileSystem.DataStream;
        }
    }

    /// <summary>
    /// Opens the exclusive prepared-output lease and qualifies any active
    /// checkpoint and physical prefix before allowing append access.
    /// </summary>
    public static async ValueTask<CsvExportPreparedOutputLease> OpenAsync(
        string destinationPath,
        CsvExportCheckpointBinding expectedBinding,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(expectedBinding);

        CsvExportHashManifest expectedBindingDigest =
            CsvExportCheckpointSerializer.ComputeBindingDigest(expectedBinding);
        (string normalizedDestination, CsvExportPreparedOutputPaths paths) =
            BindPaths(destinationPath);
        CsvExportPreparedOutputFileSystem fileSystem =
            CsvExportPreparedOutputFileSystem.Open(paths);
        try
        {
            byte[]? checkpointBytes =
                await fileSystem.ReadActiveCheckpointAsync(cancellationToken)
                    .ConfigureAwait(false);
            if (checkpointBytes is null)
            {
                CsvExportPreparedOutputState state =
                    fileSystem.DataStream.Length == 0
                        ? CsvExportPreparedOutputState.New
                        : CsvExportPreparedOutputState.UncheckpointedData;
                fileSystem.DataStream.Position = 0;
                return new CsvExportPreparedOutputLease(
                    normalizedDestination,
                    paths,
                    fileSystem,
                    expectedBindingDigest,
                    state,
                    currentCheckpoint: null,
                    currentCheckpointBytes: null);
            }

            CsvExportCheckpoint checkpoint =
                CsvExportCheckpointSerializer.Deserialize(checkpointBytes);
            VerifyBinding(
                checkpoint.BindingDigest,
                expectedBindingDigest,
                "The active CSV export checkpoint belongs to a different export binding.");
            await QualifyAndRecoverDataPrefixAsync(
                    fileSystem,
                    checkpoint.Progress,
                    cancellationToken)
                .ConfigureAwait(false);

            return new CsvExportPreparedOutputLease(
                normalizedDestination,
                paths,
                fileSystem,
                expectedBindingDigest,
                CsvExportPreparedOutputState.Recovered,
                checkpoint,
                checkpointBytes);
        }
        catch
        {
            await fileSystem.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Explicitly discards private bytes that have no active checkpoint. This
    /// never changes a final CSV or manifest destination.
    /// </summary>
    public async ValueTask ResetUncheckpointedAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await operationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (State != CsvExportPreparedOutputState.UncheckpointedData ||
                CurrentCheckpoint is not null)
            {
                throw new InvalidOperationException(
                    "Only an uncheckpointed prepared CSV output can be reset.");
            }

            cancellationToken.ThrowIfCancellationRequested();
            fileSystem.TruncateData(0);
            await fileSystem.FlushDataToDiskAsync(cancellationToken)
                .ConfigureAwait(false);
            fileSystem.DataStream.Position = 0;
            State = CsvExportPreparedOutputState.New;
        }
        finally
        {
            operationGate.Release();
        }
    }

    /// <summary>
    /// Makes one complete-record checkpoint durable using data-first ordering:
    /// durable prepared data, durable pending checkpoint, then atomic active
    /// checkpoint replacement.
    /// </summary>
    public async ValueTask PersistCheckpointAsync(
        CsvExportCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(checkpoint);

        byte[] canonicalBytes = CsvExportCheckpointSerializer.Serialize(checkpoint);
        VerifyBinding(
            checkpoint.BindingDigest,
            expectedBindingDigest,
            "The CSV export checkpoint does not match this prepared-output lease.");

        await operationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (State == CsvExportPreparedOutputState.UncheckpointedData)
            {
                throw new InvalidOperationException(
                    "Uncheckpointed prepared bytes must be explicitly reset before checkpointing.");
            }

            bool idempotent = ValidateTransition(
                CurrentCheckpoint,
                currentCheckpointBytes,
                checkpoint,
                canonicalBytes);
            FileStream data = fileSystem.DataStream;
            if (data.Length != checkpoint.Progress.DataPrefixByteLength ||
                data.Position != checkpoint.Progress.DataPrefixByteLength)
            {
                throw new InvalidOperationException(
                    "The prepared CSV stream must end exactly at the checkpoint byte boundary.");
            }

            await fileSystem.FlushDataToDiskAsync(cancellationToken)
                .ConfigureAwait(false);
            RequireCompleteRecordBoundary(data, checkpoint.Progress.DataPrefixByteLength);
            await VerifyDataPrefixDigestAsync(
                    data,
                    checkpoint.Progress,
                    cancellationToken)
                .ConfigureAwait(false);
            if (idempotent)
                return;

            await fileSystem.ReplaceCheckpointAsync(canonicalBytes, cancellationToken)
                .ConfigureAwait(false);

            CurrentCheckpoint = checkpoint;
            currentCheckpointBytes = canonicalBytes;
        }
        finally
        {
            operationGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await operationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (disposed)
                return;
            disposed = true;
            await fileSystem.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            operationGate.Release();
        }
    }

    private static async ValueTask QualifyAndRecoverDataPrefixAsync(
        CsvExportPreparedOutputFileSystem fileSystem,
        CsvExportCheckpointProgress progress,
        CancellationToken cancellationToken)
    {
        FileStream data = fileSystem.DataStream;
        long prefixLength = progress.DataPrefixByteLength;
        long originalLength = data.Length;
        if (originalLength < prefixLength)
        {
            throw new InvalidDataException(
                "The prepared CSV data is shorter than its active checkpoint.");
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(HashBufferSize);
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            data.Position = 0;
            long remaining = prefixLength;
            byte previous = 0;
            byte last = 0;
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
                        "The prepared CSV data ended before its active checkpoint boundary.");
                }

                hash.AppendData(buffer, 0, read);
                for (int index = 0; index < read; index++)
                {
                    previous = last;
                    last = buffer[index];
                }
                remaining -= read;
            }

            Span<byte> actualDigest = stackalloc byte[SHA256.HashSizeInBytes];
            if (!hash.TryGetHashAndReset(actualDigest, out int written) ||
                written != actualDigest.Length)
            {
                throw new CryptographicException(
                    "The prepared CSV prefix digest could not be finalized.");
            }
            byte[] expectedDigest =
                Convert.FromHexString(progress.DataPrefixDigest.Value);
            try
            {
                if (!CryptographicOperations.FixedTimeEquals(
                        actualDigest,
                        expectedDigest))
                {
                    throw new InvalidDataException(
                        "The prepared CSV data prefix does not match its active checkpoint.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(expectedDigest);
            }

            if (prefixLength < 2 || previous != (byte)'\r' || last != (byte)'\n')
            {
                throw new InvalidDataException(
                    "The active CSV export checkpoint does not end at a complete CRLF record boundary.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (originalLength > prefixLength)
        {
            cancellationToken.ThrowIfCancellationRequested();
            fileSystem.TruncateData(prefixLength);
            await fileSystem.FlushDataToDiskAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        data.Position = prefixLength;
    }

    private static bool ValidateTransition(
        CsvExportCheckpoint? current,
        byte[]? currentBytes,
        CsvExportCheckpoint next,
        byte[] nextBytes)
    {
        if (current is null)
        {
            if (next.Generation != 0)
            {
                throw new InvalidOperationException(
                    "The first durable CSV export checkpoint must use generation zero.");
            }
            return false;
        }

        if (next.Generation == current.Generation)
        {
            if (currentBytes is null ||
                !currentBytes.AsSpan().SequenceEqual(nextBytes))
            {
                throw new InvalidOperationException(
                    "A CSV export checkpoint generation cannot be replaced with different content.");
            }
            return true;
        }

        if (current.Phase == CsvExportCheckpointPhase.DataComplete)
        {
            throw new InvalidOperationException(
                "A data-complete CSV export checkpoint is terminal.");
        }
        if (current.Generation == long.MaxValue ||
            next.Generation != current.Generation + 1)
        {
            throw new InvalidOperationException(
                "CSV export checkpoint generations must advance by exactly one.");
        }

        CsvExportCheckpointProgress previous = current.Progress;
        CsvExportCheckpointProgress candidate = next.Progress;
        if (candidate.CompletedRowCount < previous.CompletedRowCount ||
            candidate.DataPrefixByteLength < previous.DataPrefixByteLength ||
            candidate.TransformedRowCount < previous.TransformedRowCount ||
            candidate.TransformedCellCount < previous.TransformedCellCount)
        {
            throw new InvalidOperationException(
                "CSV export checkpoint progress cannot move backward.");
        }

        bool rowAdvanced =
            candidate.CompletedRowCount > previous.CompletedRowCount;
        bool bytesAdvanced =
            candidate.DataPrefixByteLength > previous.DataPrefixByteLength;
        if (rowAdvanced != bytesAdvanced)
        {
            throw new InvalidOperationException(
                "CSV export row and byte progress must advance together.");
        }
        if (rowAdvanced)
        {
            if (previous.LastCompletedRowId is long previousId &&
                candidate.LastCompletedRowId <= previousId)
            {
                throw new InvalidOperationException(
                    "CSV export checkpoint row IDs must advance in signed ascending order.");
            }
        }
        else
        {
            if (candidate.LastCompletedRowId != previous.LastCompletedRowId ||
                candidate.DataPrefixDigest != previous.DataPrefixDigest ||
                candidate.SourceLogicalRowHashPrefixDigest !=
                    previous.SourceLogicalRowHashPrefixDigest ||
                candidate.ExportedLogicalRowHashPrefixDigest !=
                    previous.ExportedLogicalRowHashPrefixDigest ||
                candidate.TransformedRowCount != previous.TransformedRowCount ||
                candidate.TransformedCellCount != previous.TransformedCellCount)
            {
                throw new InvalidOperationException(
                    "CSV export evidence cannot change without row progress.");
            }
            if (next.Phase != CsvExportCheckpointPhase.DataComplete)
            {
                throw new InvalidOperationException(
                    "A writing checkpoint generation must advance row progress.");
            }
        }

        return false;
    }

    private static void RequireCompleteRecordBoundary(
        FileStream data,
        long prefixLength)
    {
        if (prefixLength < 2)
        {
            throw new InvalidOperationException(
                "A CSV export checkpoint must include a complete CRLF record.");
        }

        long position = data.Position;
        Span<byte> suffix = stackalloc byte[2];
        try
        {
            data.Position = prefixLength - suffix.Length;
            int offset = 0;
            while (offset < suffix.Length)
            {
                int read = data.Read(suffix[offset..]);
                if (read == 0)
                {
                    throw new InvalidOperationException(
                        "The prepared CSV stream ended before its checkpoint boundary.");
                }
                offset += read;
            }
        }
        finally
        {
            data.Position = position;
        }

        if (suffix[0] != (byte)'\r' || suffix[1] != (byte)'\n')
        {
            throw new InvalidOperationException(
                "The prepared CSV stream does not end at a complete CRLF record boundary.");
        }
    }

    private static async ValueTask VerifyDataPrefixDigestAsync(
        FileStream data,
        CsvExportCheckpointProgress progress,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(HashBufferSize);
        long position = data.Position;
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
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
                        "The prepared CSV stream ended before its checkpoint boundary.");
                }
                hash.AppendData(buffer, 0, read);
                remaining -= read;
            }

            Span<byte> actual = stackalloc byte[SHA256.HashSizeInBytes];
            if (!hash.TryGetHashAndReset(actual, out int written) ||
                written != actual.Length)
            {
                throw new CryptographicException(
                    "The prepared CSV prefix digest could not be finalized.");
            }

            byte[] expected =
                Convert.FromHexString(progress.DataPrefixDigest.Value);
            try
            {
                if (!CryptographicOperations.FixedTimeEquals(actual, expected))
                {
                    throw new InvalidDataException(
                        "The prepared CSV stream does not match the checkpoint data-prefix digest.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(expected);
            }
        }
        finally
        {
            data.Position = position;
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void VerifyBinding(
        CsvExportHashManifest supplied,
        CsvExportHashManifest expected,
        string message)
    {
        byte[] suppliedBytes = Convert.FromHexString(supplied.Value);
        byte[] expectedBytes = Convert.FromHexString(expected.Value);
        try
        {
            if (!string.Equals(
                    supplied.Algorithm,
                    expected.Algorithm,
                    StringComparison.Ordinal) ||
                !CryptographicOperations.FixedTimeEquals(
                    suppliedBytes,
                    expectedBytes))
            {
                throw new InvalidDataException(message);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(suppliedBytes);
            CryptographicOperations.ZeroMemory(expectedBytes);
        }
    }

    private static (string Destination, CsvExportPreparedOutputPaths Paths)
        BindPaths(string destinationPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        if (destinationPath.Contains('\0'))
        {
            throw new ArgumentException(
                "The CSV export destination cannot contain a null character.",
                nameof(destinationPath));
        }
        RejectInvalidUnicode(destinationPath);
        if (!Path.IsPathFullyQualified(destinationPath))
        {
            throw new ArgumentException(
                "The CSV export destination must be fully qualified.",
                nameof(destinationPath));
        }
        RejectDotSegments(destinationPath);
        RejectWindowsSpecialPath(destinationPath);

        string normalized = Path.GetFullPath(destinationPath);
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (!string.Equals(normalized, destinationPath, comparison))
        {
            throw new ArgumentException(
                "The CSV export destination must be normalized and cannot contain traversal.",
                nameof(destinationPath));
        }

        string parent = Path.GetDirectoryName(normalized) ??
            throw new ArgumentException(
                "The CSV export destination must have a parent directory.",
                nameof(destinationPath));
        string leaf = Path.GetFileName(normalized);
        if (string.IsNullOrWhiteSpace(leaf) || leaf is "." or "..")
        {
            throw new ArgumentException(
                "The CSV export destination file name is invalid.",
                nameof(destinationPath));
        }
        ValidateDirectoryChain(parent);
        if (TryGetAttributes(normalized, out FileAttributes attributes))
        {
            if ((attributes &
                 (FileAttributes.Directory |
                  FileAttributes.ReparsePoint |
                  FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The CSV export destination cannot be a link, directory, device, or special file.");
            }

            throw new InvalidDataException(
                "The CSV export destination already exists; prepared export is fail-closed and never overwrites it.");
        }

        string hashPath = OperatingSystem.IsWindows()
            ? normalized.ToUpperInvariant()
            : normalized;
        byte[] bindingBytes = Encoding.UTF8.GetBytes(
            PathBindingContract + "\0" + hashPath);
        string digest;
        try
        {
            digest = Convert.ToHexString(SHA256.HashData(bindingBytes))
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bindingBytes);
        }

        string stem = $".csharpdb-csv-export-{digest[..32]}";
        return (
            normalized,
            new CsvExportPreparedOutputPaths
            {
                PreparedDataPath = Path.Combine(parent, stem + ".prepared"),
                CheckpointPath = Path.Combine(parent, stem + ".checkpoint"),
                PendingCheckpointPath =
                    Path.Combine(parent, stem + ".checkpoint.next"),
            });
    }

    private static void RejectDotSegments(string path)
    {
        string root = Path.GetPathRoot(path) ?? string.Empty;
        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.None))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    "The CSV export destination cannot contain traversal segments.",
                    nameof(path));
            }
        }
    }

    private static void RejectInvalidUnicode(string path)
    {
        for (int index = 0; index < path.Length; index++)
        {
            char value = path[index];
            if (!char.IsSurrogate(value))
                continue;
            if (char.IsHighSurrogate(value) &&
                index + 1 < path.Length &&
                char.IsLowSurrogate(path[index + 1]))
            {
                index++;
                continue;
            }

            throw new ArgumentException(
                "The CSV export destination must contain valid Unicode scalar data.",
                nameof(path));
        }
    }

    private static void RejectWindowsSpecialPath(string path)
    {
        if (!OperatingSystem.IsWindows())
            return;
        if (path.StartsWith("\\\\?\\", StringComparison.Ordinal) ||
            path.StartsWith("\\\\.\\", StringComparison.Ordinal) ||
            path.StartsWith("\\\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths cannot be used for prepared CSV export.",
                nameof(path));
        }

        string root = Path.GetPathRoot(path) ?? string.Empty;
        if (path.AsSpan(root.Length).Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be used for prepared CSV export.",
                nameof(path));
        }
        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
            {
                throw new ArgumentException(
                    "Windows CSV export path segments cannot end in spaces or dots.",
                    nameof(path));
            }
        }

        string destinationLeaf = Path.GetFileName(path);
        int firstDot = destinationLeaf.IndexOf('.');
        string stem = (firstDot < 0
                ? destinationLeaf
                : destinationLeaf[..firstDot])
            .TrimEnd(' ', '.');
        if (stem.Equals("CON", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("AUX", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("NUL", StringComparison.OrdinalIgnoreCase) ||
            (stem.Length == 4 &&
             (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) ||
              stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase)) &&
             stem[3] is >= '1' and <= '9' or '\u00b9' or '\u00b2' or '\u00b3'))
        {
            throw new ArgumentException(
                "Windows reserved device names cannot be used for prepared CSV export.",
                nameof(path));
        }
    }

    private static void ValidateDirectoryChain(string parentPath)
    {
        if (!Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The CSV export destination parent directory does not exist.");
        }

        string root = Path.GetPathRoot(parentPath) ??
            throw new InvalidDataException(
                "The CSV export destination parent root is invalid.");
        string relative = Path.GetRelativePath(root, parentPath);
        string current = root;
        if (relative == ".")
            return;

        foreach (string segment in relative.Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, segment);
            FileAttributes attributes = File.GetAttributes(current);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                 (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The CSV export destination parent path cannot traverse a link, device, or non-directory.");
            }
        }
    }

    private static bool TryGetAttributes(
        string path,
        out FileAttributes attributes)
    {
        try
        {
            attributes = File.GetAttributes(path);
            return true;
        }
        catch (FileNotFoundException)
        {
            attributes = default;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            attributes = default;
            return false;
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);
}
