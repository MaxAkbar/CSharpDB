using System.Buffers;
using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Csv;

public static class CsvSnapshotDiagnosticRules
{
    public const string SourceLimitExceeded = "MIG-CSV-SNAPSHOT-LIMIT-001";
    public const string IntegrityMismatch = "MIG-CSV-SNAPSHOT-INTEGRITY-001";
}

public sealed class CsvSourceSnapshotException : IOException
{
    internal CsvSourceSnapshotException(string ruleId, string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>Controls the bounded-disk copy used to freeze a CSV byte stream.</summary>
public sealed record CsvSourceSnapshotOptions
{
    /// <summary>
    /// Caller-controlled parent directory for the private snapshot workspace.
    /// It must not be writable, renamed, or replaced by an untrusted principal
    /// until the snapshot and all readers are disposed. An exclusively claimed,
    /// ownership-marked child is always created. The operating-system temporary
    /// directory is used when this value is null.
    /// </summary>
    public string? WorkspacePath { get; init; }

    /// <summary>Maximum source size accepted by this snapshot operation.</summary>
    public long MaxSourceBytes { get; init; } = 1024L * 1024 * 1024 * 1024;

    public int CopyBufferBytes { get; init; } = 128 * 1024;

    public bool LeaveOpen { get; init; }
}

/// <summary>
/// A private, read-only, byte-for-byte CSV snapshot. Inspection and later reads
/// open this copy rather than reopening a mutable caller path or stream.
/// </summary>
public sealed class CsvSourceSnapshot : IAsyncDisposable
{
    internal const string IdentityAlgorithm = "csv-snapshot-v1";

    private readonly object gate = new();
    private readonly CsvSnapshotWorkspace workspace;
    private readonly string snapshotPath;
    private readonly FileStream integrityGuard;
    private int activeReaders;
    private bool cleanupRequested;
    private Task? cleanupTask;
    private Task? disposeTask;
    private int disposed;

    private CsvSourceSnapshot(
        CsvSnapshotWorkspace workspace,
        string snapshotPath,
        FileStream integrityGuard,
        long contentLength,
        string contentDigest)
    {
        this.workspace = workspace;
        this.snapshotPath = snapshotPath;
        this.integrityGuard = integrityGuard;
        ContentLength = contentLength;
        ContentDigest = contentDigest;
        SnapshotIdentity = $"{IdentityAlgorithm}:{contentDigest}:bytes:{contentLength}";
    }

    public long ContentLength { get; }

    /// <summary>SHA-256 of every raw source byte, including any BOM.</summary>
    public string ContentDigest { get; }

    public string SnapshotIdentity { get; }

    public static async ValueTask<CsvSourceSnapshot> CreateAsync(
        Stream source,
        CsvSourceSnapshotOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!source.CanRead)
            throw new ArgumentException("The CSV source stream must be readable.", nameof(source));

        CsvSourceSnapshotOptions settings = Validate(options ?? new CsvSourceSnapshotOptions());
        string parentPath = Path.GetFullPath(settings.WorkspacePath ?? Path.GetTempPath());
        CsvSnapshotWorkspace? workspace = null;
        byte[]? buffer = null;
        CsvSourceSnapshot? result = null;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            workspace = new CsvSnapshotWorkspace(parentPath);
            string snapshotPath = workspace.GetImmediateChildPath("source.snapshot");
            buffer = ArrayPool<byte>.Shared.Rent(settings.CopyBufferBytes);
            long totalBytes = 0;
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await using (FileStream destination = CreateSnapshotFile(snapshotPath, settings.CopyBufferBytes))
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    long remaining = settings.MaxSourceBytes - totalBytes;
                    int requested = remaining == 0
                        ? 1
                        : (int)Math.Min(buffer.Length, remaining + 1);
                    int read = await source.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        break;
                    if (read > remaining)
                    {
                        throw new CsvSourceSnapshotException(
                            CsvSnapshotDiagnosticRules.SourceLimitExceeded,
                            "The CSV source exceeds the configured snapshot byte limit.");
                    }

                    await destination.WriteAsync(
                            buffer.AsMemory(0, read),
                            cancellationToken)
                        .ConfigureAwait(false);
                    hasher.AppendData(buffer, 0, read);
                    totalBytes += read;
                }

                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
                destination.Flush(flushToDisk: true);
            }

            string contentDigest =
                "sha256:" + Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
            var guard = new FileStream(
                snapshotPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                settings.CopyBufferBytes,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            result = new CsvSourceSnapshot(
                workspace,
                snapshotPath,
                guard,
                totalBytes,
                contentDigest);
        }
        catch
        {
            if (workspace is not null)
            {
                try
                {
                    await workspace.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }

            if (!settings.LeaveOpen)
            {
                try
                {
                    await source.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }
            throw;
        }
        finally
        {
            if (buffer is not null)
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }

        if (!settings.LeaveOpen)
        {
            try
            {
                await source.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception sourceDisposeException)
            {
                try
                {
                    await result!.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception cleanupException)
                {
                    throw new AggregateException(sourceDisposeException, cleanupException);
                }
                throw;
            }
        }

        return result!;
    }

    public static async ValueTask<CsvSourceSnapshot> CreateFromFileAsync(
        string sourcePath,
        CsvSourceSnapshotOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        CsvSourceSnapshotOptions settings = Validate(options ?? new CsvSourceSnapshotOptions());
        var source = new FileStream(
            Path.GetFullPath(sourcePath),
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            settings.CopyBufferBytes,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        return await CreateAsync(
                source,
                settings with { LeaveOpen = false },
                cancellationToken)
            .ConfigureAwait(false);
    }

    public Stream OpenRead()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed != 0, this);
            activeReaders++;
        }

        try
        {
            var stream = new FileStream(
                snapshotPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            return new SnapshotReadStream(stream, ReleaseReaderAsync);
        }
        catch
        {
            ReleaseReaderAsync().AsTask().GetAwaiter().GetResult();
            throw;
        }
    }

    public async ValueTask VerifyIntegrityAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        await using Stream stream = OpenRead();
        byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false);
        string actual = "sha256:" + Convert.ToHexString(digest).ToLowerInvariant();
        if (!string.Equals(actual, ContentDigest, StringComparison.Ordinal))
        {
            throw new CsvSourceSnapshotException(
                CsvSnapshotDiagnosticRules.IntegrityMismatch,
                "The private CSV snapshot no longer matches its recorded content digest.");
        }
    }

    public ValueTask DisposeAsync()
    {
        Task task;
        lock (gate)
        {
            disposeTask ??= DisposeCoreAsync();
            task = disposeTask;
        }

        return new ValueTask(task);
    }

    private async Task DisposeCoreAsync()
    {
        Volatile.Write(ref disposed, 1);

        await integrityGuard.DisposeAsync().ConfigureAwait(false);
        Task? readyCleanup;
        lock (gate)
        {
            cleanupRequested = true;
            readyCleanup = GetOrStartCleanupUnderLock();
        }

        if (readyCleanup is not null)
            await readyCleanup.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private ValueTask ReleaseReaderAsync()
    {
        Task? readyCleanup;
        lock (gate)
        {
            if (activeReaders <= 0)
                throw new InvalidOperationException("The CSV snapshot reader lease is not active.");
            activeReaders--;
            readyCleanup = GetOrStartCleanupUnderLock();
        }

        return readyCleanup is null ? ValueTask.CompletedTask : new ValueTask(readyCleanup);
    }

    private Task? GetOrStartCleanupUnderLock()
    {
        if (!cleanupRequested || activeReaders != 0)
            return null;
        return cleanupTask ??= CleanupWorkspaceAsync();
    }

    private async Task CleanupWorkspaceAsync() =>
        await workspace.DisposeAsync().ConfigureAwait(false);

    private static FileStream CreateSnapshotFile(string path, int bufferSize)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = bufferSize,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
        };
        if (!OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private static CsvSourceSnapshotOptions Validate(CsvSourceSnapshotOptions options)
    {
        if (options.MaxSourceBytes < 0 || options.MaxSourceBytes == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The source byte limit must be non-negative and leave room for limit detection.");
        }

        if (options.CopyBufferBytes is < 4096 or > 16 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The copy buffer must be between 4 KiB and 16 MiB.");
        }

        if (options.WorkspacePath is not null && string.IsNullOrWhiteSpace(options.WorkspacePath))
            throw new ArgumentException("The snapshot workspace path cannot be blank.", nameof(options));

        return options;
    }

    private sealed class SnapshotReadStream : Stream
    {
        private readonly Stream inner;
        private readonly Func<ValueTask> release;
        private int disposed;

        public SnapshotReadStream(Stream inner, Func<ValueTask> release)
        {
            this.inner = inner;
            this.release = release;
        }

        public override bool CanRead => disposed == 0 && inner.CanRead;
        public override bool CanSeek => disposed == 0 && inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => inner.Length;
        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => inner.Read(buffer);

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            inner.ReadAsync(buffer, cancellationToken);

        public override int ReadByte() => inner.ReadByte();

        public override void Flush() => inner.Flush();

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (!disposing || Interlocked.Exchange(ref disposed, 1) != 0)
            {
                base.Dispose(disposing);
                return;
            }

            try
            {
                inner.Dispose();
            }
            finally
            {
                release().AsTask().GetAwaiter().GetResult();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            try
            {
                await inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await release().ConfigureAwait(false);
            }
            GC.SuppressFinalize(this);
        }
    }
}
