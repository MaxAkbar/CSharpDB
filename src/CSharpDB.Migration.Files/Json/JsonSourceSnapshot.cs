using System.Buffers;
using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Stable rules reported while freezing or verifying a JSON source.</summary>
public static class JsonSnapshotDiagnosticRules
{
    public const string SourceLimitExceeded =
        "MIG-JSON-SNAPSHOT-LIMIT-001";

    public const string IntegrityMismatch =
        "MIG-JSON-SNAPSHOT-INTEGRITY-001";
}

/// <summary>Reports a deterministic JSON snapshot failure.</summary>
public sealed class JsonSourceSnapshotException : IOException
{
    internal JsonSourceSnapshotException(string ruleId, string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>
/// Controls the bounded private-disk copy used to freeze a JSON byte stream.
/// </summary>
public sealed record JsonSourceSnapshotOptions
{
    /// <summary>
    /// Caller-controlled parent directory for the private workspace. It must
    /// remain stable and trusted until the snapshot and all readers close.
    /// </summary>
    public string? WorkspacePath { get; init; }

    /// <summary>Maximum source size accepted by this snapshot operation.</summary>
    public long MaxSourceBytes { get; init; } =
        1024L * 1024 * 1024 * 1024;

    /// <summary>Buffer used while copying and hashing source bytes.</summary>
    public int CopyBufferBytes { get; init; } = 128 * 1024;

    /// <summary>Whether the caller-owned input remains open.</summary>
    public bool LeaveOpen { get; init; }
}

/// <summary>
/// A private, read-only, byte-for-byte JSON snapshot. Inspection and replay
/// open this copy rather than reopening a mutable caller path or stream.
/// </summary>
public sealed class JsonSourceSnapshot : IAsyncDisposable
{
    internal const string IdentityAlgorithm = "json-snapshot-v1";

    private readonly object gate = new();
    private readonly JsonSnapshotWorkspace workspace;
    private readonly string snapshotPath;
    private readonly FileStream integrityGuard;
    private int activeReaders;
    private bool cleanupRequested;
    private Task? cleanupTask;
    private Task? disposeTask;
    private int disposed;

    private JsonSourceSnapshot(
        JsonSnapshotWorkspace workspace,
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
        SnapshotIdentity =
            $"{IdentityAlgorithm}:{contentDigest}:bytes:{contentLength}";
    }

    public long ContentLength { get; }

    /// <summary>SHA-256 of every raw source byte, including any BOM.</summary>
    public string ContentDigest { get; }

    /// <summary>Stable identity for the exact retained byte sequence.</summary>
    public string SnapshotIdentity { get; }

    public static async ValueTask<JsonSourceSnapshot> CreateAsync(
        Stream source,
        JsonSourceSnapshotOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!source.CanRead)
        {
            throw new ArgumentException(
                "The JSON source stream must be readable.",
                nameof(source));
        }

        JsonSourceSnapshotOptions settings = Validate(
            options ?? new JsonSourceSnapshotOptions());
        string parentPath = Path.GetFullPath(
            settings.WorkspacePath ?? Path.GetTempPath());
        JsonSnapshotWorkspace? workspace = null;
        byte[]? buffer = null;
        JsonSourceSnapshot? result = null;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            workspace = new JsonSnapshotWorkspace(parentPath);
            string snapshotPath =
                workspace.GetImmediateChildPath("source.snapshot");
            buffer = ArrayPool<byte>.Shared.Rent(
                settings.CopyBufferBytes);
            long totalBytes = 0;
            using var hasher =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await using (FileStream destination = CreateSnapshotFile(
                             snapshotPath,
                             settings.CopyBufferBytes))
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    long remaining =
                        settings.MaxSourceBytes - totalBytes;
                    int requested = remaining == 0
                        ? 1
                        : (int)Math.Min(
                            buffer.Length,
                            remaining + 1);
                    int read = await source.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        break;
                    if (read > remaining)
                    {
                        throw new JsonSourceSnapshotException(
                            JsonSnapshotDiagnosticRules.SourceLimitExceeded,
                            "The JSON source exceeds the configured snapshot byte limit.");
                    }

                    await destination.WriteAsync(
                            buffer.AsMemory(0, read),
                            cancellationToken)
                        .ConfigureAwait(false);
                    hasher.AppendData(buffer, 0, read);
                    totalBytes += read;
                }

                await destination
                    .FlushAsync(cancellationToken)
                    .ConfigureAwait(false);
                destination.Flush(flushToDisk: true);
            }

            string contentDigest =
                "sha256:" +
                Convert.ToHexString(hasher.GetHashAndReset())
                    .ToLowerInvariant();
            var guard = new FileStream(
                snapshotPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                settings.CopyBufferBytes,
                FileOptions.Asynchronous |
                FileOptions.SequentialScan);
            result = new JsonSourceSnapshot(
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
                    throw new AggregateException(
                        sourceDisposeException,
                        cleanupException);
                }
                throw;
            }
        }

        return result!;
    }

    public static async ValueTask<JsonSourceSnapshot> CreateFromFileAsync(
        string sourcePath,
        JsonSourceSnapshotOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        JsonSourceSnapshotOptions settings = Validate(
            options ?? new JsonSourceSnapshotOptions());
        var source = new FileStream(
            Path.GetFullPath(sourcePath),
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            settings.CopyBufferBytes,
            FileOptions.Asynchronous |
            FileOptions.SequentialScan);

        return await CreateAsync(
                source,
                settings with { LeaveOpen = false },
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Opens one leased read over the immutable snapshot. Snapshot disposal
    /// waits for every outstanding lease before deleting private files.
    /// </summary>
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
                FileOptions.Asynchronous |
                FileOptions.SequentialScan);
            return new SnapshotReadStream(
                stream,
                ReleaseReaderAsync);
        }
        catch
        {
            ReleaseReaderAsync().AsTask().GetAwaiter().GetResult();
            throw;
        }
    }

    /// <summary>Rehashes every retained byte and compares it with the identity.</summary>
    public async ValueTask VerifyIntegrityAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
        await using Stream stream = OpenRead();
        byte[] digest = await SHA256
            .HashDataAsync(stream, cancellationToken)
            .ConfigureAwait(false);
        string actual =
            "sha256:" +
            Convert.ToHexString(digest).ToLowerInvariant();
        CryptographicOperations.ZeroMemory(digest);
        if (!string.Equals(
                actual,
                ContentDigest,
                StringComparison.Ordinal))
        {
            throw new JsonSourceSnapshotException(
                JsonSnapshotDiagnosticRules.IntegrityMismatch,
                "The private JSON snapshot no longer matches its recorded content digest.");
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
            {
                throw new InvalidOperationException(
                    "The JSON snapshot reader lease is not active.");
            }
            activeReaders--;
            readyCleanup = GetOrStartCleanupUnderLock();
        }

        return readyCleanup is null
            ? ValueTask.CompletedTask
            : new ValueTask(readyCleanup);
    }

    private Task? GetOrStartCleanupUnderLock()
    {
        if (!cleanupRequested || activeReaders != 0)
            return null;
        return cleanupTask ??= CleanupWorkspaceAsync();
    }

    private async Task CleanupWorkspaceAsync() =>
        await workspace.DisposeAsync().ConfigureAwait(false);

    private static FileStream CreateSnapshotFile(
        string path,
        int bufferSize)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = bufferSize,
            Options = FileOptions.Asynchronous |
                FileOptions.SequentialScan,
        };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode =
                UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }
        return new FileStream(path, options);
    }

    private static JsonSourceSnapshotOptions Validate(
        JsonSourceSnapshotOptions options)
    {
        if (options.MaxSourceBytes < 0 ||
            options.MaxSourceBytes == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The JSON source byte limit must be non-negative and leave room for limit detection.");
        }

        if (options.CopyBufferBytes is < 4096 or > 16 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The JSON copy buffer must be between 4 KiB and 16 MiB.");
        }

        if (options.WorkspacePath is not null &&
            string.IsNullOrWhiteSpace(options.WorkspacePath))
        {
            throw new ArgumentException(
                "The JSON snapshot workspace path cannot be blank.",
                nameof(options));
        }

        return options;
    }

    private sealed class SnapshotReadStream : Stream
    {
        private readonly Stream inner;
        private readonly Func<ValueTask> release;
        private int disposed;

        internal SnapshotReadStream(
            Stream inner,
            Func<ValueTask> release)
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

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) =>
            inner.Read(buffer);

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            inner.ReadAsync(
                buffer,
                offset,
                count,
                cancellationToken);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            inner.ReadAsync(buffer, cancellationToken);

        public override int ReadByte() => inner.ReadByte();

        public override void Flush() => inner.Flush();

        public override long Seek(long offset, SeekOrigin origin) =>
            inner.Seek(offset, origin);

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (!disposing ||
                Interlocked.Exchange(ref disposed, 1) != 0)
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
