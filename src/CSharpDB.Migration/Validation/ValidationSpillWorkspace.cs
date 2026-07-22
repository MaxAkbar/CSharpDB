using System.Text;

namespace CSharpDB.Migration.Validation;

/// <summary>
/// Owns one unique validation spill directory and accounts for its closed spill files.
/// Only immediate-child files may be created or deleted through this workspace.
/// </summary>
public sealed class ValidationSpillWorkspace : IAsyncDisposable
{
    private const string WorkspacePrefix = "csharpdb-validation-";
    private const string OwnershipFileName = ".csharpdb-validation-owner";
    private const int MaximumCreateAttempts = 16;

    private readonly object _gate = new();
    private readonly Dictionary<string, long> _trackedFiles;
    private readonly byte[] _ownershipToken;
    private readonly string _ownershipFilePath;
    private bool _disposed;
    private long _liveSpillBytes;
    private long _maximumSpillBytes;

    public ValidationSpillWorkspace(string rootDirectory, long? maximumLiveSpillBytes = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        if (maximumLiveSpillBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(maximumLiveSpillBytes));

        RootDirectory = Path.GetFullPath(rootDirectory);
        Directory.CreateDirectory(RootDirectory);
        MaximumLiveSpillBytes = maximumLiveSpillBytes;

        string? directoryPath = null;
        byte[]? ownershipToken = null;
        string? ownershipFilePath = null;
        Exception? finalFailure = null;
        for (int attempt = 0; attempt < MaximumCreateAttempts; attempt++)
        {
            string candidateName = $"{WorkspacePrefix}{Guid.NewGuid():N}";
            string candidatePath = Path.GetFullPath(Path.Combine(RootDirectory, candidateName));
            string reservationPath = Path.GetFullPath(Path.Combine(RootDirectory, $".{candidateName}.reserve"));
            EnsureParent(candidatePath, RootDirectory, nameof(rootDirectory));
            EnsureParent(reservationPath, RootDirectory, nameof(rootDirectory));

            FileStream? reservation = null;
            bool candidateCreated = false;
            try
            {
                reservation = new FileStream(
                    reservationPath,
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 1,
                    FileOptions.WriteThrough);

                if (Directory.Exists(candidatePath) || File.Exists(candidatePath))
                    continue;

                Directory.CreateDirectory(candidatePath);
                candidateCreated = true;

                byte[] token = Encoding.ASCII.GetBytes(Guid.NewGuid().ToString("N"));
                string markerPath = Path.Combine(candidatePath, OwnershipFileName);
                using (var marker = new FileStream(
                    markerPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.Read,
                    bufferSize: 1,
                    FileOptions.WriteThrough))
                {
                    marker.Write(token);
                    marker.Flush(flushToDisk: true);
                }

                VerifyOwnership(markerPath, token);
                directoryPath = candidatePath;
                ownershipToken = token;
                ownershipFilePath = markerPath;
                break;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                finalFailure = exception;
                if (candidateCreated)
                {
                    // A non-recursive delete can remove only an empty candidate;
                    // it can never erase files placed there by another owner.
                    try
                    {
                        Directory.Delete(candidatePath, recursive: false);
                    }
                    catch (Exception cleanupException) when (
                        cleanupException is IOException or UnauthorizedAccessException or DirectoryNotFoundException)
                    {
                        // A nonempty or concurrently removed candidate is safer to
                        // preserve than to claim or recursively delete.
                    }
                }
            }
            finally
            {
                if (reservation is not null)
                {
                    reservation.Dispose();
                    try
                    {
                        File.Delete(reservationPath);
                    }
                    catch (Exception cleanupException) when (
                        cleanupException is IOException or UnauthorizedAccessException)
                    {
                        finalFailure ??= cleanupException;
                    }
                }
            }
        }

        if (directoryPath is null || ownershipToken is null || ownershipFilePath is null)
        {
            throw new IOException(
                $"Could not create an exclusively owned validation workspace under '{RootDirectory}'.",
                finalFailure);
        }

        DirectoryPath = directoryPath;
        _ownershipToken = ownershipToken;
        _ownershipFilePath = ownershipFilePath;
        _trackedFiles = new Dictionary<string, long>(PathComparer);
    }

    public string RootDirectory { get; }

    public string DirectoryPath { get; }

    /// <summary>
    /// Optional hard limit for registered, closed spill-run bytes. A run that
    /// would exceed the limit is deleted and rejected before it enters accounting.
    /// </summary>
    public long? MaximumLiveSpillBytes { get; }

    public long LiveSpillBytes
    {
        get
        {
            lock (_gate)
                return _liveSpillBytes;
        }
    }

    public long MaximumSpillBytes
    {
        get
        {
            lock (_gate)
                return _maximumSpillBytes;
        }
    }

    /// <summary>
    /// Resolves a simple file name inside this workspace. Directory components,
    /// rooted paths, and traversal are rejected.
    /// </summary>
    public string GetImmediateChildPath(string fileName)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);

        if (Path.IsPathRooted(fileName) ||
            fileName.IndexOf(Path.DirectorySeparatorChar) >= 0 ||
            fileName.IndexOf(Path.AltDirectorySeparatorChar) >= 0 ||
            fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
            !string.Equals(Path.GetFileName(fileName), fileName, StringComparison.Ordinal) ||
            fileName is "." or "..")
        {
            throw new ArgumentException(
                "A spill file name must name one immediate child of the workspace.",
                nameof(fileName));
        }

        string path = Path.GetFullPath(Path.Combine(DirectoryPath, fileName));
        EnsureParent(path, DirectoryPath, nameof(fileName));
        return path;
    }

    internal FileStream CreateNewFile(string fileName)
    {
        string path = GetImmediateChildPath(fileName);
        return new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
                BufferSize = 64 * 1024,
            });
    }

    internal void RegisterClosedFile(string path)
    {
        ThrowIfDisposed();
        path = ValidateImmediateChildPath(path, nameof(path));
        long length = new FileInfo(path).Length;

        lock (_gate)
        {
            if (_trackedFiles.ContainsKey(path))
                throw new InvalidOperationException($"Spill file '{Path.GetFileName(path)}' is already tracked.");

            long newLiveBytes = checked(_liveSpillBytes + length);
            if (MaximumLiveSpillBytes is long hardLimit && newLiveBytes > hardLimit)
            {
                var limitException = new IOException(
                    $"Registering spill file '{Path.GetFileName(path)}' ({length} bytes) would exceed " +
                    $"the {hardLimit}-byte validation spill limit.");
                try
                {
                    File.Delete(path);
                }
                catch (Exception cleanupException)
                {
                    throw new AggregateException(limitException, cleanupException);
                }

                throw limitException;
            }

            _liveSpillBytes = newLiveBytes;
            _maximumSpillBytes = Math.Max(_maximumSpillBytes, newLiveBytes);
            _trackedFiles.Add(path, length);
        }
    }

    internal void DeleteFile(string path)
    {
        ThrowIfDisposed();
        path = ValidateImmediateChildPath(path, nameof(path));

        File.Delete(path);

        lock (_gate)
        {
            if (_trackedFiles.Remove(path, out long length))
                _liveSpillBytes -= length;
        }
    }

    internal string ValidateImmediateChildPath(string path, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        string fullPath = Path.GetFullPath(path);
        EnsureParent(fullPath, DirectoryPath, parameterName);
        return fullPath;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        // Cleanup is deliberately non-cancelable. A caller cancellation must not
        // leave validation hashes behind on disk.
        Cleanup(CancellationToken.None);
        _disposed = true;
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private void Cleanup(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            VerifyOwnership(_ownershipFilePath, _ownershipToken);
            Directory.Delete(DirectoryPath, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
            // The owned directory is already gone.
        }

        lock (_gate)
        {
            _trackedFiles.Clear();
            _liveSpillBytes = 0;
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private static void EnsureParent(string path, string expectedParent, string parameterName)
    {
        string? parent = Path.GetDirectoryName(path);
        if (parent is null || !PathComparer.Equals(Path.GetFullPath(parent), Path.GetFullPath(expectedParent)))
        {
            throw new ArgumentException(
                "The path must be an immediate child of the expected directory.",
                parameterName);
        }
    }

    private static void VerifyOwnership(string markerPath, ReadOnlySpan<byte> expectedToken)
    {
        using var marker = new FileStream(
            markerPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1,
            FileOptions.SequentialScan);
        if (marker.Length != expectedToken.Length)
            throw new IOException("The validation workspace ownership marker has changed.");

        Span<byte> actualToken = stackalloc byte[32];
        if (expectedToken.Length != actualToken.Length)
            throw new IOException("The validation workspace ownership token is invalid.");
        marker.ReadExactly(actualToken);
        if (!actualToken.SequenceEqual(expectedToken))
            throw new IOException("The validation workspace ownership marker has changed.");
    }

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
}
