using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Exclusively reserves and marks one private snapshot directory before use.
/// Cleanup verifies the marker so an unowned path is never recursively removed.
/// </summary>
internal sealed class CsvSnapshotWorkspace : IAsyncDisposable
{
    private const string WorkspacePrefix = "csharpdb-csv-";
    private const string OwnershipFileName = ".csharpdb-csv-owner";
    private const int MaximumCreateAttempts = 16;

    private readonly byte[] ownershipToken;
    private readonly string ownershipFilePath;
    private int disposed;

    public CsvSnapshotWorkspace(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        RootDirectory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(rootDirectory));
        Directory.CreateDirectory(RootDirectory);

        Exception? finalFailure = null;
        for (int attempt = 0; attempt < MaximumCreateAttempts; attempt++)
        {
            string name = $"{WorkspacePrefix}{Guid.NewGuid():N}";
            string candidatePath = ImmediateChild(RootDirectory, name, nameof(rootDirectory));
            string reservationPath = ImmediateChild(RootDirectory, $".{name}.reserve", nameof(rootDirectory));
            FileStream? reservation = null;
            bool candidateCreated = false;
            try
            {
                reservation = CreatePrivateFile(
                    reservationPath,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    FileOptions.WriteThrough);
                if (Directory.Exists(candidatePath) || File.Exists(candidatePath))
                    continue;

                if (OperatingSystem.IsWindows())
                {
                    Directory.CreateDirectory(candidatePath);
                }
                else
                {
                    Directory.CreateDirectory(
                        candidatePath,
                        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
                }
                candidateCreated = true;
                byte[] token = RandomNumberGenerator.GetBytes(32);
                string markerPath = Path.Combine(candidatePath, OwnershipFileName);
                using (FileStream marker = CreatePrivateFile(
                    markerPath,
                    FileAccess.Write,
                    FileShare.Read,
                    FileOptions.WriteThrough))
                {
                    marker.Write(token);
                    marker.Flush(flushToDisk: true);
                }

                VerifyOwnership(markerPath, token);
                DirectoryPath = candidatePath;
                ownershipFilePath = markerPath;
                ownershipToken = token;
                return;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                finalFailure = exception;
                if (candidateCreated)
                {
                    try
                    {
                        Directory.Delete(candidatePath, recursive: false);
                    }
                    catch (Exception cleanupException) when (
                        cleanupException is IOException or UnauthorizedAccessException or DirectoryNotFoundException)
                    {
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

        throw new IOException(
            $"Could not create an exclusively owned CSV snapshot workspace under '{RootDirectory}'.",
            finalFailure);
    }

    public string RootDirectory { get; }

    public string DirectoryPath { get; } = null!;

    public string GetImmediateChildPath(string fileName)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        if (Path.IsPathRooted(fileName) ||
            fileName.IndexOf(Path.DirectorySeparatorChar) >= 0 ||
            fileName.IndexOf(Path.AltDirectorySeparatorChar) >= 0 ||
            fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
            !string.Equals(Path.GetFileName(fileName), fileName, StringComparison.Ordinal) ||
            fileName is "." or "..")
        {
            throw new ArgumentException(
                "A snapshot file name must identify one immediate workspace child.",
                nameof(fileName));
        }

        return ImmediateChild(DirectoryPath, fileName, nameof(fileName));
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return ValueTask.CompletedTask;

        // Cleanup is deliberately non-cancelable. Verify the marker before a
        // recursive delete so a changed or unowned path is preserved.
        VerifyOwnership(ownershipFilePath, ownershipToken);
        Directory.Delete(DirectoryPath, recursive: true);
        CryptographicOperations.ZeroMemory(ownershipToken);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private static string ImmediateChild(
        string parentPath,
        string childName,
        string parameterName)
    {
        string path = Path.GetFullPath(Path.Combine(parentPath, childName));
        string? actualParent = Path.GetDirectoryName(path);
        if (actualParent is null ||
            !PathComparer.Equals(Path.GetFullPath(actualParent), Path.GetFullPath(parentPath)))
        {
            throw new ArgumentException(
                "The path must be an immediate child of the expected directory.",
                parameterName);
        }

        return path;
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
            throw new IOException("The CSV snapshot workspace ownership marker has changed.");

        Span<byte> actual = stackalloc byte[32];
        if (expectedToken.Length != actual.Length)
            throw new IOException("The CSV snapshot workspace ownership token is invalid.");
        marker.ReadExactly(actual);
        if (!actual.SequenceEqual(expectedToken))
            throw new IOException("The CSV snapshot workspace ownership marker has changed.");
        CryptographicOperations.ZeroMemory(actual);
    }

    private static FileStream CreatePrivateFile(
        string path,
        FileAccess access,
        FileShare share,
        FileOptions fileOptions)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = access,
            Share = share,
            BufferSize = 1,
            Options = fileOptions,
        };
        if (!OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
}
