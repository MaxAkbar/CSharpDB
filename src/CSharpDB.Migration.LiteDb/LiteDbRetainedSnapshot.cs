using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using CSharpDB.Migration;
using LiteDB;

namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// A content-pinned LiteDB database captured while the source file is
/// quiesced. LiteDB does not expose a SQLite-style online backup API, so the
/// capture acquires a read-only handle that denies concurrent writers and
/// deletes for the duration of the copy.
/// </summary>
public sealed class LiteDbRetainedSnapshot
{
    private const string AdapterVersion = "csharpdb-litedb-adapter-v1";
    private const int CopyBufferBytes = 128 * 1024;

    /// <summary>
    /// Default upper bound for a retained LiteDB snapshot (1 TiB).
    /// </summary>
    public const long DefaultMaxSnapshotBytes =
        1024L * 1024 * 1024 * 1024;

    private LiteDbRetainedSnapshot(
        string filePath,
        long contentLength,
        string contentDigest)
    {
        FilePath = filePath;
        ContentLength = contentLength;
        ContentDigest = contentDigest;
        SnapshotIdentity = "litedb-snapshot:" + contentDigest;
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.LiteDb,
            Identity =
                "litedb:snapshot:" +
                contentDigest["sha256:".Length..],
            Fingerprint = contentDigest,
            ProviderVersion =
                AdapterVersion + "/" + ProviderVersion(),
            SourceVersion = "5",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description =
                    "Offline/quiesced LiteDB file capture retained as a SHA-256-pinned, read-only source.",
            },
        };
    }

    public string FilePath { get; }

    public long ContentLength { get; }

    public string ContentDigest { get; }

    public string SnapshotIdentity { get; }

    public MigrationSourceIdentity Source { get; }

    public static ValueTask<LiteDbRetainedSnapshot> CreateAsync(
        string sourceFilePath,
        string snapshotFilePath,
        CancellationToken cancellationToken = default) =>
        CreateAsync(
            sourceFilePath,
            snapshotFilePath,
            DefaultMaxSnapshotBytes,
            cancellationToken);

    public static async ValueTask<LiteDbRetainedSnapshot> CreateAsync(
        string sourceFilePath,
        string snapshotFilePath,
        long maxSnapshotBytes,
        CancellationToken cancellationToken = default)
    {
        ValidateByteLimit(maxSnapshotBytes, nameof(maxSnapshotBytes));
        string sourcePath = ResolveExistingFile(
            sourceFilePath,
            nameof(sourceFilePath));
        string destinationPath = ResolveDestination(
            snapshotFilePath,
            nameof(snapshotFilePath));
        if (string.Equals(sourcePath, destinationPath, PathComparison))
        {
            throw new ArgumentException(
                "The LiteDB source and snapshot destination must be different files.",
                nameof(snapshotFilePath));
        }
        if (File.Exists(destinationPath) ||
            Directory.Exists(destinationPath))
        {
            throw new IOException(
                "The LiteDB snapshot destination already exists.");
        }

        string destinationDirectory =
            Path.GetDirectoryName(destinationPath)!;
        Directory.CreateDirectory(destinationDirectory);
        string temporaryPath = Path.Combine(
            destinationDirectory,
            "." + Path.GetFileName(destinationPath) + "." +
            Guid.NewGuid().ToString(
                "N",
                System.Globalization.CultureInfo.InvariantCulture) +
            ".tmp");

        try
        {
            await using (var source = new FileStream(
                sourcePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                CopyBufferBytes,
                FileOptions.Asynchronous |
                FileOptions.SequentialScan))
            {
                if (source.Length > maxSnapshotBytes)
                    throw SnapshotLimitExceeded();

                await using var destination = new FileStream(
                    temporaryPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    CopyBufferBytes,
                    FileOptions.Asynchronous |
                    FileOptions.SequentialScan |
                    FileOptions.WriteThrough);
                await CopyBoundedAsync(
                        source,
                        destination,
                        maxSnapshotBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken)
                    .ConfigureAwait(false);
                destination.Flush(flushToDisk: true);
            }

            cancellationToken.ThrowIfCancellationRequested();
            LiteDbRetainedSnapshot verified =
                await OpenUnpinnedAsync(
                        temporaryPath,
                        cancellationToken)
                    .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(
                temporaryPath,
                destinationPath,
                overwrite: false);
            return new LiteDbRetainedSnapshot(
                destinationPath,
                verified.ContentLength,
                verified.ContentDigest);
        }
        catch (LiteDbMigrationException)
        {
            TryDeleteTemporary(temporaryPath);
            throw;
        }
        catch (OperationCanceledException)
        {
            TryDeleteTemporary(temporaryPath);
            throw;
        }
        catch (Exception exception) when (
            exception is LiteException or IOException or
            UnauthorizedAccessException or InvalidOperationException)
        {
            TryDeleteTemporary(temporaryPath);
            throw new LiteDbMigrationException(
                "The offline/quiesced LiteDB snapshot could not be created. Close all writers and verify that the source is an unencrypted LiteDB 5 database.",
                exception);
        }
    }

    public static async ValueTask<LiteDbRetainedSnapshot> OpenAsync(
        string snapshotFilePath,
        string expectedContentDigest,
        CancellationToken cancellationToken = default)
    {
        ValidateDigest(
            expectedContentDigest,
            nameof(expectedContentDigest));
        LiteDbRetainedSnapshot snapshot =
            await OpenUnpinnedAsync(
                    snapshotFilePath,
                    cancellationToken)
                .ConfigureAwait(false);
        if (!string.Equals(
                snapshot.ContentDigest,
                expectedContentDigest,
                StringComparison.Ordinal))
        {
            throw new LiteDbMigrationException(
                "The LiteDB snapshot does not match its trusted SHA-256 digest.");
        }

        return snapshot;
    }

    internal async ValueTask<LiteDatabase>
        OpenVerifiedReadOnlyDatabaseAsync(
            CancellationToken cancellationToken)
    {
        string before = await ComputeDigestAsync(
                FilePath,
                cancellationToken)
            .ConfigureAwait(false);
        if (!string.Equals(
                ContentDigest,
                before,
                StringComparison.Ordinal))
        {
            throw new LiteDbMigrationException(
                "The LiteDB snapshot changed after it was opened.");
        }

        LiteDatabase? database = null;
        try
        {
            database = OpenReadOnlyDatabase(FilePath);
            _ = database.UserVersion;
            string after = await ComputeDigestAsync(
                    FilePath,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!string.Equals(
                    before,
                    after,
                    StringComparison.Ordinal))
            {
                throw new LiteDbMigrationException(
                    "The LiteDB snapshot changed while it was being opened.");
            }

            return database;
        }
        catch
        {
            database?.Dispose();
            throw;
        }
    }

    private static async ValueTask<LiteDbRetainedSnapshot>
        OpenUnpinnedAsync(
            string snapshotFilePath,
            CancellationToken cancellationToken)
    {
        string path = ResolveExistingFile(
            snapshotFilePath,
            nameof(snapshotFilePath));
        try
        {
            string digest = await ComputeDigestAsync(
                    path,
                    cancellationToken)
                .ConfigureAwait(false);
            long length = new FileInfo(path).Length;
            using LiteDatabase database =
                OpenReadOnlyDatabase(path);
            _ = database.UserVersion;
            _ = database.GetCollectionNames().Count();
            return new LiteDbRetainedSnapshot(
                path,
                length,
                digest);
        }
        catch (LiteDbMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is LiteException or IOException or
            UnauthorizedAccessException or InvalidOperationException)
        {
            throw new LiteDbMigrationException(
                "The retained source is not a supported unencrypted LiteDB 5 database.",
                exception);
        }
    }

    private static LiteDatabase OpenReadOnlyDatabase(
        string path) =>
        new(
            new ConnectionString
            {
                Filename = path,
                Connection = ConnectionType.Direct,
                ReadOnly = true,
                Upgrade = false,
            },
            new BsonMapper());

    private static async ValueTask CopyBoundedAsync(
        Stream source,
        Stream destination,
        long maxSnapshotBytes,
        CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[CopyBufferBytes];
        long copied = 0;
        while (true)
        {
            int read = await source.ReadAsync(
                    buffer.AsMemory(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
                break;
            if (copied > maxSnapshotBytes - read)
                throw SnapshotLimitExceeded();
            await destination.WriteAsync(
                    buffer.AsMemory(0, read),
                    cancellationToken)
                .ConfigureAwait(false);
            copied += read;
        }
    }

    private static async ValueTask<string> ComputeDigestAsync(
        string path,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            CopyBufferBytes,
            FileOptions.Asynchronous |
            FileOptions.SequentialScan);
        byte[] digest = await SHA256.HashDataAsync(
                stream,
                cancellationToken)
            .ConfigureAwait(false);
        return "sha256:" +
            Convert.ToHexString(digest).ToLowerInvariant();
    }

    private static string ResolveExistingFile(
        string path,
        string parameterName)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException(
                "A nonblank LiteDB file path is required.",
                parameterName);
        }

        string fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException(
                "The LiteDB source file does not exist.");
        }
        FileAttributes attributes = File.GetAttributes(fullPath);
        if ((attributes & FileAttributes.Directory) != 0)
        {
            throw new ArgumentException(
                "The LiteDB source must be a regular file.",
                parameterName);
        }
        if ((attributes &
             (FileAttributes.ReparsePoint |
              FileAttributes.Device)) != 0)
        {
            throw new ArgumentException(
                "The LiteDB source cannot be a link, reparse point, or device.",
                parameterName);
        }

        return fullPath;
    }

    private static string ResolveDestination(
        string path,
        string parameterName)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException(
                "A nonblank LiteDB snapshot destination is required.",
                parameterName);
        }

        string fullPath = Path.GetFullPath(path);
        if (string.IsNullOrEmpty(Path.GetFileName(fullPath)))
        {
            throw new ArgumentException(
                "The LiteDB snapshot destination must be a file.",
                parameterName);
        }

        return fullPath;
    }

    private static void ValidateByteLimit(
        long value,
        string parameterName)
    {
        if (value < 0 || value == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                "The LiteDB snapshot byte limit must be non-negative and below Int64.MaxValue.");
        }
    }

    private static void ValidateDigest(
        string digest,
        string parameterName)
    {
        if (digest is null ||
            digest.Length != "sha256:".Length + 64 ||
            !digest.StartsWith(
                "sha256:",
                StringComparison.Ordinal) ||
            digest.AsSpan("sha256:".Length)
                .ContainsAnyExcept(
                    "0123456789abcdef".AsSpan()))
        {
            throw new ArgumentException(
                "The expected LiteDB snapshot digest must be canonical lowercase SHA-256.",
                parameterName);
        }
    }

    private static string ProviderVersion()
    {
        Assembly assembly = typeof(LiteDatabase).Assembly;
        return assembly
                   .GetCustomAttribute<
                       AssemblyInformationalVersionAttribute>()?
                   .InformationalVersion ??
            FileVersionInfo
                .GetVersionInfo(assembly.Location)
                .FileVersion ??
            assembly.GetName().Version?.ToString() ??
            "5.0.21";
    }

    private static LiteDbMigrationException
        SnapshotLimitExceeded() =>
        new(
            "The LiteDB snapshot exceeds the configured byte limit.");

    private static void TryDeleteTemporary(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
            string logPath = path + "-log";
            if (File.Exists(logPath))
                File.Delete(logPath);
        }
        catch
        {
            // Preserve the primary failure. Temporary paths are randomized and
            // never used as a future trusted snapshot.
        }
    }

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() ||
        OperatingSystem.IsMacOS()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
}
