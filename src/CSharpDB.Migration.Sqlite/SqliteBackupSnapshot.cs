using System.Reflection;
using CSharpDB.Migration;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite;

/// <summary>
/// A retained, content-pinned database produced with SQLite's coherent online
/// backup API. The live source is opened read-only; only the caller-selected
/// snapshot destination is written.
/// </summary>
public sealed class SqliteBackupSnapshot
{
    private const string AdapterVersion = "csharpdb-sqlite-adapter-v1";
    private const int BackupPagesPerStep = 256;
    private const int BackupBusyDelayMilliseconds = 10;
    private const int MaxBackupBusyRetries = 500;

    /// <summary>
    /// Default upper bound for a retained SQLite backup (1 TiB).
    /// </summary>
    public const long DefaultMaxSnapshotBytes =
        1024L * 1024 * 1024 * 1024;

    private SqliteBackupSnapshot(
        string filePath,
        long contentLength,
        string contentDigest,
        string sqliteVersion)
    {
        FilePath = filePath;
        ContentLength = contentLength;
        ContentDigest = contentDigest;
        SnapshotIdentity = "sqlite-backup:" + contentDigest;
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Sqlite,
            Identity = "sqlite:backup:" + contentDigest["sha256:".Length..],
            Fingerprint = contentDigest,
            ProviderVersion = AdapterVersion + "/" +
                (typeof(SqliteConnection).Assembly
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                    .InformationalVersion ?? "unknown"),
            SourceVersion = sqliteVersion,
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Backup,
                Description =
                    "Coherent SQLite online backup retained as a SHA-256-pinned, read-only source.",
            },
        };
    }

    public string FilePath { get; }

    public long ContentLength { get; }

    public string ContentDigest { get; }

    public string SnapshotIdentity { get; }

    public MigrationSourceIdentity Source { get; }

    public static async ValueTask<SqliteBackupSnapshot> CreateAsync(
        string sourceFilePath,
        string snapshotFilePath,
        CancellationToken cancellationToken = default) =>
        await CreateAsync(
                sourceFilePath,
                snapshotFilePath,
                DefaultMaxSnapshotBytes,
                cancellationToken)
            .ConfigureAwait(false);

    public static async ValueTask<SqliteBackupSnapshot> CreateAsync(
        string sourceFilePath,
        string snapshotFilePath,
        long maxSnapshotBytes,
        CancellationToken cancellationToken = default)
    {
        if (maxSnapshotBytes < 0 || maxSnapshotBytes == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxSnapshotBytes),
                "The SQLite snapshot byte limit must be non-negative and below Int64.MaxValue.");
        }

        string sourcePath = ResolveExistingFile(sourceFilePath, nameof(sourceFilePath));
        string destinationPath = ResolveDestination(snapshotFilePath);
        if (string.Equals(sourcePath, destinationPath, PathComparison))
        {
            throw new ArgumentException(
                "The SQLite source and snapshot destination must be different files.",
                nameof(snapshotFilePath));
        }
        if (File.Exists(destinationPath) || Directory.Exists(destinationPath))
            throw new IOException("The SQLite snapshot destination already exists.");

        string destinationDirectory = Path.GetDirectoryName(destinationPath)!;
        Directory.CreateDirectory(destinationDirectory);
        string temporaryPath = Path.Combine(
            destinationDirectory,
            "." + Path.GetFileName(destinationPath) + "." +
            Guid.NewGuid().ToString("N", System.Globalization.CultureInfo.InvariantCulture) + ".tmp");

        try
        {
            await using SqliteConnection source = SqliteConnectionFactory.CreateReadOnly(sourcePath);
            await source.OpenAsync(cancellationToken).ConfigureAwait(false);
            await SqliteConnectionFactory.ConfigureReadOnlyAsync(source, cancellationToken)
                .ConfigureAwait(false);

            await using (SqliteConnection destination =
                         SqliteConnectionFactory.CreateDestination(temporaryPath))
            {
                await destination.OpenAsync(cancellationToken).ConfigureAwait(false);
                await CreateBoundedBackupAsync(
                        source,
                        destination,
                        temporaryPath,
                        maxSnapshotBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                await destination.CloseAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            SqliteBackupSnapshot verifiedTemporary =
                await OpenUnpinnedAsync(temporaryPath, cancellationToken)
                    .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporaryPath, destinationPath, overwrite: false);
            return new SqliteBackupSnapshot(
                destinationPath,
                verifiedTemporary.ContentLength,
                verifiedTemporary.ContentDigest,
                verifiedTemporary.Source.SourceVersion ?? "unknown");
        }
        catch (SqliteMigrationException)
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
            exception is SqliteException or IOException or UnauthorizedAccessException)
        {
            TryDeleteTemporary(temporaryPath);
            throw new SqliteMigrationException(
                "The coherent SQLite backup snapshot could not be created.",
                exception);
        }
    }

    private static async ValueTask CreateBoundedBackupAsync(
        SqliteConnection source,
        SqliteConnection destination,
        string temporaryPath,
        long maxSnapshotBytes,
        CancellationToken cancellationToken)
    {
        (long pageSize, long pageCount) =
            await ReadSourceLayoutAsync(source, cancellationToken).ConfigureAwait(false);
        long maxPageCount = maxSnapshotBytes / pageSize;
        if (pageCount > maxPageCount)
            throw SnapshotLimitExceeded();

        await ConfigureDestinationLimitAsync(
                destination,
                pageSize,
                maxPageCount,
                cancellationToken)
            .ConfigureAwait(false);
        ConfigureNativeBackupBusyHandling(source, destination);

        SQLitePCL.sqlite3_backup backup = SQLitePCL.raw.sqlite3_backup_init(
            destination.Handle,
            "main",
            source.Handle,
            "main");
        if (backup is null || backup.IsInvalid)
        {
            backup?.Dispose();
            throw new SqliteMigrationException(
                "The coherent SQLite backup snapshot could not be started.");
        }

        bool backupCompleted = false;
        int busyRetryCount = 0;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int result = SQLitePCL.raw.sqlite3_backup_step(
                    backup,
                    BackupPagesPerStep);
                int primaryResult = result & 0xff;

                int reportedPageCount =
                    SQLitePCL.raw.sqlite3_backup_pagecount(backup);
                if (reportedPageCount < 0 ||
                    reportedPageCount > maxPageCount ||
                    GetFileLength(temporaryPath) > maxSnapshotBytes)
                {
                    throw SnapshotLimitExceeded();
                }

                if (primaryResult == SQLitePCL.raw.SQLITE_DONE)
                {
                    backupCompleted = true;
                    break;
                }

                if (primaryResult == SQLitePCL.raw.SQLITE_OK)
                {
                    await Task.Yield();
                    continue;
                }

                if (primaryResult == SQLitePCL.raw.SQLITE_BUSY ||
                    primaryResult == SQLitePCL.raw.SQLITE_LOCKED)
                {
                    if (busyRetryCount >= MaxBackupBusyRetries)
                    {
                        throw new SqliteMigrationException(
                            "The SQLite backup remained busy beyond the bounded retry window.");
                    }

                    busyRetryCount++;
                    await Task.Delay(
                            BackupBusyDelayMilliseconds,
                            cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                throw new SqliteMigrationException(
                    primaryResult == SQLitePCL.raw.SQLITE_FULL
                        ? "The SQLite backup snapshot exceeds the configured byte limit."
                        : "The coherent SQLite backup snapshot could not be created.");
            }
        }
        finally
        {
            int finishResult = backup.IsInvalid
                ? SQLitePCL.raw.SQLITE_OK
                : SQLitePCL.raw.sqlite3_backup_finish(backup);
            backup.Dispose();
            if (backupCompleted && finishResult != SQLitePCL.raw.SQLITE_OK)
            {
                throw new SqliteMigrationException(
                    "The coherent SQLite backup snapshot could not be finalized.");
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (GetFileLength(temporaryPath) > maxSnapshotBytes)
            throw SnapshotLimitExceeded();
    }

    private static async ValueTask<(long PageSize, long PageCount)> ReadSourceLayoutAsync(
        SqliteConnection source,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = source.CreateCommand();
        command.CommandText = "PRAGMA page_size;";
        long pageSize = Convert.ToInt64(
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
            System.Globalization.CultureInfo.InvariantCulture);
        command.CommandText = "PRAGMA page_count;";
        long pageCount = Convert.ToInt64(
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
            System.Globalization.CultureInfo.InvariantCulture);
        if (pageSize <= 0 || pageCount < 0)
        {
            throw new SqliteMigrationException(
                "The SQLite source page layout could not be bounded safely.");
        }

        return (pageSize, pageCount);
    }

    private static async ValueTask ConfigureDestinationLimitAsync(
        SqliteConnection destination,
        long pageSize,
        long maxPageCount,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = destination.CreateCommand();
        command.CommandText =
            "PRAGMA page_size = " +
            pageSize.ToString(System.Globalization.CultureInfo.InvariantCulture) +
            ";";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (maxPageCount == 0)
            return;

        command.CommandText =
            "PRAGMA max_page_count = " +
            maxPageCount.ToString(System.Globalization.CultureInfo.InvariantCulture) +
            ";";
        long appliedMaxPageCount = Convert.ToInt64(
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
            System.Globalization.CultureInfo.InvariantCulture);
        if (appliedMaxPageCount <= 0 ||
            appliedMaxPageCount > maxPageCount)
        {
            throw new SqliteMigrationException(
                "The SQLite snapshot page limit could not be established safely.");
        }
    }

    private static void ConfigureNativeBackupBusyHandling(
        SqliteConnection source,
        SqliteConnection destination)
    {
        int sourceResult = SQLitePCL.raw.sqlite3_busy_timeout(
            source.Handle,
            0);
        int destinationResult = SQLitePCL.raw.sqlite3_busy_timeout(
            destination.Handle,
            0);
        if (sourceResult != SQLitePCL.raw.SQLITE_OK ||
            destinationResult != SQLitePCL.raw.SQLITE_OK)
        {
            throw new SqliteMigrationException(
                "The bounded SQLite backup retry policy could not be established.");
        }
    }

    private static long GetFileLength(string path)
    {
        try
        {
            return new FileInfo(path).Length;
        }
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException)
        {
            throw new SqliteMigrationException(
                "The SQLite backup snapshot size could not be verified safely.",
                exception);
        }
    }

    private static SqliteMigrationException SnapshotLimitExceeded() =>
        new("The SQLite backup snapshot exceeds the configured byte limit.");

    public static async ValueTask<SqliteBackupSnapshot> OpenAsync(
        string snapshotFilePath,
        string expectedContentDigest,
        CancellationToken cancellationToken = default)
    {
        ValidateDigest(expectedContentDigest, nameof(expectedContentDigest));
        SqliteBackupSnapshot snapshot =
            await OpenUnpinnedAsync(snapshotFilePath, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(
                snapshot.ContentDigest,
                expectedContentDigest,
                StringComparison.Ordinal))
        {
            throw new SqliteMigrationException(
                "The SQLite backup snapshot does not match its trusted SHA-256 digest.");
        }

        return snapshot;
    }

    internal async ValueTask<SqliteConnection> OpenVerifiedReadOnlyConnectionAsync(
        CancellationToken cancellationToken)
    {
        string actualDigest = await SqliteStableDigest.FileAsync(FilePath, cancellationToken)
            .ConfigureAwait(false);
        if (!string.Equals(ContentDigest, actualDigest, StringComparison.Ordinal))
        {
            throw new SqliteMigrationException(
                "The SQLite backup snapshot changed after it was opened.");
        }

        var connection = SqliteConnectionFactory.CreateReadOnly(FilePath);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await SqliteConnectionFactory.ConfigureReadOnlyAsync(connection, cancellationToken)
                .ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask<SqliteBackupSnapshot> OpenUnpinnedAsync(
        string snapshotFilePath,
        CancellationToken cancellationToken)
    {
        string path = ResolveExistingFile(snapshotFilePath, nameof(snapshotFilePath));
        try
        {
            string digest = await SqliteStableDigest.FileAsync(path, cancellationToken)
                .ConfigureAwait(false);
            long length = new FileInfo(path).Length;
            await using SqliteConnection connection = SqliteConnectionFactory.CreateReadOnly(path);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await SqliteConnectionFactory.ConfigureReadOnlyAsync(connection, cancellationToken)
                .ConfigureAwait(false);
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT sqlite_version();";
            string sqliteVersion = Convert.ToString(
                    await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                    System.Globalization.CultureInfo.InvariantCulture) ??
                "unknown";
            return new SqliteBackupSnapshot(path, length, digest, sqliteVersion);
        }
        catch (SqliteMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqliteException or IOException or UnauthorizedAccessException)
        {
            throw new SqliteMigrationException(
                "The SQLite backup snapshot could not be opened read-only.",
                exception);
        }
    }

    private static string ResolveExistingFile(string path, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("A nonblank SQLite file path is required.", parameterName);
        string fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException("The SQLite source file does not exist.");
        if ((File.GetAttributes(fullPath) & FileAttributes.Directory) != 0)
            throw new ArgumentException("The SQLite source must be a regular file.", parameterName);
        return fullPath;
    }

    private static string ResolveDestination(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException(
                "A nonblank SQLite snapshot destination is required.",
                nameof(path));
        }

        string fullPath = Path.GetFullPath(path);
        if (string.IsNullOrEmpty(Path.GetFileName(fullPath)))
            throw new ArgumentException("The SQLite snapshot destination must be a file.", nameof(path));
        return fullPath;
    }

    private static void ValidateDigest(string digest, string parameterName)
    {
        if (digest is null ||
            digest.Length != "sha256:".Length + 64 ||
            !digest.StartsWith("sha256:", StringComparison.Ordinal) ||
            digest.AsSpan("sha256:".Length).ContainsAnyExcept(
                "0123456789abcdef".AsSpan()))
        {
            throw new ArgumentException(
                "The expected SQLite snapshot digest must be canonical lowercase SHA-256.",
                parameterName);
        }
    }

    private static void TryDeleteTemporary(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
            string journal = path + "-journal";
            if (File.Exists(journal))
                File.Delete(journal);
            string wal = path + "-wal";
            if (File.Exists(wal))
                File.Delete(wal);
            string shm = path + "-shm";
            if (File.Exists(shm))
                File.Delete(shm);
        }
        catch
        {
            // Preserve the primary failure. Temporary paths are randomized and
            // never used as a future trusted snapshot.
        }
    }

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
}
