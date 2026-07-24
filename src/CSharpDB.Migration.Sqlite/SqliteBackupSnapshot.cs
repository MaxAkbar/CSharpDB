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
        CancellationToken cancellationToken = default)
    {
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
                source.BackupDatabase(destination);
                await destination.CloseAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporaryPath, destinationPath, overwrite: false);
            return await OpenUnpinnedAsync(destinationPath, cancellationToken).ConfigureAwait(false);
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
