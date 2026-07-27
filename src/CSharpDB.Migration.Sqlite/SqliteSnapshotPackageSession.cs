using System.Buffers;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Sqlite;

/// <summary>
/// Bounds and environment policy for reopening a retained SQLite backup.
/// </summary>
public sealed record SqliteSnapshotPackageOpenOptions
{
    public const long DefaultMaxSourceBytes = 1024L * 1024 * 1024 * 1024;

    public const int DefaultCopyBufferBytes = 128 * 1024;

    /// <summary>
    /// Caller-controlled parent directory for a unique private copy. The
    /// retained package itself is never used as the live migration reader. The
    /// parent must not be writable, renamed, or replaced by an untrusted
    /// principal while the session is open.
    /// </summary>
    public string? WorkspacePath { get; init; }

    public long MaxSourceBytes { get; init; } = DefaultMaxSourceBytes;

    public int CopyBufferBytes { get; init; } = DefaultCopyBufferBytes;

    public required string ExpectedContentDigest { get; init; }
}

/// <summary>
/// Owns a verified private copy of one retained SQLite backup and the
/// catalog-bound data source reconstructed from it.
/// </summary>
public sealed class SqliteSnapshotPackageSession : IAsyncDisposable
{
    public const string Format = "csharpdb-sqlite-backup-v1";

    private readonly object gate = new();
    private readonly SqliteSnapshotWorkspace workspace;
    private readonly FileStream snapshotGuard;
    private readonly SqliteBackupSnapshot snapshot;
    private Task? disposeTask;

    private SqliteSnapshotPackageSession(
        SqliteSnapshotWorkspace workspace,
        FileStream snapshotGuard,
        SqliteBackupSnapshot snapshot,
        MigrationCatalog catalog,
        SqliteMigrationDataSource dataSource)
    {
        this.workspace = workspace;
        this.snapshotGuard = snapshotGuard;
        this.snapshot = snapshot;
        Catalog = catalog;
        DataSource = dataSource;
    }

    public MigrationCatalog Catalog { get; }

    public SqliteMigrationDataSource DataSource { get; }

    public string ContentDigest => snapshot.ContentDigest;

    internal string PrivateSnapshotPath => snapshot.FilePath;

    public static async ValueTask<SqliteSnapshotPackageSession> OpenAsync(
        string packagePath,
        MigrationCatalog catalog,
        SqliteSnapshotPackageOpenOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);
        MigrationContractValidator.ValidateCatalog(catalog);
        MigrationInspectionRequest inspectionRequest =
            ReconstructInspectionRequest(catalog);

        string inputPath = ResolvePackagePath(packagePath);
        string workspacePath = ResolveWorkspace(options.WorkspacePath);
        cancellationToken.ThrowIfCancellationRequested();

        SqliteSnapshotWorkspace? privateWorkspace = null;
        string? privateSnapshotPath = null;
        FileStream? snapshotGuard = null;
        SqliteMigrationDataSource? dataSource = null;
        try
        {
            await using FileStream package = OpenPackage(
                inputPath,
                options.CopyBufferBytes);
            ValidateOpenedPackage(package);
            if (package.Length > options.MaxSourceBytes)
            {
                throw new SqliteMigrationException(
                    "The retained SQLite backup exceeds the configured source byte limit.");
            }

            privateWorkspace = new SqliteSnapshotWorkspace(workspacePath);
            privateSnapshotPath =
                privateWorkspace.GetImmediateChildPath(
                    "snapshot.csdbsqlite");
            _ = privateWorkspace.GetImmediateChildPath(
                "snapshot.csdbsqlite-journal");
            _ = privateWorkspace.GetImmediateChildPath(
                "snapshot.csdbsqlite-wal");
            _ = privateWorkspace.GetImmediateChildPath(
                "snapshot.csdbsqlite-shm");
            string contentDigest = await CopyAndHashAsync(
                    package,
                    privateSnapshotPath,
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
            snapshotGuard = OpenSnapshotGuard(
                privateSnapshotPath,
                options.CopyBufferBytes);

            SqliteBackupSnapshot snapshot =
                await SqliteBackupSnapshot.OpenAsync(
                        privateSnapshotPath,
                        contentDigest,
                        cancellationToken)
                    .ConfigureAwait(false);
            MigrationCatalog reconstructed =
                await new SqliteMigrationSourceInspector(snapshot)
                    .InspectAsync(inspectionRequest, cancellationToken)
                    .ConfigureAwait(false);
            MigrationContractValidator.ValidateCatalog(reconstructed);

            string expectedCatalogDigest =
                MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
            string actualCatalogDigest =
                MigrationArtifactSerializer.ComputeCatalogDigest(reconstructed);
            if (!string.Equals(
                    expectedCatalogDigest,
                    actualCatalogDigest,
                    StringComparison.Ordinal))
            {
                throw new SqliteMigrationException(
                    "The retained SQLite backup does not match the supplied migration catalog.");
            }

            dataSource = await SqliteMigrationDataSource.CreateAsync(
                    snapshot,
                    catalog,
                    cancellationToken)
                .ConfigureAwait(false);
            var session = new SqliteSnapshotPackageSession(
                privateWorkspace,
                snapshotGuard,
                snapshot,
                catalog,
                dataSource);
            dataSource = null;
            snapshotGuard = null;
            privateWorkspace = null;
            privateSnapshotPath = null;
            return session;
        }
        catch (Exception operationFailure)
        {
            Exception? cleanupFailure = await CleanupAfterFailureAsync(
                    dataSource,
                    snapshotGuard,
                    privateWorkspace)
                .ConfigureAwait(false);
            if (cleanupFailure is not null)
                throw new AggregateException(operationFailure, cleanupFailure);

            ExceptionDispatchInfo.Capture(operationFailure).Throw();
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (gate)
        {
            disposeTask ??= DisposeCoreAsync();
            return new ValueTask(disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        var failures = new List<Exception>();
        try
        {
            await DataSource.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }

        try
        {
            await snapshotGuard.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }

        try
        {
            await workspace.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(new SqliteMigrationException(
                "The private SQLite migration workspace could not be removed.",
                exception));
        }

        if (failures.Count == 0)
        {
            GC.SuppressFinalize(this);
            return;
        }
        if (failures.Count == 1)
            ExceptionDispatchInfo.Capture(failures[0]).Throw();
        throw new AggregateException(failures);
    }

    private static void ValidateOptions(
        SqliteSnapshotPackageOpenOptions options)
    {
        if (options.MaxSourceBytes < 0 ||
            options.MaxSourceBytes == long.MaxValue)
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
        if (options.WorkspacePath is not null &&
            string.IsNullOrWhiteSpace(options.WorkspacePath))
        {
            throw new ArgumentException(
                "The snapshot workspace path cannot be blank.",
                nameof(options));
        }
        if (!IsCanonicalDigest(options.ExpectedContentDigest))
        {
            throw new ArgumentException(
                "The expected SQLite backup digest must be canonical lowercase SHA-256.",
                nameof(options));
        }
    }

    private static MigrationInspectionRequest ReconstructInspectionRequest(
        MigrationCatalog catalog)
    {
        if (catalog.Source.Kind != MigrationSourceKind.Sqlite)
        {
            throw new ArgumentException(
                "The migration catalog is not a SQLite catalog.",
                nameof(catalog));
        }

        MigrationCatalogObject[] namespaces = catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Namespace &&
                string.Equals(item.SourceName, "main", StringComparison.Ordinal))
            .ToArray();
        if (namespaces.Length != 1)
        {
            throw new ArgumentException(
                "The SQLite migration catalog contract is unsupported.",
                nameof(catalog));
        }

        MigrationCatalogObject main = namespaces[0];
        string contract = RequireSingleFacet(
            main,
            "sqliteCatalogContract",
            catalog);
        if (!string.Equals(
                contract,
                SqliteMigrationSourceInspector.CatalogContract,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The SQLite migration catalog contract is unsupported.",
                nameof(catalog));
        }

        string profileIncluded = RequireSingleFacet(
            main,
            "sqliteProfileIncluded",
            catalog);
        bool includeProfile;
        if (string.Equals(profileIncluded, "true", StringComparison.Ordinal))
            includeProfile = true;
        else if (string.Equals(profileIncluded, "false", StringComparison.Ordinal))
            includeProfile = false;
        else
        {
            throw new ArgumentException(
                "The SQLite migration catalog inspection recipe is invalid.",
                nameof(catalog));
        }

        string profileSampleSizeText = RequireSingleFacet(
            main,
            "sqliteProfileSampleSize",
            catalog);
        if (!int.TryParse(
                profileSampleSizeText,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int profileSampleSize) ||
            profileSampleSize <= 0 ||
            !string.Equals(
                profileSampleSizeText,
                profileSampleSize.ToString(CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The SQLite migration catalog inspection recipe is invalid.",
                nameof(catalog));
        }

        return new MigrationInspectionRequest
        {
            TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
            IncludeProfile = includeProfile,
            ProfileSampleSize = profileSampleSize,
        };
    }

    private static string RequireSingleFacet(
        MigrationCatalogObject item,
        string name,
        MigrationCatalog catalog)
    {
        string?[] values = item.Facets
            .Where(facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))
            .Select(facet => facet.Value)
            .ToArray();
        if (values.Length != 1 || values[0] is not string value)
        {
            throw new ArgumentException(
                "The SQLite migration catalog inspection recipe is invalid.",
                nameof(catalog));
        }

        return value;
    }

    private static async ValueTask<string> CopyAndHashAsync(
        FileStream package,
        string destinationPath,
        SqliteSnapshotPackageOpenOptions options,
        CancellationToken cancellationToken)
    {
        byte[] expectedDigest = Convert.FromHexString(
            options.ExpectedContentDigest.AsSpan("sha256:".Length));
        byte[] buffer = ArrayPool<byte>.Shared.Rent(options.CopyBufferBytes);
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        try
        {
            await using FileStream destination = CreatePrivateFile(
                destinationPath,
                options.CopyBufferBytes);
            long copied = 0;
            while (true)
            {
                int read = await package.ReadAsync(
                        buffer.AsMemory(0, options.CopyBufferBytes),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                if (copied > options.MaxSourceBytes - read)
                {
                    throw new SqliteMigrationException(
                        "The retained SQLite backup exceeds the configured source byte limit.");
                }

                hash.AppendData(buffer, 0, read);
                await destination.WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
                copied += read;
            }

            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            destination.Flush(flushToDisk: true);
            byte[] actualDigest = hash.GetHashAndReset();
            try
            {
                if (!CryptographicOperations.FixedTimeEquals(
                        expectedDigest,
                        actualDigest))
                {
                    throw new SqliteMigrationException(
                        "The retained SQLite backup does not match its trusted SHA-256 digest.");
                }

                return "sha256:" +
                    Convert.ToHexString(actualDigest).ToLowerInvariant();
            }
            finally
            {
                CryptographicOperations.ZeroMemory(actualDigest);
            }
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
            exception is IOException or UnauthorizedAccessException)
        {
            throw new SqliteMigrationException(
                "The retained SQLite backup could not be copied into its private workspace.",
                exception);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expectedDigest);
            CryptographicOperations.ZeroMemory(
                buffer.AsSpan(0, options.CopyBufferBytes));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static string ResolvePackagePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("A retained SQLite backup path is required.", nameof(path));
        string fullPath = Path.GetFullPath(path);
        RejectAlternateDataStream(fullPath);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException("The retained SQLite backup does not exist.");
        return fullPath;
    }

    private static string ResolveWorkspace(string? workspacePath)
    {
        string fullPath = Path.GetFullPath(workspacePath ?? Path.GetTempPath());
        if (!Directory.Exists(fullPath))
        {
            throw new DirectoryNotFoundException(
                "The retained-source workspace does not exist.");
        }

        FileAttributes attributes = File.GetAttributes(fullPath);
        if ((attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            throw new SqliteMigrationException(
                "The retained-source workspace cannot be a link, reparse point, or device.");
        }

        return fullPath;
    }

    private static FileStream OpenPackage(string path, int bufferSize) =>
        new(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                BufferSize = bufferSize,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            });

    private static FileStream OpenSnapshotGuard(
        string path,
        int bufferSize)
    {
        var guard = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                BufferSize = bufferSize,
                Options =
                    FileOptions.Asynchronous |
                    FileOptions.SequentialScan,
            });
        try
        {
            ValidateOpenedPackage(guard);
            return guard;
        }
        catch
        {
            guard.Dispose();
            throw;
        }
    }

    private static void ValidateOpenedPackage(FileStream package)
    {
        FileAttributes attributes = File.GetAttributes(package.SafeFileHandle);
        if ((attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0 ||
            !package.CanRead ||
            !package.CanSeek)
        {
            throw new SqliteMigrationException(
                "The retained SQLite backup handle is not a regular seekable file.");
        }
    }

    private static FileStream CreatePrivateFile(string path, int bufferSize)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = bufferSize,
            Options = FileOptions.Asynchronous |
                FileOptions.SequentialScan |
                FileOptions.WriteThrough,
        };
        if (!OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private static async ValueTask<Exception?> CleanupAfterFailureAsync(
        SqliteMigrationDataSource? dataSource,
        FileStream? snapshotGuard,
        SqliteSnapshotWorkspace? workspace)
    {
        var failures = new List<Exception>();
        if (dataSource is not null)
        {
            try
            {
                await dataSource.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (snapshotGuard is not null)
        {
            try
            {
                await snapshotGuard.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (workspace is not null)
        {
            try
            {
                await workspace.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(new SqliteMigrationException(
                    "The private SQLite migration workspace could not be removed.",
                    exception));
            }
        }

        return failures.Count switch
        {
            0 => null,
            1 => failures[0],
            _ => new AggregateException(failures),
        };
    }

    private static void RejectAlternateDataStream(string fullPath)
    {
        if (!OperatingSystem.IsWindows())
            return;
        string root = Path.GetPathRoot(fullPath) ?? string.Empty;
        if (fullPath.AsSpan(root.Length).Contains(':'))
        {
            throw new SqliteMigrationException(
                "Windows alternate data streams cannot be used as retained SQLite backups.");
        }
    }

    private static bool IsCanonicalDigest(string? digest) =>
        digest is not null &&
        digest.Length == "sha256:".Length + 64 &&
        digest.StartsWith("sha256:", StringComparison.Ordinal) &&
        !digest.AsSpan("sha256:".Length).ContainsAnyExcept(
            "0123456789abcdef".AsSpan());
}
