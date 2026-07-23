using System.Globalization;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Engine;

/// <summary>
/// Resource and workspace limits used while creating or opening a retained database snapshot.
/// </summary>
public sealed class RetainedDatabaseSnapshotOptions
{
    public const long DefaultMaxDatabaseBytes = 1L << 40;
    public const long DefaultMaxWalBytes = 4L << 30;
    public const long DefaultMaxSnapshotBytes = 1L << 40;
    public const int DefaultMaxEncodedRowBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Optional existing, trusted, link-free parent for private working directories.
    /// When null, a random private child is created directly beneath the validated
    /// physical operating-system temporary directory.
    /// </summary>
    public string? WorkspacePath { get; init; }

    public long MaxDatabaseBytes { get; init; } = DefaultMaxDatabaseBytes;

    public long MaxWalBytes { get; init; } = DefaultMaxWalBytes;

    public long MaxSnapshotBytes { get; init; } = DefaultMaxSnapshotBytes;

    /// <summary>
    /// Maximum encoded physical row payload materialized by a retained table
    /// reader. Oversized overflow records are rejected before their overflow
    /// chain is read or allocated.
    /// </summary>
    public int MaxEncodedRowBytes { get; init; } = DefaultMaxEncodedRowBytes;

    public int CopyBufferBytes { get; init; } = 128 * 1024;

    public int MaxCachedPages { get; init; } = 256;

    public int MaxCachedWalReadPages { get; init; } = 64;

    internal void Validate()
    {
        if (MaxDatabaseBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDatabaseBytes));
        if (MaxWalBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxWalBytes));
        if (MaxSnapshotBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxSnapshotBytes));
        if (MaxEncodedRowBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxEncodedRowBytes));
        if (CopyBufferBytes is < 4096 or > 16 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(CopyBufferBytes),
                "CopyBufferBytes must be between 4 KiB and 16 MiB.");
        }
        if (MaxCachedPages <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxCachedPages));
        if (MaxCachedWalReadPages < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxCachedWalReadPages));
        if (WorkspacePath is not null && string.IsNullOrWhiteSpace(WorkspacePath))
            throw new ArgumentException("WorkspacePath cannot be empty.", nameof(WorkspacePath));
    }
}

/// <summary>
/// Path-independent identity of an immutable retained database snapshot.
/// </summary>
public sealed record RetainedDatabaseSnapshotIdentity(
    long ByteLength,
    string Sha256,
    string SnapshotIdentity);

/// <summary>
/// Result of atomically publishing an immutable retained database snapshot.
/// </summary>
public sealed record RetainedDatabaseSnapshotReceipt(
    string SnapshotPath,
    long ByteLength,
    string Sha256,
    string SnapshotIdentity)
{
    public RetainedDatabaseSnapshotIdentity Identity =>
        new(ByteLength, Sha256, SnapshotIdentity);
}

/// <summary>
/// Creates and verifies immutable, portable database snapshots for long-running read workflows.
/// </summary>
public static class RetainedDatabaseSnapshot
{
    private const string IdentityPrefix = "csharpdb-retained-snapshot/v1:";

    public static async ValueTask<RetainedDatabaseSnapshotReceipt> CaptureAsync(
        string sourcePath,
        string destinationPath,
        DatabaseOptions? databaseOptions = null,
        RetainedDatabaseSnapshotOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        options ??= new RetainedDatabaseSnapshotOptions();
        options.Validate();
        databaseOptions ??= new DatabaseOptions();

        string fullSourcePath = RetainedDatabaseSnapshotFile.GetAbsolutePath(sourcePath);
        string fullDestinationPath = RetainedDatabaseSnapshotFile.GetAbsolutePath(destinationPath);
        RetainedDatabaseSnapshotFile.EnsurePairNamespacesDistinct(
            fullSourcePath,
            fullDestinationPath);
        RetainedDatabaseSnapshotFile.ValidatePublishDestination(fullDestinationPath);

        await using var workspace = RetainedDatabaseSnapshotWorkspace.Create(options.WorkspacePath);
        string privateDatabasePath = workspace.GetPath("capture.cdb");
        string privateWalPath = workspace.GetPath("capture.cdb.wal");
        bool copiedWal = false;

        await using (FileStream source = RetainedDatabaseSnapshotFile.OpenExistingRegularReadOnly(
                         fullSourcePath,
                         options.MaxDatabaseBytes))
        {
            FileStream? wal = null;
            try
            {
                string sourceWalPath = fullSourcePath + ".wal";
                try
                {
                    wal = RetainedDatabaseSnapshotFile.OpenExistingRegularReadOnly(
                        sourceWalPath,
                        options.MaxWalBytes);
                }
                catch (FileNotFoundException)
                {
                    // A genuinely absent optional sidecar is the only ignored
                    // WAL-open outcome. Access and unsafe-path failures abort.
                }

                await RetainedDatabaseSnapshotFile.CopyToNewFileAsync(
                    source,
                    privateDatabasePath,
                    options.MaxDatabaseBytes,
                    options.CopyBufferBytes,
                    ct);

                if (wal is not null)
                {
                    copiedWal = true;
                    await RetainedDatabaseSnapshotFile.CopyToNewFileAsync(
                        wal,
                        privateWalPath,
                        options.MaxWalBytes,
                        options.CopyBufferBytes,
                        ct);
                }
            }
            finally
            {
                if (wal is not null)
                    await wal.DisposeAsync();
            }
        }

        await RetainedDatabaseSnapshotFile.PreflightRecoveryExpansionAsync(
            privateDatabasePath,
            copiedWal ? privateWalPath : null,
            options.MaxSnapshotBytes,
            ct);

        await Database.RecoverPrivateSnapshotCopyAsync(
            privateDatabasePath,
            CreateBoundedDatabaseOptions(databaseOptions, options),
            ct);

        RetainedDatabaseSnapshotFileHash published = await RetainedDatabaseSnapshotFile.PublishNoOverwriteAsync(
            privateDatabasePath,
            fullDestinationPath,
            options.MaxSnapshotBytes,
            options.CopyBufferBytes,
            ct);

        var identity = CreateIdentity(published.ByteLength, published.Sha256);
        return new RetainedDatabaseSnapshotReceipt(
            fullDestinationPath,
            identity.ByteLength,
            identity.Sha256,
            identity.SnapshotIdentity);
    }

    public static async ValueTask<RetainedDatabaseSnapshotSession> OpenAsync(
        string snapshotPath,
        RetainedDatabaseSnapshotIdentity expectedIdentity,
        DatabaseOptions? databaseOptions = null,
        RetainedDatabaseSnapshotOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotPath);
        ArgumentNullException.ThrowIfNull(expectedIdentity);

        options ??= new RetainedDatabaseSnapshotOptions();
        options.Validate();
        ValidateExpectedIdentity(expectedIdentity);
        databaseOptions ??= new DatabaseOptions();

        string fullSnapshotPath = RetainedDatabaseSnapshotFile.GetAbsolutePath(snapshotPath);
        RetainedDatabaseSnapshotWorkspace? workspace =
            RetainedDatabaseSnapshotWorkspace.Create(options.WorkspacePath);
        try
        {
            string privateDatabasePath = workspace.GetPath("snapshot.cdb");
            _ = workspace.GetPath("snapshot.cdb.wal");
            RetainedDatabaseSnapshotFileHash copied;
            await using (FileStream source = RetainedDatabaseSnapshotFile.OpenExistingRegularReadOnly(
                             fullSnapshotPath,
                             options.MaxSnapshotBytes))
            {
                if (source.Length != expectedIdentity.ByteLength)
                {
                    throw new IOException(
                        $"Snapshot length mismatch. Expected {expectedIdentity.ByteLength}, found {source.Length}.");
                }

                copied = await RetainedDatabaseSnapshotFile.CopyToNewFileAndHashAsync(
                    source,
                    privateDatabasePath,
                    options.MaxSnapshotBytes,
                    options.CopyBufferBytes,
                    ct);
            }

            if (copied.ByteLength != expectedIdentity.ByteLength ||
                !RetainedDatabaseSnapshotFile.HashEquals(copied.Sha256, expectedIdentity.Sha256))
            {
                throw new IOException("Snapshot identity verification failed.");
            }

            Database database = await Database.OpenPrivateSnapshotCopyAsync(
                privateDatabasePath,
                CreateBoundedDatabaseOptions(databaseOptions, options),
                ct);
            try
            {
                Database.ReaderSession reader = database.CreateReaderSession();
                var session = new RetainedDatabaseSnapshotSession(
                    fullSnapshotPath,
                    expectedIdentity,
                    database,
                    reader,
                    workspace,
                    options.MaxEncodedRowBytes);
                workspace = null;
                return session;
            }
            catch
            {
                await database.DisposeAsync();
                throw;
            }
        }
        finally
        {
            if (workspace is not null)
                await workspace.DisposeAsync();
        }
    }

    private static RetainedDatabaseSnapshotIdentity CreateIdentity(long byteLength, string sha256)
    {
        string snapshotIdentity = IdentityPrefix +
            byteLength.ToString(CultureInfo.InvariantCulture) + ":" + sha256;
        return new RetainedDatabaseSnapshotIdentity(byteLength, sha256, snapshotIdentity);
    }

    private static void ValidateExpectedIdentity(RetainedDatabaseSnapshotIdentity identity)
    {
        if (identity.ByteLength <= 0)
            throw new ArgumentOutOfRangeException(nameof(identity), "Expected byte length must be positive.");
        if (!RetainedDatabaseSnapshotFile.IsCanonicalSha256(identity.Sha256))
            throw new ArgumentException("Expected SHA-256 must use canonical lowercase 'sha256:<64 hex>' form.", nameof(identity));

        RetainedDatabaseSnapshotIdentity canonical = CreateIdentity(identity.ByteLength, identity.Sha256);
        if (!string.Equals(identity.SnapshotIdentity, canonical.SnapshotIdentity, StringComparison.Ordinal))
            throw new ArgumentException("SnapshotIdentity is not canonical for the supplied length and SHA-256.", nameof(identity));
    }

    internal static DatabaseOptions CreateBoundedDatabaseOptions(
        DatabaseOptions source,
        RetainedDatabaseSnapshotOptions retained)
    {
        StorageEngineOptions storage = source.StorageEngineOptions;
        PagerOptions pager = storage.PagerOptions;
        var boundedPager = new PagerOptions
        {
            WriterLockTimeout = pager.WriterLockTimeout,
            CheckpointPolicy = pager.CheckpointPolicy,
            AutoCheckpointExecutionMode = pager.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = pager.AutoCheckpointMaxPagesPerStep,
            MaxCachedPages = pager.MaxCachedPages is > 0
                ? Math.Min(pager.MaxCachedPages.Value, retained.MaxCachedPages)
                : retained.MaxCachedPages,
            MaxCachedWalReadPages = Math.Min(pager.MaxCachedWalReadPages, retained.MaxCachedWalReadPages),
            PageCacheFactory = null,
            Interceptors = pager.Interceptors,
            MaxWalBytesWhenReadersActive = pager.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = pager.OnCachePageEvicted,
            UseMemoryMappedReads = false,
            EnableSequentialLeafReadAhead = pager.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = pager.PreserveOwnedPagesOnCheckpoint,
        };
        var boundedStorage = new StorageEngineOptions
        {
            DurabilityMode = storage.DurabilityMode,
            PrimaryFileShare = FileShare.Read,
            DurableGroupCommit = storage.DurableGroupCommit,
            AdvisoryStatisticsPersistenceMode = storage.AdvisoryStatisticsPersistenceMode,
            WalPreallocationChunkBytes = 0,
            PagerOptions = boundedPager,
            SerializerProvider = storage.SerializerProvider,
            IndexProvider = storage.IndexProvider,
            CatalogStore = storage.CatalogStore,
            ChecksumProvider = storage.ChecksumProvider,
        };
        return new DatabaseOptions
        {
            StorageEngineOptions = boundedStorage,
            ImplicitInsertExecutionMode = source.ImplicitInsertExecutionMode,
            AdaptiveQueryReoptimization = source.AdaptiveQueryReoptimization,
            Functions = source.Functions,
            // Private retained-snapshot copies always use the engine's bounded
            // file-backed composition root. A caller-supplied factory could
            // otherwise redirect the open or replace the bounded pager.
            StorageEngineFactory = new DefaultStorageEngineFactory(),
        };
    }
}

/// <summary>
/// Narrow, read-only session over a verified private copy of a retained snapshot.
/// </summary>
public sealed class RetainedDatabaseSnapshotSession : IAsyncDisposable
{
    private readonly Database _database;
    private readonly Database.ReaderSession _reader;
    private readonly object _lifecycleGate = new();
    private readonly int _maxEncodedRowBytes;
    private RetainedDatabaseSnapshotTableReader? _activeTableReader;
    private RetainedDatabaseSnapshotWorkspace? _workspace;
    private int _disposed;

    internal RetainedDatabaseSnapshotSession(
        string snapshotPath,
        RetainedDatabaseSnapshotIdentity identity,
        Database database,
        Database.ReaderSession reader,
        RetainedDatabaseSnapshotWorkspace workspace,
        int maxEncodedRowBytes)
    {
        SnapshotPath = snapshotPath;
        Identity = identity;
        _database = database;
        _reader = reader;
        _workspace = workspace;
        _maxEncodedRowBytes = maxEncodedRowBytes;
    }

    public string SnapshotPath { get; }

    public RetainedDatabaseSnapshotIdentity Identity { get; }

    public IReadOnlyCollection<string> GetTableNames()
    {
        ThrowIfDisposed();
        return _database.GetTableNames().ToArray();
    }

    public TableSchema? GetTableSchema(string tableName)
    {
        ThrowIfDisposed();
        TableSchema? schema = _database.GetTableSchema(tableName);
        return schema is null ? null : CopyTableSchema(schema);
    }

    public IReadOnlyCollection<IndexSchema> GetIndexes()
    {
        ThrowIfDisposed();
        return _database.GetIndexes().Select(CopyIndexSchema).ToArray();
    }

    public IReadOnlyCollection<string> GetViewNames()
    {
        ThrowIfDisposed();
        return _database.GetViewNames().ToArray();
    }

    public IReadOnlyCollection<TriggerSchema> GetTriggers()
    {
        ThrowIfDisposed();
        return _database.GetTriggers()
            .Select(static trigger => new TriggerSchema
            {
                TriggerName = trigger.TriggerName,
                TableName = trigger.TableName,
                Timing = trigger.Timing,
                Event = trigger.Event,
                BodySql = trigger.BodySql,
            })
            .ToArray();
    }

    public ValueTask<QueryResult> ExecuteReadAsync(string sql, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return _reader.ExecuteReadAsync(sql, ct);
    }

    /// <summary>
    /// Opens a forward-only physical table reader in ascending row-ID order.
    /// When supplied, <paramref name="afterRowIdExclusive"/> resumes at the
    /// first row whose physical row ID is greater than that boundary.
    /// </summary>
    /// <remarks>
    /// Views, system/internal tables, and external tables are not supported.
    /// The reader must be disposed before another read is started through this
    /// session.
    /// </remarks>
    public RetainedDatabaseSnapshotTableReader OpenTableReader(
        string tableName,
        long? afterRowIdExclusive = null)
    {
        lock (_lifecycleGate)
        {
            ThrowIfDisposed();
            RetainedDatabaseSnapshotTableReader reader =
                _reader.OpenTableReader(
                    tableName,
                    afterRowIdExclusive,
                    _maxEncodedRowBytes);
            _activeTableReader = reader;
            return reader;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        RetainedDatabaseSnapshotTableReader? activeTableReader;
        lock (_lifecycleGate)
            activeTableReader = Interlocked.Exchange(ref _activeTableReader, null);

        try
        {
            if (activeTableReader is not null)
                await activeTableReader.DisposeAsync();
        }
        finally
        {
            try
            {
                _reader.Dispose();
            }
            finally
            {
                try
                {
                    await _database.DisposeAsync();
                }
                finally
                {
                    RetainedDatabaseSnapshotWorkspace? workspace =
                        Interlocked.Exchange(ref _workspace, null);
                    if (workspace is not null)
                        await workspace.DisposeAsync();
                }
            }
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    private static TableSchema CopyTableSchema(TableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(static column => new ColumnDefinition
        {
            Name = column.Name,
            Type = column.Type,
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
        }).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(static foreignKey => new ForeignKeyDefinition
        {
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            ColumnNames = foreignKey.ColumnNames.ToArray(),
            ReferencedColumnNames = foreignKey.ReferencedColumnNames.ToArray(),
            OnDelete = foreignKey.OnDelete,
            SupportingIndexName = foreignKey.SupportingIndexName,
        }).ToArray(),
        CheckConstraints = schema.CheckConstraints.Select(static check => new CheckConstraintDefinition
        {
            ConstraintName = check.ConstraintName,
            ExpressionSql = check.ExpressionSql,
            ColumnName = check.ColumnName,
        }).ToArray(),
        KeyConstraints = schema.KeyConstraints.Select(static key => new KeyConstraintDefinition
        {
            ConstraintName = key.ConstraintName,
            Kind = key.Kind,
            Columns = key.Columns.ToArray(),
            BackingIndexName = key.BackingIndexName,
        }).ToArray(),
        NextRowId = schema.NextRowId,
        QualifiedMappings = schema.QualifiedMappings is null
            ? null
            : new Dictionary<string, int>(
                schema.QualifiedMappings,
                schema.QualifiedMappings.Comparer),
    };

    private static IndexSchema CopyIndexSchema(IndexSchema index) => new()
    {
        IndexName = index.IndexName,
        TableName = index.TableName,
        Columns = index.Columns.ToArray(),
        ColumnCollations = index.ColumnCollations.ToArray(),
        IsUnique = index.IsUnique,
        Kind = index.Kind,
        State = index.State,
        OwnerIndexName = index.OwnerIndexName,
        OptionsJson = index.OptionsJson,
    };
}
