using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Collections.Concurrent;
using CSharpDB.Primitives;
using CSharpDB.Execution;
using CSharpDB.Sql;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Transactions;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Engine;

/// <summary>
/// Top-level entry point for the CSharpDB embedded database engine.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private const int DefaultStatementCacheCapacity = 512;
    private const int DefaultImplicitConflictRetries = 10;
    private static readonly WriteTransactionOptions ImplicitAutoCommitWriteTransactionOptions = new()
    {
        MaxRetries = DefaultImplicitConflictRetries,
        InitialBackoff = TimeSpan.FromMilliseconds(0.25),
        MaxBackoff = TimeSpan.FromMilliseconds(20),
    };

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly QueryPlanner _planner;
    private readonly IRecordSerializer _recordSerializer;
    private readonly ISchemaSerializer _schemaSerializer;
    private readonly IIndexProvider _indexProvider;
    private readonly ICatalogStore _catalogStore;
    private readonly AdvisoryStatisticsPersistenceMode _advisoryStatisticsPersistenceMode;
    private readonly StatementCache _statementCache;
    private readonly HybridDatabasePersistenceCoordinator? _hybridPersistenceCoordinator;
    private readonly Dictionary<string, object> _collectionCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PendingCollectionCatalogMutation> _pendingCollectionCatalogMutations = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _sharedNextRowIdHints = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _sharedNextRowIdGate = new();
    private readonly SemaphoreSlim _writeOperationGate = new(1, 1);
    private readonly SemaphoreSlim _sharedStateGate = new(1, 1);
    private long _observedSchemaVersion;
    private ImplicitInsertExecutionMode _implicitInsertExecutionMode;
    private bool _inTransaction;

    /// <summary>
    /// When true, simple PK equality lookups (SELECT * WHERE pk = N) use a synchronous
    /// cache-only fast path, bypassing the async operator pipeline. Defaults to true.
    /// </summary>
    public bool PreferSyncPointLookups
    {
        get => _planner.PreferSyncPointLookups;
        set => _planner.PreferSyncPointLookups = value;
    }

    /// <summary>
    /// Controls how shared auto-commit INSERT statements execute on this database handle.
    /// </summary>
    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode
    {
        get => _implicitInsertExecutionMode;
        set => _implicitInsertExecutionMode = value;
    }

    public int ActiveReaderCount => _pager.ActiveReaderCount;

    internal WalFlushDiagnosticsSnapshot GetWalFlushDiagnosticsSnapshot() =>
        _pager.GetWalFlushDiagnosticsSnapshot();

    internal void ResetWalFlushDiagnostics() =>
        _pager.ResetWalFlushDiagnostics();

    internal CommitPathDiagnosticsSnapshot GetCommitPathDiagnosticsSnapshot() =>
        _pager.GetCommitPathDiagnosticsSnapshot();

    internal void ResetCommitPathDiagnostics() =>
        _pager.ResetCommitPathDiagnostics();

    private Database(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer recordSerializer,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode,
        ImplicitInsertExecutionMode implicitInsertExecutionMode = ImplicitInsertExecutionMode.Serialized,
        HybridDatabasePersistenceCoordinator? hybridPersistenceCoordinator = null)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer;
        _schemaSerializer = schemaSerializer;
        _indexProvider = indexProvider;
        _catalogStore = catalogStore;
        _advisoryStatisticsPersistenceMode = advisoryStatisticsPersistenceMode;
        _implicitInsertExecutionMode = implicitInsertExecutionMode;
        _hybridPersistenceCoordinator = hybridPersistenceCoordinator;
        _planner = new QueryPlanner(
            pager,
            catalog,
            _recordSerializer,
            nextRowIdHintProvider: TryGetSharedNextRowIdHint,
            nextRowIdReservationProvider: ReserveSharedNextRowId,
            nextRowIdObservationProvider: ObserveSharedNextRowId);
        _statementCache = new StatementCache(DefaultStatementCacheCapacity);
        _observedSchemaVersion = catalog.SchemaVersion;
        RefreshSharedNextRowIdHintsFromCatalog();
    }

    /// <summary>
    /// Begin an explicit multi-writer transaction with its own isolated catalog and planner context.
    /// </summary>
    public async ValueTask<WriteTransaction> BeginWriteTransactionAsync(CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new InvalidOperationException("Cannot start a multi-writer transaction while a legacy explicit transaction is active.");

        PagerWriteTransaction storageTransaction = await _pager.BeginWriteTransactionAsync(ct);
        try
        {
            using var binding = storageTransaction.Bind();
            var transactionCatalog = await SchemaCatalog.CreateAsync(
                _pager,
                _schemaSerializer,
                _indexProvider,
                _catalogStore,
                _advisoryStatisticsPersistenceMode,
                ct);
            var transactionPlanner = new QueryPlanner(
                _pager,
                transactionCatalog,
                _recordSerializer,
                nextRowIdHintProvider: TryGetSharedNextRowIdHint,
                nextRowIdReservationProvider: ReserveSharedNextRowId,
                nextRowIdObservationProvider: ObserveSharedNextRowId,
                useTransientNextRowIdHints: true)
            {
                PreferSyncPointLookups = PreferSyncPointLookups,
            };

            return new WriteTransaction(
                this,
                storageTransaction,
                transactionCatalog,
                transactionPlanner,
                transactionCatalog.SchemaVersion);
        }
        catch
        {
            await storageTransaction.DisposeAsync();
            throw;
        }
    }

    private long? TryGetSharedNextRowIdHint(string tableName)
    {
        return _sharedNextRowIdHints.TryGetValue(tableName, out long nextRowId) && nextRowId > 0
            ? nextRowId
            : null;
    }

    private void RefreshSharedNextRowIdHintsFromCatalog()
    {
        lock (_sharedNextRowIdGate)
        {
            var preservedHints = _sharedNextRowIdHints.Count == 0
                ? null
                : _sharedNextRowIdHints.ToArray();

            _sharedNextRowIdHints.Clear();

            foreach (string tableName in _catalog.GetTableNames())
            {
                long nextRowId = _catalog.GetTable(tableName)?.NextRowId ?? 0;
                if (nextRowId > 0)
                    _sharedNextRowIdHints[tableName] = nextRowId;
            }

            if (preservedHints is null)
                return;

            foreach ((string tableName, long nextRowId) in preservedHints)
            {
                if (nextRowId <= 0 || _catalog.GetTable(tableName) is null)
                    continue;

                _sharedNextRowIdHints.AddOrUpdate(
                    tableName,
                    nextRowId,
                    (_, existing) => Math.Max(existing, nextRowId));
            }
        }
    }

    private void ApplyCommittedNextRowIdHints(IReadOnlyCollection<KeyValuePair<string, long>> committedNextRowIds)
    {
        lock (_sharedNextRowIdGate)
        {
            foreach ((string tableName, long nextRowId) in committedNextRowIds)
            {
                if (nextRowId <= 0)
                    continue;

                _sharedNextRowIdHints.AddOrUpdate(
                    tableName,
                    nextRowId,
                    (_, existing) => Math.Max(existing, nextRowId));
            }
        }
    }

    private long ReserveSharedNextRowId(string tableName, long minimumNextRowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        long normalizedMinimum = minimumNextRowId > 0 ? minimumNextRowId : 1;
        lock (_sharedNextRowIdGate)
        {
            long currentNextRowId = _sharedNextRowIdHints.TryGetValue(tableName, out long existing)
                ? Math.Max(existing, normalizedMinimum)
                : normalizedMinimum;

            _sharedNextRowIdHints[tableName] = checked(currentNextRowId + 1);
            return currentNextRowId;
        }
    }

    private void ObserveSharedNextRowId(string tableName, long nextRowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        if (nextRowId <= 0)
            return;

        lock (_sharedNextRowIdGate)
        {
            _sharedNextRowIdHints.AddOrUpdate(
                tableName,
                nextRowId,
                (_, existing) => Math.Max(existing, nextRowId));
        }
    }

    /// <summary>
    /// Run a multi-writer transaction with automatic retry on transaction conflicts.
    /// </summary>
    public async ValueTask RunWriteTransactionAsync(
        Func<WriteTransaction, CancellationToken, ValueTask> action,
        WriteTransactionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        options ??= new WriteTransactionOptions();

        for (int attempt = 0; ; attempt++)
        {
            await using WriteTransaction transaction = await BeginWriteTransactionAsync(ct);
            try
            {
                await action(transaction, ct);
                await transaction.CommitAsync(ct);
                return;
            }
            catch (CSharpDbConflictException) when (attempt < options.MaxRetries)
            {
                await options.DelayBeforeRetryAsync(attempt, ct);
            }
        }
    }

    /// <summary>
    /// Run a multi-writer transaction with automatic retry on transaction conflicts.
    /// </summary>
    public async ValueTask<TResult> RunWriteTransactionAsync<TResult>(
        Func<WriteTransaction, CancellationToken, ValueTask<TResult>> action,
        WriteTransactionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        options ??= new WriteTransactionOptions();

        for (int attempt = 0; ; attempt++)
        {
            await using WriteTransaction transaction = await BeginWriteTransactionAsync(ct);
            try
            {
                TResult result = await action(transaction, ct);
                await transaction.CommitAsync(ct);
                return result;
            }
            catch (CSharpDbConflictException) when (attempt < options.MaxRetries)
            {
                await options.DelayBeforeRetryAsync(attempt, ct);
            }
        }
    }

    internal async ValueTask OnExternalWriteTransactionCommittedAsync(
        bool reloadSharedCatalog,
        bool schemaChanged,
        IReadOnlyCollection<KeyValuePair<string, long>> committedNextRowIds,
        IReadOnlyCollection<KeyValuePair<string, long>> committedTableRowCountDeltas,
        IReadOnlyCollection<TableStatistics> committedTableStatistics,
        IReadOnlyCollection<ColumnStatistics> committedColumnStatistics,
        CancellationToken ct)
    {
        bool applyAdvisoryStats = committedTableStatistics.Count > 0 || committedColumnStatistics.Count > 0;
        bool applyTableMetadata = committedNextRowIds.Count > 0;
        bool applyTableRowCountDeltas = committedTableRowCountDeltas.Count > 0;

        if (reloadSharedCatalog || applyTableMetadata || applyAdvisoryStats || applyTableRowCountDeltas)
        {
            await _sharedStateGate.WaitAsync(ct);
            try
            {
                TableStatistics[] preservedDirtyTableStatistics = [];
                ColumnStatistics[] preservedDirtyColumnStatistics = [];
                KeyValuePair<string, long>[] preservedTableRowCountDeltas = [];

                if (reloadSharedCatalog)
                {
                    preservedDirtyTableStatistics = _catalog.GetDirtyTableStatistics().ToArray();
                    preservedDirtyColumnStatistics = _catalog.GetDirtyColumnStatistics().ToArray();
                    preservedTableRowCountDeltas = _catalog.GetPendingTableRowCountDeltas().ToArray();

                    await _catalog.ReloadAsync(ct);
                    _collectionCache.Clear();
                    if (schemaChanged)
                        _statementCache.Clear();

                    _observedSchemaVersion = _catalog.SchemaVersion;
                    RefreshSharedNextRowIdHintsFromCatalog();

                    if (preservedDirtyTableStatistics.Length > 0 || preservedDirtyColumnStatistics.Length > 0)
                    {
                        _catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
                            preservedDirtyTableStatistics,
                            preservedDirtyColumnStatistics,
                            markDirty: true);
                    }

                    if (preservedTableRowCountDeltas.Length > 0)
                        _catalog.ApplyCommittedTableRowCountDeltas(preservedTableRowCountDeltas);
                }

                if (applyTableMetadata)
                {
                    _catalog.ApplyCommittedTableMetadataSnapshot(committedNextRowIds);
                    ApplyCommittedNextRowIdHints(committedNextRowIds);
                }

                if (applyAdvisoryStats)
                {
                    IReadOnlyCollection<TableStatistics> mergedTableStatistics =
                        MergeCommittedTableStatistics(committedTableStatistics, committedTableRowCountDeltas);
                    _catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
                        mergedTableStatistics,
                        committedColumnStatistics,
                        markDirty: true);
                }

                if (applyTableRowCountDeltas)
                    _catalog.ApplyCommittedTableRowCountDeltas(committedTableRowCountDeltas);
            }
            finally
            {
                _sharedStateGate.Release();
            }
        }

        await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
    }

    private IReadOnlyCollection<TableStatistics> MergeCommittedTableStatistics(
        IReadOnlyCollection<TableStatistics> committedTableStatistics,
        IReadOnlyCollection<KeyValuePair<string, long>> committedTableRowCountDeltas)
    {
        if (committedTableStatistics.Count == 0 || committedTableRowCountDeltas.Count == 0)
            return committedTableStatistics;

        var deltasByTable = committedTableRowCountDeltas.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.OrdinalIgnoreCase);
        var merged = new List<TableStatistics>(committedTableStatistics.Count);

        foreach (TableStatistics stats in committedTableStatistics)
        {
            if (!deltasByTable.TryGetValue(stats.TableName, out long rowCountDelta))
            {
                merged.Add(stats);
                continue;
            }

            TableStatistics? existing = _catalog.GetTableStatistics(stats.TableName);
            if (existing is null || !existing.RowCountIsExact)
            {
                merged.Add(
                    new TableStatistics
                    {
                        TableName = stats.TableName,
                        RowCount = stats.RowCount,
                        RowCountIsExact = stats.RowCountIsExact,
                        HasStaleColumns = stats.HasStaleColumns || (existing?.HasStaleColumns ?? false),
                        LastPersistedChangeCounter = existing?.LastPersistedChangeCounter ?? stats.LastPersistedChangeCounter,
                    });
                continue;
            }

            long baseRowCount = existing?.RowCount ?? 0;
            merged.Add(
                new TableStatistics
                {
                    TableName = stats.TableName,
                    RowCount = checked(baseRowCount + rowCountDelta),
                    RowCountIsExact = stats.RowCountIsExact && (existing?.RowCountIsExact ?? true),
                    HasStaleColumns = stats.HasStaleColumns || (existing?.HasStaleColumns ?? false),
                    LastPersistedChangeCounter = existing?.LastPersistedChangeCounter ?? stats.LastPersistedChangeCounter,
                });
        }

        return merged;
    }

    /// <summary>
    /// Open an existing database file, or create a new one if it doesn't exist.
    /// </summary>
    public static async ValueTask<Database> OpenAsync(string filePath, CancellationToken ct = default)
    {
        return await OpenAsync(filePath, new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a new in-memory database using default composition options.
    /// </summary>
    public static async ValueTask<Database> OpenInMemoryAsync(CancellationToken ct = default)
    {
        return await OpenInMemoryAsync(new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a new in-memory database using explicit composition options.
    /// </summary>
    public static async ValueTask<Database> OpenInMemoryAsync(
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var context = await InMemoryStorageEngineFactory.OpenAsync(options.StorageEngineOptions, ct: ct);
        return new Database(
            context.Pager,
            context.Catalog,
            context.RecordSerializer,
            context.SchemaSerializer,
            context.IndexProvider,
            context.CatalogStore,
            context.AdvisoryStatisticsPersistenceMode,
            options.ImplicitInsertExecutionMode);
    }

    /// <summary>
    /// Open a lazy-resident hybrid database that persists committed state to the specified backing file.
    /// Existing file and WAL contents are read on demand while touched pages remain resident according
    /// to the pager cache policy; snapshot mode preserves the older full-image in-memory export behavior.
    /// </summary>
    public static async ValueTask<Database> OpenHybridAsync(
        string filePath,
        CancellationToken ct = default)
    {
        return await OpenHybridAsync(filePath, new DatabaseOptions(), new HybridDatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a lazy-resident hybrid database that persists committed state to the specified backing file
    /// using explicit storage composition and persistence behavior.
    /// </summary>
    public static async ValueTask<Database> OpenHybridAsync(
        string filePath,
        DatabaseOptions options,
        HybridDatabaseOptions hybridOptions,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(hybridOptions);
        ValidateHybridHotSetOptions(options, hybridOptions);

        string fullPath = Path.GetFullPath(filePath);
        if (hybridOptions.PersistenceMode == HybridPersistenceMode.Snapshot)
        {
            StorageEngineContext snapshotContext;

            if (File.Exists(fullPath))
            {
                byte[] databaseBytes = await File.ReadAllBytesAsync(fullPath, ct);
                string walPath = fullPath + ".wal";
                byte[] walBytes = File.Exists(walPath)
                    ? await File.ReadAllBytesAsync(walPath, ct)
                    : Array.Empty<byte>();

                snapshotContext = await InMemoryStorageEngineFactory.OpenAsync(
                    options.StorageEngineOptions,
                    databaseBytes,
                    walBytes,
                    ct);
            }
            else
            {
                snapshotContext = await InMemoryStorageEngineFactory.OpenAsync(options.StorageEngineOptions, ct: ct);
            }

            var snapshotDatabase = new Database(
                snapshotContext.Pager,
                snapshotContext.Catalog,
                snapshotContext.RecordSerializer,
                snapshotContext.SchemaSerializer,
                snapshotContext.IndexProvider,
                snapshotContext.CatalogStore,
                snapshotContext.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                new HybridDatabasePersistenceCoordinator(fullPath, hybridOptions.PersistenceTriggers));
            return snapshotDatabase;
        }

        var context = await HybridStorageEngineFactory.OpenAsync(fullPath, options.StorageEngineOptions, ct);
        var database = new Database(
            context.Pager,
            context.Catalog,
            context.RecordSerializer,
            context.SchemaSerializer,
            context.IndexProvider,
            context.CatalogStore,
            context.AdvisoryStatisticsPersistenceMode,
            options.ImplicitInsertExecutionMode);
        try
        {
            await database.WarmHybridHotSetAsync(hybridOptions, ct);
            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Load an on-disk database into memory using default composition options.
    /// If a companion WAL file exists, committed WAL frames are recovered into the in-memory copy.
    /// </summary>
    public static async ValueTask<Database> LoadIntoMemoryAsync(string filePath, CancellationToken ct = default)
    {
        return await LoadIntoMemoryAsync(filePath, new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Load an on-disk database into memory using explicit composition options.
    /// If a companion WAL file exists, committed WAL frames are recovered into the in-memory copy.
    /// </summary>
    public static async ValueTask<Database> LoadIntoMemoryAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);

        string fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException("Database file not found.", fullPath);

        byte[] databaseBytes = await File.ReadAllBytesAsync(fullPath, ct);
        string walPath = fullPath + ".wal";
        byte[] walBytes = File.Exists(walPath)
            ? await File.ReadAllBytesAsync(walPath, ct)
            : Array.Empty<byte>();

        var context = await InMemoryStorageEngineFactory.OpenAsync(
            options.StorageEngineOptions,
            databaseBytes,
            walBytes,
            ct);

        return new Database(
            context.Pager,
            context.Catalog,
            context.RecordSerializer,
            context.SchemaSerializer,
            context.IndexProvider,
            context.CatalogStore,
            context.AdvisoryStatisticsPersistenceMode,
            options.ImplicitInsertExecutionMode);
    }

    /// <summary>
    /// Open an existing database file, or create a new one if it doesn't exist, using explicit composition options.
    /// </summary>
    public static async ValueTask<Database> OpenAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var context = await options.StorageEngineFactory.OpenAsync(filePath, options.StorageEngineOptions, ct);
        return new Database(
            context.Pager,
            context.Catalog,
            context.RecordSerializer,
            context.SchemaSerializer,
            context.IndexProvider,
            context.CatalogStore,
            context.AdvisoryStatisticsPersistenceMode,
            options.ImplicitInsertExecutionMode);
    }

    /// <summary>
    /// Execute a SQL statement. Returns a QueryResult with rows (for SELECT) or affected count (for DML/DDL).
    /// </summary>
    public async ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken ct = default)
    {
        InvalidateCachesIfSchemaChanged();
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);

        if (LooksLikeInsert(sql) && Parser.TryParseSimpleInsert(sql, out var simpleInsert))
            return await ExecuteSimpleInsertAsync(simpleInsert, ct);

        if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var simpleLookup))
            return await ExecuteSimplePrimaryKeyLookupAsync(simpleLookup, ct);

        if (_statementCache.TryGetOrMarkBypass(sql, out var cachedStmt, out _))
            return await ExecuteStatementAsync(cachedStmt, ct);

        var stmt = ParseCached(sql);
        return await ExecuteStatementAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteSimplePrimaryKeyLookupAsync(
        SimplePrimaryKeyLookupSql lookup,
        CancellationToken ct)
    {
        var directResult = await _planner.TryExecuteSimplePrimaryKeyLookupDirectAsync(lookup, ct);
        if (directResult != null)
            return directResult;

        if (_planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult))
            return fastResult;

        var statement = Parser.Parse(SelectToSql(lookup));
        return await ExecuteStatementAsync(statement, ct);
    }

    private static string SelectToSql(SimplePrimaryKeyLookupSql lookup)
    {
        var projection = lookup.SelectStar
            ? "*"
            : string.Join(", ", lookup.ProjectionColumns);

        var predicate = $"{lookup.PredicateColumn} = {LiteralToSql(lookup.PredicateLiteral.Type == DbType.Null ? DbValue.FromInteger(lookup.LookupValue) : lookup.PredicateLiteral)}";
        if (lookup.HasResidualPredicate)
            predicate += $" AND {lookup.ResidualPredicateColumn} = {LiteralToSql(lookup.ResidualPredicateLiteral)}";

        return $"SELECT {projection} FROM {lookup.TableName} WHERE {predicate}";
    }

    private static string LiteralToSql(DbValue value)
    {
        return value.Type switch
        {
            DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
            DbType.Real => value.AsReal.ToString(CultureInfo.InvariantCulture),
            DbType.Text => $"'{value.AsText.Replace("'", "''", StringComparison.Ordinal)}'",
            _ => "NULL",
        };
    }

    /// <summary>
    /// Execute a pre-parsed SQL statement. Used by prepared command paths
    /// to bypass SQL text parsing on repeated executions.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
    {
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);
        return await ExecuteStatementAsync(statement, ct);
    }

    internal async ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken ct = default)
    {
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);
        return await ExecuteSimpleInsertAsync(insert, ct);
    }

    /// <summary>
    /// Prepare a reusable full-row insert batch for a single table.
    /// The batch accepts DbValue rows and executes them through the simple insert path.
    /// </summary>
    public InsertBatch PrepareInsertBatch(string tableName, int initialCapacity = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentOutOfRangeException.ThrowIfNegative(initialCapacity);

        InvalidateCachesIfSchemaChanged();
        var schema = _catalog.GetTable(tableName);
        if (schema == null)
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        return new InsertBatch(this, tableName, schema.Columns.Count, _catalog.SchemaVersion, initialCapacity);
    }

    private ValueTask<QueryResult> ExecuteStatementAsync(Statement stmt, CancellationToken ct)
    {
        if (stmt is QueryStatement or WithStatement)
            return _planner.ExecuteAsync(stmt, ct);

        return ExecuteWriteStatementAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteWriteStatementAsync(Statement stmt, CancellationToken ct)
    {
        if (_inTransaction)
        {
            if (stmt is InsertStatement explicitInsert)
            {
                return await _planner.ExecuteInsertAsync(
                    explicitInsert,
                    persistRootChanges: false,
                    ct);
            }

            return await _planner.ExecuteAsync(stmt, ct);
        }

        if (stmt is InsertStatement insert)
        {
            if (ImplicitInsertExecutionMode == ImplicitInsertExecutionMode.ConcurrentWriteTransactions)
                return await ExecuteConcurrentImplicitInsertAsync(insert, ct);

            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    return await ExecuteImplicitInsertCoreAsync(insert, ct);
                }
                catch (CSharpDbConflictException) when (attempt < DefaultImplicitConflictRetries)
                {
                    await DelayImplicitConflictRetryAsync(attempt, ct);
                }
            }
        }

        return await ExecuteImplicitWriteStatementCoreAsync(stmt, ct);
    }

    private ValueTask<QueryResult> ExecuteImplicitWriteStatementCoreAsync(Statement stmt, CancellationToken ct) =>
        RunWriteTransactionAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(stmt, token),
            ImplicitAutoCommitWriteTransactionOptions,
            ct);

    private ValueTask<QueryResult> ExecuteConcurrentImplicitInsertAsync(InsertStatement insert, CancellationToken ct) =>
        RunWriteTransactionAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(insert, token),
            ImplicitAutoCommitWriteTransactionOptions,
            ct);

    private async ValueTask<QueryResult> ExecuteImplicitInsertCoreAsync(InsertStatement insert, CancellationToken ct)
    {
        QueryResult result;
        PagerCommitResult commit = PagerCommitResult.Completed;
        IDisposable? writeScope = null;
        try
        {
            writeScope = await AcquireWriteOperationScopeAsync(ct);
            await _pager.BeginTransactionAsync(ct);
            result = await _planner.ExecuteInsertAsync(
                insert,
                persistRootChanges: false,
                ct);
            commit = await BeginCommitForTableWithCatalogSyncAsync(insert.TableName, ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
        finally
        {
            if (writeScope is not null)
            {
                try
                {
                    await CompleteImplicitCommitAsync(commit, ct);
                }
                finally
                {
                    writeScope.Dispose();
                }
            }
        }

        return result;
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct)
    {
        if (_inTransaction)
        {
            return await _planner.ExecuteSimpleInsertAsync(
                insert,
                persistRootChanges: false,
                ct);
        }

        if (ImplicitInsertExecutionMode == ImplicitInsertExecutionMode.ConcurrentWriteTransactions)
            return await ExecuteConcurrentImplicitSimpleInsertAsync(insert, ct);

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await ExecuteImplicitSimpleInsertCoreAsync(insert, ct);
            }
            catch (CSharpDbConflictException) when (attempt < DefaultImplicitConflictRetries)
            {
                await DelayImplicitConflictRetryAsync(attempt, ct);
            }
        }
    }

    private async ValueTask<QueryResult> ExecuteImplicitSimpleInsertCoreAsync(SimpleInsertSql insert, CancellationToken ct)
    {
        QueryResult result;
        PagerCommitResult commit = PagerCommitResult.Completed;
        IDisposable? writeScope = null;
        try
        {
            writeScope = await AcquireWriteOperationScopeAsync(ct);
            await _pager.BeginTransactionAsync(ct);

            result = await _planner.ExecuteSimpleInsertAsync(
                insert,
                persistRootChanges: false,
                ct);

            commit = await BeginCommitForTableWithCatalogSyncAsync(insert.TableName, ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
        finally
        {
            if (writeScope is not null)
            {
                try
                {
                    await CompleteImplicitCommitAsync(commit, ct);
                }
                finally
                {
                    writeScope.Dispose();
                }
            }
        }

        return result;
    }

    private ValueTask<QueryResult> ExecuteConcurrentImplicitSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct) =>
        RunWriteTransactionAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(insert, token),
            ImplicitAutoCommitWriteTransactionOptions,
            ct);

    /// <summary>
    /// Begin an explicit transaction.
    /// </summary>
    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Transaction already active.");

        await FlushPendingAdvisoryStatisticsAsync(ct);
        await _writeOperationGate.WaitAsync(ct);
        try
        {
            await _pager.BeginTransactionAsync(ct);
        }
        catch
        {
            _writeOperationGate.Release();
            throw;
        }

        _inTransaction = true;
    }

    /// <summary>
    /// Commit the current transaction.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");

        PagerCommitResult commit;
        try
        {
            await FlushPendingCollectionCatalogMutationsAsync(ct);
            commit = await BeginCommitWithCatalogSyncAsync(ct);
        }
        catch
        {
            ClearPendingCollectionCatalogMutations();
            await RecoverCatalogStateAfterFailedCommitAsync();
            _inTransaction = false;
            ReleaseExplicitTransactionWriteGate();
            throw;
        }

        _inTransaction = false;
        ReleaseExplicitTransactionWriteGate();
        await WaitForCommitOrRecoverAsync(commit);
        await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
    }

    /// <summary>
    /// Rollback the current transaction.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");
        await _pager.RollbackAsync(ct);
        try
        {
            await _catalog.ReloadAsync(ct);
            foreach (var cached in _collectionCache.Values)
            {
                if (cached is ICollectionTreeRefresh refreshable)
                    refreshable.RefreshTreeFromCatalog();
            }

            _statementCache.Clear();
        }
        finally
        {
            ClearPendingCollectionCatalogMutations();
            _inTransaction = false;
            ReleaseExplicitTransactionWriteGate();
        }
    }

    /// <summary>
    /// Manually trigger a WAL checkpoint.
    /// </summary>
    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        await _pager.CheckpointAsync(ct);
        await PersistHybridStateAsync(HybridPersistenceTriggers.Checkpoint, ct);
    }

    /// <summary>
    /// Save the current committed database state to an on-disk database file.
    /// </summary>
    public async ValueTask SaveToFileAsync(string filePath, CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new InvalidOperationException("Cannot save while an explicit transaction is active.");

        await SaveToFileAsync(filePath, writeScopeHeld: false, ct);
    }

    internal async ValueTask SaveToFileAsync(
        string filePath,
        bool writeScopeHeld,
        CancellationToken ct = default)
    {
        IDisposable? writeScope = null;
        try
        {
            if (!writeScopeHeld)
                writeScope = await AcquireWriteOperationScopeAsync(ct);

            await FlushPendingAdvisoryStatisticsAsync(ct, writeScopeHeld: true);
            await _pager.SaveToFileAsync(filePath, ct);
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    /// <summary>
    /// Create an independent reader that sees a snapshot of the database
    /// at the current point in time. The reader does not block writers.
    /// Caller must dispose the returned ReaderSession when done.
    /// </summary>
    public ReaderSession CreateReaderSession()
    {
        var snapshot = _pager.AcquireReaderSnapshot();
        var snapshotRowCounts = CaptureSnapshotRowCounts();
        return new ReaderSession(
            _pager,
            _catalog,
            _recordSerializer,
            snapshot,
            _statementCache,
            snapshotRowCounts);
    }

    private Dictionary<string, long> CaptureSnapshotRowCounts()
    {
        var snapshotRowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (string tableName in _catalog.GetTableNames())
        {
            if (_catalog.TryGetExactTableRowCount(tableName, out long rowCount))
                snapshotRowCounts[tableName] = rowCount;
        }

        return snapshotRowCounts;
    }

    /// <summary>
    /// Returns the names of all tables in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetTableNames() => _catalog.GetTableNames();

    internal uint GetTableRootPage(string tableName) => _catalog.GetTableRootPage(tableName);

    /// <summary>
    /// Returns the schema for a table, or null if not found.
    /// </summary>
    public TableSchema? GetTableSchema(string tableName) => _catalog.GetTable(tableName);

    /// <summary>
    /// Returns all indexes defined in the database.
    /// </summary>
    public IReadOnlyCollection<IndexSchema> GetIndexes() => _catalog.GetIndexes();

    /// <summary>
    /// Ensure a full-text index exists for the supplied SQL table and TEXT columns.
    /// The index is stored inside the regular catalog/index subsystem and backfilled
    /// in the same transaction that creates it.
    /// </summary>
    public async ValueTask EnsureFullTextIndexAsync(
        string indexName,
        string tableName,
        IReadOnlyList<string> columns,
        FullTextIndexOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(columns);

        using var writeScope = _inTransaction
            ? WriteOperationScope.NoOp
            : await AcquireWriteOperationScopeAsync(ct);

        string[] normalizedColumns = columns
            .Where(static column => !string.IsNullOrWhiteSpace(column))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (normalizedColumns.Length == 0)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Full-text index must reference at least one TEXT column.");

        FullTextIndexOptions resolvedOptions = options ?? new FullTextIndexOptions();

        InvalidateCachesIfSchemaChanged();

        var existing = _catalog.GetIndex(indexName);
        if (existing != null)
        {
            if (existing.Kind == IndexKind.FullText)
            {
                if (FullTextIndexCatalog.MatchesDefinition(existing, tableName, normalizedColumns, resolvedOptions))
                    return;

                throw new CSharpDbException(
                    ErrorCode.TableAlreadyExists,
                    $"Full-text index '{indexName}' already exists with a different definition.");
            }

            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Index '{indexName}' already exists.");
        }

        if (_inTransaction)
        {
            throw new InvalidOperationException(
                "Full-text indexes cannot be created while an explicit transaction is active.");
        }

        TableSchema tableSchema = _catalog.GetTable(tableName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        var logicalIndex = FullTextIndexCatalog.CreateLogicalSchema(
            indexName,
            tableName,
            normalizedColumns,
            resolvedOptions);

        if (!FullTextIndexMaintenance.TryResolveColumnIndices(logicalIndex, tableSchema, out _))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "Full-text indexes currently support only TEXT columns.");
        }

        bool createdLogicalIndex = false;
        try
        {
            await _pager.BeginTransactionAsync(ct);
            await _catalog.CreateIndexAsync(logicalIndex, ct);
            createdLogicalIndex = true;

            foreach (var internalIndex in FullTextIndexCatalog.CreateInternalSchemas(logicalIndex))
                await _catalog.CreateIndexAsync(internalIndex, ct);

            await FullTextIndexMaintenance.BackfillAsync(
                _catalog,
                tableSchema,
                logicalIndex,
                _recordSerializer,
                ct);

            PagerCommitResult commit = await BeginCommitForTableWithCatalogSyncAsync(tableName, ct);
            await commit.WaitAsync(ct);
        }
        catch
        {
            if (createdLogicalIndex)
            {
                try
                {
                    await _catalog.DropIndexAsync(indexName, ct);
                }
                catch
                {
                    // Best-effort cleanup before rollback.
                }
            }

            try
            {
                await _pager.RollbackAsync(ct);
                await _catalog.ReloadAsync(ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }
    }

    /// <summary>
    /// Run a basic term-intersection search against a previously created full-text index.
    /// Query text is tokenized with the index's stored options; all query terms must match.
    /// </summary>
    public async ValueTask<IReadOnlyList<FullTextSearchHit>> SearchAsync(
        string indexName,
        string query,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentException.ThrowIfNullOrWhiteSpace(query);

        InvalidateCachesIfSchemaChanged();

        IndexSchema indexSchema = _catalog.GetIndex(indexName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
        if (indexSchema.Kind != IndexKind.FullText)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Index '{indexName}' is not a full-text index.");
        }

        return await FullTextIndexReader.SearchAsync(_catalog, indexSchema, query, ct);
    }

    /// <summary>
    /// Returns all view names defined in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetViewNames() => _catalog.GetViewNames();

    /// <summary>
    /// Returns view SQL text by name, or null if the view does not exist.
    /// </summary>
    public string? GetViewSql(string viewName) => _catalog.GetViewSql(viewName);

    /// <summary>
    /// Returns all triggers defined in the database.
    /// </summary>
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _catalog.GetTriggers();

    /// <summary>
    /// Monotonic in-process token that advances on schema mutations (DDL).
    /// Useful for cache invalidation.
    /// </summary>
    public long SchemaVersion => _catalog.SchemaVersion;

    /// <summary>
    /// Planner select-plan cache counters, exposed for tests and benchmarks.
    /// </summary>
    internal readonly record struct SelectPlanCacheDiagnostics(
        long HitCount,
        long MissCount,
        long ReclassificationCount,
        long StoreCount,
        int EntryCount);

    /// <summary>
    /// Returns planner select-plan cache counters.
    /// Internal-only: intended for tests and benchmarks.
    /// </summary>
    internal SelectPlanCacheDiagnostics GetSelectPlanCacheDiagnostics()
    {
        var d = _planner.GetSelectPlanCacheDiagnostics();
        return new SelectPlanCacheDiagnostics(
            d.HitCount,
            d.MissCount,
            d.ReclassificationCount,
            d.StoreCount,
            d.EntryCount);
    }

    /// <summary>
    /// Resets planner select-plan cache counters.
    /// Internal-only: intended for tests and benchmarks.
    /// </summary>
    internal void ResetSelectPlanCacheDiagnostics()
        => _planner.ResetSelectPlanCacheDiagnostics();

    // ============ Document Collection API ============

    private const string CollectionPrefix = "_col_";
    private const string GeneratedCollectionCacheSuffix = "\u0001generated";

    /// <summary>
    /// Get or create a document collection with the given name.
    /// Collections are stored as internal tables with a "_col_" prefix.
    /// </summary>
    [RequiresUnreferencedCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
    public async ValueTask<Collection<T>> GetCollectionAsync<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        T>(
        string name,
        CancellationToken ct = default)
        => await GetCollectionCoreAsync<T>(name, generatedOnly: false, ct);

    /// <summary>
    /// Get or create a trim-safe typed collection with the given name.
    /// The document type must have a generated or manually registered collection model.
    /// </summary>
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2026",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2091",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    [UnconditionalSuppressMessage(
        "Aot",
        "IL3050",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    public async ValueTask<GeneratedCollection<T>> GetGeneratedCollectionAsync<T>(
        string name,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (!CollectionModelRegistry.TryGet<T>(out _))
        {
            throw new InvalidOperationException(
                $"No generated collection model is registered for document type '{typeof(T).FullName ?? typeof(T).Name}'. " +
                "Annotate the type with [CollectionModel(typeof(YourJsonSerializerContext))] or register an ICollectionModel<T> before calling GetGeneratedCollectionAsync.");
        }

        return new GeneratedCollection<T>(await GetCollectionCoreAsync<T>(name, generatedOnly: true, ct));
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2091",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2026",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    [UnconditionalSuppressMessage(
        "Aot",
        "IL3050",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    private async ValueTask<Collection<T>> GetCollectionCoreAsync<T>(
        string name,
        bool generatedOnly,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        IDisposable? writeScope = _inTransaction
            ? WriteOperationScope.NoOp
            : await AcquireWriteOperationScopeAsync(ct);
        try
        {
            InvalidateCachesIfSchemaChanged();

            string catalogName = $"{CollectionPrefix}{name}";
            string cacheKey = BuildCollectionCacheKey(catalogName, generatedOnly);

            // Return cached instance if available
            if (_collectionCache.TryGetValue(cacheKey, out var cached))
                return (Collection<T>)cached;

            // Create the backing table if it doesn't exist
            if (_catalog.GetTable(catalogName) == null)
            {
                PagerCommitResult commit = PagerCommitResult.Completed;
                bool completeCommit = false;
                bool needsTx = !_inTransaction;
                if (needsTx) await _pager.BeginTransactionAsync(ct);
                try
                {
                    // Double-check after acquiring write lock
                    if (_catalog.GetTable(catalogName) == null)
                    {
                        var schema = new TableSchema
                        {
                            TableName = catalogName,
                            Columns = new[]
                            {
                                new ColumnDefinition { Name = "_key", Type = DbType.Text, Nullable = false },
                                new ColumnDefinition { Name = "_doc", Type = DbType.Text, Nullable = false },
                            }
                        };
                        await _catalog.CreateTableAsync(schema, ct);
                    }

                    if (needsTx)
                    {
                        commit = await BeginCommitWithCatalogSyncAsync(ct);
                        completeCommit = true;
                        writeScope?.Dispose();
                        writeScope = WriteOperationScope.NoOp;
                    }
                }
                catch
                {
                    if (needsTx)
                        await RecoverCatalogStateAfterFailedCommitAsync();
                    throw;
                }

                if (completeCommit)
                {
                    await WaitForCommitOrRecoverAsync(commit);
                    await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
                }
            }

            var tree = _catalog.GetTableTree(catalogName);
            var collection = new Collection<T>(
                _pager,
                _catalog,
                catalogName,
                tree,
                _recordSerializer,
                () => _inTransaction,
                RecordPendingCollectionCatalogMutation,
                GetPendingCollectionRowCountAsync,
                AcquireWriteOperationScopeAsync,
                BeginCommitForTableWithCatalogSyncAsync,
                ct => PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct),
                requireRegisteredFields: generatedOnly);
            _collectionCache[cacheKey] = collection;
            return collection;
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    private static string BuildCollectionCacheKey(string catalogName, bool generatedOnly)
        => generatedOnly
            ? catalogName + GeneratedCollectionCacheSuffix
            : catalogName;

    /// <summary>
    /// Returns the names of all document collections in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetCollectionNames()
    {
        return _catalog.GetTableNames()
            .Where(n => n.StartsWith(CollectionPrefix, StringComparison.Ordinal))
            .Select(n => n[CollectionPrefix.Length..])
            .ToArray();
    }

    private Statement ParseCached(string sql) =>
        _statementCache.GetOrAdd(
            sql,
            static s => Parser.TryParseSimpleSelect(s, out var stmt) ? stmt : Parser.Parse(s));

    private static bool LooksLikeInsert(string sql)
    {
        ReadOnlySpan<char> span = sql.AsSpan();
        int pos = 0;
        while (pos < span.Length && char.IsWhiteSpace(span[pos]))
            pos++;

        ReadOnlySpan<char> keyword = "INSERT";
        if (pos + keyword.Length > span.Length)
            return false;

        return span.Slice(pos, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasHybridHotSet(HybridDatabaseOptions hybridOptions)
        => hybridOptions.HotTableNames.Count > 0 || hybridOptions.HotCollectionNames.Count > 0;

    private static void ValidateHybridHotSetOptions(DatabaseOptions options, HybridDatabaseOptions hybridOptions)
    {
        if (!HasHybridHotSet(hybridOptions))
            return;

        if (hybridOptions.PersistenceMode != HybridPersistenceMode.IncrementalDurable)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming is supported only for incremental-durable hybrid mode.",
                nameof(hybridOptions));
        }

        PagerOptions pagerOptions = options.StorageEngineOptions.PagerOptions;
        if (pagerOptions.MaxCachedPages is not null)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming requires the default unbounded pager cache. Remove MaxCachedPages to enable it.",
                nameof(options));
        }

        if (pagerOptions.PageCacheFactory is not null)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming requires the default pager cache. Remove PageCacheFactory to enable it.",
                nameof(options));
        }
    }

    private async ValueTask WarmHybridHotSetAsync(HybridDatabaseOptions hybridOptions, CancellationToken ct)
    {
        if (!HasHybridHotSet(hybridOptions))
            return;

        var warmedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string tableName in EnumerateNormalizedNames(hybridOptions.HotTableNames))
        {
            await WarmSqlTableAsync(tableName, ct);
            warmedTables.Add(tableName);
        }

        foreach (string collectionName in EnumerateNormalizedNames(hybridOptions.HotCollectionNames))
        {
            string catalogName = NormalizeCollectionCatalogName(collectionName);
            if (!warmedTables.Add(catalogName))
                continue;

            if (_catalog.GetTable(catalogName) is null)
            {
                throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Collection '{collectionName}' not found.");
            }

            await _catalog.GetTableTree(catalogName).WarmOwnedPagesAsync(ct);
        }
    }

    private async ValueTask WarmSqlTableAsync(string tableName, CancellationToken ct)
    {
        if (_catalog.GetTable(tableName) is null)
        {
            throw new CSharpDbException(
                ErrorCode.TableNotFound,
                $"Table '{tableName}' not found.");
        }

        await _catalog.GetTableTree(tableName).WarmOwnedPagesAsync(ct);

        foreach (var index in _catalog.GetIndexesForTable(tableName))
            await new BTree(_pager, _catalog.GetIndexStore(index.IndexName).RootPageId).WarmOwnedPagesAsync(ct);
    }

    private static IEnumerable<string> EnumerateNormalizedNames(IReadOnlyList<string> names)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string name in names)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            string trimmed = name.Trim();
            if (seen.Add(trimmed))
                yield return trimmed;
        }
    }

    private static string NormalizeCollectionCatalogName(string collectionName)
    {
        return collectionName.StartsWith(CollectionPrefix, StringComparison.Ordinal)
            ? collectionName
            : $"{CollectionPrefix}{collectionName}";
    }

    private void RecordPendingCollectionCatalogMutation(
        string tableName,
        BTree tree,
        long rowCountDelta,
        bool requiresExactRowCountSync,
        bool hasDocumentMutation)
    {
        if (!_inTransaction)
            return;

        if (!_pendingCollectionCatalogMutations.TryGetValue(tableName, out var pending))
        {
            pending = new PendingCollectionCatalogMutation(tree);
            _pendingCollectionCatalogMutations[tableName] = pending;
        }

        pending.Record(tree, rowCountDelta, requiresExactRowCountSync, hasDocumentMutation);
    }

    private async ValueTask<long?> GetPendingCollectionRowCountAsync(string tableName, BTree tree, CancellationToken ct)
    {
        if (!_inTransaction || !_pendingCollectionCatalogMutations.ContainsKey(tableName))
            return null;

        return await tree.CountEntriesAsync(ct);
    }

    private static async ValueTask DelayImplicitConflictRetryAsync(int attempt, CancellationToken ct)
    {
        double delayMs = Math.Min(20, 0.25 * Math.Pow(2, Math.Max(0, attempt)));
        double jitterMs = delayMs <= 0 ? 0 : Random.Shared.NextDouble() * delayMs;
        TimeSpan delay = TimeSpan.FromMilliseconds(jitterMs);
        if (delay > TimeSpan.Zero)
            await Task.Delay(delay, ct);
    }

    private async ValueTask<IDisposable> AcquireWriteOperationScopeAsync(CancellationToken ct)
    {
        await _writeOperationGate.WaitAsync(ct);
        return new WriteOperationScope(_writeOperationGate);
    }

    private void ReleaseExplicitTransactionWriteGate()
    {
        _writeOperationGate.Release();
    }

    private ValueTask FlushPendingCollectionCatalogMutationsBeforeSqlAsync(CancellationToken ct)
    {
        if (!_inTransaction || _pendingCollectionCatalogMutations.Count == 0)
            return ValueTask.CompletedTask;

        return FlushPendingCollectionCatalogMutationsAsync(ct);
    }

    private void RefreshCachedCollectionsFromCatalog()
    {
        foreach (var cached in _collectionCache.Values)
        {
            if (cached is ICollectionTreeRefresh refreshable)
                refreshable.RefreshTreeFromCatalog();
        }
    }

    private async ValueTask RecoverCatalogStateAfterFailedCommitAsync()
    {
        ClearPendingCollectionCatalogMutations();
        try
        {
            await _pager.RollbackAsync(CancellationToken.None);
        }
        catch
        {
            // Preserve the original failure.
        }

        try
        {
            await _catalog.ReloadAsync(CancellationToken.None);
            RefreshCachedCollectionsFromCatalog();
            _statementCache.Clear();
        }
        catch
        {
            // Preserve the original failure.
        }
    }

    private async ValueTask WaitForCommitOrRecoverAsync(PagerCommitResult commit)
    {
        try
        {
            await commit.WaitAsync();
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
    }

    private void InvalidateCachesIfSchemaChanged()
    {
        long currentVersion = _catalog.SchemaVersion;
        if (currentVersion == _observedSchemaVersion)
            return;

        _statementCache.Clear();
        _collectionCache.Clear();
        _observedSchemaVersion = currentVersion;
    }

    private async ValueTask CommitWithCatalogSyncAsync(CancellationToken ct, bool persistHybridState = true)
    {
        PagerCommitResult commit = await BeginCommitWithCatalogSyncAsync(ct);
        await WaitForCommitOrRecoverAsync(commit);
        if (persistHybridState)
            await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
    }

    private async ValueTask CommitInsertWithCatalogSyncAsync(string tableName, CancellationToken ct)
    {
        PagerCommitResult commit = await BeginCommitForTableWithCatalogSyncAsync(tableName, ct);
        await CompleteImplicitCommitAsync(commit, ct);
    }

    private async ValueTask<PagerCommitResult> BeginCommitWithCatalogSyncAsync(CancellationToken ct)
    {
        await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
        await _catalog.PersistAllRootPageChangesAsync(ct);
        return await _pager.BeginCommitAsync(ct);
    }

    private async ValueTask<PagerCommitResult> BeginCommitForTableWithCatalogSyncAsync(string tableName, CancellationToken ct)
    {
        await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
        await _catalog.PersistRootPageChangesAsync(tableName, ct);
        return await _pager.BeginCommitAsync(ct);
    }

    private async ValueTask CompleteImplicitCommitAsync(PagerCommitResult commit, CancellationToken ct)
    {
        await WaitForCommitOrRecoverAsync(commit);
        await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct, writeScopeHeld: true);
    }

    private ValueTask PersistHybridStateAsync(
        HybridPersistenceTriggers trigger,
        CancellationToken ct,
        bool writeScopeHeld = false)
    {
        if (_hybridPersistenceCoordinator is null)
            return ValueTask.CompletedTask;

        return _hybridPersistenceCoordinator.PersistAsync(this, trigger, writeScopeHeld, ct);
    }

    public async ValueTask DisposeAsync()
    {
        bool rolledBackExplicitTransaction = false;
        if (_inTransaction)
        {
            try { await _pager.RollbackAsync(); } catch { }
            ClearPendingCollectionCatalogMutations();
            _inTransaction = false;
            ReleaseExplicitTransactionWriteGate();
            rolledBackExplicitTransaction = true;
        }

        try
        {
            if (!rolledBackExplicitTransaction)
                await FlushPendingAdvisoryStatisticsAsync(CancellationToken.None);
            await PersistHybridStateAsync(HybridPersistenceTriggers.Dispose, CancellationToken.None);
        }
        finally
        {
            await _pager.DisposeAsync();
            _hybridPersistenceCoordinator?.Dispose();
            _writeOperationGate.Dispose();
            _sharedStateGate.Dispose();
        }
    }

    private async ValueTask FlushPendingAdvisoryStatisticsAsync(
        CancellationToken ct,
        bool writeScopeHeld = false)
    {
        if (_inTransaction)
        {
            return;
        }

        IDisposable? writeScope = null;
        try
        {
            if (!writeScopeHeld)
                writeScope = await AcquireWriteOperationScopeAsync(ct);

            await FlushPendingCollectionCatalogMutationsAsync(ct);
            if (!_catalog.HasDirtyAdvisoryStatistics)
                return;

            await _pager.BeginTransactionAsync(ct);
            await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
            await _catalog.PersistAllRootPageChangesAsync(ct);
            PagerCommitResult commit = await _pager.BeginCommitAsync(ct);
            writeScope?.Dispose();
            writeScope = null;
            await commit.WaitAsync(ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();

            throw;
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    private async ValueTask FlushPendingCollectionCatalogMutationsAsync(CancellationToken ct)
    {
        if (_pendingCollectionCatalogMutations.Count == 0)
            return;

        foreach (var entry in _pendingCollectionCatalogMutations)
        {
            string tableName = entry.Key;
            PendingCollectionCatalogMutation pending = entry.Value;

            if (pending.RequiresExactRowCountSync)
            {
                long exactRowCount = await pending.Tree.CountEntriesAsync(ct);
                await _catalog.SetTableRowCountAsync(tableName, exactRowCount, ct);
            }
            else if (pending.RowCountDelta != 0)
            {
                if (_catalog.TryGetExactTableRowCount(tableName, out _))
                {
                    await _catalog.AdjustTableRowCountKnownExactAsync(tableName, pending.RowCountDelta, ct);
                }
                else
                {
                    long exactRowCount = await pending.Tree.CountEntriesAsync(ct);
                    await _catalog.SetTableRowCountAsync(tableName, exactRowCount, ct);
                }
            }

            if (pending.HasDocumentMutation)
                await _catalog.MarkTableColumnStatisticsStaleAsync(tableName, ct);
        }

        ClearPendingCollectionCatalogMutations();
    }

    private void ClearPendingCollectionCatalogMutations()
    {
        _pendingCollectionCatalogMutations.Clear();
    }

    private sealed class WriteOperationScope : IDisposable
    {
        internal static readonly WriteOperationScope NoOp = new(null);

        private SemaphoreSlim? _gate;

        internal WriteOperationScope(SemaphoreSlim? gate)
        {
            _gate = gate;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _gate, null)?.Release();
        }
    }

    private sealed class PendingCollectionCatalogMutation
    {
        internal PendingCollectionCatalogMutation(BTree tree)
        {
            Tree = tree;
        }

        internal BTree Tree { get; private set; }
        internal long RowCountDelta { get; private set; }
        internal bool RequiresExactRowCountSync { get; private set; }
        internal bool HasDocumentMutation { get; private set; }

        internal void Record(BTree tree, long rowCountDelta, bool requiresExactRowCountSync, bool hasDocumentMutation)
        {
            Tree = tree;
            RowCountDelta = checked(RowCountDelta + rowCountDelta);
            RequiresExactRowCountSync |= requiresExactRowCountSync;
            HasDocumentMutation |= hasDocumentMutation;
        }
    }

    /// <summary>
    /// An isolated read-only session that sees a consistent snapshot.
    /// Multiple ReaderSessions can exist concurrently with an active writer.
    /// </summary>
    public sealed class ReaderSession : IDisposable
    {
        private readonly Pager _pager;
        private readonly SchemaCatalog _catalog;
        private readonly IRecordSerializer _recordSerializer;
        private readonly IRecordSerializer? _collectionReadSerializer;
        private readonly StatementCache _statementCache;
        private readonly WalSnapshot _snapshot;
        private readonly IReadOnlyDictionary<string, long> _snapshotRowCounts;
        private Pager? _snapshotPager;
        private QueryPlanner? _planner;
        private string? _lastSql;
        private Statement? _lastParsedStatement;
        private bool _disposed;
        private int _activeQuery;

        internal ReaderSession(
            Pager pager,
            SchemaCatalog catalog,
            IRecordSerializer recordSerializer,
            WalSnapshot snapshot,
            StatementCache statementCache,
            IReadOnlyDictionary<string, long> snapshotRowCounts)
        {
            _pager = pager;
            _catalog = catalog;
            _recordSerializer = recordSerializer;
            _collectionReadSerializer = recordSerializer is DefaultRecordSerializer
                ? new CollectionAwareRecordSerializer(recordSerializer)
                : null;
            _statementCache = statementCache;
            _snapshot = snapshot;
            _snapshotRowCounts = snapshotRowCounts;
            _snapshotPager = pager.CreateSnapshotReader(snapshot);
            _planner = new QueryPlanner(
                _snapshotPager,
                catalog,
                recordSerializer,
                tableName => _snapshotRowCounts.TryGetValue(tableName, out long rowCount) ? rowCount : null);
        }

        /// <summary>
        /// Execute a read-only SQL query against the snapshot.
        /// Only SELECT statements are allowed.
        /// </summary>
        public async ValueTask<QueryResult> ExecuteReadAsync(string sql,
            CancellationToken ct = default)
        {
            Statement stmt;
            if (_lastSql != null &&
                string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                _lastParsedStatement != null)
            {
                stmt = _lastParsedStatement;
            }
            else
            {
                stmt = _statementCache.GetOrAdd(sql, static s => Parser.Parse(s));
                _lastSql = sql;
                _lastParsedStatement = stmt;
            }

            return await ExecuteReadAsync(stmt, ct);
        }

        /// <summary>
        /// Execute a read-only prepared statement against the snapshot.
        /// Only SELECT statements are allowed.
        /// </summary>
        public async ValueTask<QueryResult> ExecuteReadAsync(Statement stmt, CancellationToken ct = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (stmt is not QueryStatement && stmt is not WithStatement)
                throw new CSharpDbException(ErrorCode.Unknown,
                    "Reader sessions only support SELECT statements.");

            if (Interlocked.CompareExchange(ref _activeQuery, 1, 0) != 0)
            {
                throw new InvalidOperationException(
                    "ReaderSession supports only one active query at a time. Dispose the previous QueryResult before executing another query.");
            }

            try
            {
                QueryResult result;
                if (stmt is SelectStatement select && await TryExecuteFastReadAsync(select, ct) is { } fastResult)
                {
                    result = fastResult;
                }
                else
                {
                    _planner ??= new QueryPlanner(GetOrCreateSnapshotPager(), _catalog, _recordSerializer);
                    result = await _planner.ExecuteAsync(stmt, ct);
                }

                result.SetDisposeCallback(ReleaseActiveQueryAsync);
                return result;
            }
            catch
            {
                Volatile.Write(ref _activeQuery, 0);
                throw;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _snapshotPager?.Dispose();
                _pager.ReleaseReaderSnapshot(_snapshot);
                _disposed = true;
            }
        }

        private ValueTask ReleaseActiveQueryAsync()
        {
            Volatile.Write(ref _activeQuery, 0);
            return ValueTask.CompletedTask;
        }

        private async ValueTask<QueryResult?> TryExecuteFastReadAsync(SelectStatement stmt, CancellationToken ct)
        {
            if (TryExecuteCountStarFastPath(stmt, out var countResult))
                return countResult;

            return await TryExecutePrimaryKeyLookupFastPathAsync(stmt, ct);
        }

        private bool TryExecuteCountStarFastPath(SelectStatement stmt, out QueryResult result)
        {
            result = null!;

            if (stmt.From is not SimpleTableRef simpleRef)
                return false;
            if (IsSystemCatalogTable(simpleRef.TableName) || _catalog.IsView(simpleRef.TableName))
                return false;
            if (stmt.Where != null || stmt.GroupBy != null || stmt.Having != null)
                return false;
            if (stmt.OrderBy is { Count: > 0 })
                return false;
            if (stmt.Limit.HasValue || stmt.Offset.HasValue)
                return false;
            if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
                return false;
            if (stmt.Columns[0].Expression is not FunctionCallExpression func)
                return false;
            if (!func.IsStarArg || func.IsDistinct || func.Arguments.Count != 0)
                return false;
            if (!string.Equals(func.FunctionName, "COUNT", StringComparison.OrdinalIgnoreCase))
                return false;
            if (_catalog.GetTable(simpleRef.TableName) == null)
                return false;

            string outputName = stmt.Columns[0].Alias ?? "COUNT(*)";
            ColumnDefinition[] outputSchema =
            [
                new ColumnDefinition
                {
                    Name = outputName,
                    Type = DbType.Integer,
                    Nullable = false,
                },
            ];

            if (_snapshotRowCounts.TryGetValue(simpleRef.TableName, out long rowCount))
            {
                result = QueryResult.FromSyncLookup([DbValue.FromInteger(rowCount)], outputSchema);
                return true;
            }

            if (_catalog.TryGetExactTableRowCount(simpleRef.TableName, out rowCount))
            {
                result = QueryResult.FromSyncLookup([DbValue.FromInteger(rowCount)], outputSchema);
                return true;
            }

            var tableTree = _catalog.GetTableTree(simpleRef.TableName, GetOrCreateSnapshotPager());
            result = new QueryResult(new CountStarTableOperator(tableTree, outputSchema, ignoreCachedCount: true));
            return true;
        }

        private async ValueTask<QueryResult?> TryExecutePrimaryKeyLookupFastPathAsync(SelectStatement stmt, CancellationToken ct)
        {
            if (stmt.IsDistinct)
                return null;
            if (stmt.From is not SimpleTableRef simpleRef)
                return null;
            if (IsSystemCatalogTable(simpleRef.TableName) || _catalog.IsView(simpleRef.TableName))
                return null;
            if (stmt.Where == null || stmt.GroupBy != null || stmt.Having != null)
                return null;
            if (stmt.OrderBy is { Count: > 0 } || stmt.Limit.HasValue || stmt.Offset.HasValue)
                return null;

            var schema = _catalog.GetTable(simpleRef.TableName);
            if (schema == null)
                return null;

            int pkIndex = schema.PrimaryKeyColumnIndex;
            if (pkIndex < 0 || pkIndex >= schema.Columns.Count || schema.Columns[pkIndex].Type != DbType.Integer)
                return null;

            if (!TryExtractPrimaryKeyEquality(stmt.Where, simpleRef, schema, pkIndex, out long lookupValue))
                return null;

            if (!TryBuildProjection(stmt.Columns, schema, out var projectionColumnIndices, out var outputColumns, out bool selectStar))
                return null;

            var tableTree = new BTree(_pager, _catalog.GetTableRootPage(simpleRef.TableName));
            ReadOnlyMemory<byte>? payload = tableTree.TryFindSnapshotCachedMemory(
                lookupValue,
                _snapshot,
                out var cachedPayload)
                ? cachedPayload
                : await tableTree.FindMemoryAsync(lookupValue, _snapshot, ct);
            if (!payload.HasValue)
                return QueryResult.FromSyncLookup(null, outputColumns);

            if (selectStar)
            {
                var serializer = GetReadSerializer(schema);
                return QueryResult.FromSyncLookup(serializer.Decode(payload.Value.Span), outputColumns);
            }

            if (IsPrimaryKeyOnlyProjection(projectionColumnIndices, pkIndex))
            {
                var keyValue = DbValue.FromInteger(lookupValue);
                var row = new DbValue[outputColumns.Length];
                Array.Fill(row, keyValue);
                return QueryResult.FromSyncLookup(row, outputColumns);
            }

            var decoded = GetReadSerializer(schema).Decode(payload.Value.Span);
            var projected = new DbValue[projectionColumnIndices.Length];
            for (int i = 0; i < projectionColumnIndices.Length; i++)
                projected[i] = decoded[projectionColumnIndices[i]];

            return QueryResult.FromSyncLookup(projected, outputColumns);
        }

        private Pager GetOrCreateSnapshotPager()
            => _snapshotPager ??= _pager.CreateSnapshotReader(_snapshot);

        private IRecordSerializer GetReadSerializer(TableSchema schema)
            => _collectionReadSerializer != null && schema.TableName.StartsWith("_col_", StringComparison.Ordinal)
                ? _collectionReadSerializer
                : _recordSerializer;

        private static bool TryExtractPrimaryKeyEquality(
            Expression expression,
            SimpleTableRef tableRef,
            TableSchema schema,
            int primaryKeyIndex,
            out long lookupValue)
        {
            lookupValue = 0;

            if (expression is not BinaryExpression { Op: BinaryOp.Equals } equals)
                return false;

            if (TryMatchPrimaryKeyColumn(equals.Left, tableRef, schema, primaryKeyIndex) &&
                TryReadIntegerLiteral(equals.Right, out lookupValue))
            {
                return true;
            }

            if (TryMatchPrimaryKeyColumn(equals.Right, tableRef, schema, primaryKeyIndex) &&
                TryReadIntegerLiteral(equals.Left, out lookupValue))
            {
                return true;
            }

            return false;
        }

        private static bool TryMatchPrimaryKeyColumn(
            Expression expression,
            SimpleTableRef tableRef,
            TableSchema schema,
            int primaryKeyIndex)
        {
            if (expression is not ColumnRefExpression column)
                return false;

            if (column.TableAlias != null)
            {
                string expectedAlias = tableRef.Alias ?? tableRef.TableName;
                if (!string.Equals(column.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            int columnIndex = column.TableAlias != null
                ? schema.GetQualifiedColumnIndex(column.TableAlias, column.ColumnName)
                : schema.GetColumnIndex(column.ColumnName);

            return columnIndex == primaryKeyIndex;
        }

        private static bool TryReadIntegerLiteral(Expression expression, out long value)
        {
            if (expression is LiteralExpression { LiteralType: TokenType.IntegerLiteral, Value: long int64 })
            {
                value = int64;
                return true;
            }

            if (expression is LiteralExpression { LiteralType: TokenType.IntegerLiteral, Value: int int32 })
            {
                value = int32;
                return true;
            }

            value = 0;
            return false;
        }

        private static bool TryBuildProjection(
            IReadOnlyList<SelectColumn> columns,
            TableSchema schema,
            out int[] columnIndices,
            out ColumnDefinition[] outputColumns,
            out bool selectStar)
        {
            selectStar = columns.Any(static c => c.IsStar);
            if (selectStar)
            {
                if (columns.Count != 1)
                {
                    columnIndices = Array.Empty<int>();
                    outputColumns = Array.Empty<ColumnDefinition>();
                    return false;
                }

                columnIndices = Array.Empty<int>();
                outputColumns = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
                return true;
            }

            columnIndices = new int[columns.Count];
            outputColumns = new ColumnDefinition[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                if (column.Expression is not ColumnRefExpression colRef)
                    return false;

                int sourceIndex = colRef.TableAlias != null
                    ? schema.GetQualifiedColumnIndex(colRef.TableAlias, colRef.ColumnName)
                    : schema.GetColumnIndex(colRef.ColumnName);
                if (sourceIndex < 0 || sourceIndex >= schema.Columns.Count)
                    return false;

                columnIndices[i] = sourceIndex;
                var sourceColumn = schema.Columns[sourceIndex];
                outputColumns[i] = column.Alias != null
                    ? new ColumnDefinition
                    {
                        Name = column.Alias,
                        Type = sourceColumn.Type,
                        Nullable = sourceColumn.Nullable,
                        IsPrimaryKey = sourceColumn.IsPrimaryKey,
                        IsIdentity = sourceColumn.IsIdentity,
                    }
                    : sourceColumn;
            }

            return true;
        }

        private static bool IsPrimaryKeyOnlyProjection(int[] columnIndices, int primaryKeyIndex)
        {
            if (primaryKeyIndex < 0)
                return false;

            for (int i = 0; i < columnIndices.Length; i++)
            {
                if (columnIndices[i] != primaryKeyIndex)
                    return false;
            }

            return true;
        }

        private static bool IsSystemCatalogTable(string tableName) =>
            string.Equals(tableName, "sys.tables", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_tables", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.columns", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_columns", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.indexes", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_indexes", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.views", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_views", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.triggers", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_triggers", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.objects", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_objects", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.table_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_table_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.column_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_column_stats", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Thread-safe bounded cache for parsed SQL statements.
    /// </summary>
    internal sealed class StatementCache
    {
        private readonly int _capacity;
        private readonly Dictionary<string, Statement> _map = new(StringComparer.Ordinal);
        private readonly Queue<string> _insertionOrder = new();
        private readonly int[] _recentMissHashes;
        private readonly Dictionary<int, int> _recentMissHashCounts;
        private int _recentMissHashCursor;
        private int _recentMissHashCount;
        private string? _lastSql;
        private Statement? _lastStatement;
        private readonly object _gate = new();

        internal StatementCache(int capacity)
        {
            _capacity = capacity > 0 ? capacity : 0;
            int fingerprintWindowSize = _capacity <= 0 ? 0 : _capacity;
            _recentMissHashes = new int[fingerprintWindowSize];
            _recentMissHashCounts = fingerprintWindowSize > 0
                ? new Dictionary<int, int>(fingerprintWindowSize)
                : new Dictionary<int, int>(0);
            _recentMissHashCursor = 0;
            _recentMissHashCount = 0;
        }

        internal bool TryGetOrMarkBypass(string sql, out Statement statement, out bool bypassParse)
        {
            statement = null!;
            bypassParse = false;
            if (_capacity == 0)
                return false;

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    statement = _lastStatement;
                    return true;
                }

                if (_map.TryGetValue(sql, out var hitNode))
                {
                    _lastSql = sql;
                    _lastStatement = hitNode;
                    statement = hitNode;
                    return true;
                }

                if (_recentMissHashes.Length > 0 && _map.Count >= _capacity)
                {
                    int hash = StringComparer.Ordinal.GetHashCode(sql);
                    if (!HasRecentMissHash(hash))
                    {
                        RecordRecentMissHash(hash);
                        bypassParse = true;
                    }
                }
            }

            return false;
        }

        internal Statement GetOrAdd(string sql, Func<string, Statement> parse)
        {
            if (_capacity == 0)
                return parse(sql);

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    return _lastStatement;
                }

                if (_map.TryGetValue(sql, out var hitNode))
                {
                    _lastSql = sql;
                    _lastStatement = hitNode;
                    return hitNode;
                }
            }

            // Parse outside lock to avoid blocking other cache operations.
            var parsed = parse(sql);

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    return _lastStatement;
                }

                if (_map.TryGetValue(sql, out var existingNode))
                {
                    _lastSql = sql;
                    _lastStatement = existingNode;
                    return existingNode;
                }

                Statement statementToReturn = parsed;

                if (_map.Count < _capacity)
                {
                    _map[sql] = parsed;
                    _insertionOrder.Enqueue(sql);
                }
                else if ((parsed is QueryStatement or WithStatement) && ShouldPromoteQueryAtCapacity(sql))
                {
                    // Only promote read-only query statements that show short-term reuse.
                    // This avoids steady eviction churn on one-off/high-cardinality SQL.
                    EvictOldestEntry();
                    _map[sql] = parsed;
                    _insertionOrder.Enqueue(sql);
                }

                _lastSql = sql;
                _lastStatement = statementToReturn;
                return statementToReturn;
            }
        }

        private bool ShouldPromoteQueryAtCapacity(string sql)
        {
            if (_recentMissHashes.Length == 0)
                return true;

            int hash = StringComparer.Ordinal.GetHashCode(sql);
            if (HasRecentMissHash(hash))
                return true;

            RecordRecentMissHash(hash);

            return false;
        }

        private bool HasRecentMissHash(int hash) => _recentMissHashCounts.ContainsKey(hash);

        private void RecordRecentMissHash(int hash)
        {
            if (_recentMissHashes.Length == 0)
                return;

            if (_recentMissHashCount == _recentMissHashes.Length)
            {
                int evicted = _recentMissHashes[_recentMissHashCursor];
                if (_recentMissHashCounts.TryGetValue(evicted, out int evictedCount))
                {
                    if (evictedCount <= 1)
                        _recentMissHashCounts.Remove(evicted);
                    else
                        _recentMissHashCounts[evicted] = evictedCount - 1;
                }
            }
            else
            {
                _recentMissHashCount++;
            }

            _recentMissHashes[_recentMissHashCursor] = hash;
            _recentMissHashCounts.TryGetValue(hash, out int currentCount);
            _recentMissHashCounts[hash] = currentCount + 1;

            _recentMissHashCursor++;
            if (_recentMissHashCursor >= _recentMissHashes.Length)
                _recentMissHashCursor = 0;
        }

        private void EvictOldestEntry()
        {
            while (_insertionOrder.Count > 0)
            {
                string candidate = _insertionOrder.Dequeue();
                if (_map.Remove(candidate))
                    return;
            }
        }

        internal void Clear()
        {
            if (_capacity == 0)
                return;

            lock (_gate)
            {
                _map.Clear();
                _insertionOrder.Clear();
                Array.Clear(_recentMissHashes);
                _recentMissHashCounts.Clear();
                _recentMissHashCursor = 0;
                _recentMissHashCount = 0;
                _lastSql = null;
                _lastStatement = null;
            }
        }
    }

}
