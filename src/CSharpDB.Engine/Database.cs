using CSharpDB.Core;
using CSharpDB.Execution;
using CSharpDB.Sql;

namespace CSharpDB.Engine;

/// <summary>
/// Top-level entry point for the CSharpDB embedded database engine.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private const int DefaultStatementCacheCapacity = 512;

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly QueryPlanner _planner;
    private readonly IRecordSerializer _recordSerializer;
    private readonly StatementCache _statementCache;
    private readonly Dictionary<string, object> _collectionCache = new(StringComparer.Ordinal);
    private long _observedSchemaVersion;
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

    private Database(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer recordSerializer)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer;
        _planner = new QueryPlanner(pager, catalog, recordSerializer);
        _statementCache = new StatementCache(DefaultStatementCacheCapacity);
        _observedSchemaVersion = catalog.SchemaVersion;
    }

    /// <summary>
    /// Open an existing database file, or create a new one if it doesn't exist.
    /// </summary>
    public static async ValueTask<Database> OpenAsync(string filePath, CancellationToken ct = default)
    {
        return await OpenAsync(filePath, new DatabaseOptions(), ct);
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
            context.RecordSerializer);
    }

    /// <summary>
    /// Execute a SQL statement. Returns a QueryResult with rows (for SELECT) or affected count (for DML/DDL).
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken ct = default)
    {
        InvalidateCachesIfSchemaChanged();
        if (_statementCache.TryGetOrMarkBypass(sql, out var cachedStmt, out bool bypassParse))
            return ExecuteStatementAsync(cachedStmt, ct);

        if (LooksLikeInsert(sql) && Parser.TryParseSimpleInsert(sql, out var simpleInsert))
            return ExecuteSimpleInsertAsync(simpleInsert, ct);

        if (bypassParse)
        {
            if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var simpleLookup))
            {
                if (_planner.TryExecuteSimplePrimaryKeyLookup(simpleLookup, out var fastResult))
                    return ValueTask.FromResult(fastResult);
            }
        }

        var stmt = ParseCached(sql);
        return ExecuteStatementAsync(stmt, ct);
    }

    /// <summary>
    /// Execute a pre-parsed SQL statement. Used by prepared command paths
    /// to bypass SQL text parsing on repeated executions.
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
        => ExecuteStatementAsync(statement, ct);

    internal ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken ct = default)
        => ExecuteSimpleInsertAsync(insert, ct);

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
        if (stmt is SelectStatement)
            return _planner.ExecuteAsync(stmt, ct);

        return ExecuteWriteStatementAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteWriteStatementAsync(Statement stmt, CancellationToken ct)
    {
        bool explicitTransaction = _inTransaction;
        if (!_inTransaction)
            await _pager.BeginTransactionAsync(ct);

        try
        {
            QueryResult result = stmt switch
            {
                InsertStatement insert => await _planner.ExecuteInsertAsync(
                    insert,
                    persistRootChanges: !explicitTransaction,
                    ct),
                _ => await _planner.ExecuteAsync(stmt, ct),
            };

            if (!_inTransaction)
            {
                // Auto-commit
                await CommitWithCatalogSyncAsync(ct);
            }

            return result;
        }
        catch
        {
            if (!_inTransaction)
                await _pager.RollbackAsync(ct);
            throw;
        }
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct)
    {
        bool explicitTransaction = _inTransaction;
        if (!_inTransaction)
            await _pager.BeginTransactionAsync(ct);

        try
        {
            var result = await _planner.ExecuteSimpleInsertAsync(
                insert,
                persistRootChanges: !explicitTransaction,
                ct);

            if (!_inTransaction)
                await CommitWithCatalogSyncAsync(ct);

            return result;
        }
        catch
        {
            if (!_inTransaction)
                await _pager.RollbackAsync(ct);
            throw;
        }
    }

    /// <summary>
    /// Begin an explicit transaction.
    /// </summary>
    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Transaction already active.");
        await _pager.BeginTransactionAsync(ct);
        _inTransaction = true;
    }

    /// <summary>
    /// Commit the current transaction.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");
        await CommitWithCatalogSyncAsync(ct);
        _inTransaction = false;
    }

    /// <summary>
    /// Rollback the current transaction.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");
        await _pager.RollbackAsync(ct);
        _inTransaction = false;
    }

    /// <summary>
    /// Manually trigger a WAL checkpoint.
    /// </summary>
    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        await _pager.CheckpointAsync(ct);
    }

    /// <summary>
    /// Create an independent reader that sees a snapshot of the database
    /// at the current point in time. The reader does not block writers.
    /// Caller must dispose the returned ReaderSession when done.
    /// </summary>
    public ReaderSession CreateReaderSession()
    {
        var snapshot = _pager.AcquireReaderSnapshot();
        return new ReaderSession(
            _pager,
            _catalog,
            _recordSerializer,
            snapshot,
            _statementCache);
    }

    /// <summary>
    /// Returns the names of all tables in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetTableNames() => _catalog.GetTableNames();

    /// <summary>
    /// Returns the schema for a table, or null if not found.
    /// </summary>
    public TableSchema? GetTableSchema(string tableName) => _catalog.GetTable(tableName);

    /// <summary>
    /// Returns all indexes defined in the database.
    /// </summary>
    public IReadOnlyCollection<IndexSchema> GetIndexes() => _catalog.GetIndexes();

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

    /// <summary>
    /// Get or create a document collection with the given name.
    /// Collections are stored as internal tables with a "_col_" prefix.
    /// </summary>
    public async ValueTask<Collection<T>> GetCollectionAsync<T>(string name, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        InvalidateCachesIfSchemaChanged();

        string catalogName = $"{CollectionPrefix}{name}";

        // Return cached instance if available
        if (_collectionCache.TryGetValue(catalogName, out var cached))
            return (Collection<T>)cached;

        // Create the backing table if it doesn't exist
        if (_catalog.GetTable(catalogName) == null)
        {
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

                if (needsTx) await CommitWithCatalogSyncAsync(ct);
            }
            catch
            {
                if (needsTx) await _pager.RollbackAsync(ct);
                throw;
            }
        }

        var tree = _catalog.GetTableTree(catalogName);
        var collection = new Collection<T>(
            _pager,
            _catalog,
            _recordSerializer,
            catalogName,
            tree,
            () => _inTransaction);
        _collectionCache[catalogName] = collection;
        return collection;
    }

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

    private void InvalidateCachesIfSchemaChanged()
    {
        long currentVersion = _catalog.SchemaVersion;
        if (currentVersion == _observedSchemaVersion)
            return;

        _statementCache.Clear();
        _collectionCache.Clear();
        _observedSchemaVersion = currentVersion;
    }

    private async ValueTask CommitWithCatalogSyncAsync(CancellationToken ct)
    {
        await _catalog.PersistAllRootPageChangesAsync(ct);
        await _pager.CommitAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_inTransaction)
        {
            try { await _pager.RollbackAsync(); } catch { }
        }
        await _pager.DisposeAsync();
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
        private readonly WalSnapshot _snapshot;
        private readonly StatementCache _statementCache;
        private bool _disposed;

        internal ReaderSession(
            Pager pager,
            SchemaCatalog catalog,
            IRecordSerializer recordSerializer,
            WalSnapshot snapshot,
            StatementCache statementCache)
        {
            _pager = pager;
            _catalog = catalog;
            _recordSerializer = recordSerializer;
            _snapshot = snapshot;
            _statementCache = statementCache;
        }

        /// <summary>
        /// Execute a read-only SQL query against the snapshot.
        /// Only SELECT statements are allowed.
        /// </summary>
        public async ValueTask<QueryResult> ExecuteReadAsync(string sql,
            CancellationToken ct = default)
        {
            var stmt = _statementCache.GetOrAdd(sql, static s => Parser.Parse(s));
            if (stmt is not SelectStatement)
                throw new CSharpDbException(ErrorCode.Unknown,
                    "Reader sessions only support SELECT statements.");

            // Create a snapshot-aware pager for reading
            var snapshotPager = _pager.CreateSnapshotReader(_snapshot);
            var planner = new QueryPlanner(snapshotPager, _catalog, _recordSerializer);
            return await planner.ExecuteAsync(stmt, ct);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _pager.ReleaseReaderSnapshot();
                _disposed = true;
            }
        }
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
                else if (parsed is SelectStatement && ShouldPromoteSelectAtCapacity(sql))
                {
                    // Only promote SELECT statements that show short-term reuse.
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

        private bool ShouldPromoteSelectAtCapacity(string sql)
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
