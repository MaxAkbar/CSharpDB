using System.Collections.Concurrent;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class CSharpDbConnectionPoolRegistry
{
    private static readonly ConcurrentDictionary<PoolKey, CSharpDbConnectionPool> s_pools = new();
    private static readonly SemaphoreSlim s_gate = new(1, 1);
    private static readonly StringComparer s_pathComparer =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly Dictionary<string, Task> s_retiringPools = new(s_pathComparer);
    private static readonly Dictionary<string, int> s_directLeaseCounts = new(s_pathComparer);

    internal static async ValueTask<PooledDatabaseSession> OpenPooledSessionAsync(
        PoolKey key,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            CSharpDbConnectionPool pool = await GetOrCreateAsync(
                key,
                openDatabaseAsync,
                cancellationToken);
            try
            {
                return await pool.OpenSessionAsync(cancellationToken);
            }
            catch (CSharpDbConnectionPoolRetiredException)
            {
                await EvictDisabledPoolAsync(pool);
            }
        }
    }

    private static async ValueTask<CSharpDbConnectionPool> GetOrCreateAsync(
        PoolKey key,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(openDatabaseAsync);

        while (true)
        {
            Task? retirementTask = null;
            await s_gate.WaitAsync(cancellationToken);
            try
            {
                if (s_directLeaseCounts.TryGetValue(key.DataSource, out int directLeaseCount) &&
                    directLeaseCount > 0)
                {
                    throw new InvalidOperationException(
                        "Cannot open a pooled embedded connection while non-pooled connections for the same data source are open.");
                }

                if (s_retiringPools.TryGetValue(key.DataSource, out Task? retiring))
                {
                    if (retiring.IsCompletedSuccessfully)
                        s_retiringPools.Remove(key.DataSource);
                    else
                        retirementTask = retiring;
                }

                if (retirementTask is null)
                {
                    if (s_pools.TryGetValue(key, out CSharpDbConnectionPool? existing))
                        return existing;

                    KeyValuePair<PoolKey, CSharpDbConnectionPool>[] incompatiblePools = s_pools
                        .Where(pair =>
                            s_pathComparer.Equals(pair.Key.DataSource, key.DataSource) &&
                            !pair.Key.Equals(key))
                        .ToArray();

                    foreach ((PoolKey incompatibleKey, CSharpDbConnectionPool incompatiblePool) in incompatiblePools)
                    {
                        bool disabled;
                        try
                        {
                            disabled = await incompatiblePool.TryDisableIfIdleAsync();
                        }
                        catch
                        {
                            s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(
                                incompatibleKey,
                                incompatiblePool));
                            RegisterRetirement(incompatibleKey.DataSource, incompatiblePool.Retirement);
                            throw;
                        }

                        if (!disabled)
                        {
                            throw new InvalidOperationException(
                                "Cannot change pooled embedded database configuration while connections for the same data source are open.");
                        }

                        s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(
                            incompatibleKey,
                            incompatiblePool));
                    }

                    var created = new CSharpDbConnectionPool(key, key.MaxPoolSize, openDatabaseAsync);
                    if (!s_pools.TryAdd(key, created))
                        return s_pools[key];

                    return created;
                }
            }
            finally
            {
                s_gate.Release();
            }

            await retirementTask.WaitAsync(cancellationToken);
        }
    }

    internal static async ValueTask<DirectDatabaseSession> OpenDirectSessionAsync(
        string dataSource,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        ArgumentNullException.ThrowIfNull(openDatabaseAsync);

        await ReserveDirectLeaseAsync(dataSource, cancellationToken);
        try
        {
            Database database = await openDatabaseAsync(cancellationToken);
            return new DirectDatabaseSession(
                database,
                directDatabase => DisposeDirectDatabaseAsync(dataSource, directDatabase));
        }
        catch
        {
            await ReleaseDirectLeaseAsync(dataSource);
            throw;
        }
    }

    internal static async ValueTask ClearPoolAsync(PoolKey key)
    {
        await s_gate.WaitAsync();
        try
        {
            if (s_pools.TryRemove(key, out CSharpDbConnectionPool? pool))
            {
                try
                {
                    await pool.DisableAsync();
                }
                finally
                {
                    RegisterRetirement(key.DataSource, pool.Retirement);
                }
            }
        }
        finally
        {
            s_gate.Release();
        }
    }

    internal static async ValueTask ClearPoolsAsync(Func<PoolKey, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        await s_gate.WaitAsync();
        List<Exception>? errors = null;
        try
        {
            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] matches = s_pools
                .Where(pair => predicate(pair.Key))
                .ToArray();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in matches)
            {
                if (s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool)))
                {
                    try
                    {
                        await pool.DisableAsync();
                    }
                    catch (Exception exception)
                    {
                        (errors ??= []).Add(exception);
                    }
                    finally
                    {
                        RegisterRetirement(key.DataSource, pool.Retirement);
                    }
                }
            }
        }
        finally
        {
            s_gate.Release();
        }

        ThrowDisableErrors(errors);
    }

    internal static async ValueTask ClearAllAsync()
    {
        await s_gate.WaitAsync();
        List<Exception>? errors = null;
        try
        {
            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] entries = s_pools.ToArray();
            s_pools.Clear();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in entries)
            {
                try
                {
                    await pool.DisableAsync();
                }
                catch (Exception exception)
                {
                    (errors ??= []).Add(exception);
                }
                finally
                {
                    RegisterRetirement(key.DataSource, pool.Retirement);
                }
            }
        }
        finally
        {
            s_gate.Release();
        }

        ThrowDisableErrors(errors);
    }

    internal static int GetPoolCountForTest() => s_pools.Count;

    internal static int GetIdleCountForTest(PoolKey key)
    {
        return s_pools.TryGetValue(key, out CSharpDbConnectionPool? pool)
            ? pool.IdleCount
            : 0;
    }

    private static async ValueTask ReserveDirectLeaseAsync(
        string dataSource,
        CancellationToken cancellationToken)
    {
        await s_gate.WaitAsync(cancellationToken);
        try
        {
            if (s_retiringPools.TryGetValue(dataSource, out Task? retiring))
            {
                if (!retiring.IsCompleted)
                {
                    throw new InvalidOperationException(
                        "Cannot open a non-pooled embedded connection while pooled connections for the same data source are still active.");
                }

                await retiring.WaitAsync(cancellationToken);
                s_retiringPools.Remove(dataSource);
            }

            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] matchingPools = s_pools
                .Where(pair => s_pathComparer.Equals(pair.Key.DataSource, dataSource))
                .ToArray();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in matchingPools)
            {
                bool disabled;
                try
                {
                    disabled = await pool.TryDisableIfIdleAsync();
                }
                catch
                {
                    s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool));
                    RegisterRetirement(key.DataSource, pool.Retirement);
                    throw;
                }

                if (!disabled)
                {
                    throw new InvalidOperationException(
                        "Cannot open a non-pooled embedded connection while pooled connections for the same data source are open.");
                }

                s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool));
            }

            s_directLeaseCounts.TryGetValue(dataSource, out int leaseCount);
            s_directLeaseCounts[dataSource] = checked(leaseCount + 1);
        }
        finally
        {
            s_gate.Release();
        }
    }

    private static async ValueTask DisposeDirectDatabaseAsync(
        string dataSource,
        Database database)
    {
        // Keep the path leased if physical disposal fails: the database may
        // still own file/WAL handles, so allowing a pooled engine to open would
        // reintroduce the mixed-ownership corruption risk this coordinator prevents.
        await database.DisposeAsync();
        await ReleaseDirectLeaseAsync(dataSource);
    }

    private static async ValueTask ReleaseDirectLeaseAsync(string dataSource)
    {
        await s_gate.WaitAsync();
        try
        {
            if (!s_directLeaseCounts.TryGetValue(dataSource, out int leaseCount))
                return;

            if (leaseCount <= 1)
                s_directLeaseCounts.Remove(dataSource);
            else
                s_directLeaseCounts[dataSource] = leaseCount - 1;
        }
        finally
        {
            s_gate.Release();
        }
    }

    internal static async ValueTask EvictDisabledPoolAsync(CSharpDbConnectionPool pool)
    {
        ArgumentNullException.ThrowIfNull(pool);

        await s_gate.WaitAsync();
        try
        {
            PoolKey key = pool.Key;
            if (s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool)))
                RegisterRetirement(key.DataSource, pool.Retirement);
        }
        finally
        {
            s_gate.Release();
        }
    }

    private static void ThrowDisableErrors(List<Exception>? errors)
    {
        if (errors is { Count: > 0 })
        {
            throw new AggregateException(
                "One or more embedded connection pools failed to close.",
                errors);
        }
    }

    private static void RegisterRetirement(string dataSource, Task retirement)
    {
        if (retirement.IsCompletedSuccessfully)
            return;

        if (s_retiringPools.TryGetValue(dataSource, out Task? existing))
            s_retiringPools[dataSource] = Task.WhenAll(existing, retirement);
        else
            s_retiringPools.Add(dataSource, retirement);
    }
}

internal readonly record struct PoolKey(
    string DataSource,
    int MaxPoolSize,
    CSharpDbEmbeddedOpenMode EffectiveOpenMode,
    CSharpDbStoragePreset? EffectiveStoragePreset,
    bool EffectiveAdaptiveQueryReoptimization,
    object? ExplicitDirectDatabaseOptions,
    object? ExplicitHybridDatabaseOptions);

/// <summary>
/// Owns one warm embedded engine for a pool key and multiplexes logical ADO.NET
/// sessions over it. A logical close resets only session-scoped state; disabling
/// the pool performs the physical database close.
/// </summary>
internal sealed class CSharpDbConnectionPool
{
    private const string BusyMessage = "Database is busy with an active transaction.";
    private const string SchemaBusyMessage =
        "Database schema is busy with an active transaction or snapshot reader.";
    private const string PoisonedMessage =
        "The pooled database is unavailable because a prior session could not be reset safely.";

    private readonly Func<CancellationToken, ValueTask<Database>> _openDatabaseAsync;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SemaphoreSlim _sessionSlots;
    private readonly TaskCompletionSource _retirement =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Dictionary<long, HashSet<Database.ReaderSession>> _readerSessions = new();

    private readonly PoolKey _key;
    private Database? _database;
    private bool _disabled;
    private bool _poisoned;
    private bool _retirementStarted;
    private int _activeSessionCount;
    private long _nextSessionId;
    private long? _transactionOwnerSessionId;
    private IReadOnlyDictionary<string, long>? _transactionSnapshotRowCounts;
    private bool _transactionSchemaMutated;

    internal CSharpDbConnectionPool(
        PoolKey key,
        int maxPoolSize,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync)
    {
        _key = key;
        _sessionSlots = new SemaphoreSlim(maxPoolSize, maxPoolSize);
        _openDatabaseAsync = openDatabaseAsync;
    }

    internal PoolKey Key => _key;
    internal int ActiveSessionCount => Volatile.Read(ref _activeSessionCount);
    internal Task Retirement => _retirement.Task;
    internal int ActiveSnapshotReaderCountForTest => _database?.ActiveReaderCount ?? 0;

    internal int IdleCount =>
        !_disabled && _database is not null && ActiveSessionCount == 0 ? 1 : 0;

    internal async ValueTask<PooledDatabaseSession> OpenSessionAsync(
        CancellationToken cancellationToken)
    {
        await _sessionSlots.WaitAsync(cancellationToken);
        bool sessionCreated = false;
        try
        {
            await _gate.WaitAsync(cancellationToken);
            try
            {
                if (_disabled)
                    throw new CSharpDbConnectionPoolRetiredException();

                _database ??= await _openDatabaseAsync(cancellationToken);
                long sessionId = ++_nextSessionId;
                Interlocked.Increment(ref _activeSessionCount);
                sessionCreated = true;
                return new PooledDatabaseSession(this, sessionId);
            }
            finally
            {
                _gate.Release();
            }
        }
        finally
        {
            if (!sessionCreated)
                _sessionSlots.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string sql,
        CancellationToken cancellationToken)
    {
        SqlStatementClassification classification = SqlStatementClassifier.Classify(sql);
        return classification.IsReadOnly
            ? ExecuteReadAsync(sessionId, classification.Statement, cancellationToken)
            : ExecuteWriteAsync(
                sessionId,
                classification.Statement,
                database => database.ExecuteAsync(classification.Statement, cancellationToken),
                cancellationToken);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(statement);
        return SqlStatementClassifier.IsReadOnly(statement)
            ? ExecuteReadAsync(sessionId, statement, cancellationToken)
            : ExecuteWriteAsync(
                sessionId,
                statement,
                database => database.ExecuteAsync(statement, cancellationToken),
                cancellationToken);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        CancellationToken cancellationToken)
        => ExecuteWriteAsync(
            sessionId,
            statement: null,
            database => database.ExecuteAsync(insert, cancellationToken),
            cancellationToken);

    internal async ValueTask BeginTransactionAsync(
        long sessionId,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            if (_transactionOwnerSessionId == sessionId)
                throw new InvalidOperationException("A transaction is already active.");
            if (_transactionOwnerSessionId.HasValue)
                throw new InvalidOperationException(BusyMessage);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            IReadOnlyDictionary<string, long> snapshotRowCounts =
                database.CaptureReaderSnapshotRowCounts();
            await database.BeginTransactionAsync(cancellationToken);
            _transactionOwnerSessionId = sessionId;
            _transactionSnapshotRowCounts = snapshotRowCounts;
            _transactionSchemaMutated = false;
        }
        finally
        {
            _gate.Release();
        }
    }

    internal ValueTask CommitAsync(long sessionId, CancellationToken cancellationToken)
        => CompleteTransactionAsync(sessionId, commit: true, cancellationToken);

    internal ValueTask RollbackAsync(long sessionId, CancellationToken cancellationToken)
        => CompleteTransactionAsync(sessionId, commit: false, cancellationToken);

    internal async ValueTask SaveToFileAsync(
        long sessionId,
        string filePath,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);
            await GetDatabase().SaveToFileAsync(filePath, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<string> GetTableNames(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetTableNames().ToArray());

    internal TableSchema? GetTableSchema(long sessionId, string tableName)
        => ExecuteIntrospection(sessionId, database => database.GetTableSchema(tableName));

    internal IReadOnlyCollection<IndexSchema> GetIndexes(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetIndexes().ToArray());

    internal IReadOnlyCollection<string> GetViewNames(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetViewNames().ToArray());

    internal string? GetViewSql(long sessionId, string viewName)
        => ExecuteIntrospection(sessionId, database => database.GetViewSql(viewName));

    internal IReadOnlyCollection<TriggerSchema> GetTriggers(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetTriggers().ToArray());

    internal async ValueTask ReleaseSessionAsync(long sessionId)
    {
        Database? databaseToDispose = null;
        Exception? resetException = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;
        bool evictPool;

        await _gate.WaitAsync();
        try
        {
            if (_transactionOwnerSessionId == sessionId)
            {
                try
                {
                    Database database = GetDatabase();
                    using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
                    await database.RollbackAsync();
                }
                catch (Exception exception)
                {
                    // Never reuse an engine whose transaction state could not be reset.
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }

                _transactionOwnerSessionId = null;
                _transactionSnapshotRowCounts = null;
                _transactionSchemaMutated = false;
            }

            if (_database is not null)
            {
                try
                {
                    DisposeReaderSessions(sessionId);
                    using var temporaryScope = _database.EnterTemporaryTableSessionScope(sessionId);
                    await _database.ClearTemporaryTablesAsync();
                }
                catch (Exception exception)
                {
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }
            }

            if (ActiveSessionCount > 0)
                Interlocked.Decrement(ref _activeSessionCount);

            if (_disabled && ActiveSessionCount == 0)
            {
                startRetirement = TryStartRetirement(out databaseToDispose);
                if (!startRetirement)
                    retirementToAwait = Retirement;
            }

            evictPool = _disabled;
        }
        finally
        {
            _gate.Release();
        }

        try
        {
            if (startRetirement)
                await DisposeRetiredDatabaseAsync(databaseToDispose);
            else if (retirementToAwait is not null)
                await retirementToAwait;
        }
        finally
        {
            _sessionSlots.Release();
            if (evictPool)
                await CSharpDbConnectionPoolRegistry.EvictDisabledPoolAsync(this);
        }

        if (resetException is not null)
            throw new InvalidOperationException(
                "Failed to reset the pooled database session.",
                resetException);
    }

    internal async ValueTask DisableAsync()
    {
        Database? databaseToDispose = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;

        await _gate.WaitAsync();
        try
        {
            _disabled = true;
            if (ActiveSessionCount == 0)
            {
                startRetirement = TryStartRetirement(out databaseToDispose);
                if (!startRetirement)
                    retirementToAwait = Retirement;
            }
        }
        finally
        {
            _gate.Release();
        }

        if (startRetirement)
            await DisposeRetiredDatabaseAsync(databaseToDispose);
        else if (retirementToAwait is not null)
            await retirementToAwait;
    }

    internal async ValueTask<bool> TryDisableIfIdleAsync()
    {
        Database? databaseToDispose = null;
        bool startRetirement;
        Task retirementToAwait;

        await _gate.WaitAsync();
        try
        {
            if (ActiveSessionCount > 0)
                return false;

            _disabled = true;
            startRetirement = TryStartRetirement(out databaseToDispose);
            retirementToAwait = Retirement;
        }
        finally
        {
            _gate.Release();
        }

        if (startRetirement)
            await DisposeRetiredDatabaseAsync(databaseToDispose);
        else
            await retirementToAwait;

        return true;
    }

    private bool TryStartRetirement(out Database? database)
    {
        if (_retirementStarted)
        {
            database = null;
            return false;
        }

        _retirementStarted = true;
        database = _database;
        _database = null;
        return true;
    }

    private async ValueTask DisposeRetiredDatabaseAsync(Database? database)
    {
        try
        {
            if (database is not null)
                await database.DisposeAsync();

            _retirement.TrySetResult();
        }
        catch (Exception exception)
        {
            _retirement.TrySetException(exception);
            throw;
        }
    }

    private async ValueTask<QueryResult> ExecuteReadAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            if (_transactionOwnerSessionId.HasValue &&
                _transactionOwnerSessionId.Value != sessionId &&
                _transactionSchemaMutated)
            {
                throw new InvalidOperationException(SchemaBusyMessage);
            }

            if (_transactionOwnerSessionId == sessionId)
            {
                QueryResult liveResult = await database.ExecuteAsync(statement, cancellationToken);
                return await DetachQueryResultAsync(liveResult, cancellationToken);
            }

            if (database.HasTemporaryTablesForCurrentSession)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);

                QueryResult temporaryResult = await database.ExecuteAsync(statement, cancellationToken);
                return await DetachQueryResultAsync(temporaryResult, cancellationToken);
            }

            Database.ReaderSession readerSession = _transactionSnapshotRowCounts is null
                ? database.CreateReaderSession()
                : database.CreateReaderSession(
                    _transactionSnapshotRowCounts,
                    allowCurrentCatalogRowCounts: false);
            TrackReaderSession(sessionId, readerSession);
            try
            {
                QueryResult result = await readerSession.ExecuteReadAsync(statement, cancellationToken);
                result.AppendDisposeCallback(
                    () => ReleaseReaderSessionAsync(sessionId, readerSession));
                return result;
            }
            catch
            {
                UntrackReaderSession(sessionId, readerSession);
                readerSession.Dispose();
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async ValueTask<QueryResult> ExecuteWriteAsync(
        long sessionId,
        Statement? statement,
        Func<Database, ValueTask<QueryResult>> executeAsync,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            bool persistentSchemaMutation = statement is not null &&
                IsPersistentSchemaMutation(statement);
            if (persistentSchemaMutation)
            {
                if (_readerSessions.Count > 0)
                    throw new InvalidOperationException(SchemaBusyMessage);

                // Set this before execution. A failed DDL statement can leave the
                // live catalog changed until the explicit transaction is rolled back.
                if (_transactionOwnerSessionId == sessionId)
                    _transactionSchemaMutated = true;
            }

            QueryResult result = await executeAsync(database);
            return await DetachQueryResultAsync(result, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async ValueTask CompleteTransactionAsync(
        long sessionId,
        bool commit,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            if (_transactionOwnerSessionId != sessionId)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);
                throw new InvalidOperationException("No active transaction.");
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            try
            {
                if (commit)
                    await database.CommitAsync(cancellationToken);
                else
                    await database.RollbackAsync(cancellationToken);
            }
            catch
            {
                // The engine may have failed before or after changing its own
                // transaction state. Stop every logical session from using it
                // until the owner closes and retirement performs final cleanup.
                _disabled = true;
                _poisoned = true;
                throw;
            }

            _transactionOwnerSessionId = null;
            _transactionSnapshotRowCounts = null;
            _transactionSchemaMutated = false;
        }
        finally
        {
            _gate.Release();
        }
    }

    private TResult ExecuteIntrospection<TResult>(
        long sessionId,
        Func<Database, TResult> action)
    {
        _gate.Wait();
        try
        {
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);
            return action(GetDatabase());
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ThrowIfUnavailable()
    {
        if (_poisoned)
            throw new InvalidOperationException(PoisonedMessage);

        if (_database is null)
            throw new InvalidOperationException("The pooled database is not available.");
    }

    private static bool IsPersistentSchemaMutation(Statement statement)
    {
        return statement switch
        {
            CreateTableStatement { IsTemporary: false } => true,
            CreateExternalTableStatement => true,
            DropTableStatement { IsTemporary: false } => true,
            PersistTempTableStatement => true,
            DropExternalTableStatement => true,
            AlterTableStatement => true,
            CreateIndexStatement => true,
            DropIndexStatement => true,
            CreateViewStatement => true,
            DropViewStatement => true,
            CreateTriggerStatement => true,
            DropTriggerStatement => true,
            CreateValidationRuleStatement => true,
            ConditionalStatement conditional =>
                conditional.Body.Any(IsPersistentSchemaMutation),
            _ => false,
        };
    }

    private void ThrowIfOwnedByOtherSession(long sessionId)
    {
        if (_transactionOwnerSessionId.HasValue && _transactionOwnerSessionId.Value != sessionId)
            throw new InvalidOperationException(BusyMessage);
    }

    private Database GetDatabase()
        => _database ?? throw new InvalidOperationException("The pooled database is not available.");

    private void TrackReaderSession(long sessionId, Database.ReaderSession readerSession)
    {
        if (!_readerSessions.TryGetValue(sessionId, out HashSet<Database.ReaderSession>? readers))
        {
            readers = new HashSet<Database.ReaderSession>();
            _readerSessions.Add(sessionId, readers);
        }

        readers.Add(readerSession);
    }

    private void UntrackReaderSession(long sessionId, Database.ReaderSession readerSession)
    {
        if (!_readerSessions.TryGetValue(sessionId, out HashSet<Database.ReaderSession>? readers))
            return;

        readers.Remove(readerSession);
        if (readers.Count == 0)
            _readerSessions.Remove(sessionId);
    }

    private void DisposeReaderSessions(long sessionId)
    {
        if (!_readerSessions.Remove(
                sessionId,
                out HashSet<Database.ReaderSession>? readers))
        {
            return;
        }

        foreach (Database.ReaderSession reader in readers)
            reader.Dispose();
    }

    private async ValueTask ReleaseReaderSessionAsync(
        long sessionId,
        Database.ReaderSession readerSession)
    {
        await _gate.WaitAsync();
        try
        {
            if (!_readerSessions.TryGetValue(
                    sessionId,
                    out HashSet<Database.ReaderSession>? readers) ||
                !readers.Remove(readerSession))
            {
                return;
            }

            if (readers.Count == 0)
                _readerSessions.Remove(sessionId);

            readerSession.Dispose();
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async ValueTask<QueryResult> DetachQueryResultAsync(
        QueryResult result,
        CancellationToken cancellationToken)
    {
        if (!result.IsQuery)
            return result;

        await using (result)
        {
            List<DbValue[]> rows = await result.ToListAsync(cancellationToken);
            return QueryResult.FromMaterializedRows(result.Schema, rows);
        }
    }
}

internal sealed class CSharpDbConnectionPoolRetiredException : InvalidOperationException
{
    internal CSharpDbConnectionPoolRetiredException()
        : base("The connection pool is no longer accepting new sessions.")
    {
    }
}

internal sealed class PooledDatabaseSession : ICSharpDbSession
{
    private CSharpDbConnectionPool? _pool;
    private readonly CSharpDbConnectionPool _ownerPool;
    private readonly long _sessionId;

    internal PooledDatabaseSession(CSharpDbConnectionPool pool, long sessionId)
    {
        _pool = pool;
        _ownerPool = pool;
        _sessionId = sessionId;
    }

    public bool SupportsStructuredExecution => true;
    internal int ActiveSnapshotReaderCountForTest =>
        _ownerPool.ActiveSnapshotReaderCountForTest;

    public ValueTask<QueryResult> ExecuteAsync(
        string sql,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, insert, cancellationToken);

    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
        => GetPool().BeginTransactionAsync(_sessionId, cancellationToken);

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => GetPool().CommitAsync(_sessionId, cancellationToken);

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => GetPool().RollbackAsync(_sessionId, cancellationToken);

    public ValueTask SaveToFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
        => GetPool().SaveToFileAsync(_sessionId, filePath, cancellationToken);

    public IReadOnlyCollection<string> GetTableNames() => GetPool().GetTableNames(_sessionId);
    public TableSchema? GetTableSchema(string tableName) => GetPool().GetTableSchema(_sessionId, tableName);
    public IReadOnlyCollection<IndexSchema> GetIndexes() => GetPool().GetIndexes(_sessionId);
    public IReadOnlyCollection<string> GetViewNames() => GetPool().GetViewNames(_sessionId);
    public string? GetViewSql(string viewName) => GetPool().GetViewSql(_sessionId, viewName);
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => GetPool().GetTriggers(_sessionId);

    public async ValueTask DisposeAsync()
    {
        CSharpDbConnectionPool? pool = _pool;
        _pool = null;

        if (pool is not null)
            await pool.ReleaseSessionAsync(_sessionId);
    }

    private CSharpDbConnectionPool GetPool()
        => _pool ?? throw new InvalidOperationException("Session is closed.");
}
