using System.Collections.Concurrent;
using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class SharedMemoryDatabaseRegistry
{
    private static readonly ConcurrentDictionary<string, SharedMemoryDatabaseHost> s_hosts = new(StringComparer.Ordinal);

    internal static async ValueTask<ICSharpDbSession> OpenSessionAsync(
        string name,
        string? loadFromPath,
        DatabaseOptions? databaseOptions,
        DatabaseOptions runtimeDatabaseOptions,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (s_hosts.TryGetValue(name, out var existing))
            {
                return await existing.OpenSessionAsync(
                    loadFromPath,
                    databaseOptions,
                    cancellationToken);
            }

            var created = new SharedMemoryDatabaseHost(
                name,
                databaseOptions,
                runtimeDatabaseOptions);
            if (!s_hosts.TryAdd(name, created))
                continue;

            try
            {
                return await created.OpenSessionAsync(
                    loadFromPath,
                    databaseOptions,
                    cancellationToken);
            }
            catch
            {
                s_hosts.TryRemove(new KeyValuePair<string, SharedMemoryDatabaseHost>(name, created));
                await created.DisableAsync();
                throw;
            }
        }
    }

    internal static async ValueTask ClearAsync(string name)
    {
        if (s_hosts.TryRemove(name, out var host))
            await host.DisableAsync();
    }

    internal static async ValueTask ClearAllAsync()
    {
        var hosts = s_hosts.ToArray();
        s_hosts.Clear();

        foreach (var pair in hosts)
            await pair.Value.DisableAsync();
    }

    internal static int GetHostCountForTest() => s_hosts.Count;
}

internal sealed class SharedMemoryDatabaseHost
{
    private const string BusyMessage = "Database is busy with an active transaction.";

    private readonly string _name;
    private readonly DatabaseOptions? _databaseOptions;
    private readonly DatabaseOptions _runtimeDatabaseOptions;
    private readonly string? _observabilityConfiguration;
    private readonly CSharpDbObservabilityOptions? _observabilityOptionsSnapshot;
    private readonly bool _lifecycleLoggingEnabled;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private Database? _database;
    private bool _disabled;
    private int _activeSessionCount;
    private long _nextSessionId;
    private long? _transactionOwnerSessionId;
    private Database? _transactionSnapshotDatabase;
    private string? _transactionSnapshotPath;
    private bool _seedConfigured;
    private string? _seedSourcePath;

    internal SharedMemoryDatabaseHost(
        string name,
        DatabaseOptions? databaseOptions,
        DatabaseOptions runtimeDatabaseOptions)
    {
        _name = name;
        _databaseOptions = databaseOptions;
        _runtimeDatabaseOptions = runtimeDatabaseOptions ??
            throw new ArgumentNullException(nameof(runtimeDatabaseOptions));
        _observabilityConfiguration = SerializeObservabilityConfiguration(databaseOptions);
        _observabilityOptionsSnapshot = runtimeDatabaseOptions.ObservabilityOptions;
        _lifecycleLoggingEnabled =
            DataLifecycleDiagnosticBoundary.IsLifecycleLoggingEnabled(
                _observabilityOptionsSnapshot);
    }

    internal CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _observabilityOptionsSnapshot;

    internal async ValueTask<SharedMemoryDatabaseSession> OpenSessionAsync(
        string? loadFromPath,
        DatabaseOptions? databaseOptions,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_disabled)
                throw new InvalidOperationException("The shared in-memory database is no longer accepting new sessions.");
            if (!ReferenceEquals(_databaseOptions, databaseOptions) ||
                !string.Equals(
                    _observabilityConfiguration,
                    SerializeObservabilityConfiguration(databaseOptions),
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Shared in-memory database '{_name}' was already initialized with different DatabaseOptions.");
            }

            string? normalizedLoadPath = NormalizeOptionalPath(loadFromPath);
            await EnsureInitializedAsync(normalizedLoadPath, cancellationToken);

            _activeSessionCount++;
            long sessionId = ++_nextSessionId;
            return new SharedMemoryDatabaseSession(this, sessionId);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string sql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, sql, sql, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            executionSql,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(executionSql);

        using (observation?.MeasureQueueWait())
            await _gate.WaitAsync(cancellationToken);
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        try
        {
            if (OwnedByOtherSession(sessionId))
            {
                var statement = Parser.Parse(executionSql);
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                Database snapshotDatabase = GetTransactionSnapshotDatabase();
                QueryFingerprint? fingerprint =
                    QueryObservabilitySource.CreateFingerprint(snapshotDatabase, observabilitySql);
                observation?.MarkDispatchHandoff();
                await using var query = await snapshotDatabase.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                return await DetachQueryResultAsync(query, cancellationToken);
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            observation?.MarkDispatchHandoff();
            await using var liveQuery = await database.ExecuteAsync(
                executionSql,
                observabilitySql,
                cancellationToken);
            return await DetachQueryResultAsync(liveQuery, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, statement, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            statement,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(statement);

        using (observation?.MeasureQueueWait())
            await _gate.WaitAsync(cancellationToken);
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        try
        {
            if (OwnedByOtherSession(sessionId))
            {
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                Database snapshotDatabase = GetTransactionSnapshotDatabase();
                QueryFingerprint? fingerprint =
                    QueryObservabilitySource.CreateFingerprint(snapshotDatabase, observabilitySql);
                observation?.MarkDispatchHandoff();
                await using var query = await snapshotDatabase.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                return await DetachQueryResultAsync(query, cancellationToken);
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            QueryFingerprint? liveFingerprint =
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
            observation?.MarkDispatchHandoff();
            await using var liveQuery = await database.ExecuteAsync(
                statement,
                liveFingerprint,
                cancellationToken);
            return await DetachQueryResultAsync(liveQuery, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, insert, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            insert,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        using (observation?.MeasureQueueWait())
            await _gate.WaitAsync(cancellationToken);
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        try
        {
            if (OwnedByOtherSession(sessionId))
                throw new InvalidOperationException(BusyMessage);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            QueryFingerprint? fingerprint =
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
            observation?.MarkDispatchHandoff();
            await using var query = await database.ExecuteAsync(
                insert,
                fingerprint,
                cancellationToken);
            return new QueryResult(query.RowsAffected);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal async ValueTask BeginTransactionAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        Database? snapshotDatabase = null;
        string? snapshotPath = null;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_transactionOwnerSessionId == sessionId)
                throw new InvalidOperationException("A transaction is already active.");
            if (_transactionOwnerSessionId.HasValue)
                throw new InvalidOperationException(BusyMessage);

            var database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            snapshotPath = Path.Combine(Path.GetTempPath(), $"csharpdb_shared_snapshot_{Guid.NewGuid():N}.db");
            await database.SaveToFileAsync(snapshotPath, cancellationToken);
            snapshotDatabase = await Database.LoadIntoMemoryAsync(snapshotPath, cancellationToken);
            await database.BeginTransactionAsync(cancellationToken);
            _transactionOwnerSessionId = sessionId;
            _transactionSnapshotDatabase = snapshotDatabase;
            _transactionSnapshotPath = snapshotPath;
            snapshotDatabase = null;
            snapshotPath = null;
        }
        finally
        {
            _gate.Release();
        }

        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask CommitAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        Database? snapshotDatabase = null;
        string? snapshotPath = null;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_transactionOwnerSessionId != sessionId)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);
                throw new InvalidOperationException("No active transaction.");
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            await database.CommitAsync(cancellationToken);
            _transactionOwnerSessionId = null;
            snapshotDatabase = _transactionSnapshotDatabase;
            snapshotPath = _transactionSnapshotPath;
            _transactionSnapshotDatabase = null;
            _transactionSnapshotPath = null;
        }
        finally
        {
            _gate.Release();
        }

        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask RollbackAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        Database? snapshotDatabase = null;
        string? snapshotPath = null;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_transactionOwnerSessionId != sessionId)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);
                throw new InvalidOperationException("No active transaction.");
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            await database.RollbackAsync(cancellationToken);
            _transactionOwnerSessionId = null;
            snapshotDatabase = _transactionSnapshotDatabase;
            snapshotPath = _transactionSnapshotPath;
            _transactionSnapshotDatabase = null;
            _transactionSnapshotPath = null;
        }
        finally
        {
            _gate.Release();
        }

        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask SaveToFileAsync(long sessionId, string filePath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_transactionOwnerSessionId.HasValue)
                throw new InvalidOperationException("Cannot save while an explicit transaction is active.");

            await GetDatabase().SaveToFileAsync(filePath, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<string> GetTableNames(long sessionId)
    {
        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetTableNames().ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    internal TableSchema? GetTableSchema(long sessionId, string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetTableSchema(tableName);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<IndexSchema> GetIndexes(long sessionId)
    {
        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetIndexes().ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<string> GetViewNames(long sessionId)
    {
        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetViewNames().ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    internal string? GetViewSql(long sessionId, string viewName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetViewSql(viewName);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<TriggerSchema> GetTriggers(long sessionId)
    {
        _gate.Wait();
        try
        {
            ThrowIfBusyForIntrospection(sessionId);
            return GetDatabase().GetTriggers().ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    internal async ValueTask ReleaseSessionAsync(long sessionId)
    {
        Database? databaseToDispose = null;
        Database? snapshotDatabase = null;
        string? snapshotPath = null;

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
                catch
                {
                    // Best-effort rollback while tearing down a session.
                }

                _transactionOwnerSessionId = null;
                snapshotDatabase = _transactionSnapshotDatabase;
                snapshotPath = _transactionSnapshotPath;
                _transactionSnapshotDatabase = null;
                _transactionSnapshotPath = null;
            }

            if (_activeSessionCount > 0)
                _activeSessionCount--;

            if (_database is not null)
            {
                using var temporaryScope = _database.EnterTemporaryTableSessionScope(sessionId);
                await _database.ClearTemporaryTablesAsync();
            }

            if (_disabled && _activeSessionCount == 0)
            {
                databaseToDispose = _database;
                _database = null;
            }
        }
        finally
        {
            _gate.Release();
        }

        if (databaseToDispose is not null)
            await databaseToDispose.DisposeAsync();
        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask DisableAsync()
    {
        Database? databaseToDispose = null;
        Database? snapshotDatabase = null;
        string? snapshotPath = null;

        using IDisposable? lifecycleBoundary = EnterDatabaseCloseBoundary();
        await _gate.WaitAsync();
        try
        {
            _disabled = true;
            if (_activeSessionCount == 0)
            {
                databaseToDispose = _database;
                _database = null;
                snapshotDatabase = _transactionSnapshotDatabase;
                snapshotPath = _transactionSnapshotPath;
                _transactionSnapshotDatabase = null;
                _transactionSnapshotPath = null;
                _transactionOwnerSessionId = null;
            }
        }
        finally
        {
            _gate.Release();
        }

        if (databaseToDispose is not null)
            await databaseToDispose.DisposeAsync();
        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    private IDisposable? EnterDatabaseCloseBoundary()
    {
        if (!_lifecycleLoggingEnabled)
            return null;

        return DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
            OpaqueDiagnosticsId.Create());
    }

    private async ValueTask EnsureInitializedAsync(string? normalizedLoadPath, CancellationToken cancellationToken)
    {
        if (_database is null)
        {
            _database = normalizedLoadPath is null
                ? await Database.OpenInMemoryAsync(
                    _runtimeDatabaseOptions,
                    cancellationToken)
                : await Database.LoadIntoMemoryAsync(
                    normalizedLoadPath,
                    _runtimeDatabaseOptions,
                    cancellationToken);

            _seedConfigured = true;
            _seedSourcePath = normalizedLoadPath;
            return;
        }

        if (!_seedConfigured)
            return;

        if (normalizedLoadPath is null)
            return;

        if (!string.Equals(_seedSourcePath, normalizedLoadPath, GetSeedComparison()))
        {
            throw new InvalidOperationException(
                $"Shared in-memory database '{_name}' was already initialized with a different Load From source.");
        }
    }

    private Database GetDatabase()
        => _database ?? throw new InvalidOperationException("The shared in-memory database is not available.");

    private static string? SerializeObservabilityConfiguration(
        DatabaseOptions? databaseOptions)
    {
        CSharpDbObservabilityOptions? observability =
            databaseOptions?.ObservabilityOptions;
        return observability is null
            ? null
            : JsonSerializer.Serialize(
                observability,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
    }

    private bool OwnedByOtherSession(long sessionId)
        => _transactionOwnerSessionId.HasValue && _transactionOwnerSessionId.Value != sessionId;

    private Database GetTransactionSnapshotDatabase()
        => _transactionSnapshotDatabase ?? throw new InvalidOperationException("No committed snapshot is available for the active transaction.");

    private static async ValueTask DisposeSnapshotAsync(Database? snapshotDatabase, string? snapshotPath)
    {
        if (snapshotDatabase is not null)
            await snapshotDatabase.DisposeAsync();

        if (!string.IsNullOrWhiteSpace(snapshotPath))
        {
            try
            {
                if (File.Exists(snapshotPath))
                    File.Delete(snapshotPath);
            }
            catch
            {
                // Best-effort cleanup for temporary transaction snapshot files.
            }

            try
            {
                string walPath = snapshotPath + ".wal";
                if (File.Exists(walPath))
                    File.Delete(walPath);
            }
            catch
            {
                // Best-effort cleanup for temporary transaction snapshot files.
            }
        }
    }

    private void ThrowIfBusyForIntrospection(long sessionId)
    {
        if (OwnedByOtherSession(sessionId))
            throw new InvalidOperationException(BusyMessage);
    }

    private static bool IsReadOnly(Statement statement)
        => SqlStatementClassifier.IsReadOnly(statement);

    private static async ValueTask<QueryResult> DetachQueryResultAsync(QueryResult query, CancellationToken cancellationToken)
    {
        if (!query.IsQuery)
            return new QueryResult(query.RowsAffected);

        var rows = await query.ToListAsync(cancellationToken);
        return QueryResult.FromMaterializedRows(query.Schema, rows);
    }

    private static string? NormalizeOptionalPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        string fullPath = Path.GetFullPath(path);
        return OperatingSystem.IsWindows() ? fullPath.ToUpperInvariant() : fullPath;
    }

    private static StringComparison GetSeedComparison()
        => OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
}

internal sealed class SharedMemoryDatabaseSession : ICSharpDbSession
{
    private SharedMemoryDatabaseHost? _host;
    private readonly long _sessionId;

    public bool SupportsStructuredExecution => true;
    public CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        GetHost().ObservabilityOptionsSnapshot;

    internal SharedMemoryDatabaseSession(SharedMemoryDatabaseHost host, long sessionId)
    {
        _host = host;
        _sessionId = sessionId;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, executionSql, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            executionSql,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, statement, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            statement,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, insert, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, insert, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            insert,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
        => GetHost().BeginTransactionAsync(_sessionId, cancellationToken);

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => GetHost().CommitAsync(_sessionId, cancellationToken);

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => GetHost().RollbackAsync(_sessionId, cancellationToken);

    public ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default)
        => GetHost().SaveToFileAsync(_sessionId, filePath, cancellationToken);

    public IReadOnlyCollection<string> GetTableNames() => GetHost().GetTableNames(_sessionId);
    public TableSchema? GetTableSchema(string tableName) => GetHost().GetTableSchema(_sessionId, tableName);
    public IReadOnlyCollection<IndexSchema> GetIndexes() => GetHost().GetIndexes(_sessionId);
    public IReadOnlyCollection<string> GetViewNames() => GetHost().GetViewNames(_sessionId);
    public string? GetViewSql(string viewName) => GetHost().GetViewSql(_sessionId, viewName);
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => GetHost().GetTriggers(_sessionId);

    public async ValueTask DisposeAsync()
    {
        var host = _host;
        _host = null;

        if (host is not null)
            await host.ReleaseSessionAsync(_sessionId);
    }

    private SharedMemoryDatabaseHost GetHost()
        => _host ?? throw new InvalidOperationException("Session is closed.");
}
