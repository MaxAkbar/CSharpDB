using System.Collections.Concurrent;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class SharedMemoryDatabaseRegistry
{
    private static readonly ConcurrentDictionary<string, SharedMemoryDatabaseHost> s_hosts = new(StringComparer.Ordinal);

    internal static async ValueTask<ICSharpDbSession> OpenSessionAsync(
        string name,
        string? loadFromPath,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (s_hosts.TryGetValue(name, out var existing))
                return await existing.OpenSessionAsync(loadFromPath, cancellationToken);

            var created = new SharedMemoryDatabaseHost(name);
            if (!s_hosts.TryAdd(name, created))
                continue;

            try
            {
                return await created.OpenSessionAsync(loadFromPath, cancellationToken);
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

    internal SharedMemoryDatabaseHost(string name)
    {
        _name = name;
    }

    internal async ValueTask<SharedMemoryDatabaseSession> OpenSessionAsync(
        string? loadFromPath,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_disabled)
                throw new InvalidOperationException("The shared in-memory database is no longer accepting new sessions.");

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

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string sql,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (OwnedByOtherSession(sessionId))
            {
                var statement = Parser.Parse(sql);
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                await using var query = await GetTransactionSnapshotDatabase().ExecuteAsync(statement, cancellationToken);
                return await DetachQueryResultAsync(query, cancellationToken);
            }

            await using var liveQuery = await GetDatabase().ExecuteAsync(sql, cancellationToken);
            return await DetachQueryResultAsync(liveQuery, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(statement);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (OwnedByOtherSession(sessionId))
            {
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                await using var query = await GetTransactionSnapshotDatabase().ExecuteAsync(statement, cancellationToken);
                return await DetachQueryResultAsync(query, cancellationToken);
            }

            await using var liveQuery = await GetDatabase().ExecuteAsync(statement, cancellationToken);
            return await DetachQueryResultAsync(liveQuery, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (OwnedByOtherSession(sessionId))
                throw new InvalidOperationException(BusyMessage);

            await using var query = await GetDatabase().ExecuteAsync(insert, cancellationToken);
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

            await GetDatabase().CommitAsync(cancellationToken);
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

            await GetDatabase().RollbackAsync(cancellationToken);
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
                    await GetDatabase().RollbackAsync();
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

    private async ValueTask EnsureInitializedAsync(string? normalizedLoadPath, CancellationToken cancellationToken)
    {
        if (_database is null)
        {
            _database = normalizedLoadPath is null
                ? await Database.OpenInMemoryAsync(cancellationToken)
                : await Database.LoadIntoMemoryAsync(normalizedLoadPath, cancellationToken);

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
        => statement is QueryStatement or WithStatement;

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

    internal SharedMemoryDatabaseSession(SharedMemoryDatabaseHost host, long sessionId)
    {
        _host = host;
        _sessionId = sessionId;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, insert, cancellationToken);

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
