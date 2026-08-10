using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal sealed class DirectDatabaseSession : ICSharpDbSession
{
    private Database? _database;
    private readonly Func<Database, ValueTask>? _releaseAsync;
    private readonly CSharpDbObservabilityOptions? _observabilityOptionsSnapshot;

    public bool SupportsStructuredExecution => true;
    public CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _observabilityOptionsSnapshot;

    internal DirectDatabaseSession(
        Database database,
        Func<Database, ValueTask>? releaseAsync = null,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _releaseAsync = releaseAsync;
        _observabilityOptionsSnapshot = observabilityOptionsSnapshot;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            executionSql,
            observabilitySql,
            observation: null,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff();
        return database.ExecuteAsync(executionSql, observabilitySql, cancellationToken);
    }

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            statement,
            observabilitySql,
            observation: null,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        QueryFingerprint? fingerprint =
            QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff();
        return database.ExecuteAsync(statement, fingerprint, cancellationToken);
    }

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(insert, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            insert,
            observabilitySql,
            observation: null,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        QueryFingerprint? fingerprint =
            QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff();
        return database.ExecuteAsync(insert, fingerprint, cancellationToken);
    }

    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
        => GetDatabase().BeginTransactionAsync(cancellationToken);

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => GetDatabase().CommitAsync(cancellationToken);

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => GetDatabase().RollbackAsync(cancellationToken);

    public ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default)
        => GetDatabase().SaveToFileAsync(filePath, cancellationToken);

    public IReadOnlyCollection<string> GetTableNames() => GetDatabase().GetTableNames();
    public TableSchema? GetTableSchema(string tableName) => GetDatabase().GetTableSchema(tableName);
    public IReadOnlyCollection<IndexSchema> GetIndexes() => GetDatabase().GetIndexes();
    public IReadOnlyCollection<string> GetViewNames() => GetDatabase().GetViewNames();
    public string? GetViewSql(string viewName) => GetDatabase().GetViewSql(viewName);
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => GetDatabase().GetTriggers();

    public async ValueTask DisposeAsync()
    {
        var database = _database;
        _database = null;

        if (database is null)
            return;

        if (_releaseAsync is null)
            await database.DisposeAsync();
        else
            await _releaseAsync(database);
    }

    private Database GetDatabase()
        => _database ?? throw new InvalidOperationException("Session is closed.");
}
