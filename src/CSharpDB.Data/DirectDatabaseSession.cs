using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal sealed class DirectDatabaseSession : ICSharpDbSession
{
    private Database? _database;
    private readonly Func<Database, ValueTask>? _releaseAsync;

    internal DirectDatabaseSession(Database database, Func<Database, ValueTask>? releaseAsync = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _releaseAsync = releaseAsync;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => GetDatabase().ExecuteAsync(insert, cancellationToken);

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
