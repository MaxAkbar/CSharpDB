using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal interface ICSharpDbSession : IAsyncDisposable
{
    bool SupportsStructuredExecution { get; }
    CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot { get; }
    ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default);
    ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default);
    ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default);
    ValueTask CommitAsync(CancellationToken cancellationToken = default);
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);
    ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default);
    IReadOnlyCollection<string> GetTableNames();
    TableSchema? GetTableSchema(string tableName);
    IReadOnlyCollection<IndexSchema> GetIndexes();
    IReadOnlyCollection<string> GetViewNames();
    string? GetViewSql(string viewName);
    IReadOnlyCollection<TriggerSchema> GetTriggers();
}
