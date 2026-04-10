using System.Diagnostics.CodeAnalysis;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Transactions;

namespace CSharpDB.Engine;

/// <summary>
/// Explicit multi-writer transaction with isolated pager, catalog, and planner state.
/// </summary>
public sealed class WriteTransaction : IAsyncDisposable
{
    private readonly Database _database;
    private readonly PagerWriteTransaction _storageTransaction;
    private readonly SchemaCatalog _catalog;
    private readonly QueryPlanner _planner;
    private readonly long _initialSchemaVersion;
    private bool _completed;

    internal WriteTransaction(
        Database database,
        PagerWriteTransaction storageTransaction,
        SchemaCatalog catalog,
        QueryPlanner planner,
        long initialSchemaVersion)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _storageTransaction = storageTransaction ?? throw new ArgumentNullException(nameof(storageTransaction));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
        _initialSchemaVersion = initialSchemaVersion;
    }

    /// <summary>
    /// Execute SQL within this transaction.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        EnsureActive();

        using var binding = _storageTransaction.Bind();
        Statement statement = Parser.TryParseSimpleSelect(sql, out var simpleSelect)
            ? simpleSelect
            : Parser.Parse(sql);
        QueryResult result = await _planner.ExecuteAsync(statement, ct);
        if (result.IsQuery)
            result.SetExecutionScopeFactory(_storageTransaction.Bind);
        return result;
    }

    /// <summary>
    /// Execute a parsed statement within this transaction.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        using var binding = _storageTransaction.Bind();
        QueryResult result = await _planner.ExecuteAsync(statement, ct);
        if (result.IsQuery)
            result.SetExecutionScopeFactory(_storageTransaction.Bind);
        return result;
    }

    /// <summary>
    /// Commit the transaction.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        EnsureActive();

        PagerCommitResult commit;
        KeyValuePair<string, long>[] committedNextRowIds;
        KeyValuePair<string, long>[] committedTableRowCountDeltas;
        TableStatistics[] committedTableStatistics;
        ColumnStatistics[] committedColumnStatistics;
        bool schemaChanged;
        bool rootPagesChanged;
        try
        {
            using var binding = _storageTransaction.Bind();
            rootPagesChanged = await _catalog.PersistAllRootPageChangesAndDetectChangesAsync(ct);
            committedNextRowIds = _planner.GetCommittedNextRowIdHints().ToArray();
            committedTableRowCountDeltas = _catalog.GetPendingTableRowCountDeltas().ToArray();
            committedTableStatistics = _catalog.GetDirtyTableStatistics().ToArray();
            committedColumnStatistics = _catalog.GetDirtyColumnStatistics().ToArray();
            schemaChanged = _catalog.SchemaVersion != _initialSchemaVersion;
            commit = await _storageTransaction.BeginCommitAsync(ct);
        }
        catch
        {
            _completed = true;
            throw;
        }

        try
        {
            await commit.WaitAsync(ct);
            _completed = true;
        }
        catch
        {
            _completed = true;
            throw;
        }

        await _database.OnExternalWriteTransactionCommittedAsync(
            reloadSharedCatalog: rootPagesChanged || schemaChanged,
            schemaChanged,
            committedNextRowIds,
            committedTableRowCountDeltas,
            committedTableStatistics,
            committedColumnStatistics,
            ct);
    }

    /// <summary>
    /// Roll back the transaction.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_completed)
            return;

        using var binding = _storageTransaction.Bind();
        await _storageTransaction.RollbackAsync(ct);
        _completed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_completed)
            return;

        await RollbackAsync();
    }

    private void EnsureActive()
    {
        if (_completed)
            throw new InvalidOperationException("The transaction has already completed.");
    }
}
