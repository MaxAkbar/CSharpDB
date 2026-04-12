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

        Statement statement = Parser.TryParseSimpleSelect(sql, out var simpleSelect)
            ? simpleSelect
            : Parser.Parse(sql);
        return await ExecuteAsyncCore(statement, _storageTransaction.Bind, ct);
    }

    /// <summary>
    /// Execute a parsed statement within this transaction.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        return await ExecuteAsyncCore(statement, _storageTransaction.Bind, ct);
    }

    internal async ValueTask<QueryResult> ExecuteImplicitAutoCommitAsync(Statement statement, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        return await ExecuteWriteAsyncCore(statement, _storageTransaction.Bind, ct);
    }

    internal async ValueTask<QueryResult> ExecuteImplicitAutoCommitAsync(SimpleInsertSql insert, CancellationToken ct = default)
    {
        EnsureActive();

        return await ExecuteSimpleInsertAsyncCore(insert, _storageTransaction.Bind, ct);
    }

    /// <summary>
    /// Execute a read-only query within this transaction without contributing logical read conflict ranges.
    /// This weakens serializable conflict tracking for the specific query only.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteSnapshotReadAsync(string sql, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        EnsureActive();

        SqlStatementClassification classification = SqlStatementClassifier.Classify(sql);
        if (!classification.IsReadOnly)
            throw new InvalidOperationException("Snapshot-read execution only supports read-only statements.");

        return await ExecuteAsyncCore(classification.Statement, _storageTransaction.BindSnapshotRead, ct);
    }

    /// <summary>
    /// Execute a read-only query within this transaction without contributing logical read conflict ranges.
    /// This weakens serializable conflict tracking for the specific query only.
    /// </summary>
    public async ValueTask<QueryResult> ExecuteSnapshotReadAsync(Statement statement, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        if (!SqlStatementClassifier.IsReadOnly(statement))
            throw new InvalidOperationException("Snapshot-read execution only supports read-only statements.");

        return await ExecuteAsyncCore(statement, _storageTransaction.BindSnapshotRead, ct);
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
        bool advisoryCatalogContentChanged;
        try
        {
            using var binding = _storageTransaction.Bind();
            rootPagesChanged = await _catalog.PersistAllRootPageChangesAndDetectChangesAsync(ct);
            advisoryCatalogContentChanged = _catalog.HasAdvisoryCatalogContentChanges;
            committedNextRowIds = _planner.GetCommittedNextRowIdHints().ToArray();
            committedTableRowCountDeltas = _catalog.GetPendingTableRowCountDeltas().ToArray();
            committedTableStatistics = _catalog.GetDirtyTableStatistics().ToArray();
            committedColumnStatistics = _catalog.GetDirtyColumnStatistics().ToArray();
            if (advisoryCatalogContentChanged)
            {
                committedColumnStatistics = [];
                await _catalog.PersistDirtyTableStatisticsAsync(ct);
            }
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
            reloadSharedCatalog: rootPagesChanged || schemaChanged || advisoryCatalogContentChanged,
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

    private async ValueTask<QueryResult> ExecuteAsyncCore(
        Statement statement,
        Func<IDisposable> executionScopeFactory,
        CancellationToken ct)
    {
        using var scope = executionScopeFactory();
        QueryResult result = await _planner.ExecuteAsync(statement, ct);
        if (result.IsQuery)
            result.SetExecutionScopeFactory(executionScopeFactory);
        return result;
    }

    private async ValueTask<QueryResult> ExecuteWriteAsyncCore(
        Statement statement,
        Func<IDisposable> executionScopeFactory,
        CancellationToken ct)
    {
        using var scope = executionScopeFactory();
        return statement is InsertStatement insert
            ? await _planner.ExecuteInsertAsync(insert, persistRootChanges: false, ct)
            : await _planner.ExecuteAsync(statement, ct);
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertAsyncCore(
        SimpleInsertSql insert,
        Func<IDisposable> executionScopeFactory,
        CancellationToken ct)
    {
        using var scope = executionScopeFactory();
        return await _planner.ExecuteSimpleInsertAsync(insert, persistRootChanges: false, ct);
    }
}
