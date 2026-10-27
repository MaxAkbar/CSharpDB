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
    private readonly ulong _initialRowVersionHighWater;
    private readonly LifecycleOperation? _lifecycleOperation;
    private bool _completed;
    private bool _writeFailed;
    private object? _temporaryTableSessionKey;

    internal WriteTransaction(
        Database database,
        PagerWriteTransaction storageTransaction,
        SchemaCatalog catalog,
        QueryPlanner planner,
        long initialSchemaVersion,
        ulong initialRowVersionHighWater,
        LifecycleOperation? lifecycleOperation)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _storageTransaction = storageTransaction ?? throw new ArgumentNullException(nameof(storageTransaction));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
        _initialSchemaVersion = initialSchemaVersion;
        _initialRowVersionHighWater = initialRowVersionHighWater;
        _lifecycleOperation = lifecycleOperation;
    }

    // Client sessions share the database owner but have private temporary tables.
    // Deferred query iteration must enter this scope along with the pager binding.
    internal void SetTemporaryTableSession(object sessionKey)
        => _temporaryTableSessionKey = sessionKey;

    private IDisposable BindExecution()
        => BindExecution(_storageTransaction.Bind());

    private IDisposable BindSnapshotExecution()
        => BindExecution(_storageTransaction.BindSnapshotRead());

    private IDisposable BindExecution(IDisposable binding)
    {
        if (_temporaryTableSessionKey is null)
            return binding;
        try
        {
            return new SessionBinding(binding,
                _database.EnterTemporaryTableSessionScope(_temporaryTableSessionKey));
        }
        catch
        {
            binding.Dispose();
            throw;
        }
    }

    private sealed class SessionBinding(IDisposable binding, IDisposable sessionScope) : IDisposable
    {
        public void Dispose()
        {
            try { sessionScope.Dispose(); }
            finally { binding.Dispose(); }
        }
    }

    internal TableSchema? GetTableSchema(string tableName)
    {
        EnsureActive();
        using var scope = BindExecution();
        TableSchema? schema = _catalog.GetTable(tableName);
        if (schema is null)
            return null;
        long nextRowId = schema.NextRowId;
        foreach (var hint in _planner.GetCommittedNextRowIdHints())
            if (string.Equals(hint.Key, tableName, StringComparison.OrdinalIgnoreCase))
                nextRowId = Math.Max(nextRowId, hint.Value);
        return new TableSchema
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = schema.Columns,
            ForeignKeys = schema.ForeignKeys,
            CheckConstraints = schema.CheckConstraints,
            KeyConstraints = schema.KeyConstraints,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = nextRowId,
        };
    }

    internal IReadOnlyCollection<IndexSchema> GetIndexes() => _catalog.GetIndexes();
    internal IReadOnlyCollection<TriggerSchema> GetTriggers() => _catalog.GetTriggers();
    internal bool HasUpdateHostCallbacks(string tableName)
        => _database.HasUpdateHostCallbacks(GetTableSchema(tableName));

    internal async ValueTask ApplyTableSchemaIdentitiesAsync(
        string tableName, TableSchema identitySource, CancellationToken ct)
    {
        EnsureActive();
        EnsureWriteAllowed();
        try
        {
            using var scope = BindExecution();
            await _storageTransaction.AcquireSchemaWriteLockAsync(ct);
            await _catalog.ApplyTableSchemaIdentitiesAsync(tableName, identitySource, ct);
        }
        catch
        {
            _writeFailed = true;
            throw;
        }
    }

    /// <summary>
    /// Execute SQL within this transaction.
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken ct = default)
    {
        IQueryExecutionObservation? operation =
            _database.StartQueryObservability(sql);
        return operation is null
            ? ExecuteSqlCoreAsync(sql, ct)
            : Database.ObserveQueryAsync(
                operation,
                (Target: this, Sql: sql, CancellationToken: ct),
                static state => state.Target.ExecuteSqlCoreAsync(
                    state.Sql,
                    state.CancellationToken));
    }

    private async ValueTask<QueryResult> ExecuteSqlCoreAsync(string sql, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        EnsureActive();

        Statement statement = Parser.TryParseSimpleSelect(sql, out var simpleSelect)
            ? simpleSelect
            : Parser.Parse(sql);
        return await ExecuteAsyncCore(statement, BindExecution, ct);
    }

    /// <summary>
    /// Execute a parsed statement within this transaction.
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
    {
        IQueryExecutionObservation? operation =
            _database.StartQueryObservability(sql: null);
        return operation is null
            ? ExecuteStatementRootCoreAsync(statement, ct)
            : Database.ObserveQueryAsync(
                operation,
                (Target: this, Statement: statement, CancellationToken: ct),
                static state => state.Target.ExecuteStatementRootCoreAsync(
                    state.Statement,
                    state.CancellationToken));
    }

    private async ValueTask<QueryResult> ExecuteStatementRootCoreAsync(
        Statement statement,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        return await ExecuteAsyncCore(statement, BindExecution, ct);
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
    public ValueTask<QueryResult> ExecuteSnapshotReadAsync(string sql, CancellationToken ct = default)
    {
        IQueryExecutionObservation? operation =
            _database.StartQueryObservability(sql);
        return operation is null
            ? ExecuteSnapshotReadSqlCoreAsync(sql, ct)
            : Database.ObserveQueryAsync(
                operation,
                (Target: this, Sql: sql, CancellationToken: ct),
                static state => state.Target.ExecuteSnapshotReadSqlCoreAsync(
                    state.Sql,
                    state.CancellationToken));
    }

    private async ValueTask<QueryResult> ExecuteSnapshotReadSqlCoreAsync(
        string sql,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        EnsureActive();

        SqlStatementClassification classification = SqlStatementClassifier.Classify(sql);
        if (!classification.IsReadOnly)
            throw new InvalidOperationException("Snapshot-read execution only supports read-only statements.");

        return await ExecuteAsyncCore(classification.Statement, BindSnapshotExecution, ct);
    }

    /// <summary>
    /// Execute a read-only query within this transaction without contributing logical read conflict ranges.
    /// This weakens serializable conflict tracking for the specific query only.
    /// </summary>
    public ValueTask<QueryResult> ExecuteSnapshotReadAsync(Statement statement, CancellationToken ct = default)
    {
        IQueryExecutionObservation? operation =
            _database.StartQueryObservability(sql: null);
        return operation is null
            ? ExecuteSnapshotReadStatementCoreAsync(statement, ct)
            : Database.ObserveQueryAsync(
                operation,
                (Target: this, Statement: statement, CancellationToken: ct),
                static state => state.Target.ExecuteSnapshotReadStatementCoreAsync(
                    state.Statement,
                    state.CancellationToken));
    }

    private async ValueTask<QueryResult> ExecuteSnapshotReadStatementCoreAsync(
        Statement statement,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(statement);
        EnsureActive();

        if (!SqlStatementClassifier.IsReadOnly(statement))
            throw new InvalidOperationException("Snapshot-read execution only supports read-only statements.");

        return await ExecuteAsyncCore(statement, BindSnapshotExecution, ct);
    }

    /// <summary>
    /// Commit the transaction.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        EnsureActive();
        try
        {
            await CommitCoreAsync(ct);
            _lifecycleOperation?.Succeed();
        }
        catch (Exception exception)
        {
            if (_completed)
                _lifecycleOperation?.Fail(exception);
            throw;
        }
    }

    private async ValueTask CommitCoreAsync(CancellationToken ct)
    {
        if (_writeFailed)
        {
            await RollbackCoreAsync(CancellationToken.None);
            throw new CSharpDbException(
                ErrorCode.Unknown,
                "The transaction was rolled back because an earlier write failed.");
        }

        _database.BeginReaderCatalogPublication();
        bool synchronized = false;
        try
        {
            await CommitPublishingCoreAsync(ct);
            synchronized = true;
        }
        finally
        {
            // Native conflict validation rejects before appending any WAL frames.
            // Its isolated catalog never changed the shared catalog, so future
            // readers can still capture it once other publications finish.
            _database.EndReaderCatalogPublication(
                synchronized || _storageTransaction.State.CommitRejectedBeforePublication);
        }
    }

    private async ValueTask CommitPublishingCoreAsync(CancellationToken ct)
    {
        PagerCommitResult commit;
        KeyValuePair<string, long>[] committedNextRowIds;
        KeyValuePair<string, long>[] committedTableRowCountDeltas;
        TableStatistics[] committedTableStatistics;
        ColumnStatistics[] committedColumnStatistics;
        MutationTargetCollectionDiagnosticsSnapshot mutationTargetCollectionDiagnostics;
        bool schemaChanged;
        bool rootPagesChanged;
        bool advisoryCatalogContentChanged;
        bool rowVersionHighWaterChanged;
        try
        {
            using var binding = _storageTransaction.Bind();
            rootPagesChanged = await _catalog.PersistAllRootPageChangesAndDetectChangesAsync(ct);
            advisoryCatalogContentChanged = _catalog.HasAdvisoryCatalogContentChanges;
            committedNextRowIds = _planner.GetCommittedNextRowIdHints().ToArray();
            committedTableRowCountDeltas = _catalog.GetPendingTableRowCountDeltas().ToArray();
            committedTableStatistics = _catalog.GetDirtyTableStatistics().ToArray();
            committedColumnStatistics = _catalog.GetDirtyColumnStatistics().ToArray();
            var plannerMutationDiagnostics = _planner.GetMutationTargetCollectionDiagnosticsSnapshot();
            mutationTargetCollectionDiagnostics = new MutationTargetCollectionDiagnosticsSnapshot(
                plannerMutationDiagnostics.IndexedCollectionCount,
                plannerMutationDiagnostics.ScannedCollectionCount);
            if (advisoryCatalogContentChanged)
            {
                committedColumnStatistics = [];
                await _catalog.PersistDirtyTableStatisticsAsync(ct);
            }
            schemaChanged = _catalog.SchemaVersion != _initialSchemaVersion;
            rowVersionHighWaterChanged =
                _catalog.RowVersionHighWater != _initialRowVersionHighWater;
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
            reloadSharedCatalog:
                rootPagesChanged ||
                schemaChanged ||
                advisoryCatalogContentChanged ||
                rowVersionHighWaterChanged,
            schemaChanged,
            committedNextRowIds,
            committedTableRowCountDeltas,
            committedTableStatistics,
            committedColumnStatistics,
            mutationTargetCollectionDiagnostics,
            ct);
    }

    /// <summary>
    /// Roll back the transaction.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_completed)
            return;

        try
        {
            await RollbackCoreAsync(ct);
            _lifecycleOperation?.Succeed();
        }
        catch (Exception exception)
        {
            if (_completed)
                _lifecycleOperation?.Fail(exception);
            throw;
        }
    }

    private async ValueTask RollbackCoreAsync(CancellationToken ct)
    {
        using var binding = _storageTransaction.Bind();
        await _storageTransaction.RollbackAsync(ct);
        _completed = true;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!_completed)
                await RollbackAsync();
        }
        finally
        {
            // Catalog preparation can fail before commit begins. Always release
            // the storage transaction even when the logical transaction ended.
            await _storageTransaction.DisposeAsync();
        }
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
        bool isMutating = !SqlStatementClassifier.IsReadOnly(statement);
        if (isMutating)
            EnsureWriteAllowed();

        try
        {
            using var scope = executionScopeFactory();
            QueryResult result = await _planner.ExecuteAsync(statement, ct);
            if (result.IsQuery)
                result.SetExecutionScopeFactory(executionScopeFactory);
            return result;
        }
        catch
        {
            if (isMutating)
                _writeFailed = true;
            throw;
        }
    }

    private async ValueTask<QueryResult> ExecuteWriteAsyncCore(
        Statement statement,
        Func<IDisposable> executionScopeFactory,
        CancellationToken ct)
    {
        EnsureWriteAllowed();
        try
        {
            using var scope = executionScopeFactory();
            return statement is InsertStatement insert
                ? await _planner.ExecuteInsertAsync(insert, persistRootChanges: false, ct)
                : await _planner.ExecuteAsync(statement, ct);
        }
        catch
        {
            _writeFailed = true;
            throw;
        }
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertAsyncCore(
        SimpleInsertSql insert,
        Func<IDisposable> executionScopeFactory,
        CancellationToken ct)
    {
        EnsureWriteAllowed();
        try
        {
            using var scope = executionScopeFactory();
            return await _planner.ExecuteSimpleInsertAsync(insert, persistRootChanges: false, ct);
        }
        catch
        {
            _writeFailed = true;
            throw;
        }
    }

    private void EnsureWriteAllowed()
    {
        if (!_writeFailed)
            return;

        throw new CSharpDbException(
            ErrorCode.Unknown,
            "The transaction is aborted because an earlier write failed; roll it back before issuing another write.");
    }
}
