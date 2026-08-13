using CSharpDB.Client;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal sealed class DirectDatabaseSession :
    ICSharpDbSession,
    IDataRuntimeDiagnosticsContributor,
    ICSharpDbDataMetricsProvider
{
    private const long DiagnosticsSessionKey = 1;
    private Database? _database;
    private readonly Func<Database, ValueTask>? _releaseAsync;
    private readonly CSharpDbObservabilityOptions? _observabilityOptionsSnapshot;
    private readonly DirectSessionDiagnostics? _diagnostics;
    private IDisposable? _metricsRegistration;

    public bool SupportsStructuredExecution => true;
    public CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _observabilityOptionsSnapshot;
    public CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _diagnostics?.RuntimeDiagnosticsState;
    public object RuntimeDiagnosticsIdentityKey => this;
    public IDataRuntimeDiagnosticsContributor RuntimeDiagnosticsContributor => this;
    public ICSharpDbObservabilityClient? RemoteObservabilityClient => null;
    internal bool IsRuntimeDiagnosticsEnabled => _diagnostics?.Tracker is not null;
    internal bool HasDiagnosticsSidecarForTest => _diagnostics is not null;

    internal DirectDatabaseSession(
        Database database,
        Func<Database, ValueTask>? releaseAsync = null,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot = null,
        TimeProvider? diagnosticsTimeProvider = null,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null,
        Action<DirectDatabaseSession>? diagnosticsDisposed = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _releaseAsync = releaseAsync;
        _observabilityOptionsSnapshot = observabilityOptionsSnapshot;
        DataSessionRuntimeDiagnostics? tracker = DataSessionRuntimeDiagnostics.Create(
            observabilityOptionsSnapshot,
            DataConnectionOwnerKind.Direct,
            diagnosticsTimeProvider);
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState =
            database.RuntimeDiagnosticsState;
        if (tracker is not null || runtimeDiagnosticsStateOwner is not null ||
            runtimeDiagnosticsState?.IsEnabled == true)
        {
            _diagnostics = new DirectSessionDiagnostics(
                tracker,
                runtimeDiagnosticsState,
                runtimeDiagnosticsStateOwner,
                diagnosticsDisposed);
        }
        if (tracker is not null)
        {
            tracker.RegisterSession(
                DiagnosticsSessionKey,
                CSharpDbOperationScope.CurrentSessionId,
                tracker.GetUtcNowOrLast());
        }

        try
        {
            CSharpDbRuntimeDiagnosticsState? metricsState =
                runtimeDiagnosticsStateOwner?.State ?? runtimeDiagnosticsState;
            _metricsRegistration = metricsState?.RuntimeMetrics
                ?.RegisterDataProvider(
                    this,
                    CSharpDB.Observability.CSharpDbTransport.Direct);
        }
        catch
        {
            // Metrics registration is best effort and cannot prevent a session
            // from opening.
        }
    }

    bool ICSharpDbDataMetricsProvider.TryCaptureMetrics(
        out CSharpDbDataMetricSnapshot snapshot)
    {
        try
        {
            snapshot = new CSharpDbDataMetricSnapshot(
                Volatile.Read(ref _database) is null ? 0 : 1,
                _diagnostics?.Tracker?.ActiveReaderCount,
                PoolWaiters: null,
                AvailableConnections: null,
                PoolMetricsApplicable: false);
            return true;
        }
        catch
        {
            snapshot = default;
            return false;
        }
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.ExecuteAsync(sql, cancellationToken)
            : ExecuteObservedAsync(
                () => database.ExecuteAsync(sql, cancellationToken),
                observation: null);
    }

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
        if (_diagnostics?.Tracker is not null)
        {
            return ExecuteObservedAsync(
                () => database.ExecuteAsync(executionSql, observabilitySql, cancellationToken),
                observation);
        }

        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff(database);
        return database.ExecuteAsync(executionSql, observabilitySql, cancellationToken);
    }

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.ExecuteAsync(statement, cancellationToken)
            : ExecuteObservedAsync(
                () => database.ExecuteAsync(statement, cancellationToken),
                observation: null);
    }

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
        if (_diagnostics?.Tracker is not null)
        {
            return ExecuteObservedAsync(
                () => database.ExecuteAsync(statement, fingerprint, cancellationToken),
                observation);
        }

        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff(database);
        return database.ExecuteAsync(statement, fingerprint, cancellationToken);
    }

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.ExecuteAsync(insert, cancellationToken)
            : ExecuteObservedAsync(
                () => database.ExecuteAsync(insert, cancellationToken),
                observation: null);
    }

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
        if (_diagnostics?.Tracker is not null)
        {
            return ExecuteObservedAsync(
                () => database.ExecuteAsync(insert, fingerprint, cancellationToken),
                observation);
        }

        using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
        observation?.MarkDispatchHandoff(database);
        return database.ExecuteAsync(insert, fingerprint, cancellationToken);
    }

    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.BeginTransactionAsync(cancellationToken)
            : BeginObservedTransactionAsync(database, cancellationToken);
    }

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.CommitAsync(cancellationToken)
            : CompleteObservedTransactionAsync(database, commit: true, cancellationToken);
    }

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        Database database = GetDatabase();
        return _diagnostics?.Tracker is null
            ? database.RollbackAsync(cancellationToken)
            : CompleteObservedTransactionAsync(database, commit: false, cancellationToken);
    }

    public ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default)
        => GetDatabase().SaveToFileAsync(filePath, cancellationToken);

    public IReadOnlyCollection<string> GetTableNames() => GetDatabase().GetTableNames();
    public TableSchema? GetTableSchema(string tableName) => GetDatabase().GetTableSchema(tableName);
    public IReadOnlyCollection<IndexSchema> GetIndexes() => GetDatabase().GetIndexes();
    public IReadOnlyCollection<string> GetViewNames() => GetDatabase().GetViewNames();
    public string? GetViewSql(string viewName) => GetDatabase().GetViewSql(viewName);
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => GetDatabase().GetTriggers();

    public async ValueTask<DataConnectionDiagnosticsRawSnapshot?> CaptureRuntimeDiagnosticsAsync(
        int maximumSessionRecords,
        CancellationToken cancellationToken = default)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecords,
            nameof(maximumSessionRecords));
        DirectSessionDiagnostics? sidecar = _diagnostics;
        DataSessionRuntimeDiagnostics? diagnostics = sidecar?.Tracker;
        if (diagnostics is null)
            return null;

        const int MaximumConsistencyAttempts = 8;
        for (int attempt = 0; attempt < MaximumConsistencyAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!diagnostics.Consistency.TryStartRead(out long version))
            {
                await Task.Yield();
                continue;
            }

            bool disposed;
            bool hasActiveTransaction;
            long? transactionStartedTimestamp;
            lock (sidecar!.Gate)
            {
                disposed = sidecar.Disposed;
                hasActiveTransaction = sidecar.HasActiveTransaction;
                transactionStartedTimestamp = sidecar.TransactionStartedTimestamp;
            }

            DataSessionRuntimeDiagnostics.SessionStateBatch sessionBatch =
                diagnostics.CopySessions(
                    maximumSessionRecords,
                    hasActiveTransaction ? DiagnosticsSessionKey : null);
            DataSessionRuntimeDiagnostics.SessionStateCopy[] sessionCopies =
                sessionBatch.Records;
            DateTimeOffset snapshotAtUtc = diagnostics.GetUtcNowOrLast();
            long? snapshotTimestamp = hasActiveTransaction
                ? diagnostics.GetTimestampOrNull()
                : null;
            if (!diagnostics.Consistency.IsReadValid(version))
            {
                await Task.Yield();
                continue;
            }

            int expectedSessions = disposed ? 0 : 1;
            if (sessionBatch.TotalCount != expectedSessions)
                return null;

            long? transactionOwnerSessionKey = hasActiveTransaction
                ? DiagnosticsSessionKey
                : null;
            DataSessionDiagnosticsRawSnapshot[] sessions =
                DataSessionRuntimeDiagnostics.ProjectSessions(
                    sessionCopies,
                    transactionOwnerSessionKey);
            bool sessionsTruncated =
                sessionBatch.TotalCount > sessionCopies.Length;
            OpaqueDiagnosticsId? transactionOwnerSessionId = hasActiveTransaction &&
                sessionCopies.Length == 1
                    ? sessionCopies[0].SessionId
                    : null;
            TimeSpan? oldestTransactionAge = hasActiveTransaction
                ? diagnostics.GetElapsedTimeOrNull(
                    transactionStartedTimestamp,
                    snapshotTimestamp)
                : null;

            return new DataConnectionDiagnosticsRawSnapshot(
                diagnostics.ContributorId,
                diagnostics.DatabaseAlias,
                snapshotAtUtc,
                diagnostics.OwnerKind,
                null,
                null,
                null,
                sessionCopies.Length,
                sessionBatch.ActiveReaderCount,
                hasActiveTransaction ? 1 : 0,
                oldestTransactionAge,
                0,
                0,
                0,
                0,
                0,
                transactionOwnerSessionId,
                null,
                Array.AsReadOnly(sessions),
                maximumSessionRecords,
                0,
                sessionsTruncated);
        }

        return null;
    }

    private async ValueTask<QueryResult> ExecuteObservedAsync(
        Func<ValueTask<QueryResult>> executeAsync,
        AdoCommandObservation? observation)
    {
        DataSessionOperationLease? operation =
            _diagnostics?.Tracker?.TryBeginOperation(DiagnosticsSessionKey);
        try
        {
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            observation?.MarkDispatchHandoff(GetDatabase());
            QueryResult result = await executeAsync();
            return operation?.ObserveResult(result) ?? result;
        }
        catch
        {
            operation?.Complete();
            throw;
        }
    }

    private async ValueTask BeginObservedTransactionAsync(
        Database database,
        CancellationToken cancellationToken)
    {
        await database.BeginTransactionAsync(cancellationToken);
        DirectSessionDiagnostics sidecar = _diagnostics!;
        DataSessionRuntimeDiagnostics diagnostics = sidecar.Tracker!;
        DateTimeOffset startedAtUtc = diagnostics.GetUtcNowOrLast();
        long? startedTimestamp = diagnostics.GetTimestampOrNull();
        diagnostics.Consistency.BeginMutation();
        try
        {
            lock (sidecar.Gate)
            {
                sidecar.HasActiveTransaction = true;
                sidecar.TransactionStartedTimestamp = startedTimestamp;
            }
        }
        finally
        {
            try
            {
                diagnostics.TouchSession(DiagnosticsSessionKey, startedAtUtc);
            }
            catch
            {
                // Diagnostics cannot replace a successful transaction start.
            }
            finally
            {
                diagnostics.Consistency.EndMutation();
            }
        }
    }

    private async ValueTask CompleteObservedTransactionAsync(
        Database database,
        bool commit,
        CancellationToken cancellationToken)
    {
        if (commit)
            await database.CommitAsync(cancellationToken);
        else
            await database.RollbackAsync(cancellationToken);

        DirectSessionDiagnostics sidecar = _diagnostics!;
        DataSessionRuntimeDiagnostics diagnostics = sidecar.Tracker!;
        DateTimeOffset completedAtUtc = diagnostics.GetUtcNowOrLast();
        diagnostics.Consistency.BeginMutation();
        try
        {
            lock (sidecar.Gate)
            {
                sidecar.HasActiveTransaction = false;
                sidecar.TransactionStartedTimestamp = null;
            }
        }
        finally
        {
            try
            {
                diagnostics.TouchSession(DiagnosticsSessionKey, completedAtUtc);
            }
            catch
            {
                // Diagnostics cannot replace a successful transaction completion.
            }
            finally
            {
                diagnostics.Consistency.EndMutation();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        var database = _database;
        _database = null;

        if (database is null)
            return;

        DirectSessionDiagnostics? sidecar = _diagnostics;
        DataSessionRuntimeDiagnostics? diagnostics = sidecar?.Tracker;
        if (diagnostics is not null)
        {
            DateTimeOffset disposedAtUtc = diagnostics.GetUtcNowOrLast();
            diagnostics.Consistency.BeginMutation();
            try
            {
                lock (sidecar!.Gate)
                {
                    sidecar.Disposed = true;
                    sidecar.HasActiveTransaction = false;
                    sidecar.TransactionStartedTimestamp = null;
                }
                diagnostics.RemoveSession(DiagnosticsSessionKey, disposedAtUtc);
            }
            catch
            {
                // Diagnostics cannot replace session disposal.
            }
            finally
            {
                diagnostics.Consistency.EndMutation();
            }
        }

        try
        {
            if (_releaseAsync is null)
                await database.DisposeAsync();
            else
                await _releaseAsync(database);
        }
        finally
        {
            try
            {
                sidecar?.DisposedCallback?.Invoke(this);
            }
            catch
            {
                // Diagnostics deregistration cannot replace disposal behavior.
            }
            finally
            {
                try
                {
                    Interlocked.Exchange(
                        ref _metricsRegistration,
                        null)?.Dispose();
                }
                finally
                {
                    sidecar?.RuntimeDiagnosticsStateOwner?.Dispose();
                }
            }
        }
    }

    private Database GetDatabase()
        => _database ?? throw new InvalidOperationException("Session is closed.");

    private sealed class DirectSessionDiagnostics(
        DataSessionRuntimeDiagnostics? tracker,
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner,
        Action<DirectDatabaseSession>? disposedCallback)
    {
        internal object Gate { get; } = new();
        internal DataSessionRuntimeDiagnostics? Tracker { get; } = tracker;
        internal CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState { get; } =
            runtimeDiagnosticsState;
        internal DataRuntimeDiagnosticsStateOwner? RuntimeDiagnosticsStateOwner { get; } =
            runtimeDiagnosticsStateOwner;
        internal Action<DirectDatabaseSession>? DisposedCallback { get; } =
            disposedCallback;
        internal bool Disposed { get; set; }
        internal bool HasActiveTransaction { get; set; }
        internal long? TransactionStartedTimestamp { get; set; }
    }
}
