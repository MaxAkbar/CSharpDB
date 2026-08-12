using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Collections.Concurrent;
using System.Text.Json;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Execution;
using CSharpDB.Sql;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Transactions;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Engine;

internal readonly record struct RowIdReservationDiagnosticsSnapshot(
    long ReservationCount,
    long ReservedRowIdCount);

internal readonly record struct AdaptiveQueryReoptimizationDiagnosticsSnapshot(
    long EligibleQueryCount,
    long AttemptCount,
    long SuccessfulSwitchCount,
    long RejectedSwitchCount,
    long DivergenceEventCount,
    long BufferedRowCount,
    long MaxBufferedFallbackCount,
    long UnsupportedFallbackCount,
    long ReoptimizationLimitFallbackCount);

internal readonly record struct MutationTargetCollectionDiagnosticsSnapshot(
    long IndexedCollectionCount,
    long ScannedCollectionCount);

/// <summary>
/// Top-level entry point for the CSharpDB embedded database engine.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private const int DefaultStatementCacheCapacity = 512;
    private const int DefaultImplicitConflictRetries = 10;
    // ROWVERSION values are handed out from a WAL-backed high/low lease.  A
    // deliberately generous range keeps lease-renewal commits off concurrent
    // write paths while still failing closed if one transaction exhausts it.
    private const ulong RowVersionReservationSize = 1UL << 20;
    private static readonly WriteTransactionOptions ImplicitAutoCommitWriteTransactionOptions = new()
    {
        MaxRetries = DefaultImplicitConflictRetries,
        InitialBackoff = TimeSpan.FromMilliseconds(0.25),
        MaxBackoff = TimeSpan.FromMilliseconds(20),
    };

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly QueryPlanner _planner;
    private readonly IRecordSerializer _recordSerializer;
    private readonly ISchemaSerializer _schemaSerializer;
    private readonly IIndexProvider _indexProvider;
    private readonly ICatalogStore _catalogStore;
    private readonly CSharpDbObservabilityOptions? _observabilityOptions;
    private readonly CSharpDbRuntimeDiagnosticsState? _runtimeDiagnosticsState;
    private readonly bool _ownsRuntimeDiagnosticsState;
    private readonly StorageRuntimeDiagnosticsProvenance _storageRuntimeDiagnosticsProvenance;
    private readonly QueryObservability? _queryObservability;
    private readonly IQueryPlanRuntimeObserver? _queryPlanRuntimeObserver;
    private readonly TemporaryTableManager _temporaryTables;
    private readonly AdvisoryStatisticsPersistenceMode _advisoryStatisticsPersistenceMode;
    private readonly DbFunctionRegistry _functions;
    private readonly string? _databasePath;
    private readonly StatementCache _statementCache;
    private readonly HybridDatabasePersistenceCoordinator? _hybridPersistenceCoordinator;
    private readonly bool _skipDisposePersistence;
    private readonly Dictionary<string, object> _collectionCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PendingCollectionCatalogMutation> _pendingCollectionCatalogMutations = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _sharedNextRowIdHints = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _sharedNextRowIdGate = new();
    private readonly object _sharedRowVersionGate = new();
    private readonly SemaphoreSlim _sharedRowVersionReservationGate = new(1, 1);
    private readonly SemaphoreSlim _writeOperationGate = new(1, 1);
    private readonly SemaphoreSlim _sharedStateGate = new(1, 1);
    private long _rowIdReservationCount;
    private long _rowIdReservedRowCount;
    private ulong _sharedRowVersionHighWater;
    private ulong _sharedRowVersionReservationHighWater;
    private bool _requireDurableRowVersionReservation;
    private long _transactionIndexedMutationTargetCollectionCount;
    private long _transactionScannedMutationTargetCollectionCount;
    private long _observedSchemaVersion;
    private ImplicitInsertExecutionMode _implicitInsertExecutionMode;
    private bool _inTransaction;
    private bool _explicitTransactionFailed;
    private LifecycleOperation? _explicitTransactionObservation;
    private int _openCompleted;
    private int _closeObservationStarted;
    private StorageRuntimeDiagnostics.Registration?
        _storageRuntimeDiagnosticsRegistration;

    /// <summary>
    /// When true, simple PK equality lookups (SELECT * WHERE pk = N) use a synchronous
    /// cache-only fast path, bypassing the async operator pipeline. Defaults to true.
    /// </summary>
    public bool PreferSyncPointLookups
    {
        get => _planner.PreferSyncPointLookups;
        set => _planner.PreferSyncPointLookups = value;
    }

    /// <summary>
    /// Controls how shared auto-commit INSERT statements execute on this database handle.
    /// </summary>
    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode
    {
        get => _implicitInsertExecutionMode;
        set => _implicitInsertExecutionMode = value;
    }

    public int ActiveReaderCount => _pager.ActiveReaderCount;

    internal bool IsObservabilityEnabled => _observabilityOptions is not null;

    internal string? ObservabilityDatabaseAlias => _observabilityOptions?.DatabaseAlias;

    internal TimeSpan? ObservabilitySlowQueryThreshold => _observabilityOptions?.Logging.SlowQueryThreshold;

    internal CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _runtimeDiagnosticsState;

    internal BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>? GetActiveQueryDiagnosticsSnapshot(
        int maximumRecords)
        => _queryObservability?.GetActiveSnapshot(maximumRecords);

    internal BoundedDiagnosticsSnapshot<RecentQuerySnapshot>? GetRecentQueryDiagnosticsSnapshot(
        int maximumRecords)
        => _queryObservability?.GetRecentSnapshot(maximumRecords);

    internal DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>?
        GetActiveQueryDiagnosticsCollection(int maximumRecords)
        => _queryObservability?.GetActiveCollectionSnapshot(maximumRecords);

    internal DiagnosticsCollectionSnapshot<RecentQuerySnapshot>?
        GetRecentQueryDiagnosticsCollection(int maximumRecords)
        => _queryObservability?.GetRecentCollectionSnapshot(maximumRecords);

    internal QueryDiagnosticsSummary? GetQueryDiagnosticsSummary()
        => _queryObservability?.GetSummary();

    internal QueryPlanDiagnosticsSnapshot? GetQueryPlanDiagnosticsSnapshot(
        OpaqueDiagnosticsId operationId)
        => _queryObservability?.GetPlanSnapshot(operationId);

    internal QueryDetailSnapshot? GetQueryDetailDiagnosticsSnapshot(
        OpaqueDiagnosticsId operationId)
        => _queryObservability?.GetQueryDetailSnapshot(operationId);

    internal WalFlushDiagnosticsSnapshot GetWalFlushDiagnosticsSnapshot() =>
        _pager.GetWalFlushDiagnosticsSnapshot();

    internal void ResetWalFlushDiagnostics()
    {
        _pager.ResetWalFlushDiagnostics();
        _runtimeDiagnosticsState?.AdvanceCounterEpoch();
    }

    internal CommitPathDiagnosticsSnapshot GetCommitPathDiagnosticsSnapshot() =>
        _pager.GetCommitPathDiagnosticsSnapshot();

    internal void ResetCommitPathDiagnostics()
    {
        _pager.ResetCommitPathDiagnostics();
        _runtimeDiagnosticsState?.AdvanceCounterEpoch();
    }

    internal RowIdReservationDiagnosticsSnapshot GetRowIdReservationDiagnosticsSnapshot() =>
        new(
            Interlocked.Read(ref _rowIdReservationCount),
            Interlocked.Read(ref _rowIdReservedRowCount));

    internal void ResetRowIdReservationDiagnostics()
    {
        Interlocked.Exchange(ref _rowIdReservationCount, 0);
        Interlocked.Exchange(ref _rowIdReservedRowCount, 0);
        _runtimeDiagnosticsState?.AdvanceCounterEpoch();
    }

    internal AdaptiveQueryReoptimizationDiagnosticsSnapshot GetAdaptiveQueryReoptimizationDiagnosticsSnapshot()
    {
        var snapshot = _planner.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();
        return new AdaptiveQueryReoptimizationDiagnosticsSnapshot(
            snapshot.EligibleQueryCount,
            snapshot.AttemptCount,
            snapshot.SuccessfulSwitchCount,
            snapshot.RejectedSwitchCount,
            snapshot.DivergenceEventCount,
            snapshot.BufferedRowCount,
            snapshot.MaxBufferedFallbackCount,
            snapshot.UnsupportedFallbackCount,
            snapshot.ReoptimizationLimitFallbackCount);
    }

    internal void ResetAdaptiveQueryReoptimizationDiagnostics()
    {
        _planner.ResetAdaptiveQueryReoptimizationDiagnostics();
        _runtimeDiagnosticsState?.AdvanceCounterEpoch();
    }

    internal MutationTargetCollectionDiagnosticsSnapshot GetMutationTargetCollectionDiagnosticsSnapshot()
    {
        var snapshot = _planner.GetMutationTargetCollectionDiagnosticsSnapshot();
        return new MutationTargetCollectionDiagnosticsSnapshot(
            snapshot.IndexedCollectionCount + Interlocked.Read(ref _transactionIndexedMutationTargetCollectionCount),
            snapshot.ScannedCollectionCount + Interlocked.Read(ref _transactionScannedMutationTargetCollectionCount));
    }

    internal void ResetMutationTargetCollectionDiagnostics()
    {
        _planner.ResetMutationTargetCollectionDiagnostics();
        Interlocked.Exchange(ref _transactionIndexedMutationTargetCollectionCount, 0);
        Interlocked.Exchange(ref _transactionScannedMutationTargetCollectionCount, 0);
        _runtimeDiagnosticsState?.AdvanceCounterEpoch();
    }

    internal void RecordMutationTargetCollectionDiagnostics(MutationTargetCollectionDiagnosticsSnapshot snapshot)
    {
        if (snapshot.IndexedCollectionCount != 0)
            Interlocked.Add(ref _transactionIndexedMutationTargetCollectionCount, snapshot.IndexedCollectionCount);
        if (snapshot.ScannedCollectionCount != 0)
            Interlocked.Add(ref _transactionScannedMutationTargetCollectionCount, snapshot.ScannedCollectionCount);
    }

    private Database(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer recordSerializer,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        CSharpDbObservabilityOptions? observabilityOptions,
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode,
        ImplicitInsertExecutionMode implicitInsertExecutionMode = ImplicitInsertExecutionMode.Serialized,
        AdaptiveQueryReoptimizationOptions? adaptiveQueryReoptimization = null,
        DbFunctionRegistry? functions = null,
        HybridDatabasePersistenceCoordinator? hybridPersistenceCoordinator = null,
        string? databasePath = null,
        StorageEngineOptions? temporaryStorageOptions = null,
        bool skipDisposePersistence = false,
        WindowExecutionOptions? windowExecution = null,
        StorageRuntimeDiagnosticsProvenance storageRuntimeDiagnosticsProvenance =
            StorageRuntimeDiagnosticsProvenance.BuiltIn,
        bool ownsRuntimeDiagnosticsState = false,
        StorageRuntimeDiagnostics.Registration?
            storageRuntimeDiagnosticsRegistration = null)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer;
        _schemaSerializer = schemaSerializer;
        _indexProvider = indexProvider;
        _catalogStore = catalogStore;
        _observabilityOptions = observabilityOptions;
        _ownsRuntimeDiagnosticsState = ownsRuntimeDiagnosticsState ||
            observabilityOptions is not null && runtimeDiagnosticsState is null;
        _runtimeDiagnosticsState = runtimeDiagnosticsState ??
            (observabilityOptions is null
                ? null
                : new CSharpDbRuntimeDiagnosticsState(observabilityOptions));
        _storageRuntimeDiagnosticsProvenance = storageRuntimeDiagnosticsProvenance;
        _storageRuntimeDiagnosticsRegistration =
            storageRuntimeDiagnosticsRegistration;
        _queryObservability = _runtimeDiagnosticsState?.IsEnabled != true
            ? null
            : new QueryObservability(_runtimeDiagnosticsState);
        _queryPlanRuntimeObserver =
            _queryObservability is { HistoryEnabled: true } queryObservability
                ? queryObservability.PlanRuntimeObserver
                : null;
        _temporaryTables = new TemporaryTableManager(temporaryStorageOptions ?? new StorageEngineOptions());
        _advisoryStatisticsPersistenceMode = advisoryStatisticsPersistenceMode;
        _functions = functions ?? DbFunctionRegistry.Empty;
        _databasePath = string.IsNullOrWhiteSpace(databasePath) ? null : Path.GetFullPath(databasePath);
        _implicitInsertExecutionMode = implicitInsertExecutionMode;
        _hybridPersistenceCoordinator = hybridPersistenceCoordinator;
        _skipDisposePersistence = skipDisposePersistence;
        _planner = new QueryPlanner(
            pager,
            catalog,
            _recordSerializer,
            tableRowCountProvider: null,
            nextRowIdHintProvider: TryGetSharedNextRowIdHint,
            nextRowIdReservationProvider: null,
            nextRowIdRangeReservationProvider: ReserveSharedNextRowIdRange,
            nextRowIdObservationProvider: ObserveSharedNextRowId,
            useTransientNextRowIdHints: false,
            functions: _functions,
            adaptiveQueryReoptimization: adaptiveQueryReoptimization,
            externalTableBasePath: GetExternalTableBasePath(_databasePath),
            temporaryTables: _temporaryTables,
            windowExecution: windowExecution,
            rowVersionAllocator: ReserveSharedRowVersion)
        {
            PlanRuntimeObserver = _queryPlanRuntimeObserver,
        };
        _statementCache = new StatementCache(DefaultStatementCacheCapacity);
        _observedSchemaVersion = catalog.SchemaVersion;
        RefreshSharedNextRowIdHintsFromCatalog();
    }

    /// <summary>
    /// Begin an explicit multi-writer transaction with its own isolated catalog and planner context.
    /// </summary>
    public ValueTask<WriteTransaction> BeginWriteTransactionAsync(CancellationToken ct = default)
        => BeginWriteTransactionCoreAsync(observeLifecycle: true, ct);

    private async ValueTask<WriteTransaction> BeginWriteTransactionCoreAsync(
        bool observeLifecycle,
        CancellationToken ct)
    {
        if (_inTransaction)
            throw new InvalidOperationException("Cannot start a multi-writer transaction while a legacy explicit transaction is active.");

        LifecycleOperation? operation = observeLifecycle
            ? StartLifecycleObservability(
                CSharpDbLogEvents.TransactionCompleted,
                CSharpDbOperationClass.Transaction)
            : null;
        try
        {
            await EnsureSharedRowVersionReservationAsync(ct);
            PagerWriteTransaction storageTransaction = await _pager.BeginWriteTransactionAsync(ct);
            try
            {
                using var binding = storageTransaction.Bind();
                var transactionCatalog = await SchemaCatalog.CreateAsync(
                    _pager,
                    _schemaSerializer,
                    _indexProvider,
                    _catalogStore,
                    _advisoryStatisticsPersistenceMode,
                    ct);
                var transactionPlanner = new QueryPlanner(
                    _pager,
                    transactionCatalog,
                    _recordSerializer,
                    tableRowCountProvider: null,
                    nextRowIdHintProvider: TryGetSharedNextRowIdHint,
                    nextRowIdReservationProvider: null,
                    nextRowIdRangeReservationProvider: ReserveSharedNextRowIdRange,
                    nextRowIdObservationProvider: ObserveSharedNextRowId,
                    useTransientNextRowIdHints: true,
                    functions: _functions,
                    adaptiveQueryReoptimization: _planner.AdaptiveQueryReoptimization,
                    externalTableBasePath: GetExternalTableBasePath(_databasePath),
                    temporaryTables: _temporaryTables,
                    windowExecution: _planner.WindowExecution,
                    rowVersionAllocator: ReserveSharedRowVersion)
                {
                    PreferSyncPointLookups = PreferSyncPointLookups,
                    PlanRuntimeObserver = _queryPlanRuntimeObserver,
                };

                return new WriteTransaction(
                    this,
                    storageTransaction,
                    transactionCatalog,
                    transactionPlanner,
                    transactionCatalog.SchemaVersion,
                    transactionCatalog.RowVersionHighWater,
                    operation);
            }
            catch
            {
                await storageTransaction.DisposeAsync();
                throw;
            }
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    private long? TryGetSharedNextRowIdHint(string tableName)
    {
        return _sharedNextRowIdHints.TryGetValue(tableName, out long nextRowId) && nextRowId > 0
            ? nextRowId
            : null;
    }

    private void RefreshSharedNextRowIdHintsFromCatalog()
    {
        lock (_sharedNextRowIdGate)
        {
            var preservedHints = _sharedNextRowIdHints.Count == 0
                ? null
                : _sharedNextRowIdHints.ToArray();

            _sharedNextRowIdHints.Clear();

            foreach (string tableName in _catalog.GetTableNames())
            {
                long nextRowId = _catalog.GetTable(tableName)?.NextRowId ?? 0;
                if (nextRowId > 0)
                    _sharedNextRowIdHints[tableName] = nextRowId;
            }

            if (preservedHints is null)
                return;

            foreach ((string tableName, long nextRowId) in preservedHints)
            {
                if (nextRowId <= 0 || _catalog.GetTable(tableName) is null)
                    continue;

                _sharedNextRowIdHints.AddOrUpdate(
                    tableName,
                    nextRowId,
                    (_, existing) => Math.Max(existing, nextRowId));
            }
        }
    }

    private static string GetExternalTableBasePath(string? databasePath)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            return Directory.GetCurrentDirectory();

        string? directory = Path.GetDirectoryName(databasePath);
        return string.IsNullOrWhiteSpace(directory)
            ? Directory.GetCurrentDirectory()
            : directory;
    }

    private void ApplyCommittedNextRowIdHints(IReadOnlyCollection<KeyValuePair<string, long>> committedNextRowIds)
    {
        lock (_sharedNextRowIdGate)
        {
            foreach ((string tableName, long nextRowId) in committedNextRowIds)
            {
                if (nextRowId <= 0)
                    continue;

                _sharedNextRowIdHints.AddOrUpdate(
                    tableName,
                    nextRowId,
                    (_, existing) => Math.Max(existing, nextRowId));
            }
        }
    }

    private long ReserveSharedNextRowId(string tableName, long minimumNextRowId)
    {
        return ReserveSharedNextRowIdRange(tableName, minimumNextRowId, 1).Start;
    }

    private ulong ReserveSharedRowVersion(ulong minimumHighWater)
    {
        lock (_sharedRowVersionGate)
        {
            ulong highWater = _requireDurableRowVersionReservation
                ? _sharedRowVersionHighWater
                : Math.Max(_sharedRowVersionHighWater, minimumHighWater);
            if (highWater == ulong.MaxValue)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    "The database-wide ROWVERSION allocator has reached its maximum value.");
            }

            if (_requireDurableRowVersionReservation &&
                highWater >= _sharedRowVersionReservationHighWater)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    "The current transaction exhausted its durable ROWVERSION reservation; retry it in a new transaction.");
            }

            _sharedRowVersionHighWater = highWater + 1;
            return _sharedRowVersionHighWater;
        }
    }

    private async ValueTask InitializeSharedRowVersionHighWaterAsync(CancellationToken ct)
    {
        ulong storedHighWater = await _planner.GetDatabaseRowVersionHighWaterAsync(ct);
        lock (_sharedRowVersionGate)
        {
            _sharedRowVersionHighWater = Math.Max(_sharedRowVersionHighWater, storedHighWater);
            _sharedRowVersionReservationHighWater = Math.Max(
                _sharedRowVersionReservationHighWater,
                storedHighWater);
            _requireDurableRowVersionReservation = true;
        }
    }

    private async ValueTask EnsureSharedRowVersionReservationAsync(CancellationToken ct)
    {
        lock (_sharedRowVersionGate)
        {
            if (!_requireDurableRowVersionReservation ||
                _sharedRowVersionHighWater < _sharedRowVersionReservationHighWater)
            {
                return;
            }
        }

        await _sharedRowVersionReservationGate.WaitAsync(ct);
        try
        {
            ulong currentHighWater;
            ulong currentReservationHighWater;
            lock (_sharedRowVersionGate)
            {
                if (!_requireDurableRowVersionReservation ||
                    _sharedRowVersionHighWater < _sharedRowVersionReservationHighWater)
                {
                    return;
                }

                currentHighWater = _sharedRowVersionHighWater;
                currentReservationHighWater = Math.Max(
                    _sharedRowVersionReservationHighWater,
                    _catalog.RowVersionHighWater);
            }

            ulong reservationBase = Math.Max(currentHighWater, currentReservationHighWater);
            if (reservationBase > ulong.MaxValue - RowVersionReservationSize)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    "The database-wide ROWVERSION allocator cannot reserve another durable range.");
            }

            ulong reservedHighWater = reservationBase + RowVersionReservationSize;
            await _writeOperationGate.WaitAsync(ct);
            bool transactionStarted = false;
            try
            {
                await _pager.BeginTransactionAsync(ct);
                transactionStarted = true;
                await _catalog.PersistRowVersionHighWaterAsync(reservedHighWater, ct);
                PagerCommitResult commit = await BeginCommitWithCatalogSyncAsync(ct);
                transactionStarted = false;
                await WaitForCommitOrRecoverAsync(commit);
            }
            catch
            {
                if (transactionStarted)
                {
                    try { await _pager.RollbackAsync(CancellationToken.None); } catch { }
                    try { await _catalog.ReloadAsync(CancellationToken.None); } catch { }
                }

                throw;
            }
            finally
            {
                _writeOperationGate.Release();
            }

            lock (_sharedRowVersionGate)
            {
                _sharedRowVersionReservationHighWater = Math.Max(
                    _sharedRowVersionReservationHighWater,
                    reservedHighWater);
            }
        }
        finally
        {
            _sharedRowVersionReservationGate.Release();
        }
    }

    private (long Start, long EndExclusive) ReserveSharedNextRowIdRange(
        string tableName,
        long minimumNextRowId,
        int reservationCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentOutOfRangeException.ThrowIfLessThan(reservationCount, 1);

        long normalizedMinimum = minimumNextRowId > 0 ? minimumNextRowId : 1;
        lock (_sharedNextRowIdGate)
        {
            long currentNextRowId = _sharedNextRowIdHints.TryGetValue(tableName, out long existing)
                ? Math.Max(existing, normalizedMinimum)
                : normalizedMinimum;

            long endExclusive = checked(currentNextRowId + reservationCount);
            _sharedNextRowIdHints[tableName] = endExclusive;
            Interlocked.Increment(ref _rowIdReservationCount);
            Interlocked.Add(ref _rowIdReservedRowCount, reservationCount);
            return (currentNextRowId, endExclusive);
        }
    }

    private void ObserveSharedNextRowId(string tableName, long nextRowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        if (nextRowId <= 0)
            return;

        lock (_sharedNextRowIdGate)
        {
            _sharedNextRowIdHints.AddOrUpdate(
                tableName,
                nextRowId,
                (_, existing) => Math.Max(existing, nextRowId));
        }
    }

    /// <summary>
    /// Run a multi-writer transaction with automatic retry on transaction conflicts.
    /// </summary>
    public ValueTask RunWriteTransactionAsync(
        Func<WriteTransaction, CancellationToken, ValueTask> action,
        WriteTransactionOptions? options = null,
        CancellationToken ct = default)
        => RunWriteTransactionCoreAsync(action, options, observeLifecycle: true, ct);

    private async ValueTask RunWriteTransactionCoreAsync(
        Func<WriteTransaction, CancellationToken, ValueTask> action,
        WriteTransactionOptions? options,
        bool observeLifecycle,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(action);
        options ??= new WriteTransactionOptions();

        for (int attempt = 0; ; attempt++)
        {
            await using WriteTransaction transaction =
                await BeginWriteTransactionCoreAsync(observeLifecycle, ct);
            try
            {
                await action(transaction, ct);
                await transaction.CommitAsync(ct);
                return;
            }
            catch (CSharpDbConflictException) when (attempt < options.MaxRetries)
            {
                await options.DelayBeforeRetryAsync(attempt, ct);
            }
        }
    }

    /// <summary>
    /// Run a multi-writer transaction with automatic retry on transaction conflicts.
    /// </summary>
    public ValueTask<TResult> RunWriteTransactionAsync<TResult>(
        Func<WriteTransaction, CancellationToken, ValueTask<TResult>> action,
        WriteTransactionOptions? options = null,
        CancellationToken ct = default)
        => RunWriteTransactionCoreAsync(action, options, observeLifecycle: true, ct);

    private async ValueTask<TResult> RunWriteTransactionCoreAsync<TResult>(
        Func<WriteTransaction, CancellationToken, ValueTask<TResult>> action,
        WriteTransactionOptions? options,
        bool observeLifecycle,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(action);
        options ??= new WriteTransactionOptions();

        for (int attempt = 0; ; attempt++)
        {
            await using WriteTransaction transaction =
                await BeginWriteTransactionCoreAsync(observeLifecycle, ct);
            try
            {
                TResult result = await action(transaction, ct);
                await transaction.CommitAsync(ct);
                return result;
            }
            catch (CSharpDbConflictException) when (attempt < options.MaxRetries)
            {
                await options.DelayBeforeRetryAsync(attempt, ct);
            }
        }
    }

    internal async ValueTask OnExternalWriteTransactionCommittedAsync(
        bool reloadSharedCatalog,
        bool schemaChanged,
        IReadOnlyCollection<KeyValuePair<string, long>> committedNextRowIds,
        IReadOnlyCollection<KeyValuePair<string, long>> committedTableRowCountDeltas,
        IReadOnlyCollection<TableStatistics> committedTableStatistics,
        IReadOnlyCollection<ColumnStatistics> committedColumnStatistics,
        MutationTargetCollectionDiagnosticsSnapshot mutationTargetCollectionDiagnostics,
        CancellationToken ct)
    {
        RecordMutationTargetCollectionDiagnostics(mutationTargetCollectionDiagnostics);

        bool applyAdvisoryStats = committedTableStatistics.Count > 0 || committedColumnStatistics.Count > 0;
        bool applyTableMetadata = committedNextRowIds.Count > 0;
        bool applyTableRowCountDeltas = committedTableRowCountDeltas.Count > 0;

        if (reloadSharedCatalog || applyTableMetadata || applyAdvisoryStats || applyTableRowCountDeltas)
        {
            await _sharedStateGate.WaitAsync(ct);
            try
            {
                TableStatistics[] preservedDirtyTableStatistics = [];
                ColumnStatistics[] preservedDirtyColumnStatistics = [];
                KeyValuePair<string, long>[] preservedTableRowCountDeltas = [];

                if (reloadSharedCatalog)
                {
                    preservedDirtyTableStatistics = _catalog.GetDirtyTableStatistics().ToArray();
                    preservedDirtyColumnStatistics = _catalog.GetDirtyColumnStatistics().ToArray();
                    preservedTableRowCountDeltas = _catalog.GetPendingTableRowCountDeltas().ToArray();

                    await _catalog.ReloadAsync(ct);
                    _collectionCache.Clear();
                    if (schemaChanged)
                        _statementCache.Clear();

                    _observedSchemaVersion = _catalog.SchemaVersion;
                    RefreshSharedNextRowIdHintsFromCatalog();

                    if (preservedDirtyTableStatistics.Length > 0 || preservedDirtyColumnStatistics.Length > 0)
                    {
                        _catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
                            preservedDirtyTableStatistics,
                            preservedDirtyColumnStatistics,
                            markDirty: true);
                    }

                    if (preservedTableRowCountDeltas.Length > 0)
                        _catalog.ApplyCommittedTableRowCountDeltas(preservedTableRowCountDeltas);
                }

                if (applyTableMetadata)
                {
                    _catalog.ApplyCommittedTableMetadataSnapshot(committedNextRowIds);
                    ApplyCommittedNextRowIdHints(committedNextRowIds);
                }

                if (applyAdvisoryStats)
                {
                    IReadOnlyCollection<TableStatistics> mergedTableStatistics =
                        MergeCommittedTableStatistics(committedTableStatistics, committedTableRowCountDeltas);
                    _catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
                        mergedTableStatistics,
                        committedColumnStatistics,
                        markDirty: true);
                }

                if (applyTableRowCountDeltas)
                    _catalog.ApplyCommittedTableRowCountDeltas(committedTableRowCountDeltas);
            }
            finally
            {
                _sharedStateGate.Release();
            }
        }

        await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
    }

    private IReadOnlyCollection<TableStatistics> MergeCommittedTableStatistics(
        IReadOnlyCollection<TableStatistics> committedTableStatistics,
        IReadOnlyCollection<KeyValuePair<string, long>> committedTableRowCountDeltas)
    {
        if (committedTableStatistics.Count == 0 || committedTableRowCountDeltas.Count == 0)
            return committedTableStatistics;

        var deltasByTable = committedTableRowCountDeltas.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.OrdinalIgnoreCase);
        var merged = new List<TableStatistics>(committedTableStatistics.Count);

        foreach (TableStatistics stats in committedTableStatistics)
        {
            if (!deltasByTable.TryGetValue(stats.TableName, out long rowCountDelta))
            {
                merged.Add(stats);
                continue;
            }

            TableStatistics? existing = _catalog.GetTableStatistics(stats.TableName);
            if (existing is null || !existing.RowCountIsExact)
            {
                merged.Add(
                    new TableStatistics
                    {
                        TableName = stats.TableName,
                        RowCount = stats.RowCount,
                        RowCountIsExact = stats.RowCountIsExact,
                        HasStaleColumns = stats.HasStaleColumns || (existing?.HasStaleColumns ?? false),
                        LastPersistedChangeCounter = existing?.LastPersistedChangeCounter ?? stats.LastPersistedChangeCounter,
                    });
                continue;
            }

            long baseRowCount = existing?.RowCount ?? 0;
            merged.Add(
                new TableStatistics
                {
                    TableName = stats.TableName,
                    RowCount = checked(baseRowCount + rowCountDelta),
                    RowCountIsExact = stats.RowCountIsExact && (existing?.RowCountIsExact ?? true),
                    HasStaleColumns = stats.HasStaleColumns || (existing?.HasStaleColumns ?? false),
                    LastPersistedChangeCounter = existing?.LastPersistedChangeCounter ?? stats.LastPersistedChangeCounter,
                });
        }

        return merged;
    }

    /// <summary>
    /// Open an existing database file, or create a new one if it doesn't exist.
    /// </summary>
    public static async ValueTask<Database> OpenAsync(string filePath, CancellationToken ct = default)
    {
        return await OpenAsync(filePath, new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Create a new database file using default composition options, atomically refusing to open or
    /// replace an existing file.
    /// </summary>
    public static async ValueTask<Database> CreateNewAsync(string filePath, CancellationToken ct = default)
    {
        return await CreateNewAsync(filePath, new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a new in-memory database using default composition options.
    /// </summary>
    public static async ValueTask<Database> OpenInMemoryAsync(CancellationToken ct = default)
    {
        return await OpenInMemoryAsync(new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a new in-memory database using explicit composition options.
    /// </summary>
    public static async ValueTask<Database> OpenInMemoryAsync(
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);
        LifecycleOperation? operation = LifecycleObservability.Start(
            observabilityOptions,
            CSharpDbLogEvents.DatabaseOpened,
            CSharpDbOperationClass.Database);
        using StorageRuntimeOpenScope storageDiagnostics =
            StorageRuntimeOpenScope.Begin(
                options,
                observabilityOptions,
                StorageRuntimeDiagnosticsProvenance.BuiltIn,
                recoveryApplicable: false);

        try
        {
            var context = await InMemoryStorageEngineFactory.OpenAsync(
                storageDiagnostics.StorageOptions,
                ct: ct);
            var database = new Database(
                context.Pager,
                context.Catalog,
                context.RecordSerializer,
                context.SchemaSerializer,
                context.IndexProvider,
                context.CatalogStore,
                observabilityOptions,
                storageDiagnostics.RuntimeState,
                context.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                options.AdaptiveQueryReoptimization,
                options.Functions,
                temporaryStorageOptions: options.StorageEngineOptions,
                windowExecution: options.WindowExecution,
                ownsRuntimeDiagnosticsState:
                    storageDiagnostics.OwnsRuntimeState,
                storageRuntimeDiagnosticsRegistration:
                    storageDiagnostics.Registration);
            storageDiagnostics.TransferOwnership();
            database = await CompleteOpenAsync(database, ct);
            return CompleteObservedOpen(database, operation);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Open a lazy-resident hybrid database that persists committed state to the specified backing file.
    /// Existing file and WAL contents are read on demand while touched pages remain resident according
    /// to the pager cache policy; snapshot mode preserves the older full-image in-memory export behavior.
    /// </summary>
    public static async ValueTask<Database> OpenHybridAsync(
        string filePath,
        CancellationToken ct = default)
    {
        return await OpenHybridAsync(filePath, new DatabaseOptions(), new HybridDatabaseOptions(), ct);
    }

    /// <summary>
    /// Open a lazy-resident hybrid database that persists committed state to the specified backing file
    /// using explicit storage composition and persistence behavior.
    /// </summary>
    public static async ValueTask<Database> OpenHybridAsync(
        string filePath,
        DatabaseOptions options,
        HybridDatabaseOptions hybridOptions,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(hybridOptions);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);
        ValidateHybridHotSetOptions(options, hybridOptions);

        string fullPath = Path.GetFullPath(filePath);
        LifecycleOperation? operation = LifecycleObservability.Start(
            observabilityOptions,
            CSharpDbLogEvents.DatabaseOpened,
            CSharpDbOperationClass.Database);
        using StorageRuntimeOpenScope storageDiagnostics =
            StorageRuntimeOpenScope.Begin(
                options,
                observabilityOptions,
                StorageRuntimeDiagnosticsProvenance.BuiltIn,
                recoveryApplicable: File.Exists(fullPath));
        try
        {
            if (hybridOptions.PersistenceMode == HybridPersistenceMode.Snapshot)
            {
                StorageEngineContext snapshotContext;

                if (File.Exists(fullPath))
                {
                    byte[] databaseBytes = await File.ReadAllBytesAsync(fullPath, ct);
                    string walPath = fullPath + ".wal";
                    byte[] walBytes = File.Exists(walPath)
                        ? await File.ReadAllBytesAsync(walPath, ct)
                        : Array.Empty<byte>();

                    snapshotContext = await InMemoryStorageEngineFactory.OpenAsync(
                        storageDiagnostics.StorageOptions,
                        databaseBytes,
                        walBytes,
                        ct);
                }
                else
                {
                    snapshotContext = await InMemoryStorageEngineFactory.OpenAsync(
                        storageDiagnostics.StorageOptions,
                        ct: ct);
                }

                var snapshotDatabase = new Database(
                    snapshotContext.Pager,
                    snapshotContext.Catalog,
                    snapshotContext.RecordSerializer,
                    snapshotContext.SchemaSerializer,
                    snapshotContext.IndexProvider,
                    snapshotContext.CatalogStore,
                    observabilityOptions,
                    storageDiagnostics.RuntimeState,
                    snapshotContext.AdvisoryStatisticsPersistenceMode,
                    options.ImplicitInsertExecutionMode,
                    options.AdaptiveQueryReoptimization,
                    options.Functions,
                    new HybridDatabasePersistenceCoordinator(fullPath, hybridOptions.PersistenceTriggers),
                    fullPath,
                    temporaryStorageOptions: options.StorageEngineOptions,
                    windowExecution: options.WindowExecution,
                    ownsRuntimeDiagnosticsState:
                        storageDiagnostics.OwnsRuntimeState,
                    storageRuntimeDiagnosticsRegistration:
                        storageDiagnostics.Registration);
                storageDiagnostics.TransferOwnership();
                Database openedSnapshot = await CompleteOpenAsync(snapshotDatabase, ct);
                return CompleteObservedOpen(openedSnapshot, operation);
            }

            var context = await HybridStorageEngineFactory.OpenAsync(
                fullPath,
                storageDiagnostics.StorageOptions,
                ct);
            var database = new Database(
                context.Pager,
                context.Catalog,
                context.RecordSerializer,
                context.SchemaSerializer,
                context.IndexProvider,
                context.CatalogStore,
                observabilityOptions,
                storageDiagnostics.RuntimeState,
                context.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                options.AdaptiveQueryReoptimization,
                options.Functions,
                databasePath: fullPath,
                temporaryStorageOptions: options.StorageEngineOptions,
                windowExecution: options.WindowExecution,
                ownsRuntimeDiagnosticsState:
                    storageDiagnostics.OwnsRuntimeState,
                storageRuntimeDiagnosticsRegistration:
                    storageDiagnostics.Registration);
            storageDiagnostics.TransferOwnership();
            try
            {
                await database.UpgradeLegacyRowVersionAllocatorAsync(ct);
                await database.InitializeSharedRowVersionHighWaterAsync(ct);
                await database.EnsureFullTextInternalStoresOnOpenAsync(ct);
                await database.WarmHybridHotSetAsync(hybridOptions, ct);
                return CompleteObservedOpen(database, operation);
            }
            catch
            {
                await database.DisposeAsync();
                throw;
            }
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Load an on-disk database into memory using default composition options.
    /// If a companion WAL file exists, committed WAL frames are recovered into the in-memory copy.
    /// </summary>
    public static async ValueTask<Database> LoadIntoMemoryAsync(string filePath, CancellationToken ct = default)
    {
        return await LoadIntoMemoryAsync(filePath, new DatabaseOptions(), ct);
    }

    /// <summary>
    /// Load an on-disk database into memory using explicit composition options.
    /// If a companion WAL file exists, committed WAL frames are recovered into the in-memory copy.
    /// </summary>
    public static async ValueTask<Database> LoadIntoMemoryAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);

        string fullPath = Path.GetFullPath(filePath);
        LifecycleOperation? operation = LifecycleObservability.Start(
            observabilityOptions,
            CSharpDbLogEvents.DatabaseOpened,
            CSharpDbOperationClass.Database);
        using StorageRuntimeOpenScope storageDiagnostics =
            StorageRuntimeOpenScope.Begin(
                options,
                observabilityOptions,
                StorageRuntimeDiagnosticsProvenance.BuiltIn,
                recoveryApplicable: true);
        try
        {
            if (!File.Exists(fullPath))
                throw new FileNotFoundException("Database file not found.", fullPath);

            byte[] databaseBytes = await File.ReadAllBytesAsync(fullPath, ct);
            string walPath = fullPath + ".wal";
            byte[] walBytes = File.Exists(walPath)
                ? await File.ReadAllBytesAsync(walPath, ct)
                : Array.Empty<byte>();

            var context = await InMemoryStorageEngineFactory.OpenAsync(
                storageDiagnostics.StorageOptions,
                databaseBytes,
                walBytes,
                ct);

            var database = new Database(
                context.Pager,
                context.Catalog,
                context.RecordSerializer,
                context.SchemaSerializer,
                context.IndexProvider,
                context.CatalogStore,
                observabilityOptions,
                storageDiagnostics.RuntimeState,
                context.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                options.AdaptiveQueryReoptimization,
                options.Functions,
                databasePath: fullPath,
                temporaryStorageOptions: options.StorageEngineOptions,
                windowExecution: options.WindowExecution,
                ownsRuntimeDiagnosticsState:
                    storageDiagnostics.OwnsRuntimeState,
                storageRuntimeDiagnosticsRegistration:
                    storageDiagnostics.Registration);
            storageDiagnostics.TransferOwnership();
            database = await CompleteOpenAsync(database, ct);
            return CompleteObservedOpen(database, operation);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Open an existing database file, or create a new one if it doesn't exist, using explicit composition options.
    /// </summary>
    public static async ValueTask<Database> OpenAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);

        string fullPath = Path.GetFullPath(filePath);
        LifecycleOperation? operation = LifecycleObservability.Start(
            observabilityOptions,
            CSharpDbLogEvents.DatabaseOpened,
            CSharpDbOperationClass.Database);
        StorageRuntimeDiagnosticsProvenance provenance =
            GetStorageRuntimeDiagnosticsProvenance(
                options.StorageEngineFactory);
        using StorageRuntimeOpenScope storageDiagnostics =
            StorageRuntimeOpenScope.Begin(
                options,
                observabilityOptions,
                provenance,
                recoveryApplicable: File.Exists(fullPath));
        try
        {
            var context = await options.StorageEngineFactory.OpenAsync(
                fullPath,
                storageDiagnostics.StorageOptions,
                ct);
            var database = new Database(
                context.Pager,
                context.Catalog,
                context.RecordSerializer,
                context.SchemaSerializer,
                context.IndexProvider,
                context.CatalogStore,
                observabilityOptions,
                storageDiagnostics.RuntimeState,
                context.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                options.AdaptiveQueryReoptimization,
                options.Functions,
                databasePath: fullPath,
                temporaryStorageOptions: options.StorageEngineOptions,
                windowExecution: options.WindowExecution,
                storageRuntimeDiagnosticsProvenance:
                    provenance,
                ownsRuntimeDiagnosticsState:
                    storageDiagnostics.OwnsRuntimeState,
                storageRuntimeDiagnosticsRegistration:
                    storageDiagnostics.Registration);
            storageDiagnostics.TransferOwnership();
            database = await CompleteOpenAsync(database, ct);
            return CompleteObservedOpen(database, operation);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Opens an engine-owned private snapshot copy without running open-time
    /// repair routines. The caller must never expose this mutable handle.
    /// </summary>
    internal static async ValueTask<Database> OpenPrivateSnapshotCopyAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);

        string fullPath = Path.GetFullPath(filePath);
        StorageEngineOptions privateStorageOptions =
            options.StorageEngineOptions.RuntimeDiagnosticsObserver is null
                ? options.StorageEngineOptions
                : options.StorageEngineOptions.WithRuntimeDiagnosticsObserver(null);
        var context = await new DefaultStorageEngineFactory().OpenAsync(
            fullPath,
            privateStorageOptions,
            ct);
        return new Database(
            context.Pager,
            context.Catalog,
            context.RecordSerializer,
            context.SchemaSerializer,
            context.IndexProvider,
            context.CatalogStore,
            observabilityOptions,
            options.RuntimeDiagnosticsState,
            context.AdvisoryStatisticsPersistenceMode,
            options.ImplicitInsertExecutionMode,
            options.AdaptiveQueryReoptimization,
            options.Functions,
            databasePath: fullPath,
            temporaryStorageOptions: options.StorageEngineOptions,
            skipDisposePersistence: true,
            windowExecution: options.WindowExecution);
    }

    /// <summary>
    /// Recovers and checkpoints an engine-owned private snapshot pair without
    /// constructing a Database or persisting advisory/catalog metadata.
    /// </summary>
    internal static async ValueTask RecoverPrivateSnapshotCopyAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        _ = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);

        string fullPath = Path.GetFullPath(filePath);
        StorageEngineOptions privateStorageOptions =
            options.StorageEngineOptions.RuntimeDiagnosticsObserver is null
                ? options.StorageEngineOptions
                : options.StorageEngineOptions.WithRuntimeDiagnosticsObserver(null);
        var context = await new DefaultStorageEngineFactory().OpenAsync(
            fullPath,
            privateStorageOptions,
            ct);
        await context.Pager.DisposeAsync();
    }

    /// <summary>
    /// Create a new database file using explicit composition options, atomically refusing to open or
    /// replace an existing file.
    /// </summary>
    public static async ValueTask<Database> CreateNewAsync(
        string filePath,
        DatabaseOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);
        CSharpDbObservabilityOptions? observabilityOptions = CreateObservabilityOptionsSnapshot(options);
        _ = QueryPlanner.NormalizeWindowExecutionOptions(options.WindowExecution);

        string fullPath = Path.GetFullPath(filePath);
        LifecycleOperation? operation = LifecycleObservability.Start(
            observabilityOptions,
            CSharpDbLogEvents.DatabaseOpened,
            CSharpDbOperationClass.Database);
        StorageRuntimeDiagnosticsProvenance provenance =
            GetStorageRuntimeDiagnosticsProvenance(
                options.StorageEngineFactory);
        using StorageRuntimeOpenScope storageDiagnostics =
            StorageRuntimeOpenScope.Begin(
                options,
                observabilityOptions,
                provenance,
                recoveryApplicable: false);
        try
        {
            var context = await options.StorageEngineFactory.CreateNewAsync(
                fullPath,
                storageDiagnostics.StorageOptions,
                ct);
            var database = new Database(
                context.Pager,
                context.Catalog,
                context.RecordSerializer,
                context.SchemaSerializer,
                context.IndexProvider,
                context.CatalogStore,
                observabilityOptions,
                storageDiagnostics.RuntimeState,
                context.AdvisoryStatisticsPersistenceMode,
                options.ImplicitInsertExecutionMode,
                options.AdaptiveQueryReoptimization,
                options.Functions,
                databasePath: fullPath,
                temporaryStorageOptions: options.StorageEngineOptions,
                windowExecution: options.WindowExecution,
                storageRuntimeDiagnosticsProvenance:
                    provenance,
                ownsRuntimeDiagnosticsState:
                    storageDiagnostics.OwnsRuntimeState,
                storageRuntimeDiagnosticsRegistration:
                    storageDiagnostics.Registration);
            storageDiagnostics.TransferOwnership();
            database = await CompleteOpenAsync(database, ct);
            return CompleteObservedOpen(database, operation);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    private static CSharpDbObservabilityOptions? CreateObservabilityOptionsSnapshot(DatabaseOptions options)
    {
        CSharpDbObservabilityOptions? configured = options.ObservabilityOptions;
        if (configured?.Enabled != true)
            return null;

        byte[] serialized = JsonSerializer.SerializeToUtf8Bytes(
            configured,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        CSharpDbObservabilityOptions snapshot = JsonSerializer.Deserialize(
                serialized,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions)
            ?? throw new InvalidOperationException("Failed to snapshot the observability configuration.");
        snapshot.Validate();
        return snapshot;
    }

    private static async ValueTask<Database> CompleteOpenAsync(Database database, CancellationToken ct)
    {
        try
        {
            await database.UpgradeLegacyRowVersionAllocatorAsync(ct);
            await database.InitializeSharedRowVersionHighWaterAsync(ct);
            await database.EnsureFullTextInternalStoresOnOpenAsync(ct);
            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    private static Database CompleteObservedOpen(
        Database database,
        LifecycleOperation? operation)
    {
        Volatile.Write(ref database._openCompleted, 1);
        if (database._storageRuntimeDiagnosticsRegistration is { } opening)
        {
            opening.Promote(database._pager);
        }
        else
        {
            database._storageRuntimeDiagnosticsRegistration =
                StorageRuntimeDiagnostics.TryRegister(
                    database._runtimeDiagnosticsState,
                    database._pager,
                    database._storageRuntimeDiagnosticsProvenance);
        }
        operation?.Succeed();
        return database;
    }

    private static StorageRuntimeDiagnosticsProvenance
        GetStorageRuntimeDiagnosticsProvenance(IStorageEngineFactory factory)
        => factory.GetType() == typeof(DefaultStorageEngineFactory)
            ? StorageRuntimeDiagnosticsProvenance.BuiltIn
            : StorageRuntimeDiagnosticsProvenance.CustomFactory;

    private sealed class StorageRuntimeOpenScope : IDisposable
    {
        private readonly bool _ownsRuntimeState;
        private int _transferred;

        private StorageRuntimeOpenScope(
            CSharpDbRuntimeDiagnosticsState? runtimeState,
            bool ownsRuntimeState,
            StorageRuntimeDiagnosticsProvenance provenance,
            StorageEngineOptions storageOptions,
            StorageRuntimeDiagnostics.Registration? registration)
        {
            RuntimeState = runtimeState;
            _ownsRuntimeState = ownsRuntimeState;
            Provenance = provenance;
            StorageOptions = storageOptions;
            Registration = registration;
        }

        internal CSharpDbRuntimeDiagnosticsState? RuntimeState { get; }
        internal bool OwnsRuntimeState => _ownsRuntimeState;
        internal StorageRuntimeDiagnosticsProvenance Provenance { get; }
        internal StorageEngineOptions StorageOptions { get; }
        internal StorageRuntimeDiagnostics.Registration? Registration { get; }

        internal static StorageRuntimeOpenScope Begin(
            DatabaseOptions options,
            CSharpDbObservabilityOptions? observabilityOptions,
            StorageRuntimeDiagnosticsProvenance provenance,
            bool recoveryApplicable)
        {
            CSharpDbRuntimeDiagnosticsState? runtimeState =
                options.RuntimeDiagnosticsState;
            bool ownsRuntimeState = false;
            if (runtimeState is null && observabilityOptions is not null)
            {
                runtimeState = new CSharpDbRuntimeDiagnosticsState(
                    observabilityOptions);
                ownsRuntimeState = true;
            }

            StorageRuntimeDiagnostics.Registration? registration =
                provenance == StorageRuntimeDiagnosticsProvenance.BuiltIn
                    ? StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                        runtimeState,
                        recoveryApplicable)
                    : null;
            StorageEngineOptions storageOptions = options.StorageEngineOptions;
            IStorageRuntimeDiagnosticsObserver? observer =
                provenance == StorageRuntimeDiagnosticsProvenance.BuiltIn
                    ? registration?.Observer
                    : null;
            if (!ReferenceEquals(
                    storageOptions.RuntimeDiagnosticsObserver,
                    observer))
            {
                try
                {
                    storageOptions =
                        storageOptions.WithRuntimeDiagnosticsObserver(observer);
                }
                catch
                {
                    registration?.Dispose();
                    registration = null;
                    storageOptions = options.StorageEngineOptions;
                }
            }

            return new StorageRuntimeOpenScope(
                runtimeState,
                ownsRuntimeState,
                provenance,
                storageOptions,
                registration);
        }

        internal void TransferOwnership()
            => Volatile.Write(ref _transferred, 1);

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _transferred, 1) != 0)
                return;

            Registration?.Dispose();
            if (_ownsRuntimeState)
                RuntimeState?.Dispose();
        }
    }

    private async ValueTask UpgradeLegacyRowVersionAllocatorAsync(CancellationToken ct)
    {
        if (_catalog.RowVersionHighWater != 0)
            return;

        // Regenerated values must differ from every preview-era per-row token so
        // outstanding concurrency values are invalidated by the semantics upgrade.
        ulong legacyHighWater = await _planner.GetDatabaseRowVersionHighWaterAsync(ct);
        lock (_sharedRowVersionGate)
            _sharedRowVersionHighWater = Math.Max(_sharedRowVersionHighWater, legacyHighWater);

        await _writeOperationGate.WaitAsync(ct);
        bool transactionStarted = false;
        try
        {
            await _pager.BeginTransactionAsync(ct);
            transactionStarted = true;
            bool regenerated = await _planner.RegenerateLegacyRowVersionTokensAsync(ct);
            if (!regenerated)
            {
                await _pager.RollbackAsync(ct);
                transactionStarted = false;
                return;
            }

            ulong regeneratedHighWater;
            lock (_sharedRowVersionGate)
                regeneratedHighWater = _sharedRowVersionHighWater;
            await _catalog.PersistRowVersionHighWaterAsync(regeneratedHighWater, ct);

            PagerCommitResult commit = await BeginCommitWithCatalogSyncAsync(ct);
            transactionStarted = false;
            await WaitForCommitOrRecoverAsync(commit);
        }
        catch
        {
            if (transactionStarted)
            {
                try { await _pager.RollbackAsync(CancellationToken.None); } catch { }
            }
            throw;
        }
        finally
        {
            _writeOperationGate.Release();
        }
    }

    /// <summary>
    /// Execute a SQL statement. Returns a QueryResult with rows (for SELECT) or affected count (for DML/DDL).
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken ct = default)
    {
        QueryObservability? observability = _queryObservability;
        return observability is null
            ? ExecuteSqlCoreAsync(sql, ct)
            : ExecuteObservedSqlAsync(observability, sql, sql, ct);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        string sql,
        string? observabilitySql,
        CancellationToken ct = default)
    {
        QueryObservability? observability = _queryObservability;
        return observability is null
            ? ExecuteSqlCoreAsync(sql, ct)
            : ExecuteObservedSqlAsync(observability, sql, observabilitySql, ct);
    }

    private ValueTask<QueryResult> ExecuteObservedSqlAsync(
        QueryObservability observability,
        string sql,
        string? observabilitySql,
        CancellationToken ct)
    {
        IQueryExecutionObservation? operation =
            observability.StartExecution(
                observabilitySql,
                allowLeanRuntime: !LooksLikeInsert(sql));
        if (operation is null)
            return ExecuteSqlCoreAsync(sql, ct);

        IQueryPlanRuntimeObserver? explicitPlanObserver =
            operation.ExplicitPlanObserver;
        try
        {
            if (explicitPlanObserver is not null &&
                CanExecuteScopeFreeSimpleRead())
            {
                if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var lookup) &&
                    !(_temporaryTables.HasAnyTableContext &&
                      _planner.HasTemporaryTable(lookup.TableName)))
                {
                    return ObserveQueryAsync(
                        operation,
                        (
                            Target: this,
                            Operation: operation,
                            Lookup: lookup,
                            PlanObserver: explicitPlanObserver,
                            CancellationToken: ct),
                        static state => state.Target.ExecuteScopeFreePrimaryKeyLookupAsync(
                            state.Operation,
                            state.Lookup,
                            state.PlanObserver,
                            state.CancellationToken),
                        enterOperationScope: false);
                }

                if (_statementCache.TryGet(sql, out Statement cachedStatement) &&
                    cachedStatement is SelectStatement cachedSelect &&
                    _planner.CanExecuteSimpleReadWithExplicitObserver(cachedSelect))
                {
                    return ObserveQueryAsync(
                        operation,
                        (
                            Target: this,
                            Statement: cachedSelect,
                            PlanObserver: explicitPlanObserver,
                            CancellationToken: ct),
                        static state => state.Target.ExecuteScopeFreeSimpleReadAsync(
                            state.Statement,
                            state.PlanObserver,
                            state.CancellationToken),
                        enterOperationScope: false);
                }
            }
        }
        catch
        {
            // Eligibility probing is an optimization only. Re-run the exact
            // operation through the ordinary observed path so parse, catalog,
            // and cache failures retain their original query semantics and
            // cannot strand an active diagnostics lease.
        }

        return ObserveQueryAsync(
            operation,
            (Target: this, Sql: sql, CancellationToken: ct),
            static state => state.Target.ExecuteSqlCoreAsync(
                state.Sql,
                state.CancellationToken));
    }

    private bool CanExecuteScopeFreeSimpleRead()
        => !_inTransaction &&
           _pendingCollectionCatalogMutations.Count == 0 &&
           _catalog.SchemaVersion == _observedSchemaVersion;

    private ValueTask<QueryResult> ExecuteScopeFreePrimaryKeyLookupAsync(
        IQueryExecutionObservation operation,
        SimplePrimaryKeyLookupSql lookup,
        IQueryPlanRuntimeObserver planObserver,
        CancellationToken ct)
    {
        ValueTask<QueryResult?> directResult =
            _planner.TryExecuteSimplePrimaryKeyLookupDirectAsync(
                lookup,
                ct,
                planObserver,
                cachedOnly: true);
        if (!directResult.IsCompleted)
            return CompleteUnexpectedScopeFreeLookupAsync(directResult);

        QueryResult? result = directResult.GetAwaiter().GetResult();
        if (result is not null)
        {
            return ValueTask.FromResult(result);
        }

        // cachedOnly guarantees that this probe has not started I/O. Any
        // ineligible or cache-miss case is reinvoked under the ordinary
        // ambient frame so async, nested, and fallback paths keep their full
        // correlation semantics.
        return ExecuteScopedPrimaryKeyLookupAsync(operation, lookup, ct);
    }

    private static async ValueTask<QueryResult> CompleteUnexpectedScopeFreeLookupAsync(
        ValueTask<QueryResult?> directResult)
        => await directResult ?? throw new InvalidOperationException(
            "A scope-free primary-key lookup unexpectedly required fallback I/O.");

    private ValueTask<QueryResult> ExecuteScopedPrimaryKeyLookupAsync(
        IQueryExecutionObservation operation,
        SimplePrimaryKeyLookupSql lookup,
        CancellationToken ct)
    {
        using IDisposable scope = operation.EnterScope();
        return ExecuteSimplePrimaryKeyLookupAsync(lookup, ct);
    }

    private ValueTask<QueryResult> ExecuteScopeFreeSimpleReadAsync(
        SelectStatement statement,
        IQueryPlanRuntimeObserver planObserver,
        CancellationToken ct)
        => _planner.ExecuteSimpleReadAsync(statement, planObserver, ct);

    private async ValueTask<QueryResult> ExecuteSqlCoreAsync(string sql, CancellationToken ct)
    {
        InvalidateCachesIfSchemaChanged();
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);

        if (LooksLikeInsert(sql) && Parser.TryParseSimpleInsert(sql, out var simpleInsert))
            return await ExecuteSimpleInsertAsync(simpleInsert, ct);

        if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var simpleLookup))
            return await ExecuteSimplePrimaryKeyLookupAsync(simpleLookup, ct);

        if (_statementCache.TryGetOrMarkBypass(sql, out var cachedStmt, out _))
            return await ExecuteStatementAsync(cachedStmt, ct);

        var stmt = ParseCached(sql);
        return await ExecuteStatementAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteSimplePrimaryKeyLookupAsync(
        SimplePrimaryKeyLookupSql lookup,
        CancellationToken ct)
    {
        if (_temporaryTables.HasAnyTableContext && _planner.HasTemporaryTable(lookup.TableName))
        {
            var tempLookupStatement = Parser.Parse(SelectToSql(lookup));
            return await ExecuteStatementAsync(tempLookupStatement, ct);
        }

        var directResult = await _planner.TryExecuteSimplePrimaryKeyLookupDirectAsync(lookup, ct);
        if (directResult != null)
            return directResult;

        if (_planner.TryExecuteSimplePrimaryKeyLookup(lookup, out var fastResult))
            return fastResult;

        var statement = Parser.Parse(SelectToSql(lookup));
        return await ExecuteStatementAsync(statement, ct);
    }

    private static string SelectToSql(SimplePrimaryKeyLookupSql lookup)
    {
        var projection = lookup.SelectStar
            ? "*"
            : string.Join(", ", lookup.ProjectionColumns);

        var predicate = $"{lookup.PredicateColumn} = {LiteralToSql(lookup.PredicateLiteral.Type == DbType.Null ? DbValue.FromInteger(lookup.LookupValue) : lookup.PredicateLiteral)}";
        if (lookup.HasResidualPredicate)
            predicate += $" AND {lookup.ResidualPredicateColumn} = {LiteralToSql(lookup.ResidualPredicateLiteral)}";

        return $"SELECT {projection} FROM {lookup.TableName} WHERE {predicate}";
    }

    private static string LiteralToSql(DbValue value)
    {
        return value.Type switch
        {
            DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
            DbType.Real => value.AsReal.ToString(CultureInfo.InvariantCulture),
            DbType.Decimal => value.AsDecimal.ToString(CultureInfo.InvariantCulture),
            DbType.Text => $"'{value.AsText.Replace("'", "''", StringComparison.Ordinal)}'",
            DbType.Blob => $"X'{Convert.ToHexString(value.AsBlob)}'",
            _ => "NULL",
        };
    }

    /// <summary>
    /// Execute a pre-parsed SQL statement. Used by prepared command paths
    /// to bypass SQL text parsing on repeated executions.
    /// </summary>
    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken ct = default)
        => ExecuteObservedStatementAsync(statement, suppliedFingerprint: null, ct);

    internal ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        QueryFingerprint? suppliedFingerprint,
        CancellationToken ct = default)
        => ExecuteObservedStatementAsync(statement, suppliedFingerprint, ct);

    private ValueTask<QueryResult> ExecuteObservedStatementAsync(
        Statement statement,
        QueryFingerprint? suppliedFingerprint,
        CancellationToken ct)
    {
        IQueryExecutionObservation? operation =
            _queryObservability?.StartExecution(
                sql: null,
                suppliedFingerprint,
                allowLeanRuntime: statement is SelectStatement);
        if (operation is null)
            return ExecuteStatementRootCoreAsync(statement, ct);

        IQueryPlanRuntimeObserver? explicitPlanObserver =
            operation.ExplicitPlanObserver;
        try
        {
            if (explicitPlanObserver is not null &&
                CanExecuteScopeFreeSimpleRead() &&
                statement is SelectStatement select &&
                _planner.CanExecuteSimpleReadWithExplicitObserver(select))
            {
                return ObserveQueryAsync(
                    operation,
                    (
                        Target: this,
                        Statement: select,
                        PlanObserver: explicitPlanObserver,
                        CancellationToken: ct),
                    static state => state.Target.ExecuteScopeFreeSimpleReadAsync(
                        state.Statement,
                        state.PlanObserver,
                        state.CancellationToken),
                    enterOperationScope: false);
            }
        }
        catch
        {
            // Classifier/catalog failures are observed by the ordinary path.
        }

        return ObserveQueryAsync(
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
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);
        return await ExecuteStatementAsync(statement, ct);
    }

    internal ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken ct = default)
        => ExecuteObservedSimpleInsertAsync(insert, suppliedFingerprint: null, ct);

    internal ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        QueryFingerprint? suppliedFingerprint,
        CancellationToken ct = default)
        => ExecuteObservedSimpleInsertAsync(insert, suppliedFingerprint, ct);

    private ValueTask<QueryResult> ExecuteObservedSimpleInsertAsync(
        SimpleInsertSql insert,
        QueryFingerprint? suppliedFingerprint,
        CancellationToken ct)
    {
        IQueryExecutionObservation? operation =
            _queryObservability?.StartExecution(sql: null, suppliedFingerprint);
        return operation is null
            ? ExecuteSimpleInsertRootCoreAsync(insert, ct)
            : ObserveQueryAsync(
                operation,
                (Target: this, Insert: insert, CancellationToken: ct),
                static state => state.Target.ExecuteSimpleInsertRootCoreAsync(
                    state.Insert,
                    state.CancellationToken));
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertRootCoreAsync(
        SimpleInsertSql insert,
        CancellationToken ct)
    {
        await FlushPendingCollectionCatalogMutationsBeforeSqlAsync(ct);
        return await ExecuteSimpleInsertAsync(insert, ct);
    }

    internal IQueryExecutionObservation? StartQueryObservability(
        string? sql,
        QueryFingerprint? suppliedFingerprint = null)
        => _queryObservability?.StartExecution(sql, suppliedFingerprint);

    internal LifecycleOperation? StartLifecycleObservability(
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbOperationClass operationClass)
        => LifecycleObservability.Start(
            _observabilityOptions,
            definition,
            operationClass,
            _runtimeDiagnosticsState);

    internal LifecycleOperation? StartLifecycleObservabilityExact(
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationContext context,
        CSharpDbActivityOperation? activityOperation = null)
        => LifecycleObservability.StartExact(
            _observabilityOptions,
            definition,
            operationClass,
            context,
            activityOperation,
            _runtimeDiagnosticsState);

    internal static ValueTask<QueryResult> ObserveQueryAsync<TState>(
        IQueryExecutionObservation operation,
        TState state,
        Func<TState, ValueTask<QueryResult>> execution,
        bool enterOperationScope = true)
    {
        using IDisposable? scope = enterOperationScope
            ? operation.EnterScope()
            : null;
        try
        {
            operation.MarkExecuting();
            ValueTask<QueryResult> pendingResult = execution(state);
            if (!pendingResult.IsCompletedSuccessfully)
            {
                return CompleteObservedQueryAsync(
                    operation,
                    pendingResult,
                    enterOperationScope);
            }

            return ValueTask.FromResult(operation.Observe(pendingResult.Result));
        }
        catch (Exception exception)
        {
            try
            {
                operation.Fail(exception);
            }
            catch (Exception observationException)
            {
                return ValueTask.FromException<QueryResult>(observationException);
            }

            return ValueTask.FromException<QueryResult>(exception);
        }
    }

    private static async ValueTask<QueryResult> CompleteObservedQueryAsync(
        IQueryExecutionObservation operation,
        ValueTask<QueryResult> pendingResult,
        bool enterOperationScope)
    {
        // The execution's continuation already captured the invocation scope.
        // Re-enter here so terminal observation and failures remain attributed
        // after the non-async caller restores its ambient frame.
        using IDisposable? scope = enterOperationScope
            ? operation.EnterScope()
            : null;
        try
        {
            return operation.Observe(await pendingResult);
        }
        catch (Exception exception)
        {
            operation.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Prepare a reusable writable-column insert batch for a single table.
    /// Database-generated ROWVERSION columns are omitted from the required values.
    /// The batch accepts DbValue rows and executes them through the simple insert path.
    /// </summary>
    public InsertBatch PrepareInsertBatch(string tableName, int initialCapacity = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentOutOfRangeException.ThrowIfNegative(initialCapacity);

        InvalidateCachesIfSchemaChanged();
        var schema = _catalog.GetTable(tableName);
        if (schema == null)
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        string[]? writableColumnNames = schema.Columns.Any(static column => column.IsRowVersion)
            ? schema.Columns
                .Where(static column => !column.IsRowVersion)
                .Select(static column => column.Name)
                .ToArray()
            : null;
        int writableColumnCount = writableColumnNames?.Length ?? schema.Columns.Count;
        return new InsertBatch(
            this,
            tableName,
            writableColumnCount,
            writableColumnNames,
            _catalog.SchemaVersion,
            initialCapacity);
    }

    private ValueTask<QueryResult> ExecuteStatementAsync(Statement stmt, CancellationToken ct)
    {
        if (SqlStatementClassifier.IsReadOnly(stmt))
            return _planner.ExecuteAsync(stmt, ct);

        return ExecuteWriteStatementAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteWriteStatementAsync(Statement stmt, CancellationToken ct)
    {
        if (_planner.ShouldExecuteInSessionTemporaryState(stmt))
            return await _planner.ExecuteAsync(stmt, ct);

        if (_inTransaction)
        {
            return await ExecuteExplicitWriteAsync(
                token => stmt is InsertStatement explicitInsert
                    ? _planner.ExecuteInsertAsync(explicitInsert, persistRootChanges: false, token)
                    : _planner.ExecuteAsync(stmt, token),
                ct);
        }

        if (stmt is InsertStatement insert)
        {
            if (ImplicitInsertExecutionMode == ImplicitInsertExecutionMode.ConcurrentWriteTransactions)
                return await ExecuteConcurrentImplicitInsertAsync(insert, ct);

            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    return await ExecuteImplicitInsertCoreAsync(insert, ct);
                }
                catch (CSharpDbConflictException) when (attempt < DefaultImplicitConflictRetries)
                {
                    await DelayImplicitConflictRetryAsync(attempt, ct);
                }
            }
        }

        return await ExecuteImplicitWriteStatementCoreAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteExplicitWriteAsync(
        Func<CancellationToken, ValueTask<QueryResult>> action,
        CancellationToken ct)
    {
        if (_explicitTransactionFailed)
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                "The transaction is aborted because an earlier write failed; roll it back before issuing another write.");
        }

        try
        {
            return await action(ct);
        }
        catch
        {
            _explicitTransactionFailed = true;
            throw;
        }
    }

    private ValueTask<QueryResult> ExecuteImplicitWriteStatementCoreAsync(Statement stmt, CancellationToken ct) =>
        RunWriteTransactionCoreAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(stmt, token),
            ImplicitAutoCommitWriteTransactionOptions,
            observeLifecycle: false,
            ct);

    private ValueTask<QueryResult> ExecuteConcurrentImplicitInsertAsync(InsertStatement insert, CancellationToken ct) =>
        RunWriteTransactionCoreAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(insert, token),
            ImplicitAutoCommitWriteTransactionOptions,
            observeLifecycle: false,
            ct);

    private async ValueTask<QueryResult> ExecuteImplicitInsertCoreAsync(InsertStatement insert, CancellationToken ct)
    {
        QueryResult result;
        PagerCommitResult commit = PagerCommitResult.Completed;
        IDisposable? writeScope = null;
        try
        {
            await EnsureSharedRowVersionReservationAsync(ct);
            writeScope = await AcquireWriteOperationScopeAsync(ct);
            await _pager.BeginTransactionAsync(ct);
            result = await _planner.ExecuteInsertAsync(
                insert,
                persistRootChanges: false,
                ct);
            commit = await BeginCommitForTableWithCatalogSyncAsync(insert.TableName, ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
        finally
        {
            if (writeScope is not null)
            {
                try
                {
                    await CompleteImplicitCommitAsync(commit, ct);
                }
                finally
                {
                    writeScope.Dispose();
                }
            }
        }

        return result;
    }

    private async ValueTask<QueryResult> ExecuteSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct)
    {
        if (_temporaryTables.HasAnyTableContext && _planner.HasTemporaryTable(insert.TableName))
        {
            return await _planner.ExecuteSimpleInsertAsync(
                insert,
                persistRootChanges: false,
                ct);
        }

        if (_inTransaction)
        {
            return await ExecuteExplicitWriteAsync(
                token => _planner.ExecuteSimpleInsertAsync(
                    insert,
                    persistRootChanges: false,
                    token),
                ct);
        }

        if (ImplicitInsertExecutionMode == ImplicitInsertExecutionMode.ConcurrentWriteTransactions)
            return await ExecuteConcurrentImplicitSimpleInsertAsync(insert, ct);

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await ExecuteImplicitSimpleInsertCoreAsync(insert, ct);
            }
            catch (CSharpDbConflictException) when (attempt < DefaultImplicitConflictRetries)
            {
                await DelayImplicitConflictRetryAsync(attempt, ct);
            }
        }
    }

    private async ValueTask<QueryResult> ExecuteImplicitSimpleInsertCoreAsync(SimpleInsertSql insert, CancellationToken ct)
    {
        QueryResult result;
        PagerCommitResult commit = PagerCommitResult.Completed;
        IDisposable? writeScope = null;
        try
        {
            await EnsureSharedRowVersionReservationAsync(ct);
            writeScope = await AcquireWriteOperationScopeAsync(ct);
            await _pager.BeginTransactionAsync(ct);

            result = await _planner.ExecuteSimpleInsertAsync(
                insert,
                persistRootChanges: false,
                ct);

            commit = await BeginCommitForTableWithCatalogSyncAsync(insert.TableName, ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
        finally
        {
            if (writeScope is not null)
            {
                try
                {
                    await CompleteImplicitCommitAsync(commit, ct);
                }
                finally
                {
                    writeScope.Dispose();
                }
            }
        }

        return result;
    }

    private ValueTask<QueryResult> ExecuteConcurrentImplicitSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct) =>
        RunWriteTransactionCoreAsync(
            (transaction, token) => transaction.ExecuteImplicitAutoCommitAsync(insert, token),
            ImplicitAutoCommitWriteTransactionOptions,
            observeLifecycle: false,
            ct);

    /// <summary>
    /// Begin an explicit transaction.
    /// </summary>
    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Transaction already active.");

        LifecycleOperation? operation = StartLifecycleObservability(
            CSharpDbLogEvents.TransactionCompleted,
            CSharpDbOperationClass.Transaction);
        try
        {
            await FlushPendingAdvisoryStatisticsAsync(ct);
            await EnsureSharedRowVersionReservationAsync(ct);
            await _writeOperationGate.WaitAsync(ct);
            try
            {
                await _pager.BeginTransactionAsync(ct);
            }
            catch
            {
                _writeOperationGate.Release();
                throw;
            }

            _inTransaction = true;
            _explicitTransactionFailed = false;
            _explicitTransactionObservation = operation;
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    /// <summary>
    /// Commit the current transaction.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");
        if (_explicitTransactionFailed)
        {
            try
            {
                await RollbackExplicitTransactionCoreAsync(CancellationToken.None);
            }
            catch (Exception rollbackException)
            {
                if (!_inTransaction)
                    CompleteExplicitTransactionObservation(rollbackException);
                throw;
            }

            var transactionException = new CSharpDbException(
                ErrorCode.Unknown,
                "The transaction was rolled back because an earlier write failed.");
            CompleteExplicitTransactionObservation(transactionException);
            throw transactionException;
        }

        try
        {
            PagerCommitResult commit;
            try
            {
                await FlushPendingCollectionCatalogMutationsAsync(ct);
                commit = await BeginCommitWithCatalogSyncAsync(ct);
            }
            catch
            {
                ClearPendingCollectionCatalogMutations();
                await RecoverCatalogStateAfterFailedCommitAsync();
                _inTransaction = false;
                _explicitTransactionFailed = false;
                ReleaseExplicitTransactionWriteGate();
                throw;
            }

            _inTransaction = false;
            _explicitTransactionFailed = false;
            ReleaseExplicitTransactionWriteGate();
            await WaitForCommitOrRecoverAsync(commit);
            await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
            CompleteExplicitTransactionObservation(exception: null);
        }
        catch (Exception exception)
        {
            if (!_inTransaction)
                CompleteExplicitTransactionObservation(exception);
            throw;
        }
    }

    /// <summary>
    /// Rollback the current transaction.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction.");

        try
        {
            await RollbackExplicitTransactionCoreAsync(ct);
            CompleteExplicitTransactionObservation(exception: null);
        }
        catch (Exception exception)
        {
            if (!_inTransaction)
                CompleteExplicitTransactionObservation(exception);
            throw;
        }
    }

    private async ValueTask RollbackExplicitTransactionCoreAsync(CancellationToken ct)
    {
        await _pager.RollbackAsync(ct);
        try
        {
            await _catalog.ReloadAsync(ct);
            RefreshCachedCollectionsFromCatalog();
            _statementCache.Clear();
        }
        finally
        {
            ClearPendingCollectionCatalogMutations();
            _inTransaction = false;
            _explicitTransactionFailed = false;
            ReleaseExplicitTransactionWriteGate();
        }
    }

    private void CompleteExplicitTransactionObservation(Exception? exception)
    {
        LifecycleOperation? operation = Interlocked.Exchange(
            ref _explicitTransactionObservation,
            null);
        if (exception is null)
            operation?.Succeed();
        else
            operation?.Fail(exception);
    }

    /// <summary>
    /// Manually trigger a WAL checkpoint.
    /// </summary>
    public ValueTask CheckpointAsync(CancellationToken ct = default)
        => CheckpointCoreAsync(
            observation: null,
            allowStateFallback: true,
            ct: ct);

    internal ValueTask CheckpointFromClientAsync(
        MaintenanceObservation? observation,
        CancellationToken ct = default)
        => CheckpointCoreAsync(
            observation: observation,
            allowStateFallback: observation is null,
            ct: ct);

    private async ValueTask CheckpointCoreAsync(
        MaintenanceObservation? observation,
        bool allowStateFallback,
        CancellationToken ct)
    {
        MaintenanceObservation? operation = observation;
        if (operation is null && allowStateFallback)
            operation = StartDirectCheckpointObservation();
        using IDisposable? operationScope = operation is null
            ? null
            : operation.EnterScope();
        try
        {
            operation?.SetPhase(MaintenanceOperationPhase.Checkpointing);
            await _pager.CheckpointAsync(ct);
            await PersistHybridStateAsync(HybridPersistenceTriggers.Checkpoint, ct);
            operation?.Succeed();
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    private MaintenanceObservation? StartDirectCheckpointObservation()
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed)
            return null;

        CSharpDbRuntimeDiagnosticsState? runtimeState =
            _runtimeDiagnosticsState;
        if (runtimeState?.IsEnabled == true)
        {
            try
            {
                CSharpDbActivityOperation? activityOperation = null;
                CSharpDbOperationContext context;
                if (CSharpDbActivityOperation.ShouldStart(
                        runtimeState.TracingEnabled))
                {
                    activityOperation = CSharpDbActivityOperation.Start(
                        CSharpDbOperationClass.Checkpoint,
                        runtimeState,
                        static state => CreateDirectCheckpointContext(state),
                        out context);
                }
                else
                {
                    context = CreateDirectCheckpointContext(runtimeState);
                }
                MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation?
                    runtimeOperation = null;
                try
                {
                    runtimeOperation = MaintenanceRuntimeDiagnostics
                        .GetOrCreate(runtimeState)
                        ?.TryStart(
                            context,
                            MaintenanceOperationKind.Checkpoint,
                            MaintenanceOperationPhase.Checkpointing);
                }
                catch
                {
                    // Tracing and lifecycle logging remain independently
                    // useful if the bounded runtime registry is retiring.
                }
                LifecycleOperation? lifecycleOperation =
                    StartLifecycleObservabilityExact(
                        CSharpDbLogEvents.CheckpointCompleted,
                        CSharpDbOperationClass.Checkpoint,
                        context,
                        activityOperation);
                if (runtimeOperation is not null ||
                    lifecycleOperation is not null ||
                    activityOperation is not null)
                {
                    return new MaintenanceObservation(
                        context,
                        runtimeOperation,
                        lifecycleOperation,
                        activityOperation);
                }
            }
            catch
            {
                // Runtime maintenance diagnostics remain best-effort. Fall
                // through so lifecycle-only checkpoint logging is preserved.
            }
        }

        LifecycleOperation? fallbackLifecycle =
            StartLifecycleObservability(
                CSharpDbLogEvents.CheckpointCompleted,
                CSharpDbOperationClass.Checkpoint);
        return fallbackLifecycle is null
            ? null
            : new MaintenanceObservation(
                fallbackLifecycle.Context,
                runtimeOperation: null,
                fallbackLifecycle);
    }

    private static CSharpDbOperationContext CreateDirectCheckpointContext(
        CSharpDbRuntimeDiagnosticsState runtimeState)
    {
        CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
        return parent is null
            ? CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Checkpoint,
                CSharpDbOperationScope.CurrentTransport,
                runtimeState.DatabaseAlias,
                CSharpDbOperationScope.CurrentSessionId,
                timeProvider: runtimeState.TimeProvider)
            : CSharpDbOperationContext.CreateRequest(
                parent,
                CSharpDbOperationClass.Checkpoint,
                runtimeState.TimeProvider);
    }

    /// <summary>
    /// Save the current committed database state to an on-disk database file.
    /// </summary>
    public async ValueTask SaveToFileAsync(string filePath, CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new InvalidOperationException("Cannot save while an explicit transaction is active.");

        await SaveToFileAsync(filePath, writeScopeHeld: false, ct);
    }

    internal async ValueTask SaveToFileAsync(
        string filePath,
        bool writeScopeHeld,
        CancellationToken ct = default)
        => await SaveToFileAsync(
            filePath,
            writeScopeHeld,
            progressObserver: null,
            ct: ct);

    internal async ValueTask SaveToFileAsync(
        string filePath,
        bool writeScopeHeld,
        IPagerSaveToFileProgressObserver? progressObserver,
        CancellationToken ct = default)
    {
        IDisposable? writeScope = null;
        try
        {
            if (!writeScopeHeld)
                writeScope = await AcquireWriteOperationScopeAsync(ct);

            await FlushPendingAdvisoryStatisticsAsync(ct, writeScopeHeld: true);
            await _pager.SaveToFileAsync(filePath, ct, progressObserver);
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    /// <summary>
    /// Create an independent reader that sees a snapshot of the database
    /// at the current point in time. The reader does not block writers.
    /// Caller must dispose the returned ReaderSession when done.
    /// </summary>
    public ReaderSession CreateReaderSession()
        => CreateReaderSession(
            CaptureSnapshotRowCounts(),
            allowCurrentCatalogRowCounts: true);

    internal ReaderSession CreateReaderSession(
        IReadOnlyDictionary<string, long> snapshotRowCounts,
        bool allowCurrentCatalogRowCounts)
    {
        ArgumentNullException.ThrowIfNull(snapshotRowCounts);

        var snapshot = _pager.AcquireReaderSnapshot();
        return new ReaderSession(
            _pager,
            _catalog,
            _recordSerializer,
            snapshot,
            _statementCache,
            _functions,
            _planner.AdaptiveQueryReoptimization,
            _planner.WindowExecution,
            _queryObservability,
            _queryPlanRuntimeObserver,
            snapshotRowCounts,
            allowCurrentCatalogRowCounts);
    }

    internal IReadOnlyDictionary<string, long> CaptureReaderSnapshotRowCounts() =>
        CaptureSnapshotRowCounts();

    private Dictionary<string, long> CaptureSnapshotRowCounts()
    {
        var snapshotRowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (string tableName in _catalog.GetTableNames())
        {
            if (_catalog.TryGetExactTableRowCount(tableName, out long rowCount))
                snapshotRowCounts[tableName] = rowCount;
        }

        return snapshotRowCounts;
    }

    /// <summary>
    /// Returns the names of all tables in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetTableNames() => _catalog.GetTableNames();

    internal ValueTask ClearTemporaryTablesAsync() => _temporaryTables.ClearAsync();

    internal IDisposable EnterTemporaryTableSessionScope(object sessionKey) =>
        _temporaryTables.EnterSessionScope(sessionKey);

    internal bool HasTemporaryTablesForCurrentSession =>
        _temporaryTables.GetTableNames().Count != 0;

    internal bool HasTemporaryTableContextForCurrentSession =>
        _temporaryTables.HasCurrentSessionContext;

    internal async ValueTask ResetReusableSessionStateAsync()
    {
        if (_inTransaction || _explicitTransactionFailed)
            throw new InvalidOperationException("Cannot reuse a database handle with an active or failed transaction.");
        if (ActiveReaderCount != 0)
            throw new InvalidOperationException("Cannot reuse a database handle while snapshot readers are active.");

        // A physical close flushes deferred catalog state and destroys every
        // temporary-table context. Warm handle handoff must preserve that
        // boundary before the handle changes logical owners.
        await FlushPendingAdvisoryStatisticsAsync(CancellationToken.None);
        await _temporaryTables.ClearAsync();
    }

    internal uint GetTableRootPage(string tableName) => _catalog.GetTableRootPage(tableName);

    /// <summary>
    /// Returns the schema for a table, or null if not found.
    /// </summary>
    public TableSchema? GetTableSchema(string tableName) => _catalog.GetTable(tableName);

    /// <summary>
    /// Applies trusted stable identities to a structurally equivalent table.
    /// Used only by exact recovery while an explicit transaction is active.
    /// </summary>
    internal async ValueTask ApplyTableSchemaIdentitiesAsync(
        string tableName,
        TableSchema identitySource,
        CancellationToken ct = default)
    {
        if (!_inTransaction)
        {
            throw new InvalidOperationException(
                "Schema identities can only be applied inside an explicit transaction.");
        }

        await _catalog.ApplyTableSchemaIdentitiesAsync(
            tableName,
            identitySource,
            ct);
        _statementCache.Clear();
        _observedSchemaVersion = _catalog.SchemaVersion;
    }

    /// <summary>
    /// Returns all indexes defined in the database.
    /// </summary>
    public IReadOnlyCollection<IndexSchema> GetIndexes() => _catalog.GetIndexes();

    /// <summary>
    /// Ensure a full-text index exists for the supplied SQL table and TEXT columns.
    /// The index is stored inside the regular catalog/index subsystem and backfilled
    /// in the same transaction that creates it.
    /// </summary>
    public async ValueTask EnsureFullTextIndexAsync(
        string indexName,
        string tableName,
        IReadOnlyList<string> columns,
        FullTextIndexOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(columns);

        using var writeScope = _inTransaction
            ? WriteOperationScope.NoOp
            : await AcquireWriteOperationScopeAsync(ct);

        string[] normalizedColumns = columns
            .Where(static column => !string.IsNullOrWhiteSpace(column))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (normalizedColumns.Length == 0)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Full-text index must reference at least one TEXT column.");

        FullTextIndexOptions resolvedOptions = options ?? new FullTextIndexOptions();

        InvalidateCachesIfSchemaChanged();

        var existing = _catalog.GetIndex(indexName);
        if (existing != null)
        {
            if (existing.Kind == IndexKind.FullText)
            {
                if (FullTextIndexCatalog.MatchesDefinition(existing, tableName, normalizedColumns, resolvedOptions))
                {
                    await EnsureFullTextInternalStoresAsync(existing, ct);
                    return;
                }

                throw new CSharpDbException(
                    ErrorCode.TableAlreadyExists,
                    $"Full-text index '{indexName}' already exists with a different definition.");
            }

            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Index '{indexName}' already exists.");
        }

        if (_inTransaction)
        {
            throw new InvalidOperationException(
                "Full-text indexes cannot be created while an explicit transaction is active.");
        }

        if (_planner.HasTemporaryTable(tableName))
            throw new CSharpDbException(ErrorCode.SyntaxError, "Temporary tables do not support full-text indexes in V1.");

        TableSchema tableSchema = _catalog.GetTable(tableName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        var logicalIndex = FullTextIndexCatalog.CreateLogicalSchema(
            indexName,
            tableName,
            normalizedColumns,
            resolvedOptions);

        if (!FullTextIndexMaintenance.TryResolveColumnIndices(logicalIndex, tableSchema, out _))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "Full-text indexes currently support only TEXT columns.");
        }

        bool createdLogicalIndex = false;
        try
        {
            await _pager.BeginTransactionAsync(ct);
            await _catalog.CreateIndexAsync(logicalIndex, ct);
            createdLogicalIndex = true;

            foreach (var internalIndex in FullTextIndexCatalog.CreateInternalSchemas(logicalIndex))
                await _catalog.CreateIndexAsync(internalIndex, ct);

            await FullTextIndexMaintenance.BackfillAsync(
                _catalog,
                tableSchema,
                logicalIndex,
                _recordSerializer,
                ct);

            PagerCommitResult commit = await BeginCommitForTableWithCatalogSyncAsync(tableName, ct);
            await commit.WaitAsync(ct);
        }
        catch
        {
            if (createdLogicalIndex)
            {
                try
                {
                    await _catalog.DropIndexAsync(indexName, ct);
                }
                catch
                {
                    // Best-effort cleanup before rollback.
                }
            }

            try
            {
                await _pager.RollbackAsync(ct);
                await _catalog.ReloadAsync(ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }
    }

    private async ValueTask EnsureFullTextInternalStoresAsync(IndexSchema logicalIndex, CancellationToken ct)
    {
        IndexSchema[] missingStores = FullTextIndexCatalog.CreateInternalSchemas(logicalIndex)
            .Where(schema => _catalog.GetIndex(schema.IndexName) == null)
            .ToArray();
        if (missingStores.Length == 0)
            return;

        if (_inTransaction)
        {
            throw new InvalidOperationException(
                "Full-text index storage cannot be upgraded while an explicit transaction is active.");
        }

        try
        {
            await _pager.BeginTransactionAsync(ct);
            for (int i = 0; i < missingStores.Length; i++)
                await _catalog.CreateIndexAsync(missingStores[i], ct);

            PagerCommitResult commit = await BeginCommitForTableWithCatalogSyncAsync(logicalIndex.TableName, ct);
            await commit.WaitAsync(ct);
        }
        catch
        {
            try
            {
                await _pager.RollbackAsync(ct);
                await _catalog.ReloadAsync(ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }
    }

    private async ValueTask EnsureFullTextInternalStoresOnOpenAsync(CancellationToken ct)
    {
        IndexSchema[] missingStores = _catalog.GetIndexes()
            .Where(static index => index.Kind == IndexKind.FullText)
            .SelectMany(FullTextIndexCatalog.CreateInternalSchemas)
            .Where(schema => _catalog.GetIndex(schema.IndexName) == null)
            .ToArray();
        if (missingStores.Length == 0)
            return;

        try
        {
            await _pager.BeginTransactionAsync(ct);
            for (int i = 0; i < missingStores.Length; i++)
                await _catalog.CreateIndexAsync(missingStores[i], ct);

            PagerCommitResult commit = await BeginCommitWithCatalogSyncAsync(ct);
            await commit.WaitAsync(ct);
            _observedSchemaVersion = _catalog.SchemaVersion;
        }
        catch
        {
            try
            {
                await _pager.RollbackAsync(ct);
                await _catalog.ReloadAsync(ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }
    }

    /// <summary>
    /// Run a basic term-intersection search against a previously created full-text index.
    /// Query text is tokenized with the index's stored options; all query terms must match.
    /// </summary>
    public async ValueTask<IReadOnlyList<FullTextSearchHit>> SearchAsync(
        string indexName,
        string query,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentException.ThrowIfNullOrWhiteSpace(query);

        InvalidateCachesIfSchemaChanged();

        IndexSchema indexSchema = _catalog.GetIndex(indexName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
        if (indexSchema.Kind != IndexKind.FullText)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Index '{indexName}' is not a full-text index.");
        }

        return await FullTextIndexReader.SearchAsync(_catalog, indexSchema, query, ct);
    }

    /// <summary>
    /// Returns all view names defined in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetViewNames() => _catalog.GetViewNames();

    /// <summary>
    /// Returns view SQL text by name, or null if the view does not exist.
    /// </summary>
    public string? GetViewSql(string viewName) => _catalog.GetViewSql(viewName);

    /// <summary>
    /// Returns all triggers defined in the database.
    /// </summary>
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _catalog.GetTriggers();

    /// <summary>
    /// Monotonic in-process token that advances on schema mutations (DDL).
    /// Useful for cache invalidation.
    /// </summary>
    public long SchemaVersion => _catalog.SchemaVersion;

    /// <summary>
    /// Planner select-plan cache counters, exposed for tests and benchmarks.
    /// </summary>
    internal readonly record struct SelectPlanCacheDiagnostics(
        long HitCount,
        long MissCount,
        long ReclassificationCount,
        long StoreCount,
        int EntryCount);

    /// <summary>
    /// Returns planner select-plan cache counters.
    /// Internal-only: intended for tests and benchmarks.
    /// </summary>
    internal SelectPlanCacheDiagnostics GetSelectPlanCacheDiagnostics()
    {
        var d = _planner.GetSelectPlanCacheDiagnostics();
        return new SelectPlanCacheDiagnostics(
            d.HitCount,
            d.MissCount,
            d.ReclassificationCount,
            d.StoreCount,
            d.EntryCount);
    }

    /// <summary>
    /// Resets planner select-plan cache counters.
    /// Internal-only: intended for tests and benchmarks.
    /// </summary>
    internal void ResetSelectPlanCacheDiagnostics()
        => _planner.ResetSelectPlanCacheDiagnostics();

    // ============ Document Collection API ============

    private const string CollectionPrefix = "_col_";
    private const string GeneratedCollectionCacheSuffix = "\u0001generated";

    /// <summary>
    /// Ensures that a real JsonElement document collection exists in the
    /// caller's active explicit transaction.
    /// </summary>
    internal async ValueTask EnsureJsonDocumentCollectionAsync(
        string collectionName,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        RequireCanonicalJsonCollectionTransaction();
        _ = await GetCollectionCoreAsync<JsonElement>(
            collectionName,
            generatedOnly: false,
            ct);
    }

    /// <summary>
    /// Inserts one already-canonical UTF-8 JSON value into a JsonElement
    /// collection in the caller's active explicit transaction.
    /// </summary>
    internal async ValueTask InsertCanonicalJsonDocumentAsync(
        string collectionName,
        string key,
        ReadOnlyMemory<byte> canonicalUtf8Json,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        ArgumentNullException.ThrowIfNull(key);
        RequireCanonicalJsonCollectionTransaction();

        ct.ThrowIfCancellationRequested();
        if (canonicalUtf8Json.Length >
            OrderedCanonicalJsonValidator.MaximumDocumentBytes)
        {
            throw new InvalidDataException(
                $"A canonical JSON migration document cannot exceed {OrderedCanonicalJsonValidator.MaximumDocumentBytes} bytes.");
        }
        byte[] canonicalSnapshot = canonicalUtf8Json.ToArray();
        OrderedCanonicalJsonValidator.Validate(canonicalSnapshot, ct);

        Collection<JsonElement> collection =
            await GetCollectionCoreAsync<JsonElement>(
                collectionName,
                generatedOnly: false,
                ct);
        await collection.InsertValidatedCanonicalJsonAsync(
            key,
            canonicalSnapshot,
            ct);
    }

    private void RequireCanonicalJsonCollectionTransaction()
    {
        if (!_inTransaction)
        {
            throw new InvalidOperationException(
                "Canonical JSON collection writes require an active explicit transaction.");
        }
    }

    /// <summary>
    /// Get or create a document collection with the given name.
    /// Collections are stored as internal tables with a "_col_" prefix.
    /// </summary>
    [RequiresUnreferencedCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
    public async ValueTask<Collection<T>> GetCollectionAsync<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    T>(
        string name,
        CancellationToken ct = default)
        => await GetCollectionCoreAsync<T>(name, generatedOnly: false, ct);

    /// <summary>
    /// Get or create a trim-safe typed collection with the given name.
    /// The document type must have a generated or manually registered collection model.
    /// </summary>
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2026",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2091",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    [UnconditionalSuppressMessage(
        "Aot",
        "IL3050",
        Justification = "GetGeneratedCollectionAsync<T> verifies that a generated or manually supplied collection model is registered before delegating to the shared Collection<T> construction path.")]
    public async ValueTask<GeneratedCollection<T>> GetGeneratedCollectionAsync<T>(
        string name,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (!CollectionModelRegistry.TryGet<T>(out _))
        {
            throw new InvalidOperationException(
                $"No generated collection model is registered for document type '{typeof(T).FullName ?? typeof(T).Name}'. " +
                "Annotate the type with [CollectionModel(typeof(YourJsonSerializerContext))] or register an ICollectionModel<T> before calling GetGeneratedCollectionAsync.");
        }

        return new GeneratedCollection<T>(await GetCollectionCoreAsync<T>(name, generatedOnly: true, ct));
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2091",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2026",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    [UnconditionalSuppressMessage(
        "Aot",
        "IL3050",
        Justification = "GetCollectionCoreAsync<T> is shared by the reflection-based and generated-model collection entry points. The generated-model entry point verifies that a generated or manually supplied collection model is registered before calling this method.")]
    private async ValueTask<Collection<T>> GetCollectionCoreAsync<T>(
        string name,
        bool generatedOnly,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        IDisposable? writeScope = _inTransaction
            ? WriteOperationScope.NoOp
            : await AcquireWriteOperationScopeAsync(ct);
        try
        {
            InvalidateCachesIfSchemaChanged();

            string catalogName = $"{CollectionPrefix}{name}";
            string cacheKey = BuildCollectionCacheKey(catalogName, generatedOnly);

            // Return cached instance if available
            if (_collectionCache.TryGetValue(cacheKey, out var cached))
                return (Collection<T>)cached;

            // Create the backing table if it doesn't exist
            if (_catalog.GetTable(catalogName) == null)
            {
                PagerCommitResult commit = PagerCommitResult.Completed;
                bool completeCommit = false;
                bool needsTx = !_inTransaction;
                if (needsTx) await _pager.BeginTransactionAsync(ct);
                try
                {
                    // Double-check after acquiring write lock
                    if (_catalog.GetTable(catalogName) == null)
                    {
                        var schema = new TableSchema
                        {
                            TableName = catalogName,
                            Columns = new[]
                            {
                                new ColumnDefinition { Name = "_key", Type = DbType.Text, Nullable = false },
                                new ColumnDefinition { Name = "_doc", Type = DbType.Text, Nullable = false },
                            }
                        };
                        await _catalog.CreateTableAsync(schema, ct);
                    }

                    if (needsTx)
                    {
                        commit = await BeginCommitWithCatalogSyncAsync(ct);
                        completeCommit = true;
                        writeScope?.Dispose();
                        writeScope = WriteOperationScope.NoOp;
                    }
                }
                catch
                {
                    if (needsTx)
                        await RecoverCatalogStateAfterFailedCommitAsync();
                    throw;
                }

                if (completeCommit)
                {
                    await WaitForCommitOrRecoverAsync(commit);
                    await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
                }
            }

            var tree = _catalog.GetTableTree(catalogName);
            var collection = new Collection<T>(
                _pager,
                _catalog,
                catalogName,
                tree,
                _recordSerializer,
                () => _inTransaction,
                RecordPendingCollectionCatalogMutation,
                GetPendingCollectionRowCountAsync,
                AcquireWriteOperationScopeAsync,
                BeginCommitForTableWithCatalogSyncAsync,
                ct => PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct),
                requireRegisteredFields: generatedOnly);
            _collectionCache[cacheKey] = collection;
            return collection;
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    private static string BuildCollectionCacheKey(string catalogName, bool generatedOnly)
        => generatedOnly
            ? catalogName + GeneratedCollectionCacheSuffix
            : catalogName;

    private void RemoveCachedCollection(string catalogName)
    {
        _collectionCache.Remove(catalogName);
        _collectionCache.Remove(catalogName + GeneratedCollectionCacheSuffix);
    }

    /// <summary>
    /// Returns the names of all document collections in the database.
    /// </summary>
    public IReadOnlyCollection<string> GetCollectionNames()
    {
        return _catalog.GetTableNames()
            .Where(n => n.StartsWith(CollectionPrefix, StringComparison.Ordinal))
            .Select(n => n[CollectionPrefix.Length..])
            .ToArray();
    }

    /// <summary>
    /// Drop a document collection and its collection indexes.
    /// </summary>
    public async ValueTask DropCollectionAsync(string name, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        IDisposable? writeScope = _inTransaction
            ? WriteOperationScope.NoOp
            : await AcquireWriteOperationScopeAsync(ct);
        try
        {
            InvalidateCachesIfSchemaChanged();

            string catalogName = $"{CollectionPrefix}{name}";
            if (_catalog.GetTable(catalogName) is null)
            {
                throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Collection '{name}' not found.");
            }

            PagerCommitResult commit = PagerCommitResult.Completed;
            bool completeCommit = false;
            bool needsTx = !_inTransaction;
            if (needsTx)
                await _pager.BeginTransactionAsync(ct);

            try
            {
                await _catalog.DropTableAsync(catalogName, ct);
                _pendingCollectionCatalogMutations.Remove(catalogName);
                RemoveCachedCollection(catalogName);
                _statementCache.Clear();
                _observedSchemaVersion = _catalog.SchemaVersion;

                if (needsTx)
                {
                    commit = await BeginCommitWithCatalogSyncAsync(ct);
                    completeCommit = true;
                    writeScope?.Dispose();
                    writeScope = WriteOperationScope.NoOp;
                }
            }
            catch
            {
                if (needsTx)
                    await RecoverCatalogStateAfterFailedCommitAsync();
                throw;
            }

            if (completeCommit)
            {
                await WaitForCommitOrRecoverAsync(commit);
                await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
            }
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    private Statement ParseCached(string sql) =>
        _statementCache.GetOrAdd(
            sql,
            static s => Parser.TryParseSimpleSelect(s, out var stmt) ? stmt : Parser.Parse(s));

    private static bool LooksLikeInsert(string sql)
    {
        ReadOnlySpan<char> span = sql.AsSpan();
        int pos = 0;
        while (pos < span.Length && char.IsWhiteSpace(span[pos]))
            pos++;

        ReadOnlySpan<char> keyword = "INSERT";
        if (pos + keyword.Length > span.Length)
            return false;

        return span.Slice(pos, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasHybridHotSet(HybridDatabaseOptions hybridOptions)
        => hybridOptions.HotTableNames.Count > 0 || hybridOptions.HotCollectionNames.Count > 0;

    private static void ValidateHybridHotSetOptions(DatabaseOptions options, HybridDatabaseOptions hybridOptions)
    {
        if (!HasHybridHotSet(hybridOptions))
            return;

        if (hybridOptions.PersistenceMode != HybridPersistenceMode.IncrementalDurable)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming is supported only for incremental-durable hybrid mode.",
                nameof(hybridOptions));
        }

        PagerOptions pagerOptions = options.StorageEngineOptions.PagerOptions;
        if (pagerOptions.MaxCachedPages is not null)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming requires the default unbounded pager cache. Remove MaxCachedPages to enable it.",
                nameof(options));
        }

        if (pagerOptions.PageCacheFactory is not null)
        {
            throw new ArgumentException(
                "Hybrid hot-table warming requires the default pager cache. Remove PageCacheFactory to enable it.",
                nameof(options));
        }
    }

    private async ValueTask WarmHybridHotSetAsync(HybridDatabaseOptions hybridOptions, CancellationToken ct)
    {
        if (!HasHybridHotSet(hybridOptions))
            return;

        var warmedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string tableName in EnumerateNormalizedNames(hybridOptions.HotTableNames))
        {
            await WarmSqlTableAsync(tableName, ct);
            warmedTables.Add(tableName);
        }

        foreach (string collectionName in EnumerateNormalizedNames(hybridOptions.HotCollectionNames))
        {
            string catalogName = NormalizeCollectionCatalogName(collectionName);
            if (!warmedTables.Add(catalogName))
                continue;

            if (_catalog.GetTable(catalogName) is null)
            {
                throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Collection '{collectionName}' not found.");
            }

            await _catalog.GetTableTree(catalogName).WarmOwnedPagesAsync(ct);
        }
    }

    private async ValueTask WarmSqlTableAsync(string tableName, CancellationToken ct)
    {
        if (_catalog.GetTable(tableName) is null)
        {
            throw new CSharpDbException(
                ErrorCode.TableNotFound,
                $"Table '{tableName}' not found.");
        }

        await _catalog.GetTableTree(tableName).WarmOwnedPagesAsync(ct);

        foreach (var index in _catalog.GetIndexesForTable(tableName))
            await new BTree(_pager, _catalog.GetIndexStore(index.IndexName).RootPageId).WarmOwnedPagesAsync(ct);
    }

    private static IEnumerable<string> EnumerateNormalizedNames(IReadOnlyList<string> names)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string name in names)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            string trimmed = name.Trim();
            if (seen.Add(trimmed))
                yield return trimmed;
        }
    }

    private static string NormalizeCollectionCatalogName(string collectionName)
    {
        return collectionName.StartsWith(CollectionPrefix, StringComparison.Ordinal)
            ? collectionName
            : $"{CollectionPrefix}{collectionName}";
    }

    private void RecordPendingCollectionCatalogMutation(
        string tableName,
        BTree tree,
        long rowCountDelta,
        bool requiresExactRowCountSync,
        bool hasDocumentMutation)
    {
        if (!_inTransaction)
            return;

        if (!_pendingCollectionCatalogMutations.TryGetValue(tableName, out var pending))
        {
            pending = new PendingCollectionCatalogMutation(tree);
            _pendingCollectionCatalogMutations[tableName] = pending;
        }

        pending.Record(tree, rowCountDelta, requiresExactRowCountSync, hasDocumentMutation);
    }

    private async ValueTask<long?> GetPendingCollectionRowCountAsync(string tableName, BTree tree, CancellationToken ct)
    {
        if (!_inTransaction || !_pendingCollectionCatalogMutations.ContainsKey(tableName))
            return null;

        return await tree.CountEntriesAsync(ct);
    }

    private static async ValueTask DelayImplicitConflictRetryAsync(int attempt, CancellationToken ct)
    {
        double delayMs = Math.Min(20, 0.25 * Math.Pow(2, Math.Max(0, attempt)));
        double jitterMs = delayMs <= 0 ? 0 : Random.Shared.NextDouble() * delayMs;
        TimeSpan delay = TimeSpan.FromMilliseconds(jitterMs);
        if (delay > TimeSpan.Zero)
            await Task.Delay(delay, ct);
    }

    private async ValueTask<IDisposable> AcquireWriteOperationScopeAsync(CancellationToken ct)
    {
        if (_writeOperationGate.Wait(0))
            return new WriteOperationScope(_writeOperationGate);

        // Report only a real write-admission wait. The generation-safe lease
        // restores the prior query phase on acquisition/cancellation without
        // inferring time spent by a consumer between streamed rows.
        using IDisposable? waiting = _queryObservability?.EnterWaiting();
        await _writeOperationGate.WaitAsync(ct);
        return new WriteOperationScope(_writeOperationGate);
    }

    private void ReleaseExplicitTransactionWriteGate()
    {
        _writeOperationGate.Release();
    }

    private ValueTask FlushPendingCollectionCatalogMutationsBeforeSqlAsync(CancellationToken ct)
    {
        if (!_inTransaction || _pendingCollectionCatalogMutations.Count == 0)
            return ValueTask.CompletedTask;

        return FlushPendingCollectionCatalogMutationsAsync(ct);
    }

    private void RefreshCachedCollectionsFromCatalog()
    {
        foreach (var cached in _collectionCache.ToArray())
        {
            if (cached.Value is not ICollectionTreeRefresh refreshable)
                continue;

            if (_catalog.GetTable(refreshable.CatalogTableName) is null)
            {
                _collectionCache.Remove(cached.Key);
                continue;
            }

            refreshable.RefreshTreeFromCatalog();
        }
    }

    private async ValueTask RecoverCatalogStateAfterFailedCommitAsync()
    {
        ClearPendingCollectionCatalogMutations();
        try
        {
            await _pager.RollbackAsync(CancellationToken.None);
        }
        catch
        {
            // Preserve the original failure.
        }

        try
        {
            await _catalog.ReloadAsync(CancellationToken.None);
            RefreshCachedCollectionsFromCatalog();
            _statementCache.Clear();
        }
        catch
        {
            // Preserve the original failure.
        }
    }

    private async ValueTask WaitForCommitOrRecoverAsync(PagerCommitResult commit)
    {
        try
        {
            await commit.WaitAsync();
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();
            throw;
        }
    }

    private void InvalidateCachesIfSchemaChanged()
    {
        long currentVersion = _catalog.SchemaVersion;
        if (currentVersion == _observedSchemaVersion)
            return;

        _statementCache.Clear();
        _collectionCache.Clear();
        _observedSchemaVersion = currentVersion;
    }

    private async ValueTask CommitWithCatalogSyncAsync(CancellationToken ct, bool persistHybridState = true)
    {
        PagerCommitResult commit = await BeginCommitWithCatalogSyncAsync(ct);
        await WaitForCommitOrRecoverAsync(commit);
        if (persistHybridState)
            await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct);
    }

    private async ValueTask CommitInsertWithCatalogSyncAsync(string tableName, CancellationToken ct)
    {
        PagerCommitResult commit = await BeginCommitForTableWithCatalogSyncAsync(tableName, ct);
        await CompleteImplicitCommitAsync(commit, ct);
    }

    private async ValueTask<PagerCommitResult> BeginCommitWithCatalogSyncAsync(CancellationToken ct)
    {
        await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
        await _catalog.PersistAllRootPageChangesAsync(ct);
        return await _pager.BeginCommitAsync(ct);
    }

    private async ValueTask<PagerCommitResult> BeginCommitForTableWithCatalogSyncAsync(string tableName, CancellationToken ct)
    {
        await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
        await _catalog.PersistRootPageChangesAsync(tableName, ct);
        return await _pager.BeginCommitAsync(ct);
    }

    private async ValueTask CompleteImplicitCommitAsync(PagerCommitResult commit, CancellationToken ct)
    {
        await WaitForCommitOrRecoverAsync(commit);
        await PersistHybridStateAsync(HybridPersistenceTriggers.Commit, ct, writeScopeHeld: true);
    }

    private ValueTask PersistHybridStateAsync(
        HybridPersistenceTriggers trigger,
        CancellationToken ct,
        bool writeScopeHeld = false)
    {
        if (_hybridPersistenceCoordinator is null)
            return ValueTask.CompletedTask;

        return _hybridPersistenceCoordinator.PersistAsync(this, trigger, writeScopeHeld, ct);
    }

    public async ValueTask DisposeAsync()
    {
        LifecycleOperation? closeOperation =
            Volatile.Read(ref _openCompleted) != 0 &&
            Interlocked.Exchange(ref _closeObservationStarted, 1) == 0
                ? StartLifecycleObservability(
                    CSharpDbLogEvents.DatabaseClosed,
                    CSharpDbOperationClass.Database)
                : null;

        try
        {
            bool rolledBackExplicitTransaction = false;
            if (_inTransaction)
            {
                Exception? rollbackException = null;
                try
                {
                    await _pager.RollbackAsync();
                }
                catch (Exception exception)
                {
                    rollbackException = exception;
                }

                ClearPendingCollectionCatalogMutations();
                _inTransaction = false;
                ReleaseExplicitTransactionWriteGate();
                rolledBackExplicitTransaction = true;
                CompleteExplicitTransactionObservation(rollbackException);
            }

            try
            {
                if (!_skipDisposePersistence)
                {
                    if (!rolledBackExplicitTransaction)
                        await FlushPendingAdvisoryStatisticsAsync(CancellationToken.None);
                    await PersistHybridStateAsync(HybridPersistenceTriggers.Dispose, CancellationToken.None);
                }
            }
            finally
            {
                StorageRuntimeDiagnostics.Registration? storageRegistration =
                    Interlocked.Exchange(
                    ref _storageRuntimeDiagnosticsRegistration,
                    null);
                storageRegistration?.DrainProvider();
                try
                {
                    try
                    {
                        await _temporaryTables.DisposeAsync();
                    }
                    finally
                    {
                        // The raw provider is already drained, but the observer
                        // lease remains attached so a swallowed shutdown
                        // checkpoint can publish its terminal state.
                        await _pager.DisposeAsync();
                    }
                }
                finally
                {
                    storageRegistration?.Dispose();
                    if (_ownsRuntimeDiagnosticsState)
                        _runtimeDiagnosticsState?.Dispose();
                    _hybridPersistenceCoordinator?.Dispose();
                    _sharedRowVersionReservationGate.Dispose();
                    _writeOperationGate.Dispose();
                    _sharedStateGate.Dispose();
                }
            }

            closeOperation?.Succeed();
        }
        catch (Exception exception)
        {
            closeOperation?.Fail(exception);
            throw;
        }
    }

    private async ValueTask FlushPendingAdvisoryStatisticsAsync(
        CancellationToken ct,
        bool writeScopeHeld = false)
    {
        if (_inTransaction)
        {
            return;
        }

        IDisposable? writeScope = null;
        try
        {
            if (!writeScopeHeld)
                writeScope = await AcquireWriteOperationScopeAsync(ct);

            await FlushPendingCollectionCatalogMutationsAsync(ct);
            if (!_catalog.HasDirtyAdvisoryStatistics)
                return;

            await _pager.BeginTransactionAsync(ct);
            await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
            await _catalog.PersistAllRootPageChangesAsync(ct);
            PagerCommitResult commit = await _pager.BeginCommitAsync(ct);
            writeScope?.Dispose();
            writeScope = null;
            await commit.WaitAsync(ct);
        }
        catch
        {
            await RecoverCatalogStateAfterFailedCommitAsync();

            throw;
        }
        finally
        {
            writeScope?.Dispose();
        }
    }

    private async ValueTask FlushPendingCollectionCatalogMutationsAsync(CancellationToken ct)
    {
        if (_pendingCollectionCatalogMutations.Count == 0)
            return;

        foreach (var entry in _pendingCollectionCatalogMutations)
        {
            string tableName = entry.Key;
            PendingCollectionCatalogMutation pending = entry.Value;

            if (pending.RequiresExactRowCountSync)
            {
                long exactRowCount = await pending.Tree.CountEntriesAsync(ct);
                await _catalog.SetTableRowCountAsync(tableName, exactRowCount, ct);
            }
            else if (pending.RowCountDelta != 0)
            {
                if (_catalog.TryGetExactTableRowCount(tableName, out _))
                {
                    await _catalog.AdjustTableRowCountKnownExactAsync(tableName, pending.RowCountDelta, ct);
                }
                else
                {
                    long exactRowCount = await pending.Tree.CountEntriesAsync(ct);
                    await _catalog.SetTableRowCountAsync(tableName, exactRowCount, ct);
                }
            }

            if (pending.HasDocumentMutation)
                await _catalog.MarkTableColumnStatisticsStaleAsync(tableName, ct);
        }

        ClearPendingCollectionCatalogMutations();
    }

    private void ClearPendingCollectionCatalogMutations()
    {
        _pendingCollectionCatalogMutations.Clear();
    }

    private sealed class WriteOperationScope : IDisposable
    {
        internal static readonly WriteOperationScope NoOp = new(null);

        private SemaphoreSlim? _gate;

        internal WriteOperationScope(SemaphoreSlim? gate)
        {
            _gate = gate;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _gate, null)?.Release();
        }
    }

    private sealed class PendingCollectionCatalogMutation
    {
        internal PendingCollectionCatalogMutation(BTree tree)
        {
            Tree = tree;
        }

        internal BTree Tree { get; private set; }
        internal long RowCountDelta { get; private set; }
        internal bool RequiresExactRowCountSync { get; private set; }
        internal bool HasDocumentMutation { get; private set; }

        internal void Record(BTree tree, long rowCountDelta, bool requiresExactRowCountSync, bool hasDocumentMutation)
        {
            Tree = tree;
            RowCountDelta = checked(RowCountDelta + rowCountDelta);
            RequiresExactRowCountSync |= requiresExactRowCountSync;
            HasDocumentMutation |= hasDocumentMutation;
        }
    }

    /// <summary>
    /// An isolated read-only session that sees a consistent snapshot.
    /// Multiple ReaderSessions can exist concurrently with an active writer.
    /// </summary>
    public sealed class ReaderSession : IDisposable
    {
        private static readonly ColumnDefinition[] DefaultCountStarOutputSchema =
        [
            new ColumnDefinition
            {
                Name = "COUNT(*)",
                Type = DbType.Integer,
                Nullable = false,
            },
        ];

        private readonly Pager _pager;
        private readonly SchemaCatalog _catalog;
        private readonly IRecordSerializer _recordSerializer;
        private readonly IRecordSerializer? _collectionReadSerializer;
        private readonly DbFunctionRegistry _functions;
        private readonly Func<ValueTask> _releaseActiveQueryCallback;
        private readonly StatementCache _statementCache;
        private readonly WalSnapshot _snapshot;
        private readonly IReadOnlyDictionary<string, long> _snapshotRowCounts;
        private readonly bool _allowCurrentCatalogRowCounts;
        private readonly AdaptiveQueryReoptimizationOptions _adaptiveQueryReoptimization;
        private readonly WindowExecutionOptions _windowExecution;
        private readonly QueryObservability? _queryObservability;
        private readonly IQueryPlanRuntimeObserver? _queryPlanRuntimeObserver;
        private Pager? _snapshotPager;
        private QueryPlanner? _planner;
        private string? _lastSql;
        private Statement? _lastParsedStatement;
        private bool _disposed;
        private int _activeQuery;

        internal ReaderSession(
            Pager pager,
            SchemaCatalog catalog,
            IRecordSerializer recordSerializer,
            WalSnapshot snapshot,
            StatementCache statementCache,
            DbFunctionRegistry functions,
            AdaptiveQueryReoptimizationOptions adaptiveQueryReoptimization,
            WindowExecutionOptions windowExecution,
            QueryObservability? queryObservability,
            IQueryPlanRuntimeObserver? queryPlanRuntimeObserver,
            IReadOnlyDictionary<string, long> snapshotRowCounts,
            bool allowCurrentCatalogRowCounts)
        {
            _pager = pager;
            _catalog = catalog;
            _recordSerializer = recordSerializer;
            _collectionReadSerializer = recordSerializer is DefaultRecordSerializer
                ? new CollectionAwareRecordSerializer(recordSerializer)
                : null;
            _functions = functions;
            _releaseActiveQueryCallback = ReleaseActiveQueryAsync;
            _statementCache = statementCache;
            _snapshot = snapshot;
            _adaptiveQueryReoptimization = adaptiveQueryReoptimization;
            _windowExecution = windowExecution;
            _queryObservability = queryObservability;
            _queryPlanRuntimeObserver = queryPlanRuntimeObserver;
            _snapshotRowCounts = snapshotRowCounts;
            _allowCurrentCatalogRowCounts = allowCurrentCatalogRowCounts;
        }

        /// <summary>
        /// Execute a read-only SQL statement against the snapshot.
        /// </summary>
        public ValueTask<QueryResult> ExecuteReadAsync(string sql,
            CancellationToken ct = default)
        {
            IQueryExecutionObservation? operation =
                _queryObservability?.StartExecution(sql);
            return operation is null
                ? ExecuteReadSqlCoreAsync(sql, ct)
                : Database.ObserveQueryAsync(
                    operation,
                    (Target: this, Sql: sql, CancellationToken: ct),
                    static state => state.Target.ExecuteReadSqlCoreAsync(
                        state.Sql,
                        state.CancellationToken));
        }

        private ValueTask<QueryResult> ExecuteReadSqlCoreAsync(
            string sql,
            CancellationToken ct)
        {
            Statement stmt;
            if (_lastSql != null &&
                string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                _lastParsedStatement != null)
            {
                stmt = _lastParsedStatement;
            }
            else
            {
                stmt = _statementCache.GetOrAdd(sql, static s => Parser.Parse(s));
                _lastSql = sql;
                _lastParsedStatement = stmt;
            }

            return ExecuteReadCoreAsync(stmt, ct);
        }

        /// <summary>
        /// Execute a read-only prepared statement against the snapshot.
        /// </summary>
        public ValueTask<QueryResult> ExecuteReadAsync(Statement stmt, CancellationToken ct = default)
            => ExecuteReadAsync(stmt, suppliedFingerprint: null, ct);

        internal ValueTask<QueryResult> ExecuteReadAsync(
            Statement stmt,
            QueryFingerprint? suppliedFingerprint,
            CancellationToken ct = default)
        {
            IQueryExecutionObservation? operation =
                _queryObservability?.StartExecution(sql: null, suppliedFingerprint);
            return operation is null
                ? ExecuteReadCoreAsync(stmt, ct)
                : Database.ObserveQueryAsync(
                    operation,
                    (Target: this, Statement: stmt, CancellationToken: ct),
                    static state => state.Target.ExecuteReadCoreAsync(
                        state.Statement,
                        state.CancellationToken));
        }

        private ValueTask<QueryResult> ExecuteReadCoreAsync(
            Statement stmt,
            CancellationToken ct)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!SqlStatementClassifier.IsReadOnly(stmt))
                throw new CSharpDbException(ErrorCode.Unknown,
                    "Reader sessions only support read-only statements.");

            AcquireActiveRead();

            try
            {
                if (stmt is SelectStatement select)
                {
                    if (TryExecuteCountStarFastPath(select, out QueryResult fastCountResult))
                    {
                        ObserveReaderFastPlan(
                            QueryPlanAccessPathCategory.TableScan,
                            estimatedRows: 1);
                        fastCountResult.SetDisposeCallback(_releaseActiveQueryCallback);
                        return ValueTask.FromResult(fastCountResult);
                    }

                    ValueTask<QueryResult?> fastLookupTask = TryExecutePrimaryKeyLookupFastPathAsync(select, ct);
                    if (fastLookupTask.IsCompletedSuccessfully)
                    {
                        QueryResult? fastLookupResult = fastLookupTask.Result;
                        if (fastLookupResult is not null)
                        {
                            ObserveReaderFastPlan(
                                QueryPlanAccessPathCategory.PrimaryKeyLookup,
                                estimatedRows: 1);
                            fastLookupResult.SetDisposeCallback(_releaseActiveQueryCallback);
                            return ValueTask.FromResult(fastLookupResult);
                        }
                    }
                    else
                    {
                        return CompleteReadWithPrimaryKeyFastPathAsync(stmt, fastLookupTask, ct);
                    }
                }

                _planner ??= CreatePlanner();
                ValueTask<QueryResult> plannerTask = _planner.ExecuteAsync(stmt, ct);
                if (plannerTask.IsCompletedSuccessfully)
                {
                    QueryResult plannerResult = plannerTask.Result;
                    plannerResult.SetDisposeCallback(_releaseActiveQueryCallback);
                    return ValueTask.FromResult(plannerResult);
                }

                return CompleteReadWithPlannerAsync(plannerTask);
            }
            catch
            {
                Volatile.Write(ref _activeQuery, 0);
                throw;
            }
        }

        /// <summary>
        /// Opens one forward-only physical table reader over this session's
        /// immutable pager snapshot. This bypasses SQL planning so callers can
        /// bind durable progress to the actual table row ID.
        /// </summary>
        internal RetainedDatabaseSnapshotTableReader OpenTableReader(
            string tableName,
            long? afterRowIdExclusive,
            int maxEncodedRowBytes)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEncodedRowBytes);

            if (IsReservedPhysicalTableName(tableName))
            {
                throw new InvalidOperationException(
                    "Retained snapshot table readers cannot scan system or internal tables.");
            }
            if (_catalog.IsView(tableName))
            {
                throw new InvalidOperationException(
                    "Retained snapshot table readers require a physical table and cannot scan a view.");
            }

            TableSchema schema = _catalog.GetTable(tableName)
                ?? throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Local physical table '{tableName}' was not found. External tables are not retained by the database snapshot.");

            AcquireActiveRead();
            try
            {
                var tree = _catalog.GetTableTree(schema.TableName, GetOrCreateSnapshotPager());
                return new RetainedDatabaseSnapshotTableReader(
                    schema,
                    tree.CreateCursor(maxEncodedRowBytes),
                    GetReadSerializer(schema),
                    afterRowIdExclusive,
                    _releaseActiveQueryCallback);
            }
            catch
            {
                Volatile.Write(ref _activeQuery, 0);
                throw;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _snapshotPager?.Dispose();
                _pager.ReleaseReaderSnapshot(_snapshot);
                _disposed = true;
            }
        }

        private void AcquireActiveRead()
        {
            if (Interlocked.CompareExchange(ref _activeQuery, 1, 0) != 0)
            {
                throw new InvalidOperationException(
                    "ReaderSession supports only one active query or physical table reader at a time. Dispose the previous read before starting another.");
            }
        }

        private ValueTask ReleaseActiveQueryAsync()
        {
            Volatile.Write(ref _activeQuery, 0);
            return ValueTask.CompletedTask;
        }

        private async ValueTask<QueryResult> CompleteReadWithPrimaryKeyFastPathAsync(
            Statement stmt,
            ValueTask<QueryResult?> fastLookupTask,
            CancellationToken ct)
        {
            try
            {
                QueryResult? fastLookupResult = await fastLookupTask;
                if (fastLookupResult is not null)
                {
                    ObserveReaderFastPlan(
                        QueryPlanAccessPathCategory.PrimaryKeyLookup,
                        estimatedRows: 1);
                    fastLookupResult.SetDisposeCallback(_releaseActiveQueryCallback);
                    return fastLookupResult;
                }

                _planner ??= CreatePlanner();
                QueryResult plannerResult = await _planner.ExecuteAsync(stmt, ct);
                plannerResult.SetDisposeCallback(_releaseActiveQueryCallback);
                return plannerResult;
            }
            catch
            {
                Volatile.Write(ref _activeQuery, 0);
                throw;
            }
        }

        private async ValueTask<QueryResult> CompleteReadWithPlannerAsync(ValueTask<QueryResult> plannerTask)
        {
            try
            {
                QueryResult plannerResult = await plannerTask;
                plannerResult.SetDisposeCallback(_releaseActiveQueryCallback);
                return plannerResult;
            }
            catch
            {
                Volatile.Write(ref _activeQuery, 0);
                throw;
            }
        }

        private bool TryExecuteCountStarFastPath(SelectStatement stmt, out QueryResult result)
        {
            result = null!;

            if (stmt.From is not SimpleTableRef simpleRef)
                return false;
            if (IsSystemCatalogTable(simpleRef.TableName) || _catalog.IsView(simpleRef.TableName))
                return false;
            if (stmt.Where != null || stmt.GroupBy != null || stmt.Having != null)
                return false;
            if (stmt.OrderBy is { Count: > 0 })
                return false;
            if (stmt.Limit.HasValue || stmt.Offset.HasValue)
                return false;
            if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
                return false;
            if (stmt.Columns[0].Expression is not FunctionCallExpression func)
                return false;
            if (!func.IsStarArg || func.IsDistinct || func.Arguments.Count != 0)
                return false;
            if (!string.Equals(func.FunctionName, "COUNT", StringComparison.OrdinalIgnoreCase))
                return false;
            if (_catalog.GetTable(simpleRef.TableName) == null)
                return false;

            ColumnDefinition[] outputSchema = stmt.Columns[0].Alias is { Length: > 0 } alias
                ? [new ColumnDefinition { Name = alias, Type = DbType.Integer, Nullable = false }]
                : DefaultCountStarOutputSchema;

            if (_snapshotRowCounts.TryGetValue(simpleRef.TableName, out long rowCount))
            {
                result = QueryResult.FromSyncScalar(DbValue.FromInteger(rowCount), outputSchema);
                return true;
            }

            if (_allowCurrentCatalogRowCounts &&
                _catalog.TryGetExactTableRowCount(simpleRef.TableName, out rowCount))
            {
                result = QueryResult.FromSyncScalar(DbValue.FromInteger(rowCount), outputSchema);
                return true;
            }

            var tableTree = _catalog.GetTableTree(simpleRef.TableName, GetOrCreateSnapshotPager());
            result = new QueryResult(new CountStarTableOperator(tableTree, outputSchema, ignoreCachedCount: true));
            return true;
        }

        private async ValueTask<QueryResult?> TryExecutePrimaryKeyLookupFastPathAsync(SelectStatement stmt, CancellationToken ct)
        {
            if (stmt.IsDistinct)
                return null;
            if (stmt.From is not SimpleTableRef simpleRef)
                return null;
            if (IsSystemCatalogTable(simpleRef.TableName) || _catalog.IsView(simpleRef.TableName))
                return null;
            if (stmt.Where == null || stmt.GroupBy != null || stmt.Having != null)
                return null;
            if (stmt.OrderBy is { Count: > 0 } || stmt.Limit.HasValue || stmt.Offset.HasValue)
                return null;

            var schema = _catalog.GetTable(simpleRef.TableName);
            if (schema == null)
                return null;

            int pkIndex = schema.PrimaryKeyColumnIndex;
            if (pkIndex < 0 || pkIndex >= schema.Columns.Count || schema.Columns[pkIndex].Type != DbType.Integer)
                return null;

            if (!TryExtractPrimaryKeyEquality(stmt.Where, simpleRef, schema, pkIndex, out long lookupValue))
                return null;

            if (!TryBuildProjection(stmt.Columns, schema, out var projectionColumnIndices, out var outputColumns, out bool selectStar))
                return null;

            var tableTree = new BTree(_pager, _catalog.GetTableRootPage(simpleRef.TableName));
            ReadOnlyMemory<byte>? payload = tableTree.TryFindSnapshotCachedMemory(
                lookupValue,
                _snapshot,
                out var cachedPayload)
                ? cachedPayload
                : await tableTree.FindMemoryAsync(lookupValue, _snapshot, ct);
            if (!payload.HasValue)
                return QueryResult.FromSyncLookup(null, outputColumns);

            if (selectStar)
            {
                var serializer = GetReadSerializer(schema);
                return QueryResult.FromSyncLookup(serializer.Decode(payload.Value.Span), outputColumns);
            }

            if (IsPrimaryKeyOnlyProjection(projectionColumnIndices, pkIndex))
            {
                var keyValue = DbValue.FromInteger(lookupValue);
                var row = new DbValue[outputColumns.Length];
                Array.Fill(row, keyValue);
                return QueryResult.FromSyncLookup(row, outputColumns);
            }

            var decoded = GetReadSerializer(schema).Decode(payload.Value.Span);
            var projected = new DbValue[projectionColumnIndices.Length];
            for (int i = 0; i < projectionColumnIndices.Length; i++)
                projected[i] = decoded[projectionColumnIndices[i]];

            return QueryResult.FromSyncLookup(projected, outputColumns);
        }

        private Pager GetOrCreateSnapshotPager()
            => _snapshotPager ??= _pager.CreateSnapshotReader(_snapshot);

        private QueryPlanner CreatePlanner()
        {
            var planner = new QueryPlanner(
                GetOrCreateSnapshotPager(),
                _catalog,
                _recordSerializer,
                functions: _functions,
                adaptiveQueryReoptimization: _adaptiveQueryReoptimization,
                windowExecution: _windowExecution)
            {
                PlanRuntimeObserver = _queryPlanRuntimeObserver,
            };
            return planner;
        }

        private void ObserveReaderFastPlan(
            QueryPlanAccessPathCategory accessPath,
            long? estimatedRows)
        {
            IQueryPlanRuntimeObserver? observer = _queryPlanRuntimeObserver;
            if (observer is null)
                return;

            var selection = new QueryPlanRuntimeSelection(
                accessPath,
                estimatedRows);
            QueryPlanRuntimeObserver.AccessPathSelected(observer, in selection);
        }

        private IRecordSerializer GetReadSerializer(TableSchema schema)
            => _collectionReadSerializer != null && schema.TableName.StartsWith("_col_", StringComparison.Ordinal)
                ? _collectionReadSerializer
                : _recordSerializer;

        private static bool TryExtractPrimaryKeyEquality(
            Expression expression,
            SimpleTableRef tableRef,
            TableSchema schema,
            int primaryKeyIndex,
            out long lookupValue)
        {
            lookupValue = 0;

            if (expression is not BinaryExpression { Op: BinaryOp.Equals } equals)
                return false;

            if (TryMatchPrimaryKeyColumn(equals.Left, tableRef, schema, primaryKeyIndex) &&
                TryReadIntegerLiteral(equals.Right, out lookupValue))
            {
                return true;
            }

            if (TryMatchPrimaryKeyColumn(equals.Right, tableRef, schema, primaryKeyIndex) &&
                TryReadIntegerLiteral(equals.Left, out lookupValue))
            {
                return true;
            }

            return false;
        }

        private static bool TryMatchPrimaryKeyColumn(
            Expression expression,
            SimpleTableRef tableRef,
            TableSchema schema,
            int primaryKeyIndex)
        {
            if (expression is not ColumnRefExpression column)
                return false;

            if (column.TableAlias != null)
            {
                string expectedAlias = tableRef.Alias ?? tableRef.TableName;
                if (!string.Equals(column.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            int columnIndex = column.TableAlias != null
                ? schema.GetQualifiedColumnIndex(column.TableAlias, column.ColumnName)
                : schema.GetColumnIndex(column.ColumnName);

            return columnIndex == primaryKeyIndex;
        }

        private static bool TryReadIntegerLiteral(Expression expression, out long value)
        {
            if (expression is LiteralExpression { LiteralType: TokenType.IntegerLiteral, Value: long int64 })
            {
                value = int64;
                return true;
            }

            if (expression is LiteralExpression { LiteralType: TokenType.IntegerLiteral, Value: int int32 })
            {
                value = int32;
                return true;
            }

            value = 0;
            return false;
        }

        private static bool TryBuildProjection(
            IReadOnlyList<SelectColumn> columns,
            TableSchema schema,
            out int[] columnIndices,
            out ColumnDefinition[] outputColumns,
            out bool selectStar)
        {
            selectStar = columns.Any(static c => c.IsStar);
            if (selectStar)
            {
                if (columns.Count != 1)
                {
                    columnIndices = Array.Empty<int>();
                    outputColumns = Array.Empty<ColumnDefinition>();
                    return false;
                }

                columnIndices = Array.Empty<int>();
                outputColumns = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
                return true;
            }

            columnIndices = new int[columns.Count];
            outputColumns = new ColumnDefinition[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                if (column.Expression is not ColumnRefExpression colRef)
                    return false;

                int sourceIndex = colRef.TableAlias != null
                    ? schema.GetQualifiedColumnIndex(colRef.TableAlias, colRef.ColumnName)
                    : schema.GetColumnIndex(colRef.ColumnName);
                if (sourceIndex < 0 || sourceIndex >= schema.Columns.Count)
                    return false;

                columnIndices[i] = sourceIndex;
                var sourceColumn = schema.Columns[sourceIndex];
                outputColumns[i] = column.Alias != null
                    ? new ColumnDefinition
                    {
                        Name = column.Alias,
                        Type = sourceColumn.Type,
                        DeclaredType = sourceColumn.DeclaredType,
                        Nullable = sourceColumn.Nullable,
                        IsPrimaryKey = sourceColumn.IsPrimaryKey,
                        IsIdentity = sourceColumn.IsIdentity,
                        IsRowVersion = sourceColumn.IsRowVersion,
                        Collation = sourceColumn.Collation,
                    }
                    : sourceColumn;
            }

            return true;
        }

        private static bool IsPrimaryKeyOnlyProjection(int[] columnIndices, int primaryKeyIndex)
        {
            if (primaryKeyIndex < 0)
                return false;

            for (int i = 0; i < columnIndices.Length; i++)
            {
                if (columnIndices[i] != primaryKeyIndex)
                    return false;
            }

            return true;
        }

        private static bool IsSystemCatalogTable(string tableName) =>
            string.Equals(tableName, "sys.tables", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_tables", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.columns", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_columns", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.indexes", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_indexes", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.functions", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_functions", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.views", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_views", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.triggers", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_triggers", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.objects", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_objects", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.table_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_table_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.column_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_column_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.planner_histograms", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_planner_histograms", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.planner_heavy_hitters", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_planner_heavy_hitters", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.planner_index_prefix_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_planner_index_prefix_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys.validation_rules", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_validation_rules", StringComparison.OrdinalIgnoreCase);

        private static bool IsReservedPhysicalTableName(string tableName) =>
            tableName.StartsWith("sys.", StringComparison.OrdinalIgnoreCase) ||
            tableName.StartsWith("sys_", StringComparison.OrdinalIgnoreCase) ||
            tableName.StartsWith("__", StringComparison.Ordinal) ||
            tableName.StartsWith("_col_", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Thread-safe bounded cache for parsed SQL statements.
    /// </summary>
    internal sealed class StatementCache
    {
        private readonly int _capacity;
        private readonly Dictionary<string, Statement> _map = new(StringComparer.Ordinal);
        private readonly Queue<string> _insertionOrder = new();
        private readonly int[] _recentMissHashes;
        private readonly Dictionary<int, int> _recentMissHashCounts;
        private int _recentMissHashCursor;
        private int _recentMissHashCount;
        private string? _lastSql;
        private Statement? _lastStatement;
        private readonly object _gate = new();

        internal StatementCache(int capacity)
        {
            _capacity = capacity > 0 ? capacity : 0;
            int fingerprintWindowSize = _capacity <= 0 ? 0 : _capacity;
            _recentMissHashes = new int[fingerprintWindowSize];
            _recentMissHashCounts = fingerprintWindowSize > 0
                ? new Dictionary<int, int>(fingerprintWindowSize)
                : new Dictionary<int, int>(0);
            _recentMissHashCursor = 0;
            _recentMissHashCount = 0;
        }

        internal bool TryGetOrMarkBypass(string sql, out Statement statement, out bool bypassParse)
        {
            statement = null!;
            bypassParse = false;
            if (_capacity == 0)
                return false;

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    statement = _lastStatement;
                    return true;
                }

                if (_map.TryGetValue(sql, out var hitNode))
                {
                    _lastSql = sql;
                    _lastStatement = hitNode;
                    statement = hitNode;
                    return true;
                }

                if (_recentMissHashes.Length > 0 && _map.Count >= _capacity)
                {
                    int hash = StringComparer.Ordinal.GetHashCode(sql);
                    if (!HasRecentMissHash(hash))
                    {
                        RecordRecentMissHash(hash);
                        bypassParse = true;
                    }
                }
            }

            return false;
        }

        internal bool TryGet(string sql, out Statement statement)
        {
            statement = null!;
            if (_capacity == 0)
                return false;

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    statement = _lastStatement;
                    return true;
                }

                if (!_map.TryGetValue(sql, out Statement? cachedStatement))
                    return false;

                statement = cachedStatement;
                return true;
            }
        }

        internal Statement GetOrAdd(string sql, Func<string, Statement> parse)
        {
            if (_capacity == 0)
                return parse(sql);

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    return _lastStatement;
                }

                if (_map.TryGetValue(sql, out var hitNode))
                {
                    _lastSql = sql;
                    _lastStatement = hitNode;
                    return hitNode;
                }
            }

            // Parse outside lock to avoid blocking other cache operations.
            var parsed = parse(sql);

            lock (_gate)
            {
                if (_lastSql != null &&
                    string.Equals(_lastSql, sql, StringComparison.Ordinal) &&
                    _lastStatement != null)
                {
                    return _lastStatement;
                }

                if (_map.TryGetValue(sql, out var existingNode))
                {
                    _lastSql = sql;
                    _lastStatement = existingNode;
                    return existingNode;
                }

                Statement statementToReturn = parsed;

                if (_map.Count < _capacity)
                {
                    _map[sql] = parsed;
                    _insertionOrder.Enqueue(sql);
                }
                else if (SqlStatementClassifier.IsReadOnly(parsed) && ShouldPromoteQueryAtCapacity(sql))
                {
                    // Only promote read-only query statements that show short-term reuse.
                    // This avoids steady eviction churn on one-off/high-cardinality SQL.
                    EvictOldestEntry();
                    _map[sql] = parsed;
                    _insertionOrder.Enqueue(sql);
                }

                _lastSql = sql;
                _lastStatement = statementToReturn;
                return statementToReturn;
            }
        }

        private bool ShouldPromoteQueryAtCapacity(string sql)
        {
            if (_recentMissHashes.Length == 0)
                return true;

            int hash = StringComparer.Ordinal.GetHashCode(sql);
            if (HasRecentMissHash(hash))
                return true;

            RecordRecentMissHash(hash);

            return false;
        }

        private bool HasRecentMissHash(int hash) => _recentMissHashCounts.ContainsKey(hash);

        private void RecordRecentMissHash(int hash)
        {
            if (_recentMissHashes.Length == 0)
                return;

            if (_recentMissHashCount == _recentMissHashes.Length)
            {
                int evicted = _recentMissHashes[_recentMissHashCursor];
                if (_recentMissHashCounts.TryGetValue(evicted, out int evictedCount))
                {
                    if (evictedCount <= 1)
                        _recentMissHashCounts.Remove(evicted);
                    else
                        _recentMissHashCounts[evicted] = evictedCount - 1;
                }
            }
            else
            {
                _recentMissHashCount++;
            }

            _recentMissHashes[_recentMissHashCursor] = hash;
            _recentMissHashCounts.TryGetValue(hash, out int currentCount);
            _recentMissHashCounts[hash] = currentCount + 1;

            _recentMissHashCursor++;
            if (_recentMissHashCursor >= _recentMissHashes.Length)
                _recentMissHashCursor = 0;
        }

        private void EvictOldestEntry()
        {
            while (_insertionOrder.Count > 0)
            {
                string candidate = _insertionOrder.Dequeue();
                if (_map.Remove(candidate))
                    return;
            }
        }

        internal void Clear()
        {
            if (_capacity == 0)
                return;

            lock (_gate)
            {
                _map.Clear();
                _insertionOrder.Clear();
                Array.Clear(_recentMissHashes);
                _recentMissHashCounts.Clear();
                _recentMissHashCursor = 0;
                _recentMissHashCount = 0;
                _lastSql = null;
                _lastStatement = null;
            }
        }
    }

}
