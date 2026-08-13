using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace CSharpDB.Observability;

/// <summary>
/// Stable names for the version 1 CSharpDB metric schema. Names, instrument
/// kinds, units, meanings, and tag sets are compatibility contracts.
/// </summary>
public static class CSharpDbMetricInstrumentNames
{
    public const string Requests = "csharpdb.requests";
    public const string Statements = "csharpdb.statements";
    public const string QueryDuration = "csharpdb.query.duration";
    public const string RowsProduced = "csharpdb.rows.produced";
    public const string RowsAffected = "csharpdb.rows.affected";
    public const string QueriesSlow = "csharpdb.queries.slow";
    public const string QueriesActive = "csharpdb.queries.active";

    public const string Transactions = "csharpdb.transactions";
    public const string TransactionDuration = "csharpdb.transaction.duration";
    public const string TransactionsActive = "csharpdb.transactions.active";

    public const string MaintenanceOperations = "csharpdb.maintenance.operations";
    public const string MaintenanceDuration = "csharpdb.maintenance.duration";
    public const string MaintenanceActive = "csharpdb.maintenance.active";

    public const string Checkpoints = "csharpdb.checkpoints";
    public const string CheckpointDuration = "csharpdb.checkpoint.duration";
    public const string CheckpointsActive = "csharpdb.checkpoints.active";
    public const string CheckpointAge = "csharpdb.checkpoint.age";

    public const string WalRecoveries = "csharpdb.wal.recoveries";
    public const string WalRecoveryDuration = "csharpdb.wal.recovery.duration";
    public const string WalRecoveriesActive = "csharpdb.wal.recoveries.active";
    public const string WalCommitBatchSize = "csharpdb.wal.commit.batch.size";

    public const string StorageLogicalBytes = "csharpdb.storage.logical_bytes";
    public const string StorageAllocatedBytes = "csharpdb.storage.allocated_bytes";
    public const string StoragePageCount = "csharpdb.storage.page_count";
    public const string StoragePageReads = "csharpdb.storage.page.reads";
    public const string StoragePageWrites = "csharpdb.storage.page.writes";
    public const string StorageBytesRead = "csharpdb.storage.bytes.read";
    public const string StorageBytesWritten = "csharpdb.storage.bytes.written";
    public const string StorageCacheHits = "csharpdb.storage.cache.hits";
    public const string StorageCacheMisses = "csharpdb.storage.cache.misses";
    public const string StorageDirtyPages = "csharpdb.storage.dirty_pages";
    public const string StorageReadersActive = "csharpdb.storage.readers.active";
    public const string StorageWritersActive = "csharpdb.storage.writers.active";
    public const string StorageCommits = "csharpdb.storage.commits";
    public const string StorageConflicts = "csharpdb.storage.conflicts";

    public const string WalLogicalBytes = "csharpdb.wal.logical_bytes";
    public const string WalAllocatedBytes = "csharpdb.wal.allocated_bytes";
    public const string WalCommittedBytes = "csharpdb.wal.committed_bytes";
    public const string WalRetainedBytes = "csharpdb.wal.retained_bytes";
    public const string WalFrameCount = "csharpdb.wal.frame_count";
    public const string WalCommitBatches = "csharpdb.wal.commit_batches";
    public const string WalBytesWritten = "csharpdb.wal.bytes.written";
    public const string WalPendingCommits = "csharpdb.wal.commits.pending";
    public const string WalFlushedCommits = "csharpdb.wal.commits.flushed";
    public const string WalFlushes = "csharpdb.wal.flushes";
    public const string WalGroupCommitBatches = "csharpdb.wal.group_commit.batches";
    public const string WalGroupCommitCommits = "csharpdb.wal.group_commit.commits";

    public const string SessionsActive = "csharpdb.sessions.active";
    public const string ReadersActive = "csharpdb.readers.active";
    public const string PoolWaiters = "csharpdb.pool.waiters";
    public const string ConnectionsAvailable = "csharpdb.connections.available";
    public const string PoolWaitDuration = "csharpdb.pool.wait.duration";

    public const string HealthStatus = "csharpdb.health.status";
}

/// <summary>
/// UCUM-compatible units used by the stable CSharpDB metric schema.
/// </summary>
public static class CSharpDbMetricUnits
{
    public const string Seconds = "s";
    public const string Bytes = "By";
    public const string Page = "{page}";
    public const string Commit = "{commit}";
    public const string Batch = "{batch}";
    public const string Flush = "{flush}";
    public const string Conflict = "{conflict}";
    public const string Frame = "{frame}";
    public const string Session = "{session}";
    public const string Reader = "{reader}";
    public const string Writer = "{writer}";
    public const string Request = "{request}";
    public const string Statement = "{statement}";
    public const string Query = "{query}";
    public const string Row = "{row}";
    public const string Transaction = "{transaction}";
    public const string Operation = "{operation}";
    public const string Checkpoint = "{checkpoint}";
    public const string Recovery = "{recovery}";
    public const string Connection = "{connection}";
    public const string Status = "{status}";
}

internal readonly record struct CSharpDbStorageMetricSnapshot(
    long? LogicalBytes,
    long? AllocatedBytes,
    long? PageCount,
    long? PageReads,
    long? PageWrites,
    long? BytesRead,
    long? BytesWritten,
    long? CacheHits,
    long? CacheMisses,
    long? DirtyPages,
    long? ActiveReaders,
    long? ActiveWriters,
    long? Commits,
    long? Conflicts,
    long? WalLogicalBytes,
    long? WalAllocatedBytes,
    long? WalCommittedBytes,
    long? WalRetainedBytes,
    long? WalFrameCount,
    long? WalCommitBatches,
    long? WalBytesWritten,
    long? WalPendingCommits,
    long? WalFlushedCommits,
    long? WalFlushes,
    long? WalGroupCommitBatches,
    long? WalGroupCommitCommits,
    double? CheckpointAgeSeconds = null);

internal readonly record struct CSharpDbDataMetricSnapshot(
    long? ActiveSessions,
    long? ActiveReaders,
    long? PoolWaiters,
    long? AvailableConnections,
    bool PoolMetricsApplicable = true);

internal interface ICSharpDbStorageMetricsProvider
{
    bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot);
}

internal interface ICSharpDbDataMetricsProvider
{
    bool TryCaptureMetrics(out CSharpDbDataMetricSnapshot snapshot);
}

internal enum CSharpDbMetricId
{
    StorageLogicalBytes,
    StorageAllocatedBytes,
    StoragePageCount,
    StoragePageReads,
    StoragePageWrites,
    StorageBytesRead,
    StorageBytesWritten,
    StorageCacheHits,
    StorageCacheMisses,
    StorageDirtyPages,
    StorageReadersActive,
    StorageWritersActive,
    StorageCommits,
    StorageConflicts,
    WalLogicalBytes,
    WalAllocatedBytes,
    WalCommittedBytes,
    WalRetainedBytes,
    WalFrameCount,
    WalCommitBatches,
    WalBytesWritten,
    WalPendingCommits,
    WalFlushedCommits,
    WalFlushes,
    WalGroupCommitBatches,
    WalGroupCommitCommits,
    SessionsActive,
    ReadersActive,
    PoolWaiters,
    ConnectionsAvailable,
    QueriesActive,
    TransactionsActive,
    CheckpointsActive,
    WalRecoveriesActive,
}

internal enum CSharpDbMetricAvailability
{
    NotParticipating,
    Available,
    Unavailable,
    TopologyChanging,
}

/// <summary>
/// One bounded, disposable metric source for a runtime diagnostics family.
/// The source contains only a safe database alias and bounded provider
/// references; SQL, fingerprints, identifiers, paths, object names, and errors
/// never enter metric state.
/// </summary>
internal sealed class CSharpDbRuntimeMetrics : IDisposable
{
    private readonly object _providerGate = new();
    private readonly TimeProvider _timeProvider;
    private readonly string _databaseAlias;
    private readonly KeyValuePair<string, object?>[] _storageTags;
    private readonly KeyValuePair<string, object?>[][] _maintenanceActiveTags;
    private ICSharpDbStorageMetricsProvider? _storageProvider;
    private int _storageProviderRetiring;
    private int _storageProviderCountersRetired;
    private ICSharpDbDataMetricsProvider? _dataProvider;
    private KeyValuePair<string, object?>[]? _dataTags;
    private string? _dataTransportValue;
    private IDisposable? _registryRegistration;
    private long _activeQueries;
    private long _activeTransactions;
    private long _activeCheckpoints;
    private long _activeRecoveries;
    private readonly long[] _activeMaintenance = new long[5];
    private readonly long[] _lastObservedStorageCounters =
        new long[CSharpDbRuntimeMetricsRegistry.StorageCounterCount];
    private readonly int[] _hasObservedStorageCounter =
        new int[CSharpDbRuntimeMetricsRegistry.StorageCounterCount];
    private int _disposed;

    private CSharpDbRuntimeMetrics(string databaseAlias, TimeProvider timeProvider)
    {
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe bounded database alias is required.", nameof(databaseAlias));

        _databaseAlias = databaseAlias;
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _storageTags =
        [
            new(CSharpDbMetricTagNames.DatabaseAlias, databaseAlias),
        ];
        _maintenanceActiveTags =
        [
            CreateMaintenanceActiveTags(CSharpDbOperationClass.Backup, databaseAlias),
            CreateMaintenanceActiveTags(CSharpDbOperationClass.Restore, databaseAlias),
            CreateMaintenanceActiveTags(CSharpDbOperationClass.Reindex, databaseAlias),
            CreateMaintenanceActiveTags(CSharpDbOperationClass.Vacuum, databaseAlias),
            CreateMaintenanceActiveTags(CSharpDbOperationClass.Maintenance, databaseAlias),
        ];
    }

    internal static CSharpDbRuntimeMetrics? TryCreate(
        string databaseAlias,
        TimeProvider timeProvider)
    {
        try
        {
            // Observable instruments must exist before the source is exposed;
            // otherwise a metrics-only runtime with no pool waits would never
            // trigger the static instrument declarations.
            CSharpDbMetrics.EnsureInitialized();
            var source = new CSharpDbRuntimeMetrics(databaseAlias, timeProvider);
            IDisposable? registration =
                CSharpDbRuntimeMetricsRegistry.TryRegister(source);
            if (registration is null)
            {
                source.Dispose();
                return null;
            }

            source._registryRegistration = registration;
            return source;
        }
        catch
        {
            // Metrics are best effort and cannot prevent a database from opening.
            return null;
        }
    }

    internal IDisposable? RegisterStorageProvider(
        ICSharpDbStorageMetricsProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        lock (_providerGate)
        {
            if (Volatile.Read(ref _disposed) != 0 || _storageProvider is not null)
                return null;

            Volatile.Write(ref _storageProviderCountersRetired, 0);
            _storageProvider = provider;
            return new StorageProviderRegistration(this, provider);
        }
    }

    internal IDisposable? RegisterDataProvider(
        ICSharpDbDataMetricsProvider provider,
        CSharpDbTransport transport)
    {
        ArgumentNullException.ThrowIfNull(provider);
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));

        lock (_providerGate)
        {
            if (Volatile.Read(ref _disposed) != 0 || _dataProvider is not null)
                return null;

            string transportValue = CSharpDbMetricTagValues.Transport(transport);
            _dataTransportValue = transportValue;
            _dataTags =
            [
                new(CSharpDbMetricTagNames.Transport, transportValue),
                new(CSharpDbMetricTagNames.DatabaseAlias, _databaseAlias),
            ];
            _dataProvider = provider;
            return new DataProviderRegistration(this, provider);
        }
    }

    internal bool TryStartPoolWait(out long startingTimestamp)
    {
        startingTimestamp = 0;
        if (Volatile.Read(ref _disposed) != 0 ||
            !CSharpDbMetrics.PoolWaitDuration.Enabled)
        {
            return false;
        }

        try
        {
            startingTimestamp = _timeProvider.GetTimestamp();
            return true;
        }
        catch
        {
            return false;
        }
    }

    internal void RecordPoolWait(
        long startingTimestamp,
        CSharpDbOperationOutcome outcome)
    {
        if (Volatile.Read(ref _disposed) != 0 ||
            !CSharpDbMetrics.PoolWaitDuration.Enabled)
        {
            return;
        }

        try
        {
            string? transportValue = Volatile.Read(ref _dataTransportValue);
            if (transportValue is null)
                return;

            long endingTimestamp = _timeProvider.GetTimestamp();
            TimeSpan elapsed = _timeProvider.GetElapsedTime(
                startingTimestamp,
                endingTimestamp);
            if (elapsed < TimeSpan.Zero)
                return;

            TagList tags = default;
            tags.Add(
                CSharpDbMetricTagNames.Outcome,
                CSharpDbMetricTagValues.Outcome(outcome));
            tags.Add(CSharpDbMetricTagNames.Transport, transportValue);
            tags.Add(CSharpDbMetricTagNames.DatabaseAlias, _databaseAlias);
            CSharpDbMetrics.PoolWaitDuration.Record(elapsed.TotalSeconds, tags);
        }
        catch
        {
            // Clock and listener failures are isolated from pool admission.
        }
    }

    internal bool QueryStarted(CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.OperationClass is not (
                CSharpDbOperationClass.Query or
                CSharpDbOperationClass.Script or
                CSharpDbOperationClass.Procedure) ||
            !Matches(context))
            return false;

        return QueryStarted();
    }

    internal bool QueryStarted()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return false;

        IncrementSaturating(ref _activeQueries);
        return true;
    }

    internal void QueryAbandoned(bool metricsStarted)
    {
        if (metricsStarted)
            DecrementNonNegative(ref _activeQueries);
    }

    internal void QueryCompleted(
        bool metricsStarted,
        CSharpDbOperationContext context,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration,
        long rowsProduced,
        long rowsAffected,
        bool isSlow)
        => QueryCompletedCore(
            metricsStarted,
            context.OperationClass,
            context.Transport,
            context.CountsAsRequest,
            context.CountsAsStatement,
            outcome,
            duration,
            rowsProduced,
            rowsAffected,
            isSlow);

    internal void LeanQueryCompleted(
        bool metricsStarted,
        CSharpDbTransport transport,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration,
        long rowsProduced,
        long rowsAffected,
        bool isSlow)
        => QueryCompletedCore(
            metricsStarted,
            CSharpDbOperationClass.Query,
            transport,
            countsAsRequest: true,
            countsAsStatement: true,
            outcome,
            duration,
            rowsProduced,
            rowsAffected,
            isSlow);

    private void QueryCompletedCore(
        bool metricsStarted,
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        bool countsAsRequest,
        bool countsAsStatement,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration,
        long rowsProduced,
        long rowsAffected,
        bool isSlow)
    {
        if (!metricsStarted)
            return;

        DecrementNonNegative(ref _activeQueries);
        if (Volatile.Read(ref _disposed) != 0)
            return;

        // Internal physical attempts deliberately have neither a request nor
        // statement identity. Without a bounded role dimension their terminal
        // samples would be indistinguishable from logical query operations.
        if (!countsAsRequest && !countsAsStatement)
            return;

        try
        {
            long safeRowsProduced = Math.Max(0, rowsProduced);
            long safeRowsAffected = Math.Max(0, rowsAffected);
            bool anyEnabled =
                (countsAsRequest && CSharpDbMetrics.Requests.Enabled) ||
                (countsAsStatement && CSharpDbMetrics.Statements.Enabled) ||
                CSharpDbMetrics.QueryDuration.Enabled ||
                (countsAsStatement && safeRowsProduced > 0 &&
                    CSharpDbMetrics.RowsProduced.Enabled) ||
                (countsAsStatement && safeRowsAffected > 0 &&
                    CSharpDbMetrics.RowsAffected.Enabled) ||
                (isSlow && CSharpDbMetrics.QueriesSlow.Enabled);
            if (!anyEnabled)
                return;

            TagList tags = CreateOperationTags(
                operationClass,
                transport,
                outcome);
            if (countsAsRequest && CSharpDbMetrics.Requests.Enabled)
                CSharpDbMetrics.Requests.Add(1, tags);
            if (countsAsStatement && CSharpDbMetrics.Statements.Enabled)
                CSharpDbMetrics.Statements.Add(1, tags);
            if (CSharpDbMetrics.QueryDuration.Enabled)
            {
                CSharpDbMetrics.QueryDuration.Record(
                    Math.Max(0, duration.TotalSeconds),
                    tags);
            }
            if (countsAsStatement && safeRowsProduced > 0 &&
                CSharpDbMetrics.RowsProduced.Enabled)
            {
                CSharpDbMetrics.RowsProduced.Add(safeRowsProduced, tags);
            }
            if (countsAsStatement && safeRowsAffected > 0 &&
                CSharpDbMetrics.RowsAffected.Enabled)
            {
                CSharpDbMetrics.RowsAffected.Add(safeRowsAffected, tags);
            }
            if (isSlow && CSharpDbMetrics.QueriesSlow.Enabled)
                CSharpDbMetrics.QueriesSlow.Add(1, tags);
        }
        catch
        {
            // Metrics and listeners cannot affect query terminalization.
        }
    }

    internal bool SupportsLifecycle(CSharpDbOperationClass operationClass)
        => Volatile.Read(ref _disposed) == 0 &&
           (operationClass == CSharpDbOperationClass.Transaction ||
            MaintenanceIndex(operationClass) >= 0);

    internal bool LifecycleStarted(CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (!Matches(context) || Volatile.Read(ref _disposed) != 0)
            return false;

        if (context.OperationClass == CSharpDbOperationClass.Transaction)
        {
            IncrementSaturating(ref _activeTransactions);
            return true;
        }

        int maintenanceIndex = MaintenanceIndex(context.OperationClass);
        if (maintenanceIndex < 0)
            return false;

        IncrementSaturating(ref _activeMaintenance[maintenanceIndex]);
        return true;
    }

    internal void LifecycleCompleted(
        bool metricsStarted,
        CSharpDbOperationContext context,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration)
    {
        if (!metricsStarted)
            return;

        bool transaction = context.OperationClass ==
            CSharpDbOperationClass.Transaction;
        int maintenanceIndex = MaintenanceIndex(context.OperationClass);
        if (transaction)
            DecrementNonNegative(ref _activeTransactions);
        else if (maintenanceIndex >= 0)
            DecrementNonNegative(ref _activeMaintenance[maintenanceIndex]);
        else
            return;

        if (Volatile.Read(ref _disposed) != 0)
            return;

        try
        {
            Counter<long> count = transaction
                ? CSharpDbMetrics.Transactions
                : CSharpDbMetrics.MaintenanceOperations;
            Histogram<double> elapsed = transaction
                ? CSharpDbMetrics.TransactionDuration
                : CSharpDbMetrics.MaintenanceDuration;
            if (!count.Enabled && !elapsed.Enabled)
                return;

            TagList tags = CreateOperationTags(context, outcome);
            if (count.Enabled)
                count.Add(1, tags);
            if (elapsed.Enabled)
                elapsed.Record(Math.Max(0, duration.TotalSeconds), tags);
        }
        catch
        {
            // Metrics and listeners cannot affect lifecycle terminalization.
        }
    }

    internal bool CheckpointStarted()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return false;

        IncrementSaturating(ref _activeCheckpoints);
        return true;
    }

    internal void CheckpointAbandoned(bool metricsStarted)
    {
        if (metricsStarted)
            DecrementNonNegative(ref _activeCheckpoints);
    }

    internal void CheckpointCompleted(
        bool metricsStarted,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration)
        => CompleteStorageOperation(
            metricsStarted,
            ref _activeCheckpoints,
            CSharpDbMetrics.Checkpoints,
            CSharpDbMetrics.CheckpointDuration,
            outcome,
            duration);

    internal bool RecoveryStarted()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return false;

        IncrementSaturating(ref _activeRecoveries);
        return true;
    }

    internal void RecoveryAbandoned(bool metricsStarted)
    {
        if (metricsStarted)
            DecrementNonNegative(ref _activeRecoveries);
    }

    internal void RecoveryCompleted(
        bool metricsStarted,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration)
        => CompleteStorageOperation(
            metricsStarted,
            ref _activeRecoveries,
            CSharpDbMetrics.WalRecoveries,
            CSharpDbMetrics.WalRecoveryDuration,
            outcome,
            duration);

    internal void RecordWalCommitBatchSize(int logicalCommitCount)
    {
        if (logicalCommitCount <= 0 ||
            Volatile.Read(ref _disposed) != 0 ||
            !CSharpDbMetrics.WalCommitBatchSize.Enabled)
        {
            return;
        }

        try
        {
            TagList tags = default;
            tags.Add(CSharpDbMetricTagNames.DatabaseAlias, _databaseAlias);
            CSharpDbMetrics.WalCommitBatchSize.Record(logicalCommitCount, tags);
        }
        catch
        {
            // Metrics and listeners cannot affect WAL publication.
        }
    }

    internal CSharpDbMetricAvailability TryObserve(
        CSharpDbMetricId metric,
        out long value,
        out KeyValuePair<string, object?>[] tags)
    {
        value = 0;
        tags = [];
        if (Volatile.Read(ref _disposed) == 2)
            return CSharpDbMetricAvailability.NotParticipating;

        try
        {
            long? activeValue = metric switch
            {
                CSharpDbMetricId.QueriesActive =>
                    Volatile.Read(ref _activeQueries),
                CSharpDbMetricId.TransactionsActive =>
                    Volatile.Read(ref _activeTransactions),
                CSharpDbMetricId.CheckpointsActive =>
                    Volatile.Read(ref _activeCheckpoints),
                CSharpDbMetricId.WalRecoveriesActive =>
                    Volatile.Read(ref _activeRecoveries),
                _ => null,
            };
            if (activeValue is long currentActive)
            {
                value = Math.Max(0, currentActive);
                tags = _storageTags;
                return CSharpDbMetricAvailability.Available;
            }

            if (metric <= CSharpDbMetricId.WalGroupCommitCommits)
            {
                ICSharpDbStorageMetricsProvider? provider =
                    Volatile.Read(ref _storageProvider);
                if (provider is null)
                {
                    return CSharpDbMetricAvailability.NotParticipating;
                }
                if (Volatile.Read(ref _storageProviderRetiring) != 0)
                    return CSharpDbMetricAvailability.TopologyChanging;

                tags = _storageTags;
                if (!provider.TryCaptureMetrics(
                        out CSharpDbStorageMetricSnapshot snapshot))
                {
                    return CSharpDbMetricAvailability.Unavailable;
                }

                long? observed = metric switch
                {
                    CSharpDbMetricId.StorageLogicalBytes => snapshot.LogicalBytes,
                    CSharpDbMetricId.StorageAllocatedBytes => snapshot.AllocatedBytes,
                    CSharpDbMetricId.StoragePageCount => snapshot.PageCount,
                    CSharpDbMetricId.StoragePageReads => snapshot.PageReads,
                    CSharpDbMetricId.StoragePageWrites => snapshot.PageWrites,
                    CSharpDbMetricId.StorageBytesRead => snapshot.BytesRead,
                    CSharpDbMetricId.StorageBytesWritten => snapshot.BytesWritten,
                    CSharpDbMetricId.StorageCacheHits => snapshot.CacheHits,
                    CSharpDbMetricId.StorageCacheMisses => snapshot.CacheMisses,
                    CSharpDbMetricId.StorageDirtyPages => snapshot.DirtyPages,
                    CSharpDbMetricId.StorageReadersActive => snapshot.ActiveReaders,
                    CSharpDbMetricId.StorageWritersActive => snapshot.ActiveWriters,
                    CSharpDbMetricId.StorageCommits => snapshot.Commits,
                    CSharpDbMetricId.StorageConflicts => snapshot.Conflicts,
                    CSharpDbMetricId.WalLogicalBytes => snapshot.WalLogicalBytes,
                    CSharpDbMetricId.WalAllocatedBytes => snapshot.WalAllocatedBytes,
                    CSharpDbMetricId.WalCommittedBytes => snapshot.WalCommittedBytes,
                    CSharpDbMetricId.WalRetainedBytes => snapshot.WalRetainedBytes,
                    CSharpDbMetricId.WalFrameCount => snapshot.WalFrameCount,
                    CSharpDbMetricId.WalCommitBatches => snapshot.WalCommitBatches,
                    CSharpDbMetricId.WalBytesWritten => snapshot.WalBytesWritten,
                    CSharpDbMetricId.WalPendingCommits => snapshot.WalPendingCommits,
                    CSharpDbMetricId.WalFlushedCommits => snapshot.WalFlushedCommits,
                    CSharpDbMetricId.WalFlushes => snapshot.WalFlushes,
                    CSharpDbMetricId.WalGroupCommitBatches => snapshot.WalGroupCommitBatches,
                    CSharpDbMetricId.WalGroupCommitCommits => snapshot.WalGroupCommitCommits,
                    _ => null,
                };
                if (observed is not long storageValue || storageValue < 0)
                    return CSharpDbMetricAvailability.Unavailable;

                value = PreserveStorageCounterMonotonicity(
                    metric,
                    storageValue);
                return CSharpDbMetricAvailability.Available;
            }

            ICSharpDbDataMetricsProvider? dataProvider =
                Volatile.Read(ref _dataProvider);
            KeyValuePair<string, object?>[]? dataTags =
                Volatile.Read(ref _dataTags);
            if (dataProvider is null || dataTags is null)
            {
                return CSharpDbMetricAvailability.NotParticipating;
            }
            tags = dataTags;
            if (!dataProvider.TryCaptureMetrics(
                    out CSharpDbDataMetricSnapshot data))
            {
                return CSharpDbMetricAvailability.Unavailable;
            }

            if (!data.PoolMetricsApplicable &&
                metric is (CSharpDbMetricId.PoolWaiters or
                    CSharpDbMetricId.ConnectionsAvailable))
            {
                return CSharpDbMetricAvailability.NotParticipating;
            }

            long? dataValue = metric switch
            {
                CSharpDbMetricId.SessionsActive => data.ActiveSessions,
                CSharpDbMetricId.ReadersActive => data.ActiveReaders,
                CSharpDbMetricId.PoolWaiters => data.PoolWaiters,
                CSharpDbMetricId.ConnectionsAvailable => data.AvailableConnections,
                _ => null,
            };
            if (dataValue is not long observedDataValue || observedDataValue < 0)
                return CSharpDbMetricAvailability.Unavailable;

            value = observedDataValue;
            return CSharpDbMetricAvailability.Available;
        }
        catch
        {
            // A broken provider omits its measurement for this collection.
            return tags.Length == 0
                ? CSharpDbMetricAvailability.NotParticipating
                : CSharpDbMetricAvailability.Unavailable;
        }
    }

    internal bool TryObserveMaintenanceActive(
        int maintenanceIndex,
        out long value,
        out KeyValuePair<string, object?>[] tags)
    {
        value = 0;
        tags = [];
        if (Volatile.Read(ref _disposed) != 0 ||
            (uint)maintenanceIndex >= (uint)_activeMaintenance.Length)
        {
            return false;
        }

        value = Math.Max(
            0,
            Volatile.Read(ref _activeMaintenance[maintenanceIndex]));
        tags = _maintenanceActiveTags[maintenanceIndex];
        return true;
    }

    internal CSharpDbMetricAvailability TryObserveCheckpointAge(
        out double value,
        out KeyValuePair<string, object?>[] tags)
    {
        value = 0;
        tags = [];
        if (Volatile.Read(ref _disposed) == 2)
            return CSharpDbMetricAvailability.NotParticipating;

        try
        {
            ICSharpDbStorageMetricsProvider? provider =
                Volatile.Read(ref _storageProvider);
            if (provider is null)
                return CSharpDbMetricAvailability.NotParticipating;
            if (Volatile.Read(ref _storageProviderRetiring) != 0)
                return CSharpDbMetricAvailability.TopologyChanging;

            tags = _storageTags;
            if (!provider.TryCaptureMetrics(
                    out CSharpDbStorageMetricSnapshot snapshot) ||
                snapshot.CheckpointAgeSeconds is not double age ||
                !double.IsFinite(age) ||
                age < 0)
            {
                return CSharpDbMetricAvailability.Unavailable;
            }

            value = age;
            return CSharpDbMetricAvailability.Available;
        }
        catch
        {
            return tags.Length == 0
                ? CSharpDbMetricAvailability.NotParticipating
                : CSharpDbMetricAvailability.Unavailable;
        }
    }

    internal void CaptureRetiredStorageCounters(
        Span<long> values,
        Span<bool> available)
    {
        if (values.Length < CSharpDbRuntimeMetricsRegistry.StorageCounterCount ||
            available.Length < CSharpDbRuntimeMetricsRegistry.StorageCounterCount)
        {
            throw new ArgumentException("The retired counter buffer is too small.");
        }

        values[..CSharpDbRuntimeMetricsRegistry.StorageCounterCount].Clear();
        available[..CSharpDbRuntimeMetricsRegistry.StorageCounterCount].Clear();

        if (Interlocked.CompareExchange(
                ref _storageProviderCountersRetired,
                1,
                0) != 0)
        {
            return;
        }

        ICSharpDbStorageMetricsProvider? provider =
            Volatile.Read(ref _storageProvider);
        if (provider is not null)
        {
            try
            {
                if (provider.TryCaptureMetrics(
                        out CSharpDbStorageMetricSnapshot snapshot))
                {
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StoragePageReads,
                        snapshot.PageReads);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StoragePageWrites,
                        snapshot.PageWrites);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageBytesRead,
                        snapshot.BytesRead);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageBytesWritten,
                        snapshot.BytesWritten);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageCacheHits,
                        snapshot.CacheHits);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageCacheMisses,
                        snapshot.CacheMisses);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageCommits,
                        snapshot.Commits);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.StorageConflicts,
                        snapshot.Conflicts);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalCommitBatches,
                        snapshot.WalCommitBatches);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalBytesWritten,
                        snapshot.WalBytesWritten);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalFlushedCommits,
                        snapshot.WalFlushedCommits);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalFlushes,
                        snapshot.WalFlushes);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalGroupCommitBatches,
                        snapshot.WalGroupCommitBatches);
                    PreserveRetiredCounter(
                        CSharpDbMetricId.WalGroupCommitCommits,
                        snapshot.WalGroupCommitCommits);
                }
            }
            catch
            {
                // The last successfully published value remains authoritative.
            }
        }

        for (int index = 0;
             index < CSharpDbRuntimeMetricsRegistry.StorageCounterCount;
             index++)
        {
            if (Volatile.Read(ref _hasObservedStorageCounter[index]) == 0)
                continue;

            values[index] = Math.Max(
                0,
                Volatile.Read(ref _lastObservedStorageCounters[index]));
            available[index] = true;
        }
    }

    internal void ResetRetiredStorageCounterCache()
    {
        for (int index = 0;
             index < CSharpDbRuntimeMetricsRegistry.StorageCounterCount;
             index++)
        {
            Volatile.Write(ref _lastObservedStorageCounters[index], 0);
            Volatile.Write(ref _hasObservedStorageCounter[index], 0);
        }
    }

    internal string DatabaseAlias => _databaseAlias;

    private void CompleteStorageOperation(
        bool metricsStarted,
        ref long activeCount,
        Counter<long> count,
        Histogram<double> elapsed,
        CSharpDbOperationOutcome outcome,
        TimeSpan duration)
    {
        if (!metricsStarted)
            return;

        DecrementNonNegative(ref activeCount);
        if (Volatile.Read(ref _disposed) != 0 ||
            (!count.Enabled && !elapsed.Enabled))
        {
            return;
        }

        try
        {
            TagList tags = default;
            tags.Add(
                CSharpDbMetricTagNames.Outcome,
                CSharpDbMetricTagValues.Outcome(outcome));
            tags.Add(CSharpDbMetricTagNames.DatabaseAlias, _databaseAlias);
            if (count.Enabled)
                count.Add(1, tags);
            if (elapsed.Enabled)
                elapsed.Record(Math.Max(0, duration.TotalSeconds), tags);
        }
        catch
        {
            // Metrics and listeners cannot affect storage terminalization.
        }
    }

    private bool Matches(CSharpDbOperationContext context)
        => string.Equals(
            _databaseAlias,
            context.DatabaseAlias,
            StringComparison.Ordinal);

    private TagList CreateOperationTags(
        CSharpDbOperationContext context,
        CSharpDbOperationOutcome outcome)
        => CreateOperationTags(
            context.OperationClass,
            context.Transport,
            outcome);

    private TagList CreateOperationTags(
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        CSharpDbOperationOutcome outcome)
    {
        TagList tags = default;
        tags.Add(
            CSharpDbMetricTagNames.OperationClass,
            CSharpDbMetricTagValues.OperationClass(operationClass));
        tags.Add(
            CSharpDbMetricTagNames.Outcome,
            CSharpDbMetricTagValues.Outcome(outcome));
        tags.Add(
            CSharpDbMetricTagNames.Transport,
            CSharpDbMetricTagValues.Transport(transport));
        tags.Add(CSharpDbMetricTagNames.DatabaseAlias, _databaseAlias);
        return tags;
    }

    private static KeyValuePair<string, object?>[] CreateMaintenanceActiveTags(
        CSharpDbOperationClass operationClass,
        string databaseAlias)
        =>
        [
            new(
                CSharpDbMetricTagNames.OperationClass,
                CSharpDbMetricTagValues.OperationClass(operationClass)),
            new(CSharpDbMetricTagNames.DatabaseAlias, databaseAlias),
        ];

    private static int MaintenanceIndex(CSharpDbOperationClass operationClass)
        => operationClass switch
        {
            CSharpDbOperationClass.Backup => 0,
            CSharpDbOperationClass.Restore => 1,
            CSharpDbOperationClass.Reindex => 2,
            CSharpDbOperationClass.Vacuum => 3,
            CSharpDbOperationClass.Maintenance => 4,
            _ => -1,
        };

    private long PreserveStorageCounterMonotonicity(
        CSharpDbMetricId metric,
        long observed)
    {
        if (!CSharpDbRuntimeMetricsRegistry.TryGetStorageCounterIndex(
                metric,
                out int index))
        {
            return observed;
        }

        while (true)
        {
            long previous = Volatile.Read(
                ref _lastObservedStorageCounters[index]);
            if (observed <= previous)
            {
                Volatile.Write(ref _hasObservedStorageCounter[index], 1);
                return previous;
            }

            if (Interlocked.CompareExchange(
                    ref _lastObservedStorageCounters[index],
                    observed,
                    previous) == previous)
            {
                Volatile.Write(ref _hasObservedStorageCounter[index], 1);
                return observed;
            }
        }
    }

    private void PreserveRetiredCounter(
        CSharpDbMetricId metric,
        long? observed)
    {
        if (observed is not long value || value < 0)
            return;

        _ = PreserveStorageCounterMonotonicity(metric, value);
    }

    private static void IncrementSaturating(ref long value)
    {
        while (true)
        {
            long observed = Volatile.Read(ref value);
            if (observed == long.MaxValue)
                return;
            if (Interlocked.CompareExchange(
                    ref value,
                    observed + 1,
                    observed) == observed)
            {
                return;
            }
        }
    }

    private static void DecrementNonNegative(ref long value)
    {
        while (true)
        {
            long observed = Volatile.Read(ref value);
            if (observed <= 0)
                return;
            if (Interlocked.CompareExchange(
                    ref value,
                    observed - 1,
                    observed) == observed)
            {
                return;
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            return;

        IDisposable? registryRegistration = Interlocked.Exchange(
            ref _registryRegistration,
            null);
        registryRegistration?.Dispose();

        lock (_providerGate)
        {
            _storageProvider = null;
            _dataProvider = null;
            _dataTags = null;
            _dataTransportValue = null;
        }

        Volatile.Write(ref _disposed, 2);
    }

    private void UnregisterStorageProvider(
        ICSharpDbStorageMetricsProvider provider)
    {
        lock (_providerGate)
        {
            if (!ReferenceEquals(_storageProvider, provider))
                return;

            Volatile.Write(ref _storageProviderRetiring, 1);
        }

        try
        {
            CSharpDbRuntimeMetricsRegistry.RetireStorageProvider(this);
        }
        finally
        {
            lock (_providerGate)
            {
                if (ReferenceEquals(_storageProvider, provider))
                    _storageProvider = null;
                Volatile.Write(ref _storageProviderRetiring, 0);
            }

            CSharpDbRuntimeMetricsRegistry.NotifyTopologyChanged();
        }
    }

    private void UnregisterDataProvider(ICSharpDbDataMetricsProvider provider)
    {
        lock (_providerGate)
        {
            if (!ReferenceEquals(_dataProvider, provider))
                return;

            _dataProvider = null;
            _dataTags = null;
            _dataTransportValue = null;
        }
    }

    private sealed class StorageProviderRegistration(
        CSharpDbRuntimeMetrics owner,
        ICSharpDbStorageMetricsProvider provider) : IDisposable
    {
        private CSharpDbRuntimeMetrics? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?
                .UnregisterStorageProvider(provider);
    }

    private sealed class DataProviderRegistration(
        CSharpDbRuntimeMetrics owner,
        ICSharpDbDataMetricsProvider provider) : IDisposable
    {
        private CSharpDbRuntimeMetrics? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?
                .UnregisterDataProvider(provider);
    }
}

internal static class CSharpDbRuntimeMetricsRegistry
{
    internal const int StorageCounterCount = 14;

    private static readonly object s_gate = new();
    private static readonly CSharpDbRuntimeMetrics?[] s_sources =
        new CSharpDbRuntimeMetrics?[CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies];
    private static readonly string?[] s_aliases =
        new string?[CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases];
    private static readonly KeyValuePair<string, object?>[]?[] s_aliasTags =
        new KeyValuePair<string, object?>[]?[
            CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases];
    private static readonly int[] s_aliasLiveSourceCounts =
        new int[CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases];
    private static readonly int[] s_aliasPendingProviderRetirements =
        new int[CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases];
    private static readonly long[,] s_retiredStorageCounters =
        new long[
            CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases,
            StorageCounterCount];
    private static long s_topologyVersion;

    internal static IDisposable? TryRegister(CSharpDbRuntimeMetrics source)
    {
        ArgumentNullException.ThrowIfNull(source);
        lock (s_gate)
        {
            int sourceIndex = Array.FindIndex(
                s_sources,
                static candidate => candidate is null);
            if (sourceIndex < 0)
                return null;

            int aliasIndex = FindAliasIndex(source.DatabaseAlias);
            if (aliasIndex < 0)
            {
                aliasIndex = Array.FindIndex(
                    s_aliases,
                    static alias => alias is null);
                if (aliasIndex < 0)
                    return null;

                s_aliases[aliasIndex] = source.DatabaseAlias;
                s_aliasTags[aliasIndex] =
                [
                    new(
                        CSharpDbMetricTagNames.DatabaseAlias,
                        source.DatabaseAlias),
                ];
            }

            checked
            {
                s_aliasLiveSourceCounts[aliasIndex]++;
            }

            Volatile.Write(ref s_sources[sourceIndex], source);
            s_topologyVersion++;
            return new Registration(sourceIndex, aliasIndex, source);
        }
    }

    internal static void RetireStorageProvider(CSharpDbRuntimeMetrics source)
    {
        int aliasIndex;
        lock (s_gate)
        {
            if (!s_sources.Any(candidate => ReferenceEquals(candidate, source)))
                return;

            aliasIndex = FindAliasIndex(source.DatabaseAlias);
            if (aliasIndex < 0)
                return;

            checked
            {
                s_aliasPendingProviderRetirements[aliasIndex]++;
            }
        }

        Span<long> finalValues = stackalloc long[StorageCounterCount];
        Span<bool> finalAvailable = stackalloc bool[StorageCounterCount];
        try
        {
            source.CaptureRetiredStorageCounters(finalValues, finalAvailable);
        }
        finally
        {
            lock (s_gate)
            {
                AddRetiredStorageCounters(
                    aliasIndex,
                    finalValues,
                    finalAvailable);
                if (s_aliasPendingProviderRetirements[aliasIndex] > 0)
                    s_aliasPendingProviderRetirements[aliasIndex]--;
                if (s_aliasLiveSourceCounts[aliasIndex] == 0 &&
                    s_aliasPendingProviderRetirements[aliasIndex] == 0 &&
                    !HasRetiredCounters(aliasIndex))
                {
                    s_aliases[aliasIndex] = null;
                    s_aliasTags[aliasIndex] = null;
                }

                s_topologyVersion++;
            }
        }

        source.ResetRetiredStorageCounterCache();
    }

    internal static void NotifyTopologyChanged()
    {
        lock (s_gate)
            s_topologyVersion++;
    }

    internal static IEnumerable<Measurement<long>> Observe(CSharpDbMetricId metric)
    {
        // The scan is capped at the stable runtime-family limit. Providers are
        // required to return scalar snapshots and must not enumerate database
        // objects, sessions, SQL, or history records from this callback.
        const int maximumConsistencyAttempts = 4;
        for (int attempt = 0; attempt < maximumConsistencyAttempts; attempt++)
        {
            CSharpDbRuntimeMetrics?[] sources;
            long[] retiredValues;
            KeyValuePair<string, object?>[]?[] retiredTags;
            long topologyVersion;
            lock (s_gate)
            {
                sources = (CSharpDbRuntimeMetrics?[])s_sources.Clone();
                retiredValues = new long[s_aliases.Length];
                retiredTags =
                    new KeyValuePair<string, object?>[]?[s_aliases.Length];
                if (TryGetStorageCounterIndex(metric, out int counterIndex))
                {
                    for (int aliasIndex = 0;
                         aliasIndex < s_aliases.Length;
                         aliasIndex++)
                    {
                        retiredValues[aliasIndex] = s_retiredStorageCounters[
                            aliasIndex,
                            counterIndex];
                        retiredTags[aliasIndex] = s_aliasTags[aliasIndex];
                    }
                }

                topologyVersion = s_topologyVersion;
            }

            var aggregateValues = new long[sources.Length];
            var aggregateTags =
                new KeyValuePair<string, object?>[]?[sources.Length];
            var aggregateUnavailable = new bool[sources.Length];
            int aggregateCount = 0;
            for (int aliasIndex = 0;
                 aliasIndex < retiredValues.Length;
                 aliasIndex++)
            {
                long retired = retiredValues[aliasIndex];
                KeyValuePair<string, object?>[]? tags =
                    retiredTags[aliasIndex];
                if (retired <= 0 || tags is null)
                    continue;

                AddAggregate(
                    aggregateValues,
                    aggregateTags,
                    aggregateUnavailable,
                    ref aggregateCount,
                    retired,
                    tags,
                    unavailable: false);
            }

            bool retryRequired = false;
            for (int index = 0; index < sources.Length; index++)
            {
                CSharpDbRuntimeMetrics? source = sources[index];
                if (source is null)
                    continue;

                CSharpDbMetricAvailability availability = source.TryObserve(
                    metric,
                    out long value,
                    out KeyValuePair<string, object?>[] tags);
                if (availability == CSharpDbMetricAvailability.TopologyChanging)
                {
                    retryRequired = true;
                    continue;
                }
                if (availability != CSharpDbMetricAvailability.NotParticipating)
                {
                    AddAggregate(
                        aggregateValues,
                        aggregateTags,
                        aggregateUnavailable,
                        ref aggregateCount,
                        value,
                        tags,
                        availability == CSharpDbMetricAvailability.Unavailable);
                }
            }

            lock (s_gate)
            {
                if (retryRequired || topologyVersion != s_topologyVersion)
                    continue;
            }

            var measurements = new Measurement<long>[
                CountAvailable(aggregateUnavailable, aggregateCount)];
            int measurementIndex = 0;
            for (int index = 0; index < aggregateCount; index++)
            {
                if (aggregateUnavailable[index])
                    continue;

                measurements[measurementIndex++] = new Measurement<long>(
                    aggregateValues[index],
                    aggregateTags[index]!);
            }

            return measurements;
        }

        return [];
    }

    internal static IEnumerable<Measurement<double>> ObserveCheckpointAge()
    {
        const int maximumConsistencyAttempts = 4;
        for (int attempt = 0; attempt < maximumConsistencyAttempts; attempt++)
        {
            CSharpDbRuntimeMetrics?[] sources;
            long topologyVersion;
            lock (s_gate)
            {
                sources = (CSharpDbRuntimeMetrics?[])s_sources.Clone();
                topologyVersion = s_topologyVersion;
            }

            var aggregateValues = new double[sources.Length];
            var aggregateTags =
                new KeyValuePair<string, object?>[]?[sources.Length];
            var aggregateUnavailable = new bool[sources.Length];
            int aggregateCount = 0;
            bool retryRequired = false;
            for (int index = 0; index < sources.Length; index++)
            {
                CSharpDbRuntimeMetrics? source = sources[index];
                if (source is null)
                    continue;

                CSharpDbMetricAvailability availability =
                    source.TryObserveCheckpointAge(
                        out double value,
                        out KeyValuePair<string, object?>[] tags);
                if (availability == CSharpDbMetricAvailability.TopologyChanging)
                {
                    retryRequired = true;
                    continue;
                }
                if (availability == CSharpDbMetricAvailability.NotParticipating)
                    continue;

                AddMaximumAggregate(
                    aggregateValues,
                    aggregateTags,
                    aggregateUnavailable,
                    ref aggregateCount,
                    value,
                    tags,
                    availability == CSharpDbMetricAvailability.Unavailable);
            }

            lock (s_gate)
            {
                if (retryRequired || topologyVersion != s_topologyVersion)
                    continue;
            }

            var measurements = new Measurement<double>[
                CountAvailable(aggregateUnavailable, aggregateCount)];
            int measurementIndex = 0;
            for (int index = 0; index < aggregateCount; index++)
            {
                if (aggregateUnavailable[index])
                    continue;

                measurements[measurementIndex++] = new Measurement<double>(
                    aggregateValues[index],
                    aggregateTags[index]!);
            }

            return measurements;
        }

        return [];
    }

    internal static IEnumerable<Measurement<long>> ObserveMaintenanceActive()
    {
        // Five fixed maintenance classes per bounded runtime source produce a
        // stable upper bound of 320 series. Zero is an available measurement,
        // while disposed or unavailable sources are omitted.
        const int maintenanceClassCount = 5;
        const int maximumConsistencyAttempts = 4;
        for (int attempt = 0; attempt < maximumConsistencyAttempts; attempt++)
        {
            CSharpDbRuntimeMetrics?[] sources;
            long topologyVersion;
            lock (s_gate)
            {
                sources = (CSharpDbRuntimeMetrics?[])s_sources.Clone();
                topologyVersion = s_topologyVersion;
            }

            var aggregateValues =
                new long[sources.Length * maintenanceClassCount];
            var aggregateTags =
                new KeyValuePair<string, object?>[]?[
                    sources.Length * maintenanceClassCount];
            var aggregateUnavailable =
                new bool[sources.Length * maintenanceClassCount];
            int aggregateCount = 0;
            for (int sourceIndex = 0;
                 sourceIndex < sources.Length;
                 sourceIndex++)
            {
                CSharpDbRuntimeMetrics? source = sources[sourceIndex];
                if (source is null)
                    continue;

                for (int maintenanceIndex = 0;
                     maintenanceIndex < maintenanceClassCount;
                     maintenanceIndex++)
                {
                    if (source.TryObserveMaintenanceActive(
                            maintenanceIndex,
                            out long value,
                            out KeyValuePair<string, object?>[] tags))
                    {
                        AddAggregate(
                            aggregateValues,
                            aggregateTags,
                            aggregateUnavailable,
                            ref aggregateCount,
                            value,
                            tags,
                            unavailable: false);
                    }
                }
            }

            lock (s_gate)
            {
                if (topologyVersion != s_topologyVersion)
                    continue;
            }

            var measurements = new Measurement<long>[aggregateCount];
            for (int index = 0; index < aggregateCount; index++)
            {
                measurements[index] = new Measurement<long>(
                    aggregateValues[index],
                    aggregateTags[index]!);
            }

            return measurements;
        }

        return [];
    }

    private static void AddAggregate(
        long[] aggregateValues,
        KeyValuePair<string, object?>[]?[] aggregateTags,
        bool[] aggregateUnavailable,
        ref int aggregateCount,
        long value,
        KeyValuePair<string, object?>[] tags,
        bool unavailable)
    {
        for (int index = 0; index < aggregateCount; index++)
        {
            if (!TagsEqual(aggregateTags[index]!, tags))
                continue;

            aggregateValues[index] = SaturatingAdd(
                aggregateValues[index],
                value);
            aggregateUnavailable[index] |= unavailable;
            return;
        }

        aggregateValues[aggregateCount] = value;
        aggregateTags[aggregateCount] = tags;
        aggregateUnavailable[aggregateCount] = unavailable;
        aggregateCount++;
    }

    private static void AddMaximumAggregate(
        double[] aggregateValues,
        KeyValuePair<string, object?>[]?[] aggregateTags,
        bool[] aggregateUnavailable,
        ref int aggregateCount,
        double value,
        KeyValuePair<string, object?>[] tags,
        bool unavailable)
    {
        for (int index = 0; index < aggregateCount; index++)
        {
            if (!TagsEqual(aggregateTags[index]!, tags))
                continue;

            aggregateValues[index] = Math.Max(
                aggregateValues[index],
                value);
            aggregateUnavailable[index] |= unavailable;
            return;
        }

        aggregateValues[aggregateCount] = value;
        aggregateTags[aggregateCount] = tags;
        aggregateUnavailable[aggregateCount] = unavailable;
        aggregateCount++;
    }

    private static bool TagsEqual(
        KeyValuePair<string, object?>[] left,
        KeyValuePair<string, object?>[] right)
    {
        if (left.Length != right.Length)
            return false;

        for (int index = 0; index < left.Length; index++)
        {
            if (!string.Equals(left[index].Key, right[index].Key,
                    StringComparison.Ordinal) ||
                !Equals(left[index].Value, right[index].Value))
            {
                return false;
            }
        }

        return true;
    }

    private static long SaturatingAdd(long left, long right)
        => left >= long.MaxValue - right ? long.MaxValue : left + right;

    private static int CountAvailable(
        bool[] unavailable,
        int count)
    {
        int available = 0;
        for (int index = 0; index < count; index++)
        {
            if (!unavailable[index])
                available++;
        }

        return available;
    }

    internal static bool TryGetStorageCounterIndex(
        CSharpDbMetricId metric,
        out int index)
    {
        index = metric switch
        {
            CSharpDbMetricId.StoragePageReads => 0,
            CSharpDbMetricId.StoragePageWrites => 1,
            CSharpDbMetricId.StorageBytesRead => 2,
            CSharpDbMetricId.StorageBytesWritten => 3,
            CSharpDbMetricId.StorageCacheHits => 4,
            CSharpDbMetricId.StorageCacheMisses => 5,
            CSharpDbMetricId.StorageCommits => 6,
            CSharpDbMetricId.StorageConflicts => 7,
            CSharpDbMetricId.WalCommitBatches => 8,
            CSharpDbMetricId.WalBytesWritten => 9,
            CSharpDbMetricId.WalFlushedCommits => 10,
            CSharpDbMetricId.WalFlushes => 11,
            CSharpDbMetricId.WalGroupCommitBatches => 12,
            CSharpDbMetricId.WalGroupCommitCommits => 13,
            _ => -1,
        };
        return index >= 0;
    }

    private static int FindAliasIndex(string databaseAlias)
    {
        for (int index = 0; index < s_aliases.Length; index++)
        {
            if (string.Equals(
                    s_aliases[index],
                    databaseAlias,
                    StringComparison.Ordinal))
            {
                return index;
            }
        }

        return -1;
    }

    private static bool HasRetiredCounters(int aliasIndex)
    {
        for (int counterIndex = 0;
             counterIndex < StorageCounterCount;
             counterIndex++)
        {
            if (s_retiredStorageCounters[aliasIndex, counterIndex] > 0)
                return true;
        }

        return false;
    }

    private static void AddRetiredStorageCounters(
        int aliasIndex,
        ReadOnlySpan<long> values,
        ReadOnlySpan<bool> available)
    {
        for (int counterIndex = 0;
             counterIndex < StorageCounterCount;
             counterIndex++)
        {
            if (!available[counterIndex])
                continue;

            s_retiredStorageCounters[aliasIndex, counterIndex] =
                SaturatingAdd(
                    s_retiredStorageCounters[aliasIndex, counterIndex],
                    values[counterIndex]);
        }
    }

    internal static int RegisteredCount
    {
        get
        {
            lock (s_gate)
            {
                int count = 0;
                for (int index = 0; index < s_sources.Length; index++)
                {
                    if (s_sources[index] is not null)
                        count++;
                }

                return count;
            }
        }
    }

    private sealed class Registration(
        int sourceIndex,
        int aliasIndex,
        CSharpDbRuntimeMetrics source) : IDisposable
    {
        private CSharpDbRuntimeMetrics? _source = source;

        public void Dispose()
        {
            CSharpDbRuntimeMetrics? registered = Interlocked.Exchange(
                ref _source,
                null);
            if (registered is null)
                return;

            Span<long> finalValues = stackalloc long[StorageCounterCount];
            Span<bool> finalAvailable = stackalloc bool[StorageCounterCount];
            registered.CaptureRetiredStorageCounters(
                finalValues,
                finalAvailable);

            lock (s_gate)
            {
                if (!ReferenceEquals(s_sources[sourceIndex], registered))
                    return;

                AddRetiredStorageCounters(
                    aliasIndex,
                    finalValues,
                    finalAvailable);

                Volatile.Write(ref s_sources[sourceIndex], null);
                if (s_aliasLiveSourceCounts[aliasIndex] > 0)
                    s_aliasLiveSourceCounts[aliasIndex]--;
                if (s_aliasLiveSourceCounts[aliasIndex] == 0 &&
                    s_aliasPendingProviderRetirements[aliasIndex] == 0 &&
                    !HasRetiredCounters(aliasIndex))
                {
                    s_aliases[aliasIndex] = null;
                    s_aliasTags[aliasIndex] = null;
                }

                s_topologyVersion++;
            }
        }
    }
}

internal static class CSharpDbMetrics
{
    internal static Counter<long> Requests { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.Requests,
            CSharpDbMetricUnits.Request,
            "Cumulative completed query request operations by outcome.");
    internal static Counter<long> Statements { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.Statements,
            CSharpDbMetricUnits.Statement,
            "Cumulative completed query statement operations by outcome.");
    internal static Histogram<double> QueryDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.QueryDuration,
            CSharpDbMetricUnits.Seconds,
            "Completed query operation duration in seconds by outcome.");
    internal static Counter<long> RowsProduced { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.RowsProduced,
            CSharpDbMetricUnits.Row,
            "Cumulative rows produced by completed query statements.");
    internal static Counter<long> RowsAffected { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.RowsAffected,
            CSharpDbMetricUnits.Row,
            "Cumulative rows affected by completed query statements.");
    internal static Counter<long> QueriesSlow { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.QueriesSlow,
            CSharpDbMetricUnits.Query,
            "Cumulative completed query operations at or above the configured slow-query threshold.");
    private static readonly ObservableUpDownCounter<long> s_queriesActive =
        UpDown(
            CSharpDbMetricInstrumentNames.QueriesActive,
            ObserveQueriesActive,
            CSharpDbMetricUnits.Query,
            "Current active query operations.");

    internal static Counter<long> Transactions { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.Transactions,
            CSharpDbMetricUnits.Transaction,
            "Cumulative completed explicit transaction operations by outcome.");
    internal static Histogram<double> TransactionDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.TransactionDuration,
            CSharpDbMetricUnits.Seconds,
            "Completed explicit transaction duration in seconds by outcome.");
    private static readonly ObservableUpDownCounter<long> s_transactionsActive =
        UpDown(
            CSharpDbMetricInstrumentNames.TransactionsActive,
            ObserveTransactionsActive,
            CSharpDbMetricUnits.Transaction,
            "Current active explicit transaction operations.");

    internal static Counter<long> MaintenanceOperations { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.MaintenanceOperations,
            CSharpDbMetricUnits.Operation,
            "Cumulative completed backup, restore, reindex, vacuum, and generic maintenance operations by class and outcome.");
    internal static Histogram<double> MaintenanceDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.MaintenanceDuration,
            CSharpDbMetricUnits.Seconds,
            "Completed backup, restore, reindex, vacuum, and generic maintenance duration in seconds.");
    private static readonly ObservableUpDownCounter<long> s_maintenanceActive =
        UpDown(
            CSharpDbMetricInstrumentNames.MaintenanceActive,
            ObserveMaintenanceActive,
            CSharpDbMetricUnits.Operation,
            "Current active maintenance operations by bounded operation class; checkpoints are excluded.");

    internal static Counter<long> Checkpoints { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.Checkpoints,
            CSharpDbMetricUnits.Checkpoint,
            "Cumulative completed storage checkpoints, including automatic and manual checkpoints, by outcome.");
    internal static Histogram<double> CheckpointDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.CheckpointDuration,
            CSharpDbMetricUnits.Seconds,
            "Completed storage checkpoint duration in seconds by outcome.");
    private static readonly ObservableUpDownCounter<long> s_checkpointsActive =
        UpDown(
            CSharpDbMetricInstrumentNames.CheckpointsActive,
            ObserveCheckpointsActive,
            CSharpDbMetricUnits.Checkpoint,
            "Current active storage checkpoints.");
    private static readonly ObservableGauge<double> s_checkpointAge =
        Gauge(
            CSharpDbMetricInstrumentNames.CheckpointAge,
            ObserveCheckpointAge,
            CSharpDbMetricUnits.Seconds,
            "Seconds since the most recent successful checkpoint; omitted until a successful checkpoint is known.");

    internal static Counter<long> WalRecoveries { get; } =
        CSharpDbDiagnostics.Meter.CreateCounter<long>(
            CSharpDbMetricInstrumentNames.WalRecoveries,
            CSharpDbMetricUnits.Recovery,
            "Cumulative completed WAL recovery operations by outcome.");
    internal static Histogram<double> WalRecoveryDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.WalRecoveryDuration,
            CSharpDbMetricUnits.Seconds,
            "Completed WAL recovery duration in seconds by outcome.");
    private static readonly ObservableUpDownCounter<long> s_walRecoveriesActive =
        UpDown(
            CSharpDbMetricInstrumentNames.WalRecoveriesActive,
            ObserveWalRecoveriesActive,
            CSharpDbMetricUnits.Recovery,
            "Current active WAL recovery operations.");
    internal static Histogram<long> WalCommitBatchSize { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<long>(
            CSharpDbMetricInstrumentNames.WalCommitBatchSize,
            CSharpDbMetricUnits.Commit,
            "Logical commits covered by one completed WAL publication batch.");

    private static readonly ObservableGauge<long> s_storageLogicalBytes =
        Gauge(CSharpDbMetricInstrumentNames.StorageLogicalBytes, ObserveStorageLogicalBytes,
            CSharpDbMetricUnits.Bytes, "Current logical database length.");
    private static readonly ObservableGauge<long> s_storageAllocatedBytes =
        Gauge(CSharpDbMetricInstrumentNames.StorageAllocatedBytes, ObserveStorageAllocatedBytes,
            CSharpDbMetricUnits.Bytes, "Current allocated database length when the storage provider exposes it.");
    private static readonly ObservableGauge<long> s_storagePageCount =
        Gauge(CSharpDbMetricInstrumentNames.StoragePageCount, ObserveStoragePageCount,
            CSharpDbMetricUnits.Page, "Current logical database page count.");
    private static readonly ObservableCounter<long> s_storagePageReads =
        Counter(CSharpDbMetricInstrumentNames.StoragePageReads, ObserveStoragePageReads,
            CSharpDbMetricUnits.Page, "Cumulative logical storage page reads, including cache hits and misses.");
    private static readonly ObservableCounter<long> s_storagePageWrites =
        Counter(CSharpDbMetricInstrumentNames.StoragePageWrites, ObserveStoragePageWrites,
            CSharpDbMetricUnits.Page, "Cumulative logical storage pages written to committed WAL frames.");
    private static readonly ObservableCounter<long> s_storageBytesRead =
        Counter(CSharpDbMetricInstrumentNames.StorageBytesRead, ObserveStorageBytesRead,
            CSharpDbMetricUnits.Bytes, "Cumulative logical storage bytes read, derived from logical page reads.");
    private static readonly ObservableCounter<long> s_storageBytesWritten =
        Counter(CSharpDbMetricInstrumentNames.StorageBytesWritten, ObserveStorageBytesWritten,
            CSharpDbMetricUnits.Bytes, "Cumulative logical storage bytes written, derived from logical page writes.");
    private static readonly ObservableCounter<long> s_storageCacheHits =
        Counter(CSharpDbMetricInstrumentNames.StorageCacheHits, ObserveStorageCacheHits,
            CSharpDbMetricUnits.Page, "Cumulative logical page-cache hits.");
    private static readonly ObservableCounter<long> s_storageCacheMisses =
        Counter(CSharpDbMetricInstrumentNames.StorageCacheMisses, ObserveStorageCacheMisses,
            CSharpDbMetricUnits.Page, "Cumulative logical page-cache misses.");
    private static readonly ObservableGauge<long> s_storageDirtyPages =
        Gauge(CSharpDbMetricInstrumentNames.StorageDirtyPages, ObserveStorageDirtyPages,
            CSharpDbMetricUnits.Page, "Current dirty storage page count when supported.");
    private static readonly ObservableUpDownCounter<long> s_storageReadersActive =
        UpDown(CSharpDbMetricInstrumentNames.StorageReadersActive, ObserveStorageReadersActive,
            CSharpDbMetricUnits.Reader, "Current active storage reader leases.");
    private static readonly ObservableUpDownCounter<long> s_storageWritersActive =
        UpDown(CSharpDbMetricInstrumentNames.StorageWritersActive, ObserveStorageWritersActive,
            CSharpDbMetricUnits.Writer, "Current active storage writer leases.");
    private static readonly ObservableCounter<long> s_storageCommits =
        Counter(CSharpDbMetricInstrumentNames.StorageCommits, ObserveStorageCommits,
            CSharpDbMetricUnits.Commit, "Cumulative logical storage commits.");
    private static readonly ObservableCounter<long> s_storageConflicts =
        Counter(CSharpDbMetricInstrumentNames.StorageConflicts, ObserveStorageConflicts,
            CSharpDbMetricUnits.Conflict, "Cumulative terminal storage transaction conflicts.");

    private static readonly ObservableGauge<long> s_walLogicalBytes =
        Gauge(CSharpDbMetricInstrumentNames.WalLogicalBytes, ObserveWalLogicalBytes,
            CSharpDbMetricUnits.Bytes, "Current logical WAL length.");
    private static readonly ObservableGauge<long> s_walAllocatedBytes =
        Gauge(CSharpDbMetricInstrumentNames.WalAllocatedBytes, ObserveWalAllocatedBytes,
            CSharpDbMetricUnits.Bytes, "Current allocated WAL length when supported.");
    private static readonly ObservableGauge<long> s_walCommittedBytes =
        Gauge(CSharpDbMetricInstrumentNames.WalCommittedBytes, ObserveWalCommittedBytes,
            CSharpDbMetricUnits.Bytes, "Current committed WAL frame bytes.");
    private static readonly ObservableGauge<long> s_walRetainedBytes =
        Gauge(CSharpDbMetricInstrumentNames.WalRetainedBytes, ObserveWalRetainedBytes,
            CSharpDbMetricUnits.Bytes, "Current WAL bytes retained by readers or checkpoint state.");
    private static readonly ObservableGauge<long> s_walFrameCount =
        Gauge(CSharpDbMetricInstrumentNames.WalFrameCount, ObserveWalFrameCount,
            CSharpDbMetricUnits.Frame, "Current WAL frame count.");
    private static readonly ObservableCounter<long> s_walCommitBatches =
        Counter(CSharpDbMetricInstrumentNames.WalCommitBatches, ObserveWalCommitBatches,
            CSharpDbMetricUnits.Batch, "Cumulative WAL commit-publication batches.");
    private static readonly ObservableCounter<long> s_walBytesWritten =
        Counter(CSharpDbMetricInstrumentNames.WalBytesWritten, ObserveWalBytesWritten,
            CSharpDbMetricUnits.Bytes, "Cumulative committed WAL frame bytes written.");
    private static readonly ObservableUpDownCounter<long> s_walPendingCommits =
        UpDown(CSharpDbMetricInstrumentNames.WalPendingCommits, ObserveWalPendingCommits,
            CSharpDbMetricUnits.Commit, "Current logical commits awaiting WAL publication.");
    private static readonly ObservableCounter<long> s_walFlushedCommits =
        Counter(CSharpDbMetricInstrumentNames.WalFlushedCommits, ObserveWalFlushedCommits,
            CSharpDbMetricUnits.Commit, "Cumulative logical commits covered by WAL publication batches.");
    private static readonly ObservableCounter<long> s_walFlushes =
        Counter(CSharpDbMetricInstrumentNames.WalFlushes, ObserveWalFlushes,
            CSharpDbMetricUnits.Flush, "Cumulative successful durable WAL flush-to-storage calls.");
    private static readonly ObservableCounter<long> s_walGroupCommitBatches =
        Counter(CSharpDbMetricInstrumentNames.WalGroupCommitBatches, ObserveWalGroupCommitBatches,
            CSharpDbMetricUnits.Batch, "Cumulative WAL publication batches containing at least two commits.");
    private static readonly ObservableCounter<long> s_walGroupCommitCommits =
        Counter(CSharpDbMetricInstrumentNames.WalGroupCommitCommits, ObserveWalGroupCommitCommits,
            CSharpDbMetricUnits.Commit, "Cumulative commits covered by grouped WAL publication batches.");

    private static readonly ObservableUpDownCounter<long> s_sessionsActive =
        UpDown(CSharpDbMetricInstrumentNames.SessionsActive, ObserveSessionsActive,
            CSharpDbMetricUnits.Session, "Current active logical database sessions.");
    private static readonly ObservableUpDownCounter<long> s_readersActive =
        UpDown(CSharpDbMetricInstrumentNames.ReadersActive, ObserveReadersActive,
            CSharpDbMetricUnits.Reader, "Current logical sessions retaining a query result reader.");
    private static readonly ObservableUpDownCounter<long> s_poolWaiters =
        UpDown(CSharpDbMetricInstrumentNames.PoolWaiters, ObservePoolWaiters,
            CSharpDbMetricUnits.Request, "Current requests waiting for a connection-pool session slot.");
    private static readonly ObservableGauge<long> s_connectionsAvailable =
        Gauge(CSharpDbMetricInstrumentNames.ConnectionsAvailable, ObserveConnectionsAvailable,
            CSharpDbMetricUnits.Connection, "Current available connection-pool session slots.");

    private static readonly ObservableGauge<long> s_healthStatus =
        Gauge(CSharpDbMetricInstrumentNames.HealthStatus, ObserveHealthStatus,
            CSharpDbMetricUnits.Status,
            "Current liveness and readiness status; the one current status series per check has value 1.");

    internal static Histogram<double> PoolWaitDuration { get; } =
        CSharpDbDiagnostics.Meter.CreateHistogram<double>(
            CSharpDbMetricInstrumentNames.PoolWaitDuration,
            CSharpDbMetricUnits.Seconds,
            "Duration of contended connection-pool session-slot waits.");

    // An explicit type initializer makes EnsureInitialized a reliable rooting
    // seam under JIT, trimming, and NativeAOT (the type is not beforefieldinit).
    static CSharpDbMetrics()
    {
    }

    internal static void EnsureInitialized()
    {
    }

    private static ObservableCounter<long> Counter(
        string name,
        Func<IEnumerable<Measurement<long>>> observe,
        string unit,
        string description)
        => CSharpDbDiagnostics.Meter.CreateObservableCounter(
            name,
            observe,
            unit,
            description);

    private static ObservableGauge<long> Gauge(
        string name,
        Func<IEnumerable<Measurement<long>>> observe,
        string unit,
        string description)
        => CSharpDbDiagnostics.Meter.CreateObservableGauge(
            name,
            observe,
            unit,
            description);

    private static ObservableGauge<double> Gauge(
        string name,
        Func<IEnumerable<Measurement<double>>> observe,
        string unit,
        string description)
        => CSharpDbDiagnostics.Meter.CreateObservableGauge(
            name,
            observe,
            unit,
            description);

    private static ObservableUpDownCounter<long> UpDown(
        string name,
        Func<IEnumerable<Measurement<long>>> observe,
        string unit,
        string description)
        => CSharpDbDiagnostics.Meter.CreateObservableUpDownCounter(
            name,
            observe,
            unit,
            description);

    private static IEnumerable<Measurement<long>> ObserveQueriesActive() => Observe(CSharpDbMetricId.QueriesActive);
    private static IEnumerable<Measurement<long>> ObserveTransactionsActive() => Observe(CSharpDbMetricId.TransactionsActive);
    private static IEnumerable<Measurement<long>> ObserveMaintenanceActive() => CSharpDbRuntimeMetricsRegistry.ObserveMaintenanceActive();
    private static IEnumerable<Measurement<long>> ObserveCheckpointsActive() => Observe(CSharpDbMetricId.CheckpointsActive);
    private static IEnumerable<Measurement<double>> ObserveCheckpointAge() => CSharpDbRuntimeMetricsRegistry.ObserveCheckpointAge();
    private static IEnumerable<Measurement<long>> ObserveWalRecoveriesActive() => Observe(CSharpDbMetricId.WalRecoveriesActive);
    private static IEnumerable<Measurement<long>> ObserveStorageLogicalBytes() => Observe(CSharpDbMetricId.StorageLogicalBytes);
    private static IEnumerable<Measurement<long>> ObserveStorageAllocatedBytes() => Observe(CSharpDbMetricId.StorageAllocatedBytes);
    private static IEnumerable<Measurement<long>> ObserveStoragePageCount() => Observe(CSharpDbMetricId.StoragePageCount);
    private static IEnumerable<Measurement<long>> ObserveStoragePageReads() => Observe(CSharpDbMetricId.StoragePageReads);
    private static IEnumerable<Measurement<long>> ObserveStoragePageWrites() => Observe(CSharpDbMetricId.StoragePageWrites);
    private static IEnumerable<Measurement<long>> ObserveStorageBytesRead() => Observe(CSharpDbMetricId.StorageBytesRead);
    private static IEnumerable<Measurement<long>> ObserveStorageBytesWritten() => Observe(CSharpDbMetricId.StorageBytesWritten);
    private static IEnumerable<Measurement<long>> ObserveStorageCacheHits() => Observe(CSharpDbMetricId.StorageCacheHits);
    private static IEnumerable<Measurement<long>> ObserveStorageCacheMisses() => Observe(CSharpDbMetricId.StorageCacheMisses);
    private static IEnumerable<Measurement<long>> ObserveStorageDirtyPages() => Observe(CSharpDbMetricId.StorageDirtyPages);
    private static IEnumerable<Measurement<long>> ObserveStorageReadersActive() => Observe(CSharpDbMetricId.StorageReadersActive);
    private static IEnumerable<Measurement<long>> ObserveStorageWritersActive() => Observe(CSharpDbMetricId.StorageWritersActive);
    private static IEnumerable<Measurement<long>> ObserveStorageCommits() => Observe(CSharpDbMetricId.StorageCommits);
    private static IEnumerable<Measurement<long>> ObserveStorageConflicts() => Observe(CSharpDbMetricId.StorageConflicts);
    private static IEnumerable<Measurement<long>> ObserveWalLogicalBytes() => Observe(CSharpDbMetricId.WalLogicalBytes);
    private static IEnumerable<Measurement<long>> ObserveWalAllocatedBytes() => Observe(CSharpDbMetricId.WalAllocatedBytes);
    private static IEnumerable<Measurement<long>> ObserveWalCommittedBytes() => Observe(CSharpDbMetricId.WalCommittedBytes);
    private static IEnumerable<Measurement<long>> ObserveWalRetainedBytes() => Observe(CSharpDbMetricId.WalRetainedBytes);
    private static IEnumerable<Measurement<long>> ObserveWalFrameCount() => Observe(CSharpDbMetricId.WalFrameCount);
    private static IEnumerable<Measurement<long>> ObserveWalCommitBatches() => Observe(CSharpDbMetricId.WalCommitBatches);
    private static IEnumerable<Measurement<long>> ObserveWalBytesWritten() => Observe(CSharpDbMetricId.WalBytesWritten);
    private static IEnumerable<Measurement<long>> ObserveWalPendingCommits() => Observe(CSharpDbMetricId.WalPendingCommits);
    private static IEnumerable<Measurement<long>> ObserveWalFlushedCommits() => Observe(CSharpDbMetricId.WalFlushedCommits);
    private static IEnumerable<Measurement<long>> ObserveWalFlushes() => Observe(CSharpDbMetricId.WalFlushes);
    private static IEnumerable<Measurement<long>> ObserveWalGroupCommitBatches() => Observe(CSharpDbMetricId.WalGroupCommitBatches);
    private static IEnumerable<Measurement<long>> ObserveWalGroupCommitCommits() => Observe(CSharpDbMetricId.WalGroupCommitCommits);
    private static IEnumerable<Measurement<long>> ObserveSessionsActive() => Observe(CSharpDbMetricId.SessionsActive);
    private static IEnumerable<Measurement<long>> ObserveReadersActive() => Observe(CSharpDbMetricId.ReadersActive);
    private static IEnumerable<Measurement<long>> ObservePoolWaiters() => Observe(CSharpDbMetricId.PoolWaiters);
    private static IEnumerable<Measurement<long>> ObserveConnectionsAvailable() => Observe(CSharpDbMetricId.ConnectionsAvailable);
    private static IEnumerable<Measurement<long>> ObserveHealthStatus() => CSharpDbHealthMetricsRegistry.Observe();

    private static IEnumerable<Measurement<long>> Observe(CSharpDbMetricId metric)
        => CSharpDbRuntimeMetricsRegistry.Observe(metric);
}

internal static class CSharpDbMetricTagValues
{
    internal static string OperationClass(CSharpDbOperationClass value)
        => value switch
        {
            CSharpDbOperationClass.Query => "query",
            CSharpDbOperationClass.Script => "script",
            CSharpDbOperationClass.Procedure => "procedure",
            CSharpDbOperationClass.Transaction => "transaction",
            CSharpDbOperationClass.Database => "database",
            CSharpDbOperationClass.Recovery => "recovery",
            CSharpDbOperationClass.Checkpoint => "checkpoint",
            CSharpDbOperationClass.Backup => "backup",
            CSharpDbOperationClass.Restore => "restore",
            CSharpDbOperationClass.Reindex => "reindex",
            CSharpDbOperationClass.Vacuum => "vacuum",
            CSharpDbOperationClass.Maintenance => "maintenance",
            CSharpDbOperationClass.Pipeline => "pipeline",
            _ => throw new ArgumentOutOfRangeException(nameof(value)),
        };

    internal static string Transport(CSharpDbTransport value)
        => value switch
        {
            CSharpDbTransport.Embedded => "embedded",
            CSharpDbTransport.Direct => "direct",
            CSharpDbTransport.Http => "http",
            CSharpDbTransport.Grpc => "grpc",
            CSharpDbTransport.Tcp => "tcp",
            CSharpDbTransport.NamedPipe => "namedpipe",
            CSharpDbTransport.Sharded => "sharded",
            _ => throw new ArgumentOutOfRangeException(nameof(value)),
        };

    internal static string Outcome(CSharpDbOperationOutcome value)
        => value switch
        {
            CSharpDbOperationOutcome.Succeeded => "succeeded",
            CSharpDbOperationOutcome.Failed => "failed",
            CSharpDbOperationOutcome.Canceled => "canceled",
            CSharpDbOperationOutcome.Rejected => "rejected",
            _ => throw new ArgumentOutOfRangeException(nameof(value)),
        };

    internal static string HealthCheckKind(CSharpDbHealthCheckKind value)
        => value switch
        {
            CSharpDbHealthCheckKind.Liveness => "liveness",
            CSharpDbHealthCheckKind.Readiness => "readiness",
            CSharpDbHealthCheckKind.Database => "database",
            CSharpDbHealthCheckKind.Storage => "storage",
            CSharpDbHealthCheckKind.Wal => "wal",
            _ => throw new ArgumentOutOfRangeException(nameof(value)),
        };

    internal static string HealthStatus(CSharpDbHealthStatus value)
        => value switch
        {
            CSharpDbHealthStatus.Healthy => "healthy",
            CSharpDbHealthStatus.Degraded => "degraded",
            CSharpDbHealthStatus.Unhealthy => "unhealthy",
            _ => throw new ArgumentOutOfRangeException(nameof(value)),
        };
}
