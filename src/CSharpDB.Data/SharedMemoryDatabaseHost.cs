using System.Collections.Concurrent;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class SharedMemoryDatabaseRegistry
{
    private static readonly ConcurrentDictionary<string, SharedMemoryDatabaseHost> s_hosts = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<SharedMemoryDatabaseHost, byte>
        s_diagnosticHosts = new();
    private static readonly ConcurrentDictionary<SharedMemoryDatabaseHost, byte>
        s_observedRetirements = new();

    internal static async ValueTask<ICSharpDbSession> OpenSessionAsync(
        string name,
        string? loadFromPath,
        DatabaseOptions? databaseOptions,
        DatabaseOptions runtimeDatabaseOptions,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (s_hosts.TryGetValue(name, out var existing))
            {
                try
                {
                    return await existing.OpenSessionAsync(
                        loadFromPath,
                        databaseOptions,
                        cancellationToken);
                }
                finally
                {
                    if (!ReferenceEquals(
                            runtimeDiagnosticsStateOwner,
                            existing.RuntimeDiagnosticsStateOwner))
                    {
                        runtimeDiagnosticsStateOwner?.Dispose();
                    }
                }
            }

            var created = new SharedMemoryDatabaseHost(
                name,
                databaseOptions,
                runtimeDatabaseOptions,
                runtimeDiagnosticsStateOwner);
            if (!s_hosts.TryAdd(name, created))
            {
                // The candidate has not been adopted. Keep its owner live for
                // the next iteration: the winning host may disappear before
                // checkout, in which case this same resolved family can still
                // become the replacement without reusing a disposed state.
                continue;
            }

            TryRegisterDiagnosticHost(created);

            try
            {
                return await created.OpenSessionAsync(
                    loadFromPath,
                    databaseOptions,
                    cancellationToken);
            }
            catch
            {
                s_hosts.TryRemove(new KeyValuePair<string, SharedMemoryDatabaseHost>(name, created));
                try
                {
                    await created.DisableAsync();
                }
                finally
                {
                    ObserveRetirement(created);
                }
                throw;
            }
        }
    }

    internal static async ValueTask ClearAsync(string name)
    {
        if (s_hosts.TryRemove(name, out var host))
        {
            try
            {
                await host.DisableAsync();
            }
            finally
            {
                ObserveRetirement(host);
            }
        }
    }

    internal static async ValueTask ClearAllAsync()
    {
        var hosts = s_hosts.ToArray();
        s_hosts.Clear();

        foreach (var pair in hosts)
        {
            try
            {
                await pair.Value.DisableAsync();
            }
            finally
            {
                ObserveRetirement(pair.Value);
            }
        }
    }

    internal static int GetHostCountForTest() => s_hosts.Count;

    private static void ObserveRetirement(SharedMemoryDatabaseHost host)
    {
        if (!host.IsRuntimeDiagnosticsEnabled)
            return;

        try
        {
            if (!s_observedRetirements.TryAdd(host, 0))
                return;

            AsyncFlowControl flowControl = default;
            bool flowSuppressed = false;
            try
            {
                if (!ExecutionContext.IsFlowSuppressed())
                {
                    flowControl = ExecutionContext.SuppressFlow();
                    flowSuppressed = true;
                }
                _ = ObserveRetirementAsync(host);
            }
            finally
            {
                if (flowSuppressed)
                    flowControl.Undo();
            }
        }
        catch
        {
            // Diagnostics retention cannot replace host retirement behavior.
            try
            {
                s_observedRetirements.TryRemove(host, out _);
            }
            catch
            {
                // Best-effort cleanup after a diagnostic-only failure.
            }
        }
    }

    private static void TryRegisterDiagnosticHost(SharedMemoryDatabaseHost host)
    {
        if (!host.IsRuntimeDiagnosticsEnabled)
            return;

        try
        {
            s_diagnosticHosts.TryAdd(host, 0);
        }
        catch
        {
            // Diagnostics registration cannot replace host creation.
        }
    }

    private static async Task ObserveRetirementAsync(SharedMemoryDatabaseHost host)
    {
        try
        {
            await host.Retirement.ConfigureAwait(false);
        }
        catch
        {
            // The final live snapshot already exposes the failed owner state.
        }
        finally
        {
            s_diagnosticHosts.TryRemove(host, out _);
            s_observedRetirements.TryRemove(host, out _);
        }
    }

    internal static async ValueTask<DataRuntimeDiagnosticsRegistrySnapshot>
        CaptureRuntimeDiagnosticsAsync(
            int maximumContributorRecords,
            int maximumSessionRecordsPerContributor,
            CancellationToken cancellationToken = default)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumContributorRecords,
            nameof(maximumContributorRecords));
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecordsPerContributor,
            nameof(maximumSessionRecordsPerContributor));

        int sourceCount = s_diagnosticHosts.Count;
        SharedMemoryDatabaseHost[] hosts = s_diagnosticHosts.Keys
            .Take(maximumContributorRecords)
            .ToArray();
        var snapshots = new List<DataConnectionDiagnosticsRawSnapshot>(hosts.Length);
        foreach (SharedMemoryDatabaseHost host in hosts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                DataConnectionDiagnosticsRawSnapshot? snapshot =
                    await host.CaptureRuntimeDiagnosticsAsync(
                        maximumSessionRecordsPerContributor,
                        cancellationToken);
                if (snapshot is not null)
                    snapshots.Add(snapshot);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // A racing host is represented by capture truncation below.
            }
        }

        DataConnectionDiagnosticsRawSnapshot[] ordered = snapshots
            .OrderBy(static snapshot => snapshot.ContributorId.Value, StringComparer.Ordinal)
            .Take(maximumContributorRecords)
            .ToArray();
        const long dropped = 0;
        bool truncated = sourceCount > ordered.Length;
        return new DataRuntimeDiagnosticsRegistrySnapshot(
            Array.AsReadOnly(ordered),
            maximumContributorRecords,
            dropped,
            truncated);
    }
}

internal sealed class SharedMemoryDatabaseHost : IDataRuntimeDiagnosticsContributor
{
    private const string BusyMessage = "Database is busy with an active transaction.";

    private readonly string _name;
    private readonly DatabaseOptions? _databaseOptions;
    private readonly DatabaseOptions _runtimeDatabaseOptions;
    private readonly string? _observabilityConfiguration;
    private readonly CSharpDbObservabilityOptions? _observabilityOptionsSnapshot;
    private readonly bool _lifecycleLoggingEnabled;
    private readonly DataSessionRuntimeDiagnostics? _runtimeDiagnostics;
    private readonly DataRuntimeDiagnosticsStateOwner? _runtimeDiagnosticsStateOwner;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly TaskCompletionSource _retirement =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private Database? _database;
    private bool _disabled;
    private int _activeSessionCount;
    private long _nextSessionId;
    private long? _transactionOwnerSessionId;
    private long? _transactionStartedTimestamp;
    private Database? _transactionSnapshotDatabase;
    private string? _transactionSnapshotPath;
    private bool _seedConfigured;
    private string? _seedSourcePath;

    internal SharedMemoryDatabaseHost(
        string name,
        DatabaseOptions? databaseOptions,
        DatabaseOptions runtimeDatabaseOptions,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null)
    {
        _name = name;
        _databaseOptions = databaseOptions;
        _runtimeDatabaseOptions = runtimeDatabaseOptions ??
            throw new ArgumentNullException(nameof(runtimeDatabaseOptions));
        _observabilityConfiguration = SerializeObservabilityConfiguration(databaseOptions);
        _observabilityOptionsSnapshot = runtimeDatabaseOptions.ObservabilityOptions;
        _lifecycleLoggingEnabled =
            DataLifecycleDiagnosticBoundary.IsLifecycleLoggingEnabled(
                _observabilityOptionsSnapshot);
        _runtimeDiagnostics = DataSessionRuntimeDiagnostics.Create(
            _observabilityOptionsSnapshot,
            DataConnectionOwnerKind.SharedMemory,
            runtimeDatabaseOptions.RuntimeDiagnosticsState?.TimeProvider);
        _runtimeDiagnosticsStateOwner = runtimeDiagnosticsStateOwner;
    }

    internal CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _observabilityOptionsSnapshot;
    internal bool IsRuntimeDiagnosticsEnabled => _runtimeDiagnostics is not null;
    internal DataRuntimeDiagnosticsStateOwner? RuntimeDiagnosticsStateOwner =>
        _runtimeDiagnosticsStateOwner;
    internal CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _runtimeDatabaseOptions.RuntimeDiagnosticsState;
    internal Task Retirement => _retirement.Task;

    public async ValueTask<DataConnectionDiagnosticsRawSnapshot?> CaptureRuntimeDiagnosticsAsync(
        int maximumSessionRecords,
        CancellationToken cancellationToken = default)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecords,
            nameof(maximumSessionRecords));
        DataSessionRuntimeDiagnostics? diagnostics = _runtimeDiagnostics;
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

            SharedHostDiagnosticsState state;
            await _gate.WaitAsync(cancellationToken);
            try
            {
                ConnectionPoolLifecycleState lifecycleState = _disabled
                    ? Retirement.IsCompletedSuccessfully
                        ? ConnectionPoolLifecycleState.Retired
                        : Retirement.IsFaulted
                            ? ConnectionPoolLifecycleState.Poisoned
                            : ConnectionPoolLifecycleState.Retiring
                    : ConnectionPoolLifecycleState.Enabled;
                state = new SharedHostDiagnosticsState(
                    _activeSessionCount,
                    _database is not null && _activeSessionCount == 0 && !_disabled ? 1 : 0,
                    _transactionOwnerSessionId,
                    _transactionStartedTimestamp,
                    lifecycleState,
                    _disabled,
                    Retirement.IsCompletedSuccessfully,
                    Retirement.IsFaulted);
            }
            finally
            {
                _gate.Release();
            }

            DataSessionRuntimeDiagnostics.SessionStateBatch sessionBatch =
                diagnostics.CopySessions(
                    maximumSessionRecords,
                    state.TransactionOwnerSessionKey);
            DataSessionRuntimeDiagnostics.SessionStateCopy[] sessionCopies =
                sessionBatch.Records;
            DateTimeOffset snapshotAtUtc = diagnostics.GetUtcNowOrLast();
            long? snapshotTimestamp = state.TransactionOwnerSessionKey.HasValue
                ? diagnostics.GetTimestampOrNull()
                : null;
            if (!diagnostics.Consistency.IsReadValid(version))
            {
                await Task.Yield();
                continue;
            }
            if (sessionBatch.TotalCount != state.ActiveLogicalSessions)
                return null;

            DataSessionDiagnosticsRawSnapshot[] sessions =
                DataSessionRuntimeDiagnostics.ProjectSessions(
                    sessionCopies,
                    state.TransactionOwnerSessionKey);
            bool sessionsTruncated =
                sessionBatch.TotalCount > sessionCopies.Length;
            OpaqueDiagnosticsId? transactionOwnerSessionId =
                state.TransactionOwnerSessionKey is long ownerKey
                    ? sessionCopies.FirstOrDefault(
                        copy => copy.SessionKey == ownerKey).SessionId
                    : null;
            TimeSpan? oldestTransactionAge =
                state.TransactionOwnerSessionKey.HasValue
                    ? diagnostics.GetElapsedTimeOrNull(
                        state.TransactionStartedTimestamp,
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
                state.ActiveLogicalSessions,
                sessionBatch.ActiveReaderCount,
                state.TransactionOwnerSessionKey.HasValue ? 1 : 0,
                oldestTransactionAge,
                state.WarmEngineIdleCount,
                state.RetirementCompleted ? 1 : 0,
                state.RetirementFailed ? 1 : 0,
                state.Disabled ? 1 : 0,
                state.Disabled && !state.RetirementCompleted && !state.RetirementFailed ? 1 : 0,
                transactionOwnerSessionId,
                state.PoolState,
                Array.AsReadOnly(sessions),
                maximumSessionRecords,
                0,
                sessionsTruncated);
        }

        return null;
    }

    private readonly record struct SharedHostDiagnosticsState(
        int ActiveLogicalSessions,
        int WarmEngineIdleCount,
        long? TransactionOwnerSessionKey,
        long? TransactionStartedTimestamp,
        ConnectionPoolLifecycleState PoolState,
        bool Disabled,
        bool RetirementCompleted,
        bool RetirementFailed);

    internal async ValueTask<SharedMemoryDatabaseSession> OpenSessionAsync(
        string? loadFromPath,
        DatabaseOptions? databaseOptions,
        CancellationToken cancellationToken = default)
    {
        DateTimeOffset diagnosticsCreatedAtUtc =
            _runtimeDiagnostics?.GetUtcNowOrLast() ?? default;
        OpaqueDiagnosticsId? preferredSessionId = _runtimeDiagnostics is null
            ? null
            : CSharpDbOperationScope.CurrentSessionId;
        bool diagnosticsMutationStarted = false;
        long sessionId = 0;
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_disabled)
                throw new InvalidOperationException("The shared in-memory database is no longer accepting new sessions.");
            if (!ReferenceEquals(_databaseOptions, databaseOptions) ||
                !string.Equals(
                    _observabilityConfiguration,
                    SerializeObservabilityConfiguration(databaseOptions),
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Shared in-memory database '{_name}' was already initialized with different DatabaseOptions.");
            }

            string? normalizedLoadPath = NormalizeOptionalPath(loadFromPath);
            await EnsureInitializedAsync(normalizedLoadPath, cancellationToken);

            if (_runtimeDiagnostics is not null)
            {
                _runtimeDiagnostics.Consistency.BeginMutation();
                diagnosticsMutationStarted = true;
            }
            _activeSessionCount++;
            sessionId = ++_nextSessionId;
        }
        finally
        {
            _gate.Release();
        }

        if (_runtimeDiagnostics is not null)
        {
            try
            {
                _runtimeDiagnostics.RegisterSession(
                    sessionId,
                    preferredSessionId,
                    diagnosticsCreatedAtUtc);
            }
            finally
            {
                if (diagnosticsMutationStarted)
                    _runtimeDiagnostics.Consistency.EndMutation();
            }
        }

        return new SharedMemoryDatabaseSession(this, sessionId);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string sql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, sql, sql, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            executionSql,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(executionSql);

        DataSessionOperationLease? diagnosticsOperation =
            _runtimeDiagnostics?.TryBeginOperation(sessionId);
        bool gateHeld = false;
        try
        {
            using (observation?.MeasureQueueWait())
                await _gate.WaitAsync(cancellationToken);
            gateHeld = true;
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            if (OwnedByOtherSession(sessionId))
            {
                var statement = Parser.Parse(executionSql);
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                Database snapshotDatabase = GetTransactionSnapshotDatabase();
                QueryFingerprint? fingerprint =
                    QueryObservabilitySource.CreateFingerprint(snapshotDatabase, observabilitySql);
                observation?.MarkDispatchHandoff(snapshotDatabase);
                await using var query = await snapshotDatabase.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                QueryResult detached = await DetachQueryResultAsync(query, cancellationToken);
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    detached);
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            observation?.MarkDispatchHandoff(database);
            await using var liveQuery = await database.ExecuteAsync(
                executionSql,
                observabilitySql,
                cancellationToken);
            QueryResult liveDetached = await DetachQueryResultAsync(liveQuery, cancellationToken);
            return CompleteObservedResult(
                ref gateHeld,
                diagnosticsOperation,
                liveDetached);
        }
        catch
        {
            ReleaseGateIfHeld(ref gateHeld);
            diagnosticsOperation?.Complete();
            throw;
        }
        finally
        {
            if (gateHeld)
                _gate.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, statement, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            statement,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(statement);

        DataSessionOperationLease? diagnosticsOperation =
            _runtimeDiagnostics?.TryBeginOperation(sessionId);
        bool gateHeld = false;
        try
        {
            using (observation?.MeasureQueueWait())
                await _gate.WaitAsync(cancellationToken);
            gateHeld = true;
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            if (OwnedByOtherSession(sessionId))
            {
                if (!IsReadOnly(statement))
                    throw new InvalidOperationException(BusyMessage);

                Database snapshotDatabase = GetTransactionSnapshotDatabase();
                QueryFingerprint? fingerprint =
                    QueryObservabilitySource.CreateFingerprint(snapshotDatabase, observabilitySql);
                observation?.MarkDispatchHandoff(snapshotDatabase);
                await using var query = await snapshotDatabase.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                QueryResult detached = await DetachQueryResultAsync(query, cancellationToken);
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    detached);
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            QueryFingerprint? liveFingerprint =
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
            observation?.MarkDispatchHandoff(database);
            await using var liveQuery = await database.ExecuteAsync(
                statement,
                liveFingerprint,
                cancellationToken);
            QueryResult liveDetached = await DetachQueryResultAsync(liveQuery, cancellationToken);
            return CompleteObservedResult(
                ref gateHeld,
                diagnosticsOperation,
                liveDetached);
        }
        catch
        {
            ReleaseGateIfHeld(ref gateHeld);
            diagnosticsOperation?.Complete();
            throw;
        }
        finally
        {
            if (gateHeld)
                _gate.Release();
        }
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(sessionId, insert, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(
            sessionId,
            insert,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal async ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
    {
        DataSessionOperationLease? diagnosticsOperation =
            _runtimeDiagnostics?.TryBeginOperation(sessionId);
        bool gateHeld = false;
        try
        {
            using (observation?.MeasureQueueWait())
                await _gate.WaitAsync(cancellationToken);
            gateHeld = true;
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            if (OwnedByOtherSession(sessionId))
                throw new InvalidOperationException(BusyMessage);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            QueryFingerprint? fingerprint =
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
            observation?.MarkDispatchHandoff(database);
            await using var query = await database.ExecuteAsync(
                insert,
                fingerprint,
                cancellationToken);
            QueryResult result = new(query.RowsAffected);
            return CompleteObservedResult(
                ref gateHeld,
                diagnosticsOperation,
                result);
        }
        catch
        {
            ReleaseGateIfHeld(ref gateHeld);
            diagnosticsOperation?.Complete();
            throw;
        }
        finally
        {
            if (gateHeld)
                _gate.Release();
        }
    }

    private QueryResult CompleteObservedResult(
        ref bool gateHeld,
        DataSessionOperationLease? diagnosticsOperation,
        QueryResult result)
    {
        ReleaseGateIfHeld(ref gateHeld);
        return diagnosticsOperation?.ObserveResult(result) ?? result;
    }

    private void ReleaseGateIfHeld(ref bool gateHeld)
    {
        if (!gateHeld)
            return;

        _gate.Release();
        gateHeld = false;
    }

    internal async ValueTask BeginTransactionAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        DateTimeOffset? transactionStartedAtUtc = null;
        long? transactionStartedTimestamp = null;
        bool diagnosticsMutationStarted = false;
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
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            snapshotPath = Path.Combine(Path.GetTempPath(), $"csharpdb_shared_snapshot_{Guid.NewGuid():N}.db");
            await database.SaveToFileAsync(snapshotPath, cancellationToken);
            snapshotDatabase = await Database.LoadIntoMemoryAsync(
                snapshotPath,
                _runtimeDatabaseOptions,
                cancellationToken);
            await database.BeginTransactionAsync(cancellationToken);
            if (_runtimeDiagnostics is not null)
            {
                transactionStartedAtUtc = _runtimeDiagnostics.GetUtcNowOrLast();
                transactionStartedTimestamp =
                    _runtimeDiagnostics.GetTimestampOrNull();
            }
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _transactionOwnerSessionId = sessionId;
            _transactionStartedTimestamp = transactionStartedTimestamp;
            _transactionSnapshotDatabase = snapshotDatabase;
            _transactionSnapshotPath = snapshotPath;
            snapshotDatabase = null;
            snapshotPath = null;
        }
        finally
        {
            _gate.Release();
            CompleteDiagnosticsMutation(
                diagnosticsMutationStarted,
                sessionId,
                transactionStartedAtUtc);
        }

        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask CommitAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        DateTimeOffset? completedAtUtc = TryGetDiagnosticsUtcNow();
        bool diagnosticsMutationStarted = false;
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

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            await database.CommitAsync(cancellationToken);
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _transactionOwnerSessionId = null;
            _transactionStartedTimestamp = null;
            snapshotDatabase = _transactionSnapshotDatabase;
            snapshotPath = _transactionSnapshotPath;
            _transactionSnapshotDatabase = null;
            _transactionSnapshotPath = null;
        }
        finally
        {
            _gate.Release();
            CompleteDiagnosticsMutation(
                diagnosticsMutationStarted,
                sessionId,
                completedAtUtc);
        }

        await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
    }

    internal async ValueTask RollbackAsync(long sessionId, CancellationToken cancellationToken = default)
    {
        DateTimeOffset? completedAtUtc = TryGetDiagnosticsUtcNow();
        bool diagnosticsMutationStarted = false;
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

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            await database.RollbackAsync(cancellationToken);
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _transactionOwnerSessionId = null;
            _transactionStartedTimestamp = null;
            snapshotDatabase = _transactionSnapshotDatabase;
            snapshotPath = _transactionSnapshotPath;
            _transactionSnapshotDatabase = null;
            _transactionSnapshotPath = null;
        }
        finally
        {
            _gate.Release();
            CompleteDiagnosticsMutation(
                diagnosticsMutationStarted,
                sessionId,
                completedAtUtc);
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
        DateTimeOffset? releasedAtUtc = TryGetDiagnosticsUtcNow();
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        Database? snapshotDatabase = null;
        string? snapshotPath = null;
        bool completeRetirement = false;

        await _gate.WaitAsync();
        try
        {
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            if (_transactionOwnerSessionId == sessionId)
            {
                try
                {
                    Database database = GetDatabase();
                    using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
                    await database.RollbackAsync();
                }
                catch
                {
                    // Best-effort rollback while tearing down a session.
                }

                _transactionOwnerSessionId = null;
                _transactionStartedTimestamp = null;
                snapshotDatabase = _transactionSnapshotDatabase;
                snapshotPath = _transactionSnapshotPath;
                _transactionSnapshotDatabase = null;
                _transactionSnapshotPath = null;
            }

            if (_activeSessionCount > 0)
                _activeSessionCount--;

            if (_database is not null)
            {
                using var temporaryScope = _database.EnterTemporaryTableSessionScope(sessionId);
                await _database.ClearTemporaryTablesAsync();
            }

            if (_disabled && _activeSessionCount == 0)
            {
                databaseToDispose = _database;
                _database = null;
                completeRetirement = true;
            }
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
            {
                try
                {
                    _runtimeDiagnostics!.RemoveSession(sessionId, releasedAtUtc);
                }
                catch
                {
                    // Diagnostics cannot replace connection close behavior.
                }
                finally
                {
                    _runtimeDiagnostics!.Consistency.EndMutation();
                }
            }
        }

        if (completeRetirement)
        {
            await DisposeRetiredDatabaseAsync(
                databaseToDispose,
                snapshotDatabase,
                snapshotPath);
        }
        else
        {
            await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
        }
    }

    internal async ValueTask DisableAsync()
    {
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        Database? snapshotDatabase = null;
        string? snapshotPath = null;
        bool completeRetirement = false;

        using IDisposable? lifecycleBoundary = EnterDatabaseCloseBoundary();
        await _gate.WaitAsync();
        try
        {
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
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
                _transactionStartedTimestamp = null;
                completeRetirement = true;
            }
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
                _runtimeDiagnostics!.Consistency.EndMutation();
        }

        if (completeRetirement)
        {
            await DisposeRetiredDatabaseAsync(
                databaseToDispose,
                snapshotDatabase,
                snapshotPath);
        }
        else
        {
            await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
        }
    }

    private IDisposable? EnterDatabaseCloseBoundary()
    {
        if (!_lifecycleLoggingEnabled)
            return null;

        return DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
            OpaqueDiagnosticsId.Create());
    }

    private async ValueTask EnsureInitializedAsync(string? normalizedLoadPath, CancellationToken cancellationToken)
    {
        if (_database is null)
        {
            _database = normalizedLoadPath is null
                ? await Database.OpenInMemoryAsync(
                    _runtimeDatabaseOptions,
                    cancellationToken)
                : await Database.LoadIntoMemoryAsync(
                    normalizedLoadPath,
                    _runtimeDatabaseOptions,
                    cancellationToken);

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

    private static string? SerializeObservabilityConfiguration(
        DatabaseOptions? databaseOptions)
    {
        CSharpDbObservabilityOptions? observability =
            databaseOptions?.ObservabilityOptions;
        return observability is null
            ? null
            : JsonSerializer.Serialize(
                observability,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
    }

    private bool OwnedByOtherSession(long sessionId)
        => _transactionOwnerSessionId.HasValue && _transactionOwnerSessionId.Value != sessionId;

    private DateTimeOffset? TryGetDiagnosticsUtcNow()
    {
        if (_runtimeDiagnostics is null)
            return null;

        return _runtimeDiagnostics.GetUtcNowOrLast();
    }

    private void BeginDiagnosticsMutation(ref bool mutationStarted)
    {
        if (!mutationStarted && _runtimeDiagnostics is not null)
        {
            _runtimeDiagnostics.Consistency.BeginMutation();
            mutationStarted = true;
        }
    }

    private void CompleteDiagnosticsMutation(
        bool mutationStarted,
        long sessionId,
        DateTimeOffset? activeAtUtc)
    {
        if (!mutationStarted)
            return;

        try
        {
            if (activeAtUtc is DateTimeOffset activeAt)
                _runtimeDiagnostics!.TouchSession(sessionId, activeAt);
        }
        catch
        {
            // Diagnostics cannot replace the transaction result.
        }
        finally
        {
            _runtimeDiagnostics!.Consistency.EndMutation();
        }
    }

    private Database GetTransactionSnapshotDatabase()
        => _transactionSnapshotDatabase ?? throw new InvalidOperationException("No committed snapshot is available for the active transaction.");

    private async ValueTask DisposeRetiredDatabaseAsync(
        Database? database,
        Database? snapshotDatabase = null,
        string? snapshotPath = null)
    {
        try
        {
            try
            {
                if (database is not null)
                    await database.DisposeAsync();
            }
            finally
            {
                // A transaction snapshot shares this host's exact runtime
                // state. Retire it before disposing the family owner even if
                // the primary database close fails.
                await DisposeSnapshotAsync(snapshotDatabase, snapshotPath);
            }

            _runtimeDiagnostics?.Consistency.BeginMutation();
            try
            {
                _retirement.TrySetResult();
            }
            finally
            {
                _runtimeDiagnostics?.Consistency.EndMutation();
            }
        }
        catch (Exception exception)
        {
            _runtimeDiagnostics?.Consistency.BeginMutation();
            try
            {
                _retirement.TrySetException(exception);
            }
            finally
            {
                _runtimeDiagnostics?.Consistency.EndMutation();
            }
            throw;
        }
        finally
        {
            _runtimeDiagnosticsStateOwner?.Dispose();
        }
    }

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
        => SqlStatementClassifier.IsReadOnly(statement);

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
    private readonly SharedMemoryDatabaseHost _ownerHost;
    private readonly long _sessionId;

    public bool SupportsStructuredExecution => true;
    public CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _ownerHost.ObservabilityOptionsSnapshot;
    public CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _ownerHost.RuntimeDiagnosticsState;
    public object RuntimeDiagnosticsIdentityKey => _ownerHost;
    public IDataRuntimeDiagnosticsContributor RuntimeDiagnosticsContributor => _ownerHost;
    public ICSharpDbObservabilityClient? RemoteObservabilityClient => null;

    internal SharedMemoryDatabaseSession(SharedMemoryDatabaseHost host, long sessionId)
    {
        _host = host;
        _ownerHost = host;
        _sessionId = sessionId;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, executionSql, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            executionSql,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, statement, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            statement,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, insert, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(_sessionId, insert, observabilitySql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetHost().ExecuteAsync(
            _sessionId,
            insert,
            observabilitySql,
            observation,
            cancellationToken);

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
