using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Data;

/// <summary>
/// A Data-layer diagnostics source. The contributor deliberately returns raw,
/// identity-free state: the eventual client capability is responsible for
/// stamping one coherent runtime metadata envelope over all projected records.
/// </summary>
internal interface IDataRuntimeDiagnosticsContributor
{
    ValueTask<DataConnectionDiagnosticsRawSnapshot?> CaptureRuntimeDiagnosticsAsync(
        int maximumSessionRecords,
        CancellationToken cancellationToken = default);
}

internal enum DataConnectionOwnerKind
{
    Unknown = 0,
    Pooled = 1,
    Direct = 2,
    SharedMemory = 3,
}

internal sealed record DataSessionDiagnosticsRawSnapshot(
    OpaqueDiagnosticsId SessionId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset LastActiveAtUtc,
    OpaqueDiagnosticsId? CurrentOperationId,
    bool HasActiveReader,
    bool HasActiveTransaction,
    CSharpDbTransport Transport,
    DiagnosticsSessionState State);

/// <summary>
/// A contributor capture. <see cref="DroppedSessionCount"/> is reserved for
/// cumulative loss from persistent bounded storage; records omitted only from
/// this capture are represented by <see cref="SessionsTruncated"/>.
/// </summary>
internal sealed record DataConnectionDiagnosticsRawSnapshot(
    OpaqueDiagnosticsId ContributorId,
    string DatabaseAlias,
    DateTimeOffset SnapshotAtUtc,
    DataConnectionOwnerKind OwnerKind,
    int? PoolCapacity,
    int? AvailableSlots,
    int? WaiterCount,
    int ActiveLogicalSessions,
    int ActiveReaders,
    int ActiveTransactions,
    TimeSpan? OldestTransactionAge,
    int WarmEngineIdleCount,
    int RetiredPoolCount,
    int PoisonedPoolCount,
    int DisabledPoolCount,
    int RetiringPoolCount,
    OpaqueDiagnosticsId? TransactionOwnerSessionId,
    ConnectionPoolLifecycleState? PoolState,
    IReadOnlyList<DataSessionDiagnosticsRawSnapshot> Sessions,
    int SessionCapacity,
    long DroppedSessionCount,
    bool SessionsTruncated);

/// <summary>
/// A process-local contributor capture. <see cref="DroppedCount"/> is
/// cumulative persistent-registry loss; current capture/response omissions are
/// represented by <see cref="IsTruncated"/>.
/// </summary>
internal sealed record DataRuntimeDiagnosticsRegistrySnapshot(
    IReadOnlyList<DataConnectionDiagnosticsRawSnapshot> Contributors,
    int Capacity,
    long DroppedCount,
    bool IsTruncated);

/// <summary>
/// Bounded process-local enumeration used by the future optional diagnostics
/// capability. It never groups by, returns, or derives labels from a data-source
/// path.
/// </summary>
internal static class DataRuntimeDiagnosticsRegistry
{
    internal static async ValueTask<DataRuntimeDiagnosticsRegistrySnapshot> CaptureAsync(
        int maximumContributorRecords,
        int maximumSessionRecordsPerContributor,
        CancellationToken cancellationToken = default)
    {
        ValidateCapacity(maximumContributorRecords, nameof(maximumContributorRecords));
        ValidateCapacity(
            maximumSessionRecordsPerContributor,
            nameof(maximumSessionRecordsPerContributor));

        DataRuntimeDiagnosticsRegistrySnapshot pools =
            await CSharpDbConnectionPoolRegistry.CaptureRuntimeDiagnosticsAsync(
                maximumContributorRecords,
                maximumSessionRecordsPerContributor,
                cancellationToken);
        DataRuntimeDiagnosticsRegistrySnapshot sharedMemory =
            await SharedMemoryDatabaseRegistry.CaptureRuntimeDiagnosticsAsync(
                maximumContributorRecords,
                maximumSessionRecordsPerContributor,
                cancellationToken);

        DataConnectionDiagnosticsRawSnapshot[] merged = pools.Contributors
            .Concat(sharedMemory.Contributors)
            .OrderBy(static snapshot => snapshot.ContributorId.Value, StringComparer.Ordinal)
            .ToArray();
        int take = Math.Min(maximumContributorRecords, merged.Length);
        var selected = new DataConnectionDiagnosticsRawSnapshot[take];
        Array.Copy(merged, selected, take);

        long dropped = SaturatingAddNonNegative(
            pools.DroppedCount,
            sharedMemory.DroppedCount);
        bool truncated = pools.IsTruncated ||
            sharedMemory.IsTruncated ||
            merged.Length > take ||
            dropped > 0;
        return new DataRuntimeDiagnosticsRegistrySnapshot(
            Array.AsReadOnly(selected),
            maximumContributorRecords,
            dropped,
            truncated);
    }

    internal static void ValidateCapacity(int capacity, string parameterName)
    {
        if (capacity is <= 0 or > CSharpDbObservabilityOptions.MaximumActiveOperationCapacity)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                capacity,
                $"Diagnostics capacity must be between 1 and {CSharpDbObservabilityOptions.MaximumActiveOperationCapacity}.");
        }
    }

    internal static long SaturatingAddNonNegative(long left, long right)
    {
        left = Math.Max(0, left);
        right = Math.Max(0, right);
        return left > long.MaxValue - right
            ? long.MaxValue
            : left + right;
    }

    internal static int SaturatingAddNonNegative(int left, int right)
    {
        left = Math.Max(0, left);
        right = Math.Max(0, right);
        return left > int.MaxValue - right
            ? int.MaxValue
            : left + right;
    }
}

/// <summary>
/// Enabled-only session state. Pool and shared-host snapshots combine copies
/// from this tracker with their own scalar state after releasing all locks.
/// </summary>
internal sealed class DataSessionRuntimeDiagnostics
{
    private readonly object _gate = new();
    private readonly Dictionary<long, SessionState> _sessions = new();
    private readonly TimeProvider _timeProvider;
    private long _nextOperationToken;
    private long _lastUtcTicks;
    private int _activeReaderCount;

    private DataSessionRuntimeDiagnostics(
        string databaseAlias,
        DataConnectionOwnerKind ownerKind,
        TimeProvider timeProvider,
        DateTimeOffset initialUtcNow)
    {
        DatabaseAlias = databaseAlias;
        OwnerKind = ownerKind;
        _timeProvider = timeProvider;
        _lastUtcTicks = initialUtcNow.UtcTicks;
        ContributorId = OpaqueDiagnosticsId.Create();
        Consistency = new DataDiagnosticsConsistencyStamp();
    }

    internal string DatabaseAlias { get; }
    internal DataConnectionOwnerKind OwnerKind { get; }
    internal OpaqueDiagnosticsId ContributorId { get; }
    internal TimeProvider TimeProvider => _timeProvider;
    internal DataDiagnosticsConsistencyStamp Consistency { get; }
    internal int ActiveReaderCount => Math.Max(
        0,
        Volatile.Read(ref _activeReaderCount));

    internal static DataSessionRuntimeDiagnostics? Create(
        CSharpDbObservabilityOptions? options,
        DataConnectionOwnerKind ownerKind,
        TimeProvider? timeProvider = null)
    {
        if (options?.Enabled != true)
            return null;

        try
        {
            TimeProvider clock = timeProvider ?? TimeProvider.System;
            DateTimeOffset initialUtcNow = NormalizeUtc(clock.GetUtcNow());
            return new DataSessionRuntimeDiagnostics(
                options.DatabaseAlias,
                ownerKind,
                clock,
                initialUtcNow);
        }
        catch
        {
            return null;
        }
    }

    internal DateTimeOffset GetUtcNow()
    {
        DateTimeOffset now = NormalizeUtc(_timeProvider.GetUtcNow());
        Interlocked.Exchange(ref _lastUtcTicks, now.UtcTicks);
        return now;
    }

    internal DateTimeOffset GetUtcNowOrLast()
    {
        try
        {
            return GetUtcNow();
        }
        catch
        {
            return new DateTimeOffset(
                Volatile.Read(ref _lastUtcTicks),
                TimeSpan.Zero);
        }
    }

    internal long? GetTimestampOrNull()
    {
        try
        {
            return _timeProvider.GetTimestamp();
        }
        catch
        {
            return null;
        }
    }

    internal TimeSpan? GetElapsedTimeOrNull(
        long? startingTimestamp,
        long? endingTimestamp)
    {
        if (startingTimestamp is not long started ||
            endingTimestamp is not long ended)
        {
            return null;
        }

        try
        {
            TimeSpan elapsed = _timeProvider.GetElapsedTime(started, ended);
            return elapsed < TimeSpan.Zero ? null : elapsed;
        }
        catch
        {
            return null;
        }
    }

    internal void RegisterSession(
        long sessionKey,
        OpaqueDiagnosticsId? preferredSessionId,
        DateTimeOffset createdAtUtc)
    {
        Consistency.BeginMutation();
        try
        {
            OpaqueDiagnosticsId sessionId = preferredSessionId ?? OpaqueDiagnosticsId.Create();
            lock (_gate)
            {
                _sessions[sessionKey] = new SessionState(
                    sessionId,
                    createdAtUtc,
                    createdAtUtc);
            }
        }
        catch
        {
            // Diagnostics registration cannot fail an otherwise valid open.
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal void RemoveSession(long sessionKey, DateTimeOffset? lastActiveAtUtc)
    {
        Consistency.BeginMutation();
        try
        {
            lock (_gate)
            {
                if (lastActiveAtUtc is DateTimeOffset activeAt &&
                    _sessions.TryGetValue(sessionKey, out SessionState? session))
                {
                    session.LastActiveAtUtc = Max(session.LastActiveAtUtc, activeAt);
                }
                if (_sessions.Remove(sessionKey, out SessionState? removed))
                {
                    _activeReaderCount = Math.Max(
                        0,
                        _activeReaderCount -
                        (removed.ReaderOperationTokens?.Count ?? 0));
                }
            }
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal void TouchSession(long sessionKey, DateTimeOffset activeAtUtc)
    {
        Consistency.BeginMutation();
        try
        {
            lock (_gate)
            {
                if (_sessions.TryGetValue(sessionKey, out SessionState? session))
                    session.LastActiveAtUtc = Max(session.LastActiveAtUtc, activeAtUtc);
            }
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal DataSessionOperationLease? TryBeginOperation(long sessionKey)
    {
        Consistency.BeginMutation();
        try
        {
            DateTimeOffset startedAtUtc = GetUtcNowOrLast();
            OpaqueDiagnosticsId? operationId = CSharpDbOperationScope.Current?.OperationId;

            lock (_gate)
            {
                if (!_sessions.TryGetValue(sessionKey, out SessionState? session))
                    return null;

                long token = unchecked(++_nextOperationToken);
                if (token == 0)
                    token = unchecked(++_nextOperationToken);
                session.ActiveOperations ??= new Dictionary<long, OpaqueDiagnosticsId?>();
                session.ActiveOperations[token] = operationId;
                if (operationId is not null)
                {
                    session.CurrentOperationToken = token;
                    session.CurrentOperationId = operationId;
                }
                session.LastActiveAtUtc = Max(session.LastActiveAtUtc, startedAtUtc);
                return new DataSessionOperationLease(this, sessionKey, token);
            }
        }
        catch
        {
            return null;
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal void MarkReader(long sessionKey, long operationToken)
    {
        Consistency.BeginMutation();
        try
        {
            lock (_gate)
            {
                if (!_sessions.TryGetValue(sessionKey, out SessionState? session) ||
                    session.ActiveOperations?.ContainsKey(operationToken) != true)
                {
                    return;
                }

                session.ReaderOperationTokens ??= new HashSet<long>();
                if (session.ReaderOperationTokens.Add(operationToken))
                {
                    _activeReaderCount =
                        DataRuntimeDiagnosticsRegistry.SaturatingAddNonNegative(
                            _activeReaderCount,
                            1);
                }
            }
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal void CompleteOperation(long sessionKey, long operationToken)
    {
        DateTimeOffset completedAtUtc = GetUtcNowOrLast();

        Consistency.BeginMutation();
        try
        {
            lock (_gate)
            {
                if (!_sessions.TryGetValue(sessionKey, out SessionState? session))
                    return;

                session.ActiveOperations?.Remove(operationToken);
                if (session.ReaderOperationTokens?.Remove(operationToken) == true)
                    _activeReaderCount = Math.Max(0, _activeReaderCount - 1);
                if (session.CurrentOperationToken == operationToken)
                    session.RefreshCurrentOperation();
                session.LastActiveAtUtc = Max(session.LastActiveAtUtc, completedAtUtc);
            }
        }
        finally
        {
            Consistency.EndMutation();
        }
    }

    internal OpaqueDiagnosticsId? GetSessionId(long sessionKey)
    {
        lock (_gate)
        {
            return _sessions.TryGetValue(sessionKey, out SessionState? session)
                ? session.SessionId
                : null;
        }
    }

    internal SessionStateBatch CopySessions(
        int maximumSessionRecords,
        long? preferredSessionKey = null)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecords,
            nameof(maximumSessionRecords));
        lock (_gate)
        {
            int take = Math.Min(maximumSessionRecords, _sessions.Count);
            var records = new SessionStateCopy[take];
            int index = 0;
            bool preferredIncluded = false;
            foreach ((long sessionKey, SessionState session) in _sessions)
            {
                if (index == take)
                    break;
                records[index++] = session.Copy(sessionKey);
                preferredIncluded |= preferredSessionKey == sessionKey;
            }

            if (!preferredIncluded &&
                preferredSessionKey is long preferredKey &&
                take > 0 &&
                _sessions.TryGetValue(preferredKey, out SessionState? preferredSession))
            {
                records[^1] = preferredSession.Copy(preferredKey);
            }

            return new SessionStateBatch(
                records,
                _sessions.Count,
                _activeReaderCount);
        }
    }

    internal static DataSessionDiagnosticsRawSnapshot[] ProjectSessions(
        SessionStateCopy[] copies,
        long? transactionOwnerSessionKey)
    {
        var snapshots = new DataSessionDiagnosticsRawSnapshot[copies.Length];

        for (int index = 0; index < copies.Length; index++)
        {
            SessionStateCopy copy = copies[index];
            bool hasTransaction = transactionOwnerSessionKey == copy.SessionKey;
            DiagnosticsSessionState state = hasTransaction
                ? DiagnosticsSessionState.Transaction
                : copy.ActiveReaderCount > 0
                    ? DiagnosticsSessionState.SnapshotReader
                    : copy.ActiveOperationCount > 0
                        ? DiagnosticsSessionState.Active
                        : DiagnosticsSessionState.Idle;
            snapshots[index] = new DataSessionDiagnosticsRawSnapshot(
                copy.SessionId,
                copy.CreatedAtUtc,
                copy.LastActiveAtUtc,
                copy.CurrentOperationId,
                copy.ActiveReaderCount > 0,
                hasTransaction,
                CSharpDbTransport.Direct,
                state);
        }

        return snapshots;
    }

    internal readonly record struct SessionStateBatch(
        SessionStateCopy[] Records,
        int TotalCount,
        int ActiveReaderCount);

    internal readonly record struct SessionStateCopy(
        long SessionKey,
        OpaqueDiagnosticsId SessionId,
        DateTimeOffset CreatedAtUtc,
        DateTimeOffset LastActiveAtUtc,
        OpaqueDiagnosticsId? CurrentOperationId,
        int ActiveOperationCount,
        int ActiveReaderCount);

    private sealed class SessionState(
        OpaqueDiagnosticsId sessionId,
        DateTimeOffset createdAtUtc,
        DateTimeOffset lastActiveAtUtc)
    {
        internal OpaqueDiagnosticsId SessionId { get; } = sessionId;
        internal DateTimeOffset CreatedAtUtc { get; } = createdAtUtc;
        internal DateTimeOffset LastActiveAtUtc { get; set; } = lastActiveAtUtc;
        internal Dictionary<long, OpaqueDiagnosticsId?>? ActiveOperations { get; set; }
        internal HashSet<long>? ReaderOperationTokens { get; set; }
        internal long CurrentOperationToken { get; set; }
        internal OpaqueDiagnosticsId? CurrentOperationId { get; set; }

        internal SessionStateCopy Copy(long sessionKey)
        {
            return new SessionStateCopy(
                sessionKey,
                SessionId,
                CreatedAtUtc,
                LastActiveAtUtc,
                CurrentOperationId,
                ActiveOperations?.Count ?? 0,
                ReaderOperationTokens?.Count ?? 0);
        }

        internal void RefreshCurrentOperation()
        {
            CurrentOperationToken = 0;
            CurrentOperationId = null;
            if (ActiveOperations is null)
                return;

            foreach ((long token, OpaqueDiagnosticsId? operationId) in ActiveOperations)
            {
                if (operationId is not null && token > CurrentOperationToken)
                {
                    CurrentOperationToken = token;
                    CurrentOperationId = operationId;
                }
            }
        }
    }

    private static DateTimeOffset Max(DateTimeOffset left, DateTimeOffset right)
        => left >= right ? left : right;

    private static DateTimeOffset NormalizeUtc(DateTimeOffset value)
        => value.Offset == TimeSpan.Zero ? value : value.ToUniversalTime();
}

internal sealed class DataSessionOperationLease
{
    private DataSessionRuntimeDiagnostics? _owner;
    private readonly long _sessionKey;
    private readonly long _operationToken;

    internal DataSessionOperationLease(
        DataSessionRuntimeDiagnostics owner,
        long sessionKey,
        long operationToken)
    {
        _owner = owner;
        _sessionKey = sessionKey;
        _operationToken = operationToken;
    }

    internal QueryResult ObserveResult(QueryResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (!result.IsQuery)
        {
            Complete();
            return result;
        }

        DataSessionRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
        if (owner is null)
            return result;

        try
        {
            owner.MarkReader(_sessionKey, _operationToken);
            result.AppendDisposeCallback(CompleteAsync);
        }
        catch
        {
            Complete();
        }

        return result;
    }

    internal void Complete()
    {
        DataSessionRuntimeDiagnostics? owner = Interlocked.Exchange(ref _owner, null);
        if (owner is null)
            return;

        try
        {
            owner.CompleteOperation(_sessionKey, _operationToken);
        }
        catch
        {
            // Diagnostics completion cannot alter command or result disposal.
        }
    }

    private ValueTask CompleteAsync()
    {
        Complete();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// A writer-count/version stamp permits a bounded coherent snapshot retry
/// without nesting a pool/host semaphore with the tracker lock.
/// </summary>
internal sealed class DataDiagnosticsConsistencyStamp
{
    private int _activeMutations;
    private long _version;

    internal void BeginMutation()
        => Interlocked.Increment(ref _activeMutations);

    internal void EndMutation()
    {
        Interlocked.Increment(ref _version);
        Interlocked.Decrement(ref _activeMutations);
    }

    internal bool TryStartRead(out long version)
    {
        version = Volatile.Read(ref _version);
        return Volatile.Read(ref _activeMutations) == 0;
    }

    internal bool IsReadValid(long version)
        => Volatile.Read(ref _activeMutations) == 0 &&
           Volatile.Read(ref _version) == version;
}
