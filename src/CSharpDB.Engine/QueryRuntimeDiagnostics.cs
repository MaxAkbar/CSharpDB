using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Engine;

/// <summary>
/// Process-local, bounded runtime state for one database's query operations.
/// One gate owns active state, terminal transfer, recent history, and summary
/// counters so a completion is published as one coherent state transition.
/// </summary>
internal sealed partial class QueryRuntimeDiagnostics : IDisposable
{
    private static readonly TimeSpan MaximumSweepPeriod = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MinimumSweepPeriod = TimeSpan.FromMilliseconds(10);
    private readonly object _gate = new();
    private readonly Dictionary<OpaqueDiagnosticsId, QueryRuntimeOperation> _active;
    private readonly Queue<RecentQueryState> _recent;
    private readonly CSharpDbRuntimeDiagnosticsState _runtimeState;
    private readonly int _activeCapacity;
    private readonly int _recentCapacity;
    private readonly TimeSpan _recentRetention;
    private readonly long _retentionTimestampUnits;
    private readonly TimeSpan _longRunningQueryThreshold;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly bool _longRunningEventsEnabled;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private ITimer? _sweepTimer;
    private int _sweepTimerInitialization;
    private int _sweepRunning;
    private int _disposed;
    private long _activeRejectedCount;
    private long _recentDroppedCount;
    private long _requestCount;
    private long _statementExecutionCount;
    private long _succeededCount;
    private long _failedCount;
    private long _canceledCount;
    private long _slowCount;
    private long _rowsProduced;
    private long _rowsAffected;

    private QueryRuntimeDiagnostics(CSharpDbRuntimeDiagnosticsState runtimeState)
    {
        ArgumentNullException.ThrowIfNull(runtimeState);
        _runtimeState = runtimeState;
        _activeCapacity = runtimeState.ActiveQueryCapacity;
        _recentCapacity = runtimeState.RecentQueryCapacity;
        _recentRetention = runtimeState.RecentQueryRetention;
        _retentionTimestampUnits = ToTimestampUnits(
            _recentRetention,
            runtimeState.TimeProvider.TimestampFrequency);
        _longRunningQueryThreshold = runtimeState.LongRunningQueryThreshold;
        CSharpDbObservabilityOptions options = runtimeState.CreateOptionsSnapshot();
        _slowQueryThreshold =
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query);
        _longRunningEventsEnabled =
            options.Logging.Enabled && options.Logging.SlowQueries;
        _sqlTextCaptureMode = options.Logging.SqlText;
        _active = new Dictionary<OpaqueDiagnosticsId, QueryRuntimeOperation>(_activeCapacity);
        _recent = new Queue<RecentQueryState>(_recentCapacity);
        InitializeLeanRuntime();
    }

    internal static QueryRuntimeDiagnostics GetOrCreate(
        CSharpDbRuntimeDiagnosticsState runtimeState,
        bool startSweepTimer = true)
    {
        ArgumentNullException.ThrowIfNull(runtimeState);
        QueryRuntimeDiagnostics diagnostics =
            runtimeState.GetOrCreateComponent(
                () => new QueryRuntimeDiagnostics(runtimeState));
        if (startSweepTimer)
            diagnostics.EnsureSweepTimerStarted();

        return diagnostics;
    }

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase = QueryExecutionPhase.Planning)
        => TryStart(context, initialPhase, SqlTextCaptureMode.None, null, out _);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        bool suppressDiagnosticEvents)
        => TryStartCore(
            context,
            initialPhase,
            publicationClaim: null,
            plan: null,
            detail: null,
            suppressDiagnosticEvents,
            CaptureLongRunningInterest(suppressDiagnosticEvents),
            previousOwner: null,
            out _);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        out bool operationAlreadyClaimed)
        => TryStart(
            context,
            initialPhase,
            SqlTextCaptureMode.None,
            capturedSqlText: null,
            out operationAlreadyClaimed);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText)
        => TryStart(
            context,
            initialPhase,
            captureMode,
            capturedSqlText,
            out _);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText,
        out bool operationAlreadyClaimed)
        => TryStart(
            context,
            initialPhase,
            captureMode,
            capturedSqlText,
            suppressDiagnosticEvents: false,
            CaptureLongRunningInterest(suppressDiagnosticEvents: false),
            out operationAlreadyClaimed);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText,
        bool suppressDiagnosticEvents,
        out bool operationAlreadyClaimed)
        => TryStart(
            context,
            initialPhase,
            captureMode,
            capturedSqlText,
            suppressDiagnosticEvents,
            CaptureLongRunningInterest(suppressDiagnosticEvents),
            out operationAlreadyClaimed);

    internal QueryRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText,
        bool suppressDiagnosticEvents,
        bool publishLongRunningQueryEvents,
        out bool operationAlreadyClaimed)
        => TryStartCore(
            context,
            initialPhase,
            publicationClaim: null,
            plan: null,
            CreateQueryDetail(captureMode, capturedSqlText),
            suppressDiagnosticEvents,
            publishLongRunningQueryEvents,
            previousOwner: null,
            out operationAlreadyClaimed);

    private QueryRuntimeOperation? TryStartCore(
        CSharpDbOperationContext context,
        QueryExecutionPhase initialPhase,
        LongRunningPublicationClaim? publicationClaim,
        QueryPlanState? plan,
        QueryDetailState? detail,
        bool suppressDiagnosticEvents,
        bool publishLongRunningQueryEvents,
        QueryRuntimeDiagnostics? previousOwner,
        out bool operationAlreadyClaimed)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (initialPhase is QueryExecutionPhase.Unknown or QueryExecutionPhase.Completed ||
            !Enum.IsDefined(initialPhase))
        {
            throw new ArgumentOutOfRangeException(nameof(initialPhase));
        }

        operationAlreadyClaimed = false;
        if (!_runtimeState.IsEnabled || Volatile.Read(ref _disposed) != 0)
            return null;

        bool ownershipClaimed = false;
        try
        {
            bool claimed = previousOwner is null
                ? context.TryClaimRuntimeDiagnostics(this)
                : context.TryTransferRuntimeDiagnostics(previousOwner, this);
            if (!claimed)
            {
                operationAlreadyClaimed = true;
                return null;
            }

            ownershipClaimed = true;

            QueryRuntimeOperation operation;
            lock (_gate)
            {
                if (_active.ContainsKey(context.OperationId))
                {
                    // One opaque operation id has exactly one terminal owner.
                    // A duplicate start is not a capacity rejection and must
                    // not produce another recent record or counter update.
                    operationAlreadyClaimed = true;
                    return null;
                }
                else if (_active.Count + _leanActiveCount >= _activeCapacity)
                {
                    _activeRejectedCount = SaturatingIncrement(
                        _activeRejectedCount);
                    operation = new QueryRuntimeOperation(
                        this,
                        context,
                        initialPhase,
                        publicationClaim ?? (_longRunningEventsEnabled &&
                                              publishLongRunningQueryEvents
                            ? new LongRunningPublicationClaim(suppressDiagnosticEvents)
                            : null),
                        plan.GetValueOrDefault(),
                        suppressDiagnosticEvents,
                        AcceptQueryDetail(detail),
                        registered: false);
                }
                else
                {
                    operation = new QueryRuntimeOperation(
                        this,
                        context,
                        initialPhase,
                        publicationClaim ?? (_longRunningEventsEnabled &&
                                              publishLongRunningQueryEvents
                            ? new LongRunningPublicationClaim(suppressDiagnosticEvents)
                            : null),
                        plan.GetValueOrDefault(),
                        suppressDiagnosticEvents,
                        AcceptQueryDetail(detail),
                        registered: true);
                    _active.Add(context.OperationId, operation);
                }
            }

            return operation;
        }
        catch
        {
            // Runtime diagnostics are best-effort and must never make query
            // execution fail. Listener-based logging can still proceed.
            operationAlreadyClaimed = ownershipClaimed;
            return null;
        }
    }

    internal BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> GetActiveSnapshot(
        int maximumRecords)
    {
        ActiveCollectionCapture capture = CaptureActive(maximumRecords);
        return new BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>(
            capture.Records,
            capture.DroppedCount,
            capture.IsTruncated);
    }

    internal DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        GetActiveCollectionSnapshot(int maximumRecords)
    {
        ActiveCollectionCapture capture = CaptureActive(maximumRecords);
        return new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            capture.Metadata,
            capture.Records,
            _activeCapacity,
            retention: null,
            capture.DroppedCount,
            capture.IsTruncated);
    }

    internal BoundedDiagnosticsSnapshot<RecentQuerySnapshot> GetRecentSnapshot(
        int maximumRecords)
    {
        RecentCollectionCapture capture = CaptureRecent(maximumRecords);
        return new BoundedDiagnosticsSnapshot<RecentQuerySnapshot>(
            capture.Records,
            capture.DroppedCount,
            capture.IsTruncated);
    }

    internal DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        GetRecentCollectionSnapshot(int maximumRecords)
    {
        RecentCollectionCapture capture = CaptureRecent(maximumRecords);
        return new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
            capture.Metadata,
            capture.Records,
            _recentCapacity,
            _recentRetention,
            capture.DroppedCount,
            capture.IsTruncated);
    }

    internal QueryDiagnosticsSummary GetSummary()
    {
        QuerySummaryCopy copied;
        lock (_gate)
        {
            copied = new QuerySummaryCopy(
                _requestCount,
                _statementExecutionCount,
                _succeededCount,
                _failedCount,
                _canceledCount,
                _slowCount,
                _rowsProduced,
                _rowsAffected,
                _active.Count + _leanActiveCount);
        }

        return copied.CreateSnapshot(CreateMetadata(recordsTruncated: false));
    }

    internal QueryPlanDiagnosticsSnapshot? GetPlanSnapshot(
        OpaqueDiagnosticsId operationId)
    {
        QueryPlanCopy? copied = null;
        long copiedSequence = long.MinValue;
        long now = GetTimestampSafely();
        lock (_gate)
        {
            if (_active.TryGetValue(operationId, out QueryRuntimeOperation? active) &&
                !active.Completed)
            {
                copied = active.CapturePlan(actualRows: null);
            }
            else if (TryCaptureLeanActivePlanLocked(operationId, out QueryPlanCopy leanActive))
            {
                copied = leanActive;
            }
            else
            {
                PruneExpired(now);
                foreach (RecentQueryState recent in _recent)
                {
                    // Retain the newest matching record if a deliberately
                    // supplied operation id was reused after completion.
                    if (recent.Context.OperationId == operationId)
                    {
                        copied = recent.Plan;
                        copiedSequence = recent.Sequence;
                    }
                }

                if (TryCaptureLeanRecentPlanLocked(
                        operationId,
                        copiedSequence,
                        out QueryPlanCopy leanRecent,
                        out long leanSequence))
                {
                    copied = leanRecent;
                    copiedSequence = leanSequence;
                }
            }
        }

        return copied?.CreateSnapshot(CreateMetadata(recordsTruncated: false));
    }

    internal QueryDetailSnapshot? GetQueryDetailSnapshot(
        OpaqueDiagnosticsId operationId)
    {
        ArgumentNullException.ThrowIfNull(operationId);

        QueryDetailCopy? copied = null;
        long now = GetTimestampSafely();
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return null;

            if (_active.TryGetValue(operationId, out QueryRuntimeOperation? active) &&
                !active.Completed)
            {
                // The live operation is authoritative. A no-source or
                // capture-disabled active operation must shadow any older
                // deliberately reused id instead of exposing stale SQL.
                if (active.CaptureDetail() is QueryDetailState activeDetail)
                    copied = new QueryDetailCopy(active.Context, activeDetail);
            }
            else if (HasLeanActiveLocked(operationId))
            {
                // The strict lean path is available only when SqlText=None,
                // so an active lean record authoritatively has no detail.
                copied = null;
            }
            else
            {
                PruneExpired(now);
                long copiedSequence = long.MinValue;
                foreach (RecentQueryState recent in _recent)
                {
                    // Retain the newest matching record if a deliberately
                    // supplied opaque operation id was reused. A newer
                    // no-detail record shadows an older captured record.
                    if (recent.Context.OperationId == operationId)
                    {
                        copied = recent.Detail is QueryDetailState recentDetail
                            ? new QueryDetailCopy(recent.Context, recentDetail)
                            : null;
                        copiedSequence = recent.Sequence;
                    }
                }

                if (HasNewerLeanRecentLocked(operationId, copiedSequence))
                    copied = null;
            }
        }

        if (copied is not QueryDetailCopy detail)
            return null;

        DiagnosticsSnapshotMetadata metadata = _runtimeState.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            fieldsTruncated: detail.Detail.FieldsTruncated);
        return detail.CreateSnapshot(metadata);
    }

    internal IDisposable? EnterCurrentWaiting()
    {
        CSharpDbQueryRuntimeBinding binding =
            CSharpDbOperationScope.CaptureQueryRuntimeBinding();
        CSharpDbOperationContext? context = binding.Operation;
        return context is not null &&
               context.OperationClass == CSharpDbOperationClass.Query &&
               binding.RuntimeOperation is
                   QueryRuntimeOperation operation &&
               operation.Matches(this, context)
            ? operation.EnterWaiting()
            : null;
    }

    private ActiveCollectionCapture CaptureActive(int maximumRecords)
    {
        if (maximumRecords <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));

        ActiveQueryCopy[] copied;
        long rejectedCount;
        lock (_gate)
        {
            copied = new ActiveQueryCopy[_active.Count + _leanActiveCount];
            int index = 0;
            foreach (QueryRuntimeOperation state in _active.Values)
            {
                // Completed is assigned only after removal while holding this
                // same gate, so it can never escape through an active copy.
                copied[index++] = new ActiveQueryCopy(
                    state.Context.OperationId,
                    state.Context.ParentOperationId,
                    state.Context.OperationClass,
                    state.Context.Role,
                    state.Context.StartedAtUtc,
                    state.Context.StartingTimestamp,
                    state.Context.QueryFingerprint,
                    state.Context.Transport,
                    state.Context.TraceId,
                    state.Context.SessionId,
                    state.CapturePhase());
            }

            foreach (LeanActiveSlot lean in _leanActiveSlots)
            {
                if (lean.State != LeanSlotActive)
                    continue;

                LeanActiveStateSnapshot active =
                    lean.CaptureStateUnderRegistryGate();
                copied[index++] = new ActiveQueryCopy(
                    OpaqueDiagnosticsId.Create(lean.OperationId),
                    ParentOperationId: null,
                    CSharpDbOperationClass.Query,
                    CSharpDbOperationRole.Root,
                    lean.StartedAtUtc,
                    lean.StartingTimestamp,
                    lean.Fingerprint,
                    lean.Transport,
                    TraceId: null,
                    SessionId: null,
                    active.Phase);
            }

            rejectedCount = _activeRejectedCount;
        }

        ActiveQueryCopy[] selected = copied
            .OrderBy(static state => state.StartedAtUtc)
            .ThenBy(static state => state.OperationId.Value, StringComparer.Ordinal)
            .Take(maximumRecords)
            .ToArray();
        bool truncated = rejectedCount > 0 || selected.Length < copied.Length;
        SnapshotCaptureStamp capture = CaptureStamp();
        DiagnosticsSnapshotMetadata metadata = CreateMetadataAt(
            capture.CapturedAtUtc,
            truncated);
        ActiveQuerySnapshot[] records = selected
            .Select(state => state.CreateSnapshot(
                metadata,
                capture.Timestamp,
                _runtimeState.TimeProvider))
            .ToArray();
        return new ActiveCollectionCapture(
            metadata,
            records,
            rejectedCount,
            truncated);
    }

    private RecentCollectionCapture CaptureRecent(int maximumRecords)
    {
        if (maximumRecords <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));

        long now = GetTimestampSafely();
        RecentQueryCapture[] copied;
        long droppedCount;
        lock (_gate)
        {
            PruneExpired(now);
            copied = new RecentQueryCapture[_recent.Count + _leanRecentCount];
            int index = 0;
            foreach (RecentQueryState recent in _recent)
                copied[index++] = RecentQueryCapture.From(recent);
            for (int offset = 0; offset < _leanRecentCount; offset++)
            {
                int slotIndex = (_leanRecentHead + offset) % _leanRecentSlots.Length;
                copied[index++] = RecentQueryCapture.From(
                    _leanRecentSlots[slotIndex]);
            }
            droppedCount = _recentDroppedCount;
        }

        RecentQueryCapture[] selected = copied
            .OrderByDescending(static state => state.Sequence)
            .Take(maximumRecords)
            .ToArray();
        bool truncated = droppedCount > 0 || selected.Length < copied.Length;
        SnapshotCaptureStamp capture = CaptureStamp();
        DiagnosticsSnapshotMetadata metadata = CreateMetadataAt(
            capture.CapturedAtUtc,
            truncated);
        RecentQuerySnapshot[] records = selected
            .Select(state => state.CreateSnapshot(metadata))
            .ToArray();
        return new RecentCollectionCapture(
            metadata,
            records,
            droppedCount,
            truncated);
    }

    /// <summary>
    /// Runs one non-overlapping registry sweep. The manual seam uses the same
    /// path as the production timer so threshold and completion races can be
    /// tested without wall-clock delays.
    /// </summary>
    internal int SweepLongRunningQueries()
        => SweepLongRunningQueries(beforePublish: null);

    internal int SweepLongRunningQueries(Action? beforePublish)
    {
        if (!_runtimeState.IsEnabled || Volatile.Read(ref _disposed) != 0 ||
            Interlocked.CompareExchange(ref _sweepRunning, 1, 0) != 0)
        {
            return 0;
        }

        try
        {
            return SweepLongRunningQueriesCore(beforePublish);
        }
        catch
        {
            // Sweeps are best-effort diagnostics and must never surface timer,
            // clock, payload, listener, or concurrent-disposal failures.
            return 0;
        }
        finally
        {
            Volatile.Write(ref _sweepRunning, 0);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        DisposeTimer(Interlocked.Exchange(ref _sweepTimer, null));

        lock (_gate)
        {
            foreach (QueryRuntimeOperation state in _active.Values)
            {
                // Teardown abandons unfinished diagnostic leases. It does not
                // fabricate a canceled/failed recent record for application
                // results that were never drained or disposed.
                state.RetireFromOwner();
                state.Completed = true;
                state.Phase = QueryExecutionPhase.Completed;
                state.WaitingLease = null;
                state.Detail = null;
                state.Context.ReleaseRuntimeDiagnostics(this);
            }

            _active.Clear();
            // Query detail can contain explicitly captured SQL. Release every
            // retained reference at exact runtime-family teardown even when a
            // test or diagnostic caller still holds this registry object.
            _recent.Clear();
            DisposeLeanRuntimeLocked();
        }
    }

    private int SweepLongRunningQueriesCore(Action? beforePublish)
    {
        QueryRuntimeOperation[] candidates;
        lock (_gate)
        {
            if (_active.Count == 0)
                return 0;

            candidates = _active.Values
                .Where(static state => !state.Completed && !state.LongRunningMarked)
                .ToArray();
        }

        if (candidates.Length == 0)
            return 0;

        long observedAtTimestamp = _runtimeState.TimeProvider.GetTimestamp();
        DateTimeOffset observedAtUtc = _runtimeState.TimeProvider.GetUtcNow();
        var qualifying = new List<LongRunningCandidate>(candidates.Length);
        foreach (QueryRuntimeOperation candidate in candidates)
        {
            TimeSpan elapsed;
            try
            {
                elapsed = _runtimeState.TimeProvider.GetElapsedTime(
                    candidate.Context.StartingTimestamp,
                    observedAtTimestamp);
            }
            catch
            {
                continue;
            }

            if (elapsed >= _longRunningQueryThreshold)
            {
                qualifying.Add(new LongRunningCandidate(
                    candidate,
                    NonNegative(elapsed)));
            }
        }

        if (qualifying.Count == 0)
            return 0;

        List<LongRunningQueryCopy>? publishable = null;
        int markedCount = 0;
        lock (_gate)
        {
            foreach (LongRunningCandidate candidate in qualifying)
            {
                QueryRuntimeOperation state = candidate.State;
                if (state.Completed || state.LongRunningMarked ||
                    !_active.TryGetValue(state.Context.OperationId, out QueryRuntimeOperation? current) ||
                    !ReferenceEquals(current, state))
                {
                    continue;
                }

                state.LongRunningMarked = true;
                markedCount++;
                if (state.PublicationClaim is LongRunningPublicationClaim publicationClaim)
                {
                    publishable ??= new List<LongRunningQueryCopy>(qualifying.Count);
                    publishable.Add(new LongRunningQueryCopy(
                        state,
                        publicationClaim,
                        state.Context,
                        observedAtUtc,
                        candidate.Elapsed,
                        _longRunningQueryThreshold,
                        state.CapturePhase()));
                }
            }
        }

        // Listener callbacks can reenter query completion, snapshots, or
        // disposal, so payload construction and publication stay outside the
        // registry gate.
        beforePublish?.Invoke();
        if (publishable is not null && Volatile.Read(ref _disposed) == 0)
        {
            CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
            foreach (LongRunningQueryCopy item in publishable)
            {
                if (Volatile.Read(ref _disposed) != 0)
                    break;
                if (!CanPublishLongRunning(item.State))
                    continue;

                if (!item.PublicationClaim.TryClaim())
                    continue;

                // Recheck after claiming. If teardown or a queued-family
                // rebind retired this source state in the tiny claim window,
                // release the logical claim so its successor can publish.
                if (!CanPublishLongRunning(item.State))
                {
                    item.PublicationClaim.Release();
                    continue;
                }

                try
                {
                    publisher.Publish(
                        CSharpDbLogEvents.LongRunningQuery,
                        item,
                        static candidate => candidate.CreateEvent());
                }
                catch
                {
                    item.PublicationClaim.Release();
                    throw;
                }
            }
        }

        return markedCount;
    }

    private bool CanPublishLongRunning(QueryRuntimeOperation state)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return false;

        lock (_gate)
        {
            return Volatile.Read(ref _disposed) == 0 &&
                   !state.SuppressDiagnosticEvents &&
                   !state.Completed &&
                   _active.TryGetValue(
                       state.Context.OperationId,
                        out QueryRuntimeOperation? current) &&
                   ReferenceEquals(current, state);
        }
    }

    private bool CaptureLongRunningInterest(bool suppressDiagnosticEvents)
        => !suppressDiagnosticEvents &&
           _longRunningEventsEnabled &&
           CSharpDbDiagnostics.EventPublisher.IsEnabled(
               CSharpDbLogEvents.LongRunningQuery);

    private void TrySetPhase(QueryRuntimeOperation state, QueryExecutionPhase phase)
    {
        if (state.Completed)
            return;

        if (state.Phase == QueryExecutionPhase.Waiting)
        {
            // A normal forward lifecycle transition invalidates a stale
            // waiting lease. Returning to the prior phase is owned only
            // by that exact lease in ExitWaiting.
            if (phase <= QueryExecutionPhase.Waiting)
                return;

            state.WaitingLease = null;
        }
        else if (phase <= state.Phase)
        {
            return;
        }

        state.Phase = phase;
    }

    private IDisposable? EnterWaiting(QueryRuntimeOperation state)
    {
        if (state.Completed ||
            state.Phase is QueryExecutionPhase.Waiting or
                QueryExecutionPhase.Disposing or
                QueryExecutionPhase.Completed)
        {
            return null;
        }

        var lease = new WaitingPhaseLease(this, state, state.Phase);
        state.WaitingLease = lease;
        state.Phase = QueryExecutionPhase.Waiting;
        return lease;
    }

    private void ExitWaiting(
        QueryRuntimeOperation state,
        WaitingPhaseLease lease,
        QueryExecutionPhase priorPhase)
    {
        if (state.Completed ||
            state.Phase != QueryExecutionPhase.Waiting ||
            !ReferenceEquals(state.WaitingLease, lease))
        {
            return;
        }

        state.WaitingLease = null;
        state.Phase = priorPhase;
    }

    private void RecordPlanCacheLookup(QueryRuntimeOperation state, bool hit)
    {
        if (!state.Completed)
            state.Plan.RecordPlanCacheLookup(hit);
    }

    private void RecordAccessPath(
        QueryRuntimeOperation state,
        QueryPlanAccessPathCategory accessPath,
        long? estimatedRows)
    {
        if (!state.Completed)
            state.Plan.RecordAccessPath(accessPath, estimatedRows);
    }

    private void RecordPlanChange(
        QueryRuntimeOperation state,
        QueryPlanChangeKind change)
    {
        if (!state.Completed)
            state.Plan.RecordPlanChange(change);
    }

    private void TryRetainQueryDetail(
        QueryRuntimeOperation state,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText)
    {
        QueryDetailState? detail = CreateQueryDetail(
            captureMode,
            capturedSqlText);
        if (detail is null)
            return;

        if (!state.Completed && Volatile.Read(ref _disposed) == 0)
        {
            state.Detail ??= detail;
        }
    }

    private void Abandon(QueryRuntimeOperation state, bool registered)
    {
        lock (_gate)
        {
            if (state.Completed)
                return;

            if (registered &&
                _active.TryGetValue(state.Context.OperationId, out QueryRuntimeOperation? current) &&
                ReferenceEquals(current, state))
            {
                _active.Remove(state.Context.OperationId);
            }

            // Abandonment transfers no terminal ownership. It only retires a
            // queued lease whose exact runtime family can no longer execute
            // it, so history, counters, and drop accounting remain unchanged.
            state.Completed = true;
            state.Phase = QueryExecutionPhase.Completed;
            state.WaitingLease = null;
            state.Detail = null;
        }
    }

    private bool DetachForRebind(
        QueryRuntimeOperation state,
        bool registered,
        out LongRunningPublicationClaim? publicationClaim,
        out QueryPlanState plan,
        out QueryDetailState? detail)
    {
        lock (_gate)
        {
            if (state.Completed || Volatile.Read(ref _disposed) != 0)
            {
                publicationClaim = null;
                plan = default;
                detail = null;
                return false;
            }

            if (registered &&
                _active.TryGetValue(state.Context.OperationId, out QueryRuntimeOperation? current) &&
                ReferenceEquals(current, state))
            {
                _active.Remove(state.Context.OperationId);
            }

            publicationClaim = state.PublicationClaim;
            plan = state.Plan.Clone();
            detail = state.Detail;
            state.Detail = null;
            state.Completed = true;
            state.Phase = QueryExecutionPhase.Completed;
            state.WaitingLease = null;
            return true;
        }
    }

    private void Complete(
        QueryRuntimeOperation state,
        bool registered,
        CSharpDbOperationOutcome outcome,
        DateTimeOffset completedAtUtc,
        TimeSpan duration,
        TimeSpan? timeToFirstResult,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection? error,
        bool isSlow,
        long? recordedAtTimestamp)
    {
        long safeRecordedAtTimestamp =
            recordedAtTimestamp ?? GetTimestampSafely();
        TimeSpan safeDuration = NonNegative(duration);
        TimeSpan? safeTimeToFirstResult = timeToFirstResult is null
            ? null
            : NonNegative(timeToFirstResult.Value);
        if (safeTimeToFirstResult > safeDuration)
            safeTimeToFirstResult = safeDuration;
        var recentState = new RecentQueryState(
            Sequence: 0,
            state.Context,
            completedAtUtc,
            safeDuration,
            safeTimeToFirstResult,
            safeTimeToFirstResult is null
                ? null
                : NonNegative(safeDuration - safeTimeToFirstResult.Value),
            outcome,
            Math.Max(0, rowsProduced),
            Math.Max(0, rowsAffected),
            error,
            safeRecordedAtTimestamp,
            Plan: default,
            Detail: default);

        lock (_gate)
        {
            if (state.Completed || Volatile.Read(ref _disposed) != 0)
            {
                state.Completed = true;
                state.Phase = QueryExecutionPhase.Completed;
                state.WaitingLease = null;
                state.Detail = null;
                return;
            }

            if (registered &&
                _active.TryGetValue(state.Context.OperationId, out QueryRuntimeOperation? current) &&
                ReferenceEquals(current, state))
            {
                _active.Remove(state.Context.OperationId);
            }

            // Mark terminal only after removal and under the same gate. Phase
            // updates after this point are ignored and cannot resurrect it.
            state.Completed = true;
            state.Phase = QueryExecutionPhase.Completed;
            state.WaitingLease = null;

            // Terminal actual rows are captured in the same atomic transfer
            // as the rest of the active operation. A SELECT contributes rows
            // produced; a mutation contributes rows affected.
            recentState = recentState with
            {
                Sequence = NextRecentSequenceLocked(),
                Plan = state.Plan.CreateCopy(
                    state.Context,
                    Math.Max(0, rowsProduced > 0 ? rowsProduced : rowsAffected)),
                Detail = state.Detail,
            };
            state.Detail = null;

            PruneExpired(safeRecordedAtTimestamp);
            EnsureRecentCapacityLocked();
            _recent.Enqueue(recentState);
            if (state.Context.CountsAsRequest)
                _requestCount = SaturatingIncrement(_requestCount);
            if (state.Context.CountsAsStatement)
            {
                _statementExecutionCount = SaturatingIncrement(
                    _statementExecutionCount);
            }

            switch (outcome)
            {
                case CSharpDbOperationOutcome.Succeeded:
                    _succeededCount = SaturatingIncrement(_succeededCount);
                    break;
                case CSharpDbOperationOutcome.Canceled:
                    _canceledCount = SaturatingIncrement(_canceledCount);
                    break;
                default:
                    _failedCount = SaturatingIncrement(_failedCount);
                    break;
            }

            if (isSlow)
                _slowCount = SaturatingIncrement(_slowCount);
            _rowsProduced = SaturatingAdd(
                _rowsProduced,
                recentState.RowsProduced);
            _rowsAffected = SaturatingAdd(
                _rowsAffected,
                recentState.RowsAffected);
        }
    }

    private void PruneExpired(long now)
    {
        while (_recent.TryPeek(out RecentQueryState entry) &&
               IsExpired(entry.RecordedAtTimestamp, now))
        {
            _recent.Dequeue();
            _recentDroppedCount = SaturatingIncrement(
                _recentDroppedCount);
        }

        PruneExpiredLean(now);
    }

    private bool IsExpired(long recordedAt, long now)
        => now >= recordedAt && now - recordedAt > _retentionTimestampUnits;

    private long GetTimestampSafely()
    {
        try
        {
            return _runtimeState.TimeProvider.GetTimestamp();
        }
        catch
        {
            return 0;
        }
    }

    private DiagnosticsSnapshotMetadata CreateMetadata(bool recordsTruncated)
        => _runtimeState.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            databaseAlias: _runtimeState.DatabaseAlias,
            recordsTruncated: recordsTruncated,
            fieldsTruncated: false);

    private DiagnosticsSnapshotMetadata CreateMetadataAt(
        DateTimeOffset capturedAtUtc,
        bool recordsTruncated)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            capturedAtUtc,
            _runtimeState.ServerInstanceId,
            _runtimeState.CounterEpoch,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            _runtimeState.DatabaseAlias,
            recordsTruncated,
            fieldsTruncated: false);

    private SnapshotCaptureStamp CaptureStamp()
        => new(
            _runtimeState.TimeProvider.GetTimestamp(),
            _runtimeState.TimeProvider.GetUtcNow());

    private static TimeSpan NonNegative(TimeSpan value)
        => value < TimeSpan.Zero ? TimeSpan.Zero : value;

    private bool IsSlowQuery(TimeSpan duration)
        => duration >= _slowQueryThreshold;

    private static long SaturatingIncrement(long value)
        => value == long.MaxValue ? long.MaxValue : value + 1;

    private static long SaturatingAdd(long value, long increment)
    {
        if (increment <= 0)
            return value;

        return value >= long.MaxValue - increment
            ? long.MaxValue
            : value + increment;
    }

    private QueryDetailState? CreateQueryDetail(
        SqlTextCaptureMode captureMode,
        string? capturedSqlText)
    {
        if (_sqlTextCaptureMode == SqlTextCaptureMode.None ||
            captureMode != _sqlTextCaptureMode ||
            captureMode is not (SqlTextCaptureMode.Normalized or SqlTextCaptureMode.Raw) ||
            string.IsNullOrWhiteSpace(capturedSqlText))
        {
            return null;
        }

        bool fieldsTruncated =
            capturedSqlText.Length > QueryDetailSnapshot.MaximumCapturedSqlTextLength;
        string retainedText = fieldsTruncated
            ? TruncateCapturedSqlText(capturedSqlText)
            : capturedSqlText;
        return new QueryDetailState(captureMode, retainedText, fieldsTruncated);
    }

    private static string TruncateCapturedSqlText(string capturedSqlText)
    {
        int length = QueryDetailSnapshot.MaximumCapturedSqlTextLength;
        // Never split a UTF-16 surrogate pair. Source-generated JSON uses the
        // retained string directly and must not be handed malformed text at
        // the public query-detail boundary.
        if (char.IsHighSurrogate(capturedSqlText[length - 1]))
            length--;

        return capturedSqlText[..length];
    }

    private QueryDetailState? AcceptQueryDetail(QueryDetailState? detail)
        => detail is QueryDetailState value &&
           _sqlTextCaptureMode == value.CaptureMode
            ? value
            : null;

    internal void SetCumulativeCountersForTesting(
        long requestCount,
        long statementExecutionCount,
        long succeededCount,
        long failedCount,
        long canceledCount,
        long slowCount,
        long rowsProduced,
        long rowsAffected,
        long activeRejectedCount,
        long recentDroppedCount)
    {
        if (requestCount < 0 || statementExecutionCount < 0 ||
            succeededCount < 0 || failedCount < 0 || canceledCount < 0 ||
            slowCount < 0 || rowsProduced < 0 || rowsAffected < 0 ||
            activeRejectedCount < 0 || recentDroppedCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(requestCount),
                "Cumulative diagnostics counters cannot be negative.");
        }

        lock (_gate)
        {
            _requestCount = requestCount;
            _statementExecutionCount = statementExecutionCount;
            _succeededCount = succeededCount;
            _failedCount = failedCount;
            _canceledCount = canceledCount;
            _slowCount = slowCount;
            _rowsProduced = rowsProduced;
            _rowsAffected = rowsAffected;
            _activeRejectedCount = activeRejectedCount;
            _recentDroppedCount = recentDroppedCount;
        }
    }

    private static long ToTimestampUnits(TimeSpan duration, long timestampFrequency)
    {
        if (timestampFrequency <= 0)
            throw new ArgumentOutOfRangeException(nameof(timestampFrequency));

        double units = Math.Ceiling(duration.TotalSeconds * timestampFrequency);
        return units >= long.MaxValue ? long.MaxValue : Math.Max(1, (long)units);
    }

    private static TimeSpan GetSweepPeriod(TimeSpan threshold)
    {
        TimeSpan period = threshold < MaximumSweepPeriod
            ? threshold
            : MaximumSweepPeriod;
        return period < MinimumSweepPeriod
            ? MinimumSweepPeriod
            : period;
    }

    private void EnsureSweepTimerStarted()
    {
        if (!_runtimeState.IsEnabled || !_longRunningEventsEnabled ||
            Volatile.Read(ref _disposed) != 0 ||
            Interlocked.CompareExchange(ref _sweepTimerInitialization, 1, 0) != 0)
        {
            return;
        }

        ITimer? timer = null;
        try
        {
            TimeSpan period = GetSweepPeriod(_longRunningQueryThreshold);

            // A database may be opened inside a deferred transport boundary.
            // Do not let its AsyncLocal event buffer escape into a process-wide
            // timer callback after that boundary has been flushed and retired.
            if (ExecutionContext.IsFlowSuppressed())
            {
                timer = CreateSweepTimer(_runtimeState.TimeProvider, period);
            }
            else
            {
                using (ExecutionContext.SuppressFlow())
                    timer = CreateSweepTimer(_runtimeState.TimeProvider, period);
            }

            if (Volatile.Read(ref _disposed) != 0)
            {
                DisposeTimer(timer);
                return;
            }

            Interlocked.Exchange(ref _sweepTimer, timer);
            timer = null;
            if (Volatile.Read(ref _disposed) != 0)
                DisposeTimer(Interlocked.Exchange(ref _sweepTimer, null));
        }
        catch
        {
            // A custom clock or timer implementation cannot make the
            // database unavailable. Deterministic/manual sweeps remain
            // usable even when automatic scheduling is unavailable.
            DisposeTimer(timer);
            Volatile.Write(ref _sweepTimerInitialization, 0);
        }
    }

    private ITimer CreateSweepTimer(TimeProvider timeProvider, TimeSpan period)
        => timeProvider.CreateTimer(
            static state => ((QueryRuntimeDiagnostics)state!).SweepLongRunningQueries(),
            this,
            period,
            period);

    private static void DisposeTimer(ITimer? timer)
    {
        if (timer is null)
            return;

        try
        {
            timer.Dispose();
        }
        catch
        {
            // Timer teardown is best-effort diagnostics cleanup.
        }
    }

    internal sealed class QueryRuntimeOperation :
        IQueryExecutionObservation,
        IQueryPlanRuntimeObserver
    {
        private const int MutationAvailable = 0;
        private const int MutationHeld = 1;
        private const int MutationsSealed = 2;
        private readonly QueryRuntimeDiagnostics _owner;
        private readonly bool _registered;
        private int _adoptionClaimed;
        private int _leaseCompleted;
        private int _mutationState;
        private long? _firstRowTimestamp;

        internal QueryRuntimeOperation(
            QueryRuntimeDiagnostics owner,
            CSharpDbOperationContext context,
            QueryExecutionPhase initialPhase,
            LongRunningPublicationClaim? publicationClaim,
            QueryPlanState plan,
            bool suppressDiagnosticEvents,
            QueryDetailState? detail,
            bool registered)
        {
            _owner = owner;
            _registered = registered;
            Context = context;
            Phase = initialPhase;
            PublicationClaim = publicationClaim;
            Plan = plan;
            SuppressDiagnosticEvents = suppressDiagnosticEvents;
            Detail = detail;
        }

        internal CSharpDbOperationContext Context { get; }
        internal QueryExecutionPhase Phase { get; set; }
        internal LongRunningPublicationClaim? PublicationClaim { get; }
        internal QueryPlanState Plan;
        internal bool SuppressDiagnosticEvents { get; }
        internal QueryDetailState? Detail { get; set; }
        internal WaitingPhaseLease? WaitingLease { get; set; }
        internal bool Completed { get; set; }
        internal bool LongRunningMarked { get; set; }

        public IQueryPlanRuntimeObserver ExplicitPlanObserver => this;

        public IDisposable EnterScope()
            => CSharpDbOperationScope.Enter(Context, this);

        public void MarkExecuting()
            => SetPhase(QueryExecutionPhase.Executing);

        public QueryResult Observe(QueryResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            if (result.IsQuery)
            {
                SetPhase(QueryExecutionPhase.Streaming);
                result.SetObserver(this);
                if (result.RequiresRuntimeExecutionScope)
                    result.PrependExecutionScopeFactory(EnterScope);
            }
            else
            {
                CompleteExecution(
                    CSharpDbOperationOutcome.Succeeded,
                    rowsProduced: 0,
                    result.RowsAffected,
                    error: null);
            }

            return result;
        }

        public void Fail(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            CompleteExecution(
                exception is OperationCanceledException
                    ? CSharpDbOperationOutcome.Canceled
                    : CSharpDbOperationOutcome.Failed,
                rowsProduced: 0,
                rowsAffected: 0,
                QueryOperation.ProjectError(exception));
        }

        public void OnFirstRowProduced()
        {
            try
            {
                _firstRowTimestamp ??= Context.GetTimestamp();
            }
            catch
            {
                // A diagnostics clock cannot affect result consumption.
            }
        }

        public void OnDisposing()
            => SetPhase(QueryExecutionPhase.Disposing);

        public void OnCompleted(QueryResultCompletion completion)
        {
            switch (completion.Reason)
            {
                case QueryResultCompletionReason.Exhausted:
                case QueryResultCompletionReason.Disposed:
                    CompleteExecution(
                        CSharpDbOperationOutcome.Succeeded,
                        completion.RowsProduced,
                        rowsAffected: 0,
                        error: null);
                    break;
                case QueryResultCompletionReason.Canceled:
                    CompleteExecution(
                        CSharpDbOperationOutcome.Canceled,
                        completion.RowsProduced,
                        rowsAffected: 0,
                        QueryOperation.ProjectError(
                            completion.Error ?? new OperationCanceledException()));
                    break;
                default:
                    CompleteExecution(
                        CSharpDbOperationOutcome.Failed,
                        completion.RowsProduced,
                        rowsAffected: 0,
                        QueryOperation.ProjectError(
                            completion.Error ?? new InvalidOperationException()));
                    break;
            }
        }

        public void OnPlanCacheLookup(bool hit)
            => RecordPlanCacheLookup(hit);

        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection)
            => RecordAccessPath(selection.AccessPath, selection.EstimatedRows);

        public void OnPlanChanged(QueryPlanChangeKind change)
            => RecordPlanChange(change);

        private void CompleteExecution(
            CSharpDbOperationOutcome outcome,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error)
        {
            if (!TryBeginCompletion())
                return;

            long? recordedAtTimestamp = null;
            try
            {
                recordedAtTimestamp = Context.GetTimestamp();
            }
            catch
            {
                // A diagnostics clock cannot affect terminal transfer.
            }

            TimeSpan totalDuration = TimeSpan.Zero;
            if (recordedAtTimestamp is long terminalTimestamp)
            {
                try
                {
                    totalDuration = NonNegative(
                        Context.GetElapsedTime(terminalTimestamp));
                }
                catch
                {
                    // Preserve the independently captured retention timestamp.
                }
            }

            DateTimeOffset completedAtUtc = Context.StartedAtUtc;
            try
            {
                completedAtUtc = Context.GetUtcNow();
            }
            catch
            {
                // Preserve independently captured terminal duration/timestamp.
            }

            TimeSpan? timeToFirstResult = null;
            if (_firstRowTimestamp is long firstRowTimestamp)
            {
                try
                {
                    timeToFirstResult = NonNegative(
                        Context.GetElapsedTime(firstRowTimestamp));
                    if (timeToFirstResult > totalDuration)
                        timeToFirstResult = totalDuration;
                }
                catch
                {
                    // A diagnostics clock cannot affect terminal transfer.
                }
            }

            try
            {
                CompleteClaimed(
                    outcome,
                    completedAtUtc,
                    totalDuration,
                    timeToFirstResult,
                    rowsProduced,
                    rowsAffected,
                    error,
                    _owner.IsSlowQuery(totalDuration),
                    recordedAtTimestamp);
            }
            catch
            {
                // Registry/history failures cannot affect query completion.
            }
        }

        internal void CompleteFromObservation(
            CSharpDbOperationOutcome outcome,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error)
            => CompleteExecution(
                outcome,
                rowsProduced,
                rowsAffected,
                error);

        internal void SetPhase(QueryExecutionPhase phase)
        {
            if (phase is QueryExecutionPhase.Unknown or QueryExecutionPhase.Completed ||
                !Enum.IsDefined(phase))
            {
                throw new ArgumentOutOfRangeException(nameof(phase));
            }

            if (Volatile.Read(ref _leaseCompleted) != 0 || !TryEnterMutation())
                return;

            try
            {
                _owner.TrySetPhase(this, phase);
            }
            finally
            {
                ExitMutation();
            }
        }

        internal bool Matches(
            QueryRuntimeDiagnostics owner,
            CSharpDbOperationContext context)
            => ReferenceEquals(_owner, owner) &&
               ReferenceEquals(Context, context) &&
               Context.OperationId == context.OperationId &&
               Volatile.Read(ref _leaseCompleted) == 0;

        internal void RecordPlanCacheLookup(bool hit)
        {
            if (Volatile.Read(ref _leaseCompleted) == 0 && TryEnterMutation())
            {
                try
                {
                _owner.RecordPlanCacheLookup(this, hit);
                }
                finally
                {
                    ExitMutation();
                }
            }
        }

        internal void RecordAccessPath(
            QueryPlanAccessPathCategory accessPath,
            long? estimatedRows)
        {
            if (Volatile.Read(ref _leaseCompleted) == 0 && TryEnterMutation())
            {
                try
                {
                _owner.RecordAccessPath(this, accessPath, estimatedRows);
                }
                finally
                {
                    ExitMutation();
                }
            }
        }

        internal void RecordPlanChange(QueryPlanChangeKind change)
        {
            if (Volatile.Read(ref _leaseCompleted) == 0 && TryEnterMutation())
            {
                try
                {
                _owner.RecordPlanChange(this, change);
                }
                finally
                {
                    ExitMutation();
                }
            }
        }

        internal void TryRetainQueryDetail(
            SqlTextCaptureMode captureMode,
            string? capturedSqlText)
        {
            if (Volatile.Read(ref _leaseCompleted) == 0 && TryEnterMutation())
            {
                try
                {
                    _owner.TryRetainQueryDetail(
                        this,
                        captureMode,
                        capturedSqlText);
                }
                finally
                {
                    ExitMutation();
                }
            }
        }

        internal IDisposable? EnterWaiting()
        {
            if (Volatile.Read(ref _leaseCompleted) != 0 || !TryEnterMutation())
                return null;

            try
            {
                return _owner.EnterWaiting(this);
            }
            finally
            {
                ExitMutation();
            }
        }

        internal QueryRuntimeOperation? RebindTo(
            QueryRuntimeDiagnostics target,
            QueryExecutionPhase initialPhase)
        {
            ArgumentNullException.ThrowIfNull(target);
            if (initialPhase is QueryExecutionPhase.Unknown or QueryExecutionPhase.Completed ||
                !Enum.IsDefined(initialPhase))
            {
                throw new ArgumentOutOfRangeException(nameof(initialPhase));
            }

            if (ReferenceEquals(_owner, target))
            {
                SetPhase(initialPhase);
                return Volatile.Read(ref _leaseCompleted) == 0 ? this : null;
            }

            // Rebinding is valid only before the source lease has been adopted
            // by an engine query. Claim the adoption slot so dequeue/adoption,
            // rebind, and terminal completion have one deterministic winner.
            if (Interlocked.CompareExchange(ref _adoptionClaimed, 1, 0) != 0 ||
                Interlocked.CompareExchange(ref _leaseCompleted, 1, 0) != 0)
            {
                return null;
            }

            SealMutations();

            if (!_owner.DetachForRebind(
                    this,
                    _registered,
                    out LongRunningPublicationClaim? publicationClaim,
                    out QueryPlanState plan,
                    out QueryDetailState? detail))
            {
                return null;
            }

            return target.TryStartCore(
                Context,
                initialPhase,
                publicationClaim,
                plan,
                detail,
                SuppressDiagnosticEvents,
                publishLongRunningQueryEvents: publicationClaim is not null,
                previousOwner: _owner,
                out _);
        }

        internal bool TryAdopt(
            QueryRuntimeDiagnostics owner,
            CSharpDbOperationContext context)
        {
            ArgumentNullException.ThrowIfNull(owner);
            ArgumentNullException.ThrowIfNull(context);

            if (!ReferenceEquals(_owner, owner) ||
                !ReferenceEquals(Context, context) ||
                Context.OperationId != context.OperationId ||
                Volatile.Read(ref _leaseCompleted) != 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _adoptionClaimed, 1, 0) != 0)
                return false;

            return Volatile.Read(ref _leaseCompleted) == 0;
        }

        internal void Abandon()
        {
            if (Interlocked.Exchange(ref _leaseCompleted, 1) != 0)
                return;

            SealMutations();
            _owner.Abandon(this, _registered);
        }

        internal void Complete(
            CSharpDbOperationOutcome outcome,
            DateTimeOffset completedAtUtc,
            TimeSpan duration,
            TimeSpan? timeToFirstResult,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error,
            bool isSlow)
        {
            if (outcome == CSharpDbOperationOutcome.Unknown || !Enum.IsDefined(outcome))
                throw new ArgumentOutOfRangeException(nameof(outcome));
            if (!TryBeginCompletion())
                return;

            CompleteClaimed(
                outcome,
                completedAtUtc,
                duration,
                timeToFirstResult,
                rowsProduced,
                rowsAffected,
                error,
                isSlow,
                recordedAtTimestamp: null);
        }

        private bool TryBeginCompletion()
        {
            if (Interlocked.Exchange(ref _leaseCompleted, 1) != 0)
                return false;

            SealMutations();
            return true;
        }

        private void CompleteClaimed(
            CSharpDbOperationOutcome outcome,
            DateTimeOffset completedAtUtc,
            TimeSpan duration,
            TimeSpan? timeToFirstResult,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error,
            bool isSlow,
            long? recordedAtTimestamp)
        {
            _owner.Complete(
                this,
                _registered,
                outcome,
                completedAtUtc,
                duration,
                timeToFirstResult,
                rowsProduced,
                rowsAffected,
                error,
                isSlow,
                recordedAtTimestamp);
        }

        internal void ExitWaiting(
            WaitingPhaseLease lease,
            QueryExecutionPhase priorPhase)
        {
            if (!TryEnterMutation())
                return;

            try
            {
                _owner.ExitWaiting(this, lease, priorPhase);
            }
            finally
            {
                ExitMutation();
            }
        }

        internal QueryExecutionPhase CapturePhase()
        {
            if (!TryEnterMutation())
                return Phase;

            try
            {
                return Phase;
            }
            finally
            {
                ExitMutation();
            }
        }

        internal QueryPlanCopy CapturePlan(long? actualRows)
        {
            if (!TryEnterMutation())
                return Plan.CreateCopy(Context, actualRows);

            try
            {
                return Plan.CreateCopy(Context, actualRows);
            }
            finally
            {
                ExitMutation();
            }
        }

        internal QueryDetailState? CaptureDetail()
        {
            if (!TryEnterMutation())
                return Detail;

            try
            {
                return Detail;
            }
            finally
            {
                ExitMutation();
            }
        }

        internal void RetireFromOwner()
        {
            Interlocked.Exchange(ref _leaseCompleted, 1);
            SealMutations();
        }

        private bool TryEnterMutation()
        {
            var spinner = new SpinWait();
            while (true)
            {
                int observed = Volatile.Read(ref _mutationState);
                if (observed == MutationsSealed)
                    return false;
                if (observed == MutationAvailable &&
                    Interlocked.CompareExchange(
                        ref _mutationState,
                        MutationHeld,
                        MutationAvailable) == MutationAvailable)
                {
                    return true;
                }

                spinner.SpinOnce();
            }
        }

        private void ExitMutation()
            => Volatile.Write(ref _mutationState, MutationAvailable);

        private void SealMutations()
        {
            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(
                       ref _mutationState,
                       MutationsSealed,
                       MutationAvailable) != MutationAvailable)
            {
                if (Volatile.Read(ref _mutationState) == MutationsSealed)
                    return;
                spinner.SpinOnce();
            }
        }
    }

    internal struct QueryPlanState
    {
        private QueryPlanAccessPathCategory _accessPath;
        private long? _estimatedRows;
        private bool _selectionObserved;
        private bool _planCacheLookupObserved;
        private bool _planCacheHit;
        private bool _cachedPlanReclassified;
        private bool _adaptiveReclassified;
        private bool _adaptiveReoptimizationAttempted;
        private bool _adaptiveReoptimized;
        private bool _adaptiveReoptimizationRejected;

        internal void RecordPlanCacheLookup(bool hit)
        {
            // A logical statement may include nested planner work. Its cache
            // summary is a hit only when every observed lookup was a hit; one
            // miss makes the aggregate operation a miss.
            _planCacheHit = !_planCacheLookupObserved
                ? hit
                : _planCacheHit && hit;
            _planCacheLookupObserved = true;
        }

        internal void RecordAccessPath(
            QueryPlanAccessPathCategory accessPath,
            long? estimatedRows)
        {
            int candidatePrecedence = GetAccessPathPrecedence(accessPath);
            int currentPrecedence = GetAccessPathPrecedence(_accessPath);
            if (!_selectionObserved || candidatePrecedence > currentPrecedence)
            {
                // A logical statement can report multiple physical selections
                // (subqueries, triggers, or adaptive work). Use a fixed coarse
                // precedence rather than callback order so concurrent results
                // cannot make the representative category nondeterministic.
                _accessPath = accessPath;
                _estimatedRows = estimatedRows;
                _selectionObserved = true;
                return;
            }

            if (candidatePrecedence == currentPrecedence && estimatedRows is not null)
            {
                _estimatedRows = _estimatedRows is null
                    ? estimatedRows
                    : Math.Max(_estimatedRows.Value, estimatedRows.Value);
            }
        }

        internal void RecordPlanChange(QueryPlanChangeKind change)
        {
            switch (change)
            {
                case QueryPlanChangeKind.CachedPlanReclassified:
                    _cachedPlanReclassified = true;
                    break;
                case QueryPlanChangeKind.AdaptiveCardinalityReclassified:
                    _adaptiveReclassified = true;
                    break;
                case QueryPlanChangeKind.AdaptiveReoptimizationAttempted:
                    _adaptiveReoptimizationAttempted = true;
                    break;
                case QueryPlanChangeKind.AdaptiveReoptimized:
                    _adaptiveReoptimized = true;
                    break;
                case QueryPlanChangeKind.AdaptiveReoptimizationRejected:
                    _adaptiveReoptimizationRejected = true;
                    break;
            }
        }

        internal QueryPlanCopy CreateCopy(
            CSharpDbOperationContext context,
            long? actualRows)
            => new(
                context,
                MapAccessPath(_accessPath),
                _planCacheLookupObserved ? _planCacheHit : null,
                _cachedPlanReclassified,
                _adaptiveReclassified,
                _adaptiveReoptimizationAttempted,
                _adaptiveReoptimized,
                _adaptiveReoptimizationRejected,
                _estimatedRows,
                actualRows);

        internal readonly QueryPlanState Clone() => this;

        private static int GetAccessPathPrecedence(
            QueryPlanAccessPathCategory accessPath)
            => accessPath switch
            {
                QueryPlanAccessPathCategory.TableScan => 1,
                QueryPlanAccessPathCategory.IndexScan => 2,
                QueryPlanAccessPathCategory.IndexSeek => 3,
                QueryPlanAccessPathCategory.PrimaryKeyLookup => 4,
                QueryPlanAccessPathCategory.FullTextIndex => 5,
                QueryPlanAccessPathCategory.Temporary => 6,
                _ => 0,
            };
    }

    internal sealed class LongRunningPublicationClaim
    {
        private int _claimed;

        internal LongRunningPublicationClaim(bool suppressDiagnosticEvents = false)
        {
            SuppressDiagnosticEvents = suppressDiagnosticEvents;
        }

        internal bool SuppressDiagnosticEvents { get; }

        internal bool TryClaim()
            => Interlocked.CompareExchange(ref _claimed, 1, 0) == 0;

        internal void Release()
            => Interlocked.CompareExchange(ref _claimed, 0, 1);
    }

    internal sealed class WaitingPhaseLease : IDisposable
    {
        private QueryRuntimeDiagnostics? _owner;
        private QueryRuntimeOperation? _state;
        private readonly QueryExecutionPhase _priorPhase;

        internal WaitingPhaseLease(
            QueryRuntimeDiagnostics owner,
            QueryRuntimeOperation state,
            QueryExecutionPhase priorPhase)
        {
            _owner = owner;
            _state = state;
            _priorPhase = priorPhase;
        }

        public void Dispose()
        {
            QueryRuntimeDiagnostics? owner = Interlocked.Exchange(
                ref _owner,
                null);
            QueryRuntimeOperation? state = Interlocked.Exchange(ref _state, null);
            if (owner is not null && state is not null)
                state.ExitWaiting(this, _priorPhase);
        }
    }

    private readonly record struct LongRunningCandidate(
        QueryRuntimeOperation State,
        TimeSpan Elapsed);

    private readonly record struct LongRunningQueryCopy(
        QueryRuntimeOperation State,
        LongRunningPublicationClaim PublicationClaim,
        CSharpDbOperationContext Context,
        DateTimeOffset ObservedAtUtc,
        TimeSpan Elapsed,
        TimeSpan Threshold,
        QueryExecutionPhase Phase)
    {
        internal CSharpDbLongRunningQueryEvent CreateEvent()
            => new(Context, ObservedAtUtc, Elapsed, Threshold, Phase);
    }

    private readonly record struct ActiveQueryCopy(
        OpaqueDiagnosticsId OperationId,
        OpaqueDiagnosticsId? ParentOperationId,
        CSharpDbOperationClass OperationClass,
        CSharpDbOperationRole Role,
        DateTimeOffset StartedAtUtc,
        long StartingTimestamp,
        QueryFingerprint? Fingerprint,
        CSharpDbTransport Transport,
        DiagnosticsTraceId? TraceId,
        OpaqueDiagnosticsId? SessionId,
        QueryExecutionPhase Phase)
    {
        internal ActiveQuerySnapshot CreateSnapshot(
            DiagnosticsSnapshotMetadata metadata,
            long capturedTimestamp,
            TimeProvider timeProvider)
            => new(
                metadata,
                OperationId,
                ParentOperationId,
                OperationClass,
                Role,
                Phase,
                StartedAtUtc,
                GetCapturedElapsed(
                    StartingTimestamp,
                    capturedTimestamp,
                    timeProvider),
                Fingerprint,
                Transport,
                TraceId,
                SessionId);

        private static TimeSpan GetCapturedElapsed(
            long startingTimestamp,
            long capturedTimestamp,
            TimeProvider timeProvider)
        {
            try
            {
                return NonNegative(timeProvider.GetElapsedTime(
                    startingTimestamp,
                    capturedTimestamp));
            }
            catch
            {
                // Durations are monotonic by contract. A failing custom
                // provider cannot be replaced with UTC wall-clock subtraction,
                // which can jump backward or forward independently.
                return TimeSpan.Zero;
            }
        }
    }

    private readonly record struct SnapshotCaptureStamp(
        long Timestamp,
        DateTimeOffset CapturedAtUtc);

    private readonly record struct ActiveCollectionCapture(
        DiagnosticsSnapshotMetadata Metadata,
        ActiveQuerySnapshot[] Records,
        long DroppedCount,
        bool IsTruncated);

    private readonly record struct RecentQueryState(
        long Sequence,
        CSharpDbOperationContext Context,
        DateTimeOffset CompletedAtUtc,
        TimeSpan Duration,
        TimeSpan? TimeToFirstResult,
        TimeSpan? ResultConsumptionDuration,
        CSharpDbOperationOutcome Outcome,
        long RowsProduced,
        long RowsAffected,
        SafeErrorProjection? Error,
        long RecordedAtTimestamp,
        QueryPlanCopy Plan,
        QueryDetailState? Detail)
    {
        internal RecentQuerySnapshot CreateSnapshot(DiagnosticsSnapshotMetadata metadata)
            => new(
                metadata,
                Context.OperationId,
                Context.ParentOperationId,
                Context.OperationClass,
                Context.Role,
                Context.StartedAtUtc,
                CompletedAtUtc,
                Duration,
                TimeToFirstResult,
                ResultConsumptionDuration,
                Outcome,
                Context.QueryFingerprint,
                Context.Transport,
                RowsProduced,
                RowsAffected,
                Context.TraceId,
                Context.SessionId,
                Error);
    }

    internal readonly record struct QueryDetailState(
        SqlTextCaptureMode CaptureMode,
        string CapturedSqlText,
        bool FieldsTruncated);

    private readonly record struct QueryDetailCopy(
        CSharpDbOperationContext Context,
        QueryDetailState Detail)
    {
        internal QueryDetailSnapshot CreateSnapshot(
            DiagnosticsSnapshotMetadata metadata)
            => new(
                metadata,
                Context.OperationId,
                Context.QueryFingerprint,
                Detail.CaptureMode,
                Detail.CapturedSqlText);
    }

    private readonly record struct RecentCollectionCapture(
        DiagnosticsSnapshotMetadata Metadata,
        RecentQuerySnapshot[] Records,
        long DroppedCount,
        bool IsTruncated);

    internal readonly record struct QueryPlanCopy(
        CSharpDbOperationContext Context,
        QueryAccessPathCategory AccessPath,
        bool? PlanCacheHit,
        bool CachedPlanReclassified,
        bool AdaptiveReclassified,
        bool AdaptiveReoptimizationAttempted,
        bool AdaptiveReoptimized,
        bool AdaptiveReoptimizationRejected,
        long? EstimatedRows,
        long? ActualRows)
    {
        internal QueryPlanDiagnosticsSnapshot CreateSnapshot(
            DiagnosticsSnapshotMetadata metadata)
            => new(
                metadata,
                Context.OperationId,
                Context.QueryFingerprint,
                AccessPath,
                PlanCacheHit,
                AdaptiveReoptimized,
                EstimatedRows,
                ActualRows,
                PlanNodeCount: null,
                PlanTruncated: false)
            {
                Reclassified = AdaptiveReclassified,
                CachedPlanReclassified = CachedPlanReclassified,
                AdaptiveReclassified = AdaptiveReclassified,
                AdaptiveReoptimizationAttempted = AdaptiveReoptimizationAttempted,
                AdaptiveReoptimizationRejected = AdaptiveReoptimizationRejected,
            };
    }

    private static QueryAccessPathCategory MapAccessPath(
        QueryPlanAccessPathCategory accessPath)
        => accessPath switch
        {
            QueryPlanAccessPathCategory.TableScan => QueryAccessPathCategory.TableScan,
            QueryPlanAccessPathCategory.PrimaryKeyLookup => QueryAccessPathCategory.PrimaryKeyLookup,
            QueryPlanAccessPathCategory.IndexSeek => QueryAccessPathCategory.IndexSeek,
            QueryPlanAccessPathCategory.IndexScan => QueryAccessPathCategory.IndexScan,
            QueryPlanAccessPathCategory.FullTextIndex => QueryAccessPathCategory.FullTextIndex,
            QueryPlanAccessPathCategory.Temporary => QueryAccessPathCategory.Temporary,
            _ => QueryAccessPathCategory.Unknown,
        };

    private readonly record struct QuerySummaryCopy(
        long RequestCount,
        long StatementExecutionCount,
        long SucceededCount,
        long FailedCount,
        long CanceledCount,
        long SlowCount,
        long RowsProduced,
        long RowsAffected,
        int ActiveCount)
    {
        internal QueryDiagnosticsSummary CreateSnapshot(DiagnosticsSnapshotMetadata metadata)
            => new(
                metadata,
                RequestCount,
                StatementExecutionCount,
                SucceededCount,
                FailedCount,
                CanceledCount,
                SlowCount,
                RowsProduced,
                RowsAffected,
                ActiveCount);
    }
}
