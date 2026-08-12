using CSharpDB.Observability;

namespace CSharpDB.Engine;

internal readonly record struct MaintenanceRuntimeRecord(
    CSharpDbOperationContext Context,
    MaintenanceOperationKind Kind,
    MaintenanceOperationPhase Phase,
    TimeSpan Elapsed,
    long? CompletedUnits,
    long? TotalUnits,
    CSharpDbOperationOutcome Outcome,
    int WarningCount,
    int ErrorCount,
    SafeErrorProjection? Error)
{
    internal MaintenanceOperationSnapshot ToSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Context.OperationId,
            Kind,
            Phase,
            Context.StartedAtUtc,
            Elapsed,
            CompletedUnits,
            TotalUnits,
            Outcome,
            WarningCount,
            ErrorCount,
            Error);
}

internal readonly record struct MaintenanceRuntimeDiagnosticsCapture(
    MaintenanceRuntimeRecord[] Active,
    MaintenanceRuntimeRecord[] Recent,
    int Capacity,
    TimeSpan Retention,
    long ActiveRejectedCount,
    long RecentDroppedCount,
    bool CaptureFailed,
    bool ActiveSelectionTruncated = false,
    bool RecentSelectionTruncated = false)
{
    internal static MaintenanceRuntimeDiagnosticsCapture Empty { get; } =
        new([], [], 0, TimeSpan.Zero, 0, 0, CaptureFailed: false);

    internal bool ActiveRecordsTruncated =>
        ActiveRejectedCount > 0 || ActiveSelectionTruncated;

    internal bool RecentRecordsTruncated =>
        RecentDroppedCount > 0 || RecentSelectionTruncated;
}

/// <summary>
/// A bounded, process-local registry for maintenance operations. A client may
/// own one registry across runtime-family replacement; direct Engine work uses
/// the equivalent component attached to its exact runtime state.
/// </summary>
internal sealed class MaintenanceRuntimeDiagnostics : IDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<OpaqueDiagnosticsId, OperationState> _active;
    private readonly Queue<RecentState> _recent;
    private readonly int _capacity;
    private readonly TimeSpan _retention;
    private readonly long _retentionTimestampUnits;
    private readonly TimeProvider _timeProvider;
    private long _hiddenActiveCount;
    private long _recentDroppedCount;
    private bool _disposed;

    internal MaintenanceRuntimeDiagnostics(
        int capacity,
        TimeSpan retention,
        TimeProvider timeProvider)
    {
        if (capacity <= 0 ||
            capacity > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }
        if (retention <= TimeSpan.Zero ||
            retention > CSharpDbObservabilityOptions.MaximumRetention)
        {
            throw new ArgumentOutOfRangeException(nameof(retention));
        }

        ArgumentNullException.ThrowIfNull(timeProvider);
        _capacity = capacity;
        _retention = retention;
        _timeProvider = timeProvider;
        _retentionTimestampUnits = ToTimestampUnits(
            retention,
            timeProvider.TimestampFrequency);
        _active = new Dictionary<OpaqueDiagnosticsId, OperationState>(capacity);
        _recent = new Queue<RecentState>(capacity);
    }

    internal static MaintenanceRuntimeDiagnostics? GetOrCreate(
        CSharpDbRuntimeDiagnosticsState? runtimeState)
    {
        if (runtimeState?.HistoryEnabled != true)
            return null;

        try
        {
            return runtimeState.GetOrCreateComponent(
                () => new MaintenanceRuntimeDiagnostics(
                    runtimeState.RecentOperationCapacity,
                    runtimeState.RecentOperationRetention,
                    runtimeState.TimeProvider));
        }
        catch
        {
            return null;
        }
    }

    internal static MaintenanceRuntimeDiagnostics? TryGet(
        CSharpDbRuntimeDiagnosticsState? runtimeState)
        => runtimeState is not null &&
           runtimeState.TryGetComponent<MaintenanceRuntimeDiagnostics>(
               out MaintenanceRuntimeDiagnostics? diagnostics)
            ? diagnostics
            : null;

    internal MaintenanceRuntimeOperation? TryStart(
        CSharpDbOperationContext context,
        MaintenanceOperationKind kind,
        MaintenanceOperationPhase initialPhase)
    {
        ArgumentNullException.ThrowIfNull(context);
        ValidateKind(kind);
        ValidateActivePhase(initialPhase);

        bool ownershipClaimed = false;
        try
        {
            if (!context.TryClaimRuntimeDiagnostics(this))
                return null;

            ownershipClaimed = true;
            var state = new OperationState(
                context,
                kind,
                initialPhase,
                registered: false);
            var operation = new MaintenanceRuntimeOperation(this, state);
            lock (_gate)
            {
                if (_disposed)
                    return null;
                if (_active.ContainsKey(context.OperationId))
                    return null;

                bool registered = _active.Count < _capacity;
                if (registered)
                {
                    state.Registered = true;
                    _active.Add(context.OperationId, state);
                }
                else
                {
                    _hiddenActiveCount = SaturatingIncrement(
                        _hiddenActiveCount);
                }
            }

            if (!state.Registered)
            {
                // Overflow operations are deliberately not retained in the
                // bounded active table. Do not let such an operation pin the
                // immutable context's diagnostics-owner slot indefinitely if
                // it never reaches a terminal callback. Its operation handle
                // still records a bounded recent terminal when one arrives.
                context.ReleaseRuntimeDiagnostics(this);
                state.OwnershipHeld = false;
            }

            ownershipClaimed = false;
            NotifyPhaseChanged(initialPhase);
            return operation;
        }
        catch
        {
            return null;
        }
        finally
        {
            if (ownershipClaimed)
                context.ReleaseRuntimeDiagnostics(this);
        }
    }

    internal MaintenanceRuntimeDiagnosticsCapture Capture()
        => Capture(_capacity, _capacity);

    internal MaintenanceRuntimeDiagnosticsCapture Capture(
        int maximumActiveRecords,
        int maximumRecentRecords)
    {
        ValidateCaptureLimit(maximumActiveRecords, nameof(maximumActiveRecords));
        ValidateCaptureLimit(maximumRecentRecords, nameof(maximumRecentRecords));
        long? nowTimestamp = GetTimestampSafely();
        ActiveCopy[] active;
        MaintenanceRuntimeRecord[] recent;
        long activeRejectedCount;
        long recentDroppedCount;
        bool activeSelectionTruncated;
        bool recentSelectionTruncated;

        try
        {
            lock (_gate)
            {
                if (_disposed)
                    return MaintenanceRuntimeDiagnosticsCapture.Empty;

                if (nowTimestamp is long now)
                    PruneExpiredLocked(now);
                var activeSelection = new BoundedSelection<ActiveCopy>(
                    maximumActiveRecords,
                    ActiveCopyComparer.Instance,
                    static record => record.Context.OperationId.Value);
                if (maximumActiveRecords == 0)
                {
                    activeSelectionTruncated = _active.Count > 0;
                }
                else
                {
                    foreach (OperationState state in _active.Values)
                    {
                        activeSelection.Add(new ActiveCopy(
                            state.Context,
                            state.Kind,
                            state.Phase,
                            state.CompletedUnits,
                            state.TotalUnits));
                    }
                    activeSelectionTruncated = activeSelection.IsTruncated;
                }
                active = activeSelection.ToArray();

                var recentSelection = new BoundedSelection<
                    MaintenanceRuntimeRecord>(
                        maximumRecentRecords,
                        RecentRecordComparer.Instance,
                        static record => record.Context.OperationId.Value);
                if (maximumRecentRecords == 0)
                {
                    recentSelectionTruncated = _recent.Count > 0;
                }
                else
                {
                    foreach (RecentState state in _recent)
                        recentSelection.Add(state.Record);
                    recentSelectionTruncated = recentSelection.IsTruncated;
                }
                recent = recentSelection.ToArray();
                activeRejectedCount = _hiddenActiveCount;
                recentDroppedCount = _recentDroppedCount;
            }

            MaintenanceRuntimeRecord[] activeRecords = active
                .Select(static state => new MaintenanceRuntimeRecord(
                    state.Context,
                    state.Kind,
                    state.Phase,
                    GetElapsedSafely(state.Context),
                    state.CompletedUnits,
                    state.TotalUnits,
                    CSharpDbOperationOutcome.Unknown,
                    WarningCount: 0,
                    ErrorCount: 0,
                    Error: null))
                .ToArray();
            return new MaintenanceRuntimeDiagnosticsCapture(
                activeRecords,
                recent,
                _capacity,
                _retention,
                activeRejectedCount,
                recentDroppedCount,
                CaptureFailed: false,
                activeSelectionTruncated,
                recentSelectionTruncated);
        }
        catch
        {
            return new MaintenanceRuntimeDiagnosticsCapture(
                [],
                [],
                _capacity,
                _retention,
                ActiveRejectedCount: 0,
                RecentDroppedCount: 0,
                CaptureFailed: true);
        }
    }

    internal static MaintenanceRuntimeDiagnosticsCapture Merge(
        IEnumerable<MaintenanceRuntimeDiagnostics?> sources,
        bool sourcesTruncated = false)
        => Merge(
            sources,
            CSharpDbObservabilityOptions.MaximumHistoryCapacity,
            CSharpDbObservabilityOptions.MaximumHistoryCapacity,
            sourcesTruncated);

    internal static MaintenanceRuntimeDiagnosticsCapture Merge(
        IEnumerable<MaintenanceRuntimeDiagnostics?> sources,
        int maximumActiveRecords,
        int maximumRecentRecords,
        bool sourcesTruncated = false)
    {
        ArgumentNullException.ThrowIfNull(sources);
        ValidateCaptureLimit(maximumActiveRecords, nameof(maximumActiveRecords));
        ValidateCaptureLimit(maximumRecentRecords, nameof(maximumRecentRecords));
        var activeSelection = new BoundedSelection<MaintenanceRuntimeRecord>(
            maximumActiveRecords,
            ActiveRecordComparer.Instance,
            static record => record.Context.OperationId.Value);
        var recentSelection = new BoundedSelection<MaintenanceRuntimeRecord>(
            maximumRecentRecords,
            RecentRecordComparer.Instance,
            static record => record.Context.OperationId.Value);
        int capacity = 0;
        TimeSpan retention = TimeSpan.Zero;
        long activeRejectedCount = 0;
        long recentDroppedCount = 0;
        bool activeSelectionTruncated = sourcesTruncated;
        bool recentSelectionTruncated = sourcesTruncated;

        try
        {
            foreach (MaintenanceRuntimeDiagnostics? source in sources)
            {
                if (source is null)
                    continue;

                MaintenanceRuntimeDiagnosticsCapture capture = source.Capture(
                    maximumActiveRecords,
                    maximumRecentRecords);
                if (capture.CaptureFailed)
                {
                    return new MaintenanceRuntimeDiagnosticsCapture(
                        [],
                        [],
                        0,
                        TimeSpan.Zero,
                        0,
                        0,
                        CaptureFailed: true);
                }

                capacity = Math.Min(
                    CSharpDbObservabilityOptions.MaximumHistoryCapacity,
                    SaturatingAdd(capacity, capture.Capacity));
                retention = retention >= capture.Retention
                    ? retention
                    : capture.Retention;
                activeRejectedCount = SaturatingAdd(
                    activeRejectedCount,
                    capture.ActiveRejectedCount);
                recentDroppedCount = SaturatingAdd(
                    recentDroppedCount,
                    capture.RecentDroppedCount);
                activeSelectionTruncated |= capture.ActiveSelectionTruncated;
                recentSelectionTruncated |= capture.RecentSelectionTruncated;
                foreach (MaintenanceRuntimeRecord record in capture.Recent)
                    recentSelection.Add(record);
                foreach (MaintenanceRuntimeRecord record in capture.Active)
                    activeSelection.Add(record);
            }

            activeSelectionTruncated |= activeSelection.IsTruncated;
            recentSelectionTruncated |= recentSelection.IsTruncated;
            return new MaintenanceRuntimeDiagnosticsCapture(
                activeSelection.ToArray(),
                recentSelection.ToArray(),
                capacity,
                retention,
                activeRejectedCount,
                recentDroppedCount,
                CaptureFailed: false,
                activeSelectionTruncated,
                recentSelectionTruncated);
        }
        catch
        {
            return new MaintenanceRuntimeDiagnosticsCapture(
                [],
                [],
                0,
                TimeSpan.Zero,
                0,
                0,
                CaptureFailed: true);
        }
    }

    public void Dispose()
    {
        OperationState[] active;
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;
            active = _active.Values.ToArray();
            _active.Clear();
            _recent.Clear();
            _hiddenActiveCount = 0;
            foreach (OperationState state in active)
            {
                state.Completed = true;
                state.OwnershipHeld = false;
            }
        }

        foreach (OperationState state in active)
            state.Context.ReleaseRuntimeDiagnostics(this);
    }

    private bool SetPhase(
        OperationState state,
        MaintenanceOperationPhase phase)
    {
        ValidateActivePhase(phase);
        bool changed = false;
        lock (_gate)
        {
            if (!state.Completed && !_disposed && state.Phase != phase)
            {
                state.Phase = phase;
                changed = true;
            }
        }

        if (changed)
            NotifyPhaseChanged(phase);
        return changed;
    }

    private void SetProgress(
        OperationState state,
        long? completedUnits,
        long? totalUnits)
    {
        long? safeTotalUnits = NonNegative(totalUnits);
        long? safeCompletedUnits = NonNegative(completedUnits);
        if (safeCompletedUnits is not null &&
            safeTotalUnits is not null &&
            safeCompletedUnits > safeTotalUnits)
        {
            safeCompletedUnits = safeTotalUnits;
        }

        lock (_gate)
        {
            if (state.Completed || _disposed)
                return;

            state.CompletedUnits = safeCompletedUnits;
            state.TotalUnits = safeTotalUnits;
        }
    }

    private void Complete(
        OperationState state,
        CSharpDbOperationOutcome outcome,
        long? completedUnits,
        long? totalUnits,
        int warningCount,
        int errorCount,
        SafeErrorProjection? error)
    {
        TimeSpan elapsed = GetElapsedSafely(state.Context);
        long? recordedAtTimestamp = GetTimestampSafely();
        long? suppliedTotalUnits = NonNegative(totalUnits);
        long? suppliedCompletedUnits = NonNegative(completedUnits);
        if (suppliedCompletedUnits is not null &&
            suppliedTotalUnits is not null &&
            suppliedCompletedUnits > suppliedTotalUnits)
        {
            suppliedCompletedUnits = suppliedTotalUnits;
        }

        bool releaseOwnership = false;
        try
        {
            lock (_gate)
            {
                if (state.Completed)
                    return;

                long? safeTotalUnits = suppliedTotalUnits ?? state.TotalUnits;
                long? safeCompletedUnits =
                    suppliedCompletedUnits ?? state.CompletedUnits;
                if (safeCompletedUnits is not null &&
                    safeTotalUnits is not null &&
                    safeCompletedUnits > safeTotalUnits)
                {
                    safeCompletedUnits = safeTotalUnits;
                }
                var record = new MaintenanceRuntimeRecord(
                    state.Context,
                    state.Kind,
                    MaintenanceOperationPhase.Completed,
                    elapsed,
                    safeCompletedUnits,
                    safeTotalUnits,
                    outcome,
                    Math.Max(0, warningCount),
                    Math.Max(0, errorCount),
                    error);

                state.Completed = true;
                state.Phase = MaintenanceOperationPhase.Completed;
                if (state.Registered &&
                    _active.TryGetValue(
                        state.Context.OperationId,
                        out OperationState? current) &&
                    ReferenceEquals(current, state))
                {
                    _active.Remove(state.Context.OperationId);
                }
                else if (!state.Registered && _hiddenActiveCount > 0)
                {
                    _hiddenActiveCount--;
                }

                if (state.OwnershipHeld)
                {
                    state.OwnershipHeld = false;
                    releaseOwnership = true;
                }
                if (_disposed)
                    return;

                if (recordedAtTimestamp is long nowTimestamp)
                    PruneExpiredLocked(nowTimestamp);
                while (_recent.Count >= _capacity)
                {
                    _recent.Dequeue();
                    _recentDroppedCount = SaturatingIncrement(
                        _recentDroppedCount);
                }
                _recent.Enqueue(new RecentState(recordedAtTimestamp, record));
            }
        }
        finally
        {
            if (releaseOwnership)
                state.Context.ReleaseRuntimeDiagnostics(this);
        }

        NotifyPhaseChanged(MaintenanceOperationPhase.Completed);
    }

    private void PruneExpiredLocked(long nowTimestamp)
    {
        int count = _recent.Count;
        for (int i = 0; i < count; i++)
        {
            RecentState recent = _recent.Dequeue();
            if (recent.RecordedAtTimestamp is long recordedAtTimestamp &&
                IsExpired(recordedAtTimestamp, nowTimestamp))
            {
                _recentDroppedCount = SaturatingIncrement(
                    _recentDroppedCount);
            }
            else
            {
                _recent.Enqueue(recent);
            }
        }
    }

    private bool IsExpired(long recordedAtTimestamp, long nowTimestamp)
        => nowTimestamp >= recordedAtTimestamp &&
           nowTimestamp - recordedAtTimestamp > _retentionTimestampUnits;

    private long? GetTimestampSafely()
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

    private static TimeSpan GetElapsedSafely(CSharpDbOperationContext context)
    {
        try
        {
            TimeSpan elapsed = context.GetElapsedTime();
            return elapsed < TimeSpan.Zero ? TimeSpan.Zero : elapsed;
        }
        catch
        {
            return TimeSpan.Zero;
        }
    }

    private static long? NonNegative(long? value)
        => value is null ? null : Math.Max(0, value.Value);

    private static long ToTimestampUnits(
        TimeSpan duration,
        long timestampFrequency)
    {
        if (timestampFrequency <= 0)
            throw new ArgumentOutOfRangeException(nameof(timestampFrequency));

        double units = Math.Ceiling(
            duration.TotalSeconds * timestampFrequency);
        return units >= long.MaxValue
            ? long.MaxValue
            : Math.Max(1, (long)units);
    }

    private static void ValidateKind(MaintenanceOperationKind kind)
    {
        if (kind is MaintenanceOperationKind.Unknown || !Enum.IsDefined(kind))
            throw new ArgumentOutOfRangeException(nameof(kind));
    }

    private static void ValidateActivePhase(MaintenanceOperationPhase phase)
    {
        if (phase is MaintenanceOperationPhase.Unknown or
            MaintenanceOperationPhase.Completed ||
            !Enum.IsDefined(phase))
        {
            throw new ArgumentOutOfRangeException(nameof(phase));
        }
    }

    private static void ValidateCaptureLimit(int value, string parameterName)
    {
        if (value < 0 ||
            value > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    private static long SaturatingIncrement(long value)
        => value == long.MaxValue ? value : value + 1;

    private static long SaturatingAdd(long left, long right)
        => left >= long.MaxValue - right ? long.MaxValue : left + right;

    private static int SaturatingAdd(int left, int right)
        => left >= int.MaxValue - right ? int.MaxValue : left + right;

    private static void NotifyPhaseChanged(MaintenanceOperationPhase phase)
    {
        try
        {
            Volatile.Read(ref PhaseChangedForTests)?.Invoke(phase);
        }
        catch
        {
            // Test-only phase observation remains diagnostic and isolated.
        }
    }

    internal static Action<MaintenanceOperationPhase>? PhaseChangedForTests;

    private sealed class BoundedSelection<T>
    {
        private readonly int _maximumRecords;
        private readonly IComparer<T> _comparer;
        private readonly Func<T, string> _keySelector;
        private readonly HashSet<string> _selectedKeys =
            new(StringComparer.Ordinal);
        private readonly PriorityQueue<T, T> _records;

        internal BoundedSelection(
            int maximumRecords,
            IComparer<T> comparer,
            Func<T, string> keySelector)
        {
            _maximumRecords = maximumRecords;
            _comparer = comparer;
            _keySelector = keySelector;
            _records = new PriorityQueue<T, T>(
                new ReverseComparer<T>(comparer));
        }

        internal bool IsTruncated { get; private set; }

        internal void Add(T record)
        {
            string key = _keySelector(record);
            if (_selectedKeys.Contains(key))
            {
                IsTruncated = true;
                return;
            }

            if (_maximumRecords == 0)
            {
                IsTruncated = true;
                return;
            }

            if (_records.Count < _maximumRecords)
            {
                _records.Enqueue(record, record);
                _selectedKeys.Add(key);
                return;
            }

            IsTruncated = true;
            _records.TryPeek(out _, out T? worst);
            if (_comparer.Compare(record, worst!) >= 0)
                return;

            T removed = _records.Dequeue();
            _selectedKeys.Remove(_keySelector(removed));
            _records.Enqueue(record, record);
            _selectedKeys.Add(key);
        }

        internal T[] ToArray()
            => _records.UnorderedItems
                .Select(static item => item.Element)
                .OrderBy(static item => item, _comparer)
                .ToArray();
    }

    private sealed class ReverseComparer<T>(IComparer<T> inner) : IComparer<T>
    {
        public int Compare(T? left, T? right)
            => inner.Compare(right!, left!);
    }

    private sealed class ActiveCopyComparer : IComparer<ActiveCopy>
    {
        internal static ActiveCopyComparer Instance { get; } = new();

        public int Compare(ActiveCopy left, ActiveCopy right)
        {
            int result = left.Context.StartedAtUtc.CompareTo(
                right.Context.StartedAtUtc);
            return result != 0
                ? result
                : string.CompareOrdinal(
                    left.Context.OperationId.Value,
                    right.Context.OperationId.Value);
        }
    }

    private sealed class ActiveRecordComparer :
        IComparer<MaintenanceRuntimeRecord>
    {
        internal static ActiveRecordComparer Instance { get; } = new();

        public int Compare(
            MaintenanceRuntimeRecord left,
            MaintenanceRuntimeRecord right)
        {
            int result = left.Context.StartedAtUtc.CompareTo(
                right.Context.StartedAtUtc);
            return result != 0
                ? result
                : string.CompareOrdinal(
                    left.Context.OperationId.Value,
                    right.Context.OperationId.Value);
        }
    }

    private sealed class RecentRecordComparer :
        IComparer<MaintenanceRuntimeRecord>
    {
        internal static RecentRecordComparer Instance { get; } = new();

        public int Compare(
            MaintenanceRuntimeRecord left,
            MaintenanceRuntimeRecord right)
        {
            int result = right.Context.StartedAtUtc.CompareTo(
                left.Context.StartedAtUtc);
            return result != 0
                ? result
                : string.CompareOrdinal(
                    left.Context.OperationId.Value,
                    right.Context.OperationId.Value);
        }
    }

    internal sealed class MaintenanceRuntimeOperation
    {
        private readonly MaintenanceRuntimeDiagnostics _owner;
        private readonly OperationState _state;

        internal MaintenanceRuntimeOperation(
            MaintenanceRuntimeDiagnostics owner,
            OperationState state)
        {
            _owner = owner;
            _state = state;
        }

        internal CSharpDbOperationContext Context => _state.Context;

        internal IDisposable? EnterScope()
        {
            try
            {
                return CSharpDbOperationScope.Enter(Context);
            }
            catch
            {
                return null;
            }
        }

        internal void SetPhase(MaintenanceOperationPhase phase)
        {
            try
            {
                _owner.SetPhase(_state, phase);
            }
            catch
            {
                // Runtime diagnostics cannot alter maintenance execution.
            }
        }

        internal void SetProgress(
            long? completedUnits,
            long? totalUnits)
        {
            try
            {
                _owner.SetProgress(
                    _state,
                    completedUnits,
                    totalUnits);
            }
            catch
            {
                // Runtime diagnostics cannot alter maintenance execution.
            }
        }

        internal void Succeed(
            long? completedUnits = null,
            long? totalUnits = null,
            int warningCount = 0,
            int errorCount = 0)
            => Complete(
                CSharpDbOperationOutcome.Succeeded,
                completedUnits,
                totalUnits,
                warningCount,
                errorCount,
                error: null);

        internal void Reject(SafeErrorKind errorKind)
            => Complete(
                CSharpDbOperationOutcome.Rejected,
                completedUnits: null,
                totalUnits: null,
                warningCount: 0,
                errorCount: 1,
                SafeErrorProjector.Project(errorKind));

        internal void Fail(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            Complete(
                exception is OperationCanceledException
                    ? CSharpDbOperationOutcome.Canceled
                    : CSharpDbOperationOutcome.Failed,
                completedUnits: null,
                totalUnits: null,
                warningCount: 0,
                errorCount: 1,
                LifecycleOperation.ProjectError(exception));
        }

        private void Complete(
            CSharpDbOperationOutcome outcome,
            long? completedUnits,
            long? totalUnits,
            int warningCount,
            int errorCount,
            SafeErrorProjection? error)
        {
            try
            {
                _owner.Complete(
                    _state,
                    outcome,
                    completedUnits,
                    totalUnits,
                    warningCount,
                    errorCount,
                    error);
            }
            catch
            {
                // Runtime diagnostics cannot alter maintenance execution.
            }
        }
    }

    internal sealed class OperationState(
        CSharpDbOperationContext context,
        MaintenanceOperationKind kind,
        MaintenanceOperationPhase phase,
        bool registered)
    {
        internal CSharpDbOperationContext Context { get; } = context;
        internal MaintenanceOperationKind Kind { get; } = kind;
        internal MaintenanceOperationPhase Phase { get; set; } = phase;
        internal long? CompletedUnits { get; set; }
        internal long? TotalUnits { get; set; }
        internal bool Registered { get; set; } = registered;
        internal bool OwnershipHeld { get; set; } = true;
        internal bool Completed { get; set; }
    }

    private readonly record struct ActiveCopy(
        CSharpDbOperationContext Context,
        MaintenanceOperationKind Kind,
        MaintenanceOperationPhase Phase,
        long? CompletedUnits,
        long? TotalUnits);

    private sealed record RecentState(
        long? RecordedAtTimestamp,
        MaintenanceRuntimeRecord Record);
}

/// <summary>
/// One exactly-once terminal shared by the runtime registry and the typed
/// lifecycle event for a single maintenance operation context.
/// </summary>
internal sealed class MaintenanceObservation
{
    private readonly CSharpDbOperationContext _context;
    private readonly MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation?
        _runtimeOperation;
    private readonly LifecycleOperation? _lifecycleOperation;
    private readonly CSharpDbActivityOperation? _activityOperation;
    private int _completed;

    internal MaintenanceObservation(
        CSharpDbOperationContext context,
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation? runtimeOperation,
        LifecycleOperation? lifecycleOperation,
        CSharpDbActivityOperation? activityOperation = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        _activityOperation = activityOperation ??
            lifecycleOperation?.ActivityOperation;
        if (runtimeOperation is null &&
            lifecycleOperation is null &&
            _activityOperation is null)
        {
            throw new ArgumentException(
                "At least one maintenance observation sink is required.",
                nameof(runtimeOperation));
        }

        _context = context;
        _runtimeOperation = runtimeOperation;
        _lifecycleOperation = lifecycleOperation;
    }

    internal CSharpDbOperationContext Context => _context;

    internal IDisposable? EnterScope()
    {
        try
        {
            IDisposable operationScope = CSharpDbOperationScope.Enter(
                _context,
                _activityOperation);
            return _activityOperation?.WrapScope(operationScope) ??
                operationScope;
        }
        catch
        {
            return null;
        }
    }

    internal void SetPhase(MaintenanceOperationPhase phase)
    {
        if (Volatile.Read(ref _completed) != 0)
            return;

        _runtimeOperation?.SetPhase(phase);
    }

    internal void SetProgress(
        long? completedUnits,
        long? totalUnits)
    {
        if (Volatile.Read(ref _completed) != 0)
            return;

        _runtimeOperation?.SetProgress(completedUnits, totalUnits);
    }

    internal void Succeed(
        long? completedUnits = null,
        long? totalUnits = null,
        int warningCount = 0,
        int errorCount = 0)
    {
        if (!TryComplete())
            return;

        // Transfer out of Active before publishing a synchronous lifecycle
        // event so a listener's snapshot cannot observe stale active work.
        _runtimeOperation?.Succeed(
            completedUnits,
            totalUnits,
            warningCount,
            errorCount);
        _activityOperation?.CompleteMaintenance(
            CSharpDbOperationOutcome.Succeeded,
            error: null,
            completedUnits,
            totalUnits,
            warningCount,
            errorCount);
        _lifecycleOperation?.Succeed();
    }

    internal void Reject(SafeErrorKind errorKind)
    {
        if (!TryComplete())
            return;

        _runtimeOperation?.Reject(errorKind);
        _activityOperation?.CompleteMaintenance(
            CSharpDbOperationOutcome.Rejected,
            SafeErrorProjector.Project(errorKind),
            completedUnits: null,
            totalUnits: null,
            warningCount: 0,
            errorCount: 1);
        _lifecycleOperation?.Reject(errorKind);
    }

    internal void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (!TryComplete())
            return;

        _runtimeOperation?.Fail(exception);
        CSharpDbOperationOutcome outcome = exception is OperationCanceledException
            ? CSharpDbOperationOutcome.Canceled
            : CSharpDbOperationOutcome.Failed;
        _activityOperation?.CompleteMaintenance(
            outcome,
            LifecycleOperation.ProjectError(exception),
            completedUnits: null,
            totalUnits: null,
            warningCount: 0,
            errorCount: 1);
        _lifecycleOperation?.Fail(exception);
    }

    private bool TryComplete()
        => Interlocked.Exchange(ref _completed, 1) == 0;
}
