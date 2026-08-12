using System.Buffers.Binary;
using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Engine;

internal sealed partial class QueryRuntimeDiagnostics
{
    private const int LeanSlotFree = 0;
    private const int LeanSlotActive = 1;
    private const int LeanSlotPromoted = 2;
    private const int LeanMutationAvailable = 0;
    private const int LeanMutationHeld = 1;
    private const int LeanMutationsSealed = 2;

    private LeanActiveSlot[] _leanActiveSlots = null!;
    private int[] _leanFreeSlots = null!;
    private int _leanFreeCount;
    private LeanRecentSlot[] _leanRecentSlots = null!;
    private int _leanRecentHead;
    private int _leanRecentCount;
    private int _leanActiveCount;
    private long _leanIdNonce;
    private long _leanIdSequence;
    private long _recentSequence;
    private string _leanDatabaseAlias = null!;

    private void InitializeLeanRuntime()
    {
        _leanDatabaseAlias = _runtimeState.CreateOptionsSnapshot().DatabaseAlias;
        int activeCapacity = _historyEnabled ? _activeCapacity : 0;
        int recentCapacity = _historyEnabled ? _recentCapacity : 0;
        _leanActiveSlots = new LeanActiveSlot[activeCapacity];
        _leanFreeSlots = new int[activeCapacity];
        for (int index = 0; index < _leanActiveSlots.Length; index++)
        {
            _leanActiveSlots[index] = new LeanActiveSlot(index);
            _leanFreeSlots[index] = _leanActiveSlots.Length - index - 1;
        }

        _leanFreeCount = _leanFreeSlots.Length;
        _leanRecentSlots = new LeanRecentSlot[recentCapacity];
        for (int index = 0; index < _leanRecentSlots.Length; index++)
            _leanRecentSlots[index] = new LeanRecentSlot();

        Span<byte> nonce = stackalloc byte[16];
        Guid.NewGuid().TryWriteBytes(nonce);
        _leanIdNonce = BinaryPrimitives.ReadInt64LittleEndian(nonce);
    }

    private void DisposeLeanRuntimeLocked()
    {
        _leanActiveCount = 0;
        _leanFreeCount = _leanActiveSlots.Length;
        for (int index = 0; index < _leanActiveSlots.Length; index++)
        {
            _runtimeMetrics?.QueryAbandoned(
                _leanActiveSlots[index].TakeMetricsStarted());
            _leanActiveSlots[index].Reset();
            _leanFreeSlots[index] = _leanActiveSlots.Length - index - 1;
        }

        for (int index = 0; index < _leanRecentSlots.Length; index++)
            _leanRecentSlots[index].Reset();
        _leanRecentHead = 0;
        _leanRecentCount = 0;
    }

    internal IQueryExecutionObservation? TryStartLean(
        QueryFingerprint? fingerprint,
        CSharpDbTransport transport)
    {
        if (!_historyEnabled || Volatile.Read(ref _disposed) != 0)
            return null;

        DateTimeOffset startedAtUtc;
        long startingTimestamp;
        try
        {
            startedAtUtc = _runtimeState.TimeProvider.GetUtcNow();
            startingTimestamp = _runtimeState.TimeProvider.GetTimestamp();
        }
        catch
        {
            return null;
        }

        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0 ||
                _active.Count + _leanActiveCount >= _activeCapacity ||
                _leanFreeCount == 0 ||
                _leanIdSequence == long.MaxValue)
            {
                return null;
            }

            LeanActiveSlot slot;
            do
            {
                int slotIndex = _leanFreeSlots[--_leanFreeCount];
                slot = _leanActiveSlots[slotIndex];
                if (slot.Generation != long.MaxValue)
                    break;
            }
            while (_leanFreeCount > 0);

            if (slot.Generation == long.MaxValue)
                return null;

            long generation = slot.Generation + 1;
            long idSequence = ++_leanIdSequence;
            bool metricsStarted = _runtimeMetrics?.QueryStarted() == true;
            slot.Activate(
                generation,
                CreateLeanOperationId(_leanIdNonce, idSequence),
                startedAtUtc,
                startingTimestamp,
                fingerprint,
                transport,
                metricsStarted);
            _leanActiveCount++;
            return new LeanQueryExecutionObservation(this, slot, generation);
        }
    }

    private static Guid CreateLeanOperationId(long nonce, long sequence)
    {
        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, nonce);
        BinaryPrimitives.WriteInt64LittleEndian(bytes[8..], sequence);
        return new Guid(bytes);
    }

    private QueryRuntimeOperation PromoteLean(
        LeanActiveSlot slot,
        long generation)
    {
        lock (_gate)
        {
            if (!slot.IsActive(generation) || Volatile.Read(ref _disposed) != 0)
                throw new InvalidOperationException("The lean query activation is no longer active.");

            OpaqueDiagnosticsId operationId =
                OpaqueDiagnosticsId.Create(slot.OperationId);
            CSharpDbOperationContext context =
                CSharpDbOperationContext.CreateCapturedRoot(
                    operationId,
                    slot.Transport,
                    _leanDatabaseAlias,
                    slot.Fingerprint,
                    _runtimeState.TimeProvider,
                    slot.StartedAtUtc,
                    slot.StartingTimestamp);
            if (!context.TryClaimRuntimeDiagnostics(this))
                throw new InvalidOperationException("The promoted query identity is already claimed.");

            try
            {
                slot.SealMutations(generation);
                var operation = new QueryRuntimeOperation(
                    this,
                    context,
                    slot.Phase,
                    publicationClaim: null,
                    slot.Plan.Clone(),
                    suppressDiagnosticEvents: false,
                    detail: null,
                    registered: true);
                _active.Add(operationId, operation);
                operation.MetricsStarted = slot.TakeMetricsStarted();
                slot.State = LeanSlotPromoted;
                _leanActiveCount--;
                return operation;
            }
            catch
            {
                context.ReleaseRuntimeDiagnostics(this);
                if (slot.State == LeanSlotActive)
                    slot.UnsealMutations(generation);
                throw;
            }
        }
    }

    private void ReleasePromotedLeanSlot(LeanActiveSlot slot, long generation)
    {
        lock (_gate)
        {
            if (slot.Generation != generation || slot.State != LeanSlotPromoted)
                return;

            ReleaseLeanSlotLocked(slot);
        }
    }

    private void AbandonLean(LeanActiveSlot slot, long generation)
    {
        slot.SealMutations(generation);
        bool metricsStarted;
        lock (_gate)
        {
            if (!slot.IsActive(generation))
                return;

            _leanActiveCount--;
            metricsStarted = slot.TakeMetricsStarted();
            ReleaseLeanSlotLocked(slot);
        }

        _runtimeMetrics?.QueryAbandoned(metricsStarted);
    }

    private void CompleteLean(
        LeanActiveSlot slot,
        long generation,
        long? firstRowTimestamp,
        CSharpDbOperationOutcome outcome,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection? error,
        QueryExecutionPhase? terminalPhase = null)
    {
        long? recordedAtTimestamp = null;
        DateTimeOffset completedAtUtc = slot.StartedAtUtc;
        TimeSpan duration = TimeSpan.Zero;
        TimeSpan? timeToFirstResult = null;
        try
        {
            long terminalTimestamp = _runtimeState.TimeProvider.GetTimestamp();
            recordedAtTimestamp = terminalTimestamp;
            duration = NonNegative(_runtimeState.TimeProvider.GetElapsedTime(
                slot.StartingTimestamp,
                terminalTimestamp));
        }
        catch
        {
            duration = TimeSpan.Zero;
        }

        try
        {
            completedAtUtc = _runtimeState.TimeProvider.GetUtcNow();
        }
        catch
        {
            completedAtUtc = slot.StartedAtUtc;
        }

        if (firstRowTimestamp is long first)
        {
            try
            {
                timeToFirstResult = NonNegative(
                    _runtimeState.TimeProvider.GetElapsedTime(
                        slot.StartingTimestamp,
                        first));
                if (timeToFirstResult > duration)
                    timeToFirstResult = duration;
            }
            catch
            {
                timeToFirstResult = null;
            }
        }

        long safeRecordedAtTimestamp =
            recordedAtTimestamp ?? GetTimestampSafely();

        slot.SealMutations(generation, terminalPhase);
        bool metricsStarted;
        CSharpDbTransport metricTransport;
        long safeRowsProduced;
        long safeRowsAffected;
        bool isSlow;
        lock (_gate)
        {
            if (!slot.IsActive(generation) || Volatile.Read(ref _disposed) != 0)
                return;

            PruneExpired(safeRecordedAtTimestamp);
            EnsureRecentCapacityLocked();
            long sequence = NextRecentSequenceLocked();
            int tail = (_leanRecentHead + _leanRecentCount) % _leanRecentSlots.Length;
            safeRowsProduced = Math.Max(0, rowsProduced);
            safeRowsAffected = Math.Max(0, rowsAffected);
            _leanRecentSlots[tail].Capture(
                sequence,
                slot,
                completedAtUtc,
                duration,
                timeToFirstResult,
                outcome,
                safeRowsProduced,
                safeRowsAffected,
                error,
                safeRecordedAtTimestamp);
            _leanRecentCount++;

            _requestCount = SaturatingIncrement(_requestCount);
            _statementExecutionCount = SaturatingIncrement(_statementExecutionCount);
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

            isSlow = duration >= _slowQueryThreshold;
            if (isSlow)
                _slowCount = SaturatingIncrement(_slowCount);
            _rowsProduced = SaturatingAdd(_rowsProduced, safeRowsProduced);
            _rowsAffected = SaturatingAdd(_rowsAffected, safeRowsAffected);
            _leanActiveCount--;
            metricsStarted = slot.TakeMetricsStarted();
            metricTransport = slot.Transport;
            ReleaseLeanSlotLocked(slot);
        }

        _runtimeMetrics?.LeanQueryCompleted(
            metricsStarted,
            metricTransport,
            outcome,
            duration,
            safeRowsProduced,
            safeRowsAffected,
            isSlow);
    }

    private void ReleaseLeanSlotLocked(LeanActiveSlot slot)
    {
        slot.Reset();
        _leanFreeSlots[_leanFreeCount++] = slot.Index;
    }

    private long NextRecentSequenceLocked()
        => _recentSequence == long.MaxValue
            ? _recentSequence
            : ++_recentSequence;

    private void EnsureRecentCapacityLocked()
    {
        while (_recent.Count + _leanRecentCount >= _recentCapacity)
        {
            long fullSequence = _recent.TryPeek(out RecentQueryState full)
                ? full.Sequence
                : long.MaxValue;
            long leanSequence = _leanRecentCount > 0
                ? _leanRecentSlots[_leanRecentHead].Sequence
                : long.MaxValue;
            if (fullSequence <= leanSequence)
            {
                _recent.Dequeue();
            }
            else
            {
                _leanRecentSlots[_leanRecentHead].Reset();
                _leanRecentHead = (_leanRecentHead + 1) % _leanRecentSlots.Length;
                _leanRecentCount--;
            }

            _recentDroppedCount = SaturatingIncrement(_recentDroppedCount);
        }
    }

    private void PruneExpiredLean(long now)
    {
        while (_leanRecentCount > 0 &&
               IsExpired(_leanRecentSlots[_leanRecentHead].RecordedAtTimestamp, now))
        {
            _leanRecentSlots[_leanRecentHead].Reset();
            _leanRecentHead = (_leanRecentHead + 1) % _leanRecentSlots.Length;
            _leanRecentCount--;
            _recentDroppedCount = SaturatingIncrement(_recentDroppedCount);
        }
    }

    private bool TryCaptureLeanActivePlanLocked(
        OpaqueDiagnosticsId operationId,
        out QueryPlanCopy plan)
    {
        foreach (LeanActiveSlot slot in _leanActiveSlots)
        {
            if (slot.State != LeanSlotActive ||
                !operationId.Matches(slot.OperationId))
            {
                continue;
            }

            LeanActiveStateSnapshot active = slot.CaptureStateUnderRegistryGate();
            CSharpDbOperationContext context = CreateLeanContext(slot);
            plan = active.Plan.CreateCopy(context, actualRows: null);
            return true;
        }

        plan = default;
        return false;
    }

    private bool TryCaptureLeanRecentPlanLocked(
        OpaqueDiagnosticsId operationId,
        long minimumSequence,
        out QueryPlanCopy plan,
        out long sequence)
    {
        plan = default;
        sequence = minimumSequence;
        bool found = false;
        for (int offset = 0; offset < _leanRecentCount; offset++)
        {
            int index = (_leanRecentHead + offset) % _leanRecentSlots.Length;
            LeanRecentSlot slot = _leanRecentSlots[index];
            if (slot.Sequence <= sequence ||
                !operationId.Matches(slot.OperationId))
            {
                continue;
            }

            CSharpDbOperationContext context = CreateLeanContext(slot);
            plan = slot.Plan.CreateCopy(
                context,
                Math.Max(0, slot.RowsProduced > 0
                    ? slot.RowsProduced
                    : slot.RowsAffected));
            sequence = slot.Sequence;
            found = true;
        }

        return found;
    }

    private bool HasLeanActiveLocked(OpaqueDiagnosticsId operationId)
    {
        foreach (LeanActiveSlot slot in _leanActiveSlots)
        {
            if (slot.State == LeanSlotActive &&
                operationId.Matches(slot.OperationId))
            {
                return true;
            }
        }

        return false;
    }

    private bool HasNewerLeanRecentLocked(
        OpaqueDiagnosticsId operationId,
        long minimumSequence)
    {
        for (int offset = 0; offset < _leanRecentCount; offset++)
        {
            int index = (_leanRecentHead + offset) % _leanRecentSlots.Length;
            LeanRecentSlot slot = _leanRecentSlots[index];
            if (slot.Sequence > minimumSequence &&
                operationId.Matches(slot.OperationId))
            {
                return true;
            }
        }

        return false;
    }

    private CSharpDbOperationContext CreateLeanContext(LeanActiveSlot slot)
        => CSharpDbOperationContext.CreateCapturedRoot(
            OpaqueDiagnosticsId.Create(slot.OperationId),
            slot.Transport,
            _leanDatabaseAlias,
            slot.Fingerprint,
            _runtimeState.TimeProvider,
            slot.StartedAtUtc,
            slot.StartingTimestamp);

    private CSharpDbOperationContext CreateLeanContext(LeanRecentSlot slot)
        => CSharpDbOperationContext.CreateCapturedRoot(
            OpaqueDiagnosticsId.Create(slot.OperationId),
            slot.Transport,
            _leanDatabaseAlias,
            slot.Fingerprint,
            _runtimeState.TimeProvider,
            slot.StartedAtUtc,
            startingTimestamp: 0);

    private readonly record struct LeanActiveStateSnapshot(
        QueryExecutionPhase Phase,
        QueryPlanState Plan);

    private readonly record struct RecentQueryCapture(
        long Sequence,
        OpaqueDiagnosticsId OperationId,
        OpaqueDiagnosticsId? ParentOperationId,
        CSharpDbOperationClass OperationClass,
        CSharpDbOperationRole Role,
        DateTimeOffset StartedAtUtc,
        DateTimeOffset CompletedAtUtc,
        TimeSpan Duration,
        TimeSpan? TimeToFirstResult,
        CSharpDbOperationOutcome Outcome,
        QueryFingerprint? Fingerprint,
        CSharpDbTransport Transport,
        long RowsProduced,
        long RowsAffected,
        DiagnosticsTraceId? TraceId,
        OpaqueDiagnosticsId? SessionId,
        SafeErrorProjection? Error)
    {
        internal static RecentQueryCapture From(RecentQueryState recent)
            => new(
                recent.Sequence,
                recent.Context.OperationId,
                recent.Context.ParentOperationId,
                recent.Context.OperationClass,
                recent.Context.Role,
                recent.Context.StartedAtUtc,
                recent.CompletedAtUtc,
                recent.Duration,
                recent.TimeToFirstResult,
                recent.Outcome,
                recent.Context.QueryFingerprint,
                recent.Context.Transport,
                recent.RowsProduced,
                recent.RowsAffected,
                recent.Context.TraceId,
                recent.Context.SessionId,
                recent.Error);

        internal static RecentQueryCapture From(LeanRecentSlot recent)
            => new(
                recent.Sequence,
                OpaqueDiagnosticsId.Create(recent.OperationId),
                ParentOperationId: null,
                CSharpDbOperationClass.Query,
                CSharpDbOperationRole.Root,
                recent.StartedAtUtc,
                recent.CompletedAtUtc,
                recent.Duration,
                recent.TimeToFirstResult,
                recent.Outcome,
                recent.Fingerprint,
                recent.Transport,
                recent.RowsProduced,
                recent.RowsAffected,
                TraceId: null,
                SessionId: null,
                recent.Error);

        internal RecentQuerySnapshot CreateSnapshot(
            DiagnosticsSnapshotMetadata metadata)
            => new(
                metadata,
                OperationId,
                ParentOperationId,
                OperationClass,
                Role,
                StartedAtUtc,
                CompletedAtUtc,
                Duration,
                TimeToFirstResult,
                TimeToFirstResult is null
                    ? null
                    : NonNegative(Duration - TimeToFirstResult.Value),
                Outcome,
                Fingerprint,
                Transport,
                RowsProduced,
                RowsAffected,
                TraceId,
                SessionId,
                Error);
    }

    private sealed class LeanActiveSlot(int index)
    {
        internal int Index { get; } = index;
        internal long Generation;
        internal int State;
        internal int MutationState;
        internal Guid OperationId;
        internal DateTimeOffset StartedAtUtc;
        internal long StartingTimestamp;
        internal QueryFingerprint? Fingerprint;
        internal CSharpDbTransport Transport;
        internal QueryExecutionPhase Phase;
        internal QueryPlanState Plan;
        internal bool MetricsStarted;

        internal void Activate(
            long generation,
            Guid operationId,
            DateTimeOffset startedAtUtc,
            long startingTimestamp,
            QueryFingerprint? fingerprint,
            CSharpDbTransport transport,
            bool metricsStarted)
        {
            Generation = generation;
            OperationId = operationId;
            StartedAtUtc = startedAtUtc;
            StartingTimestamp = startingTimestamp;
            Fingerprint = fingerprint;
            Transport = transport;
            MetricsStarted = metricsStarted;
            Phase = QueryExecutionPhase.Planning;
            Plan = default;
            MutationState = LeanMutationAvailable;
            Volatile.Write(ref State, LeanSlotActive);
        }

        internal bool IsActive(long generation)
            => Generation == generation && Volatile.Read(ref State) == LeanSlotActive;

        internal bool TakeMetricsStarted()
        {
            bool started = MetricsStarted;
            MetricsStarted = false;
            return started;
        }

        internal bool TryEnterMutation(long generation)
        {
            var spinner = new SpinWait();
            while (Generation == generation &&
                   Volatile.Read(ref State) == LeanSlotActive)
            {
                int observed = Volatile.Read(ref MutationState);
                if (observed == LeanMutationsSealed)
                    return false;
                if (observed == LeanMutationAvailable &&
                    Interlocked.CompareExchange(
                        ref MutationState,
                        LeanMutationHeld,
                        LeanMutationAvailable) == LeanMutationAvailable)
                {
                    if (IsActive(generation))
                        return true;

                    Interlocked.CompareExchange(
                        ref MutationState,
                        LeanMutationAvailable,
                        LeanMutationHeld);
                    return false;
                }

                spinner.SpinOnce();
            }

            return false;
        }

        /// <summary>
        /// Captures a coherent active state while the registry gate prevents
        /// identity reuse. A sealed slot is immutable and can be copied
        /// directly; a held slot belongs to a writer that never takes the
        /// registry gate, so waiting only for that writer is deadlock-free.
        /// </summary>
        internal LeanActiveStateSnapshot CaptureStateUnderRegistryGate()
        {
            var spinner = new SpinWait();
            while (true)
            {
                int observed = Volatile.Read(ref MutationState);
                if (observed == LeanMutationsSealed)
                    return new LeanActiveStateSnapshot(Phase, Plan.Clone());
                if (observed == LeanMutationAvailable &&
                    Interlocked.CompareExchange(
                        ref MutationState,
                        LeanMutationHeld,
                        LeanMutationAvailable) == LeanMutationAvailable)
                {
                    try
                    {
                        return new LeanActiveStateSnapshot(Phase, Plan.Clone());
                    }
                    finally
                    {
                        ExitMutation();
                    }
                }

                spinner.SpinOnce();
            }
        }

        internal void ExitMutation()
            => Interlocked.CompareExchange(
                ref MutationState,
                LeanMutationAvailable,
                LeanMutationHeld);

        internal void SealMutations(
            long generation,
            QueryExecutionPhase? terminalPhase = null)
        {
            var spinner = new SpinWait();
            while (Generation == generation)
            {
                int state = Volatile.Read(ref MutationState);
                if (state == LeanMutationsSealed)
                    return;
                if (state == LeanMutationAvailable)
                {
                    if (terminalPhase is not QueryExecutionPhase phase)
                    {
                        if (Interlocked.CompareExchange(
                                ref MutationState,
                                LeanMutationsSealed,
                                LeanMutationAvailable) == LeanMutationAvailable)
                        {
                            return;
                        }
                    }
                    else if (Interlocked.CompareExchange(
                                 ref MutationState,
                                 LeanMutationHeld,
                                 LeanMutationAvailable) == LeanMutationAvailable)
                    {
                        if (Generation == generation &&
                            Volatile.Read(ref State) == LeanSlotActive &&
                            phase > Phase)
                        {
                            Phase = phase;
                        }

                        Volatile.Write(ref MutationState, LeanMutationsSealed);
                        return;
                    }
                }

                spinner.SpinOnce();
            }
        }

        internal void UnsealMutations(long generation)
        {
            if (Generation == generation && State == LeanSlotActive)
                Volatile.Write(ref MutationState, LeanMutationAvailable);
        }

        internal void Reset()
        {
            Volatile.Write(ref State, LeanSlotFree);
            MutationState = LeanMutationsSealed;
            Fingerprint = null;
            MetricsStarted = false;
            Phase = QueryExecutionPhase.Completed;
            Plan = default;
        }
    }

    private sealed class LeanRecentSlot
    {
        internal long Sequence;
        internal Guid OperationId;
        internal DateTimeOffset StartedAtUtc;
        internal DateTimeOffset CompletedAtUtc;
        internal TimeSpan Duration;
        internal TimeSpan? TimeToFirstResult;
        internal CSharpDbOperationOutcome Outcome;
        internal QueryFingerprint? Fingerprint;
        internal CSharpDbTransport Transport;
        internal long RowsProduced;
        internal long RowsAffected;
        internal SafeErrorProjection? Error;
        internal long RecordedAtTimestamp;
        internal QueryPlanState Plan;

        internal void Capture(
            long sequence,
            LeanActiveSlot active,
            DateTimeOffset completedAtUtc,
            TimeSpan duration,
            TimeSpan? timeToFirstResult,
            CSharpDbOperationOutcome outcome,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error,
            long recordedAtTimestamp)
        {
            Sequence = sequence;
            OperationId = active.OperationId;
            StartedAtUtc = active.StartedAtUtc;
            CompletedAtUtc = completedAtUtc;
            Duration = duration;
            TimeToFirstResult = timeToFirstResult;
            Outcome = outcome;
            Fingerprint = active.Fingerprint;
            Transport = active.Transport;
            RowsProduced = rowsProduced;
            RowsAffected = rowsAffected;
            Error = error;
            RecordedAtTimestamp = recordedAtTimestamp;
            Plan = active.Plan.Clone();
        }

        internal void Reset()
        {
            Sequence = 0;
            Fingerprint = null;
            Error = null;
            Plan = default;
        }
    }

    private sealed class LeanQueryExecutionObservation :
        IQueryExecutionObservation,
        IQueryPlanRuntimeObserver,
        IQueryResultDirectLifecycleRegistration
    {
        private const long CompletionReservedFlag = long.MinValue;
        private const long CompletionReadyFlag = 1L << 62;
        private const long FirstRowCallbackInProgressFlag = 1L << 61;
        private const long DisposeCallbackInProgressFlag = 1L << 60;
        private const int CompletionReasonShift = 58;
        private const long CompletionReasonMask = 3L << CompletionReasonShift;
        private const long RowCountMask = (1L << CompletionReasonShift) - 1;
        private const long PreTerminalCallbackMask =
            FirstRowCallbackInProgressFlag |
            DisposeCallbackInProgressFlag;
        private const int FirstRowAbsent = 0;
        private const int FirstRowWriterHeld = 1;
        private const int FirstRowCaptured = 2;
        private const int ResultDisposeStarted = 1 << 0;
        private const int LeanLifecycleCommitted = 1 << 1;
        private const int LeanLifecycleInstallReserved = 1 << 2;
        private const int LeanPromotionReserved = 1 << 3;
        private readonly QueryRuntimeDiagnostics _owner;
        private readonly LeanActiveSlot _slot;
        private readonly long _generation;
        private QueryRuntimeOperation? _promoted;
        private Exception? _completionError;
        private long _resultLifecycleState;
        private long _firstRowTimestamp;
        private int _resultState;
        private int _firstRowState;
        private int _completed;

        internal LeanQueryExecutionObservation(
            QueryRuntimeDiagnostics owner,
            LeanActiveSlot slot,
            long generation)
        {
            _owner = owner;
            _slot = slot;
            _generation = generation;
        }

        public IQueryPlanRuntimeObserver ExplicitPlanObserver => this;

        public IDisposable EnterScope()
        {
            if (Volatile.Read(ref _completed) != 0)
                return NoopScope.Instance;

            try
            {
                return EnsurePromoted().EnterScope();
            }
            catch
            {
                DisableAfterPromotionFailure();
                return NoopScope.Instance;
            }
        }

        public void MarkExecuting()
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
            {
                try
                {
                    promoted.MarkExecuting();
                }
                catch
                {
                    DisableAfterPromotionFailure();
                }
            }
            else
                SetPhase(QueryExecutionPhase.Executing);
        }

        public QueryResult Observe(QueryResult result)
        {
            ArgumentNullException.ThrowIfNull(result);
            if (Volatile.Read(ref _completed) != 0)
                return result;

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
            {
                try
                {
                    return promoted.Observe(result);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                    return result;
                }
            }

            if (!result.IsQuery)
            {
                CompleteActivation(
                    CSharpDbOperationOutcome.Succeeded,
                    rowsProduced: 0,
                    result.RowsAffected,
                    error: null);
                return result;
            }

            if (result.RequiresRuntimeExecutionScope)
            {
                try
                {
                    return EnsurePromoted().Observe(result);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                    return result;
                }
            }

            SetPhase(QueryExecutionPhase.Streaming);
            if (!TryReserveLifecycleInstall())
            {
                try
                {
                    return EnsurePromoted().Observe(result);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                    return result;
                }
            }

            QueryResultDirectLifecycleInstallResult installResult =
                result.TrySetDirectLifecycleRegistration(this);
            if (installResult == QueryResultDirectLifecycleInstallResult.Installed)
            {
                CommitLifecycleInstall();
                return result;
            }

            ReleaseLifecycleInstallReservation();
            if (installResult == QueryResultDirectLifecycleInstallResult.NeedsPromotion)
            {
                try
                {
                    return EnsurePromoted().Observe(result);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                    return result;
                }
            }

            DisableAfterPromotionFailure();
            return result;
        }

        private bool TryReserveLifecycleInstall()
        {
            while (true)
            {
                int observed = Volatile.Read(ref _resultState);
                if ((observed & (
                        LeanLifecycleCommitted |
                        LeanLifecycleInstallReserved |
                        LeanPromotionReserved)) != 0)
                {
                    return false;
                }

                if (Interlocked.CompareExchange(
                        ref _resultState,
                        observed | LeanLifecycleInstallReserved,
                        observed) == observed)
                {
                    return true;
                }
            }
        }

        private void CommitLifecycleInstall()
        {
            while (true)
            {
                int observed = Volatile.Read(ref _resultState);
                int updated =
                    (observed & ~LeanLifecycleInstallReserved) |
                    LeanLifecycleCommitted;
                if (Interlocked.CompareExchange(
                        ref _resultState,
                        updated,
                        observed) == observed)
                {
                    return;
                }
            }
        }

        private void ReleaseLifecycleInstallReservation()
            => Interlocked.And(
                ref _resultState,
                ~LeanLifecycleInstallReserved);

        public void Fail(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            if (Volatile.Read(ref _completed) != 0)
                return;

            try
            {
                CompleteResultLifecycle(
                    exception is OperationCanceledException
                        ? QueryResultCompletionReason.Canceled
                        : QueryResultCompletionReason.Failed,
                    exception,
                    authoritativeRowsProduced: null);
            }
            catch
            {
                DisableAfterPromotionFailure();
            }
        }

        public void OnFirstRowProduced()
            => RecordRowProduced();

        public void OnDisposing()
        {
            try
            {
                SetPhase(QueryExecutionPhase.Disposing);
            }
            catch
            {
                // Direct diagnostics callbacks are fail-closed.
            }
        }

        public void OnCompleted(QueryResultCompletion completion)
        {
            CompleteResultLifecycle(
                completion.Reason,
                completion.Error,
                completion.RowsProduced);
        }

        bool IQueryResultLifecycleRegistration.HasLifecycleStarted =>
            (Volatile.Read(ref _resultLifecycleState) &
                (CompletionReservedFlag |
                 PreTerminalCallbackMask |
                 RowCountMask)) != 0 ||
            (Volatile.Read(ref _resultState) & ResultDisposeStarted) != 0;

        bool IQueryResultDirectLifecycleRegistration.IsDirectLifecycleCommitted =>
            (Volatile.Read(ref _resultState) & LeanLifecycleCommitted) != 0;

        bool IQueryResultLifecycleRegistration.TryStartDisposal()
            => TryStartResultDisposal();

        void IQueryResultLifecycleRegistration.OnRowProduced()
            => RecordRowProduced();

        void IQueryResultLifecycleRegistration.Complete(
            QueryResultCompletionReason reason,
            Exception? error)
            => CompleteResultLifecycle(
                reason,
                error,
                authoritativeRowsProduced: null);

        void IQueryResultDirectLifecycleRegistration.CompleteSynchronousResult(
            QueryResultCompletionReason reason,
            long rowsProduced)
        {
            CompleteResultLifecycle(
                reason,
                error: null,
                authoritativeRowsProduced: rowsProduced);
        }

        private void RecordRowProduced()
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            long observed;
            while (true)
            {
                observed = Volatile.Read(ref _resultLifecycleState);
                if ((observed & CompletionReservedFlag) != 0)
                    return;

                long rowsProduced = observed & RowCountMask;
                long updated = rowsProduced == RowCountMask
                    ? observed
                    : observed + 1;
                if (rowsProduced == 0)
                    updated |= FirstRowCallbackInProgressFlag;
                if (Interlocked.CompareExchange(
                        ref _resultLifecycleState,
                        updated,
                        observed) == observed)
                {
                    break;
                }
            }

            if ((observed & RowCountMask) != 0)
                return;

            try
            {
                CaptureFirstRowTimestamp();
            }
            finally
            {
                ClearPreTerminalCallback(FirstRowCallbackInProgressFlag);
            }
        }

        private void CaptureFirstRowTimestamp()
        {
            if (Interlocked.CompareExchange(
                    ref _firstRowState,
                    FirstRowWriterHeld,
                    FirstRowAbsent) != FirstRowAbsent)
            {
                return;
            }

            try
            {
                long timestamp = _owner._runtimeState.TimeProvider.GetTimestamp();
                Volatile.Write(ref _firstRowTimestamp, timestamp);
                Volatile.Write(ref _firstRowState, FirstRowCaptured);
            }
            catch
            {
                Volatile.Write(ref _firstRowState, FirstRowAbsent);
            }
        }

        private bool TryStartResultDisposal()
        {
            int resultState;
            while (true)
            {
                resultState = Volatile.Read(ref _resultState);
                if ((resultState & ResultDisposeStarted) != 0)
                    return false;
                if (Interlocked.CompareExchange(
                        ref _resultState,
                        resultState | ResultDisposeStarted,
                        resultState) == resultState)
                {
                    break;
                }
            }

            if (Volatile.Read(ref _completed) != 0)
                return true;

            while (true)
            {
                long observed = Volatile.Read(ref _resultLifecycleState);
                if ((observed & CompletionReservedFlag) != 0)
                    return true;
                if (Interlocked.CompareExchange(
                        ref _resultLifecycleState,
                        observed | DisposeCallbackInProgressFlag,
                        observed) == observed)
                {
                    break;
                }
            }

            try
            {
                try
                {
                    SetPhase(QueryExecutionPhase.Disposing);
                }
                catch
                {
                    // Direct diagnostics callbacks are fail-closed.
                }
            }
            finally
            {
                ClearPreTerminalCallback(DisposeCallbackInProgressFlag);
            }

            return true;
        }

        private void CompleteResultLifecycle(
            QueryResultCompletionReason reason,
            Exception? error,
            long? authoritativeRowsProduced)
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            long observed;
            while (true)
            {
                observed = Volatile.Read(ref _resultLifecycleState);
                if ((observed & CompletionReservedFlag) != 0)
                    return;

                long rowsProduced = authoritativeRowsProduced is long authoritative
                    ? Math.Min(Math.Max(authoritative, 0), RowCountMask)
                    : observed & RowCountMask;
                long updated =
                    (observed & ~(CompletionReasonMask | RowCountMask)) |
                    CompletionReservedFlag |
                    ((long)reason << CompletionReasonShift) |
                    rowsProduced;
                if (Interlocked.CompareExchange(
                        ref _resultLifecycleState,
                        updated,
                        observed) == observed)
                {
                    break;
                }
            }

            if ((observed & PreTerminalCallbackMask) != 0)
                Volatile.Write(ref _completionError, error);

            long beforeCompletionReady = Interlocked.Or(
                ref _resultLifecycleState,
                CompletionReadyFlag);
            if ((beforeCompletionReady & PreTerminalCallbackMask) == 0)
            {
                Interlocked.Exchange(ref _completionError, null);
                InvokeResultTerminal(
                    beforeCompletionReady | CompletionReadyFlag,
                    error);
            }
        }

        private void ClearPreTerminalCallback(long callbackFlag)
        {
            long beforeClear = Interlocked.And(
                ref _resultLifecycleState,
                ~callbackFlag);
            long afterClear = beforeClear & ~callbackFlag;
            if ((beforeClear & CompletionReadyFlag) == 0 ||
                (afterClear & PreTerminalCallbackMask) != 0)
            {
                return;
            }

            Exception? error = Interlocked.Exchange(ref _completionError, null);
            InvokeResultTerminal(afterClear, error);
        }

        private void InvokeResultTerminal(
            long completionState,
            Exception? completionError)
        {
            try
            {
                QueryResultCompletionReason reason =
                    (QueryResultCompletionReason)(
                        (completionState & CompletionReasonMask) >>
                        CompletionReasonShift);
                CSharpDbOperationOutcome outcome = reason switch
                {
                    QueryResultCompletionReason.Exhausted or
                    QueryResultCompletionReason.Disposed =>
                        CSharpDbOperationOutcome.Succeeded,
                    QueryResultCompletionReason.Canceled =>
                        CSharpDbOperationOutcome.Canceled,
                    _ => CSharpDbOperationOutcome.Failed,
                };
                SafeErrorProjection? error = outcome == CSharpDbOperationOutcome.Succeeded
                    ? null
                    : QueryOperation.ProjectError(
                        completionError ??
                        (outcome == CSharpDbOperationOutcome.Canceled
                            ? new OperationCanceledException()
                            : new InvalidOperationException()));
                long? firstRowTimestamp =
                    Volatile.Read(ref _firstRowState) == FirstRowCaptured
                        ? Volatile.Read(ref _firstRowTimestamp)
                        : null;
                CompleteActivation(
                    outcome,
                    completionState & RowCountMask,
                    rowsAffected: 0,
                    error,
                    firstRowTimestamp,
                    reason == QueryResultCompletionReason.Disposed
                        ? QueryExecutionPhase.Disposing
                        : null);
            }
            catch
            {
                DisableAfterPromotionFailure();
            }
        }

        public void OnPlanCacheLookup(bool hit)
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
            {
                try
                {
                    promoted.OnPlanCacheLookup(hit);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                }
            }
            else
                MutatePlan(static (slot, state) => slot.Plan.RecordPlanCacheLookup(state), hit);
        }

        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection)
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
            {
                try
                {
                    promoted.OnAccessPathSelected(in selection);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                }
                return;
            }

            if (_slot.TryEnterMutation(_generation))
            {
                try
                {
                    _slot.Plan.RecordAccessPath(
                        selection.AccessPath,
                        selection.EstimatedRows);
                }
                finally
                {
                    _slot.ExitMutation();
                }
            }
        }

        public void OnPlanChanged(QueryPlanChangeKind change)
        {
            if (Volatile.Read(ref _completed) != 0)
                return;

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
            {
                try
                {
                    promoted.OnPlanChanged(change);
                }
                catch
                {
                    DisableAfterPromotionFailure();
                }
            }
            else
                MutatePlan(static (slot, state) => slot.Plan.RecordPlanChange(state), change);
        }

        private void MutatePlan<TState>(Action<LeanActiveSlot, TState> mutation, TState state)
        {
            if (!_slot.TryEnterMutation(_generation))
                return;

            try
            {
                mutation(_slot, state);
            }
            finally
            {
                _slot.ExitMutation();
            }
        }

        private void SetPhase(QueryExecutionPhase phase)
        {
            if (!_slot.TryEnterMutation(_generation))
                return;

            try
            {
                if (phase > _slot.Phase)
                    _slot.Phase = phase;
            }
            finally
            {
                _slot.ExitMutation();
            }
        }

        private QueryRuntimeOperation EnsurePromoted()
        {
            if (Volatile.Read(ref _completed) != 0 ||
                !TryReservePromotion())
            {
                throw new InvalidOperationException("Lean query promotion is unavailable.");
            }

            QueryRuntimeOperation? promoted = Volatile.Read(ref _promoted);
            if (promoted is not null)
                return promoted;

            lock (this)
            {
                if (Volatile.Read(ref _completed) != 0)
                {
                    throw new InvalidOperationException("Lean query promotion is unavailable.");
                }

                promoted = _promoted;
                if (promoted is not null)
                    return promoted;

                promoted = _owner.PromoteLean(_slot, _generation);
                Volatile.Write(ref _promoted, promoted);
                _owner.ReleasePromotedLeanSlot(_slot, _generation);
                return promoted;
            }
        }

        private bool TryReservePromotion()
        {
            while (true)
            {
                int observed = Volatile.Read(ref _resultState);
                if ((observed & (
                        LeanLifecycleCommitted |
                        LeanLifecycleInstallReserved)) != 0)
                {
                    return false;
                }
                if ((observed & LeanPromotionReserved) != 0)
                    return true;

                if (Interlocked.CompareExchange(
                        ref _resultState,
                        observed | LeanPromotionReserved,
                        observed) == observed)
                {
                    return true;
                }
            }
        }

        private void DisableAfterPromotionFailure()
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
                return;

            QueryRuntimeOperation? promoted;
            lock (this)
                promoted = _promoted;

            if (promoted is not null)
            {
                try
                {
                    promoted.Abandon();
                }
                catch
                {
                    // Diagnostics cleanup is best effort and cannot replace the
                    // query's own result or exception.
                }

                return;
            }

            try
            {
                _owner.AbandonLean(_slot, _generation);
            }
            catch
            {
                // Diagnostics cleanup is best effort and cannot replace the
                // query's own result or exception.
            }
        }

        private void CompleteActivation(
            CSharpDbOperationOutcome outcome,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error,
            long? firstRowTimestamp = null,
            QueryExecutionPhase? terminalPhase = null)
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
                return;

            QueryRuntimeOperation? promoted = null;
            if ((Volatile.Read(ref _resultState) & LeanLifecycleCommitted) == 0)
            {
                lock (this)
                    promoted = _promoted;
            }
            if (promoted is not null)
            {
                try
                {
                    promoted.CompleteFromObservation(
                        outcome,
                        rowsProduced,
                        rowsAffected,
                        error);
                }
                catch
                {
                    try
                    {
                        promoted.Abandon();
                    }
                    catch
                    {
                    }
                }
                return;
            }

            try
            {
                _owner.CompleteLean(
                    _slot,
                    _generation,
                    firstRowTimestamp,
                    outcome,
                    rowsProduced,
                    rowsAffected,
                    error,
                    terminalPhase);
            }
            catch
            {
                try
                {
                    _owner.AbandonLean(_slot, _generation);
                }
                catch
                {
                }
            }
        }

        private sealed class NoopScope : IDisposable
        {
            internal static NoopScope Instance { get; } = new();

            public void Dispose()
            {
            }
        }
    }
}
