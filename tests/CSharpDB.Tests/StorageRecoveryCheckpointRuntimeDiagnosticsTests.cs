using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class StorageRecoveryCheckpointRuntimeDiagnosticsTests
{
    [Fact]
    public void RecoveryBeforeProviderPromotion_IsVisibleThroughWalOnly()
    {
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-recovery-before-database");
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        using StorageRuntimeDiagnostics.Registration registration =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true));

        registration.Observer.OnRecoveryStarted();
        StorageRuntimeDiagnosticsCapture capture = Capture(state);

        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            capture.Storage.Availability);
        Assert.Equal(DiagnosticsAvailability.Available, capture.Wal.Availability);
        WalRuntimeDiagnosticsSnapshot wal = capture.Wal.Value!;
        Assert.Null(wal.LogicalBytes);
        Assert.Equal(DiagnosticsAvailability.Available, wal.Recovery.Availability);
        Assert.Equal(WalRecoveryPhase.Scanning, wal.Recovery.Value!.Phase);
        Assert.Equal(CSharpDbOperationOutcome.Unknown, wal.Recovery.Value.Outcome);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            wal.Checkpoint.Availability);
    }

    [Fact]
    public void ReversedRecoveryCompletionDelivery_RetainsLatestCompletion()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 11, 30, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-recovery-terminal-order");
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using StorageRuntimeDiagnostics.Registration first =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true));
        using StorageRuntimeDiagnostics.Registration second =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true));

        first.Observer.OnRecoveryStarted();
        clock.Advance(TimeSpan.FromSeconds(1));
        second.Observer.OnRecoveryStarted();
        StorageRuntimeDiagnostics.RecoveryOperation secondOperation =
            Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                second.Recovery);

        clock.Advance(TimeSpan.FromSeconds(10));
        second.Observer.OnRecoveryCompleted(RecoveryRaw(scannedFrameCount: 2));
        clock.Advance(TimeSpan.FromSeconds(-1));
        first.Observer.OnRecoveryCompleted(RecoveryRaw(scannedFrameCount: 1));

        WalRecoveryDiagnosticsSnapshot recovery =
            Capture(state).Wal.Value!.Recovery.Value!;
        Assert.Equal(secondOperation.OperationId, recovery.OperationId);
        Assert.Equal(secondOperation.CompletedAtUtc, recovery.CompletedAtUtc);
        Assert.Equal(2, recovery.ScannedFrameCount);
    }

    [Fact]
    public void TwoActiveRecoveries_SelectOldestThenLowerSortKeyAndBeatTerminal()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 11, 45, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-recovery-multi-active");
        using (var state = new CSharpDbRuntimeDiagnosticsState(options, clock))
        using (StorageRuntimeDiagnostics.Registration terminal =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true)))
        using (StorageRuntimeDiagnostics.Registration oldest =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true)))
        using (StorageRuntimeDiagnostics.Registration newer =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true)))
        {
            terminal.Observer.OnRecoveryStarted();
            terminal.Observer.OnRecoveryCompleted(
                RecoveryRaw(scannedFrameCount: 99));
            StorageRuntimeDiagnostics.RecoveryOperation terminalOperation =
                Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                    terminal.Recovery);

            clock.Advance(TimeSpan.FromSeconds(1));
            oldest.Observer.OnRecoveryStarted();
            StorageRuntimeDiagnostics.RecoveryOperation oldestOperation =
                Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                    oldest.Recovery);
            clock.Advance(TimeSpan.FromSeconds(1));
            newer.Observer.OnRecoveryStarted();

            StorageRuntimeDiagnosticsCapture capture = Capture(state);
            WalRecoveryDiagnosticsSnapshot recovery =
                capture.Wal.Value!.Recovery.Value!;
            Assert.True(capture.FieldsTruncated);
            Assert.True(recovery.Metadata.FieldsTruncated);
            Assert.Equal(oldestOperation.OperationId, recovery.OperationId);
            Assert.NotEqual(terminalOperation.OperationId, recovery.OperationId);
        }

        var tieClock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 11, 50, 0, TimeSpan.Zero));
        using var tieState = new CSharpDbRuntimeDiagnosticsState(options, tieClock);
        using StorageRuntimeDiagnostics.Registration first =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    tieState,
                    recoveryApplicable: true));
        using StorageRuntimeDiagnostics.Registration second =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    tieState,
                    recoveryApplicable: true));
        first.Observer.OnRecoveryStarted();
        second.Observer.OnRecoveryStarted();
        StorageRuntimeDiagnostics.RecoveryOperation firstOperation =
            Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                first.Recovery);
        StorageRuntimeDiagnostics.RecoveryOperation secondOperation =
            Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                second.Recovery);
        StorageRuntimeDiagnostics.RecoveryOperation lowerSortKey =
            string.CompareOrdinal(firstOperation.SortKey, secondOperation.SortKey) < 0
                ? firstOperation
                : secondOperation;

        StorageRuntimeDiagnosticsCapture tieCapture = Capture(tieState);
        WalRecoveryDiagnosticsSnapshot tieRecovery =
            tieCapture.Wal.Value!.Recovery.Value!;
        Assert.True(tieCapture.FieldsTruncated);
        Assert.True(tieRecovery.Metadata.FieldsTruncated);
        Assert.Equal(lowerSortKey.OperationId, tieRecovery.OperationId);
    }

    [Fact]
    public void TwoActiveCheckpoints_SelectHighestPhaseAndDiscloseTruncation()
    {
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-checkpoint-multi-active");
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        using StorageRuntimeDiagnostics.Registration first =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));
        using StorageRuntimeDiagnostics.Registration second =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));

        first.Observer.OnCheckpointStarted(
            CheckpointRaw(StorageCheckpointPhaseRaw.Copying),
            correlation: null);
        second.Observer.OnCheckpointStarted(
            CheckpointRaw(StorageCheckpointPhaseRaw.Finalizing),
            correlation: null);

        StorageRuntimeDiagnosticsCapture capture = Capture(state);

        Assert.True(capture.FieldsTruncated);
        CheckpointDiagnosticsSnapshot checkpoint =
            capture.Wal.Value!.Checkpoint.Value!;
        Assert.True(checkpoint.Metadata.FieldsTruncated);
        Assert.Equal(2, checkpoint.ActiveCount);
        Assert.Equal(2, checkpoint.AttemptCount);
        Assert.Equal(CheckpointPhase.Finalizing, checkpoint.Phase);
        Assert.Equal(checkpoint.Phase, capture.Wal.Value.CheckpointPhase);
        Assert.NotNull(checkpoint.OperationId);
    }

    [Fact]
    public void TerminalClockFailure_DoesNotLeaveCheckpointActive()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 12, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-checkpoint-clock-failure");
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using StorageRuntimeDiagnostics.Registration registration =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));

        registration.Observer.OnCheckpointStarted(
            CheckpointRaw(StorageCheckpointPhaseRaw.Copying),
            correlation: null);
        clock.Throw = true;
        registration.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded),
            correlation: null);

        StorageRuntimeDiagnosticsCapture capture = Capture(state);
        CheckpointDiagnosticsSnapshot checkpoint =
            capture.Wal.Value!.Checkpoint.Value!;
        Assert.Equal(0, checkpoint.ActiveCount);
        Assert.Equal(1, checkpoint.AttemptCount);
        Assert.Equal(1, checkpoint.SuccessCount);
        Assert.Equal(CheckpointPhase.Idle, checkpoint.Phase);
        Assert.NotNull(checkpoint.LastSuccessfulAtUtc);
        Assert.Equal(TimeSpan.Zero, checkpoint.LastElapsed);
    }

    [Fact]
    public void ProducerCapturedCheckpointStart_PreservesWorkElapsedTime()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 12, 15, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-checkpoint-producer-time");
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using StorageRuntimeDiagnostics.Registration registration =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));

        object? correlation = registration.Observer
            .CaptureCheckpointCorrelation(StorageCheckpointOriginRaw.Manual);
        Assert.NotNull(correlation);
        clock.Advance(TimeSpan.FromSeconds(2));
        registration.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                origin: StorageCheckpointOriginRaw.Manual),
            correlation);
        clock.Advance(TimeSpan.FromSeconds(3));
        object? completedCorrelation = registration.Observer
            .CaptureCheckpointCompletionCorrelation();
        DateTimeOffset completedAtUtc = clock.GetUtcNow();
        clock.Advance(TimeSpan.FromSeconds(7));
        registration.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                StorageCheckpointOriginRaw.Manual),
            completedCorrelation);

        CheckpointDiagnosticsSnapshot checkpoint =
            Capture(state).Wal.Value!.Checkpoint.Value!;
        Assert.Equal(TimeSpan.FromSeconds(5), checkpoint.LastElapsed);
        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 12, 15, 0, TimeSpan.Zero),
            checkpoint.LastStartedAtUtc);
        Assert.Equal(completedAtUtc, checkpoint.LastSuccessfulAtUtc);
    }

    [Fact]
    public void OverlappingCheckpointCompletions_KeepOneCoherentTerminalUnit()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 12, 20, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateOptions(
            "storage-checkpoint-overlap-terminal");
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using StorageRuntimeDiagnostics.Registration first =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));
        using StorageRuntimeDiagnostics.Registration second =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));

        first.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                origin: StorageCheckpointOriginRaw.Manual),
            correlation: null);
        StorageRuntimeDiagnostics.CheckpointOperation firstOperation =
            Assert.IsType<StorageRuntimeDiagnostics.CheckpointOperation>(
                first.Checkpoint);
        clock.Advance(TimeSpan.FromSeconds(1));
        second.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Finalizing,
                origin: StorageCheckpointOriginRaw.Backup),
            correlation: null);

        second.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                StorageCheckpointOriginRaw.Backup),
            correlation: null);
        clock.Advance(TimeSpan.FromSeconds(1));
        first.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Faulted,
                StorageRuntimeOperationOutcomeRaw.Failed,
                StorageCheckpointOriginRaw.Manual,
                StorageRuntimeFailureKindRaw.Io),
            correlation: null);

        CheckpointDiagnosticsSnapshot checkpoint =
            Capture(state).Wal.Value!.Checkpoint.Value!;
        Assert.Equal(CheckpointPhase.Faulted, checkpoint.Phase);
        Assert.Equal(CheckpointOrigin.Manual, checkpoint.Origin);
        Assert.Equal(firstOperation.StartedAtUtc, checkpoint.LastStartedAtUtc);
        Assert.Equal(firstOperation.Elapsed, checkpoint.LastElapsed);
        Assert.Equal(2, checkpoint.AttemptCount);
        Assert.Equal(1, checkpoint.SuccessCount);
        Assert.Equal(1, checkpoint.FailureCount);
    }

    [Fact]
    public async Task LiveProviderPhase_IsAuthoritativeAndRequiresCompatibleDetail()
    {
        CSharpDbObservabilityOptions observability = CreateOptions(
            "storage-checkpoint-live-phase");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
        };
        await using Database firstDatabase = await Database.OpenInMemoryAsync(
            databaseOptions,
            TestContext.Current.CancellationToken);
        await using Database secondDatabase = await Database.OpenInMemoryAsync(
            databaseOptions,
            TestContext.Current.CancellationToken);
        StorageRuntimeDiagnostics.Registration first =
            GetStorageRuntimeRegistration(firstDatabase);
        StorageRuntimeDiagnostics.Registration second =
            GetStorageRuntimeRegistration(secondDatabase);
        CheckpointCoordinator firstCoordinator =
            GetCheckpointCoordinator(firstDatabase);
        CheckpointCoordinator secondCoordinator =
            GetCheckpointCoordinator(secondDatabase);

        first.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                origin: StorageCheckpointOriginRaw.Manual),
            correlation: null);
        first.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Faulted,
                StorageRuntimeOperationOutcomeRaw.Failed,
                StorageCheckpointOriginRaw.Manual,
                StorageRuntimeFailureKindRaw.Io),
            correlation: null);
        firstCoordinator.SetRuntimePhase(StorageCheckpointPhaseRaw.Faulted);

        second.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Finalizing,
                origin: StorageCheckpointOriginRaw.Backup),
            correlation: null);
        secondCoordinator.SetRuntimePhase(StorageCheckpointPhaseRaw.Finalizing);

        StorageRuntimeDiagnosticsCapture mixedActive = Capture(state);
        Assert.Equal(
            CheckpointPhase.Faulted,
            mixedActive.Wal.Value!.CheckpointPhase);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            mixedActive.Wal.Value.Checkpoint.Availability);

        second.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                StorageCheckpointOriginRaw.Backup),
            correlation: null);
        secondCoordinator.SetRuntimePhase(StorageCheckpointPhaseRaw.Idle);

        CheckpointDiagnosticsSnapshot faulted =
            Capture(state).Wal.Value!.Checkpoint.Value!;
        Assert.Equal(CheckpointPhase.Faulted, faulted.Phase);
        Assert.Equal(CheckpointOrigin.Manual, faulted.Origin);

        firstCoordinator.SetRuntimePhase(StorageCheckpointPhaseRaw.Idle);
        CheckpointDiagnosticsSnapshot idle =
            Capture(state).Wal.Value!.Checkpoint.Value!;
        Assert.Equal(CheckpointPhase.Idle, idle.Phase);
        Assert.Equal(CheckpointOrigin.Backup, idle.Origin);

        secondCoordinator.SetRuntimePhase(StorageCheckpointPhaseRaw.Requested);
        StorageRuntimeDiagnosticsCapture requested = Capture(state);
        Assert.Equal(
            CheckpointPhase.Requested,
            requested.Wal.Value!.CheckpointPhase);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            requested.Wal.Value.Checkpoint.Availability);
    }

    [Fact]
    public async Task PreStartClockFailure_DowngradesOnlyCheckpointDetail()
    {
        var clock = new ThrowAfterStartTimeProvider(
            new DateTimeOffset(2026, 8, 11, 12, 30, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability = CreateOptions(
            "storage-checkpoint-pre-start-clock-failure");
        using var state = new CSharpDbRuntimeDiagnosticsState(
            observability,
            clock);
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
        };
        await using Database database = await Database.OpenInMemoryAsync(
            databaseOptions,
            TestContext.Current.CancellationToken);
        StorageRuntimeDiagnostics.Registration registration =
            GetStorageRuntimeRegistration(database);
        Pager pager = GetPager(database);
        CheckpointCoordinator checkpoints = Assert.IsType<CheckpointCoordinator>(
            typeof(Pager).GetField(
                    "_checkpoints",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(pager));

        try
        {
            clock.Throw = true;
            registration.Observer.OnCheckpointStarted(
                CheckpointRaw(StorageCheckpointPhaseRaw.Copying),
                correlation: null);
            checkpoints.SetRuntimePhase(StorageCheckpointPhaseRaw.Copying);

            StorageRuntimeDiagnosticsCapture capture = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                capture.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                capture.Wal.Availability);
            Assert.Equal(
                CheckpointPhase.Copying,
                capture.Wal.Value!.CheckpointPhase);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                capture.Wal.Value.Checkpoint.Availability);
        }
        finally
        {
            clock.Throw = false;
            checkpoints.SetRuntimePhase(StorageCheckpointPhaseRaw.Idle);
        }
    }

    [Fact]
    public async Task RequestedCheckpointWithoutHistory_PreservesCoarsePhaseAndHidesDetail()
    {
        CSharpDbObservabilityOptions observability = CreateOptions(
            "storage-checkpoint-requested-no-history");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
        };
        await using Database database = await Database.OpenInMemoryAsync(
            databaseOptions,
            TestContext.Current.CancellationToken);
        CheckpointCoordinator checkpoints = GetCheckpointCoordinator(database);

        try
        {
            checkpoints.SetRuntimePhase(StorageCheckpointPhaseRaw.Requested);

            StorageRuntimeDiagnosticsCapture capture = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                capture.Wal.Availability);
            Assert.Equal(
                CheckpointPhase.Requested,
                capture.Wal.Value!.CheckpointPhase);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                capture.Wal.Value.Checkpoint.Availability);
        }
        finally
        {
            checkpoints.SetRuntimePhase(StorageCheckpointPhaseRaw.Idle);
        }
    }

    private static StorageCheckpointRuntimeRawSnapshot CheckpointRaw(
        StorageCheckpointPhaseRaw phase,
        StorageRuntimeOperationOutcomeRaw outcome =
            StorageRuntimeOperationOutcomeRaw.Running,
        StorageCheckpointOriginRaw origin =
            StorageCheckpointOriginRaw.BackgroundAuto,
        StorageRuntimeFailureKindRaw failureKind =
            StorageRuntimeFailureKindRaw.None)
        => new(
            phase,
            origin,
            CompletedPageCount: phase == StorageCheckpointPhaseRaw.Finalizing
                ? 1
                : 0,
            TotalPageCount: 1,
            RetentionReason: StorageCheckpointRetentionReasonRaw.None,
            Outcome: outcome,
            FailureKind: failureKind);

    private static StorageRecoveryRuntimeRawSnapshot RecoveryRaw(
        long scannedFrameCount)
        => new(
            StorageRecoveryPhaseRaw.Completed,
            scannedFrameCount,
            scannedFrameCount * PageConstants.WalFrameSize,
            RecoveredFrameCount: scannedFrameCount,
            RecoveredBytes: scannedFrameCount * PageConstants.WalFrameSize,
            DiscardedFrameCount: 0,
            DiscardedBytes: 0,
            TruncationReason: StorageRecoveryTruncationReasonRaw.None,
            AttemptCount: 1,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            Outcome: StorageRuntimeOperationOutcomeRaw.Succeeded,
            FailureKind: StorageRuntimeFailureKindRaw.None);

    private static StorageRuntimeDiagnostics.Registration
        GetStorageRuntimeRegistration(Database database)
        => Assert.IsType<StorageRuntimeDiagnostics.Registration>(
            typeof(Database).GetField(
                    "_storageRuntimeDiagnosticsRegistration",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(database));

    private static Pager GetPager(Database database)
        => Assert.IsType<Pager>(
            typeof(Database).GetField(
                    "_pager",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(database));

    private static CheckpointCoordinator GetCheckpointCoordinator(
        Database database)
        => Assert.IsType<CheckpointCoordinator>(
            typeof(Pager).GetField(
                    "_checkpoints",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(GetPager(database)));

    private static StorageRuntimeDiagnosticsCapture Capture(
        CSharpDbRuntimeDiagnosticsState state)
        => StorageRuntimeDiagnostics.Capture(
            state,
            state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Engine));

    private static CSharpDbObservabilityOptions CreateOptions(string alias)
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
        };

    private sealed class ThrowAfterStartTimeProvider(DateTimeOffset utcNow)
        : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp = 1;

        internal bool Throw { get; set; }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => Throw
                ? throw new InvalidOperationException("Clock failure.")
                : _utcNow;

        public override long GetTimestamp()
            => Throw
                ? throw new InvalidOperationException("Clock failure.")
                : Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
        {
            _utcNow = _utcNow.Add(elapsed);
            Interlocked.Add(ref _timestamp, elapsed.Ticks);
        }
    }
}
