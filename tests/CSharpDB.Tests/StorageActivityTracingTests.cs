using System.Diagnostics;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class StorageActivityTracingTests
{
    private const string DatabaseAlias = "storage-activity-tests";

    [Fact]
    public void StartupRecovery_EmitsOneExactSafeActivity()
    {
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions());
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);

        registration.Observer.OnRecoveryStarted();
        StorageRuntimeDiagnostics.RecoveryOperation operation =
            Assert.IsType<StorageRuntimeDiagnostics.RecoveryOperation>(
                registration.Recovery);
        Assert.Single(activities.Started("csharpdb.recovery"));
        Assert.Empty(activities.Stopped("csharpdb.recovery"));

        registration.Observer.OnRecoveryCompleted(RecoverySucceeded());

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.recovery"));
        Assert.Equal(ActivityKind.Internal, activity.Kind);
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
        Assert.Equal(operation.OperationId.Value, Tag(activity, "csharpdb.operation.id"));
        Assert.Equal("recovery", Tag(activity, "csharpdb.operation.class"));
        Assert.Equal("embedded", Tag(activity, "csharpdb.transport"));
        Assert.Equal(DatabaseAlias, Tag(activity, "db.namespace"));
        Assert.Equal("succeeded", Tag(activity, "csharpdb.operation.outcome"));
    }

    [Theory]
    [InlineData((int)StorageCheckpointOriginRaw.ForegroundAuto)]
    [InlineData((int)StorageCheckpointOriginRaw.BackgroundAuto)]
    [InlineData((int)StorageCheckpointOriginRaw.Shutdown)]
    public void AutomaticCheckpoint_EmitsOneSafeTerminalActivity(
        int originValue)
    {
        var origin = (StorageCheckpointOriginRaw)originValue;
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions());
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: false);

        registration.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                StorageRuntimeOperationOutcomeRaw.Running,
                origin),
            correlation: null);
        StorageRuntimeDiagnostics.CheckpointOperation operation =
            Assert.IsType<StorageRuntimeDiagnostics.CheckpointOperation>(
                registration.Checkpoint);

        registration.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Faulted,
                StorageRuntimeOperationOutcomeRaw.Failed,
                origin,
                StorageRuntimeFailureKindRaw.Io),
            correlation: null);

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.checkpoint"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal(operation.OperationId.Value, Tag(activity, "csharpdb.operation.id"));
        Assert.Equal("checkpoint", Tag(activity, "csharpdb.maintenance.kind"));
        Assert.Equal("failed", Tag(activity, "csharpdb.operation.outcome"));
        Assert.Equal("database_io", Tag(activity, "error.type"));
        Assert.Equal("csharpdb.io", Tag(activity, "csharpdb.error.code"));
        Assert.DoesNotContain(
            activity.TagObjects,
            static tag => tag.Value?.ToString()?.Contains(
                "path-secret",
                StringComparison.OrdinalIgnoreCase) == true);
    }

    [Theory]
    [InlineData((int)StorageCheckpointOriginRaw.Manual)]
    [InlineData((int)StorageCheckpointOriginRaw.Backup)]
    [InlineData((int)StorageCheckpointOriginRaw.StartupRecovery)]
    public void LogicallyOwnedCheckpoint_DoesNotCreateSecondPhysicalActivity(
        int originValue)
    {
        var origin = (StorageCheckpointOriginRaw)originValue;
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions());
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);

        registration.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                StorageRuntimeOperationOutcomeRaw.Running,
                origin),
            correlation: null);
        registration.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                origin),
            correlation: null);

        Assert.Empty(activities.Started("csharpdb.checkpoint"));
        Assert.Empty(activities.Stopped("csharpdb.checkpoint"));
    }

    [Fact]
    public void RegistrationRetirement_StopsActivePhysicalActivitiesExactlyOnce()
    {
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions());
        StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);

        registration.Observer.OnRecoveryStarted();
        registration.Observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                StorageRuntimeOperationOutcomeRaw.Running,
                StorageCheckpointOriginRaw.BackgroundAuto),
            correlation: null);

        registration.Dispose();
        registration.Dispose();

        Assert.Single(activities.Stopped("csharpdb.recovery"));
        Assert.Single(activities.Stopped("csharpdb.checkpoint"));
        Assert.Null(
            Tag(
                Assert.Single(activities.Stopped("csharpdb.recovery")),
                "csharpdb.operation.outcome"));
        Assert.Null(
            Tag(
                Assert.Single(activities.Stopped("csharpdb.checkpoint")),
            "csharpdb.operation.outcome"));
    }

    [Fact]
    public void AutomaticCheckpoint_UsesCapturedIntervalAndExplicitRootContext()
    {
        DateTimeOffset startedAtUtc =
            new(2026, 8, 12, 15, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(startedAtUtc);
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: false);
        IStorageRuntimeDiagnosticsObserver observer = registration.Observer;

        using var unrelated = new Activity("unrelated-caller").Start();
        object? startCorrelation = observer.CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw.BackgroundAuto);
        clock.Advance(TimeSpan.FromSeconds(2));
        observer.OnCheckpointStarted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Copying,
                StorageRuntimeOperationOutcomeRaw.Running,
                StorageCheckpointOriginRaw.BackgroundAuto),
            startCorrelation);
        clock.Advance(TimeSpan.FromSeconds(3));
        object? completionCorrelation =
            observer.CaptureCheckpointCompletionCorrelation();
        clock.Advance(TimeSpan.FromSeconds(2));
        observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Idle,
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                StorageCheckpointOriginRaw.BackgroundAuto),
            completionCorrelation);

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.checkpoint"));
        Assert.Equal(startedAtUtc, activity.StartTimeUtc);
        Assert.Equal(TimeSpan.FromSeconds(5), activity.Duration);
        Assert.Null(activity.ParentId);
        Assert.Equal("root", Tag(activity, "csharpdb.operation.role"));
        Assert.Same(unrelated, Activity.Current);
    }

    [Fact]
    public void DuplicatePhysicalStarts_DoNotExportAbandonedCandidateActivities()
    {
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions());
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);

        registration.Observer.OnRecoveryStarted();
        registration.Observer.OnRecoveryStarted();
        StorageCheckpointRuntimeRawSnapshot checkpoint = CheckpointRaw(
            StorageCheckpointPhaseRaw.Copying,
            StorageRuntimeOperationOutcomeRaw.Running,
            StorageCheckpointOriginRaw.ForegroundAuto);
        registration.Observer.OnCheckpointStarted(checkpoint, correlation: null);
        registration.Observer.OnCheckpointStarted(checkpoint, correlation: null);

        Assert.Single(activities.Started("csharpdb.recovery"));
        Assert.Single(activities.Started("csharpdb.checkpoint"));

        registration.Dispose();
        Assert.Single(activities.Stopped("csharpdb.recovery"));
        Assert.Single(activities.Stopped("csharpdb.checkpoint"));
    }

    [Fact]
    public async Task CheckpointCompletionWhileActivityStartIsBlocked_DrainsOneExactTerminalSpan()
    {
        DateTimeOffset startedAtUtc =
            new(2026, 8, 12, 16, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(startedAtUtc);
        using var activities = new ActivityRecorder("csharpdb.checkpoint");
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: false);
        IStorageRuntimeDiagnosticsObserver observer = registration.Observer;
        object? startCorrelation = observer.CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw.BackgroundAuto);
        bool startAmbientPreserved = false;
        Task startTask = Task.Factory.StartNew(
            () =>
            {
                using var ambient = new Activity("checkpoint-start-ambient");
                ambient.Start();
                observer.OnCheckpointStarted(
                    CheckpointRaw(
                        StorageCheckpointPhaseRaw.Copying,
                        StorageRuntimeOperationOutcomeRaw.Running,
                        StorageCheckpointOriginRaw.BackgroundAuto),
                    startCorrelation);
                startAmbientPreserved = ReferenceEquals(
                    ambient,
                    Activity.Current);
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        bool completionAmbientPreserved = false;
        try
        {
            Assert.True(activities.WaitForBlockedStart(TimeSpan.FromSeconds(10)));
            clock.Advance(TimeSpan.FromSeconds(5));
            object? completionCorrelation =
                observer.CaptureCheckpointCompletionCorrelation();
            using var ambient = new Activity("checkpoint-completion-ambient");
            ambient.Start();
            observer.OnCheckpointCompleted(
                CheckpointRaw(
                    StorageCheckpointPhaseRaw.Idle,
                    StorageRuntimeOperationOutcomeRaw.Succeeded,
                    StorageCheckpointOriginRaw.BackgroundAuto),
                completionCorrelation);
            completionAmbientPreserved = ReferenceEquals(
                ambient,
                Activity.Current);
        }
        finally
        {
            activities.ReleaseBlockedStart();
            await startTask.WaitAsync(
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken);
        }

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.checkpoint"));
        Assert.Single(activities.Started("csharpdb.checkpoint"));
        Assert.Equal(startedAtUtc, activity.StartTimeUtc);
        Assert.Equal(TimeSpan.FromSeconds(5), activity.Duration);
        Assert.Equal("succeeded", Tag(activity, "csharpdb.operation.outcome"));
        Assert.Null(activity.ParentId);
        Assert.True(startAmbientPreserved);
        Assert.True(completionAmbientPreserved);
    }

    [Fact]
    public async Task RecoveryCompletionWhileActivityStartIsBlocked_DrainsOneExactTerminalSpan()
    {
        DateTimeOffset startedAtUtc =
            new(2026, 8, 12, 17, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(startedAtUtc);
        using var activities = new ActivityRecorder("csharpdb.recovery");
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);
        IStorageRuntimeDiagnosticsObserver observer = registration.Observer;
        bool startAmbientPreserved = false;
        Task startTask = Task.Factory.StartNew(
            () =>
            {
                using var ambient = new Activity("recovery-start-ambient");
                ambient.Start();
                observer.OnRecoveryStarted();
                startAmbientPreserved = ReferenceEquals(
                    ambient,
                    Activity.Current);
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        bool completionAmbientPreserved = false;
        try
        {
            Assert.True(activities.WaitForBlockedStart(TimeSpan.FromSeconds(10)));
            clock.Advance(TimeSpan.FromSeconds(7));
            using var ambient = new Activity("recovery-completion-ambient");
            ambient.Start();
            observer.OnRecoveryCompleted(RecoverySucceeded());
            completionAmbientPreserved = ReferenceEquals(
                ambient,
                Activity.Current);
        }
        finally
        {
            activities.ReleaseBlockedStart();
            await startTask.WaitAsync(
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken);
        }

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.recovery"));
        Assert.Single(activities.Started("csharpdb.recovery"));
        Assert.Equal(startedAtUtc, activity.StartTimeUtc);
        Assert.Equal(TimeSpan.FromSeconds(7), activity.Duration);
        Assert.Equal("succeeded", Tag(activity, "csharpdb.operation.outcome"));
        Assert.Null(activity.ParentId);
        Assert.True(startAmbientPreserved);
        Assert.True(completionAmbientPreserved);
    }

    [Fact]
    public async Task ConcurrentRecoveryCompletionWithoutStart_AdoptsAndExportsOneCandidate()
    {
        DateTimeOffset startedAtUtc =
            new(2026, 8, 12, 18, 0, 0, TimeSpan.Zero);
        using var clock = new CoordinatedCompletionTimeProvider(startedAtUtc);
        using var activities = new ActivityRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        using StorageRuntimeDiagnostics.Registration registration =
            CreateRegistration(state, recoveryApplicable: true);
        IStorageRuntimeDiagnosticsObserver observer = registration.Observer;
        clock.Arm();

        Task first = Task.Factory.StartNew(
            () => observer.OnRecoveryCompleted(RecoverySucceeded()),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        Task second = Task.Factory.StartNew(
            () => observer.OnRecoveryCompleted(RecoverySucceeded()),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        try
        {
            Assert.True(clock.WaitForCandidates(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            clock.ReleaseCompletions();
        }
        await Task.WhenAll(first, second).WaitAsync(
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);

        Activity activity = Assert.Single(
            activities.Stopped("csharpdb.recovery"));
        Assert.Single(activities.Started("csharpdb.recovery"));
        Assert.Equal("succeeded", Tag(activity, "csharpdb.operation.outcome"));
        Assert.Equal(
            registration.Recovery!.OperationId.Value,
            Tag(activity, "csharpdb.operation.id"));
    }

    private static StorageRuntimeDiagnostics.Registration CreateRegistration(
        CSharpDbRuntimeDiagnosticsState state,
        bool recoveryApplicable)
        => Assert.IsType<StorageRuntimeDiagnostics.Registration>(
            StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                state,
                recoveryApplicable));

    private static CSharpDbObservabilityOptions CreateOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = DatabaseAlias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                Queries = false,
                SlowQueries = false,
            },
            OpenTelemetry = new CSharpDbOpenTelemetryOptions
            {
                Enabled = true,
            },
        };

    private static StorageRecoveryRuntimeRawSnapshot RecoverySucceeded()
        => new(
            StorageRecoveryPhaseRaw.Completed,
            ScannedFrameCount: 1,
            ScannedBytes: 4096,
            RecoveredFrameCount: 1,
            RecoveredBytes: 4096,
            DiscardedFrameCount: 0,
            DiscardedBytes: 0,
            StorageRecoveryTruncationReasonRaw.None,
            AttemptCount: 1,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            Outcome: StorageRuntimeOperationOutcomeRaw.Succeeded,
            FailureKind: StorageRuntimeFailureKindRaw.None);

    private static StorageCheckpointRuntimeRawSnapshot CheckpointRaw(
        StorageCheckpointPhaseRaw phase,
        StorageRuntimeOperationOutcomeRaw outcome,
        StorageCheckpointOriginRaw origin,
        StorageRuntimeFailureKindRaw failureKind =
            StorageRuntimeFailureKindRaw.None)
        => new(
            phase,
            origin,
            CompletedPageCount: phase is StorageCheckpointPhaseRaw.Idle or
                StorageCheckpointPhaseRaw.Faulted
                    ? 1
                    : 0,
            TotalPageCount: 1,
            RetentionReason: StorageCheckpointRetentionReasonRaw.None,
            outcome,
            failureKind);

    private static string? Tag(Activity activity, string name)
        => activity.TagObjects.FirstOrDefault(
            item => string.Equals(item.Key, name, StringComparison.Ordinal)).Value
            ?.ToString();

    private sealed class ActivityRecorder : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<Activity> _started = [];
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;
        private readonly string? _blockedOperationName;
        private readonly ManualResetEventSlim _blockedStartEntered = new(false);
        private readonly ManualResetEventSlim _releaseBlockedStart = new(false);
        private int _blockedStartClaimed;

        internal ActivityRecorder(string? blockedOperationName = null)
        {
            _blockedOperationName = blockedOperationName;
            _listener = new ActivityListener
            {
                ShouldListenTo = static source =>
                    source.Name == CSharpDbDiagnostics.ActivitySourceName,
                Sample = Sample,
                SampleUsingParentId = SampleUsingParentId,
                ActivityStarted = OnStarted,
                ActivityStopped = OnStopped,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        internal bool WaitForBlockedStart(TimeSpan timeout)
            => _blockedStartEntered.Wait(timeout);

        internal void ReleaseBlockedStart()
            => _releaseBlockedStart.Set();

        internal Activity[] Started(string name)
        {
            lock (_gate)
            {
                return _started
                    .Where(activity => activity.OperationName == name)
                    .ToArray();
            }
        }

        internal Activity[] Stopped(string name)
        {
            lock (_gate)
            {
                return _stopped
                    .Where(activity => activity.OperationName == name)
                    .ToArray();
            }
        }

        public void Dispose()
        {
            _releaseBlockedStart.Set();
            _listener.Dispose();
            _blockedStartEntered.Dispose();
            _releaseBlockedStart.Dispose();
        }

        private static ActivitySamplingResult Sample(
            ref ActivityCreationOptions<ActivityContext> options)
            => ActivitySamplingResult.AllDataAndRecorded;

        private static ActivitySamplingResult SampleUsingParentId(
            ref ActivityCreationOptions<string> options)
            => ActivitySamplingResult.AllDataAndRecorded;

        private void OnStarted(Activity activity)
        {
            lock (_gate)
                _started.Add(activity);

            if (string.Equals(
                    activity.OperationName,
                    _blockedOperationName,
                    StringComparison.Ordinal) &&
                Interlocked.CompareExchange(
                    ref _blockedStartClaimed,
                    1,
                    0) == 0)
            {
                _blockedStartEntered.Set();
                if (!_releaseBlockedStart.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException(
                        "The blocked physical Activity start was not released.");
                }
            }
        }

        private void OnStopped(Activity activity)
        {
            lock (_gate)
                _stopped.Add(activity);
        }
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
        {
            _utcNow = _utcNow.Add(elapsed);
            Interlocked.Add(ref _timestamp, elapsed.Ticks);
        }
    }

    private sealed class CoordinatedCompletionTimeProvider(
        DateTimeOffset utcNow) : TimeProvider, IDisposable
    {
        private readonly CountdownEvent _candidatesCaptured = new(2);
        private readonly ManualResetEventSlim _releaseCompletions = new(false);
        private readonly ThreadLocal<int> _timestampReads = new(() => 0);
        private int _armed;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => utcNow;

        public override long GetTimestamp()
        {
            if (Volatile.Read(ref _armed) == 0)
                return 0;

            int readCount = _timestampReads.Value + 1;
            _timestampReads.Value = readCount;
            if (readCount == 2)
            {
                _candidatesCaptured.Signal();
                if (!_releaseCompletions.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException(
                        "Concurrent recovery completions were not released.");
                }
            }

            return readCount;
        }

        internal void Arm() => Volatile.Write(ref _armed, 1);

        internal bool WaitForCandidates(TimeSpan timeout)
            => _candidatesCaptured.Wait(timeout);

        internal void ReleaseCompletions()
            => _releaseCompletions.Set();

        public void Dispose()
        {
            _releaseCompletions.Set();
            _timestampReads.Dispose();
            _candidatesCaptured.Dispose();
            _releaseCompletions.Dispose();
        }
    }
}
