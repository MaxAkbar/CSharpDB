using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;
using System.Reflection;

namespace CSharpDB.Tests;

public sealed class StorageRecoveryCheckpointObserverTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly MethodInfo ReportCheckpointWorkMethod =
        typeof(Pager).GetMethod(
            "ReportRuntimeCheckpointWorkStartedOrChanged",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly MethodInfo CompleteCheckpointMethod =
        typeof(Pager).GetMethod(
            "CompleteRuntimeCheckpoint",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly MethodInfo PublishCheckpointChangedMethod =
        typeof(Pager).GetMethod(
            "PublishRuntimeCheckpointChanged",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public void StorageEngineOptions_CopiesPreserveInternalObserver()
    {
        var observer = new RecordingObserver();
        StorageEngineOptions options =
            new StorageEngineOptions().WithRuntimeDiagnosticsObserver(observer);

        StorageEngineOptions configured = options.Configure(
            builder => builder.UseDurabilityMode(DurabilityMode.Buffered));

        Assert.Same(observer, options.RuntimeDiagnosticsObserver);
        Assert.Same(observer, configured.RuntimeDiagnosticsObserver);
        Assert.Null(
            configured.WithRuntimeDiagnosticsObserver(null)
                .RuntimeDiagnosticsObserver);
    }

    [Fact]
    public async Task ManualCheckpoint_ReportsOrderedProgress_AndSkipsNoWork()
    {
        var observer = new RecordingObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });

        await using (pager)
        {
            await CommitDirtyPageAsync(pager, 0x51);
            observer.ClearCheckpointEvents();

            await pager.CheckpointAsync(Ct);

            StorageCheckpointRuntimeRawSnapshot started =
                Assert.Single(observer.CheckpointStarted);
            StorageCheckpointRuntimeRawSnapshot completed =
                Assert.Single(observer.CheckpointCompleted);
            Assert.Equal(StorageCheckpointOriginRaw.Manual, started.Origin);
            Assert.Equal(StorageCheckpointPhaseRaw.Copying, started.Phase);
            Assert.Equal(StorageRuntimeOperationOutcomeRaw.Running, started.Outcome);
            Assert.Contains(
                observer.CheckpointChanged,
                snapshot => snapshot.Phase == StorageCheckpointPhaseRaw.Finalizing);
            Assert.Equal(StorageCheckpointPhaseRaw.Idle, completed.Phase);
            Assert.Equal(StorageRuntimeOperationOutcomeRaw.Succeeded, completed.Outcome);
            Assert.NotNull(completed.CompletedPageCount);
            Assert.Equal(completed.TotalPageCount, completed.CompletedPageCount);
            Assert.Equal("checkpoint-started", observer.Order[0]);
            Assert.Equal("checkpoint-completed", observer.Order[^1]);

            observer.ClearCheckpointEvents();
            await pager.CheckpointAsync(Ct);
            Assert.Empty(observer.CheckpointStarted);
            Assert.Empty(observer.CheckpointChanged);
            Assert.Empty(observer.CheckpointCompleted);
        }
    }

    [Fact]
    public async Task IncrementalCheckpoint_ReportsExactRetentionReasons()
    {
        var observer = new RecordingObserver();
        await using Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });
        await CommitDirtyPageAsync(pager, 0x58);
        WalSnapshot reader = pager.AcquireReaderSnapshot();

        try
        {
            observer.ClearCheckpointEvents();
            await pager.CheckpointAsync(Ct);
            Assert.Contains(
                observer.CheckpointChanged,
                snapshot =>
                    snapshot.Phase == StorageCheckpointPhaseRaw.CopyCompleteAwaitingReaders &&
                    snapshot.RetentionReason == StorageCheckpointRetentionReasonRaw.ActiveReaders);

            await CommitDirtyPageAsync(pager, 0x59);
            await pager.CheckpointAsync(Ct);
            Assert.Contains(
                observer.CheckpointChanged,
                snapshot =>
                    snapshot.RetentionReason ==
                    StorageCheckpointRetentionReasonRaw.ActiveReadersAndNewerCommits);
        }
        finally
        {
            pager.ReleaseReaderSnapshot(reader);
        }

        await pager.CheckpointAsync(Ct);
        Assert.Contains(
            observer.CheckpointChanged,
            snapshot =>
                snapshot.Phase == StorageCheckpointPhaseRaw.Finalizing &&
                snapshot.RetentionReason == StorageCheckpointRetentionReasonRaw.NewerCommits);
        Assert.Equal(
            StorageRuntimeOperationOutcomeRaw.Succeeded,
            Assert.Single(observer.CheckpointCompleted).Outcome);
    }

    [Fact]
    public async Task ForegroundAndBackgroundAutoCheckpoint_ReportExactOrigins()
    {
        var foregroundObserver = new RecordingObserver();
        await using (Pager foreground = await OpenObservedMemoryPagerAsync(
                         foregroundObserver,
                         new PagerOptions
                         {
                             CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                             AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Foreground,
                         }))
        {
            foregroundObserver.ClearCheckpointEvents();
            await CommitDirtyPageAsync(foreground, 0x52);
            Assert.Equal(
                StorageCheckpointOriginRaw.ForegroundAuto,
                Assert.Single(foregroundObserver.CheckpointStarted).Origin);
            Assert.Equal(
                StorageRuntimeOperationOutcomeRaw.Succeeded,
                Assert.Single(foregroundObserver.CheckpointCompleted).Outcome);
        }

        var backgroundObserver = new RecordingObserver();
        await using (Pager background = await OpenObservedMemoryPagerAsync(
                         backgroundObserver,
                         new PagerOptions
                         {
                             CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                             AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                         }))
        {
            backgroundObserver.ClearCheckpointEvents();
            await CommitDirtyPageAsync(background, 0x53);
            StorageCheckpointRuntimeRawSnapshot completed =
                await backgroundObserver.WaitForCheckpointCompletionAsync(
                    StorageCheckpointOriginRaw.BackgroundAuto,
                    Ct);
            Assert.Equal(StorageRuntimeOperationOutcomeRaw.Succeeded, completed.Outcome);
            Assert.Contains(
                backgroundObserver.CheckpointStarted,
                snapshot => snapshot.Origin == StorageCheckpointOriginRaw.BackgroundAuto);
        }
    }

    [Fact]
    public async Task ForegroundAutoCheckpointFailure_IsReportedAlthoughCommitSwallowsIt()
    {
        var observer = new RecordingObserver();
        var device = new ArmableFlushFailingDevice(new MemoryStorageDevice());
        await using Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Foreground,
            },
            device);
        observer.ClearCheckpointEvents();
        device.Arm();

        // The durable commit succeeds and the foreground auto-checkpoint
        // deliberately defers its failure. Runtime diagnostics must still
        // receive one failed terminal operation.
        await CommitDirtyPageAsync(pager, 0x57);

        StorageCheckpointRuntimeRawSnapshot failed =
            Assert.Single(observer.CheckpointCompleted);
        Assert.Equal(StorageCheckpointOriginRaw.ForegroundAuto, failed.Origin);
        Assert.Equal(StorageCheckpointPhaseRaw.Faulted, failed.Phase);
        Assert.Equal(StorageRuntimeOperationOutcomeRaw.Failed, failed.Outcome);
        Assert.Equal(StorageRuntimeFailureKindRaw.Io, failed.FailureKind);

        device.Disarm();
        observer.ClearCheckpointEvents();
        await pager.CheckpointAsync(Ct);
        Assert.Equal(
            StorageRuntimeOperationOutcomeRaw.Succeeded,
            Assert.Single(observer.CheckpointCompleted).Outcome);
    }

    [Fact]
    public async Task BackupAndShutdownCheckpoint_ReportExactOriginsAndSwallowedFailure()
    {
        string snapshotPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_observed_backup_{Guid.NewGuid():N}.db");
        var backupObserver = new RecordingObserver();

        try
        {
            await using (Pager backupPager = await OpenObservedMemoryPagerAsync(
                             backupObserver,
                             new PagerOptions
                             {
                                 CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                             }))
            {
                await CommitDirtyPageAsync(backupPager, 0x54);
                backupObserver.ClearCheckpointEvents();
                await backupPager.SaveToFileAsync(snapshotPath, Ct);

                Assert.Equal(
                    StorageCheckpointOriginRaw.Backup,
                    Assert.Single(backupObserver.CheckpointStarted).Origin);
                Assert.Equal(
                    StorageRuntimeOperationOutcomeRaw.Succeeded,
                    Assert.Single(backupObserver.CheckpointCompleted).Outcome);
                Assert.True(File.Exists(snapshotPath));
            }
        }
        finally
        {
            if (File.Exists(snapshotPath))
                File.Delete(snapshotPath);
        }

        var shutdownObserver = new RecordingObserver();
        var failingDevice = new ArmableFlushFailingDevice(new MemoryStorageDevice());
        Pager shutdownPager = await OpenObservedMemoryPagerAsync(
            shutdownObserver,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            failingDevice);
        await CommitDirtyPageAsync(shutdownPager, 0x55);
        shutdownObserver.ClearCheckpointEvents();
        failingDevice.Arm();

        await shutdownPager.DisposeAsync();

        StorageCheckpointRuntimeRawSnapshot failed =
            Assert.Single(shutdownObserver.CheckpointCompleted);
        Assert.Equal(StorageCheckpointOriginRaw.Shutdown, failed.Origin);
        Assert.Equal(StorageCheckpointPhaseRaw.Faulted, failed.Phase);
        Assert.Equal(StorageRuntimeOperationOutcomeRaw.Failed, failed.Outcome);
        Assert.Equal(StorageRuntimeFailureKindRaw.Io, failed.FailureKind);
    }

    [Fact]
    public async Task StartupRecovery_ReportsScanCheckpointAndTerminalSuccess()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_observed_recovery_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var factory = new DefaultStorageEngineFactory();

        try
        {
            StorageEngineContext created = await factory.CreateNewAsync(
                dbPath,
                new StorageEngineOptions(),
                Ct);
            await created.Pager.DisposeAsync();

            byte[] databaseBytes = await File.ReadAllBytesAsync(dbPath, Ct);
            uint pageCount = checked((uint)(databaseBytes.Length / PageConstants.PageSize));
            byte[] pageZero = databaseBytes.AsSpan(0, PageConstants.PageSize).ToArray();
            var seedIndex = new WalIndex();
            await using (var seedWal = new WriteAheadLog(dbPath, seedIndex))
            {
                await seedWal.OpenAsync(pageCount, Ct);
                WalCommitResult commit = await seedWal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(0, pageZero) },
                    pageCount,
                    Ct);
                await commit.WaitAsync(Ct);
            }

            var observer = new RecordingObserver();
            StorageEngineOptions observedOptions =
                new StorageEngineOptions().WithRuntimeDiagnosticsObserver(observer);
            StorageEngineContext recovered = await factory.OpenAsync(
                dbPath,
                observedOptions,
                Ct);
            await using (recovered.Pager)
            {
                Assert.Equal(1, observer.RecoveryStartedCount);
                Assert.Contains(
                    observer.RecoveryChanged,
                    snapshot => snapshot.Phase == StorageRecoveryPhaseRaw.Scanning &&
                        snapshot.ScannedFrameCount == 1 &&
                        snapshot.RecoveredFrameCount == 1);
                Assert.Contains(
                    observer.RecoveryChanged,
                    snapshot => snapshot.Phase == StorageRecoveryPhaseRaw.Checkpointing);
                StorageRecoveryRuntimeRawSnapshot terminal =
                    Assert.Single(observer.RecoveryCompleted);
                Assert.Equal(StorageRecoveryPhaseRaw.Completed, terminal.Phase);
                Assert.Equal(StorageRuntimeOperationOutcomeRaw.Succeeded, terminal.Outcome);
                Assert.Equal(1, terminal.AttemptCount);
                Assert.Equal(0, terminal.RetryCount);
                Assert.Equal(StorageRuntimeFailureKindRaw.None, terminal.FailureKind);
                Assert.Contains(
                    observer.CheckpointStarted,
                    snapshot => snapshot.Origin == StorageCheckpointOriginRaw.StartupRecovery);
            }
        }
        finally
        {
            if (File.Exists(walPath))
                File.Delete(walPath);
            if (File.Exists(dbPath))
                File.Delete(dbPath);
        }
    }

    [Fact]
    public async Task ObserverFailures_AreIsolated_AndRawEventsContainNoTextOrExceptions()
    {
        var observer = new ThrowingObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });

        await pager.RecoverAsync(Ct);
        await CommitDirtyPageAsync(pager, 0x56);
        await pager.CheckpointAsync(Ct);
        await pager.DisposeAsync();

        Assert.DoesNotContain(
            typeof(StorageRecoveryRuntimeRawSnapshot).GetProperties(),
            property => property.PropertyType == typeof(string) ||
                typeof(Exception).IsAssignableFrom(property.PropertyType));
        Assert.DoesNotContain(
            typeof(StorageCheckpointRuntimeRawSnapshot).GetProperties(),
            property => property.PropertyType == typeof(string) ||
                typeof(Exception).IsAssignableFrom(property.PropertyType));
    }

    [Fact]
    public async Task CheckpointObserver_CanReenterRuntimeCaptureWithoutDeadlock()
    {
        var observer = new ReentrantCaptureObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });
        observer.Pager = pager;

        await using (pager)
        {
            await CommitDirtyPageAsync(pager, 0x5A);
            await pager.CheckpointAsync(Ct);

            Assert.True(await observer.CaptureCompleted.WaitAsync(Ct));
        }
    }

    [Fact]
    public async Task DisposeAsync_WaitsForBlockedShutdownTerminalDelivery()
    {
        var observer = new BlockingShutdownTerminalObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });
        Task? disposeTask = null;

        try
        {
            await CommitDirtyPageAsync(pager, 0x5B);
            disposeTask = Task.Run(
                async () => await pager.DisposeAsync(),
                Ct);

            await observer.TerminalEntered.WaitAsync(Ct);
            Assert.False(disposeTask.IsCompleted);

            observer.Release();
            await disposeTask.WaitAsync(Ct);
            Assert.True(observer.TerminalDelivered);
        }
        finally
        {
            observer.Release();
            if (disposeTask is not null)
                await disposeTask;
            else
                await pager.DisposeAsync();
        }
    }

    [Fact]
    public async Task StartedCorrelation_IsImmutableBeforeQueuedEventCanDrain()
    {
        var observer = new InterleavedCorrelationObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });
        Task? completionTask = null;
        Task? nextStartTask = null;

        try
        {
            ReportCheckpointWork(
                pager,
                StorageCheckpointOriginRaw.Manual,
                StorageCheckpointPhaseRaw.Copying);
            completionTask = Task.Run(
                () => CompleteCheckpoint(pager, exception: null),
                Ct);
            await observer.CompletedEntered.WaitAsync(Ct);

            nextStartTask = Task.Run(
                () => ReportCheckpointWork(
                    pager,
                    StorageCheckpointOriginRaw.Backup,
                    StorageCheckpointPhaseRaw.Copying),
                Ct);
            await observer.SecondCaptureEntered.WaitAsync(Ct);

            observer.ReleaseCompleted();
            await completionTask.WaitAsync(Ct);
            observer.ReleaseSecondCapture();
            await nextStartTask.WaitAsync(Ct);
            await observer.SecondStarted.WaitAsync(Ct);

            Assert.Same(
                observer.SecondCorrelation,
                observer.SecondStartedCorrelation);
            CompleteCheckpoint(pager, exception: null);
        }
        finally
        {
            observer.ReleaseCompleted();
            observer.ReleaseSecondCapture();
            if (completionTask is not null)
                await completionTask;
            if (nextStartTask is not null)
                await nextStartTask;
            await pager.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompletionCorrelation_IsCapturedBeforeDelayedDelivery()
    {
        var observer = new DelayedCompletionCorrelationObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            });
        Task? changedTask = null;

        try
        {
            ReportCheckpointWork(
                pager,
                StorageCheckpointOriginRaw.Manual,
                StorageCheckpointPhaseRaw.Copying);
            changedTask = Task.Run(
                () => PublishCheckpointChanged(
                    pager,
                    StorageCheckpointPhaseRaw.Finalizing),
                Ct);
            await observer.ChangedEntered.WaitAsync(Ct);

            observer.Clock = 5;
            CompleteCheckpoint(pager, exception: null);
            observer.Clock = 12;
            observer.ReleaseChanged();
            await changedTask.WaitAsync(Ct);
            await observer.Completed.WaitAsync(Ct);

            Assert.Equal(5L, observer.CompletedCorrelation);
        }
        finally
        {
            observer.ReleaseChanged();
            if (changedTask is not null)
                await changedTask;
            await pager.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Dispose_WaitsForBackgroundTerminalWhenRollbackFails(
        bool synchronous,
        bool backgroundFails)
    {
        var observer = new RecordingObserver();
        Pager pager = await OpenObservedMemoryPagerAsync(
            observer,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                AutoCheckpointExecutionMode =
                    AutoCheckpointExecutionMode.Background,
            });
        var backgroundEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBackground = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var rollbackEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var rollbackError = new IOException("Synthetic rollback failure.");
        Task? disposeTask = null;

        try
        {
            await pager.BeginTransactionAsync(Ct);
            CheckpointCoordinator coordinator = GetCheckpointCoordinator(pager);
            coordinator.RequestDeferredCheckpoint();
            Assert.True(coordinator.TryStartBackgroundCheckpoint(async _ =>
            {
                ReportCheckpointWork(
                    pager,
                    StorageCheckpointOriginRaw.BackgroundAuto,
                    StorageCheckpointPhaseRaw.Copying);
                backgroundEntered.TrySetResult();
                await releaseBackground.Task;
                InvalidOperationException? backgroundError = backgroundFails
                    ? new InvalidOperationException(
                        "Synthetic background shutdown failure.")
                    : null;
                CompleteCheckpoint(pager, backgroundError);
                if (backgroundError is not null)
                    throw backgroundError;
            }));
            await backgroundEntered.Task.WaitAsync(Ct);
            observer.ClearCheckpointEvents();
            pager.DisposeRollbackForTests = () =>
            {
                rollbackEntered.TrySetResult();
                return new ValueTask(Task.FromException(rollbackError));
            };

            disposeTask = synchronous
                ? Task.Run(pager.Dispose, Ct)
                : Task.Run(async () => await pager.DisposeAsync(), Ct);
            await rollbackEntered.Task.WaitAsync(Ct);

            Task early = await Task.WhenAny(
                disposeTask,
                Task.Delay(TimeSpan.FromMilliseconds(500), Ct));
            Assert.NotSame(disposeTask, early);

            releaseBackground.TrySetResult();
            IOException error = await Assert.ThrowsAsync<IOException>(
                async () => await disposeTask);
            Assert.Same(rollbackError, error);
            StorageCheckpointRuntimeRawSnapshot terminal =
                Assert.Single(observer.CheckpointCompleted);
            Assert.Equal(
                StorageCheckpointOriginRaw.BackgroundAuto,
                terminal.Origin);
        }
        finally
        {
            releaseBackground.TrySetResult();
            pager.DisposeRollbackForTests = null;
            if (disposeTask is not null)
            {
                try
                {
                    await disposeTask;
                }
                catch
                {
                }
            }

            try
            {
                await pager.RollbackAsync(CancellationToken.None);
            }
            catch
            {
            }
            await pager.DisposeAsync();
        }
    }

    [Fact]
    public async Task RecoveryFailure_ReportsOnlyAPathFreeSafeFailureKind()
    {
        var observer = new RecordingObserver();
        var device = new MemoryStorageDevice();
        var index = new WalIndex();
        var wal = new MemoryWriteAheadLog(
            index,
            checksumProvider: null,
            initialBytes: new byte[] { 0x43, 0x53, 0x44, 0x42 },
            runtimeDiagnosticsObserver: observer);
        Pager? pager = null;

        try
        {
            pager = await Pager.CreateAsync(
                device,
                wal,
                index,
                new PagerOptions(),
                observer,
                Ct);

            await Assert.ThrowsAsync<CSharpDbException>(
                async () => await pager.RecoverAsync(Ct));

            StorageRecoveryRuntimeRawSnapshot failed =
                Assert.Single(observer.RecoveryCompleted);
            Assert.Equal(StorageRecoveryPhaseRaw.Completed, failed.Phase);
            Assert.Equal(StorageRuntimeOperationOutcomeRaw.Failed, failed.Outcome);
            Assert.Equal(StorageRuntimeFailureKindRaw.Corrupt, failed.FailureKind);
            Assert.Equal(1L, failed.AttemptCount);
            Assert.Equal(0L, failed.RetryCount);
        }
        finally
        {
            if (pager is not null)
                await pager.DisposeAsync();
            else
            {
                await wal.DisposeAsync();
                await device.DisposeAsync();
            }
        }
    }

    private static async ValueTask<Pager> OpenObservedMemoryPagerAsync(
        IStorageRuntimeDiagnosticsObserver observer,
        PagerOptions options,
        IStorageDevice? device = null)
    {
        device ??= new MemoryStorageDevice();
        var index = new WalIndex();
        var wal = new MemoryWriteAheadLog(
            index,
            checksumProvider: null,
            initialBytes: default,
            runtimeDiagnosticsObserver: observer);
        Pager? pager = null;

        try
        {
            pager = await Pager.CreateAsync(
                device,
                wal,
                index,
                options,
                observer,
                Ct);
            await pager.InitializeNewDatabaseAsync(Ct);
            return pager;
        }
        catch
        {
            if (pager is not null)
                await pager.DisposeAsync();
            else
            {
                await wal.DisposeAsync();
                await device.DisposeAsync();
            }

            throw;
        }
    }

    private static async ValueTask CommitDirtyPageAsync(Pager pager, byte value)
    {
        await pager.BeginTransactionAsync(Ct);
        uint pageId = await pager.AllocatePageAsync(Ct);
        byte[] page = await pager.GetPageAsync(pageId, Ct);
        page[0] = value;
        await pager.MarkDirtyAsync(pageId, Ct);
        await pager.CommitAsync(Ct);
    }

    private static CheckpointCoordinator GetCheckpointCoordinator(Pager pager)
        => Assert.IsType<CheckpointCoordinator>(
            typeof(Pager).GetField(
                    "_checkpoints",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(pager));

    private static void ReportCheckpointWork(
        Pager pager,
        StorageCheckpointOriginRaw origin,
        StorageCheckpointPhaseRaw phase)
        => ReportCheckpointWorkMethod.Invoke(pager, [origin, phase]);

    private static void CompleteCheckpoint(Pager pager, Exception? exception)
        => CompleteCheckpointMethod.Invoke(pager, [exception]);

    private static void PublishCheckpointChanged(
        Pager pager,
        StorageCheckpointPhaseRaw phase)
        => PublishCheckpointChangedMethod.Invoke(pager, [phase]);

    private sealed class RecordingObserver : IStorageRuntimeDiagnosticsObserver
    {
        private readonly object _gate = new();
        private readonly List<string> _order = [];
        private readonly List<StorageRecoveryRuntimeRawSnapshot> _recoveryChanged = [];
        private readonly List<StorageRecoveryRuntimeRawSnapshot> _recoveryCompleted = [];
        private readonly List<StorageCheckpointRuntimeRawSnapshot> _checkpointStarted = [];
        private readonly List<StorageCheckpointRuntimeRawSnapshot> _checkpointChanged = [];
        private readonly List<StorageCheckpointRuntimeRawSnapshot> _checkpointCompleted = [];
        private readonly Dictionary<StorageCheckpointOriginRaw, TaskCompletionSource<StorageCheckpointRuntimeRawSnapshot>>
            _checkpointCompletionSignals = [];
        private int _recoveryStartedCount;

        public int RecoveryStartedCount => Volatile.Read(ref _recoveryStartedCount);
        public IReadOnlyList<string> Order { get { lock (_gate) return _order.ToArray(); } }
        public IReadOnlyList<StorageRecoveryRuntimeRawSnapshot> RecoveryChanged { get { lock (_gate) return _recoveryChanged.ToArray(); } }
        public IReadOnlyList<StorageRecoveryRuntimeRawSnapshot> RecoveryCompleted { get { lock (_gate) return _recoveryCompleted.ToArray(); } }
        public IReadOnlyList<StorageCheckpointRuntimeRawSnapshot> CheckpointStarted { get { lock (_gate) return _checkpointStarted.ToArray(); } }
        public IReadOnlyList<StorageCheckpointRuntimeRawSnapshot> CheckpointChanged { get { lock (_gate) return _checkpointChanged.ToArray(); } }
        public IReadOnlyList<StorageCheckpointRuntimeRawSnapshot> CheckpointCompleted { get { lock (_gate) return _checkpointCompleted.ToArray(); } }

        public object? CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin) => null;

        public object? CaptureCheckpointCompletionCorrelation() => null;

        public void OnRecoveryStarted()
        {
            Interlocked.Increment(ref _recoveryStartedCount);
            lock (_gate)
                _order.Add("recovery-started");
        }

        public void OnRecoveryChanged(in StorageRecoveryRuntimeRawSnapshot snapshot)
        {
            lock (_gate)
            {
                _recoveryChanged.Add(snapshot);
                _order.Add("recovery-changed");
            }
        }

        public void OnRecoveryCompleted(in StorageRecoveryRuntimeRawSnapshot snapshot)
        {
            lock (_gate)
            {
                _recoveryCompleted.Add(snapshot);
                _order.Add("recovery-completed");
            }
        }

        public void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            lock (_gate)
            {
                _checkpointStarted.Add(snapshot);
                _order.Add("checkpoint-started");
            }
        }

        public void OnCheckpointChanged(in StorageCheckpointRuntimeRawSnapshot snapshot)
        {
            lock (_gate)
            {
                _checkpointChanged.Add(snapshot);
                _order.Add("checkpoint-changed");
            }
        }

        public void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            TaskCompletionSource<StorageCheckpointRuntimeRawSnapshot>? signal;
            lock (_gate)
            {
                _checkpointCompleted.Add(snapshot);
                _order.Add("checkpoint-completed");
                _checkpointCompletionSignals.TryGetValue(snapshot.Origin, out signal);
            }

            signal?.TrySetResult(snapshot);
        }

        public void OnWalFlushCompleted()
        {
            lock (_gate)
                _order.Add("wal-flush-completed");
        }

        public Task<StorageCheckpointRuntimeRawSnapshot> WaitForCheckpointCompletionAsync(
            StorageCheckpointOriginRaw origin,
            CancellationToken ct)
        {
            lock (_gate)
            {
                StorageCheckpointRuntimeRawSnapshot existing =
                    _checkpointCompleted.LastOrDefault(
                        snapshot => snapshot.Origin == origin);
                if (existing.Origin == origin)
                    return Task.FromResult(existing);

                if (!_checkpointCompletionSignals.TryGetValue(origin, out var signal))
                {
                    signal = new TaskCompletionSource<StorageCheckpointRuntimeRawSnapshot>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    _checkpointCompletionSignals[origin] = signal;
                }

                return signal.Task.WaitAsync(ct);
            }
        }

        public void ClearCheckpointEvents()
        {
            lock (_gate)
            {
                _checkpointStarted.Clear();
                _checkpointChanged.Clear();
                _checkpointCompleted.Clear();
                _checkpointCompletionSignals.Clear();
                _order.Clear();
            }
        }
    }

    private sealed class ThrowingObserver : IStorageRuntimeDiagnosticsObserver
    {
        public object? CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin) =>
            throw new InvalidOperationException();
        public object? CaptureCheckpointCompletionCorrelation() =>
            throw new InvalidOperationException();

        public void OnRecoveryStarted() => throw new InvalidOperationException();
        public void OnRecoveryChanged(in StorageRecoveryRuntimeRawSnapshot snapshot) => throw new InvalidOperationException();
        public void OnRecoveryCompleted(in StorageRecoveryRuntimeRawSnapshot snapshot) => throw new InvalidOperationException();
        public void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) => throw new InvalidOperationException();
        public void OnCheckpointChanged(in StorageCheckpointRuntimeRawSnapshot snapshot) => throw new InvalidOperationException();
        public void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) => throw new InvalidOperationException();
        public void OnWalFlushCompleted() => throw new InvalidOperationException();
    }

    private abstract class NoOpObserver : IStorageRuntimeDiagnosticsObserver
    {
        public virtual object? CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin) => null;
        public virtual object? CaptureCheckpointCompletionCorrelation() => null;
        public virtual void OnRecoveryStarted() { }
        public virtual void OnRecoveryChanged(
            in StorageRecoveryRuntimeRawSnapshot snapshot) { }
        public virtual void OnRecoveryCompleted(
            in StorageRecoveryRuntimeRawSnapshot snapshot) { }
        public virtual void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }
        public virtual void OnCheckpointChanged(
            in StorageCheckpointRuntimeRawSnapshot snapshot) { }
        public virtual void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }
        public virtual void OnWalFlushCompleted() { }
    }

    private sealed class ReentrantCaptureObserver : NoOpObserver
    {
        private readonly TaskCompletionSource<bool> _captureCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private int _captureAttempted;

        internal Pager? Pager { get; set; }
        internal Task<bool> CaptureCompleted => _captureCompleted.Task;

        public override void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            if (Interlocked.Exchange(ref _captureAttempted, 1) != 0)
                return;

            Pager? pager = Pager;
            _captureCompleted.TrySetResult(
                pager is not null &&
                pager.TryGetRuntimeDiagnosticsSnapshot(out _));
        }
    }

    private sealed class BlockingShutdownTerminalObserver : NoOpObserver
    {
        private readonly ManualResetEventSlim _release = new(false);
        private readonly TaskCompletionSource _terminalEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private int _terminalDelivered;

        internal Task TerminalEntered => _terminalEntered.Task;
        internal bool TerminalDelivered =>
            Volatile.Read(ref _terminalDelivered) != 0;

        internal void Release() => _release.Set();

        public override void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            if (snapshot.Origin != StorageCheckpointOriginRaw.Shutdown)
                return;

            _terminalEntered.TrySetResult();
            if (_release.Wait(TimeSpan.FromSeconds(30)))
                Volatile.Write(ref _terminalDelivered, 1);
        }
    }

    private sealed class InterleavedCorrelationObserver : NoOpObserver
    {
        private readonly ManualResetEventSlim _releaseCompleted = new(false);
        private readonly ManualResetEventSlim _releaseSecondCapture = new(false);
        private readonly TaskCompletionSource _completedEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _secondCaptureEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _secondStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object _secondCorrelation = new();
        private int _captureCount;
        private int _startedCount;
        private int _completedCount;

        internal Task CompletedEntered => _completedEntered.Task;
        internal Task SecondCaptureEntered => _secondCaptureEntered.Task;
        internal Task SecondStarted => _secondStarted.Task;
        internal object SecondCorrelation => _secondCorrelation;
        internal object? SecondStartedCorrelation { get; private set; }

        internal void ReleaseCompleted() => _releaseCompleted.Set();
        internal void ReleaseSecondCapture() => _releaseSecondCapture.Set();

        public override object? CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin)
        {
            if (Interlocked.Increment(ref _captureCount) != 2)
                return new object();

            _secondCaptureEntered.TrySetResult();
            _releaseSecondCapture.Wait(TimeSpan.FromSeconds(30));
            return _secondCorrelation;
        }

        public override void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            if (Interlocked.Increment(ref _startedCount) != 2)
                return;

            SecondStartedCorrelation = correlation;
            _secondStarted.TrySetResult();
        }

        public override void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            if (Interlocked.Increment(ref _completedCount) != 1)
                return;

            _completedEntered.TrySetResult();
            _releaseCompleted.Wait(TimeSpan.FromSeconds(30));
        }
    }

    private sealed class DelayedCompletionCorrelationObserver : NoOpObserver
    {
        private readonly ManualResetEventSlim _releaseChanged = new(false);
        private readonly TaskCompletionSource _changedEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private long _clock;
        private long _completedCorrelation = -1;

        internal long Clock
        {
            get => Interlocked.Read(ref _clock);
            set => Interlocked.Exchange(ref _clock, value);
        }

        internal Task ChangedEntered => _changedEntered.Task;
        internal Task Completed => _completed.Task;
        internal long CompletedCorrelation =>
            Interlocked.Read(ref _completedCorrelation);

        internal void ReleaseChanged() => _releaseChanged.Set();

        public override object? CaptureCheckpointCompletionCorrelation()
            => Clock;

        public override void OnCheckpointChanged(
            in StorageCheckpointRuntimeRawSnapshot snapshot)
        {
            _changedEntered.TrySetResult();
            _releaseChanged.Wait(TimeSpan.FromSeconds(30));
        }

        public override void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            if (correlation is long captured)
                Interlocked.Exchange(ref _completedCorrelation, captured);
            _completed.TrySetResult();
        }
    }

    private sealed class ArmableFlushFailingDevice(IStorageDevice inner) : IStorageDevice
    {
        private int _armed;

        public long Length => inner.Length;

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Disarm() => Volatile.Write(ref _armed, 0);

        public ValueTask<int> ReadAsync(
            long offset,
            Memory<byte> buffer,
            CancellationToken ct = default)
            => inner.ReadAsync(offset, buffer, ct);

        public ValueTask WriteAsync(
            long offset,
            ReadOnlyMemory<byte> buffer,
            CancellationToken ct = default)
            => inner.WriteAsync(offset, buffer, ct);

        public ValueTask FlushAsync(CancellationToken ct = default)
        {
            if (Volatile.Read(ref _armed) != 0)
                throw new IOException("Synthetic flush failure.");
            return inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(
            long length,
            CancellationToken ct = default)
            => inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }
}
