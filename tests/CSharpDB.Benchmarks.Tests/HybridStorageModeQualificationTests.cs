using System.Globalization;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using BenchmarkProgram = CSharpDB.Benchmarks.Program;

namespace CSharpDB.Benchmarks.Tests;

public sealed class HybridStorageModeQualificationTests
{
    private const string FileBackedCollectionPut = "Storage_FileBacked_Collection_Put_5s";
    private const string FileBackedSqlBatch = "Storage_FileBacked_Sql_Batch100_5s";
    private const string FileBackedSqlSingleInsert = "Storage_FileBacked_Sql_SingleInsert_5s";
    private const string InMemorySqlBatch = "Storage_InMemory_Sql_Batch100_5s";
    private const string InMemorySqlSingleInsert = "Storage_InMemory_Sql_SingleInsert_5s";
    private const string FileBackedDurableWriteOptimized =
        "StoragePlan2_FileBackedDurableWriteOptimized_InsertBatch_B1000_Seed20000_10s";
    private const string HybridSqlSingleInsert =
        "Storage_HybridIncrementalDurable_Sql_SingleInsert_5s";

    [Fact]
    public void ScenarioNames_ExposeEveryExistingRowIncludingAffectedDurableRows()
    {
        IReadOnlyList<string> names = HybridStorageModeBenchmark.ScenarioNames;

        Assert.Equal(30, names.Count);
        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains(FileBackedCollectionPut, names);
        Assert.Contains(FileBackedSqlBatch, names);
        Assert.Contains(FileBackedSqlSingleInsert, names);
        Assert.Contains(FileBackedDurableWriteOptimized, names);
        Assert.Contains(HybridSqlSingleInsert, names);
    }

    [Fact]
    public async Task NamedScenario_RequiresAnExactPublishedRowName()
    {
        string wrongCase = FileBackedSqlSingleInsert.ToLowerInvariant();

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
            () => HybridStorageModeBenchmark.RunNamedScenarioAsync(wrongCase));

        Assert.Contains("exact names", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedQualificationScenario_RealInMemoryPathMeetsBothFloors()
    {
        var settings = new HybridStorageModeBenchmark.QualificationSettings(
            WarmupDuration: TimeSpan.FromMilliseconds(10),
            MinimumMeasuredDuration: TimeSpan.FromMilliseconds(50),
            MinimumLatencySamples: 1,
            MaximumMeasuredDuration: TimeSpan.FromSeconds(2));

        BenchmarkResult result = await HybridStorageModeBenchmark.RunNamedQualificationScenarioAsync(
                InMemorySqlSingleInsert,
                settings)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(InMemorySqlSingleInsert, result.Name);
        Assert.True(result.ElapsedMs >= settings.MinimumMeasuredDuration.TotalMilliseconds);
        Assert.True(result.LatencySamples >= settings.MinimumLatencySamples);
        Assert.Contains("qualification=true", result.ExtraInfo);
    }

    [Fact]
    public async Task NamedQualificationScenario_RealBatchCapRollsBackAndFailsExplicitly()
    {
        var settings = new HybridStorageModeBenchmark.QualificationSettings(
            WarmupDuration: TimeSpan.Zero,
            MinimumMeasuredDuration: TimeSpan.FromMilliseconds(50),
            MinimumLatencySamples: int.MaxValue,
            MaximumMeasuredDuration: TimeSpan.FromMilliseconds(100));

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => HybridStorageModeBenchmark.RunNamedQualificationScenarioAsync(
                    InMemorySqlBatch,
                    settings)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Contains("measurement cap", exception.Message);
        Assert.DoesNotContain("Rollback", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void QualificationDefaults_RequireDurationAndRetainedSampleFloor()
    {
        HybridStorageModeBenchmark.QualificationSettings settings =
            HybridStorageModeBenchmark.DefaultQualificationSettings;

        Assert.Equal(TimeSpan.FromSeconds(2), settings.WarmupDuration);
        Assert.Equal(TimeSpan.FromSeconds(30), settings.MinimumMeasuredDuration);
        Assert.Equal(10_000, settings.MinimumLatencySamples);
        Assert.Equal(TimeSpan.FromSeconds(120), settings.MaximumMeasuredDuration);

        Assert.False(HybridStorageModeBenchmark.HasMetQualificationTarget(
            TimeSpan.FromSeconds(29.999),
            retainedLatencySamples: 10_000,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples));
        Assert.False(HybridStorageModeBenchmark.HasMetQualificationTarget(
            TimeSpan.FromSeconds(30),
            retainedLatencySamples: 9_999,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples));
        Assert.True(HybridStorageModeBenchmark.HasMetQualificationTarget(
            TimeSpan.FromSeconds(30),
            retainedLatencySamples: 10_000,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples));
    }

    [Fact]
    public void ReleaseCoreMeasurementPolicy_PreservesWarmupAndRequiresComparatorSampleFloor()
    {
        HybridStorageModeBenchmark.QualificationSettings settings =
            HybridStorageModeBenchmark.CreateReleaseCoreMeasurementSettings(
                TimeSpan.FromSeconds(5));

        Assert.Equal(TimeSpan.FromSeconds(2), settings.WarmupDuration);
        Assert.Equal(TimeSpan.FromSeconds(5), settings.MinimumMeasuredDuration);
        Assert.Equal(100, settings.MinimumLatencySamples);
        Assert.Equal(TimeSpan.FromSeconds(90), settings.MaximumMeasuredDuration);

        Assert.False(HybridStorageModeBenchmark.HasMetQualificationTarget(
            TimeSpan.FromSeconds(5),
            retainedLatencySamples: 99,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples));
        Assert.True(HybridStorageModeBenchmark.HasMetQualificationTarget(
            TimeSpan.FromSeconds(5),
            retainedLatencySamples: 100,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples));
    }

    [Fact]
    public async Task MeasuredOperation_TargetReachedBeforeDeadline_IsAccepted()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));

        BenchmarkResult result = await HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
            FileBackedSqlSingleInsert,
            settings,
            _ => Task.CompletedTask,
            deadline,
            TimeSpan.FromMilliseconds(25));

        Assert.Equal(1, result.TotalOps);
        Assert.Equal(1, result.LatencySamples);
        Assert.Equal(1_000, result.ElapsedMs);
        Assert.False(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task MeasuredOperation_WallClockSkew_DoesNotChangeRecordedInterval()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        TimeSpan wallClockSkew = TimeSpan.FromHours(3);
        using var deadline = new ManualQualificationDeadline(wallClockSkew);
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));

        BenchmarkResult result = await HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
            FileBackedSqlSingleInsert,
            settings,
            _ => Task.CompletedTask,
            deadline,
            TimeSpan.FromMilliseconds(25));

        Dictionary<string, string> extraInfo = result.ExtraInfo!
            .Split(';', StringSplitOptions.TrimEntries)
            .Select(static token => token.Split('=', 2))
            .ToDictionary(static token => token[0], static token => token[1]);
        DateTimeOffset measurementBeginUtc = DateTimeOffset.ParseExact(
            extraInfo["measurement-begin-utc"],
            "O",
            CultureInfo.InvariantCulture);
        DateTimeOffset measurementEndUtc = DateTimeOffset.ParseExact(
            extraInfo["measurement-end-utc"],
            "O",
            CultureInfo.InvariantCulture);

        Assert.Equal(deadline.StartedUtc, measurementBeginUtc);
        Assert.Equal(deadline.StartedUtc + TimeSpan.FromSeconds(1), measurementEndUtc);
        Assert.NotEqual(deadline.UtcNow, measurementEndUtc);
        Assert.Equal(
            TimeSpan.FromMilliseconds(result.ElapsedMs),
            measurementEndUtc - measurementBeginUtc);
    }

    [Fact]
    public async Task MeasuredOperation_SampleCompletingAfterDeadline_IsRejected()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ =>
                {
                    deadline.AdvanceTo(settings.MaximumMeasuredDuration.Add(TimeSpan.FromTicks(1)), expire: true);
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Contains("measurement cap", exception.Message);
        Assert.Contains("0 retained latency samples", exception.Message);
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task MeasuredOperation_DeadlineCancellationCompletingTask_IsExplicitCap()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ =>
                {
                    deadline.AdvanceTo(settings.MaximumMeasuredDuration, expire: true);
                    return Task.FromCanceled(deadline.Token);
                },
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Contains("measurement cap", exception.Message);
        Assert.Contains("0 retained latency samples", exception.Message);
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task MeasuredOperation_CancellationIgnoringWork_IsBoundedAndExplicit()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();
        var operationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOperation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedWorker = null;

        Task<BenchmarkResult> runTask =
            HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ =>
                {
                    operationStarted.TrySetResult();
                    return releaseOperation.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                task => detachedWorker = task);

        await operationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(settings.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("measurement cap", exception.Message);
            Assert.Contains("did not stop in-flight operation", exception.Message);
            Assert.True(deadline.Token.IsCancellationRequested);
            Assert.NotNull(detachedWorker);
        }
        finally
        {
            releaseOperation.TrySetResult();
            if (detachedWorker is not null)
            {
                await AwaitDetachedWorkerAsync(detachedWorker);
            }
        }
    }

    [Fact]
    public async Task MeasuredOperation_SynchronouslyBlockingWork_IsBoundedAndExplicit()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();
        using var releaseOperation = new ManualResetEventSlim();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var operationExited = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedWorker = null;

        Task<BenchmarkResult> runTask = Task.Run(async () =>
            await HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ =>
                {
                    operationStarted.TrySetResult();
                    releaseOperation.Wait();
                    operationExited.TrySetResult();
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                task => detachedWorker = task));

        await operationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(settings.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("measurement cap", exception.Message);
            Assert.Contains("did not stop in-flight operation", exception.Message);
            Assert.NotNull(detachedWorker);
        }
        finally
        {
            releaseOperation.Set();
            await operationExited.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            if (detachedWorker is not null)
            {
                await AwaitDetachedWorkerAsync(detachedWorker);
            }
        }
    }

    [Fact]
    public async Task Warmup_SynchronouslyBlockingWork_IsBoundedAndExplicit()
    {
        using var deadline = new ManualQualificationDeadline();
        using var releaseOperation = new ManualResetEventSlim();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var operationExited = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedWorker = null;

        Task runTask = Task.Run(async () =>
            await HybridStorageModeBenchmark.RunQualificationWarmupCoreAsync(
                FileBackedSqlSingleInsert,
                _ =>
                {
                    operationStarted.TrySetResult();
                    releaseOperation.Wait();
                    operationExited.TrySetResult();
                    return Task.CompletedTask;
                },
                TimeSpan.FromSeconds(2),
                deadline,
                TimeSpan.FromMilliseconds(25),
                task => detachedWorker = task),
            TestContext.Current.CancellationToken);

        await operationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(TimeSpan.FromSeconds(2), expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("warmup operation", exception.Message);
            Assert.Contains("did not stop", exception.Message);
            Assert.NotNull(detachedWorker);
        }
        finally
        {
            releaseOperation.Set();
            await operationExited.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            if (detachedWorker is not null)
            {
                await AwaitDetachedWorkerAsync(detachedWorker);
            }
        }
    }

    [Fact]
    public async Task MeasuredOperation_DeadlineStartsOnlyAfterScheduledWorkerIsReady()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();
        Func<Task<BenchmarkResult>>? scheduledWorker = null;
        var scheduledCompletion = new TaskCompletionSource<BenchmarkResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<BenchmarkResult> ScheduleWorker(Func<Task<BenchmarkResult>> worker)
        {
            scheduledWorker = worker;
            return scheduledCompletion.Task;
        }

        Task<BenchmarkResult> runTask =
            HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ =>
                {
                    Assert.Equal(1, deadline.StartCount);
                    deadline.AdvanceTo(settings.MinimumMeasuredDuration);
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                scheduleWorker: ScheduleWorker);

        Assert.NotNull(scheduledWorker);
        Assert.Equal(0, deadline.StartCount);
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.Equal(0, deadline.StartCount);
        Assert.False(runTask.IsCompleted);

        Task scheduledExecution = CompleteScheduledWorkerAsync(
            scheduledWorker!,
            scheduledCompletion,
            TestContext.Current.CancellationToken);
        BenchmarkResult result = await runTask.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        await scheduledExecution;

        Assert.Equal(1, deadline.StartCount);
        Assert.Equal(settings.MinimumMeasuredDuration.TotalMilliseconds, result.ElapsedMs);
    }

    [Fact]
    public async Task Warmup_DeadlineStartsOnlyAfterScheduledWorkerIsReady()
    {
        using var deadline = new ManualQualificationDeadline();
        Func<Task>? scheduledWorker = null;
        var scheduledCompletion = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task ScheduleWorker(Func<Task> worker)
        {
            scheduledWorker = worker;
            return scheduledCompletion.Task;
        }

        Task runTask = HybridStorageModeBenchmark.RunQualificationWarmupCoreAsync(
            FileBackedSqlSingleInsert,
            _ =>
            {
                Assert.Equal(1, deadline.StartCount);
                deadline.AdvanceTo(TimeSpan.FromSeconds(2));
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(2),
            deadline,
            TimeSpan.FromMilliseconds(25),
            scheduleWorker: ScheduleWorker);

        Assert.NotNull(scheduledWorker);
        Assert.Equal(0, deadline.StartCount);
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.Equal(0, deadline.StartCount);
        Assert.False(runTask.IsCompleted);

        Task scheduledExecution = Task.Run(
            async () =>
            {
                try
                {
                    await scheduledWorker!();
                    scheduledCompletion.TrySetResult();
                }
                catch (Exception exception)
                {
                    scheduledCompletion.TrySetException(exception);
                }
            },
            TestContext.Current.CancellationToken);
        await runTask.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        await scheduledExecution;

        Assert.Equal(1, deadline.StartCount);
    }

    [Fact]
    public async Task MeasuredOperation_UncoordinatedCancellation_IsPropagated()
    {
        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        using var deadline = new ManualQualificationDeadline();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => HybridStorageModeBenchmark.RunQualificationMeasuredOperationCoreAsync(
                FileBackedSqlSingleInsert,
                settings,
                _ => Task.FromException(new OperationCanceledException("unexpected cancellation")),
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.False(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task ConcurrentPhase_TargetReachedBeforeDeadline_IsAccepted()
    {
        using var deadline = new ManualQualificationDeadline();
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));

        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                async (_, _, recordCompletion, ct) =>
                {
                    recordCompletion?.Invoke(TimeSpan.FromSeconds(1), 0.1);
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        Assert.True(result.Histograms.Sum(static histogram => histogram.SampleCount) >= 1);
        Assert.Equal(TimeSpan.FromSeconds(1), result.Elapsed);
    }

    [Fact]
    public async Task ConcurrentPhase_PreCapTargetTimestampSurvivesDeadlineWake()
    {
        using var deadline = new ManualQualificationDeadline();

        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 1,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                async (_, _, recordCompletion, ct) =>
                {
                    recordCompletion?.Invoke(TimeSpan.FromSeconds(1.9), 0.1);
                    deadline.AdvanceTo(TimeSpan.FromSeconds(2.1), expire: true);
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        Assert.Equal(1, result.Histograms.Sum(static histogram => histogram.SampleCount));
        Assert.Equal(TimeSpan.FromSeconds(1.9), result.Elapsed);
        Assert.Equal(deadline.StartedUtc + TimeSpan.FromSeconds(1.9), result.MeasurementEndedUtc);
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task ConcurrentPhase_PostCapSampleCannotPairWithStaleControllerElapsed()
    {
        using var deadline = new ManualQualificationDeadline();
        deadline.AdvanceTo(TimeSpan.FromSeconds(1.9));
        var readerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReader = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sampleAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> runTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 1,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                async (_, _, recordCompletion, ct) =>
                {
                    readerStarted.TrySetResult();
                    await releaseReader.Task;
                    recordCompletion?.Invoke(TimeSpan.FromSeconds(2.1), 0.1);
                    sampleAttempted.TrySetResult();
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        await readerStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        releaseReader.TrySetResult();
        await sampleAttempted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(TimeSpan.FromSeconds(2.1), expire: true);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runTask.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken));
        Assert.Contains("measurement cap", exception.Message);
        Assert.Contains("0 retained latency samples", exception.Message);
    }

    [Fact]
    public async Task ConcurrentPhase_TargetCutoffExcludesLaterCompletions()
    {
        using var deadline = new ManualQualificationDeadline();
        var secondReaderReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecondReader = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRecordAttempted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        bool secondRecordAccepted = true;

        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                async (readerIndex, _, recordCompletion, ct) =>
                {
                    if (readerIndex == 0)
                    {
                        await secondReaderReady.Task;
                        Assert.True(recordCompletion!(TimeSpan.FromSeconds(1), 0.1));
                        releaseSecondReader.TrySetResult();
                    }
                    else
                    {
                        secondReaderReady.TrySetResult();
                        await releaseSecondReader.Task;
                        secondRecordAccepted = recordCompletion!(
                            TimeSpan.FromSeconds(1.5),
                            0.2);
                        secondRecordAttempted.TrySetResult();
                    }

                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        await secondRecordAttempted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        Assert.False(secondRecordAccepted);
        Assert.Equal(1, result.Histograms.Sum(static histogram => histogram.Count));
        Assert.Equal(1, result.Histograms.Sum(static histogram => histogram.SampleCount));
        Assert.Equal(TimeSpan.FromSeconds(1), result.Elapsed);
        Assert.Equal(deadline.StartedUtc + TimeSpan.FromSeconds(1), result.MeasurementEndedUtc);
    }

    [Fact]
    public async Task ConcurrentPhase_UnexpectedReaderExit_FailsInsteadOfReducingConcurrency()
    {
        using var deadline = new ManualQualificationDeadline();

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                (readerIndex, _, _, ct) => readerIndex == 0
                    ? Task.CompletedTask
                    : WaitForCoordinatedCancellationAsync(ct),
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Contains("exit before coordinated cancellation", exception.Message);
    }

    [Fact]
    public async Task ConcurrentPhase_ReaderExitAtConfiguredCap_IsCoordinatedCompletion()
    {
        TimeSpan phaseDuration = TimeSpan.FromSeconds(2);
        using var deadline = new ManualQualificationDeadline();

        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent-warmup",
                readerCount: 1,
                latencySampleEvery: 1,
                minimumMeasuredDuration: phaseDuration,
                minimumLatencySamples: 0,
                maximumMeasuredDuration: phaseDuration,
                failAtMaximum: false,
                (_, _, recordCompletion, _) =>
                {
                    // Reproduce the production race: the operation completes exactly at the
                    // stopwatch cap before timer cancellation propagates. The reader reports
                    // that boundary through the recording gate before it returns.
                    deadline.AdvanceTo(phaseDuration);
                    Assert.False(recordCompletion!(deadline.Elapsed, null));
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        Assert.Equal(phaseDuration, result.Elapsed);
        Assert.Equal(0, result.Histograms.Sum(static histogram => histogram.SampleCount));
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task ConcurrentPhase_CancellationCallbackCompletionIsCoordinated()
    {
        TimeSpan phaseDuration = TimeSpan.FromSeconds(2);
        using var deadline = new ManualQualificationDeadline();
        var readerStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent-cancellation-callback",
                readerCount: 1,
                latencySampleEvery: 1,
                minimumMeasuredDuration: phaseDuration,
                minimumLatencySamples: 0,
                maximumMeasuredDuration: phaseDuration,
                failAtMaximum: false,
                (_, _, _, ct) =>
                {
                    var completion = new TaskCompletionSource();
                    _ = ct.Register(() => completion.TrySetResult());
                    readerStarted.TrySetResult();
                    return completion.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        await readerStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(phaseDuration, expire: true);

        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await phaseTask.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);

        Assert.Equal(phaseDuration, result.Elapsed);
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task ConcurrentPhase_PreCapReturnRemainsUnexpectedAfterDeadlineAdvances()
    {
        TimeSpan phaseDuration = TimeSpan.FromSeconds(2);
        using var deadline = new ManualQualificationDeadline();
        var readerStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReader = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent-premature-return",
                readerCount: 1,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: phaseDuration,
                failAtMaximum: true,
                (_, _, _, _) =>
                {
                    readerStarted.TrySetResult();
                    return releaseReader.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        await readerStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));
        releaseReader.TrySetResult();
        deadline.AdvanceTo(phaseDuration, expire: true);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => phaseTask.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken));

        Assert.Contains("exit before coordinated cancellation", exception.Message);
    }

    [Fact]
    public async Task ConcurrentPhase_ReaderFailure_IsPropagated()
    {
        using var deadline = new ManualQualificationDeadline();

        ApplicationException exception = await Assert.ThrowsAsync<ApplicationException>(
            () => HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                (readerIndex, _, _, ct) => readerIndex == 0
                    ? Task.FromException(new ApplicationException("reader sentinel"))
                    : WaitForCoordinatedCancellationAsync(ct),
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Equal("reader sentinel", exception.Message);
    }

    [Fact]
    public async Task ConcurrentPhase_CancellationIgnoringReaders_AreBoundedAndExplicit()
    {
        using var deadline = new ManualQualificationDeadline();
        var readersStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReaders = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedReaders = null;
        int startedCount = 0;

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> runTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                (_, _, _, _) =>
                {
                    if (Interlocked.Increment(ref startedCount) == 2)
                        readersStarted.TrySetResult();
                    return releaseReaders.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                detachedWorkRegistrar: task => detachedReaders = task);

        await readersStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(TimeSpan.FromSeconds(2), expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("measurement cap", exception.Message);
            Assert.Contains("did not stop 2 concurrent reader(s)", exception.Message);
            Assert.NotNull(detachedReaders);
        }
        finally
        {
            releaseReaders.TrySetResult();
            if (detachedReaders is not null)
            {
                await AwaitDetachedWorkerAsync(detachedReaders);
            }
        }
    }

    [Fact]
    public async Task ConcurrentPhase_UnresponsiveReaderPreservesCompletedReaderFailure()
    {
        using var deadline = new ManualQualificationDeadline();
        var readersStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failReader = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReader = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedReaders = null;
        int startedCount = 0;

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> runTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent-failure-and-timeout",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                (readerIndex, _, _, _) =>
                {
                    if (Interlocked.Increment(ref startedCount) == 2)
                        readersStarted.TrySetResult();
                    return readerIndex == 0 ? failReader.Task : releaseReader.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                detachedWorkRegistrar: task => detachedReaders = task);

        await readersStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(TimeSpan.FromSeconds(2), expire: true);
        failReader.TrySetException(new ApplicationException("hybrid reader sentinel"));
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("did not stop 1 concurrent reader(s)", exception.Message);
            Assert.Contains("hybrid reader sentinel", exception.ToString());
            Assert.NotNull(detachedReaders);
        }
        finally
        {
            releaseReader.TrySetResult();
            if (detachedReaders is not null)
            {
                try
                {
                    await AwaitDetachedWorkerAsync(detachedReaders);
                }
                catch (ApplicationException exception)
                {
                    Assert.Equal("hybrid reader sentinel", exception.Message);
                }
            }
        }
    }

    [Fact]
    public async Task ConcurrentPhase_PreStartReaderExitHasBoundedDrain()
    {
        using var deadline = new ManualQualificationDeadline();
        var releaseScheduledReader = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedReaders = null;
        int scheduledReaderCount = 0;

        Task ScheduleReader(Func<Task> _)
            => Interlocked.Increment(ref scheduledReaderCount) == 1
                ? Task.CompletedTask
                : releaseScheduledReader.Task;

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> runTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent-pre-start",
                readerCount: 2,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                (_, _, _, ct) => WaitForCoordinatedCancellationAsync(ct),
                deadline,
                TimeSpan.FromMilliseconds(25),
                ScheduleReader,
                task => detachedReaders = task);

        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("before measurement start", exception.Message);
            Assert.NotNull(detachedReaders);
            Assert.Equal(0, deadline.StartCount);
        }
        finally
        {
            releaseScheduledReader.TrySetResult();
            if (detachedReaders is not null)
            {
                await AwaitDetachedWorkerAsync(detachedReaders);
            }
        }
    }

    [Fact]
    public async Task ConcurrentPhase_DeadlineStartsOnlyAfterEveryScheduledReaderIsReady()
    {
        const int readerCount = 2;
        using var deadline = new ManualQualificationDeadline();
        var scheduledReaders = new List<Func<Task>>();
        var scheduledCompletions = new List<TaskCompletionSource>();
        var scheduledReady = new List<TaskCompletionSource>();

        Task ScheduleReader(Func<Task> reader)
        {
            var completion = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var ready = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            scheduledReaders.Add(
                async () =>
                {
                    try
                    {
                        Task readerTask = reader();
                        ready.TrySetResult();
                        await readerTask;
                        completion.TrySetResult();
                    }
                    catch (Exception exception)
                    {
                        completion.TrySetException(exception);
                    }
                });
            scheduledCompletions.Add(completion);
            scheduledReady.Add(ready);
            return completion.Task;
        }

        Task<HybridStorageModeBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            HybridStorageModeBenchmark.RunConcurrentReaderPhaseCoreAsync(
                "qualified-concurrent",
                readerCount,
                latencySampleEvery: 1,
                minimumMeasuredDuration: TimeSpan.FromSeconds(1),
                minimumLatencySamples: 1,
                maximumMeasuredDuration: TimeSpan.FromSeconds(2),
                failAtMaximum: true,
                async (_, _, recordCompletion, ct) =>
                {
                    recordCompletion!(TimeSpan.FromSeconds(1), 0.1);
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                ScheduleReader);

        Assert.Equal(readerCount, scheduledReaders.Count);
        Assert.Equal(0, deadline.StartCount);

        Task firstReader = Task.Run(
            scheduledReaders[0],
            TestContext.Current.CancellationToken);
        await scheduledReady[0].Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        Assert.Equal(0, deadline.StartCount);
        Assert.False(phaseTask.IsCompleted);

        Task secondReader = Task.Run(
            scheduledReaders[1],
            TestContext.Current.CancellationToken);
        await scheduledReady[1].Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        HybridStorageModeBenchmark.ConcurrentReaderPhaseResult result =
            await phaseTask.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
        await Task.WhenAll(firstReader, secondReader);

        Assert.Equal(1, deadline.StartCount);
        Assert.Equal(TimeSpan.FromSeconds(1), result.Elapsed);
        Assert.All(
            scheduledCompletions,
            static completion => Assert.True(completion.Task.IsCompletedSuccessfully));
    }

    [Fact]
    public async Task LegacyConcurrentPhase_DelayedSchedulingCannotCancelReadersBeforeTheyStart()
    {
        const int readerCount = 2;
        TimeSpan measuredDuration = TimeSpan.FromMilliseconds(250);
        var scheduledReaders = new List<Func<Task>>();
        var scheduledCompletions = new List<TaskCompletionSource>();
        int enteredReaderCount = 0;

        Task ScheduleReader(Func<Task> reader)
        {
            var completion = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            scheduledReaders.Add(
                async () =>
                {
                    try
                    {
                        await reader();
                        completion.TrySetResult();
                    }
                    catch (Exception exception)
                    {
                        completion.TrySetException(exception);
                    }
                });
            scheduledCompletions.Add(completion);
            return completion.Task;
        }

        Task phaseTask = HybridStorageModeBenchmark.RunLegacyConcurrentReaderWorkersAsync(
            readerCount,
            measuredDuration,
            async (_, ct) =>
            {
                Assert.False(
                    ct.IsCancellationRequested,
                    "Measurement cancellation started before every reader entered.");
                Interlocked.Increment(ref enteredReaderCount);
                await WaitForCoordinatedCancellationAsync(ct);
            },
            ScheduleReader);

        Assert.Equal(readerCount, scheduledReaders.Count);
        await Task.Delay(
            measuredDuration + TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(phaseTask.IsCompleted);

        Task[] startedReaders = scheduledReaders
            .Select(static reader => Task.Run(reader))
            .ToArray();
        await phaseTask.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);
        await Task.WhenAll(startedReaders);

        Assert.Equal(readerCount, enteredReaderCount);
        Assert.All(
            scheduledCompletions,
            static completion => Assert.True(completion.Task.IsCompletedSuccessfully));
    }

    [Fact]
    public void ConcurrentPath_DefaultsToReleaseCoreQualificationWhileReadPrimingRemainsCompatible()
    {
        Assert.Equal(
            HybridStorageModeBenchmark.ConcurrentExecutionPath.Qualification,
            HybridStorageModeBenchmark.GetConcurrentExecutionPath(qualificationSettings: null));
        Assert.True(HybridStorageModeBenchmark.UsesLegacyReadPriming(qualificationSettings: null));

        HybridStorageModeBenchmark.QualificationSettings settings = CreateFastSettings();
        Assert.Equal(
            HybridStorageModeBenchmark.ConcurrentExecutionPath.Qualification,
            HybridStorageModeBenchmark.GetConcurrentExecutionPath(settings));
        Assert.False(HybridStorageModeBenchmark.UsesLegacyReadPriming(settings));
    }

    [Fact]
    public void QualificationCapDiagnostic_ReportsTheMissingEvidenceExplicitly()
    {
        InvalidOperationException exception =
            HybridStorageModeBenchmark.CreateQualificationCapException(
                FileBackedSqlSingleInsert,
                maximumMeasuredDuration: TimeSpan.FromSeconds(120),
                elapsed: TimeSpan.FromSeconds(120),
                retainedLatencySamples: 8_500,
                minimumMeasuredDuration: TimeSpan.FromSeconds(30),
                minimumLatencySamples: 10_000);

        Assert.Contains("120-second measurement cap", exception.Message);
        Assert.Contains("8,500 retained latency samples", exception.Message);
        Assert.Contains("10,000 retained latency samples", exception.Message);
    }

    [Fact]
    public void QualificationExtraInfo_PersistsRoundTripMeasurementTimestamps()
    {
        DateTimeOffset measurementStartedUtc =
            new(2026, 7, 31, 19, 30, 0, TimeSpan.Zero);
        DateTimeOffset measurementEndedUtc = measurementStartedUtc.AddSeconds(45);

        string extraInfo = HybridStorageModeBenchmark.CreateQualificationExtraInfo(
            HybridStorageModeBenchmark.DefaultQualificationSettings,
            measurementStartedUtc,
            measurementEndedUtc);

        Assert.Contains($"measurement-begin-utc={measurementStartedUtc:O}", extraInfo);
        Assert.Contains($"measurement-end-utc={measurementEndedUtc:O}", extraInfo);
    }

    [Fact]
    public void WarmupSingleSampleValidation_AllowsNamedHybridQualification()
    {
        BenchmarkProgram.ValidateWarmupSingleSampleOption(
            "--hybrid-storage-mode-scenario",
            repeatCount: 1,
            warmupSingleSample: true);
    }

    [Fact]
    public async Task ScenarioCliPath_UsesInternalWarmupAndWritesOneFixedPrefixCsv()
    {
        string temporaryRoot = CreateTemporaryDirectory();
        int invocationCount = 0;
        try
        {
            await BenchmarkProgram.RunHybridStorageModeScenarioWithRepeatsAsync(
                FileBackedSqlSingleInsert,
                repeatCount: 1,
                warmupSingleSample: true,
                outputDirectory: temporaryRoot,
                runScenarioAsync: scenarioName =>
                {
                    invocationCount++;
                    return Task.FromResult(CreateResult(scenarioName));
                });

            Assert.Equal(1, invocationCount);
            string csvPath = Assert.Single(Directory.GetFiles(temporaryRoot, "*.csv"));
            Assert.StartsWith(
                "hybrid-storage-mode-scenario-",
                Path.GetFileName(csvPath),
                StringComparison.Ordinal);

            string[] lines = File.ReadAllLines(csvPath);
            Assert.Equal(2, lines.Length);
            Assert.StartsWith(FileBackedSqlSingleInsert + ",", lines[1], StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(temporaryRoot, recursive: true);
        }
    }

    private static BenchmarkResult CreateResult(string scenarioName)
        => new()
        {
            Name = scenarioName,
            TotalOps = 10_000,
            LatencySamples = 10_000,
            ElapsedMs = 30_000,
            P99Ms = 5,
        };

    private static Task CompleteScheduledWorkerAsync(
        Func<Task<BenchmarkResult>> scheduledWorker,
        TaskCompletionSource<BenchmarkResult> scheduledCompletion,
        CancellationToken cancellationToken)
        => Task.Run(async () =>
        {
            try
            {
                scheduledCompletion.TrySetResult(await scheduledWorker());
            }
            catch (Exception exception)
            {
                scheduledCompletion.TrySetException(exception);
            }
        }, cancellationToken);

    private static HybridStorageModeBenchmark.QualificationSettings CreateFastSettings()
        => new(
            WarmupDuration: TimeSpan.Zero,
            MinimumMeasuredDuration: TimeSpan.FromSeconds(1),
            MinimumLatencySamples: 1,
            MaximumMeasuredDuration: TimeSpan.FromSeconds(2));

    private static async Task WaitForCoordinatedCancellationAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
    }

    private static async Task AwaitDetachedWorkerAsync(Task worker)
    {
        try
        {
            await worker.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Coordinated deadline cancellation is the expected detached-worker exit.
        }
    }

    private sealed class ManualQualificationDeadline :
        HybridStorageModeBenchmark.IQualificationDeadline
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly TaskCompletionSource _expired =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TimeSpan _utcNowSkew;
        private long _elapsedTicks;

        internal ManualQualificationDeadline(TimeSpan utcNowSkew = default)
        {
            StartedUtc = new DateTimeOffset(2026, 7, 31, 19, 30, 0, TimeSpan.Zero);
            _utcNowSkew = utcNowSkew;
        }

        public TimeSpan Elapsed => TimeSpan.FromTicks(Interlocked.Read(ref _elapsedTicks));
        public DateTimeOffset StartedUtc { get; }
        public DateTimeOffset UtcNow => StartedUtc + Elapsed + _utcNowSkew;
        public CancellationToken Token => _cts.Token;
        public Task Expired => _expired.Task;
        public int StartCount => Volatile.Read(ref _startCount);

        private int _startCount;

        public void Start()
        {
            Interlocked.CompareExchange(ref _startCount, 1, 0);
        }

        internal void AdvanceTo(TimeSpan elapsed, bool expire = false)
        {
            Interlocked.Exchange(ref _elapsedTicks, elapsed.Ticks);
            if (expire)
            {
                Cancel();
                _expired.TrySetResult();
            }
        }

        public void Cancel()
        {
            if (!_cts.IsCancellationRequested)
                _cts.Cancel();
        }

        public void Dispose() => _cts.Dispose();
    }

    private static string CreateTemporaryDirectory()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-hybrid-qualification-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
