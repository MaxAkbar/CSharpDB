using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;

namespace CSharpDB.Benchmarks.Tests;

public sealed class SqliteComparisonBenchmarkTests
{
    [Fact]
    public void ReleaseCorePolicy_RequiresFiveSecondsAndOneHundredRealSamples()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy =
            SqliteComparisonBenchmark.DefaultMeasurementPolicy;

        Assert.Equal(TimeSpan.FromSeconds(2), policy.WarmupDuration);
        Assert.Equal(TimeSpan.FromSeconds(5), policy.MinimumMeasuredDuration);
        Assert.Equal(100, policy.MinimumLatencySamples);
        Assert.Equal(TimeSpan.FromSeconds(90), policy.MaximumMeasuredDuration);
        Assert.Contains(
            "SQLite_WalFull_Sql_PreparedBulk4Col_B10000_5s",
            SqliteComparisonBenchmark.ReleaseCoreScenarioNames);

        Assert.False(SqliteComparisonBenchmark.HasMetMeasurementTarget(
            TimeSpan.FromSeconds(4.999),
            retainedLatencySamples: 100,
            policy));
        Assert.False(SqliteComparisonBenchmark.HasMetMeasurementTarget(
            TimeSpan.FromSeconds(5),
            retainedLatencySamples: 99,
            policy));
        Assert.True(SqliteComparisonBenchmark.HasMetMeasurementTarget(
            TimeSpan.FromSeconds(5),
            retainedLatencySamples: 100,
            policy));
    }

    [Fact]
    public void MeasurementStopArbiter_PreservesEarlierTargetPublishedAfterDeadline()
    {
        var arbiter = new SqliteComparisonBenchmark.MeasurementStopArbiter();
        var deadlineDecision = new SqliteComparisonBenchmark.MeasurementStopDecision(
            SqliteComparisonBenchmark.ConcurrentStopReason.Deadline,
            TimeSpan.FromSeconds(90),
            RetainedLatencySamples: 500);
        var earlierTargetDecision = new SqliteComparisonBenchmark.MeasurementStopDecision(
            SqliteComparisonBenchmark.ConcurrentStopReason.TargetReached,
            TimeSpan.FromSeconds(5),
            RetainedLatencySamples: 100);

        arbiter.Publish(deadlineDecision);
        arbiter.Publish(earlierTargetDecision);

        Assert.True(arbiter.Signal.IsCompletedSuccessfully);
        Assert.Equal(earlierTargetDecision, arbiter.Decision);
    }

    [Fact]
    public void FinalWorkerStopDecision_DoesNotMaskPrematureReaderExit()
    {
        var arbiter = new SqliteComparisonBenchmark.MeasurementStopArbiter();
        arbiter.Publish(new SqliteComparisonBenchmark.MeasurementStopDecision(
            SqliteComparisonBenchmark.ConcurrentStopReason.TargetReached,
            TimeSpan.FromSeconds(5),
            RetainedLatencySamples: 100));

        SqliteComparisonBenchmark.MeasurementStopDecision? finalDecision =
            SqliteComparisonBenchmark.GetFinalWorkerStopDecision(
                SqliteComparisonBenchmark.ConcurrentStopReason.ReaderExited,
                arbiter);

        Assert.Null(finalDecision);
    }

    [Fact]
    public void ConcurrentReaderExitCoordinator_PreservesEventOrdering()
    {
        int unexpectedExitCount = 0;
        var unexpectedFirst = new SqliteComparisonBenchmark.ConcurrentReaderExitCoordinator(
            () => unexpectedExitCount++);
        var completedReader = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        completedReader.TrySetResult();
        unexpectedFirst.AttachReaderTask(completedReader.Task);
        unexpectedFirst.MarkCoordinatedExit();

        var coordinatedFirst = new SqliteComparisonBenchmark.ConcurrentReaderExitCoordinator(
            () => unexpectedExitCount++);
        var runningReader = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        coordinatedFirst.AttachReaderTask(runningReader.Task);
        coordinatedFirst.MarkCoordinatedExit();
        runningReader.TrySetResult();
        coordinatedFirst.MarkReaderCompleted();

        Assert.Equal(1, unexpectedExitCount);
    }

    [Fact]
    public async Task SequentialMeasurement_ContinuesUntilDurationAndRealSampleTargetsAreMet()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy(minimumSamples: 3);
        using var deadline = new ManualMeasurementDeadline();
        int operationCount = 0;

        BenchmarkResult result = await SqliteComparisonBenchmark.RunSequentialScenarioCoreAsync(
            "sqlite-adaptive",
            _ =>
            {
                operationCount++;
                deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                return Task.CompletedTask;
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25));

        Assert.True(operationCount >= 3);
        Assert.True(result.TotalOps >= 3);
        Assert.Equal(result.TotalOps, result.LatencySamples);
        Assert.InRange(operationCount - result.TotalOps, 0, 1);
        Assert.Equal(policy.MinimumMeasuredDuration.TotalMilliseconds, result.ElapsedMs);
    }

    [Fact]
    public async Task SequentialMeasurement_FastWorkerStopsAtItsCapturedTarget()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy(minimumSamples: 3);
        using var deadline = new ManualMeasurementDeadline();
        int operationCount = 0;

        BenchmarkResult result = await SqliteComparisonBenchmark.RunSequentialScenarioCoreAsync(
            "sqlite-worker-target",
            _ =>
            {
                int completedOperations = Interlocked.Increment(ref operationCount);
                if (completedOperations == policy.MinimumLatencySamples)
                    deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                return Task.CompletedTask;
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25)).WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);

        Assert.Equal(policy.MinimumLatencySamples, operationCount);
        Assert.Equal(policy.MinimumLatencySamples, result.TotalOps);
        Assert.Equal(policy.MinimumLatencySamples, result.LatencySamples);
        Assert.Equal(policy.MinimumMeasuredDuration.TotalMilliseconds, result.ElapsedMs);
    }

    [Fact]
    public async Task SequentialMeasurement_UnresponsiveOperationIsBoundedAndExplicit()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOperation = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedTask = null;

        Task<BenchmarkResult> runTask = SqliteComparisonBenchmark.RunSequentialScenarioCoreAsync(
            "sqlite-sequential-unresponsive",
            _ =>
            {
                operationStarted.TrySetResult();
                return releaseOperation.Task;
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25),
            detachedWorkRegistrar: task => detachedTask = task);

        await operationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("90-second measurement cap", exception.Message);
            Assert.Contains("0 retained latency samples", exception.Message);
            Assert.Contains("did not stop scenario worker", exception.Message);
        }
        finally
        {
            releaseOperation.TrySetResult();
            if (detachedTask is not null)
            {
                await detachedTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact]
    public async Task SequentialMeasurement_SynchronousBlockIsBoundedAndQuarantined()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        using var operationStarted = new ManualResetEventSlim();
        using var releaseOperation = new ManualResetEventSlim();
        Task? quarantinedTask = null;

        Task<BenchmarkResult> runTask = SqliteComparisonBenchmark.RunSequentialScenarioCoreAsync(
            "sqlite-synchronous-block",
            _ =>
            {
                operationStarted.Set();
                releaseOperation.Wait();
                return Task.CompletedTask;
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25),
            task => quarantinedTask = task);

        Assert.True(operationStarted.Wait(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken));
        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("did not stop scenario worker", exception.Message);
            Assert.NotNull(quarantinedTask);
            Assert.False(quarantinedTask.IsCompleted);
        }
        finally
        {
            releaseOperation.Set();
            if (quarantinedTask is not null)
            {
                await quarantinedTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact]
    public async Task Warmup_SynchronousBlockIsBounded()
    {
        using var deadline = new ManualMeasurementDeadline();
        using var operationStarted = new ManualResetEventSlim();
        using var releaseOperation = new ManualResetEventSlim();
        Task? detachedTask = null;

        Task warmupTask = SqliteComparisonBenchmark.RunWarmupCoreAsync(
            "sqlite-warmup-synchronous-block",
            _ =>
            {
                operationStarted.Set();
                releaseOperation.Wait();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(2),
            deadline,
            TimeSpan.FromMilliseconds(25),
            detachedWorkRegistrar: task => detachedTask = task);

        Assert.True(operationStarted.Wait(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken));
        deadline.AdvanceTo(TimeSpan.FromSeconds(2), expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => warmupTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("did not stop warmup worker", exception.Message);
        }
        finally
        {
            releaseOperation.Set();
            if (detachedTask is not null)
            {
                await detachedTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact]
    public async Task Warmup_FastWorkerStopsAtDurationWithoutTimerExpiration()
    {
        TimeSpan warmupDuration = TimeSpan.FromSeconds(2);
        using var deadline = new ManualMeasurementDeadline();
        int operationCount = 0;

        await SqliteComparisonBenchmark.RunWarmupCoreAsync(
            "sqlite-worker-warmup",
            _ =>
            {
                Interlocked.Increment(ref operationCount);
                deadline.AdvanceTo(warmupDuration);
                return Task.CompletedTask;
            },
            warmupDuration,
            deadline,
            TimeSpan.FromMilliseconds(25)).WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);

        Assert.Equal(1, operationCount);
        Assert.False(deadline.Expired.IsCompleted);
    }

    [Fact]
    public async Task SequentialMeasurement_PreservesSynchronousWorkerFailure()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => SqliteComparisonBenchmark.RunSequentialScenarioCoreAsync(
                "sqlite-synchronous-failure",
                static _ => throw new InvalidOperationException("operation failed before cap"),
                policy,
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Equal("operation failed before cap", exception.Message);
    }

    [Fact]
    public async Task ConcurrentReaders_DelayedSchedulingStartsEveryWorkerBeforeMeasurement()
    {
        const int readerCount = 2;
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy(readerCount);
        using var deadline = new ManualMeasurementDeadline();
        var scheduledReaders = new List<Func<Task>>();
        var scheduledCompletions = new List<TaskCompletionSource>();
        var scheduledReady = new List<TaskCompletionSource>();
        int enteredReaderCount = 0;

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

        Task<SqliteComparisonBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-delayed-readers",
                readerCount,
                policy,
                async (_, tryRecord, ct) =>
                {
                    Assert.False(
                        ct.IsCancellationRequested,
                        "Measurement cancellation started before every reader entered.");
                    Assert.True(tryRecord(
                        static () => { },
                        retainsLatencySample: true).Accepted);
                    if (Interlocked.Increment(ref enteredReaderCount) == readerCount)
                        deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                ScheduleReader);

        Assert.Equal(readerCount, scheduledReaders.Count);
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
        SqliteComparisonBenchmark.ConcurrentReaderPhaseResult result = await phaseTask.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);
        await Task.WhenAll(firstReader, secondReader);

        Assert.Equal(1, deadline.StartCount);
        Assert.Equal(readerCount, enteredReaderCount);
        Assert.Equal(readerCount, result.RetainedLatencySamples);
        Assert.Equal(policy.MinimumMeasuredDuration, result.Elapsed);
        Assert.All(
            scheduledCompletions,
            static completion => Assert.True(completion.Task.IsCompletedSuccessfully));
    }

    [Fact]
    public async Task ConcurrentReaders_DoNotPadRetainedSampleCount()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy(minimumSamples: 3);
        using var deadline = new ManualMeasurementDeadline();
        var firstTwoSamplesRetained = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var retainFinalSample = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<SqliteComparisonBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-real-samples",
                readerCount: 1,
                policy,
                async (_, tryRecord, ct) =>
                {
                    Assert.True(tryRecord(
                        static () => { },
                        retainsLatencySample: true).Accepted);
                    Assert.True(tryRecord(
                        static () => { },
                        retainsLatencySample: true).Accepted);
                    deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                    firstTwoSamplesRetained.TrySetResult();
                    await retainFinalSample.Task;
                    SqliteComparisonBenchmark.ConcurrentRecordResult finalRecord = tryRecord(
                        static () => { },
                        retainsLatencySample: true);
                    Assert.True(finalRecord.Accepted);
                    Assert.False(finalRecord.ShouldContinue);
                    await WaitForCoordinatedCancellationAsync(ct);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        await firstTwoSamplesRetained.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        Assert.False(phaseTask.IsCompleted);

        retainFinalSample.TrySetResult();
        SqliteComparisonBenchmark.ConcurrentReaderPhaseResult result = await phaseTask.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);

        Assert.Equal(3, result.RetainedLatencySamples);
    }

    [Fact]
    public async Task ConcurrentReaders_FastWorkerStopsAtItsCapturedTarget()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        int recordAttempts = 0;

        SqliteComparisonBenchmark.ConcurrentReaderPhaseResult result =
            await SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-concurrent-worker-target",
                readerCount: 1,
                policy,
                (_, tryRecord, _) =>
                {
                    while (true)
                    {
                        int attempt = Interlocked.Increment(ref recordAttempts);
                        if (attempt == 1)
                            deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                        if (!tryRecord(
                                static () => { },
                                retainsLatencySample: true).ShouldContinue)
                            return Task.CompletedTask;
                    }
                },
                deadline,
                TimeSpan.FromMilliseconds(25)).WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);

        Assert.Equal(1, recordAttempts);
        Assert.Equal(1, result.RetainedLatencySamples);
        Assert.Equal(policy.MinimumMeasuredDuration, result.Elapsed);
    }

    [Fact]
    public async Task ConcurrentReaders_CapBeforeRecordRejectsPostCapMutation()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        int recordedMutations = 0;

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-concurrent-worker-cap",
                readerCount: 1,
                policy,
                (_, tryRecord, _) =>
                {
                    deadline.AdvanceTo(policy.MaximumMeasuredDuration);
                    SqliteComparisonBenchmark.ConcurrentRecordResult recordResult = tryRecord(
                        () => Interlocked.Increment(ref recordedMutations),
                        retainsLatencySample: true);
                    Assert.False(recordResult.Accepted);
                    Assert.False(recordResult.ShouldContinue);
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25)).WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

        Assert.Contains("90-second measurement cap", exception.Message);
        Assert.Contains("0 retained latency samples", exception.Message);
        Assert.Equal(0, Volatile.Read(ref recordedMutations));
    }

    [Fact]
    public async Task ConcurrentReaders_PreCapCompletionWinsWhenRecordingCrossesCap()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();

        SqliteComparisonBenchmark.ConcurrentReaderPhaseResult result =
            await SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-concurrent-pre-cap-completion",
                readerCount: 1,
                policy,
                (_, tryRecord, _) =>
                {
                    deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                    SqliteComparisonBenchmark.ConcurrentRecordResult recordResult = tryRecord(
                        () => deadline.AdvanceTo(
                            policy.MaximumMeasuredDuration,
                            expire: true),
                        retainsLatencySample: true);
                    Assert.True(recordResult.Accepted);
                    Assert.False(recordResult.ShouldContinue);
                    return Task.CompletedTask;
                },
                deadline,
                TimeSpan.FromMilliseconds(25)).WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);

        Assert.Equal(policy.MinimumMeasuredDuration, result.Elapsed);
        Assert.Equal(1, result.RetainedLatencySamples);
    }

    [Fact]
    public async Task ConcurrentReaders_RecordingGateRejectsPostCancellationMutation()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var postCancellationAttempt = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int recordedMutations = 0;

        Task<SqliteComparisonBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-recording-cutoff",
                readerCount: 1,
                policy,
                async (_, tryRecord, ct) =>
                {
                    Assert.True(tryRecord(
                        () => Interlocked.Increment(ref recordedMutations),
                        retainsLatencySample: true).Accepted);
                    deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                    await WaitForCoordinatedCancellationAsync(ct);
                    bool accepted = tryRecord(
                        () => Interlocked.Increment(ref recordedMutations),
                        retainsLatencySample: true).Accepted;
                    postCancellationAttempt.TrySetResult(accepted);
                },
                deadline,
                TimeSpan.FromMilliseconds(25));

        SqliteComparisonBenchmark.ConcurrentReaderPhaseResult result = await phaseTask.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);
        bool postCancellationRecordAccepted = await postCancellationAttempt.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);

        Assert.False(postCancellationRecordAccepted);
        Assert.Equal(1, Volatile.Read(ref recordedMutations));
        Assert.Equal(1, result.RetainedLatencySamples);
    }

    [Fact]
    public async Task ConcurrentReaders_CancellationIgnoringWorkerIsBoundedAndExplicit()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var releaseWorker = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedTask = null;

        Task<SqliteComparisonBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-unresponsive-reader",
                readerCount: 1,
                policy,
                (_, tryRecord, _) =>
                {
                    Assert.True(tryRecord(
                        static () => { },
                        retainsLatencySample: true).Accepted);
                    deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                    return releaseWorker.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                detachedWorkRegistrar: task => detachedTask = task);

        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => phaseTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("sqlite-unresponsive-reader", exception.Message);
            Assert.Contains("did not stop 1 reader worker", exception.Message);
            Assert.Contains("0.025 seconds", exception.Message);
            Assert.DoesNotContain("measurement cap", exception.ToString());
        }
        finally
        {
            releaseWorker.TrySetResult();
            if (detachedTask is not null)
            {
                await detachedTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact]
    public async Task ConcurrentReaders_TimeoutPreservesCompletedWorkerFailureAndCap()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var releaseWorker = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedTask = null;

        Task<SqliteComparisonBenchmark.ConcurrentReaderPhaseResult> phaseTask =
            SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-timeout-with-reader-failure",
                readerCount: 2,
                policy,
                (readerIndex, _, _) =>
                {
                    if (readerIndex == 0)
                    {
                        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);
                        return Task.FromException(
                            new ApplicationException("sqlite reader sentinel"));
                    }

                    return releaseWorker.Task;
                },
                deadline,
                TimeSpan.FromMilliseconds(25),
                detachedWorkRegistrar: task => detachedTask = task);

        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => phaseTask.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));

            Assert.Contains("did not stop 1 reader worker", exception.Message);
            Assert.Contains("sqlite reader sentinel", exception.ToString());
            Assert.Contains("90-second measurement cap", exception.ToString());
        }
        finally
        {
            releaseWorker.TrySetResult();
            if (detachedTask is not null)
            {
                try
                {
                    await detachedTask.WaitAsync(
                        TimeSpan.FromSeconds(1),
                        TestContext.Current.CancellationToken);
                }
                catch (ApplicationException exception)
                {
                    Assert.Equal("sqlite reader sentinel", exception.Message);
                }
            }
        }
    }

    [Fact]
    public async Task ConcurrentReaders_PreserveWorkerFailures()
    {
        SqliteComparisonBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => SqliteComparisonBenchmark.RunConcurrentReaderWorkersAsync(
                "sqlite-reader-failure",
                readerCount: 1,
                policy,
                static (_, _, _) => throw new InvalidOperationException("reader failed"),
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Equal("reader failed", exception.Message);
    }

    private static SqliteComparisonBenchmark.MeasurementPolicy CreateFastPolicy(
        int minimumSamples = 1)
        => new(
            WarmupDuration: TimeSpan.Zero,
            MinimumMeasuredDuration: TimeSpan.FromSeconds(5),
            MinimumLatencySamples: minimumSamples,
            MaximumMeasuredDuration: TimeSpan.FromSeconds(90));

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

    private sealed class ManualMeasurementDeadline :
        SqliteComparisonBenchmark.IMeasurementDeadline
    {
        private readonly object _gate = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly TaskCompletionSource _expired =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly List<(TimeSpan Target, TaskCompletionSource Completion)> _waiters = [];
        private long _elapsedTicks;
        private bool _started;
        private int _startCount;

        public TimeSpan Elapsed => TimeSpan.FromTicks(Interlocked.Read(ref _elapsedTicks));
        public CancellationToken Token => _cts.Token;
        public Task Expired => _expired.Task;
        internal int StartCount => Volatile.Read(ref _startCount);

        public void Start()
        {
            if (_started)
                throw new InvalidOperationException("Manual deadline already started.");
            _started = true;
            Interlocked.Increment(ref _startCount);
        }

        public Task WaitUntilAsync(TimeSpan elapsed)
        {
            if (!_started)
                throw new InvalidOperationException("Manual deadline has not started.");
            if (Elapsed >= elapsed)
                return Task.CompletedTask;

            lock (_gate)
            {
                if (Elapsed >= elapsed)
                    return Task.CompletedTask;

                var completion = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((elapsed, completion));
                return completion.Task;
            }
        }

        internal void AdvanceTo(TimeSpan elapsed, bool expire = false)
        {
            if (!_started)
                throw new InvalidOperationException("Manual deadline has not started.");

            Interlocked.Exchange(ref _elapsedTicks, elapsed.Ticks);
            List<TaskCompletionSource> ready;
            lock (_gate)
            {
                ready = _waiters
                    .Where(waiter => waiter.Target <= elapsed)
                    .Select(static waiter => waiter.Completion)
                    .ToList();
                _waiters.RemoveAll(waiter => waiter.Target <= elapsed);
            }

            foreach (TaskCompletionSource completion in ready)
                completion.TrySetResult();

            if (expire)
            {
                if (!_cts.IsCancellationRequested)
                    _cts.Cancel();
                _expired.TrySetResult();
            }
        }

        public void Cancel()
        {
            if (!_cts.IsCancellationRequested)
                _cts.Cancel();
        }

        public void Dispose()
        {
            Cancel();
            _cts.Dispose();
        }
    }
}
