using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ReleaseCoreMeasurementFloorTests
{
    [Fact]
    public void DirectRows_RequireNominalDurationAndOneHundredRealSamples()
    {
        Assert.Equal(100, DirectFileCacheTransportBenchmark.MinimumReleaseCoreLatencySamples);
        Assert.Equal(
            TimeSpan.FromSeconds(90),
            DirectFileCacheTransportBenchmark.MaximumReleaseCoreMeasuredDuration);

        Assert.False(DirectFileCacheTransportBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(9.999),
            retainedLatencySamples: 100));
        Assert.False(DirectFileCacheTransportBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(10),
            retainedLatencySamples: 99));
        Assert.True(DirectFileCacheTransportBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(10),
            retainedLatencySamples: 100));
    }

    [Fact]
    public void DirectRows_CapDiagnosticReportsActualEvidence()
    {
        InvalidOperationException exception =
            DirectFileCacheTransportBenchmark.CreateReleaseCoreMeasurementCapException(
                "direct-row",
                TimeSpan.FromSeconds(90.25),
                retainedLatencySamples: 87);

        Assert.Contains("direct-row", exception.Message);
        Assert.Contains("90-second measurement cap", exception.Message);
        Assert.Contains("87 retained latency samples", exception.Message);
        Assert.Contains("100 retained latency samples", exception.Message);
    }

    [Fact]
    public async Task DirectController_SynchronousBlockingWorkerHasBoundedDrainAndObservedLifetime()
    {
        using var phaseCts = new CancellationTokenSource();
        using var releaseWorker = new ManualResetEventSlim();
        var workerEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var deadline = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var capException = new InvalidOperationException("cap sentinel");
        Task? quarantinedWorker = null;

        Task workerTask = DirectFileCacheTransportBenchmark.StartControllerVisibleWorkerAsync(
            _ =>
            {
                workerEntered.TrySetResult();
                releaseWorker.Wait();
                return Task.CompletedTask;
            },
            phaseCts.Token);

        await workerEntered.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        deadline.TrySetResult();
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => DirectFileCacheTransportBenchmark.AwaitControllerVisibleWorkerAsync(
                    workerTask,
                    deadline.Task,
                    phaseCts,
                    TimeSpan.FromMilliseconds(25),
                    "synchronous test worker",
                    () => capException,
                    task => quarantinedWorker = task));

            Assert.Contains("did not stop synchronous test worker", exception.Message);
            Assert.Same(capException, exception.InnerException);
            Assert.Same(workerTask, quarantinedWorker);
            Assert.True(phaseCts.IsCancellationRequested);
        }
        finally
        {
            releaseWorker.Set();
            await workerTask.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task DirectController_DeadlineDrainPreservesWorkerFault()
    {
        using var phaseCts = new CancellationTokenSource();
        var workerEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var deadline = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task workerTask = DirectFileCacheTransportBenchmark.StartControllerVisibleWorkerAsync(
            async ct =>
            {
                workerEntered.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw new ApplicationException("worker fault sentinel");
                }
            },
            phaseCts.Token);

        await workerEntered.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        deadline.TrySetResult();

        ApplicationException exception = await Assert.ThrowsAsync<ApplicationException>(
            () => DirectFileCacheTransportBenchmark.AwaitControllerVisibleWorkerAsync(
                workerTask,
                deadline.Task,
                phaseCts,
                TimeSpan.FromSeconds(1),
                "faulting test worker"));

        Assert.Equal("worker fault sentinel", exception.Message);
    }

    [Fact]
    public void ReleaseWorkerCancellationPolicy_AllowsHostedSchedulingGrace()
    {
        Assert.Equal(
            TimeSpan.FromSeconds(30),
            ReleaseWorkerCancellationPolicy.CoordinatedDrainTimeout);
    }

    [Fact]
    public async Task DirectConcurrentWorkerDrain_IsBoundedAndRetainsPendingFailure()
    {
        var workers = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var pendingFailure = new ApplicationException("early worker sentinel");
        Task? quarantinedWorkers = null;
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => DirectFileCacheTransportBenchmark.WaitForConcurrentWorkerDrainAsync(
                    workers.Task,
                    "direct test row",
                    TimeSpan.FromMilliseconds(25),
                    pendingFailure,
                    task => quarantinedWorkers = task));

            Assert.Contains("did not stop all workers", exception.Message);
            Assert.Contains("0.025 seconds", exception.Message);
            Assert.Same(pendingFailure, exception.InnerException);
            Assert.Same(workers.Task, quarantinedWorkers);
        }
        finally
        {
            workers.TrySetResult();
        }
    }

    [Fact]
    public async Task DirectConcurrentWorkerDrain_PreservesWorkerFault()
    {
        var workerFailure = new ApplicationException("direct worker fault sentinel");

        ApplicationException exception = await Assert.ThrowsAsync<ApplicationException>(
            () => DirectFileCacheTransportBenchmark.WaitForConcurrentWorkerDrainAsync(
                Task.FromException(workerFailure),
                "direct test row",
                TimeSpan.FromSeconds(1)));

        Assert.Same(workerFailure, exception);
    }

    [Fact]
    public async Task DirectWarmup_SoftDeadlineDoesNotCancelInFlightOperation()
    {
        using var warmupStopCts = new CancellationTokenSource();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOperation = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken observedOperationToken = default;
        int operationCount = 0;

        Task warmupTask = DirectFileCacheTransportBenchmark.RunWarmupCoreAsync(
            async operationToken =>
            {
                Interlocked.Increment(ref operationCount);
                observedOperationToken = operationToken;
                operationStarted.TrySetResult();
                await releaseOperation.Task.ConfigureAwait(false);
            },
            warmupStopCts,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromSeconds(1));

        await operationStarted.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        warmupStopCts.Cancel();

        Assert.False(observedOperationToken.CanBeCanceled);
        Assert.False(warmupTask.IsCompleted);

        releaseOperation.TrySetResult();
        await warmupTask.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);

        Assert.Equal(1, Volatile.Read(ref operationCount));
    }

    [Fact]
    public async Task DirectWarmup_UnresponsiveOperationHasBoundedCompletionAndObservedLifetime()
    {
        using var warmupStopCts = new CancellationTokenSource();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOperation = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? quarantinedWorker = null;

        Task warmupTask = DirectFileCacheTransportBenchmark.RunWarmupCoreAsync(
            async _ =>
            {
                operationStarted.TrySetResult();
                await releaseOperation.Task.ConfigureAwait(false);
            },
            warmupStopCts,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMilliseconds(25),
            task => quarantinedWorker = task);

        await operationStarted.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        warmupStopCts.Cancel();
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => warmupTask.WaitAsync(
                    BenchmarkTestWatchdog.SchedulingTimeout,
                    TestContext.Current.CancellationToken));

            Assert.Contains("did not stop direct benchmark warmup worker", exception.Message);
            Assert.NotNull(quarantinedWorker);
            Assert.False(quarantinedWorker.IsCompleted);
        }
        finally
        {
            releaseOperation.TrySetResult();
            if (quarantinedWorker is not null)
            {
                await quarantinedWorker.WaitAsync(
                    BenchmarkTestWatchdog.SchedulingTimeout,
                    TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact]
    public void ConcurrentDurableRows_RequireNominalDurationAndOneHundredRealSamples()
    {
        Assert.Equal(100, ConcurrentDurableWriteBenchmark.MinimumReleaseCoreLatencySamples);
        Assert.Equal(
            TimeSpan.FromSeconds(10),
            ConcurrentDurableWriteBenchmark.NominalReleaseCoreMeasuredDuration);
        Assert.Equal(
            TimeSpan.FromSeconds(90),
            ConcurrentDurableWriteBenchmark.MaximumReleaseCoreMeasuredDuration);

        Assert.False(ConcurrentDurableWriteBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(9.999),
            retainedLatencySamples: 100));
        Assert.False(ConcurrentDurableWriteBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(10),
            retainedLatencySamples: 99));
        Assert.True(ConcurrentDurableWriteBenchmark.HasMetReleaseCoreMeasurementTarget(
            TimeSpan.FromSeconds(10),
            retainedLatencySamples: 100));
    }

    [Fact]
    public void ConcurrentDurableRows_CapDiagnosticReportsActualEvidence()
    {
        InvalidOperationException exception =
            ConcurrentDurableWriteBenchmark.CreateReleaseCoreMeasurementCapException(
                TimeSpan.FromSeconds(90.5),
                retainedLatencySamples: 73);

        Assert.Contains("90-second measurement cap", exception.Message);
        Assert.Contains("73 retained latency samples", exception.Message);
        Assert.Contains("100 retained latency samples", exception.Message);
    }

    [Fact]
    public async Task ConcurrentDurableWriterDrain_IsBoundedAndRetainsPendingFailure()
    {
        var writers = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var pendingFailure = new ApplicationException("early writer sentinel");
        Task? quarantinedWriters = null;
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => ConcurrentDurableWriteBenchmark.WaitForWriterDrainAsync(
                    writers.Task,
                    TimeSpan.FromMilliseconds(25),
                    pendingFailure,
                    task => quarantinedWriters = task));

            Assert.Contains("did not stop all writers", exception.Message);
            Assert.Same(pendingFailure, exception.InnerException);
            Assert.Same(writers.Task, quarantinedWriters);
        }
        finally
        {
            writers.TrySetResult();
        }
    }

    [Fact]
    public async Task ConcurrentDurableWriterDrain_PreservesWorkerFault()
    {
        var workerFailure = new ApplicationException("writer fault sentinel");

        ApplicationException exception = await Assert.ThrowsAsync<ApplicationException>(
            () => ConcurrentDurableWriteBenchmark.WaitForWriterDrainAsync(
                Task.FromException(workerFailure),
                TimeSpan.FromSeconds(1)));

        Assert.Same(workerFailure, exception);
    }

    [Fact]
    public async Task BenchmarkDatabase_QuarantinedWorkDefersDatabaseAndFileCleanup()
    {
        BenchmarkDatabase benchmark = await BenchmarkDatabase.CreateAsync();
        string filePath = benchmark.FilePath;
        var detachedWork = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var openDatabase = benchmark.Db;
            benchmark.QuarantineDetachedWork(detachedWork.Task);

            await benchmark.DisposeAsync();

            Assert.Same(openDatabase, benchmark.Db);
            Assert.True(File.Exists(filePath));
            Assert.False(benchmark.DeferredCleanupCompletion.IsCompleted);

            detachedWork.TrySetResult();
            await benchmark.DeferredCleanupCompletion.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken);

            Assert.Throws<InvalidOperationException>(() => benchmark.Db);
            Assert.False(File.Exists(filePath));
        }
        finally
        {
            detachedWork.TrySetResult();
            await benchmark.DisposeAsync();
            await benchmark.DeferredCleanupCompletion.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken);
        }
    }
}
