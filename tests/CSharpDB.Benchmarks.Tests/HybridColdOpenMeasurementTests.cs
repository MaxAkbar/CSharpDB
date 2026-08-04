using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;

namespace CSharpDB.Benchmarks.Tests;

public sealed class HybridColdOpenMeasurementTests
{
    [Fact]
    public void DefaultPolicy_RequiresReleaseCoreSamplesAndFifteenMeasuredSeconds()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy =
            HybridColdOpenBenchmark.DefaultMeasurementPolicy;

        Assert.Equal(TimeSpan.FromSeconds(15), policy.MinimumMeasuredDuration);
        Assert.Equal(100, policy.MinimumLatencySamples);
        Assert.Equal(TimeSpan.FromSeconds(90), policy.MaximumMeasuredDuration);

        Assert.False(HybridColdOpenBenchmark.HasMetMeasurementTarget(
            TimeSpan.FromSeconds(15),
            retainedLatencySamples: 99,
            policy));
        Assert.True(HybridColdOpenBenchmark.HasMetMeasurementTarget(
            TimeSpan.FromSeconds(15),
            retainedLatencySamples: 100,
            policy));
    }

    [Fact]
    public async Task Measurement_TargetReachedBeforeDeadline_IsAccepted()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        deadline.AdvanceTo(TimeSpan.FromSeconds(1));

        BenchmarkResult result = await HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
            "cold-open-target",
            _ => Task.CompletedTask,
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25));

        Assert.Equal(1, result.TotalOps);
        Assert.Equal(1, result.LatencySamples);
        Assert.Equal(1_000, result.ElapsedMs);
    }

    [Fact]
    public async Task Measurement_InFlightOperationIsCancelledAtWallClockCap()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<BenchmarkResult> runTask = HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
            "cold-open-cancelled",
            async ct =>
            {
                operationStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25));

        await operationStarted.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runTask.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken));

        Assert.Contains("cold-open-cancelled", exception.Message);
        Assert.Contains("2-second measurement cap", exception.Message);
        Assert.Contains("0 retained latency samples", exception.Message);
        Assert.Contains("1 retained latency samples", exception.Message);
        Assert.True(deadline.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task Measurement_CancellationIgnoringOperation_IsBoundedAndExplicit()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOperation = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedWorker = null;

        Task<BenchmarkResult> runTask = HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
            "cold-open-unresponsive",
            _ =>
            {
                operationStarted.TrySetResult();
                return releaseOperation.Task;
            },
            policy,
            deadline,
            TimeSpan.FromMilliseconds(25),
            task => detachedWorker = task);

        await operationStarted.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    BenchmarkTestWatchdog.SchedulingTimeout,
                    TestContext.Current.CancellationToken));

            Assert.Contains("cold-open-unresponsive", exception.Message);
            Assert.Contains("2.0 seconds with 0 retained latency samples", exception.Message);
            Assert.Contains("did not stop the in-flight operation", exception.Message);
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
    public async Task Measurement_SynchronouslyBlockingOperation_IsBoundedAndExplicit()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        using var releaseOperation = new ManualResetEventSlim();
        var operationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var operationExited = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task? detachedWorker = null;

        Task<BenchmarkResult> runTask = Task.Run(async () =>
            await HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
                "cold-open-synchronously-blocked",
                _ =>
                {
                    operationStarted.TrySetResult();
                    releaseOperation.Wait();
                    operationExited.TrySetResult();
                    return Task.CompletedTask;
                },
                policy,
                deadline,
                TimeSpan.FromMilliseconds(25),
                task => detachedWorker = task));

        await operationStarted.Task.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        deadline.AdvanceTo(policy.MaximumMeasuredDuration, expire: true);
        try
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask.WaitAsync(
                    BenchmarkTestWatchdog.SchedulingTimeout,
                    TestContext.Current.CancellationToken));

            Assert.Contains("cold-open-synchronously-blocked", exception.Message);
            Assert.Contains("2.0 seconds with 0 retained latency samples", exception.Message);
            Assert.Contains("did not stop the in-flight operation", exception.Message);
            Assert.NotNull(detachedWorker);
        }
        finally
        {
            releaseOperation.Set();
            await operationExited.Task.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken);
            if (detachedWorker is not null)
            {
                await AwaitDetachedWorkerAsync(detachedWorker);
            }
        }
    }

    [Fact]
    public async Task Measurement_DeadlineStartsOnlyAfterScheduledWorkerIsReady()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        Func<Task<BenchmarkResult>>? scheduledWorker = null;
        var scheduledCompletion = new TaskCompletionSource<BenchmarkResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task<BenchmarkResult> ScheduleWorker(Func<Task<BenchmarkResult>> worker)
        {
            scheduledWorker = worker;
            return scheduledCompletion.Task;
        }

        Task<BenchmarkResult> runTask = HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
            "cold-open-delayed-worker",
            _ =>
            {
                deadline.AdvanceTo(policy.MinimumMeasuredDuration);
                return Task.CompletedTask;
            },
            policy,
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
                    scheduledCompletion.TrySetResult(await scheduledWorker!());
                }
                catch (Exception exception)
                {
                    scheduledCompletion.TrySetException(exception);
                }
            },
            TestContext.Current.CancellationToken);
        BenchmarkResult result = await runTask.WaitAsync(
            BenchmarkTestWatchdog.SchedulingTimeout,
            TestContext.Current.CancellationToken);
        await scheduledExecution;

        Assert.Equal(1, deadline.StartCount);
        Assert.Equal(policy.MinimumMeasuredDuration.TotalMilliseconds, result.ElapsedMs);
    }

    [Fact]
    public async Task Measurement_OperationFailureBeforeCap_IsPreserved()
    {
        HybridColdOpenBenchmark.MeasurementPolicy policy = CreateFastPolicy();
        using var deadline = new ManualMeasurementDeadline();
        var expected = new InvalidOperationException("operation failed before cap");

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => HybridColdOpenBenchmark.RunColdScenarioCoreAsync(
                "cold-open-operation-failure",
                _ => throw expected,
                policy,
                deadline,
                TimeSpan.FromMilliseconds(25)));

        Assert.Same(expected, exception);
    }

    private static HybridColdOpenBenchmark.MeasurementPolicy CreateFastPolicy()
        => new(
            MinimumMeasuredDuration: TimeSpan.FromSeconds(1),
            MinimumLatencySamples: 1,
            MaximumMeasuredDuration: TimeSpan.FromSeconds(2));

    private static async Task AwaitDetachedWorkerAsync(Task worker)
    {
        try
        {
            await worker.WaitAsync(
                BenchmarkTestWatchdog.SchedulingTimeout,
                TestContext.Current.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Coordinated deadline cancellation is the expected detached-worker exit.
        }
    }

    private sealed class ManualMeasurementDeadline :
        HybridColdOpenBenchmark.IMeasurementDeadline
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly TaskCompletionSource _expired =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _elapsedTicks;
        private int _startCount;

        public TimeSpan Elapsed => TimeSpan.FromTicks(Interlocked.Read(ref _elapsedTicks));
        public CancellationToken Token => _cts.Token;
        public Task Expired => _expired.Task;
        public int StartCount => Volatile.Read(ref _startCount);

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
}
