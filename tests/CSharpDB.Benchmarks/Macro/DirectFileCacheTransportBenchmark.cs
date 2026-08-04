using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Execution;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Macro;

public static class DirectFileCacheTransportBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int WarmupCount = 128;
    private const int ConcurrentReaderCount = 8;
    internal const int MinimumReleaseCoreLatencySamples = 100;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan MaximumReleaseCoreMeasuredDuration = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan WarmupCompletionTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan FailureCleanupDrainTimeout = TimeSpan.FromSeconds(1);
    private static readonly IReadOnlyList<MasterComparisonScenario> s_masterComparisonDurableWriteScenarios =
        Array.AsReadOnly(
        [
            MasterComparisonScenario.SqlSingleInsert,
            MasterComparisonScenario.SqlBatchInsert,
        ]);
    private static readonly IReadOnlyList<MasterComparisonScenario> s_masterComparisonHostedStableScenarios =
        Array.AsReadOnly(
        [
            MasterComparisonScenario.SqlPointLookup,
            MasterComparisonScenario.SqlConcurrentReads,
        ]);

    internal enum MasterComparisonScenario
    {
        SqlSingleInsert,
        SqlBatchInsert,
        SqlPointLookup,
        SqlConcurrentReads,
    }

    internal static IReadOnlyList<MasterComparisonScenario> MasterComparisonDurableWriteScenarios =>
        s_masterComparisonDurableWriteScenarios;

    internal static IReadOnlyList<MasterComparisonScenario> MasterComparisonHostedStableScenarios =>
        s_masterComparisonHostedStableScenarios;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        results.Add(await RunSqlSingleInsertAsync(tunedFileCache: true));
        results.Add(await RunSqlBatchInsertAsync(tunedFileCache: true));
        results.Add(await RunSqlPointLookupAsync(tunedFileCache: false));
        results.Add(await RunSqlPointLookupAsync(tunedFileCache: true));
        results.Add(await RunSqlConcurrentReadsAsync(tunedFileCache: false));
        results.Add(await RunSqlConcurrentReadsAsync(tunedFileCache: true));
        results.Add(await RunCollectionGetAsync(tunedFileCache: false));
        results.Add(await RunCollectionGetAsync(tunedFileCache: true));

        return results;
    }

    internal static async Task<List<BenchmarkResult>> RunMasterComparisonSubsetAsync()
    {
        return await RunMasterComparisonScenariosAsync(
        [
            .. s_masterComparisonDurableWriteScenarios,
            .. s_masterComparisonHostedStableScenarios,
        ]);
    }

    internal static async Task<List<BenchmarkResult>> RunMasterComparisonDurableWriteSubsetAsync()
    {
        return await RunMasterComparisonScenariosAsync(s_masterComparisonDurableWriteScenarios);
    }

    internal static async Task<List<BenchmarkResult>> RunMasterComparisonHostedStableSubsetAsync()
    {
        return await RunMasterComparisonScenariosAsync(s_masterComparisonHostedStableScenarios);
    }

    private static async Task<List<BenchmarkResult>> RunMasterComparisonScenariosAsync(
        IReadOnlyList<MasterComparisonScenario> scenarios)
    {
        var results = new List<BenchmarkResult>(scenarios.Count);
        foreach (MasterComparisonScenario scenario in scenarios)
        {
            results.Add(await (scenario switch
            {
                MasterComparisonScenario.SqlSingleInsert =>
                    RunSqlSingleInsertAsync(tunedFileCache: true),
                MasterComparisonScenario.SqlBatchInsert =>
                    RunSqlBatchInsertAsync(tunedFileCache: true),
                MasterComparisonScenario.SqlPointLookup =>
                    RunSqlPointLookupAsync(tunedFileCache: true),
                MasterComparisonScenario.SqlConcurrentReads =>
                    RunSqlConcurrentReadsAsync(tunedFileCache: true),
                _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null),
            }));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        CSharpDbClient client = context.CreateClient(tunedFileCache);
        int nextId = SeedCount + 5_000_000;

        return await RunReleaseCoreSequentialAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_SingleInsert_10s",
            async ct =>
            {
                int id = nextId++;
                string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                SqlExecutionResult result = await client.ExecuteSqlAsync(sql, ct);
                EnsureWriteSucceeded(result, 1, sql);
            },
            context.QuarantineDetachedWork);
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        CSharpDbClient client = context.CreateClient(tunedFileCache);
        int nextId = SeedCount + 6_000_000;

        return await RunReleaseCoreSequentialAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_Batch{BatchSize}_10s",
            async ct =>
            {
                TransactionSessionInfo tx = await client.BeginTransactionAsync(ct);
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                        SqlExecutionResult result = await client.ExecuteInTransactionAsync(
                            tx.TransactionId,
                            sql,
                            ct);
                        EnsureWriteSucceeded(result, 1, sql);
                    }

                    await client.CommitTransactionAsync(tx.TransactionId, ct);
                }
                catch
                {
                    try
                    {
                        await client.RollbackTransactionAsync(tx.TransactionId, CancellationToken.None);
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
            },
            context.QuarantineDetachedWork);
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        CSharpDbClient client = context.CreateClient(tunedFileCache);
        var rng = new Random(42);

        await WarmSqlLookupsAsync(client, rng, WarmupCount);

        rng = new Random(42);
        return await RunReleaseCoreSequentialAsync(
            $"{GetPrefix(tunedFileCache)}_Sql_PointLookup_{SeedCount}",
            async ct =>
            {
                int id = rng.Next(1, SeedCount + 1);
                SqlExecutionResult result = await client.ExecuteSqlAsync(
                    $"SELECT value FROM bench WHERE id = {id};",
                    ct);
                EnsureSingleRow(result, id);
            },
            context.QuarantineDetachedWork);
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        CSharpDbClient writer = context.CreateClient(tunedFileCache);
        Database database = await GetSharedDatabaseAsync(writer);
        string benchmarkName =
            $"{GetPrefix(tunedFileCache)}_Sql_ConcurrentReads_{ConcurrentReaderCount}readers";
        var readerHistograms = new LatencyHistogram[ConcurrentReaderCount];

        for (int i = 0; i < ConcurrentReaderCount; i++)
        {
            readerHistograms[i] = new LatencyHistogram();
        }

        await WarmConcurrentReadersAsync(writer, database);

        using var cts = new CancellationTokenSource();
        var allWorkersReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measurementStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        object[] readerRecorderGates = Enumerable.Range(0, ConcurrentReaderCount)
            .Select(static _ => new object())
            .ToArray();
        int measurementClosed = 0;
        int retainedLatencySamples = 0;
        int readyWorkerCount = 0;
        int totalWorkerCount = ConcurrentReaderCount + 1;
        int nextId = SeedCount + 7_000_000;
        var writerTask = Task.Run(async () =>
        {
            SignalWorkerReady();
            await measurementStart.Task.ConfigureAwait(false);
            while (!cts.Token.IsCancellationRequested)
            {
                int id = nextId++;
                try
                {
                    _ = await writer.ExecuteSqlAsync(
                        $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                        cts.Token);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    break;
                }
                catch (CSharpDbException ex) when (ex.Code == ErrorCode.Busy)
                {
                    // Ignore transient contention during the concurrent-read run.
                }
            }
        });

        var readerTasks = new Task[ConcurrentReaderCount];
        for (int i = 0; i < ConcurrentReaderCount; i++)
        {
            LatencyHistogram histogram = readerHistograms[i];
            object recorderGate = readerRecorderGates[i];
            readerTasks[i] = Task.Run(async () =>
            {
                SignalWorkerReady();
                await measurementStart.Task.ConfigureAwait(false);
                while (!cts.Token.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        using var reader = database.CreateReaderSession();
                        await using QueryResult result = await reader.ExecuteReadAsync(
                            "SELECT COUNT(*) FROM bench;",
                            cts.Token);
                        if (!await result.MoveNextAsync(cts.Token))
                            continue;
                    }
                    catch (OperationCanceledException) when (cts.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (CSharpDbException ex) when (ex.Code == ErrorCode.Busy)
                    {
                        continue;
                    }

                    sw.Stop();
                    if (Volatile.Read(ref measurementClosed) != 0)
                        return;

                    lock (recorderGate)
                    {
                        if (Volatile.Read(ref measurementClosed) != 0)
                            return;

                        histogram.Record(sw.Elapsed.TotalMilliseconds);
                        Interlocked.Increment(ref retainedLatencySamples);
                    }
                }
            });
        }

        Task[] workerTasks = [writerTask, .. readerTasks];
        Task allWorkers = Task.WhenAll(workerTasks);
        Task<Task> firstWorkerCompletion = Task.WhenAny(workerTasks);
        await allWorkersReady.Task.ConfigureAwait(false);

        var measured = Stopwatch.StartNew();
        cts.CancelAfter(MaximumReleaseCoreMeasuredDuration);
        measurementStart.TrySetResult();

        while (true)
        {
            TimeSpan elapsed = measured.Elapsed;
            int observedLatencySamples = GetRetainedLatencySamples();
            if (elapsed <= MaximumReleaseCoreMeasuredDuration &&
                HasMetReleaseCoreMeasurementTarget(elapsed, observedLatencySamples))
            {
                break;
            }

            if (cts.IsCancellationRequested || elapsed >= MaximumReleaseCoreMeasuredDuration)
            {
                TimeSpan closedElapsed = CloseMeasurement();
                int closedLatencySamples = GetRetainedLatencySamples();
                InvalidOperationException capException =
                    CreateReleaseCoreMeasurementCapException(
                        benchmarkName,
                        closedElapsed,
                        closedLatencySamples);
                cts.Cancel();
                await DrainWorkersAsync(
                    allWorkers,
                    benchmarkName,
                    capException,
                    context.QuarantineDetachedWork);
                throw capException;
            }

            if (firstWorkerCompletion.IsCompleted)
            {
                Task completedWorker = await firstWorkerCompletion;
                Exception? earlyFailure = completedWorker.Exception?.Flatten().InnerExceptions[0];
                if (earlyFailure is null && completedWorker.IsCanceled)
                {
                    earlyFailure = new TaskCanceledException(
                        "A direct concurrent-read worker was canceled before coordinated cancellation.");
                }

                _ = CloseMeasurement();
                cts.Cancel();
                await DrainWorkersAsync(
                    allWorkers,
                    benchmarkName,
                    earlyFailure,
                    context.QuarantineDetachedWork);
                throw new InvalidOperationException(
                    "Direct concurrent-read benchmark workers exited before the release-core " +
                    "measurement target was reached.",
                    earlyFailure);
            }

            await Task.WhenAny(
                firstWorkerCompletion,
                Task.Delay(TimeSpan.FromMilliseconds(10)));
        }

        TimeSpan measuredElapsed = CloseMeasurement();
        cts.Cancel();
        await DrainWorkersAsync(
            allWorkers,
            benchmarkName,
            detachedWorkRegistrar: context.QuarantineDetachedWork);

        int totalReaderOps = readerHistograms.Sum(static h => h.Count);
        return new BenchmarkResult
        {
            Name = benchmarkName,
            TotalOps = totalReaderOps,
            LatencySamples = readerHistograms.Sum(static histogram => histogram.SampleCount),
            ElapsedMs = measuredElapsed.TotalMilliseconds,
            P50Ms = readerHistograms.Average(static h => h.Percentile(0.50)),
            P90Ms = readerHistograms.Average(static h => h.Percentile(0.90)),
            P95Ms = readerHistograms.Average(static h => h.Percentile(0.95)),
            P99Ms = readerHistograms.Average(static h => h.Percentile(0.99)),
            P999Ms = readerHistograms.Average(static h => h.Percentile(0.999)),
            MinMs = readerHistograms.Min(static h => h.Min),
            MaxMs = readerHistograms.Max(static h => h.Max),
            MeanMs = readerHistograms.Average(static h => h.Mean),
            StdDevMs = readerHistograms.Average(static h => h.StdDev),
        };

        void SignalWorkerReady()
        {
            if (Interlocked.Increment(ref readyWorkerCount) == totalWorkerCount)
                allWorkersReady.TrySetResult();
        }

        int GetRetainedLatencySamples()
            => Volatile.Read(ref retainedLatencySamples);

        TimeSpan CloseMeasurement()
        {
            Interlocked.Exchange(ref measurementClosed, 1);
            TimeSpan cutoffElapsed = measured.Elapsed;
            foreach (object recorderGate in readerRecorderGates)
            {
                lock (recorderGate)
                {
                    // Establish a barrier with an in-flight recorder. New recorders
                    // observe measurementClosed before entering their per-reader gate.
                }
            }

            return cutoffElapsed;
        }
    }

    private static async Task<BenchmarkResult> RunReleaseCoreSequentialAsync(
        string benchmarkName,
        Func<CancellationToken, Task> operation,
        Action<Task> detachedWorkRegistrar)
    {
        ArgumentNullException.ThrowIfNull(detachedWorkRegistrar);
        await RunWarmupAsync(operation, detachedWorkRegistrar);
        MacroBenchmarkRunner.StabilizeAfterWarmup();

        var histogram = new LatencyHistogram();
        using var phaseCts = new CancellationTokenSource();
        var workerReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measurementStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measured = new Stopwatch();
        int retainedLatencySamples = 0;
        BenchmarkResult? result = null;
        Task workerTask = StartControllerVisibleWorkerAsync(
            async ct =>
            {
                workerReady.TrySetResult();
                await measurementStart.Task.ConfigureAwait(false);

                while (!HasMetReleaseCoreMeasurementTarget(
                           measured.Elapsed,
                           histogram.SampleCount))
                {
                    ct.ThrowIfCancellationRequested();
                    var operationStopwatch = Stopwatch.StartNew();
                    await operation(ct).ConfigureAwait(false);
                    operationStopwatch.Stop();
                    ct.ThrowIfCancellationRequested();

                    histogram.Record(operationStopwatch.Elapsed.TotalMilliseconds);
                    Volatile.Write(ref retainedLatencySamples, histogram.SampleCount);
                }

                result = BenchmarkResult.FromHistogram(
                    benchmarkName,
                    histogram,
                    measured.Elapsed.TotalMilliseconds);
            },
            phaseCts.Token);

        await workerReady.Task.ConfigureAwait(false);
        measured.Start();
        phaseCts.CancelAfter(MaximumReleaseCoreMeasuredDuration);
        Task deadlineTask = Task.Delay(Timeout.InfiniteTimeSpan, phaseCts.Token);
        measurementStart.TrySetResult();

        bool workerCompleted = await AwaitControllerVisibleWorkerAsync(
            workerTask,
            deadlineTask,
            phaseCts,
            FailureCleanupDrainTimeout,
            $"direct benchmark row '{benchmarkName}' measured worker",
            () => CreateReleaseCoreMeasurementCapException(
                benchmarkName,
                measured.Elapsed,
                Volatile.Read(ref retainedLatencySamples)),
            detachedWorkRegistrar);
        if (!workerCompleted)
        {
            throw CreateReleaseCoreMeasurementCapException(
                benchmarkName,
                measured.Elapsed,
                Volatile.Read(ref retainedLatencySamples));
        }

        BenchmarkResult completedResult = result ?? throw new InvalidOperationException(
            $"Direct benchmark row '{benchmarkName}' completed without producing a result.");
        Console.WriteLine(
            $"  {benchmarkName}: {completedResult.OpsPerSecond:N0} ops/sec, " +
            $"P50={completedResult.P50Ms:F3}ms, P99={completedResult.P99Ms:F3}ms, " +
            $"P999={completedResult.P999Ms:F3}ms " +
            $"({completedResult.LatencySamples:N0} retained samples)");
        return completedResult;
    }

    private static async Task RunWarmupAsync(
        Func<CancellationToken, Task> operation,
        Action<Task> detachedWorkRegistrar)
    {
        using var warmupStopCts = new CancellationTokenSource();
        await RunWarmupCoreAsync(
            operation,
            warmupStopCts,
            WarmupDuration,
            WarmupCompletionTimeout,
            detachedWorkRegistrar);
    }

    internal static async Task RunWarmupCoreAsync(
        Func<CancellationToken, Task> operation,
        CancellationTokenSource warmupStopCts,
        TimeSpan warmupDuration,
        TimeSpan completionTimeout,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(warmupStopCts);
        if (warmupDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(warmupDuration));
        if (completionTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(completionTimeout));

        var workerReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var warmupStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task workerTask = StartControllerVisibleWorkerAsync(
            async stopToken =>
            {
                workerReady.TrySetResult();
                await warmupStart.Task.ConfigureAwait(false);
                while (!stopToken.IsCancellationRequested)
                {
                    // The warmup duration is a soft boundary. Durable operations may
                    // become intentionally non-cancellable once commit begins, so let
                    // the in-flight operation finish and stop before starting another.
                    await operation(CancellationToken.None).ConfigureAwait(false);
                }
            },
            warmupStopCts.Token);

        await workerReady.Task.ConfigureAwait(false);
        warmupStopCts.CancelAfter(warmupDuration);
        Task deadlineTask = Task.Delay(Timeout.InfiniteTimeSpan, warmupStopCts.Token);
        warmupStart.TrySetResult();
        _ = await AwaitControllerVisibleWorkerAsync(
            workerTask,
            deadlineTask,
            warmupStopCts,
            completionTimeout,
            "direct benchmark warmup worker",
            detachedWorkRegistrar: detachedWorkRegistrar);
    }

    internal static Task StartControllerVisibleWorkerAsync(
        Func<CancellationToken, Task> worker,
        CancellationToken phaseToken)
    {
        ArgumentNullException.ThrowIfNull(worker);
        return Task.Run(
            async () => await worker(phaseToken).ConfigureAwait(false),
            CancellationToken.None);
    }

    internal static async Task<bool> AwaitControllerVisibleWorkerAsync(
        Task workerTask,
        Task deadlineTask,
        CancellationTokenSource phaseCts,
        TimeSpan cancellationDrainTimeout,
        string workerDescription,
        Func<Exception?>? deadlineFailureFactory = null,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentNullException.ThrowIfNull(workerTask);
        ArgumentNullException.ThrowIfNull(deadlineTask);
        ArgumentNullException.ThrowIfNull(phaseCts);
        ArgumentException.ThrowIfNullOrWhiteSpace(workerDescription);
        if (cancellationDrainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cancellationDrainTimeout));

        Task firstCompletion = await Task.WhenAny(workerTask, deadlineTask);
        if (firstCompletion == workerTask || workerTask.IsCompleted)
        {
            try
            {
                await workerTask;
                return !phaseCts.IsCancellationRequested;
            }
            catch (OperationCanceledException) when (
                phaseCts.IsCancellationRequested)
            {
                return false;
            }
        }

        phaseCts.Cancel();
        try
        {
            await workerTask.WaitAsync(cancellationDrainTimeout);
        }
        catch (OperationCanceledException) when (phaseCts.IsCancellationRequested)
        {
            return false;
        }
        catch (TimeoutException)
        {
            ObserveFaultEventually(workerTask);
            detachedWorkRegistrar?.Invoke(workerTask);
            throw new InvalidOperationException(
                $"Coordinated cancellation did not stop {workerDescription} within " +
                $"{cancellationDrainTimeout.TotalSeconds:F3} seconds.",
                deadlineFailureFactory?.Invoke());
        }

        return false;
    }

    private static async Task DrainWorkersAsync(
        Task allWorkers,
        string benchmarkName,
        Exception? deadlineFailure = null,
        Action<Task>? detachedWorkRegistrar = null)
        => await WaitForConcurrentWorkerDrainAsync(
            allWorkers,
            benchmarkName,
            ReleaseWorkerCancellationPolicy.CoordinatedDrainTimeout,
            deadlineFailure,
            detachedWorkRegistrar);

    internal static async Task WaitForConcurrentWorkerDrainAsync(
        Task allWorkers,
        string benchmarkName,
        TimeSpan cancellationDrainTimeout,
        Exception? deadlineFailure = null,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentNullException.ThrowIfNull(allWorkers);
        ArgumentException.ThrowIfNullOrWhiteSpace(benchmarkName);
        if (cancellationDrainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cancellationDrainTimeout));

        try
        {
            await allWorkers.WaitAsync(cancellationDrainTimeout);
        }
        catch (TimeoutException)
        {
            ObserveFaultEventually(allWorkers);
            detachedWorkRegistrar?.Invoke(allWorkers);
            throw new InvalidOperationException(
                $"Coordinated cancellation for {benchmarkName} did not stop all workers within " +
                $"{cancellationDrainTimeout.TotalSeconds:F3} seconds.",
                deadlineFailure);
        }
    }

    private static void ObserveFaultEventually(Task task)
    {
        _ = task.ContinueWith(
            static completedTask => _ = completedTask.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    internal static bool HasMetReleaseCoreMeasurementTarget(
        TimeSpan elapsed,
        int retainedLatencySamples)
        => elapsed >= MeasuredDuration &&
           retainedLatencySamples >= MinimumReleaseCoreLatencySamples;

    internal static InvalidOperationException CreateReleaseCoreMeasurementCapException(
        string benchmarkName,
        TimeSpan elapsed,
        int retainedLatencySamples)
        => new(
            $"Direct benchmark row '{benchmarkName}' reached its " +
            $"{MaximumReleaseCoreMeasuredDuration.TotalSeconds:F0}-second measurement cap after " +
            $"{elapsed.TotalSeconds:F1} seconds with {retainedLatencySamples:N0} retained latency samples. " +
            $"Release qualification requires at least {MeasuredDuration.TotalSeconds:F0} measured seconds " +
            $"and {MinimumReleaseCoreLatencySamples:N0} retained latency samples.");

    private static async Task<BenchmarkResult> RunCollectionGetAsync(bool tunedFileCache)
    {
        await using var context = await DirectBenchmarkContext.CreateAsync();
        CSharpDbClient client = context.CreateClient(tunedFileCache);
        var rng = new Random(84);

        await WarmCollectionGetsAsync(client, rng, WarmupCount);

        rng = new Random(84);
        return await MacroBenchmarkRunner.RunForDurationAsync(
            $"{GetPrefix(tunedFileCache)}_Collection_Get_{SeedCount}",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                JsonElement? document = await client.GetDocumentAsync("bench_docs", $"doc:{id}", CancellationToken.None);
                if (document is null)
                    throw new InvalidOperationException($"Document 'doc:{id}' was not found in the direct hybrid benchmark dataset.");
            });
    }

    private static async Task WarmSqlLookupsAsync(ICSharpDbClient client, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            SqlExecutionResult result = await client.ExecuteSqlAsync(
                $"SELECT value FROM bench WHERE id = {id};",
                CancellationToken.None);
            EnsureSingleRow(result, id);
        }
    }

    private static async Task WarmCollectionGetsAsync(ICSharpDbClient client, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            JsonElement? document = await client.GetDocumentAsync("bench_docs", $"doc:{id}", CancellationToken.None);
            if (document is null)
                throw new InvalidOperationException($"Document 'doc:{id}' was not found during direct benchmark warmup.");
        }
    }

    private static async Task WarmConcurrentReadersAsync(ICSharpDbClient writer, Database database)
    {
        for (int i = 0; i < 16; i++)
        {
            _ = await writer.ExecuteSqlAsync(
                $"INSERT INTO bench VALUES ({SeedCount + 8_000_000 + i}, {(SeedCount + i) * 10L}, 'warmup');",
                CancellationToken.None);
        }

        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            for (int i = 0; i < 8; i++)
            {
                using var reader = database.CreateReaderSession();
                await using QueryResult result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", CancellationToken.None);
                _ = await result.MoveNextAsync(CancellationToken.None);
            }
        }
    }

    private static async Task<Database> GetSharedDatabaseAsync(CSharpDbClient writer)
    {
        Database? database = await writer.TryGetDatabaseAsync(CancellationToken.None);
        if (database is null)
            throw new InvalidOperationException("The direct hybrid benchmark requires an engine-backed client.");

        return database;
    }

    private static bool IsSuccessfulSingleRow(SqlExecutionResult result)
        => string.IsNullOrWhiteSpace(result.Error)
            && result.IsQuery
            && result.Rows is { Count: 1 };

    private static void EnsureSingleRow(SqlExecutionResult result, int id)
    {
        if (!IsSuccessfulSingleRow(result))
        {
            throw new InvalidOperationException(
                $"Expected one row for SQL lookup '{id}', but received error='{result.Error}', isQuery={result.IsQuery}, rowCount={result.Rows?.Count ?? 0}.");
        }
    }

    private static void EnsureWriteSucceeded(SqlExecutionResult result, int expectedRowsAffected, string sql)
    {
        if (!string.IsNullOrWhiteSpace(result.Error) || result.IsQuery || result.RowsAffected != expectedRowsAffected)
        {
            throw new InvalidOperationException(
                $"Expected write success for SQL '{sql}', but received error='{result.Error}', isQuery={result.IsQuery}, rowsAffected={result.RowsAffected}.");
        }
    }

    private static string GetPrefix(bool tunedFileCache)
        => tunedFileCache ? "Direct_DirectLookupPreset" : "Direct_Default";

    private static async Task<string> CreateSeededDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_direct_hybrid_{Guid.NewGuid():N}.db");

        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");
        var collection = await db.GetCollectionAsync<JsonElement>("bench_docs");
        var batch = db.PrepareInsertBatch("bench", initialCapacity: 512);

        const int batchSize = 512;
        for (int batchStart = 1; batchStart <= SeedCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            try
            {
                int batchEnd = Math.Min(batchStart + batchSize - 1, SeedCount);
                for (int id = batchStart; id <= batchEnd; id++)
                {
                    batch.AddRow(
                        CSharpDB.Primitives.DbValue.FromInteger(id),
                        CSharpDB.Primitives.DbValue.FromInteger(id * 10L),
                        CSharpDB.Primitives.DbValue.FromText(GetCategory(id)));
                    await collection.PutAsync($"doc:{id}", CreateBenchDocument(id));
                }

                int batchCount = batchEnd - batchStart + 1;
                AssertBatchCount(batchCount, await batch.ExecuteAsync(CancellationToken.None));
                await db.CommitAsync();
            }
            catch
            {
                await db.RollbackAsync();
                throw;
            }
        }

        await db.CheckpointAsync();
        return filePath;
    }

    private static DatabaseOptions CreateDirectDatabaseOptions(bool tunedFileCache)
    {
        if (!tunedFileCache)
        {
            return BenchmarkDurability.Apply();
        }

        return BenchmarkDurability.Apply(new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions().Configure(static builder => builder.UseDirectLookupOptimizedPreset()),
        });
    }

    private static void AssertBatchCount(int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException(
                $"Failed to seed the direct hybrid benchmark dataset. Expected {expected} batched inserts but observed {actual}.");
        }
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static JsonElement CreateBenchDocument(int id)
    {
        using var document = JsonDocument.Parse(
            $$"""{"name":"User_{{id}}","value":{{id}},"category":"{{GetCategory(id)}}" }""");
        return document.RootElement.Clone();
    }

    private sealed class DirectBenchmarkContext : IAsyncDisposable
    {
        private readonly object _lifetimeGate = new();
        private readonly string _dbPath;
        private readonly List<CSharpDbClient> _ownedClients = [];
        private readonly List<Task> _detachedWork = [];
        private int _resourcesDisposed;

        private DirectBenchmarkContext(string dbPath)
        {
            _dbPath = dbPath;
        }

        internal static async Task<DirectBenchmarkContext> CreateAsync()
            => new(await CreateSeededDatabaseAsync());

        internal CSharpDbClient CreateClient(bool tunedFileCache)
        {
            var client = (CSharpDbClient)CSharpDbClient.Create(new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Direct,
                DataSource = _dbPath,
                DirectDatabaseOptions = CreateDirectDatabaseOptions(tunedFileCache),
            });
            lock (_lifetimeGate)
                _ownedClients.Add(client);
            return client;
        }

        internal void QuarantineDetachedWork(Task task)
        {
            ArgumentNullException.ThrowIfNull(task);
            ObserveFaultEventually(task);
            lock (_lifetimeGate)
                _detachedWork.Add(task);
        }

        public ValueTask DisposeAsync()
        {
            Task[] pendingWork;
            lock (_lifetimeGate)
            {
                pendingWork = _detachedWork
                    .Where(static task => !task.IsCompleted)
                    .ToArray();
            }

            if (pendingWork.Length == 0)
                return DisposeResourcesAsync();

            _ = DisposeAfterDetachedWorkAsync(Task.WhenAll(pendingWork));
            return ValueTask.CompletedTask;
        }

        private async Task DisposeAfterDetachedWorkAsync(Task lifetime)
        {
            try
            {
                await lifetime.ConfigureAwait(false);
            }
            catch
            {
                // The benchmark diagnostic already owns detached worker failures.
            }

            try
            {
                await DisposeResourcesAsync().ConfigureAwait(false);
            }
            catch
            {
                // Deferred cleanup cannot replace the benchmark's explicit failure.
            }
        }

        private async ValueTask DisposeResourcesAsync()
        {
            if (Interlocked.Exchange(ref _resourcesDisposed, 1) != 0)
                return;

            CSharpDbClient[] ownedClients;
            lock (_lifetimeGate)
                ownedClients = _ownedClients.ToArray();

            for (int i = ownedClients.Length - 1; i >= 0; i--)
            {
                try
                {
                    await ownedClients[i].DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Cleanup must not hide the benchmark's explicit result.
                }
            }

            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_dbPath);
        }
    }
}
