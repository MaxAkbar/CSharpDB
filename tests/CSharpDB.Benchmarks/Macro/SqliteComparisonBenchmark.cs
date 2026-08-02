using System.Diagnostics;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Macro;

public static class SqliteComparisonBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int MatchedBulkBatchSize1000 = 1000;
    private const int MatchedBulkBatchSize10000 = 10000;
    private const int SeedBatchSize = 500;
    private const int WarmupCount = 128;
    private const int ConcurrentReaderCount = 8;
    private const int ReusedSessionBurstReads = 32;
    private const int HighThroughputLatencySampleEvery = 128;
    private static readonly TimeSpan CancellationDrainTimeout = TimeSpan.FromSeconds(1);
    internal static MeasurementPolicy DefaultMeasurementPolicy { get; } = new(
        WarmupDuration: TimeSpan.FromSeconds(2),
        MinimumMeasuredDuration: TimeSpan.FromSeconds(5),
        MinimumLatencySamples: 100,
        MaximumMeasuredDuration: TimeSpan.FromSeconds(90));
    internal static IReadOnlyList<string> ReleaseCoreScenarioNames { get; } =
    [
        GetSingleInsertName(),
        GetBatchInsertName(),
        GetPreparedBulkInsertName(MatchedBulkBatchSize1000),
        GetPreparedBulkInsertName(MatchedBulkBatchSize10000),
        GetPointLookupName(),
        GetConcurrentReadsName(reuseSessionBurstReads: false),
        GetConcurrentReadsName(reuseSessionBurstReads: true),
    ];
    private static readonly string s_providerInfo = $"provider=Microsoft.Data.Sqlite/{GetProviderVersion()}";
    private const string ConnectionInfo = "cache=private; pooling=false; journal_mode=wal; synchronous=full";

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        return
        [
            await RunSqlSingleInsertAsync(),
            await RunSqlBatchInsertAsync(),
            await RunMatchedBulkInsertPreparedAsync(MatchedBulkBatchSize1000),
            await RunMatchedBulkInsertPreparedAsync(MatchedBulkBatchSize10000),
            await RunSqlPointLookupAsync(),
            await RunSqlConcurrentReadsAsync(reuseSessionBurstReads: false),
            await RunSqlConcurrentReadsAsync(reuseSessionBurstReads: true),
        ];
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateWritableAsync("sqlite-compare-single");
        int nextId = SeedCount + 1_000_000;

        BenchmarkResult result = await RunSequentialScenarioAsync(
            GetSingleInsertName(),
            async ct =>
            {
                int id = nextId++;
                int rowsAffected = await ExecuteNonQueryAsync(
                    context.KeeperConnection,
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                    ct: ct);
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
            },
            context.QuarantineDetachedWork);

        return CloneResult(
            result,
            extraInfo: context.WithNotes(
                "workload=single-row auto-commit raw SQL",
                CreateMeasurementPolicyNote(DefaultMeasurementPolicy)));
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateWritableAsync("sqlite-compare-batch");
        int nextId = SeedCount + 2_000_000;

        BenchmarkResult transactionResult = await RunSequentialScenarioAsync(
            GetBatchInsertName(),
            async ct =>
            {
                using var transaction = context.KeeperConnection.BeginTransaction();
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        int rowsAffected = await ExecuteNonQueryAsync(
                            context.KeeperConnection,
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                            transaction,
                            ct);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                    }

                    transaction.Commit();
                }
                catch
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
            },
            context.QuarantineDetachedWork);

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=raw SQL statements inside one explicit transaction",
                CreateMeasurementPolicyNote(DefaultMeasurementPolicy)));
    }

    private static async Task<BenchmarkResult> RunMatchedBulkInsertPreparedAsync(int batchSize)
    {
        await using var context = await SqliteBenchmarkContext.CreateWritableAsync(
            $"sqlite-compare-bulk4col-b{batchSize}",
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);");
        int nextId = SeedCount + 3_000_000 + batchSize;

        SqliteCommand command = context.Own(context.KeeperConnection.CreateCommand());
        command.CommandText = "INSERT INTO bench VALUES (@id, @value, @text_col, @category);";
        SqliteParameter idParam = AddParameter(command, "@id", 0);
        SqliteParameter valueParam = AddParameter(command, "@value", 0);
        SqliteParameter textParam = AddParameter(command, "@text_col", "durable_batch");
        SqliteParameter categoryParam = AddParameter(command, "@category", "Alpha");
        command.Prepare();

        BenchmarkResult transactionResult = await RunSequentialScenarioAsync(
            GetPreparedBulkInsertName(batchSize),
            async ct =>
            {
                using var transaction = context.KeeperConnection.BeginTransaction();
                command.Transaction = transaction;
                try
                {
                    for (int i = 0; i < batchSize; i++)
                    {
                        int id = nextId++;
                        idParam.Value = id;
                        valueParam.Value = id;
                        textParam.Value = "durable_batch";
                        categoryParam.Value = "Alpha";

                        int rowsAffected = await command.ExecuteNonQueryAsync(ct);
                        if (rowsAffected != 1)
                        {
                            throw new InvalidOperationException(
                                $"Expected one inserted row for id={id}, observed {rowsAffected}.");
                        }
                    }

                    transaction.Commit();
                }
                catch
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
                finally
                {
                    command.Transaction = null;
                }
            },
            context.QuarantineDetachedWork);

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * batchSize,
            extraInfo: context.WithNotes(
                $"batch-size={batchSize}",
                "schema=id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT",
                "throughput-unit=rows/sec from explicit prepared transactions",
                "workload=prepared statement reuse inside one explicit transaction",
                "surface=sqlite-adonet",
                CreateMeasurementPolicyNote(DefaultMeasurementPolicy)));
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateReadSeededAsync("sqlite-compare-lookup");
        SqliteConnection connection = context.Own(await context.OpenReadOnlyConnectionAsync());
        var rng = new Random(42);

        await WarmSqlLookupsAsync(connection, rng, WarmupCount);

        rng = new Random(42);
        BenchmarkResult result = await RunSequentialScenarioAsync(
            GetPointLookupName(),
            async ct =>
            {
                int id = rng.Next(1, SeedCount + 1);
                long value = await ExecuteScalarInt64Async(
                    connection,
                    $"SELECT value FROM bench WHERE id = {id};",
                    ct);
                if (value != id * 10L)
                    throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result '{value}'.");
            },
            context.QuarantineDetachedWork);

        return CloneResult(
            result,
            extraInfo: context.WithNotes(
                $"warmup-lookups={WarmupCount}",
                "workload=single-connection point lookup",
                CreateMeasurementPolicyNote(DefaultMeasurementPolicy)));
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(bool reuseSessionBurstReads)
    {
        await using var context = await SqliteBenchmarkContext.CreateReadSeededAsync(
            reuseSessionBurstReads ? "sqlite-compare-burst" : "sqlite-compare-concurrent");
        var histograms = new LatencyHistogram[ConcurrentReaderCount];
        int latencySampleEvery = reuseSessionBurstReads ? HighThroughputLatencySampleEvery : 1;

        for (int i = 0; i < ConcurrentReaderCount; i++)
            histograms[i] = new LatencyHistogram(latencySampleEvery);

        await WarmConcurrentReadersAsync(context);

        string name = GetConcurrentReadsName(reuseSessionBurstReads);
        using var deadline = new StopwatchMeasurementDeadline(
            DefaultMeasurementPolicy.MaximumMeasuredDuration);
        ConcurrentReaderPhaseResult phase = await RunConcurrentReaderWorkersAsync(
            name,
            ConcurrentReaderCount,
            DefaultMeasurementPolicy,
            (readerIndex, sampleRetained, ct) => reuseSessionBurstReads
                ? RunReusedReaderLoopAsync(context, histograms[readerIndex], sampleRetained, ct)
                : RunPerQueryReaderLoopAsync(context, histograms[readerIndex], sampleRetained, ct),
            deadline,
            CancellationDrainTimeout,
            detachedWorkRegistrar: context.QuarantineDetachedWork);

        int retainedLatencySamples = histograms.Sum(static histogram => histogram.SampleCount);
        if (retainedLatencySamples != phase.RetainedLatencySamples)
        {
            throw new InvalidOperationException(
                $"SQLite scenario '{name}' retained {retainedLatencySamples:N0} histogram samples " +
                $"but its measurement controller observed {phase.RetainedLatencySamples:N0}.");
        }

        return new BenchmarkResult
        {
            Name = name,
            TotalOps = histograms.Sum(static histogram => histogram.Count),
            LatencySamples = retainedLatencySamples,
            ElapsedMs = phase.Elapsed.TotalMilliseconds,
            P50Ms = histograms.Average(static histogram => histogram.Percentile(0.50)),
            P90Ms = histograms.Average(static histogram => histogram.Percentile(0.90)),
            P95Ms = histograms.Average(static histogram => histogram.Percentile(0.95)),
            P99Ms = histograms.Average(static histogram => histogram.Percentile(0.99)),
            P999Ms = histograms.Average(static histogram => histogram.Percentile(0.999)),
            MinMs = histograms.Min(static histogram => histogram.Min),
            MaxMs = histograms.Max(static histogram => histogram.Max),
            MeanMs = histograms.Average(static histogram => histogram.Mean),
            StdDevMs = histograms.Average(static histogram => histogram.StdDev),
            ExtraInfo = context.WithNotes(
                reuseSessionBurstReads
                    ? $"session-mode=reused read-only connection; burst-reads={ReusedSessionBurstReads}; latency-sampling=1/{latencySampleEvery}"
                    : "session-mode=per-query read-only connection",
                $"readers={ConcurrentReaderCount}",
                "workload=select count(*) from bench",
                CreateMeasurementPolicyNote(DefaultMeasurementPolicy))
        };
    }

    private static async Task<BenchmarkResult> RunSequentialScenarioAsync(
        string name,
        Func<CancellationToken, Task> operation,
        Action<Task> detachedWorkRegistrar)
    {
        await RunWarmupAsync(
            name,
            operation,
            DefaultMeasurementPolicy.WarmupDuration,
            CancellationDrainTimeout,
            detachedWorkRegistrar);
        MacroBenchmarkRunner.StabilizeAfterWarmup();

        using var deadline = new StopwatchMeasurementDeadline(
            DefaultMeasurementPolicy.MaximumMeasuredDuration);
        BenchmarkResult result = await RunSequentialScenarioCoreAsync(
            name,
            operation,
            DefaultMeasurementPolicy,
            deadline,
            CancellationDrainTimeout,
            detachedWorkRegistrar);
        return CloneResult(
            result,
            extraInfo: CreateMeasurementPolicyNote(DefaultMeasurementPolicy));
    }

    internal static async Task<BenchmarkResult> RunSequentialScenarioCoreAsync(
        string name,
        Func<CancellationToken, Task> operation,
        MeasurementPolicy policy,
        IMeasurementDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(deadline);
        ValidateCancellationDrainTimeout(cancellationDrainTimeout);
        policy.Validate();
        var histogram = new LatencyHistogram();
        var workerReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measurementStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var minimumSamplesReached = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var sampleCounter = new MeasurementSampleCounter(
            policy.MinimumLatencySamples,
            minimumSamplesReached);
        var recordingGate = new ConcurrentRecordingGate(sampleCounter);
        Task scenarioWorker = Task.Run(
            async () =>
            {
                workerReady.TrySetResult();
                await measurementStart.Task.ConfigureAwait(false);
                try
                {
                    while (!deadline.Token.IsCancellationRequested)
                    {
                        var operationStopwatch = Stopwatch.StartNew();
                        await operation(deadline.Token).ConfigureAwait(false);
                        operationStopwatch.Stop();
                        if (!recordingGate.TryRecord(
                                () => histogram.Record(operationStopwatch.Elapsed.TotalMilliseconds),
                                retainsLatencySample: true))
                        {
                            return;
                        }
                    }
                }
                catch (OperationCanceledException) when (deadline.Token.IsCancellationRequested)
                {
                    // Coordinated measurement cancellation is an expected worker exit.
                }
            },
            CancellationToken.None);

        await workerReady.Task.ConfigureAwait(false);
        deadline.Start();
        measurementStart.TrySetResult();
        Task minimumDurationReached = deadline.WaitUntilAsync(policy.MinimumMeasuredDuration);
        ConcurrentStopReason stopReason;
        TimeSpan measurementElapsed;

        while (true)
        {
            measurementElapsed = deadline.Elapsed;
            int sampleCount = sampleCounter.RetainedLatencySamples;
            if (!deadline.Expired.IsCompleted &&
                measurementElapsed < policy.MaximumMeasuredDuration &&
                HasMetMeasurementTarget(measurementElapsed, sampleCount, policy))
            {
                stopReason = ConcurrentStopReason.TargetReached;
                break;
            }

            if (scenarioWorker.IsFaulted)
            {
                stopReason = ConcurrentStopReason.ReaderExited;
                break;
            }

            if (deadline.Expired.IsCompleted ||
                measurementElapsed >= policy.MaximumMeasuredDuration)
            {
                stopReason = ConcurrentStopReason.Deadline;
                break;
            }

            if (scenarioWorker.IsCompleted)
            {
                stopReason = ConcurrentStopReason.ReaderExited;
                break;
            }

            Task durationSignal = minimumDurationReached.IsCompleted
                ? Task.Delay(Timeout.InfiniteTimeSpan)
                : minimumDurationReached;
            Task sampleSignal = minimumSamplesReached.Task.IsCompleted
                ? Task.Delay(Timeout.InfiniteTimeSpan)
                : minimumSamplesReached.Task;
            await Task.WhenAny(
                durationSignal,
                sampleSignal,
                deadline.Expired,
                scenarioWorker).ConfigureAwait(false);
        }

        await recordingGate.CloseAsync();
        var recordingSnapshot = new ConcurrentRecordingSnapshot(
            sampleCounter.RetainedLatencySamples);
        measurementElapsed = deadline.Elapsed;
        if (stopReason == ConcurrentStopReason.TargetReached &&
            (deadline.Expired.IsCompleted ||
             measurementElapsed >= policy.MaximumMeasuredDuration))
        {
            stopReason = ConcurrentStopReason.Deadline;
        }

        deadline.Cancel();
        bool workerStopped = await WaitForTaskCompletionWithinAsync(
            scenarioWorker,
            cancellationDrainTimeout);
        if (!workerStopped)
        {
            ObserveFaultEventually(scenarioWorker);
            detachedWorkRegistrar?.Invoke(scenarioWorker);
            Exception? innerException = stopReason == ConcurrentStopReason.Deadline
                ? CreateMeasurementCapException(
                    name,
                    policy,
                    measurementElapsed,
                    recordingSnapshot.RetainedLatencySamples)
                : null;
            throw CreateUnresponsiveException(
                name,
                "scenario worker",
                cancellationDrainTimeout,
                innerException);
        }

        await scenarioWorker.ConfigureAwait(false);

        if (stopReason == ConcurrentStopReason.ReaderExited)
        {
            throw new InvalidOperationException(
                $"SQLite release-core scenario '{name}' exited before the measurement target was reached.");
        }

        if (stopReason == ConcurrentStopReason.Deadline)
        {
            throw CreateMeasurementCapException(
                name,
                policy,
                measurementElapsed,
                recordingSnapshot.RetainedLatencySamples);
        }

        if (histogram.SampleCount != recordingSnapshot.RetainedLatencySamples)
        {
            throw new InvalidOperationException(
                $"SQLite scenario '{name}' retained {histogram.SampleCount:N0} histogram samples " +
                $"but its recording cutoff captured {recordingSnapshot.RetainedLatencySamples:N0}.");
        }

        return CreateAndPrintResult(name, histogram, measurementElapsed);
    }

    internal static async Task<ConcurrentReaderPhaseResult> RunConcurrentReaderWorkersAsync(
        string name,
        int readerCount,
        MeasurementPolicy policy,
        Func<int, TryRecordConcurrentOperation, CancellationToken, Task> readerLoop,
        IMeasurementDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        Func<Func<Task>, Task>? scheduleReader = null,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(readerCount, 1);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(readerLoop);
        ArgumentNullException.ThrowIfNull(deadline);
        ValidateCancellationDrainTimeout(cancellationDrainTimeout);
        policy.Validate();

        scheduleReader ??= static reader => Task.Run(reader);

        var allReadersReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measurementStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var minimumSamplesReached = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var readerExited = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var readerTasks = new Task[readerCount];
        int readyReaderCount = 0;
        var sampleCounter = new MeasurementSampleCounter(
            policy.MinimumLatencySamples,
            minimumSamplesReached);
        ConcurrentRecordingGate[] recordingGates = Enumerable
            .Range(0, readerCount)
            .Select(_ => new ConcurrentRecordingGate(sampleCounter))
            .ToArray();

        for (int readerIndex = 0; readerIndex < readerTasks.Length; readerIndex++)
        {
            int capturedReaderIndex = readerIndex;
            readerTasks[readerIndex] = scheduleReader(
                async () =>
                {
                    if (Interlocked.Increment(ref readyReaderCount) == readerCount)
                        allReadersReady.TrySetResult();

                    await measurementStart.Task.ConfigureAwait(false);
                    await readerLoop(
                        capturedReaderIndex,
                        recordingGates[capturedReaderIndex].TryRecord,
                        deadline.Token).ConfigureAwait(false);
                });

            _ = readerTasks[readerIndex].ContinueWith(
                static (_, state) => ((TaskCompletionSource)state!).TrySetResult(),
                readerExited,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        await allReadersReady.Task.ConfigureAwait(false);
        deadline.Start();
        measurementStart.TrySetResult();
        Task minimumDurationReached = deadline.WaitUntilAsync(policy.MinimumMeasuredDuration);
        Task allReaders = Task.WhenAll(readerTasks);
        ConcurrentStopReason stopReason;
        TimeSpan measurementElapsed;

        while (true)
        {
            measurementElapsed = deadline.Elapsed;
            int sampleCount = sampleCounter.RetainedLatencySamples;
            if (!deadline.Expired.IsCompleted &&
                measurementElapsed < policy.MaximumMeasuredDuration &&
                HasMetMeasurementTarget(measurementElapsed, sampleCount, policy))
            {
                stopReason = ConcurrentStopReason.TargetReached;
                break;
            }

            if (deadline.Expired.IsCompleted ||
                measurementElapsed >= policy.MaximumMeasuredDuration)
            {
                stopReason = ConcurrentStopReason.Deadline;
                break;
            }

            if (readerExited.Task.IsCompleted)
            {
                stopReason = ConcurrentStopReason.ReaderExited;
                break;
            }

            Task durationSignal = minimumDurationReached.IsCompleted
                ? Task.Delay(Timeout.InfiniteTimeSpan)
                : minimumDurationReached;
            Task sampleSignal = minimumSamplesReached.Task.IsCompleted
                ? Task.Delay(Timeout.InfiniteTimeSpan)
                : minimumSamplesReached.Task;
            await Task.WhenAny(
                durationSignal,
                sampleSignal,
                deadline.Expired,
                readerExited.Task).ConfigureAwait(false);
        }

        await Task.WhenAll(recordingGates.Select(static gate => gate.CloseAsync()));
        var recordingSnapshot = new ConcurrentRecordingSnapshot(
            sampleCounter.RetainedLatencySamples);
        measurementElapsed = deadline.Elapsed;
        if (stopReason == ConcurrentStopReason.TargetReached &&
            (deadline.Expired.IsCompleted ||
             measurementElapsed >= policy.MaximumMeasuredDuration))
        {
            stopReason = ConcurrentStopReason.Deadline;
        }
        deadline.Cancel();
        bool readersStopped = await WaitForTaskCompletionWithinAsync(
            allReaders,
            cancellationDrainTimeout);
        if (!readersStopped)
        {
            ObserveFaultEventually(allReaders);
            Exception? innerException = stopReason == ConcurrentStopReason.Deadline
                ? CreateMeasurementCapException(
                    name,
                    policy,
                    measurementElapsed,
                    recordingSnapshot.RetainedLatencySamples)
                : GetCompletedWorkerFailure(readerTasks);
            detachedWorkRegistrar?.Invoke(allReaders);
            throw CreateUnresponsiveException(
                name,
                $"{readerCount} reader workers",
                cancellationDrainTimeout,
                innerException);
        }

        await allReaders.ConfigureAwait(false);

        if (stopReason == ConcurrentStopReason.ReaderExited)
        {
            throw new InvalidOperationException(
                "A SQLite concurrent-reader worker exited before the measurement target was reached.");
        }

        if (stopReason == ConcurrentStopReason.Deadline)
        {
            throw CreateMeasurementCapException(
                name,
                policy,
                measurementElapsed,
                recordingSnapshot.RetainedLatencySamples);
        }

        return new ConcurrentReaderPhaseResult(
            measurementElapsed,
            recordingSnapshot.RetainedLatencySamples);
    }

    private static async Task RunPerQueryReaderLoopAsync(
        SqliteBenchmarkContext context,
        LatencyHistogram histogram,
        TryRecordConcurrentOperation tryRecord,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                using var connection = await context.OpenReadOnlyConnectionAsync(ct);
                long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;", ct);
                if (count != SeedCount)
                    throw new InvalidOperationException($"Expected COUNT(*)={SeedCount}, observed {count}.");
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }

            sw.Stop();
            _ = tryRecord(
                () => histogram.Record(sw.Elapsed.TotalMilliseconds),
                retainsLatencySample: true);
        }
    }

    private static async Task RunReusedReaderLoopAsync(
        SqliteBenchmarkContext context,
        LatencyHistogram histogram,
        TryRecordConcurrentOperation tryRecord,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var connection = await context.OpenReadOnlyConnectionAsync(ct);
                for (int i = 0; i < ReusedSessionBurstReads && !ct.IsCancellationRequested; i++)
                {
                    Stopwatch? sw = histogram.ShouldSampleNext() ? Stopwatch.StartNew() : null;
                    long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;", ct);
                    if (count != SeedCount)
                        throw new InvalidOperationException($"Expected COUNT(*)={SeedCount}, observed {count}.");

                    if (sw is null)
                    {
                        _ = tryRecord(
                            histogram.RecordUnsampled,
                            retainsLatencySample: false);
                    }
                    else
                    {
                        sw.Stop();
                        _ = tryRecord(
                            () => histogram.Record(sw.Elapsed.TotalMilliseconds),
                            retainsLatencySample: true);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private static async Task RunWarmupAsync(
        string name,
        Func<CancellationToken, Task> operation,
        TimeSpan warmupDuration,
        TimeSpan cancellationDrainTimeout,
        Action<Task>? detachedWorkRegistrar)
    {
        if (warmupDuration == TimeSpan.Zero)
            return;

        using var deadline = new StopwatchMeasurementDeadline(warmupDuration);
        await RunWarmupCoreAsync(
            name,
            operation,
            warmupDuration,
            deadline,
            cancellationDrainTimeout,
            detachedWorkRegistrar);
    }

    internal static async Task RunWarmupCoreAsync(
        string name,
        Func<CancellationToken, Task> operation,
        TimeSpan warmupDuration,
        IMeasurementDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        Action<Task>? detachedWorkRegistrar = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(deadline);
        if (warmupDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(warmupDuration));
        ValidateCancellationDrainTimeout(cancellationDrainTimeout);

        var workerReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var warmupStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task warmupWorker = Task.Run(
            async () =>
            {
                workerReady.TrySetResult();
                await warmupStart.Task.ConfigureAwait(false);
                try
                {
                    while (!deadline.Token.IsCancellationRequested)
                        await operation(deadline.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (deadline.Token.IsCancellationRequested)
                {
                    // The warmup deadline owns this cancellation.
                }
            },
            CancellationToken.None);

        await workerReady.Task.ConfigureAwait(false);
        deadline.Start();
        warmupStart.TrySetResult();
        Task completedTask = await Task.WhenAny(warmupWorker, deadline.Expired);
        if (completedTask == warmupWorker && warmupWorker.IsFaulted)
        {
            await warmupWorker.ConfigureAwait(false);
        }

        deadline.Cancel();
        bool workerStopped = await WaitForTaskCompletionWithinAsync(
            warmupWorker,
            cancellationDrainTimeout);
        if (!workerStopped)
        {
            ObserveFaultEventually(warmupWorker);
            detachedWorkRegistrar?.Invoke(warmupWorker);
            throw CreateUnresponsiveException(
                name,
                "warmup worker",
                cancellationDrainTimeout);
        }

        await warmupWorker.ConfigureAwait(false);
    }

    internal static bool HasMetMeasurementTarget(
        TimeSpan elapsed,
        int retainedLatencySamples,
        MeasurementPolicy policy)
        => elapsed >= policy.MinimumMeasuredDuration &&
           retainedLatencySamples >= policy.MinimumLatencySamples;

    internal static InvalidOperationException CreateMeasurementCapException(
        string name,
        MeasurementPolicy policy,
        TimeSpan elapsed,
        int retainedLatencySamples)
        => new(
            $"SQLite release-core scenario '{name}' reached its " +
            $"{policy.MaximumMeasuredDuration.TotalSeconds:F0}-second measurement cap after " +
            $"{elapsed.TotalSeconds:F1} seconds with {retainedLatencySamples:N0} retained latency samples. " +
            $"Qualification requires at least {policy.MinimumMeasuredDuration.TotalSeconds:F0} measured seconds " +
            $"and {policy.MinimumLatencySamples:N0} retained latency samples.");

    private static async Task<bool> WaitForTaskCompletionWithinAsync(Task task, TimeSpan timeout)
    {
        if (task.IsCompleted)
            return true;

        try
        {
            await task.WaitAsync(timeout);
            return true;
        }
        catch (TimeoutException) when (!task.IsCompleted)
        {
            return false;
        }
        catch
        {
            return true;
        }
    }

    private static Exception? GetCompletedWorkerFailure(IEnumerable<Task> workerTasks)
    {
        Exception[] failures = workerTasks
            .Where(static task => task.IsFaulted)
            .SelectMany(static task => task.Exception?.InnerExceptions ?? [])
            .ToArray();
        return failures.Length switch
        {
            0 => null,
            1 => failures[0],
            _ => new AggregateException(failures),
        };
    }

    private static void ObserveFaultEventually(Task task)
    {
        _ = task.ContinueWith(
            static completedTask => _ = completedTask.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private static InvalidOperationException CreateUnresponsiveException(
        string name,
        string pendingWorkDescription,
        TimeSpan cancellationDrainTimeout,
        Exception? innerException = null)
    {
        string prefix = innerException is null ? string.Empty : innerException.Message + " ";
        return new InvalidOperationException(
            prefix +
            $"Coordinated cancellation for SQLite release-core scenario '{name}' did not stop " +
            $"{pendingWorkDescription} within {cancellationDrainTimeout.TotalSeconds:F3} seconds.",
            innerException);
    }

    private static BenchmarkResult CreateAndPrintResult(
        string name,
        LatencyHistogram histogram,
        TimeSpan elapsed)
    {
        BenchmarkResult result = BenchmarkResult.FromHistogram(
            name,
            histogram,
            elapsed.TotalMilliseconds);
        Console.WriteLine(
            $"  {name}: {result.OpsPerSecond:N0} ops/sec, " +
            $"P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, " +
            $"P999={result.P999Ms:F3}ms ({result.LatencySamples:N0} retained samples)");
        return result;
    }

    private static string CreateMeasurementPolicyNote(MeasurementPolicy policy)
        => $"minimum-measured-seconds={policy.MinimumMeasuredDuration.TotalSeconds:F0}; " +
           $"minimum-retained-latency-samples={policy.MinimumLatencySamples}; " +
           $"measurement-cap-seconds={policy.MaximumMeasuredDuration.TotalSeconds:F0}";

    private static void ValidateCancellationDrainTimeout(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout));
    }

    private static string GetSingleInsertName()
        => "SQLite_WalFull_Sql_SingleInsert_5s";

    private static string GetBatchInsertName()
        => "SQLite_WalFull_Sql_Batch100_5s";

    private static string GetPreparedBulkInsertName(int batchSize)
        => $"SQLite_WalFull_Sql_PreparedBulk4Col_B{batchSize}_5s";

    private static string GetPointLookupName()
        => "SQLite_WalFull_Sql_PointLookup_20000";

    private static string GetConcurrentReadsName(bool reuseSessionBurstReads)
        => reuseSessionBurstReads
            ? $"SQLite_WalFull_Sql_ConcurrentReadsBurst{ReusedSessionBurstReads}_{ConcurrentReaderCount}readers"
            : $"SQLite_WalFull_Sql_ConcurrentReads_{ConcurrentReaderCount}readers";

    private static async Task WarmSqlLookupsAsync(SqliteConnection connection, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            long value = await ExecuteScalarInt64Async(connection, $"SELECT value FROM bench WHERE id = {id};");
            if (value != id * 10L)
                throw new InvalidOperationException($"Warm lookup for id={id} returned an unexpected result '{value}'.");
        }
    }

    private static async Task WarmConcurrentReadersAsync(SqliteBenchmarkContext context)
    {
        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            using var connection = await context.OpenReadOnlyConnectionAsync();
            for (int i = 0; i < 8; i++)
            {
                long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;");
                if (count != SeedCount)
                    throw new InvalidOperationException($"Warm concurrent read expected COUNT(*)={SeedCount}, observed {count}.");
            }
        }
    }

    private static async Task<int> ExecuteNonQueryAsync(
        SqliteConnection connection,
        string sql,
        SqliteTransaction? transaction = null,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return await command.ExecuteNonQueryAsync(ct);
    }

    private static SqliteParameter AddParameter(SqliteCommand command, string name, object? value)
    {
        SqliteParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static async Task<long> ExecuteScalarInt64Async(
        SqliteConnection connection,
        string sql,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        object? value = await command.ExecuteScalarAsync(ct);
        return value switch
        {
            long longValue => longValue,
            int intValue => intValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            null => throw new InvalidOperationException($"SQL '{sql}' returned null."),
            _ => Convert.ToInt64(value)
        };
    }

    private static async Task<string> ExecuteScalarTextAsync(
        SqliteConnection connection,
        string sql,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        object? value = await command.ExecuteScalarAsync(ct);
        return value?.ToString()?.Trim() ?? throw new InvalidOperationException($"SQL '{sql}' returned null.");
    }

    private static BenchmarkResult CloneResult(
        BenchmarkResult source,
        int? totalOps = null,
        string? extraInfo = null)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = totalOps ?? source.TotalOps,
            LatencySamples = source.LatencySamples,
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo ?? source.ExtraInfo
        };
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static string AppendExtraInfo(string? existing, params string?[] notes)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(existing))
            parts.Add(existing);

        foreach (string? note in notes)
        {
            if (!string.IsNullOrWhiteSpace(note))
                parts.Add(note);
        }

        return string.Join(", ", parts);
    }

    private static string GetProviderVersion()
    {
        Assembly assembly = typeof(SqliteConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    internal sealed record MeasurementPolicy(
        TimeSpan WarmupDuration,
        TimeSpan MinimumMeasuredDuration,
        int MinimumLatencySamples,
        TimeSpan MaximumMeasuredDuration)
    {
        internal void Validate()
        {
            if (WarmupDuration < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(WarmupDuration),
                    "SQLite warmup duration cannot be negative.");
            }

            if (MinimumMeasuredDuration <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumMeasuredDuration),
                    "SQLite minimum measured duration must be positive.");
            }

            if (MinimumLatencySamples <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumLatencySamples),
                    "SQLite minimum latency sample count must be positive.");
            }

            if (MaximumMeasuredDuration < MinimumMeasuredDuration)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MaximumMeasuredDuration),
                    "SQLite measurement cap must be at least the minimum measured duration.");
            }
        }
    }

    internal interface IMeasurementDeadline : IDisposable
    {
        TimeSpan Elapsed { get; }
        CancellationToken Token { get; }
        Task Expired { get; }
        void Start();
        Task WaitUntilAsync(TimeSpan elapsed);
        void Cancel();
    }

    internal sealed record ConcurrentReaderPhaseResult(
        TimeSpan Elapsed,
        int RetainedLatencySamples);

    internal delegate bool TryRecordConcurrentOperation(
        Action record,
        bool retainsLatencySample);

    private sealed class ConcurrentRecordingGate
    {
        private readonly MeasurementSampleCounter _sampleCounter;
        private readonly TaskCompletionSource _drained =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _closed;
        private int _inFlightRecorders;

        internal ConcurrentRecordingGate(MeasurementSampleCounter sampleCounter)
        {
            _sampleCounter = sampleCounter;
        }

        internal bool TryRecord(Action record, bool retainsLatencySample)
        {
            ArgumentNullException.ThrowIfNull(record);

            if (Volatile.Read(ref _closed) != 0)
                return false;

            Interlocked.Increment(ref _inFlightRecorders);
            if (Volatile.Read(ref _closed) != 0)
            {
                ExitRecorder();
                return false;
            }

            try
            {
                record();
                if (retainsLatencySample)
                    _sampleCounter.RecordRetainedSample();
                return true;
            }
            finally
            {
                ExitRecorder();
            }
        }

        internal async Task CloseAsync()
        {
            Interlocked.Exchange(ref _closed, 1);
            if (Volatile.Read(ref _inFlightRecorders) == 0)
                _drained.TrySetResult();
            await _drained.Task.ConfigureAwait(false);
        }

        private void ExitRecorder()
        {
            if (Interlocked.Decrement(ref _inFlightRecorders) == 0 &&
                Volatile.Read(ref _closed) != 0)
            {
                _drained.TrySetResult();
            }
        }
    }

    private sealed class MeasurementSampleCounter
    {
        private readonly int _minimumLatencySamples;
        private readonly TaskCompletionSource _minimumSamplesReached;
        private int _retainedLatencySamples;

        internal MeasurementSampleCounter(
            int minimumLatencySamples,
            TaskCompletionSource minimumSamplesReached)
        {
            _minimumLatencySamples = minimumLatencySamples;
            _minimumSamplesReached = minimumSamplesReached;
        }

        internal int RetainedLatencySamples => Volatile.Read(ref _retainedLatencySamples);

        internal void RecordRetainedSample()
        {
            int sampleCount = Interlocked.Increment(ref _retainedLatencySamples);
            if (sampleCount == _minimumLatencySamples)
                _minimumSamplesReached.TrySetResult();
        }
    }

    private sealed record ConcurrentRecordingSnapshot(int RetainedLatencySamples);

    private enum ConcurrentStopReason
    {
        TargetReached,
        Deadline,
        ReaderExited,
    }

    private sealed class StopwatchMeasurementDeadline : IMeasurementDeadline
    {
        private readonly TimeSpan _maximumDuration;
        private readonly Stopwatch _stopwatch = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _expired;
        private bool _started;

        internal StopwatchMeasurementDeadline(TimeSpan maximumDuration)
        {
            if (maximumDuration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(maximumDuration));

            _maximumDuration = maximumDuration;
            _expired = Task.Delay(Timeout.InfiniteTimeSpan, _cts.Token);
        }

        public TimeSpan Elapsed => _stopwatch.Elapsed;
        public CancellationToken Token => _cts.Token;
        public Task Expired => _expired;

        public void Start()
        {
            if (_started)
                throw new InvalidOperationException("The SQLite measurement deadline has already started.");

            _started = true;
            _stopwatch.Start();
            _cts.CancelAfter(_maximumDuration);
        }

        public Task WaitUntilAsync(TimeSpan elapsed)
        {
            if (!_started)
                throw new InvalidOperationException("The SQLite measurement deadline has not started.");
            if (elapsed < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(elapsed));

            TimeSpan remaining = elapsed - Elapsed;
            return remaining <= TimeSpan.Zero
                ? Task.CompletedTask
                : Task.Delay(remaining, _cts.Token);
        }

        public void Cancel()
        {
            if (!_cts.IsCancellationRequested)
                _cts.Cancel();
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            Cancel();
            _cts.Dispose();
        }
    }

    private sealed class SqliteBenchmarkContext : IAsyncDisposable
    {
        private readonly object _lifetimeGate = new();
        private readonly string _filePath;
        private readonly List<IDisposable> _ownedResources = [];
        private readonly List<Task> _detachedWork = [];
        private int _resourcesDisposed;

        private SqliteBenchmarkContext(string filePath, SqliteConnection keeperConnection)
        {
            _filePath = filePath;
            KeeperConnection = keeperConnection;
        }

        internal SqliteConnection KeeperConnection { get; }

        internal T Own<T>(T resource)
            where T : IDisposable
        {
            lock (_lifetimeGate)
                _ownedResources.Add(resource);
            return resource;
        }

        internal void QuarantineDetachedWork(Task task)
        {
            ArgumentNullException.ThrowIfNull(task);
            ObserveFaultEventually(task);
            lock (_lifetimeGate)
                _detachedWork.Add(task);
        }

        internal static async Task<SqliteBenchmarkContext> CreateWritableAsync(string prefix)
        {
            string filePath = NewTempDbPath(prefix);
            SqliteConnection keeperConnection = await OpenWritableConnectionAsync(filePath);
            await CreateSchemaAsync(keeperConnection);
            return new SqliteBenchmarkContext(filePath, keeperConnection);
        }

        internal static async Task<SqliteBenchmarkContext> CreateWritableAsync(string prefix, string createTableSql)
        {
            string filePath = NewTempDbPath(prefix);
            SqliteConnection keeperConnection = await OpenWritableConnectionAsync(filePath);
            await CreateSchemaAsync(keeperConnection, createTableSql);
            return new SqliteBenchmarkContext(filePath, keeperConnection);
        }

        internal static async Task<SqliteBenchmarkContext> CreateReadSeededAsync(string prefix)
        {
            string filePath = NewTempDbPath(prefix);
            SqliteConnection keeperConnection = await OpenWritableConnectionAsync(filePath);
            await CreateSchemaAsync(keeperConnection);
            await SeedAsync(keeperConnection);
            return new SqliteBenchmarkContext(filePath, keeperConnection);
        }

        internal async Task<SqliteConnection> OpenReadOnlyConnectionAsync(CancellationToken ct = default)
        {
            var connection = new SqliteConnection(CreateConnectionString(_filePath, SqliteOpenMode.ReadOnly));
            await connection.OpenAsync(ct);
            return connection;
        }

        internal string WithNotes(params string?[] notes)
            => AppendExtraInfo($"{s_providerInfo}, {ConnectionInfo}", notes);

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
            {
                DisposeResources();
                return ValueTask.CompletedTask;
            }

            Task lifetime = Task.WhenAll(pendingWork);
            _ = lifetime.ContinueWith(
                static (completedTask, state) =>
                {
                    _ = completedTask.Exception;
                    ((SqliteBenchmarkContext)state!).DisposeResources();
                },
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return ValueTask.CompletedTask;
        }

        private void DisposeResources()
        {
            if (Interlocked.Exchange(ref _resourcesDisposed, 1) != 0)
                return;

            IDisposable[] ownedResources;
            lock (_lifetimeGate)
                ownedResources = _ownedResources.ToArray();

            for (int i = ownedResources.Length - 1; i >= 0; i--)
            {
                try
                {
                    ownedResources[i].Dispose();
                }
                catch
                {
                    // Cleanup must not replace the benchmark's explicit failure.
                }
            }

            KeeperConnection.Dispose();
            DeleteSqliteFiles(_filePath);
        }

        private static async Task<SqliteConnection> OpenWritableConnectionAsync(string filePath, CancellationToken ct = default)
        {
            var connection = new SqliteConnection(CreateConnectionString(filePath, SqliteOpenMode.ReadWriteCreate));
            await connection.OpenAsync(ct);
            await ApplyAndVerifyWritePragmasAsync(connection, ct);
            return connection;
        }

        private static async Task ApplyAndVerifyWritePragmasAsync(SqliteConnection connection, CancellationToken ct)
        {
            string journalMode = await ExecuteScalarTextAsync(connection, "PRAGMA journal_mode=WAL;", ct);
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal, observed '{journalMode}'.");

            await ExecuteNonQueryAsync(connection, "PRAGMA synchronous=FULL;", ct: ct);

            string verifiedJournalMode = await ExecuteScalarTextAsync(connection, "PRAGMA journal_mode;", ct);
            string verifiedSynchronous = await ExecuteScalarTextAsync(connection, "PRAGMA synchronous;", ct);

            if (!verifiedJournalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal after verification, observed '{verifiedJournalMode}'.");

            if (!verifiedSynchronous.Equals("full", StringComparison.OrdinalIgnoreCase) &&
                !verifiedSynchronous.Equals("2", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Expected synchronous=FULL, observed '{verifiedSynchronous}'.");
            }
        }

        private static async Task CreateSchemaAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            await CreateSchemaAsync(
                connection,
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);",
                ct);
        }

        private static async Task CreateSchemaAsync(
            SqliteConnection connection,
            string createTableSql,
            CancellationToken ct = default)
        {
            await ExecuteNonQueryAsync(
                connection,
                createTableSql,
                ct: ct);
        }

        private static async Task SeedAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += SeedBatchSize)
            {
                using var transaction = connection.BeginTransaction();
                try
                {
                    int batchEnd = Math.Min(batchStart + SeedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        int rowsAffected = await ExecuteNonQueryAsync(
                            connection,
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                            transaction,
                            ct);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted seed row for id={id}, observed {rowsAffected}.");
                    }

                    transaction.Commit();
                }
                catch
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // Preserve the original seed failure.
                    }

                    throw;
                }
            }
        }

        private static string CreateConnectionString(string filePath, SqliteOpenMode mode)
        {
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = filePath,
                Mode = mode,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            };

            return builder.ToString();
        }

        private static void DeleteSqliteFiles(string filePath)
        {
            try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
            try { if (File.Exists(filePath + "-wal")) File.Delete(filePath + "-wal"); } catch { }
            try { if (File.Exists(filePath + "-shm")) File.Delete(filePath + "-shm"); } catch { }
        }

        private static string NewTempDbPath(string prefix)
            => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
    }
}
