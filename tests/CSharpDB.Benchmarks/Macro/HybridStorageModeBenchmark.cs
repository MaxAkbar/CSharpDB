using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridStorageModeBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int InsertTradeoffRowsPerCommit = 1_000;
    private const int InsertTradeoffSeedRows = 20_000;
    private const int ConcurrentReaderCount = 8;
    private const int ReusedSessionBurstReads = 32;
    private const int HighThroughputLatencySampleEvery = 128;
    private static readonly TimeSpan QualificationCancellationDrainTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan QualificationControllerPollInterval = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan InsertTradeoffMeasuredDuration = TimeSpan.FromSeconds(10);
    private static readonly InsertTradeoffScenario[] s_insertTradeoffScenarios = CreateInsertTradeoffScenarios();
    private static readonly ScenarioDefinition[] s_scenarios = CreateScenarioDefinitions();
    private static readonly IReadOnlyList<string> s_scenarioNames = Array.AsReadOnly(
        s_scenarios.Select(static scenario => scenario.Name).ToArray());

    internal static QualificationSettings DefaultQualificationSettings { get; } = new(
        WarmupDuration: TimeSpan.FromSeconds(2),
        MinimumMeasuredDuration: TimeSpan.FromSeconds(30),
        MinimumLatencySamples: 10_000,
        MaximumMeasuredDuration: TimeSpan.FromSeconds(120));

    private sealed record BenchDoc(string Name, int Value, string Category);

    private enum StorageMode
    {
        FileBacked,
        InMemory,
        HybridIncrementalDurable,
    }

    internal enum ConcurrentExecutionPath
    {
        Legacy,
        Qualification,
    }

    /// <summary>
    /// Exact result-row names accepted by <see cref="RunNamedScenarioAsync"/> and
    /// <see cref="RunNamedQualificationScenarioAsync"/>.
    /// </summary>
    public static IReadOnlyList<string> ScenarioNames => s_scenarioNames;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);
        foreach (ScenarioDefinition scenario in s_scenarios)
            results.Add(await scenario.RunAsync(null));

        return results;
    }

    public static Task<BenchmarkResult> RunNamedScenarioAsync(string scenarioName)
        => GetScenario(scenarioName).RunAsync(null);

    public static Task<BenchmarkResult> RunNamedQualificationScenarioAsync(string scenarioName)
        => RunNamedQualificationScenarioAsync(scenarioName, DefaultQualificationSettings);

    internal static Task<BenchmarkResult> RunNamedQualificationScenarioAsync(
        string scenarioName,
        QualificationSettings qualificationSettings)
    {
        ArgumentNullException.ThrowIfNull(qualificationSettings);
        qualificationSettings.Validate();
        return GetScenario(scenarioName).RunAsync(qualificationSettings);
    }

    private static ScenarioDefinition GetScenario(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);

        ScenarioDefinition? scenario = s_scenarios.FirstOrDefault(
            scenario => string.Equals(scenario.Name, scenarioName, StringComparison.Ordinal));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown hybrid storage-mode scenario '{scenarioName}'. " +
                "Use one of the exact names returned by HybridStorageModeBenchmark.ScenarioNames.",
                nameof(scenarioName));
        }

        return scenario;
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateSqlWriteAsync(mode);
        var db = context.Database;
        int nextId = SeedCount + 1_000_000;

        string benchmarkName = GetSqlSingleInsertName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                int id = nextId++;
                await using var result = await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                    ct);
                if (result.RowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {result.RowsAffected}.");
            });
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateSqlWriteAsync(mode);
        var db = context.Database;
        int nextId = SeedCount + 2_000_000;

        string benchmarkName = GetSqlBatchInsertName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                await db.BeginTransactionAsync(ct);
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        await using var result = await db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                            ct);
                        if (result.RowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {result.RowsAffected}.");
                    }

                    await db.CommitAsync(ct);
                }
                catch
                {
                    await RollbackQuietlyAsync(db);
                    throw;
                }
            });
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateSqlReadAsync(mode);
        var db = context.Database;
        var rng = new Random(42);

        if (UsesLegacyReadPriming(qualificationSettings))
        {
            await WarmSqlLookupsAsync(db, rng, 128);
            rng = new Random(42);
        }
        string benchmarkName = GetSqlPointLookupName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                int id = rng.Next(1, SeedCount + 1);
                await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};", ct);
                if (!await result.MoveNextAsync(ct) || result.Current[0].AsInteger != id * 10L)
                    throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result.");
            });
    }

    private static async Task<BenchmarkResult> RunCollectionPutAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateCollectionWriteAsync(mode);
        var collection = context.Collection!;
        int nextId = SeedCount + 3_000_000;

        string benchmarkName = GetCollectionPutName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                int id = nextId++;
                await collection.PutAsync(
                    $"doc:{id}",
                    new BenchDoc($"User_{id}", id, GetCategory(id)),
                    ct);
            });
    }

    private static async Task<BenchmarkResult> RunCollectionBatchInsertAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateCollectionWriteAsync(mode);
        var db = context.Database;
        var collection = context.Collection!;
        int nextId = SeedCount + 4_000_000;

        string benchmarkName = GetCollectionBatchInsertName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                await db.BeginTransactionAsync(ct);
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        await collection.PutAsync(
                            $"doc:{id}",
                            new BenchDoc($"User_{id}", id, GetCategory(id)),
                            ct);
                    }

                    await db.CommitAsync(ct);
                }
                catch
                {
                    await RollbackQuietlyAsync(db);
                    throw;
                }
            });
    }

    private static async Task<BenchmarkResult> RunCollectionGetAsync(
        StorageMode mode,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateCollectionReadAsync(mode);
        var collection = context.Collection!;
        var rng = new Random(84);

        if (UsesLegacyReadPriming(qualificationSettings))
        {
            await WarmCollectionGetsAsync(collection, rng, 128);
            rng = new Random(84);
        }
        string benchmarkName = GetCollectionGetName(mode);
        return await RunTimedOperationAsync(
            benchmarkName,
            MeasuredDuration,
            qualificationSettings,
            async ct =>
            {
                int id = rng.Next(1, SeedCount + 1);
                BenchDoc? document = await collection.GetAsync($"doc:{id}", ct);
                if (document is null || document.Value != id)
                    throw new InvalidOperationException($"Document 'doc:{id}' was not found or was invalid.");
            });
    }

    private static async Task<BenchmarkResult> RunInsertTradeoffScenarioAsync(
        InsertTradeoffScenario scenario,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await InsertTradeoffContext.CreateAsync(scenario);
        var db = context.Database;
        var batch = db.PrepareInsertBatch("bench", initialCapacity: InsertTradeoffRowsPerCommit);
        var rowBuffer = new DbValue[4];
        DbValue textValue = DbValue.FromText("durable_batch");
        DbValue categoryValue = DbValue.FromText("Alpha");
        int nextSequence = scenario.SeedRows;

        async Task OperationAsync(CancellationToken ct)
        {
            nextSequence = await ExecuteInsertBatchCommitAsync(
                db,
                batch,
                rowBuffer,
                nextSequence,
                InsertTradeoffRowsPerCommit,
                textValue,
                categoryValue,
                ct);
        }

        string benchmarkName = GetInsertTradeoffName(scenario);
        BenchmarkResult rawResult = await RunTimedOperationAsync(
            benchmarkName,
            InsertTradeoffMeasuredDuration,
            qualificationSettings,
            OperationAsync);

        double rowsPerSecond = rawResult.OpsPerSecond * InsertTradeoffRowsPerCommit;
        string? extraInfo = AppendExtraInfo(
            rawResult.ExtraInfo,
            $"throughput-unit=commits/sec; rowsPerSec={rowsPerSecond:F1}",
            $"rowsPerCommit={InsertTradeoffRowsPerCommit}",
            $"seedRows={scenario.SeedRows}",
            "schema=bench(id,value,text_col,category)",
            "keyPattern=monotonic",
            $"preset={scenario.PresetLabel}",
            $"durability={scenario.DurabilitySemantics}",
            $"residency={scenario.ResidencySemantics}");

        Console.WriteLine(
            $"    rows/sec={rowsPerSecond:N0}, P50={rawResult.P50Ms:F3}ms, P95={rawResult.P95Ms:F3}ms, P99={rawResult.P99Ms:F3}ms");
        Console.WriteLine($"    durability={scenario.DurabilitySemantics}");
        Console.WriteLine($"    residency={scenario.ResidencySemantics}");

        return CloneResult(rawResult, extraInfo);
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(
        StorageMode mode,
        bool reuseSessionBurstReads,
        QualificationSettings? qualificationSettings)
    {
        await using var context = await BenchmarkContext.CreateSqlReadAsync(mode);
        var db = context.Database;
        int latencySampleEvery = reuseSessionBurstReads ? HighThroughputLatencySampleEvery : 1;
        string benchmarkName = GetSqlConcurrentReadsName(mode, reuseSessionBurstReads);

        if (GetConcurrentExecutionPath(qualificationSettings) == ConcurrentExecutionPath.Legacy)
        {
            return await RunLegacyConcurrentReadsAsync(
                db,
                reuseSessionBurstReads,
                latencySampleEvery,
                benchmarkName);
        }

        return await RunQualifiedConcurrentReadsAsync(
            db,
            reuseSessionBurstReads,
            latencySampleEvery,
            benchmarkName,
            qualificationSettings!);
    }

    internal static ConcurrentExecutionPath GetConcurrentExecutionPath(
        QualificationSettings? qualificationSettings)
        => qualificationSettings is null
            ? ConcurrentExecutionPath.Legacy
            : ConcurrentExecutionPath.Qualification;

    internal static bool UsesLegacyReadPriming(QualificationSettings? qualificationSettings)
        => qualificationSettings is null;

    private static async Task<BenchmarkResult> RunLegacyConcurrentReadsAsync(
        Database db,
        bool reuseSessionBurstReads,
        int latencySampleEvery,
        string benchmarkName)
    {
        var histograms = new LatencyHistogram[ConcurrentReaderCount];
        for (int i = 0; i < histograms.Length; i++)
            histograms[i] = new LatencyHistogram(latencySampleEvery);

        using var cts = new CancellationTokenSource(MeasuredDuration);
        var readerTasks = new Task[ConcurrentReaderCount];
        for (int readerIndex = 0; readerIndex < readerTasks.Length; readerIndex++)
        {
            LatencyHistogram histogram = histograms[readerIndex];
            readerTasks[readerIndex] = Task.Run(
                () => reuseSessionBurstReads
                    ? RunReusedReaderLoopAsync(
                        db,
                        histogram,
                        latencySampleRecorded: null,
                        completionElapsedProvider: null,
                        maximumMeasuredDuration: TimeSpan.Zero,
                        discardCompletedAfterCancellation: false,
                        cts.Token)
                    : RunPerQueryReaderLoopAsync(
                        db,
                        histogram,
                        latencySampleRecorded: null,
                        completionElapsedProvider: null,
                        maximumMeasuredDuration: TimeSpan.Zero,
                        discardCompletedAfterCancellation: false,
                        cts.Token),
                cts.Token);
        }

        await Task.WhenAll(readerTasks);

        return CreateConcurrentReadResult(
            benchmarkName,
            histograms,
            MeasuredDuration,
            reuseSessionBurstReads,
            latencySampleEvery,
            qualificationSettings: null,
            measurementStartedUtc: null,
            measurementEndedUtc: null);
    }

    private static async Task<BenchmarkResult> RunQualifiedConcurrentReadsAsync(
        Database db,
        bool reuseSessionBurstReads,
        int latencySampleEvery,
        string benchmarkName,
        QualificationSettings qualificationSettings)
    {
        if (qualificationSettings.WarmupDuration > TimeSpan.Zero)
        {
            await RunConcurrentReaderPhaseAsync(
                db,
                reuseSessionBurstReads,
                latencySampleEvery,
                minimumMeasuredDuration: qualificationSettings.WarmupDuration,
                minimumLatencySamples: 0,
                maximumMeasuredDuration: qualificationSettings.WarmupDuration,
                failAtMaximum: false,
                $"{benchmarkName} warmup");
            MacroBenchmarkRunner.StabilizeAfterWarmup();
        }

        ConcurrentReaderPhaseResult qualifiedPhase = await RunConcurrentReaderPhaseAsync(
            db,
            reuseSessionBurstReads,
            latencySampleEvery,
            qualificationSettings.MinimumMeasuredDuration,
            qualificationSettings.MinimumLatencySamples,
            qualificationSettings.MaximumMeasuredDuration,
            failAtMaximum: true,
            benchmarkName);

        return CreateConcurrentReadResult(
            benchmarkName,
            qualifiedPhase.Histograms,
            qualifiedPhase.Elapsed,
            reuseSessionBurstReads,
            latencySampleEvery,
            qualificationSettings,
            qualifiedPhase.MeasurementStartedUtc,
            qualifiedPhase.MeasurementEndedUtc);
    }

    private static async Task RunPerQueryReaderLoopAsync(
        Database db,
        LatencyHistogram histogram,
        Action<TimeSpan>? latencySampleRecorded,
        Func<TimeSpan>? completionElapsedProvider,
        TimeSpan maximumMeasuredDuration,
        bool discardCompletedAfterCancellation,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                using var reader = db.CreateReaderSession();
                await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", ct);
                _ = await result.MoveNextAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }

            TimeSpan completionElapsed = completionElapsedProvider?.Invoke() ?? TimeSpan.Zero;
            if (discardCompletedAfterCancellation &&
                (ct.IsCancellationRequested || completionElapsed > maximumMeasuredDuration))
            {
                return;
            }

            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
            latencySampleRecorded?.Invoke(completionElapsed);
        }
    }

    private static async Task RunReusedReaderLoopAsync(
        Database db,
        LatencyHistogram histogram,
        Action<TimeSpan>? latencySampleRecorded,
        Func<TimeSpan>? completionElapsedProvider,
        TimeSpan maximumMeasuredDuration,
        bool discardCompletedAfterCancellation,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            using var reader = db.CreateReaderSession();
            for (int i = 0; i < ReusedSessionBurstReads && !ct.IsCancellationRequested; i++)
            {
                Stopwatch? sw = histogram.ShouldSampleNext() ? Stopwatch.StartNew() : null;
                try
                {
                    await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench;", ct);
                    _ = await result.MoveNextAsync(ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }

                TimeSpan completionElapsed = completionElapsedProvider?.Invoke() ?? TimeSpan.Zero;
                if (discardCompletedAfterCancellation &&
                    (ct.IsCancellationRequested || completionElapsed > maximumMeasuredDuration))
                {
                    return;
                }

                if (sw is null)
                {
                    histogram.RecordUnsampled();
                }
                else
                {
                    sw.Stop();
                    histogram.Record(sw.Elapsed.TotalMilliseconds);
                    latencySampleRecorded?.Invoke(completionElapsed);
                }
            }
        }
    }

    private static async Task<ConcurrentReaderPhaseResult> RunConcurrentReaderPhaseAsync(
        Database db,
        bool reuseSessionBurstReads,
        int latencySampleEvery,
        TimeSpan minimumMeasuredDuration,
        int minimumLatencySamples,
        TimeSpan maximumMeasuredDuration,
        bool failAtMaximum,
        string? benchmarkName = null)
    {
        using var deadline = new StopwatchQualificationDeadline(maximumMeasuredDuration);
        return await RunConcurrentReaderPhaseCoreAsync(
            benchmarkName ?? "hybrid concurrent-read scenario",
            ConcurrentReaderCount,
            latencySampleEvery,
            minimumMeasuredDuration,
            minimumLatencySamples,
            maximumMeasuredDuration,
            failAtMaximum,
            (_, histogram, latencySampleRecorded, ct) => reuseSessionBurstReads
                ? RunReusedReaderLoopAsync(
                    db,
                    histogram,
                    latencySampleRecorded,
                    () => deadline.Elapsed,
                    maximumMeasuredDuration,
                    discardCompletedAfterCancellation: true,
                    ct)
                : RunPerQueryReaderLoopAsync(
                    db,
                    histogram,
                    latencySampleRecorded,
                    () => deadline.Elapsed,
                    maximumMeasuredDuration,
                    discardCompletedAfterCancellation: true,
                    ct),
            deadline,
            QualificationCancellationDrainTimeout);
    }

    internal static async Task<ConcurrentReaderPhaseResult> RunConcurrentReaderPhaseCoreAsync(
        string benchmarkName,
        int readerCount,
        int latencySampleEvery,
        TimeSpan minimumMeasuredDuration,
        int minimumLatencySamples,
        TimeSpan maximumMeasuredDuration,
        bool failAtMaximum,
        Func<int, LatencyHistogram, Action<TimeSpan>?, CancellationToken, Task> readerLoop,
        IQualificationDeadline deadline,
        TimeSpan cancellationDrainTimeout)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(benchmarkName);
        ArgumentOutOfRangeException.ThrowIfLessThan(readerCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(latencySampleEvery, 1);
        ArgumentNullException.ThrowIfNull(readerLoop);
        ArgumentNullException.ThrowIfNull(deadline);
        if (cancellationDrainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cancellationDrainTimeout));

        var histograms = new LatencyHistogram[readerCount];
        for (int i = 0; i < histograms.Length; i++)
            histograms[i] = new LatencyHistogram(latencySampleEvery);

        long retainedLatencySamples = 0;
        long targetReachedTicks = -1;
        int coordinatedStopRequested = 0;
        int unexpectedReaderExit = 0;
        Action<TimeSpan> latencySampleRecorded = completionElapsed =>
        {
            TimeSpan callbackElapsed = deadline.Elapsed;
            TimeSpan sampleElapsed = completionElapsed > callbackElapsed
                ? completionElapsed
                : callbackElapsed;
            if (sampleElapsed < TimeSpan.Zero ||
                sampleElapsed > maximumMeasuredDuration)
            {
                return;
            }

            long retainedSamples = Interlocked.Increment(ref retainedLatencySamples);
            if (retainedSamples >= minimumLatencySamples &&
                sampleElapsed >= minimumMeasuredDuration)
            {
                Interlocked.CompareExchange(
                    ref targetReachedTicks,
                    sampleElapsed.Ticks,
                    comparand: -1);
            }
        };
        using var phaseCts = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        var readerTasks = new Task[readerCount];
        for (int readerIndex = 0; readerIndex < readerTasks.Length; readerIndex++)
        {
            int capturedReaderIndex = readerIndex;
            LatencyHistogram histogram = histograms[readerIndex];
            readerTasks[readerIndex] = Task.Run(
                async () =>
                {
                    try
                    {
                        await readerLoop(
                            capturedReaderIndex,
                            histogram,
                            latencySampleRecorded,
                            phaseCts.Token);
                    }
                    finally
                    {
                        if (Volatile.Read(ref coordinatedStopRequested) == 0 &&
                            !phaseCts.IsCancellationRequested)
                        {
                            Interlocked.Exchange(ref unexpectedReaderExit, 1);
                        }
                    }
                });
        }

        Task allReaders = Task.WhenAll(readerTasks);
        Task<Task> firstReaderCompletion = Task.WhenAny(readerTasks);
        ConcurrentPhaseStopReason stopReason;
        TimeSpan stoppedElapsed;
        DateTimeOffset measurementEndedUtc;

        while (true)
        {
            if (Volatile.Read(ref unexpectedReaderExit) != 0)
            {
                stopReason = ConcurrentPhaseStopReason.UnexpectedReaderExit;
                stoppedElapsed = deadline.Elapsed;
                break;
            }

            long observedTargetTicks = Volatile.Read(ref targetReachedTicks);
            if (observedTargetTicks >= 0)
            {
                stopReason = ConcurrentPhaseStopReason.TargetReached;
                stoppedElapsed = TimeSpan.FromTicks(observedTargetTicks);
                break;
            }

            stoppedElapsed = deadline.Elapsed;
            if (deadline.Expired.IsCompleted || stoppedElapsed >= maximumMeasuredDuration)
            {
                stopReason = ConcurrentPhaseStopReason.Deadline;
                break;
            }

            await Task.WhenAny(
                deadline.Expired,
                firstReaderCompletion,
                Task.Delay(QualificationControllerPollInterval));
        }

        if (stopReason == ConcurrentPhaseStopReason.Deadline)
        {
            long observedTargetTicks = Volatile.Read(ref targetReachedTicks);
            if (observedTargetTicks >= 0 && observedTargetTicks <= maximumMeasuredDuration.Ticks)
            {
                stopReason = ConcurrentPhaseStopReason.TargetReached;
                stoppedElapsed = TimeSpan.FromTicks(observedTargetTicks);
            }
        }

        measurementEndedUtc = stopReason == ConcurrentPhaseStopReason.TargetReached
            ? deadline.StartedUtc + stoppedElapsed
            : deadline.UtcNow;
        Interlocked.Exchange(ref coordinatedStopRequested, 1);
        if (stopReason != ConcurrentPhaseStopReason.UnexpectedReaderExit &&
            Volatile.Read(ref unexpectedReaderExit) != 0)
        {
            stopReason = ConcurrentPhaseStopReason.UnexpectedReaderExit;
        }
        if (stopReason == ConcurrentPhaseStopReason.Deadline)
            deadline.Cancel();
        phaseCts.Cancel();

        bool readersStopped = await WaitForTaskCompletionWithinAsync(
            allReaders,
            cancellationDrainTimeout);
        if (!readersStopped)
        {
            int outstandingReaders = readerTasks.Count(static task => !task.IsCompleted);
            Exception? capException = failAtMaximum && stopReason == ConcurrentPhaseStopReason.Deadline
                ? CreateQualificationCapException(
                    benchmarkName,
                    maximumMeasuredDuration,
                    stoppedElapsed,
                    checked((int)Math.Min(Volatile.Read(ref retainedLatencySamples), int.MaxValue)),
                    minimumMeasuredDuration,
                    minimumLatencySamples)
                : null;
            throw CreateQualificationUnresponsiveException(
                benchmarkName,
                $"{outstandingReaders} concurrent reader(s)",
                cancellationDrainTimeout,
                capException);
        }

        if (stopReason == ConcurrentPhaseStopReason.UnexpectedReaderExit)
        {
            await allReaders;
            throw new InvalidOperationException(
                $"Hybrid storage qualification scenario '{benchmarkName}' had a concurrent reader " +
                "exit before coordinated cancellation; the requested reader count was not maintained.");
        }

        await allReaders;
        int finalLatencySamples = checked((int)Math.Min(
            Volatile.Read(ref retainedLatencySamples),
            int.MaxValue));
        if (stopReason == ConcurrentPhaseStopReason.Deadline && failAtMaximum)
        {
            throw CreateQualificationCapException(
                benchmarkName,
                maximumMeasuredDuration,
                stoppedElapsed,
                finalLatencySamples,
                minimumMeasuredDuration,
                minimumLatencySamples);
        }

        return new ConcurrentReaderPhaseResult(
            histograms,
            stoppedElapsed,
            deadline.StartedUtc,
            measurementEndedUtc);
    }

    private static BenchmarkResult CreateConcurrentReadResult(
        string benchmarkName,
        LatencyHistogram[] histograms,
        TimeSpan elapsed,
        bool reuseSessionBurstReads,
        int latencySampleEvery,
        QualificationSettings? qualificationSettings,
        DateTimeOffset? measurementStartedUtc,
        DateTimeOffset? measurementEndedUtc)
    {
        string baseExtraInfo = reuseSessionBurstReads
            ? $"session-mode=reused reader session; burst-reads={ReusedSessionBurstReads}; readers={ConcurrentReaderCount}; latency-sampling=1/{latencySampleEvery}"
            : $"session-mode=per-query reader session; readers={ConcurrentReaderCount}";

        return new BenchmarkResult
        {
            Name = benchmarkName,
            TotalOps = histograms.Sum(static histogram => histogram.Count),
            LatencySamples = histograms.Sum(static histogram => histogram.SampleCount),
            ElapsedMs = elapsed.TotalMilliseconds,
            P50Ms = histograms.Average(static histogram => histogram.Percentile(0.50)),
            P90Ms = histograms.Average(static histogram => histogram.Percentile(0.90)),
            P95Ms = histograms.Average(static histogram => histogram.Percentile(0.95)),
            P99Ms = histograms.Average(static histogram => histogram.Percentile(0.99)),
            P999Ms = histograms.Average(static histogram => histogram.Percentile(0.999)),
            MinMs = histograms.Min(static histogram => histogram.Min),
            MaxMs = histograms.Max(static histogram => histogram.Max),
            MeanMs = histograms.Average(static histogram => histogram.Mean),
            StdDevMs = histograms.Average(static histogram => histogram.StdDev),
            ExtraInfo = AppendExtraInfo(
                baseExtraInfo,
                qualificationSettings is null
                    ? null
                    : CreateQualificationExtraInfo(
                        qualificationSettings,
                        measurementStartedUtc!.Value,
                        measurementEndedUtc!.Value)),
        };
    }

    private static async Task<BenchmarkResult> RunTimedOperationAsync(
        string benchmarkName,
        TimeSpan normalMeasuredDuration,
        QualificationSettings? qualificationSettings,
        Func<CancellationToken, Task> operation)
    {
        if (qualificationSettings is null)
        {
            return await MacroBenchmarkRunner.RunForDurationAsync(
                benchmarkName,
                WarmupDuration,
                normalMeasuredDuration,
                () => operation(CancellationToken.None));
        }

        qualificationSettings.Validate();
        await RunQualificationWarmupAsync(
            benchmarkName,
            operation,
            qualificationSettings.WarmupDuration);
        MacroBenchmarkRunner.StabilizeAfterWarmup();

        using var deadline = new StopwatchQualificationDeadline(
            qualificationSettings.MaximumMeasuredDuration);
        return await RunQualificationMeasuredOperationCoreAsync(
            benchmarkName,
            qualificationSettings,
            operation,
            deadline,
            QualificationCancellationDrainTimeout);
    }

    internal static async Task<BenchmarkResult> RunQualificationMeasuredOperationCoreAsync(
        string benchmarkName,
        QualificationSettings qualificationSettings,
        Func<CancellationToken, Task> operation,
        IQualificationDeadline deadline,
        TimeSpan cancellationDrainTimeout)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(benchmarkName);
        ArgumentNullException.ThrowIfNull(qualificationSettings);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(deadline);
        if (cancellationDrainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cancellationDrainTimeout));
        qualificationSettings.Validate();

        var histogram = new LatencyHistogram();
        while (true)
        {
            TimeSpan elapsed = deadline.Elapsed;
            bool deadlineReached = deadline.Expired.IsCompleted ||
                                   elapsed > qualificationSettings.MaximumMeasuredDuration;
            if (!deadlineReached && HasMetQualificationTarget(
                    elapsed,
                    histogram.SampleCount,
                    qualificationSettings.MinimumMeasuredDuration,
                    qualificationSettings.MinimumLatencySamples))
            {
                DateTimeOffset measurementEndedUtc = deadline.UtcNow;
                BenchmarkResult result = BenchmarkResult.FromHistogram(
                    benchmarkName,
                    histogram,
                    elapsed.TotalMilliseconds);
                Console.WriteLine(
                    $"  {benchmarkName}: {result.OpsPerSecond:N0} ops/sec, " +
                    $"P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, " +
                    $"P999={result.P999Ms:F3}ms ({result.LatencySamples:N0} retained samples)");

                return CloneResult(
                    result,
                    CreateQualificationExtraInfo(
                        qualificationSettings,
                        deadline.StartedUtc,
                        measurementEndedUtc));
            }

            if (deadlineReached || elapsed >= qualificationSettings.MaximumMeasuredDuration)
            {
                throw await CreateQualificationCapAfterCancellationAsync(
                    benchmarkName,
                    qualificationSettings,
                    elapsed,
                    histogram.SampleCount,
                    inFlightTask: null,
                    deadline,
                    cancellationDrainTimeout,
                    "in-flight operation");
            }

            var operationStopwatch = Stopwatch.StartNew();
            Task operationTask = operation(deadline.Token);
            if (!operationTask.IsCompleted)
            {
                Task completedTask = await Task.WhenAny(operationTask, deadline.Expired);
                if (completedTask != operationTask && !operationTask.IsCompleted)
                {
                    throw await CreateQualificationCapAfterCancellationAsync(
                        benchmarkName,
                        qualificationSettings,
                        deadline.Elapsed,
                        histogram.SampleCount,
                        operationTask,
                        deadline,
                        cancellationDrainTimeout,
                        "in-flight operation");
                }
            }

            await operationTask;
            operationStopwatch.Stop();

            TimeSpan completionElapsed = deadline.Elapsed;
            if (deadline.Expired.IsCompleted ||
                completionElapsed > qualificationSettings.MaximumMeasuredDuration)
            {
                throw await CreateQualificationCapAfterCancellationAsync(
                    benchmarkName,
                    qualificationSettings,
                    completionElapsed,
                    histogram.SampleCount,
                    inFlightTask: null,
                    deadline,
                    cancellationDrainTimeout,
                    "in-flight operation");
            }

            histogram.Record(operationStopwatch.Elapsed.TotalMilliseconds);
        }
    }

    private static async Task RunQualificationWarmupAsync(
        string benchmarkName,
        Func<CancellationToken, Task> operation,
        TimeSpan warmupDuration)
    {
        if (warmupDuration == TimeSpan.Zero)
            return;

        using var deadline = new StopwatchQualificationDeadline(warmupDuration);
        while (!deadline.Expired.IsCompleted && deadline.Elapsed < warmupDuration)
        {
            Task operationTask = operation(deadline.Token);
            if (!operationTask.IsCompleted)
            {
                Task completedTask = await Task.WhenAny(operationTask, deadline.Expired);
                if (completedTask != operationTask && !operationTask.IsCompleted)
                {
                    deadline.Cancel();
                    bool operationStopped = await WaitForTaskCompletionWithinAsync(
                        operationTask,
                        QualificationCancellationDrainTimeout);
                    if (!operationStopped)
                    {
                        throw CreateQualificationUnresponsiveException(
                            benchmarkName,
                            "warmup operation",
                            QualificationCancellationDrainTimeout);
                    }
                }
            }

            try
            {
                await operationTask;
            }
            catch (OperationCanceledException) when (deadline.Token.IsCancellationRequested)
            {
                return;
            }

            if (deadline.Expired.IsCompleted || deadline.Elapsed >= warmupDuration)
            {
                deadline.Cancel();
                return;
            }
        }

        deadline.Cancel();
    }

    private static async Task<InvalidOperationException> CreateQualificationCapAfterCancellationAsync(
        string benchmarkName,
        QualificationSettings settings,
        TimeSpan elapsed,
        int retainedLatencySamples,
        Task? inFlightTask,
        IQualificationDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        string pendingWorkDescription)
    {
        deadline.Cancel();
        InvalidOperationException capException = CreateQualificationCapException(
            benchmarkName,
            settings.MaximumMeasuredDuration,
            elapsed,
            retainedLatencySamples,
            settings.MinimumMeasuredDuration,
            settings.MinimumLatencySamples);

        if (inFlightTask is null)
            return capException;

        bool operationStopped = await WaitForTaskCompletionWithinAsync(
            inFlightTask,
            cancellationDrainTimeout);
        if (!operationStopped)
        {
            return CreateQualificationUnresponsiveException(
                benchmarkName,
                pendingWorkDescription,
                cancellationDrainTimeout,
                capException);
        }

        try
        {
            await inFlightTask;
        }
        catch (OperationCanceledException) when (deadline.Token.IsCancellationRequested)
        {
            // The cap owns this cancellation; preserve its explicit diagnostic.
        }

        return capException;
    }

    private static async Task<bool> WaitForTaskCompletionWithinAsync(
        Task task,
        TimeSpan timeout)
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
            ObserveFaultEventually(task);
            return false;
        }
        catch
        {
            return true;
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

    private static InvalidOperationException CreateQualificationUnresponsiveException(
        string benchmarkName,
        string pendingWorkDescription,
        TimeSpan cancellationDrainTimeout,
        Exception? innerException = null)
    {
        string prefix = innerException is null ? string.Empty : innerException.Message + " ";
        return new InvalidOperationException(
            prefix +
            $"Coordinated cancellation for hybrid storage qualification scenario '{benchmarkName}' " +
            $"did not stop {pendingWorkDescription} within " +
            $"{cancellationDrainTimeout.TotalSeconds:F3} seconds.",
            innerException);
    }

    internal static bool HasMetQualificationTarget(
        TimeSpan elapsed,
        int retainedLatencySamples,
        TimeSpan minimumMeasuredDuration,
        int minimumLatencySamples)
        => elapsed >= minimumMeasuredDuration && retainedLatencySamples >= minimumLatencySamples;

    internal static InvalidOperationException CreateQualificationCapException(
        string benchmarkName,
        TimeSpan maximumMeasuredDuration,
        TimeSpan elapsed,
        int retainedLatencySamples,
        TimeSpan minimumMeasuredDuration,
        int minimumLatencySamples)
        => new(
            $"Hybrid storage qualification scenario '{benchmarkName}' reached its " +
            $"{maximumMeasuredDuration.TotalSeconds:F0}-second measurement cap after " +
            $"{elapsed.TotalSeconds:F1} seconds with {retainedLatencySamples:N0} retained latency samples. " +
            $"Qualification requires at least {minimumMeasuredDuration.TotalSeconds:F0} measured seconds " +
            $"and {minimumLatencySamples:N0} retained latency samples.");

    internal static string CreateQualificationExtraInfo(
        QualificationSettings settings,
        DateTimeOffset measurementStartedUtc,
        DateTimeOffset measurementEndedUtc)
        => $"qualification=true; unrecorded-warmup-seconds={settings.WarmupDuration.TotalSeconds:F0}; " +
           $"minimum-measured-seconds={settings.MinimumMeasuredDuration.TotalSeconds:F0}; " +
           $"minimum-retained-latency-samples={settings.MinimumLatencySamples}; " +
           $"measurement-cap-seconds={settings.MaximumMeasuredDuration.TotalSeconds:F0}; " +
           $"measurement-begin-utc={measurementStartedUtc:O}; " +
           $"measurement-end-utc={measurementEndedUtc:O}";

    private static async Task WarmSqlLookupsAsync(Database db, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {id};");
            _ = await result.MoveNextAsync();
        }
    }

    private static async Task WarmCollectionGetsAsync(Collection<BenchDoc> collection, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            _ = await collection.GetAsync($"doc:{id}");
        }
    }

    private static ScenarioDefinition[] CreateScenarioDefinitions()
    {
        var scenarios = new List<ScenarioDefinition>();
        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            StorageMode capturedMode = mode;
            scenarios.Add(new(
                GetSqlSingleInsertName(capturedMode),
                settings => RunSqlSingleInsertAsync(capturedMode, settings)));
            scenarios.Add(new(
                GetSqlBatchInsertName(capturedMode),
                settings => RunSqlBatchInsertAsync(capturedMode, settings)));
            scenarios.Add(new(
                GetSqlPointLookupName(capturedMode),
                settings => RunSqlPointLookupAsync(capturedMode, settings)));
            scenarios.Add(new(
                GetSqlConcurrentReadsName(capturedMode, reuseSessionBurstReads: false),
                settings => RunSqlConcurrentReadsAsync(
                    capturedMode,
                    reuseSessionBurstReads: false,
                    settings)));
            scenarios.Add(new(
                GetSqlConcurrentReadsName(capturedMode, reuseSessionBurstReads: true),
                settings => RunSqlConcurrentReadsAsync(
                    capturedMode,
                    reuseSessionBurstReads: true,
                    settings)));
            scenarios.Add(new(
                GetCollectionPutName(capturedMode),
                settings => RunCollectionPutAsync(capturedMode, settings)));
            scenarios.Add(new(
                GetCollectionBatchInsertName(capturedMode),
                settings => RunCollectionBatchInsertAsync(capturedMode, settings)));
            scenarios.Add(new(
                GetCollectionGetName(capturedMode),
                settings => RunCollectionGetAsync(capturedMode, settings)));
        }

        foreach (InsertTradeoffScenario scenario in s_insertTradeoffScenarios)
        {
            InsertTradeoffScenario capturedScenario = scenario;
            scenarios.Add(new(
                GetInsertTradeoffName(capturedScenario),
                settings => RunInsertTradeoffScenarioAsync(capturedScenario, settings)));
        }

        string[] duplicateNames = scenarios
            .GroupBy(static scenario => scenario.Name, StringComparer.Ordinal)
            .Where(static group => group.Count() > 1)
            .Select(static group => group.Key)
            .ToArray();
        if (duplicateNames.Length > 0)
        {
            throw new InvalidOperationException(
                "Hybrid storage-mode scenario names must be unique: " +
                string.Join(", ", duplicateNames));
        }

        return scenarios.ToArray();
    }

    private static string GetSqlSingleInsertName(StorageMode mode)
        => $"{GetPrefix(mode)}_Sql_SingleInsert_5s";

    private static string GetSqlBatchInsertName(StorageMode mode)
        => $"{GetPrefix(mode)}_Sql_Batch{BatchSize}_5s";

    private static string GetSqlPointLookupName(StorageMode mode)
        => $"{GetPrefix(mode)}_Sql_PointLookup_{SeedCount}";

    private static string GetSqlConcurrentReadsName(StorageMode mode, bool reuseSessionBurstReads)
        => reuseSessionBurstReads
            ? $"{GetPrefix(mode)}_Sql_ConcurrentReadsBurst{ReusedSessionBurstReads}_{ConcurrentReaderCount}readers"
            : $"{GetPrefix(mode)}_Sql_ConcurrentReads_{ConcurrentReaderCount}readers";

    private static string GetCollectionPutName(StorageMode mode)
        => $"{GetPrefix(mode)}_Collection_Put_5s";

    private static string GetCollectionBatchInsertName(StorageMode mode)
        => $"{GetPrefix(mode)}_Collection_Batch{BatchSize}_5s";

    private static string GetCollectionGetName(StorageMode mode)
        => $"{GetPrefix(mode)}_Collection_Get_{SeedCount}";

    private static string GetInsertTradeoffName(InsertTradeoffScenario scenario)
        => $"StoragePlan2_{scenario.Name}_InsertBatch_B{InsertTradeoffRowsPerCommit}_Seed{scenario.SeedRows}_10s";

    private static InsertTradeoffScenario[] CreateInsertTradeoffScenarios()
    {
        return
        [
            new(
                "FileBackedDurableWriteOptimized",
                InsertTradeoffMode.FileBacked,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "full durable file-backed commits; each acknowledged commit forces durable backing-file visibility",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "FileBackedDurableLowLatency",
                InsertTradeoffMode.FileBacked,
                InsertTradeoffSeedRows,
                StoragePreset.LowLatency,
                "full durable file-backed commits; same durability as the baseline with deferred planner-stat persistence",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "FileBackedBufferedWriteOptimized",
                InsertTradeoffMode.FileBackedBuffered,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "buffered file-backed commits; managed buffers are flushed but recent commits remain more exposed on OS crash or power loss",
                "file-backed pages stay on disk and are cached on demand"),
            new(
                "InMemoryFresh",
                InsertTradeoffMode.InMemoryFresh,
                InsertTradeoffSeedRows,
                StoragePreset.NotApplicable,
                "no crash durability; the database exists only in private process memory",
                "new private in-memory database with no backing file"),
            new(
                "LoadIntoMemory",
                InsertTradeoffMode.LoadIntoMemory,
                InsertTradeoffSeedRows,
                StoragePreset.NotApplicable,
                "no crash durability after load; persistence requires an explicit later save back to disk",
                "an existing file plus committed WAL state are loaded once, then inserts run entirely in memory"),
            new(
                "HybridIncrementalDurable",
                InsertTradeoffMode.HybridIncrementalDurable,
                InsertTradeoffSeedRows,
                StoragePreset.WriteOptimized,
                "full durable commits through the hybrid WAL and checkpoint path",
                "the durable backing file remains authoritative while touched pages stay resident by cache policy"),
        ];
    }

    private static async Task InitializeInsertTradeoffDatabaseAsync(Database db, int seedRows)
    {
        await using var _ = await db.ExecuteAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);");

        if (seedRows <= 0)
            return;

        var batch = db.PrepareInsertBatch("bench", initialCapacity: InsertTradeoffRowsPerCommit);
        var rowBuffer = new DbValue[4];
        DbValue textValue = DbValue.FromText("durable_batch");
        DbValue categoryValue = DbValue.FromText("Alpha");
        int nextSequence = 0;

        while (nextSequence < seedRows)
        {
            int remaining = seedRows - nextSequence;
            int rowsThisCommit = Math.Min(InsertTradeoffRowsPerCommit, remaining);
            nextSequence = await ExecuteInsertBatchCommitAsync(
                db,
                batch,
                rowBuffer,
                nextSequence,
                rowsThisCommit,
                textValue,
                categoryValue);
        }
    }

    private static async Task<int> ExecuteInsertBatchCommitAsync(
        Database db,
        InsertBatch batch,
        DbValue[] rowBuffer,
        int nextSequence,
        int rowsToInsert,
        DbValue textValue,
        DbValue categoryValue,
        CancellationToken ct = default)
    {
        batch.Clear();
        await db.BeginTransactionAsync(ct);
        try
        {
            for (int i = 0; i < rowsToInsert; i++)
            {
                nextSequence++;
                PopulateInsertTradeoffRow(rowBuffer, nextSequence, textValue, categoryValue);
                batch.AddRow(rowBuffer);
            }

            int rowsAffected = await batch.ExecuteAsync(ct);
            if (rowsAffected != rowsToInsert)
            {
                throw new InvalidOperationException(
                    $"Expected {rowsToInsert} inserted rows, observed {rowsAffected}.");
            }

            await db.CommitAsync(ct);
            return nextSequence;
        }
        catch
        {
            await RollbackQuietlyAsync(db);
            throw;
        }
    }

    private static void PopulateInsertTradeoffRow(
        DbValue[] row,
        int sequence,
        DbValue textValue,
        DbValue categoryValue)
    {
        row[0] = DbValue.FromInteger(sequence);
        row[1] = DbValue.FromInteger(sequence);
        row[2] = textValue;
        row[3] = categoryValue;
    }

    private static async Task RollbackQuietlyAsync(Database db)
    {
        try
        {
            await db.RollbackAsync();
        }
        catch
        {
            // Preserve the original benchmark failure.
        }
    }

    private static DatabaseOptions CreateInsertTradeoffOptions(StoragePreset preset, DurabilityMode durabilityMode)
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UseDurabilityMode(durabilityMode);

            if (preset == StoragePreset.LowLatency)
            {
                builder.UseLowLatencyDurableWritePreset();
            }
            else
            {
                builder.UseWriteOptimizedPreset();
            }
        });
    }

    private static BenchmarkResult CloneResult(BenchmarkResult source, string? extraInfo)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = source.TotalOps,
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
            ExtraInfo = extraInfo,
        };
    }

    private static string? AppendExtraInfo(params string?[] values)
    {
        var parts = new List<string>(values.Length);
        foreach (string? value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                parts.Add(value);
        }

        return parts.Count == 0 ? null : string.Join("; ", parts);
    }

    private static string NewInsertTradeoffDbPath(string prefix)
        => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "Storage_FileBacked",
            StorageMode.InMemory => "Storage_InMemory",
            StorageMode.HybridIncrementalDurable => "Storage_HybridIncrementalDurable",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private sealed class BenchmarkContext : IAsyncDisposable
    {
        private readonly string? _filePath;

        private BenchmarkContext(Database database, Collection<BenchDoc>? collection, string? filePath)
        {
            Database = database;
            Collection = collection;
            _filePath = filePath;
        }

        internal Database Database { get; }
        internal Collection<BenchDoc>? Collection { get; }

        internal static async Task<BenchmarkContext> CreateSqlWriteAsync(StorageMode mode)
        {
            var (database, filePath) = await OpenDatabaseAsync(mode);
            await database.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");
            return new BenchmarkContext(database, collection: null, filePath);
        }

        internal static async Task<BenchmarkContext> CreateSqlReadAsync(StorageMode mode)
        {
            string? seededFilePath = null;
            Database database;

            switch (mode)
            {
                case StorageMode.FileBacked:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await Database.OpenAsync(seededFilePath, BenchmarkDurability.Apply());
                    break;
                case StorageMode.InMemory:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await Database.LoadIntoMemoryAsync(seededFilePath);
                    break;
                case StorageMode.HybridIncrementalDurable:
                    seededFilePath = await CreateSeededSqlDatabaseAsync();
                    database = await OpenHybridAsync(seededFilePath);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            return new BenchmarkContext(database, collection: null, seededFilePath);
        }

        internal static async Task<BenchmarkContext> CreateCollectionWriteAsync(StorageMode mode)
        {
            var (database, filePath) = await OpenDatabaseAsync(mode);
            var collection = await database.GetCollectionAsync<BenchDoc>("bench_docs");
            return new BenchmarkContext(database, collection, filePath);
        }

        internal static async Task<BenchmarkContext> CreateCollectionReadAsync(StorageMode mode)
        {
            string? seededFilePath = null;
            Database database;

            switch (mode)
            {
                case StorageMode.FileBacked:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await Database.OpenAsync(seededFilePath, BenchmarkDurability.Apply());
                    break;
                case StorageMode.InMemory:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await Database.LoadIntoMemoryAsync(seededFilePath);
                    break;
                case StorageMode.HybridIncrementalDurable:
                    seededFilePath = await CreateSeededCollectionDatabaseAsync();
                    database = await OpenHybridAsync(seededFilePath);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            var collection = await database.GetCollectionAsync<BenchDoc>("bench_docs");
            return new BenchmarkContext(database, collection, seededFilePath);
        }

        public async ValueTask DisposeAsync()
        {
            await Database.DisposeAsync();
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_filePath);
        }

        private static async Task<(Database Database, string? FilePath)> OpenDatabaseAsync(StorageMode mode)
        {
            return mode switch
            {
                StorageMode.FileBacked => await OpenFileBackedAsync(),
                StorageMode.InMemory => (await Database.OpenInMemoryAsync(), null),
                StorageMode.HybridIncrementalDurable => await OpenHybridModeAsync(),
                _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
            };
        }

        private static async Task<(Database Database, string FilePath)> OpenFileBackedAsync()
        {
            string filePath = NewTempDbPath("storage-file");
            return (await Database.OpenAsync(filePath, BenchmarkDurability.Apply()), filePath);
        }

        private static async Task<(Database Database, string FilePath)> OpenHybridModeAsync()
        {
            string filePath = NewTempDbPath("storage-hybrid");
            return (await OpenHybridAsync(filePath), filePath);
        }

        private static async Task<Database> OpenHybridAsync(string filePath)
        {
            return await Database.OpenHybridAsync(
                filePath,
                BenchmarkDurability.Apply(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                });
        }

        private static async Task<string> CreateSeededSqlDatabaseAsync()
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"storage-hybrid-sql_{Guid.NewGuid():N}.db");
            await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
            await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");

            const int seedBatchSize = 500;
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += seedBatchSize)
            {
                await db.BeginTransactionAsync();
                try
                {
                    int batchEnd = Math.Min(batchStart + seedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        await db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');");
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            }

            return filePath;
        }

        private static async Task<string> CreateSeededCollectionDatabaseAsync()
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"storage-hybrid-col_{Guid.NewGuid():N}.db");
            await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
            var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");

            const int seedBatchSize = 500;
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += seedBatchSize)
            {
                await db.BeginTransactionAsync();
                try
                {
                    int batchEnd = Math.Min(batchStart + seedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        await collection.PutAsync(
                            $"doc:{id}",
                            new BenchDoc($"User_{id}", id, GetCategory(id)));
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }
            }

            return filePath;
        }

        private static string NewTempDbPath(string prefix)
            => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
    }

    private sealed class InsertTradeoffContext : IAsyncDisposable
    {
        private readonly string[] _cleanupPaths;

        private InsertTradeoffContext(Database database, params string?[] cleanupPaths)
        {
            Database = database;
            _cleanupPaths = cleanupPaths
                .Where(static path => !string.IsNullOrWhiteSpace(path))
                .Select(static path => path!)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        internal Database Database { get; }

        internal static async Task<InsertTradeoffContext> CreateAsync(InsertTradeoffScenario scenario)
        {
            switch (scenario.Mode)
            {
                case InsertTradeoffMode.FileBacked:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-file");
                    var database = await Database.OpenAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Durable));
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                case InsertTradeoffMode.FileBackedBuffered:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-buffered");
                    var database = await Database.OpenAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Buffered));
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                case InsertTradeoffMode.InMemoryFresh:
                {
                    var database = await Database.OpenInMemoryAsync();
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database);
                }

                case InsertTradeoffMode.LoadIntoMemory:
                {
                    string sourcePath = NewInsertTradeoffDbPath("storage-plan2-load");
                    await using (var source = await Database.OpenAsync(
                                     sourcePath,
                                     CreateInsertTradeoffOptions(StoragePreset.WriteOptimized, DurabilityMode.Durable)))
                    {
                        await InitializeInsertTradeoffDatabaseAsync(source, scenario.SeedRows);
                    }

                    var database = await Database.LoadIntoMemoryAsync(sourcePath);
                    return new InsertTradeoffContext(database, sourcePath);
                }

                case InsertTradeoffMode.HybridIncrementalDurable:
                {
                    string filePath = NewInsertTradeoffDbPath("storage-plan2-hybrid");
                    var database = await Database.OpenHybridAsync(
                        filePath,
                        CreateInsertTradeoffOptions(scenario.Preset, DurabilityMode.Durable),
                        new HybridDatabaseOptions
                        {
                            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                        });
                    await InitializeInsertTradeoffDatabaseAsync(database, scenario.SeedRows);
                    return new InsertTradeoffContext(database, filePath);
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(scenario.Mode), scenario.Mode, null);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Database.DisposeAsync();
            foreach (string path in _cleanupPaths)
                InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(path);
        }
    }

    internal sealed record QualificationSettings(
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
                    "Qualification warmup duration cannot be negative.");
            }

            if (MinimumMeasuredDuration <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumMeasuredDuration),
                    "Qualification minimum measured duration must be positive.");
            }

            if (MinimumLatencySamples <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumLatencySamples),
                    "Qualification minimum latency sample count must be positive.");
            }

            if (MaximumMeasuredDuration < MinimumMeasuredDuration)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MaximumMeasuredDuration),
                    "Qualification measurement cap must be at least the minimum measured duration.");
            }
        }
    }

    private sealed record ScenarioDefinition(
        string Name,
        Func<QualificationSettings?, Task<BenchmarkResult>> RunAsync);

    internal interface IQualificationDeadline : IDisposable
    {
        TimeSpan Elapsed { get; }
        DateTimeOffset StartedUtc { get; }
        DateTimeOffset UtcNow { get; }
        CancellationToken Token { get; }
        Task Expired { get; }
        void Cancel();
    }

    internal sealed record ConcurrentReaderPhaseResult(
        LatencyHistogram[] Histograms,
        TimeSpan Elapsed,
        DateTimeOffset MeasurementStartedUtc,
        DateTimeOffset MeasurementEndedUtc);

    private enum ConcurrentPhaseStopReason
    {
        TargetReached,
        Deadline,
        UnexpectedReaderExit,
    }

    private sealed class StopwatchQualificationDeadline : IQualificationDeadline
    {
        private readonly Stopwatch _stopwatch;
        private readonly CancellationTokenSource _operationCts = new();

        internal StopwatchQualificationDeadline(TimeSpan maximumDuration)
        {
            if (maximumDuration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(maximumDuration));

            StartedUtc = DateTimeOffset.UtcNow;
            _stopwatch = Stopwatch.StartNew();
            _operationCts.CancelAfter(maximumDuration);
            Expired = Task.Delay(Timeout.InfiniteTimeSpan, _operationCts.Token);
        }

        public TimeSpan Elapsed => _stopwatch.Elapsed;
        public DateTimeOffset StartedUtc { get; }
        public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
        public CancellationToken Token => _operationCts.Token;
        public Task Expired { get; }

        public void Cancel()
        {
            if (!_operationCts.IsCancellationRequested)
                _operationCts.Cancel();
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            Cancel();
            _operationCts.Dispose();
        }
    }

    private sealed record InsertTradeoffScenario(
        string Name,
        InsertTradeoffMode Mode,
        int SeedRows,
        StoragePreset Preset,
        string DurabilitySemantics,
        string ResidencySemantics)
    {
        public string PresetLabel => Preset switch
        {
            StoragePreset.WriteOptimized => "write-optimized",
            StoragePreset.LowLatency => "low-latency durable",
            StoragePreset.NotApplicable => "n/a",
            _ => throw new ArgumentOutOfRangeException(nameof(Preset), Preset, null),
        };
    }

    private enum InsertTradeoffMode
    {
        FileBacked,
        FileBackedBuffered,
        InMemoryFresh,
        LoadIntoMemory,
        HybridIncrementalDurable,
    }

    private enum StoragePreset
    {
        WriteOptimized,
        LowLatency,
        NotApplicable,
    }
}
