using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridColdOpenBenchmark
{
    private const int SeedCount = 200_000;
    private const int SqlLookupId = 175_321;
    private const int CollectionLookupId = 175_321;
    private static readonly TimeSpan FailureCleanupDrainTimeout = TimeSpan.FromSeconds(1);

    internal static MeasurementPolicy DefaultMeasurementPolicy { get; } = new(
        MinimumMeasuredDuration: TimeSpan.FromSeconds(15),
        MinimumLatencySamples: 100,
        MaximumMeasuredDuration: TimeSpan.FromSeconds(90));

    private sealed record BenchDoc(string Name, int Value, string Category);

    private enum StorageMode
    {
        FileBacked,
        InMemory,
        HybridIncrementalDurable,
        HybridHotSetIncrementalDurable,
    }

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        List<IReadOnlyList<BenchmarkResult>> runs = await RunRepeatedAsync(1);
        return runs[0].ToList();
    }

    public static async Task<List<IReadOnlyList<BenchmarkResult>>> RunRepeatedAsync(
        int repeatCount,
        bool warmupSingleSample = false)
    {
        bool warmUpEachScenario = ShouldWarmUpEachScenario(repeatCount, warmupSingleSample);

        await using var inputs = await SeededInputs.CreateAsync();
        await PrimeCodePathsAsync();

        var scenarios = new List<Func<Task<BenchmarkResult>>>();
        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            StorageMode scenarioMode = mode;
            scenarios.Add(() => RunSqlOpenOnlyAsync(
                scenarioMode,
                inputs.SqlFilePath,
                inputs.QuarantineDetachedWork));
            scenarios.Add(() => RunSqlOpenAndFirstLookupAsync(
                scenarioMode,
                inputs.SqlFilePath,
                inputs.QuarantineDetachedWork));
            scenarios.Add(() => RunCollectionOpenOnlyAsync(
                scenarioMode,
                inputs.CollectionFilePath,
                inputs.QuarantineDetachedWork));
            scenarios.Add(() => RunCollectionOpenAndFirstGetAsync(
                scenarioMode,
                inputs.CollectionFilePath,
                inputs.QuarantineDetachedWork));
        }

        return await ScenarioMajorBenchmarkRunner.RunAsync(
            scenarios,
            repeatCount,
            warmUpEachScenario);
    }

    internal static bool ShouldWarmUpEachScenario(
        int repeatCount,
        bool warmupSingleSample)
    {
        if (repeatCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(repeatCount), "Repeat count must be positive.");
        if (warmupSingleSample && repeatCount != 1)
        {
            throw new ArgumentException(
                "Single-sample warmup requires exactly one recorded repeat.",
                nameof(warmupSingleSample));
        }

        return repeatCount > 1 || warmupSingleSample;
    }

    private static Task<BenchmarkResult> RunSqlOpenOnlyAsync(
        StorageMode mode,
        string filePath,
        Action<Task> detachedWorkRegistrar)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Sql_OpenOnly_{SeedCount}",
            async ct =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath, ct);
            },
            detachedWorkRegistrar);
    }

    private static Task<BenchmarkResult> RunSqlOpenAndFirstLookupAsync(
        StorageMode mode,
        string filePath,
        Action<Task> detachedWorkRegistrar)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Sql_OpenAndFirstLookup_{SeedCount}",
            async ct =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath, ct);
                await using var result = await db.ExecuteAsync(
                    $"SELECT value FROM bench WHERE id = {SqlLookupId};",
                    ct);
                if (!await result.MoveNextAsync(ct) || result.Current[0].AsInteger != SqlLookupId * 10L)
                    throw new InvalidOperationException($"Lookup for id={SqlLookupId} returned an unexpected result.");
            },
            detachedWorkRegistrar);
    }

    private static Task<BenchmarkResult> RunCollectionOpenOnlyAsync(
        StorageMode mode,
        string filePath,
        Action<Task> detachedWorkRegistrar)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Collection_OpenOnly_{SeedCount}",
            async ct =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath, ct);
            },
            detachedWorkRegistrar);
    }

    private static Task<BenchmarkResult> RunCollectionOpenAndFirstGetAsync(
        StorageMode mode,
        string filePath,
        Action<Task> detachedWorkRegistrar)
    {
        return RunColdScenarioAsync(
            $"{GetPrefix(mode)}_Collection_OpenAndFirstGet_{SeedCount}",
            async ct =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath, ct);
                var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                BenchDoc? document = await collection.GetAsync($"doc:{CollectionLookupId}", ct);
                if (document is null || document.Value != CollectionLookupId)
                    throw new InvalidOperationException($"Document 'doc:{CollectionLookupId}' was not found or was invalid.");
            },
            detachedWorkRegistrar);
    }

    private static async Task<BenchmarkResult> RunColdScenarioAsync(
        string name,
        Func<CancellationToken, Task> operation,
        Action<Task> detachedWorkRegistrar)
    {
        MacroBenchmarkRunner.StabilizeAfterWarmup();

        using var deadline = new StopwatchMeasurementDeadline(
            DefaultMeasurementPolicy.MaximumMeasuredDuration);
        return await RunColdScenarioCoreAsync(
            name,
            operation,
            DefaultMeasurementPolicy,
            deadline,
            FailureCleanupDrainTimeout,
            detachedWorkRegistrar);
    }

    internal static async Task<BenchmarkResult> RunColdScenarioCoreAsync(
        string name,
        Func<CancellationToken, Task> operation,
        MeasurementPolicy policy,
        IMeasurementDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        Action<Task>? detachedWorkRegistrar = null,
        Func<Func<Task<BenchmarkResult>>, Task<BenchmarkResult>>? scheduleWorker = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(deadline);
        if (cancellationDrainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cancellationDrainTimeout));
        policy.Validate();

        scheduleWorker ??= static worker => Task.Run(worker);
        var progress = new MeasurementProgress();
        CancellationToken operationCancellationToken = deadline.Token;
        Task expirationTask = deadline.Expired;
        var workerReady = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var measurementStart = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task<BenchmarkResult> workerTask = scheduleWorker(
            async () =>
            {
                workerReady.TrySetResult();
                await measurementStart.Task.ConfigureAwait(false);
                return await RunColdScenarioWorkerAsync(
                name,
                operation,
                policy,
                deadline,
                progress,
                operationCancellationToken,
                expirationTask).ConfigureAwait(false);
            });

        Task readinessWinner = await Task.WhenAny(workerReady.Task, workerTask);
        if (readinessWinner == workerTask && !workerReady.Task.IsCompleted)
            return await workerTask;

        deadline.Start();
        measurementStart.TrySetResult();

        Task completedTask = await Task.WhenAny(workerTask, expirationTask);
        if (completedTask == workerTask || workerTask.IsCompleted)
        {
            try
            {
                return await workerTask;
            }
            catch (OperationCanceledException) when (operationCancellationToken.IsCancellationRequested)
            {
                throw await CreateCapAfterCancellationAsync(
                    name,
                    policy,
                    deadline.Elapsed,
                    progress.RetainedLatencySamples,
                    inFlightTask: null,
                    deadline,
                    cancellationDrainTimeout);
            }
            finally
            {
                deadline.Cancel();
            }
        }

        throw await CreateCapAfterCancellationAsync(
            name,
            policy,
            deadline.Elapsed,
            progress.RetainedLatencySamples,
            workerTask,
            deadline,
            cancellationDrainTimeout,
            detachedWorkRegistrar);
    }

    private static async Task<BenchmarkResult> RunColdScenarioWorkerAsync(
        string name,
        Func<CancellationToken, Task> operation,
        MeasurementPolicy policy,
        IMeasurementDeadline deadline,
        MeasurementProgress progress,
        CancellationToken operationCancellationToken,
        Task expirationTask)
    {
        var histogram = new LatencyHistogram();
        while (true)
        {
            TimeSpan elapsed = deadline.Elapsed;
            if (!expirationTask.IsCompleted &&
                elapsed < policy.MaximumMeasuredDuration &&
                HasMetMeasurementTarget(elapsed, histogram.SampleCount, policy))
            {
                BenchmarkResult result = BenchmarkResult.FromHistogram(
                    name,
                    histogram,
                    elapsed.TotalMilliseconds);
                Console.WriteLine(
                    $"  {name}: {result.OpsPerSecond:N0} ops/sec, " +
                    $"P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, " +
                    $"P999={result.P999Ms:F3}ms");
                return result;
            }

            if (expirationTask.IsCompleted)
                throw new OperationCanceledException(operationCancellationToken);

            if (elapsed >= policy.MaximumMeasuredDuration)
            {
                deadline.Cancel();
                throw new OperationCanceledException(operationCancellationToken);
            }

            // This stopwatch intentionally starts inside the single scenario worker so
            // thread-pool scheduling delay is not reported as operation latency.
            var operationStopwatch = Stopwatch.StartNew();
            await operation(operationCancellationToken);
            operationStopwatch.Stop();

            TimeSpan completionElapsed = deadline.Elapsed;
            if (expirationTask.IsCompleted)
                throw new OperationCanceledException(operationCancellationToken);

            if (completionElapsed > policy.MaximumMeasuredDuration)
            {
                deadline.Cancel();
                throw new OperationCanceledException(operationCancellationToken);
            }

            histogram.Record(operationStopwatch.Elapsed.TotalMilliseconds);
            progress.PublishRetainedLatencySamples(histogram.SampleCount);
        }
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
            $"Cold-open scenario '{name}' reached its " +
            $"{policy.MaximumMeasuredDuration.TotalSeconds:F0}-second measurement cap after " +
            $"{elapsed.TotalSeconds:F1} seconds with {retainedLatencySamples:N0} retained latency samples. " +
            $"Measurement requires at least {policy.MinimumMeasuredDuration.TotalSeconds:F0} measured seconds " +
            $"and {policy.MinimumLatencySamples:N0} retained latency samples.");

    private static async Task<InvalidOperationException> CreateCapAfterCancellationAsync(
        string name,
        MeasurementPolicy policy,
        TimeSpan elapsed,
        int retainedLatencySamples,
        Task? inFlightTask,
        IMeasurementDeadline deadline,
        TimeSpan cancellationDrainTimeout,
        Action<Task>? detachedWorkRegistrar = null)
    {
        deadline.Cancel();
        InvalidOperationException capException = CreateMeasurementCapException(
            name,
            policy,
            elapsed,
            retainedLatencySamples);

        if (inFlightTask is null)
            return capException;

        bool operationStopped;
        try
        {
            await inFlightTask.WaitAsync(cancellationDrainTimeout);
            operationStopped = true;
        }
        catch (TimeoutException) when (!inFlightTask.IsCompleted)
        {
            operationStopped = false;
        }
        catch (OperationCanceledException) when (deadline.Token.IsCancellationRequested)
        {
            operationStopped = true;
        }
        catch (Exception exception)
        {
            return new InvalidOperationException(
                capException.Message +
                " The in-flight operation failed while responding to measurement cancellation.",
                exception);
        }

        if (operationStopped)
            return capException;

        ObserveFaultEventually(inFlightTask);
        detachedWorkRegistrar?.Invoke(inFlightTask);
        return new InvalidOperationException(
            capException.Message +
            $" Coordinated cancellation did not stop the in-flight operation within " +
            $"{cancellationDrainTimeout.TotalSeconds:F3} seconds.",
            capException);
    }

    private static void ObserveFaultEventually(Task task)
    {
        _ = task.ContinueWith(
            static completedTask => _ = completedTask.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private sealed class MeasurementProgress
    {
        private int _retainedLatencySamples;

        internal int RetainedLatencySamples => Volatile.Read(ref _retainedLatencySamples);

        internal void PublishRetainedLatencySamples(int value)
            => Volatile.Write(ref _retainedLatencySamples, value);
    }

    private static async Task PrimeCodePathsAsync()
    {
        string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
            "hybrid-cold-open-prime-sql",
            rowCount: 32);
        string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
            "hybrid-cold-open-prime-col",
            rowCount: 32);

        try
        {
            foreach (StorageMode mode in Enum.GetValues<StorageMode>())
            {
                await using (var db = await OpenSqlDatabaseAsync(mode, sqlFilePath))
                {
                    await using var result = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 7;");
                    _ = await result.MoveNextAsync();
                }

                await using (var db = await OpenCollectionDatabaseAsync(mode, collectionFilePath))
                {
                    var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                    _ = await collection.GetAsync("doc:7");
                }
            }
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(sqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(collectionFilePath);
        }
    }

    private static ValueTask<Database> OpenSqlDatabaseAsync(
        StorageMode mode,
        string filePath,
        CancellationToken ct = default)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, ct),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, ct),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
                ct),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotTableNames = new[] { "bench" },
                },
                ct),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static ValueTask<Database> OpenCollectionDatabaseAsync(
        StorageMode mode,
        string filePath,
        CancellationToken ct = default)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath, ct),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath, ct),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
                ct),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotCollectionNames = new[] { "bench_docs" },
                },
                ct),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "ColdOpen_FileBacked",
            StorageMode.InMemory => "ColdOpen_InMemory",
            StorageMode.HybridIncrementalDurable => "ColdOpen_HybridIncrementalDurable",
            StorageMode.HybridHotSetIncrementalDurable => "ColdOpen_HybridHotSetIncrementalDurable",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    internal sealed record MeasurementPolicy(
        TimeSpan MinimumMeasuredDuration,
        int MinimumLatencySamples,
        TimeSpan MaximumMeasuredDuration)
    {
        internal void Validate()
        {
            if (MinimumMeasuredDuration <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumMeasuredDuration),
                    "Cold-open minimum measured duration must be positive.");
            }

            if (MinimumLatencySamples <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MinimumLatencySamples),
                    "Cold-open minimum latency sample count must be positive.");
            }

            if (MaximumMeasuredDuration < MinimumMeasuredDuration)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(MaximumMeasuredDuration),
                    "Cold-open measurement cap must be at least the minimum measured duration.");
            }
        }
    }

    internal interface IMeasurementDeadline : IDisposable
    {
        TimeSpan Elapsed { get; }
        CancellationToken Token { get; }
        Task Expired { get; }
        void Start();
        void Cancel();
    }

    private sealed class StopwatchMeasurementDeadline : IMeasurementDeadline
    {
        private readonly TimeSpan _maximumDuration;
        private readonly Stopwatch _stopwatch;
        private readonly CancellationTokenSource _operationCts = new();
        private int _started;

        internal StopwatchMeasurementDeadline(TimeSpan maximumDuration)
        {
            if (maximumDuration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(maximumDuration));

            _maximumDuration = maximumDuration;
            _stopwatch = new Stopwatch();
            Expired = Task.Delay(Timeout.InfiniteTimeSpan, _operationCts.Token);
        }

        public TimeSpan Elapsed => _stopwatch.Elapsed;
        public CancellationToken Token => _operationCts.Token;
        public Task Expired { get; }

        public void Start()
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                return;

            _stopwatch.Start();
            _operationCts.CancelAfter(_maximumDuration);
        }

        public void Cancel()
        {
            if (!_operationCts.IsCancellationRequested)
                _operationCts.Cancel();
        }

        public void Dispose()
        {
            Cancel();
            _operationCts.Dispose();
        }
    }

    private sealed class SeededInputs : IAsyncDisposable
    {
        private readonly object _lifetimeGate = new();
        private readonly List<Task> _detachedWork = [];
        private int _resourcesDisposed;

        private SeededInputs(string sqlFilePath, string collectionFilePath)
        {
            SqlFilePath = sqlFilePath;
            CollectionFilePath = collectionFilePath;
        }

        public string SqlFilePath { get; }
        public string CollectionFilePath { get; }

        internal void QuarantineDetachedWork(Task task)
        {
            ArgumentNullException.ThrowIfNull(task);
            ObserveFaultEventually(task);
            lock (_lifetimeGate)
                _detachedWork.Add(task);
        }

        public static async Task<SeededInputs> CreateAsync()
        {
            string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
                "hybrid-cold-open-sql",
                SeedCount);
            string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
                "hybrid-cold-open-col",
                SeedCount);
            return new SeededInputs(sqlFilePath, collectionFilePath);
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

            if (pendingWork.Length != 0)
            {
                Task deferredCleanup = DeleteAfterDetachedWorkAsync(pendingWork);
                ObserveFaultEventually(deferredCleanup);
                return ValueTask.CompletedTask;
            }

            DeleteFiles();
            return ValueTask.CompletedTask;
        }

        private async Task DeleteAfterDetachedWorkAsync(Task[] pendingWork)
        {
            try
            {
                await Task.WhenAll(pendingWork).ConfigureAwait(false);
            }
            catch
            {
                // Detached worker failures are already observed by the benchmark controller.
            }

            DeleteFiles();
        }

        private void DeleteFiles()
        {
            if (Interlocked.Exchange(ref _resourcesDisposed, 1) != 0)
                return;

            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(SqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(CollectionFilePath);
        }
    }
}
