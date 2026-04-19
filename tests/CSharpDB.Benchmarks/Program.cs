using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using CSharpDB.Benchmarks.Stress;
using CSharpDB.Benchmarks.Scaling;
using System.Reflection;
using System.Text.Json;

namespace CSharpDB.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintHelp();
            return;
        }

        if (args[0].Equals("--crash-harness", StringComparison.OrdinalIgnoreCase))
        {
            await CrashHarness.RunAsync(args[1..]);
            return;
        }

        int repeatCount = ParseRepeatCount(args);
        bool enableRepro = HasFlag(args, "--repro");
        int? requestedCpuThreads = ParseCpuThreads(args);
        bool reproConfigured = false;

        void EnsureReproConfigured()
        {
            if (reproConfigured)
                return;

            BenchmarkProcessTuner.ConfigureIfRequested(enableRepro, requestedCpuThreads);
            reproConfigured = true;
        }

        var mode = GetPrimaryMode(args);
        switch (mode)
        {
            case "--micro":
                RunMicroBenchmarks(
                    StripCustomArgs(RemoveFirstToken(args, "--micro")),
                    excludePrGuardrailsWhenFilterMissing: true);
                return;

            case "--filter":
                RunMicroBenchmarks(StripCustomArgs(args));
                return;

            case "--macro-batch-memory":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("macro-batch-memory", RunInMemoryBatchBenchmarksOnceAsync, repeatCount);
                return;

            case "--write-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("write-diagnostics", RunWriteDiagnosticsOnceAsync, repeatCount);
                return;

            case "--durable-sql-batching":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("durable-sql-batching", RunDurableSqlBatchingOnceAsync, repeatCount);
                return;

            case "--durable-sql-batching-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"durable-sql-batching-scenario-{GetRequiredOptionValue(args, "--durable-sql-batching-scenario")}",
                    () => RunDurableSqlBatchingScenarioOnceAsync(GetRequiredOptionValue(args, "--durable-sql-batching-scenario")),
                    repeatCount);
                return;

            case "--write-transaction-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("write-transaction-diagnostics", RunWriteTransactionDiagnosticsOnceAsync, repeatCount);
                return;

            case "--commit-fan-in-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("commit-fan-in-diagnostics", RunCommitFanInDiagnosticsOnceAsync, repeatCount);
                return;

            case "--commit-fan-in-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"commit-fan-in-scenario-{GetRequiredOptionValue(args, "--commit-fan-in-scenario")}",
                    () => RunCommitFanInScenarioOnceAsync(GetRequiredOptionValue(args, "--commit-fan-in-scenario")),
                    repeatCount);
                return;

            case "--insert-fan-in-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("insert-fan-in-diagnostics", RunInsertFanInDiagnosticsOnceAsync, repeatCount);
                return;

            case "--insert-fan-in-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"insert-fan-in-scenario-{GetRequiredOptionValue(args, "--insert-fan-in-scenario")}",
                    () => RunInsertFanInScenarioOnceAsync(GetRequiredOptionValue(args, "--insert-fan-in-scenario")),
                    repeatCount);
                return;

            case "--checkpoint-retention-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("checkpoint-retention-diagnostics", RunCheckpointRetentionDiagnosticsOnceAsync, repeatCount);
                return;

            case "--checkpoint-retention-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"checkpoint-retention-scenario-{GetRequiredOptionValue(args, "--checkpoint-retention-scenario")}",
                    () => RunCheckpointRetentionScenarioOnceAsync(GetRequiredOptionValue(args, "--checkpoint-retention-scenario")),
                    repeatCount);
                return;

            case "--write-transaction-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"write-transaction-scenario-{GetRequiredOptionValue(args, "--write-transaction-scenario")}",
                    () => RunWriteTransactionScenarioOnceAsync(GetRequiredOptionValue(args, "--write-transaction-scenario")),
                    repeatCount);
                return;

            case "--concurrent-write-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount);
                return;

            case "--concurrent-write-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"concurrent-write-scenario-{GetRequiredOptionValue(args, "--concurrent-write-scenario")}",
                    () => RunConcurrentWriteScenarioOnceAsync(GetRequiredOptionValue(args, "--concurrent-write-scenario")),
                    repeatCount);
                return;

            case "--direct-file-cache-transport":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount);
                return;

            case "--hybrid-storage-mode":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount);
                return;

            case "--master-table":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("master-table", RunMasterComparisonOnceAsync, repeatCount);
                return;

            case "--sqlite-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("sqlite-compare", RunSqliteComparisonOnceAsync, repeatCount);
                return;

            case "--strict-insert-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("strict-insert-compare", RunStrictInsertComparisonOnceAsync, repeatCount);
                return;

            case "--native-aot-insert-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("native-aot-insert-compare", RunNativeAotInsertComparisonOnceAsync, repeatCount);
                return;

            case "--hybrid-cold-open":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("hybrid-cold-open", RunHybridColdOpenOnceAsync, repeatCount);
                return;

            case "--hybrid-hot-set-read":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("hybrid-hot-set-read", RunHybridHotSetReadOnceAsync, repeatCount);
                return;

            case "--hybrid-post-checkpoint":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("hybrid-post-checkpoint", RunHybridPostCheckpointOnceAsync, repeatCount);
                return;

            case "--pr":
                await RunBenchmarkPlanAsync(
                    thresholdsFileName: "perf-thresholds-pr.json",
                    microHeading: "=== PR Micro Guardrails ===",
                    nonMicroHeading: "=== PR Non-Micro Guardrails ===");
                return;

            case "--release":
                await RunBenchmarkPlanAsync(
                    thresholdsFileName: "perf-thresholds.json",
                    microHeading: "=== Release Micro Guardrails ===",
                    nonMicroHeading: "=== Release Non-Micro Guardrails ===");
                return;

            case "--all":
                Console.WriteLine("=== Micro-Benchmarks (BenchmarkDotNet) ===");
                RunAllMicroBenchmarks();
                Console.WriteLine();
                Console.WriteLine("=== Macro-Benchmarks ===");
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("macro", RunMacroBenchmarksOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Direct File-Cache Transport Benchmark ===");
                await RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Concurrent Durable Write Benchmark ===");
                await RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Durable SQL Batching Benchmark ===");
                await RunSuiteWithRepeatsAsync("durable-sql-batching", RunDurableSqlBatchingOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Explicit WriteTransaction Benchmark ===");
                await RunSuiteWithRepeatsAsync("write-transaction-diagnostics", RunWriteTransactionDiagnosticsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Commit Fan-In Benchmark ===");
                await RunSuiteWithRepeatsAsync("commit-fan-in-diagnostics", RunCommitFanInDiagnosticsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Insert Fan-In Benchmark ===");
                await RunSuiteWithRepeatsAsync("insert-fan-in-diagnostics", RunInsertFanInDiagnosticsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Checkpoint Retention Benchmark ===");
                await RunSuiteWithRepeatsAsync("checkpoint-retention-diagnostics", RunCheckpointRetentionDiagnosticsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Storage Mode Benchmark ===");
                await RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Cold Open Benchmark ===");
                await RunSuiteWithRepeatsAsync("hybrid-cold-open", RunHybridColdOpenOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Hot-Set Read Benchmark ===");
                await RunSuiteWithRepeatsAsync("hybrid-hot-set-read", RunHybridHotSetReadOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Post Checkpoint Benchmark ===");
                await RunSuiteWithRepeatsAsync("hybrid-post-checkpoint", RunHybridPostCheckpointOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Stress Tests ===");
                await RunSuiteWithRepeatsAsync("stress", RunStressTestsOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Scaling Experiments ===");
                await RunSuiteWithRepeatsAsync("scaling", RunScalingExperimentsOnceAsync, repeatCount);
                return;
        }

        // Non-micro modes can be combined in one invocation (e.g., --macro --stress --scaling).
        var requestedModes = new HashSet<string>(args.Select(static a => a.ToLowerInvariant()), StringComparer.Ordinal);
        bool ranAny = false;

        if (requestedModes.Contains("--macro"))
        {
            EnsureReproConfigured();
            await RunSuiteWithRepeatsAsync("macro", RunMacroBenchmarksOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--macro-batch-memory"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("macro-batch-memory", RunInMemoryBatchBenchmarksOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--write-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("write-diagnostics", RunWriteDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--durable-sql-batching"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("durable-sql-batching", RunDurableSqlBatchingOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--write-transaction-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("write-transaction-diagnostics", RunWriteTransactionDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--commit-fan-in-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("commit-fan-in-diagnostics", RunCommitFanInDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--checkpoint-retention-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("checkpoint-retention-diagnostics", RunCheckpointRetentionDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--concurrent-write-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--direct-file-cache-transport"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--hybrid-storage-mode"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--master-table"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("master-table", RunMasterComparisonOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--sqlite-compare"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("sqlite-compare", RunSqliteComparisonOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--strict-insert-compare"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("strict-insert-compare", RunStrictInsertComparisonOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--native-aot-insert-compare"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("native-aot-insert-compare", RunNativeAotInsertComparisonOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--hybrid-cold-open"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("hybrid-cold-open", RunHybridColdOpenOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--hybrid-hot-set-read"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("hybrid-hot-set-read", RunHybridHotSetReadOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--hybrid-post-checkpoint"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("hybrid-post-checkpoint", RunHybridPostCheckpointOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--stress"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("stress", RunStressTestsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--scaling"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("scaling", RunScalingExperimentsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (!ranAny)
        {
            Console.WriteLine($"Unknown mode: {mode}");
            PrintHelp();
        }
    }

    private static void RunMicroBenchmarks(string[] args, bool excludePrGuardrailsWhenFilterMissing = false)
    {
        if (excludePrGuardrailsWhenFilterMissing && !ContainsExplicitFilter(args))
        {
            RunMicroBenchmarksWithoutPrGuardrails(args);
            return;
        }

        var switcher = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly);
        switcher.Run(args);
    }

    private static void RunAllMicroBenchmarks()
    {
        RunMicroBenchmarksWithoutPrGuardrails([]);
    }

    private static void RunMicroBenchmarksWithoutPrGuardrails(string[] args)
    {
        var benchmarkTypes = typeof(Program).Assembly
            .GetTypes()
            .Where(static type =>
                type is { IsClass: true, IsAbstract: false } &&
                string.Equals(type.Namespace, "CSharpDB.Benchmarks.Micro", StringComparison.Ordinal) &&
                !type.Name.EndsWith("GuardrailBenchmarks", StringComparison.Ordinal) &&
                type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                    .Any(static method => method.GetCustomAttribute<BenchmarkAttribute>() is not null))
            .ToArray();

        var switcher = BenchmarkSwitcher.FromTypes(benchmarkTypes);
        switcher.Run(args);
    }

    private static async Task RunBenchmarkPlanAsync(
        string thresholdsFileName,
        string microHeading,
        string nonMicroHeading)
    {
        var plan = LoadBenchmarkPlan(thresholdsFileName);

        if (plan.MicroFilters.Count == 0 && plan.Suites.Count == 0)
            throw new InvalidOperationException("Release benchmark plan is empty.");

        if (plan.MicroFilters.Count > 0)
        {
            Console.WriteLine(microHeading);
            foreach (string filter in plan.MicroFilters)
            {
                Console.WriteLine($"--- Micro ({filter}) ---");
                RunMicroBenchmarks(["--filter", filter]);
                Console.WriteLine();
            }
        }

        if (plan.Suites.Count == 0)
            return;

        bool reproConfigured = false;
        int? configuredCpuThreads = null;

        void EnsureReleaseReproConfigured(bool enableRepro, int? requestedCpuThreads)
        {
            if (!enableRepro || reproConfigured)
                return;

            configuredCpuThreads = requestedCpuThreads;
            BenchmarkProcessTuner.ConfigureIfRequested(enableRepro, requestedCpuThreads);
            reproConfigured = true;
        }

        Console.WriteLine(nonMicroHeading);
        foreach (var suite in plan.Suites)
        {
            int repeatCount = ParseRepeatCount(suite.Arguments);
            bool enableRepro = HasFlag(suite.Arguments, "--repro");
            int? requestedCpuThreads = ParseCpuThreads(suite.Arguments);

            if (reproConfigured && requestedCpuThreads != configuredCpuThreads)
            {
                throw new InvalidOperationException(
                    $"Release suite '{suite.Key}' requested cpu threads '{requestedCpuThreads}', but release mode was already configured with '{configuredCpuThreads}'.");
            }

            EnsureReleaseReproConfigured(enableRepro, requestedCpuThreads);
            Console.WriteLine($"--- {suite.Label} ---");
            await RunSuiteByKeyAsync(suite.Key, repeatCount);
            Console.WriteLine();
        }
    }

    private static async Task<List<BenchmarkResult>> RunMacroBenchmarksOnceAsync()
    {
        var results = new List<BenchmarkResult>();

        Console.WriteLine("--- Sustained Write Benchmark ---");
        results.AddRange(await SustainedWriteBenchmark.RunAsync());

        Console.WriteLine("--- Mixed Workload Benchmark ---");
        results.AddRange(await MixedWorkloadBenchmark.RunAsync());

        Console.WriteLine("--- Reader Scaling Benchmark ---");
        results.AddRange(await ReaderScalingBenchmark.RunAsync());

        Console.WriteLine("--- Write Amplification Benchmark ---");
        results.AddRange(await WriteAmplificationBenchmark.RunAsync());

        Console.WriteLine("--- Checkpoint Under Load Benchmark ---");
        results.AddRange(await CheckpointUnderLoadBenchmark.RunAsync());

        Console.WriteLine("--- Collection (NoSQL) Benchmark ---");
        results.AddRange(await CollectionBenchmark.RunAsync());

        Console.WriteLine("--- In-Memory Workload Benchmark ---");
        results.AddRange(await InMemoryWorkloadBenchmark.RunAsync());

        Console.WriteLine("--- Shared Memory ADO.NET Benchmark ---");
        results.AddRange(await SharedMemoryAdoNetBenchmark.RunAsync());

        Console.WriteLine("--- In-Memory Persistence Benchmark ---");
        results.AddRange(await InMemoryPersistenceBenchmark.RunAsync());

        return results;
    }

    private static async Task<List<BenchmarkResult>> RunInMemoryBatchBenchmarksOnceAsync()
    {
        Console.WriteLine("--- In-Memory Batch Benchmark ---");
        return await InMemoryBatchBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunWriteDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Durable Write Diagnostics Benchmark ---");
        return await DurableWriteDiagnosticsBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunDurableSqlBatchingOnceAsync()
    {
        Console.WriteLine("--- Durable SQL Batching Benchmark ---");
        return await DurableSqlBatchingBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunDurableSqlBatchingScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Durable SQL Batching Scenario: {scenarioName} ---");
        return [await DurableSqlBatchingBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunWriteTransactionDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Explicit WriteTransaction Benchmark ---");
        return await WriteTransactionDiagnosticsBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunCommitFanInDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Commit Fan-In Benchmark ---");
        return await CommitFanInDiagnosticsBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunCommitFanInScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Commit Fan-In Scenario: {scenarioName} ---");
        return [await CommitFanInDiagnosticsBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunInsertFanInDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Insert Fan-In Benchmark ---");
        return await InsertFanInDiagnosticsBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunInsertFanInScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Insert Fan-In Scenario: {scenarioName} ---");
        return [await InsertFanInDiagnosticsBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunCheckpointRetentionDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Checkpoint Retention Benchmark ---");
        return await CheckpointRetentionDiagnosticsBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunCheckpointRetentionScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Checkpoint Retention Scenario: {scenarioName} ---");
        return [await CheckpointRetentionDiagnosticsBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunWriteTransactionScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Explicit WriteTransaction Scenario: {scenarioName} ---");
        return [await WriteTransactionDiagnosticsBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunConcurrentWriteDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Concurrent Durable Write Benchmark ---");
        return await ConcurrentDurableWriteBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunConcurrentWriteScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Concurrent Durable Write Scenario: {scenarioName} ---");
        return [await ConcurrentDurableWriteBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunDirectFileCacheTransportOnceAsync()
    {
        Console.WriteLine("--- Direct File-Cache Transport Benchmark ---");
        return await DirectFileCacheTransportBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunHybridStorageModeOnceAsync()
    {
        Console.WriteLine("--- Hybrid Storage Mode Benchmark ---");
        return await HybridStorageModeBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunMasterComparisonOnceAsync()
    {
        Console.WriteLine("--- Master Comparison Benchmark ---");
        return await MasterComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunSqliteComparisonOnceAsync()
    {
        Console.WriteLine("--- SQLite Comparison Benchmark ---");
        return await SqliteComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunStrictInsertComparisonOnceAsync()
    {
        Console.WriteLine("--- Strict Insert Comparison Benchmark ---");
        return await StrictInsertComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunNativeAotInsertComparisonOnceAsync()
    {
        Console.WriteLine("--- NativeAOT Insert Comparison Benchmark ---");
        return await NativeAotInsertComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunHybridColdOpenOnceAsync()
    {
        Console.WriteLine("--- Hybrid Cold Open Benchmark ---");
        return await HybridColdOpenBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunHybridHotSetReadOnceAsync()
    {
        Console.WriteLine("--- Hybrid Hot-Set Read Benchmark ---");
        return await HybridHotSetReadBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunHybridPostCheckpointOnceAsync()
    {
        Console.WriteLine("--- Hybrid Post Checkpoint Benchmark ---");
        return await HybridPostCheckpointBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunStressTestsOnceAsync()
    {
        var results = new List<BenchmarkResult>();

        Console.WriteLine("--- Crash Recovery Benchmark ---");
        results.AddRange(await CrashRecoveryBenchmark.RunAsync());

        Console.WriteLine("--- Logical Conflict Range Benchmark ---");
        results.AddRange(await LogicalConflictRangeBenchmark.RunAsync());

        Console.WriteLine("--- WAL Growth Benchmark ---");
        results.AddRange(await WalGrowthBenchmark.RunAsync());

        return results;
    }

    private static async Task<List<BenchmarkResult>> RunScalingExperimentsOnceAsync()
    {
        var results = new List<BenchmarkResult>();

        Console.WriteLine("--- Row Count Scaling Benchmark ---");
        results.AddRange(await RowCountScalingBenchmark.RunAsync());

        Console.WriteLine("--- B+Tree Depth Benchmark ---");
        results.AddRange(await BTreeDepthBenchmark.RunAsync());

        return results;
    }

    private static Task RunSuiteByKeyAsync(string suiteKey, int repeatCount)
    {
        return suiteKey switch
        {
            "macro" => RunSuiteWithRepeatsAsync("macro", RunMacroBenchmarksOnceAsync, repeatCount),
            "macro-batch-memory" => RunSuiteWithRepeatsAsync("macro-batch-memory", RunInMemoryBatchBenchmarksOnceAsync, repeatCount),
            "write-diagnostics" => RunSuiteWithRepeatsAsync("write-diagnostics", RunWriteDiagnosticsOnceAsync, repeatCount),
            "durable-sql-batching" => RunSuiteWithRepeatsAsync("durable-sql-batching", RunDurableSqlBatchingOnceAsync, repeatCount),
            "write-transaction-diagnostics" => RunSuiteWithRepeatsAsync("write-transaction-diagnostics", RunWriteTransactionDiagnosticsOnceAsync, repeatCount),
            "commit-fan-in-diagnostics" => RunSuiteWithRepeatsAsync("commit-fan-in-diagnostics", RunCommitFanInDiagnosticsOnceAsync, repeatCount),
            "insert-fan-in-diagnostics" => RunSuiteWithRepeatsAsync("insert-fan-in-diagnostics", RunInsertFanInDiagnosticsOnceAsync, repeatCount),
            "checkpoint-retention-diagnostics" => RunSuiteWithRepeatsAsync("checkpoint-retention-diagnostics", RunCheckpointRetentionDiagnosticsOnceAsync, repeatCount),
            "concurrent-write-diagnostics" => RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount),
            "direct-file-cache-transport" => RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount),
            "hybrid-storage-mode" => RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount),
            "master-table" => RunSuiteWithRepeatsAsync("master-table", RunMasterComparisonOnceAsync, repeatCount),
            "sqlite-compare" => RunSuiteWithRepeatsAsync("sqlite-compare", RunSqliteComparisonOnceAsync, repeatCount),
            "strict-insert-compare" => RunSuiteWithRepeatsAsync("strict-insert-compare", RunStrictInsertComparisonOnceAsync, repeatCount),
            "native-aot-insert-compare" => RunSuiteWithRepeatsAsync("native-aot-insert-compare", RunNativeAotInsertComparisonOnceAsync, repeatCount),
            "hybrid-cold-open" => RunSuiteWithRepeatsAsync("hybrid-cold-open", RunHybridColdOpenOnceAsync, repeatCount),
            "hybrid-hot-set-read" => RunSuiteWithRepeatsAsync("hybrid-hot-set-read", RunHybridHotSetReadOnceAsync, repeatCount),
            "hybrid-post-checkpoint" => RunSuiteWithRepeatsAsync("hybrid-post-checkpoint", RunHybridPostCheckpointOnceAsync, repeatCount),
            "stress" => RunSuiteWithRepeatsAsync("stress", RunStressTestsOnceAsync, repeatCount),
            "scaling" => RunSuiteWithRepeatsAsync("scaling", RunScalingExperimentsOnceAsync, repeatCount),
            _ => throw new ArgumentException($"Unknown release suite key '{suiteKey}'.", nameof(suiteKey)),
        };
    }

    private static async Task RunSuiteWithRepeatsAsync(
        string suiteName,
        Func<Task<List<BenchmarkResult>>> runOnceAsync,
        int repeatCount)
    {
        var outputDir = Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        string runStamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        var allRuns = new List<IReadOnlyList<BenchmarkResult>>(repeatCount);

        if (repeatCount > 1)
        {
            Console.WriteLine($"=== {suiteName.ToUpperInvariant()} Warmup (not recorded) ===");
            await runOnceAsync();
            Console.WriteLine();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        for (int i = 0; i < repeatCount; i++)
        {
            if (repeatCount > 1)
                Console.WriteLine($"=== {suiteName.ToUpperInvariant()} Run {i + 1}/{repeatCount} ===");

            var runResults = await runOnceAsync();
            allRuns.Add(runResults);

            string outputFileName = repeatCount == 1
                ? $"{suiteName}-{runStamp}.csv"
                : $"{suiteName}-{runStamp}-run{i + 1}.csv";
            string outputPath = Path.Combine(outputDir, outputFileName);
            CsvReporter.WriteResults(outputPath, runResults);
            Console.WriteLine($"\nResults written to {outputPath}");
            CsvReporter.PrintSummaryTable(runResults);

            if (repeatCount > 1 && i < repeatCount - 1)
                Console.WriteLine();
        }

        if (repeatCount <= 1)
            return;

        var medianResults = BenchmarkResultAggregator.MedianAcrossRuns(allRuns);
        string medianOutputPath = Path.Combine(outputDir, $"{suiteName}-{runStamp}-median-of-{repeatCount}.csv");
        CsvReporter.WriteResults(medianOutputPath, medianResults);
        Console.WriteLine($"\nMedian summary written to {medianOutputPath}");
        CsvReporter.PrintSummaryTable(medianResults);
    }

    private static string[] StripCustomArgs(string[] args)
    {
        var filtered = new List<string>(args.Length);
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].Equals("--repeat", StringComparison.OrdinalIgnoreCase) ||
                args[i].Equals("--cpu-threads", StringComparison.OrdinalIgnoreCase))
            {
                i++;
                continue;
            }

            if (args[i].Equals("--repro", StringComparison.OrdinalIgnoreCase))
                continue;

            filtered.Add(args[i]);
        }

        return filtered.ToArray();
    }

    private static ReleaseBenchmarkPlan LoadBenchmarkPlan(string thresholdsFileName)
    {
        string thresholdsPath = ResolvePerfThresholdsPath(thresholdsFileName);
        using var document = JsonDocument.Parse(File.ReadAllText(thresholdsPath));

        if (!document.RootElement.TryGetProperty("checks", out JsonElement checksElement) ||
            checksElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException($"Benchmark threshold file '{thresholdsPath}' does not define a 'checks' array.");
        }

        var microFilters = new SortedSet<string>(StringComparer.Ordinal);
        var suites = new List<ReleaseSuite>();
        var seenSuites = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (JsonElement check in checksElement.EnumerateArray())
        {
            if (check.TryGetProperty("csv", out JsonElement csvElement))
            {
                string? filter = TryGetMicroFilterFromCsv(csvElement.GetString());
                if (!string.IsNullOrWhiteSpace(filter))
                    microFilters.Add(filter);
            }

            if (!check.TryGetProperty("suiteKey", out JsonElement suiteKeyElement) ||
                suiteKeyElement.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            string suiteKey = suiteKeyElement.GetString()!;
            if (!seenSuites.Add(suiteKey))
                continue;

            string label = check.TryGetProperty("suiteLabel", out JsonElement labelElement) && labelElement.ValueKind == JsonValueKind.String
                ? labelElement.GetString()!
                : suiteKey;
            string[] suiteArgs = check.TryGetProperty("suiteArgs", out JsonElement argsElement) && argsElement.ValueKind == JsonValueKind.Array
                ? argsElement.EnumerateArray()
                    .Where(static item => item.ValueKind == JsonValueKind.String)
                    .Select(static item => item.GetString()!)
                    .ToArray()
                : [];

            if (suiteArgs.Length == 0)
                throw new InvalidOperationException($"Release suite '{suiteKey}' in '{thresholdsPath}' is missing 'suiteArgs'.");

            suites.Add(new ReleaseSuite(suiteKey, label, suiteArgs));
        }

        return new ReleaseBenchmarkPlan(microFilters.ToArray(), suites);
    }

    private static string ResolvePerfThresholdsPath(string thresholdsFileName)
    {
        DirectoryInfo? current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current != null)
        {
            string candidate = Path.Combine(current.FullName, thresholdsFileName);
            if (File.Exists(candidate))
                return candidate;

            current = current.Parent;
        }

        throw new FileNotFoundException(
            $"Could not locate {thresholdsFileName} from the benchmark runner base directory.",
            thresholdsFileName);
    }

    private static string? TryGetMicroFilterFromCsv(string? csvName)
    {
        const string prefix = "CSharpDB.Benchmarks.Micro.";
        const string suffix = "-report.csv";

        if (string.IsNullOrWhiteSpace(csvName) ||
            !csvName.StartsWith(prefix, StringComparison.Ordinal) ||
            !csvName.EndsWith(suffix, StringComparison.Ordinal))
        {
            return null;
        }

        string benchmarkName = csvName[prefix.Length..^suffix.Length];
        return benchmarkName.Length == 0
            ? null
            : $"*{benchmarkName}*";
    }

    private static int ParseRepeatCount(string[] args)
    {
        int repeatCount = 1;
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].Equals("--repeat", StringComparison.OrdinalIgnoreCase))
                continue;

            if (i + 1 >= args.Length || !int.TryParse(args[i + 1], out int parsedCount) || parsedCount <= 0)
                throw new ArgumentException("Invalid --repeat value. Use a positive integer (for example, --repeat 3).");

            repeatCount = parsedCount;
            i++;
        }

        return repeatCount;
    }

    private static int? ParseCpuThreads(string[] args)
    {
        int? cpuThreads = null;
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].Equals("--cpu-threads", StringComparison.OrdinalIgnoreCase))
                continue;

            if (i + 1 >= args.Length || !int.TryParse(args[i + 1], out int parsedCount) || parsedCount <= 0)
                throw new ArgumentException("Invalid --cpu-threads value. Use a positive integer (for example, --cpu-threads 8).");

            cpuThreads = parsedCount;
            i++;
        }

        return cpuThreads;
    }

    private static bool HasFlag(string[] args, string flag)
    {
        return args.Any(a => a.Equals(flag, StringComparison.OrdinalIgnoreCase));
    }

    private static string GetRequiredOptionValue(string[] args, string option)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].Equals(option, StringComparison.OrdinalIgnoreCase))
                continue;

            if (i + 1 >= args.Length || string.IsNullOrWhiteSpace(args[i + 1]))
                throw new ArgumentException($"Missing value for {option}.");

            return args[i + 1];
        }

        throw new ArgumentException($"Missing required option {option}.");
    }

    private static bool ContainsExplicitFilter(string[] args)
    {
        return args.Any(static arg => arg.Equals("--filter", StringComparison.OrdinalIgnoreCase));
    }

    private static string[] RemoveFirstToken(string[] args, string token)
    {
        var result = new List<string>(args.Length);
        bool removed = false;
        foreach (string arg in args)
        {
            if (!removed && arg.Equals(token, StringComparison.OrdinalIgnoreCase))
            {
                removed = true;
                continue;
            }

            result.Add(arg);
        }

        return result.ToArray();
    }

    private static string GetPrimaryMode(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].Equals("--repeat", StringComparison.OrdinalIgnoreCase) ||
                args[i].Equals("--cpu-threads", StringComparison.OrdinalIgnoreCase))
            {
                i++;
                continue;
            }

            if (args[i].Equals("--repro", StringComparison.OrdinalIgnoreCase))
                continue;

            return args[i].ToLowerInvariant();
        }

        return string.Empty;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("CSharpDB Benchmark Suite");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -- --micro              Run BenchmarkDotNet micro-benchmarks");
        Console.WriteLine("  dotnet run -- --micro --filter *Insert*   Filter micro-benchmarks");
        Console.WriteLine("  dotnet run -- --macro              Run macro-benchmarks (sustained workloads)");
        Console.WriteLine("  dotnet run -- --macro-batch-memory Run in-memory rotating batch throughput benchmark");
        Console.WriteLine("  dotnet run -- --write-diagnostics  Run focused pager/WAL durable-write diagnostics");
        Console.WriteLine("  dotnet run -- --durable-sql-batching  Run focused durable SQL batching benchmark");
        Console.WriteLine("  dotnet run -- --durable-sql-batching-scenario TxBatch10_LowLatency  Run one durable SQL batching scenario");
        Console.WriteLine("  dotnet run -- --write-transaction-diagnostics  Run focused explicit WriteTransaction diagnostics");
        Console.WriteLine("  dotnet run -- --commit-fan-in-diagnostics  Compare shared auto-commit vs explicit WriteTransaction fan-in");
        Console.WriteLine("  dotnet run -- --commit-fan-in-scenario ExplicitTx_DisjointUpdate_W8_Batch250us  Run one commit fan-in scenario");
        Console.WriteLine("  dotnet run -- --insert-fan-in-diagnostics  Compare insert-side fan-in across auto-commit and explicit WriteTransaction");
        Console.WriteLine("  dotnet run -- --insert-fan-in-scenario AutoCommit_ExplicitId_W8_Batch250us  Run one insert fan-in scenario");
        Console.WriteLine("  dotnet run -- --checkpoint-retention-diagnostics  Run focused background-checkpoint retention diagnostics");
        Console.WriteLine("  dotnet run -- --checkpoint-retention-scenario W8_Blocker3s_Batch250us  Run one checkpoint-retention scenario");
        Console.WriteLine("  dotnet run -- --write-transaction-scenario UpdateDisjoint_W8_Rows1_Batch250us_Prealloc1MiB  Run one explicit WriteTransaction scenario");
        Console.WriteLine("  dotnet run -- --concurrent-write-diagnostics  Run focused multi-writer durable commit diagnostics");
        Console.WriteLine("  dotnet run -- --concurrent-write-scenario W8_Batch250us_Prealloc1MiB  Run one concurrent durable-write scenario");
        Console.WriteLine("  dotnet run -- --direct-file-cache-transport  Run focused direct default-vs-tuned file-cache benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-storage-mode  Run focused storage-mode coverage plus the Plan 2 bulk insert durability/residency matrix");
        Console.WriteLine("  dotnet run -- --master-table  Run only the CSharpDB rows used by the README master comparison table");
        Console.WriteLine("  dotnet run -- --sqlite-compare  Run local SQLite WAL+FULL apples-to-apples SQL comparison rows");
        Console.WriteLine("  dotnet run -- --strict-insert-compare  Run strict ADO.NET raw-vs-prepared insert comparison for CSharpDB and SQLite");
        Console.WriteLine("  dotnet run -- --native-aot-insert-compare  Run raw+prepared insert comparison for CSharpDB ADO.NET, CSharpDB NativeAOT FFI, and SQLite");
        Console.WriteLine("  dotnet run -- --hybrid-cold-open  Run focused engine-cold open + first read benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-hot-set-read  Run focused post-open hot-set read benchmark including hybrid warm-set mode");
        Console.WriteLine("  dotnet run -- --hybrid-post-checkpoint  Run focused post-checkpoint hot reread benchmark");
        Console.WriteLine("  dotnet run -- --pr                 Run the fast PR guardrail subset from perf-thresholds-pr.json");
        Console.WriteLine("  dotnet run -- --release            Run the focused release guardrail subset from perf-thresholds.json");
        Console.WriteLine("  dotnet run -- --stress             Run stress & durability tests");
        Console.WriteLine("  dotnet run -- --scaling            Run scaling experiments");
        Console.WriteLine("  dotnet run -- --macro --stress --scaling --write-diagnostics --durable-sql-batching --write-transaction-diagnostics --commit-fan-in-diagnostics --insert-fan-in-diagnostics --checkpoint-retention-diagnostics --concurrent-write-diagnostics --direct-file-cache-transport --hybrid-storage-mode --master-table --sqlite-compare --strict-insert-compare --native-aot-insert-compare --hybrid-cold-open --hybrid-hot-set-read --hybrid-post-checkpoint   Run non-micro suites in one invocation");
        Console.WriteLine("  dotnet run -- --macro --repeat 3   Repeat suite and emit median-of-N CSV");
        Console.WriteLine("  dotnet run -- --master-table --repeat 3 --repro   Run a stable median master comparison refresh");
        Console.WriteLine("  dotnet run -- --sqlite-compare --repeat 3 --repro   Run a stable local SQLite median comparison capture");
        Console.WriteLine("  dotnet run -- --strict-insert-compare --repeat 3 --repro   Run a stable strict insert comparison capture");
        Console.WriteLine("  dotnet run -- --native-aot-insert-compare --repeat 3 --repro   Run a stable NativeAOT raw+prepared insert comparison capture");
        Console.WriteLine("  dotnet run -- --scaling --repro    Run non-micro suite with high-priority + pinned CPU affinity");
        Console.WriteLine("  dotnet run -- --scaling --repro --cpu-threads 8   Pin to first 8 logical CPUs");
        Console.WriteLine("  --repro applies to non-micro suites only (micro remains BenchmarkDotNet-managed)");
        Console.WriteLine("  dotnet run -- --all                Run everything in sequence (full micro sweep, very slow)");
    }

    private sealed record ReleaseBenchmarkPlan(IReadOnlyList<string> MicroFilters, IReadOnlyList<ReleaseSuite> Suites);

    private sealed record ReleaseSuite(string Key, string Label, string[] Arguments);
}
