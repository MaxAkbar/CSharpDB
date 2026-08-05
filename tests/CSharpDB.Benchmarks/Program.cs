using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using CSharpDB.Benchmarks.Micro;
using CSharpDB.Benchmarks.Stress;
using CSharpDB.Benchmarks.Scaling;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;

namespace CSharpDB.Benchmarks;

public static class Program
{
    internal const int MinimumReleaseCoreLatencySamples = 100;
    internal const string DurableMasterWriteSuiteKey = "master-table-durable-writes";
    internal const string DurableMasterWriteScenarioSuiteKey =
        "master-table-durable-write-scenario";
    internal const string HostedStableMasterSuiteKey = "master-table-hosted-stable";

    private static readonly string[] s_releaseCoreSuiteKeys =
    [
        "master-table",
        "durable-sql-batching",
        "concurrent-write-diagnostics",
        "hybrid-storage-mode",
        "hybrid-hot-set-read",
        "hybrid-cold-open",
        "sqlite-compare",
    ];
    private static readonly string[] s_releaseEvidenceSuiteKeys =
    [
        .. s_releaseCoreSuiteKeys,
        DurableMasterWriteSuiteKey,
        DurableMasterWriteScenarioSuiteKey,
        HostedStableMasterSuiteKey,
    ];

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
        bool warmupSingleSample = HasFlag(args, "--warmup-single-sample");
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
        ValidateWarmupSingleSampleOption(mode, repeatCount, warmupSingleSample);
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

            case "--fts-hot-token-smoke":
                EnsureReproConfigured();
                await RunFullTextHotTokenSmokeAsync(args);
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
                await RunSuiteWithRepeatsAsync(
                    "durable-sql-batching",
                    RunDurableSqlBatchingOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--durable-sql-batching-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"durable-sql-batching-scenario-{GetRequiredOptionValue(args, "--durable-sql-batching-scenario")}",
                    () => RunDurableSqlBatchingScenarioOnceAsync(GetRequiredOptionValue(args, "--durable-sql-batching-scenario")),
                    repeatCount);
                return;

            case "--migration-target-throughput":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "migration-target-throughput",
                    RunMigrationTargetThroughputOnceAsync,
                    repeatCount);
                return;

            case "--migration-target-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"migration-target-scenario-{GetRequiredOptionValue(args, "--migration-target-scenario")}",
                    () => RunMigrationTargetScenarioOnceAsync(GetRequiredOptionValue(args, "--migration-target-scenario")),
                    repeatCount);
                return;

            case "--csv-retained-migration":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "csv-retained-migration",
                    RunCsvRetainedMigrationOnceAsync,
                    repeatCount);
                return;

            case "--csv-retained-migration-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"csv-retained-migration-scenario-{GetRequiredOptionValue(args, "--csv-retained-migration-scenario")}",
                    () => RunCsvRetainedMigrationScenarioOnceAsync(
                        GetRequiredOptionValue(args, "--csv-retained-migration-scenario")),
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

            case "--optimizer-closeout":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("optimizer-closeout", RunOptimizerCloseOutOnceAsync, repeatCount);
                return;

            case "--adaptive-reoptimization":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("adaptive-reoptimization", RunAdaptiveReoptimizationOnceAsync, repeatCount);
                return;

            case "--async-io-closeout":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("async-io-closeout", RunAsyncIoCloseOutOnceAsync, repeatCount);
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
                await RunSuiteWithRepeatsAsync(
                    "concurrent-write-diagnostics",
                    RunConcurrentWriteDiagnosticsOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--concurrent-write-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"concurrent-write-scenario-{GetRequiredOptionValue(args, "--concurrent-write-scenario")}",
                    () => RunConcurrentWriteScenarioOnceAsync(GetRequiredOptionValue(args, "--concurrent-write-scenario")),
                    repeatCount);
                return;

            case "--concurrent-sqlite-capi-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("concurrent-sqlite-capi-compare", RunConcurrentSqliteCApiComparisonOnceAsync, repeatCount);
                return;

            case "--concurrent-sqlite-capi-compare-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"concurrent-sqlite-capi-compare-scenario-{GetRequiredOptionValue(args, "--concurrent-sqlite-capi-compare-scenario")}",
                    () => RunConcurrentSqliteCApiComparisonScenarioOnceAsync(GetRequiredOptionValue(args, "--concurrent-sqlite-capi-compare-scenario")),
                    repeatCount);
                return;

            case "--concurrent-adonet-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("concurrent-adonet-compare", RunConcurrentAdoNetComparisonOnceAsync, repeatCount);
                return;

            case "--concurrent-adonet-compare-scenario":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    $"concurrent-adonet-compare-scenario-{GetRequiredOptionValue(args, "--concurrent-adonet-compare-scenario")}",
                    () => RunConcurrentAdoNetComparisonScenarioOnceAsync(GetRequiredOptionValue(args, "--concurrent-adonet-compare-scenario")),
                    repeatCount);
                return;

            case "--direct-file-cache-transport":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount);
                return;

            case "--hybrid-storage-mode":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "hybrid-storage-mode",
                    RunHybridStorageModeOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--hybrid-storage-mode-scenario":
                EnsureReproConfigured();
                await RunHybridStorageModeScenarioWithRepeatsAsync(
                    GetRequiredOptionValue(args, "--hybrid-storage-mode-scenario"),
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--master-table":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "master-table",
                    RunMasterComparisonOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--master-table-durable-writes":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    DurableMasterWriteSuiteKey,
                    RunMasterComparisonDurableWritesOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--master-table-durable-write-scenario":
                EnsureReproConfigured();
                await RunMasterComparisonDurableWriteScenarioWithRepeatsAsync(
                    GetRequiredOptionValue(args, "--master-table-durable-write-scenario"),
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--master-table-hosted-stable":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    HostedStableMasterSuiteKey,
                    RunMasterComparisonHostedStableOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--sqlite-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "sqlite-compare",
                    RunSqliteComparisonOnceAsync,
                    repeatCount,
                    warmupSingleSample);
                return;

            case "--strict-insert-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("strict-insert-compare", RunStrictInsertComparisonOnceAsync, repeatCount);
                return;

            case "--native-aot-insert-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("native-aot-insert-compare", RunNativeAotInsertComparisonOnceAsync, repeatCount);
                return;

            case "--efcore-compare":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("efcore-compare", RunEfCoreComparisonOnceAsync, repeatCount);
                return;

            case "--efcore-compare-auto-open-close":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "efcore-compare-auto-open-close",
                    RunEfCoreAutoOpenCloseComparisonOnceAsync,
                    repeatCount);
                return;

            case "--efcore-compare-hybrid-shared-connection":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "efcore-compare-hybrid-shared-connection",
                    RunEfCoreHybridSharedConnectionComparisonOnceAsync,
                    repeatCount);
                return;

            case "--hybrid-cold-open":
                EnsureReproConfigured();
                await RunHybridColdOpenWithRepeatsAsync(repeatCount, warmupSingleSample);
                return;

            case "--hybrid-hot-set-read":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync(
                    "hybrid-hot-set-read",
                    RunHybridHotSetReadOnceAsync,
                    repeatCount,
                    warmupSingleSample);
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

            case "--release-core":
                EnsureReproConfigured();
                await RunReleaseCoreAsync(repeatCount);
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
                Console.WriteLine("=== Optimizer Close-Out Benchmark ===");
                await RunSuiteWithRepeatsAsync("optimizer-closeout", RunOptimizerCloseOutOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Adaptive Reoptimization Benchmark ===");
                await RunSuiteWithRepeatsAsync("adaptive-reoptimization", RunAdaptiveReoptimizationOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Async I/O Close-Out Benchmark ===");
                await RunSuiteWithRepeatsAsync("async-io-closeout", RunAsyncIoCloseOutOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Storage Mode Benchmark ===");
                await RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount);
                Console.WriteLine();
                Console.WriteLine("=== Hybrid Cold Open Benchmark ===");
                await RunHybridColdOpenWithRepeatsAsync(repeatCount);
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

        if (requestedModes.Contains("--optimizer-closeout"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("optimizer-closeout", RunOptimizerCloseOutOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--adaptive-reoptimization"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("adaptive-reoptimization", RunAdaptiveReoptimizationOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--async-io-closeout"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("async-io-closeout", RunAsyncIoCloseOutOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--concurrent-write-diagnostics"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--concurrent-sqlite-capi-compare"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("concurrent-sqlite-capi-compare", RunConcurrentSqliteCApiComparisonOnceAsync, repeatCount);
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

        if (requestedModes.Contains("--efcore-compare"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync("efcore-compare", RunEfCoreComparisonOnceAsync, repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--efcore-compare-auto-open-close"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync(
                "efcore-compare-auto-open-close",
                RunEfCoreAutoOpenCloseComparisonOnceAsync,
                repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--efcore-compare-hybrid-shared-connection"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunSuiteWithRepeatsAsync(
                "efcore-compare-hybrid-shared-connection",
                RunEfCoreHybridSharedConnectionComparisonOnceAsync,
                repeatCount);
            ranAny = true;
        }

        if (requestedModes.Contains("--hybrid-cold-open"))
        {
            EnsureReproConfigured();
            if (ranAny) Console.WriteLine();
            await RunHybridColdOpenWithRepeatsAsync(repeatCount);
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
        RunMicroBenchmarksWithoutPrGuardrails(["--filter", "*"]);
    }

    private static async Task RunFullTextHotTokenSmokeAsync(string[] args)
    {
        int postingCount = HasFlag(args, "--postings")
            ? ParsePositiveInt(GetRequiredOptionValue(args, "--postings"), "--postings")
            : 20_000;

        await using var bench = await FullTextHotTokenBenchmarkData.CreateEmptyAsync();

        var build = Stopwatch.StartNew();
        await bench.Db.EnsureFullTextIndexAsync(
            FullTextHotTokenBenchmarkData.IndexName,
            FullTextHotTokenBenchmarkData.TableName,
            [FullTextHotTokenBenchmarkData.BodyColumn]);
        await FullTextHotTokenBenchmarkData.SeedHotTokenRowsAsync(bench, postingCount);
        build.Stop();

        long bytesAfterBuild = FullTextHotTokenBenchmarkData.GetDatabaseBytes(bench);

        var query = Stopwatch.StartNew();
        var hits = await bench.Db.SearchAsync(
            FullTextHotTokenBenchmarkData.IndexName,
            FullTextHotTokenBenchmarkData.HotQuery);
        query.Stop();
        if (hits.Count != postingCount)
            throw new InvalidOperationException($"Expected {postingCount} hot-token hits, got {hits.Count}.");

        int updateId = Math.Max(1, postingCount / 2);
        var update = Stopwatch.StartNew();
        await bench.Db.ExecuteAsync(
            FormattableString.Invariant($"UPDATE {FullTextHotTokenBenchmarkData.TableName} SET body = 'cool unique_{postingCount}' WHERE id = {updateId}"));
        update.Stop();

        var uniqueHits = await bench.Db.SearchAsync(
            FullTextHotTokenBenchmarkData.IndexName,
            FormattableString.Invariant($"unique_{postingCount}"));
        if (uniqueHits.Count != 1 || uniqueHits[0].RowId != updateId)
            throw new InvalidOperationException("Updated full-text token was not searchable.");

        int deleteId = postingCount <= 1
            ? updateId
            : updateId == 1 ? 2 : updateId - 1;
        var delete = Stopwatch.StartNew();
        await bench.Db.ExecuteAsync(
            FormattableString.Invariant($"DELETE FROM {FullTextHotTokenBenchmarkData.TableName} WHERE id = {deleteId}"));
        delete.Stop();

        var deletedHits = await bench.Db.SearchAsync(
            FullTextHotTokenBenchmarkData.IndexName,
            FormattableString.Invariant($"payload_{deleteId:D8}"));
        if (deletedHits.Count != 0)
            throw new InvalidOperationException("Deleted full-text token remained searchable.");

        long finalBytes = FullTextHotTokenBenchmarkData.GetDatabaseBytes(bench);

        Console.WriteLine("FullTextHotTokenSmoke");
        Console.WriteLine($"posting_count={postingCount.ToString(CultureInfo.InvariantCulture)}");
        Console.WriteLine($"build_insert_index_ms={FormatMilliseconds(build.Elapsed)}");
        Console.WriteLine($"query_hot_token_ms={FormatMilliseconds(query.Elapsed)}");
        Console.WriteLine($"update_single_row_ms={FormatMilliseconds(update.Elapsed)}");
        Console.WriteLine($"delete_single_row_ms={FormatMilliseconds(delete.Elapsed)}");
        Console.WriteLine($"db_bytes_after_build={bytesAfterBuild.ToString(CultureInfo.InvariantCulture)}");
        Console.WriteLine($"db_bytes_final={finalBytes.ToString(CultureInfo.InvariantCulture)}");
    }

    private static string FormatMilliseconds(TimeSpan elapsed) =>
        elapsed.TotalMilliseconds.ToString("F1", CultureInfo.InvariantCulture);

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
        switcher.Run(args.Length == 0 && !ContainsExplicitFilter(args) ? ["--filter", "*"] : args);
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

    private static async Task<List<BenchmarkResult>> RunMigrationTargetThroughputOnceAsync()
    {
        Console.WriteLine("--- Migration Staged-Target Throughput And Memory Qualification ---");
        return await MigrationTargetThroughputBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunMigrationTargetScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Migration Staged-Target Scenario: {scenarioName} ---");
        return [await MigrationTargetThroughputBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunCsvRetainedMigrationOnceAsync()
    {
        Console.WriteLine("--- Retained CSV Migration Benchmark ---");
        return await CsvRetainedMigrationBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunCsvRetainedMigrationScenarioOnceAsync(
        string scenarioName)
    {
        Console.WriteLine($"--- Retained CSV Migration Scenario: {scenarioName} ---");
        return await CsvRetainedMigrationBenchmark.RunNamedScenarioAsync(scenarioName);
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

    private static async Task<List<BenchmarkResult>> RunOptimizerCloseOutOnceAsync()
    {
        Console.WriteLine("--- Optimizer Close-Out Benchmark ---");
        return await OptimizerCloseOutBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunAdaptiveReoptimizationOnceAsync()
    {
        Console.WriteLine("--- Adaptive Reoptimization Benchmark ---");
        return await AdaptiveReoptimizationBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunAsyncIoCloseOutOnceAsync()
    {
        Console.WriteLine("--- Async I/O Close-Out Benchmark ---");
        return await AsyncIoCloseOutBenchmark.RunAsync();
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

    private static async Task<List<BenchmarkResult>> RunConcurrentSqliteCApiComparisonOnceAsync()
    {
        Console.WriteLine("--- Concurrent SQLite C-API Comparison Benchmark ---");
        return await ConcurrentSqliteCApiComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunConcurrentSqliteCApiComparisonScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Concurrent SQLite C-API Comparison Scenario: {scenarioName} ---");
        return [await ConcurrentSqliteCApiComparisonBenchmark.RunNamedScenarioAsync(scenarioName)];
    }

    private static async Task<List<BenchmarkResult>> RunConcurrentAdoNetComparisonOnceAsync()
    {
        Console.WriteLine("--- Concurrent ADO.NET Comparison Benchmark ---");
        return await ConcurrentAdoNetComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunConcurrentAdoNetComparisonScenarioOnceAsync(string scenarioName)
    {
        Console.WriteLine($"--- Concurrent ADO.NET Comparison Scenario: {scenarioName} ---");
        return [await ConcurrentAdoNetComparisonBenchmark.RunNamedScenarioAsync(scenarioName)];
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

    internal static Task RunHybridStorageModeScenarioWithRepeatsAsync(
        string scenarioName,
        int repeatCount,
        bool warmupSingleSample,
        string? outputDirectory = null,
        Func<string, Task<BenchmarkResult>>? runScenarioAsync = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);
        runScenarioAsync ??= HybridStorageModeBenchmark.RunNamedQualificationScenarioAsync;

        return RunSuiteWithRepeatsAsync(
            "hybrid-storage-mode-scenario",
            async () => [await runScenarioAsync(scenarioName)],
            repeatCount,
            warmupSingleSample,
            outputDirectory,
            scenarioProvidesWarmup: true);
    }

    private static async Task<List<BenchmarkResult>> RunMasterComparisonOnceAsync()
    {
        Console.WriteLine("--- Master Comparison Benchmark ---");
        return await MasterComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunMasterComparisonDurableWritesOnceAsync()
    {
        Console.WriteLine("--- Durable Master Comparison Writes ---");
        return await MasterComparisonBenchmark.RunDurableWritesAsync();
    }

    internal static Task RunMasterComparisonDurableWriteScenarioWithRepeatsAsync(
        string masterRowName,
        int repeatCount,
        bool warmupSingleSample,
        string? outputDirectory = null,
        Func<string, Task<BenchmarkResult>>? runScenarioAsync = null)
    {
        _ = MasterComparisonBenchmark.GetDurableWriteQualificationSourceName(masterRowName);
        runScenarioAsync ??= MasterComparisonBenchmark.RunDurableWriteQualificationAsync;

        return RunSuiteWithRepeatsAsync(
            DurableMasterWriteScenarioSuiteKey,
            async () =>
            {
                BenchmarkResult result = await runScenarioAsync(masterRowName);
                if (!string.Equals(result.Name, masterRowName, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Durable-write qualification requested row '{masterRowName}' " +
                        $"but received '{result.Name}'.");
                }

                return [result];
            },
            repeatCount,
            warmupSingleSample,
            outputDirectory,
            scenarioProvidesWarmup: true);
    }

    private static async Task<List<BenchmarkResult>> RunMasterComparisonHostedStableOnceAsync()
    {
        Console.WriteLine("--- Hosted-Stable Master Comparison Rows ---");
        return await MasterComparisonBenchmark.RunHostedStableRowsAsync();
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

    private static async Task<List<BenchmarkResult>> RunEfCoreComparisonOnceAsync()
    {
        Console.WriteLine("--- EF Core Comparison Benchmark (Open Once) ---");
        return await EfCoreComparisonBenchmark.RunAsync();
    }

    private static async Task<List<BenchmarkResult>> RunEfCoreAutoOpenCloseComparisonOnceAsync()
    {
        Console.WriteLine("--- EF Core Comparison Benchmark (Auto Open/Close) ---");
        return await EfCoreComparisonBenchmark.RunAsync(
            EfCoreComparisonBenchmark.ConnectionLifetimeMode.AutoOpenClosePerSaveChanges);
    }

    private static async Task<List<BenchmarkResult>> RunEfCoreHybridSharedConnectionComparisonOnceAsync()
    {
        Console.WriteLine("--- EF Core Comparison Benchmark (Hybrid Shared Connection) ---");
        return await EfCoreComparisonBenchmark.RunAsync(
            EfCoreComparisonBenchmark.ConnectionLifetimeMode.HybridSharedConnectionPerRun);
    }

    private static async Task RunHybridColdOpenWithRepeatsAsync(
        int repeatCount,
        bool warmupSingleSample = false)
    {
        Console.WriteLine("--- Hybrid Cold Open Benchmark ---");
        List<IReadOnlyList<BenchmarkResult>> runs =
            await HybridColdOpenBenchmark.RunRepeatedAsync(repeatCount, warmupSingleSample);
        WriteSuiteResults("hybrid-cold-open", runs);
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

    private static async Task RunReleaseCoreAsync(int repeatCount)
    {
        Console.WriteLine("=== Release Core Benchmark Suite ===");
        Console.WriteLine("Runs only the benchmark suites that feed the published README tables.");

        for (int i = 0; i < s_releaseCoreSuiteKeys.Length; i++)
        {
            if (i > 0)
                Console.WriteLine();

            await RunSuiteByKeyAsync(s_releaseCoreSuiteKeys[i], repeatCount);
        }
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
            "optimizer-closeout" => RunSuiteWithRepeatsAsync("optimizer-closeout", RunOptimizerCloseOutOnceAsync, repeatCount),
            "adaptive-reoptimization" => RunSuiteWithRepeatsAsync("adaptive-reoptimization", RunAdaptiveReoptimizationOnceAsync, repeatCount),
            "async-io-closeout" => RunSuiteWithRepeatsAsync("async-io-closeout", RunAsyncIoCloseOutOnceAsync, repeatCount),
            "concurrent-write-diagnostics" => RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount),
            "concurrent-sqlite-capi-compare" => RunSuiteWithRepeatsAsync("concurrent-sqlite-capi-compare", RunConcurrentSqliteCApiComparisonOnceAsync, repeatCount),
            "concurrent-adonet-compare" => RunSuiteWithRepeatsAsync("concurrent-adonet-compare", RunConcurrentAdoNetComparisonOnceAsync, repeatCount),
            "direct-file-cache-transport" => RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount),
            "hybrid-storage-mode" => RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount),
            "master-table" => RunSuiteWithRepeatsAsync("master-table", RunMasterComparisonOnceAsync, repeatCount),
            "sqlite-compare" => RunSuiteWithRepeatsAsync("sqlite-compare", RunSqliteComparisonOnceAsync, repeatCount),
            "strict-insert-compare" => RunSuiteWithRepeatsAsync("strict-insert-compare", RunStrictInsertComparisonOnceAsync, repeatCount),
            "native-aot-insert-compare" => RunSuiteWithRepeatsAsync("native-aot-insert-compare", RunNativeAotInsertComparisonOnceAsync, repeatCount),
            "efcore-compare" => RunSuiteWithRepeatsAsync("efcore-compare", RunEfCoreComparisonOnceAsync, repeatCount),
            "efcore-compare-auto-open-close" => RunSuiteWithRepeatsAsync("efcore-compare-auto-open-close", RunEfCoreAutoOpenCloseComparisonOnceAsync, repeatCount),
            "efcore-compare-hybrid-shared-connection" => RunSuiteWithRepeatsAsync("efcore-compare-hybrid-shared-connection", RunEfCoreHybridSharedConnectionComparisonOnceAsync, repeatCount),
            "hybrid-cold-open" => RunHybridColdOpenWithRepeatsAsync(repeatCount),
            "hybrid-hot-set-read" => RunSuiteWithRepeatsAsync("hybrid-hot-set-read", RunHybridHotSetReadOnceAsync, repeatCount),
            "hybrid-post-checkpoint" => RunSuiteWithRepeatsAsync("hybrid-post-checkpoint", RunHybridPostCheckpointOnceAsync, repeatCount),
            "stress" => RunSuiteWithRepeatsAsync("stress", RunStressTestsOnceAsync, repeatCount),
            "scaling" => RunSuiteWithRepeatsAsync("scaling", RunScalingExperimentsOnceAsync, repeatCount),
            _ => throw new ArgumentException($"Unknown release suite key '{suiteKey}'.", nameof(suiteKey)),
        };
    }

    internal static async Task RunSuiteWithRepeatsAsync(
        string suiteName,
        Func<Task<List<BenchmarkResult>>> runOnceAsync,
        int repeatCount,
        bool warmupSingleSample = false,
        string? outputDirectory = null,
        bool scenarioProvidesWarmup = false)
    {
        if (repeatCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(repeatCount), "Repeat count must be positive.");
        if (warmupSingleSample && repeatCount != 1)
        {
            throw new ArgumentException(
                "Single-sample warmup requires exactly one recorded repeat.",
                nameof(warmupSingleSample));
        }

        string outputDir = outputDirectory ?? Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        string runStamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        var allRuns = new List<IReadOnlyList<BenchmarkResult>>(repeatCount);

        bool runOuterWarmup = !scenarioProvidesWarmup && (repeatCount > 1 || warmupSingleSample);
        if (runOuterWarmup)
        {
            Console.WriteLine($"=== {suiteName.ToUpperInvariant()} Warmup (not recorded) ===");
            await runOnceAsync();
            Console.WriteLine();
            MacroBenchmarkRunner.StabilizeAfterWarmup();
        }

        for (int i = 0; i < repeatCount; i++)
        {
            if (repeatCount > 1 || warmupSingleSample)
                Console.WriteLine($"=== {suiteName.ToUpperInvariant()} Run {i + 1}/{repeatCount} ===");

            var runResults = await runOnceAsync();
            allRuns.Add(runResults);

            string outputFileName = repeatCount == 1
                ? $"{suiteName}-{runStamp}.csv"
                : $"{suiteName}-{runStamp}-run{i + 1}.csv";
            string outputPath = Path.Combine(outputDir, outputFileName);
            ValidateReleaseCoreResults(suiteName, runResults);
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
        ValidateReleaseCoreResults(suiteName, medianResults);
        CsvReporter.WriteResults(medianOutputPath, medianResults);
        Console.WriteLine($"\nMedian summary written to {medianOutputPath}");
        CsvReporter.PrintSummaryTable(medianResults);
    }

    internal static void WriteSuiteResults(
        string suiteName,
        IReadOnlyList<IReadOnlyList<BenchmarkResult>> allRuns,
        string? outputDirectory = null)
    {
        if (allRuns.Count == 0)
            throw new InvalidOperationException($"Benchmark suite '{suiteName}' produced no runs.");

        string outputDir = outputDirectory ?? Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        string runStamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");

        for (int runIndex = 0; runIndex < allRuns.Count; runIndex++)
        {
            string outputFileName = allRuns.Count == 1
                ? $"{suiteName}-{runStamp}.csv"
                : $"{suiteName}-{runStamp}-run{runIndex + 1}.csv";
            string outputPath = Path.Combine(outputDir, outputFileName);
            ValidateReleaseCoreResults(suiteName, allRuns[runIndex]);
            CsvReporter.WriteResults(outputPath, allRuns[runIndex]);
            Console.WriteLine($"\nResults written to {outputPath}");
            CsvReporter.PrintSummaryTable(allRuns[runIndex]);
        }

        if (allRuns.Count <= 1)
            return;

        var medianResults = BenchmarkResultAggregator.MedianAcrossRuns(allRuns);
        string medianOutputPath = Path.Combine(
            outputDir,
            $"{suiteName}-{runStamp}-median-of-{allRuns.Count}.csv");
        ValidateReleaseCoreResults(suiteName, medianResults);
        CsvReporter.WriteResults(medianOutputPath, medianResults);
        Console.WriteLine($"\nMedian summary written to {medianOutputPath}");
        CsvReporter.PrintSummaryTable(medianResults);
    }

    internal static void ValidateReleaseCoreResults(
        string suiteName,
        IReadOnlyList<BenchmarkResult> results)
    {
        if (!s_releaseEvidenceSuiteKeys.Contains(suiteName, StringComparer.Ordinal))
            return;

        if (results.Count == 0)
        {
            throw new InvalidOperationException(
                $"Release-core suite '{suiteName}' produced no benchmark rows; " +
                "a release qualification CSV cannot be emitted without measurement evidence.");
        }

        if (string.Equals(
                suiteName,
                DurableMasterWriteScenarioSuiteKey,
                StringComparison.Ordinal))
        {
            ValidateDurableWriteScenarioResults(results);
        }

        foreach (BenchmarkResult result in results)
        {
            if (result.LatencySamples >= MinimumReleaseCoreLatencySamples)
            {
                ValidatePositiveReleaseCoreMetric(
                    suiteName,
                    result.Name,
                    "TotalOps",
                    result.TotalOps,
                    "0");
                ValidatePositiveReleaseCoreMetric(
                    suiteName,
                    result.Name,
                    "ElapsedMs",
                    result.ElapsedMs,
                    "F2");
                ValidatePositiveReleaseCoreMetric(
                    suiteName,
                    result.Name,
                    "OpsPerSec",
                    result.OpsPerSecond,
                    "F1");
                ValidatePositiveReleaseCoreMetric(
                    suiteName,
                    result.Name,
                    "P99",
                    result.P99Ms,
                    "F4");
                continue;
            }

            throw new InvalidOperationException(
                $"Release-core suite '{suiteName}' row '{result.Name}' produced " +
                $"{result.LatencySamples:N0} retained latency samples; at least " +
                $"{MinimumReleaseCoreLatencySamples:N0} are required before CSV emission.");
        }
    }

    private static void ValidateDurableWriteScenarioResults(
        IReadOnlyList<BenchmarkResult> results)
    {
        if (results.Count != 1)
        {
            throw new InvalidOperationException(
                $"Release-core suite '{DurableMasterWriteScenarioSuiteKey}' produced " +
                $"{results.Count} rows; exactly one durable master write row is required.");
        }

        BenchmarkResult result = results[0];
        if (!MasterComparisonBenchmark.DurableWriteRowNames.Contains(
                result.Name,
                StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"Release-core suite '{DurableMasterWriteScenarioSuiteKey}' produced " +
                $"unknown row '{result.Name}'.");
        }

        ReleaseQualificationSettings settings = ReleaseQualificationSettings.DurableWrite;
        if (result.LatencySamples < settings.MinimumLatencySamples)
        {
            throw new InvalidOperationException(
                $"Release-core suite '{DurableMasterWriteScenarioSuiteKey}' row " +
                $"'{result.Name}' produced {result.LatencySamples:N0} retained latency samples; " +
                $"at least {settings.MinimumLatencySamples:N0} are required before CSV emission.");
        }
        if (!double.IsFinite(result.ElapsedMs) ||
            result.ElapsedMs < settings.MinimumMeasuredDuration.TotalMilliseconds ||
            result.ElapsedMs > settings.MaximumMeasuredDuration.TotalMilliseconds)
        {
            throw new InvalidOperationException(
                $"Release-core suite '{DurableMasterWriteScenarioSuiteKey}' row " +
                $"'{result.Name}' produced elapsed time {result.ElapsedMs:F3} ms; expected " +
                $"between {settings.MinimumMeasuredDuration.TotalMilliseconds:F0} and " +
                $"{settings.MaximumMeasuredDuration.TotalMilliseconds:F0} ms.");
        }

        (DateTimeOffset beginUtc, DateTimeOffset endUtc) =
            settings.ParseAndValidateExtraInfo(result.ExtraInfo);
        double declaredElapsedMilliseconds = (endUtc - beginUtc).TotalMilliseconds;
        if (Math.Abs(declaredElapsedMilliseconds - result.ElapsedMs) > 1.0)
        {
            throw new InvalidOperationException(
                $"Release-core suite '{DurableMasterWriteScenarioSuiteKey}' row " +
                $"'{result.Name}' elapsed time does not match its UTC measurement interval.");
        }
    }

    private static void ValidatePositiveReleaseCoreMetric(
        string suiteName,
        string rowName,
        string metricName,
        double value,
        string csvFormat)
    {
        string emittedValue = value.ToString(csvFormat, CultureInfo.InvariantCulture);
        if (double.TryParse(
                emittedValue,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out double parsedValue) &&
            double.IsFinite(parsedValue) &&
            parsedValue > 0)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Release-core suite '{suiteName}' row '{rowName}' produced invalid " +
            $"{metricName} '{value}' (CSV value '{emittedValue}'); a positive finite value " +
            "is required before CSV emission.");
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

            if (args[i].Equals("--repro", StringComparison.OrdinalIgnoreCase) ||
                args[i].Equals("--warmup-single-sample", StringComparison.OrdinalIgnoreCase))
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

    internal static void ValidateWarmupSingleSampleOption(
        string primaryMode,
        int repeatCount,
        bool warmupSingleSample)
    {
        if (!warmupSingleSample)
            return;

        if (repeatCount != 1)
        {
            throw new ArgumentException(
                "--warmup-single-sample is valid only with --repeat 1.");
        }

        string suiteKey = primaryMode.StartsWith("--", StringComparison.Ordinal)
            ? primaryMode[2..]
            : primaryMode;
        bool isHybridStorageScenario = suiteKey.Equals(
            "hybrid-storage-mode-scenario",
            StringComparison.OrdinalIgnoreCase);
        if (!isHybridStorageScenario &&
            !s_releaseEvidenceSuiteKeys.Contains(suiteKey, StringComparer.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "--warmup-single-sample is supported only by the direct release-evidence " +
                "suite modes and --hybrid-storage-mode-scenario: " +
                string.Join(", ", s_releaseEvidenceSuiteKeys.Select(static key => $"--{key}")) +
                ".");
        }
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

    private static int ParsePositiveInt(string value, string option)
    {
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int parsed) || parsed <= 0)
            throw new ArgumentException($"Invalid {option} value. Use a positive integer.");

        return parsed;
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

            if (args[i].Equals("--repro", StringComparison.OrdinalIgnoreCase) ||
                args[i].Equals("--warmup-single-sample", StringComparison.OrdinalIgnoreCase))
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
        Console.WriteLine("  dotnet run -- --fts-hot-token-smoke --postings 20000  Time one hot-token FTS build/query/update/delete smoke run");
        Console.WriteLine("  dotnet run -- --macro              Run macro-benchmarks (sustained workloads)");
        Console.WriteLine("  dotnet run -- --macro-batch-memory Run in-memory rotating batch throughput benchmark");
        Console.WriteLine("  dotnet run -- --write-diagnostics  Run focused pager/WAL durable-write diagnostics");
        Console.WriteLine("  dotnet run -- --durable-sql-batching  Run focused durable SQL batching benchmark");
        Console.WriteLine("  dotnet run -- --durable-sql-batching-scenario TxBatch10_LowLatency  Run one durable SQL batching scenario");
        Console.WriteLine("  dotnet run -- --migration-target-throughput  Measure staged migration target throughput and bounded batch memory");
        Console.WriteLine("  dotnet run -- --migration-target-scenario Rows100K_Batch1000_Text64  Run one migration target scenario");
        Console.WriteLine("  dotnet run -- --csv-retained-migration  Measure retained CSV inspect, package, replay, apply, resume, and checksum validation");
        Console.WriteLine("  dotnet run -- --csv-retained-migration-scenario Rows100K_Batch1000_Text64  Run one retained CSV scenario");
        Console.WriteLine("  dotnet run -- --write-transaction-diagnostics  Run focused explicit WriteTransaction diagnostics");
        Console.WriteLine("  dotnet run -- --commit-fan-in-diagnostics  Compare shared auto-commit vs explicit WriteTransaction fan-in");
        Console.WriteLine("  dotnet run -- --commit-fan-in-scenario ExplicitTx_DisjointUpdate_W8_Batch250us  Run one commit fan-in scenario");
        Console.WriteLine("  dotnet run -- --insert-fan-in-diagnostics  Compare insert-side fan-in across auto-commit and explicit WriteTransaction");
        Console.WriteLine("  dotnet run -- --insert-fan-in-scenario AutoCommit_ExplicitId_W8_Batch250us  Run one insert fan-in scenario");
        Console.WriteLine("  dotnet run -- --checkpoint-retention-diagnostics  Run focused background-checkpoint retention diagnostics");
        Console.WriteLine("  dotnet run -- --checkpoint-retention-scenario W8_Blocker3s_Batch250us  Run one checkpoint-retention scenario");
        Console.WriteLine("  dotnet run -- --optimizer-closeout  Run focused advanced optimizer close-out diagnostics");
        Console.WriteLine("  dotnet run -- --adaptive-reoptimization  Run focused opt-in adaptive join reoptimization diagnostics");
        Console.WriteLine("  dotnet run -- --async-io-closeout  Run focused async I/O batching close-out diagnostics");
        Console.WriteLine("  dotnet run -- --write-transaction-scenario UpdateDisjoint_W8_Rows1_Batch250us_Prealloc1MiB  Run one explicit WriteTransaction scenario");
        Console.WriteLine("  dotnet run -- --concurrent-write-diagnostics  Run focused multi-writer durable commit diagnostics");
        Console.WriteLine("  dotnet run -- --concurrent-write-scenario W8_Batch250us_Prealloc1MiB  Run one concurrent durable-write scenario");
        Console.WriteLine("  dotnet run -- --concurrent-sqlite-capi-compare  Run concurrent engine-vs-SQLite C-API auto-commit insert comparisons");
        Console.WriteLine("  dotnet run -- --concurrent-sqlite-capi-compare-scenario SQLite_W8  Run one concurrent engine-vs-SQLite C-API scenario");
        Console.WriteLine("  dotnet run -- --concurrent-adonet-compare  Run concurrent prepared ADO.NET insert comparisons for CSharpDB and SQLite");
        Console.WriteLine("  dotnet run -- --concurrent-adonet-compare-scenario SQLite_AdoNet_Disjoint_W8  Run one concurrent ADO.NET comparison scenario");
        Console.WriteLine("  dotnet run -- --direct-file-cache-transport  Run focused direct default-vs-tuned file-cache benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-storage-mode  Run focused storage-mode coverage plus the Plan 2 bulk insert durability/residency matrix");
        Console.WriteLine("  dotnet run -- --hybrid-storage-mode-scenario <exact-row-name>  Run one storage-mode row with qualification timing and sample floors");
        Console.WriteLine("  dotnet run -- --master-table  Run only the CSharpDB rows used by the README master comparison table");
        Console.WriteLine("  dotnet run -- --master-table-durable-writes  Run the ten durable write rows used by local release qualification");
        Console.WriteLine("  dotnet run -- --master-table-durable-write-scenario <exact-master-row-name>  Run one durable master-table write row with qualification timing and sample floors");
        Console.WriteLine("  dotnet run -- --master-table-hosted-stable  Run the eighteen read/in-memory rows used by hosted release qualification");
        Console.WriteLine("  dotnet run -- --sqlite-compare  Run local SQLite WAL+FULL apples-to-apples SQL comparison rows");
        Console.WriteLine("  dotnet run -- --strict-insert-compare  Run strict ADO.NET raw-vs-prepared insert comparison for CSharpDB and SQLite");
        Console.WriteLine("  dotnet run -- --native-aot-insert-compare  Run raw+prepared insert comparison for CSharpDB ADO.NET, CSharpDB NativeAOT FFI, and SQLite");
        Console.WriteLine("  dotnet run -- --efcore-compare  Run steady-state EF Core insert comparisons with one open connection per timed run");
        Console.WriteLine("  dotnet run -- --efcore-compare-hybrid-shared-connection  Run EF Core insert comparisons with short-lived DbContexts over one externally-owned open connection");
        Console.WriteLine("  dotnet run -- --efcore-compare-auto-open-close  Run EF Core insert comparisons with EF-managed auto open/close around SaveChanges");
        Console.WriteLine("  dotnet run -- --hybrid-cold-open  Run focused engine-cold open + first read benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-hot-set-read  Run focused post-open hot-set read benchmark including hybrid warm-set mode");
        Console.WriteLine("  dotnet run -- --hybrid-post-checkpoint  Run focused post-checkpoint hot reread benchmark");
        Console.WriteLine("  dotnet run -- --pr                 Run the fast PR guardrail subset from perf-thresholds-pr.json");
        Console.WriteLine("  dotnet run -- --release            Run the focused release guardrail subset from perf-thresholds.json");
        Console.WriteLine("  dotnet run -- --release-core --repeat 3 --repro  Run only the suites that feed published README tables");
        Console.WriteLine("  dotnet run -- --stress             Run stress & durability tests");
        Console.WriteLine("  dotnet run -- --scaling            Run scaling experiments");
        Console.WriteLine("  dotnet run -- --macro --stress --scaling --write-diagnostics --durable-sql-batching --write-transaction-diagnostics --commit-fan-in-diagnostics --insert-fan-in-diagnostics --checkpoint-retention-diagnostics --optimizer-closeout --adaptive-reoptimization --async-io-closeout --concurrent-write-diagnostics --concurrent-sqlite-capi-compare --direct-file-cache-transport --hybrid-storage-mode --master-table --sqlite-compare --strict-insert-compare --native-aot-insert-compare --efcore-compare --efcore-compare-hybrid-shared-connection --efcore-compare-auto-open-close --hybrid-cold-open --hybrid-hot-set-read --hybrid-post-checkpoint   Run non-micro suites in one invocation");
        Console.WriteLine("  dotnet run -- --macro --repeat 3   Repeat suite and emit median-of-N CSV");
        Console.WriteLine("  dotnet run -- --master-table --repeat 1 --warmup-single-sample --repro   Warm up without recording, then emit one release-core suite sample");
        Console.WriteLine("  dotnet run -- --master-table-durable-writes --repeat 1 --warmup-single-sample --repro   Warm up and record one local durable qualification sample");
        Console.WriteLine("  dotnet run -- --master-table-hosted-stable --repeat 1 --warmup-single-sample --repro   Warm up and record one hosted-stable qualification sample");
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
