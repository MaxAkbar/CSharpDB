using BenchmarkDotNet.Running;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using CSharpDB.Benchmarks.Stress;
using CSharpDB.Benchmarks.Scaling;

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
                RunMicroBenchmarks(StripCustomArgs(RemoveFirstToken(args, "--micro")));
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

            case "--concurrent-write-diagnostics":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("concurrent-write-diagnostics", RunConcurrentWriteDiagnosticsOnceAsync, repeatCount);
                return;

            case "--direct-file-cache-transport":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("direct-file-cache-transport", RunDirectFileCacheTransportOnceAsync, repeatCount);
                return;

            case "--hybrid-storage-mode":
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("hybrid-storage-mode", RunHybridStorageModeOnceAsync, repeatCount);
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

            case "--all":
                Console.WriteLine("=== Micro-Benchmarks (BenchmarkDotNet) ===");
                RunMicroBenchmarks(StripCustomArgs(RemoveFirstToken(args, "--all")));
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

    private static void RunMicroBenchmarks(string[] args)
    {
        var switcher = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly);
        switcher.Run(args);
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

    private static async Task<List<BenchmarkResult>> RunConcurrentWriteDiagnosticsOnceAsync()
    {
        Console.WriteLine("--- Concurrent Durable Write Benchmark ---");
        return await ConcurrentDurableWriteBenchmark.RunAsync();
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
        Console.WriteLine("  dotnet run -- --concurrent-write-diagnostics  Run focused multi-writer durable commit diagnostics");
        Console.WriteLine("  dotnet run -- --direct-file-cache-transport  Run focused direct default-vs-tuned file-cache benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-storage-mode  Run focused file-backed vs in-memory vs persistent-memory hybrid benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-cold-open  Run focused engine-cold open + first read benchmark");
        Console.WriteLine("  dotnet run -- --hybrid-hot-set-read  Run focused post-open hot-set read benchmark including hybrid warm-set mode");
        Console.WriteLine("  dotnet run -- --hybrid-post-checkpoint  Run focused post-checkpoint hot reread benchmark");
        Console.WriteLine("  dotnet run -- --stress             Run stress & durability tests");
        Console.WriteLine("  dotnet run -- --scaling            Run scaling experiments");
        Console.WriteLine("  dotnet run -- --macro --stress --scaling --write-diagnostics --concurrent-write-diagnostics --direct-file-cache-transport --hybrid-storage-mode --hybrid-cold-open --hybrid-hot-set-read --hybrid-post-checkpoint   Run non-micro suites in one invocation");
        Console.WriteLine("  dotnet run -- --macro --repeat 3   Repeat suite and emit median-of-N CSV");
        Console.WriteLine("  dotnet run -- --scaling --repro    Run non-micro suite with high-priority + pinned CPU affinity");
        Console.WriteLine("  dotnet run -- --scaling --repro --cpu-threads 8   Pin to first 8 logical CPUs");
        Console.WriteLine("  --repro applies to non-micro suites only (micro remains BenchmarkDotNet-managed)");
        Console.WriteLine("  dotnet run -- --all                Run everything in sequence");
    }
}
