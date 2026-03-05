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

            case "--all":
                Console.WriteLine("=== Micro-Benchmarks (BenchmarkDotNet) ===");
                RunMicroBenchmarks(StripCustomArgs(RemoveFirstToken(args, "--all")));
                Console.WriteLine();
                Console.WriteLine("=== Macro-Benchmarks ===");
                EnsureReproConfigured();
                await RunSuiteWithRepeatsAsync("macro", RunMacroBenchmarksOnceAsync, repeatCount);
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

        return results;
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
        Console.WriteLine("  dotnet run -- --stress             Run stress & durability tests");
        Console.WriteLine("  dotnet run -- --scaling            Run scaling experiments");
        Console.WriteLine("  dotnet run -- --macro --stress --scaling   Run non-micro suites in one invocation");
        Console.WriteLine("  dotnet run -- --macro --repeat 3   Repeat suite and emit median-of-N CSV");
        Console.WriteLine("  dotnet run -- --scaling --repro    Run non-micro suite with high-priority + pinned CPU affinity");
        Console.WriteLine("  dotnet run -- --scaling --repro --cpu-threads 8   Pin to first 8 logical CPUs");
        Console.WriteLine("  --repro applies to macro/stress/scaling only (micro remains BenchmarkDotNet-managed)");
        Console.WriteLine("  dotnet run -- --all                Run everything in sequence");
    }
}
