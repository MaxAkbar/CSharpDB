using BenchmarkDotNet.Running;
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

        var mode = args[0].ToLowerInvariant();
        switch (mode)
        {
            case "--micro":
                RunMicroBenchmarks(args.Skip(1).ToArray());
                break;

            case "--macro":
                await RunMacroBenchmarksAsync();
                break;

            case "--stress":
                await RunStressTestsAsync();
                break;

            case "--scaling":
                await RunScalingExperimentsAsync();
                break;

            case "--all":
                Console.WriteLine("=== Micro-Benchmarks (BenchmarkDotNet) ===");
                RunMicroBenchmarks(args.Skip(1).ToArray());
                Console.WriteLine();
                Console.WriteLine("=== Macro-Benchmarks ===");
                await RunMacroBenchmarksAsync();
                Console.WriteLine();
                Console.WriteLine("=== Stress Tests ===");
                await RunStressTestsAsync();
                Console.WriteLine();
                Console.WriteLine("=== Scaling Experiments ===");
                await RunScalingExperimentsAsync();
                break;

            default:
                Console.WriteLine($"Unknown mode: {mode}");
                PrintHelp();
                break;
        }
    }

    private static void RunMicroBenchmarks(string[] args)
    {
        var switcher = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly);
        switcher.Run(args);
    }

    private static async Task RunMacroBenchmarksAsync()
    {
        var results = new List<Infrastructure.BenchmarkResult>();

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

        var outputDir = Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        var outputPath = Path.Combine(outputDir, $"macro-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv");
        Infrastructure.CsvReporter.WriteResults(outputPath, results);
        Console.WriteLine($"\nResults written to {outputPath}");

        Infrastructure.CsvReporter.PrintSummaryTable(results);
    }

    private static async Task RunStressTestsAsync()
    {
        var results = new List<Infrastructure.BenchmarkResult>();

        Console.WriteLine("--- Crash Recovery Benchmark ---");
        results.AddRange(await CrashRecoveryBenchmark.RunAsync());

        Console.WriteLine("--- WAL Growth Benchmark ---");
        results.AddRange(await WalGrowthBenchmark.RunAsync());

        var outputDir = Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        var outputPath = Path.Combine(outputDir, $"stress-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv");
        Infrastructure.CsvReporter.WriteResults(outputPath, results);
        Console.WriteLine($"\nResults written to {outputPath}");

        Infrastructure.CsvReporter.PrintSummaryTable(results);
    }

    private static async Task RunScalingExperimentsAsync()
    {
        var results = new List<Infrastructure.BenchmarkResult>();

        Console.WriteLine("--- Row Count Scaling Benchmark ---");
        results.AddRange(await RowCountScalingBenchmark.RunAsync());

        Console.WriteLine("--- B+Tree Depth Benchmark ---");
        results.AddRange(await BTreeDepthBenchmark.RunAsync());

        var outputDir = Path.Combine(AppContext.BaseDirectory, "results");
        Directory.CreateDirectory(outputDir);
        var outputPath = Path.Combine(outputDir, $"scaling-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv");
        Infrastructure.CsvReporter.WriteResults(outputPath, results);
        Console.WriteLine($"\nResults written to {outputPath}");

        Infrastructure.CsvReporter.PrintSummaryTable(results);
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
        Console.WriteLine("  dotnet run -- --all                Run everything in sequence");
    }
}
