using System.Globalization;
using System.Text;
using BenchmarkDotNet.Running;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Samples.CompressionSdk.Benchmarks;
using CSharpDB.Samples.CompressionSdk.Infrastructure;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Samples.CompressionSdk;

public static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintHelp();
            return;
        }

        string mode = args[0];
        switch (mode)
        {
            case "--demo":
                await RunDemoAsync();
                return;

            case "--e2e":
                await RunSuiteWithRepeatsAsync("compression-e2e", () => CompressionEndToEndBenchmark.RunAsync(), ParseRepeatCount(args));
                return;

            case "--e2e-quick":
                await RunSuiteWithRepeatsAsync("compression-e2e-quick", () => CompressionEndToEndBenchmark.RunAsync(quick: true), ParseRepeatCount(args));
                return;

            case "--micro":
                BenchmarkSwitcher
                    .FromTypes([typeof(CompressionCandidateBenchmarks)])
                    .Run(args[1..]);
                return;

            default:
                PrintHelp();
                return;
        }
    }

    private static async Task RunDemoAsync()
    {
        string root = Path.Combine(AppContext.BaseDirectory, "artifacts", "demo");
        Directory.CreateDirectory(root);
        string dbPath = Path.Combine(root, "compression-sdk-demo.db");
        if (File.Exists(dbPath))
            File.Delete(dbPath);
        if (File.Exists(dbPath + ".wal"))
            File.Delete(dbPath + ".wal");

        await using var db = await Database.OpenAsync(dbPath, CreateOptions());
        await ExecuteCommandAsync(db, "CREATE TABLE payloads (id INTEGER PRIMARY KEY, codec TEXT, original_bytes INTEGER, payload BLOB);");

        string document = CreateDemoDocument();
        CompressedPayload compressed = PayloadCompression.CompressText(document, CompressionCodec.GZip, minimumBytes: 1024);

        var batch = db.PrepareInsertBatch("payloads", initialCapacity: 1);
        batch.AddRow(
        [
            DbValue.FromInteger(1),
            DbValue.FromText(compressed.Codec.ToString()),
            DbValue.FromInteger(compressed.OriginalByteCount),
            DbValue.FromBlob(compressed.Bytes),
        ]);
        _ = await batch.ExecuteAsync();
        await db.CheckpointAsync();

        await using var result = await db.ExecuteAsync("SELECT codec, original_bytes, payload FROM payloads WHERE id = 1;");
        if (!await result.MoveNextAsync())
            throw new InvalidOperationException("Demo row was not found.");

        var stored = new CompressedPayload(
            Enum.Parse<CompressionCodec>(result.Current[0].AsText),
            result.Current[2].AsBlob,
            (int)result.Current[1].AsInteger);

        string roundTrip = PayloadCompression.DecompressText(stored);
        if (!StringComparer.Ordinal.Equals(document, roundTrip))
            throw new InvalidOperationException("Compressed payload did not round-trip.");

        Console.WriteLine($"Stored demo database: {dbPath}");
        Console.WriteLine($"Original bytes: {compressed.OriginalByteCount:N0}");
        Console.WriteLine($"Stored payload bytes: {compressed.Bytes.Length:N0}");
        Console.WriteLine($"Payload ratio: {compressed.Bytes.Length / (double)compressed.OriginalByteCount:P2}");
    }

    private static async Task RunSuiteWithRepeatsAsync(
        string suiteName,
        Func<Task<List<BenchmarkResult>>> runOnceAsync,
        int repeatCount)
    {
        if (repeatCount <= 1)
        {
            List<BenchmarkResult> results = await runOnceAsync();
            WriteResults(suiteName, results);
            return;
        }

        var runs = new List<List<BenchmarkResult>>(repeatCount);
        for (int run = 1; run <= repeatCount; run++)
        {
            Console.WriteLine($"=== {suiteName.ToUpperInvariant()} Run {run}/{repeatCount} ===");
            List<BenchmarkResult> results = await runOnceAsync();
            runs.Add(results);
            WriteResults($"{suiteName}-{Timestamp()}-run{run}", results, includeTimestamp: false);
            Console.WriteLine();
        }

        List<BenchmarkResult> median = MedianResults(runs);
        WriteResults($"{suiteName}-{Timestamp()}-median-of-{repeatCount}", median, includeTimestamp: false);
    }

    private static void WriteResults(string suiteName, List<BenchmarkResult> results, bool includeTimestamp = true)
    {
        string fileName = includeTimestamp
            ? $"{suiteName}-{Timestamp()}.csv"
            : $"{suiteName}.csv";
        string resultsDirectory = Path.Combine(AppContext.BaseDirectory, "artifacts", "results");
        Directory.CreateDirectory(resultsDirectory);
        string path = Path.Combine(resultsDirectory, fileName);

        WriteCsv(path, results);
        Console.WriteLine($"Results written to {path}");
        PrintSummary(results);
    }

    private static void PrintSummary(List<BenchmarkResult> results)
    {
        Console.WriteLine();
        Console.WriteLine($"{"Benchmark",-42} {"ops/sec",12} {"P50(ms)",12} {"P95(ms)",12} {"P99(ms)",12} {"P99.9(ms)",12}");
        Console.WriteLine(new string('-', 107));

        foreach (BenchmarkResult result in results)
        {
            Console.WriteLine(
                $"{Truncate(result.Name, 42),-42} " +
                $"{result.OpsPerSecond,12:N0} " +
                $"{result.P50Ms,12:F4} " +
                $"{result.P95Ms,12:F4} " +
                $"{result.P99Ms,12:F4} " +
                $"{result.P999Ms,12:F4}");
        }
    }

    private static void WriteCsv(string path, IReadOnlyList<BenchmarkResult> results)
    {
        using var writer = new StreamWriter(path, append: false, Encoding.UTF8);
        writer.WriteLine("Name,TotalOps,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo");
        foreach (BenchmarkResult result in results)
        {
            writer.WriteLine(string.Join(
                ',',
                Csv(result.Name),
                result.TotalOps.ToString(CultureInfo.InvariantCulture),
                result.ElapsedMs.ToString("F2", CultureInfo.InvariantCulture),
                result.OpsPerSecond.ToString("F1", CultureInfo.InvariantCulture),
                result.P50Ms.ToString("F4", CultureInfo.InvariantCulture),
                result.P90Ms.ToString("F4", CultureInfo.InvariantCulture),
                result.P95Ms.ToString("F4", CultureInfo.InvariantCulture),
                result.P99Ms.ToString("F4", CultureInfo.InvariantCulture),
                result.P999Ms.ToString("F4", CultureInfo.InvariantCulture),
                result.MinMs.ToString("F4", CultureInfo.InvariantCulture),
                result.MaxMs.ToString("F4", CultureInfo.InvariantCulture),
                result.MeanMs.ToString("F4", CultureInfo.InvariantCulture),
                result.StdDevMs.ToString("F4", CultureInfo.InvariantCulture),
                Csv(result.ExtraInfo ?? "")));
        }
    }

    private static List<BenchmarkResult> MedianResults(IReadOnlyList<List<BenchmarkResult>> runs)
    {
        var results = new List<BenchmarkResult>();
        foreach (string name in runs[0].Select(result => result.Name))
        {
            BenchmarkResult[] rows = runs
                .Select(run => run.Single(result => result.Name == name))
                .OrderBy(result => result.OpsPerSecond)
                .ToArray();
            results.Add(rows[rows.Length / 2]);
        }

        return results;
    }

    private static string Csv(string value)
        => "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static string Timestamp()
        => DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);

    private static int ParseRepeatCount(string[] args)
    {
        int index = Array.IndexOf(args, "--repeat");
        if (index < 0)
            return 1;

        if (index == args.Length - 1 || !int.TryParse(args[index + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
            throw new ArgumentException("--repeat requires an integer value.");

        return Math.Max(1, value);
    }

    private static string Truncate(string value, int width)
        => value.Length <= width ? value : value[..width];

    private static DatabaseOptions CreateOptions()
        => new DatabaseOptions().ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

    private static async Task ExecuteCommandAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql);
        _ = result.RowsAffected;
    }

    private static string CreateDemoDocument()
    {
        var builder = new StringBuilder(16 * 1024);
        builder.Append("{\"tenant\":\"northwind\",\"events\":[");
        for (int i = 0; builder.Length < 16 * 1024; i++)
        {
            if (i > 0)
                builder.Append(',');
            builder.Append("{\"id\":").Append(i)
                .Append(",\"kind\":\"inventory-adjustment\",\"message\":\"repeatable sample payload for application-level compression\"}");
        }

        builder.Append("]}");
        return builder.ToString();
    }

    private static void PrintHelp()
    {
        Console.WriteLine("CSharpDB compression SDK sample");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --demo");
        Console.WriteLine("  dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --e2e --repeat 3");
        Console.WriteLine("  dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --e2e-quick");
        Console.WriteLine("  dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --micro --filter *CompressionCandidateBenchmarks*");
    }
}
