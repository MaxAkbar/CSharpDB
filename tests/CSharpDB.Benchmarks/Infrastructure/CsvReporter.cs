using System.Text;

namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Writes benchmark results to CSV files and prints console summary tables.
/// </summary>
public static class CsvReporter
{
    /// <summary>
    /// Write benchmark results to a CSV file.
    /// </summary>
    public static void WriteResults(string outputPath, IReadOnlyList<BenchmarkResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Name,TotalOps,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo");

        foreach (var r in results)
        {
            sb.AppendLine(string.Join(",",
                Escape(r.Name),
                r.TotalOps,
                r.ElapsedMs.ToString("F2"),
                r.OpsPerSecond.ToString("F1"),
                r.P50Ms.ToString("F4"),
                r.P90Ms.ToString("F4"),
                r.P95Ms.ToString("F4"),
                r.P99Ms.ToString("F4"),
                r.P999Ms.ToString("F4"),
                r.MinMs.ToString("F4"),
                r.MaxMs.ToString("F4"),
                r.MeanMs.ToString("F4"),
                r.StdDevMs.ToString("F4"),
                Escape(r.ExtraInfo ?? "")));
        }

        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        File.WriteAllText(outputPath, sb.ToString());
    }

    /// <summary>
    /// Print a summary table to the console.
    /// </summary>
    public static void PrintSummaryTable(IReadOnlyList<BenchmarkResult> results)
    {
        if (results.Count == 0) return;

        const int nameWidth = 40;
        const int numWidth = 12;

        Console.WriteLine();
        var header = $"{"Benchmark",-nameWidth} {"ops/sec",numWidth} {"P50(ms)",numWidth} {"P95(ms)",numWidth} {"P99(ms)",numWidth} {"P99.9(ms)",numWidth}";
        Console.WriteLine(header);
        Console.WriteLine(new string('-', header.Length));

        foreach (var r in results)
        {
            var name = r.Name.Length > nameWidth ? r.Name[..nameWidth] : r.Name;
            Console.WriteLine(
                $"{name,-nameWidth} {r.OpsPerSecond,numWidth:N0} {r.P50Ms,numWidth:F3} {r.P95Ms,numWidth:F3} {r.P99Ms,numWidth:F3} {r.P999Ms,numWidth:F3}");
        }
        Console.WriteLine();
    }

    private static string Escape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }
}
