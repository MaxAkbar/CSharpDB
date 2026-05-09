using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused close-out coverage for the phase-2 stats-guided optimizer work.
/// The rows intentionally compare the same query shape before and after ANALYZE.
/// </summary>
public static class OptimizerCloseOutBenchmark
{
    private const int Iterations = 120;

    private static readonly OptimizerScenario[] s_scenarios =
    [
        new(
            "HeavyHitterEquality",
            "SELECT COUNT(*) FROM optimizer_skew WHERE hot_code = 1",
            "heavy hitters suppress unselective non-unique lookup choices"),
        new(
            "HistogramColdRange",
            "SELECT COUNT(*) FROM optimizer_hist WHERE value BETWEEN 1000 AND 1099",
            "equi-depth histograms guide skewed numeric range estimates"),
        new(
            "CompositeCorrelation",
            "SELECT COUNT(*) FROM optimizer_corr WHERE region = 'East' AND city = 'EastCity'",
            "composite-prefix distinct counts preserve correlated equality selectivity"),
        new(
            "BoundedJoinReorder",
            "SELECT COUNT(*) FROM optimizer_big b JOIN optimizer_mid m ON b.code = m.code JOIN optimizer_small s ON m.code = s.code JOIN optimizer_tiny t ON s.code = t.code WHERE b.id BETWEEN 1 AND 25",
            "bounded DP reorders small inner-join chains with selective predicates"),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        string seedPath = await CreateSeedDatabaseAsync();
        string noAnalyzePath = CloneDatabaseFiles(seedPath, "optimizer-closeout-no-analyze");
        string analyzedPath = CloneDatabaseFiles(seedPath, "optimizer-closeout-analyzed");

        try
        {
            await using var noAnalyzeDb = await Database.OpenAsync(noAnalyzePath, BenchmarkDurability.Apply());
            await using var analyzedDb = await Database.OpenAsync(analyzedPath, BenchmarkDurability.Apply());
            await AnalyzeCloseOutTablesAsync(analyzedDb);

            var results = new List<BenchmarkResult>(s_scenarios.Length * 2);
            foreach (OptimizerScenario scenario in s_scenarios)
            {
                results.Add(await RunScenarioAsync(noAnalyzeDb, scenario, analyzed: false));
                results.Add(await RunScenarioAsync(analyzedDb, scenario, analyzed: true));
            }

            return results;
        }
        finally
        {
            DeleteDatabaseFiles(seedPath);
            DeleteDatabaseFiles(noAnalyzePath);
            DeleteDatabaseFiles(analyzedPath);
        }
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(
        Database db,
        OptimizerScenario scenario,
        bool analyzed)
    {
        for (int i = 0; i < 8; i++)
            _ = await ExecuteScalarCountAsync(db, scenario.Sql);

        var histogram = new LatencyHistogram();
        long checksum = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            var querySw = Stopwatch.StartNew();
            checksum += await ExecuteScalarCountAsync(db, scenario.Sql);
            querySw.Stop();
            histogram.Record(querySw.Elapsed.TotalMilliseconds);
        }

        sw.Stop();

        string phase = analyzed ? "Analyzed" : "NoAnalyze";
        var result = new BenchmarkResult
        {
            Name = $"OptimizerCloseOut_{phase}_{scenario.Name}",
            TotalOps = Iterations,
            ElapsedMs = sw.Elapsed.TotalMilliseconds,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo = $"analyzed={analyzed}, iterations={Iterations}, checksum={checksum}, focus={scenario.Focus}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} queries/sec, P50={result.P50Ms:F4}ms, P99={result.P99Ms:F4}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static async Task<long> ExecuteScalarCountAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql);
        if (!await result.MoveNextAsync())
            throw new InvalidOperationException($"Query did not return a row: {sql}");

        return result.Current[0].AsInteger;
    }

    private static async Task AnalyzeCloseOutTablesAsync(Database db)
    {
        string[] tableNames =
        [
            "optimizer_skew",
            "optimizer_hist",
            "optimizer_corr",
            "optimizer_big",
            "optimizer_mid",
            "optimizer_small",
            "optimizer_tiny",
        ];

        foreach (string tableName in tableNames)
            await db.ExecuteAsync($"ANALYZE {tableName}");
    }

    private static async Task<string> CreateSeedDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"optimizer_closeout_seed_{Guid.NewGuid():N}.db");
        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());

        await CreateSkewTableAsync(db);
        await CreateHistogramTableAsync(db);
        await CreateCorrelationTableAsync(db);
        await CreateJoinTablesAsync(db);
        await db.CheckpointAsync();

        return filePath;
    }

    private static async Task CreateSkewTableAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE optimizer_skew (id INTEGER PRIMARY KEY, hot_code INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_skew_hot_code ON optimizer_skew(hot_code)");

        await db.BeginTransactionAsync();
        for (int i = 1; i <= 9_000; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_skew VALUES ({i}, 1, {i * 3})");

        for (int i = 9_001; i <= 10_000; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_skew VALUES ({i}, {i}, {i * 3})");

        await db.CommitAsync();
    }

    private static async Task CreateHistogramTableAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE optimizer_hist (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_hist_value ON optimizer_hist(value)");

        await db.BeginTransactionAsync();
        int id = 1;
        for (int value = 1; value <= 10; value++)
        {
            for (int repeat = 0; repeat < 700; repeat++, id++)
                await db.ExecuteAsync($"INSERT INTO optimizer_hist VALUES ({id}, {value}, {id * 5})");
        }

        for (int value = 1000; value < 1100; value++, id++)
            await db.ExecuteAsync($"INSERT INTO optimizer_hist VALUES ({id}, {value}, {id * 5})");

        await db.CommitAsync();
    }

    private static async Task CreateCorrelationTableAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE optimizer_corr (id INTEGER PRIMARY KEY, region TEXT NOT NULL, city TEXT NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_corr_region_city ON optimizer_corr(region, city)");

        await db.BeginTransactionAsync();
        for (int i = 1; i <= 4_000; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_corr VALUES ({i}, 'East', 'EastCity', {i})");

        for (int i = 4_001; i <= 8_000; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_corr VALUES ({i}, 'West', 'WestCity', {i})");

        await db.CommitAsync();
    }

    private static async Task CreateJoinTablesAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE optimizer_big (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE optimizer_mid (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE optimizer_small (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE optimizer_tiny (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_big_code ON optimizer_big(code)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_mid_code ON optimizer_mid(code)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_small_code ON optimizer_small(code)");
        await db.ExecuteAsync("CREATE INDEX idx_optimizer_tiny_code ON optimizer_tiny(code)");

        await db.BeginTransactionAsync();
        for (int i = 1; i <= 5_000; i++)
        {
            int code = ((i - 1) % 200) + 1;
            await db.ExecuteAsync($"INSERT INTO optimizer_big VALUES ({i}, {code}, {i * 3})");
        }

        for (int i = 1; i <= 200; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_mid VALUES ({i}, {i}, {i * 5})");

        for (int i = 1; i <= 10; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_small VALUES ({i}, {i}, {i * 7})");

        for (int i = 1; i <= 2; i++)
            await db.ExecuteAsync($"INSERT INTO optimizer_tiny VALUES ({i}, {i}, {i * 11})");

        await db.CommitAsync();
    }

    private static string CloneDatabaseFiles(string sourceFilePath, string prefix)
    {
        string destinationFilePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
        File.Copy(sourceFilePath, destinationFilePath, overwrite: true);

        string sourceWalPath = sourceFilePath + ".wal";
        if (File.Exists(sourceWalPath))
            File.Copy(sourceWalPath, destinationFilePath + ".wal", overwrite: true);

        return destinationFilePath;
    }

    private static void DeleteDatabaseFiles(string? filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return;

        try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
        try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
    }

    private sealed record OptimizerScenario(string Name, string Sql, string Focus);
}
