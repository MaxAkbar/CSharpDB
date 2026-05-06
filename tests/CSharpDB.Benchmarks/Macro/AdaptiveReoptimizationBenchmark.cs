using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused diagnostics for opt-in adaptive join re-optimization.
/// Rows report internal adaptive counters so the benchmark is useful even when
/// a workload is intentionally stable and should not switch plans.
/// </summary>
public static class AdaptiveReoptimizationBenchmark
{
    private const int Iterations = 80;

    private static readonly AdaptiveScenario[] s_scenarios =
    [
        new(
            "DisabledStableJoin",
            false,
            AdaptiveThresholdMode.Default,
            "SELECT COUNT(*) FROM adaptive_orders o JOIN adaptive_customers c ON c.id = o.customer_id WHERE o.status = 2",
            "default-disabled baseline for a stable selective join"),
        new(
            "EnabledStableNoSwitch",
            true,
            AdaptiveThresholdMode.High,
            "SELECT COUNT(*) FROM adaptive_orders o JOIN adaptive_customers c ON c.id = o.customer_id WHERE o.status = 2",
            "enabled wrapper overhead when thresholds avoid adaptation"),
        new(
            "StaleStatJoinFanout",
            true,
            AdaptiveThresholdMode.Low,
            "SELECT COUNT(*) FROM adaptive_orders o JOIN adaptive_customers c ON c.id = o.customer_id WHERE o.amount > 0",
            "large post-ANALYZE fan-out shape that exposes stale range-stat divergence when the plan is lookup-driven"),
        new(
            "ParameterSensitiveSmall",
            true,
            AdaptiveThresholdMode.Low,
            "SELECT COUNT(*) FROM adaptive_orders o JOIN adaptive_customers c ON c.id = o.customer_id WHERE o.status = 3",
            "small selective value from the same cached query family"),
        new(
            "WrongHashBuildSide",
            true,
            AdaptiveThresholdMode.Low,
            "SELECT COUNT(*) FROM adaptive_hash_left l JOIN adaptive_hash_right r ON r.code = l.code WHERE l.keep = 1 AND r.flag > 0",
            "hash-build-side diagnostics for a large observed build candidate"),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        string seedPath = await CreateSeedDatabaseAsync();
        try
        {
            var results = new List<BenchmarkResult>(s_scenarios.Length + 2);
            foreach (AdaptiveScenario scenario in s_scenarios)
                results.Add(await RunScenarioAsync(seedPath, scenario));

            results.Add(await RunSyntheticIndexSwitchAsync());
            results.Add(await RunSyntheticHashBuildSwitchAsync());

            return results;
        }
        finally
        {
            DeleteDatabaseFiles(seedPath);
        }
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(string seedPath, AdaptiveScenario scenario)
    {
        string databasePath = CloneDatabaseFiles(seedPath, "adaptive-reoptimization");
        try
        {
            DatabaseOptions options = BenchmarkDurability.Apply();
            if (scenario.Enabled)
                options = options.EnableAdaptiveQueryReoptimization(builder => ConfigureThresholds(builder, scenario.ThresholdMode));

            await using var db = await Database.OpenAsync(databasePath, options);
            db.ResetAdaptiveQueryReoptimizationDiagnostics();

            for (int i = 0; i < 6; i++)
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
            var diagnostics = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();

            var result = new BenchmarkResult
            {
                Name = $"AdaptiveReoptimization_{scenario.Name}",
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
                ExtraInfo =
                    $"enabled={scenario.Enabled}, mode={scenario.ThresholdMode}, checksum={checksum}, " +
                    $"eligible={diagnostics.EligibleQueryCount}, attempts={diagnostics.AttemptCount}, " +
                    $"switches={diagnostics.SuccessfulSwitchCount}, rejected={diagnostics.RejectedSwitchCount}, " +
                    $"divergence={diagnostics.DivergenceEventCount}, bufferedRows={diagnostics.BufferedRowCount}, " +
                    $"maxBufferedFallback={diagnostics.MaxBufferedFallbackCount}, unsupportedFallback={diagnostics.UnsupportedFallbackCount}, " +
                    $"limitFallback={diagnostics.ReoptimizationLimitFallbackCount}, focus={scenario.Focus}",
            };

            Console.WriteLine(
                $"  {result.Name}: {result.OpsPerSecond:N0} queries/sec, P50={result.P50Ms:F4}ms, P99={result.P99Ms:F4}ms");
            Console.WriteLine($"    {result.ExtraInfo}");
            return result;
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    private static async Task<BenchmarkResult> RunSyntheticIndexSwitchAsync()
    {
        var rows = CreateSingleColumnRows(4_096);
        ColumnDefinition[] schema = [new() { Name = "value", Type = DbType.Integer }];
        var counters = new AdaptiveRuntimeCounters();

        for (int i = 0; i < 6; i++)
            _ = await ExecuteSyntheticIndexSwitchOnceAsync(rows, schema, counters);

        var histogram = new LatencyHistogram();
        long checksum = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            var querySw = Stopwatch.StartNew();
            checksum += await ExecuteSyntheticIndexSwitchOnceAsync(rows, schema, counters);
            querySw.Stop();
            histogram.Record(querySw.Elapsed.TotalMilliseconds);
        }

        sw.Stop();
        var result = CreateSyntheticResult(
            "AdaptiveReoptimization_SyntheticIndexSwitch",
            sw.Elapsed.TotalMilliseconds,
            histogram,
            checksum,
            counters,
            "operator-level index-to-hash switch validation with a deliberately low outer estimate");
        PrintResult(result);
        return result;
    }

    private static async Task<BenchmarkResult> RunSyntheticHashBuildSwitchAsync()
    {
        ColumnDefinition[] leftSchema =
        [
            new() { Name = "id", Type = DbType.Integer },
            new() { Name = "code", Type = DbType.Integer },
        ];
        ColumnDefinition[] rightSchema =
        [
            new() { Name = "code", Type = DbType.Integer },
            new() { Name = "payload", Type = DbType.Integer },
        ];
        var leftRows = Enumerable.Range(1, 64)
            .Select(i => new[] { DbValue.FromInteger(i), DbValue.FromInteger(i) })
            .ToList();
        var rightRows = Enumerable.Range(1, 4_096)
            .Select(i => new[] { DbValue.FromInteger(((i - 1) % 64) + 1), DbValue.FromInteger(i * 10) })
            .ToList();
        var counters = new AdaptiveRuntimeCounters();

        for (int i = 0; i < 6; i++)
            _ = await ExecuteSyntheticHashSwitchOnceAsync(leftRows, rightRows, leftSchema, rightSchema, counters);

        var histogram = new LatencyHistogram();
        long checksum = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            var querySw = Stopwatch.StartNew();
            checksum += await ExecuteSyntheticHashSwitchOnceAsync(leftRows, rightRows, leftSchema, rightSchema, counters);
            querySw.Stop();
            histogram.Record(querySw.Elapsed.TotalMilliseconds);
        }

        sw.Stop();
        var result = CreateSyntheticResult(
            "AdaptiveReoptimization_SyntheticHashBuildSwitch",
            sw.Elapsed.TotalMilliseconds,
            histogram,
            checksum,
            counters,
            "operator-level hash build-side switch validation with a deliberately low build estimate");
        PrintResult(result);
        return result;
    }

    private static async Task<long> ExecuteSyntheticIndexSwitchOnceAsync(
        List<DbValue[]> rows,
        ColumnDefinition[] schema,
        AdaptiveRuntimeCounters counters)
    {
        var lease = CreateSyntheticLease();
        var op = new AdaptiveIndexNestedLoopJoinOperator(
            new MaterializedOperator(rows, schema),
            new MaterializedOperator([], schema),
            schema,
            source => source,
            source => source,
            lease,
            counters.Diagnostics,
            estimatedOuterRows: 64,
            estimatedRowCount: rows.Count);

        long count = 0;
        await op.OpenAsync();
        try
        {
            while (await op.MoveNextAsync())
                count++;
        }
        finally
        {
            await op.DisposeAsync();
        }

        return count;
    }

    private static async Task<long> ExecuteSyntheticHashSwitchOnceAsync(
        List<DbValue[]> leftRows,
        List<DbValue[]> rightRows,
        ColumnDefinition[] leftSchema,
        ColumnDefinition[] rightSchema,
        AdaptiveRuntimeCounters counters)
    {
        var compositeSchema = new TableSchema
        {
            TableName = "synthetic_hash_join",
            Columns = leftSchema.Concat(rightSchema).ToArray(),
        };
        var lease = CreateSyntheticLease();
        var op = new AdaptiveHashJoinOperator(
            new MaterializedOperator(leftRows, leftSchema),
            new MaterializedOperator(rightRows, rightSchema),
            JoinType.Inner,
            residualCondition: null,
            compositeSchema,
            leftColCount: leftSchema.Length,
            rightColCount: rightSchema.Length,
            leftKeyIndices: [1],
            rightKeyIndices: [0],
            plannedBuildRightSide: true,
            estimatedLeftRows: leftRows.Count,
            estimatedRightRows: 64,
            estimatedRowCount: rightRows.Count,
            DbFunctionRegistry.Empty,
            lease,
            counters.Diagnostics);

        long count = 0;
        await op.OpenAsync();
        try
        {
            while (await op.MoveNextAsync())
                count++;
        }
        finally
        {
            await op.DisposeAsync();
        }

        return count;
    }

    private static BenchmarkResult CreateSyntheticResult(
        string name,
        double elapsedMs,
        LatencyHistogram histogram,
        long checksum,
        AdaptiveRuntimeCounters counters,
        string focus)
    {
        return new BenchmarkResult
        {
            Name = name,
            TotalOps = Iterations,
            ElapsedMs = elapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo =
                $"enabled=True, mode=Synthetic, checksum={checksum}, eligible=0, " +
                $"attempts={counters.AttemptCount}, switches={counters.SuccessfulSwitchCount}, " +
                $"rejected={counters.RejectedSwitchCount}, divergence={counters.DivergenceCount}, " +
                $"bufferedRows={counters.BufferedRowCount}, maxBufferedFallback={counters.MaxBufferedFallbackCount}, " +
                $"unsupportedFallback={counters.UnsupportedFallbackCount}, limitFallback={counters.ReoptimizationLimitFallbackCount}, " +
                $"focus={focus}",
        };
    }

    private static AdaptiveQueryExecutionLease CreateSyntheticLease()
        => new(new AdaptiveQueryReoptimizationOptions
        {
            Enabled = true,
            DivergenceFactor = 2,
            MinimumObservedRows = 64,
            MaxBufferedRows = 65_536,
            MaxReoptimizationsPerQuery = 1,
        });

    private static AdaptiveQueryReoptimizationOptionsBuilder ConfigureThresholds(
        AdaptiveQueryReoptimizationOptionsBuilder builder,
        AdaptiveThresholdMode mode)
    {
        return mode switch
        {
            AdaptiveThresholdMode.Low => builder
                .WithDivergenceFactor(2)
                .WithMinimumObservedRows(32)
                .WithMaxBufferedRows(65_536)
                .WithMaxReoptimizationsPerQuery(1),
            AdaptiveThresholdMode.High => builder
                .WithDivergenceFactor(128)
                .WithMinimumObservedRows(65_536)
                .WithMaxBufferedRows(65_536)
                .WithMaxReoptimizationsPerQuery(1),
            _ => builder,
        };
    }

    private static async Task<long> ExecuteScalarCountAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql);
        if (!await result.MoveNextAsync())
            throw new InvalidOperationException($"Query did not return a row: {sql}");

        return result.Current[0].AsInteger;
    }

    private static async Task<string> CreateSeedDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"adaptive_reoptimization_seed_{Guid.NewGuid():N}.db");
        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());

        await db.ExecuteAsync("CREATE TABLE adaptive_customers (id INTEGER PRIMARY KEY, region INTEGER NOT NULL, segment INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE adaptive_regions (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE adaptive_orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, region INTEGER NOT NULL, status INTEGER NOT NULL, amount INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE adaptive_hash_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, keep INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE TABLE adaptive_hash_right (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, flag INTEGER NOT NULL, payload INTEGER NOT NULL)");
        await db.ExecuteAsync("CREATE INDEX idx_adaptive_orders_status ON adaptive_orders(status)");
        await db.ExecuteAsync("CREATE INDEX idx_adaptive_orders_customer ON adaptive_orders(customer_id)");
        await db.ExecuteAsync("CREATE INDEX idx_adaptive_orders_region ON adaptive_orders(region)");

        await db.BeginTransactionAsync();
        for (int region = 1; region <= 16; region++)
            await db.ExecuteAsync($"INSERT INTO adaptive_regions VALUES ({region}, 'Region {region}')");

        for (int customer = 1; customer <= 2_000; customer++)
        {
            int region = ((customer - 1) % 16) + 1;
            int segment = customer <= 64 ? 1 : 2;
            await db.ExecuteAsync($"INSERT INTO adaptive_customers VALUES ({customer}, {region}, {segment})");
        }

        int orderId = 1;
        for (; orderId <= 256; orderId++)
        {
            int customerId = ((orderId - 1) % 2_000) + 1;
            int region = ((customerId - 1) % 16) + 1;
            int status = orderId <= 32 ? 1 : 3;
            await db.ExecuteAsync($"INSERT INTO adaptive_orders VALUES ({orderId}, {customerId}, {region}, {status}, {orderId * 7})");
        }

        for (int id = 1; id <= 64; id++)
            await db.ExecuteAsync($"INSERT INTO adaptive_hash_left VALUES ({id}, {id}, 1)");

        for (int id = 1; id <= 16; id++)
            await db.ExecuteAsync($"INSERT INTO adaptive_hash_right VALUES ({id}, {id}, 1, {id * 11})");

        await db.CommitAsync();

        await db.ExecuteAsync("ANALYZE adaptive_customers");
        await db.ExecuteAsync("ANALYZE adaptive_regions");
        await db.ExecuteAsync("ANALYZE adaptive_orders");
        await db.ExecuteAsync("ANALYZE adaptive_hash_left");
        await db.ExecuteAsync("ANALYZE adaptive_hash_right");

        await db.BeginTransactionAsync();
        for (; orderId <= 24_000; orderId++)
        {
            int customerId = ((orderId - 1) % 2_000) + 1;
            int region = ((customerId - 1) % 16) + 1;
            int status = orderId <= 20_000 ? 1 : 2;
            await db.ExecuteAsync($"INSERT INTO adaptive_orders VALUES ({orderId}, {customerId}, {region}, {status}, {orderId * 7})");
        }

        for (int id = 17; id <= 24_000; id++)
        {
            int code = ((id - 1) % 64) + 1;
            await db.ExecuteAsync($"INSERT INTO adaptive_hash_right VALUES ({id}, {code}, 1, {id * 11})");
        }

        await db.CommitAsync();
        await db.CheckpointAsync();

        return filePath;
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

    private static List<DbValue[]> CreateSingleColumnRows(int count)
    {
        var rows = new List<DbValue[]>(count);
        for (int i = 1; i <= count; i++)
            rows.Add([DbValue.FromInteger(i)]);

        return rows;
    }

    private static void PrintResult(BenchmarkResult result)
    {
        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F4}ms, P99={result.P99Ms:F4}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
    }

    private sealed record AdaptiveScenario(
        string Name,
        bool Enabled,
        AdaptiveThresholdMode ThresholdMode,
        string Sql,
        string Focus);

    private enum AdaptiveThresholdMode
    {
        Default,
        Low,
        High,
    }

    private sealed class AdaptiveRuntimeCounters
    {
        public long AttemptCount { get; private set; }
        public long SuccessfulSwitchCount { get; private set; }
        public long RejectedSwitchCount { get; private set; }
        public long DivergenceCount { get; private set; }
        public long BufferedRowCount { get; private set; }
        public long MaxBufferedFallbackCount { get; private set; }
        public long ReoptimizationLimitFallbackCount { get; private set; }
        public long UnsupportedFallbackCount { get; private set; }

        public AdaptiveQueryReoptimizationRuntimeDiagnostics Diagnostics { get; }

        public AdaptiveRuntimeCounters()
        {
            Diagnostics = new AdaptiveQueryReoptimizationRuntimeDiagnostics(
                () => AttemptCount++,
                () => SuccessfulSwitchCount++,
                RecordRejectedSwitch,
                () => DivergenceCount++,
                count => BufferedRowCount += count);
        }

        private void RecordRejectedSwitch(AdaptiveQueryReoptimizationFallbackReason reason)
        {
            RejectedSwitchCount++;
            switch (reason)
            {
                case AdaptiveQueryReoptimizationFallbackReason.MaxBufferedRows:
                    MaxBufferedFallbackCount++;
                    break;
                case AdaptiveQueryReoptimizationFallbackReason.ReoptimizationLimit:
                    ReoptimizationLimitFallbackCount++;
                    break;
                case AdaptiveQueryReoptimizationFallbackReason.Unsupported:
                    UnsupportedFallbackCount++;
                    break;
            }
        }
    }
}
