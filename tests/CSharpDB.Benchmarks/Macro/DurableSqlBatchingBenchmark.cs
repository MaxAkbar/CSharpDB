using System.Diagnostics;
using System.Text;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused file-backed durable single-writer ingest benchmark that keeps storage semantics fixed
/// while comparing the embedded bulk APIs, batch-size effects, row-width effects, secondary-index
/// maintenance, and primary-key locality.
/// </summary>
public static class DurableSqlBatchingBenchmark
{
    private static readonly RowProfile s_baselineRow = new("Baseline", "durable_batch", "Alpha");
    private static readonly RowProfile s_mediumRow = new("Medium", new string('m', 64), "CategoryMedium");
    private static readonly RowProfile s_wideRow = new("Wide", new string('w', 512), "CategoryWide");

    private static readonly IndexLayout s_pkOnlyIndexes = new("PkOnly", []);
    private static readonly IndexLayout s_secondary1Indexes = new(
        "Idx1",
        ["CREATE INDEX idx_bench_value ON bench(value)"]);
    private static readonly IndexLayout s_secondary2Indexes = new(
        "Idx2",
        [
            "CREATE INDEX idx_bench_value ON bench(value)",
            "CREATE INDEX idx_bench_category ON bench(category)",
        ]);
    private static readonly IndexLayout s_compositeCategoryValueIndex = new(
        "IdxCompositeCategoryValue",
        ["CREATE INDEX idx_bench_category_value ON bench(category, value)"]);
    private static readonly IndexLayout s_secondary4Indexes = new(
        "Idx4",
        [
            "CREATE INDEX idx_bench_value ON bench(value)",
            "CREATE INDEX idx_bench_category ON bench(category)",
            "CREATE INDEX idx_bench_text_col ON bench(text_col)",
            "CREATE INDEX idx_bench_category_value ON bench(category, value)",
        ]);

    private static readonly SqlBatchScenario[] s_scenarios = CreateScenarios();

    private static int _nextSequence;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (var scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));

        return results;
    }

    public static Task<BenchmarkResult> RunNamedScenarioAsync(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);

        SqlBatchScenario? scenario = s_scenarios.FirstOrDefault(
            scenario => scenario.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown durable SQL batching scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static SqlBatchScenario[] CreateScenarios()
    {
        return
        [
            // Historical controls retained for existing guardrails and prior published notes.
            new(
                "AutoCommitSingle_WriteOptimized",
                OperationPath.AutoCommitSingleSql,
                RowsPerCommit: 1,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "AutoCommitSingle_WriteOptimized_Analyzed",
                OperationPath.AutoCommitSingleSql,
                RowsPerCommit: 1,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic,
                AnalyzeBeforeRun: true),
            new(
                "AutoCommitSingle_LowLatency_Analyzed",
                OperationPath.AutoCommitSingleSql,
                RowsPerCommit: 1,
                Preset: StoragePreset.LowLatency,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic,
                AnalyzeBeforeRun: true),
            new(
                "TxBatch10_LowLatency",
                OperationPath.InsertBatch,
                RowsPerCommit: 10,
                Preset: StoragePreset.LowLatency,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "TxBatch100_LowLatency",
                OperationPath.InsertBatch,
                RowsPerCommit: 100,
                Preset: StoragePreset.LowLatency,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "TxBatch1000_LowLatency",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.LowLatency,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),

            // Plan 1: single-writer durable bulk-path matrix.
            new(
                "ApiPath_InsertBatch_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "ApiPath_MultiRowSql_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.MultiRowSql,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "ApiPath_RowSqlTx_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.RowSqlInExplicitTransaction,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),

            new(
                "BatchSweep_InsertBatch_B1_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "BatchSweep_InsertBatch_B10_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 10,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "BatchSweep_InsertBatch_B100_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 100,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 10000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),

            new(
                "RowWidth_InsertBatch_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "RowWidth_InsertBatch_B1000_Medium_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_mediumRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "RowWidth_InsertBatch_B1000_Wide_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_wideRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),

            new(
                "IndexSweep_InsertBatch_B1000_Baseline_Idx0_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "IndexSweep_InsertBatch_B1000_Baseline_Idx1_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_secondary1Indexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "IndexSweep_InsertBatch_B1000_Baseline_Idx2_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_secondary2Indexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "IndexSweep_InsertBatch_B1000_Baseline_IdxCompositeCategoryValue_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_compositeCategoryValueIndex,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "IndexSweep_InsertBatch_B1000_Baseline_Idx4_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_secondary4Indexes,
                KeyPattern: KeyPattern.Monotonic),

            new(
                "KeySweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Monotonic),
            new(
                "KeySweep_InsertBatch_B1000_Baseline_PkOnly_Random",
                OperationPath.InsertBatch,
                RowsPerCommit: 1000,
                Preset: StoragePreset.WriteOptimized,
                RowProfile: s_baselineRow,
                Indexes: s_pkOnlyIndexes,
                KeyPattern: KeyPattern.Random),
        ];
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(SqlBatchScenario scenario)
    {
        _nextSequence = 0;
        DatabaseOptions options = CreateOptions(scenario.Preset);

        await using var bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)",
            options);

        foreach (string createIndexSql in scenario.Indexes.CreateStatements)
            await ExecuteCommandAsync(bench.Db, createIndexSql);

        if (scenario.AnalyzeBeforeRun)
        {
            await ExecuteNonQueryAsync(
                bench.Db,
                $"INSERT INTO bench VALUES (0, 0, 'seed', '{s_baselineRow.CategoryValue}')");
            await ExecuteCommandAsync(bench.Db, "ANALYZE bench");
        }

        InsertBatch? batch = scenario.Path == OperationPath.InsertBatch
            ? bench.Db.PrepareInsertBatch("bench", initialCapacity: scenario.RowsPerCommit)
            : null;
        var rowBuffer = new DbValue[4];
        DbValue textValue = DbValue.FromText(scenario.RowProfile.TextValue);
        DbValue categoryValue = DbValue.FromText(scenario.RowProfile.CategoryValue);
        int sqlBuilderCapacity = EstimateSqlBuilderCapacity(scenario.RowsPerCommit, scenario.RowProfile);
        var multiRowSqlBuilder = scenario.Path == OperationPath.MultiRowSql
            ? new StringBuilder(sqlBuilderCapacity)
            : null;

        async Task OperationAsync()
        {
            switch (scenario.Path)
            {
                case OperationPath.AutoCommitSingleSql:
                {
                    int sequence = Interlocked.Increment(ref _nextSequence);
                    long id = MapId(sequence, scenario.KeyPattern);
                    string sql = BuildSingleInsertSql(id, sequence, scenario.RowProfile);
                    await ExecuteNonQueryAsync(bench.Db, sql);
                    return;
                }

                case OperationPath.InsertBatch:
                {
                    batch!.Clear();
                    await bench.Db.BeginTransactionAsync();
                    try
                    {
                        for (int i = 0; i < scenario.RowsPerCommit; i++)
                        {
                            int sequence = Interlocked.Increment(ref _nextSequence);
                            long id = MapId(sequence, scenario.KeyPattern);
                            PopulateRow(rowBuffer, id, sequence, textValue, categoryValue);
                            batch.AddRow(rowBuffer);
                        }

                        int rowsAffected = await batch.ExecuteAsync();
                        if (rowsAffected != scenario.RowsPerCommit)
                        {
                            throw new InvalidOperationException(
                                $"Expected {scenario.RowsPerCommit} inserted rows, observed {rowsAffected}.");
                        }

                        await bench.Db.CommitAsync();
                    }
                    catch
                    {
                        await RollbackQuietlyAsync(bench.Db);
                        throw;
                    }

                    return;
                }

                case OperationPath.MultiRowSql:
                {
                    await bench.Db.BeginTransactionAsync();
                    try
                    {
                        string sql = BuildMultiRowInsertSql(
                            multiRowSqlBuilder!,
                            scenario.RowsPerCommit,
                            scenario.KeyPattern,
                            scenario.RowProfile);
                        await ExecuteNonQueryAsync(bench.Db, sql, scenario.RowsPerCommit);
                        await bench.Db.CommitAsync();
                    }
                    catch
                    {
                        await RollbackQuietlyAsync(bench.Db);
                        throw;
                    }

                    return;
                }

                case OperationPath.RowSqlInExplicitTransaction:
                {
                    await bench.Db.BeginTransactionAsync();
                    try
                    {
                        for (int i = 0; i < scenario.RowsPerCommit; i++)
                        {
                            int sequence = Interlocked.Increment(ref _nextSequence);
                            long id = MapId(sequence, scenario.KeyPattern);
                            string sql = BuildSingleInsertSql(id, sequence, scenario.RowProfile);
                            await ExecuteNonQueryAsync(bench.Db, sql);
                        }

                        await bench.Db.CommitAsync();
                    }
                    catch
                    {
                        await RollbackQuietlyAsync(bench.Db);
                        throw;
                    }

                    return;
                }

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        TimeSpan warmupDuration = TimeSpan.FromSeconds(2);
        TimeSpan measuredDuration = TimeSpan.FromSeconds(10);

        var warmupEnd = DateTime.UtcNow + warmupDuration;
        while (DateTime.UtcNow < warmupEnd)
            await OperationAsync();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();

        var histogram = new LatencyHistogram();
        var totalSw = Stopwatch.StartNew();
        var measuredEnd = DateTime.UtcNow + measuredDuration;

        while (DateTime.UtcNow < measuredEnd)
        {
            var sw = Stopwatch.StartNew();
            await OperationAsync();
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        totalSw.Stop();

        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        var commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();
        double elapsedSeconds = totalSw.Elapsed.TotalSeconds <= 0 ? 0 : totalSw.Elapsed.TotalSeconds;
        double rowsPerSecond = elapsedSeconds <= 0
            ? 0
            : histogram.Count * (double)scenario.RowsPerCommit / elapsedSeconds;
        double commitsPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : (double)walDiagnostics.FlushedCommitCount / walDiagnostics.FlushCount;
        double kibPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : walDiagnostics.FlushedByteCount / (double)walDiagnostics.FlushCount / 1024.0;
        string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics);

        var result = new BenchmarkResult
        {
            Name = $"DurableSqlBatching_{scenario.Name}_10s",
            TotalOps = histogram.Count,
            ElapsedMs = totalSw.Elapsed.TotalMilliseconds,
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
                $"path={scenario.Path}, preset={scenario.Preset}, analyzed={scenario.AnalyzeBeforeRun}, rowsPerCommit={scenario.RowsPerCommit}, rowShape={scenario.RowProfile.Name}, secondaryIndexes={scenario.Indexes.SecondaryIndexCount}, keyPattern={scenario.KeyPattern}, rowsPerSec={rowsPerSecond:F1}, flushes={walDiagnostics.FlushCount}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, {commitSummary}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, rows/sec={rowsPerSecond:N0}, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static DatabaseOptions CreateOptions(StoragePreset preset)
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            if (preset == StoragePreset.LowLatency)
                builder.UseLowLatencyDurableWritePreset();
            else
                builder.UseWriteOptimizedPreset();
        });
    }

    private static async Task ExecuteNonQueryAsync(Database db, string sql, int expectedRowsAffected = 1)
    {
        int rowsAffected;
        await using (var result = await db.ExecuteAsync(sql))
        {
            rowsAffected = result.RowsAffected;
        }

        if (rowsAffected != expectedRowsAffected)
        {
            throw new InvalidOperationException(
                $"Expected {expectedRowsAffected} affected rows for SQL '{sql}', observed {rowsAffected}.");
        }
    }

    private static async Task ExecuteCommandAsync(Database db, string sql)
    {
        await using var _ = await db.ExecuteAsync(sql);
    }

    private static async Task RollbackQuietlyAsync(Database db)
    {
        try
        {
            await db.RollbackAsync();
        }
        catch
        {
            // Preserve the original benchmark failure.
        }
    }

    private static long MapId(int sequence, KeyPattern keyPattern)
    {
        return keyPattern switch
        {
            KeyPattern.Monotonic => sequence,
            KeyPattern.Random => unchecked((long)((uint)sequence * 2654435761u)) + 1,
            _ => throw new ArgumentOutOfRangeException(nameof(keyPattern), keyPattern, null),
        };
    }

    private static void PopulateRow(
        DbValue[] row,
        long id,
        int sequence,
        DbValue textValue,
        DbValue categoryValue)
    {
        row[0] = DbValue.FromInteger(id);
        row[1] = DbValue.FromInteger(sequence);
        row[2] = textValue;
        row[3] = categoryValue;
    }

    private static string BuildSingleInsertSql(long id, int sequence, RowProfile rowProfile)
    {
        return $"INSERT INTO bench VALUES ({id}, {sequence}, '{rowProfile.TextValue}', '{rowProfile.CategoryValue}')";
    }

    private static string BuildMultiRowInsertSql(
        StringBuilder builder,
        int rowsPerCommit,
        KeyPattern keyPattern,
        RowProfile rowProfile)
    {
        builder.Clear();
        builder.Append("INSERT INTO bench VALUES ");

        for (int i = 0; i < rowsPerCommit; i++)
        {
            if (i > 0)
                builder.Append(", ");

            int sequence = Interlocked.Increment(ref _nextSequence);
            long id = MapId(sequence, keyPattern);
            builder.Append('(');
            builder.Append(id);
            builder.Append(", ");
            builder.Append(sequence);
            builder.Append(", '");
            builder.Append(rowProfile.TextValue);
            builder.Append("', '");
            builder.Append(rowProfile.CategoryValue);
            builder.Append("')");
        }

        return builder.ToString();
    }

    private static int EstimateSqlBuilderCapacity(int rowsPerCommit, RowProfile rowProfile)
    {
        int perRowCapacity = 48 + rowProfile.TextValue.Length + rowProfile.CategoryValue.Length;
        return 32 + perRowCapacity * Math.Max(rowsPerCommit, 1);
    }

    private sealed record SqlBatchScenario(
        string Name,
        OperationPath Path,
        int RowsPerCommit,
        StoragePreset Preset,
        RowProfile RowProfile,
        IndexLayout Indexes,
        KeyPattern KeyPattern,
        bool AnalyzeBeforeRun = false);

    private sealed record RowProfile(string Name, string TextValue, string CategoryValue);

    private sealed record IndexLayout(string Name, string[] CreateStatements)
    {
        public int SecondaryIndexCount => CreateStatements.Length;
    }

    private enum OperationPath
    {
        AutoCommitSingleSql,
        InsertBatch,
        MultiRowSql,
        RowSqlInExplicitTransaction,
    }

    private enum StoragePreset
    {
        WriteOptimized,
        LowLatency,
    }

    private enum KeyPattern
    {
        Monotonic,
        Random,
    }
}
