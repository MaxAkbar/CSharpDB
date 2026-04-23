using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Compares CSharpDB concurrent auto-commit insert surfaces against direct SQLite
/// C-API prepared inserts under the same multi-writer shape.
/// </summary>
public static class ConcurrentSqliteCApiComparisonBenchmark
{
    private const int ExplicitIdWriterRangeSpan = 1_000_000;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(10);
    private static readonly byte[] s_textColUtf8 = "durable"u8.ToArray();
    private static readonly byte[] s_categoryUtf8 = "Alpha"u8.ToArray();

    private const int WalAutoCheckpointPages = 4096;
    private const int AutoCheckpointMaxPagesPerStep = 256;
    private const int SqliteBusyTimeoutMs = 1000;

    private static readonly ConcurrentComparisonScenario[] s_scenarios = CreateScenarios();

    private static ConcurrentComparisonScenario[] CreateScenarios()
    {
        var scenarios = new List<ConcurrentComparisonScenario>(capacity: 10);
        int[] writerCounts = [4, 8];
        TimeSpan[] batchWindows =
        [
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(0.25),
        ];

        foreach (int writerCount in writerCounts)
        {
            foreach (TimeSpan batchWindow in batchWindows)
            {
                string batchToken = FormatBatchWindowToken(batchWindow);
                scenarios.Add(new ConcurrentComparisonScenario(
                    $"CSharpDB_RawSql_DisjointConcurrent_W{writerCount}_{batchToken}",
                    ComparisonSurface.CSharpDbRawSql,
                    writerCount,
                    batchWindow,
                    InsertKeyPattern.DisjointWriterRange,
                    ImplicitInsertExecutionMode.ConcurrentWriteTransactions));
                scenarios.Add(new ConcurrentComparisonScenario(
                    $"CSharpDB_InsertBatch_DisjointConcurrent_W{writerCount}_{batchToken}",
                    ComparisonSurface.CSharpDbInsertBatch,
                    writerCount,
                    batchWindow,
                    InsertKeyPattern.DisjointWriterRange,
                    ImplicitInsertExecutionMode.ConcurrentWriteTransactions));
            }

            scenarios.Add(new ConcurrentComparisonScenario(
                $"SQLite_CApi_Disjoint_W{writerCount}",
                ComparisonSurface.SqliteCApi,
                writerCount,
                TimeSpan.Zero,
                InsertKeyPattern.DisjointWriterRange,
                ImplicitInsertExecutionMode.Serialized));
        }

        scenarios.Add(new ConcurrentComparisonScenario(
            "CSharpDB_RawSql_HotRightEdgeConcurrent_W8_Batch250us",
            ComparisonSurface.CSharpDbRawSql,
            WriterCount: 8,
            BatchWindow: TimeSpan.FromMilliseconds(0.25),
            KeyPattern: InsertKeyPattern.HotRightEdge,
            CSharpDbImplicitInsertExecutionMode: ImplicitInsertExecutionMode.ConcurrentWriteTransactions));
        scenarios.Add(new ConcurrentComparisonScenario(
            "CSharpDB_InsertBatch_HotRightEdgeConcurrent_W8_Batch250us",
            ComparisonSurface.CSharpDbInsertBatch,
            WriterCount: 8,
            BatchWindow: TimeSpan.FromMilliseconds(0.25),
            KeyPattern: InsertKeyPattern.HotRightEdge,
            CSharpDbImplicitInsertExecutionMode: ImplicitInsertExecutionMode.ConcurrentWriteTransactions));
        scenarios.Add(new ConcurrentComparisonScenario(
            "SQLite_CApi_HotRightEdge_W8",
            ComparisonSurface.SqliteCApi,
            WriterCount: 8,
            BatchWindow: TimeSpan.Zero,
            KeyPattern: InsertKeyPattern.HotRightEdge,
            CSharpDbImplicitInsertExecutionMode: ImplicitInsertExecutionMode.Serialized));

        return scenarios.ToArray();
    }

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

        ConcurrentComparisonScenario? scenario = s_scenarios.FirstOrDefault(
            scenario => scenario.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown concurrent SQLite C-API comparison scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static Task<BenchmarkResult> RunScenarioAsync(ConcurrentComparisonScenario scenario)
        => scenario.Surface switch
        {
            ComparisonSurface.CSharpDbRawSql => RunCSharpDbScenarioAsync(scenario),
            ComparisonSurface.CSharpDbInsertBatch => RunCSharpDbScenarioAsync(scenario),
            ComparisonSurface.SqliteCApi => RunSqliteScenarioAsync(scenario),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario.Surface, null),
        };

    private static async Task<BenchmarkResult> RunCSharpDbScenarioAsync(ConcurrentComparisonScenario scenario)
    {
        var idAllocator = new ExplicitIdAllocator(scenario.WriterCount, scenario.KeyPattern);
        var options = new DatabaseOptions
        {
            ImplicitInsertExecutionMode = scenario.CSharpDbImplicitInsertExecutionMode,
        }.ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(WalAutoCheckpointPages),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = AutoCheckpointMaxPagesPerStep,
            });
            builder.UseDurableGroupCommit(scenario.BatchWindow);
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(options: options);
        await bench.ReopenAsync();

        await RunCSharpDbPhaseAsync(bench.Db, scenario, WarmupDuration, recordLatencies: false, idAllocator);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();
        ConcurrentPhaseStats stats = await RunCSharpDbPhaseAsync(bench.Db, scenario, MeasuredDuration, recordLatencies: true, idAllocator);
        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

        BenchmarkResult result = CreateResult(
            $"ConcurrentCompare_{scenario.Name}_10s",
            stats,
            $"provider=CSharpDB.Engine, surface={DescribeCSharpDbSurface(scenario.Surface)}, implicitInsertMode={scenario.CSharpDbImplicitInsertExecutionMode}, keyPattern={scenario.KeyPattern}, durability=default-durable, writers={scenario.WriterCount}, checkpoint=FrameCount({WalAutoCheckpointPages})+Background({AutoCheckpointMaxPagesPerStep}), batchWindow={FormatBatchWindow(scenario.BatchWindow)}, successfulCommits={stats.SuccessfulCommits}, busy={stats.BusyCount}, fatalErrors={stats.FatalErrorCount}, flushes={walDiagnostics.FlushCount}, flushesPerSec={FormatRate(walDiagnostics.FlushCount, stats.ElapsedMs)}, commitsPerFlush={FormatCommitsPerFlush(walDiagnostics.FlushedCommitCount, walDiagnostics.FlushCount)}, KiBPerFlush={FormatKiBPerFlush(walDiagnostics.FlushedByteCount, walDiagnostics.FlushCount)}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, {CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics)}");

        PrintResult(result);
        return result;
    }

    private static async Task<BenchmarkResult> RunSqliteScenarioAsync(ConcurrentComparisonScenario scenario)
    {
        var idAllocator = new ExplicitIdAllocator(scenario.WriterCount, scenario.KeyPattern);
        using var context = SqliteComparisonContext.Create(scenario.Name);

        await RunSqlitePhaseAsync(context.FilePath, scenario, WarmupDuration, recordLatencies: false, idAllocator);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        ConcurrentPhaseStats stats = await RunSqlitePhaseAsync(context.FilePath, scenario, MeasuredDuration, recordLatencies: true, idAllocator);

        BenchmarkResult result = CreateResult(
            $"ConcurrentCompare_{scenario.Name}_10s",
            stats,
            $"provider=SQLite/{SqliteCApiDatabase.LibraryVersion}, surface=sqlite3_prepare_v2+sqlite3_step, keyPattern={scenario.KeyPattern}, writers={scenario.WriterCount}, journal_mode=wal, synchronous=full, wal_autocheckpoint={WalAutoCheckpointPages}, busyTimeoutMs={SqliteBusyTimeoutMs}, successfulCommits={stats.SuccessfulCommits}, busy={stats.BusyCount}, fatalErrors={stats.FatalErrorCount}");

        PrintResult(result);
        return result;
    }

    private static async Task<ConcurrentPhaseStats> RunCSharpDbPhaseAsync(
        Database db,
        ConcurrentComparisonScenario scenario,
        TimeSpan duration,
        bool recordLatencies,
        ExplicitIdAllocator idAllocator)
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerTasks = new Task<WriterStats>[scenario.WriterCount];
        long durationTicks = (long)(duration.TotalSeconds * Stopwatch.Frequency);

        for (int writerIndex = 0; writerIndex < scenario.WriterCount; writerIndex++)
        {
            int localWriterIndex = writerIndex;
            writerTasks[writerIndex] = Task.Run(async () =>
            {
                InsertBatch? batch = null;
                DbValue[]? row = null;
                InitializeBatchForWriter();

                var localLatencies = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int busyCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    int id = idAllocator.NextId(localWriterIndex);

                    var sw = Stopwatch.StartNew();
                    try
                    {
                        int rowsAffected;
                        if (scenario.Surface == ComparisonSurface.CSharpDbInsertBatch)
                        {
                            rowsAffected = await ExecuteInsertBatchOnceAsync(id).ConfigureAwait(false);
                        }
                        else
                        {
                            string sql =
                                $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {localWriterIndex}, 'durable', 'Alpha')";
                            await using var result = await db.ExecuteAsync(sql, CancellationToken.None).ConfigureAwait(false);
                            rowsAffected = result.RowsAffected;
                        }

                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");

                        sw.Stop();
                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbException ex) when (ex.Code == ErrorCode.Busy)
                    {
                        sw.Stop();
                        batch?.Clear();
                        busyCount++;
                    }
                    catch
                    {
                        batch?.Clear();
                        fatalErrorCount++;
                        throw;
                    }
                }

                return new WriterStats(localLatencies ?? [], successfulCommits, busyCount, fatalErrorCount);

                void InitializeBatchForWriter()
                {
                    if (scenario.Surface != ComparisonSurface.CSharpDbInsertBatch)
                        return;

                    batch = db.PrepareInsertBatch("bench", initialCapacity: 1);
                    row = new DbValue[4];
                    row[1] = DbValue.FromInteger(localWriterIndex);
                    row[2] = DbValue.FromText("durable");
                    row[3] = DbValue.FromText("Alpha");
                }

                async Task<int> ExecuteInsertBatchOnceAsync(int id)
                {
                    for (; ; )
                    {
                        try
                        {
                            row![0] = DbValue.FromInteger(id);
                            batch!.AddRow(row);
                            return await batch.ExecuteAsync().ConfigureAwait(false);
                        }
                        catch (InvalidOperationException ex) when (IsBatchSchemaInvalidation(ex))
                        {
                            InitializeBatchForWriter();
                        }
                    }
                }
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks).ConfigureAwait(false);
        totalSw.Stop();

        return CombineWriterStats(completed, totalSw.Elapsed.TotalMilliseconds);
    }

    private static async Task<ConcurrentPhaseStats> RunSqlitePhaseAsync(
        string filePath,
        ConcurrentComparisonScenario scenario,
        TimeSpan duration,
        bool recordLatencies,
        ExplicitIdAllocator idAllocator)
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerTasks = new Task<WriterStats>[scenario.WriterCount];
        long durationTicks = (long)(duration.TotalSeconds * Stopwatch.Frequency);

        for (int writerIndex = 0; writerIndex < scenario.WriterCount; writerIndex++)
        {
            int localWriterIndex = writerIndex;
            writerTasks[writerIndex] = Task.Run(async () =>
            {
                using var connection = OpenSqliteWriter(filePath);
                using var statement = connection.Prepare(
                    "INSERT INTO bench (id, value, text_col, category) VALUES (?1, ?2, ?3, ?4);");

                statement.BindInt64(2, localWriterIndex);
                statement.BindText(3, s_textColUtf8);
                statement.BindText(4, s_categoryUtf8);

                var localLatencies = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int busyCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    int id = idAllocator.NextId(localWriterIndex);
                    statement.BindInt64(1, id);

                    var sw = Stopwatch.StartNew();
                    int rc = statement.Step();
                    sw.Stop();

                    statement.Reset(allowBusy: true);

                    if (rc == SqliteCApiDatabase.Done)
                    {
                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                        continue;
                    }

                    if (rc == SqliteCApiDatabase.Busy)
                    {
                        busyCount++;
                        continue;
                    }

                    fatalErrorCount++;
                    throw statement.CreateException("step", rc);
                }

                return new WriterStats(localLatencies ?? [], successfulCommits, busyCount, fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks).ConfigureAwait(false);
        totalSw.Stop();

        return CombineWriterStats(completed, totalSw.Elapsed.TotalMilliseconds);
    }

    private static SqliteCApiDatabase OpenSqliteWriter(string filePath)
    {
        var connection = SqliteCApiDatabase.Open(filePath);
        connection.SetBusyTimeout(SqliteBusyTimeoutMs);
        connection.ExecuteNonQuery("PRAGMA synchronous=FULL;");
        connection.ExecuteNonQuery($"PRAGMA wal_autocheckpoint={WalAutoCheckpointPages.ToString(System.Globalization.CultureInfo.InvariantCulture)};");
        return connection;
    }

    private static BenchmarkResult CreateResult(string name, ConcurrentPhaseStats stats, string extraInfo)
    {
        var histogram = new LatencyHistogram();
        foreach (double latencyMs in stats.CommitLatenciesMs)
            histogram.Record(latencyMs);

        return new BenchmarkResult
        {
            Name = name,
            TotalOps = stats.SuccessfulCommits,
            ElapsedMs = stats.ElapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo = extraInfo,
        };
    }

    private static ConcurrentPhaseStats CombineWriterStats(WriterStats[] completed, double elapsedMs)
    {
        var combinedLatencies = new List<double>(completed.Sum(static s => s.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int busyCount = 0;
        int fatalErrorCount = 0;

        foreach (var writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            busyCount += writer.BusyCount;
            fatalErrorCount += writer.FatalErrorCount;
        }

        return new ConcurrentPhaseStats(combinedLatencies, successfulCommits, busyCount, fatalErrorCount, elapsedMs);
    }

    private static string DescribeCSharpDbSurface(ComparisonSurface surface)
        => surface switch
        {
            ComparisonSurface.CSharpDbRawSql => "Database.ExecuteAsync(sql)",
            ComparisonSurface.CSharpDbInsertBatch => "InsertBatch(1-row reused)",
            _ => throw new ArgumentOutOfRangeException(nameof(surface), surface, null),
        };

    private static bool IsBatchSchemaInvalidation(InvalidOperationException ex)
        => ex.Message.Contains("Insert batch for table", StringComparison.Ordinal) &&
           ex.Message.Contains("schema changed", StringComparison.Ordinal);

    private static string FormatBatchWindow(TimeSpan batchWindow)
    {
        if (batchWindow == TimeSpan.Zero)
            return "0";

        double microseconds = batchWindow.TotalMilliseconds * 1000.0;
        if (microseconds < 1000.0)
            return $"{microseconds:F0}us";

        return $"{batchWindow.TotalMilliseconds:F3}ms";
    }

    private static string FormatBatchWindowToken(TimeSpan batchWindow)
    {
        if (batchWindow == TimeSpan.Zero)
            return "Batch0";

        double microseconds = batchWindow.TotalMilliseconds * 1000.0;
        if (microseconds < 1000.0)
            return $"Batch{microseconds:F0}us";

        return $"Batch{batchWindow.TotalMilliseconds:F3}ms";
    }

    private static string FormatRate(long count, double elapsedMs)
    {
        if (count == 0 || elapsedMs <= 0)
            return "0.0";

        double elapsedSeconds = elapsedMs / 1000.0;
        return (count / elapsedSeconds).ToString("F1", System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string FormatCommitsPerFlush(long flushedCommitCount, long flushCount)
    {
        if (flushCount == 0)
            return "0.00";

        return (flushedCommitCount / (double)flushCount).ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string FormatKiBPerFlush(long flushedByteCount, long flushCount)
    {
        if (flushCount == 0)
            return "0.0";

        return (flushedByteCount / (double)flushCount / 1024.0).ToString("F1", System.Globalization.CultureInfo.InvariantCulture);
    }

    private static void PrintResult(BenchmarkResult result)
    {
        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");

        if (!string.IsNullOrWhiteSpace(result.ExtraInfo))
            Console.WriteLine($"    {result.ExtraInfo}");
    }

    private enum ComparisonSurface
    {
        CSharpDbRawSql,
        CSharpDbInsertBatch,
        SqliteCApi,
    }

    private enum InsertKeyPattern
    {
        HotRightEdge,
        DisjointWriterRange,
    }

    private sealed record ConcurrentComparisonScenario(
        string Name,
        ComparisonSurface Surface,
        int WriterCount,
        TimeSpan BatchWindow,
        InsertKeyPattern KeyPattern,
        ImplicitInsertExecutionMode CSharpDbImplicitInsertExecutionMode);

    private sealed record WriterStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount);

    private sealed record ConcurrentPhaseStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount,
        double ElapsedMs);

    private sealed class ExplicitIdAllocator
    {
        private int _nextHotRightEdgeId;
        private readonly int[] _nextDisjointIds;
        private readonly InsertKeyPattern _keyPattern;

        internal ExplicitIdAllocator(int writerCount, InsertKeyPattern keyPattern)
        {
            _keyPattern = keyPattern;
            _nextDisjointIds = Enumerable.Range(0, writerCount)
                .Select(writerIndex => writerIndex * ExplicitIdWriterRangeSpan)
                .ToArray();
        }

        internal int NextId(int writerIndex)
            => _keyPattern switch
            {
                InsertKeyPattern.HotRightEdge => Interlocked.Increment(ref _nextHotRightEdgeId),
                InsertKeyPattern.DisjointWriterRange => Interlocked.Increment(ref _nextDisjointIds[writerIndex]),
                _ => throw new ArgumentOutOfRangeException(nameof(writerIndex), _keyPattern, null),
            };
    }

    private sealed class SqliteComparisonContext : IDisposable
    {
        private SqliteComparisonContext(string filePath)
        {
            FilePath = filePath;
        }

        internal string FilePath { get; }

        internal static SqliteComparisonContext Create(string prefix)
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

            using var connection = SqliteCApiDatabase.Open(filePath);
            connection.SetBusyTimeout(SqliteBusyTimeoutMs);
            connection.ExecuteNonQuery("PRAGMA journal_mode=WAL;");
            connection.ExecuteNonQuery("PRAGMA synchronous=FULL;");
            connection.ExecuteNonQuery($"PRAGMA wal_autocheckpoint={WalAutoCheckpointPages.ToString(System.Globalization.CultureInfo.InvariantCulture)};");

            string journalMode = connection.ExecuteScalarText("PRAGMA journal_mode;");
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SQLite journal_mode=wal, observed '{journalMode}'.");

            int synchronous = connection.ExecuteScalarInt32("PRAGMA synchronous;");
            if (synchronous != 2)
                throw new InvalidOperationException($"Expected SQLite synchronous=FULL (2), observed '{synchronous}'.");

            int autoCheckpoint = connection.ExecuteScalarInt32("PRAGMA wal_autocheckpoint;");
            if (autoCheckpoint != WalAutoCheckpointPages)
            {
                throw new InvalidOperationException(
                    $"Expected SQLite wal_autocheckpoint={WalAutoCheckpointPages}, observed '{autoCheckpoint}'.");
            }

            connection.ExecuteNonQuery(
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);");

            return new SqliteComparisonContext(filePath);
        }

        public void Dispose()
        {
            try { if (File.Exists(FilePath)) File.Delete(FilePath); } catch { }
            try { if (File.Exists(FilePath + "-wal")) File.Delete(FilePath + "-wal"); } catch { }
            try { if (File.Exists(FilePath + "-shm")) File.Delete(FilePath + "-shm"); } catch { }
        }
    }
}
