using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Stress;

/// <summary>
/// Exercises explicit write transactions that perform predicate reads while
/// concurrent writers update rows either inside or outside the tracked range.
/// This gives the stress suite a direct signal for logical conflict quality:
/// overlapping writers should create reader conflicts, while disjoint writers
/// should mostly avoid them.
/// </summary>
public static class LogicalConflictRangeBenchmark
{
    private const int SeedRowCount = 4_096;
    private const int ReaderCount = 4;
    private const int WriterCount = 4;
    private const int RowsPerWriterTransaction = 8;
    private static readonly TimeSpan ReaderHoldDuration = TimeSpan.FromMilliseconds(2);
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

    private static readonly LogicalConflictScenario[] s_scenarios =
    [
        new(
            "Overlap",
            ReaderRangeStart: 1,
            ReaderRangeEnd: 1_024,
            WriterRangeStart: 1,
            WriterRangeEnd: 1_024,
            ExpectedBehavior: "high-read-conflict"),
        new(
            "Disjoint",
            ReaderRangeStart: 1,
            ReaderRangeEnd: 1_024,
            WriterRangeStart: 2_049,
            WriterRangeEnd: 3_072,
            ExpectedBehavior: "low-read-conflict"),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length * 2);

        foreach (var scenario in s_scenarios)
            results.AddRange(await RunScenarioAsync(scenario));

        return results;
    }

    private static async Task<List<BenchmarkResult>> RunScenarioAsync(LogicalConflictScenario scenario)
    {
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(4096),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = 256,
            });
        });

        await using var bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE logical_conflict_items (id INTEGER PRIMARY KEY, value INTEGER, writer INTEGER)",
            options);
        await bench.Db.ExecuteAsync("CREATE INDEX idx_logical_conflict_items_value ON logical_conflict_items(value)");
        await bench.SeedAsync(
            "logical_conflict_items",
            SeedRowCount,
            static id => $"INSERT INTO logical_conflict_items VALUES ({id + 1}, {id + 1}, 0)");
        await bench.ReopenAsync();

        await RunPhaseAsync(bench.Db, scenario, WarmupDuration, recordLatencies: false);
        await bench.Db.ExecuteAsync("UPDATE logical_conflict_items SET writer = 0");

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var stats = await RunPhaseAsync(bench.Db, scenario, MeasuredDuration, recordLatencies: true);
        long finalRowCount = await ScalarIntAsync(bench.Db, "SELECT COUNT(*) FROM logical_conflict_items");
        long finalWriterSum = await ScalarIntAsync(bench.Db, "SELECT SUM(writer) FROM logical_conflict_items");
        long expectedWriterSum = (long)stats.WriterSuccessfulCommits * RowsPerWriterTransaction;

        if (finalRowCount != SeedRowCount)
        {
            throw new InvalidOperationException(
                $"Logical conflict stress invariant failed for scenario '{scenario.Name}': expected {SeedRowCount} rows, observed {finalRowCount}.");
        }

        if (finalWriterSum != expectedWriterSum)
        {
            throw new InvalidOperationException(
                $"Logical conflict stress invariant failed for scenario '{scenario.Name}': expected writer sum {expectedWriterSum}, observed {finalWriterSum}.");
        }

        var results = new List<BenchmarkResult>(capacity: 2)
        {
            CreateResult(
                scenario,
                actorName: "ReadTx",
                latenciesMs: stats.ReaderLatenciesMs,
                successfulCommits: stats.ReaderSuccessfulCommits,
                conflictCount: stats.ReaderConflictCount,
                fatalErrorCount: stats.ReaderFatalErrorCount,
                elapsedMs: stats.ElapsedMs,
                finalRowCount,
                finalWriterSum,
                expectedWriterSum,
                otherActorSuccessfulCommits: stats.WriterSuccessfulCommits,
                otherActorConflictCount: stats.WriterConflictCount,
                otherActorFatalErrorCount: stats.WriterFatalErrorCount),
            CreateResult(
                scenario,
                actorName: "WriteTx",
                latenciesMs: stats.WriterLatenciesMs,
                successfulCommits: stats.WriterSuccessfulCommits,
                conflictCount: stats.WriterConflictCount,
                fatalErrorCount: stats.WriterFatalErrorCount,
                elapsedMs: stats.ElapsedMs,
                finalRowCount,
                finalWriterSum,
                expectedWriterSum,
                otherActorSuccessfulCommits: stats.ReaderSuccessfulCommits,
                otherActorConflictCount: stats.ReaderConflictCount,
                otherActorFatalErrorCount: stats.ReaderFatalErrorCount),
        };

        Console.WriteLine(
            $"  LogicalConflictRange_{scenario.Name}: readers ok/conflict={stats.ReaderSuccessfulCommits}/{stats.ReaderConflictCount}, writers ok/conflict={stats.WriterSuccessfulCommits}/{stats.WriterConflictCount}");

        return results;
    }

    private static BenchmarkResult CreateResult(
        LogicalConflictScenario scenario,
        string actorName,
        IReadOnlyList<double> latenciesMs,
        int successfulCommits,
        int conflictCount,
        int fatalErrorCount,
        double elapsedMs,
        long finalRowCount,
        long finalWriterSum,
        long expectedWriterSum,
        int otherActorSuccessfulCommits,
        int otherActorConflictCount,
        int otherActorFatalErrorCount)
    {
        var histogram = new LatencyHistogram();
        for (int i = 0; i < latenciesMs.Count; i++)
            histogram.Record(latenciesMs[i]);

        var result = BenchmarkResult.FromHistogram(
            $"LogicalConflictRange_{scenario.Name}_{actorName}_{(int)MeasuredDuration.TotalSeconds}s",
            histogram,
            elapsedMs);

        double conflictRate = (successfulCommits + conflictCount) == 0
            ? 0
            : conflictCount / (double)(successfulCommits + conflictCount);

        string otherActorName = string.Equals(actorName, "ReadTx", StringComparison.Ordinal)
            ? "write"
            : "read";

        return new BenchmarkResult
        {
            Name = result.Name,
            TotalOps = successfulCommits,
            ElapsedMs = result.ElapsedMs,
            P50Ms = result.P50Ms,
            P90Ms = result.P90Ms,
            P95Ms = result.P95Ms,
            P99Ms = result.P99Ms,
            P999Ms = result.P999Ms,
            MinMs = result.MinMs,
            MaxMs = result.MaxMs,
            MeanMs = result.MeanMs,
            StdDevMs = result.StdDevMs,
            ExtraInfo =
                $"scenario={scenario.Name}, expected={scenario.ExpectedBehavior}, readers={ReaderCount}, writers={WriterCount}, rowsPerWriterTx={RowsPerWriterTransaction}, readerRange=[{scenario.ReaderRangeStart},{scenario.ReaderRangeEnd}], writerRange=[{scenario.WriterRangeStart},{scenario.WriterRangeEnd}], successfulCommits={successfulCommits}, conflicts={conflictCount}, conflictRate={conflictRate:P1}, fatalErrors={fatalErrorCount}, {otherActorName}SuccessfulCommits={otherActorSuccessfulCommits}, {otherActorName}Conflicts={otherActorConflictCount}, {otherActorName}FatalErrors={otherActorFatalErrorCount}, finalRows={finalRowCount}, writerSum={finalWriterSum}, expectedWriterSum={expectedWriterSum}",
        };
    }

    private static async Task<LogicalConflictPhaseStats> RunPhaseAsync(
        Database db,
        LogicalConflictScenario scenario,
        TimeSpan duration,
        bool recordLatencies)
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long durationTicks = (long)(duration.TotalSeconds * Stopwatch.Frequency);
        var readerTasks = new Task<ActorStats>[ReaderCount];
        var writerTasks = new Task<ActorStats>[WriterCount];

        for (int readerIndex = 0; readerIndex < ReaderCount; readerIndex++)
        {
            readerTasks[readerIndex] = Task.Run(async () =>
            {
                var latenciesMs = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int conflictCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        await using var tx = await db.BeginWriteTransactionAsync(CancellationToken.None).ConfigureAwait(false);
                        await using var result = await tx.ExecuteAsync(
                            $"SELECT SUM(writer) FROM logical_conflict_items WHERE value BETWEEN {scenario.ReaderRangeStart} AND {scenario.ReaderRangeEnd}",
                            CancellationToken.None).ConfigureAwait(false);
                        await result.ToListAsync(CancellationToken.None).ConfigureAwait(false);
                        await Task.Delay(ReaderHoldDuration, CancellationToken.None).ConfigureAwait(false);
                        await tx.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                        sw.Stop();

                        successfulCommits++;
                        if (recordLatencies)
                            latenciesMs!.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbConflictException)
                    {
                        sw.Stop();
                        conflictCount++;
                    }
                    catch
                    {
                        fatalErrorCount++;
                        throw;
                    }
                }

                return new ActorStats(latenciesMs ?? [], successfulCommits, conflictCount, fatalErrorCount);
            });
        }

        for (int writerIndex = 0; writerIndex < WriterCount; writerIndex++)
        {
            int localWriterIndex = writerIndex;
            writerTasks[writerIndex] = Task.Run(async () =>
            {
                var latenciesMs = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int conflictCount = 0;
                int fatalErrorCount = 0;
                int partitionSize = Math.Max(
                    RowsPerWriterTransaction,
                    (scenario.WriterRangeEnd - scenario.WriterRangeStart + 1) / WriterCount);
                int partitionStart = scenario.WriterRangeStart + (localWriterIndex * partitionSize);
                int updateCursor = 0;
                var ids = new int[RowsPerWriterTransaction];

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    PopulateWriterIds(ids, partitionStart, partitionSize, ref updateCursor);
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        await using var tx = await db.BeginWriteTransactionAsync(CancellationToken.None).ConfigureAwait(false);
                        for (int i = 0; i < ids.Length; i++)
                        {
                            await tx.ExecuteAsync(
                                $"UPDATE logical_conflict_items SET writer = writer + 1 WHERE id = {ids[i]}",
                                CancellationToken.None).ConfigureAwait(false);
                        }

                        await tx.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                        sw.Stop();

                        successfulCommits++;
                        if (recordLatencies)
                            latenciesMs!.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbConflictException)
                    {
                        sw.Stop();
                        conflictCount++;
                    }
                    catch
                    {
                        fatalErrorCount++;
                        throw;
                    }
                }

                return new ActorStats(latenciesMs ?? [], successfulCommits, conflictCount, fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        ActorStats[] completedReaders = await Task.WhenAll(readerTasks).ConfigureAwait(false);
        ActorStats[] completedWriters = await Task.WhenAll(writerTasks).ConfigureAwait(false);
        totalSw.Stop();

        var readerLatenciesMs = new List<double>(completedReaders.Sum(static reader => reader.LatenciesMs.Count));
        int readerSuccessfulCommits = 0;
        int readerConflictCount = 0;
        int readerFatalErrorCount = 0;
        foreach (ActorStats reader in completedReaders)
        {
            readerSuccessfulCommits += reader.SuccessfulCommits;
            readerConflictCount += reader.ConflictCount;
            readerFatalErrorCount += reader.FatalErrorCount;
            readerLatenciesMs.AddRange(reader.LatenciesMs);
        }

        var writerLatenciesMs = new List<double>(completedWriters.Sum(static writer => writer.LatenciesMs.Count));
        int writerSuccessfulCommits = 0;
        int writerConflictCount = 0;
        int writerFatalErrorCount = 0;
        foreach (ActorStats writer in completedWriters)
        {
            writerSuccessfulCommits += writer.SuccessfulCommits;
            writerConflictCount += writer.ConflictCount;
            writerFatalErrorCount += writer.FatalErrorCount;
            writerLatenciesMs.AddRange(writer.LatenciesMs);
        }

        return new LogicalConflictPhaseStats(
            readerLatenciesMs,
            writerLatenciesMs,
            readerSuccessfulCommits,
            readerConflictCount,
            readerFatalErrorCount,
            writerSuccessfulCommits,
            writerConflictCount,
            writerFatalErrorCount,
            totalSw.Elapsed.TotalMilliseconds);
    }

    private static void PopulateWriterIds(int[] ids, int partitionStart, int partitionSize, ref int updateCursor)
    {
        for (int i = 0; i < ids.Length; i++)
            ids[i] = partitionStart + ((updateCursor + i) % partitionSize);

        updateCursor += ids.Length;
    }

    private static async Task<long> ScalarIntAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql).ConfigureAwait(false);
        DbValue[] row = await result.ToListAsync().ConfigureAwait(false) switch
        {
            [var single] => single,
            _ => throw new InvalidOperationException($"Expected scalar result for '{sql}'."),
        };

        return row[0].IsNull ? 0 : row[0].AsInteger;
    }

    private sealed record LogicalConflictScenario(
        string Name,
        int ReaderRangeStart,
        int ReaderRangeEnd,
        int WriterRangeStart,
        int WriterRangeEnd,
        string ExpectedBehavior);

    private sealed record ActorStats(
        IReadOnlyList<double> LatenciesMs,
        int SuccessfulCommits,
        int ConflictCount,
        int FatalErrorCount);

    private sealed record LogicalConflictPhaseStats(
        IReadOnlyList<double> ReaderLatenciesMs,
        IReadOnlyList<double> WriterLatenciesMs,
        int ReaderSuccessfulCommits,
        int ReaderConflictCount,
        int ReaderFatalErrorCount,
        int WriterSuccessfulCommits,
        int WriterConflictCount,
        int WriterFatalErrorCount,
        double ElapsedMs);
}
