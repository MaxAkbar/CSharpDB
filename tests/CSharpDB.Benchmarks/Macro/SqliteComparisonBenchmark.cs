using System.Diagnostics;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Macro;

public static class SqliteComparisonBenchmark
{
    private const int SeedCount = 20_000;
    private const int BatchSize = 100;
    private const int SeedBatchSize = 500;
    private const int WarmupCount = 128;
    private const int ConcurrentReaderCount = 8;
    private const int ReusedSessionBurstReads = 32;
    private const int HighThroughputLatencySampleEvery = 128;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);
    private static readonly string s_providerInfo = $"provider=Microsoft.Data.Sqlite/{GetProviderVersion()}";
    private const string ConnectionInfo = "cache=private; pooling=false; journal_mode=wal; synchronous=full";

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        return
        [
            await RunSqlSingleInsertAsync(),
            await RunSqlBatchInsertAsync(),
            await RunSqlPointLookupAsync(),
            await RunSqlConcurrentReadsAsync(reuseSessionBurstReads: false),
            await RunSqlConcurrentReadsAsync(reuseSessionBurstReads: true),
        ];
    }

    private static async Task<BenchmarkResult> RunSqlSingleInsertAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateWritableAsync("sqlite-compare-single");
        int nextId = SeedCount + 1_000_000;

        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            "SQLite_WalFull_Sql_SingleInsert_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                int rowsAffected = await ExecuteNonQueryAsync(
                    context.KeeperConnection,
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');");
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
            });

        return CloneResult(result, extraInfo: context.WithNotes("workload=single-row auto-commit raw SQL"));
    }

    private static async Task<BenchmarkResult> RunSqlBatchInsertAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateWritableAsync("sqlite-compare-batch");
        int nextId = SeedCount + 2_000_000;

        BenchmarkResult transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
            "SQLite_WalFull_Sql_Batch100_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                using var transaction = context.KeeperConnection.BeginTransaction();
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        int rowsAffected = await ExecuteNonQueryAsync(
                            context.KeeperConnection,
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                            transaction);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                    }

                    transaction.Commit();
                }
                catch
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
            });

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=raw SQL statements inside one explicit transaction"));
    }

    private static async Task<BenchmarkResult> RunSqlPointLookupAsync()
    {
        await using var context = await SqliteBenchmarkContext.CreateReadSeededAsync("sqlite-compare-lookup");
        using var connection = await context.OpenReadOnlyConnectionAsync();
        var rng = new Random(42);

        await WarmSqlLookupsAsync(connection, rng, WarmupCount);

        rng = new Random(42);
        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            "SQLite_WalFull_Sql_PointLookup_20000",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = rng.Next(1, SeedCount + 1);
                long value = await ExecuteScalarInt64Async(connection, $"SELECT value FROM bench WHERE id = {id};");
                if (value != id * 10L)
                    throw new InvalidOperationException($"Lookup for id={id} returned an unexpected result '{value}'.");
            });

        return CloneResult(result, extraInfo: context.WithNotes($"warmup-lookups={WarmupCount}", "workload=single-connection point lookup"));
    }

    private static async Task<BenchmarkResult> RunSqlConcurrentReadsAsync(bool reuseSessionBurstReads)
    {
        await using var context = await SqliteBenchmarkContext.CreateReadSeededAsync(
            reuseSessionBurstReads ? "sqlite-compare-burst" : "sqlite-compare-concurrent");
        var histograms = new LatencyHistogram[ConcurrentReaderCount];
        int latencySampleEvery = reuseSessionBurstReads ? HighThroughputLatencySampleEvery : 1;

        for (int i = 0; i < ConcurrentReaderCount; i++)
            histograms[i] = new LatencyHistogram(latencySampleEvery);

        await WarmConcurrentReadersAsync(context);

        using var cts = new CancellationTokenSource(MeasuredDuration);
        var readerTasks = new Task[ConcurrentReaderCount];
        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            LatencyHistogram histogram = histograms[readerIndex];
            readerTasks[readerIndex] = Task.Run(
                () => reuseSessionBurstReads
                    ? RunReusedReaderLoopAsync(context, histogram, cts.Token)
                    : RunPerQueryReaderLoopAsync(context, histogram, cts.Token),
                cts.Token);
        }

        await Task.WhenAll(readerTasks);

        return new BenchmarkResult
        {
            Name = reuseSessionBurstReads
                ? $"SQLite_WalFull_Sql_ConcurrentReadsBurst{ReusedSessionBurstReads}_{ConcurrentReaderCount}readers"
                : $"SQLite_WalFull_Sql_ConcurrentReads_{ConcurrentReaderCount}readers",
            TotalOps = histograms.Sum(static histogram => histogram.Count),
            ElapsedMs = MeasuredDuration.TotalMilliseconds,
            P50Ms = histograms.Average(static histogram => histogram.Percentile(0.50)),
            P90Ms = histograms.Average(static histogram => histogram.Percentile(0.90)),
            P95Ms = histograms.Average(static histogram => histogram.Percentile(0.95)),
            P99Ms = histograms.Average(static histogram => histogram.Percentile(0.99)),
            P999Ms = histograms.Average(static histogram => histogram.Percentile(0.999)),
            MinMs = histograms.Min(static histogram => histogram.Min),
            MaxMs = histograms.Max(static histogram => histogram.Max),
            MeanMs = histograms.Average(static histogram => histogram.Mean),
            StdDevMs = histograms.Average(static histogram => histogram.StdDev),
            ExtraInfo = context.WithNotes(
                reuseSessionBurstReads
                    ? $"session-mode=reused read-only connection; burst-reads={ReusedSessionBurstReads}; latency-sampling=1/{latencySampleEvery}"
                    : "session-mode=per-query read-only connection",
                $"readers={ConcurrentReaderCount}",
                "workload=select count(*) from bench")
        };
    }

    private static async Task RunPerQueryReaderLoopAsync(
        SqliteBenchmarkContext context,
        LatencyHistogram histogram,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                using var connection = await context.OpenReadOnlyConnectionAsync(ct);
                long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;", ct);
                if (count != SeedCount)
                    throw new InvalidOperationException($"Expected COUNT(*)={SeedCount}, observed {count}.");
            }
            catch (OperationCanceledException)
            {
                return;
            }

            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }
    }

    private static async Task RunReusedReaderLoopAsync(
        SqliteBenchmarkContext context,
        LatencyHistogram histogram,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var connection = await context.OpenReadOnlyConnectionAsync(ct);
                for (int i = 0; i < ReusedSessionBurstReads && !ct.IsCancellationRequested; i++)
                {
                    Stopwatch? sw = histogram.ShouldSampleNext() ? Stopwatch.StartNew() : null;
                    long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;", ct);
                    if (count != SeedCount)
                        throw new InvalidOperationException($"Expected COUNT(*)={SeedCount}, observed {count}.");

                    if (sw is null)
                    {
                        histogram.RecordUnsampled();
                    }
                    else
                    {
                        sw.Stop();
                        histogram.Record(sw.Elapsed.TotalMilliseconds);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static async Task WarmSqlLookupsAsync(SqliteConnection connection, Random rng, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int id = rng.Next(1, SeedCount + 1);
            long value = await ExecuteScalarInt64Async(connection, $"SELECT value FROM bench WHERE id = {id};");
            if (value != id * 10L)
                throw new InvalidOperationException($"Warm lookup for id={id} returned an unexpected result '{value}'.");
        }
    }

    private static async Task WarmConcurrentReadersAsync(SqliteBenchmarkContext context)
    {
        for (int readerIndex = 0; readerIndex < ConcurrentReaderCount; readerIndex++)
        {
            using var connection = await context.OpenReadOnlyConnectionAsync();
            for (int i = 0; i < 8; i++)
            {
                long count = await ExecuteScalarInt64Async(connection, "SELECT COUNT(*) FROM bench;");
                if (count != SeedCount)
                    throw new InvalidOperationException($"Warm concurrent read expected COUNT(*)={SeedCount}, observed {count}.");
            }
        }
    }

    private static async Task<int> ExecuteNonQueryAsync(
        SqliteConnection connection,
        string sql,
        SqliteTransaction? transaction = null,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ExecuteScalarInt64Async(
        SqliteConnection connection,
        string sql,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        object? value = await command.ExecuteScalarAsync(ct);
        return value switch
        {
            long longValue => longValue,
            int intValue => intValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            null => throw new InvalidOperationException($"SQL '{sql}' returned null."),
            _ => Convert.ToInt64(value)
        };
    }

    private static async Task<string> ExecuteScalarTextAsync(
        SqliteConnection connection,
        string sql,
        CancellationToken ct = default)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        object? value = await command.ExecuteScalarAsync(ct);
        return value?.ToString()?.Trim() ?? throw new InvalidOperationException($"SQL '{sql}' returned null.");
    }

    private static BenchmarkResult CloneResult(
        BenchmarkResult source,
        int? totalOps = null,
        string? extraInfo = null)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = totalOps ?? source.TotalOps,
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo ?? source.ExtraInfo
        };
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static string AppendExtraInfo(string? existing, params string?[] notes)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(existing))
            parts.Add(existing);

        foreach (string? note in notes)
        {
            if (!string.IsNullOrWhiteSpace(note))
                parts.Add(note);
        }

        return string.Join(", ", parts);
    }

    private static string GetProviderVersion()
    {
        Assembly assembly = typeof(SqliteConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private sealed class SqliteBenchmarkContext : IAsyncDisposable
    {
        private readonly string _filePath;

        private SqliteBenchmarkContext(string filePath, SqliteConnection keeperConnection)
        {
            _filePath = filePath;
            KeeperConnection = keeperConnection;
        }

        internal SqliteConnection KeeperConnection { get; }

        internal static async Task<SqliteBenchmarkContext> CreateWritableAsync(string prefix)
        {
            string filePath = NewTempDbPath(prefix);
            SqliteConnection keeperConnection = await OpenWritableConnectionAsync(filePath);
            await CreateSchemaAsync(keeperConnection);
            return new SqliteBenchmarkContext(filePath, keeperConnection);
        }

        internal static async Task<SqliteBenchmarkContext> CreateReadSeededAsync(string prefix)
        {
            string filePath = NewTempDbPath(prefix);
            SqliteConnection keeperConnection = await OpenWritableConnectionAsync(filePath);
            await CreateSchemaAsync(keeperConnection);
            await SeedAsync(keeperConnection);
            return new SqliteBenchmarkContext(filePath, keeperConnection);
        }

        internal async Task<SqliteConnection> OpenReadOnlyConnectionAsync(CancellationToken ct = default)
        {
            var connection = new SqliteConnection(CreateConnectionString(_filePath, SqliteOpenMode.ReadOnly));
            await connection.OpenAsync(ct);
            return connection;
        }

        internal string WithNotes(params string?[] notes)
            => AppendExtraInfo($"{s_providerInfo}, {ConnectionInfo}", notes);

        public ValueTask DisposeAsync()
        {
            KeeperConnection.Dispose();
            DeleteSqliteFiles(_filePath);
            return ValueTask.CompletedTask;
        }

        private static async Task<SqliteConnection> OpenWritableConnectionAsync(string filePath, CancellationToken ct = default)
        {
            var connection = new SqliteConnection(CreateConnectionString(filePath, SqliteOpenMode.ReadWriteCreate));
            await connection.OpenAsync(ct);
            await ApplyAndVerifyWritePragmasAsync(connection, ct);
            return connection;
        }

        private static async Task ApplyAndVerifyWritePragmasAsync(SqliteConnection connection, CancellationToken ct)
        {
            string journalMode = await ExecuteScalarTextAsync(connection, "PRAGMA journal_mode=WAL;", ct);
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal, observed '{journalMode}'.");

            await ExecuteNonQueryAsync(connection, "PRAGMA synchronous=FULL;", ct: ct);

            string verifiedJournalMode = await ExecuteScalarTextAsync(connection, "PRAGMA journal_mode;", ct);
            string verifiedSynchronous = await ExecuteScalarTextAsync(connection, "PRAGMA synchronous;", ct);

            if (!verifiedJournalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal after verification, observed '{verifiedJournalMode}'.");

            if (!verifiedSynchronous.Equals("full", StringComparison.OrdinalIgnoreCase) &&
                !verifiedSynchronous.Equals("2", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Expected synchronous=FULL, observed '{verifiedSynchronous}'.");
            }
        }

        private static async Task CreateSchemaAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            await ExecuteNonQueryAsync(
                connection,
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);",
                ct: ct);
        }

        private static async Task SeedAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            for (int batchStart = 1; batchStart <= SeedCount; batchStart += SeedBatchSize)
            {
                using var transaction = connection.BeginTransaction();
                try
                {
                    int batchEnd = Math.Min(batchStart + SeedBatchSize - 1, SeedCount);
                    for (int id = batchStart; id <= batchEnd; id++)
                    {
                        int rowsAffected = await ExecuteNonQueryAsync(
                            connection,
                            $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');",
                            transaction,
                            ct);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted seed row for id={id}, observed {rowsAffected}.");
                    }

                    transaction.Commit();
                }
                catch
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // Preserve the original seed failure.
                    }

                    throw;
                }
            }
        }

        private static string CreateConnectionString(string filePath, SqliteOpenMode mode)
        {
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = filePath,
                Mode = mode,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            };

            return builder.ToString();
        }

        private static void DeleteSqliteFiles(string filePath)
        {
            try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
            try { if (File.Exists(filePath + "-wal")) File.Delete(filePath + "-wal"); } catch { }
            try { if (File.Exists(filePath + "-shm")) File.Delete(filePath + "-shm"); } catch { }
        }

        private static string NewTempDbPath(string prefix)
            => Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
    }
}
