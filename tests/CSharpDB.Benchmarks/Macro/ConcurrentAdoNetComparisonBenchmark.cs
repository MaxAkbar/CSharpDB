using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;
using CSharpDB.Primitives;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Compares CSharpDB and SQLite through their ADO.NET providers under the same
/// prepared multi-writer auto-commit insert shape.
/// </summary>
public static class ConcurrentAdoNetComparisonBenchmark
{
    private const int ExplicitIdWriterRangeSpan = 1_000_000;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(10);
    private static readonly ConcurrentComparisonScenario[] s_scenarios = CreateScenarios();

    private const int SqliteBusyTimeoutMs = 1000;
    private const int SqliteBusy = 5;
    private const int SqliteLocked = 6;

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
                $"Unknown concurrent ADO.NET comparison scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static ConcurrentComparisonScenario[] CreateScenarios()
    {
        var scenarios = new List<ConcurrentComparisonScenario>(capacity: 6);

        foreach (int writerCount in new[] { 4, 8 })
        {
            scenarios.Add(new ConcurrentComparisonScenario(
                $"CSharpDB_AdoNet_Disjoint_W{writerCount}",
                ProviderKind.CSharpDb,
                writerCount,
                InsertKeyPattern.DisjointWriterRange));
            scenarios.Add(new ConcurrentComparisonScenario(
                $"SQLite_AdoNet_Disjoint_W{writerCount}",
                ProviderKind.Sqlite,
                writerCount,
                InsertKeyPattern.DisjointWriterRange));
        }

        scenarios.Add(new ConcurrentComparisonScenario(
            "CSharpDB_AdoNet_HotRightEdge_W8",
            ProviderKind.CSharpDb,
            WriterCount: 8,
            KeyPattern: InsertKeyPattern.HotRightEdge));
        scenarios.Add(new ConcurrentComparisonScenario(
            "SQLite_AdoNet_HotRightEdge_W8",
            ProviderKind.Sqlite,
            WriterCount: 8,
            KeyPattern: InsertKeyPattern.HotRightEdge));

        return scenarios.ToArray();
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(ConcurrentComparisonScenario scenario)
    {
        var idAllocator = new ExplicitIdAllocator(scenario.WriterCount, scenario.KeyPattern);
        await using var context = await ComparisonContext.CreateAsync(scenario.Provider, scenario.Name);

        await RunPhaseAsync(context, scenario, WarmupDuration, recordLatencies: false, idAllocator);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        ConcurrentPhaseStats stats = await RunPhaseAsync(context, scenario, MeasuredDuration, recordLatencies: true, idAllocator);

        BenchmarkResult result = CreateResult(
            $"ConcurrentAdoNetCompare_{scenario.Name}_10s",
            stats,
            context.BuildExtraInfo(scenario));

        PrintResult(result);
        return result;
    }

    private static async Task<ConcurrentPhaseStats> RunPhaseAsync(
        ComparisonContext context,
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
                await using DbConnection connection = await context.OpenWriterConnectionAsync();
                using DbCommand command = connection.CreateCommand();
                command.CommandText = "INSERT INTO bench VALUES (@id, @value, @text_col, @category);";
                DbParameter idParam = AddParameter(command, "@id", 0);
                DbParameter valueParam = AddParameter(command, "@value", 0);
                DbParameter textParam = AddParameter(command, "@text_col", "durable");
                DbParameter categoryParam = AddParameter(command, "@category", "Alpha");
                command.Prepare();

                var localLatencies = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int busyCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    int id = idAllocator.NextId(localWriterIndex);
                    idParam.Value = id;
                    valueParam.Value = localWriterIndex;
                    textParam.Value = "durable";
                    categoryParam.Value = "Alpha";

                    var sw = Stopwatch.StartNew();
                    try
                    {
                        int rowsAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");

                        sw.Stop();
                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbDataException ex) when (ex.ErrorCode == ErrorCode.Busy)
                    {
                        sw.Stop();
                        busyCount++;
                    }
                    catch (SqliteException ex) when (IsSqliteBusy(ex))
                    {
                        sw.Stop();
                        busyCount++;
                    }
                    catch
                    {
                        fatalErrorCount++;
                        throw;
                    }
                }

                return new WriterStats(
                    localLatencies ?? [],
                    successfulCommits,
                    busyCount,
                    fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks).ConfigureAwait(false);
        totalSw.Stop();

        var combinedLatencies = new List<double>(completed.Sum(static writer => writer.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int busyCount = 0;
        int fatalErrorCount = 0;

        foreach (WriterStats writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            busyCount += writer.BusyCount;
            fatalErrorCount += writer.FatalErrorCount;
        }

        return new ConcurrentPhaseStats(
            combinedLatencies,
            successfulCommits,
            busyCount,
            fatalErrorCount,
            totalSw.Elapsed.TotalMilliseconds);
    }

    private static DbParameter AddParameter(DbCommand command, string name, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static bool IsSqliteBusy(SqliteException ex)
        => ex.SqliteErrorCode == SqliteBusy || ex.SqliteErrorCode == SqliteLocked;

    private static BenchmarkResult CreateResult(
        string name,
        ConcurrentPhaseStats stats,
        string extraInfo)
    {
        var histogram = new LatencyHistogram();
        foreach (double latency in stats.CommitLatenciesMs)
            histogram.Record(latency);

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
            ExtraInfo = $"{extraInfo}, successfulCommits={stats.SuccessfulCommits}, busy={stats.BusyCount}, fatalErrors={stats.FatalErrorCount}"
        };
    }

    private static void PrintResult(BenchmarkResult result)
    {
        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");

        if (!string.IsNullOrWhiteSpace(result.ExtraInfo))
            Console.WriteLine($"    {result.ExtraInfo}");
    }

    private enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }

    private enum InsertKeyPattern
    {
        HotRightEdge,
        DisjointWriterRange,
    }

    private sealed record ConcurrentComparisonScenario(
        string Name,
        ProviderKind Provider,
        int WriterCount,
        InsertKeyPattern KeyPattern);

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

    private sealed class ComparisonContext : IAsyncDisposable
    {
        private readonly ProviderKind _provider;
        private DbConnection? _keeperConnection;

        private ComparisonContext(ProviderKind provider, string connectionIdentity)
        {
            _provider = provider;
            ConnectionIdentity = connectionIdentity;
        }

        internal string ConnectionIdentity { get; }

        internal static async Task<ComparisonContext> CreateAsync(ProviderKind provider, string prefix)
        {
            string connectionIdentity = $"{prefix}_{Guid.NewGuid():N}";
            var context = new ComparisonContext(provider, connectionIdentity);
            await context.InitializeAsync().ConfigureAwait(false);
            return context;
        }

        internal async Task<DbConnection> OpenWriterConnectionAsync()
            => await OpenWriterConnectionCoreAsync().ConfigureAwait(false);

        internal string BuildExtraInfo(ConcurrentComparisonScenario scenario)
        {
            return _provider switch
            {
                ProviderKind.CSharpDb => string.Create(
                    CultureInfo.InvariantCulture,
                    $"provider=CSharpDB.Data/{GetCSharpDbProviderVersion()}, surface=DbCommand.Prepare+ExecuteNonQueryAsync, storage=shared-memory, durability=none, storage-tuning=n/a(named-shared-memory target), keyPattern={scenario.KeyPattern}, writers={scenario.WriterCount}"),
                ProviderKind.Sqlite => string.Create(
                    CultureInfo.InvariantCulture,
                    $"provider=Microsoft.Data.Sqlite/{GetSqliteProviderVersion()}, surface=DbCommand.Prepare+ExecuteNonQueryAsync, storage=shared-memory, durability=none, cache=shared, pooling=false, journal_mode=memory, busyTimeoutMs={SqliteBusyTimeoutMs}, keyPattern={scenario.KeyPattern}, writers={scenario.WriterCount}"),
                _ => throw new ArgumentOutOfRangeException(),
            };
        }

        public async ValueTask DisposeAsync()
        {
            if (_keeperConnection is not null)
            {
                await _keeperConnection.DisposeAsync().ConfigureAwait(false);
                _keeperConnection = null;
            }

            if (_provider == ProviderKind.CSharpDb)
                await CSharpDbConnection.ClearPoolAsync(GetCSharpDbConnectionString(ConnectionIdentity)).ConfigureAwait(false);
        }

        private async Task InitializeAsync()
        {
            _keeperConnection = await OpenWriterConnectionCoreAsync().ConfigureAwait(false);
            using DbCommand command = _keeperConnection.CreateCommand();
            command.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);";
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        private async Task<DbConnection> OpenWriterConnectionCoreAsync()
        {
            switch (_provider)
            {
                case ProviderKind.CSharpDb:
                {
                    var connection = new CSharpDbConnection(GetCSharpDbConnectionString(ConnectionIdentity));
                    await connection.OpenAsync().ConfigureAwait(false);
                    return connection;
                }
                case ProviderKind.Sqlite:
                {
                    var connection = new SqliteConnection(CreateSqliteConnectionString(ConnectionIdentity));

                    await connection.OpenAsync().ConfigureAwait(false);
                    await ConfigureSqliteConnectionAsync(connection).ConfigureAwait(false);
                    return connection;
                }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static async Task ConfigureSqliteConnectionAsync(SqliteConnection connection)
        {
            using (var busyTimeoutCommand = connection.CreateCommand())
            {
                busyTimeoutCommand.CommandText = $"PRAGMA busy_timeout={SqliteBusyTimeoutMs.ToString(CultureInfo.InvariantCulture)};";
                await busyTimeoutCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            using (var journalCommand = connection.CreateCommand())
            {
                journalCommand.CommandText = "PRAGMA journal_mode=MEMORY;";
                string journalMode = (Convert.ToString(await journalCommand.ExecuteScalarAsync().ConfigureAwait(false)) ?? string.Empty).Trim();
                if (!journalMode.Equals("memory", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"Expected SQLite journal_mode=memory, observed '{journalMode}'.");
            }
        }

        private static string GetCSharpDbConnectionString(string connectionIdentity)
            => $"Data Source=:memory:{connectionIdentity}";

        private static string CreateSqliteConnectionString(string connectionIdentity)
            => new SqliteConnectionStringBuilder
            {
                DataSource = connectionIdentity,
                Mode = SqliteOpenMode.Memory,
                Cache = SqliteCacheMode.Shared,
                Pooling = false,
                DefaultTimeout = 30,
            }.ToString();
    }

    private static string GetSqliteProviderVersion()
    {
        Assembly assembly = typeof(SqliteConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private static string GetCSharpDbProviderVersion()
    {
        Assembly assembly = typeof(CSharpDbConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }
}
