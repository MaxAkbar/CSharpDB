using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Macro;

public static class StrictInsertComparisonBenchmark
{
    private const int BatchSize = 100;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(capacity: 8);

        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
        {
            results.Add(await RunSingleInsertRawAsync(provider));
            results.Add(await RunSingleInsertPreparedAsync(provider));
            results.Add(await RunBatchInsertRawAsync(provider));
            results.Add(await RunBatchInsertPreparedAsync(provider));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSingleInsertRawAsync(ProviderKind provider)
    {
        await using var context = await StrictInsertContext.CreateAsync(provider, "strict-insert-raw-single");
        int nextId = 1_000_000;
        using DbCommand command = context.Connection.CreateCommand();

        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Raw_SingleInsert_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                command.CommandText = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                int rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
            });

        return CloneResult(result, extraInfo: context.WithNotes("mode=raw-sql", "workload=single-row auto-commit", "surface=adonet"));
    }

    private static async Task<BenchmarkResult> RunSingleInsertPreparedAsync(ProviderKind provider)
    {
        await using var context = await StrictInsertContext.CreateAsync(provider, "strict-insert-prepared-single");
        int nextId = 2_000_000;
        using DbCommand command = context.Connection.CreateCommand();
        command.CommandText = "INSERT INTO bench VALUES (@id, @value, @category);";
        DbParameter idParam = AddParameter(command, "@id", 0);
        DbParameter valueParam = AddParameter(command, "@value", 0L);
        DbParameter categoryParam = AddParameter(command, "@category", "");
        command.Prepare();

        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Prepared_SingleInsert_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                idParam.Value = id;
                valueParam.Value = id * 10L;
                categoryParam.Value = GetCategory(id);
                int rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
            });

        return CloneResult(result, extraInfo: context.WithNotes("mode=prepared", "workload=single-row auto-commit", "surface=adonet"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertRawAsync(ProviderKind provider)
    {
        await using var context = await StrictInsertContext.CreateAsync(provider, "strict-insert-raw-batch");
        int nextId = 3_000_000;
        using DbCommand command = context.Connection.CreateCommand();

        BenchmarkResult transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Raw_Batch100_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                await using DbTransaction transaction = await context.Connection.BeginTransactionAsync();
                command.Transaction = transaction;
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        command.CommandText = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                        int rowsAffected = await command.ExecuteNonQueryAsync();
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                    }

                    await transaction.CommitAsync();
                }
                catch
                {
                    try
                    {
                        await transaction.RollbackAsync();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
                finally
                {
                    command.Transaction = null;
                }
            });

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                "mode=raw-sql",
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=explicit transaction batch",
                "surface=adonet"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertPreparedAsync(ProviderKind provider)
    {
        await using var context = await StrictInsertContext.CreateAsync(provider, "strict-insert-prepared-batch");
        int nextId = 4_000_000;
        using DbCommand command = context.Connection.CreateCommand();
        command.CommandText = "INSERT INTO bench VALUES (@id, @value, @category);";
        DbParameter idParam = AddParameter(command, "@id", 0);
        DbParameter valueParam = AddParameter(command, "@value", 0L);
        DbParameter categoryParam = AddParameter(command, "@category", "");
        command.Prepare();

        BenchmarkResult transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Prepared_Batch100_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                await using DbTransaction transaction = await context.Connection.BeginTransactionAsync();
                command.Transaction = transaction;
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        idParam.Value = id;
                        valueParam.Value = id * 10L;
                        categoryParam.Value = GetCategory(id);
                        int rowsAffected = await command.ExecuteNonQueryAsync();
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                    }

                    await transaction.CommitAsync();
                }
                catch
                {
                    try
                    {
                        await transaction.RollbackAsync();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
                finally
                {
                    command.Transaction = null;
                }
            });

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                "mode=prepared",
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=explicit transaction batch",
                "surface=adonet"));
    }

    private static DbParameter AddParameter(DbCommand command, string name, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
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

    private enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }

    private sealed class StrictInsertContext : IAsyncDisposable
    {
        private readonly string _filePath;
        private readonly ProviderKind _provider;

        private StrictInsertContext(ProviderKind provider, string filePath, DbConnection connection, string namePrefix, string baseExtraInfo)
        {
            _provider = provider;
            _filePath = filePath;
            Connection = connection;
            NamePrefix = namePrefix;
            BaseExtraInfo = baseExtraInfo;
        }

        internal DbConnection Connection { get; }
        internal string NamePrefix { get; }
        internal string BaseExtraInfo { get; }

        internal static async Task<StrictInsertContext> CreateAsync(ProviderKind provider, string prefix)
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
            DbConnection connection;
            string namePrefix;
            string extraInfo;

            switch (provider)
            {
                case ProviderKind.CSharpDb:
                    connection = new CSharpDbConnection($"Data Source={filePath};Pooling=false");
                    await connection.OpenAsync();
                    namePrefix = "StrictCompare_CSharpDB_AdoNet_DefaultDurable";
                    extraInfo = $"provider=CSharpDB.Data/{GetCSharpDbProviderVersion()}, pooling=false, durability=default-durable";
                    break;
                case ProviderKind.Sqlite:
                    var sqliteConnection = new SqliteConnection(new SqliteConnectionStringBuilder
                    {
                        DataSource = filePath,
                        Mode = SqliteOpenMode.ReadWriteCreate,
                        Cache = SqliteCacheMode.Private,
                        Pooling = false,
                        DefaultTimeout = 30,
                    }.ToString());
                    await sqliteConnection.OpenAsync();
                    await ApplyAndVerifySqlitePragmasAsync(sqliteConnection);
                    connection = sqliteConnection;
                    namePrefix = "StrictCompare_SQLite_AdoNet_WalFull";
                    extraInfo = $"provider=Microsoft.Data.Sqlite/{GetSqliteProviderVersion()}, cache=private, pooling=false, journal_mode=wal, synchronous=full";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(provider), provider, null);
            }

            using DbCommand command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);";
            await command.ExecuteNonQueryAsync();

            return new StrictInsertContext(provider, filePath, connection, namePrefix, extraInfo);
        }

        internal string WithNotes(params string[] notes)
        {
            if (notes.Length == 0)
                return BaseExtraInfo;

            return $"{BaseExtraInfo}, {string.Join(", ", notes)}";
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            DeleteFiles(_provider, _filePath);
        }

        private static async Task ApplyAndVerifySqlitePragmasAsync(SqliteConnection connection)
        {
            using var journalCommand = connection.CreateCommand();
            journalCommand.CommandText = "PRAGMA journal_mode=WAL;";
            string journalMode = (Convert.ToString(await journalCommand.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal, observed '{journalMode}'.");

            using var syncSetCommand = connection.CreateCommand();
            syncSetCommand.CommandText = "PRAGMA synchronous=FULL;";
            await syncSetCommand.ExecuteNonQueryAsync();

            using var journalVerifyCommand = connection.CreateCommand();
            journalVerifyCommand.CommandText = "PRAGMA journal_mode;";
            string verifiedJournalMode = (Convert.ToString(await journalVerifyCommand.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!verifiedJournalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal after verification, observed '{verifiedJournalMode}'.");

            using var syncVerifyCommand = connection.CreateCommand();
            syncVerifyCommand.CommandText = "PRAGMA synchronous;";
            string verifiedSynchronous = (Convert.ToString(await syncVerifyCommand.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!verifiedSynchronous.Equals("full", StringComparison.OrdinalIgnoreCase) &&
                !verifiedSynchronous.Equals("2", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Expected synchronous=FULL, observed '{verifiedSynchronous}'.");
            }
        }

        private static void DeleteFiles(ProviderKind provider, string filePath)
        {
            try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }

            switch (provider)
            {
                case ProviderKind.CSharpDb:
                    try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
                    break;
                case ProviderKind.Sqlite:
                    try { if (File.Exists(filePath + "-wal")) File.Delete(filePath + "-wal"); } catch { }
                    try { if (File.Exists(filePath + "-shm")) File.Delete(filePath + "-shm"); } catch { }
                    break;
            }
        }
    }
}
