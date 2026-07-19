using System.Data.Common;
using BenchmarkDotNet.Attributes;
using CSharpDB.Data;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Compares durable file-backed ADO.NET connection lifecycle cost for CSharpDB
/// and SQLite. Each benchmark case owns a separately prepared database so pooled
/// and non-pooled physical ownership never overlap.
/// </summary>
[BenchmarkCategory("ADO.NET", "ConnectionLifecycle")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class AdoNetConnectionLifecycleBenchmarks
{
    private string _databasePath = null!;
    private string _connectionString = null!;
    private DbConnection _reusedConnection = null!;

    [Params(LifecycleProvider.CSharpDb, LifecycleProvider.Sqlite)]
    public LifecycleProvider Provider { get; set; }

    [Params(false, true)]
    public bool Pooling { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        SqliteConnection.ClearAllPools();

        _databasePath = Path.Combine(
            Path.GetTempPath(),
            $"adonet_lifecycle_{Provider}_{(Pooling ? "pool" : "direct")}_{Guid.NewGuid():N}.db");

        await PrepareDatabaseAsync();
        _connectionString = CreateConnectionString(Provider, _databasePath, Pooling);

        if (Pooling)
        {
            await using DbConnection warmConnection = CreateConnection();
            await warmConnection.OpenAsync();

            if (warmConnection is SqliteConnection sqliteConnection)
                await ApplyAndVerifySqlitePragmasAsync(sqliteConnection);

            await warmConnection.CloseAsync();
        }

        _reusedConnection = CreateConnection();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_reusedConnection is not null)
            await _reusedConnection.DisposeAsync();

        await CSharpDbConnection.ClearAllPoolsAsync();
        SqliteConnection.ClearAllPools();

        DeleteIfExists(_databasePath);
        DeleteIfExists(_databasePath + ".wal");
        DeleteIfExists(_databasePath + "-wal");
        DeleteIfExists(_databasePath + "-shm");
        DeleteIfExists(_databasePath + "-journal");
    }

    [Benchmark(Baseline = true, Description = "Reuse connection object: OpenAsync+CloseAsync")]
    public async Task ReusedConnection_OpenCloseAsync()
    {
        await _reusedConnection.OpenAsync();
        await _reusedConnection.CloseAsync();
    }

    [Benchmark(Description = "New connection: construct+OpenAsync+CloseAsync+DisposeAsync")]
    public async Task NewConnection_OpenCloseDisposeAsync()
    {
        await using DbConnection connection = CreateConnection();
        await connection.OpenAsync();
        await connection.CloseAsync();
    }

    private async Task PrepareDatabaseAsync()
    {
        string preparationConnectionString = CreateConnectionString(
            Provider,
            _databasePath,
            pooling: false);

        await using DbConnection connection = CreateConnection(Provider, preparationConnectionString);
        await connection.OpenAsync();

        if (connection is SqliteConnection sqliteConnection)
            await ApplyAndVerifySqlitePragmasAsync(sqliteConnection);

        await using DbCommand command = connection.CreateCommand();
        command.CommandText =
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT);";
        await command.ExecuteNonQueryAsync();
    }

    private DbConnection CreateConnection()
        => CreateConnection(Provider, _connectionString);

    private static DbConnection CreateConnection(
        LifecycleProvider provider,
        string connectionString)
        => provider switch
        {
            LifecycleProvider.CSharpDb => new CSharpDbConnection(connectionString),
            LifecycleProvider.Sqlite => new SqliteConnection(connectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static string CreateConnectionString(
        LifecycleProvider provider,
        string databasePath,
        bool pooling)
        => provider switch
        {
            LifecycleProvider.CSharpDb =>
                $"Data Source={databasePath};Pooling={pooling};Max Pool Size=16;Storage Preset=WriteOptimized;Embedded Open Mode=Direct",
            LifecycleProvider.Sqlite => new SqliteConnectionStringBuilder
            {
                DataSource = databasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = pooling,
                DefaultTimeout = 30,
            }.ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static async Task ApplyAndVerifySqlitePragmasAsync(
        SqliteConnection connection)
    {
        await using (SqliteCommand journal = connection.CreateCommand())
        {
            journal.CommandText = "PRAGMA journal_mode=WAL;";
            string journalMode =
                (Convert.ToString(await journal.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Expected SQLite journal_mode=wal, observed '{journalMode}'.");
            }
        }

        await using (SqliteCommand synchronous = connection.CreateCommand())
        {
            synchronous.CommandText = "PRAGMA synchronous=FULL;";
            await synchronous.ExecuteNonQueryAsync();
        }

        await using (SqliteCommand verifySynchronous = connection.CreateCommand())
        {
            verifySynchronous.CommandText = "PRAGMA synchronous;";
            string synchronousMode =
                (Convert.ToString(await verifySynchronous.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!synchronousMode.Equals("full", StringComparison.OrdinalIgnoreCase) &&
                !synchronousMode.Equals("2", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Expected SQLite synchronous=FULL, observed '{synchronousMode}'.");
            }
        }
    }

    private static void DeleteIfExists(string? path)
    {
        if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
            File.Delete(path);
    }

    public enum LifecycleProvider
    {
        CSharpDb,
        Sqlite,
    }
}
