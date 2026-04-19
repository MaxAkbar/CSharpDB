using System.Data.Common;
using CSharpDB.Data;
using CSharpDB.Primitives;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ComparativeAdoNetSmokeTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_adonet_compare_{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();

        if (Directory.Exists(_workspace))
            Directory.Delete(_workspace, recursive: true);
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task PreparedInsertAndQueryRoundTrip_Succeeds(ProviderKind provider)
    {
        string connectionTarget = GetFileConnectionTarget(provider, "roundtrip");

        await using DbConnection connection = await OpenConnectionAsync(provider, connectionTarget);
        await CreateSchemaAsync(connection);

        using (DbCommand insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO bench VALUES (@id, @value, @text_col, @category);";
            AddParameter(insert, "@id", 1);
            AddParameter(insert, "@value", 42L);
            AddParameter(insert, "@text_col", "durable");
            AddParameter(insert, "@category", "Alpha");
            insert.Prepare();

            Assert.Equal(1, await insert.ExecuteNonQueryAsync(Ct));
        }

        using DbCommand query = connection.CreateCommand();
        query.CommandText = "SELECT value, text_col, category FROM bench WHERE id = @id;";
        AddParameter(query, "@id", 1);

        await using DbDataReader reader = await query.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(42L, Convert.ToInt64(reader.GetValue(0)));
        Assert.Equal("durable", reader.GetString(1));
        Assert.Equal("Alpha", reader.GetString(2));
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ConcurrentPreparedDisjointWriters_ProduceExpectedRowCount(ProviderKind provider)
    {
        string connectionTarget = GetSharedConnectionTarget(provider, "concurrent");

        await using DbConnection setup = await OpenConnectionAsync(provider, connectionTarget);
        await CreateSchemaAsync(setup);

        const int writerCount = 4;
        const int insertsPerWriter = 32;
        Task[] writers = Enumerable.Range(0, writerCount)
            .Select(writerId => RunWriterAsync(provider, connectionTarget, writerId, insertsPerWriter))
            .ToArray();

        await Task.WhenAll(writers);

        await using DbConnection verify = await OpenConnectionAsync(provider, connectionTarget);
        using DbCommand count = verify.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM bench;";

        object? scalar = await count.ExecuteScalarAsync(Ct);
        Assert.Equal(writerCount * insertsPerWriter, Convert.ToInt32(scalar));
    }

    private async Task RunWriterAsync(ProviderKind provider, string connectionTarget, int writerId, int insertsPerWriter)
    {
        await using DbConnection connection = await OpenConnectionAsync(provider, connectionTarget);
        using DbCommand command = connection.CreateCommand();
        command.CommandText = "INSERT INTO bench VALUES (@id, @value, @text_col, @category);";
        DbParameter idParam = AddParameter(command, "@id", 0);
        DbParameter valueParam = AddParameter(command, "@value", 0);
        DbParameter textParam = AddParameter(command, "@text_col", "durable");
        DbParameter categoryParam = AddParameter(command, "@category", "Alpha");
        command.Prepare();

        for (int i = 0; i < insertsPerWriter; i++)
        {
            int id = (writerId * 10_000) + i + 1;
            idParam.Value = id;
            valueParam.Value = writerId;
            textParam.Value = "durable";
            categoryParam.Value = "Alpha";

            await ExecuteWithBusyRetryAsync(provider, () => command.ExecuteNonQueryAsync(Ct));
        }
    }

    private static async Task ExecuteWithBusyRetryAsync(ProviderKind provider, Func<Task<int>> operation)
    {
        const int maxAttempts = 40;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                int rowsAffected = await operation();
                Assert.Equal(1, rowsAffected);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && IsBusy(provider, ex))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5), Ct);
            }
        }
    }

    private static bool IsBusy(ProviderKind provider, Exception ex)
        => provider switch
        {
            ProviderKind.CSharpDb => FindException<CSharpDbDataException>(ex, static dataEx => dataEx.ErrorCode == ErrorCode.Busy),
            ProviderKind.Sqlite => FindException<SqliteException>(ex, static sqliteEx => sqliteEx.SqliteErrorCode is 5 or 6),
            _ => false,
        };

    private static bool FindException<TException>(Exception? ex, Func<TException, bool> predicate)
        where TException : Exception
    {
        while (ex is not null)
        {
            if (ex is TException typed && predicate(typed))
                return true;

            ex = ex.InnerException;
        }

        return false;
    }

    private static async Task<DbConnection> OpenConnectionAsync(ProviderKind provider, string connectionTarget)
    {
        switch (provider)
        {
            case ProviderKind.CSharpDb:
            {
                var connection = new CSharpDbConnection(connectionTarget);
                await connection.OpenAsync(Ct);
                return connection;
            }
            case ProviderKind.Sqlite:
            {
                var connection = new SqliteConnection(connectionTarget);

                await connection.OpenAsync(Ct);
                bool isSharedMemory = connectionTarget.Contains("Mode=Memory", StringComparison.OrdinalIgnoreCase);
                await ConfigureSqliteConnectionAsync(connection, isSharedMemory);
                return connection;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(provider), provider, null);
        }
    }

    private static async Task ConfigureSqliteConnectionAsync(SqliteConnection connection, bool isSharedMemory)
    {
        using (var busyTimeout = connection.CreateCommand())
        {
            busyTimeout.CommandText = "PRAGMA busy_timeout=1000;";
            await busyTimeout.ExecuteNonQueryAsync(Ct);
        }

        using (var journal = connection.CreateCommand())
        {
            journal.CommandText = isSharedMemory ? "PRAGMA journal_mode=MEMORY;" : "PRAGMA journal_mode=WAL;";
            string mode = (Convert.ToString(await journal.ExecuteScalarAsync(Ct)) ?? string.Empty).Trim();
            string expectedMode = isSharedMemory ? "memory" : "wal";
            Assert.True(string.Equals(mode, expectedMode, StringComparison.OrdinalIgnoreCase), $"Expected SQLite journal_mode={expectedMode}, observed '{mode}'.");
        }

        if (!isSharedMemory)
        {
            using var sync = connection.CreateCommand();
            sync.CommandText = "PRAGMA synchronous=FULL;";
            await sync.ExecuteNonQueryAsync(Ct);
        }
    }

    private static async Task CreateSchemaAsync(DbConnection connection)
    {
        using DbCommand create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT);";
        await create.ExecuteNonQueryAsync(Ct);
    }

    private static DbParameter AddParameter(DbCommand command, string name, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private string GetFileConnectionTarget(ProviderKind provider, string suffix)
        => provider switch
        {
            ProviderKind.CSharpDb => $"Data Source={Path.Combine(_workspace, $"{provider.ToString().ToLowerInvariant()}-{suffix}.db")}",
            ProviderKind.Sqlite => new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(_workspace, $"{provider.ToString().ToLowerInvariant()}-{suffix}.db"),
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            }.ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static string GetSharedConnectionTarget(ProviderKind provider, string suffix)
    {
        string sharedName = $"compare_{suffix}_{Guid.NewGuid():N}";
        return provider switch
        {
            ProviderKind.CSharpDb => $"Data Source=:memory:{sharedName}",
            ProviderKind.Sqlite => new SqliteConnectionStringBuilder
            {
                DataSource = sharedName,
                Mode = SqliteOpenMode.Memory,
                Cache = SqliteCacheMode.Shared,
                Pooling = false,
                DefaultTimeout = 30,
            }.ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };
    }

    public enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }
}
