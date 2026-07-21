using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ConnectionOpenPlanTests : IAsyncLifetime
{
    private readonly string _firstPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_open_plan_first_{Guid.NewGuid():N}.db");
    private readonly string _secondPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_open_plan_second_{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteDatabaseFiles(_firstPath);
        DeleteDatabaseFiles(_secondPath);
    }

    [Fact]
    public async Task Reopen_AfterConnectionStringChange_UsesNewPreparedTarget()
    {
        string firstConnectionString = $"Data Source={_firstPath};Pooling=true";
        string secondConnectionString = $"Data Source={_secondPath};Pooling=true";

        await using var connection = new CSharpDbConnection(firstConnectionString);
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(connection, "CREATE TABLE first_target (id INTEGER PRIMARY KEY);");
        await connection.CloseAsync();

        connection.ConnectionString = secondConnectionString;
        Assert.Equal(_secondPath, connection.DataSource);

        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(connection, "CREATE TABLE second_target (id INTEGER PRIMARY KEY);");
        await connection.CloseAsync();

        await using var first = new CSharpDbConnection(firstConnectionString);
        await first.OpenAsync(Ct);
        Assert.Contains("first_target", first.GetTableNames());
        Assert.DoesNotContain("second_target", first.GetTableNames());

        await using var second = new CSharpDbConnection(secondConnectionString);
        await second.OpenAsync(Ct);
        Assert.Contains("second_target", second.GetTableNames());
        Assert.DoesNotContain("first_target", second.GetTableNames());
    }

    [Fact]
    public async Task ShortLivedPooledConnections_SharePreparedAbsoluteOpenPlan()
    {
        string connectionString =
            $"Data Source={_firstPath};Pooling=true;Max Pool Size=1;" +
            "Storage Preset=WriteOptimized;Embedded Open Mode=Direct";

        Assert.False(CSharpDbConnection.HasSharedPooledOpenPlanForTest(connectionString));

        await using (var first = new CSharpDbConnection(connectionString))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        Assert.True(CSharpDbConnection.HasSharedPooledOpenPlanForTest(connectionString));

        await using (var second = new CSharpDbConnection(connectionString))
        {
            await second.OpenAsync(Ct);
            await second.CloseAsync();
        }
    }

    [Fact]
    public async Task SharedPreparedPlan_DoesNotOverrideExplicitOptions()
    {
        string connectionString = $"Data Source={_firstPath};Pooling=true;Max Pool Size=1";

        await using (var defaultConnection = new CSharpDbConnection(connectionString))
        {
            await defaultConnection.OpenAsync(Ct);
            await defaultConnection.CloseAsync();
        }

        Assert.True(CSharpDbConnection.HasSharedPooledOpenPlanForTest(connectionString));

        var factory = new CountingStorageEngineFactory();
        await using var configuredConnection = new CSharpDbConnection(
            connectionString,
            new DatabaseOptions { StorageEngineFactory = factory });
        await configuredConnection.OpenAsync(Ct);
        await configuredConnection.CloseAsync();

        Assert.Equal(1, factory.OpenCount);
    }

    [Fact]
    public async Task Reopen_AfterDirectOptionsChange_UsesNewPreparedConfiguration()
    {
        string connectionString = $"Data Source={_firstPath};Pooling=true;Max Pool Size=1";
        var firstFactory = new CountingStorageEngineFactory();
        var secondFactory = new CountingStorageEngineFactory();

        await using var connection = new CSharpDbConnection(
            connectionString,
            new DatabaseOptions { StorageEngineFactory = firstFactory });

        await connection.OpenAsync(Ct);
        await connection.CloseAsync();
        Assert.Equal(1, firstFactory.OpenCount);

        connection.DirectDatabaseOptions = new DatabaseOptions
        {
            StorageEngineFactory = secondFactory,
        };

        await connection.OpenAsync(Ct);
        await connection.CloseAsync();

        Assert.Equal(1, firstFactory.OpenCount);
        Assert.Equal(1, secondFactory.OpenCount);
    }

    [Fact]
    public async Task Reopen_AfterHybridOptionsChange_UsesNewPreparedConfiguration()
    {
        string connectionString = $"Data Source={_firstPath};Pooling=true;Max Pool Size=1";
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
        };

        await using var connection = new CSharpDbConnection(connectionString);
        await connection.OpenAsync(Ct);
        await connection.CloseAsync();

        connection.HybridDatabaseOptions = hybridOptions;
        await connection.OpenAsync(Ct);
        await connection.CloseAsync();

        Assert.Equal(
            1,
            CSharpDbConnection.GetIdlePoolSizeForTest(
                connectionString,
                directDatabaseOptions: null,
                hybridDatabaseOptions: hybridOptions));
    }

    [Fact]
    public async Task Open_InvalidFileLoadFrom_PrecedesMalformedPoolSettings()
    {
        await using var connection = new CSharpDbConnection(
            $"Data Source={_firstPath};Load From={_secondPath};" +
            "Pooling=not-a-boolean;Max Pool Size=not-an-integer");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connection.OpenAsync(Ct));

        Assert.Contains(
            "Load From is only supported for in-memory data sources",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reopen_WithRelativeDataSource_TracksCurrentDirectory()
    {
        string originalDirectory = Environment.CurrentDirectory;
        string root = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_open_plan_relative_{Guid.NewGuid():N}");
        string firstDirectory = Path.Combine(root, "first");
        string secondDirectory = Path.Combine(root, "second");
        Directory.CreateDirectory(firstDirectory);
        Directory.CreateDirectory(secondDirectory);

        try
        {
            string connectionString = "Data Source=relative.db;Pooling=true";
            await using var connection = new CSharpDbConnection(
                connectionString);

            Environment.CurrentDirectory = firstDirectory;
            await connection.OpenAsync(Ct);
            await ExecuteNonQueryAsync(
                connection,
                "CREATE TABLE first_relative_target (id INTEGER PRIMARY KEY);");
            await connection.CloseAsync();
            Assert.False(CSharpDbConnection.HasSharedPooledOpenPlanForTest(connectionString));

            Environment.CurrentDirectory = secondDirectory;
            await connection.OpenAsync(Ct);
            await ExecuteNonQueryAsync(
                connection,
                "CREATE TABLE second_relative_target (id INTEGER PRIMARY KEY);");
            await connection.CloseAsync();

            Assert.True(File.Exists(Path.Combine(firstDirectory, "relative.db")));
            Assert.True(File.Exists(Path.Combine(secondDirectory, "relative.db")));
        }
        finally
        {
            Environment.CurrentDirectory = originalDirectory;
            await CSharpDbConnection.ClearAllPoolsAsync();
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch
            {
                // Best-effort cleanup for temporary test files.
            }
        }
    }

    private static async Task ExecuteNonQueryAsync(CSharpDbConnection connection, string sql)
    {
        await using CSharpDbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static void DeleteDatabaseFiles(string path)
    {
        try
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
        catch
        {
            // Best-effort cleanup for temporary test files.
        }
    }

    private sealed class CountingStorageEngineFactory : IStorageEngineFactory
    {
        private readonly DefaultStorageEngineFactory _inner = new();
        private int _openCount;

        internal int OpenCount => Volatile.Read(ref _openCount);

        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            Interlocked.Increment(ref _openCount);
            return _inner.OpenAsync(filePath, options, ct);
        }
    }
}
