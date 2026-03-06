using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public sealed class ConnectionPoolingTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _dbPathNoPool;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ConnectionPoolingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pool_test_{Guid.NewGuid():N}.db");
        _dbPathNoPool = Path.Combine(Path.GetTempPath(), $"csharpdb_pool_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_dbPath);
        DeleteIfExists(_dbPath + ".wal");
        DeleteIfExists(_dbPathNoPool);
        DeleteIfExists(_dbPathNoPool + ".wal");
    }

    [Fact]
    public async Task OpenClose_WithPoolingEnabled_StoresAndRentsIdleDatabase()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var first = new CSharpDbConnection(cs))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));

        await using (var second = new CSharpDbConnection(cs))
        {
            await second.OpenAsync(Ct);
            Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
            await second.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
    }

    [Fact]
    public async Task ClearPoolAsync_RemovesIdleEntries()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var conn = new CSharpDbConnection(cs))
        {
            await conn.OpenAsync(Ct);
            await conn.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));

        await CSharpDbConnection.ClearPoolAsync(cs);

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
    }

    [Fact]
    public async Task OpenClose_WithPoolingDisabled_DoesNotPopulatePool()
    {
        string cs = $"Data Source={_dbPathNoPool};Pooling=false";

        await using var conn = new CSharpDbConnection(cs);
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
    }

    [Fact]
    public async Task OpenAsync_CanceledPooledOpen_DoesNotReturnLaterDatabaseToStalePool()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPathNoPool};Pooling=false";

        await using var conn = new CSharpDbConnection(pooledCs);
        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.OpenAsync(cts.Token));
        }

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));

        conn.ConnectionString = nonPooledCs;
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));
    }

    [Fact]
    public void ConnectionStringBuilder_ParsesPoolingOptions()
    {
        var csb = new CSharpDbConnectionStringBuilder("Data Source=my.db;Pooling=true;Max Pool Size=7");

        Assert.Equal("my.db", csb.DataSource);
        Assert.True(csb.Pooling);
        Assert.Equal(7, csb.MaxPoolSize);
    }

    [Fact]
    public void ConnectionStringBuilder_UsesPoolingDefaults_WhenNotConfigured()
    {
        var csb = new CSharpDbConnectionStringBuilder("Data Source=my.db");

        Assert.False(csb.Pooling);
        Assert.Equal(CSharpDbConnectionStringBuilder.DefaultMaxPoolSize, csb.MaxPoolSize);
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temp benchmark/test files.
        }
    }
}
