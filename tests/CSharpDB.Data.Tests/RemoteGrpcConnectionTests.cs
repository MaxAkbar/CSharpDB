using System.Net;
using CSharpDB.Data;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Data.Tests;

public sealed class RemoteGrpcConnectionTests : IAsyncLifetime
{
    private string _dbPath = null!;
    private TestDaemonFactory _factory = null!;
    private HttpClient _transportClient = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_data_grpc_{Guid.NewGuid():N}.db");
        _factory = new TestDaemonFactory(_dbPath);
        _transportClient = CreateGrpcHttpClient(_factory);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _transportClient.Dispose();
        await _factory.DisposeAsync();
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public void DataSource_ReturnsEndpoint_ForGrpcConnectionStrings()
    {
        using var conn = CreateConnection();
        Assert.Equal("http://localhost", conn.DataSource);
    }

    [Fact]
    public async Task OpenAsync_ExecutesSqlOverGrpcDaemon()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO users VALUES (1, 'Ada');";
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.CommandText = "SELECT name FROM users WHERE id = 1;";
        Assert.Equal("Ada", await cmd.ExecuteScalarAsync(Ct));

        Assert.Contains("users", conn.GetTableNames());
    }

    [Fact]
    public async Task Prepare_RemoteGrpcConnections_FallBackToSqlBinding()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO items VALUES (@id, @name);";
        var id = cmd.Parameters.AddWithValue("@id", 1);
        var name = cmd.Parameters.AddWithValue("@name", "first");
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        id.Value = 2;
        name.Value = "second";
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT COUNT(*) FROM items;";
        Assert.Equal(2L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Transactions_CommitAndRollback_OverGrpcDaemon()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, message TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO audit_log VALUES (1, 'committed');";
            await cmd.ExecuteNonQueryAsync(Ct);
            await tx.CommitAsync(Ct);
        }

        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO audit_log VALUES (2, 'rolled back');";
            await cmd.ExecuteNonQueryAsync(Ct);
            await tx.RollbackAsync(Ct);
        }

        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM audit_log;";
        Assert.Equal(1L, await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task GetSchema_RemoteGrpcConnection_UsesDaemonMetadata()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT COLLATE NOCASE, qty INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        var schema = conn.GetTableSchema("products");
        Assert.NotNull(schema);
        Assert.Equal(3, schema!.Columns.Count);
        Assert.Equal("sku", schema.Columns[1].Name);
        Assert.Equal(CSharpDB.Primitives.DbType.Text, schema.Columns[1].Type);
        Assert.Equal("NOCASE", schema.Columns[1].Collation);
    }

    private CSharpDbConnection CreateConnection()
        => new("Transport=Grpc;Endpoint=http://localhost", _transportClient);

    private static HttpClient CreateGrpcHttpClient(TestDaemonFactory factory)
    {
        return new HttpClient(factory.Server.CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Ignore transient test cleanup file locks.
        }
    }

    private sealed class TestDaemonFactory(string dbPath) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                });
            });
        }
    }
}
