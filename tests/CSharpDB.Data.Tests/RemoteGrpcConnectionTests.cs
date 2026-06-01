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
    public async Task TempTables_RequireTransactionSession_OverGrpcDaemon()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using (var rejected = conn.CreateCommand())
        {
            rejected.CommandText = "CREATE TEMP TABLE grpc_temp (id INTEGER PRIMARY KEY);";
            var ex = await Assert.ThrowsAsync<CSharpDbDataException>(() => rejected.ExecuteNonQueryAsync(Ct));
            Assert.Contains("transaction session", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        await using var tx = await conn.BeginTransactionAsync(Ct);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TEMP TABLE grpc_temp (id INTEGER PRIMARY KEY);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO grpc_temp VALUES (1);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "SELECT COUNT(*) FROM grpc_temp;";
            Assert.Equal(1L, await cmd.ExecuteScalarAsync(Ct));
        }

        await tx.CommitAsync(Ct);
    }

    [Fact]
    public async Task ShardRouteConnectionString_RoutesCommandsToOneShard()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_data_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            await using var factory = new TestDaemonFactory(
                Path.Combine(directory, "unused.db"),
                CreateShardingConfig(directory));
            using var transportClient = CreateGrpcHttpClient(factory);

            await using var tenantA = new CSharpDbConnection(
                "Transport=Grpc;Endpoint=http://localhost;Shard Keyspace=tenants;Shard Key=tenant-a",
                transportClient);
            await tenantA.OpenAsync(Ct);
            using (var cmd = tenantA.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE routed_ado (id INTEGER PRIMARY KEY, name TEXT);";
                await cmd.ExecuteNonQueryAsync(Ct);
                cmd.CommandText = "INSERT INTO routed_ado VALUES (1, 'tenant-a');";
                await cmd.ExecuteNonQueryAsync(Ct);
            }

            await using var tenantB = new CSharpDbConnection(
                "Transport=Grpc;Endpoint=http://localhost;Shard Keyspace=tenants;Shard Key=tenant-b",
                transportClient);
            await tenantB.OpenAsync(Ct);
            using (var cmd = tenantB.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE routed_ado (id INTEGER PRIMARY KEY, name TEXT);";
                await cmd.ExecuteNonQueryAsync(Ct);
                cmd.CommandText = "INSERT INTO routed_ado VALUES (1, 'tenant-b');";
                await cmd.ExecuteNonQueryAsync(Ct);
                cmd.CommandText = "SELECT name FROM routed_ado WHERE id = 1;";
                Assert.Equal("tenant-b", await cmd.ExecuteScalarAsync(Ct));
            }

            using (var verify = tenantA.CreateCommand())
            {
                verify.CommandText = "SELECT name FROM routed_ado WHERE id = 1;";
                Assert.Equal("tenant-a", await verify.ExecuteScalarAsync(Ct));
            }
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
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

    private static IReadOnlyDictionary<string, string?> CreateShardingConfig(string directory)
        => new Dictionary<string, string?>
        {
            ["CSharpDB:Sharding:Enabled"] = "true",
            ["CSharpDB:Sharding:Keyspace"] = "tenants",
            ["CSharpDB:Sharding:MapVersion"] = "1",
            ["CSharpDB:Sharding:VirtualBucketCount"] = "4",
            ["CSharpDB:Sharding:Shards:0:ShardId"] = "s0",
            ["CSharpDB:Sharding:Shards:0:DataSource"] = Path.Combine(directory, "s0.db"),
            ["CSharpDB:Sharding:Shards:1:ShardId"] = "s1",
            ["CSharpDB:Sharding:Shards:1:DataSource"] = Path.Combine(directory, "s1.db"),
            ["CSharpDB:Sharding:BucketRanges:0:StartBucketInclusive"] = "0",
            ["CSharpDB:Sharding:BucketRanges:0:EndBucketExclusive"] = "2",
            ["CSharpDB:Sharding:BucketRanges:0:ShardId"] = "s0",
            ["CSharpDB:Sharding:BucketRanges:1:StartBucketInclusive"] = "2",
            ["CSharpDB:Sharding:BucketRanges:1:EndBucketExclusive"] = "4",
            ["CSharpDB:Sharding:BucketRanges:1:ShardId"] = "s1",
            ["CSharpDB:Sharding:ExactKeyPins:tenant-a"] = "s0",
            ["CSharpDB:Sharding:ExactKeyPins:tenant-b"] = "s1",
        };

    private sealed class TestDaemonFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                var values = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                };

                if (extraConfig is not null)
                {
                    foreach (var pair in extraConfig)
                        values[pair.Key] = pair.Value;
                }

                config.AddInMemoryCollection(values);
            });
        }
    }
}
