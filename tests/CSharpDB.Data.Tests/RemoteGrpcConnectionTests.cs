using System.Net;
using System.Data;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Data;
using CSharpDB.Engine;
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
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateShardingOptions(directory));
            await using var factory = new TestDaemonFactory(masterDbPath);
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

    [Fact]
    public async Task GetSchema_RemoteGrpcConnection_PreservesDefaultsChecksAndLogicalKeys()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            CREATE TABLE grpc_data_metadata (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL,
                code TEXT DEFAULT 'new',
                score INTEGER,
                CONSTRAINT ck_grpc_data_score CHECK (score >= 0),
                CONSTRAINT uq_grpc_data_tenant_code UNIQUE (tenant, code)
            );
            """;
        await cmd.ExecuteNonQueryAsync(Ct);

        CSharpDB.Primitives.TableSchema? schema = conn.GetTableSchema("grpc_data_metadata");

        Assert.NotNull(schema);
        Assert.Equal("'new'", Assert.Single(schema!.Columns, column => column.Name == "code").DefaultSql);
        CSharpDB.Primitives.CheckConstraintDefinition check = Assert.Single(schema.CheckConstraints);
        Assert.Equal("ck_grpc_data_score", check.ConstraintName);
        CSharpDB.Primitives.KeyConstraintDefinition unique = Assert.Single(
            schema.KeyConstraints,
            key => key.Kind == CSharpDB.Primitives.KeyConstraintKind.Unique);
        Assert.Equal(["tenant", "code"], unique.Columns);

        DataRow column = Assert.Single(
            conn.GetSchema("Columns", [null, null, "grpc_data_metadata", "code"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("'new'", column["COLUMN_DEFAULT"]);

        DataRow checkRow = Assert.Single(
            conn.GetSchema(
                    "CheckConstraints",
                    [null, null, "grpc_data_metadata", "ck_grpc_data_score"])
                .Rows
                .Cast<DataRow>());
        Assert.Contains("\"score\"", (string)checkRow["CHECK_CLAUSE"], StringComparison.Ordinal);

        DataRow[] keyColumns = conn.GetSchema(
                "KeyColumns",
                [null, null, "grpc_data_metadata", "uq_grpc_data_tenant_code"])
            .Rows
            .Cast<DataRow>()
            .OrderBy(row => (int)row["ORDINAL_POSITION"])
            .ToArray();
        Assert.Equal(["tenant", "code"], keyColumns.Select(row => (string)row["COLUMN_NAME"]));
        Assert.Equal([1, 2], keyColumns.Select(row => (int)row["ORDINAL_POSITION"]));
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

    private static async Task SeedMasterCatalogAsync(string masterDbPath, CSharpDbShardingOptions options)
    {
        await CSharpDbShardedClient.SeedMasterCatalogAsync(
            new CSharpDbClientOptions
            {
                DataSource = masterDbPath,
                DirectDatabaseOptions = CreateSeedDirectDatabaseOptions(),
                HybridDatabaseOptions = new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
            },
            options,
            Ct);
    }

    private static DatabaseOptions CreateSeedDirectDatabaseOptions()
        => new DatabaseOptions
        {
            ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
        }.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

    private static CSharpDbShardingOptions CreateShardingOptions(string directory)
        => new()
        {
            Keyspace = "tenants",
            MapVersion = 1,
            VirtualBucketCount = 4,
            Shards =
            [
                new CSharpDbShardDefinition { ShardId = "s0", DataSource = Path.Combine(directory, "s0.db") },
                new CSharpDbShardDefinition { ShardId = "s1", DataSource = Path.Combine(directory, "s1.db") },
            ],
            BucketRanges =
            [
                new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 2, ShardId = "s0" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 2, EndBucketExclusive = 4, ShardId = "s1" },
            ],
            ExactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant-a"] = "s0",
                ["tenant-b"] = "s1",
            },
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
