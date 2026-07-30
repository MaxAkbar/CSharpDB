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
    public async Task QuerySchema_AllNullIntegerColumn_PreservesDeclaredTypeOverGrpc()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE grpc_nullable_type (value INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO grpc_nullable_type VALUES (NULL);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "SELECT value FROM grpc_nullable_type;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal("INTEGER", reader.GetDataTypeName(0));
        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.True(await reader.ReadAsync(Ct));
        Assert.True(reader.IsDBNull(0));
    }

    [Fact]
    public async Task PhysicalExplainSchema_IsEquivalentAcrossDirectHttpAndGrpc()
    {
        await using var directConnection =
            new CSharpDbConnection("Data Source=:memory:");
        using HttpClient httpTransportClient = _factory.CreateClient();
        await using var httpConnection =
            new CSharpDbConnection(
                "Transport=Http;Endpoint=http://localhost",
                httpTransportClient);
        await using CSharpDbConnection grpcConnection =
            CreateConnection();

        await directConnection.OpenAsync(Ct);
        await httpConnection.OpenAsync(Ct);
        await grpcConnection.OpenAsync(Ct);

        string[] directSchema =
            await CapturePhysicalExplainSchemaAsync(directConnection);
        Assert.Equal(
            directSchema,
            await CapturePhysicalExplainSchemaAsync(httpConnection));
        Assert.Equal(
            directSchema,
            await CapturePhysicalExplainSchemaAsync(grpcConnection));

        Assert.Contains("node_id|INTEGER|False", directSchema);
        Assert.Contains("operator_type|TEXT|False", directSchema);
        Assert.Contains("status|TEXT|False", directSchema);
        Assert.Contains("parent_node_id|INTEGER|True", directSchema);
        Assert.Contains("estimated_cost|REAL|True", directSchema);
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

    [Fact]
    public async Task GetSchema_OrdinarySqlMetadata_IsEquivalentAcrossDirectHttpAndGrpc()
    {
        await using var directConnection =
            new CSharpDbConnection("Data Source=:memory:");
        using HttpClient httpTransportClient = _factory.CreateClient();
        await using var httpConnection =
            new CSharpDbConnection(
                "Transport=Http;Endpoint=http://localhost",
                httpTransportClient);
        await using CSharpDbConnection grpcConnection =
            CreateConnection();

        await directConnection.OpenAsync(Ct);
        await httpConnection.OpenAsync(Ct);
        await grpcConnection.OpenAsync(Ct);

        await CreateOrdinarySchemaParityFixtureAsync(
            directConnection);
        await CreateOrdinarySchemaParityFixtureAsync(
            httpConnection);

        string[] directSemantics =
            CaptureOrdinarySchemaSemantics(directConnection);
        string[] httpSemantics =
            CaptureOrdinarySchemaSemantics(httpConnection);
        string[] grpcSemantics =
            CaptureOrdinarySchemaSemantics(grpcConnection);

        Assert.Equal(directSemantics, httpSemantics);
        Assert.Equal(directSemantics, grpcSemantics);

        AssertOrdinarySchemaIdentityGraph(directConnection);
        AssertOrdinarySchemaIdentityGraph(httpConnection);
        AssertOrdinarySchemaIdentityGraph(grpcConnection);

        Assert.Equal(
            CaptureOrdinarySchemaIdentities(httpConnection),
            CaptureOrdinarySchemaIdentities(grpcConnection));

        DataRow index = Assert.Single(
            grpcConnection.GetSchema(
                    "Indexes",
                    [null, null, "parity_children", "ix_parity_children_label"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("Sql", index["INDEX_TYPE"]);
        Assert.Equal("Ready", index["INDEX_STATE"]);
        Assert.Equal("NOCASE", index["COLLATION_LIST"]);
    }

    private static async Task CreateOrdinarySchemaParityFixtureAsync(
        CSharpDbConnection connection)
    {
        foreach (string sql in
                 new[]
                 {
                     """
                     CREATE TABLE parity_parents (
                         tenant_id INTEGER NOT NULL,
                         code TEXT COLLATE NOCASE NOT NULL,
                         CONSTRAINT pk_parity_parents
                             PRIMARY KEY (tenant_id, code)
                     );
                     """,
                     """
                     CREATE TABLE parity_children (
                         id INTEGER NOT NULL,
                         tenant_id INTEGER,
                         parent_code TEXT COLLATE NOCASE,
                         label TEXT COLLATE NOCASE DEFAULT 'new',
                         score INTEGER,
                         CONSTRAINT pk_parity_children PRIMARY KEY (id),
                         CONSTRAINT ck_parity_children_score
                             CHECK (score >= 0),
                         CONSTRAINT fk_parity_children_parent
                             FOREIGN KEY (tenant_id, parent_code)
                             REFERENCES parity_parents (tenant_id, code)
                             ON DELETE CASCADE
                             ON UPDATE SET NULL
                     );
                     """,
                     // Keep this parity fixture on ordinary SQL indexes. The
                     // full-text kind/state transport contract is tracked
                     // separately.
                     """
                     CREATE INDEX ix_parity_children_label
                         ON parity_children (label);
                     """,
                 })
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(Ct);
        }
    }

    private static string[] CaptureOrdinarySchemaSemantics(
        CSharpDbConnection connection)
    {
        var values = new List<string>();
        AddSchemaRows(
            values,
            connection.GetSchema("Tables"),
            "Tables",
            "TABLE_NAME",
            "TABLE_TYPE");
        AddSchemaRows(
            values,
            connection.GetSchema("Columns"),
            "Columns",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "IS_PRIMARY_KEY",
            "IS_IDENTITY",
            "COLLATION_NAME",
            "IS_ROW_VERSION");
        AddSchemaRows(
            values,
            connection.GetSchema("CheckConstraints"),
            "CheckConstraints",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "CHECK_CLAUSE",
            "COLUMN_NAME");
        AddSchemaRows(
            values,
            connection.GetSchema("KeyConstraints"),
            "KeyConstraints",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "CONSTRAINT_TYPE",
            "BACKING_INDEX_NAME",
            "COLUMN_COUNT");
        AddSchemaRows(
            values,
            connection.GetSchema("KeyColumns"),
            "KeyColumns",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION");
        AddSchemaRows(
            values,
            connection.GetSchema("ForeignKeys"),
            "ForeignKeys",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "COLUMN_NAME",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME",
            "DELETE_RULE",
            "UPDATE_RULE",
            "SUPPORTING_INDEX_NAME",
            "ORDINAL_POSITION");
        AddSchemaRows(
            values,
            connection.GetSchema("Indexes"),
            "Indexes",
            "TABLE_NAME",
            "INDEX_NAME",
            "IS_UNIQUE",
            "INDEX_TYPE",
            "INDEX_STATE",
            "COLUMN_LIST",
            "COLLATION_LIST");
        values.Sort(StringComparer.Ordinal);
        return values.ToArray();
    }

    private static string[] CaptureOrdinarySchemaIdentities(
        CSharpDbConnection connection)
    {
        var values = new List<string>();
        AddSchemaRows(
            values,
            connection.GetSchema("Tables"),
            "Tables",
            "TABLE_NAME",
            "SCHEMA_ID");
        AddSchemaRows(
            values,
            connection.GetSchema("Columns"),
            "Columns",
            "TABLE_NAME",
            "COLUMN_NAME",
            "TABLE_SCHEMA_ID",
            "COLUMN_SCHEMA_ID");
        AddSchemaRows(
            values,
            connection.GetSchema("CheckConstraints"),
            "CheckConstraints",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "TABLE_SCHEMA_ID",
            "CONSTRAINT_SCHEMA_ID");
        AddSchemaRows(
            values,
            connection.GetSchema("KeyConstraints"),
            "KeyConstraints",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "TABLE_SCHEMA_ID",
            "CONSTRAINT_SCHEMA_ID");
        AddSchemaRows(
            values,
            connection.GetSchema("KeyColumns"),
            "KeyColumns",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "COLUMN_NAME",
            "TABLE_SCHEMA_ID",
            "CONSTRAINT_SCHEMA_ID",
            "COLUMN_SCHEMA_ID");
        AddSchemaRows(
            values,
            connection.GetSchema("ForeignKeys"),
            "ForeignKeys",
            "TABLE_NAME",
            "CONSTRAINT_NAME",
            "COLUMN_NAME",
            "TABLE_SCHEMA_ID",
            "CONSTRAINT_SCHEMA_ID",
            "COLUMN_SCHEMA_ID",
            "REFERENCED_TABLE_SCHEMA_ID",
            "REFERENCED_COLUMN_SCHEMA_ID",
            "REFERENCED_KEY_SCHEMA_ID");
        values.Sort(StringComparer.Ordinal);
        return values.ToArray();
    }

    private static void AddSchemaRows(
        ICollection<string> destination,
        DataTable table,
        string collectionName,
        params string[] columnNames)
    {
        foreach (DataRow row in table.Rows)
        {
            destination.Add(
                $"{collectionName}|{string.Join("|", columnNames.Select(columnName => FormatSchemaValue(row[columnName])))}");
        }
    }

    private static string FormatSchemaValue(object value)
        => value is DBNull
            ? "<null>"
            : Convert.ToString(
                value,
                CultureInfo.InvariantCulture) ?? "<null>";

    private static void AssertOrdinarySchemaIdentityGraph(
        CSharpDbConnection connection)
    {
        Dictionary<string, Guid> tableIds = connection.GetSchema("Tables")
            .Rows
            .Cast<DataRow>()
            .ToDictionary(
                row => (string)row["TABLE_NAME"],
                row => AssertSchemaId(row, "SCHEMA_ID"),
                StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, tableIds.Count);

        Dictionary<(string Table, string Column), Guid> columnIds =
            connection.GetSchema("Columns")
                .Rows
                .Cast<DataRow>()
                .ToDictionary(
                    row => (
                        (string)row["TABLE_NAME"],
                        (string)row["COLUMN_NAME"]),
                    row =>
                    {
                        Assert.Equal(
                            tableIds[(string)row["TABLE_NAME"]],
                            AssertSchemaId(row, "TABLE_SCHEMA_ID"));
                        return AssertSchemaId(
                            row,
                            "COLUMN_SCHEMA_ID");
                    });

        Dictionary<(string Table, string Constraint), Guid> keyIds =
            connection.GetSchema("KeyConstraints")
                .Rows
                .Cast<DataRow>()
                .ToDictionary(
                    row => (
                        (string)row["TABLE_NAME"],
                        (string)row["CONSTRAINT_NAME"]),
                    row =>
                    {
                        Assert.Equal(
                            tableIds[(string)row["TABLE_NAME"]],
                            AssertSchemaId(row, "TABLE_SCHEMA_ID"));
                        return AssertSchemaId(
                            row,
                            "CONSTRAINT_SCHEMA_ID");
                    });

        foreach (DataRow row in
                 connection.GetSchema("KeyColumns")
                     .Rows
                     .Cast<DataRow>())
        {
            var key = (
                (string)row["TABLE_NAME"],
                (string)row["CONSTRAINT_NAME"]);
            var column = (
                (string)row["TABLE_NAME"],
                (string)row["COLUMN_NAME"]);
            Assert.Equal(
                tableIds[key.Item1],
                AssertSchemaId(row, "TABLE_SCHEMA_ID"));
            Assert.Equal(
                keyIds[key],
                AssertSchemaId(row, "CONSTRAINT_SCHEMA_ID"));
            Assert.Equal(
                columnIds[column],
                AssertSchemaId(row, "COLUMN_SCHEMA_ID"));
        }

        DataRow check = Assert.Single(
            connection.GetSchema(
                    "CheckConstraints",
                    [null, null, "parity_children", "ck_parity_children_score"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal(
            tableIds["parity_children"],
            AssertSchemaId(check, "TABLE_SCHEMA_ID"));
        AssertSchemaId(check, "CONSTRAINT_SCHEMA_ID");

        DataRow[] foreignKeyRows = connection.GetSchema(
                "ForeignKeys",
                [null, null, "parity_children", "fk_parity_children_parent"])
            .Rows
            .Cast<DataRow>()
            .OrderBy(row => (int)row["ORDINAL_POSITION"])
            .ToArray();
        Assert.Equal(2, foreignKeyRows.Length);
        Assert.Equal(
            ["tenant_id", "parent_code"],
            foreignKeyRows.Select(row => (string)row["COLUMN_NAME"]));
        Assert.Equal(
            ["tenant_id", "code"],
            foreignKeyRows.Select(
                row => (string)row["REFERENCED_COLUMN_NAME"]));
        Assert.All(
            foreignKeyRows,
            row =>
            {
                Assert.Equal(
                    "CASCADE",
                    row["DELETE_RULE"]);
                Assert.Equal(
                    "SET NULL",
                    row["UPDATE_RULE"]);
                Assert.Equal(
                    tableIds["parity_children"],
                    AssertSchemaId(row, "TABLE_SCHEMA_ID"));
                Assert.Equal(
                    columnIds[
                        ("parity_children",
                            (string)row["COLUMN_NAME"])],
                    AssertSchemaId(row, "COLUMN_SCHEMA_ID"));
                Assert.Equal(
                    tableIds["parity_parents"],
                    AssertSchemaId(
                        row,
                        "REFERENCED_TABLE_SCHEMA_ID"));
                Assert.Equal(
                    columnIds[
                        ("parity_parents",
                            (string)row["REFERENCED_COLUMN_NAME"])],
                    AssertSchemaId(
                        row,
                        "REFERENCED_COLUMN_SCHEMA_ID"));
                Assert.Equal(
                    keyIds[
                        ("parity_parents",
                            "pk_parity_parents")],
                    AssertSchemaId(
                        row,
                        "REFERENCED_KEY_SCHEMA_ID"));
                AssertSchemaId(
                    row,
                    "CONSTRAINT_SCHEMA_ID");
            });
        Assert.Single(
            foreignKeyRows
                .Select(row => (Guid)row["CONSTRAINT_SCHEMA_ID"])
                .Distinct());
    }

    private static Guid AssertSchemaId(
        DataRow row,
        string columnName)
    {
        Guid id = Assert.IsType<Guid>(row[columnName]);
        Assert.NotEqual(Guid.Empty, id);
        return id;
    }

    private CSharpDbConnection CreateConnection()
        => new("Transport=Grpc;Endpoint=http://localhost", _transportClient);

    private static async Task<string[]> CapturePhysicalExplainSchemaAsync(
        CSharpDbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "EXPLAIN SELECT 1;";
        await using var reader = await command.ExecuteReaderAsync(Ct);
        DataTable schema = Assert.IsType<DataTable>(reader.GetSchemaTable());
        return schema.Rows
            .Cast<DataRow>()
            .Select(row =>
                $"{row.Field<string>("ColumnName")}|" +
                $"{row.Field<string>("DataTypeName")}|" +
                $"{row.Field<bool>("AllowDBNull")}")
            .ToArray();
    }

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
