using System.Data.Common;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class EfFoundationTests : IAsyncLifetime
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_ef_foundation_{Guid.NewGuid():N}.db");
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_dbPath);
        DeleteIfExists(_dbPath + ".wal");
    }

    [Fact]
    public async Task OpenAsync_FileConnection_UsesDirectDatabaseSession()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");

        await conn.OpenAsync(Ct);

        Assert.IsType<DirectDatabaseSession>(conn.GetSession());
    }

    [Fact]
    public async Task OpenAsync_PrivateMemoryConnection_UsesDirectDatabaseSession()
    {
        await using var conn = new CSharpDbConnection("Data Source=:memory:");

        await conn.OpenAsync(Ct);

        Assert.IsType<DirectDatabaseSession>(conn.GetSession());
    }

    [Fact]
    public async Task OpenAsync_PooledFileConnection_KeepsRemoteSession()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath};Pooling=true;Max Pool Size=1");

        await conn.OpenAsync(Ct);

        Assert.IsType<RemoteDatabaseSession>(conn.GetSession());
    }

    [Fact]
    public async Task ExecuteCommandAsync_SingleRowIdentityInsert_ExposesGeneratedIntegerKey()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY IDENTITY, name TEXT);";
            await create.ExecuteNonQueryAsync(Ct);
        }

        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO items VALUES (NULL, @name);";
        insert.Parameters.AddWithValue("@name", "alpha");

        CSharpDbCommandExecutionResult execution = await insert.ExecuteCommandAsync(Ct);
        await using var _ = execution.Result;

        Assert.Equal(1, execution.Result.RowsAffected);
        Assert.Equal(1L, execution.GeneratedIntegerKey);
    }

    [Fact]
    public async Task ExecuteCommandAsync_ColumnListIdentityInsert_ExposesGeneratedIntegerKey()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY IDENTITY, name TEXT, age INTEGER);";
            await create.ExecuteNonQueryAsync(Ct);
        }

        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO items (name, age) VALUES (@name, @age);";
        insert.Parameters.AddWithValue("@name", "alpha");
        insert.Parameters.AddWithValue("@age", 42);

        CSharpDbCommandExecutionResult execution = await insert.ExecuteCommandAsync(Ct);
        await using var _ = execution.Result;

        Assert.Equal(1, execution.Result.RowsAffected);
        Assert.Equal(1L, execution.GeneratedIntegerKey);
    }

    [Fact]
    public async Task ExecuteNonQuery_WithBlobParameter_WorksForDirectStructuredExecution()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB);";
            await create.ExecuteNonQueryAsync(Ct);
        }

        byte[] payload = [0x01, 0x02, 0xFE, 0xFF];

        await using (var insert = conn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO files VALUES (@id, @payload);";
            insert.Parameters.AddWithValue("@id", 7);
            insert.Parameters.AddWithValue("@payload", payload);

            int rowsAffected = await insert.ExecuteNonQueryAsync(Ct);
            Assert.Equal(1, rowsAffected);
        }

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT payload FROM files WHERE id = @id;";
        select.Parameters.AddWithValue("@id", 7);

        await using DbDataReader reader = await select.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(payload, (byte[])reader.GetValue(0));
    }

    [Fact]
    public async Task ExecuteScalar_WithBlobParameter_WorksForPooledSqlBinding()
    {
        string connectionString = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        await using var conn = new CSharpDbConnection(connectionString);
        await conn.OpenAsync(Ct);

        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB);";
            await create.ExecuteNonQueryAsync(Ct);
        }

        byte[] payload = [0x10, 0x20, 0x30, 0x40];

        await using (var insert = conn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO files VALUES (@id, @payload);";
            insert.Parameters.AddWithValue("@id", 3);
            insert.Parameters.AddWithValue("@payload", payload);
            await insert.ExecuteNonQueryAsync(Ct);
        }

        await using var count = conn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM files WHERE payload = @payload;";
        count.Parameters.AddWithValue("@payload", payload);

        object? value = await count.ExecuteScalarAsync(Ct);
        Assert.Equal(1L, Assert.IsType<long>(value));
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
