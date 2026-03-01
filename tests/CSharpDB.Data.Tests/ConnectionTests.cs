using System.Data;
using CSharpDB.Core;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class ConnectionTests : IDisposable
{
    private readonly string _dbPath;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ConnectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_data_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task OpenAsync_CreatesDatabase()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        Assert.Equal(ConnectionState.Closed, conn.State);

        await conn.OpenAsync(Ct);
        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.True(File.Exists(_dbPath));
    }

    [Fact]
    public async Task CloseAsync_SetsStateToClosed()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task OpenAsync_WhenAlreadyOpen_Throws()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => conn.OpenAsync(Ct));
    }

    [Fact]
    public async Task OpenAsync_WithEmptyDataSource_Throws()
    {
        await using var conn = new CSharpDbConnection("Data Source=");
        await Assert.ThrowsAsync<InvalidOperationException>(() => conn.OpenAsync(Ct));
    }

    [Fact]
    public void DataSource_ParsesConnectionString()
    {
        var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        Assert.Equal(_dbPath, conn.DataSource);
    }

    [Fact]
    public void ChangeDatabase_ThrowsNotSupported()
    {
        var conn = new CSharpDbConnection();
        Assert.Throws<NotSupportedException>(() => conn.ChangeDatabase("other"));
    }

    [Fact]
    public async Task CreateCommand_ReturnsCommandWithConnection()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        using var cmd = conn.CreateCommand();
        Assert.NotNull(cmd);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public async Task GetTableNames_ReturnsCreatedTables()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = (CSharpDbCommand)conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE alpha (id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE TABLE beta (id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        var names = conn.GetTableNames();
        Assert.Contains("alpha", names);
        Assert.Contains("beta", names);
        Assert.Equal(2, names.Count);
    }

    [Fact]
    public async Task GetTableSchema_ReturnsColumns()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = (CSharpDbCommand)conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        var schema = conn.GetTableSchema("users");
        Assert.NotNull(schema);
        Assert.Equal("users", schema!.TableName);
        Assert.Equal(3, schema.Columns.Count);
        Assert.Equal("id", schema.Columns[0].Name);
        Assert.True(schema.Columns[0].IsPrimaryKey);
        Assert.Equal(CSharpDB.Core.DbType.Integer, schema.Columns[0].Type);
        Assert.Equal("name", schema.Columns[1].Name);
        Assert.Equal(CSharpDB.Core.DbType.Text, schema.Columns[1].Type);
    }

    [Fact]
    public async Task GetTableSchema_NonExistent_ReturnsNull()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        Assert.Null(conn.GetTableSchema("nonexistent"));
    }
}

