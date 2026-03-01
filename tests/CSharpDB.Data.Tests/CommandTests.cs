using System.Data;
using System.Data.Common;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class CommandTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private CSharpDbConnection _conn = null!;

    public CommandTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_cmd_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await _conn.OpenAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CreateTable()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
        int result = await cmd.ExecuteNonQueryAsync(Ct);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_Insert_ReturnsRowsAffected()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1, 'Alice');";
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithParameters()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@id, @name, @age);";
        cmd.Parameters.AddWithValue("@id", 1);
        cmd.Parameters.AddWithValue("@name", "Alice");
        cmd.Parameters.AddWithValue("@age", 30);
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("Alice", reader.GetString(1));
        Assert.Equal(30, reader.GetInt32(2));
    }

    [Fact]
    public async Task Prepare_CachesParameterizedTemplate_AndReusesAcrossValues()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@id, @name);";
        var id = cmd.Parameters.AddWithValue("@id", 1);
        var name = cmd.Parameters.AddWithValue("@name", "Alice");
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        id.Value = 2;
        name.Value = "Bob";
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT COUNT(*) FROM t;";
        Assert.Equal(2L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Prepare_InvalidatedWhenCommandTextChanges()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@id, @val);";
        cmd.Parameters.AddWithValue("@id", 1);
        cmd.Parameters.AddWithValue("@val", 10);
        cmd.Prepare();
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT val FROM t WHERE id = 1;";
        Assert.Equal(10L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task ExecuteReaderAsync_ParameterizedLimit_FallsBackToSqlBindingPath()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (2);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (3);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT id FROM t ORDER BY id LIMIT @lim;";
        cmd.Parameters.AddWithValue("@lim", 2);

        var rows = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
            rows.Add(reader.GetInt64(0));

        Assert.Equal([1L, 2L], rows);
    }

    [Fact]
    public async Task Prepare_UnsupportedTemplate_FallsBackWithoutBreakingExecution()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT id FROM t LIMIT @lim;";
        cmd.Parameters.AddWithValue("@lim", 1);

        cmd.Prepare(); // unsupported LIMIT parameterized template should gracefully fallback
        Assert.Equal(1L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task ExecuteScalarAsync_ReturnsFirstColumnFirstRow()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1, 'Alice');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (2, 'Bob');";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "SELECT name FROM t WHERE id = 1;";
        var result = await cmd.ExecuteScalarAsync(Ct);
        Assert.Equal("Alice", result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_EmptyResult_ReturnsNull()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "SELECT * FROM t;";
        var result = await cmd.ExecuteScalarAsync(Ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteReaderAsync_WithStringEscaping()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@name);";
        cmd.Parameters.AddWithValue("@name", "O'Brien");
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("O'Brien", reader.GetString(0));
    }

    [Fact]
    public async Task ExecuteAsync_InvalidSql_ThrowsCSharpDbDataException()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "SELECET INVALID;";
        await Assert.ThrowsAsync<CSharpDbDataException>(() => cmd.ExecuteNonQueryAsync(Ct));
    }

    [Fact]
    public void CommandType_OnlyTextSupported()
    {
        using var cmd = new CSharpDbCommand();
        Assert.Equal(CommandType.Text, cmd.CommandType);
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
    }
}

