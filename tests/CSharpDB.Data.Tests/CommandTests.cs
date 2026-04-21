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
    public async Task Prepare_ParameterizedInsert_WithLiteralValue_ReusesAcrossValues()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, kind TEXT, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@id, 'fixed', @name);";
        var id = cmd.Parameters.AddWithValue("@id", 1);
        var name = cmd.Parameters.AddWithValue("@name", "Alice");
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        id.Value = 2;
        name.Value = "Bob";
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT kind, name FROM t ORDER BY id;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("fixed", reader.GetString(0));
        Assert.Equal("Alice", reader.GetString(1));

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("fixed", reader.GetString(0));
        Assert.Equal("Bob", reader.GetString(1));
        Assert.False(await reader.ReadAsync(Ct));
    }

    [Fact]
    public async Task Prepare_ParameterizedMultiRowInsert_ReusesAcrossExecutions()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (@id1, @name1), (@id2, @name2);";
        var id1 = cmd.Parameters.AddWithValue("@id1", 1);
        var name1 = cmd.Parameters.AddWithValue("@name1", "Alice");
        var id2 = cmd.Parameters.AddWithValue("@id2", 2);
        var name2 = cmd.Parameters.AddWithValue("@name2", "Bob");
        cmd.Prepare();

        Assert.Equal(2, await cmd.ExecuteNonQueryAsync(Ct));

        id1.Value = 3;
        name1.Value = "Cara";
        id2.Value = 4;
        name2.Value = "Drew";
        Assert.Equal(2, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT COUNT(*) FROM t;";
        Assert.Equal(4L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Prepare_ParameterizedColumnListInsert_StillExecutes()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t (id, name, age) VALUES (@id, @name, @age);";
        var id = cmd.Parameters.AddWithValue("@id", 1);
        var name = cmd.Parameters.AddWithValue("@name", "Alice");
        var age = cmd.Parameters.AddWithValue("@age", 30);
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        id.Value = 2;
        name.Value = "Bob";
        age.Value = 31;
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT age FROM t WHERE id = 2;";
        Assert.Equal(31L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Prepare_ParameterizedColumnListInsert_WithIdentityAndSubset_ReusesAcrossValues()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY IDENTITY, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t (name, age) VALUES (@name, @age);";
        var name = cmd.Parameters.AddWithValue("@name", "Alice");
        var age = cmd.Parameters.AddWithValue("@age", 30);
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        name.Value = "Bob";
        age.Value = 31;
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT id, name, age FROM t ORDER BY id;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("Alice", reader.GetString(1));
        Assert.Equal(30, reader.GetInt32(2));

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("Bob", reader.GetString(1));
        Assert.Equal(31, reader.GetInt32(2));
        Assert.False(await reader.ReadAsync(Ct));
    }

    [Fact]
    public async Task Prepare_ConstantIdentityInsert_ReusesTemplateWithoutReusingGeneratedIds()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY IDENTITY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (NULL, 'fixed');";
        cmd.Prepare();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));
        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(Ct));

        cmd.CommandText = "SELECT id, name FROM t ORDER BY id;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("fixed", reader.GetString(1));

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("fixed", reader.GetString(1));
        Assert.False(await reader.ReadAsync(Ct));
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
    public async Task Prepare_SelectDistinct_PreservesDistinctSemantics()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1, 'Alice');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (2, 'Alice');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (3, 'Bob');";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT DISTINCT name FROM t WHERE id >= @minId ORDER BY name;";
        var minId = cmd.Parameters.AddWithValue("@minId", 1);
        cmd.Prepare();

        static async Task<List<string>> ReadNamesAsync(CSharpDbCommand command, CancellationToken ct)
        {
            var names = new List<string>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                names.Add(reader.GetString(0));
            return names;
        }

        var allNames = await ReadNamesAsync(cmd, Ct);
        Assert.Equal(["Alice", "Bob"], allNames);

        minId.Value = 2;
        var filteredNames = await ReadNamesAsync(cmd, Ct);
        Assert.Equal(["Alice", "Bob"], filteredNames);
    }

    [Fact]
    public async Task Prepare_SelectWithSubquery_BindsParametersInsideSubquery()
    {
        var cmd = (CSharpDbCommand)_conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE TABLE featured_users (user_id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO users VALUES (1, 'Ada');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO users VALUES (2, 'Grace');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO users VALUES (3, 'Linus');";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO featured_users VALUES (2);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO featured_users VALUES (3);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT name FROM users WHERE id IN (SELECT user_id FROM featured_users WHERE user_id >= @minId) ORDER BY id;";
        var minId = cmd.Parameters.AddWithValue("@minId", 2);
        cmd.Prepare();

        await using (var reader = await cmd.ExecuteReaderAsync(Ct))
        {
            Assert.True(await reader.ReadAsync(Ct));
            Assert.Equal("Grace", reader.GetString(0));
            Assert.True(await reader.ReadAsync(Ct));
            Assert.Equal("Linus", reader.GetString(0));
            Assert.False(await reader.ReadAsync(Ct));
        }

        minId.Value = 3;

        await using var narrowedReader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await narrowedReader.ReadAsync(Ct));
        Assert.Equal("Linus", narrowedReader.GetString(0));
        Assert.False(await narrowedReader.ReadAsync(Ct));
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

