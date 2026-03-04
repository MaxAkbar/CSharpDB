using System.Data;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class DataReaderTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private CSharpDbConnection _conn = null!;

    public DataReaderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_reader_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await _conn.OpenAsync(Ct);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, data TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "INSERT INTO t VALUES (1, 'Alice', 95.5, NULL);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO t VALUES (2, 'Bob', 87.3, 'some data');";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task ReadAsync_IteratesRows()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.True(await reader.ReadAsync(Ct));
        Assert.False(await reader.ReadAsync(Ct));
    }

    [Fact]
    public async Task FieldCount_ReturnsColumnCount()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.Equal(4, reader.FieldCount);
    }

    [Fact]
    public async Task GetName_ReturnsColumnName()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal("score", reader.GetName(2));
        Assert.Equal("data", reader.GetName(3));
    }

    [Fact]
    public async Task GetOrdinal_ReturnsIndex()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal(1, reader.GetOrdinal("name"));
        Assert.Equal(1, reader.GetOrdinal("NAME")); // case-insensitive
    }

    [Fact]
    public async Task GetOrdinal_UnknownColumn_Throws()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("nonexistent"));
    }

    [Fact]
    public async Task GetInt64_ReturnsInteger()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1L, reader.GetInt64(0));
    }

    [Fact]
    public async Task GetInt32_NarrowsInteger()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1, reader.GetInt32(0));
    }

    [Fact]
    public async Task GetString_ReturnsText()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("Alice", reader.GetString(1));
    }

    [Fact]
    public async Task GetDouble_ReturnsReal()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(95.5, reader.GetDouble(2));
    }

    [Fact]
    public async Task IsDBNull_ReturnsTrueForNull()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 1;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.True(reader.IsDBNull(3));   // data column is NULL for row 1
        Assert.False(reader.IsDBNull(1));  // name is not NULL
    }

    [Fact]
    public async Task GetValue_ReturnsDBNullForNull()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 1;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(DBNull.Value, reader.GetValue(3));
    }

    [Fact]
    public async Task GetValue_ReturnsTypedValues()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 1;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.IsType<long>(reader.GetValue(0));
        Assert.IsType<string>(reader.GetValue(1));
        Assert.IsType<double>(reader.GetValue(2));
    }

    [Fact]
    public async Task Indexer_ByName_ReturnsValue()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 2;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("Bob", reader["name"]);
    }

    [Fact]
    public async Task NextResult_ReturnsFalse()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.False(await reader.NextResultAsync(Ct));
    }

    [Fact]
    public async Task GetFieldType_ReturnsClrType()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));
        Assert.Equal(typeof(double), reader.GetFieldType(2));
    }

    [Fact]
    public async Task GetDataTypeName_ReturnsDbTypeName()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal("INTEGER", reader.GetDataTypeName(0));
        Assert.Equal("TEXT", reader.GetDataTypeName(1));
        Assert.Equal("REAL", reader.GetDataTypeName(2));
    }

    [Fact]
    public async Task GetSchemaTable_ReturnsColumnMetadata()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        var schemaTable = reader.GetSchemaTable();
        Assert.NotNull(schemaTable);
        Assert.Equal(4, schemaTable.Rows.Count);
        Assert.Equal("id", schemaTable.Rows[0]["ColumnName"]);
        Assert.Equal(0, schemaTable.Rows[0]["ColumnOrdinal"]);
    }

    [Fact]
    public async Task RecordsAffected_ForInsert()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "INSERT INTO t VALUES (3, 'Charlie', 70.0, 'test');";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        // Drain the reader
        while (await reader.ReadAsync(Ct)) { }
        Assert.Equal(1, reader.RecordsAffected);
    }

    [Fact]
    public async Task HasRows_TrueWhenDataExists()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        // HasRows should be true even before first Read
        Assert.True(reader.HasRows);
    }

    [Fact]
    public async Task GetBoolean_ConvertsFromInteger()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE bools (val INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO bools VALUES (1);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO bools VALUES (0);";
        await cmd.ExecuteNonQueryAsync(Ct);

        cmd.CommandText = "SELECT * FROM bools;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.True(reader.GetBoolean(0));

        Assert.True(await reader.ReadAsync(Ct));
        Assert.False(reader.GetBoolean(0));
    }

    [Fact]
    public async Task GetValues_FillsArray()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 1;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        var values = new object[4];
        int count = reader.GetValues(values);
        Assert.Equal(4, count);
        Assert.Equal(1L, values[0]);
        Assert.Equal("Alice", values[1]);
    }
}

