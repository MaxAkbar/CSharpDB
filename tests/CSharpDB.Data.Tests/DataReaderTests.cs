using System.Data;
using CSharpDB.Data;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CoreDbType = CSharpDB.Primitives.DbType;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;

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
    public async Task ComputedExpressionMetadataReportsDecimalFacetsAndBooleanType()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "SELECT -CAST(1.20 AS DECIMAL(4,2)) AS negated, " +
            "CAST(1.20 AS DECIMAL(4,2)) + CAST(2 AS DECIMAL(3,0)) AS total, " +
            "1 < 2 AS is_less;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        Assert.Equal(typeof(decimal), reader.GetFieldType(0));
        Assert.Equal(typeof(decimal), reader.GetFieldType(1));
        Assert.Equal(typeof(bool), reader.GetFieldType(2));
        Assert.Equal("DECIMAL(4,2)", reader.GetDataTypeName(0));
        Assert.Equal("DECIMAL(6,2)", reader.GetDataTypeName(1));
        Assert.Equal("BOOLEAN", reader.GetDataTypeName(2));

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(-1.20m, reader.GetValue(0));
        Assert.Equal(3.20m, reader.GetValue(1));
        Assert.Equal(true, reader.GetValue(2));
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
        Assert.Equal(8, schemaTable.Rows[0]["ColumnSize"]);
        Assert.Equal((short)19, schemaTable.Rows[0]["NumericPrecision"]);
        Assert.Equal((short)0, schemaTable.Rows[0]["NumericScale"]);
        Assert.Equal((int)CSharpDB.Primitives.DbType.Integer, schemaTable.Rows[0]["ProviderType"]);
        Assert.Equal("INTEGER", schemaTable.Rows[0]["DataTypeName"]);
        Assert.False((bool)schemaTable.Rows[0]["AllowDBNull"]);
        Assert.True((bool)schemaTable.Rows[0]["IsKey"]);
        Assert.True((bool)schemaTable.Rows[0]["IsIdentity"]);
        Assert.True((bool)schemaTable.Rows[0]["IsAutoIncrement"]);
        Assert.False((bool)schemaTable.Rows[0]["IsRowVersion"]);
        Assert.Equal(DBNull.Value, schemaTable.Rows[0]["CollationName"]);

        Assert.Equal((short)15, schemaTable.Rows[2]["NumericPrecision"]);
        Assert.Equal(DBNull.Value, schemaTable.Rows[2]["NumericScale"]);
    }

    [Fact]
    public async Task GetSchemaTable_ReportsRowVersionMetadata()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE versioned (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "INSERT INTO versioned (id) VALUES (1);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "SELECT version FROM versioned;";

        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        DataTable schemaTable = Assert.IsType<DataTable>(reader.GetSchemaTable());

        DataRow row = Assert.Single(schemaTable.Rows.Cast<DataRow>());
        Assert.True(
            schemaTable.Columns["IsRowVersion"]!.Ordinal >
            schemaTable.Columns["CollationName"]!.Ordinal);
        Assert.Equal("version", row["ColumnName"]);
        Assert.True((bool)row["IsRowVersion"]);
        Assert.False((bool)row["AllowDBNull"]);
    }

    [Fact]
    public async Task GetSchemaTable_DoesNotInventUnavailableBaseLineage()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT score + 1 AS adjusted_score FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        DataTable schemaTable = Assert.IsType<DataTable>(reader.GetSchemaTable());

        Assert.False(schemaTable.Columns.Contains("BaseCatalogName"));
        Assert.False(schemaTable.Columns.Contains("BaseSchemaName"));
        Assert.False(schemaTable.Columns.Contains("BaseTableName"));
        Assert.False(schemaTable.Columns.Contains("BaseColumnName"));
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

    [Fact]
    public async Task LogicalTypes_ControlMetadataAndClrMaterialization()
    {
        Guid id = Guid.Parse("12345678-1234-5678-9abc-def012345678");
        DateOnly date = new(2026, 8, 5);
        TimeOnly time = new(14, 30, 15, 125);
        DateTime timestamp = new(2026, 8, 5, 14, 30, 15, DateTimeKind.Unspecified);
        DateTimeOffset timestampWithZone = new(2026, 8, 5, 14, 30, 15, TimeSpan.FromHours(-7));

        ColumnDefinition[] schema =
        [
            LogicalColumn("flag", CoreDbType.Integer, SqlTypeKind.Boolean),
            LogicalColumn("tiny", CoreDbType.Integer, SqlTypeKind.TinyInt),
            LogicalColumn("small", CoreDbType.Integer, SqlTypeKind.SmallInt),
            LogicalColumn("number", CoreDbType.Integer, SqlTypeKind.Integer),
            LogicalColumn("big", CoreDbType.Integer, SqlTypeKind.BigInt),
            LogicalColumn("single_value", CoreDbType.Real, SqlTypeKind.Real),
            LogicalColumn("double_value", CoreDbType.Real, SqlTypeKind.Double),
            new ColumnDefinition
            {
                Name = "amount",
                Type = CoreDbType.Decimal,
                DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 18, scale: 3),
            },
            LogicalColumn("id", CoreDbType.Blob, SqlTypeKind.Uuid),
            LogicalColumn("created_date", CoreDbType.Text, SqlTypeKind.Date),
            LogicalColumn("created_time", CoreDbType.Text, SqlTypeKind.Time),
            LogicalColumn("created_at", CoreDbType.Text, SqlTypeKind.Timestamp),
            LogicalColumn("created_at_tz", CoreDbType.Text, SqlTypeKind.TimestampWithTimeZone),
            LogicalColumn("elapsed", CoreDbType.Text, SqlTypeKind.IntervalDayToSecond),
        ];
        DbValue[] row =
        [
            DbValue.FromInteger(1),
            DbValue.FromInteger(255),
            DbValue.FromInteger(-1234),
            DbValue.FromInteger(123456),
            DbValue.FromInteger(long.MaxValue),
            DbValue.FromReal(1.25),
            DbValue.FromReal(2.5),
            DbValue.FromDecimal(123456789012345.678m),
            DbValue.FromBlob(id.ToByteArray(bigEndian: true)),
            DbValue.FromText(CSharpDbTextCodec.FormatDate(date)),
            DbValue.FromText(CSharpDbTextCodec.FormatTime(time)),
            DbValue.FromText(CSharpDbTextCodec.FormatDateTime(timestamp)),
            DbValue.FromText(CSharpDbTextCodec.FormatDateTimeOffset(timestampWithZone)),
            DbValue.FromText("1.02:03:04.5"),
        ];

        await using var reader = new CSharpDbDataReader(
            QueryResult.FromMaterializedRows(schema, [row]),
            CommandBehavior.Default,
            connection: null);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(typeof(bool), reader.GetFieldType(0));
        Assert.Equal(typeof(byte), reader.GetFieldType(1));
        Assert.Equal(typeof(short), reader.GetFieldType(2));
        Assert.Equal(typeof(long), reader.GetFieldType(3));
        Assert.Equal(typeof(long), reader.GetFieldType(4));
        Assert.Equal(typeof(double), reader.GetFieldType(5));
        Assert.Equal(typeof(double), reader.GetFieldType(6));
        Assert.Equal(typeof(decimal), reader.GetFieldType(7));
        Assert.Equal(typeof(Guid), reader.GetFieldType(8));
        Assert.Equal(typeof(DateOnly), reader.GetFieldType(9));
        Assert.Equal(typeof(TimeOnly), reader.GetFieldType(10));
        Assert.Equal(typeof(DateTime), reader.GetFieldType(11));
        Assert.Equal(typeof(DateTimeOffset), reader.GetFieldType(12));
        Assert.Equal(typeof(TimeSpan), reader.GetFieldType(13));

        Assert.Equal("DECIMAL(18,3)", reader.GetDataTypeName(7));
        Assert.IsType<bool>(reader.GetValue(0));
        Assert.IsType<byte>(reader.GetValue(1));
        Assert.IsType<double>(reader.GetValue(5));
        Assert.Equal(123456789012345.678m, reader.GetDecimal(7));
        Assert.Equal(id, reader.GetGuid(8));
        Assert.Equal(date, reader.GetDateOnly(9));
        Assert.Equal(time, reader.GetTimeOnly(10));
        Assert.Equal(timestamp, reader.GetDateTime(11));
        Assert.Equal(timestampWithZone, reader.GetDateTimeOffset(12));
        Assert.Equal(TimeSpan.Parse("1.02:03:04.5"), reader.GetValue(13));

        DataRow amountSchema = reader.GetSchemaTable().Rows[7];
        Assert.Equal((short)18, amountSchema["NumericPrecision"]);
        Assert.Equal((short)3, amountSchema["NumericScale"]);
    }

    [Fact]
    public async Task BitStrings_PreservePerValueLengthWhileBlobReadsStayBinary()
    {
        using var command = _conn.CreateCommand();
        command.CommandText =
            "CREATE TABLE ado_bits (" +
            "id INTEGER PRIMARY KEY, short_bits VARBIT(8), " +
            "full_bits VARBIT(8), payload BLOB);";
        await command.ExecuteNonQueryAsync(Ct);

        command.CommandText =
            "INSERT INTO ado_bits (id, short_bits, full_bits, payload) " +
            "VALUES (@id, @short, @full, @payload);";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@id", 1);
        command.Parameters.AddWithValue("@short", new SqlBitString([0x80], 1));
        command.Parameters.AddWithValue("@full", new SqlBitString([0x80], 8));
        command.Parameters.AddWithValue("@payload", new byte[] { 0x80 });
        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));

        command.CommandText =
            "SELECT short_bits, full_bits, payload FROM ado_bits WHERE id = 1;";
        command.Parameters.Clear();
        await using var baseReader = await command.ExecuteReaderAsync(Ct);
        CSharpDbDataReader reader = Assert.IsType<CSharpDbDataReader>(baseReader);
        Assert.True(await reader.ReadAsync(Ct));

        Assert.Equal(typeof(SqlBitString), reader.GetFieldType(0));
        SqlBitString shortBits = Assert.IsType<SqlBitString>(reader.GetValue(0));
        SqlBitString fullBits = reader.GetBitString(1);
        Assert.Equal(1, shortBits.BitLength);
        Assert.Equal(8, fullBits.BitLength);
        Assert.Equal(new byte[] { 0x80 }, shortBits.PackedBytes.ToArray());
        Assert.Equal(new byte[] { 0x80 }, fullBits.PackedBytes.ToArray());
        Assert.Equal(1, reader.GetBitLength(0));
        Assert.Equal(8, reader.GetBitLength(1));

        Assert.Equal(1, reader.GetBytes(0, 0, null, 0, 0));
        Assert.Equal(new byte[] { 0x80 }, reader.GetFieldValue<byte[]>(0));
        Assert.Equal(new byte[] { 0x80 }, Assert.IsType<byte[]>(reader.GetValue(2)));
        Assert.Throws<InvalidCastException>(() => reader.GetBitLength(2));
    }

    private static ColumnDefinition LogicalColumn(
        string name,
        CoreDbType storageType,
        SqlTypeKind kind)
        => new()
        {
            Name = name,
            Type = storageType,
            DeclaredType = SqlTypeDescriptor.Create(kind),
        };
}
