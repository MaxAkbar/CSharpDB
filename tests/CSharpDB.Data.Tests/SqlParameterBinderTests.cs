using CSharpDB.Data;
using CSharpDB.Primitives;
using SysDbType = System.Data.DbType;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;

namespace CSharpDB.Data.Tests;

public class SqlParameterBinderTests
{
    [Fact]
    public void Bind_NoParameters_ReturnsOriginalSql()
    {
        var parameters = new CSharpDbParameterCollection();
        string result = SqlParameterBinder.Bind("SELECT * FROM t", parameters);
        Assert.Equal("SELECT * FROM t", result);
    }

    [Fact]
    public void Bind_IntegerParameter()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@id", 42);
        string result = SqlParameterBinder.Bind("SELECT * FROM t WHERE id = @id", parameters);
        Assert.Equal("SELECT * FROM t WHERE id = 42", result);
    }

    [Fact]
    public void Bind_StringParameter_EscapesSingleQuotes()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@name", "O'Brien");
        string result = SqlParameterBinder.Bind("SELECT * FROM t WHERE name = @name", parameters);
        Assert.Equal("SELECT * FROM t WHERE name = 'O''Brien'", result);
    }

    [Fact]
    public void Bind_NullParameter()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@val", null);
        string result = SqlParameterBinder.Bind("INSERT INTO t VALUES (@val)", parameters);
        Assert.Equal("INSERT INTO t VALUES (NULL)", result);
    }

    [Fact]
    public void Bind_BoolParameter()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@flag", true);
        string result = SqlParameterBinder.Bind("SELECT * FROM t WHERE flag = @flag", parameters);
        Assert.Equal("SELECT * FROM t WHERE flag = 1", result);
    }

    [Fact]
    public void Bind_RealParameter()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@price", 9.99);
        string result = SqlParameterBinder.Bind("SELECT * FROM t WHERE price > @price", parameters);
        Assert.Equal("SELECT * FROM t WHERE price > 9.99", result);
    }

    [Fact]
    public void Bind_DecimalParameter_EmitsExactTypedLiteral()
    {
        var parameters = new CSharpDbParameterCollection();
        CSharpDbParameter parameter = parameters.AddWithValue("@amount", 1.2300m);
        parameter.DbType = SysDbType.Decimal;
        parameter.Precision = 10;
        parameter.Scale = 4;

        string result = SqlParameterBinder.Bind(
            "SELECT @amount + CAST(0.0001 AS DECIMAL(10,4))",
            parameters);

        Assert.Equal(
            "SELECT CAST(1.2300 AS DECIMAL(10,4)) + CAST(0.0001 AS DECIMAL(10,4))",
            result);
    }

    [Fact]
    public void Bind_InferredDecimalParameter_PreservesFractionalScale()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@amount", 0.100m);

        string result = SqlParameterBinder.Bind("SELECT @amount", parameters);

        Assert.Equal("SELECT CAST(0.100 AS DECIMAL(18,3))", result);
    }

    [Fact]
    public void Bind_LogicalParameters_EmitCanonicalTypedCasts()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue(
            "@uid",
            Guid.Parse("ABCDEFAB-1234-5678-9ABC-DEF012345678"));
        parameters.AddWithValue("@day", new DateOnly(2026, 8, 5));
        parameters.AddWithValue("@clock", new TimeOnly(14, 30, 15, 125));
        parameters.AddWithValue(
            "@stamp",
            new DateTime(2026, 8, 5, 14, 30, 15, DateTimeKind.Unspecified));
        parameters.AddWithValue(
            "@zoned",
            new DateTimeOffset(2026, 8, 5, 14, 30, 15, TimeSpan.FromHours(-7)));

        string result = SqlParameterBinder.Bind(
            "SELECT @uid, @day, @clock, @stamp, @zoned",
            parameters);

        Assert.Equal(
            "SELECT " +
            "CAST('abcdefab-1234-5678-9abc-def012345678' AS UUID), " +
            "CAST('2026-08-05' AS DATE), " +
            "CAST('14:30:15.1250000' AS TIME), " +
            "CAST('2026-08-05 14:30:15' AS TIMESTAMP), " +
            "CAST('2026-08-05 14:30:15-07:00' AS TIMESTAMP WITH TIME ZONE)",
            result);
    }

    [Fact]
    public void Bind_MultipleParameters()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@id", 1);
        parameters.AddWithValue("@name", "Alice");
        string result = SqlParameterBinder.Bind(
            "INSERT INTO t VALUES (@id, @name)", parameters);
        Assert.Equal("INSERT INTO t VALUES (1, 'Alice')", result);
    }

    [Fact]
    public void Bind_ParameterInsideStringLiteral_IsNotReplaced()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@id", 1);
        string result = SqlParameterBinder.Bind(
            "SELECT * FROM t WHERE name = '@id' AND id = @id", parameters);
        Assert.Equal("SELECT * FROM t WHERE name = '@id' AND id = 1", result);
    }

    [Fact]
    public void Bind_MissingParameter_Throws()
    {
        var parameters = new CSharpDbParameterCollection();
        Assert.Throws<InvalidOperationException>(() =>
            SqlParameterBinder.Bind("SELECT * FROM t WHERE id = @missing", parameters));
    }

    [Fact]
    public void Bind_LongParameter()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@big", long.MaxValue);
        string result = SqlParameterBinder.Bind("SELECT @big", parameters);
        Assert.Equal($"SELECT {long.MaxValue}", result);
    }

    [Fact]
    public void Bind_ParameterWithoutAtPrefix_MatchesPlaceholder()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("id", 7);
        string result = SqlParameterBinder.Bind("SELECT * FROM t WHERE id = @id", parameters);
        Assert.Equal("SELECT * FROM t WHERE id = 7", result);
    }

    [Fact]
    public void Bind_RepeatedParameter_ReplacesAllOccurrences()
    {
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@id", 5);
        string result = SqlParameterBinder.Bind("SELECT @id, @id, @id", parameters);
        Assert.Equal("SELECT 5, 5, 5", result);
    }

    [Fact]
    public void EscapeValue_DBNull_ReturnsNULL()
    {
        Assert.Equal("NULL", SqlParameterBinder.EscapeValue(DBNull.Value));
    }

    [Fact]
    public void EscapeValue_DateTime_FormatsAsIso()
    {
        var dt = new DateTime(2026, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        string result = SqlParameterBinder.EscapeValue(dt);
        Assert.Equal("'2026-01-15 10:30:00'", result);
    }

    [Fact]
    public void EscapeValue_Blob_FormatsAsBlobLiteral()
    {
        Assert.Equal("X'010203'", SqlParameterBinder.EscapeValue(new byte[] { 1, 2, 3 }));
    }

    [Fact]
    public void EscapeValue_BitString_FormatsExactBitLiteral()
    {
        Assert.Equal(
            "B'101'",
            SqlParameterBinder.EscapeValue(new SqlBitString([0xA0], 3)));
    }

    [Fact]
    public void EscapeValue_Decimal_PreservesExactDigits()
    {
        const decimal value = 123456789012345.678m;

        Assert.Equal("123456789012345.678", SqlParameterBinder.EscapeValue(value));
    }

    [Fact]
    public void EscapeValue_LogicalClrTypes_UseCanonicalTextCodecs()
    {
        Guid guid = Guid.Parse("ABCDEFAB-1234-5678-9ABC-DEF012345678");
        DateOnly date = new(2026, 8, 5);
        TimeOnly time = new(14, 30, 15, 125);
        DateTimeOffset timestamp = new(2026, 8, 5, 14, 30, 15, TimeSpan.FromHours(-7));

        Assert.Equal("'abcdefab-1234-5678-9abc-def012345678'", SqlParameterBinder.EscapeValue(guid));
        Assert.Equal("'2026-08-05'", SqlParameterBinder.EscapeValue(date));
        Assert.Equal("'14:30:15.1250000'", SqlParameterBinder.EscapeValue(time));
        Assert.Equal("'2026-08-05 14:30:15-07:00'", SqlParameterBinder.EscapeValue(timestamp));
    }

    [Fact]
    public void PreparedSimpleInsert_DecimalParameterRemainsExact()
    {
        const decimal value = 123456789012345.678m;
        PreparedStatementTemplate template = PreparedStatementTemplate.Create(
            "INSERT INTO amounts VALUES (@value)");
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@value", value);

        Assert.True(template.TryBindSimpleInsert(parameters, out var insert));
        Assert.Equal(DbType.Decimal, insert.Values[0].Type);
        Assert.Equal(value, insert.Values[0].AsDecimal);
    }
    [Fact]
    public void PreparedSimpleInsert_BitStringParameterPreservesExactLength()
    {
        PreparedStatementTemplate template = PreparedStatementTemplate.Create(
            "INSERT INTO bit_values VALUES (@value)");
        var parameters = new CSharpDbParameterCollection();
        parameters.AddWithValue("@value", new SqlBitString([0x80], 1));

        Assert.True(template.TryBindSimpleInsert(parameters, out var insert));
        Assert.True(insert.Values[0].IsBitString);
        Assert.Equal(1, insert.Values[0].BitLength);
        Assert.Equal(new byte[] { 0x80 }, insert.Values[0].AsBlob);
    }
}
