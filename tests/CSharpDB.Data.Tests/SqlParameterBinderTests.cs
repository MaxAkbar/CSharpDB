using CSharpDB.Data;

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
        Assert.StartsWith("'2026-01-15T10:30:00", result);
    }

    [Fact]
    public void EscapeValue_Blob_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            SqlParameterBinder.EscapeValue(new byte[] { 1, 2, 3 }));
    }
}
