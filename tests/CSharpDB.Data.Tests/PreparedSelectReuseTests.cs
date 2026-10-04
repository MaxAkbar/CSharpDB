using System.Data;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public sealed class PreparedSelectReuseTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<CSharpDbConnection> OpenAsync(int offset = 0)
    {
        var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        await ExecuteAsync(connection, "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)");
        await ExecuteAsync(connection, $"INSERT INTO t VALUES (1, {1 + offset}), (2, {2 + offset}), (3, {3 + offset}), (4, {4 + offset}), (5, NULL)");
        await ExecuteAsync(connection, "CREATE INDEX ix_value ON t(value)");
        return connection;
    }

    private static async Task ExecuteAsync(CSharpDbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<long[]> ReadAsync(CSharpDbCommand command)
    {
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var values = new List<long>();
        while (await reader.ReadAsync(Ct)) values.Add(reader.GetInt64(0));
        return values.ToArray();
    }

    [Theory]
    [InlineData("value")]
    [InlineData("id")]
    public async Task ChangingValuesAndLogicalTypes_MatchOrdinaryBinding(string keyColumn)
    {
        await using var connection = await OpenAsync();
        using var prepared = connection.CreateCommand();
        prepared.CommandText = $"SELECT id FROM t WHERE {keyColumn} = @p";
        var parameter = prepared.Parameters.AddWithValue("p", 2L);
        prepared.Prepare();
        using var ordinary = connection.CreateCommand();
        // The constant conjunct is deliberately outside the prepared index shape, but logically equivalent.
        ordinary.CommandText = $"SELECT id FROM t WHERE {keyColumn} = @p AND 1 = 1";
        var controlParameter = ordinary.Parameters.AddWithValue("p", 2L);
        object?[] values = [2L, 3, (short)4, (uint)1, DBNull.Value, null, 2m, 2.5m, 2d, "2", true, new byte[] { 2 }, 2L];
        foreach (object? value in values)
        {
            parameter.Value = controlParameter.Value = value;
            long[]? actual = null, expected = null;
            Exception? actualError = await Record.ExceptionAsync(async () => actual = await ReadAsync(prepared));
            Exception? expectedError = await Record.ExceptionAsync(async () => expected = await ReadAsync(ordinary));
            Assert.Equal(expectedError?.GetType(), actualError?.GetType());
            if (actualError is null) Assert.Equal(expected, actual);
        }
        parameter.Value = controlParameter.Value = 1;
        parameter.DbType = controlParameter.DbType = DbType.Boolean;
        Assert.Equal(await ReadAsync(ordinary), await ReadAsync(prepared));
        parameter.DbType = controlParameter.DbType = DbType.Int64;
        Assert.Equal(new long[] { 1 }, await ReadAsync(prepared));
    }

    [Fact]
    public async Task DefaultSyncIndexedEquality_PreservesProjectionsDuplicatesAndRowWindows()
    {
        await using var connection = await OpenAsync();
        await ExecuteAsync(connection, "INSERT INTO t VALUES (6, 2)");
        long[][] fixtureRows = [[1, 1], [2, 2], [3, 3], [4, 4], [6, 2]];
        (string Projection, int[] Ordinals, string[] Names, string Window, int Offset, int? Limit)[] cases =
        [
            ("*", [0, 1], ["id", "value"], "", 0, null),
            ("value", [1], ["value"], "", 0, null),
            ("value, id, value AS repeated", [1, 0, 1], ["value", "id", "repeated"], "", 0, null),
            ("id", [0], ["id"], " LIMIT 1", 0, 1),
            ("id", [0], ["id"], " LIMIT 1 OFFSET 1", 1, 1),
            ("id", [0], ["id"], " LIMIT 0", 0, 0),
        ];

        foreach (var query in cases)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {query.Projection} FROM t WHERE value = @p{query.Window}";
            var parameter = command.Parameters.AddWithValue("p", 1L);
            command.Prepare();
            foreach (long value in new long[] { 1, 1, 2, 900, 4, 2, 1 })
            {
                parameter.Value = value;
                await using var reader = await command.ExecuteReaderAsync(Ct);
                Assert.Equal(query.Names.Length, reader.FieldCount);
                for (int column = 0; column < query.Names.Length; column++)
                {
                    Assert.Equal(query.Names[column], reader.GetName(column));
                    Assert.Equal("INTEGER", reader.GetDataTypeName(column));
                }

                var actual = new List<string>();
                while (await reader.ReadAsync(Ct))
                    actual.Add(string.Join(",", Enumerable.Range(0, reader.FieldCount).Select(reader.GetInt64)));

                string[] matches = fixtureRows.Where(row => row[1] == value)
                    .Select(row => string.Join(",", query.Ordinals.Select(ordinal => row[ordinal]))).ToArray();
                Assert.Equal(matches.Skip(query.Offset).Take(query.Limit ?? int.MaxValue).Count(), actual.Count);
                foreach (string row in actual) Assert.Contains(row, matches);
                // Without ORDER BY, a row window may select either matching duplicate.
                if (query.Limit is null && query.Offset == 0)
                    Assert.Equal(matches.Order(), actual.Order());
            }
        }
    }

    [Theory]
    [InlineData("id")]
    [InlineData("value")]
    public async Task PreparedStarAfterNullableColumnAdded_ReturnsFullWidthRows(string keyColumn)
    {
        await using var connection = await OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM t WHERE {keyColumn} = @p ORDER BY {keyColumn}";
        var parameter = command.Parameters.AddWithValue("p", 1L);
        command.Prepare();

        async Task AssertRowAsync(long value, int expectedWidth)
        {
            parameter.Value = value;
            await using var reader = await command.ExecuteReaderAsync(Ct);
            Assert.Equal(expectedWidth, reader.FieldCount);
            Assert.Equal("id", reader.GetName(0));
            Assert.Equal("value", reader.GetName(1));
            if (expectedWidth == 3)
            {
                Assert.Equal("added", reader.GetName(2));
                Assert.Equal("INTEGER", reader.GetDataTypeName(2));
            }

            bool expectedRow = value != 99;
            Assert.Equal(expectedRow, await reader.ReadAsync(Ct));
            if (expectedRow)
            {
                Assert.Equal(value, reader.GetInt64(0));
                Assert.Equal(value, reader.GetInt64(1));
                if (expectedWidth == 3) Assert.True(reader.IsDBNull(2));
            }
            Assert.False(await reader.ReadAsync(Ct));
        }

        foreach (long value in new long[] { 1, 2, 1 })
            await AssertRowAsync(value, 2);

        // Nullable additions without defaults update metadata without rewriting existing records.
        await ExecuteAsync(connection, "ALTER TABLE t ADD COLUMN added INTEGER");
        foreach (long value in new long[] { 1, 1, 3, 99, 2, 2 })
            await AssertRowAsync(value, 3);
    }

    [Fact]
    public async Task MissingOrRenamedParameter_IsRejectedAfterCachedExecutions()
    {
        await using var connection = await OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id FROM t WHERE value >= @lower AND value < @upper ORDER BY value";
        command.Parameters.AddWithValue("lower", 2);
        var upper = command.Parameters.AddWithValue("upper", 4);
        command.Prepare();
        Assert.Equal(new long[] { 2, 3 }, await ReadAsync(command));
        upper.ParameterName = "different";
        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(command));
        upper.ParameterName = "@UPPER";
        Assert.Equal(new long[] { 2, 3 }, await ReadAsync(command));
        command.Parameters.Remove(upper);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(command));
    }

    [Fact]
    public async Task ReusingCommandWithOpenReaders_DoesNotChangeTheirValuesOrMetadata()
    {
        await using var connection = await OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id AS identifier, value FROM t WHERE value >= @p ORDER BY value LIMIT 2";
        var parameter = command.Parameters.AddWithValue("p", 1);
        command.Prepare();
        await using var first = await command.ExecuteReaderAsync(Ct);
        parameter.Value = 3;
        await using var second = await command.ExecuteReaderAsync(Ct);
        parameter.Value = 99;
        Assert.Equal("identifier", first.GetName(0));
        Assert.Equal("INTEGER", first.GetDataTypeName(0));
        Assert.True(await second.ReadAsync(Ct)); Assert.Equal(3, second.GetInt64(0));
        Assert.True(await first.ReadAsync(Ct)); Assert.Equal(1, first.GetInt64(0));
        Assert.True(await second.ReadAsync(Ct)); Assert.Equal(4, second.GetInt64(0));
        Assert.True(await first.ReadAsync(Ct)); Assert.Equal(2, first.GetInt64(0));
        Assert.False(await second.ReadAsync(Ct)); Assert.False(await first.ReadAsync(Ct));
    }

    [Fact]
    public async Task SharedSqlTemplate_UsesEachConnectionsCatalogAndValues()
    {
        await using var first = await OpenAsync();
        await using var second = await OpenAsync(100);
        using var a = first.CreateCommand();
        using var b = second.CreateCommand();
        a.CommandText = b.CommandText = "SELECT id FROM t WHERE value = @p";
        var av = a.Parameters.AddWithValue("p", 1);
        var bv = b.Parameters.AddWithValue("p", 101);
        a.Prepare(); b.Prepare();
        for (int i = 0; i < 150; i++)
        {
            av.Value = i % 4 + 1;
            bv.Value = 100 + (i + 1) % 4 + 1;
            Task<long[]> at = ReadAsync(a), bt = ReadAsync(b);
            Assert.Equal(new[] { (long)(i % 4 + 1) }, await at);
            Assert.Equal(new[] { (long)((i + 1) % 4 + 1) }, await bt);
        }
        await ExecuteAsync(second, "DROP INDEX ix_value");
        av.Value = 3; bv.Value = 104;
        Assert.Equal(new long[] { 3 }, await ReadAsync(a));
        Assert.Equal(new long[] { 4 }, await ReadAsync(b));
    }

    [Theory]
    [InlineData("SELECT id AS value FROM t WHERE value >= @p ORDER BY value DESC LIMIT 2", 1, 4, 3)]
    [InlineData("SELECT (id + 10) FROM t WHERE value >= @p ORDER BY value LIMIT 2", 2, 12, 13)]
    [InlineData("SELECT id FROM t WHERE value >= @p OR value IS NULL ORDER BY id LIMIT 2", 4, 4, 5)]
    public async Task UnsupportedShapes_RetainTheirSemantics(string sql, long parameter, long first, long second)
    {
        await using var connection = await OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("p", parameter);
        command.Prepare();
        Assert.Equal(new[] { first, second }, await ReadAsync(command));
    }

    [Fact]
    public async Task OrderByOutputAlias_PreservesOrdinaryOrdering()
    {
        await using var connection = await OpenAsync();
        await ExecuteAsync(connection, "UPDATE t SET value = 10 - id WHERE id <= 4");
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id AS value FROM t WHERE value >= @p ORDER BY value LIMIT 3";
        command.Parameters.AddWithValue("p", 1);
        command.Prepare();
        using var ordinary = connection.CreateCommand();
        ordinary.CommandText = "SELECT id AS value FROM t WHERE value >= @p AND 1 = 1 ORDER BY value LIMIT 3";
        ordinary.Parameters.AddWithValue("p", 1);
        Assert.Equal(await ReadAsync(ordinary), await ReadAsync(command));
    }
}
