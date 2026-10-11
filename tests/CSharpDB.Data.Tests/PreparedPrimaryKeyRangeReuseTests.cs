using System.Data;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public sealed class PreparedPrimaryKeyRangeReuseTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<CSharpDbConnection> OpenAsync()
    {
        var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        await ExecuteAsync(connection, "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)");
        await ExecuteAsync(connection, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, NULL)");
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

    [Fact]
    public async Task ChangingBoundTypesAndNulls_MatchesOrdinaryBinding()
    {
        await using var connection = await OpenAsync();
        using var prepared = connection.CreateCommand();
        prepared.CommandText = "SELECT id FROM t WHERE id >= @lower AND id < @upper ORDER BY id";
        var lower = prepared.Parameters.AddWithValue("lower", 1L);
        prepared.Parameters.AddWithValue("upper", 5L);
        prepared.Prepare();
        using var ordinary = connection.CreateCommand();
        ordinary.CommandText = "SELECT id FROM t WHERE id >= @lower AND id < @upper AND 1 = 1 ORDER BY id";
        var controlLower = ordinary.Parameters.AddWithValue("lower", 1L);
        ordinary.Parameters.AddWithValue("upper", 5L);

        async Task CompareAsync()
        {
            long[]? actual = null, expected = null;
            Exception? actualError = await Record.ExceptionAsync(async () => actual = await ReadAsync(prepared));
            Exception? expectedError = await Record.ExceptionAsync(async () => expected = await ReadAsync(ordinary));
            Assert.Equal(expectedError?.GetType(), actualError?.GetType());
            if (actualError is null) Assert.Equal(expected, actual);
        }

        object?[] values = [1L, 3, (short)2, (uint)4, null, DBNull.Value, 2m, 2.5m, 2d, "2", true, new byte[] { 2 }, 1L];
        foreach (object? value in values)
        {
            lower.Value = controlLower.Value = value;
            await CompareAsync();
        }
        lower.Value = controlLower.Value = 1;
        lower.DbType = controlLower.DbType = DbType.Boolean;
        await CompareAsync();
        lower.DbType = controlLower.DbType = DbType.Int64;
        Assert.Equal(new long[] { 1, 2, 3, 4 }, await ReadAsync(prepared));
    }

    [Fact]
    public async Task OpenReadersRetainBounds_AndLaterSchemaChangesRefreshStarMetadata()
    {
        await using var connection = await OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id LIMIT 2";
        var lower = command.Parameters.AddWithValue("lower", 1L);
        var upper = command.Parameters.AddWithValue("upper", 5L);
        command.Prepare();
        await using (var first = await command.ExecuteReaderAsync(Ct))
        {
            lower.Value = 3L; upper.Value = 6L;
            await using var second = await command.ExecuteReaderAsync(Ct);
            lower.Value = 99L;
            Assert.True(await second.ReadAsync(Ct)); Assert.Equal(3, second.GetInt64(0));
            Assert.True(await first.ReadAsync(Ct)); Assert.Equal(1, first.GetInt64(0));
            Assert.True(await second.ReadAsync(Ct)); Assert.Equal(4, second.GetInt64(0));
            Assert.True(await first.ReadAsync(Ct)); Assert.Equal(2, first.GetInt64(0));
            Assert.False(await second.ReadAsync(Ct)); Assert.False(await first.ReadAsync(Ct));
        }
        await ExecuteAsync(connection, "ALTER TABLE t ADD COLUMN added TEXT");
        lower.Value = 4L; upper.Value = 6L;
        await using var expanded = await command.ExecuteReaderAsync(Ct);
        Assert.Equal(3, expanded.FieldCount);
        Assert.Equal("added", expanded.GetName(2));
        foreach (long id in new long[] { 4, 5 })
        {
            Assert.True(await expanded.ReadAsync(Ct));
            Assert.Equal(id, expanded.GetInt64(0));
            Assert.True(expanded.IsDBNull(2));
            if (id == 5) Assert.True(expanded.IsDBNull(1));
        }
        Assert.False(await expanded.ReadAsync(Ct));
    }

    [Theory]
    [InlineData("SMALLINT", false)]
    [InlineData("INTEGER", true)]
    public async Task BackedAndCompositePrimaryKeyRanges_MatchOrdinaryExecution(string keyType, bool composite)
    {
        await using var connection = await OpenAsync();
        await ExecuteAsync(connection, "DROP TABLE t");
        await ExecuteAsync(connection, composite
            ? $"CREATE TABLE t (id {keyType}, value INTEGER, PRIMARY KEY (id, value))"
            : $"CREATE TABLE t (id {keyType} PRIMARY KEY, value INTEGER)");
        await ExecuteAsync(connection, composite
            ? "INSERT INTO t VALUES (11, 1), (11, 2), (22, 3)"
            : "INSERT INTO t VALUES (11, 1), (22, 3)");
        using var prepared = connection.CreateCommand();
        prepared.CommandText = "SELECT id FROM t WHERE id >= @lower AND id < @upper ORDER BY id";
        var lower = prepared.Parameters.AddWithValue("lower", 10L);
        var upper = prepared.Parameters.AddWithValue("upper", 30L);
        prepared.Prepare();
        Assert.Equal(composite ? new long[] { 11, 11, 22 } : [11L, 22L], await ReadAsync(prepared));
        lower.Value = 20L; upper.Value = 23L;
        Assert.Equal(new long[] { 22 }, await ReadAsync(prepared));
        lower.Value = 1L; upper.Value = 3L;
        Assert.Empty(await ReadAsync(prepared));
    }
}
