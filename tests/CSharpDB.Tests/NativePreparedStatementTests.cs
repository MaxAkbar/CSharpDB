using CSharpDB.Engine;
using CSharpDB.Native;

namespace CSharpDB.Tests;

public sealed class NativePreparedStatementTests
{
    [Fact]
    public async Task ExecuteAsync_ReusesParameterizedInsertAcrossValues()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(db, "INSERT INTO t VALUES (@id, @name);");

        statement.BindInt64("@id", 1);
        statement.BindText("@name", "Alice");
        await using (var insert = await statement.ExecuteAsync(ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        statement.BindInt64("@id", 2);
        statement.BindText("@name", "Bob");
        await using (var insert = await statement.ExecuteAsync(ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        await using var query = await db.ExecuteAsync("SELECT name FROM t ORDER BY id;", ct);
        Assert.True(await query.MoveNextAsync(ct));
        Assert.Equal("Alice", query.Current[0].AsText);
        Assert.True(await query.MoveNextAsync(ct));
        Assert.Equal("Bob", query.Current[0].AsText);
        Assert.False(await query.MoveNextAsync(ct));
    }

    [Fact]
    public async Task ExecuteAsync_UnsupportedPreparedTemplate_FallsBackToSqlBinding()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY);", ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        await using (var insert = await db.ExecuteAsync("INSERT INTO t VALUES (1);", ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        await using (var insert = await db.ExecuteAsync("INSERT INTO t VALUES (2);", ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(db, "SELECT id FROM t ORDER BY id LIMIT @lim;");
        statement.BindInt64("@lim", 1);

        await using var result = await statement.ExecuteAsync(ct);
        Assert.True(result.IsQuery);
        Assert.True(await result.MoveNextAsync(ct));
        Assert.Equal(1L, result.Current[0].AsInteger);
        Assert.False(await result.MoveNextAsync(ct));
    }
}
