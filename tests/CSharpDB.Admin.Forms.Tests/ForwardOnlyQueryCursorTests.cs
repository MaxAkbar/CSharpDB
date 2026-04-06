using CSharpDB.Client;

namespace CSharpDB.Admin.Forms.Tests;

public sealed class ForwardOnlyQueryCursorTests
{
    [Fact]
    public async Task ForwardOnlyQueryCursor_ReadsSequentialChunks()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var client = Assert.IsType<CSharpDbClient>(db.Client);

        await db.ExecuteAsync("CREATE TABLE cursor_rows (id INTEGER PRIMARY KEY, name TEXT);");
        await db.ExecuteAsync("INSERT INTO cursor_rows VALUES (1, 'Ada');");
        await db.ExecuteAsync("INSERT INTO cursor_rows VALUES (2, 'Grace');");
        await db.ExecuteAsync("INSERT INTO cursor_rows VALUES (3, 'Linus');");

        await using var cursor = await client.TryOpenForwardOnlyQueryCursorAsync(
            "SELECT id, name FROM cursor_rows ORDER BY id",
            TestContext.Current.CancellationToken);

        Assert.NotNull(cursor);
        Assert.Equal(["id", "name"], cursor.ColumnNames, StringComparer.OrdinalIgnoreCase);

        List<object?[]> first = await cursor.ReadNextAsync(2, TestContext.Current.CancellationToken);
        List<object?[]> second = await cursor.ReadNextAsync(2, TestContext.Current.CancellationToken);
        List<object?[]> third = await cursor.ReadNextAsync(2, TestContext.Current.CancellationToken);

        Assert.Equal(2, first.Count);
        Assert.Equal(1L, first[0][0]);
        Assert.Equal("Ada", first[0][1]);
        Assert.Equal(2L, first[1][0]);
        Assert.Equal("Grace", first[1][1]);

        Assert.Single(second);
        Assert.Equal(3L, second[0][0]);
        Assert.Equal("Linus", second[0][1]);

        Assert.Empty(third);
    }
}
