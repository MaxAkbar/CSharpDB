using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Native;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class WindowFunctionPhase5PreparedTests
{
    [Fact]
    public async Task NativePreparedStatement_RebindsWindowArgumentsAcrossExecutions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE samples (id INTEGER PRIMARY KEY, group_id INTEGER, score INTEGER)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO samples VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 40)",
            ct);

        NativePreparedStatement statement = NativePreparedStatement.Create(
            database,
            """
            SELECT id,
                   LAG(score, @offset, @fallback) OVER (
                       PARTITION BY group_id ORDER BY id
                       ROWS BETWEEN CURRENT ROW AND CURRENT ROW
                   ) AS previous_score
            FROM samples
            WHERE group_id = @group_id
            ORDER BY id
            """);

        statement.BindInt64("@offset", 1);
        statement.BindInt64("@fallback", -1);
        statement.BindInt64("@group_id", 1);
        await using (QueryResult first = await statement.ExecuteAsync(ct))
        {
            List<DbValue[]> rows = await first.ToListAsync(ct);
            Assert.Equal([1L, -1L], ToIntegers(rows[0]));
            Assert.Equal([2L, 10L], ToIntegers(rows[1]));
            Assert.Equal([3L, 20L], ToIntegers(rows[2]));
        }

        statement.BindInt64("@offset", 2);
        statement.BindInt64("@fallback", -2);
        await using (QueryResult second = await statement.ExecuteAsync(ct))
        {
            List<DbValue[]> rows = await second.ToListAsync(ct);
            Assert.Equal([1L, -2L], ToIntegers(rows[0]));
            Assert.Equal([2L, -2L], ToIntegers(rows[1]));
            Assert.Equal([3L, 10L], ToIntegers(rows[2]));
        }
    }

    private static long[] ToIntegers(DbValue[] row) =>
        row.Select(value => value.AsInteger).ToArray();
}
