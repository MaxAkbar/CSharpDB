using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class WindowIntervalSemanticTests
{
    [Fact]
    public async Task YearMonthIntervals_UseDurationOrderingForRanksAndMinMax()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await CreateIntervalDatabaseAsync(ct);

        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT id,
                   RANK() OVER ordered AS interval_rank,
                   DENSE_RANK() OVER ordered AS interval_dense_rank,
                   MIN(year_month) OVER whole_partition AS minimum_interval,
                   MAX(year_month) OVER whole_partition AS maximum_interval
            FROM interval_window_values
            WINDOW ordered AS (ORDER BY year_month),
                   whole_partition AS (
                       ORDER BY year_month
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            ORDER BY id
            """,
            ct);

        List<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(4, rows.Count);
        Assert.Equal([1L, 1L], ToRanks(rows[0]));
        Assert.Equal([2L, 2L], ToRanks(rows[1]));
        Assert.Equal([4L, 3L], ToRanks(rows[2]));
        Assert.Equal([2L, 2L], ToRanks(rows[3]));
        Assert.All(rows, static row =>
        {
            Assert.Equal("-1-00", row[3].AsText);
            Assert.Equal("10-00", row[4].AsText);
        });
    }

    [Fact]
    public async Task DaySecondIntervals_UseDurationOrderingForRanksAndMinMax()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await CreateIntervalDatabaseAsync(ct);

        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT id,
                   RANK() OVER ordered AS interval_rank,
                   DENSE_RANK() OVER ordered AS interval_dense_rank,
                   MIN(day_second) OVER whole_partition AS minimum_interval,
                   MAX(day_second) OVER whole_partition AS maximum_interval
            FROM interval_window_values
            WINDOW ordered AS (ORDER BY day_second),
                   whole_partition AS (
                       ORDER BY day_second
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            ORDER BY id
            """,
            ct);

        List<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(4, rows.Count);
        Assert.Equal([1L, 1L], ToRanks(rows[0]));
        Assert.Equal([2L, 2L], ToRanks(rows[1]));
        Assert.Equal([4L, 3L], ToRanks(rows[2]));
        Assert.Equal([2L, 2L], ToRanks(rows[3]));
        Assert.All(rows, static row =>
        {
            Assert.Equal(TimeSpan.FromDays(-1), TimeSpan.Parse(row[3].AsText));
            Assert.Equal(TimeSpan.FromDays(10), TimeSpan.Parse(row[4].AsText));
        });
    }

    private static async Task<Database> CreateIntervalDatabaseAsync(CancellationToken ct)
    {
        Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE interval_window_values (" +
            "id INTEGER PRIMARY KEY, " +
            "year_month INTERVAL YEAR TO MONTH, " +
            "day_second INTERVAL DAY TO SECOND)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO interval_window_values VALUES " +
            "(1, '-1-00', '-1.00:00:00'), " +
            "(2, '2-00', '2.00:00:00'), " +
            "(3, '10-00', '10.00:00:00'), " +
            "(4, '2-00', '2.00:00:00')",
            ct);
        return database;
    }

    private static long[] ToRanks(DbValue[] row) =>
        [row[1].AsInteger, row[2].AsInteger];
}
