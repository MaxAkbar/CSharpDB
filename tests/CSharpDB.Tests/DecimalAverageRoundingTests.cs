using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DecimalAverageRoundingTests
{
    private static readonly decimal RepeatingAverage = 1.66666666666666667m;

    [Fact]
    public void Accumulator_RoundsRepeatingDecimalAverageToEighteenSignificantDigits()
    {
        var accumulator = new NumericAggregateAccumulator();
        accumulator.Add(DbValue.FromDecimal(1m));
        accumulator.Add(DbValue.FromDecimal(2m));
        accumulator.Add(DbValue.FromDecimal(2m));

        DbValue average = accumulator.GetAverageOrNull();

        Assert.Equal(DbType.Decimal, average.Type);
        Assert.Equal(RepeatingAverage, average.AsDecimal);
        Assert.Equal(18, average.DecimalCoefficient.ToString().Length);
        Assert.Equal(17, average.DecimalScale);
    }

    [Fact]
    public void Accumulator_UsesMidpointToEvenAtMaximumDecimalScale()
    {
        var roundsDownToEven = new NumericAggregateAccumulator();
        roundsDownToEven.Add(DbValue.FromDecimalParts(1, 18));
        roundsDownToEven.Add(DbValue.FromDecimalParts(0, 0));

        var roundsUpToEven = new NumericAggregateAccumulator();
        roundsUpToEven.Add(DbValue.FromDecimalParts(3, 18));
        roundsUpToEven.Add(DbValue.FromDecimalParts(0, 0));

        Assert.Equal(0m, roundsDownToEven.GetAverageOrNull().AsDecimal);
        Assert.Equal(0.000000000000000002m, roundsUpToEven.GetAverageOrNull().AsDecimal);
    }

    [Fact]
    public async Task ScalarFilteredGroupedDistinctAndWindowAvg_UseOneDecimalContract()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE decimal_average_paths (" +
            "id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, amount DECIMAL(18,0) NOT NULL)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO decimal_average_paths VALUES (1, 7, 1), (2, 7, 2), (3, 7, 2)",
            ct);
        await database.ExecuteAsync(
            "CREATE INDEX ix_decimal_average_paths_bucket ON decimal_average_paths(bucket)",
            ct);

        await AssertSingleAverageAsync(
            database,
            "SELECT AVG(amount) FROM decimal_average_paths",
            RepeatingAverage,
            ct);
        await AssertSingleAverageAsync(
            database,
            "SELECT AVG(amount) FROM decimal_average_paths WHERE bucket = 7",
            RepeatingAverage,
            ct);

        await using (QueryResult grouped = await database.ExecuteAsync(
            "SELECT bucket, AVG(amount) AS average_amount " +
            "FROM decimal_average_paths GROUP BY bucket",
            ct))
        {
            AssertAverageMetadata(grouped.Schema[1]);
            DbValue[] row = Assert.Single(await grouped.ToListAsync(ct));
            Assert.Equal(7L, row[0].AsInteger);
            Assert.Equal(RepeatingAverage, row[1].AsDecimal);
        }

        await AssertSingleAverageAsync(
            database,
            "SELECT AVG(DISTINCT amount) FROM decimal_average_paths",
            1.5m,
            ct);

        await using (QueryResult window = await database.ExecuteAsync(
            "SELECT id, AVG(amount) OVER (PARTITION BY bucket) AS average_amount " +
            "FROM decimal_average_paths ORDER BY id",
            ct))
        {
            AssertAverageMetadata(window.Schema[1]);
            List<DbValue[]> rows = await window.ToListAsync(ct);
            Assert.Equal(3, rows.Count);
            Assert.All(rows, row => Assert.Equal(RepeatingAverage, row[1].AsDecimal));
        }
    }

    private static async Task AssertSingleAverageAsync(
        Database database,
        string sql,
        decimal expected,
        CancellationToken ct)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, ct);
        AssertAverageMetadata(result.Schema[0]);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(DbType.Decimal, row[0].Type);
        Assert.Equal(expected, row[0].AsDecimal);
    }

    private static void AssertAverageMetadata(ColumnDefinition column)
    {
        Assert.Equal(DbType.Decimal, column.Type);
        Assert.Equal(
            DecimalAggregateSemantics.AverageResultType,
            column.DeclaredType);
        Assert.Equal("DECIMAL(18)", column.DeclaredType!.ToSql());
    }
}
