using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DecimalScalarAggregateFastPathTests
{
    [Fact]
    public async Task SumAndAvg_FastPathsPreserveEighteenDigitDecimals()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE decimal_fast_path (" +
            "id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, amount DECIMAL(18,2) NOT NULL)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO decimal_fast_path VALUES " +
            "(1, 7, 9999999999999999.99), " +
            "(2, 7, -9999999999999999.98)",
            ct);
        await database.ExecuteAsync(
            "CREATE INDEX ix_decimal_fast_path_bucket ON decimal_fast_path(bucket)",
            ct);

        await using (QueryResult sum = await database.ExecuteAsync(
            "SELECT SUM(amount) FROM decimal_fast_path",
            ct))
        {
            Assert.IsType<ScalarAggregateTableOperator>(sum.PhysicalRootOperator);
            Assert.Equal(0.01m, await ReadDecimalAsync(sum, ct));
        }

        await using (QueryResult average = await database.ExecuteAsync(
            "SELECT AVG(amount) FROM decimal_fast_path",
            ct))
        {
            Assert.IsType<ScalarAggregateTableOperator>(average.PhysicalRootOperator);
            Assert.Equal(0.005m, await ReadDecimalAsync(average, ct));
        }

        await using (QueryResult primaryKeyLookup = await database.ExecuteAsync(
            "SELECT SUM(amount) FROM decimal_fast_path WHERE id = 1",
            ct))
        {
            Assert.IsType<ScalarAggregateLookupOperator>(primaryKeyLookup.PhysicalRootOperator);
            Assert.Equal(9_999_999_999_999_999.99m, await ReadDecimalAsync(primaryKeyLookup, ct));
        }

        await using (QueryResult indexLookup = await database.ExecuteAsync(
            "SELECT AVG(amount) FROM decimal_fast_path WHERE bucket = 7",
            ct))
        {
            Assert.IsType<ScalarAggregateLookupOperator>(indexLookup.PhysicalRootOperator);
            Assert.Equal(0.005m, await ReadDecimalAsync(indexLookup, ct));
        }
    }

    private static async Task<decimal> ReadDecimalAsync(QueryResult result, CancellationToken ct)
    {
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(DbType.Decimal, row[0].Type);
        return row[0].AsDecimal;
    }
}
