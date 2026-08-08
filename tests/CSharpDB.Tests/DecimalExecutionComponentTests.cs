using CSharpDB.Execution;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class DecimalExecutionComponentTests
{
    [Fact]
    public async Task AnalyzeDecimalColumn_PersistsExactBoundsAndBuildsRangeEstimate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_decimal_stats_{Guid.NewGuid():N}.db");

        try
        {
            await using (Database database = await Database.OpenAsync(path, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE decimal_stats (" +
                    "id INTEGER PRIMARY KEY, amount DECIMAL(10,2), category TEXT)",
                    ct);
                await database.ExecuteAsync(
                    "CREATE INDEX ix_decimal_stats_amount_category " +
                    "ON decimal_stats(amount, category)",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO decimal_stats VALUES " +
                    "(1, 1.10, 'a'), (2, 2.20, 'b'), (3, 3.30, 'c')",
                    ct);
                await database.ExecuteAsync("ANALYZE decimal_stats", ct);
            }

            await using Database reopened = await Database.OpenAsync(path, ct);
            await using QueryResult stats = await reopened.ExecuteAsync(
                "SELECT table_name, column_name, min_value, max_value FROM sys.column_stats",
                ct);
            DbValue[] amountStats = Assert.Single(
                await stats.ToListAsync(ct),
                static row => row[0].AsText == "decimal_stats" && row[1].AsText == "amount");
            Assert.Equal(1.1m, amountStats[2].AsDecimal);
            Assert.Equal(3.3m, amountStats[3].AsDecimal);

            await using QueryResult estimate = await reopened.ExecuteAsync(
                "EXPLAIN ESTIMATE FOR SELECT * FROM decimal_stats " +
                "WHERE amount BETWEEN 1.00 AND 2.50",
                ct);
            Assert.NotEmpty(await estimate.ToListAsync(ct));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task ScalarAggregate_CastAroundDecimalSum_IsCollectedAndEvaluated()
    {
        ColumnDefinition[] columns =
        [
            new() { Name = "amount", Type = DbType.Decimal, Nullable = false },
        ];
        var schema = new TableSchema { TableName = "values", Columns = columns };
        var source = new MaterializedOperator(
            [
                [DbValue.FromDecimal(1.25m)],
                [DbValue.FromDecimal(2.75m)],
            ],
            columns);
        var sum = new FunctionCallExpression
        {
            FunctionName = "SUM",
            Arguments = [new ColumnRefExpression { ColumnName = "amount" }],
        };
        var targetType = new SqlTypeDescriptor(
            SqlTypeKind.Decimal,
            precision: 10,
            scale: 1);
        var aggregate = new ScalarAggregateOperator(
            source,
            [new SelectColumn
            {
                Expression = new CastExpression
                {
                    Operand = sum,
                    TargetType = targetType,
                },
            }],
            havingExpr: null,
            schema,
            [new ColumnDefinition
            {
                Name = "total",
                Type = DbType.Decimal,
                DeclaredType = targetType,
                Nullable = true,
            }]);

        CancellationToken ct = TestContext.Current.CancellationToken;
        await aggregate.OpenAsync(ct);
        try
        {
            Assert.True(await aggregate.MoveNextAsync(ct));
            Assert.Equal(DbType.Decimal, aggregate.Current[0].Type);
            Assert.Equal(4.0m, aggregate.Current[0].AsDecimal);
            Assert.False(await aggregate.MoveNextAsync(ct));
        }
        finally
        {
            await aggregate.DisposeAsync();
        }
    }

    [Fact]
    public void WindowRewrite_PreservesCastAndReplacesNestedWindowSlot()
    {
        var window = new WindowFunctionExpression
        {
            Function = new FunctionCallExpression
            {
                FunctionName = "SUM",
                Arguments = [new ColumnRefExpression { ColumnName = "amount" }],
            },
            Window = new WindowSpecification(),
        };
        var targetType = new SqlTypeDescriptor(
            SqlTypeKind.Decimal,
            precision: 12,
            scale: 2);
        var expression = new CastExpression
        {
            Operand = window,
            TargetType = targetType,
        };
        var slots = new Dictionary<string, string>
        {
            [WindowExpressionSupport.GetExpressionKey(window)] = "__window_0",
        };

        var rewritten = Assert.IsType<CastExpression>(
            WindowExpressionSupport.RewriteWindowFunctions(expression, slots));
        Assert.Equal(targetType, rewritten.TargetType);
        Assert.Equal(
            "__window_0",
            Assert.IsType<ColumnRefExpression>(rewritten.Operand).ColumnName);
    }

    [Fact]
    public void NumericAggregateAccumulator_PreservesExactDecimalSumAndAverage()
    {
        var accumulator = new NumericAggregateAccumulator();
        accumulator.Add(DbValue.FromDecimal(1.1m));
        accumulator.Add(DbValue.FromInteger(2));
        accumulator.Add(DbValue.FromDecimal(3.5m));

        DbValue sum = accumulator.GetSumOrZero();
        DbValue average = accumulator.GetAverageOrNull();

        Assert.Equal(DbType.Decimal, sum.Type);
        Assert.Equal(6.6m, sum.AsDecimal);
        Assert.Equal(DbType.Decimal, average.Type);
        Assert.Equal(2.2m, average.AsDecimal);
    }

    [Fact]
    public void NumericAggregateAccumulator_MixedRealAndDecimalKeepsRealCompatibility()
    {
        var accumulator = new NumericAggregateAccumulator();
        accumulator.Add(DbValue.FromDecimal(1.25m));
        accumulator.Add(DbValue.FromReal(2.75d));

        Assert.Equal(DbType.Real, accumulator.GetSumOrZero().Type);
        Assert.Equal(4d, accumulator.GetSumOrZero().AsReal);
        Assert.Equal(DbType.Real, accumulator.GetAverageOrNull().Type);
        Assert.Equal(2d, accumulator.GetAverageOrNull().AsReal);
    }

    [Fact]
    public void NumericScalarFunctions_PreserveDecimalValues()
    {
        Assert.True(DbBuiltInScalarFunctions.TryEvaluate(
            "ABS",
            [DbValue.FromDecimal(-12.34m)],
            out DbValue absolute));
        Assert.Equal(DbType.Decimal, absolute.Type);
        Assert.Equal(12.34m, absolute.AsDecimal);

        Assert.True(DbBuiltInScalarFunctions.TryEvaluate(
            "ROUND",
            [DbValue.FromDecimal(12.345m), DbValue.FromInteger(2)],
            out DbValue rounded));
        Assert.Equal(DbType.Decimal, rounded.Type);
        Assert.Equal(12.34m, rounded.AsDecimal);

        Assert.Equal(
            "12.34",
            DbBuiltInScalarFunctions.ToDisplayText(DbValue.FromDecimal(12.34m)));
    }
}
