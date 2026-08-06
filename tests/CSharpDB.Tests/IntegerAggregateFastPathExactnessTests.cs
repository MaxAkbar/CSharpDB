using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class IntegerAggregateFastPathExactnessTests : IAsyncLifetime
{
    private Database _database = null!;

    public async ValueTask InitializeAsync()
    {
        _database = await Database.OpenInMemoryAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task PrimaryKeySum_PreservesUnitBeyondDoubleExactIntegerBoundary()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE exact_pk_sum (id INTEGER PRIMARY KEY, payload TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO exact_pk_sum VALUES (1, 'one'), (9007199254740992, 'large')",
            ct);

        await using QueryResult result = await ExecutePlannedAsync(
            "SELECT SUM(id) FROM exact_pk_sum",
            ct);

        Assert.IsType<TableKeyAggregateOperator>(GetRootOperator(result));
        List<CSharpDB.Primitives.DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(9007199254740993L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task SecondaryIndexSum_PreservesUnitBeyondDoubleExactIntegerBoundary()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE exact_index_sum (id INTEGER PRIMARY KEY, score INTEGER)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO exact_index_sum VALUES (1, 2), (2, 9007199254740993)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX idx_exact_index_sum_score ON exact_index_sum(score)",
            ct);

        await using QueryResult result = await ExecutePlannedAsync(
            "SELECT SUM(score) FROM exact_index_sum",
            ct);

        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));
        List<CSharpDB.Primitives.DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(9007199254740995L, rows[0][0].AsInteger);

        await using QueryResult averageResult = await ExecutePlannedAsync(
            "SELECT AVG(score) FROM exact_index_sum",
            ct);

        Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(averageResult));
        List<CSharpDB.Primitives.DbValue[]> averageRows = await averageResult.ToListAsync(ct);
        Assert.Single(averageRows);
        Assert.Equal(4503599627370498d, averageRows[0][0].AsReal);
    }

    [Fact]
    public async Task GroupedIndexSum_MultipliesRepeatedLargeKeysExactly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE exact_grouped_sum (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO exact_grouped_sum VALUES " +
            "(1, 9007199254740993), (2, 9007199254740993), (3, 9007199254740993)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX idx_exact_grouped_sum_score ON exact_grouped_sum(score)",
            ct);

        await using QueryResult result = await ExecutePlannedAsync(
            "SELECT score, SUM(score), AVG(score) FROM exact_grouped_sum GROUP BY score",
            ct);

        Assert.IsType<IndexGroupedAggregateOperator>(GetRootOperator(result));
        List<CSharpDB.Primitives.DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal(9007199254740993L, rows[0][0].AsInteger);
        Assert.Equal(27021597764222979L, rows[0][1].AsInteger);
        Assert.Equal(9007199254740994d, rows[0][2].AsReal);
    }

    [Fact]
    public async Task SecondaryIndexSum_ThrowsOnIntegerOverflowLikeGenericAggregate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE overflowing_index_sum (id INTEGER PRIMARY KEY, score INTEGER)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO overflowing_index_sum VALUES (1, 1), (2, 9223372036854775807)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX idx_overflowing_index_sum_score ON overflowing_index_sum(score)",
            ct);

        await Assert.ThrowsAsync<OverflowException>(async () =>
        {
            await using QueryResult result = await ExecutePlannedAsync(
                "SELECT SUM(score) FROM overflowing_index_sum",
                ct);
            Assert.IsType<IndexKeyAggregateOperator>(GetRootOperator(result));
            await result.ToListAsync(ct);
        });

        await Assert.ThrowsAsync<OverflowException>(async () =>
        {
            await using QueryResult result = await _database.ExecuteAsync(
                "SELECT SUM(score + 0) FROM overflowing_index_sum",
                ct);
            await result.ToListAsync(ct);
        });
    }

    private async ValueTask<QueryResult> ExecutePlannedAsync(string sql, CancellationToken ct)
    {
        SelectStatement statement = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        return await GetPlanner().ExecuteAsync(statement, ct);
    }

    private QueryPlanner GetPlanner()
    {
        FieldInfo plannerField = typeof(Database).GetField(
            "_planner",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Database planner field not found.");
        return (QueryPlanner?)plannerField.GetValue(_database)
            ?? throw new InvalidOperationException("Database planner was not initialized.");
    }

    private static IOperator GetRootOperator(QueryResult result)
    {
        FieldInfo operatorField = typeof(QueryResult).GetField(
            "_operator",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field not found.");
        if (operatorField.GetValue(result) is IOperator storedOperator)
            return UnwrapBatchAdapter(storedOperator);

        FieldInfo batchOperatorField = typeof(QueryResult).GetField(
            "_batchOperator",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
        return UnwrapBatchAdapter(
            (IOperator?)batchOperatorField.GetValue(result)
            ?? throw new InvalidOperationException("QueryResult did not contain an operator."));
    }

    private static IOperator UnwrapBatchAdapter(IOperator source)
        => source is BatchToRowOperatorAdapter adapter
            ? adapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : source;
}
