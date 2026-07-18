using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class SqlThreeValuedLogicTests
{
    private static readonly TableSchema EmptySchema = new()
    {
        TableName = "dual",
        Columns = Array.Empty<ColumnDefinition>(),
    };

    [Theory]
    [InlineData("NULL = 1")]
    [InlineData("NULL <> 1")]
    [InlineData("NULL < 1")]
    [InlineData("NULL > 1")]
    [InlineData("NOT NULL")]
    [InlineData("1 AND NULL")]
    [InlineData("0 OR NULL")]
    public void UnknownExpressions_ReturnNull(string sqlExpression)
    {
        DbValue result = Evaluate(sqlExpression);

        Assert.True(result.IsNull);
    }

    [Theory]
    [InlineData("0 AND NULL", 0)]
    [InlineData("NULL AND 0", 0)]
    [InlineData("1 OR NULL", 1)]
    [InlineData("NULL OR 1", 1)]
    public void LogicalOperators_ApplySqlThreeValuedTruthTables(string sqlExpression, long expected)
    {
        DbValue result = Evaluate(sqlExpression);

        Assert.Equal(DbType.Integer, result.Type);
        Assert.Equal(expected, result.AsInteger);
    }

    private static DbValue Evaluate(string sqlExpression)
    {
        var statement = Assert.IsType<SelectStatement>(Parser.Parse($"SELECT {sqlExpression}"));
        Expression expression = Assert.IsAssignableFrom<Expression>(statement.Columns[0].Expression);
        return ExpressionEvaluator.Evaluate(expression, Array.Empty<DbValue>(), EmptySchema);
    }
}
