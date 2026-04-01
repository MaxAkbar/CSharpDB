using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class ExpressionCompilerContractTests
{
    [Fact]
    public void CompileSpan_EvaluatesBetweenAndIsNullPredicates()
    {
        var schema = new TableSchema
        {
            TableName = "single_row",
            Columns =
            [
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "tag", Type = DbType.Text, Nullable = true },
            ],
        };

        Expression expression = new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = new BetweenExpression
            {
                Operand = new ColumnRefExpression { ColumnName = "score" },
                Low = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 10L },
                High = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 20L },
                Negated = false,
            },
            Right = new IsNullExpression
            {
                Operand = new ColumnRefExpression { ColumnName = "tag" },
                Negated = true,
            },
        };

        var evaluator = ExpressionCompiler.CompileSpan(expression, schema);

        Assert.True(evaluator([DbValue.FromInteger(15), DbValue.FromText("live")]).IsTruthy);
        Assert.False(evaluator([DbValue.FromInteger(25), DbValue.FromText("live")]).IsTruthy);
        Assert.False(evaluator([DbValue.FromInteger(15), DbValue.Null]).IsTruthy);
    }

    [Fact]
    public void CompileSpan_UnsupportedFunctionFallsBackToRowEvaluator()
    {
        var schema = new TableSchema
        {
            TableName = "fallback_row",
            Columns =
            [
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression expression = new FunctionCallExpression
        {
            FunctionName = "UNKNOWN_FUNC",
            Arguments = [new ColumnRefExpression { ColumnName = "score" }],
        };

        var evaluator = ExpressionCompiler.CompileSpan(expression, schema);

        var ex = Assert.Throws<CSharpDbException>(() => evaluator([DbValue.FromInteger(7)]));
        Assert.Contains("Unknown scalar function: UNKNOWN_FUNC", ex.Message);
    }

    [Fact]
    public void CompileJoinSpan_EvaluatesArithmeticAndTextFunctionPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "joined",
            Columns =
            [
                new ColumnDefinition { Name = "left_id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "left_score", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "right_id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "right_code", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression expression = new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new BinaryExpression
                {
                    Op = BinaryOp.Plus,
                    Left = new ColumnRefExpression { ColumnName = "left_id" },
                    Right = new ColumnRefExpression { ColumnName = "left_score" },
                },
                Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 12L },
            },
            Right = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new FunctionCallExpression
                {
                    FunctionName = "TEXT",
                    Arguments = [new ColumnRefExpression { ColumnName = "right_id" }],
                },
                Right = new ColumnRefExpression { ColumnName = "right_code" },
            },
        };

        var evaluator = ExpressionCompiler.CompileJoinSpan(expression, schema, leftColumnCount: 2);

        var matches = evaluator(
            [DbValue.FromInteger(2), DbValue.FromInteger(10)],
            [DbValue.FromInteger(3), DbValue.FromText("3")]);
        var misses = evaluator(
            [DbValue.FromInteger(1), DbValue.FromInteger(10)],
            [DbValue.FromInteger(4), DbValue.FromText("three")]);

        Assert.True(matches.IsTruthy);
        Assert.False(misses.IsTruthy);
    }

    [Fact]
    public void CompileJoinSpan_EvaluatesLikeInAndBetweenPredicates()
    {
        var schema = new TableSchema
        {
            TableName = "joined_predicates",
            Columns =
            [
                new ColumnDefinition { Name = "left_name", Type = DbType.Text, Nullable = false },
                new ColumnDefinition { Name = "left_score", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "right_code", Type = DbType.Text, Nullable = false },
                new ColumnDefinition { Name = "right_limit", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression expression = new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = new LikeExpression
            {
                Operand = new ColumnRefExpression { ColumnName = "left_name" },
                Pattern = new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "A%" },
                EscapeChar = null,
                Negated = false,
            },
            Right = new BinaryExpression
            {
                Op = BinaryOp.And,
                Left = new InExpression
                {
                    Operand = new ColumnRefExpression { ColumnName = "right_code" },
                    Values =
                    [
                        new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "X" },
                        new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "Y" },
                    ],
                    Negated = false,
                },
                Right = new BetweenExpression
                {
                    Operand = new ColumnRefExpression { ColumnName = "left_score" },
                    Low = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 10L },
                    High = new ColumnRefExpression { ColumnName = "right_limit" },
                    Negated = false,
                },
            },
        };

        var evaluator = ExpressionCompiler.CompileJoinSpan(expression, schema, leftColumnCount: 2);

        var matches = evaluator(
            [DbValue.FromText("Alpha"), DbValue.FromInteger(15)],
            [DbValue.FromText("Y"), DbValue.FromInteger(20)]);
        var misses = evaluator(
            [DbValue.FromText("Beta"), DbValue.FromInteger(25)],
            [DbValue.FromText("Z"), DbValue.FromInteger(20)]);

        Assert.True(matches.IsTruthy);
        Assert.False(misses.IsTruthy);
    }

    [Fact]
    public void CompileJoinSpan_WithRightColumnMap_EvaluatesCompactedJoinPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "joined_compacted",
            Columns =
            [
                new ColumnDefinition { Name = "left_id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "left_score", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "right_code", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "right_unused", Type = DbType.Text, Nullable = true },
                new ColumnDefinition { Name = "right_tail", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression expression = new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "right_code" },
                Right = new BinaryExpression
                {
                    Op = BinaryOp.Divide,
                    Left = new ColumnRefExpression { ColumnName = "left_score" },
                    Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 10L },
                },
            },
            Right = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "right_tail" },
                Right = new FunctionCallExpression
                {
                    FunctionName = "TEXT",
                    Arguments = [new ColumnRefExpression { ColumnName = "left_id" }],
                },
            },
        };

        var evaluator = ExpressionCompiler.CompileJoinSpan(
            expression,
            schema,
            leftColumnCount: 2,
            leftColumnMap: null,
            rightColumnMap: [0, -1, 1]);

        var matches = evaluator(
            [DbValue.FromInteger(2), DbValue.FromInteger(20)],
            [DbValue.FromInteger(2), DbValue.FromText("2")]);
        var misses = evaluator(
            [DbValue.FromInteger(3), DbValue.FromInteger(30)],
            [DbValue.FromInteger(2), DbValue.FromText("3")]);

        Assert.True(matches.IsTruthy);
        Assert.False(misses.IsTruthy);
    }
}
