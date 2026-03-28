using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class BatchEvaluationContractTests
{
    [Fact]
    public void RowSelection_GrowsAndPreservesOrder()
    {
        var selection = new RowSelection(initialCapacity: 1);

        selection.Add(3);
        selection.Add(7);
        selection.Add(11);

        Assert.Equal(new[] { 3, 7, 11 }, selection.AsSpan().ToArray());
    }

    [Fact]
    public void DelegateFilterProjectionBatchPlan_FiltersAndProjectsExpressions()
    {
        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);

        var plan = new DelegateFilterProjectionBatchPlan(
            predicateEvaluator: row => DbValue.FromInteger(row[1].AsInteger >= 20 ? 1 : 0),
            columnIndices: Array.Empty<int>(),
            expressionEvaluators:
            [
                row => DbValue.FromInteger(row[0].AsInteger),
                row => DbValue.FromInteger(row[0].AsInteger + row[1].AsInteger),
            ]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 2, capacity: 4);

        int written = plan.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 1, 2 }, selection.AsSpan().ToArray());
        Assert.Equal(2, destination.Count);
        Assert.Equal(2L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(22L, destination.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(3L, destination.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(33L, destination.GetRowSpan(1)[1].AsInteger);
    }

    [Fact]
    public void DelegateFilterProjectionBatchPlan_ProjectsColumnsWithoutPredicate()
    {
        var source = new RowBatch(columnCount: 3, capacity: 3);
        source.CopyRowFrom(0, [DbValue.FromInteger(10), DbValue.FromInteger(100), DbValue.FromInteger(1000)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(20), DbValue.FromInteger(200), DbValue.FromInteger(2000)]);

        var plan = new DelegateFilterProjectionBatchPlan(
            predicateEvaluator: null,
            columnIndices: [2, 0],
            expressionEvaluators: null);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 2, capacity: 3);

        int written = plan.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 0, 1 }, selection.AsSpan().ToArray());
        Assert.Equal(1000L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(10L, destination.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(2000L, destination.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(20L, destination.GetRowSpan(1)[1].AsInteger);
    }

    [Fact]
    public async Task ProjectionOperator_BatchPlanPath_ProjectsBatchSource()
    {
        var ct = TestContext.Current.CancellationToken;
        var schema = new[]
        {
            new ColumnDefinition { Name = "a", Type = DbType.Integer },
            new ColumnDefinition { Name = "b", Type = DbType.Integer },
        };

        var source = new BatchSourceStub(
            schema,
            CreateBatch(
                [DbValue.FromInteger(1), DbValue.FromInteger(10)],
                [DbValue.FromInteger(2), DbValue.FromInteger(20)]));

        var plan = new DelegateFilterProjectionBatchPlan(
            predicateEvaluator: null,
            columnIndices: Array.Empty<int>(),
            expressionEvaluators:
            [
                row => DbValue.FromInteger(row[0].AsInteger),
                row => DbValue.FromInteger(row[0].AsInteger + row[1].AsInteger),
            ]);

        var op = new ProjectionOperator(
            source,
            Array.Empty<int>(),
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer },
                new ColumnDefinition { Name = "sum", Type = DbType.Integer },
            ],
            [
                row => DbValue.FromInteger(row[0].AsInteger),
                row => DbValue.FromInteger(row[0].AsInteger + row[1].AsInteger),
            ],
            plan);

        await op.OpenAsync(ct);
        Assert.True(await op.MoveNextBatchAsync(ct));

        var batch = ((IBatchOperator)op).CurrentBatch;
        Assert.Equal(2, batch.Count);
        Assert.Equal(1L, batch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(11L, batch.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(2L, batch.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(22L, batch.GetRowSpan(1)[1].AsInteger);
    }

    [Fact]
    public async Task FilterProjectionOperator_BatchPlanPath_SkipsEmptyBatchAndReturnsMatches()
    {
        var ct = TestContext.Current.CancellationToken;
        var schema = new[]
        {
            new ColumnDefinition { Name = "id", Type = DbType.Integer },
            new ColumnDefinition { Name = "score", Type = DbType.Integer },
        };

        var source = new BatchSourceStub(
            schema,
            CreateBatch(
                [DbValue.FromInteger(1), DbValue.FromInteger(5)],
                [DbValue.FromInteger(2), DbValue.FromInteger(8)]),
            CreateBatch(
                [DbValue.FromInteger(3), DbValue.FromInteger(15)],
                [DbValue.FromInteger(4), DbValue.FromInteger(25)]));

        Func<DbValue[], DbValue> predicate = row => DbValue.FromInteger(row[1].AsInteger >= 10 ? 1 : 0);
        Func<DbValue[], DbValue>[] expressions =
        [
            row => DbValue.FromInteger(row[0].AsInteger),
            row => DbValue.FromInteger(row[1].AsInteger * 2),
        ];

        var plan = new DelegateFilterProjectionBatchPlan(
            predicate,
            Array.Empty<int>(),
            expressions);

        var op = new FilterProjectionOperator(
            source,
            predicate,
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer },
                new ColumnDefinition { Name = "double_score", Type = DbType.Integer },
            ],
            expressions,
            plan);

        await op.OpenAsync(ct);
        Assert.True(await op.MoveNextBatchAsync(ct));

        var batch = ((IBatchOperator)op).CurrentBatch;
        Assert.Equal(2, batch.Count);
        Assert.Equal(3L, batch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(30L, batch.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(4L, batch.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(50L, batch.GetRowSpan(1)[1].AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsSimpleIntegerFilterAndProjection()
    {
        var schema = new TableSchema
        {
            TableName = "bench",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "value", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.LessThan,
            Left = new ColumnRefExpression { ColumnName = "value" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 25L },
        };
        Expression[] projections =
        [
            new ColumnRefExpression { ColumnName = "id" },
            new BinaryExpression
            {
                Op = BinaryOp.Plus,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new ColumnRefExpression { ColumnName = "value" },
            },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate, projections, schema);

        Assert.NotNull(plan);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(plan);

        var source = new RowBatch(columnCount: 3, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10), DbValue.FromText("A")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20), DbValue.FromText("B")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30), DbValue.FromText("C")]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 2, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 0, 1 }, selection.AsSpan().ToArray());
        Assert.Equal(1L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(11L, destination.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(2L, destination.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(22L, destination.GetRowSpan(1)[1].AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_ReturnsNull_ForUnsupportedProjectionShape()
    {
        var schema = new TableSchema
        {
            TableName = "bench",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression[] projections =
        [
            new FunctionCallExpression
            {
                FunctionName = "LOWER",
                Arguments = [new ColumnRefExpression { ColumnName = "category" }],
            },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate: null, projections, schema);

        Assert.Null(plan);
    }

    [Fact]
    public async Task FilterProjectionOperator_BatchPlanPath_UsesSpecializedPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        var schema = new[]
        {
            new ColumnDefinition { Name = "id", Type = DbType.Integer },
            new ColumnDefinition { Name = "value", Type = DbType.Integer },
        };

        var tableSchema = new TableSchema
        {
            TableName = "bench",
            Columns = schema,
        };

        var source = new BatchSourceStub(
            schema,
            CreateBatch(
                [DbValue.FromInteger(1), DbValue.FromInteger(10)],
                [DbValue.FromInteger(2), DbValue.FromInteger(20)]),
            CreateBatch(
                [DbValue.FromInteger(3), DbValue.FromInteger(30)],
                [DbValue.FromInteger(4), DbValue.FromInteger(40)]));

        Expression predicateExpression = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "value" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 20L },
        };
        Expression[] projectionExpressions =
        [
            new ColumnRefExpression { ColumnName = "id" },
            new BinaryExpression
            {
                Op = BinaryOp.Plus,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new ColumnRefExpression { ColumnName = "value" },
            },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicateExpression, projectionExpressions, tableSchema);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(plan);

        Func<DbValue[], DbValue> predicate = row => DbValue.FromInteger(row[1].AsInteger >= 20 ? 1 : 0);
        Func<DbValue[], DbValue>[] expressions =
        [
            row => DbValue.FromInteger(row[0].AsInteger),
            row => DbValue.FromInteger(row[0].AsInteger + row[1].AsInteger),
        ];

        var op = new FilterProjectionOperator(
            source,
            predicate,
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer },
                new ColumnDefinition { Name = "sum", Type = DbType.Integer },
            ],
            expressions,
            plan);

        await op.OpenAsync(ct);
        Assert.True(await op.MoveNextBatchAsync(ct));

        var firstBatch = ((IBatchOperator)op).CurrentBatch;
        Assert.Equal(1, firstBatch.Count);
        Assert.Equal(2L, firstBatch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(22L, firstBatch.GetRowSpan(0)[1].AsInteger);

        Assert.True(await op.MoveNextBatchAsync(ct));

        var secondBatch = ((IBatchOperator)op).CurrentBatch;
        Assert.Equal(2, secondBatch.Count);
        Assert.Equal(3L, secondBatch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(33L, secondBatch.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(4L, secondBatch.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(44L, secondBatch.GetRowSpan(1)[1].AsInteger);
        Assert.False(await op.MoveNextBatchAsync(ct));
    }

    [Fact]
    public void BatchPlanCompiler_BindsRealBetweenFilterAndProjection()
    {
        var schema = new TableSchema
        {
            TableName = "bench_real",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "price", Type = DbType.Real, Nullable = false },
            ],
        };

        Expression predicate = new BetweenExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "price" },
            Low = new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = 2.0d },
            High = new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = 3.5d },
            Negated = false,
        };
        Expression[] projections =
        [
            new ColumnRefExpression { ColumnName = "id" },
            new BinaryExpression
            {
                Op = BinaryOp.Plus,
                Left = new ColumnRefExpression { ColumnName = "price" },
                Right = new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = 0.5d },
            },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate, projections, schema);

        Assert.NotNull(plan);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromReal(1.5d)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromReal(2.0d)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromReal(3.0d)]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromReal(4.0d)]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 2, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 1, 2 }, selection.AsSpan().ToArray());
        Assert.Equal(2L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(2.5d, destination.GetRowSpan(0)[1].AsReal);
        Assert.Equal(3L, destination.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(3.5d, destination.GetRowSpan(1)[1].AsReal);
    }

    [Fact]
    public void BatchPlanCompiler_BindsOrderedTextPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "bench_text",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = new BinaryExpression
            {
                Op = BinaryOp.GreaterOrEqual,
                Left = new ColumnRefExpression { ColumnName = "category" },
                Right = new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "B" },
            },
            Right = new BinaryExpression
            {
                Op = BinaryOp.LessThan,
                Left = new ColumnRefExpression { ColumnName = "category" },
                Right = new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "D" },
            },
        };
        Expression[] projections =
        [
            new ColumnRefExpression { ColumnName = "category" },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate, projections, schema);

        Assert.NotNull(plan);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("A")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("B")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromText("C")]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromText("D")]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 1, 2 }, selection.AsSpan().ToArray());
        Assert.Equal("B", destination.GetRowSpan(0)[0].AsText);
        Assert.Equal("C", destination.GetRowSpan(1)[0].AsText);
    }

    [Fact]
    public void BatchPlanCompiler_BindsIntegerInPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "bench_in",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "value", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression predicate = new InExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "value" },
            Values =
            [
                new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 10L },
                new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 30L },
            ],
            Negated = false,
        };
        Expression[] projections =
        [
            new ColumnRefExpression { ColumnName = "id" },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate, projections, schema);

        Assert.NotNull(plan);
        Assert.IsType<SpecializedFilterProjectionBatchPlan>(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 0, 2 }, selection.AsSpan().ToArray());
        Assert.Equal(1L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(3L, destination.GetRowSpan(1)[0].AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsTextInPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "bench_text_in",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression predicate = new InExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "category" },
            Values =
            [
                new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "Beta" },
                new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "Gamma" },
            ],
            Negated = false,
        };

        var plan = BatchPlanCompiler.TryCreate(predicate, [new ColumnRefExpression { ColumnName = "category" }], schema);

        Assert.NotNull(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("Alpha")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("Beta")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromText("Gamma")]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal("Beta", destination.GetRowSpan(0)[0].AsText);
        Assert.Equal("Gamma", destination.GetRowSpan(1)[0].AsText);
    }

    [Fact]
    public void BatchPlanCompiler_BindsRealInPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "bench_real_in",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "price", Type = DbType.Real, Nullable = false },
            ],
        };

        Expression predicate = new InExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "price" },
            Values =
            [
                new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = 2.0d },
                new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 4L },
            ],
            Negated = false,
        };
        Expression[] projections =
        [
            new BinaryExpression
            {
                Op = BinaryOp.Plus,
                Left = new ColumnRefExpression { ColumnName = "price" },
                Right = new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = 0.25d },
            },
        ];

        var plan = BatchPlanCompiler.TryCreate(predicate, projections, schema);

        Assert.NotNull(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromReal(1.5d)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromReal(2.0d)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromReal(4.0d)]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(2.25d, destination.GetRowSpan(0)[0].AsReal);
        Assert.Equal(4.25d, destination.GetRowSpan(1)[0].AsReal);
    }

    [Fact]
    public void BatchPlanCompiler_BindsNegatedBetweenPredicate()
    {
        var schema = new TableSchema
        {
            TableName = "bench_not_between",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression predicate = new BetweenExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "score" },
            Low = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 20L },
            High = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 30L },
            Negated = true,
        };

        var plan = BatchPlanCompiler.TryCreate(predicate, [new ColumnRefExpression { ColumnName = "id" }], schema);

        Assert.NotNull(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromInteger(40)]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 0, 3 }, selection.AsSpan().ToArray());
        Assert.Equal(1L, destination.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(4L, destination.GetRowSpan(1)[0].AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsNegatedInPredicate_WithNullLiteral()
    {
        var schema = new TableSchema
        {
            TableName = "bench_not_in",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = true },
            ],
        };

        Expression predicate = new InExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "category" },
            Values =
            [
                new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "Alpha" },
                new LiteralExpression { LiteralType = TokenType.Null, Value = null },
            ],
            Negated = true,
        };

        var plan = BatchPlanCompiler.TryCreate(predicate, [new ColumnRefExpression { ColumnName = "id" }], schema);

        Assert.NotNull(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("Alpha")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("Beta")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.Null]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(0, written);
        Assert.Empty(selection.AsSpan().ToArray());
    }

    [Fact]
    public void BatchPlanCompiler_BindsLikePredicateWithEscape()
    {
        var schema = new TableSchema
        {
            TableName = "bench_like",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "label", Type = DbType.Text, Nullable = false },
            ],
        };

        Expression predicate = new LikeExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "label" },
            Pattern = new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "%!%%" },
            EscapeChar = new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = "!" },
            Negated = false,
        };

        var plan = BatchPlanCompiler.TryCreate(predicate, [new ColumnRefExpression { ColumnName = "label" }], schema);

        Assert.NotNull(plan);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("plain")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("100%")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromText("x%y")]);

        var selection = new RowSelection();
        var destination = new RowBatch(columnCount: 1, capacity: 4);
        int written = plan!.Execute(source, selection, destination);

        Assert.Equal(2, written);
        Assert.Equal(new[] { 1, 2 }, selection.AsSpan().ToArray());
        Assert.Equal("100%", destination.GetRowSpan(0)[0].AsText);
        Assert.Equal("x%y", destination.GetRowSpan(1)[0].AsText);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarCountStarAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_count_star",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression predicate = new BetweenExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "score" },
            Low = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 20L },
            High = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 40L },
            Negated = false,
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "COUNT",
            columnIndex: -1,
            isCountStar: true,
            isDistinct: false,
            schema);

        Assert.NotNull(plan);
        Assert.IsType<SpecializedScalarAggregateBatchPlan>(plan);
        Assert.Equal(2, plan!.PushdownFilters.Length);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromInteger(50)]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal(2L, plan.GetResult().AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarSumAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_sum",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = false },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "score" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 20L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "SUM",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: false,
            schema);

        Assert.NotNull(plan);
        Assert.Single(plan!.PushdownFilters);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.Null]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal(50L, plan.GetResult().AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarAvgAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_avg",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 2L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "AVG",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: false,
            schema);

        Assert.NotNull(plan);
        Assert.Single(plan!.PushdownFilters);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.Null]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromInteger(35)]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal(27.5d, plan.GetResult().AsReal);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarDistinctCountAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_count_distinct",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 2L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "COUNT",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: true,
            schema);

        Assert.NotNull(plan);
        Assert.Single(plan!.PushdownFilters);

        var source = new RowBatch(columnCount: 2, capacity: 5);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("alpha")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("beta")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromText("beta")]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.Null]);
        source.CopyRowFrom(4, [DbValue.FromInteger(5), DbValue.FromText("gamma")]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal(2L, plan.GetResult().AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarDistinctAvgAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_avg_distinct",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 2L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "AVG",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: true,
            schema);

        Assert.NotNull(plan);
        Assert.Single(plan!.PushdownFilters);

        var source = new RowBatch(columnCount: 2, capacity: 5);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(20)]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(20)]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.Null]);
        source.CopyRowFrom(4, [DbValue.FromInteger(5), DbValue.FromInteger(35)]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal(27.5d, plan.GetResult().AsReal);
    }

    [Fact]
    public void BatchPlanCompiler_DistinctScalarAggregatePlan_DeduplicatesAcrossBatches()
    {
        var schema = new TableSchema
        {
            TableName = "bench_count_distinct_batches",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 2L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "COUNT",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: true,
            schema);

        Assert.NotNull(plan);

        var firstBatch = new RowBatch(columnCount: 2, capacity: 3);
        firstBatch.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("alpha")]);
        firstBatch.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("beta")]);
        firstBatch.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromText("gamma")]);

        var secondBatch = new RowBatch(columnCount: 2, capacity: 3);
        secondBatch.CopyRowFrom(0, [DbValue.FromInteger(4), DbValue.FromText("beta")]);
        secondBatch.CopyRowFrom(1, [DbValue.FromInteger(5), DbValue.FromText("delta")]);
        secondBatch.CopyRowFrom(2, [DbValue.FromInteger(6), DbValue.Null]);

        plan!.Reset();
        plan.Accumulate(firstBatch);
        plan.Accumulate(secondBatch);

        Assert.Equal(3L, plan.GetResult().AsInteger);
    }

    [Fact]
    public void BatchPlanCompiler_DistinctScalarAggregatePlan_ResetClearsDistinctState()
    {
        var schema = new TableSchema
        {
            TableName = "bench_avg_distinct_batches",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "score", Type = DbType.Integer, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 1L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "AVG",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: true,
            schema);

        Assert.NotNull(plan);

        var firstRun = new RowBatch(columnCount: 2, capacity: 3);
        firstRun.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(10)]);
        firstRun.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(10)]);
        firstRun.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.FromInteger(30)]);

        var secondRun = new RowBatch(columnCount: 2, capacity: 2);
        secondRun.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromInteger(5)]);
        secondRun.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromInteger(15)]);

        plan!.Reset();
        plan.Accumulate(firstRun);
        Assert.Equal(20d, plan.GetResult().AsReal);

        plan.Reset();
        plan.Accumulate(secondRun);
        Assert.Equal(10d, plan.GetResult().AsReal);
    }

    [Fact]
    public void BatchPlanCompiler_BindsScalarTextMinAggregatePlan()
    {
        var schema = new TableSchema
        {
            TableName = "bench_min_text",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "category", Type = DbType.Text, Nullable = true },
            ],
        };

        Expression predicate = new BinaryExpression
        {
            Op = BinaryOp.GreaterOrEqual,
            Left = new ColumnRefExpression { ColumnName = "id" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 2L },
        };

        var plan = BatchPlanCompiler.TryCreateScalarAggregate(
            predicate,
            functionName: "MIN",
            columnIndex: 1,
            isCountStar: false,
            isDistinct: false,
            schema);

        Assert.NotNull(plan);
        Assert.Single(plan!.PushdownFilters);

        var source = new RowBatch(columnCount: 2, capacity: 4);
        source.CopyRowFrom(0, [DbValue.FromInteger(1), DbValue.FromText("zulu")]);
        source.CopyRowFrom(1, [DbValue.FromInteger(2), DbValue.FromText("delta")]);
        source.CopyRowFrom(2, [DbValue.FromInteger(3), DbValue.Null]);
        source.CopyRowFrom(3, [DbValue.FromInteger(4), DbValue.FromText("echo")]);

        plan.Reset();
        plan.Accumulate(source);

        Assert.Equal("delta", plan.GetResult().AsText);
    }

    private static RowBatch CreateBatch(params DbValue[][] rows)
    {
        int columnCount = rows.Length == 0 ? 0 : rows[0].Length;
        var batch = new RowBatch(columnCount, Math.Max(1, rows.Length));
        for (int i = 0; i < rows.Length; i++)
            batch.CopyRowFrom(i, rows[i]);

        return batch;
    }

    private sealed class BatchSourceStub : IOperator, IBatchOperator
    {
        private readonly RowBatch[] _batches;
        private int _index = -1;

        public BatchSourceStub(ColumnDefinition[] schema, params RowBatch[] batches)
        {
            OutputSchema = schema;
            _batches = batches;
        }

        public ColumnDefinition[] OutputSchema { get; }
        public DbValue[] Current => throw new NotSupportedException();
        public bool ReusesCurrentRowBuffer => false;
        public bool ReusesCurrentBatch => false;
        public RowBatch CurrentBatch { get; private set; } = new RowBatch(0, 0);

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            _index = -1;
            CurrentBatch = new RowBatch(0, 0);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
            => throw new NotSupportedException();

        public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        {
            _index++;
            if (_index >= _batches.Length)
                return ValueTask.FromResult(false);

            CurrentBatch = _batches[_index];
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
