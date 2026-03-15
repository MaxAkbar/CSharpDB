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

        var batch = ((IBatchOperator)op).CurrentBatch;
        Assert.Equal(3, batch.Count);
        Assert.Equal(2L, batch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(22L, batch.GetRowSpan(0)[1].AsInteger);
        Assert.Equal(3L, batch.GetRowSpan(1)[0].AsInteger);
        Assert.Equal(33L, batch.GetRowSpan(1)[1].AsInteger);
        Assert.Equal(4L, batch.GetRowSpan(2)[0].AsInteger);
        Assert.Equal(44L, batch.GetRowSpan(2)[1].AsInteger);
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
