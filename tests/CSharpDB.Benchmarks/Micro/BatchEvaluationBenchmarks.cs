using BenchmarkDotNet.Attributes;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class BatchEvaluationBenchmarks
{
    private const int BatchSize = 64;

    [Params(16_384)]
    public int RowCount { get; set; }

    private ColumnDefinition[] _schema = null!;
    private ColumnDefinition[] _outputSchema = null!;
    private TableSchema _tableSchema = null!;
    private RowBatch[] _batches = null!;
    private Func<DbValue[], DbValue> _predicate = null!;
    private Func<DbValue[], DbValue>[] _expressions = null!;
    private IFilterProjectionBatchPlan _delegateFilterPlan = null!;
    private IFilterProjectionBatchPlan _specializedFilterPlan = null!;
    private IFilterProjectionBatchPlan _delegateProjectionPlan = null!;
    private IFilterProjectionBatchPlan _specializedProjectionPlan = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _schema =
        [
            new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
            new ColumnDefinition { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        _outputSchema =
        [
            new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
            new ColumnDefinition { Name = "sum", Type = DbType.Integer, Nullable = false },
        ];
        _tableSchema = new TableSchema
        {
            TableName = "bench",
            Columns = _schema,
        };

        _predicate = row => DbValue.FromInteger(row[1].AsInteger < 80_000 ? 1 : 0);
        _expressions =
        [
            row => DbValue.FromInteger(row[0].AsInteger),
            row => DbValue.FromInteger(row[0].AsInteger + row[1].AsInteger),
        ];

        _delegateFilterPlan = new DelegateFilterProjectionBatchPlan(
            _predicate,
            Array.Empty<int>(),
            _expressions);
        _delegateProjectionPlan = new DelegateFilterProjectionBatchPlan(
            predicateEvaluator: null,
            Array.Empty<int>(),
            _expressions);

        Expression predicateExpression = new BinaryExpression
        {
            Op = BinaryOp.LessThan,
            Left = new ColumnRefExpression { ColumnName = "value" },
            Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = 80_000L },
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

        _specializedFilterPlan = BatchPlanCompiler.TryCreate(predicateExpression, projectionExpressions, _tableSchema)
            ?? throw new InvalidOperationException("Expected specialized filter batch plan.");
        _specializedProjectionPlan = BatchPlanCompiler.TryCreate(predicate: null, projectionExpressions, _tableSchema)
            ?? throw new InvalidOperationException("Expected specialized projection batch plan.");

        _batches = CreateBatches(RowCount);
    }

    [Benchmark(Description = "Delegate batch plan: projection")]
    public async Task<long> DelegateProjection()
        => await RunProjectionAsync(_delegateProjectionPlan);

    [Benchmark(Description = "Specialized batch plan: projection")]
    public async Task<long> SpecializedProjection()
        => await RunProjectionAsync(_specializedProjectionPlan);

    [Benchmark(Description = "Delegate batch plan: filter + projection")]
    public async Task<long> DelegateFilterProjection()
        => await RunFilterProjectionAsync(_delegateFilterPlan);

    [Benchmark(Description = "Specialized batch plan: filter + projection")]
    public async Task<long> SpecializedFilterProjection()
        => await RunFilterProjectionAsync(_specializedFilterPlan);

    private async Task<long> RunProjectionAsync(IFilterProjectionBatchPlan plan)
    {
        var source = new BatchSourceStub(_schema, _batches);
        var op = new ProjectionOperator(
            source,
            Array.Empty<int>(),
            _outputSchema,
            _expressions,
            plan);

        await op.OpenAsync();
        long checksum = 0;
        while (await op.MoveNextBatchAsync())
        {
            var batch = ((IBatchOperator)op).CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                checksum += batch.GetRowSpan(rowIndex)[1].AsInteger;
        }

        await op.DisposeAsync();
        return checksum;
    }

    private async Task<long> RunFilterProjectionAsync(IFilterProjectionBatchPlan plan)
    {
        var source = new BatchSourceStub(_schema, _batches);
        var op = new FilterProjectionOperator(
            source,
            _predicate,
            _outputSchema,
            _expressions,
            plan);

        await op.OpenAsync();
        long checksum = 0;
        while (await op.MoveNextBatchAsync())
        {
            var batch = ((IBatchOperator)op).CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                checksum += batch.GetRowSpan(rowIndex)[1].AsInteger;
        }

        await op.DisposeAsync();
        return checksum;
    }

    private static RowBatch[] CreateBatches(int rowCount)
    {
        int batchCount = (rowCount + BatchSize - 1) / BatchSize;
        var batches = new RowBatch[batchCount];
        int id = 1;

        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
        {
            int rowsInBatch = Math.Min(BatchSize, rowCount - (batchIndex * BatchSize));
            var batch = new RowBatch(columnCount: 2, capacity: rowsInBatch);
            for (int rowIndex = 0; rowIndex < rowsInBatch; rowIndex++, id++)
            {
                batch.CopyRowFrom(
                    rowIndex,
                    [DbValue.FromInteger(id), DbValue.FromInteger(id * 10L)]);
            }

            batches[batchIndex] = batch;
        }

        return batches;
    }

    private sealed class BatchSourceStub : IOperator, IBatchOperator
    {
        private readonly RowBatch[] _batches;
        private int _index = -1;

        public BatchSourceStub(ColumnDefinition[] schema, RowBatch[] batches)
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
