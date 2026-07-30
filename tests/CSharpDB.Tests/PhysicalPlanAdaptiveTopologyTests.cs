using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class PhysicalPlanAdaptiveTopologyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnalyzeCapture_RefreshesAdaptiveNodeToSelectedTopology()
    {
        ColumnDefinition[] schema =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        List<DbValue[]> rows = Enumerable.Range(1, 5)
            .Select(static value => new[] { DbValue.FromInteger(value) })
            .ToList();
        var lease = new AdaptiveQueryExecutionLease(
            new AdaptiveQueryReoptimizationOptions
            {
                Enabled = true,
                DivergenceFactor = 2,
                MinimumObservedRows = 1,
                MaxBufferedRows = 16,
                MaxReoptimizationsPerQuery = 1,
            });
        var diagnostics = new AdaptiveQueryReoptimizationRuntimeDiagnostics(
            recordAttempt: static () => { },
            recordSuccessfulSwitch: static () => { },
            recordRejectedSwitch: static _ => { },
            recordDivergence: static () => { },
            recordBufferedRows: static _ => { });

        using PhysicalPlanCaptureScope capture =
            PhysicalPlanCapture.Begin(collectActuals: true);
        const string redactedJoinPredicate = "(\"left_id\" = \"right_id\")";
        IOperator adaptive = new AdaptiveIndexNestedLoopJoinOperator(
            new MaterializedOperator(rows, schema),
            new MaterializedOperator([], schema),
            schema,
            createLookupJoin: static source => source,
            createHashJoin: static source => source,
            lease,
            diagnostics,
            estimatedOuterRows: 1,
            estimatedRowCount: null);
        _ = PhysicalPlanCapture.AnnotatePredicateIfActive(
            adaptive,
            redactedJoinPredicate);
        IOperator root = PhysicalPlanCapture.WrapRootIfActive(adaptive);

        long actualRows = 0;
        await root.OpenAsync(Ct);
        try
        {
            while (await root.MoveNextAsync(Ct))
                actualRows++;
        }
        finally
        {
            await root.DisposeAsync();
        }

        PhysicalPlan plan = capture.Context.CreatePlan(
            PhysicalOperatorType.Query,
            analyzesTarget: true,
            actualRows: actualRows,
            elapsed: TimeSpan.Zero);

        PhysicalPlanNode adaptiveNode = Assert.Single(plan.Root.Children);
        Assert.Equal(
            PhysicalOperatorType.AdaptiveIndexNestedLoopJoin,
            adaptiveNode.OperatorType);
        Assert.Equal(redactedJoinPredicate, adaptiveNode.Predicate);
        PhysicalPlanNode selectedChild = Assert.Single(adaptiveNode.Children);
        Assert.Equal(redactedJoinPredicate, selectedChild.Predicate);
        Assert.DoesNotContain(
            Enumerate(adaptiveNode),
            node =>
                node != adaptiveNode &&
                node.Status == PhysicalPlanNodeStatus.Planned);
    }

    [Fact]
    public async Task AnalyzeCapture_CountsBatchBackedRowAdapterMetrics()
    {
        using PhysicalPlanCaptureScope capture =
            PhysicalPlanCapture.Begin(collectActuals: true);
        IOperator root = PhysicalPlanCapture.WrapRootIfActive(
            new BatchToRowOperatorAdapter(new TwoRowBatchOperator()));

        List<DbValue[]> rows;
        await using (var result = new QueryResult(root))
            rows = await result.ToListAsync(Ct);

        PhysicalPlan plan = capture.Context.CreatePlan(
            PhysicalOperatorType.Query,
            analyzesTarget: true,
            actualRows: rows.Count,
            elapsed: TimeSpan.Zero);

        PhysicalPlanNode adapter = Assert.Single(plan.Root.Children);
        Assert.Equal(PhysicalOperatorType.BatchToRow, adapter.OperatorType);
        Assert.Equal(2, adapter.ActualRows);
        Assert.Equal(1, adapter.ActualLoops);
        Assert.Equal(PhysicalPlanNodeStatus.Completed, adapter.Status);
    }

    private static IEnumerable<PhysicalPlanNode> Enumerate(PhysicalPlanNode root)
    {
        var pending = new Stack<PhysicalPlanNode>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            PhysicalPlanNode node = pending.Pop();
            yield return node;
            for (int i = node.Children.Count - 1; i >= 0; i--)
                pending.Push(node.Children[i]);
        }
    }

    private sealed class TwoRowBatchOperator : IBatchOperator
    {
        private readonly RowBatch _batch = new(columnCount: 1, capacity: 2);
        private bool _emitted;

        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];

        public bool ReusesCurrentBatch => true;
        public RowBatch CurrentBatch => _batch;

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _emitted = false;
            _batch.Reset();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextBatchAsync(
            CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _batch.Reset();
            if (_emitted)
                return ValueTask.FromResult(false);

            _emitted = true;
            _batch.AppendRow([DbValue.FromInteger(1)]);
            _batch.AppendRow([DbValue.FromInteger(2)]);
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
