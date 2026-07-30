using System.Diagnostics;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal sealed class PhysicalPlanCaptureScope : IDisposable
{
    private readonly PhysicalPlanCaptureContext? _previous;
    private bool _disposed;

    internal PhysicalPlanCaptureScope(bool collectActuals)
    {
        _previous = PhysicalPlanCapture.Current;
        Context = new PhysicalPlanCaptureContext(collectActuals);
        PhysicalPlanCapture.Current = Context;
    }

    internal PhysicalPlanCaptureContext Context { get; }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        PhysicalPlanCapture.Current = _previous;
    }
}

internal static class PhysicalPlanCapture
{
    private static readonly AsyncLocal<PhysicalPlanCaptureContext?> s_current = new();

    internal static PhysicalPlanCaptureContext? Current
    {
        get => s_current.Value;
        set => s_current.Value = value;
    }

    internal static PhysicalPlanCaptureScope Begin(bool collectActuals)
        => new(collectActuals);

    internal static bool IsActive => Current is not null;

    internal static IOperator WrapIfActive(IOperator source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return Current?.Wrap(source) ?? source;
    }

    internal static IOperator WrapRootIfActive(IOperator source)
    {
        IOperator wrapped = WrapIfActive(source);
        Current?.MarkRoot(wrapped);
        return wrapped;
    }

    internal static void MarkRootIfActive(IOperator source)
        => Current?.MarkRoot(source);

    internal static IOperator AnnotatePredicateIfActive(
        IOperator source,
        string? predicate)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!string.IsNullOrWhiteSpace(predicate))
            Current?.AnnotatePredicate(source, predicate);

        return source;
    }

    internal static IOperator Unwrap(IOperator source)
        => source is PhysicalProfilingOperator profiled
            ? profiled.Source
            : source;
}

internal sealed class PhysicalPlanCaptureContext
{
    private readonly Dictionary<IOperator, PhysicalProfilingOperator> _wrappers =
        new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<IOperator, string> _predicates =
        new(ReferenceEqualityComparer.Instance);
    private readonly List<PhysicalPlanNode> _nodes = [];
    private readonly List<PhysicalPlanNode> _preferredRoots = [];

    internal PhysicalPlanCaptureContext(bool collectActuals)
    {
        CollectActuals = collectActuals;
    }

    internal bool CollectActuals { get; }

    internal IOperator Wrap(IOperator source)
    {
        if (source is PhysicalProfilingOperator existingWrapper)
            return existingWrapper;

        if (_wrappers.TryGetValue(source, out PhysicalProfilingOperator? existing))
            return existing;

        PhysicalPlanNode node = PhysicalOperatorDescriptorFactory.Create(source);
        if (_predicates.TryGetValue(source, out string? predicate))
            node.ApplyMetadata(new PhysicalOperatorMetadata(Predicate: predicate));

        PhysicalProfilingOperator wrapper = source switch
        {
            IBatchOperator batchSource =>
                new PhysicalProfilingBatchOperator(
                    source,
                    batchSource,
                    node,
                    CollectActuals,
                    this),
            IBatchBackedRowOperator batchBacked =>
                new PhysicalProfilingBatchBackedRowOperator(
                    source,
                    batchBacked.BatchSource,
                    node,
                    CollectActuals,
                    this),
            _ => new PhysicalProfilingOperator(
                source,
                node,
                CollectActuals,
                this),
        };
        _wrappers.Add(source, wrapper);
        _nodes.Add(node);
        RefreshTopology(source, node);

        return wrapper;
    }

    internal void AnnotatePredicate(IOperator source, string predicate)
    {
        IOperator unwrapped = PhysicalPlanCapture.Unwrap(source);
        if (_predicates.TryGetValue(unwrapped, out string? existingPredicate) &&
            !string.Equals(existingPredicate, predicate, StringComparison.Ordinal))
        {
            predicate = $"({existingPredicate}) AND ({predicate})";
        }

        _predicates[unwrapped] = predicate;
        if (_wrappers.TryGetValue(unwrapped, out PhysicalProfilingOperator? wrapper))
        {
            wrapper.Node.ApplyMetadata(
                new PhysicalOperatorMetadata(Predicate: predicate));
        }
    }

    internal void MarkRoot(IOperator source)
    {
        PhysicalProfilingOperator? wrapper = source as PhysicalProfilingOperator;
        if (wrapper is null)
        {
            IOperator unwrapped = PhysicalPlanCapture.Unwrap(source);
            _wrappers.TryGetValue(unwrapped, out wrapper);
        }

        if (wrapper is not null && !_preferredRoots.Contains(wrapper.Node))
            _preferredRoots.Add(wrapper.Node);
    }

    internal void RefreshTopology(IOperator source, PhysicalPlanNode node)
    {
        if (source is not IPhysicalOperatorChildren children)
            return;

        IReadOnlyList<IOperator> physicalChildren = children.PhysicalChildren;
        var childNodes = new List<PhysicalPlanNode>(physicalChildren.Count);
        string? adaptivePredicate = null;
        bool propagateAdaptivePredicate =
            source is AdaptiveIndexNestedLoopJoinOperator &&
            physicalChildren.Count == 1 &&
            _predicates.TryGetValue(source, out adaptivePredicate);
        for (int i = 0; i < physicalChildren.Count; i++)
        {
            IOperator child = physicalChildren[i];
            if (propagateAdaptivePredicate)
                AnnotatePredicate(child, adaptivePredicate!);

            PhysicalProfilingOperator childWrapper = child as PhysicalProfilingOperator
                ?? (PhysicalProfilingOperator)Wrap(child);
            childNodes.Add(childWrapper.Node);
        }

        node.ReplaceChildren(childNodes);
    }

    internal PhysicalPlan CreatePlan(
        PhysicalOperatorType statementType,
        bool analyzesTarget,
        string? objectName = null,
        long? actualRows = null,
        TimeSpan? elapsed = null,
        string? predicate = null)
    {
        var statementRoot = new PhysicalPlanNode
        {
            OperatorType = statementType,
            ObjectName = objectName,
            Predicate = predicate,
        };

        var childNodes = new HashSet<PhysicalPlanNode>(
            ReferenceEqualityComparer.Instance);
        for (int i = 0; i < _nodes.Count; i++)
        {
            IReadOnlyList<PhysicalPlanNode> children = _nodes[i].Children;
            for (int childIndex = 0; childIndex < children.Count; childIndex++)
                childNodes.Add(children[childIndex]);
        }

        var reachable = new HashSet<PhysicalPlanNode>(
            ReferenceEqualityComparer.Instance);
        for (int i = 0; i < _preferredRoots.Count; i++)
        {
            PhysicalPlanNode preferredRoot = _preferredRoots[i];
            if (childNodes.Contains(preferredRoot))
                continue;

            statementRoot.AddChild(preferredRoot);
            AddReachable(preferredRoot, reachable);
        }

        if (CollectActuals)
        {
            for (int i = 0; i < _nodes.Count; i++)
            {
                PhysicalPlanNode node = _nodes[i];
                if (reachable.Contains(node) ||
                    childNodes.Contains(node) ||
                    !node.ActualLoops.HasValue)
                {
                    continue;
                }

                statementRoot.AddChild(node);
                AddReachable(node, reachable);
            }
        }

        if (analyzesTarget && actualRows.HasValue && elapsed.HasValue)
        {
            statementRoot.SetActuals(actualRows.Value, loops: 1, elapsed.Value);
            statementRoot.MarkCompleted();
        }

        PopulateDerivedEstimatesAndCosts(statementRoot);
        return new PhysicalPlan(statementRoot, analyzesTarget);
    }

    private static void PopulateDerivedEstimatesAndCosts(PhysicalPlanNode root)
    {
        var completed = new HashSet<PhysicalPlanNode>(
            ReferenceEqualityComparer.Instance);
        var active = new HashSet<PhysicalPlanNode>(
            ReferenceEqualityComparer.Instance);
        PopulateDerivedEstimatesAndCosts(root, completed, active);
    }

    private static double PopulateDerivedEstimatesAndCosts(
        PhysicalPlanNode node,
        HashSet<PhysicalPlanNode> completed,
        HashSet<PhysicalPlanNode> active)
    {
        if (completed.Contains(node))
            return node.EstimatedCost ?? 0d;
        if (!active.Add(node))
            return 0d;

        double childCost = 0d;
        for (int i = 0; i < node.Children.Count; i++)
        {
            childCost = SaturatingAdd(
                childCost,
                PopulateDerivedEstimatesAndCosts(
                    node.Children[i],
                    completed,
                    active));
        }

        node.EstimatedRows ??= DeriveEstimatedRows(node);
        if (!node.EstimatedCost.HasValue)
        {
            // Stable relative row-work units: one setup unit per operator,
            // plus its estimated output rows and all child work. This is a
            // planning comparison metric, not elapsed time.
            double localWork = 1d + Math.Max(0d, node.EstimatedRows ?? 0L);
            node.EstimatedCost = SaturatingAdd(childCost, localWork);
        }

        active.Remove(node);
        completed.Add(node);
        return node.EstimatedCost.Value;
    }

    private static long? DeriveEstimatedRows(PhysicalPlanNode node)
    {
        if (node.Children.Count == 0)
            return null;

        if (node.OperatorType == PhysicalOperatorType.Concatenate)
        {
            long total = 0;
            for (int i = 0; i < node.Children.Count; i++)
            {
                long? childRows = node.Children[i].EstimatedRows;
                if (!childRows.HasValue)
                    return null;

                total = childRows.Value > long.MaxValue - total
                    ? long.MaxValue
                    : total + childRows.Value;
            }

            return total;
        }

        if (node.OperatorType is
            PhysicalOperatorType.ProjectionGate or
            PhysicalOperatorType.HashJoin or
            PhysicalOperatorType.AdaptiveHashJoin or
            PhysicalOperatorType.IndexNestedLoopJoin or
            PhysicalOperatorType.AdaptiveIndexNestedLoopJoin or
            PhysicalOperatorType.NumericRelationshipJoin or
            PhysicalOperatorType.NestedLoopJoin)
        {
            return null;
        }

        return node.Children.Count == 1 ||
               node.OperatorType is
                   PhysicalOperatorType.Query or
                   PhysicalOperatorType.Insert or
                   PhysicalOperatorType.Update or
                   PhysicalOperatorType.Delete
            ? node.Children[0].EstimatedRows
            : null;
    }

    private static double SaturatingAdd(double left, double right)
    {
        double result = left + right;
        return double.IsFinite(result) ? result : double.MaxValue;
    }

    private static void AddReachable(
        PhysicalPlanNode root,
        HashSet<PhysicalPlanNode> destination)
    {
        var pending = new Stack<PhysicalPlanNode>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            PhysicalPlanNode node = pending.Pop();
            if (!destination.Add(node))
                continue;

            for (int i = node.Children.Count - 1; i >= 0; i--)
                pending.Push(node.Children[i]);
        }
    }
}

internal class PhysicalProfilingOperator :
    IOperator,
    IRowBufferReuseController,
    IPreDecodeFilterSupport,
    IEstimatedRowCountProvider,
    IProjectionPushdownTarget,
    IEncodedPayloadSource,
    IMaterializedRowsProvider
{
    private readonly IOperator _source;
    private readonly bool _collectActuals;
    private readonly PhysicalPlanCaptureContext _context;
    private bool _disposed;

    internal PhysicalProfilingOperator(
        IOperator source,
        PhysicalPlanNode node,
        bool collectActuals,
        PhysicalPlanCaptureContext context)
    {
        _source = source;
        Node = node;
        _collectActuals = collectActuals;
        _context = context;
    }

    internal IOperator Source => _source;
    internal PhysicalPlanNode Node { get; }

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;
    int? IEstimatedRowCountProvider.EstimatedRowCount =>
        (_source as IEstimatedRowCountProvider)?.EstimatedRowCount;
    ReadOnlyMemory<byte> IEncodedPayloadSource.CurrentPayload =>
        (_source as IEncodedPayloadSource)?.CurrentPayload
        ?? throw new InvalidOperationException("The profiled operator has no encoded payload.");

    public ValueTask OpenAsync(CancellationToken ct = default)
        => ProfileOpenAsync(_source.OpenAsync, ct);

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        => ProfileMoveNextAsync(ct);

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }

    public void SetPreDecodeFilter(
        int columnIndex,
        CSharpDB.Sql.BinaryOp op,
        DbValue literal)
    {
        if (_source is not IPreDecodeFilterSupport support)
            throw new InvalidOperationException("The profiled operator does not support pre-decode filtering.");

        support.SetPreDecodeFilter(columnIndex, op, literal);
    }

    public void SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_source is not IPreDecodeFilterSupport support)
            throw new InvalidOperationException("The profiled operator does not support pre-decode filtering.");

        support.SetPreDecodeFilter(filter);
    }

    public bool TrySetOutputProjection(
        int[] columnIndices,
        ColumnDefinition[] outputSchema)
        => _source is IProjectionPushdownTarget target &&
           target.TrySetOutputProjection(columnIndices, outputSchema);

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_source is not IMaterializedRowsProvider provider ||
            !provider.TryTakeMaterializedRows(out rows))
        {
            rows = null!;
            return false;
        }

        if (_collectActuals)
        {
            Node.RecordRows(rows.Count);
            Node.MarkCompleted();
        }

        return true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        long started = Stopwatch.GetTimestamp();
        try
        {
            await _source.DisposeAsync();
        }
        finally
        {
            if (_collectActuals)
            {
                Node.RecordElapsed(Stopwatch.GetElapsedTime(started));
                if (Node.Status == PhysicalPlanNodeStatus.Running)
                    Node.MarkPartial("execution_stopped_early");
            }
        }
    }

    protected async ValueTask ProfileOpenAsync(
        Func<CancellationToken, ValueTask> open,
        CancellationToken ct)
    {
        if (_collectActuals)
            Node.RecordOpen();

        long started = Stopwatch.GetTimestamp();
        try
        {
            await open(ct);
            _context.RefreshTopology(_source, Node);
            PhysicalOperatorDescriptorFactory.Refresh(_source, Node);
        }
        catch (OperationCanceledException)
        {
            if (_collectActuals)
                Node.MarkCancelled();
            throw;
        }
        catch (CSharpDbException ex)
        {
            if (_collectActuals)
                Node.MarkError(ToDiagnosticCode(ex.Code));
            throw;
        }
        catch
        {
            if (_collectActuals)
                Node.MarkError("execution_error");
            throw;
        }
        finally
        {
            if (_collectActuals)
                Node.RecordElapsed(Stopwatch.GetElapsedTime(started));
        }
    }

    private async ValueTask<bool> ProfileMoveNextAsync(CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        try
        {
            bool hasRow = await _source.MoveNextAsync(ct);
            if (_collectActuals)
            {
                if (hasRow)
                    Node.RecordRows(1);
                else
                {
                    Node.MarkCompleted();
                    PhysicalOperatorDescriptorFactory.Refresh(_source, Node);
                }
            }

            return hasRow;
        }
        catch (OperationCanceledException)
        {
            if (_collectActuals)
                Node.MarkCancelled();
            throw;
        }
        catch (CSharpDbException ex)
        {
            if (_collectActuals)
                Node.MarkError(ToDiagnosticCode(ex.Code));
            throw;
        }
        catch
        {
            if (_collectActuals)
                Node.MarkError("execution_error");
            throw;
        }
        finally
        {
            if (_collectActuals)
                Node.RecordElapsed(Stopwatch.GetElapsedTime(started));
        }
    }

    protected async ValueTask<bool> ProfileMoveNextBatchAsync(
        IBatchOperator batchSource,
        CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        try
        {
            bool hasBatch = await batchSource.MoveNextBatchAsync(ct);
            if (_collectActuals)
            {
                if (hasBatch)
                    Node.RecordRows(batchSource.CurrentBatch.Count);
                else
                {
                    Node.MarkCompleted();
                    PhysicalOperatorDescriptorFactory.Refresh(_source, Node);
                }
            }

            return hasBatch;
        }
        catch (OperationCanceledException)
        {
            if (_collectActuals)
                Node.MarkCancelled();
            throw;
        }
        catch (CSharpDbException ex)
        {
            if (_collectActuals)
                Node.MarkError(ToDiagnosticCode(ex.Code));
            throw;
        }
        catch
        {
            if (_collectActuals)
                Node.MarkError("execution_error");
            throw;
        }
        finally
        {
            if (_collectActuals)
                Node.RecordElapsed(Stopwatch.GetElapsedTime(started));
        }
    }

    private static string ToDiagnosticCode(ErrorCode code)
        => code.ToString() switch
        {
            "Unknown" => "unknown",
            "SyntaxError" => "syntax_error",
            "TableNotFound" => "table_not_found",
            "ColumnNotFound" => "column_not_found",
            "TypeMismatch" => "type_mismatch",
            "ConstraintViolation" => "constraint_violation",
            "TransactionConflict" => "transaction_conflict",
            "ResourceLimitExceeded" => "resource_limit",
            _ => "execution_error",
        };
}

internal sealed class PhysicalProfilingBatchOperator :
    PhysicalProfilingOperator,
    IBatchOperator,
    IBatchBackedRowOperator,
    IBatchBufferReuseController
{
    private readonly IBatchOperator _batchSource;

    internal PhysicalProfilingBatchOperator(
        IOperator source,
        IBatchOperator batchSource,
        PhysicalPlanNode node,
        bool collectActuals,
        PhysicalPlanCaptureContext context)
        : base(source, node, collectActuals, context)
    {
        _batchSource = batchSource;
    }

    ColumnDefinition[] IBatchOperator.OutputSchema => OutputSchema;
    bool IBatchOperator.ReusesCurrentBatch => _batchSource.ReusesCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _batchSource.CurrentBatch;
    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    ValueTask IBatchOperator.OpenAsync(CancellationToken ct)
        => ProfileOpenAsync(_batchSource.OpenAsync, ct);

    ValueTask<bool> IBatchOperator.MoveNextBatchAsync(CancellationToken ct)
        => ProfileMoveNextBatchAsync(_batchSource, ct);

    public void SetReuseCurrentBatch(bool reuse)
    {
        if (_batchSource is IBatchBufferReuseController controller)
            controller.SetReuseCurrentBatch(reuse);
    }
}

internal sealed class PhysicalProfilingBatchBackedRowOperator :
    PhysicalProfilingOperator,
    IBatchBackedRowOperator
{
    private readonly ProfiledBatchSource _profiledBatchSource;

    internal PhysicalProfilingBatchBackedRowOperator(
        IOperator source,
        IBatchOperator batchSource,
        PhysicalPlanNode node,
        bool collectActuals,
        PhysicalPlanCaptureContext context)
        : base(source, node, collectActuals, context)
    {
        _profiledBatchSource = new ProfiledBatchSource(this, batchSource);
    }

    IBatchOperator IBatchBackedRowOperator.BatchSource => _profiledBatchSource;

    private ValueTask OpenBatchAsync(
        IBatchOperator batchSource,
        CancellationToken ct)
        => ProfileOpenAsync(batchSource.OpenAsync, ct);

    private ValueTask<bool> MoveNextBatchAsync(
        IBatchOperator batchSource,
        CancellationToken ct)
        => ProfileMoveNextBatchAsync(batchSource, ct);

    private sealed class ProfiledBatchSource(
        PhysicalProfilingBatchBackedRowOperator owner,
        IBatchOperator source) :
        IBatchOperator,
        IBatchBufferReuseController
    {
        public ColumnDefinition[] OutputSchema => source.OutputSchema;
        public bool ReusesCurrentBatch => source.ReusesCurrentBatch;
        public RowBatch CurrentBatch => source.CurrentBatch;

        public ValueTask OpenAsync(CancellationToken ct = default)
            => owner.OpenBatchAsync(source, ct);

        public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
            => owner.MoveNextBatchAsync(source, ct);

        public ValueTask DisposeAsync() => owner.DisposeAsync();

        public void SetReuseCurrentBatch(bool reuse)
        {
            if (source is IBatchBufferReuseController controller)
                controller.SetReuseCurrentBatch(reuse);
        }
    }
}

internal static class PhysicalOperatorDescriptorFactory
{
    internal static PhysicalPlanNode Create(IOperator source)
    {
        PhysicalOperatorMetadata supplied =
            source is IPhysicalOperatorMetadataProvider provider
                ? provider.GetPhysicalOperatorMetadata()
                : default;
        (PhysicalOperatorType operatorType, PhysicalAccessPath defaultAccessPath, long? defaultRows) =
            Classify(source);
        long? estimatedRows = supplied.EstimatedRows ?? defaultRows;

        return new PhysicalPlanNode
        {
            OperatorType = operatorType,
            EstimatedRows = estimatedRows,
            EstimatedCost = supplied.EstimatedCost,
            AccessPath = supplied.AccessPath != PhysicalAccessPath.None
                ? supplied.AccessPath
                : defaultAccessPath,
            ObjectName = supplied.ObjectName,
            IndexName = supplied.IndexName,
            JoinType = supplied.JoinType,
            Predicate = supplied.Predicate,
        };
    }

    internal static void Refresh(IOperator source, PhysicalPlanNode node)
    {
        if (source is not IPhysicalOperatorMetadataProvider provider)
            return;

        PhysicalOperatorMetadata metadata =
            provider.GetPhysicalOperatorMetadata();
        if (source is
            AdaptiveIndexNestedLoopJoinOperator or
            ProjectionGatedNumericRelationshipJoinOperator)
        {
            // These descriptors can choose a different access implementation
            // during Open. Replace selected-path fields so a hash fallback does
            // not retain stale index metadata from the planned alternative.
            node.AccessPath = metadata.AccessPath;
            node.ObjectName = metadata.ObjectName;
            node.IndexName = metadata.IndexName;
            if (!string.IsNullOrEmpty(metadata.JoinType))
                node.JoinType = metadata.JoinType;
            if (!string.IsNullOrEmpty(metadata.Predicate))
                node.Predicate = metadata.Predicate;
        }
    }

    private static (
        PhysicalOperatorType OperatorType,
        PhysicalAccessPath AccessPath,
        long? EstimatedRows) Classify(IOperator source)
        => source switch
        {
            BatchToRowOperatorAdapter =>
                (PhysicalOperatorType.BatchToRow, PhysicalAccessPath.None, null),
            ProjectionGatedNumericRelationshipJoinOperator =>
                (PhysicalOperatorType.ProjectionGate, PhysicalAccessPath.None, null),
            FilterProjectionOperator =>
                (PhysicalOperatorType.FilterProjection, PhysicalAccessPath.None, null),
            ProjectionOperator or CompactPayloadProjectionOperator =>
                (PhysicalOperatorType.Projection, PhysicalAccessPath.None, null),
            FilterOperator =>
                (PhysicalOperatorType.Filter, PhysicalAccessPath.None, null),
            CompactTableScanProjectionOperator =>
                (PhysicalOperatorType.CompactTableScan, PhysicalAccessPath.TableScan, null),
            TableScanOperator =>
                (PhysicalOperatorType.TableScan, PhysicalAccessPath.TableScan, null),
            PrimaryKeyLookupOperator or PrimaryKeyProjectionLookupOperator =>
                (PhysicalOperatorType.PrimaryKeyLookup, PhysicalAccessPath.PrimaryKey, 1),
            UniqueIndexLookupOperator or UniqueIndexProjectionLookupOperator =>
                (PhysicalOperatorType.IndexLookup, PhysicalAccessPath.UniqueIndex, 1),
            IndexOrderedScanOperator or IndexOrderedProjectionScanOperator =>
                (PhysicalOperatorType.IndexOrderedScan, PhysicalAccessPath.OrderedIndex, null),
            IndexScanOperator =>
                (PhysicalOperatorType.IndexLookup, PhysicalAccessPath.Index, null),
            IndexScanProjectionOperator or HashedIndexProjectionLookupOperator =>
                (PhysicalOperatorType.IndexProjection, PhysicalAccessPath.Index, null),
            ExternalTablePrimaryKeyLookupOperator =>
                (PhysicalOperatorType.PrimaryKeyLookup, PhysicalAccessPath.ExternalTable, 1),
            ExternalTableScanOperator =>
                (PhysicalOperatorType.ExternalTableScan, PhysicalAccessPath.ExternalTable, null),
            BufferedReplayOperator =>
                (PhysicalOperatorType.BufferedReplay, PhysicalAccessPath.Materialized, null),
            MaterializedOperator =>
                (PhysicalOperatorType.MaterializedScan, PhysicalAccessPath.Materialized, null),
            HashAggregateOperator =>
                (PhysicalOperatorType.HashAggregate, PhysicalAccessPath.None, null),
            ScalarAggregateOperator or ScalarAggregateLookupOperator or
                ScalarAggregateTableOperator or FilteredScalarAggregateTableOperator or
                FilteredScalarAggregatePayloadOperator or CountStarTableOperator =>
                (PhysicalOperatorType.ScalarAggregate, PhysicalAccessPath.None, 1),
            IndexKeyAggregateOperator or IndexGroupedAggregateOperator or
                CompositeIndexGroupedAggregateOperator or TableKeyAggregateOperator =>
                (PhysicalOperatorType.IndexAggregate, PhysicalAccessPath.Index, null),
            WindowOperator =>
                (PhysicalOperatorType.Window, PhysicalAccessPath.None, null),
            TopNSortOperator =>
                (PhysicalOperatorType.TopNSort, PhysicalAccessPath.None, null),
            SortOperator =>
                (PhysicalOperatorType.Sort, PhysicalAccessPath.None, null),
            DistinctOperator =>
                (PhysicalOperatorType.Distinct, PhysicalAccessPath.None, null),
            LimitOperator =>
                (PhysicalOperatorType.Limit, PhysicalAccessPath.None, null),
            OffsetOperator =>
                (PhysicalOperatorType.Offset, PhysicalAccessPath.None, null),
            AdaptiveHashJoinOperator =>
                (PhysicalOperatorType.AdaptiveHashJoin, PhysicalAccessPath.None, null),
            HashJoinOperator =>
                (PhysicalOperatorType.HashJoin, PhysicalAccessPath.None, null),
            AdaptiveIndexNestedLoopJoinOperator =>
                (PhysicalOperatorType.AdaptiveIndexNestedLoopJoin, PhysicalAccessPath.Index, null),
            NumericRelationshipIndexJoinOperator =>
                (PhysicalOperatorType.NumericRelationshipJoin, PhysicalAccessPath.Index, null),
            IndexNestedLoopJoinOperator or HashedIndexNestedLoopJoinOperator or
                ExternalIndexNestedLoopJoinOperator =>
                (PhysicalOperatorType.IndexNestedLoopJoin, PhysicalAccessPath.Index, null),
            NestedLoopJoinOperator =>
                (PhysicalOperatorType.NestedLoopJoin, PhysicalAccessPath.None, null),
            ConcatenateOperator =>
                (PhysicalOperatorType.Concatenate, PhysicalAccessPath.None, null),
            IMaterializedRowsProvider =>
                (PhysicalOperatorType.MaterializedScan, PhysicalAccessPath.Materialized, null),
            _ => (PhysicalOperatorType.Unknown, PhysicalAccessPath.None, null),
        };
}
