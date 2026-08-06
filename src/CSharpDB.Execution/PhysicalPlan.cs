using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal enum PhysicalOperatorType
{
    Unknown,
    Query,
    Projection,
    Filter,
    FilterProjection,
    TableScan,
    CompactTableScan,
    PrimaryKeyLookup,
    IndexLookup,
    IndexOrderedScan,
    IndexProjection,
    SystemCatalogScan,
    ConstantScan,
    ExternalTableScan,
    TemporaryTableScan,
    MaterializedScan,
    BufferedReplay,
    BatchToRow,
    ProjectionGate,
    HashJoin,
    AdaptiveHashJoin,
    IndexNestedLoopJoin,
    AdaptiveIndexNestedLoopJoin,
    NumericRelationshipJoin,
    NestedLoopJoin,
    ScalarAggregate,
    HashAggregate,
    IndexAggregate,
    Window,
    Sort,
    TopNSort,
    Distinct,
    Limit,
    Offset,
    Concatenate,
    Insert,
    Update,
    Delete,
    Diagnostic,
}

internal enum PhysicalAccessPath
{
    None,
    TableScan,
    PrimaryKey,
    UniqueIndex,
    Index,
    OrderedIndex,
    SystemCatalog,
    Constant,
    ExternalTable,
    TemporaryTable,
    Materialized,
}

internal enum PhysicalPlanNodeStatus
{
    Planned,
    Running,
    Completed,
    Partial,
    Cancelled,
    Error,
}

internal readonly record struct PhysicalOperatorMetadata(
    long? EstimatedRows = null,
    double? EstimatedCost = null,
    PhysicalAccessPath AccessPath = PhysicalAccessPath.None,
    string? ObjectName = null,
    string? IndexName = null,
    string? JoinType = null,
    string? Predicate = null);

/// <summary>
/// Optional operator metadata used by the physical-plan capture layer. Predicate
/// text supplied here must describe shape only and must never contain literal or
/// parameter values.
/// </summary>
internal interface IPhysicalOperatorMetadataProvider
{
    PhysicalOperatorMetadata GetPhysicalOperatorMetadata();
}

/// <summary>
/// Exposes the finalized physical children of an operator without making the
/// public execution interface depend on plan formatting.
/// </summary>
internal interface IPhysicalOperatorChildren
{
    IReadOnlyList<IOperator> PhysicalChildren { get; }
}

internal sealed class PhysicalPlanNode
{
    private readonly List<PhysicalPlanNode> _children = [];
    private long _actualRows;
    private long _actualLoops;
    private long _elapsedTicks;
    private int _hasActualRows;
    private int _hasActualLoops;
    private int _hasElapsed;
    private int _status = (int)PhysicalPlanNodeStatus.Planned;

    internal required PhysicalOperatorType OperatorType { get; set; }
    internal long? EstimatedRows { get; set; }
    internal double? EstimatedCost { get; set; }
    internal PhysicalAccessPath AccessPath { get; set; }
    internal string? ObjectName { get; set; }
    internal string? IndexName { get; set; }
    internal string? JoinType { get; set; }
    internal string? Predicate { get; set; }
    internal string? DiagnosticCode { get; private set; }
    internal IReadOnlyList<PhysicalPlanNode> Children => _children;
    internal long? ActualRows => Volatile.Read(ref _hasActualRows) != 0
        ? Interlocked.Read(ref _actualRows)
        : null;
    internal long? ActualLoops => Volatile.Read(ref _hasActualLoops) != 0
        ? Interlocked.Read(ref _actualLoops)
        : null;
    internal TimeSpan? Elapsed => Volatile.Read(ref _hasElapsed) != 0
        ? TimeSpan.FromTicks(Interlocked.Read(ref _elapsedTicks))
        : null;
    internal PhysicalPlanNodeStatus Status =>
        (PhysicalPlanNodeStatus)Volatile.Read(ref _status);

    internal void AddChild(PhysicalPlanNode child)
    {
        ArgumentNullException.ThrowIfNull(child);
        if (!ReferenceEquals(this, child) && !_children.Contains(child))
            _children.Add(child);
    }

    internal void ReplaceChildren(IEnumerable<PhysicalPlanNode> children)
    {
        ArgumentNullException.ThrowIfNull(children);
        _children.Clear();
        foreach (PhysicalPlanNode child in children)
            AddChild(child);
    }

    internal void RecordOpen()
    {
        Interlocked.Increment(ref _actualLoops);
        Volatile.Write(ref _hasActualLoops, 1);
        TryTransition(PhysicalPlanNodeStatus.Planned, PhysicalPlanNodeStatus.Running);
    }

    internal void RecordRows(long rows)
    {
        if (rows <= 0)
            return;

        Interlocked.Add(ref _actualRows, rows);
        Volatile.Write(ref _hasActualRows, 1);
    }

    internal void RecordElapsed(TimeSpan elapsed)
    {
        Interlocked.Add(ref _elapsedTicks, Math.Max(0, elapsed.Ticks));
        Volatile.Write(ref _hasElapsed, 1);
    }

    internal void MarkCompleted()
    {
        EnsureActualRows();
        Volatile.Write(ref _status, (int)PhysicalPlanNodeStatus.Completed);
    }

    internal void MarkPartial(string? diagnosticCode = null)
    {
        SetDiagnostic(diagnosticCode);
        if (Status is not (PhysicalPlanNodeStatus.Error or PhysicalPlanNodeStatus.Cancelled))
            Volatile.Write(ref _status, (int)PhysicalPlanNodeStatus.Partial);
    }

    internal void MarkCancelled()
    {
        SetDiagnostic("cancelled");
        Volatile.Write(ref _status, (int)PhysicalPlanNodeStatus.Cancelled);
    }

    internal void MarkError(string? diagnosticCode)
    {
        SetDiagnostic(diagnosticCode ?? "execution_error");
        Volatile.Write(ref _status, (int)PhysicalPlanNodeStatus.Error);
    }

    internal void SetActuals(long rows, long loops, TimeSpan elapsed)
    {
        Interlocked.Exchange(ref _actualRows, Math.Max(0, rows));
        Interlocked.Exchange(ref _actualLoops, Math.Max(0, loops));
        Interlocked.Exchange(ref _elapsedTicks, Math.Max(0, elapsed.Ticks));
        Volatile.Write(ref _hasActualRows, 1);
        Volatile.Write(ref _hasActualLoops, 1);
        Volatile.Write(ref _hasElapsed, 1);
    }

    internal void ApplyMetadata(
        PhysicalOperatorMetadata metadata,
        bool preserveExisting = true)
    {
        if (metadata.EstimatedRows.HasValue || !preserveExisting)
            EstimatedRows = metadata.EstimatedRows;
        if (metadata.EstimatedCost.HasValue || !preserveExisting)
            EstimatedCost = metadata.EstimatedCost;
        if (metadata.AccessPath != PhysicalAccessPath.None || !preserveExisting)
            AccessPath = metadata.AccessPath;
        if (!string.IsNullOrEmpty(metadata.ObjectName) || !preserveExisting)
            ObjectName = metadata.ObjectName;
        if (!string.IsNullOrEmpty(metadata.IndexName) || !preserveExisting)
            IndexName = metadata.IndexName;
        if (!string.IsNullOrEmpty(metadata.JoinType) || !preserveExisting)
            JoinType = metadata.JoinType;
        if (!string.IsNullOrEmpty(metadata.Predicate) || !preserveExisting)
            Predicate = metadata.Predicate;
    }

    private void EnsureActualRows()
    {
        if (Volatile.Read(ref _hasActualRows) == 0)
        {
            Interlocked.Exchange(ref _actualRows, 0);
            Volatile.Write(ref _hasActualRows, 1);
        }
    }

    private void SetDiagnostic(string? diagnosticCode)
    {
        if (!string.IsNullOrWhiteSpace(diagnosticCode))
            DiagnosticCode = diagnosticCode;
    }

    private void TryTransition(
        PhysicalPlanNodeStatus expected,
        PhysicalPlanNodeStatus next)
    {
        Interlocked.CompareExchange(
            ref _status,
            (int)next,
            (int)expected);
    }
}

internal sealed class PhysicalPlan
{
    internal PhysicalPlan(PhysicalPlanNode root, bool analyzesTarget)
    {
        Root = root ?? throw new ArgumentNullException(nameof(root));
        AnalyzesTarget = analyzesTarget;
    }

    internal PhysicalPlanNode Root { get; }
    internal bool AnalyzesTarget { get; }
}

internal static class PhysicalPlanResultFormatter
{
    internal const int MaxNodeCount = 500;
    internal const int MaxTextLength = 512;
    internal const int MaxSerializedBytes = 256 * 1024;
    private const string TruncatedDiagnosticCode = "plan_truncated";

    internal static readonly ColumnDefinition[] Columns =
    [
        new ColumnDefinition { Name = "node_id", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "parent_node_id", Type = DbType.Integer, Nullable = true },
        new ColumnDefinition { Name = "operator_type", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "estimated_rows", Type = DbType.Integer, Nullable = true },
        new ColumnDefinition { Name = "estimated_cost", Type = DbType.Real, Nullable = true },
        new ColumnDefinition { Name = "actual_rows", Type = DbType.Integer, Nullable = true },
        new ColumnDefinition { Name = "actual_loops", Type = DbType.Integer, Nullable = true },
        new ColumnDefinition { Name = "elapsed_microseconds", Type = DbType.Integer, Nullable = true },
        new ColumnDefinition { Name = "access_path", Type = DbType.Text, Nullable = true },
        new ColumnDefinition { Name = "object_name", Type = DbType.Text, Nullable = true },
        new ColumnDefinition { Name = "index_name", Type = DbType.Text, Nullable = true },
        new ColumnDefinition { Name = "join_type", Type = DbType.Text, Nullable = true },
        new ColumnDefinition { Name = "predicate", Type = DbType.Text, Nullable = true },
        new ColumnDefinition { Name = "status", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "diagnostic_code", Type = DbType.Text, Nullable = true },
    ];

    internal static QueryResult Format(PhysicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        List<FlattenedNode> flattened = FlattenBounded(plan.Root, out bool truncated);
        var rows = new List<DbValue[]>(Math.Min(flattened.Count + 1, MaxNodeCount));
        int serializedBytes = 0;

        for (int i = 0; i < flattened.Count; i++)
        {
            if (HasOversizedText(flattened[i].Node))
                truncated = true;

            DbValue[] row = CreateRow(flattened[i]);
            int rowBytes = EstimateSerializedBytes(row);
            if (serializedBytes + rowBytes > MaxSerializedBytes)
            {
                truncated = true;
                break;
            }

            rows.Add(row);
            serializedBytes += rowBytes;
        }

        if (rows.Count < flattened.Count)
            truncated = true;

        if (truncated)
        {
            if (rows.Count >= MaxNodeCount)
                rows.RemoveAt(rows.Count - 1);

            rows.Add(CreateDiagnosticRow(rows.Count + 1, TruncatedDiagnosticCode));
        }

        return QueryResult.FromMaterializedRows(Columns, rows);
    }

    private static List<FlattenedNode> FlattenBounded(
        PhysicalPlanNode root,
        out bool truncated)
    {
        var result = new List<FlattenedNode>(Math.Min(MaxNodeCount, 64));
        var pending = new Stack<(PhysicalPlanNode Node, int? ParentNodeId)>();
        var visited = new HashSet<PhysicalPlanNode>(ReferenceEqualityComparer.Instance);
        pending.Push((root, null));
        truncated = false;

        // Reserve one row for an explicit truncation diagnostic.
        while (pending.Count > 0 && result.Count < MaxNodeCount - 1)
        {
            (PhysicalPlanNode node, int? parentNodeId) = pending.Pop();
            if (!visited.Add(node))
            {
                truncated = true;
                continue;
            }

            int nodeId = result.Count + 1;
            result.Add(new FlattenedNode(node, nodeId, parentNodeId));

            for (int i = node.Children.Count - 1; i >= 0; i--)
                pending.Push((node.Children[i], nodeId));
        }

        if (pending.Count > 0)
            truncated = true;

        return result;
    }

    private static DbValue[] CreateRow(FlattenedNode flattened)
    {
        PhysicalPlanNode node = flattened.Node;
        return
        [
            DbValue.FromInteger(flattened.NodeId),
            flattened.ParentNodeId.HasValue
                ? DbValue.FromInteger(flattened.ParentNodeId.Value)
                : DbValue.Null,
            DbValue.FromText(ToStableOperatorName(node.OperatorType)),
            node.EstimatedRows.HasValue
                ? DbValue.FromInteger(node.EstimatedRows.Value)
                : DbValue.Null,
            node.EstimatedCost.HasValue
                ? DbValue.FromReal(node.EstimatedCost.Value)
                : DbValue.Null,
            node.ActualRows.HasValue
                ? DbValue.FromInteger(node.ActualRows.Value)
                : DbValue.Null,
            node.ActualLoops.HasValue
                ? DbValue.FromInteger(node.ActualLoops.Value)
                : DbValue.Null,
            node.Elapsed.HasValue
                ? DbValue.FromInteger(ToMicroseconds(node.Elapsed.Value))
                : DbValue.Null,
            node.AccessPath != PhysicalAccessPath.None
                ? DbValue.FromText(ToStableName(node.AccessPath))
                : DbValue.Null,
            TextOrNull(node.ObjectName),
            TextOrNull(node.IndexName),
            TextOrNull(node.JoinType),
            TextOrNull(node.Predicate),
            DbValue.FromText(ToStableName(node.Status)),
            TextOrNull(node.DiagnosticCode),
        ];
    }

    private static DbValue[] CreateDiagnosticRow(int nodeId, string diagnosticCode)
        =>
        [
            DbValue.FromInteger(nodeId),
            DbValue.Null,
            DbValue.FromText("diagnostic"),
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.FromText("partial"),
            DbValue.FromText(diagnosticCode),
        ];

    private static DbValue TextOrNull(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return DbValue.Null;

        string bounded = value.Length <= MaxTextLength
            ? value
            : value[..MaxTextLength];
        return DbValue.FromText(bounded);
    }

    private static bool HasOversizedText(PhysicalPlanNode node)
        => IsOversized(node.ObjectName) ||
           IsOversized(node.IndexName) ||
           IsOversized(node.JoinType) ||
           IsOversized(node.Predicate) ||
           IsOversized(node.DiagnosticCode);

    private static bool IsOversized(string? value)
        => value is { Length: > MaxTextLength };

    private static int EstimateSerializedBytes(DbValue[] row)
    {
        int bytes = 0;
        for (int i = 0; i < row.Length; i++)
        {
            bytes += row[i].Type switch
            {
                DbType.Text => Encoding.UTF8.GetByteCount(row[i].AsText),
                DbType.Blob => row[i].AsBlob.Length,
                DbType.Integer or DbType.Real => sizeof(long),
                DbType.Decimal => sizeof(long) + sizeof(byte),
                _ => 1,
            };
        }

        return bytes;
    }

    private static long ToMicroseconds(TimeSpan elapsed)
        => Math.Max(0, elapsed.Ticks / (TimeSpan.TicksPerMillisecond / 1000));

    internal static string ToStableOperatorName(PhysicalOperatorType value)
        => value switch
        {
            PhysicalOperatorType.Unknown => "unknown",
            PhysicalOperatorType.Query => "query",
            PhysicalOperatorType.Projection => "projection",
            PhysicalOperatorType.Filter => "filter",
            PhysicalOperatorType.FilterProjection => "filter_projection",
            PhysicalOperatorType.TableScan => "table_scan",
            PhysicalOperatorType.CompactTableScan => "compact_table_scan",
            PhysicalOperatorType.PrimaryKeyLookup => "primary_key_lookup",
            PhysicalOperatorType.IndexLookup => "index_lookup",
            PhysicalOperatorType.IndexOrderedScan => "index_ordered_scan",
            PhysicalOperatorType.IndexProjection => "index_projection",
            PhysicalOperatorType.SystemCatalogScan => "system_catalog_scan",
            PhysicalOperatorType.ConstantScan => "constant_scan",
            PhysicalOperatorType.ExternalTableScan => "external_table_scan",
            PhysicalOperatorType.TemporaryTableScan => "temporary_table_scan",
            PhysicalOperatorType.MaterializedScan => "materialized_scan",
            PhysicalOperatorType.BufferedReplay => "buffered_replay",
            PhysicalOperatorType.BatchToRow => "batch_to_row",
            PhysicalOperatorType.ProjectionGate => "projection_gate",
            PhysicalOperatorType.HashJoin => "hash_join",
            PhysicalOperatorType.AdaptiveHashJoin => "adaptive_hash_join",
            PhysicalOperatorType.IndexNestedLoopJoin => "index_nested_loop_join",
            PhysicalOperatorType.AdaptiveIndexNestedLoopJoin => "adaptive_index_nested_loop_join",
            PhysicalOperatorType.NumericRelationshipJoin => "numeric_relationship_join",
            PhysicalOperatorType.NestedLoopJoin => "nested_loop_join",
            PhysicalOperatorType.ScalarAggregate => "scalar_aggregate",
            PhysicalOperatorType.HashAggregate => "hash_aggregate",
            PhysicalOperatorType.IndexAggregate => "index_aggregate",
            PhysicalOperatorType.Window => "window",
            PhysicalOperatorType.Sort => "sort",
            PhysicalOperatorType.TopNSort => "top_n_sort",
            PhysicalOperatorType.Distinct => "distinct",
            PhysicalOperatorType.Limit => "limit",
            PhysicalOperatorType.Offset => "offset",
            PhysicalOperatorType.Concatenate => "concatenate",
            PhysicalOperatorType.Insert => "insert",
            PhysicalOperatorType.Update => "update",
            PhysicalOperatorType.Delete => "delete",
            PhysicalOperatorType.Diagnostic => "diagnostic",
            _ => "unknown",
        };

    private static string ToStableName(PhysicalAccessPath value)
        => value switch
        {
            PhysicalAccessPath.None => "none",
            PhysicalAccessPath.TableScan => "table_scan",
            PhysicalAccessPath.PrimaryKey => "primary_key",
            PhysicalAccessPath.UniqueIndex => "unique_index",
            PhysicalAccessPath.Index => "index",
            PhysicalAccessPath.OrderedIndex => "ordered_index",
            PhysicalAccessPath.SystemCatalog => "system_catalog",
            PhysicalAccessPath.Constant => "constant",
            PhysicalAccessPath.ExternalTable => "external_table",
            PhysicalAccessPath.TemporaryTable => "temporary_table",
            PhysicalAccessPath.Materialized => "materialized",
            _ => "none",
        };

    private static string ToStableName(PhysicalPlanNodeStatus value)
        => value switch
        {
            PhysicalPlanNodeStatus.Planned => "planned",
            PhysicalPlanNodeStatus.Running => "running",
            PhysicalPlanNodeStatus.Completed => "completed",
            PhysicalPlanNodeStatus.Partial => "partial",
            PhysicalPlanNodeStatus.Cancelled => "cancelled",
            PhysicalPlanNodeStatus.Error => "error",
            _ => "error",
        };

    private readonly record struct FlattenedNode(
        PhysicalPlanNode Node,
        int NodeId,
        int? ParentNodeId);
}
