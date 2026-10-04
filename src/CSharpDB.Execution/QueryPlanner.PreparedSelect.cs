using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Execution;

public sealed partial class QueryPlanner
{
    private const int MaxPreparedSelectPlans = 256;
    // ReaderSession currently shares the writer catalog, whose roots can outlive its snapshot.
    // Keep its existing range path until snapshot catalog resolution is qualified separately.
    internal bool EnablePreparedPrimaryKeyRanges { get; init; } = true;
    private readonly Dictionary<PreparedSelectTemplate, PreparedIndexPlan?> _preparedSelectPlans = new();
    private readonly Queue<PreparedSelectTemplate> _preparedSelectPlanOrder = new();

    private QueryResult ExecutePreparedSelect(PreparedSelectStatement execution)
    {
        PreparedSelectTemplate template = execution.Template;
        // Temporary tables may shadow a persistent table without changing its catalog schema version.
        if (_cteData is not null || HasTemporaryTables || PhysicalPlanCapture.IsActive)
            return ExecuteSelect(execution.BindLiterals());

        bool hit = _preparedSelectPlans.TryGetValue(template, out PreparedIndexPlan? plan);
        if (!hit)
        {
            plan = TryPrepareIndexPlan(template);
            _preparedSelectPlans.Add(template, plan);
            _preparedSelectPlanOrder.Enqueue(template);
            if (_preparedSelectPlans.Count > MaxPreparedSelectPlans)
                _preparedSelectPlans.Remove(_preparedSelectPlanOrder.Dequeue());
        }
        if (plan is null) return ExecuteSelect(execution.BindLiterals());

        SelectStatement statement = template.Statement;
        IndexScanRange range = execution.GetRange();
        bool empty = range.LowerBound.HasValue && range.UpperBound.HasValue &&
            (range.LowerBound > range.UpperBound || range.LowerBound == range.UpperBound &&
                (!range.LowerInclusive || !range.UpperInclusive));
        IOperator op;
        bool equality = range.LowerBound.HasValue && range.LowerBound == range.UpperBound;
        // ANALYZE can make an equality index lookup unattractive for a frequent value.
        // Retain the existing value-sensitive choice instead of freezing it into this plan.
        if (!empty && equality && plan.Index is { IsUnique: false } &&
            TryEstimateLookupRowCount(plan.Schema.TableName, plan.Schema.Columns[plan.KeyColumnIndex].Name,
                DbValue.FromInteger(range.LowerBound!.Value), out long estimatedRows, out long tableRows) &&
            !ShouldPreferNonUniqueLookup(tableRows, estimatedRows, requiresResidualPredicate: false))
            return ExecuteSelect(execution.BindLiterals());

        if (hit) _selectPlanCacheHitCount++;
        else { _selectPlanCacheMissCount++; _selectPlanCacheStoreCount++; }
        ObservePlanCacheLookup(hit);
        if (!empty && equality && plan.Index is null)
            return ObserveSelectedPlan(ExecutePreparedPrimaryKey(statement, plan, range.LowerBound!.Value),
                SelectPlanKind.FastPrimaryKeyLookup, QueryPlanAccessPathCategory.PrimaryKeyLookup);
        if (empty)
        {
            op = new MaterializedOperator([], plan.OutputColumns);
        }
        else if (plan.Index is null)
        {
            op = new PrimaryKeyRangeScanOperator(_catalog.GetTableTree(plan.Schema.TableName, _pager),
                plan.Schema, range, GetReadSerializer(plan.Schema), plan.Projection, plan.OutputColumns);
            op = ApplyOffsetAndLimit(op, statement.Offset, statement.Limit);
        }
        else
        {
            IOperator source = equality
                ? BuildLookupOperator(plan.Schema.TableName, plan.Schema, false, plan.Index, range.LowerBound!.Value)
                : new IndexOrderedScanOperator(_catalog.GetIndexStore(plan.Index!.IndexName, _pager),
                    _catalog.GetTableTree(plan.Schema.TableName, _pager), plan.Schema, plan.KeyColumnIndex, range,
                    recordSerializer: GetReadSerializer(plan.Schema));
            if (equality && CanUseSyncPointLookups && CanUseSyncIndexedLookupShortcut(statement) &&
                TryMaterializeSyncIndexedLookupResult(source, plan.Schema, GetReadSerializer(plan.Schema),
                    plan.OutputColumns, plan.Projection, out var syncResult))
                return ObserveSelectedPlan(syncResult, SelectPlanKind.FastIndexedLookup, QueryPlanAccessPathCategory.IndexSeek);
            // Each call creates new operators with copied bounds; cached plans contain no cursor or values.
            if (plan.Projection is null) op = source;
            else if (source is IndexOrderedScanOperator ordered &&
                TryBuildCoveredOrderedIndexProjectionOperator(ordered, plan.Schema, plan.Projection, plan.OutputColumns, out var covered))
                op = covered;
            else op = new ProjectionOperator(source, plan.Projection, plan.OutputColumns, plan.Schema);
            op = ApplyOffsetAndLimit(op, statement.Offset, statement.Limit);
        }
        // Tiny windows do not amortize a batch buffer; offsets may still require consuming many rows.
        QueryResult result = statement.Limit is >= 0 and <= LimitOperator.SmallLimitRowModeThreshold &&
            statement.Offset.GetValueOrDefault() == 0
            ? new QueryResult(op) : CreateQueryResult(op);
        return ObserveSelectedPlan(result,
            plan.Index is null ? (equality ? SelectPlanKind.FastPrimaryKeyLookup : SelectPlanKind.General) : SelectPlanKind.FastIndexedLookup,
            plan.Index is null ? (equality ? QueryPlanAccessPathCategory.PrimaryKeyLookup : QueryPlanAccessPathCategory.IndexScan) :
                equality ? QueryPlanAccessPathCategory.IndexSeek : QueryPlanAccessPathCategory.IndexScan);
    }

    private QueryResult ExecutePreparedPrimaryKey(SelectStatement statement, PreparedIndexPlan plan, long key)
    {
        var tableTree = _catalog.GetTableTree(plan.Schema.TableName, _pager);
        // Preserve the ordinary PK path's cache-only star and decode-free key projections.
        if (CanUseSyncPointLookups && !statement.Offset.HasValue && !statement.Limit.HasValue &&
            (plan.Projection is null || plan.PrimaryKeyOnlyProjection) &&
            tableTree.TryFindCachedMemory(key, out var payload))
        {
            DbValue[]? row = null;
            if (payload is { } memory)
            {
                if (plan.Projection is null)
                    row = DecodeRowWithNullPadding(GetReadSerializer(plan.Schema), memory.Span, plan.OutputColumns.Length);
                else
                {
                    row = new DbValue[plan.OutputColumns.Length];
                    Array.Fill(row, DbValue.FromInteger(key));
                }
            }
            return QueryResult.FromSyncLookup(row, plan.OutputColumns);
        }

        IOperator op;
        if (plan.PrimaryKeyOnlyProjection)
            op = new PrimaryKeyProjectionLookupOperator(tableTree, key, plan.OutputColumns);
        else
        {
            var lookup = new PrimaryKeyLookupOperator(tableTree, plan.Schema, key, GetReadSerializer(plan.Schema));
            if (plan.Projection is null) op = lookup;
            else
            {
                lookup.SetDecodedColumnUpperBound(plan.MaxProjectedColumn);
                op = new ProjectionOperator(lookup, plan.Projection, plan.OutputColumns, plan.Schema);
            }
        }
        // A physical PK equality yields at most one row; avoid allocating a projection batch.
        return new QueryResult(ApplyOffsetAndLimit(op, statement.Offset, statement.Limit));
    }

    private PreparedIndexPlan? TryPrepareIndexPlan(PreparedSelectTemplate template)
    {
        var statement = template.Statement;
        var table = (SimpleTableRef)statement.From;
        if (_catalog.IsView(table.TableName) || IsSystemCatalogTable(table.TableName) || IsExternalTable(table.TableName)) return null;
        var baseSchema = _catalog.GetTable(table.TableName);
        if (baseSchema is null) return null;
        var schema = CreateSimpleTableQuerySchema(baseSchema, table.Alias);
        int Resolve(ColumnRefExpression column) => column.TableAlias is null
            ? schema.GetColumnIndex(column.ColumnName) : schema.GetQualifiedColumnIndex(column.TableAlias, column.ColumnName);
        int key = Resolve(template.KeyColumn);
        if (key < 0 || key >= schema.Columns.Count || schema.Columns[key].Type != DbType.Integer ||
            schema.Columns[key].EffectiveType.Kind == SqlTypeKind.Boolean) return null;
        if (statement.OrderBy is { Count: 1 } && Resolve((ColumnRefExpression)statement.OrderBy[0].Expression) != key) return null;
        bool primaryKey = (template.HasEquality || EnablePreparedPrimaryKeyRanges) && schema.PrimaryKeyColumnIndex == key &&
            ResolvePrimaryKeyDefinition(baseSchema) is { } keyDefinition &&
            UsesPhysicalIntegerPrimaryKey(baseSchema, keyDefinition);
        IndexSchema? selected = null;
        if (!primaryKey)
        {
            foreach (var index in _catalog.GetSqlIndexesForTable(table.TableName))
            {
                if (!UsesDirectIntegerIndexKey(index, schema) || schema.GetColumnIndex(index.Columns[0]) != key) continue;
                if (selected is null || index.IsUnique) selected = index;
                if (index.IsUnique) break;
            }
            if (selected is null) return null;
        }
        int[]? projection = null;
        ColumnDefinition[] output;
        if (statement.Columns.Count == 1 && statement.Columns[0].IsStar) output = GetSchemaColumnsArray(schema);
        else if (!TryBuildColumnProjection(statement.Columns, schema, out projection, out output)) return null;
        return new PreparedIndexPlan(schema, selected, key, projection, output,
            primaryKey && projection is not null && IsPrimaryKeyOnlyProjection(projection, key),
            projection is { Length: > 0 } ? projection.Max() : -1);
    }

    private void ClearPreparedSelectPlans()
    {
        _preparedSelectPlans.Clear();
        _preparedSelectPlanOrder.Clear();
    }

    // A null secondary index denotes access through the physical integer primary key.
    private sealed record PreparedIndexPlan(TableSchema Schema, IndexSchema? Index, int KeyColumnIndex,
        int[]? Projection, ColumnDefinition[] OutputColumns, bool PrimaryKeyOnlyProjection, int MaxProjectedColumn);
}
