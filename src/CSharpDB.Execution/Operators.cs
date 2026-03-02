using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using CSharpDB.Core;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Full table scan operator — reads all rows from a B+tree via cursor.
/// </summary>
public sealed class TableScanOperator : IOperator, IRowBufferReuseController, IPreDecodeFilterSupport
{
    private readonly BTree _tree;
    private readonly TableSchema _schema;
    private readonly IRecordSerializer _recordSerializer;
    private BTreeCursor? _cursor;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private int? _maxDecodedColumnIndex;
    private int _preDecodeFilterColumnIndex;
    private BinaryOp _preDecodeFilterOp;
    private DbValue _preDecodeFilterLiteral;
    private byte[]? _preDecodeFilterTextBytes;
    private bool _hasPreDecodeFilter;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }

    public TableScanOperator(BTree tree, TableSchema schema, IRecordSerializer? recordSerializer = null)
    {
        _tree = tree;
        _schema = schema;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    /// <summary>
    /// Hint the scan to decode only columns up to this index.
    /// Used by scalar aggregate paths to avoid decoding unused trailing columns.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    /// <summary>
    /// Hint the scan to evaluate a simple predicate from encoded payload first.
    /// Rows that fail this predicate are skipped before full row decode.
    /// </summary>
    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        _preDecodeFilterColumnIndex = columnIndex;
        _preDecodeFilterOp = op;
        _preDecodeFilterLiteral = literal;
        _preDecodeFilterTextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _tree.CreateCursor();
        _rowBuffer = null;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null) return false;

        while (await _cursor.MoveNextAsync(ct))
        {
            var payload = _cursor.CurrentValue.Span;
            if (_hasPreDecodeFilter)
            {
                if (!EvaluatePreDecodeFilter(payload))
                    continue;
            }

            CurrentRowId = _cursor.CurrentKey;
            int targetColumnCount = _maxDecodedColumnIndex.HasValue
                ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
                : _schema.Columns.Count;

            if (_reuseCurrentRowBuffer)
            {
                EnsureRowBuffer(targetColumnCount);
                int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
                if (decodedCount < targetColumnCount)
                    Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

                Current = _rowBuffer!;
            }
            else
            {
                var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
                int decodedCount = _recordSerializer.DecodeInto(payload, row);
                if (decodedCount < targetColumnCount)
                    Array.Fill(row, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

                Current = row;
            }
            return true;
        }

        return false;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private bool EvaluatePreDecodeFilter(DbValue value)
    {
        int cmp = DbValue.Compare(value, _preDecodeFilterLiteral);
        return _preDecodeFilterOp switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        var textBytes = _preDecodeFilterTextBytes;
        if (textBytes != null &&
            _recordSerializer.TryColumnTextEquals(payload, _preDecodeFilterColumnIndex, textBytes, out bool textEquals))
        {
            return _preDecodeFilterOp == BinaryOp.Equals ? textEquals : !textEquals;
        }

        var filterValue = _recordSerializer.DecodeColumn(payload, _preDecodeFilterColumnIndex);
        return EvaluatePreDecodeFilter(filterValue);
    }
}

/// <summary>
/// Filter operator — applies a WHERE predicate.
/// </summary>
public sealed class FilterOperator : IOperator, IRowBufferReuseController
{
    private readonly IOperator _source;
    private readonly Func<DbValue[], DbValue> _predicateEvaluator;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;

    public FilterOperator(IOperator source, Expression predicate, TableSchema schema)
        : this(source, ExpressionCompiler.Compile(predicate, schema))
    {
    }

    public FilterOperator(IOperator source, Func<DbValue[], DbValue> predicateEvaluator)
    {
        _source = source;
        _predicateEvaluator = predicateEvaluator;
    }

    public ValueTask OpenAsync(CancellationToken ct = default) => _source.OpenAsync(ct);

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (await _source.MoveNextAsync(ct))
        {
            var result = _predicateEvaluator(_source.Current);
            if (result.IsTruthy) return true;
        }
        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }
}

/// <summary>
/// Projection operator — selects and reorders columns.
/// </summary>
public sealed class ProjectionOperator : IOperator, IRowBufferReuseController
{
    private readonly IOperator _source;
    private readonly int[] _columnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private bool _reuseCurrentRowBuffer = true;
    private DbValue[]? _rowBuffer;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public ProjectionOperator(IOperator source, int[] columnIndices, ColumnDefinition[] outputSchema, TableSchema schema, Expression[]? expressions = null)
    {
        _source = source;
        _columnIndices = columnIndices;
        if (expressions != null)
        {
            _expressionEvaluators = new Func<DbValue[], DbValue>[expressions.Length];
            for (int i = 0; i < expressions.Length; i++)
                _expressionEvaluators[i] = ExpressionCompiler.Compile(expressions[i], schema);
        }
        OutputSchema = outputSchema;
    }

    public ProjectionOperator(
        IOperator source,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators)
    {
        _source = source;
        _columnIndices = columnIndices;
        _expressionEvaluators = expressionEvaluators;
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _rowBuffer = null;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!await _source.MoveNextAsync(ct)) return false;

        if (_expressionEvaluators != null)
        {
            int valueCount = _expressionEvaluators.Length;
            var target = _reuseCurrentRowBuffer
                ? EnsureRowBuffer(valueCount)
                : valueCount == 0 ? Array.Empty<DbValue>() : new DbValue[valueCount];

            for (int i = 0; i < valueCount; i++)
                target[i] = _expressionEvaluators[i](_source.Current);

            Current = target;
        }
        else
        {
            int valueCount = _columnIndices.Length;
            if (CanPassThroughSourceRow(valueCount))
            {
                Current = _source.Current;
                return true;
            }

            var target = _reuseCurrentRowBuffer
                ? EnsureRowBuffer(valueCount)
                : valueCount == 0 ? Array.Empty<DbValue>() : new DbValue[valueCount];

            for (int i = 0; i < valueCount; i++)
                target[i] = _source.Current[_columnIndices[i]];

            Current = target;
        }
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private DbValue[] EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _rowBuffer;
    }

    private bool CanPassThroughSourceRow(int valueCount)
    {
        if (_source.ReusesCurrentRowBuffer)
            return false;

        var sourceRow = _source.Current;
        if (sourceRow.Length != valueCount)
            return false;

        for (int i = 0; i < valueCount; i++)
        {
            if (_columnIndices[i] != i)
                return false;
        }

        return true;
    }
}

/// <summary>
/// Offset operator — skips the first N rows from the source.
/// </summary>
public sealed class OffsetOperator : IOperator, IRowBufferReuseController
{
    private readonly IOperator _source;
    private readonly int _offset;
    private int _skipped;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;

    public OffsetOperator(IOperator source, int offset)
    {
        _source = source;
        _offset = offset;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _skipped = 0;
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (_skipped < _offset)
        {
            if (!await _source.MoveNextAsync(ct)) return false;
            _skipped++;
        }
        return await _source.MoveNextAsync(ct);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }
}

/// <summary>
/// Limit operator — caps the number of output rows.
/// </summary>
public sealed class LimitOperator : IOperator, IRowBufferReuseController
{
    private readonly IOperator _source;
    private readonly int _limit;
    private int _count;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;

    public LimitOperator(IOperator source, int limit)
    {
        _source = source;
        _limit = limit;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _count = 0;
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_count >= _limit) return false;
        if (!await _source.MoveNextAsync(ct)) return false;
        _count++;
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }
}

/// <summary>
/// Tracks DISTINCT aggregate argument values with a compact integer fast path.
/// </summary>
internal sealed class AggregateDistinctValueSet
{
    // Bitmap covers [0, 1_048_575] and stays compact (~128 KB).
    private const int IntegerBitmapMaxValue = (1 << 20) - 1;
    private const int IntegerBitmapWordCount = (IntegerBitmapMaxValue + 64) / 64;
    private const int BitmapPromotionDistinctCount = 1_024;

    private ulong[]? _integerBitmap;
    private HashSet<long>? _integerValues;
    private HashSet<DbValue>? _values;

    public bool Add(DbValue value)
    {
        if (value.Type == DbType.Integer)
            return AddInteger(value.AsInteger);

        EnsureGeneralSet();
        return _values!.Add(value);
    }

    private bool AddInteger(long value)
    {
        if (_values != null)
            return _values.Add(DbValue.FromInteger(value));

        if (_integerBitmap != null)
        {
            bool? bitmapAdded = TryAddToBitmap(value);
            if (bitmapAdded.HasValue)
                return bitmapAdded.Value;

            EnsureIntegerSet();
            return _integerValues!.Add(value);
        }

        _integerValues ??= new HashSet<long>();
        bool added = _integerValues.Add(value);
        if (!added)
            return false;

        if (_integerValues.Count == BitmapPromotionDistinctCount && CanPromoteToBitmap())
            PromoteIntegerSetToBitmap();

        return true;
    }

    private bool? TryAddToBitmap(long value)
    {
        if (value < 0 || value > IntegerBitmapMaxValue)
            return null;

        _integerBitmap ??= new ulong[IntegerBitmapWordCount];
        int bitIndex = (int)value;
        int wordIndex = bitIndex >> 6;
        ulong mask = 1UL << (bitIndex & 63);

        ref ulong word = ref _integerBitmap[wordIndex];
        if ((word & mask) != 0)
            return false;

        word |= mask;
        return true;
    }

    private void EnsureIntegerSet()
    {
        if (_integerValues != null)
            return;

        _integerValues = new HashSet<long>();
        if (_integerBitmap == null)
            return;

        for (int wordIndex = 0; wordIndex < _integerBitmap.Length; wordIndex++)
        {
            ulong word = _integerBitmap[wordIndex];
            while (word != 0)
            {
                int bit = System.Numerics.BitOperations.TrailingZeroCount(word);
                _integerValues.Add((wordIndex << 6) + bit);
                word &= word - 1;
            }
        }

        _integerBitmap = null;
    }

    private bool CanPromoteToBitmap()
    {
        return _integerValues != null
            && _integerValues.Count >= BitmapPromotionDistinctCount
            && _integerValues.All(static value => value >= 0 && value <= IntegerBitmapMaxValue);
    }

    private void PromoteIntegerSetToBitmap()
    {
        _integerBitmap = new ulong[IntegerBitmapWordCount];
        foreach (long integerValue in _integerValues!)
        {
            int bitIndex = (int)integerValue;
            _integerBitmap[bitIndex >> 6] |= 1UL << (bitIndex & 63);
        }

        _integerValues = null;
    }

    private void EnsureGeneralSet()
    {
        if (_values != null)
            return;

        _values = _integerValues != null
            ? new HashSet<DbValue>(_integerValues.Count)
            : new HashSet<DbValue>();

        if (_integerValues != null)
        {
            foreach (long integerValue in _integerValues)
                _values.Add(DbValue.FromInteger(integerValue));
        }
        else if (_integerBitmap != null)
        {
            for (int wordIndex = 0; wordIndex < _integerBitmap.Length; wordIndex++)
            {
                ulong word = _integerBitmap[wordIndex];
                while (word != 0)
                {
                    int bit = System.Numerics.BitOperations.TrailingZeroCount(word);
                    _values.Add(DbValue.FromInteger((wordIndex << 6) + bit));
                    word &= word - 1;
                }
            }
        }

        _integerValues = null;
        _integerBitmap = null;
    }
}

/// <summary>
/// Hash aggregate operator — groups rows and computes aggregate functions.
/// Used for GROUP BY and queries with aggregate functions (COUNT, SUM, AVG, MIN, MAX).
/// </summary>
public sealed class HashAggregateOperator : IOperator
{
    private readonly IOperator _source;
    private readonly List<SelectColumn> _selectColumns;
    private readonly List<Expression>? _groupByExprs;
    private readonly Expression? _havingExpr;
    private readonly TableSchema _inputSchema;
    private readonly List<FunctionCallExpression> _aggregateFunctions = new();
    private readonly Dictionary<FunctionCallExpression, int> _aggregateIndices = new();
    private readonly Func<DbValue[], DbValue>[]? _groupByEvaluators;
    private readonly bool _groupByIsConstant;
    private List<DbValue[]>? _results;
    private int _index;

    private static readonly GroupKeyComparer s_groupKeyComparer = new();

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public HashAggregateOperator(
        IOperator source,
        List<SelectColumn> selectColumns,
        List<Expression>? groupByExprs,
        Expression? havingExpr,
        TableSchema inputSchema,
        ColumnDefinition[] outputSchema)
    {
        _source = source;
        _selectColumns = selectColumns;
        _groupByExprs = groupByExprs;
        _havingExpr = havingExpr;
        _inputSchema = inputSchema;
        OutputSchema = outputSchema;
        if (_groupByExprs is { Count: > 0 })
        {
            _groupByEvaluators = BuildGroupByEvaluators(_groupByExprs, _inputSchema);
            _groupByIsConstant = _groupByExprs.All(e => e is LiteralExpression);
        }

        foreach (var col in _selectColumns)
        {
            if (col.Expression != null)
                CollectAggregates(col.Expression);
        }

        if (_havingExpr != null)
            CollectAggregates(_havingExpr);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await _source.OpenAsync(ct);

        _results = new List<DbValue[]>();
        bool hasGroupBy = _groupByExprs is { Count: > 0 };
        if (hasGroupBy)
        {
            if (_groupByIsConstant)
            {
                GroupState? singleGroup = null;
                while (await _source.MoveNextAsync(ct))
                {
                    var row = _source.Current;
                    if (singleGroup == null)
                    {
                        singleGroup = new GroupState(
                            firstRow: (DbValue[])row.Clone(),
                            aggregateStates: CreateAggregateStates());
                    }
                    singleGroup.Accumulate(row);
                }

                if (singleGroup != null)
                    EmitGroupResult(singleGroup);
            }
            else
            {
                // Stream rows into group accumulators; preserve first-seen group order.
                var groups = new List<GroupState>();
                var groupIndex = new Dictionary<GroupKey, int>(s_groupKeyComparer);

                while (await _source.MoveNextAsync(ct))
                {
                    var row = _source.Current;
                    var key = BuildGroupKey(row);
                    if (!groupIndex.TryGetValue(key, out int idx))
                    {
                        idx = groups.Count;
                        groupIndex[key] = idx;
                        groups.Add(new GroupState(
                            firstRow: (DbValue[])row.Clone(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[idx].Accumulate(row);
                }

                foreach (var group in groups)
                    EmitGroupResult(group);
            }
        }
        else
        {
            // No GROUP BY: aggregate entire table as one implicit group.
            var singleGroup = new GroupState(firstRow: null, aggregateStates: CreateAggregateStates());
            while (await _source.MoveNextAsync(ct))
            {
                var row = _source.Current;
                singleGroup.FirstRow ??= (DbValue[])row.Clone();
                singleGroup.Accumulate(row);
            }

            EmitGroupResult(singleGroup);
        }

        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_index >= _results!.Count) return ValueTask.FromResult(false);
        Current = _results[_index];
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    private void CollectAggregates(Expression expr)
    {
        switch (expr)
        {
            case FunctionCallExpression func:
                if (!_aggregateIndices.ContainsKey(func))
                {
                    _aggregateIndices.Add(func, _aggregateFunctions.Count);
                    _aggregateFunctions.Add(func);
                }
                break;
            case BinaryExpression bin:
                CollectAggregates(bin.Left);
                CollectAggregates(bin.Right);
                break;
            case UnaryExpression un:
                CollectAggregates(un.Operand);
                break;
        }
    }

    private AggregateState[] CreateAggregateStates()
    {
        var states = new AggregateState[_aggregateFunctions.Count];
        for (int i = 0; i < states.Length; i++)
            states[i] = new AggregateState(_aggregateFunctions[i], _inputSchema);
        return states;
    }

    private void EmitGroupResult(GroupState group)
    {
        var outputRow = new DbValue[_selectColumns.Count];
        for (int i = 0; i < _selectColumns.Count; i++)
        {
            if (_selectColumns[i].IsStar)
                outputRow[i] = DbValue.Null;
            else
                outputRow[i] = EvalWithAggregates(_selectColumns[i].Expression!, group);
        }

        if (_havingExpr != null)
        {
            var havingResult = EvalWithAggregates(_havingExpr, group);
            if (!havingResult.IsTruthy) return;
        }

        _results!.Add(outputRow);
    }

    private GroupKey BuildGroupKey(DbValue[] row)
    {
        if (_groupByEvaluators == null || _groupByEvaluators.Length == 0)
            return GroupKey.Empty;

        var values = new DbValue[_groupByEvaluators.Length];
        var hash = new HashCode();
        for (int i = 0; i < _groupByEvaluators.Length; i++)
        {
            var val = _groupByEvaluators[i](row);
            values[i] = val;
            hash.Add(val);
        }

        return new GroupKey(values, hash.ToHashCode());
    }

    private static Func<DbValue[], DbValue>[] BuildGroupByEvaluators(List<Expression> expressions, TableSchema schema)
    {
        var evaluators = new Func<DbValue[], DbValue>[expressions.Count];
        for (int i = 0; i < expressions.Count; i++)
            evaluators[i] = BuildGroupByEvaluator(expressions[i], schema);
        return evaluators;
    }

    private static Func<DbValue[], DbValue> BuildGroupByEvaluator(Expression expr, TableSchema schema)
    {
        return ExpressionCompiler.Compile(expr, schema);
    }

    private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
    {
        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        if (idx < 0)
        {
            string colName = col.TableAlias != null
                ? $"{col.TableAlias}.{col.ColumnName}"
                : col.ColumnName;
            throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
        }

        return row => idx < row.Length ? row[idx] : DbValue.Null;
    }

    private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
    {
        var value = lit.Value == null
            ? DbValue.Null
            : lit.LiteralType switch
            {
                TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                TokenType.Null => DbValue.Null,
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
            };

        return _ => value;
    }

    /// <summary>
    /// Evaluates an expression tree, replacing aggregate function calls with computed results.
    /// Non-aggregate expressions are evaluated against the first row of the group.
    /// </summary>
    private DbValue EvalWithAggregates(Expression expr, GroupState group)
    {
        return expr switch
        {
            FunctionCallExpression func => EvaluateAggregate(func, group),
            BinaryExpression bin => EvalBinaryWithAgg(bin, group),
            UnaryExpression un => EvalUnaryWithAgg(un, group),
            _ => group.FirstRow != null
                ? ExpressionEvaluator.Evaluate(expr, group.FirstRow, _inputSchema)
                : DbValue.Null,
        };
    }

    private DbValue EvaluateAggregate(FunctionCallExpression func, GroupState group)
    {
        if (_aggregateIndices.TryGetValue(func, out int idx))
            return group.AggregateStates[idx].GetFinalValue();

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {func.FunctionName}");
    }

    private DbValue EvalBinaryWithAgg(BinaryExpression bin, GroupState group)
    {
        var left = EvalWithAggregates(bin.Left, group);
        var right = EvalWithAggregates(bin.Right, group);

        return bin.Op switch
        {
            BinaryOp.Equals => BoolToDb(DbValue.Compare(left, right) == 0),
            BinaryOp.NotEquals => BoolToDb(DbValue.Compare(left, right) != 0),
            BinaryOp.LessThan => BoolToDb(DbValue.Compare(left, right) < 0),
            BinaryOp.GreaterThan => BoolToDb(DbValue.Compare(left, right) > 0),
            BinaryOp.LessOrEqual => BoolToDb(DbValue.Compare(left, right) <= 0),
            BinaryOp.GreaterOrEqual => BoolToDb(DbValue.Compare(left, right) >= 0),
            BinaryOp.And => BoolToDb(left.IsTruthy && right.IsTruthy),
            BinaryOp.Or => BoolToDb(left.IsTruthy || right.IsTruthy),
            BinaryOp.Plus => ArithOp(left, right, (a, b) => a + b, (a, b) => a + b),
            BinaryOp.Minus => ArithOp(left, right, (a, b) => a - b, (a, b) => a - b),
            BinaryOp.Multiply => ArithOp(left, right, (a, b) => a * b, (a, b) => a * b),
            BinaryOp.Divide => ArithOp(left, right, (a, b) => b != 0 ? a / b : 0, (a, b) => b != 0 ? a / b : 0),
            _ => DbValue.Null,
        };
    }

    private DbValue EvalUnaryWithAgg(UnaryExpression un, GroupState group)
    {
        var operand = EvalWithAggregates(un.Operand, group);
        return un.Op switch
        {
            TokenType.Not => BoolToDb(!operand.IsTruthy),
            TokenType.Minus when operand.Type == DbType.Integer => DbValue.FromInteger(-operand.AsInteger),
            TokenType.Minus when operand.Type == DbType.Real => DbValue.FromReal(-operand.AsReal),
            _ => DbValue.Null,
        };
    }

    private static DbValue BoolToDb(bool v) => DbValue.FromInteger(v ? 1 : 0);

    private static DbValue ArithOp(DbValue l, DbValue r, Func<long, long, long> intOp, Func<double, double, double> realOp)
    {
        if (l.IsNull || r.IsNull) return DbValue.Null;
        if (l.Type == DbType.Real || r.Type == DbType.Real) return DbValue.FromReal(realOp(l.AsReal, r.AsReal));
        if (l.Type == DbType.Integer && r.Type == DbType.Integer) return DbValue.FromInteger(intOp(l.AsInteger, r.AsInteger));
        return DbValue.Null;
    }

    private sealed class GroupState
    {
        public DbValue[]? FirstRow { get; set; }
        public AggregateState[] AggregateStates { get; }

        public GroupState(DbValue[]? firstRow, AggregateState[] aggregateStates)
        {
            FirstRow = firstRow;
            AggregateStates = aggregateStates;
        }

        public void Accumulate(DbValue[] row)
        {
            for (int i = 0; i < AggregateStates.Length; i++)
                AggregateStates[i].Accumulate(row);
        }
    }

    private readonly struct GroupKey
    {
        public static readonly GroupKey Empty = new(Array.Empty<DbValue>(), 0);

        public DbValue[] Values { get; }
        public int HashCode { get; }

        public GroupKey(DbValue[] values, int hashCode)
        {
            Values = values;
            HashCode = hashCode;
        }
    }

    private sealed class GroupKeyComparer : IEqualityComparer<GroupKey>
    {
        public bool Equals(GroupKey x, GroupKey y)
        {
            if (x.HashCode != y.HashCode) return false;
            if (x.Values.Length != y.Values.Length) return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }
            return true;
        }

        public int GetHashCode(GroupKey obj) => obj.HashCode;
    }

    private sealed class AggregateState
    {
        private readonly string _name;
        private readonly Func<DbValue[], DbValue>? _argumentEvaluator;
        private readonly bool _isDistinct;
        private readonly bool _isStarArg;

        private AggregateDistinctValueSet? _distinctValues;
        private long _count;
        private double _sum;
        private bool _hasReal;
        private bool _hasAny;
        private DbValue? _best;

        public AggregateState(FunctionCallExpression func, TableSchema schema)
        {
            _name = func.FunctionName;
            _argumentEvaluator = BuildAggregateArgumentEvaluator(func, schema);
            _isDistinct = func.IsDistinct;
            _isStarArg = func.IsStarArg;
            Reset();
        }

        public void Reset()
        {
            _distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
            _count = 0;
            _sum = 0;
            _hasReal = false;
            _hasAny = false;
            _best = null;
        }

        public void Accumulate(DbValue[] row)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        private static Func<DbValue[], DbValue>? BuildAggregateArgumentEvaluator(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg || func.Arguments.Count == 0)
                return null;

            return ExpressionCompiler.Compile(func.Arguments[0], schema);
        }

        private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
        {
            int idx = col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);

            if (idx < 0)
            {
                string colName = col.TableAlias != null
                    ? $"{col.TableAlias}.{col.ColumnName}"
                    : col.ColumnName;
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
            }

            return row => idx < row.Length ? row[idx] : DbValue.Null;
        }

        private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
        {
            var value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };

            return _ => value;
        }

        public DbValue GetFinalValue()
        {
            if (_name == "COUNT")
                return DbValue.FromInteger(_count);

            if (_name == "SUM")
            {
                if (!_hasAny) return DbValue.FromInteger(0);
                return _hasReal ? DbValue.FromReal(_sum) : DbValue.FromInteger((long)_sum);
            }

            if (_name == "AVG")
            {
                if (!_hasAny) return DbValue.Null;
                return DbValue.FromReal(_sum / _count);
            }

            if (_name is "MIN" or "MAX")
                return _best ?? DbValue.Null;

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }
    }
}

/// <summary>
/// Scalar aggregate operator — computes aggregate expressions over a single implicit group
/// without materializing all source rows.
/// Used for aggregate queries that do not have GROUP BY.
/// </summary>
public sealed class ScalarAggregateOperator : IOperator
{
    private readonly IOperator _source;
    private readonly List<SelectColumn> _selectColumns;
    private readonly Expression? _havingExpr;
    private readonly TableSchema _inputSchema;
    private readonly Dictionary<FunctionCallExpression, AggregateState> _aggregateStates = new();
    private readonly List<FunctionCallExpression> _aggregateFunctions = new();
    private readonly AggregateState[] _aggregateStateList;

    private bool _emitResult;
    private DbValue[]? _firstRow;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public ScalarAggregateOperator(
        IOperator source,
        List<SelectColumn> selectColumns,
        Expression? havingExpr,
        TableSchema inputSchema,
        ColumnDefinition[] outputSchema)
    {
        _source = source;
        _selectColumns = selectColumns;
        _havingExpr = havingExpr;
        _inputSchema = inputSchema;
        OutputSchema = outputSchema;

        foreach (var col in _selectColumns)
        {
            if (col.Expression != null)
                CollectAggregates(col.Expression);
        }

        if (_havingExpr != null)
            CollectAggregates(_havingExpr);

        _aggregateStateList = new AggregateState[_aggregateFunctions.Count];
        for (int i = 0; i < _aggregateFunctions.Count; i++)
        {
            var func = _aggregateFunctions[i];
            var state = new AggregateState(func, _inputSchema);
            _aggregateStateList[i] = state;
            _aggregateStates[func] = state;
        }
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await _source.OpenAsync(ct);

        for (int i = 0; i < _aggregateStateList.Length; i++)
            _aggregateStateList[i].Reset();

        _firstRow = null;

        if (_aggregateFunctions.Count == 0)
        {
            if (await _source.MoveNextAsync(ct))
                _firstRow = (DbValue[])_source.Current.Clone();
        }
        else
        {
            while (await _source.MoveNextAsync(ct))
            {
                var row = _source.Current;
                _firstRow ??= (DbValue[])row.Clone();

                for (int i = 0; i < _aggregateStateList.Length; i++)
                    _aggregateStateList[i].Accumulate(row);
            }
        }

        _emitResult = true;
        if (_havingExpr != null)
        {
            var havingResult = EvalWithAggregates(_havingExpr, _firstRow);
            if (!havingResult.IsTruthy)
                _emitResult = false;
        }

        if (_emitResult)
        {
            var outputRow = new DbValue[_selectColumns.Count];
            for (int i = 0; i < _selectColumns.Count; i++)
            {
                if (_selectColumns[i].IsStar)
                    outputRow[i] = DbValue.Null;
                else
                    outputRow[i] = EvalWithAggregates(_selectColumns[i].Expression!, _firstRow);
            }
            Current = outputRow;
        }
        else
        {
            Current = Array.Empty<DbValue>();
        }
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!_emitResult) return ValueTask.FromResult(false);
        _emitResult = false;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    private void CollectAggregates(Expression expr)
    {
        switch (expr)
        {
            case FunctionCallExpression func:
                if (!_aggregateStates.ContainsKey(func))
                {
                    _aggregateStates.Add(func, new AggregateState(func, _inputSchema));
                    _aggregateFunctions.Add(func);
                }
                break;
            case BinaryExpression bin:
                CollectAggregates(bin.Left);
                CollectAggregates(bin.Right);
                break;
            case UnaryExpression un:
                CollectAggregates(un.Operand);
                break;
        }
    }

    private DbValue EvalWithAggregates(Expression expr, DbValue[]? firstRow)
    {
        return expr switch
        {
            FunctionCallExpression func => EvaluateAggregate(func),
            BinaryExpression bin => EvalBinaryWithAgg(bin, firstRow),
            UnaryExpression un => EvalUnaryWithAgg(un, firstRow),
            _ => firstRow != null
                ? ExpressionEvaluator.Evaluate(expr, firstRow, _inputSchema)
                : DbValue.Null,
        };
    }

    private DbValue EvaluateAggregate(FunctionCallExpression func)
    {
        if (_aggregateStates.TryGetValue(func, out var state))
            return state.GetFinalValue();

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {func.FunctionName}");
    }

    private DbValue EvalBinaryWithAgg(BinaryExpression bin, DbValue[]? firstRow)
    {
        var left = EvalWithAggregates(bin.Left, firstRow);
        var right = EvalWithAggregates(bin.Right, firstRow);

        return bin.Op switch
        {
            BinaryOp.Equals => BoolToDb(DbValue.Compare(left, right) == 0),
            BinaryOp.NotEquals => BoolToDb(DbValue.Compare(left, right) != 0),
            BinaryOp.LessThan => BoolToDb(DbValue.Compare(left, right) < 0),
            BinaryOp.GreaterThan => BoolToDb(DbValue.Compare(left, right) > 0),
            BinaryOp.LessOrEqual => BoolToDb(DbValue.Compare(left, right) <= 0),
            BinaryOp.GreaterOrEqual => BoolToDb(DbValue.Compare(left, right) >= 0),
            BinaryOp.And => BoolToDb(left.IsTruthy && right.IsTruthy),
            BinaryOp.Or => BoolToDb(left.IsTruthy || right.IsTruthy),
            BinaryOp.Plus => ArithOp(left, right, (a, b) => a + b, (a, b) => a + b),
            BinaryOp.Minus => ArithOp(left, right, (a, b) => a - b, (a, b) => a - b),
            BinaryOp.Multiply => ArithOp(left, right, (a, b) => a * b, (a, b) => a * b),
            BinaryOp.Divide => ArithOp(left, right, (a, b) => b != 0 ? a / b : 0, (a, b) => b != 0 ? a / b : 0),
            _ => DbValue.Null,
        };
    }

    private DbValue EvalUnaryWithAgg(UnaryExpression un, DbValue[]? firstRow)
    {
        var operand = EvalWithAggregates(un.Operand, firstRow);
        return un.Op switch
        {
            TokenType.Not => BoolToDb(!operand.IsTruthy),
            TokenType.Minus when operand.Type == DbType.Integer => DbValue.FromInteger(-operand.AsInteger),
            TokenType.Minus when operand.Type == DbType.Real => DbValue.FromReal(-operand.AsReal),
            _ => DbValue.Null,
        };
    }

    private static DbValue BoolToDb(bool v) => DbValue.FromInteger(v ? 1 : 0);

    private static DbValue ArithOp(DbValue l, DbValue r, Func<long, long, long> intOp, Func<double, double, double> realOp)
    {
        if (l.IsNull || r.IsNull) return DbValue.Null;
        if (l.Type == DbType.Real || r.Type == DbType.Real) return DbValue.FromReal(realOp(l.AsReal, r.AsReal));
        if (l.Type == DbType.Integer && r.Type == DbType.Integer) return DbValue.FromInteger(intOp(l.AsInteger, r.AsInteger));
        return DbValue.Null;
    }

    private sealed class AggregateState
    {
        private readonly string _name;
        private readonly Func<DbValue[], DbValue>? _argumentEvaluator;
        private readonly bool _isDistinct;
        private readonly bool _isStarArg;

        private AggregateDistinctValueSet? _distinctValues;
        private long _count;
        private double _sum;
        private bool _hasReal;
        private bool _hasAny;
        private DbValue? _best;

        public AggregateState(FunctionCallExpression func, TableSchema schema)
        {
            _name = func.FunctionName;
            _argumentEvaluator = BuildAggregateArgumentEvaluator(func, schema);
            _isDistinct = func.IsDistinct;
            _isStarArg = func.IsStarArg;
            Reset();
        }

        public void Reset()
        {
            _distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
            _count = 0;
            _sum = 0;
            _hasReal = false;
            _hasAny = false;
            _best = null;
        }

        public void Accumulate(DbValue[] row)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        private static Func<DbValue[], DbValue>? BuildAggregateArgumentEvaluator(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg || func.Arguments.Count == 0)
                return null;

            return ExpressionCompiler.Compile(func.Arguments[0], schema);
        }

        private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
        {
            int idx = col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);

            if (idx < 0)
            {
                string colName = col.TableAlias != null
                    ? $"{col.TableAlias}.{col.ColumnName}"
                    : col.ColumnName;
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
            }

            return row => idx < row.Length ? row[idx] : DbValue.Null;
        }

        private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
        {
            var value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };

            return _ => value;
        }

        public DbValue GetFinalValue()
        {
            if (_name == "COUNT")
                return DbValue.FromInteger(_count);

            if (_name == "SUM")
            {
                if (!_hasAny) return DbValue.FromInteger(0);
                return _hasReal ? DbValue.FromReal(_sum) : DbValue.FromInteger((long)_sum);
            }

            if (_name == "AVG")
            {
                if (!_hasAny) return DbValue.Null;
                return DbValue.FromReal(_sum / _count);
            }

            if (_name is "MIN" or "MAX")
                return _best ?? DbValue.Null;

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }
    }
}

/// <summary>
/// Sort operator — materializes all input then sorts. Used for ORDER BY.
/// </summary>
public sealed class SortOperator : IOperator
{
    private const int TypedPrecomputedKeyMinRowCount = 50_000;

    private enum PrecomputedSingleKeyKind
    {
        None,
        Integer,
        Real,
        Text,
    }

    private enum SingleClauseComparerKind
    {
        Default,
        Integer,
        Real,
        Text,
    }

    private readonly struct CompiledSortClause
    {
        public readonly Expression Expression;
        public readonly int ColumnIndex;
        public readonly int KeyIndex;
        public readonly int Direction;
        public readonly Func<DbValue[], DbValue>? KeyEvaluator;

        public CompiledSortClause(
            Expression expression,
            int columnIndex,
            int keyIndex,
            bool descending,
            Func<DbValue[], DbValue>? keyEvaluator)
        {
            Expression = expression;
            ColumnIndex = columnIndex;
            KeyIndex = keyIndex;
            Direction = descending ? -1 : 1;
            KeyEvaluator = keyEvaluator;
        }

        public DbValue EvaluateRow(DbValue[] row)
        {
            return ColumnIndex >= 0 && ColumnIndex < row.Length ? row[ColumnIndex] : DbValue.Null;
        }

        public DbValue EvaluateSortKey(DbValue[] row)
        {
            return KeyEvaluator != null ? KeyEvaluator(row) : EvaluateRow(row);
        }
    }

    private readonly IOperator _source;
    private readonly CompiledSortClause[] _compiledOrderBy;
    private readonly int _precomputedKeyCount;
    private readonly bool _hasSingleOrderByClause;
    private readonly int _singleClauseColumnIndex;
    private readonly int _singleClauseKeyIndex;
    private readonly int _singleClauseDirection;
    private readonly SingleClauseComparerKind _singleClauseComparerKind;
    private readonly TableSchema _schema;
    private List<DbValue[]>? _sortedRows;
    private int[]? _sortedRowIndices;
    private DbValue[][]? _precomputedKeyColumns;
    private PrecomputedSingleKeyKind _singlePrecomputedKeyKind;
    private long[]? _singlePrecomputedIntKeys;
    private double[]? _singlePrecomputedRealKeys;
    private string?[]? _singlePrecomputedTextKeys;
    private bool[]? _singlePrecomputedNulls;
    private int _pooledRowCount;
    private int _index;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public SortOperator(IOperator source, List<OrderByClause> orderBy, TableSchema schema)
    {
        _source = source;
        _schema = schema;
        _compiledOrderBy = CompileOrderBy(orderBy, schema, out _precomputedKeyCount);
        if (_compiledOrderBy.Length == 1)
        {
            var clause = _compiledOrderBy[0];
            _hasSingleOrderByClause = true;
            _singleClauseColumnIndex = clause.ColumnIndex;
            _singleClauseKeyIndex = clause.KeyIndex;
            _singleClauseDirection = clause.Direction;
            _singleClauseComparerKind = ResolveSingleClauseComparerKind(clause, schema);
        }
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(false);

        await _source.OpenAsync(ct);
        bool cloneRows = _source.ReusesCurrentRowBuffer;
        ReleasePooledBuffers();
        _sortedRows = new List<DbValue[]>();
        _sortedRowIndices = null;
        _precomputedKeyColumns = null;

        while (await _source.MoveNextAsync(ct))
        {
            var row = _source.Current;
            _sortedRows.Add(cloneRows ? (DbValue[])row.Clone() : row);
        }

        if (_precomputedKeyCount == 0)
        {
            _sortedRows.Sort(CompareRows);
        }
        else
        {
            int rowCount = _sortedRows.Count;
            _pooledRowCount = rowCount;
            if (!TryPrecomputeSingleClauseKeys(_sortedRows, rowCount))
            {
                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                for (int i = 0; i < _precomputedKeyColumns.Length; i++)
                    _precomputedKeyColumns[i] = ArrayPool<DbValue>.Shared.Rent(rowCount);

                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    var row = _sortedRows[rowIndex];
                    for (int clauseIndex = 0; clauseIndex < _compiledOrderBy.Length; clauseIndex++)
                    {
                        var clause = _compiledOrderBy[clauseIndex];
                        if (clause.KeyIndex < 0) continue;
                        _precomputedKeyColumns[clause.KeyIndex][rowIndex] = clause.EvaluateSortKey(row);
                    }
                }
            }

            _sortedRowIndices = ArrayPool<int>.Shared.Rent(rowCount);
            for (int i = 0; i < rowCount; i++)
                _sortedRowIndices[i] = i;
            _sortedRowIndices.AsSpan(0, rowCount).Sort(CompareRowIndices);
        }

        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_sortedRows == null || _index >= _sortedRows.Count)
            return ValueTask.FromResult(false);

        if (_sortedRowIndices != null)
        {
            Current = _sortedRows[_sortedRowIndices[_index]];
            return ValueTask.FromResult(true);
        }

        Current = _sortedRows[_index];
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync()
    {
        ReleasePooledBuffers();
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return _source.DisposeAsync();
    }

    private int CompareRows(DbValue[] a, DbValue[] b)
    {
        if (_hasSingleOrderByClause)
        {
            int idx = _singleClauseColumnIndex;
            var va = idx < a.Length ? a[idx] : DbValue.Null;
            var vb = idx < b.Length ? b[idx] : DbValue.Null;
            return CompareSingleClauseValues(va, vb) * _singleClauseDirection;
        }

        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            var va = clause.EvaluateRow(a);
            var vb = clause.EvaluateRow(b);
            int cmp = DbValue.Compare(va, vb);
            if (cmp != 0) return cmp * clause.Direction;
        }

        return 0;
    }

    private int CompareRowIndices(int aIndex, int bIndex)
    {
        var rows = _sortedRows!;
        var keyColumns = _precomputedKeyColumns!;
        if (_hasSingleOrderByClause)
        {
            if (_singlePrecomputedKeyKind != PrecomputedSingleKeyKind.None)
                return CompareSingleClausePrecomputedKeys(aIndex, bIndex) * _singleClauseDirection;

            var va = _singleClauseKeyIndex >= 0
                ? keyColumns[_singleClauseKeyIndex][aIndex]
                : (_singleClauseColumnIndex < rows[aIndex].Length
                    ? rows[aIndex][_singleClauseColumnIndex]
                    : DbValue.Null);
            var vb = _singleClauseKeyIndex >= 0
                ? keyColumns[_singleClauseKeyIndex][bIndex]
                : (_singleClauseColumnIndex < rows[bIndex].Length
                    ? rows[bIndex][_singleClauseColumnIndex]
                    : DbValue.Null);
            if (_singleClauseComparerKind == SingleClauseComparerKind.Default)
                return DbValue.Compare(va, vb) * _singleClauseDirection;
            return CompareSingleClauseValues(va, vb) * _singleClauseDirection;
        }

        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            var va = clause.KeyIndex >= 0
                ? keyColumns[clause.KeyIndex][aIndex]
                : clause.EvaluateRow(rows[aIndex]);
            var vb = clause.KeyIndex >= 0
                ? keyColumns[clause.KeyIndex][bIndex]
                : clause.EvaluateRow(rows[bIndex]);
            int cmp = DbValue.Compare(va, vb);
            if (cmp != 0) return cmp * clause.Direction;
        }

        return 0;
    }

    private static CompiledSortClause[] CompileOrderBy(List<OrderByClause> orderBy, TableSchema schema, out int precomputedKeyCount)
    {
        precomputedKeyCount = 0;
        var compiled = new CompiledSortClause[orderBy.Count];

        for (int i = 0; i < orderBy.Count; i++)
        {
            var clause = orderBy[i];
            int columnIndex = ResolveColumnIndex(clause.Expression, schema);
            int keyIndex = columnIndex >= 0 ? -1 : precomputedKeyCount++;
            Func<DbValue[], DbValue>? keyEvaluator = keyIndex >= 0
                ? ExpressionCompiler.Compile(clause.Expression, schema)
                : null;
            compiled[i] = new CompiledSortClause(clause.Expression, columnIndex, keyIndex, clause.Descending, keyEvaluator);
        }

        return compiled;
    }

    private static int ResolveColumnIndex(Expression expression, TableSchema schema)
    {
        if (expression is not ColumnRefExpression col)
            return -1;

        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        return idx;
    }

    private static SingleClauseComparerKind ResolveSingleClauseComparerKind(
        CompiledSortClause clause,
        TableSchema schema)
    {
        if (clause.KeyIndex >= 0 || clause.ColumnIndex < 0 || clause.ColumnIndex >= schema.Columns.Count)
            return SingleClauseComparerKind.Default;

        return schema.Columns[clause.ColumnIndex].Type switch
        {
            DbType.Integer => SingleClauseComparerKind.Integer,
            DbType.Real => SingleClauseComparerKind.Real,
            DbType.Text => SingleClauseComparerKind.Text,
            _ => SingleClauseComparerKind.Default,
        };
    }

    private int CompareSingleClauseValues(DbValue a, DbValue b)
    {
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        switch (_singleClauseComparerKind)
        {
            case SingleClauseComparerKind.Integer:
                if (a.Type == DbType.Integer && b.Type == DbType.Integer)
                    return a.AsInteger.CompareTo(b.AsInteger);
                break;

            case SingleClauseComparerKind.Real:
                if (a.Type is DbType.Integer or DbType.Real
                    && b.Type is DbType.Integer or DbType.Real)
                    return a.AsReal.CompareTo(b.AsReal);
                break;

            case SingleClauseComparerKind.Text:
                if (a.Type == DbType.Text && b.Type == DbType.Text)
                    return string.Compare(a.AsText, b.AsText, StringComparison.Ordinal);
                break;
        }

        return DbValue.Compare(a, b);
    }

    private bool TryPrecomputeSingleClauseKeys(List<DbValue[]> rows, int rowCount)
    {
        _singlePrecomputedKeyKind = PrecomputedSingleKeyKind.None;
        _singlePrecomputedIntKeys = null;
        _singlePrecomputedRealKeys = null;
        _singlePrecomputedTextKeys = null;
        _singlePrecomputedNulls = null;

        if (!_hasSingleOrderByClause
            || _singleClauseKeyIndex < 0
            || rowCount < TypedPrecomputedKeyMinRowCount
            || _compiledOrderBy.Length != 1)
            return false;

        var clause = _compiledOrderBy[0];
        if (clause.KeyIndex < 0 || clause.KeyEvaluator == null)
            return false;

        bool[]? nulls = null;
        long[]? intValues = null;
        double[]? realValues = null;
        string?[]? textValues = null;
        DbValue[]? genericValues = null;
        PrecomputedSingleKeyKind kind = PrecomputedSingleKeyKind.None;
        try
        {
            nulls = ArrayPool<bool>.Shared.Rent(rowCount);
            Array.Clear(nulls, 0, rowCount);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                var key = clause.EvaluateSortKey(rows[rowIndex]);
                if (genericValues != null)
                {
                    genericValues[rowIndex] = key;
                    continue;
                }

                if (key.IsNull)
                {
                    nulls[rowIndex] = true;
                    continue;
                }

                switch (key.Type)
                {
                    case DbType.Integer:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Integer;
                            intValues = ArrayPool<long>.Shared.Rent(rowCount);
                        }

                        if (kind == PrecomputedSingleKeyKind.Integer)
                        {
                            intValues![rowIndex] = key.AsInteger;
                            continue;
                        }

                        if (kind == PrecomputedSingleKeyKind.Real)
                        {
                            realValues![rowIndex] = key.AsReal;
                            continue;
                        }

                        break;
                    }

                    case DbType.Real:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Real;
                            realValues = ArrayPool<double>.Shared.Rent(rowCount);
                        }
                        else if (kind == PrecomputedSingleKeyKind.Integer)
                        {
                            kind = PrecomputedSingleKeyKind.Real;
                            var upgraded = ArrayPool<double>.Shared.Rent(rowCount);
                            for (int i = 0; i < rowIndex; i++)
                            {
                                if (!nulls[i])
                                    upgraded[i] = intValues![i];
                            }

                            ArrayPool<long>.Shared.Return(intValues!, clearArray: false);
                            intValues = null;
                            realValues = upgraded;
                        }

                        if (kind == PrecomputedSingleKeyKind.Real)
                        {
                            realValues![rowIndex] = key.AsReal;
                            continue;
                        }

                        break;
                    }

                    case DbType.Text:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Text;
                            textValues = ArrayPool<string?>.Shared.Rent(rowCount);
                            Array.Clear(textValues, 0, rowCount);
                        }

                        if (kind == PrecomputedSingleKeyKind.Text)
                        {
                            textValues![rowIndex] = key.AsText;
                            continue;
                        }

                        break;
                    }
                }

                // Mixed or unsupported types: fallback to generic DbValue key storage.
                genericValues = ArrayPool<DbValue>.Shared.Rent(rowCount);
                for (int i = 0; i < rowIndex; i++)
                {
                    if (nulls[i])
                    {
                        genericValues[i] = DbValue.Null;
                        continue;
                    }

                    genericValues[i] = kind switch
                    {
                        PrecomputedSingleKeyKind.Integer => DbValue.FromInteger(intValues![i]),
                        PrecomputedSingleKeyKind.Real => DbValue.FromReal(realValues![i]),
                        PrecomputedSingleKeyKind.Text => DbValue.FromText(textValues![i]!),
                        _ => DbValue.Null,
                    };
                }

                genericValues[rowIndex] = key;

                if (intValues != null)
                {
                    ArrayPool<long>.Shared.Return(intValues, clearArray: false);
                    intValues = null;
                }

                if (realValues != null)
                {
                    ArrayPool<double>.Shared.Return(realValues, clearArray: false);
                    realValues = null;
                }

                if (textValues != null)
                {
                    if (rowIndex > 0)
                        Array.Clear(textValues, 0, rowIndex);
                    ArrayPool<string?>.Shared.Return(textValues, clearArray: false);
                    textValues = null;
                }
            }

            if (genericValues != null)
            {
                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                _precomputedKeyColumns[_singleClauseKeyIndex] = genericValues;
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                return true;
            }

            if (kind == PrecomputedSingleKeyKind.None)
            {
                // All keys are NULL; generic keys preserve ordering semantics.
                var allNullKeys = ArrayPool<DbValue>.Shared.Rent(rowCount);
                Array.Fill(allNullKeys, DbValue.Null, 0, rowCount);
                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                _precomputedKeyColumns[_singleClauseKeyIndex] = allNullKeys;
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                return true;
            }

            _singlePrecomputedKeyKind = kind;
            _singlePrecomputedIntKeys = intValues;
            _singlePrecomputedRealKeys = realValues;
            _singlePrecomputedTextKeys = textValues;
            _singlePrecomputedNulls = nulls;
            return true;
        }
        catch
        {
            if (nulls != null)
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);

            if (intValues != null)
                ArrayPool<long>.Shared.Return(intValues, clearArray: false);

            if (realValues != null)
                ArrayPool<double>.Shared.Return(realValues, clearArray: false);

            if (textValues != null)
            {
                Array.Clear(textValues, 0, rowCount);
                ArrayPool<string?>.Shared.Return(textValues, clearArray: false);
            }

            if (genericValues != null)
            {
                Array.Clear(genericValues, 0, rowCount);
                ArrayPool<DbValue>.Shared.Return(genericValues, clearArray: false);
            }

            throw;
        }
    }

    private int CompareSingleClausePrecomputedKeys(int aIndex, int bIndex)
    {
        var nulls = _singlePrecomputedNulls!;
        bool aNull = nulls[aIndex];
        bool bNull = nulls[bIndex];
        if (aNull || bNull)
        {
            if (aNull == bNull) return 0;
            return aNull ? -1 : 1;
        }

        return _singlePrecomputedKeyKind switch
        {
            PrecomputedSingleKeyKind.Integer => _singlePrecomputedIntKeys![aIndex].CompareTo(_singlePrecomputedIntKeys![bIndex]),
            PrecomputedSingleKeyKind.Real => _singlePrecomputedRealKeys![aIndex].CompareTo(_singlePrecomputedRealKeys![bIndex]),
            PrecomputedSingleKeyKind.Text => string.Compare(_singlePrecomputedTextKeys![aIndex], _singlePrecomputedTextKeys![bIndex], StringComparison.Ordinal),
            _ => 0,
        };
    }

    private void ReleasePooledBuffers()
    {
        if (_singlePrecomputedIntKeys != null)
        {
            ArrayPool<long>.Shared.Return(_singlePrecomputedIntKeys, clearArray: false);
            _singlePrecomputedIntKeys = null;
        }

        if (_singlePrecomputedRealKeys != null)
        {
            ArrayPool<double>.Shared.Return(_singlePrecomputedRealKeys, clearArray: false);
            _singlePrecomputedRealKeys = null;
        }

        if (_singlePrecomputedTextKeys != null)
        {
            if (_pooledRowCount > 0)
                Array.Clear(_singlePrecomputedTextKeys, 0, _pooledRowCount);
            ArrayPool<string?>.Shared.Return(_singlePrecomputedTextKeys, clearArray: false);
            _singlePrecomputedTextKeys = null;
        }

        if (_singlePrecomputedNulls != null)
        {
            if (_pooledRowCount > 0)
                Array.Clear(_singlePrecomputedNulls, 0, _pooledRowCount);
            ArrayPool<bool>.Shared.Return(_singlePrecomputedNulls, clearArray: false);
            _singlePrecomputedNulls = null;
        }

        _singlePrecomputedKeyKind = PrecomputedSingleKeyKind.None;

        if (_precomputedKeyColumns != null)
        {
            for (int i = 0; i < _precomputedKeyColumns.Length; i++)
            {
                var column = _precomputedKeyColumns[i];
                if (column == null) continue;
                if (_pooledRowCount > 0)
                    Array.Clear(column, 0, _pooledRowCount);
                ArrayPool<DbValue>.Shared.Return(column, clearArray: false);
            }

            _precomputedKeyColumns = null;
        }

        if (_sortedRowIndices != null)
        {
            ArrayPool<int>.Shared.Return(_sortedRowIndices, clearArray: false);
            _sortedRowIndices = null;
        }

        _pooledRowCount = 0;
    }
}

/// <summary>
/// ORDER BY with a bounded top-N heap.
/// Keeps only the best N rows in memory and does a final in-memory sort
/// over that bounded set.
/// </summary>
public sealed class TopNSortOperator : IOperator
{
    private readonly struct CompiledSortClause
    {
        public readonly int ColumnIndex;
        public readonly int KeyIndex;
        public readonly int Direction;
        public readonly Func<DbValue[], DbValue>? KeyEvaluator;

        public CompiledSortClause(int columnIndex, int keyIndex, bool descending, Func<DbValue[], DbValue>? keyEvaluator)
        {
            ColumnIndex = columnIndex;
            KeyIndex = keyIndex;
            Direction = descending ? -1 : 1;
            KeyEvaluator = keyEvaluator;
        }

        public DbValue EvaluateRow(DbValue[] row)
        {
            return ColumnIndex >= 0 && ColumnIndex < row.Length ? row[ColumnIndex] : DbValue.Null;
        }

        public DbValue EvaluateSortKey(DbValue[] row)
        {
            return KeyEvaluator != null ? KeyEvaluator(row) : EvaluateRow(row);
        }
    }

    private readonly struct RankedRow
    {
        public readonly DbValue[] Row;
        public readonly DbValue[]? Keys;

        public RankedRow(DbValue[] row, DbValue[]? keys)
        {
            Row = row;
            Keys = keys;
        }
    }

    private readonly IOperator _source;
    private readonly CompiledSortClause[] _compiledOrderBy;
    private readonly int _precomputedKeyCount;
    private readonly int _topN;

    private List<DbValue[]>? _sortedRows;
    private int _index;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public TopNSortOperator(IOperator source, List<OrderByClause> orderBy, TableSchema schema, int topN)
    {
        _source = source;
        _compiledOrderBy = CompileOrderBy(orderBy, schema, out _precomputedKeyCount);
        _topN = Math.Max(0, topN);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await _source.OpenAsync(ct);
        _sortedRows = new List<DbValue[]>();

        if (_topN == 0)
        {
            _index = -1;
            return;
        }

        bool sourceReusesCurrentRowBuffer = _source.ReusesCurrentRowBuffer;
        var heap = new List<RankedRow>(_topN);

        while (await _source.MoveNextAsync(ct))
        {
            var row = _source.Current;
            var rankedRow = BuildRankedRow(row);

            if (heap.Count < _topN)
            {
                HeapPush(heap, EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer));
                continue;
            }

            // Root stores the current worst row among the retained set.
            // Replace only when the new row is strictly better.
            if (CompareRankedRows(rankedRow, heap[0]) < 0)
            {
                heap[0] = EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer);
                HeapSiftDown(heap, 0);
            }
        }

        heap.Sort(CompareRankedRows);
        _sortedRows.Capacity = heap.Count;
        for (int i = 0; i < heap.Count; i++)
            _sortedRows.Add(heap[i].Row);

        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_sortedRows == null || _index >= _sortedRows.Count)
            return ValueTask.FromResult(false);

        Current = _sortedRows[_index];
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync()
    {
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return _source.DisposeAsync();
    }

    private RankedRow BuildRankedRow(DbValue[] row)
    {
        if (_precomputedKeyCount == 0)
            return new RankedRow(row, null);

        var keys = new DbValue[_precomputedKeyCount];
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            if (clause.KeyIndex < 0) continue;
            keys[clause.KeyIndex] = clause.EvaluateSortKey(row);
        }

        return new RankedRow(row, keys);
    }

    private int CompareRankedRows(RankedRow left, RankedRow right)
    {
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            var leftValue = clause.KeyIndex >= 0
                ? left.Keys![clause.KeyIndex]
                : clause.EvaluateRow(left.Row);
            var rightValue = clause.KeyIndex >= 0
                ? right.Keys![clause.KeyIndex]
                : clause.EvaluateRow(right.Row);

            int cmp = DbValue.Compare(leftValue, rightValue);
            if (cmp != 0)
                return cmp * clause.Direction;
        }

        return 0;
    }

    private void HeapPush(List<RankedRow> heap, RankedRow value)
    {
        heap.Add(value);
        HeapSiftUp(heap, heap.Count - 1);
    }

    private void HeapSiftUp(List<RankedRow> heap, int index)
    {
        while (index > 0)
        {
            int parent = (index - 1) >> 1;
            // Max-heap by ORDER BY rank: "larger" means worse.
            if (CompareRankedRows(heap[index], heap[parent]) <= 0)
                break;

            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private void HeapSiftDown(List<RankedRow> heap, int index)
    {
        int count = heap.Count;
        while (true)
        {
            int left = (index << 1) + 1;
            if (left >= count)
                return;

            int right = left + 1;
            int largest = left;
            if (right < count && CompareRankedRows(heap[right], heap[left]) > 0)
                largest = right;

            if (CompareRankedRows(heap[largest], heap[index]) <= 0)
                return;

            (heap[index], heap[largest]) = (heap[largest], heap[index]);
            index = largest;
        }
    }

    private static RankedRow EnsureOwnedRow(RankedRow row, bool sourceReusesCurrentRowBuffer)
    {
        if (!sourceReusesCurrentRowBuffer)
            return row;

        return new RankedRow((DbValue[])row.Row.Clone(), row.Keys);
    }

    private static CompiledSortClause[] CompileOrderBy(List<OrderByClause> orderBy, TableSchema schema, out int precomputedKeyCount)
    {
        precomputedKeyCount = 0;
        var compiled = new CompiledSortClause[orderBy.Count];

        for (int i = 0; i < orderBy.Count; i++)
        {
            var clause = orderBy[i];
            int columnIndex = ResolveColumnIndex(clause.Expression, schema);
            int keyIndex = columnIndex >= 0 ? -1 : precomputedKeyCount++;
            Func<DbValue[], DbValue>? keyEvaluator = keyIndex >= 0
                ? ExpressionCompiler.Compile(clause.Expression, schema)
                : null;
            compiled[i] = new CompiledSortClause(columnIndex, keyIndex, clause.Descending, keyEvaluator);
        }

        return compiled;
    }

    private static int ResolveColumnIndex(Expression expression, TableSchema schema)
    {
        if (expression is not ColumnRefExpression col)
            return -1;

        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        return idx;
    }
}

/// <summary>
/// Hash-join operator for equi-joins (with optional residual predicate).
/// Supports INNER, LEFT OUTER, and RIGHT OUTER joins.
/// </summary>
public sealed class HashJoinOperator : IOperator
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly JoinType _joinType;
    private readonly bool _buildRightSide;
    private readonly int[] _leftKeyIndices;
    private readonly int[] _rightKeyIndices;
    private readonly Func<DbValue[], DbValue>? _residualPredicate;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly bool _singleKeyFastPath;
    private readonly int _singleBuildKeyIndex;
    private readonly int _singleProbeKeyIndex;
    private Dictionary<HashJoinKey, List<DbValue[]>>? _hashTable;
    private Dictionary<DbValue, List<DbValue[]>>? _singleKeyHashTable;
    private List<DbValue[]>? _allRightRows;
    private HashSet<DbValue[]>? _matchedRightRows;
    private DbValue[]? _activeProbeRow;
    private List<DbValue[]>? _activeBuildMatches;
    private int _activeBuildMatchIndex;
    private bool _activeProbeMatched;
    private bool _probeExhausted;
    private int _rightOuterEmitIndex;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public HashJoinOperator(
        IOperator left,
        IOperator right,
        JoinType joinType,
        Expression? residualCondition,
        TableSchema compositeSchema,
        int leftColCount,
        int rightColCount,
        int[] leftKeyIndices,
        int[] rightKeyIndices,
        bool buildRightSide = true)
    {
        _left = left;
        _right = right;
        _joinType = joinType;
        _buildRightSide = buildRightSide;
        _leftKeyIndices = leftKeyIndices;
        _rightKeyIndices = rightKeyIndices;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _singleKeyFastPath = leftKeyIndices.Length == 1 && rightKeyIndices.Length == 1;
        _singleBuildKeyIndex = _buildRightSide ? rightKeyIndices[0] : leftKeyIndices[0];
        _singleProbeKeyIndex = _buildRightSide ? leftKeyIndices[0] : rightKeyIndices[0];
        _residualPredicate = residualCondition != null
            ? ExpressionCompiler.Compile(residualCondition, compositeSchema)
            : null;
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_joinType is not (JoinType.Inner or JoinType.LeftOuter or JoinType.RightOuter))
            throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported hash join type: {_joinType}");
        if (!_buildRightSide && _joinType != JoinType.Inner)
            throw new CSharpDbException(ErrorCode.Unknown, "Swapped hash build side is supported for INNER JOIN only.");

        if (_left is IRowBufferReuseController leftController)
            leftController.SetReuseCurrentRowBuffer(false);
        if (_right is IRowBufferReuseController rightController)
            rightController.SetReuseCurrentRowBuffer(false);

        await _left.OpenAsync(ct);
        await _right.OpenAsync(ct);

        _hashTable = _singleKeyFastPath
            ? null
            : new Dictionary<HashJoinKey, List<DbValue[]>>(HashJoinKeyComparer.Instance);
        _singleKeyHashTable = _singleKeyFastPath
            ? new Dictionary<DbValue, List<DbValue[]>>()
            : null;
        _allRightRows = _buildRightSide && _joinType == JoinType.RightOuter ? new List<DbValue[]>() : null;
        _matchedRightRows = _buildRightSide && _joinType == JoinType.RightOuter
            ? new HashSet<DbValue[]>(ReferenceEqualityComparer.Instance)
            : null;

        var buildSource = BuildSource;
        while (await buildSource.MoveNextAsync(ct))
        {
            var buildCurrent = buildSource.Current;
            var buildRow = buildSource.ReusesCurrentRowBuffer
                ? (DbValue[])buildCurrent.Clone()
                : buildCurrent;
            AddBuildRow(buildRow);
        }

        Current = Array.Empty<DbValue>();
        ResetProbeState();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_activeProbeRow != null)
            {
                while (_activeBuildMatches != null && _activeBuildMatchIndex < _activeBuildMatches.Count)
                {
                    var buildMatch = _activeBuildMatches[_activeBuildMatchIndex++];
                    var combined = CombineProbeAndBuildRows(_activeProbeRow, buildMatch);
                    if (!PassesResidual(combined))
                        continue;

                    _activeProbeMatched = true;
                    if (_buildRightSide && _joinType == JoinType.RightOuter)
                        _matchedRightRows?.Add(buildMatch);

                    Current = combined;
                    return true;
                }

                if (_buildRightSide && !_activeProbeMatched && _joinType == JoinType.LeftOuter)
                {
                    Current = CombineWithNulls(_activeProbeRow, _rightColCount, padRight: true);
                    ClearActiveProbeState();
                    return true;
                }

                ClearActiveProbeState();
                continue;
            }

            if (!_probeExhausted)
            {
                var probeSource = ProbeSource;
                if (await probeSource.MoveNextAsync(ct))
                {
                    var probeRow = probeSource.Current;

                    _activeProbeRow = probeRow;
                    _activeProbeMatched = false;
                    _activeBuildMatchIndex = 0;

                    if (_singleKeyFastPath)
                    {
                        var keyValue = ExtractSingleJoinKey(probeRow, _singleProbeKeyIndex);
                        _singleKeyHashTable!.TryGetValue(keyValue, out _activeBuildMatches);
                    }
                    else
                    {
                        var key = ExtractJoinKey(probeRow, ProbeKeyIndices);
                        _hashTable!.TryGetValue(key, out _activeBuildMatches);
                    }
                    continue;
                }

                _probeExhausted = true;
            }

            if (_buildRightSide &&
                _joinType == JoinType.RightOuter &&
                TryEmitUnmatchedRightRow(out var unmatchedRightRow))
            {
                Current = unmatchedRightRow;
                return true;
            }

            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync();
        await _right.DisposeAsync();
    }

    private IOperator BuildSource => _buildRightSide ? _right : _left;
    private IOperator ProbeSource => _buildRightSide ? _left : _right;
    private int[] BuildKeyIndices => _buildRightSide ? _rightKeyIndices : _leftKeyIndices;
    private int[] ProbeKeyIndices => _buildRightSide ? _leftKeyIndices : _rightKeyIndices;

    private void AddBuildRow(DbValue[] buildRow)
    {
        if (_buildRightSide && _allRightRows != null)
            _allRightRows.Add(buildRow);

        if (_singleKeyFastPath)
        {
            var keyValue = ExtractSingleJoinKey(buildRow, _singleBuildKeyIndex);
            if (!_singleKeyHashTable!.TryGetValue(keyValue, out var singleBucket))
            {
                singleBucket = new List<DbValue[]>();
                _singleKeyHashTable[keyValue] = singleBucket;
            }

            singleBucket.Add(buildRow);
            return;
        }

        var key = ExtractJoinKey(buildRow, BuildKeyIndices);
        if (!_hashTable!.TryGetValue(key, out var bucket))
        {
            bucket = new List<DbValue[]>();
            _hashTable[key] = bucket;
        }

        bucket.Add(buildRow);
    }

    private bool TryEmitUnmatchedRightRow(out DbValue[] row)
    {
        row = Array.Empty<DbValue>();
        if (_allRightRows == null)
            return false;

        while (_rightOuterEmitIndex < _allRightRows.Count)
        {
            var rightRow = _allRightRows[_rightOuterEmitIndex++];
            if (_matchedRightRows != null && _matchedRightRows.Contains(rightRow))
                continue;

            row = CombineWithNulls(rightRow, _leftColCount, padRight: false);
            return true;
        }

        return false;
    }

    private void ResetProbeState()
    {
        _activeProbeRow = null;
        _activeBuildMatches = null;
        _activeBuildMatchIndex = 0;
        _activeProbeMatched = false;
        _probeExhausted = false;
        _rightOuterEmitIndex = 0;
    }

    private void ClearActiveProbeState()
    {
        _activeProbeRow = null;
        _activeBuildMatches = null;
        _activeBuildMatchIndex = 0;
        _activeProbeMatched = false;
    }

    private bool PassesResidual(DbValue[] combined)
    {
        return _residualPredicate == null || _residualPredicate(combined).IsTruthy;
    }

    private DbValue[] CombineProbeAndBuildRows(DbValue[] probeRow, DbValue[] buildRow)
    {
        // Output row layout is always [left | right] regardless of build side.
        return _buildRightSide
            ? CombineRows(probeRow, buildRow)
            : CombineRows(buildRow, probeRow);
    }

    private static DbValue ExtractSingleJoinKey(DbValue[] row, int keyIndex)
    {
        return keyIndex < row.Length ? row[keyIndex] : DbValue.Null;
    }

    private static HashJoinKey ExtractJoinKey(DbValue[] row, int[] keyIndices)
    {
        var values = new DbValue[keyIndices.Length];
        for (int i = 0; i < keyIndices.Length; i++)
        {
            int keyIndex = keyIndices[i];
            values[i] = keyIndex < row.Length ? row[keyIndex] : DbValue.Null;
        }

        return new HashJoinKey(values);
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }

    private readonly struct HashJoinKey
    {
        public readonly DbValue[] Values;

        public HashJoinKey(DbValue[] values)
        {
            Values = values;
        }
    }

    private sealed class HashJoinKeyComparer : IEqualityComparer<HashJoinKey>
    {
        public static readonly HashJoinKeyComparer Instance = new();

        public bool Equals(HashJoinKey x, HashJoinKey y)
        {
            if (ReferenceEquals(x.Values, y.Values))
                return true;

            if (x.Values.Length != y.Values.Length)
                return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }

            return true;
        }

        public int GetHashCode(HashJoinKey obj)
        {
            var hash = new HashCode();
            for (int i = 0; i < obj.Values.Length; i++)
                hash.Add(obj.Values[i]);
            return hash.ToHashCode();
        }
    }
}

/// <summary>
/// Index nested-loop join operator.
/// Uses a right-side PRIMARY KEY or unique single-column index for lookup joins.
/// Supports INNER and LEFT OUTER joins.
/// </summary>
public sealed class IndexNestedLoopJoinOperator : IOperator
{
    private readonly IOperator _outer;
    private readonly BTree _innerTableTree;
    private readonly IIndexStore? _innerIndexStore;
    private readonly JoinType _joinType;
    private readonly int _outerKeyIndex;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly Func<DbValue[], DbValue>? _residualPredicate;
    private readonly IRecordSerializer _recordSerializer;
    private DbValue[]? _activeOuterRow;
    private bool _activeOuterMatched;
    private bool _pendingPrimaryRowId;
    private long _primaryRowId;
    private ReadOnlyMemory<byte> _pendingIndexPayload;
    private int _pendingIndexOffset;
    private DbValue[]? _rightRowBuffer;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public IndexNestedLoopJoinOperator(
        IOperator outer,
        BTree innerTableTree,
        IIndexStore? innerIndexStore,
        JoinType joinType,
        int outerKeyIndex,
        int leftColCount,
        int rightColCount,
        Expression? residualCondition,
        TableSchema compositeSchema,
        IRecordSerializer? recordSerializer = null)
    {
        _outer = outer;
        _innerTableTree = innerTableTree;
        _innerIndexStore = innerIndexStore;
        _joinType = joinType;
        _outerKeyIndex = outerKeyIndex;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _residualPredicate = residualCondition != null
            ? ExpressionCompiler.Compile(residualCondition, compositeSchema)
            : null;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_outer is IRowBufferReuseController outerController)
            outerController.SetReuseCurrentRowBuffer(false);

        await _outer.OpenAsync(ct);
        Current = Array.Empty<DbValue>();
        _rightRowBuffer = _rightColCount == 0 ? Array.Empty<DbValue>() : new DbValue[_rightColCount];
        ResetActiveOuterState();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_activeOuterRow != null)
            {
                while (TryReadPendingRowId(out long rowId))
                {
                    var payload = await _innerTableTree.FindAsync(rowId, ct);
                    if (payload == null)
                        continue;

                    var rightRow = DecodeRightRowIntoBuffer(payload);
                    var combined = CombineRows(_activeOuterRow, rightRow);
                    if (!PassesResidual(combined))
                        continue;

                    _activeOuterMatched = true;
                    Current = combined;
                    return true;
                }

                if (!_activeOuterMatched && _joinType == JoinType.LeftOuter)
                {
                    Current = CombineWithNulls(_activeOuterRow, _rightColCount, padRight: true);
                    ResetActiveOuterState();
                    return true;
                }

                ResetActiveOuterState();
                continue;
            }

            if (!await _outer.MoveNextAsync(ct))
                return false;

            var outerRow = _outer.Current;
            var keyValue = _outerKeyIndex < outerRow.Length ? outerRow[_outerKeyIndex] : DbValue.Null;
            if (!TryConvertLookupKey(keyValue, out long lookupKey))
            {
                if (_joinType == JoinType.LeftOuter)
                {
                    Current = CombineWithNulls(outerRow, _rightColCount, padRight: true);
                    return true;
                }

                continue;
            }

            _activeOuterRow = outerRow;
            _activeOuterMatched = false;

            if (_innerIndexStore == null)
            {
                _pendingPrimaryRowId = true;
                _primaryRowId = lookupKey;
                _pendingIndexPayload = ReadOnlyMemory<byte>.Empty;
                _pendingIndexOffset = 0;
            }
            else
            {
                var indexPayload = await _innerIndexStore.FindAsync(lookupKey, ct);
                _pendingPrimaryRowId = false;
                _pendingIndexPayload = indexPayload == null
                    ? ReadOnlyMemory<byte>.Empty
                    : new ReadOnlyMemory<byte>(indexPayload);
                _pendingIndexOffset = 0;
            }
        }
    }

    public ValueTask DisposeAsync() => _outer.DisposeAsync();

    private static bool TryConvertLookupKey(DbValue value, out long key)
    {
        key = 0;

        if (value.IsNull)
            return false;

        if (value.Type == DbType.Integer)
        {
            key = value.AsInteger;
            return true;
        }

        if (value.Type == DbType.Real)
        {
            double real = value.AsReal;
            if (real < long.MinValue || real > long.MaxValue)
                return false;

            double truncated = Math.Truncate(real);
            if (truncated != real)
                return false;

            key = (long)real;
            return true;
        }

        return false;
    }

    private bool PassesResidual(DbValue[] row)
    {
        return _residualPredicate == null || _residualPredicate(row).IsTruthy;
    }

    private bool TryReadPendingRowId(out long rowId)
    {
        if (_pendingPrimaryRowId)
        {
            _pendingPrimaryRowId = false;
            rowId = _primaryRowId;
            return true;
        }

        if (_pendingIndexOffset + 8 <= _pendingIndexPayload.Length)
        {
            rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _pendingIndexPayload.Span.Slice(_pendingIndexOffset, 8));
            _pendingIndexOffset += 8;
            return true;
        }

        rowId = 0;
        return false;
    }

    private void ResetActiveOuterState()
    {
        _activeOuterRow = null;
        _activeOuterMatched = false;
        _pendingPrimaryRowId = false;
        _primaryRowId = 0;
        _pendingIndexPayload = ReadOnlyMemory<byte>.Empty;
        _pendingIndexOffset = 0;
    }

    private DbValue[] DecodeRightRowIntoBuffer(byte[] payload)
    {
        var row = _rightRowBuffer ??= _rightColCount == 0
            ? Array.Empty<DbValue>()
            : new DbValue[_rightColCount];

        int decoded = _recordSerializer.DecodeInto(payload, row);
        if (decoded < _rightColCount)
            Array.Fill(row, DbValue.Null, decoded, _rightColCount - decoded);
        return row;
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }
}

/// <summary>
/// Nested-loop join operator — materializes both sides and computes the join.
/// Supports INNER, LEFT OUTER, RIGHT OUTER, and CROSS joins.
/// </summary>
public sealed class NestedLoopJoinOperator : IOperator
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly JoinType _joinType;
    private readonly Expression? _condition;
    private readonly TableSchema _compositeSchema;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private List<DbValue[]>? _results;
    private int _index;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public NestedLoopJoinOperator(
        IOperator left, IOperator right,
        JoinType joinType, Expression? condition,
        TableSchema compositeSchema,
        int leftColCount, int rightColCount)
    {
        _left = left;
        _right = right;
        _joinType = joinType;
        _condition = condition;
        _compositeSchema = compositeSchema;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_left is IRowBufferReuseController leftController)
            leftController.SetReuseCurrentRowBuffer(false);
        if (_right is IRowBufferReuseController rightController)
            rightController.SetReuseCurrentRowBuffer(false);

        await _left.OpenAsync(ct);
        await _right.OpenAsync(ct);

        // Materialize both sides
        var leftRows = new List<DbValue[]>();
        bool cloneLeftRows = _left.ReusesCurrentRowBuffer;
        while (await _left.MoveNextAsync(ct))
            leftRows.Add(cloneLeftRows ? (DbValue[])_left.Current.Clone() : _left.Current);

        var rightRows = new List<DbValue[]>();
        bool cloneRightRows = _right.ReusesCurrentRowBuffer;
        while (await _right.MoveNextAsync(ct))
            rightRows.Add(cloneRightRows ? (DbValue[])_right.Current.Clone() : _right.Current);

        _results = ComputeJoin(leftRows, rightRows);
        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_index >= _results!.Count) return ValueTask.FromResult(false);
        Current = _results[_index];
        return ValueTask.FromResult(true);
    }

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync();
        await _right.DisposeAsync();
    }

    private List<DbValue[]> ComputeJoin(List<DbValue[]> leftRows, List<DbValue[]> rightRows)
    {
        var results = new List<DbValue[]>();

        switch (_joinType)
        {
            case JoinType.Cross:
                foreach (var left in leftRows)
                    foreach (var right in rightRows)
                        results.Add(CombineRows(left, right));
                break;

            case JoinType.Inner:
                foreach (var left in leftRows)
                {
                    foreach (var right in rightRows)
                    {
                        var combined = CombineRows(left, right);
                        if (_condition == null || ExpressionEvaluator.Evaluate(_condition, combined, _compositeSchema).IsTruthy)
                            results.Add(combined);
                    }
                }
                break;

            case JoinType.LeftOuter:
                foreach (var left in leftRows)
                {
                    bool matched = false;
                    foreach (var right in rightRows)
                    {
                        var combined = CombineRows(left, right);
                        if (_condition == null || ExpressionEvaluator.Evaluate(_condition, combined, _compositeSchema).IsTruthy)
                        {
                            results.Add(combined);
                            matched = true;
                        }
                    }
                    if (!matched)
                        results.Add(CombineWithNulls(left, _rightColCount, padRight: true));
                }
                break;

            case JoinType.RightOuter:
                var rightMatched = new bool[rightRows.Count];
                foreach (var left in leftRows)
                {
                    for (int ri = 0; ri < rightRows.Count; ri++)
                    {
                        var combined = CombineRows(left, rightRows[ri]);
                        if (_condition == null || ExpressionEvaluator.Evaluate(_condition, combined, _compositeSchema).IsTruthy)
                        {
                            results.Add(combined);
                            rightMatched[ri] = true;
                        }
                    }
                }
                // Emit unmatched right rows with NULLs on the left
                for (int ri = 0; ri < rightRows.Count; ri++)
                {
                    if (!rightMatched[ri])
                        results.Add(CombineWithNulls(rightRows[ri], _leftColCount, padRight: false));
                }
                break;
        }

        return results;
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }
}

/// <summary>
/// Index scan operator — uses an index B+tree for equality lookups.
/// The index stores: key = indexed column value, payload = list of rowids (each 8 bytes).
/// For each matching rowid, looks up the actual row in the table's B+tree.
/// </summary>
public sealed class IndexScanOperator : IOperator, IRowBufferReuseController, IPreDecodeFilterSupport
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekValue;
    private readonly IRecordSerializer _recordSerializer;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private int? _maxDecodedColumnIndex;
    private int _preDecodeFilterColumnIndex;
    private BinaryOp _preDecodeFilterOp;
    private DbValue _preDecodeFilterLiteral;
    private byte[]? _preDecodeFilterTextBytes;
    private bool _hasPreDecodeFilter;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;
    internal long SeekValue => _seekValue;

    public IndexScanOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        long seekValue,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _seekValue = seekValue;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        _preDecodeFilterColumnIndex = columnIndex;
        _preDecodeFilterOp = op;
        _preDecodeFilterLiteral = literal;
        _preDecodeFilterTextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedPayload))
        {
            _rowIdPayload = cachedPayload ?? ReadOnlyMemory<byte>.Empty;
            _rowIdPayloadOffset = 0;
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }

        return OpenUncachedAsync(ct);
    }

    private async ValueTask OpenUncachedAsync(CancellationToken ct)
    {
        var payload = await _indexStore.FindAsync(_seekValue, ct);
        _rowIdPayload = payload ?? ReadOnlyMemory<byte>.Empty;
        _rowIdPayloadOffset = 0;

        _rowBuffer = null;
        Current = Array.Empty<DbValue>();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_rowIdPayloadOffset + 8 > _rowIdPayload.Length)
                return ValueTask.FromResult(false);

            long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, 8));
            _rowIdPayloadOffset += 8;
            CurrentRowId = rowId;

            if (_tableTree.TryFindCached(rowId, out var cachedPayload))
            {
                if (cachedPayload == null)
                    continue; // deleted row

                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(cachedPayload))
                    continue;

                PopulateCurrentFromPayload(cachedPayload);
                return ValueTask.FromResult(true);
            }

            return MoveNextUncachedAsync(rowId, ct);
        }
    }

    private async ValueTask<bool> MoveNextUncachedAsync(long rowId, CancellationToken ct)
    {
        while (true)
        {
            var payload = await _tableTree.FindAsync(rowId, ct);
            if (payload != null &&
                (!_hasPreDecodeFilter || EvaluatePreDecodeFilter(payload)))
            {
                PopulateCurrentFromPayload(payload);
                return true;
            }

            if (_rowIdPayloadOffset + 8 > _rowIdPayload.Length)
                return false;

            rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, 8));
            _rowIdPayloadOffset += 8;
            CurrentRowId = rowId;

            if (_tableTree.TryFindCached(rowId, out var cachedPayload))
            {
                if (cachedPayload == null)
                    continue;

                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(cachedPayload))
                    continue;

                PopulateCurrentFromPayload(cachedPayload);
                return true;
            }
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private void PopulateCurrentFromPayload(ReadOnlySpan<byte> payload)
    {
        int targetColumnCount = _maxDecodedColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
            : _schema.Columns.Count;

        if (_reuseCurrentRowBuffer)
        {
            EnsureRowBuffer(targetColumnCount);
            int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
            if (decodedCount < targetColumnCount)
                Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

            Current = _rowBuffer!;
        }
        else
        {
            var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
            int decodedCount = _recordSerializer.DecodeInto(payload, row);
            if (decodedCount < targetColumnCount)
                Array.Fill(row, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

            Current = row;
        }
    }

    private bool EvaluatePreDecodeFilter(DbValue value)
    {
        int cmp = DbValue.Compare(value, _preDecodeFilterLiteral);
        return _preDecodeFilterOp switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        var textBytes = _preDecodeFilterTextBytes;
        if (textBytes != null &&
            _recordSerializer.TryColumnTextEquals(payload, _preDecodeFilterColumnIndex, textBytes, out bool textEquals))
        {
            return _preDecodeFilterOp == BinaryOp.Equals ? textEquals : !textEquals;
        }

        var filterValue = _recordSerializer.DecodeColumn(payload, _preDecodeFilterColumnIndex);
        return EvaluatePreDecodeFilter(filterValue);
    }
}

/// <summary>
/// Unique-index lookup operator — performs a direct secondary-index equality lookup
/// and resolves exactly one rowid from the index payload.
/// </summary>
public sealed class UniqueIndexLookupOperator : IOperator, IPreDecodeFilterSupport
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekValue;
    private readonly IRecordSerializer _recordSerializer;
    private bool _consumed;
    private int? _maxDecodedColumnIndex;
    private int _preDecodeFilterColumnIndex;
    private BinaryOp _preDecodeFilterOp;
    private DbValue _preDecodeFilterLiteral;
    private byte[]? _preDecodeFilterTextBytes;
    private bool _hasPreDecodeFilter;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;
    internal long SeekValue => _seekValue;

    public UniqueIndexLookupOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        long seekValue,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _seekValue = seekValue;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        _preDecodeFilterColumnIndex = columnIndex;
        _preDecodeFilterOp = op;
        _preDecodeFilterLiteral = literal;
        _preDecodeFilterTextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return false;
        _consumed = true;

        byte[]? indexPayload;
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedIndexPayload))
        {
            indexPayload = cachedIndexPayload;
        }
        else
        {
            indexPayload = await _indexStore.FindAsync(_seekValue, ct);
        }

        if (indexPayload == null || indexPayload.Length < 8)
            return false;

        long rowId = BinaryPrimitives.ReadInt64LittleEndian(indexPayload.AsSpan(0, 8));
        byte[]? rowPayload;
        if (_tableTree.TryFindCached(rowId, out var cachedRowPayload))
        {
            rowPayload = cachedRowPayload;
        }
        else
        {
            rowPayload = await _tableTree.FindAsync(rowId, ct);
        }

        if (rowPayload == null)
            return false;

        if (_hasPreDecodeFilter)
        {
            if (!EvaluatePreDecodeFilter(rowPayload))
                return false;
        }

        CurrentRowId = rowId;
        var decoded = _maxDecodedColumnIndex.HasValue
            ? _recordSerializer.DecodeUpTo(rowPayload, _maxDecodedColumnIndex.Value)
            : _recordSerializer.Decode(rowPayload);

        if (!_maxDecodedColumnIndex.HasValue && decoded.Length < _schema.Columns.Count)
        {
            var padded = new DbValue[_schema.Columns.Count];
            decoded.CopyTo(padded, 0);
            for (int i = decoded.Length; i < padded.Length; i++)
                padded[i] = DbValue.Null;
            Current = padded;
        }
        else
        {
            Current = decoded;
        }

        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private bool EvaluatePreDecodeFilter(DbValue value)
    {
        int cmp = DbValue.Compare(value, _preDecodeFilterLiteral);
        return _preDecodeFilterOp switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        var textBytes = _preDecodeFilterTextBytes;
        if (textBytes != null &&
            _recordSerializer.TryColumnTextEquals(payload, _preDecodeFilterColumnIndex, textBytes, out bool textEquals))
        {
            return _preDecodeFilterOp == BinaryOp.Equals ? textEquals : !textEquals;
        }

        var filterValue = _recordSerializer.DecodeColumn(payload, _preDecodeFilterColumnIndex);
        return EvaluatePreDecodeFilter(filterValue);
    }
}

/// <summary>
/// Ordered index scan operator — walks an index B+tree in key order and fetches table rows by rowid.
/// Used to satisfy ORDER BY on indexed INTEGER columns without a Sort operator.
/// </summary>
public sealed class IndexOrderedScanOperator : IOperator, IRowBufferReuseController, IPreDecodeFilterSupport
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly IndexScanRange _scanRange;
    private readonly IRecordSerializer _recordSerializer;
    private IIndexCursor? _cursor;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private int? _maxDecodedColumnIndex;
    private int _preDecodeFilterColumnIndex;
    private BinaryOp _preDecodeFilterOp;
    private DbValue _preDecodeFilterLiteral;
    private byte[]? _preDecodeFilterTextBytes;
    private bool _hasPreDecodeFilter;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }

    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;

    public IndexOrderedScanOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        IndexScanRange scanRange,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _scanRange = scanRange;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    /// <summary>
    /// Hint the scan to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        _preDecodeFilterColumnIndex = columnIndex;
        _preDecodeFilterOp = op;
        _preDecodeFilterLiteral = literal;
        _preDecodeFilterTextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _indexStore.CreateCursor(_scanRange);
        _rowIdPayload = ReadOnlyMemory<byte>.Empty;
        _rowIdPayloadOffset = 0;
        _rowBuffer = null;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null) return false;

        while (true)
        {
            if (_rowIdPayloadOffset + 8 <= _rowIdPayload.Length)
            {
                long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                    _rowIdPayload.Span.Slice(_rowIdPayloadOffset, 8));
                _rowIdPayloadOffset += 8;

                var payload = await _tableTree.FindAsync(rowId, ct);
                if (payload == null)
                    continue; // Skip deleted rows

                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                    continue;

                CurrentRowId = rowId;

                int targetColumnCount = _maxDecodedColumnIndex.HasValue
                    ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
                    : _schema.Columns.Count;

                if (_reuseCurrentRowBuffer)
                {
                    EnsureRowBuffer(targetColumnCount);
                    int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
                    if (decodedCount < targetColumnCount)
                        Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);
                    Current = _rowBuffer!;
                }
                else
                {
                    var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
                    int decodedCount = _recordSerializer.DecodeInto(payload, row);
                    if (decodedCount < targetColumnCount)
                        Array.Fill(row, DbValue.Null, decodedCount, targetColumnCount - decodedCount);
                    Current = row;
                }

                return true;
            }

            if (!await _cursor.MoveNextAsync(ct))
                return false;

            _rowIdPayload = _cursor.CurrentValue;
            _rowIdPayloadOffset = 0;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private bool EvaluatePreDecodeFilter(DbValue value)
    {
        int cmp = DbValue.Compare(value, _preDecodeFilterLiteral);
        return _preDecodeFilterOp switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        var textBytes = _preDecodeFilterTextBytes;
        if (textBytes != null &&
            _recordSerializer.TryColumnTextEquals(payload, _preDecodeFilterColumnIndex, textBytes, out bool textEquals))
        {
            return _preDecodeFilterOp == BinaryOp.Equals ? textEquals : !textEquals;
        }

        var filterValue = _recordSerializer.DecodeColumn(payload, _preDecodeFilterColumnIndex);
        return EvaluatePreDecodeFilter(filterValue);
    }
}

/// <summary>
/// Primary-key lookup operator — performs a direct B+tree key lookup against the table.
/// </summary>
public sealed class PrimaryKeyLookupOperator : IOperator, IPreDecodeFilterSupport, IRowBufferReuseController
{
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekKey;
    private readonly IRecordSerializer _recordSerializer;
    private bool _consumed;
    private int? _maxDecodedColumnIndex;
    private int _preDecodeFilterColumnIndex;
    private BinaryOp _preDecodeFilterOp;
    private DbValue _preDecodeFilterLiteral;
    private byte[]? _preDecodeFilterTextBytes;
    private bool _hasPreDecodeFilter;
    private DbValue[]? _rowBuffer;
    private bool _reuseBuffer = true;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    internal BTree TableTree => _tableTree;
    internal long SeekKey => _seekKey;

    public PrimaryKeyLookupOperator(
        BTree tableTree,
        TableSchema schema,
        long seekKey,
        IRecordSerializer? recordSerializer = null)
    {
        _tableTree = tableTree;
        _schema = schema;
        _seekKey = seekKey;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseBuffer = reuse;
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        _preDecodeFilterColumnIndex = columnIndex;
        _preDecodeFilterOp = op;
        _preDecodeFilterLiteral = literal;
        _preDecodeFilterTextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return ValueTask.FromResult(false);
        _consumed = true;

        if (_tableTree.TryFindCached(_seekKey, out var cachedPayload))
            return ValueTask.FromResult(EmitFromPayload(cachedPayload));

        return MoveNextUncachedAsync(ct);
    }

    private async ValueTask<bool> MoveNextUncachedAsync(CancellationToken ct)
    {
        var payload = await _tableTree.FindAsync(_seekKey, ct);
        return EmitFromPayload(payload);
    }

    private bool EmitFromPayload(byte[]? payload)
    {
        if (payload == null)
            return false;

        if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
            return false;

        CurrentRowId = _seekKey;

        if (_reuseBuffer)
        {
            int targetCount = _maxDecodedColumnIndex.HasValue
                ? _maxDecodedColumnIndex.Value + 1
                : _schema.Columns.Count;
            if (_rowBuffer == null || _rowBuffer.Length < targetCount)
                _rowBuffer = new DbValue[targetCount];
            int decoded = _recordSerializer.DecodeInto(payload, _rowBuffer.AsSpan(0, targetCount));
            for (int i = decoded; i < targetCount; i++)
                _rowBuffer[i] = DbValue.Null;
            Current = _rowBuffer;
        }
        else
        {
            var decoded = _maxDecodedColumnIndex.HasValue
                ? _recordSerializer.DecodeUpTo(payload, _maxDecodedColumnIndex.Value)
                : _recordSerializer.Decode(payload);

            if (!_maxDecodedColumnIndex.HasValue && decoded.Length < _schema.Columns.Count)
            {
                var padded = new DbValue[_schema.Columns.Count];
                decoded.CopyTo(padded, 0);
                for (int i = decoded.Length; i < padded.Length; i++)
                    padded[i] = DbValue.Null;
                Current = padded;
            }
            else
            {
                Current = decoded;
            }
        }

        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private bool EvaluatePreDecodeFilter(DbValue value)
    {
        int cmp = DbValue.Compare(value, _preDecodeFilterLiteral);
        return _preDecodeFilterOp switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        var textBytes = _preDecodeFilterTextBytes;
        if (textBytes != null &&
            _recordSerializer.TryColumnTextEquals(payload, _preDecodeFilterColumnIndex, textBytes, out bool textEquals))
        {
            return _preDecodeFilterOp == BinaryOp.Equals ? textEquals : !textEquals;
        }

        var filterValue = _recordSerializer.DecodeColumn(payload, _preDecodeFilterColumnIndex);
        return EvaluatePreDecodeFilter(filterValue);
    }
}

/// <summary>
/// Primary-key lookup projection fast path.
/// Verifies row existence via table key lookup and returns one row where every projected value is the PK key.
/// </summary>
public sealed class PrimaryKeyProjectionLookupOperator : IOperator
{
    private readonly BTree _tableTree;
    private readonly long _seekKey;
    private readonly DbValue[] _projectedRow;
    private bool _consumed;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public PrimaryKeyProjectionLookupOperator(BTree tableTree, long seekKey, ColumnDefinition[] outputSchema)
    {
        _tableTree = tableTree;
        _seekKey = seekKey;
        OutputSchema = outputSchema;

        if (outputSchema.Length == 0)
        {
            _projectedRow = Array.Empty<DbValue>();
        }
        else
        {
            _projectedRow = new DbValue[outputSchema.Length];
            var keyValue = DbValue.FromInteger(seekKey);
            for (int i = 0; i < _projectedRow.Length; i++)
                _projectedRow[i] = keyValue;
        }
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return ValueTask.FromResult(false);
        _consumed = true;

        if (_tableTree.TryFindCached(_seekKey, out var cachedPayload))
        {
            if (cachedPayload == null)
                return ValueTask.FromResult(false);

            Current = _projectedRow;
            return ValueTask.FromResult(true);
        }

        return MoveNextUncachedAsync(ct);
    }

    private async ValueTask<bool> MoveNextUncachedAsync(CancellationToken ct)
    {
        // Preserve semantics by checking that the row actually exists.
        var payload = await _tableTree.FindAsync(_seekKey, ct);
        if (payload == null)
            return false;

        Current = _projectedRow;
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Scalar SUM/AVG/COUNT/MIN/MAX fast path for point/range lookups from PK or a single-column index equality lookup.
/// Avoids generic operator-pipeline overhead by aggregating directly on payloads.
/// </summary>
public sealed class ScalarAggregateLookupOperator : IOperator
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private enum LookupKind
    {
        PrimaryKey,
        IndexEquality,
    }

    private readonly LookupKind _lookupKind;
    private readonly BTree _tableTree;
    private readonly IIndexStore? _indexStore;
    private readonly long _lookupValue;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly IRecordSerializer _recordSerializer;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public ScalarAggregateLookupOperator(
        BTree tableTree,
        long seekKey,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        IRecordSerializer? recordSerializer = null)
    {
        _lookupKind = LookupKind.PrimaryKey;
        _tableTree = tableTree;
        _indexStore = null;
        _lookupValue = seekKey;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _kind = ParseKind(functionName);
        OutputSchema = outputSchema;
    }

    public ScalarAggregateLookupOperator(
        IIndexStore indexStore,
        BTree tableTree,
        long seekValue,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        IRecordSerializer? recordSerializer = null)
    {
        _lookupKind = LookupKind.IndexEquality;
        _indexStore = indexStore;
        _tableTree = tableTree;
        _lookupValue = seekValue;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _kind = ParseKind(functionName);
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;

        void Accumulate(ReadOnlySpan<byte> payload)
        {
            if (_kind == AggregateKind.Count && distinctValues == null)
            {
                if (!_recordSerializer.IsColumnNull(payload, _columnIndex))
                    count++;
                return;
            }

            if ((_kind == AggregateKind.Sum || _kind == AggregateKind.Avg) && distinctValues == null)
            {
                if (!_recordSerializer.TryDecodeNumericColumn(payload, _columnIndex, out long intVal, out double realVal, out bool isReal))
                    return;

                hasAny = true;
                if (isReal)
                {
                    hasReal = true;
                    sum += realVal;
                }
                else
                {
                    sum += intVal;
                }
                count++;
                return;
            }

            var val = _recordSerializer.DecodeColumn(payload, _columnIndex);
            if (val.IsNull) return;
            if (distinctValues != null && !distinctValues.Add(val)) return;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (val.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += val.AsReal;
                    }
                    else
                    {
                        sum += val.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(val, best.Value) < 0)
                        best = val;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(val, best.Value) > 0)
                        best = val;
                    break;
            }
        }

        if (_lookupKind == LookupKind.PrimaryKey)
        {
            var payload = await _tableTree.FindAsync(_lookupValue, ct);
            if (payload != null)
                Accumulate(payload.AsSpan());
        }
        else
        {
            var indexPayload = await _indexStore!.FindAsync(_lookupValue, ct);
            if (indexPayload != null)
            {
                int rowIdCount = indexPayload.Length / 8;
                for (int i = 0; i < rowIdCount; i++)
                {
                    long rowId = BinaryPrimitives.ReadInt64LittleEndian(indexPayload.AsSpan(i * 8, 8));
                    var payload = await _tableTree.FindAsync(rowId, ct);
                    if (payload == null) continue;
                    Accumulate(payload.AsSpan());
                }
            }
        }

        DbValue aggregate = _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => best ?? DbValue.Null,
            AggregateKind.Max => best ?? DbValue.Null,
            _ => DbValue.Null,
        };

        Current = new[] { aggregate };
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted) return ValueTask.FromResult(false);
        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static AggregateKind ParseKind(string functionName)
    {
        return functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
    }
}

/// <summary>
/// Scalar SUM/AVG/COUNT(column) fast path for a single table with no filters/grouping.
/// Scans the table B+tree directly and decodes only the target column.
/// Produces exactly one row.
/// </summary>
public sealed class ScalarAggregateTableOperator : IOperator
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private readonly BTree _tableTree;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly bool _emitOnEmptyInput;
    private readonly IRecordSerializer _recordSerializer;
    private bool _emitted;
    private bool _hasResult;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public ScalarAggregateTableOperator(
        BTree tableTree,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        bool emitOnEmptyInput = true,
        IRecordSerializer? recordSerializer = null)
    {
        _tableTree = tableTree;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _emitOnEmptyInput = emitOnEmptyInput;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        var cursor = _tableTree.CreateCursor();

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        bool sawRow = false;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;

        while (await cursor.MoveNextAsync(ct))
        {
            sawRow = true;

            if (_kind == AggregateKind.Count && distinctValues == null)
            {
                if (!_recordSerializer.IsColumnNull(cursor.CurrentValue.Span, _columnIndex))
                    count++;
                continue;
            }

            if ((_kind == AggregateKind.Sum || _kind == AggregateKind.Avg) && distinctValues == null)
            {
                if (!_recordSerializer.TryDecodeNumericColumn(
                        cursor.CurrentValue.Span,
                        _columnIndex,
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    continue;
                }

                hasAny = true;
                if (isReal)
                {
                    hasReal = true;
                    sum += realVal;
                }
                else
                {
                    sum += intVal;
                }
                count++;
                continue;
            }

            var val = _recordSerializer.DecodeColumn(cursor.CurrentValue.Span, _columnIndex);
            if (val.IsNull) continue;
            if (distinctValues != null && !distinctValues.Add(val)) continue;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (val.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += val.AsReal;
                    }
                    else
                    {
                        sum += val.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(val, best.Value) < 0)
                        best = val;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(val, best.Value) > 0)
                        best = val;
                    break;
            }
        }

        DbValue aggregate = _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => best ?? DbValue.Null,
            AggregateKind.Max => best ?? DbValue.Null,
            _ => DbValue.Null,
        };

        _hasResult = _emitOnEmptyInput || sawRow;
        Current = _hasResult ? new[] { aggregate } : Array.Empty<DbValue>();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted || !_hasResult) return ValueTask.FromResult(false);
        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// COUNT(*) fast path for a single table with no filters.
/// Produces exactly one row with the table entry count.
/// </summary>
public sealed class CountStarTableOperator : IOperator
{
    private readonly BTree _tableTree;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public CountStarTableOperator(BTree tableTree, ColumnDefinition[] outputSchema)
    {
        _tableTree = tableTree;
        OutputSchema = outputSchema;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted) return false;
        _emitted = true;

        long count = await _tableTree.CountEntriesAsync(ct);
        Current = new[] { DbValue.FromInteger(count) };
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Yields pre-materialized rows. Used for CTEs whose results have been computed upfront.
/// </summary>
public sealed class MaterializedOperator : IOperator
{
    private readonly List<DbValue[]> _rows;
    private int _index;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public MaterializedOperator(List<DbValue[]> rows, ColumnDefinition[] outputSchema)
    {
        _rows = rows;
        OutputSchema = outputSchema;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _index = -1;
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_index >= _rows.Count) return ValueTask.FromResult(false);
        Current = _rows[_index];
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
