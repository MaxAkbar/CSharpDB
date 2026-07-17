using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Experimental in-memory window stage. The planner currently supplies one
/// compatible partition/order specification per stage.
/// </summary>
internal sealed class WindowOperator : IOperator, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private enum WindowFunctionKind
    {
        RowNumber,
        Rank,
        DenseRank,
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private sealed class RuntimeFunction
    {
        public required WindowFunctionKind Kind { get; init; }
        public SpanExpressionEvaluator? ArgumentEvaluator { get; init; }
        public string? ArgumentCollation { get; init; }
        public bool IsCountStar { get; init; }
    }

    private sealed class MaterializedRow
    {
        public required DbValue[] Values { get; init; }
        public required DbValue[] PartitionKeys { get; init; }
        public required DbValue[] OrderKeys { get; init; }
        public required int OriginalIndex { get; init; }
    }

    private sealed class AggregateState
    {
        private readonly WindowFunctionKind _kind;
        private readonly string? _collation;
        private long _count;
        private long _integerSum;
        private double _realSum;
        private bool _hasReal;
        private bool _hasValue;
        private DbValue _best = DbValue.Null;

        public AggregateState(WindowFunctionKind kind, string? collation)
        {
            _kind = kind;
            _collation = collation;
        }

        public void Accumulate(DbValue value, bool countStar)
        {
            if (_kind == WindowFunctionKind.Count)
            {
                if (countStar || !value.IsNull)
                    _count++;
                return;
            }

            if (value.IsNull)
                return;

            switch (_kind)
            {
                case WindowFunctionKind.Sum:
                case WindowFunctionKind.Avg:
                    if (value.Type is not (DbType.Integer or DbType.Real))
                    {
                        throw new CSharpDbException(
                            ErrorCode.TypeMismatch,
                            $"{_kind.ToString().ToUpperInvariant()} window argument must be numeric.");
                    }

                    if (value.Type == DbType.Real)
                    {
                        if (!_hasReal)
                        {
                            _realSum = _integerSum;
                            _hasReal = true;
                        }

                        _realSum += value.AsReal;
                    }
                    else if (_hasReal)
                    {
                        _realSum += value.AsInteger;
                    }
                    else
                    {
                        try
                        {
                            _integerSum = checked(_integerSum + value.AsInteger);
                        }
                        catch (OverflowException ex)
                        {
                            throw new CSharpDbException(
                                ErrorCode.TypeMismatch,
                                "Integer SUM window overflowed the supported 64-bit range.",
                                ex);
                        }
                    }

                    _count++;
                    _hasValue = true;
                    return;

                case WindowFunctionKind.Min:
                    if (!_hasValue || CollationSupport.Compare(value, _best, _collation) < 0)
                        _best = value;
                    _hasValue = true;
                    return;

                case WindowFunctionKind.Max:
                    if (!_hasValue || CollationSupport.Compare(value, _best, _collation) > 0)
                        _best = value;
                    _hasValue = true;
                    return;

                default:
                    throw new InvalidOperationException($"{_kind} is not an aggregate window function.");
            }
        }

        public DbValue GetValue()
        {
            return _kind switch
            {
                WindowFunctionKind.Count => DbValue.FromInteger(_count),
                WindowFunctionKind.Sum when !_hasValue => DbValue.Null,
                WindowFunctionKind.Sum when _hasReal => DbValue.FromReal(_realSum),
                WindowFunctionKind.Sum => DbValue.FromInteger(_integerSum),
                WindowFunctionKind.Avg when !_hasValue => DbValue.Null,
                WindowFunctionKind.Avg => DbValue.FromReal(
                    (_hasReal ? _realSum : _integerSum) / _count),
                WindowFunctionKind.Min or WindowFunctionKind.Max =>
                    _hasValue ? _best : DbValue.Null,
                _ => throw new InvalidOperationException($"{_kind} is not an aggregate window function."),
            };
        }
    }

    private readonly IOperator _source;
    private readonly TableSchema _inputSchema;
    private readonly SpanExpressionEvaluator[] _partitionEvaluators;
    private readonly SpanExpressionEvaluator[] _orderEvaluators;
    private readonly string?[] _partitionCollations;
    private readonly string?[] _orderCollations;
    private readonly int[] _orderDirections;
    private readonly RuntimeFunction[] _functions;
    private List<DbValue[]>? _results;
    private int _index;
    private bool _sourceDisposed;

    public WindowOperator(
        IOperator source,
        TableSchema inputSchema,
        IReadOnlyList<WindowFunctionExpression> windowFunctions,
        ColumnDefinition[] outputSchema,
        DbFunctionRegistry? functions = null)
    {
        if (windowFunctions.Count == 0)
            throw new ArgumentException("At least one window function is required.", nameof(windowFunctions));

        _source = source;
        _inputSchema = inputSchema;
        OutputSchema = outputSchema;

        WindowSpecification specification = windowFunctions[0].Window;
        _partitionEvaluators = specification.PartitionBy
            .Select(expression => ExpressionCompiler.CompileSpan(expression, inputSchema, functions))
            .ToArray();
        _orderEvaluators = specification.OrderBy
            .Select(clause => ExpressionCompiler.CompileSpan(clause.Expression, inputSchema, functions))
            .ToArray();
        _partitionCollations = specification.PartitionBy
            .Select(expression => CollationSupport.ResolveExpressionCollation(expression, inputSchema))
            .ToArray();
        _orderCollations = specification.OrderBy
            .Select(clause => CollationSupport.ResolveExpressionCollation(clause.Expression, inputSchema))
            .ToArray();
        _orderDirections = specification.OrderBy
            .Select(clause => clause.Descending ? -1 : 1)
            .ToArray();
        _functions = windowFunctions.Select(function => CompileFunction(function, inputSchema, functions)).ToArray();
    }

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _results?.Count;

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        _sourceDisposed = false;

        var rows = new List<MaterializedRow>();
        try
        {
            await _source.OpenAsync(ct);
            bool cloneRows = _source.ReusesCurrentRowBuffer;
            int originalIndex = 0;
            while (await _source.MoveNextAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
                DbValue[] values = cloneRows ? (DbValue[])_source.Current.Clone() : _source.Current;
                rows.Add(new MaterializedRow
                {
                    Values = values,
                    PartitionKeys = EvaluateKeys(_partitionEvaluators, values),
                    OrderKeys = EvaluateKeys(_orderEvaluators, values),
                    OriginalIndex = originalIndex++,
                });
            }

            ct.ThrowIfCancellationRequested();
            rows.Sort(CompareRows);
            ct.ThrowIfCancellationRequested();
            _results = EvaluateWindows(rows, ct);
        }
        catch
        {
            await DisposeSourceOnceAsync();
            throw;
        }
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _index++;
        if (_results == null || _index >= _results.Count)
        {
            Current = Array.Empty<DbValue>();
            return ValueTask.FromResult(false);
        }

        Current = _results[_index];
        return ValueTask.FromResult(true);
    }

    public async ValueTask DisposeAsync()
    {
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        await DisposeSourceOnceAsync();
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_results == null)
        {
            rows = [];
            return false;
        }

        rows = _results;
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return true;
    }

    private List<DbValue[]> EvaluateWindows(List<MaterializedRow> rows, CancellationToken ct)
    {
        var results = new List<DbValue[]>(rows.Count);
        for (int i = 0; i < rows.Count; i++)
            results.Add(new DbValue[OutputSchema.Length]);

        int partitionStart = 0;
        while (partitionStart < rows.Count)
        {
            ct.ThrowIfCancellationRequested();
            int partitionEnd = partitionStart + 1;
            while (partitionEnd < rows.Count &&
                   KeysEqual(
                       rows[partitionStart].PartitionKeys,
                       rows[partitionEnd].PartitionKeys,
                       _partitionCollations))
            {
                partitionEnd++;
            }

            EvaluatePartition(rows, results, partitionStart, partitionEnd, ct);
            partitionStart = partitionEnd;
        }

        return results;
    }

    private void EvaluatePartition(
        List<MaterializedRow> rows,
        List<DbValue[]> results,
        int partitionStart,
        int partitionEnd,
        CancellationToken ct)
    {
        var aggregateStates = new AggregateState?[_functions.Length];
        for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
        {
            RuntimeFunction function = _functions[functionIndex];
            if (IsAggregate(function.Kind))
                aggregateStates[functionIndex] = new AggregateState(function.Kind, function.ArgumentCollation);
        }

        if (_orderEvaluators.Length == 0)
        {
            for (int rowIndex = partitionStart; rowIndex < partitionEnd; rowIndex++)
            {
                ct.ThrowIfCancellationRequested();
                AccumulateAggregates(rows[rowIndex].Values, aggregateStates);
            }

            DbValue[] aggregateValues = SnapshotAggregates(aggregateStates);
            for (int rowIndex = partitionStart; rowIndex < partitionEnd; rowIndex++)
            {
                WriteResultRow(
                    rows[rowIndex].Values,
                    results[rowIndex],
                    rowNumber: rowIndex - partitionStart + 1,
                    rank: 1,
                    denseRank: 1,
                    aggregateValues);
            }

            return;
        }

        int peerStart = partitionStart;
        long denseRank = 0;
        while (peerStart < partitionEnd)
        {
            ct.ThrowIfCancellationRequested();
            int peerEnd = peerStart + 1;
            while (peerEnd < partitionEnd &&
                   KeysEqual(rows[peerStart].OrderKeys, rows[peerEnd].OrderKeys, _orderCollations))
            {
                peerEnd++;
            }

            denseRank++;
            for (int rowIndex = peerStart; rowIndex < peerEnd; rowIndex++)
                AccumulateAggregates(rows[rowIndex].Values, aggregateStates);

            DbValue[] aggregateValues = SnapshotAggregates(aggregateStates);
            long rank = peerStart - partitionStart + 1L;
            for (int rowIndex = peerStart; rowIndex < peerEnd; rowIndex++)
            {
                WriteResultRow(
                    rows[rowIndex].Values,
                    results[rowIndex],
                    rowNumber: rowIndex - partitionStart + 1L,
                    rank,
                    denseRank,
                    aggregateValues);
            }

            peerStart = peerEnd;
        }
    }

    private void AccumulateAggregates(DbValue[] row, AggregateState?[] states)
    {
        for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
        {
            AggregateState? state = states[functionIndex];
            if (state == null)
                continue;

            RuntimeFunction function = _functions[functionIndex];
            DbValue value = function.IsCountStar
                ? DbValue.Null
                : function.ArgumentEvaluator!(row);
            state.Accumulate(value, function.IsCountStar);
        }
    }

    private static DbValue[] SnapshotAggregates(AggregateState?[] states)
    {
        var values = new DbValue[states.Length];
        for (int i = 0; i < states.Length; i++)
            values[i] = states[i]?.GetValue() ?? DbValue.Null;
        return values;
    }

    private void WriteResultRow(
        DbValue[] source,
        DbValue[] destination,
        long rowNumber,
        long rank,
        long denseRank,
        DbValue[] aggregateValues)
    {
        if (source.Length != _inputSchema.Columns.Count)
        {
            throw new InvalidOperationException(
                "Window evaluation requires full-width source rows.");
        }

        source.CopyTo(destination, 0);
        int outputOffset = _inputSchema.Columns.Count;
        for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
        {
            destination[outputOffset + functionIndex] = _functions[functionIndex].Kind switch
            {
                WindowFunctionKind.RowNumber => DbValue.FromInteger(rowNumber),
                WindowFunctionKind.Rank => DbValue.FromInteger(rank),
                WindowFunctionKind.DenseRank => DbValue.FromInteger(denseRank),
                _ => aggregateValues[functionIndex],
            };
        }
    }

    private int CompareRows(MaterializedRow left, MaterializedRow right)
    {
        for (int i = 0; i < left.PartitionKeys.Length; i++)
        {
            int comparison = CollationSupport.Compare(
                left.PartitionKeys[i],
                right.PartitionKeys[i],
                _partitionCollations[i]);
            if (comparison != 0)
                return comparison;
        }

        for (int i = 0; i < left.OrderKeys.Length; i++)
        {
            int comparison = CollationSupport.Compare(
                left.OrderKeys[i],
                right.OrderKeys[i],
                _orderCollations[i]);
            if (comparison != 0)
                return comparison * _orderDirections[i];
        }

        return left.OriginalIndex.CompareTo(right.OriginalIndex);
    }

    private static bool KeysEqual(DbValue[] left, DbValue[] right, string?[] collations)
    {
        for (int i = 0; i < left.Length; i++)
        {
            if (CollationSupport.Compare(left[i], right[i], collations[i]) != 0)
                return false;
        }

        return true;
    }

    private static DbValue[] EvaluateKeys(SpanExpressionEvaluator[] evaluators, DbValue[] row)
    {
        var keys = new DbValue[evaluators.Length];
        for (int i = 0; i < evaluators.Length; i++)
            keys[i] = evaluators[i](row);
        return keys;
    }

    private static RuntimeFunction CompileFunction(
        WindowFunctionExpression expression,
        TableSchema schema,
        DbFunctionRegistry? functions)
    {
        FunctionCallExpression function = expression.Function;
        WindowFunctionKind kind = function.FunctionName.ToUpperInvariant() switch
        {
            "ROW_NUMBER" => WindowFunctionKind.RowNumber,
            "RANK" => WindowFunctionKind.Rank,
            "DENSE_RANK" => WindowFunctionKind.DenseRank,
            "COUNT" => WindowFunctionKind.Count,
            "SUM" => WindowFunctionKind.Sum,
            "AVG" => WindowFunctionKind.Avg,
            "MIN" => WindowFunctionKind.Min,
            "MAX" => WindowFunctionKind.Max,
            _ => throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Window function '{function.FunctionName}' is not supported in the experimental window-function tier."),
        };

        Expression? argument = function.Arguments.Count == 1 ? function.Arguments[0] : null;
        return new RuntimeFunction
        {
            Kind = kind,
            IsCountStar = kind == WindowFunctionKind.Count && function.IsStarArg,
            ArgumentEvaluator = argument == null
                ? null
                : ExpressionCompiler.CompileSpan(argument, schema, functions),
            ArgumentCollation = argument == null
                ? null
                : CollationSupport.ResolveExpressionCollation(argument, schema),
        };
    }

    private static bool IsAggregate(WindowFunctionKind kind) =>
        kind is WindowFunctionKind.Count
            or WindowFunctionKind.Sum
            or WindowFunctionKind.Avg
            or WindowFunctionKind.Min
            or WindowFunctionKind.Max;

    private async ValueTask DisposeSourceOnceAsync()
    {
        if (_sourceDisposed)
            return;

        _sourceDisposed = true;
        await _source.DisposeAsync();
    }
}
