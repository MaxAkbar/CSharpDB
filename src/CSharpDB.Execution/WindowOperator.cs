using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Bounded in-memory window stage. The planner supplies one compatible
/// partition/order specification per stage.
/// </summary>
internal sealed class WindowOperator :
    IOperator,
    IEstimatedRowCountProvider,
    IMaterializedRowsProvider,
    IPhysicalOperatorChildren
{
    private const int CancellationCheckInterval = 1024;

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
        Lag,
        Lead,
        FirstValue,
        LastValue,
    }

    private sealed class RuntimeFunction
    {
        public required WindowFunctionKind Kind { get; init; }
        public SpanExpressionEvaluator? ValueEvaluator { get; init; }
        public SpanExpressionEvaluator? OffsetEvaluator { get; init; }
        public SpanExpressionEvaluator? DefaultEvaluator { get; init; }
        public string? ArgumentCollation { get; init; }
        public bool IsCountStar { get; init; }
        public WindowFrame? Frame { get; init; }
    }

    private sealed class MaterializedRow
    {
        public required DbValue[] Values { get; init; }
        public required DbValue[] PartitionKeys { get; init; }
        public required DbValue[] OrderKeys { get; init; }
        public required int OriginalIndex { get; init; }
    }

    private readonly record struct FrameRange(int Start, int End)
    {
        public bool IsEmpty => Start > End;
    }

    private sealed class SlidingAggregateState
    {
        private readonly WindowFunctionKind _kind;
        private long _count;
        private Int128 _integerSum;
        private double _realSum;
        private double _realCompensation;
        private long _realCount;
        private long _finiteRealCount;
        private long _nanRealCount;
        private long _positiveInfinityCount;
        private long _negativeInfinityCount;

        public SlidingAggregateState(WindowFunctionKind kind)
        {
            _kind = kind;
        }

        public void Add(DbValue value, bool countStar)
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
                    EnsureNumeric(value);
                    if (value.Type == DbType.Real)
                    {
                        AddReal(value.AsReal);
                    }
                    else
                    {
                        _integerSum += value.AsInteger;
                    }
                    _count++;
                    return;

                default:
                    throw new InvalidOperationException($"{_kind} does not use a numeric sliding state.");
            }
        }

        public void Remove(DbValue value, bool countStar)
        {
            if (_kind == WindowFunctionKind.Count)
            {
                if (countStar || !value.IsNull)
                    _count--;
                return;
            }

            if (value.IsNull)
                return;

            EnsureNumeric(value);
            if (value.Type == DbType.Real)
            {
                RemoveReal(value.AsReal);
            }
            else
            {
                _integerSum -= value.AsInteger;
            }

            _count--;
        }

        public DbValue GetValue()
        {
            return _kind switch
            {
                WindowFunctionKind.Count => DbValue.FromInteger(_count),
                WindowFunctionKind.Sum when _count == 0 => DbValue.Null,
                WindowFunctionKind.Sum when _realCount > 0 =>
                    DbValue.FromReal((double)_integerSum + GetRealSum()),
                WindowFunctionKind.Sum => DbValue.FromInteger(GetIntegerSum()),
                WindowFunctionKind.Avg when _count == 0 => DbValue.Null,
                WindowFunctionKind.Avg => DbValue.FromReal(
                    ((double)_integerSum + GetRealSum()) / _count),
                _ => throw new InvalidOperationException($"{_kind} does not use a numeric sliding state."),
            };
        }

        private void AddReal(double value)
        {
            _realCount++;
            if (double.IsNaN(value))
            {
                _nanRealCount++;
            }
            else if (double.IsPositiveInfinity(value))
            {
                _positiveInfinityCount++;
            }
            else if (double.IsNegativeInfinity(value))
            {
                _negativeInfinityCount++;
            }
            else
            {
                _finiteRealCount++;
                AccumulateFiniteReal(value);
            }
        }

        private void RemoveReal(double value)
        {
            if (double.IsNaN(value))
            {
                _nanRealCount--;
            }
            else if (double.IsPositiveInfinity(value))
            {
                _positiveInfinityCount--;
            }
            else if (double.IsNegativeInfinity(value))
            {
                _negativeInfinityCount--;
            }
            else
            {
                AccumulateFiniteReal(-value);
                _finiteRealCount--;
                if (_finiteRealCount == 0)
                    ResetFiniteRealSum();
            }

            _realCount--;
            if (_realCount == 0)
            {
                _nanRealCount = 0;
                _positiveInfinityCount = 0;
                _negativeInfinityCount = 0;
                _finiteRealCount = 0;
                ResetFiniteRealSum();
            }
        }

        private void AccumulateFiniteReal(double value)
        {
            double next = _realSum + value;
            if (!double.IsFinite(_realSum) ||
                !double.IsFinite(next))
            {
                _realSum = next;
                _realCompensation = 0;
                return;
            }

            _realCompensation += Math.Abs(_realSum) >= Math.Abs(value)
                ? (_realSum - next) + value
                : (value - next) + _realSum;
            _realSum = next;
        }

        private double GetRealSum()
        {
            if (_nanRealCount > 0 ||
                _positiveInfinityCount > 0 && _negativeInfinityCount > 0)
            {
                return double.NaN;
            }

            if (_positiveInfinityCount > 0)
                return double.PositiveInfinity;
            if (_negativeInfinityCount > 0)
                return double.NegativeInfinity;
            return _realSum + _realCompensation;
        }

        private void ResetFiniteRealSum()
        {
            _realSum = 0;
            _realCompensation = 0;
        }

        private static void EnsureNumeric(DbValue value)
        {
            if (value.Type is DbType.Integer or DbType.Real)
                return;

            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "SUM/AVG window argument must be numeric.");
        }

        private long GetIntegerSum()
        {
            if (_integerSum < (Int128)long.MinValue ||
                _integerSum > (Int128)long.MaxValue)
            {
                throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    "Integer SUM window overflowed the supported 64-bit range.");
            }

            return (long)_integerSum;
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
    private readonly WindowExecutionOptions _options;
    private List<DbValue[]>? _results;
    private int _index;
    private bool _sourceDisposed;
    private CancellationToken _sortCancellationToken;
    private int _sortComparisonCount;

    public WindowOperator(
        IOperator source,
        TableSchema inputSchema,
        IReadOnlyList<WindowFunctionExpression> windowFunctions,
        ColumnDefinition[] outputSchema,
        DbFunctionRegistry? functions = null,
        WindowExecutionOptions? options = null)
    {
        if (windowFunctions.Count == 0)
            throw new ArgumentException("At least one window function is required.", nameof(windowFunctions));

        options ??= new WindowExecutionOptions();
        if (options.MaxPartitionRows <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WindowExecutionOptions.MaxPartitionRows),
                options.MaxPartitionRows,
                "The maximum window partition row count must be greater than zero.");
        }

        if (options.MaxBufferedRows <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WindowExecutionOptions.MaxBufferedRows),
                options.MaxBufferedRows,
                "The maximum buffered window row count must be greater than zero.");
        }

        if (options.MaxBufferedRows < options.MaxPartitionRows)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WindowExecutionOptions.MaxBufferedRows),
                options.MaxBufferedRows,
                "The maximum buffered window row count cannot be smaller than the maximum partition row count.");
        }

        _source = PhysicalPlanCapture.WrapIfActive(source);
        _inputSchema = inputSchema;
        _options = new WindowExecutionOptions
        {
            MaxPartitionRows = options.MaxPartitionRows,
            MaxBufferedRows = options.MaxBufferedRows,
        };
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
    IReadOnlyList<IOperator> IPhysicalOperatorChildren.PhysicalChildren => [_source];

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
                if (rows.Count >= _options.MaxBufferedRows)
                {
                    throw new CSharpDbException(
                        ErrorCode.ResourceLimitExceeded,
                        $"Window stage exceeded the configured MaxBufferedRows limit of {_options.MaxBufferedRows} rows.");
                }

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
            _sortCancellationToken = ct;
            _sortComparisonCount = 0;
            try
            {
                rows.Sort(CompareRows);
            }
            catch (InvalidOperationException ex)
                when (ct.IsCancellationRequested &&
                      ex.InnerException is OperationCanceledException cancellation)
            {
                throw new OperationCanceledException(
                    "Window sorting was canceled.",
                    cancellation,
                    ct);
            }
            finally
            {
                _sortCancellationToken = default;
                _sortComparisonCount = 0;
            }

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
        ValidatePartitionSizes(rows, ct);
        var results = new List<DbValue[]>(rows.Count);

        int partitionStart = 0;
        while (partitionStart < rows.Count)
        {
            ct.ThrowIfCancellationRequested();
            int partitionEnd = FindPartitionEnd(rows, partitionStart, ct);
            InitializePartitionResults(rows, results, partitionStart, partitionEnd, ct);

            EvaluatePartition(rows, results, partitionStart, partitionEnd, ct);
            partitionStart = partitionEnd;
        }

        return results;
    }

    private void ValidatePartitionSizes(List<MaterializedRow> rows, CancellationToken ct)
    {
        int partitionStart = 0;
        while (partitionStart < rows.Count)
        {
            ct.ThrowIfCancellationRequested();
            int partitionEnd = FindPartitionEnd(rows, partitionStart, ct);
            int partitionRowCount = partitionEnd - partitionStart;
            if (partitionRowCount > _options.MaxPartitionRows)
            {
                throw new CSharpDbException(
                    ErrorCode.ResourceLimitExceeded,
                    $"Window partition exceeded the configured MaxPartitionRows limit of {_options.MaxPartitionRows} rows.");
            }

            partitionStart = partitionEnd;
        }
    }

    private int FindPartitionEnd(
        List<MaterializedRow> rows,
        int partitionStart,
        CancellationToken ct)
    {
        int partitionEnd = partitionStart + 1;
        while (partitionEnd < rows.Count &&
               KeysEqual(
                   rows[partitionStart].PartitionKeys,
                   rows[partitionEnd].PartitionKeys,
                   _partitionCollations))
        {
            ThrowIfCancellationRequestedPeriodically(partitionEnd - partitionStart, ct);
            partitionEnd++;
        }

        return partitionEnd;
    }

    private void InitializePartitionResults(
        List<MaterializedRow> rows,
        List<DbValue[]> results,
        int partitionStart,
        int partitionEnd,
        CancellationToken ct)
    {
        for (int rowIndex = partitionStart; rowIndex < partitionEnd; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex - partitionStart, ct);
            DbValue[] source = rows[rowIndex].Values;
            if (source.Length != _inputSchema.Columns.Count)
            {
                throw new InvalidOperationException(
                    "Window evaluation requires full-width source rows.");
            }

            var destination = new DbValue[OutputSchema.Length];
            source.CopyTo(destination, 0);
            results.Add(destination);
        }
    }

    private void EvaluatePartition(
        List<MaterializedRow> rows,
        List<DbValue[]> results,
        int partitionStart,
        int partitionEnd,
        CancellationToken ct)
    {
        int partitionRowCount = partitionEnd - partitionStart;
        bool needsPeerEnds = _orderEvaluators.Length > 0 &&
            _functions.Any(function =>
                function.Kind is WindowFunctionKind.Rank or WindowFunctionKind.DenseRank ||
                IsFrameSensitive(function.Kind) && function.Frame == null);
        int[]? peerEnds = needsPeerEnds
            ? BuildPeerEnds(rows, partitionStart, partitionEnd, ct)
            : null;

        EvaluateRankingFunctions(
            results,
            partitionStart,
            partitionRowCount,
            peerEnds,
            ct);

        for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
        {
            ct.ThrowIfCancellationRequested();
            RuntimeFunction function = _functions[functionIndex];
            if (IsRanking(function.Kind))
                continue;

            DbValue[]? values = function.IsCountStar
                ? null
                : EvaluateValueArguments(
                    rows,
                    partitionStart,
                    partitionEnd,
                    function.ValueEvaluator!,
                    ct);

            if (IsAggregate(function.Kind))
            {
                FrameRange[] ranges = BuildFrameRanges(
                    function,
                    partitionRowCount,
                    peerEnds,
                    ct);
                EvaluateAggregate(
                    function,
                    values,
                    ranges,
                    results,
                    partitionStart,
                    functionIndex,
                    ct);
                continue;
            }

            switch (function.Kind)
            {
                case WindowFunctionKind.Lag:
                case WindowFunctionKind.Lead:
                    EvaluateOffsetValueFunction(
                        function,
                        values!,
                        rows,
                        results,
                        partitionStart,
                        partitionEnd,
                        functionIndex,
                        ct);
                    break;

                case WindowFunctionKind.FirstValue:
                case WindowFunctionKind.LastValue:
                {
                    FrameRange[] ranges = BuildFrameRanges(
                        function,
                        partitionRowCount,
                        peerEnds,
                        ct);
                    EvaluateFrameValueFunction(
                        function.Kind,
                        values!,
                        ranges,
                        results,
                        partitionStart,
                        functionIndex,
                        ct);
                    break;
                }

                default:
                    throw new InvalidOperationException(
                        $"Unsupported window runtime function: {function.Kind}.");
            }
        }
    }

    private int[] BuildPeerEnds(
        List<MaterializedRow> rows,
        int partitionStart,
        int partitionEnd,
        CancellationToken ct)
    {
        int partitionRowCount = partitionEnd - partitionStart;
        var peerEnds = new int[partitionRowCount];
        int peerStart = 0;
        while (peerStart < partitionRowCount)
        {
            ct.ThrowIfCancellationRequested();
            int peerEnd = peerStart + 1;
            while (peerEnd < partitionRowCount &&
                   KeysEqual(
                       rows[partitionStart + peerStart].OrderKeys,
                       rows[partitionStart + peerEnd].OrderKeys,
                       _orderCollations))
            {
                ThrowIfCancellationRequestedPeriodically(peerEnd - peerStart, ct);
                peerEnd++;
            }

            int peerEndInclusive = peerEnd - 1;
            for (int rowIndex = peerStart; rowIndex < peerEnd; rowIndex++)
            {
                ThrowIfCancellationRequestedPeriodically(rowIndex - peerStart, ct);
                peerEnds[rowIndex] = peerEndInclusive;
            }

            peerStart = peerEnd;
        }

        return peerEnds;
    }

    private void EvaluateRankingFunctions(
        List<DbValue[]> results,
        int partitionStart,
        int partitionRowCount,
        int[]? peerEnds,
        CancellationToken ct)
    {
        int outputOffset = _inputSchema.Columns.Count;
        for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
        {
            if (_functions[functionIndex].Kind != WindowFunctionKind.RowNumber)
                continue;

            for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++)
            {
                ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
                results[partitionStart + rowIndex][outputOffset + functionIndex] =
                    DbValue.FromInteger(rowIndex + 1L);
            }
        }

        bool hasPeerRanking = _functions.Any(function =>
            function.Kind is WindowFunctionKind.Rank or WindowFunctionKind.DenseRank);
        if (!hasPeerRanking)
            return;

        if (_orderEvaluators.Length == 0)
        {
            for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++)
            {
                ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
                for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
                {
                    if (_functions[functionIndex].Kind is WindowFunctionKind.Rank or WindowFunctionKind.DenseRank)
                    {
                        results[partitionStart + rowIndex][outputOffset + functionIndex] =
                            DbValue.FromInteger(1);
                    }
                }
            }

            return;
        }

        if (peerEnds == null)
            throw new InvalidOperationException("Window peer metadata was not initialized.");

        int peerStart = 0;
        long denseRank = 0;
        while (peerStart < partitionRowCount)
        {
            ct.ThrowIfCancellationRequested();
            int peerEnd = peerEnds[peerStart];
            denseRank++;
            long rank = peerStart + 1L;
            for (int rowIndex = peerStart; rowIndex <= peerEnd; rowIndex++)
            {
                ThrowIfCancellationRequestedPeriodically(rowIndex - peerStart, ct);
                for (int functionIndex = 0; functionIndex < _functions.Length; functionIndex++)
                {
                    DbValue value = _functions[functionIndex].Kind switch
                    {
                        WindowFunctionKind.Rank => DbValue.FromInteger(rank),
                        WindowFunctionKind.DenseRank => DbValue.FromInteger(denseRank),
                        _ => DbValue.Null,
                    };
                    if (!value.IsNull)
                    {
                        results[partitionStart + rowIndex][outputOffset + functionIndex] = value;
                    }
                }
            }

            peerStart = peerEnd + 1;
        }
    }

    private static DbValue[] EvaluateValueArguments(
        List<MaterializedRow> rows,
        int partitionStart,
        int partitionEnd,
        SpanExpressionEvaluator evaluator,
        CancellationToken ct)
    {
        var values = new DbValue[partitionEnd - partitionStart];
        for (int rowIndex = partitionStart; rowIndex < partitionEnd; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex - partitionStart, ct);
            values[rowIndex - partitionStart] = evaluator(rows[rowIndex].Values);
        }

        return values;
    }

    private FrameRange[] BuildFrameRanges(
        RuntimeFunction function,
        int partitionRowCount,
        int[]? peerEnds,
        CancellationToken ct)
    {
        var ranges = new FrameRange[partitionRowCount];
        if (function.Frame == null)
        {
            if (_orderEvaluators.Length == 0)
            {
                Array.Fill(ranges, new FrameRange(0, partitionRowCount - 1));
                ct.ThrowIfCancellationRequested();
                return ranges;
            }

            if (peerEnds == null)
                throw new InvalidOperationException("Window peer metadata was not initialized.");

            for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++)
            {
                ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
                ranges[rowIndex] = new FrameRange(0, peerEnds[rowIndex]);
            }

            return ranges;
        }

        for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
            ranges[rowIndex] = ResolveExplicitFrame(
                function.Frame,
                rowIndex,
                partitionRowCount);
        }

        return ranges;
    }

    private static FrameRange ResolveExplicitFrame(
        WindowFrame frame,
        int rowIndex,
        int partitionRowCount)
    {
        long rawStart = ResolveFrameBound(frame.Start, rowIndex);
        long rawEnd = ResolveFrameBound(frame.End, rowIndex);

        int start = rawStart <= 0
            ? 0
            : rawStart >= partitionRowCount
                ? partitionRowCount
                : (int)rawStart;
        int end = rawEnd < 0
            ? -1
            : rawEnd >= partitionRowCount - 1L
                ? partitionRowCount - 1
                : (int)rawEnd;

        return new FrameRange(start, end);
    }

    private static long ResolveFrameBound(WindowFrameBound bound, int rowIndex)
    {
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => long.MinValue,
            WindowFrameBoundKind.Preceding => rowIndex - RequireFrameOffset(bound),
            WindowFrameBoundKind.CurrentRow => rowIndex,
            WindowFrameBoundKind.Following => AddFrameOffset(rowIndex, RequireFrameOffset(bound)),
            WindowFrameBoundKind.UnboundedFollowing => long.MaxValue,
            _ => throw new InvalidOperationException($"Unknown window frame bound: {bound.Kind}."),
        };
    }

    private static long RequireFrameOffset(WindowFrameBound bound)
    {
        if (!bound.Offset.HasValue)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Window frame {bound.Kind.ToString().ToUpperInvariant()} bound requires an offset.");
        }

        if (bound.Offset.Value < 0)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window frame offsets must be non-negative.");
        }

        return bound.Offset.Value;
    }

    private static long AddFrameOffset(int rowIndex, long offset) =>
        offset > long.MaxValue - rowIndex
            ? long.MaxValue
            : rowIndex + offset;

    private void EvaluateAggregate(
        RuntimeFunction function,
        DbValue[]? values,
        FrameRange[] ranges,
        List<DbValue[]> results,
        int partitionStart,
        int functionIndex,
        CancellationToken ct)
    {
        if (function.Kind is WindowFunctionKind.Min or WindowFunctionKind.Max)
        {
            EvaluateMinMax(
                function,
                values!,
                ranges,
                results,
                partitionStart,
                functionIndex,
                ct);
            return;
        }

        var state = new SlidingAggregateState(function.Kind);
        int currentStart = ranges.Length == 0 ? 0 : ranges[0].Start;
        int currentEnd = currentStart - 1;
        int operationCount = 0;
        int outputOffset = _inputSchema.Columns.Count;
        for (int rowIndex = 0; rowIndex < ranges.Length; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
            FrameRange range = ranges[rowIndex];
            EnsureMonotonicFrame(range, currentStart, currentEnd);

            while (currentStart < range.Start)
            {
                if (currentStart <= currentEnd)
                {
                    state.Remove(
                        function.IsCountStar ? DbValue.Null : values![currentStart],
                        function.IsCountStar);
                }
                currentStart++;
                ThrowIfCancellationRequestedPeriodically(++operationCount, ct);
            }

            while (currentEnd < range.End)
            {
                currentEnd++;
                if (currentEnd >= currentStart)
                {
                    state.Add(
                        function.IsCountStar ? DbValue.Null : values![currentEnd],
                        function.IsCountStar);
                }
                ThrowIfCancellationRequestedPeriodically(++operationCount, ct);
            }

            results[partitionStart + rowIndex][outputOffset + functionIndex] =
                state.GetValue();
        }
    }

    private void EvaluateMinMax(
        RuntimeFunction function,
        DbValue[] values,
        FrameRange[] ranges,
        List<DbValue[]> results,
        int partitionStart,
        int functionIndex,
        CancellationToken ct)
    {
        var deque = new int[values.Length];
        int head = 0;
        int tail = 0;
        int currentStart = ranges.Length == 0 ? 0 : ranges[0].Start;
        int currentEnd = currentStart - 1;
        int operationCount = 0;
        int outputOffset = _inputSchema.Columns.Count;
        bool isMin = function.Kind == WindowFunctionKind.Min;

        for (int rowIndex = 0; rowIndex < ranges.Length; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
            FrameRange range = ranges[rowIndex];
            EnsureMonotonicFrame(range, currentStart, currentEnd);

            while (currentEnd < range.End)
            {
                currentEnd++;
                if (currentEnd >= currentStart && !values[currentEnd].IsNull)
                {
                    while (tail > head)
                    {
                        int comparison = CollationSupport.Compare(
                            values[deque[tail - 1]],
                            values[currentEnd],
                            function.ArgumentCollation);
                        if (isMin ? comparison <= 0 : comparison >= 0)
                            break;

                        tail--;
                        ThrowIfCancellationRequestedPeriodically(++operationCount, ct);
                    }

                    deque[tail++] = currentEnd;
                }
                ThrowIfCancellationRequestedPeriodically(++operationCount, ct);
            }

            currentStart = range.Start;
            while (head < tail && deque[head] < currentStart)
            {
                head++;
                ThrowIfCancellationRequestedPeriodically(++operationCount, ct);
            }

            results[partitionStart + rowIndex][outputOffset + functionIndex] =
                head < tail ? values[deque[head]] : DbValue.Null;
        }
    }

    private static void EnsureMonotonicFrame(
        FrameRange range,
        int currentStart,
        int currentEnd)
    {
        if (range.Start < currentStart || range.End < currentEnd)
        {
            throw new InvalidOperationException(
                "Resolved ROWS frame bounds must advance monotonically.");
        }
    }

    private void EvaluateOffsetValueFunction(
        RuntimeFunction function,
        DbValue[] values,
        List<MaterializedRow> rows,
        List<DbValue[]> results,
        int partitionStart,
        int partitionEnd,
        int functionIndex,
        CancellationToken ct)
    {
        int partitionRowCount = partitionEnd - partitionStart;
        int outputOffset = _inputSchema.Columns.Count;
        for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
            long offset = 1;
            if (function.OffsetEvaluator != null)
            {
                DbValue offsetValue = function.OffsetEvaluator(
                    rows[partitionStart + rowIndex].Values);
                if (offsetValue.IsNull ||
                    offsetValue.Type != DbType.Integer ||
                    offsetValue.AsInteger < 0)
                {
                    throw new CSharpDbException(
                        ErrorCode.TypeMismatch,
                        $"{function.Kind.ToString().ToUpperInvariant()} window offset must be a nonnegative INTEGER.");
                }

                offset = offsetValue.AsInteger;
            }

            bool outOfRange;
            int targetIndex = 0;
            if (function.Kind == WindowFunctionKind.Lag)
            {
                outOfRange = offset > rowIndex;
                if (!outOfRange)
                    targetIndex = rowIndex - (int)offset;
            }
            else
            {
                long rowsAfter = partitionRowCount - 1L - rowIndex;
                outOfRange = offset > rowsAfter;
                if (!outOfRange)
                    targetIndex = rowIndex + (int)offset;
            }

            DbValue result = outOfRange
                ? function.DefaultEvaluator?.Invoke(rows[partitionStart + rowIndex].Values) ??
                  DbValue.Null
                : values[targetIndex];
            results[partitionStart + rowIndex][outputOffset + functionIndex] = result;
        }
    }

    private void EvaluateFrameValueFunction(
        WindowFunctionKind kind,
        DbValue[] values,
        FrameRange[] ranges,
        List<DbValue[]> results,
        int partitionStart,
        int functionIndex,
        CancellationToken ct)
    {
        int outputOffset = _inputSchema.Columns.Count;
        for (int rowIndex = 0; rowIndex < ranges.Length; rowIndex++)
        {
            ThrowIfCancellationRequestedPeriodically(rowIndex, ct);
            FrameRange range = ranges[rowIndex];
            DbValue value = range.IsEmpty
                ? DbValue.Null
                : kind == WindowFunctionKind.FirstValue
                    ? values[range.Start]
                    : values[range.End];
            results[partitionStart + rowIndex][outputOffset + functionIndex] = value;
        }
    }

    private int CompareRows(MaterializedRow left, MaterializedRow right)
    {
        _sortComparisonCount = unchecked(_sortComparisonCount + 1);
        ThrowIfCancellationRequestedPeriodically(
            _sortComparisonCount,
            _sortCancellationToken);

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

    private static void ThrowIfCancellationRequestedPeriodically(int iteration, CancellationToken ct)
    {
        if (iteration % CancellationCheckInterval == 0)
            ct.ThrowIfCancellationRequested();
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
            "LAG" => WindowFunctionKind.Lag,
            "LEAD" => WindowFunctionKind.Lead,
            "FIRST_VALUE" => WindowFunctionKind.FirstValue,
            "LAST_VALUE" => WindowFunctionKind.LastValue,
            _ => throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Window function '{function.FunctionName}' is not supported."),
        };

        Expression? valueArgument = function.Arguments.Count > 0
            ? function.Arguments[0]
            : null;
        Expression? offsetArgument =
            kind is WindowFunctionKind.Lag or WindowFunctionKind.Lead &&
            function.Arguments.Count > 1
                ? function.Arguments[1]
                : null;
        Expression? defaultArgument =
            kind is WindowFunctionKind.Lag or WindowFunctionKind.Lead &&
            function.Arguments.Count > 2
                ? function.Arguments[2]
                : null;
        return new RuntimeFunction
        {
            Kind = kind,
            IsCountStar = kind == WindowFunctionKind.Count && function.IsStarArg,
            ValueEvaluator = valueArgument == null
                ? null
                : ExpressionCompiler.CompileSpan(valueArgument, schema, functions),
            OffsetEvaluator = offsetArgument == null
                ? null
                : ExpressionCompiler.CompileSpan(offsetArgument, schema, functions),
            DefaultEvaluator = defaultArgument == null
                ? null
                : ExpressionCompiler.CompileSpan(defaultArgument, schema, functions),
            ArgumentCollation = valueArgument == null
                ? null
                : CollationSupport.ResolveExpressionCollation(valueArgument, schema),
            Frame = expression.Window.Frame,
        };
    }

    private static bool IsAggregate(WindowFunctionKind kind) =>
        kind is WindowFunctionKind.Count
            or WindowFunctionKind.Sum
            or WindowFunctionKind.Avg
            or WindowFunctionKind.Min
            or WindowFunctionKind.Max;

    private static bool IsRanking(WindowFunctionKind kind) =>
        kind is WindowFunctionKind.RowNumber
            or WindowFunctionKind.Rank
            or WindowFunctionKind.DenseRank;

    private static bool IsFrameSensitive(WindowFunctionKind kind) =>
        IsAggregate(kind) ||
        kind is WindowFunctionKind.FirstValue or WindowFunctionKind.LastValue;

    private async ValueTask DisposeSourceOnceAsync()
    {
        if (_sourceDisposed)
            return;

        _sourceDisposed = true;
        await _source.DisposeAsync();
    }
}
