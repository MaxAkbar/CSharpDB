using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal sealed class RowSelection
{
    private int[] _indices;

    public RowSelection(int initialCapacity = 0)
    {
        if (initialCapacity < 0)
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));

        _indices = initialCapacity == 0 ? Array.Empty<int>() : new int[initialCapacity];
    }

    public int Count { get; private set; }

    public void Reset() => Count = 0;

    public void EnsureCapacity(int capacity)
    {
        if (capacity <= _indices.Length)
            return;

        int newCapacity = _indices.Length == 0 ? 4 : _indices.Length * 2;
        while (newCapacity < capacity)
            newCapacity *= 2;

        Array.Resize(ref _indices, newCapacity);
    }

    public void Add(int rowIndex)
    {
        if (rowIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(rowIndex));

        EnsureCapacity(Count + 1);
        _indices[Count++] = rowIndex;
    }

    public ReadOnlySpan<int> AsSpan() => _indices.AsSpan(0, Count);
}

internal interface IFilterProjectionBatchPlan
{
    BatchPushdownFilter[] PushdownFilters { get; }

    int OutputColumnCount { get; }

    int Execute(RowBatch sourceBatch, RowSelection selection, RowBatch destination);
}

internal interface IScalarAggregateBatchPlan
{
    BatchPushdownFilter[] PushdownFilters { get; }

    void Reset();

    void Accumulate(RowBatch sourceBatch);

    DbValue GetResult();
}

internal enum BatchPushdownFilterKind
{
    Comparison,
    IntegerIn,
    NumericIn,
    TextIn,
}

internal readonly struct BatchPushdownFilter
{
    public BatchPushdownFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        ColumnIndex = columnIndex;
        Kind = BatchPushdownFilterKind.Comparison;
        Op = op;
        Literal = literal;
        IntegerSet = null;
        NumericSet = null;
        TextSet = null;
    }

    private BatchPushdownFilter(
        int columnIndex,
        long[]? integerSet,
        double[]? numericSet,
        string[]? textSet,
        BatchPushdownFilterKind kind)
    {
        ColumnIndex = columnIndex;
        Kind = kind;
        Op = BinaryOp.Equals;
        Literal = DbValue.Null;
        IntegerSet = integerSet;
        NumericSet = numericSet;
        TextSet = textSet;
    }

    public int ColumnIndex { get; }
    public BatchPushdownFilterKind Kind { get; }
    public BinaryOp Op { get; }
    public DbValue Literal { get; }
    public long[]? IntegerSet { get; }
    public double[]? NumericSet { get; }
    public string[]? TextSet { get; }

    public static BatchPushdownFilter CreateIntegerIn(int columnIndex, long[] values)
        => new(columnIndex, values, null, null, BatchPushdownFilterKind.IntegerIn);

    public static BatchPushdownFilter CreateNumericIn(int columnIndex, double[] values)
        => new(columnIndex, null, values, null, BatchPushdownFilterKind.NumericIn);

    public static BatchPushdownFilter CreateTextIn(int columnIndex, string[] values)
        => new(columnIndex, null, null, values, BatchPushdownFilterKind.TextIn);
}

internal sealed class DelegateFilterProjectionBatchPlan : IFilterProjectionBatchPlan
{
    private readonly Func<DbValue[], DbValue>? _predicateEvaluator;
    private readonly int[] _columnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private DbValue[]? _rowBuffer;

    public DelegateFilterProjectionBatchPlan(
        Func<DbValue[], DbValue>? predicateEvaluator,
        int[] columnIndices,
        Func<DbValue[], DbValue>[]? expressionEvaluators)
    {
        _predicateEvaluator = predicateEvaluator;
        _columnIndices = columnIndices ?? throw new ArgumentNullException(nameof(columnIndices));
        _expressionEvaluators = expressionEvaluators;
    }

    public int OutputColumnCount => _expressionEvaluators?.Length ?? _columnIndices.Length;
    public BatchPushdownFilter[] PushdownFilters => Array.Empty<BatchPushdownFilter>();

    public int Execute(RowBatch sourceBatch, RowSelection selection, RowBatch destination)
    {
        ArgumentNullException.ThrowIfNull(selection);
        ArgumentNullException.ThrowIfNull(destination);

        if (destination.ColumnCount != OutputColumnCount)
        {
            throw new ArgumentException(
                "Destination batch width does not match plan output width.",
                nameof(destination));
        }

        selection.Reset();
        destination.Reset();

        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            DbValue[]? sourceRowBuffer = null;
            if (_predicateEvaluator != null)
            {
                sourceRowBuffer = EnsureRowBuffer(sourceBatch.ColumnCount);
                sourceBatch.CopyRowTo(rowIndex, sourceRowBuffer);
                if (!_predicateEvaluator(sourceRowBuffer).IsTruthy)
                    continue;
            }

            selection.Add(rowIndex);
            int destinationRowIndex = destination.Count;
            Span<DbValue> destinationRow = destination.GetWritableRowSpan(destinationRowIndex);
            WriteProjectedRow(sourceBatch, rowIndex, destinationRow, sourceRowBuffer);
            destination.CommitWrittenRow(destinationRowIndex);
        }

        return destination.Count;
    }

    private void WriteProjectedRow(RowBatch sourceBatch, int rowIndex, Span<DbValue> destination, DbValue[]? sourceRowBuffer)
    {
        if (_expressionEvaluators != null)
        {
            var sourceRow = sourceRowBuffer ?? EnsureRowBuffer(sourceBatch.ColumnCount);
            if (!ReferenceEquals(sourceRow, sourceRowBuffer))
                sourceBatch.CopyRowTo(rowIndex, sourceRow);

            for (int i = 0; i < _expressionEvaluators.Length; i++)
                destination[i] = _expressionEvaluators[i](sourceRow);

            return;
        }

        var sourceRowSpan = sourceBatch.GetRowSpan(rowIndex);
        for (int i = 0; i < _columnIndices.Length; i++)
            destination[i] = sourceRowSpan[_columnIndices[i]];
    }

    private DbValue[] EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _rowBuffer;
    }
}

internal static class BatchPlanCompiler
{
    public static IFilterProjectionBatchPlan? TryCreateFilter(
        Expression? predicate,
        TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        if (predicate == null)
            return null;

        var boundPredicate = TryBindPredicate(predicate, schema);
        if (boundPredicate == null)
            return null;

        var projections = new BatchProjectionTerm[schema.Columns.Count];
        for (int i = 0; i < projections.Length; i++)
            projections[i] = BatchProjectionTerm.CreateColumn(i);

        return new SpecializedFilterProjectionBatchPlan(
            boundPredicate,
            projections,
            CreatePushdownFilters(boundPredicate));
    }

    public static IFilterProjectionBatchPlan? TryCreate(
        Expression? predicate,
        IReadOnlyList<Expression> projectionExpressions,
        TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(projectionExpressions);
        ArgumentNullException.ThrowIfNull(schema);

        var boundPredicate = TryBindPredicate(predicate, schema);
        if (predicate != null && boundPredicate == null)
            return null;

        var projections = TryBindProjections(projectionExpressions, schema);
        if (projections == null)
            return null;

        return new SpecializedFilterProjectionBatchPlan(
            boundPredicate,
            projections,
            CreatePushdownFilters(boundPredicate));
    }

    public static BatchProjectionTerm[]? TryBindProjectionTerms(
        IReadOnlyList<Expression> projections,
        TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(projections);
        ArgumentNullException.ThrowIfNull(schema);

        return TryBindProjections(projections, schema);
    }

    public static IScalarAggregateBatchPlan? TryCreateScalarAggregate(
        Expression? predicate,
        string functionName,
        int columnIndex,
        bool isCountStar,
        bool isDistinct,
        TableSchema schema)
        => TryCreateScalarAggregateCore(
            predicate,
            functionName,
            aggregateExpression: null,
            columnIndex,
            isCountStar,
            isDistinct,
            schema);

    public static IScalarAggregateBatchPlan? TryCreateScalarAggregate(
        Expression? predicate,
        string functionName,
        Expression? aggregateExpression,
        bool isCountStar,
        bool isDistinct,
        TableSchema schema)
        => TryCreateScalarAggregateCore(
            predicate,
            functionName,
            aggregateExpression,
            columnIndex: -1,
            isCountStar,
            isDistinct,
            schema);

    private static IScalarAggregateBatchPlan? TryCreateScalarAggregateCore(
        Expression? predicate,
        string functionName,
        Expression? aggregateExpression,
        int columnIndex,
        bool isCountStar,
        bool isDistinct,
        TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(functionName);
        ArgumentNullException.ThrowIfNull(schema);

        if (predicate == null)
            return null;

        var boundPredicate = TryBindPredicate(predicate, schema);
        if (boundPredicate == null)
            return null;

        var kind = functionName switch
        {
            "COUNT" => BatchScalarAggregateKind.Count,
            "SUM" => BatchScalarAggregateKind.Sum,
            "AVG" => BatchScalarAggregateKind.Avg,
            "MIN" => BatchScalarAggregateKind.Min,
            "MAX" => BatchScalarAggregateKind.Max,
            _ => BatchScalarAggregateKind.None,
        };

        if (kind == BatchScalarAggregateKind.None)
            return null;

        SpanExpressionEvaluator? aggregateArgumentEvaluator = null;
        if (!isCountStar)
        {
            if ((uint)columnIndex >= (uint)schema.Columns.Count)
            {
                if (aggregateExpression == null)
                    return null;

                if (TryResolveColumnIndex(aggregateExpression, schema, out int resolvedColumnIndex))
                {
                    columnIndex = resolvedColumnIndex;
                }
                else
                {
                    if (kind is BatchScalarAggregateKind.Sum or BatchScalarAggregateKind.Avg &&
                        !CanUseNumericScalarAggregateExpression(aggregateExpression, schema))
                    {
                        return null;
                    }

                    aggregateArgumentEvaluator = ExpressionCompiler.CompileSpan(aggregateExpression, schema);
                }
            }

            if (aggregateArgumentEvaluator == null &&
                kind is BatchScalarAggregateKind.Sum or BatchScalarAggregateKind.Avg &&
                !IsNumericType(schema.Columns[columnIndex].Type))
            {
                return null;
            }
        }

        return new SpecializedScalarAggregateBatchPlan(
            boundPredicate,
            CreatePushdownFilters(boundPredicate),
            kind,
            columnIndex,
            isCountStar,
            isDistinct,
            aggregateArgumentEvaluator);
    }

    private static bool CanUseNumericScalarAggregateExpression(Expression expression, TableSchema schema)
    {
        if (TryCreateLiteral(expression, out DbValue literalValue))
            return literalValue.IsNull || IsNumericType(literalValue.Type);

        return TryBindNumericExpression(expression, schema, out _);
    }

    private static BatchPredicateExpression? TryBindPredicate(Expression? predicate, TableSchema schema)
    {
        if (predicate == null)
            return null;

        return TryBindPredicateExpression(predicate, schema, out var boundPredicate)
            ? boundPredicate
            : null;
    }

    private static bool TryBindPredicateExpression(Expression predicate, TableSchema schema, out BatchPredicateExpression boundPredicate)
    {
        if (predicate is UnaryExpression { Op: TokenType.Not } unaryNot)
        {
            if (TryBindPredicateExpression(unaryNot.Operand, schema, out var innerPredicate))
            {
                boundPredicate = BatchPredicateExpression.CreateNot(innerPredicate);
                return true;
            }

            boundPredicate = null!;
            return false;
        }

        if (predicate is BinaryExpression { Op: BinaryOp.And } andExpression)
        {
            if (TryBindPredicateExpression(andExpression.Left, schema, out var leftPredicate) &&
                TryBindPredicateExpression(andExpression.Right, schema, out var rightPredicate))
            {
                boundPredicate = BatchPredicateExpression.CreateAnd(leftPredicate, rightPredicate);
                return true;
            }

            boundPredicate = null!;
            return false;
        }

        if (predicate is BinaryExpression { Op: BinaryOp.Or } orExpression)
        {
            if (TryBindPredicateExpression(orExpression.Left, schema, out var leftPredicate) &&
                TryBindPredicateExpression(orExpression.Right, schema, out var rightPredicate))
            {
                boundPredicate = BatchPredicateExpression.CreateOr(leftPredicate, rightPredicate);
                return true;
            }

            boundPredicate = null!;
            return false;
        }

        if (TryBindPredicateLeaf(predicate, schema, out var predicateTerm))
        {
            boundPredicate = BatchPredicateExpression.CreateLeaf(predicateTerm);
            return true;
        }

        boundPredicate = null!;
        return false;
    }

    private static bool TryBindPredicateLeaf(Expression predicate, TableSchema schema, out BatchPredicateTerm predicateTerm)
    {
        if (predicate is InExpression inExpression)
            return TryBindInPredicate(inExpression, schema, out predicateTerm);

        if (predicate is BetweenExpression betweenExpression)
            return TryBindBetweenPredicate(betweenExpression, schema, out predicateTerm);

        if (predicate is LikeExpression likeExpression)
            return TryBindLikePredicate(likeExpression, schema, out predicateTerm);

        if (predicate is IsNullExpression isNull &&
            TryResolveColumnIndex(isNull.Operand, schema, out int isNullColumnIndex))
        {
            predicateTerm = BatchPredicateTerm.CreateNullCheck(isNullColumnIndex, isNull.Negated);
            return true;
        }

        if (predicate is not BinaryExpression comparison ||
            !IsComparisonOp(comparison.Op))
        {
            predicateTerm = default;
            return false;
        }

        if (TryBindColumnLiteralPredicate(comparison.Left, comparison.Right, comparison.Op, schema, out var boundTerm) ||
            TryBindColumnLiteralPredicate(comparison.Right, comparison.Left, ReverseComparison(comparison.Op), schema, out boundTerm))
        {
            predicateTerm = boundTerm;
            return true;
        }

        predicateTerm = default;
        return false;
    }

    private static bool TryBindLikePredicate(LikeExpression likeExpression, TableSchema schema, out BatchPredicateTerm predicateTerm)
    {
        if (!TryCreateLiteral(likeExpression.Pattern, out DbValue patternValue) ||
            patternValue.IsNull ||
            patternValue.Type != DbType.Text ||
            !TryCreateEscapeChar(likeExpression.EscapeChar, out char? escapeChar))
        {
            predicateTerm = default;
            return false;
        }

        if (TryResolveTextFunctionColumn(likeExpression.Operand, schema, out int textifiedColumnIndex))
        {
            predicateTerm = BatchPredicateTerm.CreateTextifiedLike(textifiedColumnIndex, patternValue.AsText, escapeChar, likeExpression.Negated);
            return true;
        }

        if (TryBindTextProjectionExpression(likeExpression.Operand, schema, out var textProjection))
        {
            predicateTerm = BatchPredicateTerm.CreateTextExpressionLike(textProjection, patternValue.AsText, escapeChar, likeExpression.Negated);
            return true;
        }

        if (!TryResolveColumnIndex(likeExpression.Operand, schema, out int columnIndex) ||
            schema.Columns[columnIndex].Type != DbType.Text)
        {
            predicateTerm = default;
            return false;
        }

        predicateTerm = BatchPredicateTerm.CreateLike(columnIndex, patternValue.AsText, escapeChar, likeExpression.Negated);
        return true;
    }

    private static bool TryBindInPredicate(InExpression inExpression, TableSchema schema, out BatchPredicateTerm predicateTerm)
    {
        if (inExpression.Values.Count == 0)
        {
            predicateTerm = default;
            return false;
        }

        if (TryResolveTextFunctionColumn(inExpression.Operand, schema, out int textifiedColumnIndex))
        {
            var values = new List<string>(inExpression.Values.Count);
            bool hasNull = false;
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                inExpression.Operand,
                inExpression.Values[0],
                schema);
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Text)
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsText);
            }

            predicateTerm = BatchPredicateTerm.CreateTextifiedIn(
                textifiedColumnIndex,
                values.ToArray(),
                inExpression.Negated,
                hasNull,
                textCollation);
            return true;
        }

        if (TryBindTextProjectionExpression(inExpression.Operand, schema, out var textProjection))
        {
            var values = new List<string>(inExpression.Values.Count);
            bool hasNull = false;
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                inExpression.Operand,
                inExpression.Values[0],
                schema);
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Text)
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsText);
            }

            predicateTerm = BatchPredicateTerm.CreateTextExpressionIn(
                textProjection,
                values.ToArray(),
                inExpression.Negated,
                hasNull,
                textCollation);
            return true;
        }

        if (TryBindNumericProjection(inExpression.Operand, schema, out var numericProjection))
        {
            var values = new List<double>(inExpression.Values.Count);
            bool hasNull = false;
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (!IsNumericType(value.Type))
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsReal);
            }

            predicateTerm = BatchPredicateTerm.CreateNumericExpressionIn(
                numericProjection,
                values.ToArray(),
                inExpression.Negated,
                hasNull);
            return true;
        }

        if (!TryResolveColumnIndex(inExpression.Operand, schema, out int columnIndex))
        {
            predicateTerm = default;
            return false;
        }

        DbType columnType = schema.Columns[columnIndex].Type;
        if (columnType == DbType.Integer)
        {
            var values = new List<long>(inExpression.Values.Count);
            bool hasNull = false;
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Integer)
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsInteger);
            }

            predicateTerm = BatchPredicateTerm.CreateIntegerIn(columnIndex, values.ToArray(), inExpression.Negated, hasNull);
            return true;
        }

        if (IsNumericType(columnType))
        {
            var values = new List<double>(inExpression.Values.Count);
            bool hasNull = false;
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (!IsNumericType(value.Type))
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsReal);
            }

            predicateTerm = BatchPredicateTerm.CreateNumericIn(columnIndex, values.ToArray(), inExpression.Negated, hasNull);
            return true;
        }

        if (columnType == DbType.Text)
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                inExpression.Operand,
                inExpression.Values[0],
                schema);
            var values = new List<string>(inExpression.Values.Count);
            bool hasNull = false;
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    predicateTerm = default;
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Text)
                {
                    predicateTerm = default;
                    return false;
                }

                values.Add(value.AsText);
            }

            predicateTerm = BatchPredicateTerm.CreateTextIn(
                columnIndex,
                values.ToArray(),
                inExpression.Negated,
                hasNull,
                textCollation);
            return true;
        }

        predicateTerm = default;
        return false;
    }

    private static bool TryBindBetweenPredicate(BetweenExpression between, TableSchema schema, out BatchPredicateTerm predicateTerm)
    {
        if (!TryCreateLiteral(between.Low, out DbValue lowValue) ||
            !TryCreateLiteral(between.High, out DbValue highValue) ||
            lowValue.IsNull ||
            highValue.IsNull)
        {
            predicateTerm = default;
            return false;
        }

        if (TryResolveTextFunctionColumn(between.Operand, schema, out int textifiedColumnIndex) &&
            lowValue.Type == DbType.Text &&
            highValue.Type == DbType.Text)
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                between.Operand,
                between.Low,
                schema);
            predicateTerm = BatchPredicateTerm.CreateTextifiedRange(
                textifiedColumnIndex,
                lowValue.AsText,
                highValue.AsText,
                between.Negated,
                textCollation);
            return true;
        }

        if (lowValue.Type == DbType.Text &&
            highValue.Type == DbType.Text &&
            TryBindTextProjectionExpression(between.Operand, schema, out var textProjection))
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                between.Operand,
                between.Low,
                schema);
            predicateTerm = BatchPredicateTerm.CreateTextExpressionRange(
                textProjection,
                lowValue.AsText,
                highValue.AsText,
                between.Negated,
                textCollation);
            return true;
        }

        if (IsNumericType(lowValue.Type) &&
            IsNumericType(highValue.Type) &&
            TryBindNumericProjection(between.Operand, schema, out var numericProjection))
        {
            predicateTerm = BatchPredicateTerm.CreateNumericExpressionRange(
                numericProjection,
                lowValue.AsReal,
                highValue.AsReal,
                between.Negated);
            return true;
        }

        if (!TryResolveColumnIndex(between.Operand, schema, out int columnIndex))
        {
            predicateTerm = default;
            return false;
        }

        DbType columnType = schema.Columns[columnIndex].Type;
        if (columnType == DbType.Integer &&
            lowValue.Type == DbType.Integer &&
            highValue.Type == DbType.Integer)
        {
            predicateTerm = BatchPredicateTerm.CreateIntegerRange(
                columnIndex,
                lowValue.AsInteger,
                highValue.AsInteger,
                between.Negated);
            return true;
        }

        if (IsNumericType(columnType) &&
            IsNumericType(lowValue.Type) &&
            IsNumericType(highValue.Type))
        {
            predicateTerm = BatchPredicateTerm.CreateNumericRange(
                columnIndex,
                lowValue.AsReal,
                highValue.AsReal,
                between.Negated);
            return true;
        }

        if (columnType == DbType.Text &&
            lowValue.Type == DbType.Text &&
            highValue.Type == DbType.Text)
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(
                between.Operand,
                between.Low,
                schema);
            predicateTerm = BatchPredicateTerm.CreateTextRange(
                columnIndex,
                lowValue.AsText,
                highValue.AsText,
                between.Negated,
                textCollation);
            return true;
        }

        predicateTerm = default;
        return false;
    }

    private static bool TryBindColumnLiteralPredicate(
        Expression left,
        Expression right,
        BinaryOp op,
        TableSchema schema,
        out BatchPredicateTerm predicate)
    {
        predicate = default;
        if (!TryCreateLiteral(right, out DbValue literalValue))
        {
            return false;
        }

        if (literalValue.IsNull)
            return false;

        if (TryResolveTextFunctionColumn(left, schema, out int textifiedColumnIndex) &&
            literalValue.Type == DbType.Text)
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(left, right, schema);
            predicate = BatchPredicateTerm.CreateTextifiedCompare(
                textifiedColumnIndex,
                op,
                literalValue.AsText,
                textCollation);
            return true;
        }

        if (literalValue.Type == DbType.Text &&
            TryBindTextProjectionExpression(left, schema, out var textProjection))
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(left, right, schema);
            predicate = BatchPredicateTerm.CreateTextExpressionCompare(
                textProjection,
                op,
                literalValue.AsText,
                textCollation);
            return true;
        }

        if (IsNumericType(literalValue.Type) &&
            TryBindNumericProjection(left, schema, out var numericProjection))
        {
            predicate = BatchPredicateTerm.CreateNumericExpressionCompare(numericProjection, op, literalValue);
            return true;
        }

        if (!TryResolveColumnIndex(left, schema, out int columnIndex))
            return false;

        var columnType = schema.Columns[columnIndex].Type;
        if (columnType == DbType.Integer && literalValue.Type == DbType.Integer)
        {
            predicate = BatchPredicateTerm.CreateIntegerCompare(columnIndex, op, literalValue.AsInteger);
            return true;
        }

        if (IsNumericType(columnType) && IsNumericType(literalValue.Type))
        {
            predicate = BatchPredicateTerm.CreateNumericCompare(columnIndex, op, literalValue.AsReal);
            return true;
        }

        if (columnType == DbType.Text &&
            literalValue.Type == DbType.Text)
        {
            string? textCollation = CollationSupport.ResolveComparisonCollation(left, right, schema);
            predicate = BatchPredicateTerm.CreateTextCompare(
                columnIndex,
                op,
                literalValue.AsText,
                textCollation);
            return true;
        }

        return false;
    }

    private static bool TryResolveTextFunctionColumn(Expression expression, TableSchema schema, out int columnIndex)
    {
        columnIndex = -1;
        expression = CollationSupport.StripCollation(expression);
        if (expression is not FunctionCallExpression
            {
                IsStarArg: false,
                IsDistinct: false,
                Arguments.Count: 1
            } function ||
            !string.Equals(function.FunctionName, "TEXT", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return TryResolveColumnIndex(function.Arguments[0], schema, out columnIndex);
    }

    private static bool TryBindTextProjectionExpression(Expression expression, TableSchema schema, out BatchProjectionTerm term)
    {
        term = default;
        expression = CollationSupport.StripCollation(expression);
        return expression is FunctionCallExpression function &&
               TryBindTextProjection(function, schema, out term);
    }

    private static bool TryCreateEscapeChar(Expression? escapeExpression, out char? escapeChar)
    {
        escapeChar = null;
        if (escapeExpression == null)
            return true;

        if (!TryCreateLiteral(escapeExpression, out DbValue escapeValue))
        {
            return false;
        }

        if (escapeValue.IsNull)
            return true;

        if (escapeValue.Type != DbType.Text)
            return false;

        string escapeText = escapeValue.AsText;
        if (escapeText.Length != 1)
            return false;

        escapeChar = escapeText[0];
        return true;
    }

    private static BatchProjectionTerm[]? TryBindProjections(IReadOnlyList<Expression> projections, TableSchema schema)
    {
        var bound = new BatchProjectionTerm[projections.Count];
        for (int i = 0; i < projections.Count; i++)
        {
            if (!TryBindProjection(projections[i], schema, out bound[i]))
                return null;
        }

        return bound;
    }

    private static bool TryBindProjection(Expression projection, TableSchema schema, out BatchProjectionTerm term)
    {
        term = default;

        if (TryResolveColumnIndex(projection, schema, out int columnIndex))
        {
            term = BatchProjectionTerm.CreateColumn(columnIndex);
            return true;
        }

        if (projection is LiteralExpression literal &&
            TryCreateLiteral(literal, out DbValue literalValue))
        {
            term = BatchProjectionTerm.CreateConstant(literalValue);
            return true;
        }

        if (TryBindNumericProjection(projection, schema, out term))
            return true;

        if (projection is FunctionCallExpression function &&
            TryBindTextProjection(function, schema, out term))
        {
            return true;
        }

        return false;
    }

    private static bool TryBindNumericProjection(Expression expression, TableSchema schema, out BatchProjectionTerm term)
    {
        term = default;
        expression = CollationSupport.StripCollation(expression);

        if (expression is UnaryExpression { Op: TokenType.Minus } unaryMinus &&
            TryBindNumericExpression(unaryMinus.Operand, schema, out var unaryOperand))
        {
            term = BatchProjectionTerm.CreateNumericExpression(BatchNumericExpression.CreateNegated(unaryOperand));
            return true;
        }

        if (expression is BinaryExpression arithmetic &&
            IsArithmeticOp(arithmetic.Op) &&
            TryBindNumericExpression(arithmetic.Left, schema, out var leftOperand) &&
            TryBindNumericExpression(arithmetic.Right, schema, out var rightOperand))
        {
            term = BatchProjectionTerm.CreateNumericExpression(
                BatchNumericExpression.CreateArithmetic(arithmetic.Op, leftOperand, rightOperand));
            return true;
        }

        return false;
    }

    private static bool TryBindTextProjection(
        FunctionCallExpression function,
        TableSchema schema,
        out BatchProjectionTerm term)
    {
        term = default;
        if (!string.Equals(function.FunctionName, "TEXT", StringComparison.OrdinalIgnoreCase) ||
            function.IsStarArg ||
            function.IsDistinct ||
            function.Arguments.Count != 1)
        {
            return false;
        }

        Expression argument = function.Arguments[0];
        if (TryResolveColumnIndex(argument, schema, out int columnIndex))
        {
            term = BatchProjectionTerm.CreateTextColumn(columnIndex);
            return true;
        }

        if (TryBindNumericExpression(argument, schema, out var numericExpression))
        {
            term = BatchProjectionTerm.CreateTextNumericExpression(numericExpression);
            return true;
        }

        if (TryCreateLiteral(argument, out DbValue literalValue))
        {
            term = BatchProjectionTerm.CreateConstant(ScalarFunctionEvaluator.EvaluateTextValue(literalValue));
            return true;
        }

        return false;
    }

    private static bool TryBindNumericExpression(Expression expression, TableSchema schema, out BatchNumericExpression operand)
    {
        operand = null!;
        expression = CollationSupport.StripCollation(expression);

        if (TryResolveColumnIndex(expression, schema, out int columnIndex))
        {
            if (!IsNumericType(schema.Columns[columnIndex].Type))
                return false;

            operand = BatchNumericExpression.CreateColumn(columnIndex);
            return true;
        }

        if (expression is LiteralExpression literal &&
            TryCreateLiteral(literal, out DbValue literalValue) &&
            IsNumericType(literalValue.Type))
        {
            operand = BatchNumericExpression.CreateConstant(literalValue);
            return true;
        }

        if (expression is UnaryExpression { Op: TokenType.Minus } unaryMinus &&
            TryBindNumericExpression(unaryMinus.Operand, schema, out var innerOperand))
        {
            operand = BatchNumericExpression.CreateNegated(innerOperand);
            return true;
        }

        if (expression is BinaryExpression arithmetic &&
            IsArithmeticOp(arithmetic.Op) &&
            TryBindNumericExpression(arithmetic.Left, schema, out var leftOperand) &&
            TryBindNumericExpression(arithmetic.Right, schema, out var rightOperand))
        {
            operand = BatchNumericExpression.CreateArithmetic(arithmetic.Op, leftOperand, rightOperand);
            return true;
        }

        return false;
    }

    private static bool TryResolveColumnIndex(Expression expression, TableSchema schema, out int columnIndex)
    {
        columnIndex = -1;
        expression = CollationSupport.StripCollation(expression);
        if (expression is not ColumnRefExpression columnRef)
            return false;

        columnIndex = columnRef.TableAlias != null
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);
        return columnIndex >= 0;
    }

    private static bool TryCreateLiteral(Expression expression, out DbValue value)
    {
        expression = CollationSupport.StripCollation(expression);
        if (expression is not LiteralExpression literal)
        {
            value = DbValue.Null;
            return false;
        }

        return TryCreateLiteral(literal, out value);
    }

    private static bool TryCreateLiteral(LiteralExpression literal, out DbValue value)
    {
        value = DbValue.Null;
        if (literal.Value == null || literal.LiteralType == TokenType.Null)
            return true;

        switch (literal.LiteralType)
        {
            case TokenType.IntegerLiteral:
                value = DbValue.FromInteger((long)literal.Value);
                return true;
            case TokenType.RealLiteral:
                value = DbValue.FromReal((double)literal.Value);
                return true;
            case TokenType.StringLiteral:
                value = DbValue.FromText((string)literal.Value);
                return true;
            default:
                return false;
        }
    }

    private static bool IsComparisonOp(BinaryOp op)
        => op is BinaryOp.Equals or BinaryOp.NotEquals or BinaryOp.LessThan or BinaryOp.GreaterThan or BinaryOp.LessOrEqual or BinaryOp.GreaterOrEqual;

    private static bool IsArithmeticOp(BinaryOp op)
        => op is BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide;

    private static bool IsNumericType(DbType type)
        => type is DbType.Integer or DbType.Real;

    private static BinaryOp ReverseComparison(BinaryOp op)
        => op switch
        {
            BinaryOp.LessThan => BinaryOp.GreaterThan,
            BinaryOp.GreaterThan => BinaryOp.LessThan,
            BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
            BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual,
            _ => op,
        };

    private static BatchPushdownFilter[] CreatePushdownFilters(BatchPredicateExpression? predicate)
    {
        if (predicate == null)
            return Array.Empty<BatchPushdownFilter>();

        var filters = new List<BatchPushdownFilter>();
        if (!predicate.TryAppendPushdownFilters(filters))
            return Array.Empty<BatchPushdownFilter>();

        return filters.Count == 0 ? Array.Empty<BatchPushdownFilter>() : filters.ToArray();
    }
}

internal sealed class SpecializedFilterProjectionBatchPlan : IFilterProjectionBatchPlan
{
    private readonly BatchPredicateExpression? _predicate;
    private readonly BatchPredicateTerm[]? _conjunctiveTerms;
    private readonly BatchProjectionTerm[] _projections;
    private readonly int[]? _directProjectionColumns;
    private readonly bool _isPassthroughProjection;
    private readonly BatchPushdownFilter[] _pushdownFilters;

    public SpecializedFilterProjectionBatchPlan(
        BatchPredicateExpression? predicate,
        BatchProjectionTerm[] projections,
        BatchPushdownFilter[] pushdownFilters)
    {
        _predicate = predicate;
        _conjunctiveTerms = TryFlattenConjunctiveTerms(predicate);
        _projections = projections ?? throw new ArgumentNullException(nameof(projections));
        _directProjectionColumns = TryGetDirectProjectionColumns(_projections);
        _isPassthroughProjection = IsPassthroughProjection(_directProjectionColumns, _projections.Length);
        _pushdownFilters = pushdownFilters ?? Array.Empty<BatchPushdownFilter>();
    }

    public int OutputColumnCount => _projections.Length;
    public BatchPushdownFilter[] PushdownFilters => _pushdownFilters;

    public int Execute(RowBatch sourceBatch, RowSelection selection, RowBatch destination)
    {
        ArgumentNullException.ThrowIfNull(selection);
        ArgumentNullException.ThrowIfNull(destination);

        if (destination.ColumnCount != _projections.Length)
        {
            throw new ArgumentException(
                "Destination batch width does not match plan output width.",
                nameof(destination));
        }

        selection.Reset();
        destination.Reset();

        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            var row = sourceBatch.GetRowSpan(rowIndex);
            if (!MatchesPredicates(row))
                continue;

            selection.Add(rowIndex);
            int destinationRowIndex = destination.Count;
            Span<DbValue> destinationRow = destination.GetWritableRowSpan(destinationRowIndex);
            WriteProjectedRow(row, destinationRow);
            destination.CommitWrittenRow(destinationRowIndex);
        }

        return destination.Count;
    }

    private void WriteProjectedRow(ReadOnlySpan<DbValue> sourceRow, Span<DbValue> destinationRow)
    {
        if (_isPassthroughProjection)
        {
            int copiedCount = Math.Min(sourceRow.Length, destinationRow.Length);
            if (copiedCount > 0)
                sourceRow[..copiedCount].CopyTo(destinationRow);

            if (copiedCount < destinationRow.Length)
                destinationRow[copiedCount..].Fill(DbValue.Null);
            return;
        }

        if (_directProjectionColumns is { Length: > 0 } directProjectionColumns)
        {
            for (int projectionIndex = 0; projectionIndex < directProjectionColumns.Length; projectionIndex++)
            {
                int sourceColumnIndex = directProjectionColumns[projectionIndex];
                destinationRow[projectionIndex] = (uint)sourceColumnIndex < (uint)sourceRow.Length
                    ? sourceRow[sourceColumnIndex]
                    : DbValue.Null;
            }
            return;
        }

        for (int projectionIndex = 0; projectionIndex < _projections.Length; projectionIndex++)
            destinationRow[projectionIndex] = _projections[projectionIndex].Evaluate(sourceRow);
    }

    private bool MatchesPredicates(ReadOnlySpan<DbValue> row)
    {
        if (_conjunctiveTerms is { Length: > 0 } conjunctiveTerms)
        {
            for (int i = 0; i < conjunctiveTerms.Length; i++)
            {
                if (!conjunctiveTerms[i].Evaluate(row))
                    return false;
            }

            return true;
        }

        return _predicate == null || _predicate.Evaluate(row);
    }

    private static BatchPredicateTerm[]? TryFlattenConjunctiveTerms(BatchPredicateExpression? predicate)
    {
        if (predicate == null)
            return Array.Empty<BatchPredicateTerm>();

        var terms = new List<BatchPredicateTerm>();
        return predicate.TryCollectConjunctiveTerms(terms)
            ? terms.ToArray()
            : null;
    }

    private static int[]? TryGetDirectProjectionColumns(BatchProjectionTerm[] projections)
    {
        if (projections.Length == 0)
            return Array.Empty<int>();

        var directColumns = new int[projections.Length];
        for (int i = 0; i < projections.Length; i++)
        {
            if (!projections[i].TryGetDirectColumnIndex(out directColumns[i]))
                return null;
        }

        return directColumns;
    }

    private static bool IsPassthroughProjection(int[]? directProjectionColumns, int projectionCount)
    {
        if (directProjectionColumns == null || directProjectionColumns.Length != projectionCount)
            return false;

        for (int i = 0; i < directProjectionColumns.Length; i++)
        {
            if (directProjectionColumns[i] != i)
                return false;
        }

        return true;
    }
}

internal sealed class BatchPredicateExpression
{
    private readonly BatchPredicateTerm _term;
    private readonly BatchPredicateExpression? _left;
    private readonly BatchPredicateExpression? _right;
    private readonly BatchPredicateExpressionKind _kind;

    private BatchPredicateExpression(
        BatchPredicateTerm term,
        BatchPredicateExpression? left,
        BatchPredicateExpression? right,
        BatchPredicateExpressionKind kind)
    {
        _term = term;
        _left = left;
        _right = right;
        _kind = kind;
    }

    public static BatchPredicateExpression CreateLeaf(BatchPredicateTerm term)
        => new(term, null, null, BatchPredicateExpressionKind.Leaf);

    public static BatchPredicateExpression CreateAnd(BatchPredicateExpression left, BatchPredicateExpression right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new(default, left, right, BatchPredicateExpressionKind.And);
    }

    public static BatchPredicateExpression CreateOr(BatchPredicateExpression left, BatchPredicateExpression right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new(default, left, right, BatchPredicateExpressionKind.Or);
    }

    public static BatchPredicateExpression CreateNot(BatchPredicateExpression operand)
    {
        ArgumentNullException.ThrowIfNull(operand);
        return new(default, operand, null, BatchPredicateExpressionKind.Not);
    }

    public bool Evaluate(ReadOnlySpan<DbValue> row)
    {
        return _kind switch
        {
            BatchPredicateExpressionKind.Leaf => _term.Evaluate(row),
            BatchPredicateExpressionKind.And => _left != null && _right != null && _left.Evaluate(row) && _right.Evaluate(row),
            BatchPredicateExpressionKind.Or => _left != null && _right != null && (_left.Evaluate(row) || _right.Evaluate(row)),
            BatchPredicateExpressionKind.Not => _left != null && !_left.Evaluate(row),
            _ => false,
        };
    }

    public bool TryCollectConjunctiveTerms(List<BatchPredicateTerm> terms)
    {
        ArgumentNullException.ThrowIfNull(terms);

        switch (_kind)
        {
            case BatchPredicateExpressionKind.Leaf:
                terms.Add(_term);
                return true;
            case BatchPredicateExpressionKind.And:
                return _left != null &&
                    _right != null &&
                    _left.TryCollectConjunctiveTerms(terms) &&
                    _right.TryCollectConjunctiveTerms(terms);
            default:
                return false;
        }
    }

    public bool TryAppendPushdownFilters(List<BatchPushdownFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);

        int originalCount = filters.Count;
        switch (_kind)
        {
            case BatchPredicateExpressionKind.Leaf:
                _term.AppendPushdownFilters(filters);
                return true;
            case BatchPredicateExpressionKind.And:
                if (_left != null && _right != null &&
                    _left.TryAppendPushdownFilters(filters) &&
                    _right.TryAppendPushdownFilters(filters))
                {
                    return true;
                }

                break;
        }

        if (filters.Count > originalCount)
            filters.RemoveRange(originalCount, filters.Count - originalCount);

        return false;
    }
}

internal readonly struct BatchPredicateTerm
{
    private readonly int _columnIndex;
    private readonly BinaryOp _op;
    private readonly long _integerLiteral;
    private readonly long _integerUpperLiteral;
    private readonly double _numericLiteral;
    private readonly double _numericUpperLiteral;
    private readonly string? _textLiteral;
    private readonly string? _textUpperLiteral;
    private readonly long[]? _integerSet;
    private readonly double[]? _numericSet;
    private readonly string[]? _textSet;
    private readonly char _escapeChar;
    private readonly bool _hasEscapeChar;
    private readonly bool _hasNullSetValue;
    private readonly string? _textCollation;
    private readonly BatchProjectionTerm _projectionTerm;
    private readonly DbValue _comparisonLiteral;
    private readonly BatchPredicateKind _kind;
    private readonly bool _negated;

    private BatchPredicateTerm(
        int columnIndex,
        BinaryOp op,
        long integerLiteral,
        long integerUpperLiteral,
        double numericLiteral,
        double numericUpperLiteral,
        string? textLiteral,
        string? textUpperLiteral,
        long[]? integerSet,
        double[]? numericSet,
        string[]? textSet,
        char escapeChar,
        bool hasEscapeChar,
        bool hasNullSetValue,
        string? textCollation,
        BatchPredicateKind kind,
        bool negated)
    {
        _columnIndex = columnIndex;
        _op = op;
        _integerLiteral = integerLiteral;
        _integerUpperLiteral = integerUpperLiteral;
        _numericLiteral = numericLiteral;
        _numericUpperLiteral = numericUpperLiteral;
        _textLiteral = textLiteral;
        _textUpperLiteral = textUpperLiteral;
        _integerSet = integerSet;
        _numericSet = numericSet;
        _textSet = textSet;
        _escapeChar = escapeChar;
        _hasEscapeChar = hasEscapeChar;
        _hasNullSetValue = hasNullSetValue;
        _textCollation = CollationSupport.NormalizeMetadataName(textCollation);
        _projectionTerm = default;
        _comparisonLiteral = DbValue.Null;
        _kind = kind;
        _negated = negated;
    }

    private BatchPredicateTerm(
        BatchProjectionTerm projectionTerm,
        BinaryOp op,
        DbValue comparisonLiteral,
        BatchPredicateKind kind)
    {
        _columnIndex = 0;
        _op = op;
        _integerLiteral = 0;
        _integerUpperLiteral = 0;
        _numericLiteral = 0;
        _numericUpperLiteral = 0;
        _textLiteral = null;
        _textUpperLiteral = null;
        _integerSet = null;
        _numericSet = null;
        _textSet = null;
        _escapeChar = '\0';
        _hasEscapeChar = false;
        _hasNullSetValue = false;
        _textCollation = null;
        _projectionTerm = projectionTerm;
        _comparisonLiteral = comparisonLiteral;
        _kind = kind;
        _negated = false;
    }

    private BatchPredicateTerm(
        BatchProjectionTerm projectionTerm,
        double numericLiteral,
        double numericUpperLiteral,
        double[]? numericSet,
        BatchPredicateKind kind,
        bool negated,
        bool hasNullSetValue = false)
    {
        _columnIndex = 0;
        _op = BinaryOp.Equals;
        _integerLiteral = 0;
        _integerUpperLiteral = 0;
        _numericLiteral = numericLiteral;
        _numericUpperLiteral = numericUpperLiteral;
        _textLiteral = null;
        _textUpperLiteral = null;
        _integerSet = null;
        _numericSet = numericSet;
        _textSet = null;
        _escapeChar = '\0';
        _hasEscapeChar = false;
        _hasNullSetValue = hasNullSetValue;
        _textCollation = null;
        _projectionTerm = projectionTerm;
        _comparisonLiteral = DbValue.Null;
        _kind = kind;
        _negated = negated;
    }

    private BatchPredicateTerm(
        BatchProjectionTerm projectionTerm,
        BinaryOp op,
        string textLiteral,
        string? textCollation,
        BatchPredicateKind kind)
    {
        _columnIndex = 0;
        _op = op;
        _integerLiteral = 0;
        _integerUpperLiteral = 0;
        _numericLiteral = 0;
        _numericUpperLiteral = 0;
        _textLiteral = NormalizeTextLiteral(textLiteral, textCollation);
        _textUpperLiteral = null;
        _integerSet = null;
        _numericSet = null;
        _textSet = null;
        _escapeChar = '\0';
        _hasEscapeChar = false;
        _hasNullSetValue = false;
        _textCollation = CollationSupport.NormalizeMetadataName(textCollation);
        _projectionTerm = projectionTerm;
        _comparisonLiteral = DbValue.Null;
        _kind = kind;
        _negated = false;
    }

    private BatchPredicateTerm(
        BatchProjectionTerm projectionTerm,
        string? textLiteral,
        string? textUpperLiteral,
        string[]? textSet,
        char escapeChar,
        bool hasEscapeChar,
        bool hasNullSetValue,
        string? textCollation,
        BatchPredicateKind kind,
        bool negated)
    {
        _columnIndex = 0;
        _op = BinaryOp.Equals;
        _integerLiteral = 0;
        _integerUpperLiteral = 0;
        _numericLiteral = 0;
        _numericUpperLiteral = 0;
        _textLiteral = textLiteral != null ? NormalizeTextLiteral(textLiteral, textCollation) : null;
        _textUpperLiteral = textUpperLiteral != null ? NormalizeTextLiteral(textUpperLiteral, textCollation) : null;
        _integerSet = null;
        _numericSet = null;
        _textSet = textSet != null ? NormalizeTextLiterals(textSet, textCollation) : null;
        _escapeChar = escapeChar;
        _hasEscapeChar = hasEscapeChar;
        _hasNullSetValue = hasNullSetValue;
        _textCollation = CollationSupport.NormalizeMetadataName(textCollation);
        _projectionTerm = projectionTerm;
        _comparisonLiteral = DbValue.Null;
        _kind = kind;
        _negated = negated;
    }

    public static BatchPredicateTerm CreateIntegerCompare(int columnIndex, BinaryOp op, long integerLiteral)
        => new(columnIndex, op, integerLiteral, 0, 0, 0, null, null, null, null, null, '\0', false, false, null, BatchPredicateKind.IntegerCompare, negated: false);

    public static BatchPredicateTerm CreateNumericCompare(int columnIndex, BinaryOp op, double numericLiteral)
        => new(columnIndex, op, 0, 0, numericLiteral, 0, null, null, null, null, null, '\0', false, false, null, BatchPredicateKind.NumericCompare, negated: false);

    public static BatchPredicateTerm CreateTextCompare(int columnIndex, BinaryOp op, string textLiteral, string? textCollation)
        => new(columnIndex, op, 0, 0, 0, 0, NormalizeTextLiteral(textLiteral, textCollation), null, null, null, null, '\0', false, false, textCollation, BatchPredicateKind.TextCompare, negated: false);

    public static BatchPredicateTerm CreateTextifiedCompare(int columnIndex, BinaryOp op, string textLiteral, string? textCollation)
        => new(columnIndex, op, 0, 0, 0, 0, NormalizeTextLiteral(textLiteral, textCollation), null, null, null, null, '\0', false, false, textCollation, BatchPredicateKind.TextifiedCompare, negated: false);

    public static BatchPredicateTerm CreateTextExpressionCompare(BatchProjectionTerm projectionTerm, BinaryOp op, string textLiteral, string? textCollation)
        => new(projectionTerm, op, textLiteral, textCollation, BatchPredicateKind.TextExpressionCompare);

    public static BatchPredicateTerm CreateNumericExpressionCompare(BatchProjectionTerm projectionTerm, BinaryOp op, DbValue comparisonLiteral)
        => new(projectionTerm, op, comparisonLiteral, BatchPredicateKind.NumericExpressionCompare);

    public static BatchPredicateTerm CreateNumericExpressionIn(BatchProjectionTerm projectionTerm, double[] values, bool negated, bool hasNullSetValue)
        => new(projectionTerm, 0, 0, values, BatchPredicateKind.NumericExpressionIn, negated, hasNullSetValue);

    public static BatchPredicateTerm CreateTextExpressionIn(BatchProjectionTerm projectionTerm, string[] values, bool negated, bool hasNullSetValue, string? textCollation)
        => new(projectionTerm, null, null, values, '\0', false, hasNullSetValue, textCollation, BatchPredicateKind.TextExpressionIn, negated);

    public static BatchPredicateTerm CreateTextifiedIn(int columnIndex, string[] values, bool negated, bool hasNullSetValue, string? textCollation)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, null, NormalizeTextLiterals(values, textCollation), '\0', false, hasNullSetValue, textCollation, BatchPredicateKind.TextifiedIn, negated);

    public static BatchPredicateTerm CreateIntegerIn(int columnIndex, long[] values, bool negated, bool hasNullSetValue)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, values, null, null, '\0', false, hasNullSetValue, null, BatchPredicateKind.IntegerIn, negated);

    public static BatchPredicateTerm CreateNumericIn(int columnIndex, double[] values, bool negated, bool hasNullSetValue)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, values, null, '\0', false, hasNullSetValue, null, BatchPredicateKind.NumericIn, negated);

    public static BatchPredicateTerm CreateTextIn(int columnIndex, string[] values, bool negated, bool hasNullSetValue, string? textCollation)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, null, NormalizeTextLiterals(values, textCollation), '\0', false, hasNullSetValue, textCollation, BatchPredicateKind.TextIn, negated);

    public static BatchPredicateTerm CreateIntegerRange(int columnIndex, long lowerInclusive, long upperInclusive, bool negated)
        => new(columnIndex, BinaryOp.Equals, lowerInclusive, upperInclusive, 0, 0, null, null, null, null, null, '\0', false, false, null, BatchPredicateKind.IntegerRange, negated);

    public static BatchPredicateTerm CreateNumericRange(int columnIndex, double lowerInclusive, double upperInclusive, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, lowerInclusive, upperInclusive, null, null, null, null, null, '\0', false, false, null, BatchPredicateKind.NumericRange, negated);

    public static BatchPredicateTerm CreateNumericExpressionRange(BatchProjectionTerm projectionTerm, double lowerInclusive, double upperInclusive, bool negated)
        => new(projectionTerm, lowerInclusive, upperInclusive, null, BatchPredicateKind.NumericExpressionRange, negated);

    public static BatchPredicateTerm CreateTextRange(int columnIndex, string lowerInclusive, string upperInclusive, bool negated, string? textCollation)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, NormalizeTextLiteral(lowerInclusive, textCollation), NormalizeTextLiteral(upperInclusive, textCollation), null, null, null, '\0', false, false, textCollation, BatchPredicateKind.TextRange, negated);

    public static BatchPredicateTerm CreateTextifiedRange(int columnIndex, string lowerInclusive, string upperInclusive, bool negated, string? textCollation)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, NormalizeTextLiteral(lowerInclusive, textCollation), NormalizeTextLiteral(upperInclusive, textCollation), null, null, null, '\0', false, false, textCollation, BatchPredicateKind.TextifiedRange, negated);

    public static BatchPredicateTerm CreateTextExpressionRange(BatchProjectionTerm projectionTerm, string lowerInclusive, string upperInclusive, bool negated, string? textCollation)
        => new(projectionTerm, lowerInclusive, upperInclusive, null, '\0', false, false, textCollation, BatchPredicateKind.TextExpressionRange, negated);

    public static BatchPredicateTerm CreateLike(int columnIndex, string pattern, char? escapeChar, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, pattern, null, null, null, null, escapeChar.GetValueOrDefault(), escapeChar.HasValue, false, null, BatchPredicateKind.LikeMatch, negated);

    public static BatchPredicateTerm CreateTextifiedLike(int columnIndex, string pattern, char? escapeChar, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, pattern, null, null, null, null, escapeChar.GetValueOrDefault(), escapeChar.HasValue, false, null, BatchPredicateKind.TextifiedLikeMatch, negated);

    public static BatchPredicateTerm CreateTextExpressionLike(BatchProjectionTerm projectionTerm, string pattern, char? escapeChar, bool negated)
        => new(projectionTerm, pattern, null, null, escapeChar.GetValueOrDefault(), escapeChar.HasValue, false, null, BatchPredicateKind.TextExpressionLikeMatch, negated);

    public static BatchPredicateTerm CreateNullCheck(int columnIndex, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, null, null, '\0', false, false, null, BatchPredicateKind.NullCheck, negated);

    public void AppendPushdownFilters(List<BatchPushdownFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);

        switch (_kind)
        {
            case BatchPredicateKind.IntegerCompare:
                filters.Add(new BatchPushdownFilter(_columnIndex, _op, DbValue.FromInteger(_integerLiteral)));
                break;

            case BatchPredicateKind.NumericCompare:
                filters.Add(new BatchPushdownFilter(_columnIndex, _op, DbValue.FromReal(_numericLiteral)));
                break;

            case BatchPredicateKind.TextCompare when _textLiteral != null && CollationSupport.IsBinaryOrDefault(_textCollation):
                filters.Add(new BatchPushdownFilter(_columnIndex, _op, DbValue.FromText(_textLiteral)));
                break;

            case BatchPredicateKind.IntegerIn when !_negated && !_hasNullSetValue && _integerSet != null:
                filters.Add(BatchPushdownFilter.CreateIntegerIn(_columnIndex, _integerSet));
                break;

            case BatchPredicateKind.NumericIn when !_negated && !_hasNullSetValue && _numericSet != null:
                filters.Add(BatchPushdownFilter.CreateNumericIn(_columnIndex, _numericSet));
                break;

            case BatchPredicateKind.TextIn when !_negated && !_hasNullSetValue && CollationSupport.IsBinaryOrDefault(_textCollation) && _textSet != null:
                filters.Add(BatchPushdownFilter.CreateTextIn(_columnIndex, _textSet));
                break;

            case BatchPredicateKind.IntegerRange when !_negated:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromInteger(_integerLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromInteger(_integerUpperLiteral)));
                break;

            case BatchPredicateKind.NumericRange when !_negated:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromReal(_numericLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromReal(_numericUpperLiteral)));
                break;

            case BatchPredicateKind.TextRange when !_negated && CollationSupport.IsBinaryOrDefault(_textCollation) && _textLiteral != null && _textUpperLiteral != null:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromText(_textLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromText(_textUpperLiteral)));
                break;
        }
    }

    public bool Evaluate(ReadOnlySpan<DbValue> row)
    {
        DbValue value = (uint)_columnIndex < (uint)row.Length
            ? row[_columnIndex]
            : DbValue.Null;
        return _kind switch
        {
            BatchPredicateKind.NullCheck => _negated ? !value.IsNull : value.IsNull,
            BatchPredicateKind.IntegerCompare => EvaluateIntegerCompare(value),
            BatchPredicateKind.NumericCompare => EvaluateNumericCompare(value),
            BatchPredicateKind.TextCompare => EvaluateTextCompare(value),
            BatchPredicateKind.TextifiedCompare => EvaluateTextifiedCompare(value),
            BatchPredicateKind.TextExpressionCompare => EvaluateTextExpressionCompare(row),
            BatchPredicateKind.NumericExpressionCompare => EvaluateNumericExpressionCompare(row),
            BatchPredicateKind.IntegerIn => EvaluateIntegerIn(value),
            BatchPredicateKind.NumericIn => EvaluateNumericIn(value),
            BatchPredicateKind.NumericExpressionIn => EvaluateNumericExpressionIn(row),
            BatchPredicateKind.TextIn => EvaluateTextIn(value),
            BatchPredicateKind.TextifiedIn => EvaluateTextifiedIn(value),
            BatchPredicateKind.TextExpressionIn => EvaluateTextExpressionIn(row),
            BatchPredicateKind.IntegerRange => EvaluateIntegerRange(value),
            BatchPredicateKind.NumericRange => EvaluateNumericRange(value),
            BatchPredicateKind.NumericExpressionRange => EvaluateNumericExpressionRange(row),
            BatchPredicateKind.TextRange => EvaluateTextRange(value),
            BatchPredicateKind.TextifiedRange => EvaluateTextifiedRange(value),
            BatchPredicateKind.TextExpressionRange => EvaluateTextExpressionRange(row),
            BatchPredicateKind.LikeMatch => EvaluateLike(value),
            BatchPredicateKind.TextifiedLikeMatch => EvaluateTextifiedLike(value),
            BatchPredicateKind.TextExpressionLikeMatch => EvaluateTextExpressionLike(row),
            _ => false,
        };
    }

    private bool EvaluateIntegerCompare(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Integer)
            return false;

        long actual = value.AsInteger;
        return _op switch
        {
            BinaryOp.Equals => actual == _integerLiteral,
            BinaryOp.NotEquals => actual != _integerLiteral,
            BinaryOp.LessThan => actual < _integerLiteral,
            BinaryOp.GreaterThan => actual > _integerLiteral,
            BinaryOp.LessOrEqual => actual <= _integerLiteral,
            BinaryOp.GreaterOrEqual => actual >= _integerLiteral,
            _ => false,
        };
    }

    private bool EvaluateNumericCompare(DbValue value)
    {
        if (value.IsNull || (value.Type is not DbType.Integer and not DbType.Real))
            return false;

        double actual = value.AsReal;
        return _op switch
        {
            BinaryOp.Equals => actual == _numericLiteral,
            BinaryOp.NotEquals => actual != _numericLiteral,
            BinaryOp.LessThan => actual < _numericLiteral,
            BinaryOp.GreaterThan => actual > _numericLiteral,
            BinaryOp.LessOrEqual => actual <= _numericLiteral,
            BinaryOp.GreaterOrEqual => actual >= _numericLiteral,
            _ => false,
        };
    }

    private bool EvaluateNumericExpressionCompare(ReadOnlySpan<DbValue> row)
    {
        if (_comparisonLiteral.IsNull)
            return false;

        DbValue actual = _projectionTerm.Evaluate(row);
        if (actual.IsNull || !IsNumericComparable(actual))
            return false;

        int compare = DbValue.Compare(actual, _comparisonLiteral);
        return _op switch
        {
            BinaryOp.Equals => compare == 0,
            BinaryOp.NotEquals => compare != 0,
            BinaryOp.LessThan => compare < 0,
            BinaryOp.GreaterThan => compare > 0,
            BinaryOp.LessOrEqual => compare <= 0,
            BinaryOp.GreaterOrEqual => compare >= 0,
            _ => false,
        };
    }

    private bool EvaluateTextExpressionCompare(ReadOnlySpan<DbValue> row)
    {
        if (_textLiteral == null)
            return false;

        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || actualValue.Type != DbType.Text)
            return false;

        string actual = NormalizeTextLiteral(actualValue.AsText, _textCollation);
        int compare = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        return _op switch
        {
            BinaryOp.Equals => compare == 0,
            BinaryOp.NotEquals => compare != 0,
            BinaryOp.LessThan => compare < 0,
            BinaryOp.GreaterThan => compare > 0,
            BinaryOp.LessOrEqual => compare <= 0,
            BinaryOp.GreaterOrEqual => compare >= 0,
            _ => false,
        };
    }

    private bool EvaluateTextCompare(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textLiteral == null)
            return false;

        string actual = NormalizeTextLiteral(value.AsText, _textCollation);
        int compare = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        return _op switch
        {
            BinaryOp.Equals => compare == 0,
            BinaryOp.NotEquals => compare != 0,
            BinaryOp.LessThan => compare < 0,
            BinaryOp.GreaterThan => compare > 0,
            BinaryOp.LessOrEqual => compare <= 0,
            BinaryOp.GreaterOrEqual => compare >= 0,
            _ => false,
        };
    }

    private bool EvaluateTextifiedCompare(DbValue value)
    {
        if (_textLiteral == null)
            return false;

        string actual = NormalizeTextLiteral(
            ScalarFunctionEvaluator.EvaluateTextValue(value).AsText,
            _textCollation);
        int compare = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        return _op switch
        {
            BinaryOp.Equals => compare == 0,
            BinaryOp.NotEquals => compare != 0,
            BinaryOp.LessThan => compare < 0,
            BinaryOp.GreaterThan => compare > 0,
            BinaryOp.LessOrEqual => compare <= 0,
            BinaryOp.GreaterOrEqual => compare >= 0,
            _ => false,
        };
    }

    private bool EvaluateIntegerIn(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Integer || _integerSet == null)
            return false;

        long actual = value.AsInteger;
        for (int i = 0; i < _integerSet.Length; i++)
        {
            if (actual == _integerSet[i])
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateNumericIn(DbValue value)
    {
        if (value.IsNull || (value.Type is not DbType.Integer and not DbType.Real) || _numericSet == null)
            return false;

        double actual = value.AsReal;
        for (int i = 0; i < _numericSet.Length; i++)
        {
            if (actual == _numericSet[i])
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateTextIn(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textSet == null)
            return false;

        string actual = NormalizeTextLiteral(value.AsText, _textCollation);
        for (int i = 0; i < _textSet.Length; i++)
        {
            if (string.Equals(actual, _textSet[i], StringComparison.Ordinal))
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateTextifiedIn(DbValue value)
    {
        if (_textSet == null)
            return false;

        string actual = NormalizeTextLiteral(
            ScalarFunctionEvaluator.EvaluateTextValue(value).AsText,
            _textCollation);
        for (int i = 0; i < _textSet.Length; i++)
        {
            if (string.Equals(actual, _textSet[i], StringComparison.Ordinal))
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateNumericExpressionIn(ReadOnlySpan<DbValue> row)
    {
        if (_numericSet == null)
            return false;

        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || !IsNumericComparable(actualValue))
            return false;

        double actual = actualValue.AsReal;
        for (int i = 0; i < _numericSet.Length; i++)
        {
            if (actual == _numericSet[i])
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateTextExpressionIn(ReadOnlySpan<DbValue> row)
    {
        if (_textSet == null)
            return false;

        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || actualValue.Type != DbType.Text)
            return false;

        string actual = NormalizeTextLiteral(actualValue.AsText, _textCollation);
        for (int i = 0; i < _textSet.Length; i++)
        {
            if (string.Equals(actual, _textSet[i], StringComparison.Ordinal))
                return !_negated;
        }

        return _negated && !_hasNullSetValue;
    }

    private bool EvaluateIntegerRange(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Integer)
            return false;

        bool inRange = value.AsInteger >= _integerLiteral && value.AsInteger <= _integerUpperLiteral;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateNumericRange(DbValue value)
    {
        if (value.IsNull || (value.Type is not DbType.Integer and not DbType.Real))
            return false;

        double actual = value.AsReal;
        bool inRange = actual >= _numericLiteral && actual <= _numericUpperLiteral;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateTextRange(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textLiteral == null || _textUpperLiteral == null)
            return false;

        string actual = NormalizeTextLiteral(value.AsText, _textCollation);
        int compareLow = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        int compareHigh = string.Compare(actual, _textUpperLiteral, StringComparison.Ordinal);
        bool inRange = compareLow >= 0 && compareHigh <= 0;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateTextifiedRange(DbValue value)
    {
        if (_textLiteral == null || _textUpperLiteral == null)
            return false;

        string actual = NormalizeTextLiteral(
            ScalarFunctionEvaluator.EvaluateTextValue(value).AsText,
            _textCollation);
        int compareLow = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        int compareHigh = string.Compare(actual, _textUpperLiteral, StringComparison.Ordinal);
        bool inRange = compareLow >= 0 && compareHigh <= 0;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateNumericExpressionRange(ReadOnlySpan<DbValue> row)
    {
        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || !IsNumericComparable(actualValue))
            return false;

        double actual = actualValue.AsReal;
        bool inRange = actual >= _numericLiteral && actual <= _numericUpperLiteral;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateTextExpressionRange(ReadOnlySpan<DbValue> row)
    {
        if (_textLiteral == null || _textUpperLiteral == null)
            return false;

        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || actualValue.Type != DbType.Text)
            return false;

        string actual = NormalizeTextLiteral(actualValue.AsText, _textCollation);
        int compareLow = string.Compare(actual, _textLiteral, StringComparison.Ordinal);
        int compareHigh = string.Compare(actual, _textUpperLiteral, StringComparison.Ordinal);
        bool inRange = compareLow >= 0 && compareHigh <= 0;
        return _negated ? !inRange : inRange;
    }

    private bool EvaluateLike(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textLiteral == null)
            return false;

        char? escape = _hasEscapeChar ? _escapeChar : null;
        bool match = ExpressionEvaluator.SqlLikeMatch(value.AsText, _textLiteral, escape);
        return _negated ? !match : match;
    }

    private bool EvaluateTextifiedLike(DbValue value)
    {
        if (_textLiteral == null)
            return false;

        char? escape = _hasEscapeChar ? _escapeChar : null;
        bool match = ExpressionEvaluator.SqlLikeMatch(
            ScalarFunctionEvaluator.EvaluateTextValue(value).AsText,
            _textLiteral,
            escape);
        return _negated ? !match : match;
    }

    private bool EvaluateTextExpressionLike(ReadOnlySpan<DbValue> row)
    {
        if (_textLiteral == null)
            return false;

        DbValue actualValue = _projectionTerm.Evaluate(row);
        if (actualValue.IsNull || actualValue.Type != DbType.Text)
            return false;

        char? escape = _hasEscapeChar ? _escapeChar : null;
        bool match = ExpressionEvaluator.SqlLikeMatch(actualValue.AsText, _textLiteral, escape);
        return _negated ? !match : match;
    }

    private static string NormalizeTextLiteral(string value, string? textCollation) =>
        CollationSupport.NormalizeText(value, textCollation);

    private static bool IsNumericComparable(DbValue value)
        => value.Type is DbType.Integer or DbType.Real;

    private static string[] NormalizeTextLiterals(string[] values, string? textCollation)
    {
        if (CollationSupport.IsBinaryOrDefault(textCollation))
            return values;

        var normalized = new string[values.Length];
        for (int i = 0; i < values.Length; i++)
            normalized[i] = NormalizeTextLiteral(values[i], textCollation);

        return normalized;
    }
}

internal sealed class SpecializedScalarAggregateBatchPlan : IScalarAggregateBatchPlan
{
    private readonly BatchPredicateExpression? _predicate;
    private readonly BatchPushdownFilter[] _pushdownFilters;
    private readonly BatchScalarAggregateKind _kind;
    private readonly int _columnIndex;
    private readonly bool _isCountStar;
    private readonly bool _isDistinct;
    private readonly SpanExpressionEvaluator? _aggregateArgumentEvaluator;

    private long _count;
    private double _sum;
    private bool _hasReal;
    private bool _hasAny;
    private DbValue? _best;
    private AggregateDistinctValueSet? _distinctValues;

    public SpecializedScalarAggregateBatchPlan(
        BatchPredicateExpression? predicate,
        BatchPushdownFilter[] pushdownFilters,
        BatchScalarAggregateKind kind,
        int columnIndex,
        bool isCountStar,
        bool isDistinct,
        SpanExpressionEvaluator? aggregateArgumentEvaluator = null)
    {
        _predicate = predicate;
        _pushdownFilters = pushdownFilters ?? Array.Empty<BatchPushdownFilter>();
        _kind = kind;
        _columnIndex = columnIndex;
        _isCountStar = isCountStar;
        _isDistinct = isDistinct;
        _aggregateArgumentEvaluator = aggregateArgumentEvaluator;
    }

    public BatchPushdownFilter[] PushdownFilters => _pushdownFilters;

    public void Reset()
    {
        _count = 0;
        _sum = 0;
        _hasReal = false;
        _hasAny = false;
        _best = null;
        _distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
    }

    public void Accumulate(RowBatch sourceBatch)
    {
        if (_isCountStar)
        {
            AccumulateCountStar(sourceBatch);
            return;
        }

        switch (_kind)
        {
            case BatchScalarAggregateKind.Count:
                AccumulateCount(sourceBatch);
                break;
            case BatchScalarAggregateKind.Sum:
            case BatchScalarAggregateKind.Avg:
                AccumulateNumeric(sourceBatch);
                break;
            case BatchScalarAggregateKind.Min:
                AccumulateMin(sourceBatch);
                break;
            case BatchScalarAggregateKind.Max:
                AccumulateMax(sourceBatch);
                break;
        }
    }

    public DbValue GetResult()
        => _kind switch
        {
            BatchScalarAggregateKind.Count => DbValue.FromInteger(_count),
            BatchScalarAggregateKind.Sum => !_hasAny ? DbValue.FromInteger(0)
                : _hasReal ? DbValue.FromReal(_sum) : DbValue.FromInteger((long)_sum),
            BatchScalarAggregateKind.Avg => !_hasAny ? DbValue.Null : DbValue.FromReal(_sum / _count),
            BatchScalarAggregateKind.Min => _best ?? DbValue.Null,
            BatchScalarAggregateKind.Max => _best ?? DbValue.Null,
            _ => DbValue.Null,
        };

    private void AccumulateCountStar(RowBatch sourceBatch)
    {
        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            if (MatchesPredicates(sourceBatch.GetRowSpan(rowIndex)))
                _count++;
        }
    }

    private void AccumulateCount(RowBatch sourceBatch)
    {
        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            var row = sourceBatch.GetRowSpan(rowIndex);
            if (!MatchesPredicates(row))
                continue;

            DbValue value = GetAggregateValue(row);
            if (!value.IsNull && IsDistinctValue(value))
                _count++;
        }
    }

    private void AccumulateNumeric(RowBatch sourceBatch)
    {
        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            var row = sourceBatch.GetRowSpan(rowIndex);
            if (!MatchesPredicates(row))
                continue;

            DbValue value = GetAggregateValue(row);
            if (value.IsNull)
                continue;
            if (!IsDistinctValue(value))
                continue;

            _hasAny = true;
            if (value.Type == DbType.Real)
            {
                _hasReal = true;
                _sum += value.AsReal;
            }
            else
            {
                _sum += value.AsInteger;
            }

            _count++;
        }
    }

    private void AccumulateMin(RowBatch sourceBatch)
    {
        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            var row = sourceBatch.GetRowSpan(rowIndex);
            if (!MatchesPredicates(row))
                continue;

            DbValue value = GetAggregateValue(row);
            if (value.IsNull)
                continue;
            if (!IsDistinctValue(value))
                continue;

            if (_best == null || DbValue.Compare(value, _best.Value) < 0)
                _best = value;
        }
    }

    private void AccumulateMax(RowBatch sourceBatch)
    {
        for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
        {
            var row = sourceBatch.GetRowSpan(rowIndex);
            if (!MatchesPredicates(row))
                continue;

            DbValue value = GetAggregateValue(row);
            if (value.IsNull)
                continue;
            if (!IsDistinctValue(value))
                continue;

            if (_best == null || DbValue.Compare(value, _best.Value) > 0)
                _best = value;
        }
    }

    private bool IsDistinctValue(DbValue value)
        => _distinctValues == null || _distinctValues.Add(value);

    private DbValue GetAggregateValue(ReadOnlySpan<DbValue> row)
    {
        if (_aggregateArgumentEvaluator != null)
            return _aggregateArgumentEvaluator(row);

        return (uint)_columnIndex < (uint)row.Length
            ? row[_columnIndex]
            : DbValue.Null;
    }

    private bool MatchesPredicates(ReadOnlySpan<DbValue> row)
        => _predicate == null || _predicate.Evaluate(row);
}

internal readonly struct BatchProjectionTerm
{
    private readonly int _columnIndex;
    private readonly DbValue _constant;
    private readonly BatchNumericExpression? _numericExpression;
    private readonly BatchProjectionKind _kind;

    private BatchProjectionTerm(
        int columnIndex,
        DbValue constant,
        BatchNumericExpression? numericExpression,
        BatchProjectionKind kind)
    {
        _columnIndex = columnIndex;
        _constant = constant;
        _numericExpression = numericExpression;
        _kind = kind;
    }

    public static BatchProjectionTerm CreateColumn(int columnIndex)
        => new(columnIndex, DbValue.Null, null, BatchProjectionKind.Column);

    public static BatchProjectionTerm CreateConstant(DbValue constant)
        => new(0, constant, null, BatchProjectionKind.Constant);

    public static BatchProjectionTerm CreateNumericExpression(BatchNumericExpression expression)
        => new(0, DbValue.Null, expression ?? throw new ArgumentNullException(nameof(expression)), BatchProjectionKind.NumericExpression);

    public static BatchProjectionTerm CreateTextColumn(int columnIndex)
        => new(columnIndex, DbValue.Null, null, BatchProjectionKind.TextColumn);

    public static BatchProjectionTerm CreateTextNumericExpression(BatchNumericExpression expression)
        => new(0, DbValue.Null, expression ?? throw new ArgumentNullException(nameof(expression)), BatchProjectionKind.TextNumericExpression);

    public bool TryGetDirectColumnIndex(out int columnIndex)
    {
        if (_kind == BatchProjectionKind.Column)
        {
            columnIndex = _columnIndex;
            return true;
        }

        columnIndex = -1;
        return false;
    }

    public DbValue Evaluate(ReadOnlySpan<DbValue> row)
    {
        return _kind switch
        {
            BatchProjectionKind.Column => (uint)_columnIndex < (uint)row.Length
                ? row[_columnIndex]
                : DbValue.Null,
            BatchProjectionKind.Constant => _constant,
            BatchProjectionKind.NumericExpression => EvaluateNumericExpression(row),
            BatchProjectionKind.TextColumn => ScalarFunctionEvaluator.EvaluateTextValue(
                (uint)_columnIndex < (uint)row.Length
                    ? row[_columnIndex]
                    : DbValue.Null),
            BatchProjectionKind.TextNumericExpression => EvaluateTextNumericExpression(row),
            _ => DbValue.Null,
        };
    }

    private DbValue EvaluateNumericExpression(ReadOnlySpan<DbValue> row)
    {
        if (_numericExpression == null ||
            !_numericExpression.TryGetValue(row, out DbValue value))
        {
            return DbValue.Null;
        }

        return value;
    }

    private DbValue EvaluateTextNumericExpression(ReadOnlySpan<DbValue> row)
        => ScalarFunctionEvaluator.EvaluateTextValue(EvaluateNumericExpression(row));
}

internal sealed class BatchNumericExpression
{
    private readonly int _columnIndex;
    private readonly DbValue _constant;
    private readonly BinaryOp _op;
    private readonly BatchNumericExpression? _left;
    private readonly BatchNumericExpression? _right;
    private readonly BatchNumericExpressionKind _kind;

    private BatchNumericExpression(
        int columnIndex,
        DbValue constant,
        BinaryOp op,
        BatchNumericExpression? left,
        BatchNumericExpression? right,
        BatchNumericExpressionKind kind)
    {
        _columnIndex = columnIndex;
        _constant = constant;
        _op = op;
        _left = left;
        _right = right;
        _kind = kind;
    }

    public static BatchNumericExpression CreateColumn(int columnIndex)
        => new(columnIndex, DbValue.Null, BinaryOp.Equals, null, null, BatchNumericExpressionKind.Column);

    public static BatchNumericExpression CreateConstant(DbValue constant)
        => new(0, constant, BinaryOp.Equals, null, null, BatchNumericExpressionKind.Constant);

    public static BatchNumericExpression CreateNegated(BatchNumericExpression operand)
    {
        ArgumentNullException.ThrowIfNull(operand);

        if (operand._kind == BatchNumericExpressionKind.Constant)
            return CreateConstant(Negate(operand._constant));

        return new(0, DbValue.Null, BinaryOp.Minus, CreateConstant(DbValue.FromInteger(0)), operand, BatchNumericExpressionKind.Arithmetic);
    }

    public static BatchNumericExpression CreateArithmetic(BinaryOp op, BatchNumericExpression left, BatchNumericExpression right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        return new(0, DbValue.Null, op, left, right, BatchNumericExpressionKind.Arithmetic);
    }

    public bool TryGetValue(ReadOnlySpan<DbValue> row, out DbValue value)
    {
        value = DbValue.Null;
        switch (_kind)
        {
            case BatchNumericExpressionKind.Constant:
                value = _constant;
                return true;
            case BatchNumericExpressionKind.Column:
                if ((uint)_columnIndex >= (uint)row.Length)
                    return false;

                DbValue cell = row[_columnIndex];
                if (cell.IsNull || (cell.Type is not DbType.Integer and not DbType.Real))
                    return false;

                value = cell;
                return true;
            case BatchNumericExpressionKind.Arithmetic:
                if (_left == null ||
                    _right == null ||
                    !_left.TryGetValue(row, out DbValue left) ||
                    !_right.TryGetValue(row, out DbValue right))
                {
                    return false;
                }

                value = EvaluateArithmetic(_op, left, right);
                return true;
            default:
                return false;
        }
    }

    private static DbValue EvaluateArithmetic(BinaryOp op, DbValue left, DbValue right)
    {
        if (left.Type == DbType.Real || right.Type == DbType.Real)
        {
            double leftReal = left.AsReal;
            double rightReal = right.AsReal;
            return op switch
            {
                BinaryOp.Plus => DbValue.FromReal(leftReal + rightReal),
                BinaryOp.Minus => DbValue.FromReal(leftReal - rightReal),
                BinaryOp.Multiply => DbValue.FromReal(leftReal * rightReal),
                BinaryOp.Divide => rightReal != 0
                    ? DbValue.FromReal(leftReal / rightReal)
                    : throw new CSharpDbException(ErrorCode.Unknown, "Division by zero."),
                _ => DbValue.Null,
            };
        }

        long leftInteger = left.AsInteger;
        long rightInteger = right.AsInteger;
        return op switch
        {
            BinaryOp.Plus => DbValue.FromInteger(leftInteger + rightInteger),
            BinaryOp.Minus => DbValue.FromInteger(leftInteger - rightInteger),
            BinaryOp.Multiply => DbValue.FromInteger(leftInteger * rightInteger),
            BinaryOp.Divide => rightInteger != 0
                ? DbValue.FromInteger(leftInteger / rightInteger)
                : throw new CSharpDbException(ErrorCode.Unknown, "Division by zero."),
            _ => DbValue.Null,
        };
    }

    private static DbValue Negate(DbValue value)
    {
        return value.Type switch
        {
            DbType.Integer => DbValue.FromInteger(-value.AsInteger),
            DbType.Real => DbValue.FromReal(-value.AsReal),
            _ => DbValue.Null,
        };
    }
}

internal enum BatchPredicateKind
{
    IntegerCompare,
    NumericCompare,
    TextCompare,
    TextifiedCompare,
    TextExpressionCompare,
    NumericExpressionCompare,
    IntegerIn,
    NumericIn,
    NumericExpressionIn,
    TextIn,
    TextifiedIn,
    TextExpressionIn,
    IntegerRange,
    NumericRange,
    NumericExpressionRange,
    TextRange,
    TextifiedRange,
    TextExpressionRange,
    LikeMatch,
    TextifiedLikeMatch,
    TextExpressionLikeMatch,
    NullCheck,
}

internal enum BatchPredicateExpressionKind
{
    Leaf,
    And,
    Or,
    Not,
}

internal enum BatchScalarAggregateKind
{
    None,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

internal enum BatchProjectionKind
{
    Column,
    Constant,
    NumericExpression,
    TextColumn,
    TextNumericExpression,
}

internal enum BatchNumericExpressionKind
{
    Constant,
    Column,
    Arithmetic,
}
