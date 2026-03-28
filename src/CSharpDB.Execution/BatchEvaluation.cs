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

internal readonly struct BatchPushdownFilter
{
    public BatchPushdownFilter(int columnIndex, BinaryOp op, DbValue literal)
    {
        ColumnIndex = columnIndex;
        Op = op;
        Literal = literal;
    }

    public int ColumnIndex { get; }
    public BinaryOp Op { get; }
    public DbValue Literal { get; }
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
            if (_predicateEvaluator != null)
            {
                var sourceRow = EnsureRowBuffer(sourceBatch.ColumnCount);
                sourceBatch.CopyRowTo(rowIndex, sourceRow);
                if (!_predicateEvaluator(sourceRow).IsTruthy)
                    continue;
            }

            selection.Add(rowIndex);
        }

        var selectedRows = selection.AsSpan();
        for (int i = 0; i < selectedRows.Length; i++)
        {
            int sourceRowIndex = selectedRows[i];
            Span<DbValue> destinationRow = destination.GetWritableRowSpan(destination.Count);
            WriteProjectedRow(sourceBatch, sourceRowIndex, destinationRow);
            destination.CommitWrittenRow(destination.Count);
        }

        return destination.Count;
    }

    private void WriteProjectedRow(RowBatch sourceBatch, int rowIndex, Span<DbValue> destination)
    {
        if (_expressionEvaluators != null)
        {
            var sourceRow = EnsureRowBuffer(sourceBatch.ColumnCount);
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

        return new SpecializedFilterProjectionBatchPlan(boundPredicate, projections);
    }

    public static IScalarAggregateBatchPlan? TryCreateScalarAggregate(
        Expression? predicate,
        string functionName,
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

        if (!isCountStar)
        {
            if ((uint)columnIndex >= (uint)schema.Columns.Count)
                return null;

            if (kind is BatchScalarAggregateKind.Sum or BatchScalarAggregateKind.Avg &&
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
            isDistinct);
    }

    private static BatchPredicateTerm[]? TryBindPredicate(Expression? predicate, TableSchema schema)
    {
        if (predicate == null)
            return Array.Empty<BatchPredicateTerm>();

        var terms = new List<BatchPredicateTerm>();
        return TryCollectPredicateTerms(predicate, schema, terms) ? terms.ToArray() : null;
    }

    private static bool TryCollectPredicateTerms(Expression predicate, TableSchema schema, List<BatchPredicateTerm> terms)
    {
        if (predicate is BinaryExpression { Op: BinaryOp.And } andExpression)
            return TryCollectPredicateTerms(andExpression.Left, schema, terms) &&
                   TryCollectPredicateTerms(andExpression.Right, schema, terms);

        if (predicate is InExpression inExpression)
            return TryBindInPredicate(inExpression, schema, terms);

        if (predicate is BetweenExpression betweenExpression)
            return TryBindBetweenPredicate(betweenExpression, schema, terms);

        if (predicate is LikeExpression likeExpression)
            return TryBindLikePredicate(likeExpression, schema, terms);

        if (predicate is IsNullExpression isNull &&
            TryResolveColumnIndex(isNull.Operand, schema, out int isNullColumnIndex))
        {
            terms.Add(BatchPredicateTerm.CreateNullCheck(isNullColumnIndex, isNull.Negated));
            return true;
        }

        if (predicate is not BinaryExpression comparison ||
            !IsComparisonOp(comparison.Op))
        {
            return false;
        }

        if (TryBindColumnLiteralPredicate(comparison.Left, comparison.Right, comparison.Op, schema, out var predicateTerm) ||
            TryBindColumnLiteralPredicate(comparison.Right, comparison.Left, ReverseComparison(comparison.Op), schema, out predicateTerm))
        {
            terms.Add(predicateTerm);
            return true;
        }

        return false;
    }

    private static bool TryBindLikePredicate(LikeExpression likeExpression, TableSchema schema, List<BatchPredicateTerm> terms)
    {
        if (!TryResolveColumnIndex(likeExpression.Operand, schema, out int columnIndex) ||
            schema.Columns[columnIndex].Type != DbType.Text ||
            !TryCreateLiteral(likeExpression.Pattern, out DbValue patternValue) ||
            patternValue.IsNull ||
            patternValue.Type != DbType.Text ||
            !TryCreateEscapeChar(likeExpression.EscapeChar, out char? escapeChar))
        {
            return false;
        }

        terms.Add(BatchPredicateTerm.CreateLike(columnIndex, patternValue.AsText, escapeChar, likeExpression.Negated));
        return true;
    }

    private static bool TryBindInPredicate(InExpression inExpression, TableSchema schema, List<BatchPredicateTerm> terms)
    {
        if (!TryResolveColumnIndex(inExpression.Operand, schema, out int columnIndex) ||
            inExpression.Values.Count == 0)
        {
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
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Integer)
                    return false;

                values.Add(value.AsInteger);
            }

            terms.Add(BatchPredicateTerm.CreateIntegerIn(columnIndex, values.ToArray(), inExpression.Negated, hasNull));
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
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (!IsNumericType(value.Type))
                    return false;

                values.Add(value.AsReal);
            }

            terms.Add(BatchPredicateTerm.CreateNumericIn(columnIndex, values.ToArray(), inExpression.Negated, hasNull));
            return true;
        }

        if (columnType == DbType.Text)
        {
            bool useNoCase = CollationSupport.IsNoCase(
                CollationSupport.ResolveComparisonCollation(inExpression.Operand, inExpression.Values[0], schema));
            var values = new List<string>(inExpression.Values.Count);
            bool hasNull = false;
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (!TryCreateLiteral(inExpression.Values[i], out DbValue value))
                {
                    return false;
                }

                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (value.Type != DbType.Text)
                    return false;

                values.Add(value.AsText);
            }

            terms.Add(BatchPredicateTerm.CreateTextIn(columnIndex, values.ToArray(), inExpression.Negated, hasNull, useNoCase));
            return true;
        }

        return false;
    }

    private static bool TryBindBetweenPredicate(BetweenExpression between, TableSchema schema, List<BatchPredicateTerm> terms)
    {
        if (!TryResolveColumnIndex(between.Operand, schema, out int columnIndex) ||
            !TryCreateLiteral(between.Low, out DbValue lowValue) ||
            !TryCreateLiteral(between.High, out DbValue highValue) ||
            lowValue.IsNull ||
            highValue.IsNull)
        {
            return false;
        }

        DbType columnType = schema.Columns[columnIndex].Type;
        if (columnType == DbType.Integer &&
            lowValue.Type == DbType.Integer &&
            highValue.Type == DbType.Integer)
        {
            terms.Add(BatchPredicateTerm.CreateIntegerRange(
                columnIndex,
                lowValue.AsInteger,
                highValue.AsInteger,
                between.Negated));
            return true;
        }

        if (IsNumericType(columnType) &&
            IsNumericType(lowValue.Type) &&
            IsNumericType(highValue.Type))
        {
            terms.Add(BatchPredicateTerm.CreateNumericRange(
                columnIndex,
                lowValue.AsReal,
                highValue.AsReal,
                between.Negated));
            return true;
        }

        if (columnType == DbType.Text &&
            lowValue.Type == DbType.Text &&
            highValue.Type == DbType.Text)
        {
            bool useNoCase = CollationSupport.IsNoCase(
                CollationSupport.ResolveComparisonCollation(between.Operand, between.Low, schema));
            terms.Add(BatchPredicateTerm.CreateTextRange(
                columnIndex,
                lowValue.AsText,
                highValue.AsText,
                between.Negated,
                useNoCase));
            return true;
        }

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
        if (!TryResolveColumnIndex(left, schema, out int columnIndex) ||
            !TryCreateLiteral(right, out DbValue literalValue))
        {
            return false;
        }

        var columnType = schema.Columns[columnIndex].Type;
        if (literalValue.IsNull)
            return false;

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
            predicate = BatchPredicateTerm.CreateTextCompare(
                columnIndex,
                op,
                literalValue.AsText,
                CollationSupport.IsNoCase(CollationSupport.ResolveComparisonCollation(left, right, schema)));
            return true;
        }

        return false;
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

        if (projection is UnaryExpression { Op: TokenType.Minus } unaryMinus &&
            TryBindNumericOperand(unaryMinus.Operand, schema, out var unaryOperand))
        {
            term = BatchProjectionTerm.CreateNegatedNumeric(unaryOperand);
            return true;
        }

        if (projection is BinaryExpression arithmetic &&
            IsArithmeticOp(arithmetic.Op) &&
            TryBindNumericOperand(arithmetic.Left, schema, out var leftOperand) &&
            TryBindNumericOperand(arithmetic.Right, schema, out var rightOperand))
        {
            term = BatchProjectionTerm.CreateNumericArithmetic(arithmetic.Op, leftOperand, rightOperand);
            return true;
        }

        return false;
    }

    private static bool TryBindNumericOperand(Expression expression, TableSchema schema, out BatchNumericOperand operand)
    {
        operand = default;

        if (TryResolveColumnIndex(expression, schema, out int columnIndex))
        {
            if (!IsNumericType(schema.Columns[columnIndex].Type))
                return false;

            operand = BatchNumericOperand.CreateColumn(columnIndex);
            return true;
        }

        if (expression is LiteralExpression literal &&
            TryCreateLiteral(literal, out DbValue literalValue) &&
            IsNumericType(literalValue.Type))
        {
            operand = BatchNumericOperand.CreateConstant(literalValue);
            return true;
        }

        if (expression is UnaryExpression { Op: TokenType.Minus } unaryMinus &&
            TryBindNumericOperand(unaryMinus.Operand, schema, out var innerOperand))
        {
            operand = innerOperand.Negated();
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

    private static BatchPushdownFilter[] CreatePushdownFilters(BatchPredicateTerm[] predicateTerms)
    {
        if (predicateTerms.Length == 0)
            return Array.Empty<BatchPushdownFilter>();

        var filters = new List<BatchPushdownFilter>(predicateTerms.Length * 2);
        for (int i = 0; i < predicateTerms.Length; i++)
            predicateTerms[i].AppendPushdownFilters(filters);

        return filters.Count == 0 ? Array.Empty<BatchPushdownFilter>() : filters.ToArray();
    }
}

internal sealed class SpecializedFilterProjectionBatchPlan : IFilterProjectionBatchPlan
{
    private readonly BatchPredicateTerm[] _predicateTerms;
    private readonly BatchProjectionTerm[] _projections;

    public SpecializedFilterProjectionBatchPlan(
        BatchPredicateTerm[]? predicateTerms,
        BatchProjectionTerm[] projections)
    {
        _predicateTerms = predicateTerms ?? Array.Empty<BatchPredicateTerm>();
        _projections = projections ?? throw new ArgumentNullException(nameof(projections));
    }

    public int OutputColumnCount => _projections.Length;

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
        }

        ReadOnlySpan<int> selectedRows = selection.AsSpan();
        for (int i = 0; i < selectedRows.Length; i++)
        {
            ReadOnlySpan<DbValue> row = sourceBatch.GetRowSpan(selectedRows[i]);
            int destinationRowIndex = destination.Count;
            Span<DbValue> destinationRow = destination.GetWritableRowSpan(destinationRowIndex);
            for (int projectionIndex = 0; projectionIndex < _projections.Length; projectionIndex++)
                destinationRow[projectionIndex] = _projections[projectionIndex].Evaluate(row);
            destination.CommitWrittenRow(destinationRowIndex);
        }

        return destination.Count;
    }

    private bool MatchesPredicates(ReadOnlySpan<DbValue> row)
    {
        for (int i = 0; i < _predicateTerms.Length; i++)
        {
            if (!_predicateTerms[i].Evaluate(row))
                return false;
        }

        return true;
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
    private readonly bool _useNoCase;
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
        bool useNoCase,
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
        _useNoCase = useNoCase;
        _kind = kind;
        _negated = negated;
    }

    public static BatchPredicateTerm CreateIntegerCompare(int columnIndex, BinaryOp op, long integerLiteral)
        => new(columnIndex, op, integerLiteral, 0, 0, 0, null, null, null, null, null, '\0', false, false, false, BatchPredicateKind.IntegerCompare, negated: false);

    public static BatchPredicateTerm CreateNumericCompare(int columnIndex, BinaryOp op, double numericLiteral)
        => new(columnIndex, op, 0, 0, numericLiteral, 0, null, null, null, null, null, '\0', false, false, false, BatchPredicateKind.NumericCompare, negated: false);

    public static BatchPredicateTerm CreateTextCompare(int columnIndex, BinaryOp op, string textLiteral, bool useNoCase)
        => new(columnIndex, op, 0, 0, 0, 0, NormalizeTextLiteral(textLiteral, useNoCase), null, null, null, null, '\0', false, false, useNoCase, BatchPredicateKind.TextCompare, negated: false);

    public static BatchPredicateTerm CreateIntegerIn(int columnIndex, long[] values, bool negated, bool hasNullSetValue)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, values, null, null, '\0', false, hasNullSetValue, false, BatchPredicateKind.IntegerIn, negated);

    public static BatchPredicateTerm CreateNumericIn(int columnIndex, double[] values, bool negated, bool hasNullSetValue)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, values, null, '\0', false, hasNullSetValue, false, BatchPredicateKind.NumericIn, negated);

    public static BatchPredicateTerm CreateTextIn(int columnIndex, string[] values, bool negated, bool hasNullSetValue, bool useNoCase)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, null, NormalizeTextLiterals(values, useNoCase), '\0', false, hasNullSetValue, useNoCase, BatchPredicateKind.TextIn, negated);

    public static BatchPredicateTerm CreateIntegerRange(int columnIndex, long lowerInclusive, long upperInclusive, bool negated)
        => new(columnIndex, BinaryOp.Equals, lowerInclusive, upperInclusive, 0, 0, null, null, null, null, null, '\0', false, false, false, BatchPredicateKind.IntegerRange, negated);

    public static BatchPredicateTerm CreateNumericRange(int columnIndex, double lowerInclusive, double upperInclusive, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, lowerInclusive, upperInclusive, null, null, null, null, null, '\0', false, false, false, BatchPredicateKind.NumericRange, negated);

    public static BatchPredicateTerm CreateTextRange(int columnIndex, string lowerInclusive, string upperInclusive, bool negated, bool useNoCase)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, NormalizeTextLiteral(lowerInclusive, useNoCase), NormalizeTextLiteral(upperInclusive, useNoCase), null, null, null, '\0', false, false, useNoCase, BatchPredicateKind.TextRange, negated);

    public static BatchPredicateTerm CreateLike(int columnIndex, string pattern, char? escapeChar, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, pattern, null, null, null, null, escapeChar.GetValueOrDefault(), escapeChar.HasValue, false, false, BatchPredicateKind.LikeMatch, negated);

    public static BatchPredicateTerm CreateNullCheck(int columnIndex, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, 0, 0, 0, null, null, null, null, null, '\0', false, false, false, BatchPredicateKind.NullCheck, negated);

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

            case BatchPredicateKind.TextCompare when _textLiteral != null && !_useNoCase:
                filters.Add(new BatchPushdownFilter(_columnIndex, _op, DbValue.FromText(_textLiteral)));
                break;

            case BatchPredicateKind.IntegerRange when !_negated:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromInteger(_integerLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromInteger(_integerUpperLiteral)));
                break;

            case BatchPredicateKind.NumericRange when !_negated:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromReal(_numericLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromReal(_numericUpperLiteral)));
                break;

            case BatchPredicateKind.TextRange when !_negated && !_useNoCase && _textLiteral != null && _textUpperLiteral != null:
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.GreaterOrEqual, DbValue.FromText(_textLiteral)));
                filters.Add(new BatchPushdownFilter(_columnIndex, BinaryOp.LessOrEqual, DbValue.FromText(_textUpperLiteral)));
                break;
        }
    }

    public bool Evaluate(ReadOnlySpan<DbValue> row)
    {
        DbValue value = row[_columnIndex];
        return _kind switch
        {
            BatchPredicateKind.NullCheck => _negated ? !value.IsNull : value.IsNull,
            BatchPredicateKind.IntegerCompare => EvaluateIntegerCompare(value),
            BatchPredicateKind.NumericCompare => EvaluateNumericCompare(value),
            BatchPredicateKind.TextCompare => EvaluateTextCompare(value),
            BatchPredicateKind.IntegerIn => EvaluateIntegerIn(value),
            BatchPredicateKind.NumericIn => EvaluateNumericIn(value),
            BatchPredicateKind.TextIn => EvaluateTextIn(value),
            BatchPredicateKind.IntegerRange => EvaluateIntegerRange(value),
            BatchPredicateKind.NumericRange => EvaluateNumericRange(value),
            BatchPredicateKind.TextRange => EvaluateTextRange(value),
            BatchPredicateKind.LikeMatch => EvaluateLike(value),
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

    private bool EvaluateTextCompare(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textLiteral == null)
            return false;

        string actual = NormalizeTextLiteral(value.AsText, _useNoCase);
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

        string actual = NormalizeTextLiteral(value.AsText, _useNoCase);
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

        string actual = NormalizeTextLiteral(value.AsText, _useNoCase);
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

    private static string NormalizeTextLiteral(string value, bool useNoCase) =>
        useNoCase ? CollationSupport.NormalizeText(value, CollationSupport.NoCaseCollation) : value;

    private static string[] NormalizeTextLiterals(string[] values, bool useNoCase)
    {
        if (!useNoCase)
            return values;

        var normalized = new string[values.Length];
        for (int i = 0; i < values.Length; i++)
            normalized[i] = NormalizeTextLiteral(values[i], useNoCase);

        return normalized;
    }
}

internal sealed class SpecializedScalarAggregateBatchPlan : IScalarAggregateBatchPlan
{
    private readonly BatchPredicateTerm[] _predicateTerms;
    private readonly BatchPushdownFilter[] _pushdownFilters;
    private readonly BatchScalarAggregateKind _kind;
    private readonly int _columnIndex;
    private readonly bool _isCountStar;
    private readonly bool _isDistinct;

    private long _count;
    private double _sum;
    private bool _hasReal;
    private bool _hasAny;
    private DbValue? _best;
    private AggregateDistinctValueSet? _distinctValues;

    public SpecializedScalarAggregateBatchPlan(
        BatchPredicateTerm[] predicateTerms,
        BatchPushdownFilter[] pushdownFilters,
        BatchScalarAggregateKind kind,
        int columnIndex,
        bool isCountStar,
        bool isDistinct)
    {
        _predicateTerms = predicateTerms ?? Array.Empty<BatchPredicateTerm>();
        _pushdownFilters = pushdownFilters ?? Array.Empty<BatchPushdownFilter>();
        _kind = kind;
        _columnIndex = columnIndex;
        _isCountStar = isCountStar;
        _isDistinct = isDistinct;
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

            DbValue value = row[_columnIndex];
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

            DbValue value = row[_columnIndex];
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

            DbValue value = row[_columnIndex];
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

            DbValue value = row[_columnIndex];
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

    private bool MatchesPredicates(ReadOnlySpan<DbValue> row)
    {
        for (int i = 0; i < _predicateTerms.Length; i++)
        {
            if (!_predicateTerms[i].Evaluate(row))
                return false;
        }

        return true;
    }
}

internal readonly struct BatchProjectionTerm
{
    private readonly int _columnIndex;
    private readonly DbValue _constant;
    private readonly BinaryOp _arithmeticOp;
    private readonly BatchNumericOperand _leftOperand;
    private readonly BatchNumericOperand _rightOperand;
    private readonly BatchProjectionKind _kind;

    private BatchProjectionTerm(
        int columnIndex,
        DbValue constant,
        BinaryOp arithmeticOp,
        BatchNumericOperand leftOperand,
        BatchNumericOperand rightOperand,
        BatchProjectionKind kind)
    {
        _columnIndex = columnIndex;
        _constant = constant;
        _arithmeticOp = arithmeticOp;
        _leftOperand = leftOperand;
        _rightOperand = rightOperand;
        _kind = kind;
    }

    public static BatchProjectionTerm CreateColumn(int columnIndex)
        => new(columnIndex, DbValue.Null, BinaryOp.Equals, default, default, BatchProjectionKind.Column);

    public static BatchProjectionTerm CreateConstant(DbValue constant)
        => new(0, constant, BinaryOp.Equals, default, default, BatchProjectionKind.Constant);

    public static BatchProjectionTerm CreateNumericArithmetic(BinaryOp op, BatchNumericOperand left, BatchNumericOperand right)
        => new(0, DbValue.Null, op, left, right, BatchProjectionKind.NumericArithmetic);

    public static BatchProjectionTerm CreateNegatedNumeric(BatchNumericOperand operand)
        => new(0, DbValue.Null, BinaryOp.Minus, BatchNumericOperand.CreateConstant(DbValue.FromInteger(0)), operand, BatchProjectionKind.NumericArithmetic);

    public DbValue Evaluate(ReadOnlySpan<DbValue> row)
    {
        return _kind switch
        {
            BatchProjectionKind.Column => row[_columnIndex],
            BatchProjectionKind.Constant => _constant,
            BatchProjectionKind.NumericArithmetic => EvaluateNumericArithmetic(row),
            _ => DbValue.Null,
        };
    }

    private DbValue EvaluateNumericArithmetic(ReadOnlySpan<DbValue> row)
    {
        if (!_leftOperand.TryGetValue(row, out DbValue left) ||
            !_rightOperand.TryGetValue(row, out DbValue right))
        {
            return DbValue.Null;
        }

        if (left.Type == DbType.Real || right.Type == DbType.Real)
        {
            double leftReal = left.AsReal;
            double rightReal = right.AsReal;
            return _arithmeticOp switch
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

        return _arithmeticOp switch
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
}

internal readonly struct BatchNumericOperand
{
    private readonly int _columnIndex;
    private readonly DbValue _constant;
    private readonly BatchNumericOperandKind _kind;

    private BatchNumericOperand(int columnIndex, DbValue constant, BatchNumericOperandKind kind)
    {
        _columnIndex = columnIndex;
        _constant = constant;
        _kind = kind;
    }

    public static BatchNumericOperand CreateColumn(int columnIndex)
        => new(columnIndex, DbValue.Null, BatchNumericOperandKind.Column);

    public static BatchNumericOperand CreateConstant(DbValue constant)
        => new(0, constant, BatchNumericOperandKind.Constant);

    public BatchNumericOperand Negated()
        => _kind switch
        {
            BatchNumericOperandKind.Constant => CreateConstant(Negate(_constant)),
            BatchNumericOperandKind.Column => new(_columnIndex, DbValue.Null, BatchNumericOperandKind.NegatedColumn),
            BatchNumericOperandKind.NegatedColumn => CreateColumn(_columnIndex),
            _ => this,
        };

    public bool TryGetValue(ReadOnlySpan<DbValue> row, out DbValue value)
    {
        value = DbValue.Null;
        switch (_kind)
        {
            case BatchNumericOperandKind.Constant:
                value = _constant;
                return true;
            case BatchNumericOperandKind.Column:
            case BatchNumericOperandKind.NegatedColumn:
                DbValue cell = row[_columnIndex];
                if (cell.IsNull || (cell.Type is not DbType.Integer and not DbType.Real))
                    return false;

                value = _kind == BatchNumericOperandKind.NegatedColumn ? Negate(cell) : cell;
                return true;
            default:
                return false;
        }
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
    IntegerIn,
    NumericIn,
    TextIn,
    IntegerRange,
    NumericRange,
    TextRange,
    LikeMatch,
    NullCheck,
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
    NumericArithmetic,
}

internal enum BatchNumericOperandKind
{
    Constant,
    Column,
    NegatedColumn,
}
