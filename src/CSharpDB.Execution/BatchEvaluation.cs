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

    private static bool TryBindColumnLiteralPredicate(
        Expression left,
        Expression right,
        BinaryOp op,
        TableSchema schema,
        out BatchPredicateTerm predicate)
    {
        predicate = default;
        if (!TryResolveColumnIndex(left, schema, out int columnIndex) ||
            right is not LiteralExpression literal ||
            !TryCreateLiteral(literal, out DbValue literalValue))
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

        if (columnType == DbType.Text &&
            literalValue.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals))
        {
            predicate = BatchPredicateTerm.CreateTextCompare(columnIndex, op, literalValue.AsText);
            return true;
        }

        return false;
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
            TryBindIntegerOperand(unaryMinus.Operand, schema, out var unaryOperand))
        {
            term = BatchProjectionTerm.CreateNegatedInteger(unaryOperand);
            return true;
        }

        if (projection is BinaryExpression arithmetic &&
            IsArithmeticOp(arithmetic.Op) &&
            TryBindIntegerOperand(arithmetic.Left, schema, out var leftOperand) &&
            TryBindIntegerOperand(arithmetic.Right, schema, out var rightOperand))
        {
            term = BatchProjectionTerm.CreateIntegerArithmetic(arithmetic.Op, leftOperand, rightOperand);
            return true;
        }

        return false;
    }

    private static bool TryBindIntegerOperand(Expression expression, TableSchema schema, out BatchIntegerOperand operand)
    {
        operand = default;

        if (TryResolveColumnIndex(expression, schema, out int columnIndex))
        {
            if (schema.Columns[columnIndex].Type != DbType.Integer)
                return false;

            operand = BatchIntegerOperand.CreateColumn(columnIndex);
            return true;
        }

        if (expression is LiteralExpression literal &&
            TryCreateLiteral(literal, out DbValue literalValue) &&
            literalValue.Type == DbType.Integer)
        {
            operand = BatchIntegerOperand.CreateConstant(literalValue.AsInteger);
            return true;
        }

        if (expression is UnaryExpression { Op: TokenType.Minus } unaryMinus &&
            TryBindIntegerOperand(unaryMinus.Operand, schema, out var innerOperand))
        {
            operand = innerOperand.Negated();
            return true;
        }

        return false;
    }

    private static bool TryResolveColumnIndex(Expression expression, TableSchema schema, out int columnIndex)
    {
        columnIndex = -1;
        if (expression is not ColumnRefExpression columnRef)
            return false;

        columnIndex = columnRef.TableAlias != null
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);
        return columnIndex >= 0;
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

    private static BinaryOp ReverseComparison(BinaryOp op)
        => op switch
        {
            BinaryOp.LessThan => BinaryOp.GreaterThan,
            BinaryOp.GreaterThan => BinaryOp.LessThan,
            BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
            BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual,
            _ => op,
        };
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
    private readonly string? _textLiteral;
    private readonly BatchPredicateKind _kind;
    private readonly bool _negated;

    private BatchPredicateTerm(
        int columnIndex,
        BinaryOp op,
        long integerLiteral,
        string? textLiteral,
        BatchPredicateKind kind,
        bool negated)
    {
        _columnIndex = columnIndex;
        _op = op;
        _integerLiteral = integerLiteral;
        _textLiteral = textLiteral;
        _kind = kind;
        _negated = negated;
    }

    public static BatchPredicateTerm CreateIntegerCompare(int columnIndex, BinaryOp op, long integerLiteral)
        => new(columnIndex, op, integerLiteral, null, BatchPredicateKind.IntegerCompare, negated: false);

    public static BatchPredicateTerm CreateTextCompare(int columnIndex, BinaryOp op, string textLiteral)
        => new(columnIndex, op, 0, textLiteral, BatchPredicateKind.TextCompare, negated: false);

    public static BatchPredicateTerm CreateNullCheck(int columnIndex, bool negated)
        => new(columnIndex, BinaryOp.Equals, 0, null, BatchPredicateKind.NullCheck, negated);

    public bool Evaluate(ReadOnlySpan<DbValue> row)
    {
        DbValue value = row[_columnIndex];
        return _kind switch
        {
            BatchPredicateKind.NullCheck => _negated ? !value.IsNull : value.IsNull,
            BatchPredicateKind.IntegerCompare => EvaluateIntegerCompare(value),
            BatchPredicateKind.TextCompare => EvaluateTextCompare(value),
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

    private bool EvaluateTextCompare(DbValue value)
    {
        if (value.IsNull || value.Type != DbType.Text || _textLiteral == null)
            return false;

        bool equal = string.Equals(value.AsText, _textLiteral, StringComparison.Ordinal);
        return _op switch
        {
            BinaryOp.Equals => equal,
            BinaryOp.NotEquals => !equal,
            _ => false,
        };
    }
}

internal readonly struct BatchProjectionTerm
{
    private readonly int _columnIndex;
    private readonly DbValue _constant;
    private readonly BinaryOp _arithmeticOp;
    private readonly BatchIntegerOperand _leftOperand;
    private readonly BatchIntegerOperand _rightOperand;
    private readonly BatchProjectionKind _kind;

    private BatchProjectionTerm(
        int columnIndex,
        DbValue constant,
        BinaryOp arithmeticOp,
        BatchIntegerOperand leftOperand,
        BatchIntegerOperand rightOperand,
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

    public static BatchProjectionTerm CreateIntegerArithmetic(BinaryOp op, BatchIntegerOperand left, BatchIntegerOperand right)
        => new(0, DbValue.Null, op, left, right, BatchProjectionKind.IntegerArithmetic);

    public static BatchProjectionTerm CreateNegatedInteger(BatchIntegerOperand operand)
        => new(0, DbValue.Null, BinaryOp.Minus, BatchIntegerOperand.CreateConstant(0), operand, BatchProjectionKind.IntegerArithmetic);

    public DbValue Evaluate(ReadOnlySpan<DbValue> row)
    {
        return _kind switch
        {
            BatchProjectionKind.Column => row[_columnIndex],
            BatchProjectionKind.Constant => _constant,
            BatchProjectionKind.IntegerArithmetic => EvaluateIntegerArithmetic(row),
            _ => DbValue.Null,
        };
    }

    private DbValue EvaluateIntegerArithmetic(ReadOnlySpan<DbValue> row)
    {
        if (!_leftOperand.TryGetValue(row, out long left) ||
            !_rightOperand.TryGetValue(row, out long right))
        {
            return DbValue.Null;
        }

        return _arithmeticOp switch
        {
            BinaryOp.Plus => DbValue.FromInteger(left + right),
            BinaryOp.Minus => DbValue.FromInteger(left - right),
            BinaryOp.Multiply => DbValue.FromInteger(left * right),
            BinaryOp.Divide => right != 0
                ? DbValue.FromInteger(left / right)
                : throw new CSharpDbException(ErrorCode.Unknown, "Division by zero."),
            _ => DbValue.Null,
        };
    }
}

internal readonly struct BatchIntegerOperand
{
    private readonly int _columnIndex;
    private readonly long _constant;
    private readonly BatchIntegerOperandKind _kind;

    private BatchIntegerOperand(int columnIndex, long constant, BatchIntegerOperandKind kind)
    {
        _columnIndex = columnIndex;
        _constant = constant;
        _kind = kind;
    }

    public static BatchIntegerOperand CreateColumn(int columnIndex)
        => new(columnIndex, 0, BatchIntegerOperandKind.Column);

    public static BatchIntegerOperand CreateConstant(long constant)
        => new(0, constant, BatchIntegerOperandKind.Constant);

    public BatchIntegerOperand Negated()
        => _kind switch
        {
            BatchIntegerOperandKind.Constant => CreateConstant(-_constant),
            BatchIntegerOperandKind.Column => new(_columnIndex, 0, BatchIntegerOperandKind.NegatedColumn),
            BatchIntegerOperandKind.NegatedColumn => CreateColumn(_columnIndex),
            _ => this,
        };

    public bool TryGetValue(ReadOnlySpan<DbValue> row, out long value)
    {
        value = 0;
        switch (_kind)
        {
            case BatchIntegerOperandKind.Constant:
                value = _constant;
                return true;
            case BatchIntegerOperandKind.Column:
            case BatchIntegerOperandKind.NegatedColumn:
                DbValue cell = row[_columnIndex];
                if (cell.IsNull || cell.Type != DbType.Integer)
                    return false;

                value = cell.AsInteger;
                if (_kind == BatchIntegerOperandKind.NegatedColumn)
                    value = -value;
                return true;
            default:
                return false;
        }
    }
}

internal enum BatchPredicateKind
{
    IntegerCompare,
    TextCompare,
    NullCheck,
}

internal enum BatchProjectionKind
{
    Column,
    Constant,
    IntegerArithmetic,
}

internal enum BatchIntegerOperandKind
{
    Constant,
    Column,
    NegatedColumn,
}
