using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal delegate DbValue SpanExpressionEvaluator(ReadOnlySpan<DbValue> row);
internal delegate DbValue JoinSpanExpressionEvaluator(ReadOnlySpan<DbValue> leftRow, ReadOnlySpan<DbValue> rightRow);

/// <summary>
/// Compiles expression trees into delegates that bind column indices once.
/// This avoids repeated schema lookups on every row.
/// </summary>
internal static class ExpressionCompiler
{
    public static Func<DbValue[], DbValue> Compile(Expression expr, TableSchema schema)
    {
        var spanEvaluator = CompileSpan(expr, schema);
        return row => spanEvaluator(row);
    }

    public static SpanExpressionEvaluator CompileSpan(Expression expr, TableSchema schema)
    {
        var evaluator = CompileMappedCore(
            expr,
            schema,
            leftColumnCount: schema.Columns.Count,
            leftColumnMap: null,
            rightColumnMap: null,
            singleRowOnly: true);
        return row => evaluator(row, default);
    }

    public static JoinSpanExpressionEvaluator CompileJoinSpan(Expression expr, TableSchema schema, int leftColumnCount)
        => CompileJoinSpan(expr, schema, leftColumnCount, leftColumnMap: null, rightColumnMap: null);

    public static JoinSpanExpressionEvaluator CompileJoinSpan(
        Expression expr,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap)
        => CompileMappedCore(expr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly: false);

    /// <summary>
    /// Shared compiler for row spans and join spans. Single-row mode treats the left span
    /// as the full schema row; join mode interprets the schema as left+right with optional
    /// column maps for compacted join payloads.
    /// </summary>
    private static JoinSpanExpressionEvaluator CompileMappedCore(
        Expression expr,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        return expr switch
        {
            LiteralExpression lit => CompileMappedLiteral(lit),
            ParameterExpression param => CompileMappedParameter(param),
            ColumnRefExpression col => CompileMappedColumn(col, schema, leftColumnCount, leftColumnMap, rightColumnMap),
            BinaryExpression bin => CompileMappedBinary(bin, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            UnaryExpression un => CompileMappedUnary(un, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            CollateExpression collate => CompileMappedCore(collate.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            FunctionCallExpression func => CompileMappedFunction(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            LikeExpression like => CompileMappedLike(like, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            InExpression inExpr => CompileMappedIn(inExpr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            BetweenExpression between => CompileMappedBetween(between, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            IsNullExpression isNull => CompileMappedIsNull(isNull, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            _ => CompileMappedFallback(expr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedLiteral(LiteralExpression lit)
    {
        DbValue value = lit.Value == null
            ? DbValue.Null
            : lit.LiteralType switch
            {
                TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                TokenType.Null => DbValue.Null,
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
            };

        return (_, _) => value;
    }

    private static JoinSpanExpressionEvaluator CompileMappedParameter(ParameterExpression param)
        => (_, _) => throw new CSharpDbException(ErrorCode.SyntaxError, $"Unbound parameter '@{param.Name}'.");

    private static JoinSpanExpressionEvaluator CompileMappedColumn(
        ColumnRefExpression col,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap)
    {
        int columnIndex = ResolveColumnIndex(col, schema);
        return (leftRow, rightRow) => GetJoinedValue(leftRow, rightRow, columnIndex, leftColumnCount, leftColumnMap, rightColumnMap);
    }

    private static JoinSpanExpressionEvaluator CompileMappedBinary(
        BinaryExpression bin,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var left = CompileMappedCore(bin.Left, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var right = CompileMappedCore(bin.Right, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        string? collation = CollationSupport.ResolveComparisonCollation(bin.Left, bin.Right, schema);

        return (leftRow, rightRow) =>
        {
            var leftValue = left(leftRow, rightRow);
            var rightValue = right(leftRow, rightRow);

            return bin.Op switch
            {
                BinaryOp.Equals => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) == 0),
                BinaryOp.NotEquals => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) != 0),
                BinaryOp.LessThan => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) < 0),
                BinaryOp.GreaterThan => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) > 0),
                BinaryOp.LessOrEqual => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) <= 0),
                BinaryOp.GreaterOrEqual => BoolToDb(CollationSupport.Compare(leftValue, rightValue, collation) >= 0),
                BinaryOp.And => BoolToDb(leftValue.IsTruthy && rightValue.IsTruthy),
                BinaryOp.Or => BoolToDb(leftValue.IsTruthy || rightValue.IsTruthy),
                BinaryOp.Plus => ArithmeticOp(leftValue, rightValue, static (a, b) => a + b, static (a, b) => a + b),
                BinaryOp.Minus => ArithmeticOp(leftValue, rightValue, static (a, b) => a - b, static (a, b) => a - b),
                BinaryOp.Multiply => ArithmeticOp(leftValue, rightValue, static (a, b) => a * b, static (a, b) => a * b),
                BinaryOp.Divide => ArithmeticOp(
                    leftValue,
                    rightValue,
                    static (a, b) => b != 0 ? a / b : throw DivZero(),
                    static (a, b) => b != 0 ? a / b : throw DivZero()),
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown binary op: {bin.Op}"),
            };
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedUnary(
        UnaryExpression un,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var operand = CompileMappedCore(un.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);

        return (leftRow, rightRow) =>
        {
            var operandValue = operand(leftRow, rightRow);
            return un.Op switch
            {
                TokenType.Not => BoolToDb(!operandValue.IsTruthy),
                TokenType.Minus => operandValue.Type switch
                {
                    DbType.Integer => DbValue.FromInteger(-operandValue.AsInteger),
                    DbType.Real => DbValue.FromReal(-operandValue.AsReal),
                    _ => throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot negate non-numeric value."),
                },
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown unary op: {un.Op}"),
            };
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedFunction(
        FunctionCallExpression func,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (ScalarFunctionEvaluator.IsAggregateFunction(functionName))
            return CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);

        return functionName switch
        {
            "TEXT" => CompileMappedTextFunction(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
            _ => CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly),
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedTextFunction(
        FunctionCallExpression func,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        if (func.IsStarArg || func.IsDistinct || func.Arguments.Count != 1)
            return CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);

        var argumentEvaluator = CompileMappedCore(func.Arguments[0], schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        return (leftRow, rightRow) => ScalarFunctionEvaluator.EvaluateTextValue(argumentEvaluator(leftRow, rightRow));
    }

    private static JoinSpanExpressionEvaluator CompileMappedLike(
        LikeExpression like,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var operandEval = CompileMappedCore(like.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var patternEval = CompileMappedCore(like.Pattern, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var escapeEval = like.EscapeChar != null
            ? CompileMappedCore(like.EscapeChar, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly)
            : null;

        return (leftRow, rightRow) =>
        {
            var operand = operandEval(leftRow, rightRow);
            var pattern = patternEval(leftRow, rightRow);
            if (operand.IsNull || pattern.IsNull)
                return DbValue.Null;

            char? escape = null;
            if (escapeEval != null)
            {
                var escapeValue = escapeEval(leftRow, rightRow);
                if (!escapeValue.IsNull)
                {
                    string escapeText = escapeValue.AsText;
                    if (escapeText.Length == 1)
                        escape = escapeText[0];
                }
            }

            bool match = SqlLikeMatch(operand.AsText, pattern.AsText, escape);
            return BoolToDb(like.Negated ? !match : match);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedIn(
        InExpression inExpr,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var operandEval = CompileMappedCore(inExpr.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var valueEvals = new JoinSpanExpressionEvaluator[inExpr.Values.Count];
        for (int i = 0; i < inExpr.Values.Count; i++)
            valueEvals[i] = CompileMappedCore(inExpr.Values[i], schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        string? collation = CollationSupport.ResolveExpressionCollation(inExpr.Operand, schema);

        return (leftRow, rightRow) =>
        {
            var operand = operandEval(leftRow, rightRow);
            if (operand.IsNull)
                return DbValue.Null;

            bool found = false;
            bool hasNull = false;
            for (int i = 0; i < valueEvals.Length; i++)
            {
                var value = valueEvals[i](leftRow, rightRow);
                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (CollationSupport.Compare(operand, value, collation) == 0)
                {
                    found = true;
                    break;
                }
            }

            if (found)
                return BoolToDb(!inExpr.Negated);
            if (hasNull)
                return DbValue.Null;
            return BoolToDb(inExpr.Negated);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedBetween(
        BetweenExpression between,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var operandEval = CompileMappedCore(between.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var lowEval = CompileMappedCore(between.Low, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        var highEval = CompileMappedCore(between.High, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        string? collation = CollationSupport.ResolveExpressionCollation(between.Operand, schema);

        return (leftRow, rightRow) =>
        {
            var operand = operandEval(leftRow, rightRow);
            var low = lowEval(leftRow, rightRow);
            var high = highEval(leftRow, rightRow);
            if (operand.IsNull || low.IsNull || high.IsNull)
                return DbValue.Null;

            bool inRange =
                CollationSupport.Compare(operand, low, collation) >= 0 &&
                CollationSupport.Compare(operand, high, collation) <= 0;
            return BoolToDb(between.Negated ? !inRange : inRange);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedIsNull(
        IsNullExpression isNull,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        var operandEval = CompileMappedCore(isNull.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly);
        return (leftRow, rightRow) =>
        {
            var operand = operandEval(leftRow, rightRow);
            bool result = operand.IsNull;
            return BoolToDb(isNull.Negated ? !result : result);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedFallback(
        Expression expr,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        return (leftRow, rightRow) =>
        {
            var materializedRow = MaterializeMappedRow(
                leftRow,
                rightRow,
                schema.Columns.Count,
                leftColumnCount,
                leftColumnMap,
                rightColumnMap,
                singleRowOnly);
            return ExpressionEvaluator.Evaluate(expr, materializedRow, schema);
        };
    }

    private static DbValue[] MaterializeMappedRow(
        ReadOnlySpan<DbValue> leftRow,
        ReadOnlySpan<DbValue> rightRow,
        int totalColumnCount,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly)
    {
        if (singleRowOnly && leftColumnMap == null)
            return leftRow.ToArray();

        var row = new DbValue[totalColumnCount];
        if (singleRowOnly)
        {
            for (int i = 0; i < totalColumnCount; i++)
            {
                int sourceIndex = ResolveMappedColumnIndex(i, leftColumnMap);
                row[i] = sourceIndex >= 0 && sourceIndex < leftRow.Length
                    ? leftRow[sourceIndex]
                    : DbValue.Null;
            }

            return row;
        }

        for (int i = 0; i < leftColumnCount && i < totalColumnCount; i++)
        {
            int sourceIndex = ResolveMappedColumnIndex(i, leftColumnMap);
            row[i] = sourceIndex >= 0 && sourceIndex < leftRow.Length
                ? leftRow[sourceIndex]
                : DbValue.Null;
        }

        for (int i = leftColumnCount; i < totalColumnCount; i++)
        {
            int sourceIndex = ResolveMappedColumnIndex(i - leftColumnCount, rightColumnMap);
            row[i] = sourceIndex >= 0 && sourceIndex < rightRow.Length
                ? rightRow[sourceIndex]
                : DbValue.Null;
        }

        return row;
    }

    private static int ResolveColumnIndex(ColumnRefExpression col, TableSchema schema)
    {
        if (col.TableAlias != null)
        {
            int qualifiedIndex = schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName);
            if (qualifiedIndex >= 0)
                return qualifiedIndex;

            throw new CSharpDbException(
                ErrorCode.ColumnNotFound,
                $"Column '{col.TableAlias}.{col.ColumnName}' not found.");
        }

        int index = schema.GetColumnIndex(col.ColumnName);
        if (index >= 0)
            return index;

        throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{col.ColumnName}' not found.");
    }

    private static DbValue GetJoinedValue(
        ReadOnlySpan<DbValue> leftRow,
        ReadOnlySpan<DbValue> rightRow,
        int compositeIndex,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap)
    {
        if (compositeIndex < leftColumnCount)
        {
            int leftIndex = ResolveMappedColumnIndex(compositeIndex, leftColumnMap);
            return leftIndex >= 0 && leftIndex < leftRow.Length ? leftRow[leftIndex] : DbValue.Null;
        }

        int rightIndex = compositeIndex - leftColumnCount;
        rightIndex = ResolveMappedColumnIndex(rightIndex, rightColumnMap);
        return rightIndex >= 0 && rightIndex < rightRow.Length ? rightRow[rightIndex] : DbValue.Null;
    }

    private static int ResolveMappedColumnIndex(int logicalIndex, int[]? columnMap)
    {
        if (columnMap == null)
            return logicalIndex;

        return (uint)logicalIndex < (uint)columnMap.Length
            ? columnMap[logicalIndex]
            : -1;
    }

    private static DbValue BoolToDb(bool value) => DbValue.FromInteger(value ? 1 : 0);

    private static DbValue ArithmeticOp(
        DbValue left,
        DbValue right,
        Func<long, long, long> intOp,
        Func<double, double, double> realOp)
    {
        if (left.IsNull || right.IsNull)
            return DbValue.Null;

        if (left.Type == DbType.Real || right.Type == DbType.Real)
            return DbValue.FromReal(realOp(left.AsReal, right.AsReal));

        if (left.Type == DbType.Integer && right.Type == DbType.Integer)
            return DbValue.FromInteger(intOp(left.AsInteger, right.AsInteger));

        throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot perform arithmetic on non-numeric values.");
    }

    private static Exception DivZero() =>
        new CSharpDbException(ErrorCode.Unknown, "Division by zero.");

    private static bool SqlLikeMatch(string text, string pattern, char? escape)
    {
        int ti = 0;
        int pi = 0;
        int starTi = -1;
        int starPi = -1;

        while (ti < text.Length)
        {
            if (pi < pattern.Length && escape.HasValue && pattern[pi] == escape.Value && pi + 1 < pattern.Length)
            {
                pi++;
                if (ti < text.Length && char.ToUpperInvariant(text[ti]) == char.ToUpperInvariant(pattern[pi]))
                {
                    ti++;
                    pi++;
                }
                else if (starPi >= 0)
                {
                    ti = ++starTi;
                    pi = starPi;
                }
                else
                {
                    return false;
                }
            }
            else if (pi < pattern.Length && pattern[pi] == '%')
            {
                starPi = ++pi;
                starTi = ti;
            }
            else if (pi < pattern.Length &&
                (pattern[pi] == '_' || char.ToUpperInvariant(text[ti]) == char.ToUpperInvariant(pattern[pi])))
            {
                ti++;
                pi++;
            }
            else if (starPi >= 0)
            {
                ti = ++starTi;
                pi = starPi;
            }
            else
            {
                return false;
            }
        }

        while (pi < pattern.Length && pattern[pi] == '%')
            pi++;

        return pi == pattern.Length;
    }
}
