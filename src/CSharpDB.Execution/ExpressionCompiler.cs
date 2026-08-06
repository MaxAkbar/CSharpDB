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
    public static Func<DbValue[], DbValue> Compile(Expression expr, TableSchema schema, DbFunctionRegistry? functions = null)
    {
        var spanEvaluator = CompileSpan(expr, schema, functions);
        return row => spanEvaluator(row);
    }

    public static SpanExpressionEvaluator CompileSpan(Expression expr, TableSchema schema, DbFunctionRegistry? functions = null)
    {
        var evaluator = CompileMappedCore(
            expr,
            schema,
            leftColumnCount: schema.Columns.Count,
            leftColumnMap: null,
            rightColumnMap: null,
            singleRowOnly: true,
            functions: functions);
        return row => evaluator(row, default);
    }

    public static JoinSpanExpressionEvaluator CompileJoinSpan(Expression expr, TableSchema schema, int leftColumnCount, DbFunctionRegistry? functions = null)
        => CompileJoinSpan(expr, schema, leftColumnCount, leftColumnMap: null, rightColumnMap: null, functions: functions);

    public static JoinSpanExpressionEvaluator CompileJoinSpan(
        Expression expr,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        DbFunctionRegistry? functions = null)
        => CompileMappedCore(expr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly: false, functions: functions);

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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        return expr switch
        {
            LiteralExpression lit => CompileMappedLiteral(lit),
            ParameterExpression param => CompileMappedParameter(param),
            ColumnRefExpression col => CompileMappedColumn(col, schema, leftColumnCount, leftColumnMap, rightColumnMap),
            BinaryExpression bin => CompileMappedBinary(bin, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            UnaryExpression un => CompileMappedUnary(un, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            CollateExpression collate => CompileMappedCore(collate.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            CastExpression cast => CompileMappedCast(cast, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            FunctionCallExpression func => CompileMappedFunction(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            WindowFunctionExpression => (_, _) => throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window functions require window planning context and cannot be compiled as scalar expressions."),
            LikeExpression like => CompileMappedLike(like, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            InExpression inExpr => CompileMappedIn(inExpr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            BetweenExpression between => CompileMappedBetween(between, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            IsNullExpression isNull => CompileMappedIsNull(isNull, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            _ => CompileMappedFallback(expr, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
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
                TokenType.BlobLiteral => DbValue.FromBlob((byte[])lit.Value),
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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var left = CompileMappedCore(bin.Left, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var right = CompileMappedCore(bin.Right, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        string? collation = CollationSupport.ResolveComparisonCollation(bin.Left, bin.Right, schema);
        SqlTypeDescriptor? comparisonType = ExpressionEvaluator.ResolveComparisonDeclaredType(bin.Left, bin.Right, schema);

        return (leftRow, rightRow) =>
        {
            var leftValue = left(leftRow, rightRow);
            var rightValue = right(leftRow, rightRow);

            if (bin.Op is BinaryOp.Equals or BinaryOp.NotEquals or
                BinaryOp.LessThan or BinaryOp.GreaterThan or
                BinaryOp.LessOrEqual or BinaryOp.GreaterOrEqual)
            {
                ExpressionEvaluator.CoerceComparisonOperands(
                    bin.Left,
                    bin.Right,
                    schema,
                    ref leftValue,
                    ref rightValue);
            }

            return bin.Op switch
            {
                BinaryOp.Equals => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison == 0),
                BinaryOp.NotEquals => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison != 0),
                BinaryOp.LessThan => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison < 0),
                BinaryOp.GreaterThan => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison > 0),
                BinaryOp.LessOrEqual => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison <= 0),
                BinaryOp.GreaterOrEqual => CompareOrNull(leftValue, rightValue, comparisonType, collation, static comparison => comparison >= 0),
                BinaryOp.And => SqlAnd(leftValue, rightValue),
                BinaryOp.Or => SqlOr(leftValue, rightValue),
                BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide =>
                    ExpressionEvaluator.EvaluateArithmetic(bin.Op, leftValue, rightValue),
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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var operand = CompileMappedCore(un.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);

        return (leftRow, rightRow) =>
        {
            var operandValue = operand(leftRow, rightRow);
            return un.Op switch
            {
                TokenType.Not => operandValue.IsNull
                    ? DbValue.Null
                    : BoolToDb(!operandValue.IsTruthy),
                TokenType.Minus => ExpressionEvaluator.NegateNumeric(operandValue),
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown unary op: {un.Op}"),
            };
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedCast(
        CastExpression cast,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        if (cast.TargetType.Kind == SqlTypeKind.Decimal &&
            ExpressionEvaluator.TryGetExactNumericLiteral(cast.Operand, out decimal exact))
        {
            DbValue exactValue = SqlTypeCoercion.Cast(DbValue.FromDecimal(exact), cast.TargetType);
            return (_, _) => exactValue;
        }

        if (cast.Operand is LiteralExpression)
        {
            DbValue value = ExpressionEvaluator.Evaluate(
                cast,
                ReadOnlySpan<DbValue>.Empty,
                schema,
                functions);
            return (_, _) => value;
        }

        JoinSpanExpressionEvaluator operand = CompileMappedCore(
            cast.Operand,
            schema,
            leftColumnCount,
            leftColumnMap,
            rightColumnMap,
            singleRowOnly,
            functions);
        SqlTypeDescriptor? sourceType = ExpressionEvaluator.ResolveDeclaredType(cast.Operand, schema);
        return (leftRow, rightRow) =>
            SqlTypeCoercion.Cast(operand(leftRow, rightRow), cast.TargetType, sourceType);
    }

    private static JoinSpanExpressionEvaluator CompileMappedFunction(
        FunctionCallExpression func,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (ScalarFunctionEvaluator.IsAggregateFunction(functionName))
            return CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);

        return functionName switch
        {
            "TEXT" => CompileMappedTextFunction(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
            _ => CompileMappedUserFunction(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions),
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedTextFunction(
        FunctionCallExpression func,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        if (func.IsStarArg || func.IsDistinct || func.Arguments.Count != 1)
            return CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);

        var argumentEvaluator = CompileMappedCore(func.Arguments[0], schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        return (leftRow, rightRow) => ScalarFunctionEvaluator.EvaluateTextValue(argumentEvaluator(leftRow, rightRow));
    }

    private static JoinSpanExpressionEvaluator CompileMappedUserFunction(
        FunctionCallExpression func,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        if (func.IsStarArg || func.IsDistinct)
            return CompileMappedFallback(func, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);

        var argumentEvaluators = new JoinSpanExpressionEvaluator[func.Arguments.Count];
        for (int i = 0; i < argumentEvaluators.Length; i++)
            argumentEvaluators[i] = CompileMappedCore(func.Arguments[i], schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);

        return (leftRow, rightRow) =>
        {
            var arguments = new DbValue[argumentEvaluators.Length];
            for (int i = 0; i < argumentEvaluators.Length; i++)
                arguments[i] = argumentEvaluators[i](leftRow, rightRow);

            return ScalarFunctionEvaluator.Evaluate(func, arguments, functions);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedLike(
        LikeExpression like,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var operandEval = CompileMappedCore(like.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var patternEval = CompileMappedCore(like.Pattern, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var escapeEval = like.EscapeChar != null
            ? CompileMappedCore(like.EscapeChar, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions)
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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var operandEval = CompileMappedCore(inExpr.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var valueEvals = new JoinSpanExpressionEvaluator[inExpr.Values.Count];
        for (int i = 0; i < inExpr.Values.Count; i++)
            valueEvals[i] = CompileMappedCore(inExpr.Values[i], schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        string? collation = CollationSupport.ResolveExpressionCollation(inExpr.Operand, schema);
        SqlTypeDescriptor? comparisonType = ExpressionEvaluator.ResolveDeclaredType(inExpr.Operand, schema);

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

                value = ExpressionEvaluator.CoerceValueForTypedExpression(
                    inExpr.Operand,
                    inExpr.Values[i],
                    value,
                    schema);

                if (ExpressionEvaluator.CompareValues(operand, value, comparisonType, collation) == 0)
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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var operandEval = CompileMappedCore(between.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var lowEval = CompileMappedCore(between.Low, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        var highEval = CompileMappedCore(between.High, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
        string? collation = CollationSupport.ResolveExpressionCollation(between.Operand, schema);
        SqlTypeDescriptor? comparisonType = ExpressionEvaluator.ResolveDeclaredType(between.Operand, schema);

        return (leftRow, rightRow) =>
        {
            var operand = operandEval(leftRow, rightRow);
            var low = lowEval(leftRow, rightRow);
            var high = highEval(leftRow, rightRow);
            if (operand.IsNull || low.IsNull || high.IsNull)
                return DbValue.Null;

            low = ExpressionEvaluator.CoerceValueForTypedExpression(
                between.Operand,
                between.Low,
                low,
                schema);
            high = ExpressionEvaluator.CoerceValueForTypedExpression(
                between.Operand,
                between.High,
                high,
                schema);

            bool inRange =
                ExpressionEvaluator.CompareValues(operand, low, comparisonType, collation) >= 0 &&
                ExpressionEvaluator.CompareValues(operand, high, comparisonType, collation) <= 0;
            return BoolToDb(between.Negated ? !inRange : inRange);
        };
    }

    private static JoinSpanExpressionEvaluator CompileMappedIsNull(
        IsNullExpression isNull,
        TableSchema schema,
        int leftColumnCount,
        int[]? leftColumnMap,
        int[]? rightColumnMap,
        bool singleRowOnly,
        DbFunctionRegistry? functions)
    {
        var operandEval = CompileMappedCore(isNull.Operand, schema, leftColumnCount, leftColumnMap, rightColumnMap, singleRowOnly, functions);
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
        bool singleRowOnly,
        DbFunctionRegistry? functions)
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
            return ExpressionEvaluator.Evaluate(expr, materializedRow, schema, functions);
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

    private static DbValue CompareOrNull(
        DbValue left,
        DbValue right,
        SqlTypeDescriptor? declaredType,
        string? collation,
        Func<int, bool> predicate)
    {
        if (left.IsNull || right.IsNull)
            return DbValue.Null;

        return BoolToDb(
            predicate(ExpressionEvaluator.CompareValues(left, right, declaredType, collation)));
    }

    private static DbValue SqlAnd(DbValue left, DbValue right)
    {
        if ((!left.IsNull && !left.IsTruthy) ||
            (!right.IsNull && !right.IsTruthy))
        {
            return BoolToDb(false);
        }

        return left.IsNull || right.IsNull
            ? DbValue.Null
            : BoolToDb(true);
    }

    private static DbValue SqlOr(DbValue left, DbValue right)
    {
        if ((!left.IsNull && left.IsTruthy) ||
            (!right.IsNull && right.IsTruthy))
        {
            return BoolToDb(true);
        }

        return left.IsNull || right.IsNull
            ? DbValue.Null
            : BoolToDb(false);
    }

    private static bool SqlLikeMatch(string text, string pattern, char? escape)
    {
        int ti = 0;
        int pi = 0;
        int starTi = -1;
        int starPi = -1;

        while (ti < text.Length)
        {
            if (pi < pattern.Length &&
                escape.HasValue &&
                pattern[pi] == escape.Value)
            {
                if (pi + 1 >= pattern.Length)
                    return false;

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

        while (pi < pattern.Length &&
               pattern[pi] == '%' &&
               (!escape.HasValue ||
                pattern[pi] != escape.Value))
        {
            pi++;
        }

        return pi == pattern.Length;
    }
}
