using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

public static class ExpressionEvaluator
{
    public static DbValue Evaluate(Expression expr, DbValue[] row, TableSchema schema)
        => Evaluate(expr, row.AsSpan(), schema, DbFunctionRegistry.Empty);

    public static DbValue Evaluate(Expression expr, DbValue[] row, TableSchema schema, DbFunctionRegistry? functions)
        => Evaluate(expr, row.AsSpan(), schema, functions);

    public static DbValue Evaluate(Expression expr, ReadOnlySpan<DbValue> row, TableSchema schema)
        => Evaluate(expr, row, schema, DbFunctionRegistry.Empty);

    public static DbValue Evaluate(Expression expr, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        return expr switch
        {
            LiteralExpression lit => EvalLiteral(lit),
            ParameterExpression param => EvalParameter(param),
            ColumnRefExpression col => EvalColumn(col, row, schema),
            BinaryExpression bin => EvalBinary(bin, row, schema, functions),
            UnaryExpression un => EvalUnary(un, row, schema, functions),
            CollateExpression collate => Evaluate(collate.Operand, row, schema, functions),
            FunctionCallExpression func => EvalFunction(func, row, schema, functions),
            LikeExpression like => EvalLike(like, row, schema, functions),
            InExpression inExpr => EvalIn(inExpr, row, schema, functions),
            BetweenExpression bet => EvalBetween(bet, row, schema, functions),
            IsNullExpression isNull => EvalIsNull(isNull, row, schema, functions),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown expression type: {expr.GetType().Name}"),
        };
    }

    private static DbValue EvalLiteral(LiteralExpression lit)
    {
        if (lit.Value == null) return DbValue.Null;
        return lit.LiteralType switch
        {
            TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
            TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
            TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
            TokenType.BlobLiteral => DbValue.FromBlob((byte[])lit.Value),
            TokenType.Null => DbValue.Null,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
        };
    }

    private static DbValue EvalParameter(ParameterExpression param) =>
        throw new CSharpDbException(ErrorCode.SyntaxError, $"Unbound parameter '@{param.Name}'.");

    private static DbValue EvalColumn(ColumnRefExpression col, ReadOnlySpan<DbValue> row, TableSchema schema)
    {
        int idx;
        if (col.TableAlias != null)
        {
            idx = schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName);
            if (idx < 0)
                throw new CSharpDbException(ErrorCode.ColumnNotFound,
                    $"Column '{col.TableAlias}.{col.ColumnName}' not found.");
        }
        else
        {
            idx = schema.GetColumnIndex(col.ColumnName);
            if (idx < 0)
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{col.ColumnName}' not found.");
        }
        return idx < row.Length ? row[idx] : DbValue.Null;
    }

    private static DbValue EvalBinary(BinaryExpression bin, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var left = Evaluate(bin.Left, row, schema, functions);
        var right = Evaluate(bin.Right, row, schema, functions);
        string? collation = CollationSupport.ResolveComparisonCollation(bin.Left, bin.Right, schema);

        return bin.Op switch
        {
            BinaryOp.Equals => BoolToDb(CollationSupport.Compare(left, right, collation) == 0),
            BinaryOp.NotEquals => BoolToDb(CollationSupport.Compare(left, right, collation) != 0),
            BinaryOp.LessThan => BoolToDb(CollationSupport.Compare(left, right, collation) < 0),
            BinaryOp.GreaterThan => BoolToDb(CollationSupport.Compare(left, right, collation) > 0),
            BinaryOp.LessOrEqual => BoolToDb(CollationSupport.Compare(left, right, collation) <= 0),
            BinaryOp.GreaterOrEqual => BoolToDb(CollationSupport.Compare(left, right, collation) >= 0),
            BinaryOp.And => BoolToDb(left.IsTruthy && right.IsTruthy),
            BinaryOp.Or => BoolToDb(left.IsTruthy || right.IsTruthy),
            BinaryOp.Plus => ArithmeticOp(left, right, (a, b) => a + b, (a, b) => a + b),
            BinaryOp.Minus => ArithmeticOp(left, right, (a, b) => a - b, (a, b) => a - b),
            BinaryOp.Multiply => ArithmeticOp(left, right, (a, b) => a * b, (a, b) => a * b),
            BinaryOp.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : throw DivZero(), (a, b) => b != 0 ? a / b : throw DivZero()),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown binary op: {bin.Op}"),
        };
    }

    private static DbValue EvalUnary(UnaryExpression un, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(un.Operand, row, schema, functions);
        return un.Op switch
        {
            TokenType.Not => BoolToDb(!operand.IsTruthy),
            TokenType.Minus => operand.Type switch
            {
                DbType.Integer => DbValue.FromInteger(-operand.AsInteger),
                DbType.Real => DbValue.FromReal(-operand.AsReal),
                _ => throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot negate non-numeric value."),
            },
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown unary op: {un.Op}"),
        };
    }

    private static DbValue EvalFunction(FunctionCallExpression func, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (ScalarFunctionEvaluator.IsAggregateFunction(functionName))
            throw new CSharpDbException(ErrorCode.Unknown, $"Aggregate function '{func.FunctionName}' requires aggregate context.");

        var materializedRow = row.ToArray();
        return ScalarFunctionEvaluator.Evaluate(
            func,
            arg => Evaluate(arg, materializedRow, schema, functions),
            functions);
    }

    private static DbValue BoolToDb(bool value) => DbValue.FromInteger(value ? 1 : 0);

    private static DbValue ArithmeticOp(DbValue left, DbValue right,
        Func<long, long, long> intOp, Func<double, double, double> realOp)
    {
        if (left.IsNull || right.IsNull) return DbValue.Null;

        if (left.Type == DbType.Real || right.Type == DbType.Real)
            return DbValue.FromReal(realOp(left.AsReal, right.AsReal));

        if (left.Type == DbType.Integer && right.Type == DbType.Integer)
            return DbValue.FromInteger(intOp(left.AsInteger, right.AsInteger));

        throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot perform arithmetic on non-numeric values.");
    }

    private static Exception DivZero() =>
        new CSharpDbException(ErrorCode.Unknown, "Division by zero.");

    private static DbValue EvalLike(LikeExpression like, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(like.Operand, row, schema, functions);
        var pattern = Evaluate(like.Pattern, row, schema, functions);
        if (operand.IsNull || pattern.IsNull) return DbValue.Null;

        char? escape = null;
        if (like.EscapeChar != null)
        {
            var esc = Evaluate(like.EscapeChar, row, schema, functions);
            if (!esc.IsNull)
            {
                string escStr = esc.AsText;
                if (escStr.Length == 1) escape = escStr[0];
            }
        }

        bool match = SqlLikeMatch(operand.AsText, pattern.AsText, escape);
        return BoolToDb(like.Negated ? !match : match);
    }

    private static DbValue EvalIn(InExpression inExpr, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(inExpr.Operand, row, schema, functions);
        if (operand.IsNull) return DbValue.Null;

        string? collation = CollationSupport.ResolveExpressionCollation(inExpr.Operand, schema);
        bool found = false;
        bool hasNull = false;
        foreach (var valExpr in inExpr.Values)
        {
            var val = Evaluate(valExpr, row, schema, functions);
            if (val.IsNull) { hasNull = true; continue; }
            if (CollationSupport.Compare(operand, val, collation) == 0) { found = true; break; }
        }

        if (found) return BoolToDb(!inExpr.Negated);
        if (hasNull) return DbValue.Null;
        return BoolToDb(inExpr.Negated);
    }

    private static DbValue EvalBetween(BetweenExpression bet, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(bet.Operand, row, schema, functions);
        var low = Evaluate(bet.Low, row, schema, functions);
        var high = Evaluate(bet.High, row, schema, functions);
        if (operand.IsNull || low.IsNull || high.IsNull) return DbValue.Null;

        string? collation = CollationSupport.ResolveExpressionCollation(bet.Operand, schema);
        bool inRange = CollationSupport.Compare(operand, low, collation) >= 0 &&
            CollationSupport.Compare(operand, high, collation) <= 0;
        return BoolToDb(bet.Negated ? !inRange : inRange);
    }

    private static DbValue EvalIsNull(IsNullExpression isNull, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(isNull.Operand, row, schema, functions);
        bool result = operand.IsNull;
        return BoolToDb(isNull.Negated ? !result : result);
    }

    /// <summary>SQL LIKE pattern matcher. % matches any sequence, _ matches any single char.</summary>
    internal static bool SqlLikeMatch(string text, string pattern, char? escape)
    {
        int ti = 0, pi = 0;
        int starTi = -1, starPi = -1;

        while (ti < text.Length)
        {
            if (pi < pattern.Length && escape.HasValue && pattern[pi] == escape.Value && pi + 1 < pattern.Length)
            {
                // Escaped character — must match literally
                pi++;
                if (ti < text.Length && char.ToUpperInvariant(text[ti]) == char.ToUpperInvariant(pattern[pi]))
                { ti++; pi++; }
                else if (starPi >= 0) { ti = ++starTi; pi = starPi; }
                else return false;
            }
            else if (pi < pattern.Length && pattern[pi] == '%')
            {
                starPi = ++pi;
                starTi = ti;
            }
            else if (pi < pattern.Length && (pattern[pi] == '_' || char.ToUpperInvariant(text[ti]) == char.ToUpperInvariant(pattern[pi])))
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

        while (pi < pattern.Length && pattern[pi] == '%') pi++;
        return pi == pattern.Length;
    }
}
