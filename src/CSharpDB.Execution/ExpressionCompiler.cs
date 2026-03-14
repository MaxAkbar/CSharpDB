using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Compiles expression trees into delegates that bind column indices once.
/// This avoids repeated schema lookups on every row.
/// </summary>
internal static class ExpressionCompiler
{
    public static Func<DbValue[], DbValue> Compile(Expression expr, TableSchema schema)
    {
        return expr switch
        {
            LiteralExpression lit => CompileLiteral(lit),
            ParameterExpression param => CompileParameter(param),
            ColumnRefExpression col => CompileColumn(col, schema),
            BinaryExpression bin => CompileBinary(bin, schema),
            UnaryExpression un => CompileUnary(un, schema),
            FunctionCallExpression func => CompileFunction(func, schema),
            LikeExpression like => CompileLike(like, schema),
            InExpression inExpr => CompileIn(inExpr, schema),
            BetweenExpression between => CompileBetween(between, schema),
            IsNullExpression isNull => CompileIsNull(isNull, schema),
            _ => row => ExpressionEvaluator.Evaluate(expr, row, schema),
        };
    }

    private static Func<DbValue[], DbValue> CompileLiteral(LiteralExpression lit)
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

        return _ => value;
    }

    private static Func<DbValue[], DbValue> CompileParameter(ParameterExpression param)
    {
        return _ => throw new CSharpDbException(ErrorCode.SyntaxError, $"Unbound parameter '@{param.Name}'.");
    }

    private static Func<DbValue[], DbValue> CompileColumn(ColumnRefExpression col, TableSchema schema)
    {
        int idx;
        if (col.TableAlias != null)
        {
            idx = schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName);
            if (idx < 0)
                throw new CSharpDbException(
                    ErrorCode.ColumnNotFound,
                    $"Column '{col.TableAlias}.{col.ColumnName}' not found.");
        }
        else
        {
            idx = schema.GetColumnIndex(col.ColumnName);
            if (idx < 0)
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{col.ColumnName}' not found.");
        }

        return row => idx < row.Length ? row[idx] : DbValue.Null;
    }

    private static Func<DbValue[], DbValue> CompileBinary(BinaryExpression bin, TableSchema schema)
    {
        var left = Compile(bin.Left, schema);
        var right = Compile(bin.Right, schema);

        return row =>
        {
            var lv = left(row);
            var rv = right(row);

            return bin.Op switch
            {
                BinaryOp.Equals => BoolToDb(DbValue.Compare(lv, rv) == 0),
                BinaryOp.NotEquals => BoolToDb(DbValue.Compare(lv, rv) != 0),
                BinaryOp.LessThan => BoolToDb(DbValue.Compare(lv, rv) < 0),
                BinaryOp.GreaterThan => BoolToDb(DbValue.Compare(lv, rv) > 0),
                BinaryOp.LessOrEqual => BoolToDb(DbValue.Compare(lv, rv) <= 0),
                BinaryOp.GreaterOrEqual => BoolToDb(DbValue.Compare(lv, rv) >= 0),
                BinaryOp.And => BoolToDb(lv.IsTruthy && rv.IsTruthy),
                BinaryOp.Or => BoolToDb(lv.IsTruthy || rv.IsTruthy),
                BinaryOp.Plus => ArithmeticOp(lv, rv, static (a, b) => a + b, static (a, b) => a + b),
                BinaryOp.Minus => ArithmeticOp(lv, rv, static (a, b) => a - b, static (a, b) => a - b),
                BinaryOp.Multiply => ArithmeticOp(lv, rv, static (a, b) => a * b, static (a, b) => a * b),
                BinaryOp.Divide => ArithmeticOp(
                    lv,
                    rv,
                    static (a, b) => b != 0 ? a / b : throw DivZero(),
                    static (a, b) => b != 0 ? a / b : throw DivZero()),
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown binary op: {bin.Op}"),
            };
        };
    }

    private static Func<DbValue[], DbValue> CompileUnary(UnaryExpression un, TableSchema schema)
    {
        var operand = Compile(un.Operand, schema);

        return row =>
        {
            var ov = operand(row);
            return un.Op switch
            {
                TokenType.Not => BoolToDb(!ov.IsTruthy),
                TokenType.Minus => ov.Type switch
                {
                    DbType.Integer => DbValue.FromInteger(-ov.AsInteger),
                    DbType.Real => DbValue.FromReal(-ov.AsReal),
                    _ => throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot negate non-numeric value."),
                },
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown unary op: {un.Op}"),
            };
        };
    }

    private static Func<DbValue[], DbValue> CompileFunction(FunctionCallExpression func, TableSchema schema)
    {
        if (ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName.ToUpperInvariant()))
            return row => ExpressionEvaluator.Evaluate(func, row, schema);

        var argumentEvaluators = new Func<DbValue[], DbValue>[func.Arguments.Count];
        for (int i = 0; i < func.Arguments.Count; i++)
            argumentEvaluators[i] = Compile(func.Arguments[i], schema);

        return row => ScalarFunctionEvaluator.Evaluate(func, expr =>
        {
            for (int i = 0; i < func.Arguments.Count; i++)
            {
                if (ReferenceEquals(func.Arguments[i], expr))
                    return argumentEvaluators[i](row);
            }

            return ExpressionEvaluator.Evaluate(expr, row, schema);
        });
    }

    private static Func<DbValue[], DbValue> CompileLike(LikeExpression like, TableSchema schema)
    {
        var operandEval = Compile(like.Operand, schema);
        var patternEval = Compile(like.Pattern, schema);
        var escapeEval = like.EscapeChar != null ? Compile(like.EscapeChar, schema) : null;

        return row =>
        {
            var operand = operandEval(row);
            var pattern = patternEval(row);
            if (operand.IsNull || pattern.IsNull)
                return DbValue.Null;

            char? escape = null;
            if (escapeEval != null)
            {
                var esc = escapeEval(row);
                if (!esc.IsNull)
                {
                    string escStr = esc.AsText;
                    if (escStr.Length == 1)
                        escape = escStr[0];
                }
            }

            bool match = SqlLikeMatch(operand.AsText, pattern.AsText, escape);
            return BoolToDb(like.Negated ? !match : match);
        };
    }

    private static Func<DbValue[], DbValue> CompileIn(InExpression inExpr, TableSchema schema)
    {
        var operandEval = Compile(inExpr.Operand, schema);
        var valueEvals = new Func<DbValue[], DbValue>[inExpr.Values.Count];
        for (int i = 0; i < inExpr.Values.Count; i++)
            valueEvals[i] = Compile(inExpr.Values[i], schema);

        return row =>
        {
            var operand = operandEval(row);
            if (operand.IsNull) return DbValue.Null;

            bool found = false;
            bool hasNull = false;
            for (int i = 0; i < valueEvals.Length; i++)
            {
                var value = valueEvals[i](row);
                if (value.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                if (DbValue.Compare(operand, value) == 0)
                {
                    found = true;
                    break;
                }
            }

            if (found) return BoolToDb(!inExpr.Negated);
            if (hasNull) return DbValue.Null;
            return BoolToDb(inExpr.Negated);
        };
    }

    private static Func<DbValue[], DbValue> CompileBetween(BetweenExpression between, TableSchema schema)
    {
        var operandEval = Compile(between.Operand, schema);
        var lowEval = Compile(between.Low, schema);
        var highEval = Compile(between.High, schema);

        return row =>
        {
            var operand = operandEval(row);
            var low = lowEval(row);
            var high = highEval(row);
            if (operand.IsNull || low.IsNull || high.IsNull)
                return DbValue.Null;

            bool inRange = DbValue.Compare(operand, low) >= 0 && DbValue.Compare(operand, high) <= 0;
            return BoolToDb(between.Negated ? !inRange : inRange);
        };
    }

    private static Func<DbValue[], DbValue> CompileIsNull(IsNullExpression isNull, TableSchema schema)
    {
        var operandEval = Compile(isNull.Operand, schema);
        return row =>
        {
            var operand = operandEval(row);
            bool result = operand.IsNull;
            return BoolToDb(isNull.Negated ? !result : result);
        };
    }

    private static DbValue BoolToDb(bool value) => DbValue.FromInteger(value ? 1 : 0);

    private static DbValue ArithmeticOp(
        DbValue left,
        DbValue right,
        Func<long, long, long> intOp,
        Func<double, double, double> realOp)
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

    private static bool SqlLikeMatch(string text, string pattern, char? escape)
    {
        int ti = 0, pi = 0;
        int starTi = -1, starPi = -1;

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
            else if (pi < pattern.Length
                && (pattern[pi] == '_' || char.ToUpperInvariant(text[ti]) == char.ToUpperInvariant(pattern[pi])))
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
