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
            CastExpression cast => EvalCast(cast, row, schema, functions),
            FunctionCallExpression func => EvalFunction(func, row, schema, functions),
            WindowFunctionExpression => throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window functions require window planning context and cannot be evaluated as scalar expressions."),
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
        if (bin.Op is BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide)
            ValidateArithmeticOperands(bin.Left, bin.Right, schema);

        SqlTypeDescriptor? arithmeticType =
            bin.Op is BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide
                ? ResolveArithmeticDeclaredType(bin.Left, bin.Right, schema)
                : null;

        var left = Evaluate(bin.Left, row, schema, functions);
        var right = Evaluate(bin.Right, row, schema, functions);
        string? collation = CollationSupport.ResolveComparisonCollation(bin.Left, bin.Right, schema);

        if (bin.Op is BinaryOp.Equals or BinaryOp.NotEquals or
            BinaryOp.LessThan or BinaryOp.GreaterThan or
            BinaryOp.LessOrEqual or BinaryOp.GreaterOrEqual)
        {
            CoerceComparisonOperands(bin.Left, bin.Right, schema, ref left, ref right);
        }

        SqlTypeDescriptor? comparisonType = ResolveComparisonDeclaredType(bin.Left, bin.Right, schema);

        return bin.Op switch
        {
            BinaryOp.Equals => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison == 0),
            BinaryOp.NotEquals => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison != 0),
            BinaryOp.LessThan => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison < 0),
            BinaryOp.GreaterThan => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison > 0),
            BinaryOp.LessOrEqual => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison <= 0),
            BinaryOp.GreaterOrEqual => CompareOrNull(left, right, comparisonType, collation, static comparison => comparison >= 0),
            BinaryOp.And => SqlAnd(left, right),
            BinaryOp.Or => SqlOr(left, right),
            BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide =>
                EvaluateArithmetic(bin.Op, left, right, arithmeticType),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown binary op: {bin.Op}"),
        };
    }

    internal static void CoerceComparisonOperands(
        Expression leftExpression,
        Expression rightExpression,
        TableSchema schema,
        ref DbValue left,
        ref DbValue right)
    {
        SqlTypeDescriptor? leftType = ResolveDeclaredType(leftExpression, schema);
        SqlTypeDescriptor? rightType = ResolveDeclaredType(rightExpression, schema);

        if (leftType is not null && rightType is null)
            right = CoerceValueForDeclaredType(rightExpression, right, leftType);
        else if (rightType is not null && leftType is null)
            left = CoerceValueForDeclaredType(leftExpression, left, rightType);
    }

    internal static DbValue CoerceValueForTypedExpression(
        Expression typedExpression,
        Expression valueExpression,
        DbValue value,
        TableSchema schema)
    {
        SqlTypeDescriptor? declaredType = ResolveDeclaredType(typedExpression, schema);
        return declaredType is null
            ? value
            : CoerceValueForDeclaredType(valueExpression, value, declaredType);
    }

    internal static SqlTypeDescriptor? ResolveDeclaredType(Expression expression, TableSchema schema)
    {
        while (expression is CollateExpression collate)
            expression = collate.Operand;

        if (expression is LiteralExpression
            {
                LiteralType: TokenType.IntegerLiteral,
                Value: long integerValue,
            })
        {
            return IntegerTypeForValue(integerValue);
        }

        if (expression is CastExpression cast)
            return cast.TargetType;

        if (expression is UnaryExpression unary)
        {
            return unary.Op == TokenType.Minus
                ? ResolveNegatedDeclaredType(unary.Operand, schema)
                : ResolveDeclaredType(unary.Operand, schema);
        }

        if (expression is BinaryExpression binary &&
            binary.Op is BinaryOp.Plus or BinaryOp.Minus or BinaryOp.Multiply or BinaryOp.Divide)
        {
            return ResolveArithmeticDeclaredType(binary.Left, binary.Right, schema);
        }

        if (expression is FunctionCallExpression
            {
                Arguments.Count: 1,
                FunctionName: var functionName,
            } function &&
            functionName.ToUpperInvariant() is "MIN" or "MAX" or "SUM" or "AVG")
        {
            SqlTypeDescriptor? argumentType = ResolveDeclaredType(function.Arguments[0], schema);
            return functionName.Equals("SUM", StringComparison.OrdinalIgnoreCase) &&
                argumentType is not null &&
                IsIntegralType(argumentType.Kind)
                    ? SqlTypeDescriptor.Create(SqlTypeKind.BigInt)
                    : argumentType;
        }

        if (expression is FunctionCallExpression
            {
                Arguments.Count: >= 1,
                FunctionName: var preservingFunctionName,
            } preservingFunction &&
            preservingFunctionName.ToUpperInvariant() is "ABS" or "ROUND" or "INT" or "FLOOR" or "FIX")
        {
            SqlTypeDescriptor? argumentType = ResolveDeclaredType(
                preservingFunction.Arguments[0],
                schema);
            return argumentType?.Kind switch
            {
                SqlTypeKind.Integer => SqlTypeDescriptor.Create(SqlTypeKind.Integer),
                SqlTypeKind.BigInt => SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
                SqlTypeKind.TinyInt or SqlTypeKind.SmallInt =>
                    SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
                _ => argumentType,
            };
        }

        if (expression is FunctionCallExpression scalarFunction &&
            DbBuiltInFunctionRegistry.TryGet(
                scalarFunction.FunctionName,
                out DbBuiltInFunctionDescriptor descriptor) &&
            descriptor.DeclaredReturnType is not null)
        {
            return descriptor.DeclaredReturnType;
        }

        if (expression is WindowFunctionExpression window)
        {
            string windowFunctionName = window.Function.FunctionName.ToUpperInvariant();
            if (windowFunctionName is "ROW_NUMBER" or "RANK" or "DENSE_RANK" or "COUNT")
                return SqlTypeDescriptor.Create(SqlTypeKind.BigInt);

            if (window.Function.Arguments.Count > 0)
                return ResolveDeclaredType(window.Function.Arguments[0], schema);
        }

        return TryResolveDeclaredColumn(expression, schema, out ColumnDefinition? column)
            ? column!.EffectiveType
            : null;
    }

    internal static SqlTypeDescriptor? ResolveArithmeticDeclaredType(
        Expression leftExpression,
        Expression rightExpression,
        TableSchema schema)
    {
        SqlTypeDescriptor? left = ResolveDeclaredType(leftExpression, schema);
        SqlTypeDescriptor? right = ResolveDeclaredType(rightExpression, schema);

        if (left is null || right is null)
        {
            SqlTypeDescriptor? known = left ?? right;
            return known is not null && IsIntegralType(known.Kind)
                ? known
                : null;
        }

        if (!IsIntegralType(left.Kind) || !IsIntegralType(right.Kind))
            return null;

        if (left.Kind == SqlTypeKind.BigInt || right.Kind == SqlTypeKind.BigInt)
            return SqlTypeDescriptor.Create(SqlTypeKind.BigInt);

        if (left.Kind == SqlTypeKind.Integer && right.Kind == SqlTypeKind.Integer)
            return SqlTypeDescriptor.Create(SqlTypeKind.Integer);

        // Preserve the existing widening behavior for the narrower integer
        // families. The SQL 4.5 semantic change specifically narrows INTEGER
        // arithmetic; legacy descriptor-less integers resolve as BIGINT above.
        return SqlTypeDescriptor.Create(SqlTypeKind.BigInt);
    }

    internal static SqlTypeDescriptor? ResolveNegatedDeclaredType(
        Expression operand,
        TableSchema schema)
    {
        while (operand is CollateExpression collate)
            operand = collate.Operand;

        if (operand is LiteralExpression
            {
                LiteralType: TokenType.IntegerLiteral,
                Value: long value,
            })
        {
            // 2147483648 is BIGINT as a positive literal, but its directly
            // negated spelling is the representable INTEGER minimum.
            if (value is >= 0 and <= 2_147_483_648L)
                return SqlTypeDescriptor.Create(SqlTypeKind.Integer);

            return SqlTypeDescriptor.Create(SqlTypeKind.BigInt);
        }

        SqlTypeDescriptor? type = ResolveDeclaredType(operand, schema);
        return type?.Kind switch
        {
            SqlTypeKind.Integer => SqlTypeDescriptor.Create(SqlTypeKind.Integer),
            SqlTypeKind.BigInt => SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
            SqlTypeKind.TinyInt or SqlTypeKind.SmallInt =>
                SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
            _ => type,
        };
    }

    internal static SqlTypeDescriptor IntegerTypeForValue(long value) =>
        SqlTypeDescriptor.Create(
            value is >= int.MinValue and <= int.MaxValue
                ? SqlTypeKind.Integer
                : SqlTypeKind.BigInt);

    private static bool IsIntegralType(SqlTypeKind kind) =>
        kind is SqlTypeKind.TinyInt or
            SqlTypeKind.SmallInt or
            SqlTypeKind.Integer or
            SqlTypeKind.BigInt;

    internal static SqlTypeDescriptor? ResolveComparisonDeclaredType(
        Expression leftExpression,
        Expression rightExpression,
        TableSchema schema)
    {
        SqlTypeDescriptor? left = ResolveDeclaredType(leftExpression, schema);
        SqlTypeDescriptor? right = ResolveDeclaredType(rightExpression, schema);
        bool leftIsInterval = SqlTypeCoercion.IsInterval(left);
        bool rightIsInterval = SqlTypeCoercion.IsInterval(right);

        if (leftIsInterval && rightIsInterval && left!.Kind != right!.Kind)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Cannot compare {left.ToSql()} with {right.ToSql()}.");
        }

        return leftIsInterval ? left : rightIsInterval ? right : null;
    }

    internal static int CompareValues(
        DbValue left,
        DbValue right,
        SqlTypeDescriptor? declaredType,
        string? collation = null) =>
        SqlTypeCoercion.Compare(left, right, declaredType, collation);

    private static DbValue CoerceValueForDeclaredType(
        Expression valueExpression,
        DbValue value,
        SqlTypeDescriptor declaredType)
    {
        if (value.IsNull)
            return value;

        if (declaredType.Kind == SqlTypeKind.Decimal &&
            TryGetExactNumericLiteral(valueExpression, out decimal exact))
        {
            value = DbValue.FromDecimal(exact);
        }

        var column = new ColumnDefinition
        {
            Name = "<expression>",
            Type = declaredType.StorageType,
            DeclaredType = declaredType,
            Nullable = true,
        };
        return SqlTypeCoercion.CoerceForAssignment(value, column);
    }

    private static bool TryResolveDeclaredColumn(
        Expression expression,
        TableSchema schema,
        out ColumnDefinition? column)
    {
        while (expression is CollateExpression collate)
            expression = collate.Operand;

        if (expression is not ColumnRefExpression columnRef)
        {
            column = null;
            return false;
        }

        int index = columnRef.TableAlias is null
            ? schema.GetColumnIndex(columnRef.ColumnName)
            : schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName);
        if (index < 0 || index >= schema.Columns.Count || schema.Columns[index].DeclaredType is null)
        {
            column = null;
            return false;
        }

        column = schema.Columns[index];
        return true;
    }

    internal static bool TryGetExactNumericLiteral(Expression expression, out decimal value)
    {
        while (expression is CollateExpression collate)
            expression = collate.Operand;

        bool negate = false;
        if (expression is UnaryExpression { Op: TokenType.Minus } unary)
        {
            negate = true;
            expression = unary.Operand;
        }

        if (expression is LiteralExpression
            {
                RawText: { Length: > 0 } rawText,
                LiteralType: TokenType.IntegerLiteral or TokenType.RealLiteral,
            } &&
            decimal.TryParse(
                rawText,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out value))
        {
            if (negate)
                value = -value;
            return true;
        }

        value = default;
        return false;
    }

    private static DbValue EvalUnary(UnaryExpression un, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        if (un.Op == TokenType.Minus)
            ValidateNumericNegationOperand(un.Operand, schema);

        SqlTypeDescriptor? resultType = un.Op == TokenType.Minus
            ? ResolveNegatedDeclaredType(un.Operand, schema)
            : null;

        var operand = Evaluate(un.Operand, row, schema, functions);
        return un.Op switch
        {
            TokenType.Not => operand.IsNull ? DbValue.Null : BoolToDb(!operand.IsTruthy),
            TokenType.Minus => NegateNumeric(operand, resultType),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown unary op: {un.Op}"),
        };
    }

    internal static DbValue NegateNumeric(
        DbValue operand,
        SqlTypeDescriptor? resultType = null)
    {
        DbValue result = operand.Type switch
        {
            DbType.Null => DbValue.Null,
            DbType.Integer => DbValue.FromInteger(checked(-operand.AsInteger)),
            DbType.Real => DbValue.FromReal(-operand.AsReal),
            DbType.Decimal => DbValue.FromDecimal(checked(-operand.AsDecimal)),
            _ => throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot negate non-numeric value."),
        };
        return EnforceDeclaredIntegerRange(result, resultType);
    }

    internal static void ValidateArithmeticOperands(
        Expression left,
        Expression right,
        TableSchema schema)
    {
        if (IsBooleanExpression(left, schema) || IsBooleanExpression(right, schema))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "BOOLEAN values do not participate in numeric arithmetic; CAST explicitly to an integer type first.");
        }
    }

    internal static void ValidateNumericNegationOperand(Expression operand, TableSchema schema)
    {
        if (IsBooleanExpression(operand, schema))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "BOOLEAN values cannot be numerically negated; CAST explicitly to an integer type first.");
        }
    }

    private static bool IsBooleanExpression(Expression expression, TableSchema schema)
    {
        while (expression is CollateExpression collate)
            expression = collate.Operand;

        if (expression is CastExpression cast)
            return cast.TargetType.Kind == SqlTypeKind.Boolean;

        if (expression is ColumnRefExpression)
        {
            return TryResolveDeclaredColumn(expression, schema, out ColumnDefinition? column) &&
                column!.EffectiveType.Kind == SqlTypeKind.Boolean;
        }

        if (expression is UnaryExpression { Op: TokenType.Not })
            return true;

        if (expression is BinaryExpression binary)
        {
            return binary.Op is BinaryOp.Equals or
                BinaryOp.NotEquals or
                BinaryOp.LessThan or
                BinaryOp.GreaterThan or
                BinaryOp.LessOrEqual or
                BinaryOp.GreaterOrEqual or
                BinaryOp.And or
                BinaryOp.Or;
        }

        if (expression is LikeExpression or
            InExpression or
            InSubqueryExpression or
            ExistsExpression or
            BetweenExpression or
            IsNullExpression)
        {
            return true;
        }

        return ResolveDeclaredType(expression, schema)?.Kind == SqlTypeKind.Boolean;
    }

    private static DbValue EvalFunction(FunctionCallExpression func, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (ScalarFunctionEvaluator.IsAggregateFunction(functionName))
            throw new CSharpDbException(ErrorCode.Unknown, $"Aggregate function '{func.FunctionName}' requires aggregate context.");

        var materializedRow = row.ToArray();
        DbValue result = ScalarFunctionEvaluator.Evaluate(
            func,
            arg => Evaluate(arg, materializedRow, schema, functions),
            functions);
        return EnforceDeclaredIntegerRange(result, ResolveDeclaredType(func, schema));
    }

    private static DbValue EvalCast(
        CastExpression cast,
        ReadOnlySpan<DbValue> row,
        TableSchema schema,
        DbFunctionRegistry? functions)
    {
        // Preserve the original spelling for exact-numeric casts. Parsing a
        // REAL token through double first would make decimal(18,s) lossy.
        if (cast.TargetType.Kind == SqlTypeKind.Decimal &&
            TryGetExactNumericLiteral(cast.Operand, out decimal exact))
        {
            return SqlTypeCoercion.Cast(DbValue.FromDecimal(exact), cast.TargetType);
        }

        return SqlTypeCoercion.Cast(
            Evaluate(cast.Operand, row, schema, functions),
            cast.TargetType,
            ResolveDeclaredType(cast.Operand, schema));
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

        return BoolToDb(predicate(CompareValues(left, right, declaredType, collation)));
    }

    private static DbValue SqlAnd(DbValue left, DbValue right)
    {
        if ((!left.IsNull && !left.IsTruthy) || (!right.IsNull && !right.IsTruthy))
            return BoolToDb(false);

        if (left.IsNull || right.IsNull)
            return DbValue.Null;

        return BoolToDb(true);
    }

    private static DbValue SqlOr(DbValue left, DbValue right)
    {
        if ((!left.IsNull && left.IsTruthy) || (!right.IsNull && right.IsTruthy))
            return BoolToDb(true);

        if (left.IsNull || right.IsNull)
            return DbValue.Null;

        return BoolToDb(false);
    }

    internal static DbValue EvaluateArithmetic(
        BinaryOp op,
        DbValue left,
        DbValue right,
        SqlTypeDescriptor? resultType = null)
    {
        if (left.IsNull || right.IsNull) return DbValue.Null;

        if (left.Type == DbType.Real || right.Type == DbType.Real)
        {
            double leftValue = left.AsReal;
            double rightValue = right.AsReal;
            return DbValue.FromReal(op switch
            {
                BinaryOp.Plus => leftValue + rightValue,
                BinaryOp.Minus => leftValue - rightValue,
                BinaryOp.Multiply => leftValue * rightValue,
                BinaryOp.Divide => rightValue != 0d ? leftValue / rightValue : throw DivZero(),
                _ => throw new ArgumentOutOfRangeException(nameof(op), op, null),
            });
        }

        if (left.Type == DbType.Decimal || right.Type == DbType.Decimal)
        {
            decimal leftValue = left.Type == DbType.Decimal ? left.AsDecimal : left.AsInteger;
            decimal rightValue = right.Type == DbType.Decimal ? right.AsDecimal : right.AsInteger;
            decimal result = op switch
            {
                BinaryOp.Plus => checked(leftValue + rightValue),
                BinaryOp.Minus => checked(leftValue - rightValue),
                BinaryOp.Multiply => checked(leftValue * rightValue),
                BinaryOp.Divide => rightValue != 0m ? leftValue / rightValue : throw DivZero(),
                _ => throw new ArgumentOutOfRangeException(nameof(op), op, null),
            };
            return CreateDecimalResult(result);
        }

        if (left.Type == DbType.Integer && right.Type == DbType.Integer)
        {
            long leftValue = left.AsInteger;
            long rightValue = right.AsInteger;
            DbValue result = DbValue.FromInteger(op switch
            {
                BinaryOp.Plus => checked(leftValue + rightValue),
                BinaryOp.Minus => checked(leftValue - rightValue),
                BinaryOp.Multiply => checked(leftValue * rightValue),
                BinaryOp.Divide => rightValue != 0 ? checked(leftValue / rightValue) : throw DivZero(),
                _ => throw new ArgumentOutOfRangeException(nameof(op), op, null),
            });
            return EnforceDeclaredIntegerRange(result, resultType);
        }

        throw new CSharpDbException(ErrorCode.TypeMismatch, "Cannot perform arithmetic on non-numeric values.");
    }

    internal static DbValue EnforceDeclaredIntegerRange(
        DbValue value,
        SqlTypeDescriptor? resultType)
    {
        if (value.Type == DbType.Integer &&
            resultType?.Kind == SqlTypeKind.Integer &&
            value.AsInteger is < int.MinValue or > int.MaxValue)
        {
            throw new OverflowException(
                $"INTEGER result {value.AsInteger} is outside the range " +
                $"{int.MinValue} through {int.MaxValue}.");
        }

        return value;
    }

    private static DbValue CreateDecimalResult(decimal value)
    {
        try
        {
            return DbValue.FromDecimal(value);
        }
        catch (OverflowException)
        {
            decimal integral = decimal.Truncate(decimal.Abs(value));
            int integralDigits = integral == 0m
                ? 0
                : integral.ToString("0", System.Globalization.CultureInfo.InvariantCulture).Length;
            int scale = SqlTypeDescriptor.MaximumDecimalPrecision - integralDigits;
            if (scale < 0)
                throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    $"Decimal arithmetic result '{value}' exceeds {SqlTypeDescriptor.MaximumDecimalPrecision} digits.");

            decimal rounded = decimal.Round(value, scale, MidpointRounding.ToEven);
            try
            {
                return DbValue.FromDecimal(rounded);
            }
            catch (OverflowException ex)
            {
                throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    $"Decimal arithmetic result '{value}' exceeds {SqlTypeDescriptor.MaximumDecimalPrecision} digits.",
                    ex);
            }
        }
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
        SqlTypeDescriptor? comparisonType = ResolveDeclaredType(inExpr.Operand, schema);
        bool found = false;
        bool hasNull = false;
        foreach (var valExpr in inExpr.Values)
        {
            var val = Evaluate(valExpr, row, schema, functions);
            if (val.IsNull) { hasNull = true; continue; }
            val = CoerceValueForTypedExpression(inExpr.Operand, valExpr, val, schema);
            if (CompareValues(operand, val, comparisonType, collation) == 0) { found = true; break; }
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

        low = CoerceValueForTypedExpression(bet.Operand, bet.Low, low, schema);
        high = CoerceValueForTypedExpression(bet.Operand, bet.High, high, schema);

        string? collation = CollationSupport.ResolveExpressionCollation(bet.Operand, schema);
        SqlTypeDescriptor? comparisonType = ResolveDeclaredType(bet.Operand, schema);
        bool inRange = CompareValues(operand, low, comparisonType, collation) >= 0 &&
            CompareValues(operand, high, comparisonType, collation) <= 0;
        return BoolToDb(bet.Negated ? !inRange : inRange);
    }

    private static DbValue EvalIsNull(IsNullExpression isNull, ReadOnlySpan<DbValue> row, TableSchema schema, DbFunctionRegistry? functions)
    {
        var operand = Evaluate(isNull.Operand, row, schema, functions);
        bool result = operand.IsNull;
        return BoolToDb(isNull.Negated ? !result : result);
    }

    /// <summary>SQL LIKE pattern matcher. % matches any sequence, _ matches one UTF-16 code unit.</summary>
    internal static bool SqlLikeMatch(string text, string pattern, char? escape)
    {
        int ti = 0, pi = 0;
        int starTi = -1, starPi = -1;

        while (ti < text.Length)
        {
            if (pi < pattern.Length &&
                escape.HasValue &&
                pattern[pi] == escape.Value)
            {
                if (pi + 1 >= pattern.Length)
                    return false;

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
