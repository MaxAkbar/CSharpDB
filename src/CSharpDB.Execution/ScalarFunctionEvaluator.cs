using System.Globalization;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class ScalarFunctionEvaluator
{
    public static bool IsAggregateFunction(string functionName)
        => functionName.ToUpperInvariant() is "COUNT" or "SUM" or "AVG" or "MIN" or "MAX";

    public static DbValue Evaluate(FunctionCallExpression func, Func<Expression, DbValue> evaluateArgument)
        => Evaluate(func, evaluateArgument, DbFunctionRegistry.Empty);

    public static DbValue Evaluate(
        FunctionCallExpression func,
        Func<Expression, DbValue> evaluateArgument,
        DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        return functionName switch
        {
            "TEXT" => EvaluateText(func, evaluateArgument),
            _ => EvaluateUserFunction(func, evaluateArgument, functions ?? DbFunctionRegistry.Empty),
        };
    }

    public static DbValue Evaluate(
        FunctionCallExpression func,
        DbValue[] arguments,
        DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (functionName == "TEXT")
        {
            if (func.IsStarArg || func.IsDistinct || arguments.Length != 1)
                throw new CSharpDbException(ErrorCode.SyntaxError, "TEXT() requires exactly one argument.");

            return EvaluateTextValue(arguments[0]);
        }

        return EvaluateUserFunction(func, arguments, functions ?? DbFunctionRegistry.Empty);
    }

    private static DbValue EvaluateText(FunctionCallExpression func, Func<Expression, DbValue> evaluateArgument)
    {
        if (func.IsStarArg || func.IsDistinct || func.Arguments.Count != 1)
            throw new CSharpDbException(ErrorCode.SyntaxError, "TEXT() requires exactly one argument.");

        DbValue value = evaluateArgument(func.Arguments[0]);
        return EvaluateTextValue(value);
    }

    internal static DbValue EvaluateTextValue(DbValue value)
        => DbValue.FromText(ToDisplayText(value));

    private static DbValue EvaluateUserFunction(
        FunctionCallExpression func,
        Func<Expression, DbValue> evaluateArgument,
        DbFunctionRegistry functions)
    {
        if (func.IsStarArg || func.IsDistinct)
            throw new CSharpDbException(ErrorCode.SyntaxError, $"Scalar function '{func.FunctionName}' does not support DISTINCT or * arguments.");

        if (!functions.TryGetScalar(func.FunctionName, func.Arguments.Count, out var definition))
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Unknown scalar function: {func.FunctionName}");
        }

        var arguments = new DbValue[func.Arguments.Count];
        for (int i = 0; i < arguments.Length; i++)
            arguments[i] = evaluateArgument(func.Arguments[i]);

        if (definition.Options.NullPropagating && arguments.Any(static value => value.IsNull))
            return DbValue.Null;

        try
        {
            return definition.Invoke(arguments);
        }
        catch (Exception ex)
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Scalar function '{definition.Name}' failed: {ex.Message}",
                ex);
        }
    }

    private static DbValue EvaluateUserFunction(
        FunctionCallExpression func,
        DbValue[] arguments,
        DbFunctionRegistry functions)
    {
        if (func.IsStarArg || func.IsDistinct)
            throw new CSharpDbException(ErrorCode.SyntaxError, $"Scalar function '{func.FunctionName}' does not support DISTINCT or * arguments.");

        if (!functions.TryGetScalar(func.FunctionName, arguments.Length, out var definition))
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Unknown scalar function: {func.FunctionName}");
        }

        if (definition.Options.NullPropagating && arguments.Any(static value => value.IsNull))
            return DbValue.Null;

        try
        {
            return definition.Invoke(arguments);
        }
        catch (Exception ex)
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Scalar function '{definition.Name}' failed: {ex.Message}",
                ex);
        }
    }

    private static string ToDisplayText(DbValue value) => value.Type switch
    {
        DbType.Null => "NULL",
        DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
        DbType.Real => value.AsReal.ToString(CultureInfo.InvariantCulture),
        DbType.Text => value.AsText,
        DbType.Blob => $"[{value.AsBlob.Length} bytes]",
        _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported DbValue type '{value.Type}'."),
    };
}
