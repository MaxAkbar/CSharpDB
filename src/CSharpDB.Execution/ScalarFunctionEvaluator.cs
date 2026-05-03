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
        if (DbBuiltInScalarFunctions.IsBuiltInFunctionName(functionName))
        {
            if (func.IsStarArg || func.IsDistinct)
                throw new CSharpDbException(ErrorCode.SyntaxError, $"Scalar function '{func.FunctionName}' does not support DISTINCT or * arguments.");

            var arguments = new DbValue[func.Arguments.Count];
            for (int i = 0; i < arguments.Length; i++)
                arguments[i] = evaluateArgument(func.Arguments[i]);

            if (DbBuiltInScalarFunctions.TryEvaluate(functionName, arguments, out DbValue builtInValue))
                return builtInValue;
        }

        return EvaluateUserFunction(func, evaluateArgument, functions ?? DbFunctionRegistry.Empty);
    }

    public static DbValue Evaluate(
        FunctionCallExpression func,
        DbValue[] arguments,
        DbFunctionRegistry? functions)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        if (DbBuiltInScalarFunctions.IsBuiltInFunctionName(functionName))
        {
            if (func.IsStarArg || func.IsDistinct)
                throw new CSharpDbException(ErrorCode.SyntaxError, $"Scalar function '{func.FunctionName}' does not support DISTINCT or * arguments.");

            if (DbBuiltInScalarFunctions.TryEvaluate(functionName, arguments, out DbValue builtInValue))
                return builtInValue;
        }

        return EvaluateUserFunction(func, arguments, functions ?? DbFunctionRegistry.Empty);
    }

    internal static DbValue EvaluateTextValue(DbValue value)
        => DbValue.FromText(DbBuiltInScalarFunctions.ToDisplayText(value));

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
            return definition.Invoke(arguments, CreateSqlCallbackMetadata(func.FunctionName));
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
            return definition.Invoke(arguments, CreateSqlCallbackMetadata(func.FunctionName));
        }
        catch (Exception ex)
        {
            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Scalar function '{definition.Name}' failed: {ex.Message}",
                ex);
        }
    }

    private static IReadOnlyDictionary<string, string>? CreateSqlCallbackMetadata(string functionName)
        => DbCallbackDiagnostics.IsInvocationEnabled
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "SQL",
                ["location"] = $"functions.{functionName}",
            }
            : null;
}
