using System.Globalization;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class ScalarFunctionEvaluator
{
    public static bool IsAggregateFunction(string functionName)
        => functionName.ToUpperInvariant() is "COUNT" or "SUM" or "AVG" or "MIN" or "MAX";

    public static DbValue Evaluate(FunctionCallExpression func, Func<Expression, DbValue> evaluateArgument)
    {
        string functionName = func.FunctionName.ToUpperInvariant();
        return functionName switch
        {
            "TEXT" => EvaluateText(func, evaluateArgument),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown scalar function: {func.FunctionName}"),
        };
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
