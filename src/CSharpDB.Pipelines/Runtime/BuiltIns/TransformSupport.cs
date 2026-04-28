using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

internal static class TransformSupport
{
    public static object? ConvertValue(object? value, DbType targetType)
    {
        if (value is null)
        {
            return null;
        }

        return targetType switch
        {
            DbType.Integer => value switch
            {
                long longValue => longValue,
                int intValue => (long)intValue,
                double doubleValue => checked((long)doubleValue),
                bool boolValue => boolValue ? 1L : 0L,
                string textValue => long.Parse(textValue, CultureInfo.InvariantCulture),
                _ => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            },
            DbType.Real => value switch
            {
                double doubleValue => doubleValue,
                float floatValue => (double)floatValue,
                long longValue => (double)longValue,
                int intValue => intValue,
                bool boolValue => boolValue ? 1d : 0d,
                string textValue => double.Parse(textValue, CultureInfo.InvariantCulture),
                _ => Convert.ToDouble(value, CultureInfo.InvariantCulture),
            },
            DbType.Text => value switch
            {
                string textValue => textValue,
                IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
                _ => value.ToString(),
            },
            DbType.Blob => value switch
            {
                byte[] bytes => bytes,
                string textValue => Convert.FromBase64String(textValue),
                _ => throw new InvalidCastException($"Cannot convert value of type '{value.GetType().Name}' to blob."),
            },
            DbType.Null => null,
            _ => value,
        };
    }

    public static bool EvaluateFilter(
        string expression,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions = null)
    {
        string[] operators = ["==", "!=", ">=", "<=", ">", "<"];
        foreach (string op in operators)
        {
            int index = expression.IndexOf(op, StringComparison.Ordinal);
            if (index < 0)
            {
                continue;
            }

            string left = expression[..index].Trim();
            string right = expression[(index + op.Length)..].Trim();
            object? leftValue = EvaluateFilterLeft(left, row, functions);
            object? rightValue = ParseLiteral(right, row, functions);
            int comparison = Compare(leftValue, rightValue);

            return op switch
            {
                "==" => Equals(leftValue, rightValue),
                "!=" => !Equals(leftValue, rightValue),
                ">=" => comparison >= 0,
                "<=" => comparison <= 0,
                ">" => comparison > 0,
                "<" => comparison < 0,
                _ => throw new InvalidOperationException($"Unsupported operator '{op}'."),
            };
        }

        throw new InvalidOperationException($"Unsupported filter expression '{expression}'.");
    }

    public static object? EvaluateDerivedExpression(
        string expression,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions = null)
    {
        string trimmed = expression.Trim();
        if (row.TryGetValue(trimmed, out var columnValue))
        {
            return columnValue;
        }

        return ParseLiteral(trimmed, row, functions);
    }

    private static object? EvaluateValue(
        string token,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions)
    {
        if (row.TryGetValue(token, out var columnValue))
            return columnValue;

        return ParseLiteral(token, row, functions);
    }

    private static object? EvaluateFilterLeft(
        string token,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions)
    {
        if (row.TryGetValue(token, out var columnValue))
            return columnValue;

        if (TryEvaluateFunctionCall(token, row, functions, out object? functionValue))
            return functionValue;

        return null;
    }

    private static object? ParseLiteral(
        string token,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions)
    {
        if (TryEvaluateFunctionCall(token, row, functions, out object? functionValue))
            return functionValue;

        if (token.StartsWith('\'') && token.EndsWith('\'') && token.Length >= 2)
        {
            return token[1..^1];
        }

        if (string.Equals(token, "null", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (string.Equals(token, "true", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(token, "false", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (long.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
        {
            return longValue;
        }

        if (double.TryParse(token, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double doubleValue))
        {
            return doubleValue;
        }

        if (row.TryGetValue(token, out var columnValue))
        {
            return columnValue;
        }

        return token;
    }

    private static bool TryEvaluateFunctionCall(
        string expression,
        IReadOnlyDictionary<string, object?> row,
        DbFunctionRegistry? functions,
        out object? value)
    {
        value = null;
        int openParen = expression.IndexOf('(');
        if (openParen <= 0 || !expression.EndsWith(')'))
            return false;

        string name = expression[..openParen].Trim();
        if (!IsIdentifier(name))
            return false;

        string argumentsText = expression[(openParen + 1)..^1];
        string[] argumentTokens = SplitArguments(argumentsText);

        DbFunctionRegistry registry = functions ?? DbFunctionRegistry.Empty;
        if (!registry.TryGetScalar(name, argumentTokens.Length, out var definition))
            throw new InvalidOperationException($"Unknown scalar function '{name}'.");

        var arguments = new DbValue[argumentTokens.Length];
        for (int i = 0; i < argumentTokens.Length; i++)
            arguments[i] = ToDbValue(ParseLiteral(argumentTokens[i].Trim(), row, functions));

        if (definition.Options.NullPropagating && arguments.Any(static argument => argument.IsNull))
        {
            value = null;
            return true;
        }

        try
        {
            value = FromDbValue(definition.Invoke(arguments));
            return true;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Scalar function '{definition.Name}' failed: {ex.Message}", ex);
        }
    }

    private static string[] SplitArguments(string argumentsText)
    {
        if (string.IsNullOrWhiteSpace(argumentsText))
            return [];

        var arguments = new List<string>();
        int start = 0;
        int depth = 0;
        bool inString = false;
        for (int i = 0; i < argumentsText.Length; i++)
        {
            char ch = argumentsText[i];
            if (ch == '\'')
            {
                inString = !inString;
                continue;
            }

            if (inString)
                continue;

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth < 0)
                    throw new InvalidOperationException($"Malformed function expression '{argumentsText}'.");
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                arguments.Add(argumentsText[start..i].Trim());
                start = i + 1;
            }
        }

        if (inString || depth != 0)
            throw new InvalidOperationException($"Malformed function expression '{argumentsText}'.");

        arguments.Add(argumentsText[start..].Trim());
        if (arguments.Any(static argument => argument.Length == 0))
            throw new InvalidOperationException($"Malformed function expression '{argumentsText}'.");

        return arguments.ToArray();
    }

    private static DbValue ToDbValue(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue dbValue => dbValue,
        bool boolValue => DbValue.FromInteger(boolValue ? 1 : 0),
        byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float or double or decimal => DbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        string text => DbValue.FromText(text),
        byte[] bytes => DbValue.FromBlob(bytes),
        _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private static object? FromDbValue(DbValue value) => value.Type switch
    {
        DbType.Null => null,
        DbType.Integer => value.AsInteger,
        DbType.Real => value.AsReal,
        DbType.Text => value.AsText,
        DbType.Blob => value.AsBlob,
        _ => null,
    };

    private static bool IsIdentifier(string value)
    {
        if (value.Length == 0 || (!char.IsLetter(value[0]) && value[0] != '_'))
            return false;

        for (int i = 1; i < value.Length; i++)
        {
            if (!char.IsLetterOrDigit(value[i]) && value[i] != '_')
                return false;
        }

        return true;
    }

    private static int Compare(object? left, object? right)
    {
        if (left is null && right is null)
        {
            return 0;
        }

        if (left is null)
        {
            return -1;
        }

        if (right is null)
        {
            return 1;
        }

        if (left is string || right is string)
        {
            return string.CompareOrdinal(Convert.ToString(left, CultureInfo.InvariantCulture), Convert.ToString(right, CultureInfo.InvariantCulture));
        }

        if (left is IConvertible && right is IConvertible)
        {
            double leftDouble = Convert.ToDouble(left, CultureInfo.InvariantCulture);
            double rightDouble = Convert.ToDouble(right, CultureInfo.InvariantCulture);
            return leftDouble.CompareTo(rightDouble);
        }

        return string.CompareOrdinal(left.ToString(), right.ToString());
    }
}
