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

    public static bool EvaluateFilter(string expression, IReadOnlyDictionary<string, object?> row)
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
            object? leftValue = row.TryGetValue(left, out var value) ? value : null;
            object? rightValue = ParseLiteral(right, row);
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

    public static object? EvaluateDerivedExpression(string expression, IReadOnlyDictionary<string, object?> row)
    {
        string trimmed = expression.Trim();
        if (row.TryGetValue(trimmed, out var columnValue))
        {
            return columnValue;
        }

        return ParseLiteral(trimmed, row);
    }

    private static object? ParseLiteral(string token, IReadOnlyDictionary<string, object?> row)
    {
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
