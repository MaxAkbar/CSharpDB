using System.Globalization;
using System.Text.Json;

namespace CSharpDB.Admin.Forms.Services;

internal static class FormActionConditionEvaluator
{
    private static readonly string[] Operators = [">=", "<=", "==", "!=", "<>", "=", ">", "<"];

    public static bool TryEvaluate(
        string? condition,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, object?>? stepArguments,
        out bool result,
        out string? error)
    {
        result = true;
        error = null;

        if (string.IsNullOrWhiteSpace(condition))
            return true;

        string expression = condition.Trim();
        if (expression.StartsWith('=') && !expression.StartsWith("==", StringComparison.Ordinal))
            expression = expression[1..].Trim();

        if (expression.Length == 0)
        {
            error = "Condition is empty.";
            return false;
        }

        Dictionary<string, object?> values = BuildValues(record, bindingArguments, runtimeArguments, stepArguments);
        if (TryFindOperator(expression, out int operatorIndex, out string? op))
        {
            string leftText = expression[..operatorIndex].Trim();
            string rightText = expression[(operatorIndex + op.Length)..].Trim();
            if (leftText.Length == 0 || rightText.Length == 0)
            {
                error = $"Condition '{condition}' is missing a comparison operand.";
                return false;
            }

            if (!TryResolveValue(leftText, values, requireKnownIdentifier: true, out object? left, out error) ||
                !TryResolveValue(rightText, values, requireKnownIdentifier: false, out object? right, out error))
            {
                return false;
            }

            result = Compare(left, right, op);
            return true;
        }

        if (!TryResolveValue(expression, values, requireKnownIdentifier: true, out object? value, out error))
            return false;

        result = IsTruthy(value);
        return true;
    }

    private static Dictionary<string, object?> BuildValues(params IReadOnlyDictionary<string, object?>?[] sources)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (IReadOnlyDictionary<string, object?>? source in sources)
        {
            if (source is null)
                continue;

            foreach ((string key, object? value) in source)
                values[key] = NormalizeValue(value);
        }

        return values;
    }

    private static bool TryFindOperator(string expression, out int index, out string op)
    {
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        bool inBracket = false;

        for (int i = 0; i < expression.Length; i++)
        {
            char ch = expression[i];
            if (ch == '\'' && !inDoubleQuote && !inBracket)
            {
                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (ch == '"' && !inSingleQuote && !inBracket)
            {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (ch == '[' && !inSingleQuote && !inDoubleQuote)
            {
                inBracket = true;
                continue;
            }

            if (ch == ']' && inBracket)
            {
                inBracket = false;
                continue;
            }

            if (inSingleQuote || inDoubleQuote || inBracket)
                continue;

            foreach (string candidate in Operators)
            {
                if (expression.AsSpan(i).StartsWith(candidate, StringComparison.Ordinal))
                {
                    index = i;
                    op = candidate;
                    return true;
                }
            }
        }

        index = -1;
        op = string.Empty;
        return false;
    }

    private static bool TryResolveValue(
        string token,
        IReadOnlyDictionary<string, object?> values,
        bool requireKnownIdentifier,
        out object? value,
        out string? error)
    {
        error = null;
        token = token.Trim();
        if (token.Length == 0)
        {
            value = null;
            error = "Condition contains an empty value.";
            return false;
        }

        if (TryReadQuotedString(token, out string? quoted))
        {
            value = quoted;
            return true;
        }

        if (token.StartsWith('[') && token.EndsWith(']') && token.Length > 2)
        {
            string name = token[1..^1].Trim();
            if (values.TryGetValue(name, out value))
                return true;

            error = $"Condition references unknown value '{name}'.";
            return false;
        }

        if (string.Equals(token, "null", StringComparison.OrdinalIgnoreCase))
        {
            value = null;
            return true;
        }

        if (bool.TryParse(token, out bool boolean))
        {
            value = boolean;
            return true;
        }

        if (long.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
        {
            value = integer;
            return true;
        }

        if (double.TryParse(token, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double real))
        {
            value = real;
            return true;
        }

        if (IsIdentifier(token))
        {
            if (values.TryGetValue(token, out value))
                return true;

            if (requireKnownIdentifier)
            {
                error = $"Condition references unknown value '{token}'.";
                return false;
            }
        }

        value = token;
        return true;
    }

    private static bool TryReadQuotedString(string token, out string? value)
    {
        value = null;
        if (token.Length < 2)
            return false;

        char quote = token[0];
        if ((quote != '\'' && quote != '"') || token[^1] != quote)
            return false;

        value = token[1..^1].Replace($"{quote}{quote}", quote.ToString(), StringComparison.Ordinal);
        return true;
    }

    private static bool Compare(object? left, object? right, string op)
    {
        int comparison = CompareValues(left, right);
        return op switch
        {
            "=" or "==" => comparison == 0,
            "!=" or "<>" => comparison != 0,
            ">" => comparison > 0,
            ">=" => comparison >= 0,
            "<" => comparison < 0,
            "<=" => comparison <= 0,
            _ => false,
        };
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null || right is null)
            return left is null && right is null ? 0 : left is null ? -1 : 1;

        if (TryConvertDouble(left, out double leftNumber) &&
            TryConvertDouble(right, out double rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        if (left is bool leftBool && right is bool rightBool)
            return leftBool.CompareTo(rightBool);

        return string.Compare(
            Convert.ToString(left, CultureInfo.InvariantCulture),
            Convert.ToString(right, CultureInfo.InvariantCulture),
            StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTruthy(object? value)
    {
        value = NormalizeValue(value);
        if (value is null)
            return false;

        if (value is bool boolean)
            return boolean;

        if (TryConvertDouble(value, out double number))
            return Math.Abs(number) > double.Epsilon;

        return !string.IsNullOrWhiteSpace(Convert.ToString(value, CultureInfo.InvariantCulture));
    }

    private static bool TryConvertDouble(object? value, out double result)
    {
        value = NormalizeValue(value);
        return value switch
        {
            byte number => Set(number, out result),
            short number => Set(number, out result),
            int number => Set(number, out result),
            long number => Set(number, out result),
            float number => Set(number, out result),
            double number => Set(number, out result),
            decimal number => Set((double)number, out result),
            string text => double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out result),
            _ => Set(0, out result, success: false),
        };
    }

    private static bool Set(double value, out double result, bool success = true)
    {
        result = value;
        return success;
    }

    private static object? NormalizeValue(object? value)
        => value is JsonElement json ? NormalizeJsonValue(json) : value;

    private static object? NormalizeJsonValue(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.TryGetInt64(out long integer) ? integer : value.GetDouble(),
            _ => value.ToString(),
        };

    private static bool IsIdentifier(string value)
        => value.Length > 0
            && (char.IsLetter(value[0]) || value[0] == '_')
            && value.All(static ch => char.IsLetterOrDigit(ch) || ch == '_');
}
