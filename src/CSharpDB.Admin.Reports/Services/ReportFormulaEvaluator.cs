using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Services;

public static class ReportFormulaEvaluator
{
    private static readonly string[] AggregateFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];

    public static double? EvaluateNumeric(string? expression, Func<string, double?> fieldResolver)
        => EvaluateNumeric(expression, fieldResolver, DbFunctionRegistry.Empty);

    public static double? EvaluateNumeric(
        string? expression,
        Func<string, double?> fieldResolver,
        DbFunctionRegistry? functions)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return null;

        string expr = expression.Trim();
        if (!expr.StartsWith('='))
            return null;

        expr = expr[1..].Trim();
        if (expr.Length == 0)
            return null;

        try
        {
            var parser = new Parser(expr, fieldResolver, functions ?? DbFunctionRegistry.Empty);
            double? result = parser.ParseExpression();
            if (parser.Position < parser.Input.Length)
                return null;

            return result;
        }
        catch
        {
            return null;
        }
    }

    public static bool TryEvaluateScalar(
        string? expression,
        Func<string, object?> fieldResolver,
        DbFunctionRegistry? functions,
        out object? value)
    {
        value = null;
        if (functions == null || string.IsNullOrWhiteSpace(expression))
            return false;

        string expr = expression.Trim();
        if (!expr.StartsWith('='))
            return false;

        expr = expr[1..].Trim();
        if (!TryEvaluateFunctionCall(expr, fieldResolver, functions, out DbValue dbValue))
            return false;

        value = FromDbValue(dbValue);
        return true;
    }

    public static bool TryParseAggregate(string? expression, out string functionName, out string fieldName)
    {
        functionName = fieldName = string.Empty;
        if (string.IsNullOrWhiteSpace(expression))
            return false;

        string expr = expression.Trim();
        if (!expr.StartsWith('='))
            return false;

        expr = expr[1..].Trim();
        foreach (string function in AggregateFunctions)
        {
            if (!expr.StartsWith(function, StringComparison.OrdinalIgnoreCase))
                continue;

            string rest = expr[function.Length..].Trim();
            if (!rest.StartsWith('(') || !rest.EndsWith(')'))
                continue;

            string inner = rest[1..^1].Trim();
            if (TryReadFieldReference(inner, out string parsedField))
            {
                functionName = function;
                fieldName = parsedField;
                return true;
            }
        }

        return false;
    }

    public static double? EvaluateAggregate(string functionName, IEnumerable<object?> values)
    {
        List<double?> numerics = values
            .Select(value => ReportSql.TryConvertToDouble(value, out double numeric) ? numeric : (double?)null)
            .ToList();

        return functionName.ToUpperInvariant() switch
        {
            "SUM" => numerics.Count == 0 ? 0.0 : numerics.All(value => value is null) ? null : numerics.Where(value => value.HasValue).Sum(value => value!.Value),
            "COUNT" => numerics.Count(value => value.HasValue),
            "AVG" => numerics.Any(value => value.HasValue) ? numerics.Where(value => value.HasValue).Average(value => value!.Value) : null,
            "MIN" => numerics.Any(value => value.HasValue) ? numerics.Where(value => value.HasValue).Min(value => value!.Value) : null,
            "MAX" => numerics.Any(value => value.HasValue) ? numerics.Where(value => value.HasValue).Max(value => value!.Value) : null,
            _ => null,
        };
    }

    public static bool TryReadFieldReference(string text, out string fieldName)
    {
        fieldName = string.Empty;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        string trimmed = text.Trim();
        if (trimmed.StartsWith('[') && trimmed.EndsWith(']') && trimmed.Length >= 3)
        {
            fieldName = trimmed[1..^1];
            return fieldName.Length > 0;
        }

        if (trimmed.Length > 0 && (char.IsLetter(trimmed[0]) || trimmed[0] == '_') && trimmed.All(ch => char.IsLetterOrDigit(ch) || ch == '_'))
        {
            fieldName = trimmed;
            return true;
        }

        return false;
    }

    private ref struct Parser
    {
        public ReadOnlySpan<char> Input;
        public int Position;

        private readonly Func<string, double?> _fieldResolver;
        private readonly DbFunctionRegistry _functions;

        public Parser(string input, Func<string, double?> fieldResolver, DbFunctionRegistry functions)
        {
            Input = input.AsSpan();
            Position = 0;
            _fieldResolver = fieldResolver;
            _functions = functions;
        }

        public double? ParseExpression()
        {
            double? left = ParseTerm();
            while (Position < Input.Length)
            {
                SkipWhitespace();
                if (Position >= Input.Length)
                    break;

                char current = Input[Position];
                if (current == '+')
                {
                    Position++;
                    double? right = ParseTerm();
                    if (left is null || right is null)
                        return null;

                    left += right;
                }
                else if (current == '-')
                {
                    Position++;
                    double? right = ParseTerm();
                    if (left is null || right is null)
                        return null;

                    left -= right;
                }
                else
                {
                    break;
                }
            }

            return left;
        }

        private double? ParseTerm()
        {
            double? left = ParseFactor();
            while (Position < Input.Length)
            {
                SkipWhitespace();
                if (Position >= Input.Length)
                    break;

                char current = Input[Position];
                if (current == '*')
                {
                    Position++;
                    double? right = ParseFactor();
                    if (left is null || right is null)
                        return null;

                    left *= right;
                }
                else if (current == '/')
                {
                    Position++;
                    double? right = ParseFactor();
                    if (left is null || right is null || right == 0)
                        return null;

                    left /= right;
                }
                else
                {
                    break;
                }
            }

            return left;
        }

        private double? ParseFactor()
        {
            SkipWhitespace();
            if (Position < Input.Length && Input[Position] == '-')
            {
                Position++;
                double? value = ParseAtom();
                return value.HasValue ? -value.Value : null;
            }

            return ParseAtom();
        }

        private double? ParseAtom()
        {
            SkipWhitespace();
            if (Position >= Input.Length)
                return null;

            if (Input[Position] == '(')
            {
                Position++;
                double? value = ParseExpression();
                SkipWhitespace();
                if (Position >= Input.Length || Input[Position] != ')')
                    return null;

                Position++;
                return value;
            }

            if (char.IsDigit(Input[Position]) || Input[Position] == '.')
                return ParseNumber();

            string? fieldReference = ParseFieldReference();
            if (fieldReference is null)
                return null;

            SkipWhitespace();
            if (Position < Input.Length && Input[Position] == '(' && IsIdentifier(fieldReference))
                return ParseFunctionCall(fieldReference);

            return _fieldResolver(fieldReference);
        }

        private double? ParseNumber()
        {
            int start = Position;
            while (Position < Input.Length && (char.IsDigit(Input[Position]) || Input[Position] == '.'))
                Position++;

            return double.TryParse(Input[start..Position], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double numeric)
                ? numeric
                : null;
        }

        private string? ParseFieldReference()
        {
            if (Position >= Input.Length)
                return null;

            if (Input[Position] == '[')
            {
                Position++;
                int start = Position;
                while (Position < Input.Length && Input[Position] != ']')
                    Position++;

                if (Position >= Input.Length)
                    return null;

                string field = Input[start..Position].ToString();
                Position++;
                return field;
            }

            if (!char.IsLetter(Input[Position]) && Input[Position] != '_')
                return null;

            int identifierStart = Position;
            while (Position < Input.Length && (char.IsLetterOrDigit(Input[Position]) || Input[Position] == '_'))
                Position++;

            return Input[identifierStart..Position].ToString();
        }

        private double? ParseFunctionCall(string functionName)
        {
            Position++;
            var arguments = new List<DbValue>();
            SkipWhitespace();
            if (Position < Input.Length && Input[Position] == ')')
            {
                Position++;
                return InvokeFunction(functionName, arguments);
            }

            while (Position < Input.Length)
            {
                double? argument = ParseExpression();
                arguments.Add(argument.HasValue ? DbValue.FromReal(argument.Value) : DbValue.Null);

                SkipWhitespace();
                if (Position < Input.Length && Input[Position] == ',')
                {
                    Position++;
                    continue;
                }

                if (Position < Input.Length && Input[Position] == ')')
                {
                    Position++;
                    return InvokeFunction(functionName, arguments);
                }

                return null;
            }

            return null;
        }

        private double? InvokeFunction(string functionName, List<DbValue> arguments)
        {
            if (!_functions.TryGetScalar(functionName, arguments.Count, out var definition))
                return null;

            if (definition.Options.NullPropagating && arguments.Any(static argument => argument.IsNull))
                return null;

            try
            {
                DbValue value = definition.Invoke(arguments.ToArray());
                return value.Type switch
                {
                    DbType.Integer => value.AsInteger,
                    DbType.Real => value.AsReal,
                    _ => null,
                };
            }
            catch
            {
                return null;
            }
        }

        private void SkipWhitespace()
        {
            while (Position < Input.Length && char.IsWhiteSpace(Input[Position]))
                Position++;
        }
    }

    private static bool TryEvaluateFunctionCall(
        string expression,
        Func<string, object?> fieldResolver,
        DbFunctionRegistry functions,
        out DbValue value)
    {
        value = DbValue.Null;
        int openParen = expression.IndexOf('(');
        if (openParen <= 0 || !expression.EndsWith(')'))
            return false;

        string name = expression[..openParen].Trim();
        if (!IsIdentifier(name))
            return false;

        string[] argumentTokens = SplitArguments(expression[(openParen + 1)..^1]);
        if (!functions.TryGetScalar(name, argumentTokens.Length, out var definition))
            return false;

        var arguments = new DbValue[argumentTokens.Length];
        for (int i = 0; i < argumentTokens.Length; i++)
            arguments[i] = EvaluateScalarArgument(argumentTokens[i].Trim(), fieldResolver, functions);

        if (definition.Options.NullPropagating && arguments.Any(static argument => argument.IsNull))
        {
            value = DbValue.Null;
            return true;
        }

        value = definition.Invoke(arguments);
        return true;
    }

    private static DbValue EvaluateScalarArgument(
        string token,
        Func<string, object?> fieldResolver,
        DbFunctionRegistry functions)
    {
        if (TryEvaluateFunctionCall(token, fieldResolver, functions, out DbValue nestedValue))
            return nestedValue;

        if (token.StartsWith('\'') && token.EndsWith('\'') && token.Length >= 2)
            return DbValue.FromText(token[1..^1]);

        if (string.Equals(token, "null", StringComparison.OrdinalIgnoreCase))
            return DbValue.Null;

        if (long.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
            return DbValue.FromInteger(integer);

        if (double.TryParse(token, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double real))
            return DbValue.FromReal(real);

        if (TryReadFieldReference(token, out string fieldName))
            return ToDbValue(fieldResolver(fieldName));

        return DbValue.FromText(token);
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
                    return [];
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                arguments.Add(argumentsText[start..i].Trim());
                start = i + 1;
            }
        }

        if (inString || depth != 0)
            return [];

        arguments.Add(argumentsText[start..].Trim());
        return arguments.Any(static argument => argument.Length == 0) ? [] : arguments.ToArray();
    }

    private static DbValue ToDbValue(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue dbValue => dbValue,
        bool boolean => DbValue.FromInteger(boolean ? 1 : 0),
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
}
