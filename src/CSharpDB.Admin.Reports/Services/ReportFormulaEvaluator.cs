namespace CSharpDB.Admin.Reports.Services;

public static class ReportFormulaEvaluator
{
    private static readonly string[] AggregateFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];

    public static double? EvaluateNumeric(string? expression, Func<string, double?> fieldResolver)
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
            var parser = new Parser(expr, fieldResolver);
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

        public Parser(string input, Func<string, double?> fieldResolver)
        {
            Input = input.AsSpan();
            Position = 0;
            _fieldResolver = fieldResolver;
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
            return fieldReference is null ? null : _fieldResolver(fieldReference);
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

        private void SkipWhitespace()
        {
            while (Position < Input.Length && char.IsWhiteSpace(Input[Position]))
                Position++;
        }
    }
}
