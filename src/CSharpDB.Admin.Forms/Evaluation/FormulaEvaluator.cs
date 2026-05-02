using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Evaluation;

/// <summary>
/// Evaluates formulas for computed fields.
///
/// Field formulas start with '=' and support arithmetic (+, -, *, /), parentheses,
/// numeric literals, and field references (e.g., =Quantity * UnitPrice).
///
/// Aggregate formulas reference child table fields: =SUM(OrderItems.LineTotal),
/// =COUNT(OrderItems.LineTotal), =AVG(...), =MIN(...), =MAX(...)
/// </summary>
public static class FormulaEvaluator
{
    private static readonly string[] AggregateFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];

    /// <summary>
    /// Evaluate a field-level formula (e.g., =Quantity * UnitPrice).
    /// Returns null if any referenced field is null, formula is invalid, or division by zero.
    /// </summary>
    public static double? Evaluate(string? formula, Func<string, double?> fieldResolver)
        => Evaluate(formula, fieldResolver, DbFunctionRegistry.Empty);

    public static double? Evaluate(
        string? formula,
        Func<string, double?> fieldResolver,
        DbFunctionRegistry? functions)
        => Evaluate(formula, fieldResolver, functions, callbackPolicy: null);

    public static double? Evaluate(
        string? formula,
        Func<string, double?> fieldResolver,
        DbFunctionRegistry? functions,
        DbExtensionPolicy? callbackPolicy)
    {
        if (string.IsNullOrWhiteSpace(formula)) return null;

        var expr = formula.Trim();
        if (!expr.StartsWith('=')) return null;
        expr = expr[1..].Trim();
        if (expr.Length == 0) return null;

        try
        {
            var parser = new Parser(expr, fieldResolver, functions ?? DbFunctionRegistry.Empty, callbackPolicy);
            var result = parser.ParseExpression();
            // Ensure we consumed all input
            if (parser.Position < parser.Input.Length)
                return null;
            return result;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Try to parse an aggregate formula like =SUM(TableName.FieldName).
    /// Returns true if the formula matches the aggregate pattern.
    /// </summary>
    public static bool TryParseAggregate(string? formula, out string func, out string table, out string field)
    {
        func = table = field = "";
        if (string.IsNullOrWhiteSpace(formula)) return false;

        var expr = formula.Trim();
        if (!expr.StartsWith('=')) return false;
        expr = expr[1..].Trim();

        // Match: FUNC(Table.Field)
        foreach (var fn in AggregateFunctions)
        {
            if (!expr.StartsWith(fn, StringComparison.OrdinalIgnoreCase)) continue;
            var rest = expr[fn.Length..].Trim();
            if (!rest.StartsWith('(') || !rest.EndsWith(')')) continue;

            var inner = rest[1..^1].Trim();
            var dotIdx = inner.IndexOf('.');
            if (dotIdx <= 0 || dotIdx >= inner.Length - 1) continue;

            var tbl = inner[..dotIdx].Trim();
            var fld = inner[(dotIdx + 1)..].Trim();

            if (tbl.Length == 0 || fld.Length == 0) continue;
            if (!IsValidIdentifier(tbl) || !IsValidIdentifier(fld)) continue;

            func = fn.ToUpperInvariant();
            table = tbl;
            field = fld;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Evaluate an aggregate function over a set of values.
    /// Returns null if values is empty (except COUNT which returns 0).
    /// </summary>
    public static double? EvaluateAggregate(string func, IEnumerable<double?> values)
    {
        var list = values.ToList();

        return func.ToUpperInvariant() switch
        {
            "SUM" => list.Count == 0 ? 0.0 : list.All(v => v is null) ? null : list.Where(v => v.HasValue).Sum(v => v!.Value),
            "COUNT" => list.Count(v => v.HasValue),
            "AVG" => list.Any(v => v.HasValue) ? list.Where(v => v.HasValue).Average(v => v!.Value) : null,
            "MIN" => list.Any(v => v.HasValue) ? list.Where(v => v.HasValue).Min(v => v!.Value) : null,
            "MAX" => list.Any(v => v.HasValue) ? list.Where(v => v.HasValue).Max(v => v!.Value) : null,
            _ => null
        };
    }

    private static bool IsValidIdentifier(string s) =>
        s.Length > 0 && (char.IsLetter(s[0]) || s[0] == '_') && s.All(c => char.IsLetterOrDigit(c) || c == '_');

    /// <summary>
    /// Recursive-descent parser for arithmetic expressions with field references.
    /// Grammar:
    ///   Expression = Term (('+' | '-') Term)*
    ///   Term       = Factor (('*' | '/') Factor)*
    ///   Factor     = ['-'] Atom
    ///   Atom       = Number | '(' Expression ')' | FieldName
    /// </summary>
    private ref struct Parser
    {
        public ReadOnlySpan<char> Input;
        public int Position;
        private readonly Func<string, double?> _fieldResolver;
        private readonly DbFunctionRegistry _functions;
        private readonly DbExtensionPolicy? _callbackPolicy;

        public Parser(
            string input,
            Func<string, double?> fieldResolver,
            DbFunctionRegistry functions,
            DbExtensionPolicy? callbackPolicy)
        {
            Input = input.AsSpan();
            Position = 0;
            _fieldResolver = fieldResolver;
            _functions = functions;
            _callbackPolicy = callbackPolicy;
        }

        public double? ParseExpression()
        {
            var left = ParseTerm();
            while (Position < Input.Length)
            {
                SkipWhitespace();
                if (Position >= Input.Length) break;
                var ch = Input[Position];
                if (ch == '+')
                {
                    Position++;
                    var right = ParseTerm();
                    if (left is null || right is null) return null;
                    left = left.Value + right.Value;
                }
                else if (ch == '-')
                {
                    Position++;
                    var right = ParseTerm();
                    if (left is null || right is null) return null;
                    left = left.Value - right.Value;
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
            var left = ParseFactor();
            while (Position < Input.Length)
            {
                SkipWhitespace();
                if (Position >= Input.Length) break;
                var ch = Input[Position];
                if (ch == '*')
                {
                    Position++;
                    var right = ParseFactor();
                    if (left is null || right is null) return null;
                    left = left.Value * right.Value;
                }
                else if (ch == '/')
                {
                    Position++;
                    var right = ParseFactor();
                    if (left is null || right is null) return null;
                    if (right.Value == 0) return null; // Division by zero → null
                    left = left.Value / right.Value;
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
                var val = ParseAtom();
                return val.HasValue ? -val.Value : null;
            }
            return ParseAtom();
        }

        private double? ParseAtom()
        {
            SkipWhitespace();
            if (Position >= Input.Length) return null;

            var ch = Input[Position];

            // Parenthesized expression
            if (ch == '(')
            {
                Position++;
                var val = ParseExpression();
                SkipWhitespace();
                if (Position < Input.Length && Input[Position] == ')')
                    Position++;
                else
                    return null; // Missing closing paren
                return val;
            }

            // Number literal
            if (char.IsDigit(ch) || ch == '.')
            {
                var start = Position;
                while (Position < Input.Length && (char.IsDigit(Input[Position]) || Input[Position] == '.'))
                    Position++;
                var numStr = Input[start..Position].ToString();
                return double.TryParse(numStr, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var num) ? num : null;
            }

            // Field reference (identifier)
            if (char.IsLetter(ch) || ch == '_')
            {
                var start = Position;
                while (Position < Input.Length && (char.IsLetterOrDigit(Input[Position]) || Input[Position] == '_'))
                    Position++;
                var fieldName = Input[start..Position].ToString();
                SkipWhitespace();
                if (Position < Input.Length && Input[Position] == '(')
                    return ParseFunctionCall(fieldName);

                return _fieldResolver(fieldName);
            }

            return null; // Unexpected character
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
            {
                DbCallbackDiagnostics.WriteMissingScalarInvocation(
                    functionName,
                    arguments.Count,
                    CreateFormCallbackMetadata(functionName),
                    $"Unknown scalar function '{functionName}'.");
                return null;
            }

            if (definition.Options.NullPropagating && arguments.Any(static argument => argument.IsNull))
                return null;

            try
            {
                DbValue[] dbArguments = arguments.ToArray();
                IReadOnlyDictionary<string, string>? metadata = CreateFormCallbackMetadata(functionName);
                DbValue value = _callbackPolicy is null
                    ? definition.Invoke(dbArguments, metadata)
                    : definition.Invoke(dbArguments, metadata, _callbackPolicy, DbExtensionHostMode.Embedded);
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

    private static IReadOnlyDictionary<string, string>? CreateFormCallbackMetadata(string functionName)
        => DbCallbackDiagnostics.IsInvocationEnabled
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "AdminForms",
                ["location"] = $"formulas.functions.{functionName}",
                ["correlationId"] = Guid.NewGuid().ToString("N"),
            }
            : null;
}
