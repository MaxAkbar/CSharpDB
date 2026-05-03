using System.Globalization;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Evaluation;

public sealed record FormulaDomainFunctionRequest(
    string FunctionName,
    string Expression,
    string Domain,
    string? Criteria);

public delegate object? FormulaDomainFunctionResolver(FormulaDomainFunctionRequest request);

/// <summary>
/// Evaluates Admin Forms formulas.
///
/// Formulas start with '=' and support arithmetic, comparisons, logical
/// operators, field references, registered scalar callbacks, and Access-style
/// built-in functions.
/// </summary>
public static class FormulaEvaluator
{
    private static readonly string[] AggregateFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];
    private static readonly string[] DomainFunctionNames = ["DLOOKUP", "DCOUNT", "DSUM", "DAVG", "DMIN", "DMAX"];
    private static readonly HashSet<string> BuiltInFunctionNameSet =
        new(FormulaFunctionCatalog.BuiltInFunctionNames, StringComparer.OrdinalIgnoreCase);

    public static IReadOnlyCollection<string> BuiltInFunctionNames => FormulaFunctionCatalog.BuiltInFunctionNames;

    /// <summary>
    /// Evaluate a field-level formula as a number.
    /// Returns null if the formula is invalid, returns a nonnumeric value, or
    /// hits an invalid numeric operation.
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
        object? value = EvaluateValue(
            formula,
            field => fieldResolver(field),
            functions,
            callbackPolicy);

        return TryConvertDouble(value, out double numeric) ? numeric : null;
    }

    public static object? EvaluateValue(string? formula, Func<string, object?> fieldResolver)
        => EvaluateValue(formula, fieldResolver, DbFunctionRegistry.Empty);

    public static object? EvaluateValue(
        string? formula,
        Func<string, object?> fieldResolver,
        DbFunctionRegistry? functions)
        => EvaluateValue(formula, fieldResolver, functions, callbackPolicy: null);

    public static object? EvaluateValue(
        string? formula,
        Func<string, object?> fieldResolver,
        DbFunctionRegistry? functions,
        DbExtensionPolicy? callbackPolicy,
        FormulaDomainFunctionResolver? domainResolver = null)
    {
        if (string.IsNullOrWhiteSpace(formula))
            return null;

        string expr = formula.Trim();
        if (!expr.StartsWith('='))
            return null;

        expr = expr[1..].Trim();
        if (expr.Length == 0)
            return null;

        try
        {
            var parser = new Parser(
                expr,
                fieldResolver,
                functions ?? DbFunctionRegistry.Empty,
                callbackPolicy,
                domainResolver);
            object? result = parser.ParseExpression();
            parser.SkipWhitespace();
            return parser.Failed || parser.Position < parser.Input.Length ? null : NormalizeValue(result);
        }
        catch
        {
            return null;
        }
    }

    public static bool IsBuiltInFunctionName(string name)
        => BuiltInFunctionNameSet.Contains(name);

    public static IReadOnlyList<string> GetDomainReferences(string? formula)
    {
        string? expression = GetExpressionBody(formula);
        if (expression is null)
            return [];

        var domains = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ReadOnlySpan<char> input = expression.AsSpan();
        for (int i = 0; i < input.Length; i++)
        {
            char current = input[i];
            if (current is '\'' or '"')
            {
                i = SkipQuoted(input, i, current);
                continue;
            }

            if (current == '[')
            {
                i = SkipBracketed(input, i);
                continue;
            }

            if (!IsIdentifierStart(current))
                continue;

            int start = i;
            i++;
            while (i < input.Length && IsIdentifierPart(input[i]))
                i++;

            string name = input[start..i].ToString();
            if (!DomainFunctionNames.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                i--;
                continue;
            }

            int cursor = i;
            while (cursor < input.Length && char.IsWhiteSpace(input[cursor]))
                cursor++;

            if (cursor >= input.Length || input[cursor] != '(')
            {
                i--;
                continue;
            }

            if (TryReadFunctionArguments(input, cursor, out string argumentsText, out int closeParen))
            {
                string[] arguments = SplitTopLevelArguments(argumentsText);
                if (arguments.Length >= 2 && TryReadLiteralText(arguments[1], out string? domain) && !string.IsNullOrWhiteSpace(domain))
                    domains.Add(domain);

                i = closeParen;
            }
            else
            {
                i--;
            }
        }

        return domains.OrderBy(static domain => domain, StringComparer.OrdinalIgnoreCase).ToArray();
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
    /// Returns null if values is empty (except COUNT and SUM which return 0).
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

    private static string? GetExpressionBody(string? formula)
    {
        if (string.IsNullOrWhiteSpace(formula))
            return null;

        string expression = formula.Trim();
        if (!expression.StartsWith('='))
            return null;

        expression = expression[1..].Trim();
        return expression.Length == 0 ? null : expression;
    }

    private static bool IsValidIdentifier(string s) =>
        s.Length > 0 && (char.IsLetter(s[0]) || s[0] == '_') && s.All(c => char.IsLetterOrDigit(c) || c == '_');

    private ref struct Parser
    {
        public ReadOnlySpan<char> Input;
        public int Position;
        public bool Failed;

        private readonly Func<string, object?> _fieldResolver;
        private readonly DbFunctionRegistry _functions;
        private readonly DbExtensionPolicy? _callbackPolicy;
        private readonly FormulaDomainFunctionResolver? _domainResolver;

        public Parser(
            string input,
            Func<string, object?> fieldResolver,
            DbFunctionRegistry functions,
            DbExtensionPolicy? callbackPolicy,
            FormulaDomainFunctionResolver? domainResolver)
        {
            Input = input.AsSpan();
            Position = 0;
            Failed = false;
            _fieldResolver = fieldResolver;
            _functions = functions;
            _callbackPolicy = callbackPolicy;
            _domainResolver = domainResolver;
        }

        public object? ParseExpression() => ParseOr();

        private object? ParseOr()
        {
            object? left = ParseAnd();
            while (!Failed)
            {
                if (!MatchKeyword("OR"))
                    return left;

                object? right = ParseAnd();
                left = IsTruthy(left) || IsTruthy(right);
            }

            return null;
        }

        private object? ParseAnd()
        {
            object? left = ParseNot();
            while (!Failed)
            {
                if (!MatchKeyword("AND"))
                    return left;

                object? right = ParseNot();
                left = IsTruthy(left) && IsTruthy(right);
            }

            return null;
        }

        private object? ParseNot()
        {
            if (MatchKeyword("NOT"))
                return !IsTruthy(ParseNot());

            return ParseComparison();
        }

        private object? ParseComparison()
        {
            object? left = ParseAdditive();
            SkipWhitespace();
            string? op = MatchComparisonOperator();
            if (op is null)
                return left;

            object? right = ParseAdditive();
            return Compare(left, right, op);
        }

        private object? ParseAdditive()
        {
            object? left = ParseTerm();
            while (!Failed)
            {
                SkipWhitespace();
                if (Position >= Input.Length)
                    return left;

                char ch = Input[Position];
                if (ch == '+' || ch == '-')
                {
                    Position++;
                    object? right = ParseTerm();
                    if (left is null || right is null)
                    {
                        left = null;
                    }
                    else if (TryConvertDouble(left, out double leftNumber) && TryConvertDouble(right, out double rightNumber))
                    {
                        left = ch == '+' ? leftNumber + rightNumber : leftNumber - rightNumber;
                    }
                    else if (ch == '+')
                    {
                        left = ToFormulaString(left) + ToFormulaString(right);
                    }
                    else
                    {
                        Failed = true;
                        return null;
                    }

                    continue;
                }

                if (ch == '&')
                {
                    Position++;
                    object? right = ParseTerm();
                    left = ToFormulaString(left) + ToFormulaString(right);
                    continue;
                }

                return left;
            }

            return null;
        }

        private object? ParseTerm()
        {
            object? left = ParseFactor();
            while (!Failed)
            {
                SkipWhitespace();
                if (Position >= Input.Length)
                    return left;

                char ch = Input[Position];
                if (ch != '*' && ch != '/')
                    return left;

                Position++;
                object? right = ParseFactor();
                if (left is null || right is null)
                {
                    left = null;
                    continue;
                }

                if (!TryConvertDouble(left, out double leftNumber) || !TryConvertDouble(right, out double rightNumber))
                {
                    Failed = true;
                    return null;
                }

                if (ch == '/' && Math.Abs(rightNumber) < double.Epsilon)
                {
                    left = null;
                    continue;
                }

                left = ch == '*' ? leftNumber * rightNumber : leftNumber / rightNumber;
            }

            return null;
        }

        private object? ParseFactor()
        {
            SkipWhitespace();
            if (Position < Input.Length && Input[Position] == '-')
            {
                Position++;
                object? value = ParseFactor();
                return TryConvertDouble(value, out double numeric) ? -numeric : null;
            }

            if (Position < Input.Length && Input[Position] == '+')
            {
                Position++;
                object? value = ParseFactor();
                return TryConvertDouble(value, out double numeric) ? numeric : null;
            }

            return ParseAtom();
        }

        private object? ParseAtom()
        {
            SkipWhitespace();
            if (Position >= Input.Length)
            {
                Failed = true;
                return null;
            }

            char ch = Input[Position];
            if (ch == '(')
            {
                Position++;
                object? value = ParseExpression();
                SkipWhitespace();
                if (Position < Input.Length && Input[Position] == ')')
                {
                    Position++;
                    return value;
                }

                Failed = true;
                return null;
            }

            if (ch is '\'' or '"')
                return ParseString(ch);

            if (ch == '[')
                return ParseBracketedField();

            if (char.IsDigit(ch) || ch == '.')
                return ParseNumber();

            if (IsIdentifierStart(ch))
            {
                string identifier = ParseIdentifier();
                if (string.Equals(identifier, "NULL", StringComparison.OrdinalIgnoreCase))
                    return null;
                if (string.Equals(identifier, "TRUE", StringComparison.OrdinalIgnoreCase))
                    return true;
                if (string.Equals(identifier, "FALSE", StringComparison.OrdinalIgnoreCase))
                    return false;

                SkipWhitespace();
                if (Position < Input.Length && Input[Position] == '(')
                    return ParseFunctionCall(identifier);

                return NormalizeValue(_fieldResolver(identifier));
            }

            Failed = true;
            return null;
        }

        private string ParseString(char quote)
        {
            Position++;
            var builder = new System.Text.StringBuilder();
            while (Position < Input.Length)
            {
                char ch = Input[Position++];
                if (ch == quote)
                {
                    if (Position < Input.Length && Input[Position] == quote)
                    {
                        builder.Append(quote);
                        Position++;
                        continue;
                    }

                    return builder.ToString();
                }

                builder.Append(ch);
            }

            Failed = true;
            return string.Empty;
        }

        private object? ParseBracketedField()
        {
            Position++;
            int start = Position;
            while (Position < Input.Length && Input[Position] != ']')
                Position++;

            if (Position >= Input.Length)
            {
                Failed = true;
                return null;
            }

            string fieldName = Input[start..Position].ToString();
            Position++;
            return NormalizeValue(_fieldResolver(fieldName));
        }

        private object? ParseNumber()
        {
            int start = Position;
            bool hasDecimal = false;
            while (Position < Input.Length)
            {
                char current = Input[Position];
                if (char.IsDigit(current))
                {
                    Position++;
                    continue;
                }

                if (current == '.' && !hasDecimal)
                {
                    hasDecimal = true;
                    Position++;
                    continue;
                }

                break;
            }

            ReadOnlySpan<char> number = Input[start..Position];
            if (!hasDecimal && long.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
                return integer;

            return double.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out double real)
                ? real
                : Fail();
        }

        private string ParseIdentifier()
        {
            int start = Position;
            Position++;
            while (Position < Input.Length && IsIdentifierPart(Input[Position]))
                Position++;

            return Input[start..Position].ToString();
        }

        private object? ParseFunctionCall(string functionName)
        {
            Position++;
            var arguments = new List<object?>();
            SkipWhitespace();
            if (Position < Input.Length && Input[Position] == ')')
            {
                Position++;
                return InvokeFunction(functionName, arguments);
            }

            while (!Failed && Position < Input.Length)
            {
                arguments.Add(ParseExpression());

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

                Failed = true;
                return null;
            }

            Failed = true;
            return null;
        }

        private object? InvokeFunction(string functionName, List<object?> arguments)
        {
            if (TryInvokeBuiltInFunction(functionName, arguments, _domainResolver, out object? builtInValue))
                return NormalizeValue(builtInValue);

            return InvokeRegisteredFunction(functionName, arguments);
        }

        private object? InvokeRegisteredFunction(string functionName, List<object?> arguments)
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

            DbValue[] dbArguments = arguments.Select(ToDbValue).ToArray();
            if (definition.Options.NullPropagating && dbArguments.Any(static argument => argument.IsNull))
                return null;

            try
            {
                IReadOnlyDictionary<string, string>? metadata = CreateFormCallbackMetadata(functionName);
                DbValue value = _callbackPolicy is null
                    ? definition.Invoke(dbArguments, metadata)
                    : definition.Invoke(dbArguments, metadata, _callbackPolicy, DbExtensionHostMode.Embedded);
                return FromDbValue(value);
            }
            catch
            {
                return null;
            }
        }

        public void SkipWhitespace()
        {
            while (Position < Input.Length && char.IsWhiteSpace(Input[Position]))
                Position++;
        }

        private bool MatchKeyword(string keyword)
        {
            SkipWhitespace();
            if (!Input[Position..].StartsWith(keyword, StringComparison.OrdinalIgnoreCase))
                return false;

            int end = Position + keyword.Length;
            if (end < Input.Length && IsIdentifierPart(Input[end]))
                return false;

            Position = end;
            return true;
        }

        private string? MatchComparisonOperator()
        {
            SkipWhitespace();
            foreach (string op in new[] { ">=", "<=", "==", "!=", "<>", "=", ">", "<" })
            {
                if (Input[Position..].StartsWith(op, StringComparison.Ordinal))
                {
                    Position += op.Length;
                    return op;
                }
            }

            return null;
        }

        private object? Fail()
        {
            Failed = true;
            return null;
        }
    }

    private static bool TryInvokeBuiltInFunction(
        string functionName,
        IReadOnlyList<object?> args,
        FormulaDomainFunctionResolver? domainResolver,
        out object? value)
    {
        value = null;
        switch (functionName.ToUpperInvariant())
        {
            case "NZ":
                value = args.Count is 1 or 2
                    ? IsNullOrEmpty(args[0]) ? args.Count == 2 ? args[1] : string.Empty : args[0]
                    : null;
                return args.Count is 1 or 2;
            case "ISNULL":
                value = args.Count == 1 && NormalizeValue(args[0]) is null;
                return args.Count == 1;
            case "ISEMPTY":
                value = args.Count == 1 && IsNullOrEmpty(args[0]);
                return args.Count == 1;
            case "IIF":
                value = args.Count == 3 ? IsTruthy(args[0]) ? args[1] : args[2] : null;
                return args.Count == 3;
            case "SWITCH":
                if (args.Count == 0 || args.Count % 2 != 0)
                    return true;
                for (int i = 0; i < args.Count; i += 2)
                {
                    if (IsTruthy(args[i]))
                    {
                        value = args[i + 1];
                        return true;
                    }
                }
                return true;
            case "CHOOSE":
                if (args.Count < 2 || !TryConvertLong(args[0], out long index))
                    return true;
                value = index >= 1 && index < args.Count ? args[(int)index] : null;
                return true;
            case "LEN":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).Length : null;
                return args.Count == 1;
            case "LEFT":
                value = args.Count == 2 && TryConvertLong(args[1], out long leftCount)
                    ? Left(ToFormulaString(args[0]), leftCount)
                    : null;
                return args.Count == 2;
            case "RIGHT":
                value = args.Count == 2 && TryConvertLong(args[1], out long rightCount)
                    ? Right(ToFormulaString(args[0]), rightCount)
                    : null;
                return args.Count == 2;
            case "MID":
                value = EvaluateMid(args);
                return args.Count is 2 or 3;
            case "TRIM":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).Trim() : null;
                return args.Count == 1;
            case "LTRIM":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).TrimStart() : null;
                return args.Count == 1;
            case "RTRIM":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).TrimEnd() : null;
                return args.Count == 1;
            case "UCASE":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).ToUpperInvariant() : null;
                return args.Count == 1;
            case "LCASE":
                value = args.Count == 1 && args[0] is not null ? ToFormulaString(args[0]).ToLowerInvariant() : null;
                return args.Count == 1;
            case "INSTR":
                value = EvaluateInStr(args);
                return args.Count is 2 or 3;
            case "REPLACE":
                value = args.Count == 3 && args[0] is not null && args[1] is not null
                    ? ToFormulaString(args[0]).Replace(ToFormulaString(args[1]), ToFormulaString(args[2]), StringComparison.Ordinal)
                    : null;
                return args.Count == 3;
            case "STRCOMP":
                value = EvaluateStrComp(args);
                return args.Count is 2 or 3;
            case "VAL":
                value = args.Count == 1 ? ParseLeadingNumber(ToFormulaString(args[0])) : null;
                return args.Count == 1;
            case "DATE":
                value = args.Count == 0 ? DateTime.Now.Date : null;
                return args.Count == 0;
            case "TIME":
                value = args.Count == 0 ? DateTime.Now.TimeOfDay : null;
                return args.Count == 0;
            case "NOW":
                value = args.Count == 0 ? DateTime.Now : null;
                return args.Count == 0;
            case "YEAR":
                value = args.Count == 1 && TryConvertDateTime(args[0], out DateTime yearDate) ? yearDate.Year : null;
                return args.Count == 1;
            case "MONTH":
                value = args.Count == 1 && TryConvertDateTime(args[0], out DateTime monthDate) ? monthDate.Month : null;
                return args.Count == 1;
            case "DAY":
                value = args.Count == 1 && TryConvertDateTime(args[0], out DateTime dayDate) ? dayDate.Day : null;
                return args.Count == 1;
            case "HOUR":
                value = args.Count == 1 && TryConvertTime(args[0], out TimeSpan hourTime) ? hourTime.Hours : null;
                return args.Count == 1;
            case "MINUTE":
                value = args.Count == 1 && TryConvertTime(args[0], out TimeSpan minuteTime) ? minuteTime.Minutes : null;
                return args.Count == 1;
            case "SECOND":
                value = args.Count == 1 && TryConvertTime(args[0], out TimeSpan secondTime) ? secondTime.Seconds : null;
                return args.Count == 1;
            case "DATEADD":
                value = EvaluateDateAdd(args);
                return args.Count == 3;
            case "DATEDIFF":
                value = EvaluateDateDiff(args);
                return args.Count == 3;
            case "DATEPART":
                value = EvaluateDatePart(args);
                return args.Count == 2;
            case "DATESERIAL":
                value = EvaluateDateSerial(args);
                return args.Count == 3;
            case "TIMESERIAL":
                value = EvaluateTimeSerial(args);
                return args.Count == 3;
            case "WEEKDAY":
                value = args.Count == 1 && TryConvertDateTime(args[0], out DateTime weekdayDate)
                    ? ((int)weekdayDate.DayOfWeek) + 1
                    : null;
                return args.Count == 1;
            case "MONTHNAME":
                value = EvaluateMonthName(args);
                return args.Count is 1 or 2;
            case "ABS":
                value = args.Count == 1 && TryConvertDouble(args[0], out double absValue) ? Math.Abs(absValue) : null;
                return args.Count == 1;
            case "ROUND":
                value = EvaluateRound(args);
                return args.Count is 1 or 2;
            case "INT":
                value = args.Count == 1 && TryConvertDouble(args[0], out double intValue) ? Math.Floor(intValue) : null;
                return args.Count == 1;
            case "FIX":
                value = args.Count == 1 && TryConvertDouble(args[0], out double fixValue) ? Math.Truncate(fixValue) : null;
                return args.Count == 1;
            case "SGN":
                value = args.Count == 1 && TryConvertDouble(args[0], out double sgnValue) ? Math.Sign(sgnValue) : null;
                return args.Count == 1;
            case "CSTR":
                value = args.Count == 1 ? ToFormulaString(args[0]) : null;
                return args.Count == 1;
            case "CINT":
            case "CLNG":
                value = args.Count == 1 && TryConvertDouble(args[0], out double integerValue)
                    ? Convert.ToInt64(Math.Round(integerValue, MidpointRounding.ToEven), CultureInfo.InvariantCulture)
                    : null;
                return args.Count == 1;
            case "CDBL":
                value = args.Count == 1 && TryConvertDouble(args[0], out double doubleValue) ? doubleValue : null;
                return args.Count == 1;
            case "CBOOL":
                value = args.Count == 1 && TryConvertBoolean(args[0], out bool boolValue) ? boolValue : null;
                return args.Count == 1;
            case "CDATE":
                value = args.Count == 1 && TryConvertDateTime(args[0], out DateTime dateValue) ? dateValue : null;
                return args.Count == 1;
            case "FORMAT":
                value = args.Count == 2 ? FormatValue(args[0], ToFormulaString(args[1])) : null;
                return args.Count == 2;
            case "DLOOKUP":
            case "DCOUNT":
            case "DSUM":
            case "DAVG":
            case "DMIN":
            case "DMAX":
                value = EvaluateDomainFunction(functionName, args, domainResolver);
                return args.Count is 2 or 3;
            default:
                return false;
        }
    }

    private static object? EvaluateMid(IReadOnlyList<object?> args)
    {
        if (args.Count is not (2 or 3) || args[0] is null || !TryConvertLong(args[1], out long start))
            return null;

        string value = ToFormulaString(args[0]);
        int zeroBasedStart = Math.Max(0, (int)start - 1);
        if (zeroBasedStart >= value.Length)
            return string.Empty;

        if (args.Count == 2)
            return value[zeroBasedStart..];

        if (!TryConvertLong(args[2], out long count))
            return null;

        int length = Math.Clamp((int)count, 0, value.Length - zeroBasedStart);
        return value.Substring(zeroBasedStart, length);
    }

    private static object? EvaluateInStr(IReadOnlyList<object?> args)
    {
        if (args.Count is not (2 or 3))
            return null;

        long start = 1;
        object? source = args[0];
        object? search = args[1];
        if (args.Count == 3)
        {
            if (!TryConvertLong(args[0], out start))
                return null;

            source = args[1];
            search = args[2];
        }

        if (source is null || search is null)
            return null;

        string sourceText = ToFormulaString(source);
        string searchText = ToFormulaString(search);
        int zeroBasedStart = Math.Clamp((int)start - 1, 0, sourceText.Length);
        int index = sourceText.IndexOf(searchText, zeroBasedStart, StringComparison.OrdinalIgnoreCase);
        return index < 0 ? 0 : index + 1;
    }

    private static object? EvaluateStrComp(IReadOnlyList<object?> args)
    {
        if (args.Count is not (2 or 3) || args[0] is null || args[1] is null)
            return null;

        StringComparison comparison = StringComparison.Ordinal;
        if (args.Count == 3)
        {
            string mode = ToFormulaString(args[2]);
            if (string.Equals(mode, "text", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(mode, "1", StringComparison.OrdinalIgnoreCase))
            {
                comparison = StringComparison.OrdinalIgnoreCase;
            }
        }

        int result = string.Compare(ToFormulaString(args[0]), ToFormulaString(args[1]), comparison);
        return result < 0 ? -1 : result > 0 ? 1 : 0;
    }

    private static object? EvaluateDateAdd(IReadOnlyList<object?> args)
    {
        if (args.Count != 3 ||
            !TryConvertLong(args[1], out long amount) ||
            !TryConvertDateTime(args[2], out DateTime date))
        {
            return null;
        }

        return AddDateInterval(ToFormulaString(args[0]), date, amount);
    }

    private static object? EvaluateDateDiff(IReadOnlyList<object?> args)
    {
        if (args.Count != 3 ||
            !TryConvertDateTime(args[1], out DateTime start) ||
            !TryConvertDateTime(args[2], out DateTime end))
        {
            return null;
        }

        return DiffDateInterval(ToFormulaString(args[0]), start, end);
    }

    private static object? EvaluateDatePart(IReadOnlyList<object?> args)
    {
        if (args.Count != 2 || !TryConvertDateTime(args[1], out DateTime date))
            return null;

        return GetDatePart(ToFormulaString(args[0]), date);
    }

    private static object? EvaluateDateSerial(IReadOnlyList<object?> args)
    {
        if (args.Count != 3 ||
            !TryConvertLong(args[0], out long year) ||
            !TryConvertLong(args[1], out long month) ||
            !TryConvertLong(args[2], out long day))
        {
            return null;
        }

        try
        {
            return new DateTime((int)year, 1, 1).AddMonths((int)month - 1).AddDays((int)day - 1);
        }
        catch
        {
            return null;
        }
    }

    private static object? EvaluateTimeSerial(IReadOnlyList<object?> args)
    {
        if (args.Count != 3 ||
            !TryConvertLong(args[0], out long hour) ||
            !TryConvertLong(args[1], out long minute) ||
            !TryConvertLong(args[2], out long second))
        {
            return null;
        }

        try
        {
            return TimeSpan.FromHours(hour) + TimeSpan.FromMinutes(minute) + TimeSpan.FromSeconds(second);
        }
        catch
        {
            return null;
        }
    }

    private static object? EvaluateMonthName(IReadOnlyList<object?> args)
    {
        if (args.Count is not (1 or 2) || !TryConvertLong(args[0], out long month) || month is < 1 or > 12)
            return null;

        bool abbreviate = args.Count == 2 && TryConvertBoolean(args[1], out bool abbreviated) && abbreviated;
        DateTimeFormatInfo format = CultureInfo.InvariantCulture.DateTimeFormat;
        return abbreviate ? format.GetAbbreviatedMonthName((int)month) : format.GetMonthName((int)month);
    }

    private static object? EvaluateRound(IReadOnlyList<object?> args)
    {
        if (args.Count is not (1 or 2) || !TryConvertDouble(args[0], out double value))
            return null;

        int digits = 0;
        if (args.Count == 2)
        {
            if (!TryConvertLong(args[1], out long parsedDigits) || parsedDigits is < 0 or > 15)
                return null;

            digits = (int)parsedDigits;
        }

        return Math.Round(value, digits, MidpointRounding.ToEven);
    }

    private static object? EvaluateDomainFunction(
        string functionName,
        IReadOnlyList<object?> args,
        FormulaDomainFunctionResolver? domainResolver)
    {
        if (args.Count is not (2 or 3) || domainResolver is null)
            return null;

        string expression = ToFormulaString(args[0]).Trim();
        string domain = ToFormulaString(args[1]).Trim();
        string? criteria = args.Count == 3 ? ToFormulaString(args[2]) : null;
        if (string.IsNullOrWhiteSpace(expression) || string.IsNullOrWhiteSpace(domain))
            return null;

        return domainResolver(new FormulaDomainFunctionRequest(
            functionName.ToUpperInvariant(),
            expression,
            domain,
            string.IsNullOrWhiteSpace(criteria) ? null : criteria));
    }

    private static DateTime? AddDateInterval(string interval, DateTime date, long amount)
    {
        try
        {
            return NormalizeInterval(interval) switch
            {
                "yyyy" => date.AddYears((int)amount),
                "q" => date.AddMonths((int)amount * 3),
                "m" => date.AddMonths((int)amount),
                "y" or "d" or "w" => date.AddDays(amount),
                "ww" => date.AddDays(amount * 7),
                "h" => date.AddHours(amount),
                "n" => date.AddMinutes(amount),
                "s" => date.AddSeconds(amount),
                _ => null,
            };
        }
        catch
        {
            return null;
        }
    }

    private static long? DiffDateInterval(string interval, DateTime start, DateTime end)
        => NormalizeInterval(interval) switch
        {
            "yyyy" => end.Year - start.Year,
            "q" => ((end.Year - start.Year) * 4) + ((end.Month - 1) / 3) - ((start.Month - 1) / 3),
            "m" => ((end.Year - start.Year) * 12) + end.Month - start.Month,
            "y" or "d" or "w" => (long)(end.Date - start.Date).TotalDays,
            "ww" => (long)Math.Floor((end.Date - start.Date).TotalDays / 7),
            "h" => (long)(end - start).TotalHours,
            "n" => (long)(end - start).TotalMinutes,
            "s" => (long)(end - start).TotalSeconds,
            _ => null,
        };

    private static long? GetDatePart(string interval, DateTime date)
        => NormalizeInterval(interval) switch
        {
            "yyyy" => date.Year,
            "q" => ((date.Month - 1) / 3) + 1,
            "m" => date.Month,
            "y" => date.DayOfYear,
            "d" => date.Day,
            "w" => ((int)date.DayOfWeek) + 1,
            "ww" => ISOWeek.GetWeekOfYear(date),
            "h" => date.Hour,
            "n" => date.Minute,
            "s" => date.Second,
            _ => null,
        };

    private static string NormalizeInterval(string interval)
        => interval.Trim().Trim('"', '\'').ToLowerInvariant();

    private static string Left(string value, long count)
    {
        int length = Math.Clamp((int)count, 0, value.Length);
        return value[..length];
    }

    private static string Right(string value, long count)
    {
        int length = Math.Clamp((int)count, 0, value.Length);
        return value[(value.Length - length)..];
    }

    private static double ParseLeadingNumber(string text)
    {
        text = text.TrimStart();
        if (text.Length == 0)
            return 0;

        int index = 0;
        if (text[index] is '+' or '-')
            index++;

        bool hasDigit = false;
        bool hasDecimal = false;
        while (index < text.Length)
        {
            char ch = text[index];
            if (char.IsDigit(ch))
            {
                hasDigit = true;
                index++;
                continue;
            }

            if (ch == '.' && !hasDecimal)
            {
                hasDecimal = true;
                index++;
                continue;
            }

            break;
        }

        if (!hasDigit)
            return 0;

        if (index < text.Length && text[index] is 'e' or 'E')
        {
            int exponentStart = index;
            index++;
            if (index < text.Length && text[index] is '+' or '-')
                index++;

            bool hasExponentDigit = false;
            while (index < text.Length && char.IsDigit(text[index]))
            {
                hasExponentDigit = true;
                index++;
            }

            if (!hasExponentDigit)
                index = exponentStart;
        }

        return double.TryParse(text[..index], NumberStyles.Float, CultureInfo.InvariantCulture, out double result)
            ? result
            : 0;
    }

    private static object? FormatValue(object? value, string format)
    {
        value = NormalizeValue(value);
        if (value is null)
            return null;

        try
        {
            if (value is DateTime dateTime)
                return dateTime.ToString(format, CultureInfo.InvariantCulture);

            if (value is TimeSpan time)
                return time.ToString(format, CultureInfo.InvariantCulture);

            if (TryConvertDouble(value, out double number))
                return number.ToString(format, CultureInfo.InvariantCulture);

            if (value is IFormattable formattable)
                return formattable.ToString(format, CultureInfo.InvariantCulture);
        }
        catch
        {
            return ToFormulaString(value);
        }

        return ToFormulaString(value);
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
        left = NormalizeValue(left);
        right = NormalizeValue(right);

        if (left is null || right is null)
            return left is null && right is null ? 0 : left is null ? -1 : 1;

        if (TryConvertDouble(left, out double leftNumber) && TryConvertDouble(right, out double rightNumber))
            return leftNumber.CompareTo(rightNumber);

        if (TryConvertDateTime(left, out DateTime leftDate) && TryConvertDateTime(right, out DateTime rightDate))
            return leftDate.CompareTo(rightDate);

        if (TryConvertBoolean(left, out bool leftBool) && TryConvertBoolean(right, out bool rightBool))
            return leftBool.CompareTo(rightBool);

        return string.Compare(ToFormulaString(left), ToFormulaString(right), StringComparison.OrdinalIgnoreCase);
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

        string text = ToFormulaString(value);
        if (bool.TryParse(text, out bool parsed))
            return parsed;

        return !string.IsNullOrWhiteSpace(text);
    }

    private static bool IsNullOrEmpty(object? value)
    {
        value = NormalizeValue(value);
        return value is null || value is string text && text.Length == 0;
    }

    private static bool TryConvertDouble(object? value, out double result)
    {
        value = NormalizeValue(value);
        switch (value)
        {
            case byte number:
                result = number;
                return true;
            case sbyte number:
                result = number;
                return true;
            case short number:
                result = number;
                return true;
            case ushort number:
                result = number;
                return true;
            case int number:
                result = number;
                return true;
            case uint number:
                result = number;
                return true;
            case long number:
                result = number;
                return true;
            case ulong number:
                result = number;
                return true;
            case float number:
                result = number;
                return true;
            case double number:
                result = number;
                return true;
            case decimal number:
                result = (double)number;
                return true;
            case bool boolean:
                result = boolean ? 1 : 0;
                return true;
            case string text:
                return double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out result);
            default:
                result = 0;
                return false;
        }
    }

    private static bool TryConvertLong(object? value, out long result)
    {
        value = NormalizeValue(value);
        switch (value)
        {
            case byte number:
                result = number;
                return true;
            case sbyte number:
                result = number;
                return true;
            case short number:
                result = number;
                return true;
            case ushort number:
                result = number;
                return true;
            case int number:
                result = number;
                return true;
            case uint number:
                result = number;
                return true;
            case long number:
                result = number;
                return true;
            case float or double or decimal:
                if (TryConvertDouble(value, out double numeric))
                {
                    result = Convert.ToInt64(Math.Round(numeric, MidpointRounding.ToEven), CultureInfo.InvariantCulture);
                    return true;
                }

                break;
            case string text when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer):
                result = integer;
                return true;
            case string text when double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double real):
                result = Convert.ToInt64(Math.Round(real, MidpointRounding.ToEven), CultureInfo.InvariantCulture);
                return true;
        }

        result = 0;
        return false;
    }

    private static bool TryConvertBoolean(object? value, out bool result)
    {
        value = NormalizeValue(value);
        switch (value)
        {
            case bool boolean:
                result = boolean;
                return true;
            case string text:
                if (bool.TryParse(text, out result))
                    return true;
                if (string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(text, "y", StringComparison.OrdinalIgnoreCase))
                {
                    result = true;
                    return true;
                }
                if (string.Equals(text, "no", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(text, "n", StringComparison.OrdinalIgnoreCase))
                {
                    result = false;
                    return true;
                }
                if (TryConvertDouble(text, out double numericText))
                {
                    result = Math.Abs(numericText) > double.Epsilon;
                    return true;
                }
                break;
            default:
                if (TryConvertDouble(value, out double numeric))
                {
                    result = Math.Abs(numeric) > double.Epsilon;
                    return true;
                }
                break;
        }

        result = false;
        return false;
    }

    private static bool TryConvertDateTime(object? value, out DateTime result)
    {
        value = NormalizeValue(value);
        switch (value)
        {
            case DateTime dateTime:
                result = dateTime;
                return true;
            case DateTimeOffset dateTimeOffset:
                result = dateTimeOffset.LocalDateTime;
                return true;
            case DateOnly dateOnly:
                result = dateOnly.ToDateTime(TimeOnly.MinValue);
                return true;
            case string text:
                if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result) ||
                    DateTime.TryParse(text, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces, out result))
                {
                    return true;
                }
                break;
            default:
                if (TryConvertDouble(value, out double numeric))
                {
                    try
                    {
                        result = DateTime.FromOADate(numeric);
                        return true;
                    }
                    catch
                    {
                    }
                }
                break;
        }

        result = default;
        return false;
    }

    private static bool TryConvertTime(object? value, out TimeSpan result)
    {
        value = NormalizeValue(value);
        switch (value)
        {
            case TimeSpan timeSpan:
                result = timeSpan;
                return true;
            case TimeOnly timeOnly:
                result = timeOnly.ToTimeSpan();
                return true;
            case DateTime dateTime:
                result = dateTime.TimeOfDay;
                return true;
            case DateTimeOffset dateTimeOffset:
                result = dateTimeOffset.TimeOfDay;
                return true;
            case string text:
                if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out result))
                    return true;
                if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime parsedDate))
                {
                    result = parsedDate.TimeOfDay;
                    return true;
                }
                break;
        }

        result = default;
        return false;
    }

    private static string ToFormulaString(object? value)
    {
        value = NormalizeValue(value);
        return value switch
        {
            null => string.Empty,
            DateTime dateTime => dateTime.TimeOfDay == TimeSpan.Zero
                ? dateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                : dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            TimeSpan timeSpan => timeSpan.ToString(@"hh\:mm\:ss", CultureInfo.InvariantCulture),
            bool boolean => boolean ? "True" : "False",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };
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
            JsonValueKind.Number when value.TryGetInt64(out long integer) => integer,
            JsonValueKind.Number => value.GetDouble(),
            _ => value.ToString(),
        };

    private static DbValue ToDbValue(object? value)
    {
        value = NormalizeValue(value);
        return value switch
        {
            null => DbValue.Null,
            DbValue dbValue => dbValue,
            bool boolean => DbValue.FromInteger(boolean ? 1 : 0),
            byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
            float or double or decimal => DbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
            string text => DbValue.FromText(text),
            byte[] bytes => DbValue.FromBlob(bytes),
            DateTime or DateTimeOffset or DateOnly or TimeOnly or TimeSpan => DbValue.FromText(ToFormulaString(value)),
            _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
        };
    }

    private static object? FromDbValue(DbValue value) => value.Type switch
    {
        DbType.Null => null,
        DbType.Integer => value.AsInteger,
        DbType.Real => value.AsReal,
        DbType.Text => value.AsText,
        DbType.Blob => value.AsBlob,
        _ => null,
    };

    private static bool TryReadFunctionArguments(
        ReadOnlySpan<char> input,
        int openParen,
        out string argumentsText,
        out int closeParen)
    {
        int depth = 0;
        for (int i = openParen; i < input.Length; i++)
        {
            char ch = input[i];
            if (ch is '\'' or '"')
            {
                i = SkipQuoted(input, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketed(input, i);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    argumentsText = input[(openParen + 1)..i].ToString();
                    closeParen = i;
                    return true;
                }
            }
        }

        argumentsText = string.Empty;
        closeParen = -1;
        return false;
    }

    private static string[] SplitTopLevelArguments(string argumentsText)
    {
        if (string.IsNullOrWhiteSpace(argumentsText))
            return [];

        var arguments = new List<string>();
        int start = 0;
        int depth = 0;
        bool inBracket = false;
        char quote = '\0';
        for (int i = 0; i < argumentsText.Length; i++)
        {
            char ch = argumentsText[i];
            if (quote != '\0')
            {
                if (ch == quote)
                {
                    if (i + 1 < argumentsText.Length && argumentsText[i + 1] == quote)
                    {
                        i++;
                        continue;
                    }

                    quote = '\0';
                }

                continue;
            }

            if (inBracket)
            {
                if (ch == ']')
                    inBracket = false;
                continue;
            }

            if (ch is '\'' or '"')
            {
                quote = ch;
                continue;
            }

            if (ch == '[')
            {
                inBracket = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                arguments.Add(argumentsText[start..i].Trim());
                start = i + 1;
            }
        }

        arguments.Add(argumentsText[start..].Trim());
        return arguments.Any(static argument => argument.Length == 0) ? [] : arguments.ToArray();
    }

    private static bool TryReadLiteralText(string token, out string? value)
    {
        value = null;
        token = token.Trim();
        if (token.Length == 0)
            return false;

        if (token.Length >= 2 && token[0] is '\'' or '"' && token[^1] == token[0])
        {
            char quote = token[0];
            value = token[1..^1].Replace($"{quote}{quote}", quote.ToString(), StringComparison.Ordinal);
            return true;
        }

        if (token.StartsWith('[') && token.EndsWith(']') && token.Length > 2)
        {
            value = token[1..^1].Trim();
            return value.Length > 0;
        }

        if (IsValidIdentifier(token))
        {
            value = token;
            return true;
        }

        return false;
    }

    private static int SkipQuoted(ReadOnlySpan<char> input, int start, char quote)
    {
        for (int i = start + 1; i < input.Length; i++)
        {
            if (input[i] != quote)
                continue;

            if (i + 1 < input.Length && input[i + 1] == quote)
            {
                i++;
                continue;
            }

            return i;
        }

        return input.Length - 1;
    }

    private static int SkipBracketed(ReadOnlySpan<char> input, int start)
    {
        for (int i = start + 1; i < input.Length; i++)
        {
            if (input[i] == ']')
                return i;
        }

        return input.Length - 1;
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';

    private static bool IsIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_';

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
