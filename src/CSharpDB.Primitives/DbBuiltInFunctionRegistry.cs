namespace CSharpDB.Primitives;

/// <summary>Describes a built-in CSharpDB SQL function independently of its evaluator.</summary>
public sealed record DbBuiltInFunctionDescriptor(
    string Name,
    IReadOnlyList<string> Aliases,
    DbBuiltInFunctionKind Kind,
    int MinimumArity,
    int? MaximumArity,
    string AcceptedTypes,
    DbType? ReturnType,
    string ReturnTypeRule,
    DbFunctionNullBehavior NullBehavior,
    DbFunctionVolatility Volatility,
    bool SupportsBatch,
    bool AllowedInDefaults,
    bool AllowedInChecks,
    string CollationBehavior,
    string Semantics)
{
    /// <summary>
    /// Logical SQL return type when it is more specific than the shared
    /// physical <see cref="ReturnType"/> carrier.
    /// </summary>
    public SqlTypeDescriptor? DeclaredReturnType { get; init; }

    public bool IsDeterministic => Volatility == DbFunctionVolatility.Immutable;

    public bool AcceptsArity(int arity) =>
        arity >= MinimumArity && (!MaximumArity.HasValue || arity <= MaximumArity.Value);

    public string Signature => MaximumArity switch
    {
        null => $"{Name}({MinimumArity}+)",
        var maximum when maximum == MinimumArity => $"{Name}({MinimumArity})",
        _ => $"{Name}({MinimumArity}..{MaximumArity})",
    };
}

public enum DbBuiltInFunctionKind
{
    Scalar,
    Aggregate,
    Window,
}

public enum DbFunctionNullBehavior
{
    Propagates,
    HandlesNull,
    AggregateIgnoresNulls,
    NotApplicable,
}

public enum DbFunctionVolatility
{
    Immutable,
    StatementStable,
    Volatile,
}

/// <summary>
/// Canonical catalog for SQL built-ins. Evaluators retain the implementation,
/// while binders, diagnostics, and system catalogs consume this metadata.
/// </summary>
public static class DbBuiltInFunctionRegistry
{
    private static SqlTypeDescriptor BooleanType { get; } =
        SqlTypeDescriptor.Create(SqlTypeKind.Boolean);

    private static SqlTypeDescriptor IntegerType { get; } =
        SqlTypeDescriptor.Create(SqlTypeKind.Integer);

    private static SqlTypeDescriptor BigIntType { get; } =
        SqlTypeDescriptor.Create(SqlTypeKind.BigInt);

    private static readonly DbBuiltInFunctionDescriptor[] s_functions =
    [
        Scalar("TEXT", [], 1, 1, "any", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "CSharpDB display text"),
        Scalar("NZ", [], 1, 2, "any", null, "first non-empty argument or text", DbFunctionNullBehavior.HandlesNull, "Access-like"),
        Scalar("ISNULL", [], 1, 1, "any", DbType.Integer, "boolean integer", DbFunctionNullBehavior.HandlesNull, "CSharpDB null predicate", declaredReturnType: BooleanType),
        Scalar("ISEMPTY", [], 1, 1, "any", DbType.Integer, "boolean integer", DbFunctionNullBehavior.HandlesNull, "CSharpDB null-or-empty predicate", declaredReturnType: BooleanType),
        Scalar("IIF", [], 3, 3, "any", null, "selected branch", DbFunctionNullBehavior.HandlesNull, "Access-like"),
        Scalar("SWITCH", [], 2, null, "any", null, "selected value", DbFunctionNullBehavior.HandlesNull, "Access-like condition/value pairs"),
        Scalar("CHOOSE", [], 2, null, "integer, any", null, "selected value", DbFunctionNullBehavior.HandlesNull, "Access-like one-based selection"),
        Scalar("COALESCE", [], 1, null, "any", null, "first non-null argument", DbFunctionNullBehavior.HandlesNull, "SQL-standard"),
        Scalar("IFNULL", [], 2, 2, "any", null, "first non-null argument", DbFunctionNullBehavior.HandlesNull, "SQLite-like"),
        Scalar("NULLIF", [], 2, 2, "comparable", null, "first argument or null", DbFunctionNullBehavior.HandlesNull, "SQL-standard"),

        Scalar("LEN", ["LENGTH"], 1, 1, "any", DbType.Integer, "BIGINT", DbFunctionNullBehavior.Propagates, "Access-like; LENGTH is alias", declaredReturnType: BigIntType),
        Scalar("LEFT", [], 2, 2, "text, integer", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("RIGHT", [], 2, 2, "text, integer", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("MID", ["SUBSTR", "SUBSTRING"], 2, 3, "text, integer[, integer]", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access/SQLite-like"),
        Scalar("TRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("LTRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("RTRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("UPPER", ["UCASE"], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant casing"),
        Scalar("LOWER", ["LCASE"], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant casing"),
        Scalar("INSTR", [], 2, 3, "text[, text], text", DbType.Integer, "BIGINT", DbFunctionNullBehavior.Propagates, "Access-like case-insensitive search", declaredReturnType: BigIntType),
        Scalar("ORDINAL_STARTS_WITH", [], 2, 2, "text, text", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "ordinal case-sensitive prefix search", declaredReturnType: BooleanType),
        Scalar("ORDINAL_ENDS_WITH", [], 2, 2, "text, text", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "ordinal case-sensitive suffix search", declaredReturnType: BooleanType),
        Scalar("ORDINAL_CONTAINS", [], 2, 2, "text, text", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "ordinal case-sensitive substring search", declaredReturnType: BooleanType),
        Scalar("REPLACE", [], 3, 3, "text, text, text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "ordinal replacement"),
        Scalar("STRCOMP", [], 2, 3, "text, text[, mode]", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like comparison"),
        Scalar("VAL", [], 1, 1, "any", DbType.Real, "real", DbFunctionNullBehavior.HandlesNull, "Access-like leading-number conversion"),

        Scalar("XML_EXISTS", ["XMLEXISTS"], 2, 3, "xml/text, xpath[, namespace JSON]", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "XPath 1.0 effective boolean value", declaredReturnType: BooleanType),
        Scalar("XML_VALUE", [], 2, 3, "xml/text, xpath[, namespace JSON]", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "XPath 1.0 scalar string value; node sets must contain at most one node"),

        Scalar("DATE", [], 0, 0, "none", DbType.Text, "ISO date text", DbFunctionNullBehavior.NotApplicable, "current local date", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("TIME", [], 0, 0, "none", DbType.Text, "ISO time text", DbFunctionNullBehavior.NotApplicable, "current local time", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("NOW", ["DATETIME"], 0, 0, "none", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.NotApplicable, "current local time", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("YEAR", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "date component", declaredReturnType: IntegerType),
        Scalar("MONTH", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "date component", declaredReturnType: IntegerType),
        Scalar("DAY", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "date component", declaredReturnType: IntegerType),
        Scalar("HOUR", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "time component", declaredReturnType: IntegerType),
        Scalar("MINUTE", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "time component", declaredReturnType: IntegerType),
        Scalar("SECOND", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "time component", declaredReturnType: IntegerType),
        Scalar("DATEADD", [], 3, 3, "interval, integer, date/time", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("DATEDIFF", [], 3, 3, "interval, date/time, date/time", DbType.Integer, "BIGINT", DbFunctionNullBehavior.Propagates, "Access-like", declaredReturnType: BigIntType),
        Scalar("DATEPART", [], 2, 2, "interval, date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "Access-like", declaredReturnType: IntegerType),
        Scalar("DATESERIAL", [], 3, 3, "integer, integer, integer", DbType.Text, "ISO date text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("TIMESERIAL", [], 3, 3, "integer, integer, integer", DbType.Text, "ISO time text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("WEEKDAY", [], 1, 1, "date/time", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "Access-like Sunday=1", declaredReturnType: IntegerType),
        Scalar("MONTHNAME", [], 1, 2, "integer[, boolean]", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant culture"),

        Scalar("ABS", [], 1, 1, "numeric", null, "input numeric type", DbFunctionNullBehavior.Propagates, "numeric"),
        Scalar("ROUND", [], 1, 2, "numeric[, integer]", null, "input numeric type", DbFunctionNullBehavior.Propagates, "numeric"),
        Scalar("INT", ["FLOOR"], 1, 1, "numeric", null, "input numeric type", DbFunctionNullBehavior.Propagates, "floor"),
        Scalar("FIX", [], 1, 1, "numeric", null, "input numeric type", DbFunctionNullBehavior.Propagates, "truncate toward zero"),
        Scalar("SGN", [], 1, 1, "numeric", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "numeric sign", declaredReturnType: IntegerType),
        Scalar("CSTR", [], 1, 1, "any", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "invariant conversion"),
        Scalar("CINT", [], 1, 1, "convertible", DbType.Integer, "INTEGER", DbFunctionNullBehavior.Propagates, "invariant 32-bit conversion", declaredReturnType: IntegerType),
        Scalar("CLNG", [], 1, 1, "convertible", DbType.Integer, "BIGINT", DbFunctionNullBehavior.Propagates, "invariant 64-bit conversion", declaredReturnType: BigIntType),
        Scalar("CDBL", [], 1, 1, "convertible", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("CBOOL", [], 1, 1, "convertible", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "invariant conversion", declaredReturnType: BooleanType),
        Scalar("CDATE", [], 1, 1, "convertible", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("FORMAT", [], 2, 2, "any, text", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "invariant culture"),

        Aggregate("COUNT", 0, 1, DbType.Integer, "BIGINT", "row count", BigIntType),
        Aggregate("SUM", 1, 1, DbType.Real, "real", "numeric aggregate"),
        Aggregate("AVG", 1, 1, DbType.Real, "real", "numeric aggregate"),
        Aggregate("MIN", 1, 1, null, "input type", "comparison aggregate"),
        Aggregate("MAX", 1, 1, null, "input type", "comparison aggregate"),

        Window("ROW_NUMBER", 0, 0, "none", DbType.Integer, "BIGINT", "one-based row position within the ordered partition", BigIntType),
        Window("RANK", 0, 0, "none", DbType.Integer, "BIGINT", "one-based peer rank with gaps", BigIntType),
        Window("DENSE_RANK", 0, 0, "none", DbType.Integer, "BIGINT", "one-based peer rank without gaps", BigIntType),
        Window("LAG", 1, 3, "value[, offset[, default]]", null, "first argument type", "value from a preceding partition row"),
        Window("LEAD", 1, 3, "value[, offset[, default]]", null, "first argument type", "value from a following partition row"),
        Window("FIRST_VALUE", 1, 1, "value", null, "first argument type", "value from the first row in the effective frame"),
        Window("LAST_VALUE", 1, 1, "value", null, "first argument type", "value from the last row in the effective frame"),
    ];

    private static readonly IReadOnlyDictionary<string, DbBuiltInFunctionDescriptor> s_byName = BuildByName();

    public static IReadOnlyList<DbBuiltInFunctionDescriptor> Functions => s_functions;

    public static bool TryGet(string name, out DbBuiltInFunctionDescriptor descriptor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return s_byName.TryGetValue(name, out descriptor!);
    }

    public static bool IsBuiltInName(string name) => TryGet(name, out _);

    private static IReadOnlyDictionary<string, DbBuiltInFunctionDescriptor> BuildByName()
    {
        var byName = new Dictionary<string, DbBuiltInFunctionDescriptor>(StringComparer.OrdinalIgnoreCase);
        foreach (DbBuiltInFunctionDescriptor descriptor in s_functions)
        {
            byName.Add(descriptor.Name, descriptor);
            foreach (string alias in descriptor.Aliases)
                byName.Add(alias, descriptor);
        }

        return byName;
    }

    private static DbBuiltInFunctionDescriptor Scalar(
        string name, IReadOnlyList<string> aliases, int minimum, int? maximum, string acceptedTypes,
        DbType? returnType, string returnTypeRule, DbFunctionNullBehavior nullBehavior, string semantics,
        DbFunctionVolatility volatility = DbFunctionVolatility.Immutable, bool defaults = true, bool checks = true,
        SqlTypeDescriptor? declaredReturnType = null)
        => new(name, aliases, DbBuiltInFunctionKind.Scalar, minimum, maximum, acceptedTypes, returnType,
            returnTypeRule, nullBehavior, volatility, SupportsBatch: false, defaults, checks,
            "function-defined", semantics)
        {
            DeclaredReturnType = declaredReturnType,
        };

    private static DbBuiltInFunctionDescriptor Aggregate(
        string name, int minimum, int maximum, DbType? returnType, string returnTypeRule, string semantics,
        SqlTypeDescriptor? declaredReturnType = null)
        => new(name, [], DbBuiltInFunctionKind.Aggregate, minimum, maximum, "any", returnType,
            returnTypeRule, DbFunctionNullBehavior.AggregateIgnoresNulls, DbFunctionVolatility.Immutable,
            SupportsBatch: false, AllowedInDefaults: false, AllowedInChecks: false, "input collation", semantics)
        {
            DeclaredReturnType = declaredReturnType,
        };

    private static DbBuiltInFunctionDescriptor Window(
        string name,
        int minimum,
        int maximum,
        string acceptedTypes,
        DbType? returnType,
        string returnTypeRule,
        string semantics,
        SqlTypeDescriptor? declaredReturnType = null)
        => new(name, [], DbBuiltInFunctionKind.Window, minimum, maximum, acceptedTypes, returnType,
            returnTypeRule, DbFunctionNullBehavior.NotApplicable, DbFunctionVolatility.Immutable,
            SupportsBatch: false, AllowedInDefaults: false, AllowedInChecks: false, "input collation", semantics)
        {
            DeclaredReturnType = declaredReturnType,
        };
}
