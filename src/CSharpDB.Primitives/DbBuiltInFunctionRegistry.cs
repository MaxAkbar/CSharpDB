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
    private static readonly DbBuiltInFunctionDescriptor[] s_functions =
    [
        Scalar("TEXT", [], 1, 1, "any", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "CSharpDB display text"),
        Scalar("NZ", [], 1, 2, "any", null, "first non-empty argument or text", DbFunctionNullBehavior.HandlesNull, "Access-like"),
        Scalar("ISNULL", [], 1, 1, "any", DbType.Integer, "boolean integer", DbFunctionNullBehavior.HandlesNull, "CSharpDB null predicate"),
        Scalar("ISEMPTY", [], 1, 1, "any", DbType.Integer, "boolean integer", DbFunctionNullBehavior.HandlesNull, "CSharpDB null-or-empty predicate"),
        Scalar("IIF", [], 3, 3, "any", null, "selected branch", DbFunctionNullBehavior.HandlesNull, "Access-like"),
        Scalar("SWITCH", [], 2, null, "any", null, "selected value", DbFunctionNullBehavior.HandlesNull, "Access-like condition/value pairs"),
        Scalar("CHOOSE", [], 2, null, "integer, any", null, "selected value", DbFunctionNullBehavior.HandlesNull, "Access-like one-based selection"),
        Scalar("COALESCE", [], 1, null, "any", null, "first non-null argument", DbFunctionNullBehavior.HandlesNull, "SQL-standard"),
        Scalar("IFNULL", [], 2, 2, "any", null, "first non-null argument", DbFunctionNullBehavior.HandlesNull, "SQLite-like"),
        Scalar("NULLIF", [], 2, 2, "comparable", null, "first argument or null", DbFunctionNullBehavior.HandlesNull, "SQL-standard"),

        Scalar("LEN", ["LENGTH"], 1, 1, "any", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like; LENGTH is alias"),
        Scalar("LEFT", [], 2, 2, "text, integer", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("RIGHT", [], 2, 2, "text, integer", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("MID", ["SUBSTR", "SUBSTRING"], 2, 3, "text, integer[, integer]", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "Access/SQLite-like"),
        Scalar("TRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("LTRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("RTRIM", [], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant ordinal"),
        Scalar("UPPER", ["UCASE"], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant casing"),
        Scalar("LOWER", ["LCASE"], 1, 1, "text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant casing"),
        Scalar("INSTR", [], 2, 3, "text[, text], text", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like case-insensitive search"),
        Scalar("REPLACE", [], 3, 3, "text, text, text", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "ordinal replacement"),
        Scalar("STRCOMP", [], 2, 3, "text, text[, mode]", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like comparison"),
        Scalar("VAL", [], 1, 1, "any", DbType.Real, "real", DbFunctionNullBehavior.HandlesNull, "Access-like leading-number conversion"),

        Scalar("DATE", [], 0, 0, "none", DbType.Text, "ISO date text", DbFunctionNullBehavior.NotApplicable, "current local date", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("TIME", [], 0, 0, "none", DbType.Text, "ISO time text", DbFunctionNullBehavior.NotApplicable, "current local time", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("NOW", ["DATETIME"], 0, 0, "none", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.NotApplicable, "current local time", DbFunctionVolatility.StatementStable, defaults: false, checks: false),
        Scalar("YEAR", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "date component"),
        Scalar("MONTH", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "date component"),
        Scalar("DAY", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "date component"),
        Scalar("HOUR", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "time component"),
        Scalar("MINUTE", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "time component"),
        Scalar("SECOND", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "time component"),
        Scalar("DATEADD", [], 3, 3, "interval, integer, date/time", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("DATEDIFF", [], 3, 3, "interval, date/time, date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("DATEPART", [], 2, 2, "interval, date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("DATESERIAL", [], 3, 3, "integer, integer, integer", DbType.Text, "ISO date text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("TIMESERIAL", [], 3, 3, "integer, integer, integer", DbType.Text, "ISO time text", DbFunctionNullBehavior.Propagates, "Access-like"),
        Scalar("WEEKDAY", [], 1, 1, "date/time", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "Access-like Sunday=1"),
        Scalar("MONTHNAME", [], 1, 2, "integer[, boolean]", DbType.Text, "text", DbFunctionNullBehavior.Propagates, "invariant culture"),

        Scalar("ABS", [], 1, 1, "numeric", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "numeric"),
        Scalar("ROUND", [], 1, 2, "numeric[, integer]", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "numeric"),
        Scalar("INT", [], 1, 1, "numeric", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "floor"),
        Scalar("FIX", [], 1, 1, "numeric", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "truncate toward zero"),
        Scalar("SGN", [], 1, 1, "numeric", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "numeric sign"),
        Scalar("CSTR", [], 1, 1, "any", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "invariant conversion"),
        Scalar("CINT", ["CLNG"], 1, 1, "convertible", DbType.Integer, "integer", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("CDBL", [], 1, 1, "convertible", DbType.Real, "real", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("CBOOL", [], 1, 1, "convertible", DbType.Integer, "boolean integer", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("CDATE", [], 1, 1, "convertible", DbType.Text, "ISO datetime text", DbFunctionNullBehavior.Propagates, "invariant conversion"),
        Scalar("FORMAT", [], 2, 2, "any, text", DbType.Text, "text", DbFunctionNullBehavior.HandlesNull, "invariant culture"),

        Aggregate("COUNT", 0, 1, DbType.Integer, "integer", "row count"),
        Aggregate("SUM", 1, 1, DbType.Real, "real", "numeric aggregate"),
        Aggregate("AVG", 1, 1, DbType.Real, "real", "numeric aggregate"),
        Aggregate("MIN", 1, 1, null, "input type", "comparison aggregate"),
        Aggregate("MAX", 1, 1, null, "input type", "comparison aggregate"),
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
        DbFunctionVolatility volatility = DbFunctionVolatility.Immutable, bool defaults = true, bool checks = true)
        => new(name, aliases, DbBuiltInFunctionKind.Scalar, minimum, maximum, acceptedTypes, returnType,
            returnTypeRule, nullBehavior, volatility, SupportsBatch: false, defaults, checks,
            "function-defined", semantics);

    private static DbBuiltInFunctionDescriptor Aggregate(
        string name, int minimum, int maximum, DbType? returnType, string returnTypeRule, string semantics)
        => new(name, [], DbBuiltInFunctionKind.Aggregate, minimum, maximum, "any", returnType,
            returnTypeRule, DbFunctionNullBehavior.AggregateIgnoresNulls, DbFunctionVolatility.Immutable,
            SupportsBatch: false, AllowedInDefaults: false, AllowedInChecks: false, "input collation", semantics);
}
