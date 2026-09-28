using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Helpers;

public enum SqlCompletionSourceKind
{
    Table,
    View,
    SystemCatalog,
}

public enum SqlCompletionSuggestionKind
{
    Keyword,
    Source,
    Column,
    Procedure,
    Function,
}

public sealed record SqlCompletionSource(string Name, SqlCompletionSourceKind Kind);

public sealed record SqlCompletionColumn(string Name, string? Type, string SourceName);

public sealed record SqlCompletionFunction(
    string Name,
    int? Arity,
    string? ReturnType,
    string? Description,
    bool CanRunWithoutFrom);

public sealed class SqlCompletionCatalog
{
    public static readonly SqlCompletionCatalog Empty = new();

    public IReadOnlyList<SqlCompletionSource> Sources { get; init; } = [];

    public IReadOnlyDictionary<string, IReadOnlyList<SqlCompletionColumn>> ColumnsBySource { get; init; } =
        new Dictionary<string, IReadOnlyList<SqlCompletionColumn>>(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyList<string> Procedures { get; init; } = [];

    public IReadOnlyList<SqlCompletionFunction> Functions { get; init; } = [];

    public IReadOnlyList<SqlCompletionColumn> GetColumnsForSource(string sourceName)
        => ColumnsBySource.TryGetValue(sourceName, out var columns) ? columns : [];
}

public sealed record SqlCompletionSuggestion(
    string Label,
    string InsertText,
    string Detail,
    SqlCompletionSuggestionKind Kind,
    int ReplacementStart,
    int ReplacementEnd,
    int CaretOffset)
{
    public int CaretPosition => ReplacementStart + CaretOffset;
}

public sealed record SqlCompletionResult(IReadOnlyList<SqlCompletionSuggestion> Suggestions)
{
    public static readonly SqlCompletionResult Empty = new([]);
}

public static partial class SqlCompletionProvider
{
    private const int MaxSuggestions = 12;

    private static readonly SqlCompletionKeyword[] s_keywords =
    [
        new("SELECT", "SELECT ", "query rows"),
        new("FROM", "FROM ", "choose source"),
        new("WHERE", "WHERE ", "filter rows"),
        new("LIKE", "LIKE ", "match a text pattern"),
        new("SET", "SET ", "assign columns"),
        new("ORDER BY", "ORDER BY ", "sort rows"),
        new("GROUP BY", "GROUP BY ", "group rows"),
        new("HAVING", "HAVING ", "filter groups"),
        new("LIMIT", "LIMIT ", "limit rows"),
        new("OFFSET", "OFFSET ", "skip rows"),
        new("JOIN", "JOIN ", "join source"),
        new("LEFT JOIN", "LEFT JOIN ", "join optional source"),
        new("INSERT INTO", "INSERT INTO ", "insert rows"),
        new("VALUES", "VALUES ", "provide row values"),
        new("UPDATE", "UPDATE ", "update rows"),
        new("DELETE FROM", "DELETE FROM ", "delete rows"),
        new("CREATE TABLE", "CREATE TABLE ", "create table"),
        new("CREATE INDEX", "CREATE INDEX ", "create index"),
        new("EXEC", "EXEC ", "execute procedure"),
        new("FIND DUPLICATES IN", "FIND DUPLICATES IN ", "preview duplicate groups"),
        new("DEDUP", "DEDUP ", "delete duplicate rows"),
        new("MERGE DUPLICATES", "MERGE DUPLICATES ", "merge duplicate rows"),
        new("CREATE VALIDATION RULE", "CREATE VALIDATION RULE ", "create audit rule"),
        new("VALIDATE TABLE", "VALIDATE TABLE ", "run validation rules"),
        new("FIND ORPHANS IN", "FIND ORPHANS IN ", "find orphaned child rows"),
    ];

    private static readonly SqlCompletionKeyword[] s_functions =
    [
        new("COUNT", "COUNT()", "aggregate"),
        new("SUM", "SUM()", "aggregate"),
        new("AVG", "AVG()", "aggregate"),
        new("MIN", "MIN()", "aggregate"),
        new("MAX", "MAX()", "aggregate"),
        new("CAST", "CAST()", "convert a value with AS"),
        new("ABS", "ABS()", "function"),
        new("COALESCE", "COALESCE()", "function"),
        new("DATE", "DATE()", "function"),
        new("DATETIME", "DATETIME()", "function"),
        new("IFNULL", "IFNULL()", "function"),
        new("LEN", "LEN()", "function"),
        new("LOWER", "LOWER()", "function"),
        new("NOW", "NOW()", "function"),
        new("ROUND", "ROUND()", "function"),
        new("TIME", "TIME()", "function"),
        new("UPPER", "UPPER()", "function"),
        new("LENGTH", "LENGTH()", "function"),
    ];

    private static readonly SqlCompletionKeyword[] s_allKeywords = s_keywords
        .Concat(SqlKeywordCatalog.CompletionKeywords
            .Except(s_keywords.Select(static keyword => keyword.Label), StringComparer.OrdinalIgnoreCase)
            .Except(s_functions
                .Where(static function => function.Label is not ("DATE" or "TIME" or "DATETIME"))
                .Select(static function => function.Label), StringComparer.OrdinalIgnoreCase)
            .Order(StringComparer.Ordinal)
            .Select(static keyword => new SqlCompletionKeyword(keyword, keyword + " ", "SQL keyword")))
        .ToArray();

    public static SqlCompletionResult GetCompletions(
        string sql,
        int caret,
        SqlCompletionCatalog catalog,
        bool explicitTrigger = false)
    {
        sql ??= string.Empty;
        catalog ??= SqlCompletionCatalog.Empty;
        caret = Math.Clamp(caret, 0, sql.Length);

        var context = SqlCompletionContext.Read(sql, caret);
        if (context.Suppressed)
            return SqlCompletionResult.Empty;

        SqlCompletionResult result;
        var nextKeywords = context.NextKeywords.Where(next => MatchesPrefix(next, context.Prefix)).ToArray();
        if (nextKeywords.Length > 0)
            result = new SqlCompletionResult(nextKeywords.Select(next => new SqlCompletionSuggestion(next, next + " ", "SQL keyword",
                SqlCompletionSuggestionKind.Keyword, context.Start, context.End, next.Length + 1)).ToArray());
        else if (context.SourcePosition)
            result = BuildSourceSuggestions(catalog, context.Prefix, context.Start, context.End, sourceForSelectList: false);
        else if (context.ProcedurePosition)
            result = BuildProcedureSuggestions(catalog, context.Prefix, context.Start, context.End);
        else if (context.Qualifier is not null)
        {
            var source = context.Sources.FirstOrDefault(source =>
                context.Qualifier.Equals(source.Alias ?? source.Name, StringComparison.OrdinalIgnoreCase));
            result = BuildColumnSuggestions(catalog, source?.Name ?? context.Qualifier,
                context.Prefix, context.Start, context.End, context.Wildcard, context.Quoted);
        }
        else if (context.SelectList)
            result = BuildSelectListSuggestions(
                BuildScopeColumnSuggestions(catalog, context),
                BuildFunctionSuggestions(catalog, context.Prefix, context.Start, context.End),
                context.Prefix, context.Start, context.End, context.Sources.Count == 0, catalog);
        else if (context.ColumnPosition)
            result = MergeKeywordSuggestions(BuildScopeColumnSuggestions(catalog, context),
                context.Prefix, context.Start, context.End);
        else if (context.Prefix.Length == 0 && !explicitTrigger)
            return SqlCompletionResult.Empty;
        else
            result = BuildKeywordSuggestions(context.Prefix, context.Start, context.End);

        if (context.Quoted)
            result = new SqlCompletionResult(result.Suggestions
                .Where(s => s.Kind is SqlCompletionSuggestionKind.Column or SqlCompletionSuggestionKind.Source or SqlCompletionSuggestionKind.Procedure)
                .Select(s => s.Kind == SqlCompletionSuggestionKind.Column ? s
                    : s with { InsertText = SqlIdentifierRules.Quote(s.Label), CaretOffset = SqlIdentifierRules.Quote(s.Label).Length })
                .ToArray());
        return MaybeSuppressExactMatch(result, context.Prefix, explicitTrigger);
    }

    private static SqlCompletionResult BuildScopeColumnSuggestions(SqlCompletionCatalog catalog, SqlCompletionContext context)
    {
        var columns = context.Sources.SelectMany(source => catalog.GetColumnsForSource(source.Name)
            .Select(column => (Source: source, Column: column))).ToArray();
        var ambiguous = columns.GroupBy(c => c.Column.Name, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1).Select(group => group.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var suggestions = new List<SqlCompletionSuggestion>();
        if (context.Wildcard && context.Prefix.Length == 0 && columns.Length > 0)
            suggestions.Add(new("*", "*", "all columns", SqlCompletionSuggestionKind.Column, context.Start, context.End, 1));
        suggestions.AddRange(columns.Where(c => MatchesPrefix(c.Column.Name, context.Prefix))
            .OrderBy(c => c.Column.Name, StringComparer.OrdinalIgnoreCase)
            .Select(c =>
            {
                string name = context.Quoted ? SqlIdentifierRules.Quote(c.Column.Name) : FormatIdentifier(c.Column.Name);
                string label = c.Column.Name;
                if (ambiguous.Contains(c.Column.Name))
                {
                    string qualifier = c.Source.Alias ?? c.Source.Name;
                    name = FormatIdentifier(qualifier) + "." + name;
                    label = qualifier + "." + label;
                }
                return new SqlCompletionSuggestion(label, name,
                    c.Column.Type is null ? c.Column.SourceName : $"{c.Column.SourceName} - {c.Column.Type}",
                    SqlCompletionSuggestionKind.Column, context.Start, context.End, name.Length);
            }).Take(MaxSuggestions - suggestions.Count));
        return new SqlCompletionResult(suggestions);
    }

    private static string FormatIdentifier(string name)
        => name.Length > 0 && (char.IsLetter(name[0]) || name[0] == '_')
            && name.All(c => char.IsLetterOrDigit(c) || c == '_')
            && !Tokenizer.ReservedKeywords.Contains(name, StringComparer.OrdinalIgnoreCase)
                ? name : SqlIdentifierRules.Quote(name);

    private static SqlCompletionResult MaybeSuppressExactMatch(
        SqlCompletionResult result,
        string prefix,
        bool explicitTrigger)
    {
        if (explicitTrigger || prefix.Length == 0 || result.Suggestions.Count == 0)
            return result;

        bool hasExactMatch = result.Suggestions.Any(suggestion => suggestion.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase));
        bool hasLongerKeyword = result.Suggestions.Any(suggestion =>
            suggestion.Kind == SqlCompletionSuggestionKind.Keyword
            && suggestion.Label.Length > prefix.Length
            && suggestion.Label.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));

        return hasExactMatch && !hasLongerKeyword
            ? SqlCompletionResult.Empty
            : result;
    }

    private static SqlCompletionResult MergeKeywordSuggestions(
        SqlCompletionResult primary,
        string prefix,
        int replacementStart,
        int replacementEnd)
    {
        if (prefix.Length == 0)
            return primary;

        var keywords = BuildKeywordSuggestions(prefix, replacementStart, replacementEnd).Suggestions;
        var exact = primary.Suggestions.Concat(keywords)
            .Where(suggestion => suggestion.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase))
            .DistinctBy(static suggestion => (suggestion.Kind, suggestion.Label))
            .ToArray();
        var remainingKeywords = keywords
            .Where(suggestion => !suggestion.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        // Retain schema/function suggestions while reserving room for keyword
        // matches, even when many column names share the user's prefix.
        int primaryLimit = Math.Max(0, MaxSuggestions - exact.Length - Math.Min(3, remainingKeywords.Length));
        var suggestions = exact.Concat(primary.Suggestions
                .Where(suggestion => !suggestion.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase))
                .Take(primaryLimit))
            .Concat(remainingKeywords)
            .DistinctBy(static suggestion => (suggestion.Kind, suggestion.Label))
            .Take(MaxSuggestions)
            .ToArray();
        return suggestions.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static SqlCompletionResult BuildKeywordSuggestions(
        string prefix,
        int replacementStart,
        int replacementEnd)
    {
        var suggestions = s_allKeywords
            .Where(keyword => MatchesPrefix(keyword.Label, prefix))
            .Concat(s_functions.Where(function => MatchesPrefix(function.Label, prefix)))
            .OrderByDescending(keyword => keyword.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase))
            .Select(keyword =>
            {
                bool isFunction = s_functions.Contains(keyword);
                string insertText = keyword.InsertText;
                int caretOffset = isFunction && insertText.EndsWith("()", StringComparison.Ordinal)
                    ? insertText.Length - 1
                    : insertText.Length;
                return new SqlCompletionSuggestion(
                    keyword.Label,
                    insertText,
                    keyword.Detail,
                    isFunction ? SqlCompletionSuggestionKind.Function : SqlCompletionSuggestionKind.Keyword,
                    replacementStart,
                    replacementEnd,
                    caretOffset);
            })
            .Take(MaxSuggestions)
            .ToArray();

        return suggestions.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static SqlCompletionResult BuildSelectListSuggestions(
        SqlCompletionResult columns,
        SqlCompletionResult functions,
        string prefix,
        int replacementStart,
        int replacementEnd,
        bool includeSources,
        SqlCompletionCatalog catalog)
    {
        var suggestions = new List<SqlCompletionSuggestion>(MaxSuggestions);

        if (columns.Suggestions.Count > 0)
            suggestions.AddRange(columns.Suggestions);

        if (includeSources)
            suggestions.AddRange(BuildSourceSuggestions(catalog, prefix, replacementStart, replacementEnd, sourceForSelectList: true).Suggestions);

        suggestions.AddRange(functions.Suggestions);

        SqlCompletionSuggestion[] distinct = suggestions
            .GroupBy(static suggestion => $"{(int)suggestion.Kind}\u001f{suggestion.Label}", StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .Take(MaxSuggestions)
            .ToArray();

        return MergeKeywordSuggestions(
            distinct.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(distinct),
            prefix,
            replacementStart,
            replacementEnd);
    }

    private static SqlCompletionResult BuildSourceSuggestions(
        SqlCompletionCatalog catalog,
        string prefix,
        int replacementStart,
        int replacementEnd,
        bool sourceForSelectList)
    {
        var suggestions = catalog.Sources
            .Where(source => MatchesPrefix(source.Name, prefix))
            .OrderBy(source => source.Kind)
            .ThenBy(source => source.Name, StringComparer.OrdinalIgnoreCase)
            .Take(MaxSuggestions)
            .Select(source =>
            {
                string sourceSql = source.Kind == SqlCompletionSourceKind.SystemCatalog
                    ? string.Join(".", source.Name.Split('.').Select(FormatIdentifier))
                    : FormatIdentifier(source.Name);
                string insertText = sourceForSelectList
                    ? $"{Environment.NewLine}FROM {sourceSql}"
                    : sourceSql;
                int caretOffset = sourceForSelectList ? 0 : insertText.Length;
                return new SqlCompletionSuggestion(
                    source.Name,
                    insertText,
                    source.Kind switch
                    {
                        SqlCompletionSourceKind.View => "view",
                        SqlCompletionSourceKind.SystemCatalog => "system catalog",
                        _ => "table",
                    },
                    SqlCompletionSuggestionKind.Source,
                    replacementStart,
                    replacementEnd,
                    caretOffset);
            })
            .ToArray();

        return suggestions.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static SqlCompletionResult BuildColumnSuggestions(
        SqlCompletionCatalog catalog,
        string sourceName,
        string prefix,
        int replacementStart,
        int replacementEnd,
        bool includeWildcard = true,
        bool quote = false)
    {
        var columns = catalog.GetColumnsForSource(sourceName);
        if (columns.Count == 0)
            return SqlCompletionResult.Empty;

        var suggestions = new List<SqlCompletionSuggestion>();
        if (includeWildcard && (prefix.Length == 0 || MatchesPrefix("*", prefix)))
        {
            suggestions.Add(new SqlCompletionSuggestion(
                "*",
                "*",
                sourceName,
                SqlCompletionSuggestionKind.Column,
                replacementStart,
                replacementEnd,
                1));
        }

        suggestions.AddRange(columns
            .Where(column => MatchesPrefix(column.Name, prefix))
            .OrderBy(column => column.Name, StringComparer.OrdinalIgnoreCase)
            .Take(MaxSuggestions - suggestions.Count)
            .Select(column => new SqlCompletionSuggestion(
                column.Name,
                quote ? SqlIdentifierRules.Quote(column.Name) : FormatIdentifier(column.Name),
                column.Type is null ? column.SourceName : $"{column.SourceName} - {column.Type}",
                SqlCompletionSuggestionKind.Column,
                replacementStart,
                replacementEnd,
                (quote ? SqlIdentifierRules.Quote(column.Name) : FormatIdentifier(column.Name)).Length)));

        return suggestions.Count == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static SqlCompletionResult BuildProcedureSuggestions(
        SqlCompletionCatalog catalog,
        string prefix,
        int replacementStart,
        int replacementEnd)
    {
        var suggestions = catalog.Procedures
            .Where(name => MatchesPrefix(name, prefix))
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Take(MaxSuggestions)
            .Select(name => new SqlCompletionSuggestion(
                name,
                name,
                "procedure",
                SqlCompletionSuggestionKind.Procedure,
                replacementStart,
                replacementEnd,
                name.Length))
            .ToArray();

        return suggestions.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static SqlCompletionResult BuildFunctionSuggestions(
        SqlCompletionCatalog catalog,
        string prefix,
        int replacementStart,
        int replacementEnd)
    {
        var catalogFunctions = catalog.Functions
            .Where(function => function.CanRunWithoutFrom && MatchesPrefix(function.Name, prefix))
            .OrderBy(function => function.Name, StringComparer.OrdinalIgnoreCase)
            .Select(function =>
            {
                string insertText = $"{function.Name}()";
                int caretOffset = function.Arity == 0 ? insertText.Length : insertText.Length - 1;
                string detail = BuildFunctionDetail(function);
                return new SqlCompletionSuggestion(
                    function.Name,
                    insertText,
                    detail,
                    SqlCompletionSuggestionKind.Function,
                    replacementStart,
                    replacementEnd,
                    caretOffset);
            });

        var builtIns = s_functions
            .Where(function => MatchesPrefix(function.Label, prefix))
            .OrderBy(function => function.Label, StringComparer.OrdinalIgnoreCase)
            .Select(function =>
            {
                int caretOffset = function.InsertText.EndsWith("()", StringComparison.Ordinal)
                    ? function.InsertText.Length - 1
                    : function.InsertText.Length;
                return new SqlCompletionSuggestion(
                    function.Label,
                    function.InsertText,
                    function.Detail,
                    SqlCompletionSuggestionKind.Function,
                    replacementStart,
                    replacementEnd,
                    caretOffset);
            });

        SqlCompletionSuggestion[] suggestions = builtIns
            .Concat(catalogFunctions)
            .GroupBy(static suggestion => suggestion.Label, StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .Take(MaxSuggestions)
            .ToArray();

        return suggestions.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(suggestions);
    }

    private static string BuildFunctionDetail(SqlCompletionFunction function)
    {
        string arity = function.Arity switch
        {
            0 => "0 args",
            1 => "1 arg",
            int count => $"{count} args",
            _ => "scalar",
        };
        string type = string.IsNullOrWhiteSpace(function.ReturnType) ? "scalar" : function.ReturnType;
        string description = string.IsNullOrWhiteSpace(function.Description) ? string.Empty : $" - {function.Description}";
        return $"host function - {type} - {arity}{description}";
    }

    private static bool MatchesPrefix(string value, string prefix)
        => prefix.Length == 0 || value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);

    private sealed record SqlCompletionKeyword(string Label, string InsertText, string Detail);

}
