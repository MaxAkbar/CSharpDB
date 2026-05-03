using System.Text.RegularExpressions;

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

    private static readonly string[] s_columnContextPreviousTokens =
    [
        "WHERE", "AND", "OR", "ON", "BY", "HAVING", "SET"
    ];

    private static readonly SqlCompletionKeyword[] s_keywords =
    [
        new("SELECT", "SELECT ", "query rows"),
        new("FROM", "FROM ", "choose source"),
        new("WHERE", "WHERE ", "filter rows"),
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
    ];

    private static readonly SqlCompletionKeyword[] s_functions =
    [
        new("COUNT", "COUNT()", "aggregate"),
        new("SUM", "SUM()", "aggregate"),
        new("AVG", "AVG()", "aggregate"),
        new("MIN", "MIN()", "aggregate"),
        new("MAX", "MAX()", "aggregate"),
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

    public static SqlCompletionResult GetCompletions(
        string sql,
        int caret,
        SqlCompletionCatalog catalog,
        bool explicitTrigger = false)
    {
        sql ??= string.Empty;
        catalog ??= SqlCompletionCatalog.Empty;
        caret = Math.Clamp(caret, 0, sql.Length);

        if (IsInsideSingleQuotedString(sql, caret))
            return SqlCompletionResult.Empty;

        var token = ReadCurrentToken(sql, caret);
        if (TryGetDotColumnCompletions(sql, caret, token, catalog, out var dotResult))
            return MaybeSuppressExactMatch(dotResult, token.Prefix, explicitTrigger);

        if (TryGetSelectListCompletions(sql, caret, token, catalog, out var selectResult))
            return MaybeSuppressExactMatch(selectResult, token.Prefix, explicitTrigger);

        string? previousToken = ReadPreviousToken(sql, token.Start);
        if (IsSourceContext(previousToken))
            return MaybeSuppressExactMatch(
                BuildSourceSuggestions(catalog, token.Prefix, token.Start, caret, sourceForSelectList: false),
                token.Prefix,
                explicitTrigger);

        if (IsExecContext(previousToken))
            return MaybeSuppressExactMatch(
                BuildProcedureSuggestions(catalog, token.Prefix, token.Start, caret),
                token.Prefix,
                explicitTrigger);

        if (TryGetInsertCompletions(sql, caret, token, catalog, out var insertResult))
            return MaybeSuppressExactMatch(insertResult, token.Prefix, explicitTrigger);

        if (previousToken is not null
            && s_columnContextPreviousTokens.Contains(previousToken, StringComparer.OrdinalIgnoreCase)
            && TryFindPrimarySource(sql, caret, out string sourceName))
        {
            return MaybeSuppressExactMatch(
                BuildColumnSuggestions(catalog, sourceName, token.Prefix, token.Start, caret),
                token.Prefix,
                explicitTrigger);
        }

        if (TryGetUpdateSetCompletions(sql, caret, token, out var updateSetResult))
            return updateSetResult;

        if (token.Prefix.Length == 0 && !explicitTrigger)
            return SqlCompletionResult.Empty;

        return MaybeSuppressExactMatch(
            BuildKeywordSuggestions(token.Prefix, token.Start, caret, includeAllWhenEmpty: explicitTrigger),
            token.Prefix,
            explicitTrigger);
    }

    private static SqlCompletionResult MaybeSuppressExactMatch(
        SqlCompletionResult result,
        string prefix,
        bool explicitTrigger)
    {
        if (explicitTrigger || prefix.Length == 0 || result.Suggestions.Count == 0)
            return result;

        return result.Suggestions.Any(suggestion => suggestion.Label.Equals(prefix, StringComparison.OrdinalIgnoreCase))
            ? SqlCompletionResult.Empty
            : result;
    }

    private static bool TryGetSelectListCompletions(
        string sql,
        int caret,
        SqlCompletionToken token,
        SqlCompletionCatalog catalog,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;

        if (!TryGetStatementBounds(sql, caret, out int statementStart, out int statementEnd))
            return false;

        string statement = sql[statementStart..statementEnd];
        int relativeCaret = caret - statementStart;
        if (!TryFindSelectList(statement, relativeCaret, out int selectEnd))
            return false;

        string betweenSelectAndCaret = statement[selectEnd..relativeCaret];
        if (ContainsWholeWord(betweenSelectAndCaret, "FROM"))
            return false;

        if (TryFindSourceAfterCaret(statement, relativeCaret, out string sourceAfterCaret))
        {
            result = BuildSelectListSuggestions(
                BuildColumnSuggestions(catalog, sourceAfterCaret, token.Prefix, token.Start, caret),
                BuildFunctionSuggestions(catalog, token.Prefix, token.Start, caret),
                token.Prefix,
                token.Start,
                caret,
                includeSources: false,
                catalog);
            return result.Suggestions.Count > 0;
        }

        result = BuildSelectListSuggestions(
            SqlCompletionResult.Empty,
            BuildFunctionSuggestions(catalog, token.Prefix, token.Start, caret),
            token.Prefix,
            token.Start,
            caret,
            includeSources: true,
            catalog);
        return result.Suggestions.Count > 0;
    }

    private static bool TryGetDotColumnCompletions(
        string sql,
        int caret,
        SqlCompletionToken token,
        SqlCompletionCatalog catalog,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;
        int dotOffset = token.Prefix.LastIndexOf('.');
        if (dotOffset >= 0)
        {
            string embeddedQualifier = token.Prefix[..dotOffset];
            string embeddedPrefix = token.Prefix[(dotOffset + 1)..];
            if (embeddedQualifier.Length == 0)
                return false;

            string embeddedSourceName = ResolveSourceQualifier(sql, caret, embeddedQualifier);
            result = BuildColumnSuggestions(
                catalog,
                embeddedSourceName,
                embeddedPrefix,
                token.Start + dotOffset + 1,
                caret);
            return result.Suggestions.Count > 0;
        }

        if (token.Start <= 0 || sql[token.Start - 1] != '.')
            return false;

        if (!TryReadIdentifierBefore(sql, token.Start - 1, out string qualifier))
            return false;

        string sourceName = ResolveSourceQualifier(sql, caret, qualifier);
        result = BuildColumnSuggestions(catalog, sourceName, token.Prefix, token.Start, caret);
        return result.Suggestions.Count > 0;
    }

    private static SqlCompletionResult BuildKeywordSuggestions(
        string prefix,
        int replacementStart,
        int replacementEnd,
        bool includeAllWhenEmpty)
    {
        var suggestions = s_keywords
            .Where(keyword => includeAllWhenEmpty || MatchesPrefix(keyword.Label, prefix))
            .Concat(s_functions.Where(function => includeAllWhenEmpty || MatchesPrefix(function.Label, prefix)))
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

        return distinct.Length == 0 ? SqlCompletionResult.Empty : new SqlCompletionResult(distinct);
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
                string insertText = sourceForSelectList
                    ? $"{Environment.NewLine}FROM {source.Name}"
                    : source.Name;
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
        bool includeWildcard = true)
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
                column.Name,
                column.Type is null ? column.SourceName : $"{column.SourceName} - {column.Type}",
                SqlCompletionSuggestionKind.Column,
                replacementStart,
                replacementEnd,
                column.Name.Length)));

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

    private static SqlCompletionToken ReadCurrentToken(string sql, int caret)
    {
        int start = caret;
        while (start > 0 && IsIdentifierPart(sql[start - 1]))
            start--;

        return new SqlCompletionToken(start, caret, sql[start..caret]);
    }

    private static string? ReadPreviousToken(string sql, int beforeIndex)
    {
        int index = Math.Clamp(beforeIndex, 0, sql.Length);
        while (index > 0 && char.IsWhiteSpace(sql[index - 1]))
            index--;

        while (index > 0 && !IsIdentifierPart(sql[index - 1]))
            index--;

        int end = index;
        while (index > 0 && IsIdentifierPart(sql[index - 1]))
            index--;

        return end > index ? sql[index..end] : null;
    }

    private static bool TryFindSelectList(string statement, int relativeCaret, out int selectEnd)
    {
        selectEnd = -1;
        foreach (Match match in SelectKeywordRegex().Matches(statement[..relativeCaret]))
            selectEnd = match.Index + match.Length;

        return selectEnd >= 0 && relativeCaret >= selectEnd;
    }

    private static bool TryFindSourceAfterCaret(string statement, int relativeCaret, out string sourceName)
    {
        sourceName = string.Empty;
        var match = FromSourceRegex().Match(statement[relativeCaret..]);
        if (!match.Success)
            return false;

        sourceName = match.Groups["source"].Value;
        return true;
    }

    private static bool TryFindPrimarySource(string sql, int caret, out string sourceName)
    {
        sourceName = string.Empty;
        if (!TryGetStatementBounds(sql, caret, out int statementStart, out int statementEnd))
            return false;

        string statement = sql[statementStart..statementEnd];

        var match = UpdateSourceRegex().Match(statement);
        if (!match.Success)
            match = FromSourceRegex().Match(statement);

        if (!match.Success)
            return false;

        sourceName = match.Groups["source"].Value;
        return true;
    }

    private static bool TryGetInsertCompletions(
        string sql,
        int caret,
        SqlCompletionToken token,
        SqlCompletionCatalog catalog,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;
        if (!TryGetStatementBounds(sql, caret, out int statementStart, out _))
            return false;

        string beforeCaret = sql[statementStart..caret];
        if (TryGetInsertColumnCompletions(beforeCaret, token, catalog, out result))
            return true;

        return TryGetInsertValuesCompletions(beforeCaret, token, out result);
    }

    private static bool TryGetInsertColumnCompletions(
        string beforeCaret,
        SqlCompletionToken token,
        SqlCompletionCatalog catalog,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;
        var match = InsertColumnListOpenRegex().Match(beforeCaret);
        if (!match.Success)
            return false;

        string sourceName = match.Groups["source"].Value;
        result = BuildColumnSuggestions(
            catalog,
            sourceName,
            token.Prefix,
            token.Start,
            token.End,
            includeWildcard: false);

        return result.Suggestions.Count > 0;
    }

    private static bool TryGetInsertValuesCompletions(
        string beforeCaret,
        SqlCompletionToken token,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;
        var match = InsertColumnListClosedRegex().Match(beforeCaret);
        if (!match.Success || !MatchesPrefix("VALUES", token.Prefix))
            return false;

        result = new SqlCompletionResult(
        [
            new SqlCompletionSuggestion(
                "VALUES",
                "VALUES ",
                "provide row values",
                SqlCompletionSuggestionKind.Keyword,
                token.Start,
                token.End,
                7)
        ]);

        return true;
    }

    private static bool TryGetUpdateSetCompletions(
        string sql,
        int caret,
        SqlCompletionToken token,
        out SqlCompletionResult result)
    {
        result = SqlCompletionResult.Empty;
        if (!TryGetStatementBounds(sql, caret, out int statementStart, out _))
            return false;

        string beforeCaret = sql[statementStart..caret];
        var match = UpdateSourceRegex().Match(beforeCaret);
        if (!match.Success)
            return false;

        string trailingText = beforeCaret[match.Length..];
        if (ContainsWholeWord(trailingText, "SET"))
            return false;

        string trimmedTrailingText = trailingText.Trim();
        if (trimmedTrailingText.Length > 0
            && (!trimmedTrailingText.Equals(token.Prefix, StringComparison.Ordinal)
                || !MatchesPrefix("SET", token.Prefix)))
        {
            return false;
        }

        result = new SqlCompletionResult(
        [
            new SqlCompletionSuggestion(
                "SET",
                "SET ",
                "assign columns",
                SqlCompletionSuggestionKind.Keyword,
                token.Start,
                caret,
                4)
        ]);

        return true;
    }

    private static string ResolveSourceQualifier(string sql, int caret, string qualifier)
    {
        if (!TryGetStatementBounds(sql, caret, out int statementStart, out int statementEnd))
            return qualifier;

        foreach (Match match in SourceWithAliasRegex().Matches(sql[statementStart..statementEnd]))
        {
            string source = match.Groups["source"].Value;
            string alias = match.Groups["alias"].Success ? match.Groups["alias"].Value : string.Empty;
            if (source.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
                || alias.Equals(qualifier, StringComparison.OrdinalIgnoreCase))
            {
                return source;
            }
        }

        return qualifier;
    }

    private static bool TryGetStatementBounds(string sql, int caret, out int start, out int end)
    {
        start = sql.LastIndexOf(';', Math.Max(0, caret - 1)) + 1;
        int nextSemicolon = sql.IndexOf(';', caret);
        end = nextSemicolon < 0 ? sql.Length : nextSemicolon;
        return start >= 0 && end >= start;
    }

    private static bool TryReadIdentifierBefore(string sql, int beforeDotIndex, out string identifier)
    {
        identifier = string.Empty;
        int end = beforeDotIndex;
        while (end > 0 && char.IsWhiteSpace(sql[end - 1]))
            end--;

        int start = end;
        while (start > 0 && IsIdentifierPart(sql[start - 1]))
            start--;

        if (end <= start)
            return false;

        identifier = sql[start..end];
        return true;
    }

    private static bool IsSourceContext(string? previousToken)
        => previousToken is not null
            && (previousToken.Equals("FROM", StringComparison.OrdinalIgnoreCase)
                || previousToken.Equals("JOIN", StringComparison.OrdinalIgnoreCase)
                || previousToken.Equals("UPDATE", StringComparison.OrdinalIgnoreCase)
                || previousToken.Equals("INTO", StringComparison.OrdinalIgnoreCase));

    private static bool IsExecContext(string? previousToken)
        => previousToken is not null
            && (previousToken.Equals("EXEC", StringComparison.OrdinalIgnoreCase)
                || previousToken.Equals("EXECUTE", StringComparison.OrdinalIgnoreCase));

    private static bool ContainsWholeWord(string text, string word)
        => Regex.IsMatch(text, $@"\b{Regex.Escape(word)}\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static bool MatchesPrefix(string value, string prefix)
        => prefix.Length == 0 || value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);

    private static bool IsIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_' || value == '.';

    private static bool IsInsideSingleQuotedString(string sql, int caret)
    {
        bool inString = false;
        for (int i = 0; i < caret; i++)
        {
            if (sql[i] != '\'')
                continue;

            if (inString && i + 1 < caret && sql[i + 1] == '\'')
            {
                i++;
                continue;
            }

            inString = !inString;
        }

        return inString;
    }

    [GeneratedRegex(@"\bSELECT\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex SelectKeywordRegex();

    [GeneratedRegex(@"\bFROM\s+(?<source>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex FromSourceRegex();

    [GeneratedRegex(@"^\s*INSERT\s+INTO\s+(?<source>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*\((?<columns>[^)]*)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex InsertColumnListOpenRegex();

    [GeneratedRegex(@"^\s*INSERT\s+INTO\s+(?<source>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*\((?<columns>[^)]*)\)\s*(?<keyword>[A-Za-z_]*)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex InsertColumnListClosedRegex();

    [GeneratedRegex(@"^\s*UPDATE\s+(?<source>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex UpdateSourceRegex();

    [GeneratedRegex(@"\b(?:FROM|JOIN)\s+(?<source>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)(?:\s+(?:AS\s+)?(?<alias>[A-Za-z_][A-Za-z0-9_]*))?", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex SourceWithAliasRegex();

    private sealed record SqlCompletionKeyword(string Label, string InsertText, string Detail);

    private sealed record SqlCompletionToken(int Start, int End, string Prefix);
}
