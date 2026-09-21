namespace CSharpDB.Admin.Helpers;

// Incomplete editor text cannot be parsed as a complete SQL statement. This small
// lexer keeps source names, query scopes and replacement spans without treating
// keywords inside comments, literals or quoted identifiers as SQL clauses.
internal sealed class SqlCompletionContext
{
    internal sealed record Lexeme(string Text, int Start, int End, int Depth, bool Word = false, bool Quoted = false)
    {
        internal bool Is(string text) => !Quoted && Text.Equals(text, StringComparison.OrdinalIgnoreCase);
        internal bool IsName => Word && (Quoted || !SqlKeywordCatalog.Keywords.Contains(Text));
    }

    internal sealed record Source(string Name, string? Alias);
    internal List<Source> Sources { get; } = [];
    internal string Prefix { get; private set; } = "";
    internal int Start { get; private set; }
    internal int End { get; private set; }
    internal bool Quoted { get; private set; }
    internal bool Suppressed { get; private set; }
    internal bool SourcePosition { get; private set; }
    internal bool ProcedurePosition { get; private set; }
    internal bool ColumnPosition { get; private set; }
    internal bool SelectList { get; private set; }
    internal bool Wildcard { get; private set; }
    internal string? Qualifier { get; private set; }
    internal IReadOnlyList<string> NextKeywords { get; private set; } = [];

    internal static SqlCompletionContext Read(string sql, int caret)
    {
        var context = new SqlCompletionContext { Start = caret, End = caret };
        var tokens = Lex(sql, caret, out bool suppressed);
        context.Suppressed = suppressed;
        if (suppressed) return context;

        int statementStart = tokens.FindLastIndex(t => t.Is(";") && t.End <= caret) + 1;
        int statementEnd = tokens.FindIndex(statementStart, t => t.Is(";") && t.Start >= caret);
        if (statementEnd < 0) statementEnd = tokens.Count;
        tokens = tokens.GetRange(statementStart, statementEnd - statementStart);

        // Pick the innermost SELECT whose parentheses still contain the caret.
        // Parentheses in functions/predicates retain their surrounding query scope.
        int scopeStart = 0, scopeEnd = tokens.Count, depth = 0;
        for (int i = 0; i < tokens.Count && tokens[i].End <= caret; i++)
        {
            if (!tokens[i].Is("(")) continue;
            int close = tokens.FindIndex(i + 1, t => t.Is(")") && t.Depth == tokens[i].Depth);
            if (close >= 0 && tokens[close].Start < caret) continue;
            if (i + 1 < tokens.Count && (tokens[i + 1].Is("SELECT") || tokens[i + 1].Is("WITH")))
            {
                scopeStart = i + 1;
                scopeEnd = close < 0 ? tokens.Count : close;
                depth = tokens[i].Depth + 1;
            }
        }
        tokens = tokens.GetRange(scopeStart, scopeEnd - scopeStart);
        int branchStart = 0, branchEnd = tokens.Count;
        for (int i = 0; i < tokens.Count; i++)
        {
            if (tokens[i].Depth != depth || !(tokens[i].Is("UNION") || tokens[i].Is("INTERSECT") || tokens[i].Is("EXCEPT"))) continue;
            if (tokens[i].End < caret) branchStart = i + 1;
            else if (tokens[i].Start < caret) continue;
            else { branchEnd = i; break; }
        }
        tokens = tokens.GetRange(branchStart, branchEnd - branchStart);
        var current = tokens.LastOrDefault(t => t.Word && t.Start < caret && t.End >= caret);
        if (current is not null)
        {
            context.Start = current.Start;
            context.Quoted = current.Quoted;
            context.Prefix = sql[current.Start..caret];
            if (current.Quoted)
            {
                context.Prefix = context.Prefix[1..].Replace("\"\"", "\"", StringComparison.Ordinal);
                if (caret == current.End && sql[caret - 1] == '"') context.Prefix = current.Text;
                context.End = current.End;
            }
        }
        var before = tokens.Where(t => t.End <= context.Start).ToList();
        var previous = before.LastOrDefault();
        if (previous?.Is("@") == true && previous.End == context.Start)
        {
            context.Suppressed = true;
            return context;
        }

        // A dotted source name (sys.tables) is one source prefix, whereas c.Name
        // replaces only the column component after a qualifier.
        int nameStart = before.Count;
        string? qualifier = null;
        if (previous?.Is(".") == true)
        {
            nameStart--;
            while (nameStart > 0 && before[nameStart - 1].Word)
            {
                nameStart--;
                qualifier = qualifier is null ? before[nameStart].Text : before[nameStart].Text + "." + qualifier;
                if (nameStart == 0 || !before[nameStart - 1].Is(".")) break;
                nameStart--;
            }
        }
        var precedingName = nameStart > 0 ? before[nameStart - 1] : null;
        context.SourcePosition = IsSourceIntroducer(before, nameStart - 1);
        context.ProcedurePosition = precedingName?.Is("EXEC") == true || precedingName?.Is("EXECUTE") == true;
        if (context.SourcePosition && qualifier is not null)
        {
            context.Start = before[nameStart].Start;
            context.Prefix = qualifier + "." + context.Prefix;
        }
        else context.Qualifier = qualifier;

        var top = tokens.Where(t => t.Depth == depth).ToList();
        for (int i = 0; i < top.Count; i++)
        {
            if (!IsSourceIntroducer(top, i)) continue;
            int j = i + 1;
            if (j >= top.Count || !top[j].IsName) continue;
            string name = top[j++].Text;
            while (j + 1 < top.Count && top[j].Is(".") && top[j + 1].Word)
            {
                name += "." + top[j + 1].Text;
                j += 2;
            }
            if (j < top.Count && top[j].Is("AS")) j++;
            string? alias = j < top.Count && top[j].IsName ? top[j].Text : null;
            context.Sources.Add(new Source(name, alias));
            i = j - 1;
        }

        string clause = "";
        foreach (var token in top.Where(t => t.End <= context.Start))
        {
            if (token.Quoted) continue;
            string word = token.Text.ToUpperInvariant();
            if (word is "SELECT" or "FROM" or "JOIN" or "WHERE" or "HAVING" or "ON" or "SET" or "ORDER" or "GROUP" or "LIMIT" or "OFFSET" or "VALUES" or "KEEP" or "MESSAGE")
                clause = word;
        }
        context.SelectList = clause == "SELECT";
        context.Wildcard = context.SelectList && (previous?.Is("(") != true || before.Count >= 2 && before[^2].Is("COUNT"));
        bool expressionClause = clause is "WHERE" or "HAVING" or "ON" or "SET" or "ORDER" or "GROUP";
        bool expectsOperand = previous is not null && (previous.Text.ToUpperInvariant() is
            "WHERE" or "HAVING" or "ON" or "SET" or "BY" or "AND" or "OR" or "NOT" or "BETWEEN" or "LIKE" or "ESCAPE" or
            "(" or "," or "=" or "<" or ">" or "!" or "+" or "-" or "*" or "/" or "%" or "|");
        context.ColumnPosition = context.SelectList || expressionClause && (context.Prefix.Length > 0 || expectsOperand);

        var first = top.FirstOrDefault();
        if (first?.Is("INSERT") == true && clause != "SELECT")
        {
            var open = top.FirstOrDefault(t => t.Is("("));
            var close = top.FirstOrDefault(t => t.Is(")"));
            if (open is not null && open.End <= caret && (close is null || close.Start >= caret) && clause != "VALUES")
                context.ColumnPosition = true;
            else if (close is not null && close.End <= context.Start && clause != "VALUES")
                context.NextKeywords = ["VALUES"];
        }
        if (first?.Is("UPDATE") == true && !context.SourcePosition && context.Sources.Count > 0 && clause == "")
            context.NextKeywords = ["SET"];
        if (previous is { Word: true, Quoted: false })
        {
            context.NextKeywords = previous.Text.ToUpperInvariant() switch
            {
                "ORDER" or "GROUP" or "PARTITION" => ["BY"],
                "INNER" or "CROSS" or "OUTER" => ["JOIN"],
                "LEFT" or "RIGHT" => ["JOIN", "OUTER JOIN"],
                "IS" => ["NULL", "NOT NULL"],
                "NOT" when before.Count > 1 && before[^2].Is("IS") => ["NULL"],
                "KEEP" => ["FIRST", "LAST"],
                "INSERT" when !top.Any(t => t.Is("TRIGGER")) => ["INTO"],
                "DELETE" when !top.Any(t => t.Is("TRIGGER")) && !top.Any(t => t.Is("REFERENCES")) => ["FROM"],
                "FIND" => ["DUPLICATES", "ORPHANS"],
                "DUPLICATES" or "ORPHANS" when before.Count > 1 && before[^2].Is("FIND") => ["IN"],
                "MERGE" => ["DUPLICATES"],
                "VALIDATE" => ["TABLE"],
                _ => context.NextKeywords,
            };
        }
        return context;
    }

    private static bool IsSourceIntroducer(List<Lexeme> tokens, int index)
    {
        if (index < 0) return false;
        var token = tokens[index];
        if (token.Quoted) return false;
        if (token.Text.ToUpperInvariant() is "FROM" or "JOIN" or "UPDATE" or "INTO" or "REFERENCES" or "DEDUP" or "ANALYZE") return true;
        if (token.Is("TABLE")) return tokens.Take(index).Any(t => t.Is("ALTER") || t.Is("DROP") || t.Is("VALIDATE"));
        if (token.Is("IN")) return index > 1 && tokens[index - 2].Is("FIND") && (tokens[index - 1].Is("DUPLICATES") || tokens[index - 1].Is("ORPHANS"));
        if (token.Is("DUPLICATES")) return index > 0 && tokens[index - 1].Is("MERGE");
        if (token.Is("ON")) return tokens.Take(index).Any(t => t.Is("CREATE")) && tokens.Take(index).Any(t => t.Is("INDEX") || t.Is("TRIGGER") || t.Is("RULE"));
        return false;
    }

    private static List<Lexeme> Lex(string sql, int caret, out bool suppressed)
    {
        var tokens = new List<Lexeme>();
        suppressed = false;
        int depth = 0;
        for (int i = 0; i < sql.Length;)
        {
            if (char.IsWhiteSpace(sql[i])) { i++; continue; }
            int start = i;
            bool lineComment = sql[i] == '-' && i + 1 < sql.Length && sql[i + 1] == '-';
            bool blockComment = sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*';
            if (lineComment || blockComment)
            {
                i += 2;
                if (lineComment) while (i < sql.Length && sql[i] is not ('\r' or '\n')) i++;
                else
                {
                    while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i = i + 1 < sql.Length ? i + 2 : sql.Length;
                }
                bool closed = blockComment && i >= 2 && sql[(i - 2)..i] == "*/";
                if (caret > start && (caret < i || caret == i && !closed)) suppressed = true;
                continue;
            }
            if (sql[i] is '\'' or '"')
            {
                char quote = sql[i++];
                bool closed = false;
                while (i < sql.Length)
                {
                    if (sql[i++] != quote) continue;
                    if (i < sql.Length && sql[i] == quote) { i++; continue; }
                    closed = true;
                    break;
                }
                if (quote == '\'' && caret > start && (caret < i || caret == i && !closed)) suppressed = true;
                string value = quote == '"' ? sql[(start + 1)..(closed ? i - 1 : i)].Replace("\"\"", "\"", StringComparison.Ordinal) : "";
                tokens.Add(new(value, start, i, depth, quote == '"', quote == '"'));
                continue;
            }
            bool word = char.IsLetter(sql[i]) || sql[i] == '_';
            if (word)
            {
                while (i < sql.Length && (char.IsLetterOrDigit(sql[i]) || sql[i] == '_')) i++;
            }
            else i++;
            string text = sql[start..i];
            if (text == ")") depth = Math.Max(0, depth - 1);
            tokens.Add(new(text, start, i, depth, word));
            if (text == "(") depth++;
        }
        return tokens;
    }
}
