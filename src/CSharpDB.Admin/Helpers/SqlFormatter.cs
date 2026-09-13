using System.Text;

namespace CSharpDB.Admin.Helpers;

/// <summary>
/// Basic SQL formatter: uppercases keywords and adds newlines before major clauses.
/// </summary>
public static partial class SqlFormatter
{
    private static readonly HashSet<string> NewlineBeforeClauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "WHERE", "AND", "OR", "ORDER", "GROUP", "HAVING",
        "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS",
        "ON", "SET", "VALUES", "UNION", "MESSAGE", "REFERENCES"
    };

    public static string Format(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return sql;

        var tokens = Tokenize(sql);
        var sb = new StringBuilder();
        bool isFirstClause = true;

        foreach (var token in tokens)
        {
            if (token.IsWord && SqlKeywordCatalog.Keywords.Contains(token.Text))
            {
                string upper = token.Text.ToUpperInvariant();

                if (NewlineBeforeClauses.Contains(token.Text) && !isFirstClause && sb.Length > 0)
                {
                    // Trim trailing whitespace before adding newline
                    while (sb.Length > 0 && sb[sb.Length - 1] == ' ')
                        sb.Length--;
                    if (sb.Length == 0 || sb[^1] != '\n')
                        sb.Append('\n');
                }

                sb.Append(upper);
                isFirstClause = false;
            }
            else
            {
                sb.Append(token.Text);
            }
        }

        string formatted = sb.ToString();
        return tokens.Count > 0 && tokens[^1].IsOpaque
            ? formatted.TrimStart()
            : formatted.Trim();
    }

    private static List<Token> Tokenize(string sql)
    {
        var tokens = new List<Token>();
        int i = 0;

        while (i < sql.Length)
        {
            // Whitespace
            if (char.IsWhiteSpace(sql[i]))
            {
                while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;
                tokens.Add(new Token(" ", false)); // Normalize whitespace to single space
                continue;
            }

            // String literals and quoted identifiers, including doubled quote escapes.
            if (sql[i] is '\'' or '"')
            {
                char quote = sql[i];
                int start = i;
                i++;
                while (i < sql.Length)
                {
                    if (sql[i] == quote && i + 1 < sql.Length && sql[i + 1] == quote)
                    { i += 2; continue; }
                    if (sql[i] == quote) { i++; break; }
                    i++;
                }
                tokens.Add(new Token(sql[start..i], false, IsOpaque: true));
                continue;
            }

            // Retain the line terminator so later SQL cannot become part of the comment.
            if (i < sql.Length - 1 && sql[i] == '-' && sql[i + 1] == '-')
            {
                int end = sql.IndexOf('\n', i);
                if (end < 0) end = sql.Length;
                else end++;
                tokens.Add(new Token(sql[i..end], false, IsOpaque: true));
                i = end;
                continue;
            }

            // Block comments may contain whitespace, keywords and quotes verbatim.
            if (i < sql.Length - 1 && sql[i] == '/' && sql[i + 1] == '*')
            {
                int end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = end < 0 ? sql.Length : end + 2;
                tokens.Add(new Token(sql[i..end], false, IsOpaque: true));
                i = end;
                continue;
            }

            // Word (identifier or keyword)
            if (char.IsLetter(sql[i]) || sql[i] == '_')
            {
                int start = i;
                while (i < sql.Length && (char.IsLetterOrDigit(sql[i]) || sql[i] == '_')) i++;
                tokens.Add(new Token(sql[start..i], true));
                continue;
            }

            // Everything else
            tokens.Add(new Token(sql[i].ToString(), false));
            i++;
        }

        return tokens;
    }

    private readonly record struct Token(string Text, bool IsWord, bool IsOpaque = false);
}
