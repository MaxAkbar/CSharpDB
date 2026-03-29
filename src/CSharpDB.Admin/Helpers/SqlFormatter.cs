using System.Text;
using System.Text.RegularExpressions;

namespace CSharpDB.Admin.Helpers;

/// <summary>
/// Basic SQL formatter: uppercases keywords and adds newlines before major clauses.
/// </summary>
public static partial class SqlFormatter
{
    private static readonly HashSet<string> MajorClauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER", "GROUP", "HAVING",
        "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS",
        "OUTER", "ON", "SET", "VALUES", "INTO", "INSERT", "UPDATE",
        "DELETE", "CREATE", "ALTER", "DROP", "BEGIN", "END", "UNION"
    };

    private static readonly HashSet<string> NewlineBeforeClauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "WHERE", "AND", "OR", "ORDER", "GROUP", "HAVING",
        "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS",
        "ON", "SET", "VALUES", "UNION"
    };

    private static readonly HashSet<string> AllKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW", "TRIGGER",
        "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "ON",
        "ORDER", "BY", "ASC", "DESC", "GROUP", "HAVING", "LIMIT", "OFFSET",
        "AS", "DISTINCT", "ALL", "EXISTS", "BETWEEN", "LIKE", "COLLATE", "UNION",
        "BEGIN", "END", "COMMIT", "ROLLBACK", "TRANSACTION",
        "PRIMARY", "KEY", "UNIQUE", "DEFAULT", "CHECK", "FOREIGN", "REFERENCES",
        "IF", "ELSE", "CASE", "WHEN", "THEN", "COLUMN", "ADD", "RENAME", "TO",
        "BEFORE", "AFTER", "FOR", "EACH", "ROW",
        "IDENTITY", "AUTOINCREMENT",
        "INTEGER", "TEXT", "REAL", "BLOB", "BOOLEAN", "INT", "VARCHAR", "CHAR",
        "TRUE", "FALSE", "NOT"
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
            if (token.IsWord && AllKeywords.Contains(token.Text))
            {
                string upper = token.Text.ToUpperInvariant();

                if (NewlineBeforeClauses.Contains(token.Text) && !isFirstClause && sb.Length > 0)
                {
                    // Trim trailing whitespace before adding newline
                    while (sb.Length > 0 && sb[sb.Length - 1] == ' ')
                        sb.Length--;
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

        return sb.ToString().Trim();
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
                int start = i;
                while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;
                tokens.Add(new Token(" ", false)); // Normalize whitespace to single space
                continue;
            }

            // String literal
            if (sql[i] == '\'')
            {
                int start = i;
                i++;
                while (i < sql.Length)
                {
                    if (sql[i] == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                    { i += 2; continue; }
                    if (sql[i] == '\'') { i++; break; }
                    i++;
                }
                tokens.Add(new Token(sql[start..i], false));
                continue;
            }

            // Comment
            if (i < sql.Length - 1 && sql[i] == '-' && sql[i + 1] == '-')
            {
                int end = sql.IndexOf('\n', i);
                if (end < 0) end = sql.Length;
                tokens.Add(new Token(sql[i..end], false));
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

    private readonly record struct Token(string Text, bool IsWord);
}
