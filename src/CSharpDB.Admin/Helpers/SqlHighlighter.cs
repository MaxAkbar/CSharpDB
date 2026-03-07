using System.Text;
using System.Text.RegularExpressions;

namespace CSharpDB.Admin.Helpers;

/// <summary>
/// Tokenizes SQL text and produces HTML with syntax-highlighting spans.
/// Used by the SqlEditor component's overlay layer.
/// </summary>
public static partial class SqlHighlighter
{
    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW", "TRIGGER",
        "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "ON",
        "ORDER", "BY", "ASC", "DESC", "GROUP", "HAVING", "LIMIT", "OFFSET",
        "AS", "DISTINCT", "ALL", "EXISTS", "BETWEEN", "LIKE", "UNION",
        "BEGIN", "END", "COMMIT", "ROLLBACK", "TRANSACTION",
        "PRIMARY", "KEY", "UNIQUE", "NOT", "DEFAULT", "CHECK", "FOREIGN", "REFERENCES",
        "IF", "ELSE", "CASE", "WHEN", "THEN", "COLUMN", "ADD", "RENAME", "TO",
        "BEFORE", "AFTER", "FOR", "EACH", "ROW", "INSTEAD", "OF",
        "IDENTITY", "AUTOINCREMENT",
        "INTEGER", "TEXT", "REAL", "BLOB", "BOOLEAN", "INT", "VARCHAR", "CHAR",
        "TRUE", "FALSE"
    };

    private static readonly HashSet<string> Functions = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "COALESCE", "IFNULL",
        "UPPER", "LOWER", "LENGTH", "SUBSTR", "TRIM", "REPLACE", "ABS", "ROUND",
        "DATE", "TIME", "DATETIME", "TYPEOF", "TOTAL", "GROUP_CONCAT"
    };

    public static string Highlight(string sql)
    {
        if (string.IsNullOrEmpty(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length * 2);
        int i = 0;

        while (i < sql.Length)
        {
            // Line comments: --
            if (i < sql.Length - 1 && sql[i] == '-' && sql[i + 1] == '-')
            {
                int end = sql.IndexOf('\n', i);
                if (end < 0) end = sql.Length;
                sb.Append("<span class=\"hl-comment\">");
                AppendEscaped(sb, sql, i, end - i);
                sb.Append("</span>");
                i = end;
                continue;
            }

            // Block comments: /* ... */
            if (i < sql.Length - 1 && sql[i] == '/' && sql[i + 1] == '*')
            {
                int end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (end < 0) end = sql.Length - 2;
                end += 2;
                sb.Append("<span class=\"hl-comment\">");
                AppendEscaped(sb, sql, i, end - i);
                sb.Append("</span>");
                i = end;
                continue;
            }

            // Strings: 'text'
            if (sql[i] == '\'')
            {
                int end = i + 1;
                while (end < sql.Length)
                {
                    if (sql[end] == '\'' && end + 1 < sql.Length && sql[end + 1] == '\'')
                    {
                        end += 2; // escaped quote
                        continue;
                    }
                    if (sql[end] == '\'') { end++; break; }
                    end++;
                }
                sb.Append("<span class=\"hl-string\">");
                AppendEscaped(sb, sql, i, end - i);
                sb.Append("</span>");
                i = end;
                continue;
            }

            // Numbers
            if (char.IsDigit(sql[i]) || (sql[i] == '.' && i + 1 < sql.Length && char.IsDigit(sql[i + 1])))
            {
                int end = i;
                while (end < sql.Length && (char.IsDigit(sql[end]) || sql[end] == '.'))
                    end++;
                sb.Append("<span class=\"hl-number\">");
                AppendEscaped(sb, sql, i, end - i);
                sb.Append("</span>");
                i = end;
                continue;
            }

            // Identifiers / keywords / functions
            if (char.IsLetter(sql[i]) || sql[i] == '_')
            {
                int end = i;
                while (end < sql.Length && (char.IsLetterOrDigit(sql[end]) || sql[end] == '_'))
                    end++;
                string word = sql[i..end];

                if (Keywords.Contains(word))
                {
                    sb.Append("<span class=\"hl-keyword\">");
                    AppendEscaped(sb, sql, i, end - i);
                    sb.Append("</span>");
                }
                else if (Functions.Contains(word))
                {
                    sb.Append("<span class=\"hl-function\">");
                    AppendEscaped(sb, sql, i, end - i);
                    sb.Append("</span>");
                }
                else
                {
                    AppendEscaped(sb, sql, i, end - i);
                }
                i = end;
                continue;
            }

            // Operators
            if ("=<>!+-*/%".Contains(sql[i]))
            {
                sb.Append("<span class=\"hl-operator\">");
                AppendEscaped(sb, sql[i]);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Everything else (whitespace, parens, commas, etc.)
            AppendEscaped(sb, sql[i]);
            i++;
        }

        return sb.ToString();
    }

    private static void AppendEscaped(StringBuilder sb, string text, int start, int length)
    {
        for (int i = start; i < start + length && i < text.Length; i++)
            AppendEscaped(sb, text[i]);
    }

    private static void AppendEscaped(StringBuilder sb, char c)
    {
        switch (c)
        {
            case '<': sb.Append("&lt;"); break;
            case '>': sb.Append("&gt;"); break;
            case '&': sb.Append("&amp;"); break;
            case '"': sb.Append("&quot;"); break;
            default: sb.Append(c); break;
        }
    }
}
