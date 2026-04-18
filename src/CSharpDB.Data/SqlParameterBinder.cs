using System.Globalization;
using System.Text;

namespace CSharpDB.Data;

/// <summary>
/// Substitutes @param placeholders in SQL with properly escaped literal values.
/// Single-pass scanner that respects string literals.
/// </summary>
internal static class SqlParameterBinder
{
    public static string Bind(string sql, CSharpDbParameterCollection parameters)
    {
        // Fast path: no placeholders in SQL.
        if (sql.IndexOf('@') < 0)
            return sql;

        ReadOnlySpan<char> sqlSpan = sql.AsSpan();
        StringBuilder? sb = null;
        int segmentStart = 0;
        int i = 0;

        while (i < sql.Length)
        {
            char c = sqlSpan[i];

            // Skip string literals ('...' with '' escaping)
            if (c == '\'')
            {
                i++;
                while (i < sql.Length)
                {
                    char sc = sqlSpan[i];
                    if (sc == '\'')
                    {
                        i++;
                        if (i < sql.Length && sqlSpan[i] == '\'')
                        {
                            i++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    else
                    {
                        i++;
                    }
                }
                continue;
            }

            // Parameter placeholder
            if (c == '@' && i + 1 < sql.Length && IsIdentStart(sqlSpan[i + 1]))
            {
                int placeholderStart = i;
                i++; // skip @
                int start = i;
                while (i < sql.Length && IsIdentChar(sqlSpan[i]))
                    i++;

                ReadOnlySpan<char> name = sqlSpan[start..i];
                if (!parameters.TryGetValue(name, out object? value))
                    throw new InvalidOperationException($"Parameter '@{name.ToString()}' was not supplied.");

                sb ??= new StringBuilder(sql.Length);
                sb.Append(sql, segmentStart, placeholderStart - segmentStart);
                sb.Append(EscapeValue(value));
                segmentStart = i;

                continue;
            }

            i++;
        }

        if (sb == null)
            return sql;

        if (segmentStart < sql.Length)
            sb.Append(sql, segmentStart, sql.Length - segmentStart);

        return sb.ToString();
    }

    internal static string EscapeValue(object? value)
    {
        if (value is null or DBNull)
            return "NULL";

        return value switch
        {
            long l => l.ToString(CultureInfo.InvariantCulture),
            int iv => iv.ToString(CultureInfo.InvariantCulture),
            short s => s.ToString(CultureInfo.InvariantCulture),
            byte b => b.ToString(CultureInfo.InvariantCulture),
            sbyte sb => sb.ToString(CultureInfo.InvariantCulture),
            uint ui => ui.ToString(CultureInfo.InvariantCulture),
            ushort us => us.ToString(CultureInfo.InvariantCulture),
            ulong ul => ul.ToString(CultureInfo.InvariantCulture),
            bool bv => bv ? "1" : "0",
            double d => FormatReal(d),
            float f => FormatReal(f),
            decimal m => FormatReal((double)m),
            string sv => $"'{sv.Replace("'", "''")}'",
            DateTime dt => $"'{dt.ToString("O", CultureInfo.InvariantCulture)}'",
            Guid g => $"'{g}'",
            byte[] blob => FormatBlob(blob),
            ReadOnlyMemory<byte> blob => FormatBlob(blob.Span),
            _ => $"'{value.ToString()!.Replace("'", "''")}'",
        };
    }

    private static string FormatReal(double d)
    {
        if (double.IsNaN(d) || double.IsInfinity(d))
            throw new InvalidOperationException("Cannot use NaN or Infinity as a parameter value.");

        string s = d.ToString("G", CultureInfo.InvariantCulture);
        if (!s.Contains('.') && !s.Contains('E') && !s.Contains('e'))
            s += ".0";
        return s;
    }

    private static string FormatBlob(ReadOnlySpan<byte> value)
        => $"X'{Convert.ToHexString(value)}'";

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';
    private static bool IsIdentChar(char c) => char.IsLetterOrDigit(c) || c == '_';
}
