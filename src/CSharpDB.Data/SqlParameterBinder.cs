using System.Globalization;
using System.Text;
using CSharpDB.Primitives;
using SysDbType = System.Data.DbType;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;

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
                if (!parameters.TryGetParameter(name, out CSharpDbParameter? parameter))
                    throw new InvalidOperationException($"Parameter '@{name.ToString()}' was not supplied.");

                sb ??= new StringBuilder(sql.Length);
                sb.Append(sql, segmentStart, placeholderStart - segmentStart);
                sb.Append(EscapeParameter(parameter));
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
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            string sv => $"'{sv.Replace("'", "''")}'",
            char character => $"'{character.ToString().Replace("'", "''")}'",
            Guid g => QuoteText(CSharpDB.Primitives.CSharpDbTextCodec.FormatGuid(g)),
            DateOnly date => QuoteText(CSharpDB.Primitives.CSharpDbTextCodec.FormatDate(date)),
            TimeOnly time => QuoteText(CSharpDB.Primitives.CSharpDbTextCodec.FormatTime(time)),
            DateTime dt => QuoteText(CSharpDB.Primitives.CSharpDbTextCodec.FormatDateTime(dt)),
            DateTimeOffset dateTimeOffset => QuoteText(
                CSharpDB.Primitives.CSharpDbTextCodec.FormatDateTimeOffset(dateTimeOffset)),
            SqlBitString bits => $"B'{bits.ToBitString()}'",
            byte[] blob => FormatBlob(blob),
            ReadOnlyMemory<byte> blob => FormatBlob(blob.Span),
            _ => $"'{value.ToString()!.Replace("'", "''")}'",
        };
    }

    private static string EscapeParameter(CSharpDbParameter parameter)
    {
        object? value = parameter.Value;
        if (value is null or DBNull)
            return "NULL";

        if (TryFormatLogicalParameter(parameter, value, out string? logicalLiteral))
            return logicalLiteral!;

        if (parameter.DbType != SysDbType.Decimal && value is not decimal)
            return EscapeValue(value);

        decimal decimalValue;
        try
        {
            decimalValue = value is decimal exact
                ? exact
                : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            throw new InvalidOperationException(
                $"Parameter '{parameter.ParameterName}' cannot be represented as DECIMAL.",
                ex);
        }

        int derivedScale = (decimal.GetBits(decimalValue)[3] >> 16) & 0x7F;
        int? requestedPrecision = parameter.Precision > 0 ? parameter.Precision : null;
        int? requestedScale = requestedPrecision.HasValue ? parameter.Scale : derivedScale;
        (int precision, int scale) = CSharpDbDecimalCodec.ResolveFacets(
            requestedPrecision,
            requestedScale);
        string literal = decimalValue.ToString(CultureInfo.InvariantCulture);
        return $"CAST({literal} AS DECIMAL({precision},{scale}))";
    }

    private static bool TryFormatLogicalParameter(
        CSharpDbParameter parameter,
        object value,
        out string? literal)
    {
        if (parameter.DbType == SysDbType.Boolean || value is bool)
        {
            bool boolean;
            try
            {
                boolean = value switch
                {
                    bool exact => exact,
                    double number when double.IsFinite(number) => number != 0d,
                    float number when float.IsFinite(number) => number != 0f,
                    double or float => throw new InvalidCastException(
                        "BOOLEAN parameters require a finite numeric value."),
                    sbyte or byte or short or ushort or int or uint or long or ulong or decimal =>
                        Convert.ToDecimal(value, CultureInfo.InvariantCulture) != decimal.Zero,
                    _ => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
                };
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw new InvalidOperationException(
                    $"Parameter '{parameter.ParameterName}' cannot be represented as BOOLEAN.",
                    ex);
            }

            literal = $"CAST({(boolean ? 1 : 0)} AS BOOLEAN)";
            return true;
        }

        if (parameter.DbType == SysDbType.Guid || value is Guid)
        {
            Guid guid = value is Guid exact
                ? exact
                : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!);
            literal = $"CAST({QuoteText(CSharpDbTextCodec.FormatGuid(guid))} AS UUID)";
            return true;
        }

        if (parameter.DbType == SysDbType.Date || value is DateOnly)
        {
            DateOnly date = value switch
            {
                DateOnly exact => exact,
                DateTime dateTime => DateOnly.FromDateTime(dateTime),
                _ => DateOnly.Parse(
                    Convert.ToString(value, CultureInfo.InvariantCulture)!,
                    CultureInfo.InvariantCulture),
            };
            literal = $"CAST({QuoteText(CSharpDbTextCodec.FormatDate(date))} AS DATE)";
            return true;
        }

        if (parameter.DbType == SysDbType.Time || value is TimeOnly)
        {
            TimeOnly time = value switch
            {
                TimeOnly exact => exact,
                DateTime dateTime => TimeOnly.FromDateTime(dateTime),
                _ => TimeOnly.Parse(
                    Convert.ToString(value, CultureInfo.InvariantCulture)!,
                    CultureInfo.InvariantCulture),
            };
            literal = $"CAST({QuoteText(CSharpDbTextCodec.FormatTime(time))} AS TIME)";
            return true;
        }

        if (parameter.DbType == SysDbType.DateTimeOffset || value is DateTimeOffset)
        {
            DateTimeOffset timestamp = value is DateTimeOffset exact
                ? exact
                : DateTimeOffset.Parse(
                    Convert.ToString(value, CultureInfo.InvariantCulture)!,
                    CultureInfo.InvariantCulture);
            literal = $"CAST({QuoteText(CSharpDbTextCodec.FormatDateTimeOffset(timestamp))} AS DATETIMEOFFSET)";
            return true;
        }

        if (parameter.DbType is SysDbType.DateTime or SysDbType.DateTime2 || value is DateTime)
        {
            DateTime timestamp = value is DateTime exact
                ? exact
                : DateTime.Parse(
                    Convert.ToString(value, CultureInfo.InvariantCulture)!,
                    CultureInfo.InvariantCulture);
            literal = $"CAST({QuoteText(CSharpDbTextCodec.FormatDateTime(timestamp))} AS DATETIME2)";
            return true;
        }

        literal = null;
        return false;
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

    private static string QuoteText(string value)
        => $"'{value.Replace("'", "''")}'";

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';
    private static bool IsIdentChar(char c) => char.IsLetterOrDigit(c) || c == '_';
}
