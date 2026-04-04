using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Services;

internal static partial class ReportSql
{
    private static readonly Regex s_identifierPattern = IdentifierPattern();
    private static readonly Regex s_savedQueryParameterPattern = SavedQueryParameterPattern();
    private static readonly Regex s_limitPattern = LimitPattern();

    public static string RequireIdentifier(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (!s_identifierPattern.IsMatch(value))
            throw new InvalidOperationException($"Identifier '{value}' is not supported.");

        return value;
    }

    public static string FormatLiteral(object? value)
    {
        object? normalized = NormalizeValue(value);
        return normalized switch
        {
            null => "NULL",
            long integer => integer.ToString(CultureInfo.InvariantCulture),
            double real => real.ToString(CultureInfo.InvariantCulture),
            string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            byte[] => throw new InvalidOperationException("Blob literals are not supported."),
            _ => $"'{Convert.ToString(normalized, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal) ?? string.Empty}'",
        };
    }

    public static string NormalizeSqlText(string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return sql.Trim().TrimEnd(';').Trim();
    }

    public static bool IsSavedQueryParameterless(string sql)
        => !s_savedQueryParameterPattern.IsMatch(sql);

    public static bool HasLimitClause(string sql)
        => s_limitPattern.IsMatch(sql);

    public static string AppendLimit(string sql, int limit)
    {
        string normalized = NormalizeSqlText(sql);
        if (HasLimitClause(normalized))
            return normalized;

        return $"{normalized}\nLIMIT {limit};";
    }

    public static IReadOnlyList<Dictionary<string, object?>> ReadRows(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        if (result.ColumnNames is null || result.Rows is null)
            return [];

        var rows = new List<Dictionary<string, object?>>(result.Rows.Count);
        foreach (object?[] row in result.Rows)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < result.ColumnNames.Length && i < row.Length; i++)
                dict[result.ColumnNames[i]] = row[i];

            rows.Add(dict);
        }

        return rows;
    }

    public static void ThrowIfError(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    public static object? NormalizeValue(object? value) => value switch
    {
        null => null,
        JsonElement json => NormalizeJsonElement(json),
        bool boolean => boolean ? 1L : 0L,
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        float or double or decimal => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        DateTime dateTime => dateTime,
        DateTimeOffset dateTimeOffset => dateTimeOffset,
        Guid guid => guid.ToString("D"),
        string text => text,
        byte[] blob => blob,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

    public static string ComputeSignature(object payload)
    {
        string json = JsonSerializer.Serialize(payload);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json)));
    }

    public static bool TryConvertToDouble(object? value, out double numeric)
    {
        object? normalized = NormalizeValue(value);
        switch (normalized)
        {
            case null:
                numeric = 0;
                return false;
            case double real:
                numeric = real;
                return true;
            case long integer:
                numeric = integer;
                return true;
            default:
                return double.TryParse(Convert.ToString(normalized, CultureInfo.InvariantCulture), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out numeric);
        }
    }

    public static string FormatDisplayValue(object? value, string? formatString = null)
    {
        object? normalized = NormalizeValue(value);
        if (normalized is null)
            return string.Empty;

        if (normalized is DateTime dateTime)
            return string.IsNullOrWhiteSpace(formatString)
                ? dateTime.ToString("g", CultureInfo.InvariantCulture)
                : dateTime.ToString(formatString, CultureInfo.InvariantCulture);

        if (normalized is DateTimeOffset dateTimeOffset)
            return string.IsNullOrWhiteSpace(formatString)
                ? dateTimeOffset.ToString("g", CultureInfo.InvariantCulture)
                : dateTimeOffset.ToString(formatString, CultureInfo.InvariantCulture);

        if (!string.IsNullOrWhiteSpace(formatString))
        {
            try
            {
                return normalized switch
                {
                    IFormattable formattable => formattable.ToString(formatString, CultureInfo.InvariantCulture),
                    _ => Convert.ToString(normalized, CultureInfo.InvariantCulture) ?? string.Empty,
                };
            }
            catch
            {
            }
        }

        return Convert.ToString(normalized, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static object? NormalizeJsonElement(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.False => 0L,
        JsonValueKind.True => 1L,
        JsonValueKind.Number when value.TryGetInt64(out long integer) => integer,
        JsonValueKind.Number => value.GetDouble(),
        _ => value.GetRawText(),
    };

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex IdentifierPattern();

    [GeneratedRegex("(^|[^A-Za-z0-9_])@[A-Za-z_][A-Za-z0-9_]*", RegexOptions.Compiled)]
    private static partial Regex SavedQueryParameterPattern();

    [GeneratedRegex(@"\bLIMIT\b", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex LimitPattern();
}
