using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Forms.Services;

internal static partial class FormSql
{
    private static readonly Regex s_identifierPattern = IdentifierPattern();

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
            byte[] blob => $"X'{Convert.ToHexString(blob)}'",
            _ => $"'{Convert.ToString(normalized, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal) ?? string.Empty}'",
        };
    }

    public static string EscapeLikePattern(string value)
        => value
            .Replace("!", "!!", StringComparison.Ordinal)
            .Replace("%", "!%", StringComparison.Ordinal)
            .Replace("_", "!_", StringComparison.Ordinal);

    public static IReadOnlyList<Dictionary<string, object?>> ReadRows(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        if (result.ColumnNames is null || result.Rows is null)
            return [];

        var rows = new List<Dictionary<string, object?>>(result.Rows.Count);
        foreach (var row in result.Rows)
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
        DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
        Guid guid => guid.ToString("D"),
        string text => text,
        byte[] blob => blob,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

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
}
