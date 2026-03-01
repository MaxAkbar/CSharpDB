using System.Text.Json;

namespace CSharpDB.Api.Helpers;

/// <summary>
/// Converts <see cref="JsonElement"/> values that arrive from System.Text.Json deserialization
/// into CLR primitives that the CSharpDB engine understands.
/// </summary>
public static class JsonHelper
{
    public static object? CoerceJsonElement(object? value) => value switch
    {
        JsonElement { ValueKind: JsonValueKind.Null } => null,
        JsonElement { ValueKind: JsonValueKind.String } e => e.GetString(),
        JsonElement { ValueKind: JsonValueKind.Number } e => e.TryGetInt64(out long l) ? (object)l : e.GetDouble(),
        JsonElement { ValueKind: JsonValueKind.True } => 1L,
        JsonElement { ValueKind: JsonValueKind.False } => 0L,
        _ => value,
    };

    /// <summary>
    /// Coerces all values in a dictionary from JsonElement to CLR types.
    /// </summary>
    public static Dictionary<string, object?> CoerceDictionary(Dictionary<string, object?> dict)
    {
        var result = new Dictionary<string, object?>(dict.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, val) in dict)
            result[key] = CoerceJsonElement(val);
        return result;
    }

    /// <summary>
    /// Converts positional row data (object?[]) into named dictionaries using column names.
    /// </summary>
    public static List<Dictionary<string, object?>> RowsToNamedDictionaries(
        string[] columnNames, IReadOnlyList<object?[]> rows)
    {
        var result = new List<Dictionary<string, object?>>(rows.Count);
        foreach (var row in rows)
        {
            var dict = new Dictionary<string, object?>(columnNames.Length);
            for (int i = 0; i < columnNames.Length; i++)
                dict[columnNames[i]] = i < row.Length ? row[i] : null;
            result.Add(dict);
        }
        return result;
    }
}
