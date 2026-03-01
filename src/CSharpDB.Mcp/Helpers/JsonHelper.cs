using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Mcp.Helpers;

internal static class JsonHelper
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>Serialize an object to a JSON string.</summary>
    public static string Serialize(object? value) =>
        JsonSerializer.Serialize(value, s_options);

    /// <summary>
    /// Parse a JSON object string into a <see cref="Dictionary{TKey, TValue}"/>
    /// with values coerced to types that CSharpDB expects (long, double, string, null).
    /// </summary>
    public static Dictionary<string, object?> ParseAndCoerceValues(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            result[prop.Name] = CoerceValue(prop.Value);
        }

        return result;
    }

    private static object? CoerceValue(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => element.GetString(),
        JsonValueKind.True => 1L,   // CSharpDB stores booleans as INTEGER
        JsonValueKind.False => 0L,
        JsonValueKind.Number => element.TryGetInt64(out long l) ? l : element.GetDouble(),
        _ => element.GetRawText(),
    };
}
