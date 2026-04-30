using System.Text.Json;

namespace CSharpDB.Admin.Helpers;

public static class CollectionJsonFormatter
{
    private static readonly JsonSerializerOptions s_indentedOptions = new()
    {
        WriteIndented = true,
    };

    public static string Format(JsonElement document)
        => JsonSerializer.Serialize(document, s_indentedOptions);

    public static bool TryFormat(string json, out string formatted, out string? error)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            formatted = Format(document.RootElement);
            error = null;
            return true;
        }
        catch (JsonException ex)
        {
            formatted = json;
            error = ex.Message;
            return false;
        }
    }

    public static bool TryClone(string json, out JsonElement document, out string? error)
    {
        try
        {
            using var parsed = JsonDocument.Parse(json);
            document = parsed.RootElement.Clone();
            error = null;
            return true;
        }
        catch (JsonException ex)
        {
            document = default;
            error = ex.Message;
            return false;
        }
    }

    public static string GetKindLabel(JsonElement document)
        => document.ValueKind switch
        {
            JsonValueKind.Object => "object",
            JsonValueKind.Array => "array",
            JsonValueKind.String => "string",
            JsonValueKind.Number => "number",
            JsonValueKind.True => "boolean",
            JsonValueKind.False => "boolean",
            JsonValueKind.Null => "null",
            _ => "value",
        };

    public static string GetPreview(JsonElement document, int maxLength = 120)
    {
        string preview = document.ValueKind switch
        {
            JsonValueKind.Object => BuildObjectPreview(document),
            JsonValueKind.Array => BuildArrayPreview(document),
            JsonValueKind.String => document.GetString() ?? string.Empty,
            JsonValueKind.Number => document.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => document.GetRawText(),
        };

        preview = NormalizeWhitespace(preview);
        if (preview.Length <= maxLength)
            return preview;

        return maxLength <= 3
            ? preview[..maxLength]
            : string.Concat(preview.AsSpan(0, maxLength - 3), "...");
    }

    private static string BuildObjectPreview(JsonElement document)
    {
        var parts = new List<string>();
        foreach (var property in document.EnumerateObject().Take(4))
            parts.Add($"{property.Name}: {GetScalarPreview(property.Value)}");

        return parts.Count == 0
            ? "{}"
            : string.Join(", ", parts);
    }

    private static string BuildArrayPreview(JsonElement document)
    {
        int count = 0;
        var parts = new List<string>();
        foreach (var item in document.EnumerateArray())
        {
            count++;
            if (parts.Count < 4)
                parts.Add(GetScalarPreview(item));
        }

        return count == 0
            ? "[]"
            : $"{count} item{(count == 1 ? string.Empty : "s")}: {string.Join(", ", parts)}";
    }

    private static string GetScalarPreview(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Object => "{...}",
            JsonValueKind.Array => "[...]",
            JsonValueKind.String => value.GetString() ?? string.Empty,
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => value.GetRawText(),
        };

    private static string NormalizeWhitespace(string value)
        => string.Join(" ", value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
}
