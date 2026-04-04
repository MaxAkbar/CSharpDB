using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Admin.Reports.Serialization;

public sealed class ObjectDictionaryConverter : JsonConverter<IReadOnlyDictionary<string, object?>>
{
    public override IReadOnlyDictionary<string, object?> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject.");

        return ReadDictionary(ref reader);
    }

    public override void Write(Utf8JsonWriter writer, IReadOnlyDictionary<string, object?> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        foreach (var kvp in value)
        {
            writer.WritePropertyName(kvp.Key);
            JsonSerializer.Serialize(writer, kvp.Value, options);
        }

        writer.WriteEndObject();
    }

    private static Dictionary<string, object?> ReadDictionary(ref Utf8JsonReader reader)
    {
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dict;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName.");

            string key = reader.GetString() ?? string.Empty;
            reader.Read();
            dict[key] = ReadValue(ref reader);
        }

        throw new JsonException("Unexpected end of JSON.");
    }

    private static object? ReadValue(ref Utf8JsonReader reader)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => null,
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.Number => reader.TryGetInt64(out long integer) ? integer : reader.GetDouble(),
            JsonTokenType.StartObject => ReadDictionary(ref reader),
            JsonTokenType.StartArray => ReadArray(ref reader),
            _ => throw new JsonException($"Unexpected token {reader.TokenType}."),
        };
    }

    private static object?[] ReadArray(ref Utf8JsonReader reader)
    {
        var values = new List<object?>();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return values.ToArray();

            values.Add(ReadValue(ref reader));
        }

        throw new JsonException("Unexpected end of JSON in array.");
    }
}
