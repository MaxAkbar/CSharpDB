using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Pipelines.Serialization;

internal sealed class ObjectDictionaryConverter : JsonConverter<IReadOnlyDictionary<string, object?>>
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
        foreach ((string key, object? item) in value)
        {
            writer.WritePropertyName(key);
            WriteValue(writer, item, options);
        }

        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool boolValue:
                writer.WriteBooleanValue(boolValue);
                break;
            case byte or sbyte or short or ushort or int or uint or long:
                writer.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;
            case float or double or decimal:
                writer.WriteNumberValue(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                break;
            case string text:
                writer.WriteStringValue(text);
                break;
            default:
                JsonSerializer.Serialize(writer, value, options);
                break;
        }
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
            JsonTokenType.Number => ReadNumber(ref reader),
            JsonTokenType.StartObject => ReadDictionary(ref reader),
            JsonTokenType.StartArray => ReadArray(ref reader),
            _ => throw new JsonException($"Unexpected token {reader.TokenType}."),
        };
    }

    private static object ReadNumber(ref Utf8JsonReader reader)
    {
        decimal number = reader.GetDecimal();
        if (number >= long.MinValue && number <= long.MaxValue && decimal.Truncate(number) == number)
            return (long)number;

        return (double)number;
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
