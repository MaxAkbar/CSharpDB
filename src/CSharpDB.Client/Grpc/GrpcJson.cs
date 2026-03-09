using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Client.Grpc;

public static class GrpcJson
{
    public static JsonSerializerOptions SerializerOptions { get; } = CreateSerializerOptions();

    public static string Serialize<T>(T value)
        => JsonSerializer.Serialize(value, SerializerOptions);

    public static string SerializeObject(object value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return JsonSerializer.Serialize(value, value.GetType(), SerializerOptions);
    }

    public static T? Deserialize<T>(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
            return default;

        return JsonSerializer.Deserialize<T>(payloadJson, SerializerOptions);
    }

    public static T DeserializeRequired<T>(string? payloadJson)
    {
        T? value = Deserialize<T>(payloadJson);
        return value is null
            ? throw new InvalidOperationException($"Expected a '{typeof(T).Name}' payload.")
            : value;
    }

    private static JsonSerializerOptions CreateSerializerOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
        };

        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        options.Converters.Add(new InferredObjectJsonConverter());
        return options;
    }

    private sealed class InferredObjectJsonConverter : JsonConverter<object?>
    {
        private const string BlobTypeName = "blob";
        private const string BooleanTypeName = "bool";
        private const string IntegerTypeName = "int64";
        private const string JsonTypeName = "json";
        private const string NumberTypeName = "double";
        private const string StringTypeName = "string";
        private const string TypePropertyName = "$type";
        private const string Base64PropertyName = "base64";
        private const string ValuePropertyName = "value";

        public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType == JsonTokenType.True)
                return true;

            if (reader.TokenType == JsonTokenType.False)
                return false;

            if (reader.TokenType == JsonTokenType.Number)
                return reader.TryGetInt64(out long integer) ? integer : reader.GetDouble();

            if (reader.TokenType == JsonTokenType.String)
                return reader.GetString();

            using JsonDocument document = JsonDocument.ParseValue(ref reader);
            JsonElement element = document.RootElement.Clone();
            return TryReadEnvelope(element, out object? value)
                ? value
                : element;
        }

        public override void Write(Utf8JsonWriter writer, object? value, JsonSerializerOptions options)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    return;
                case JsonElement element:
                    WriteJsonEnvelope(writer, element);
                    return;
                case byte[] blob:
                    WriteBlobEnvelope(writer, blob);
                    return;
                case bool boolean:
                    WriteValueEnvelope(writer, BooleanTypeName, boolean);
                    return;
                case byte number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case sbyte number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case short number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case ushort number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case int number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case uint number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case long number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case ulong number:
                    WriteValueEnvelope(writer, IntegerTypeName, number);
                    return;
                case float number:
                    WriteValueEnvelope(writer, NumberTypeName, number);
                    return;
                case double number:
                    WriteValueEnvelope(writer, NumberTypeName, number);
                    return;
                case decimal number:
                    WriteValueEnvelope(writer, NumberTypeName, number);
                    return;
                case string text:
                    WriteValueEnvelope(writer, StringTypeName, text);
                    return;
                default:
                    WriteJsonEnvelope(writer, JsonSerializer.SerializeToElement(value, value.GetType(), options));
                    return;
            }
        }

        private static bool TryReadEnvelope(JsonElement element, out object? value)
        {
            if (element.ValueKind != JsonValueKind.Object
                || !element.TryGetProperty(TypePropertyName, out JsonElement typeElement)
                || typeElement.ValueKind != JsonValueKind.String)
            {
                value = null;
                return false;
            }

            string? typeName = typeElement.GetString();
            switch (typeName)
            {
                case BlobTypeName when element.TryGetProperty(Base64PropertyName, out JsonElement base64Element)
                    && base64Element.ValueKind == JsonValueKind.String:
                    value = Convert.FromBase64String(base64Element.GetString()!);
                    return true;
                case BooleanTypeName when element.TryGetProperty(ValuePropertyName, out JsonElement boolElement):
                    value = boolElement.GetBoolean();
                    return true;
                case IntegerTypeName when element.TryGetProperty(ValuePropertyName, out JsonElement intElement):
                    value = intElement.GetInt64();
                    return true;
                case NumberTypeName when element.TryGetProperty(ValuePropertyName, out JsonElement doubleElement):
                    value = doubleElement.GetDouble();
                    return true;
                case StringTypeName when element.TryGetProperty(ValuePropertyName, out JsonElement stringElement):
                    value = stringElement.GetString();
                    return true;
                case JsonTypeName when element.TryGetProperty(ValuePropertyName, out JsonElement jsonElement):
                    value = jsonElement.Clone();
                    return true;
                default:
                    value = null;
                    return false;
            }
        }

        private static void WriteBlobEnvelope(Utf8JsonWriter writer, byte[] blob)
        {
            writer.WriteStartObject();
            writer.WriteString(TypePropertyName, BlobTypeName);
            writer.WriteString(Base64PropertyName, Convert.ToBase64String(blob));
            writer.WriteEndObject();
        }

        private static void WriteValueEnvelope<T>(Utf8JsonWriter writer, string typeName, T value)
        {
            writer.WriteStartObject();
            writer.WriteString(TypePropertyName, typeName);
            writer.WritePropertyName(ValuePropertyName);
            JsonSerializer.Serialize(writer, value, SerializerOptions);
            writer.WriteEndObject();
        }

        private static void WriteJsonEnvelope(Utf8JsonWriter writer, JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Undefined)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStartObject();
            writer.WriteString(TypePropertyName, JsonTypeName);
            writer.WritePropertyName(ValuePropertyName);
            element.WriteTo(writer);
            writer.WriteEndObject();
        }
    }
}
