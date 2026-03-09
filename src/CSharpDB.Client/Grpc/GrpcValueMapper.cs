using System.Collections;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace CSharpDB.Client.Grpc;

public static class GrpcValueMapper
{
    public static VariantValue ToMessage(object? value)
    {
        var message = new VariantValue();

        switch (value)
        {
            case null:
                message.NullValue = NullValue.NullValue;
                return message;
            case JsonElement element:
                return ToMessage(element);
            case bool boolean:
                message.BoolValue = boolean;
                return message;
            case byte number:
                message.Int64Value = number;
                return message;
            case sbyte number:
                message.Int64Value = number;
                return message;
            case short number:
                message.Int64Value = number;
                return message;
            case ushort number:
                message.Int64Value = number;
                return message;
            case int number:
                message.Int64Value = number;
                return message;
            case uint number:
                message.Int64Value = number;
                return message;
            case long number:
                message.Int64Value = number;
                return message;
            case ulong number when number <= long.MaxValue:
                message.Int64Value = (long)number;
                return message;
            case ulong number:
                message.DoubleValue = number;
                return message;
            case float number:
                message.DoubleValue = number;
                return message;
            case double number:
                message.DoubleValue = number;
                return message;
            case decimal number:
                message.DoubleValue = (double)number;
                return message;
            case string text:
                message.StringValue = text;
                return message;
            case byte[] bytes:
                message.BytesValue = ByteString.CopyFrom(bytes);
                return message;
            case IReadOnlyDictionary<string, object?> dictionary:
                message.ObjectValue = ToObject(dictionary);
                return message;
            case IDictionary<string, object?> dictionary:
                message.ObjectValue = ToObject(dictionary);
                return message;
            case IEnumerable enumerable:
                message.ArrayValue = ToArray(enumerable.Cast<object?>());
                return message;
            default:
                return ToMessage(JsonSerializer.SerializeToElement(value, value.GetType()));
        }
    }

    public static VariantValue ToMessage(JsonElement element)
    {
        var message = new VariantValue();

        switch (element.ValueKind)
        {
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                message.NullValue = NullValue.NullValue;
                break;
            case JsonValueKind.False:
                message.BoolValue = false;
                break;
            case JsonValueKind.True:
                message.BoolValue = true;
                break;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out long integer))
                    message.Int64Value = integer;
                else
                    message.DoubleValue = element.GetDouble();
                break;
            case JsonValueKind.String:
                message.StringValue = element.GetString() ?? string.Empty;
                break;
            case JsonValueKind.Object:
                var objectValue = new VariantObject();
                foreach (JsonProperty property in element.EnumerateObject())
                    objectValue.Fields[property.Name] = ToMessage(property.Value);
                message.ObjectValue = objectValue;
                break;
            case JsonValueKind.Array:
                var arrayValue = new VariantArray();
                foreach (JsonElement item in element.EnumerateArray())
                    arrayValue.Items.Add(ToMessage(item));
                message.ArrayValue = arrayValue;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(element), element.ValueKind, "Unsupported JSON value kind.");
        }

        return message;
    }

    public static VariantObject ToObject(IEnumerable<KeyValuePair<string, object?>> dictionary)
    {
        var message = new VariantObject();
        foreach (KeyValuePair<string, object?> entry in dictionary)
            message.Fields[entry.Key] = ToMessage(entry.Value);
        return message;
    }

    public static VariantArray ToArray(IEnumerable<object?> values)
    {
        var message = new VariantArray();
        foreach (object? value in values)
            message.Items.Add(ToMessage(value));
        return message;
    }

    public static object? FromMessage(VariantValue? value)
    {
        if (value is null)
            return null;

        return value.KindCase switch
        {
            VariantValue.KindOneofCase.NullValue => null,
            VariantValue.KindOneofCase.BoolValue => value.BoolValue,
            VariantValue.KindOneofCase.Int64Value => value.Int64Value,
            VariantValue.KindOneofCase.DoubleValue => value.DoubleValue,
            VariantValue.KindOneofCase.StringValue => value.StringValue,
            VariantValue.KindOneofCase.BytesValue => value.BytesValue.ToByteArray(),
            VariantValue.KindOneofCase.ObjectValue => ToDictionary(value.ObjectValue),
            VariantValue.KindOneofCase.ArrayValue => ToArray(value.ArrayValue),
            VariantValue.KindOneofCase.None => null,
            _ => throw new ArgumentOutOfRangeException(nameof(value.KindCase), value.KindCase, "Unsupported value kind."),
        };
    }

    public static Dictionary<string, object?> ToDictionary(VariantObject? value)
    {
        if (value is null)
            return [];

        return value.Fields.ToDictionary(entry => entry.Key, entry => FromMessage(entry.Value));
    }

    public static object?[] ToArray(VariantArray? value)
    {
        if (value is null)
            return [];

        return value.Items.Select(FromMessage).ToArray();
    }

    public static JsonElement ToJsonElement(VariantValue value)
        => JsonSerializer.SerializeToElement(ToJsonCompatible(value));

    private static object? ToJsonCompatible(VariantValue value)
    {
        return value.KindCase switch
        {
            VariantValue.KindOneofCase.NullValue => null,
            VariantValue.KindOneofCase.BoolValue => value.BoolValue,
            VariantValue.KindOneofCase.Int64Value => value.Int64Value,
            VariantValue.KindOneofCase.DoubleValue => value.DoubleValue,
            VariantValue.KindOneofCase.StringValue => value.StringValue,
            VariantValue.KindOneofCase.ObjectValue => value.ObjectValue.Fields.ToDictionary(
                entry => entry.Key,
                entry => ToJsonCompatible(entry.Value)),
            VariantValue.KindOneofCase.ArrayValue => value.ArrayValue.Items.Select(ToJsonCompatible).ToList(),
            VariantValue.KindOneofCase.BytesValue => throw new InvalidOperationException("Blob values cannot be converted to a JSON document."),
            VariantValue.KindOneofCase.None => null,
            _ => throw new ArgumentOutOfRangeException(nameof(value.KindCase), value.KindCase, "Unsupported value kind."),
        };
    }
}
