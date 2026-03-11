using System.Text.Json;
using CSharpDB.Core;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

internal static class CollectionIndexedFieldReader
{
    public static bool TryReadValue(ReadOnlySpan<byte> payload, string jsonPropertyName, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.IsDirectPayload(payload))
            return false;

        var reader = new Utf8JsonReader(CollectionPayloadCodec.GetJsonUtf8(payload), isFinalBlock: true, state: default);
        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName || reader.CurrentDepth != 1)
                continue;

            if (!reader.ValueTextEquals(jsonPropertyName.AsSpan()))
                continue;

            if (!reader.Read())
                return false;

            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    value = DbValue.FromText(reader.GetString()!);
                    return true;
                case JsonTokenType.Number when reader.TryGetInt64(out long integerValue):
                    value = DbValue.FromInteger(integerValue);
                    return true;
                default:
                    return false;
            }
        }

        return false;
    }

    public static bool TryTextEquals(ReadOnlySpan<byte> payload, string jsonPropertyName, string expectedValue)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(payload))
            return false;

        var reader = new Utf8JsonReader(CollectionPayloadCodec.GetJsonUtf8(payload), isFinalBlock: true, state: default);
        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName || reader.CurrentDepth != 1)
                continue;

            if (!reader.ValueTextEquals(jsonPropertyName.AsSpan()))
                continue;

            if (!reader.Read())
                return false;

            return reader.TokenType == JsonTokenType.String &&
                   reader.ValueTextEquals(expectedValue.AsSpan());
        }

        return false;
    }
}
