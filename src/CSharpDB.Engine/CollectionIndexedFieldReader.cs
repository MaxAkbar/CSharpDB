using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

internal static class CollectionIndexedFieldReader
{
    public static bool TryReadValue(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out DbValue value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadValue(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

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

    public static bool TryTextEquals(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, string expectedValue)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        ArgumentNullException.ThrowIfNull(expectedValue);
        return TryTextEquals(payload, accessor.JsonPathSegmentsUtf8, expectedValue);
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

    private static bool TryReadValue(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.IsDirectPayload(payload))
            return false;

        var reader = new Utf8JsonReader(CollectionPayloadCodec.GetJsonUtf8(payload), isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return false;

        return TryReadValueFromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryTextEquals(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, string expectedValue)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(payload))
            return false;

        var reader = new Utf8JsonReader(CollectionPayloadCodec.GetJsonUtf8(payload), isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return false;

        return TryTextEqualsFromObject(ref reader, jsonPathSegmentsUtf8, 0, expectedValue);
    }

    private static bool TryReadValueFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        out DbValue value)
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            bool matches = reader.ValueTextEquals(jsonPathSegmentsUtf8[pathIndex]);
            if (!reader.Read())
                break;

            if (!matches)
            {
                if (!SkipValue(ref reader))
                    break;

                continue;
            }

            if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
                return TryConvertCurrentTokenToDbValue(ref reader, out value);

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadValueFromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, out value);
        }

        value = default;
        return false;
    }

    private static bool TryTextEqualsFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        string expectedValue)
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            bool matches = reader.ValueTextEquals(jsonPathSegmentsUtf8[pathIndex]);
            if (!reader.Read())
                break;

            if (!matches)
            {
                if (!SkipValue(ref reader))
                    break;

                continue;
            }

            if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
            {
                return reader.TokenType == JsonTokenType.String &&
                       reader.ValueTextEquals(expectedValue.AsSpan());
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryTextEqualsFromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, expectedValue);
        }

        return false;
    }

    private static bool TryConvertCurrentTokenToDbValue(ref Utf8JsonReader reader, out DbValue value)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                value = DbValue.FromText(reader.GetString()!);
                return true;
            case JsonTokenType.Number when reader.TryGetInt64(out long integerValue):
                value = DbValue.FromInteger(integerValue);
                return true;
            default:
                value = default;
                return false;
        }
    }

    private static bool SkipValue(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.StartObject &&
            reader.TokenType != JsonTokenType.StartArray)
        {
            return true;
        }

        int containerDepth = reader.CurrentDepth;
        while (reader.Read())
        {
            if (reader.CurrentDepth == containerDepth &&
                (reader.TokenType == JsonTokenType.EndObject || reader.TokenType == JsonTokenType.EndArray))
            {
                return true;
            }
        }

        return false;
    }
}
