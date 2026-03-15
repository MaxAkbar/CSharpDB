using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

internal static class CollectionIndexedFieldReader
{
    public static bool TryReadInt64(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out long value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadInt64(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadInt64(ReadOnlySpan<byte> payload, string jsonPropertyName, out long value)
    {
        ArgumentNullException.ThrowIfNull(jsonPropertyName);
        byte[][] pathSegments = [System.Text.Encoding.UTF8.GetBytes(jsonPropertyName)];
        return TryReadInt64(payload, pathSegments, out value);
    }

    public static bool TryReadString(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out string? value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadString(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadString(ReadOnlySpan<byte> payload, string jsonPropertyName, out string? value)
    {
        ArgumentNullException.ThrowIfNull(jsonPropertyName);
        byte[][] pathSegments = [System.Text.Encoding.UTF8.GetBytes(jsonPropertyName)];
        return TryReadString(payload, pathSegments, out value);
    }

    public static bool TryReadBoolean(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out bool value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadBoolean(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadDecimal(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out decimal value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadDecimal(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadValue(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out DbValue value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadValue(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadValue(ReadOnlySpan<byte> payload, string jsonPropertyName, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
            return false;

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            byte[][] pathSegments = [System.Text.Encoding.UTF8.GetBytes(jsonPropertyName)];
            return CollectionBinaryDocumentCodec.TryReadValue(
                documentPayload,
                pathSegments,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
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
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
            return false;

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            byte[][] pathSegments = [System.Text.Encoding.UTF8.GetBytes(jsonPropertyName)];
            return CollectionBinaryDocumentCodec.TryTextEquals(
                documentPayload,
                pathSegments,
                expectedValue);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
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

    private static bool TryReadInt64(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out long value)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            value = default;
            return false;
        }

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryReadInt64(
                documentPayload,
                jsonPathSegmentsUtf8,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            value = default;
            return false;
        }

        return TryReadInt64FromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryReadString(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out string? value)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            value = null;
            return false;
        }

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryReadString(
                documentPayload,
                jsonPathSegmentsUtf8,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            value = null;
            return false;
        }

        return TryReadStringFromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryReadBoolean(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out bool value)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            value = default;
            return false;
        }

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryReadBoolean(
                documentPayload,
                jsonPathSegmentsUtf8,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            value = default;
            return false;
        }

        return TryReadBooleanFromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryReadDecimal(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out decimal value)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            value = default;
            return false;
        }

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryReadDecimal(
                documentPayload,
                jsonPathSegmentsUtf8,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            value = default;
            return false;
        }

        return TryReadDecimalFromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryReadValue(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
            return false;

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryReadValue(
                documentPayload,
                jsonPathSegmentsUtf8,
                out value);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return false;

        return TryReadValueFromObject(ref reader, jsonPathSegmentsUtf8, 0, out value);
    }

    private static bool TryTextEquals(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, string expectedValue)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
            return false;

        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
        if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            return CollectionBinaryDocumentCodec.TryTextEquals(
                documentPayload,
                jsonPathSegmentsUtf8,
                expectedValue);
        }

        var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
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

    private static bool TryReadInt64FromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        out long value)
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
                if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out value))
                    return true;

                break;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadInt64FromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, out value);
        }

        value = default;
        return false;
    }

    private static bool TryReadStringFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        out string? value)
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
                if (reader.TokenType == JsonTokenType.String)
                {
                    value = reader.GetString();
                    return true;
                }

                break;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadStringFromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, out value);
        }

        value = null;
        return false;
    }

    private static bool TryReadBooleanFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        out bool value)
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
                if (reader.TokenType == JsonTokenType.True)
                {
                    value = true;
                    return true;
                }

                if (reader.TokenType == JsonTokenType.False)
                {
                    value = false;
                    return true;
                }

                break;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadBooleanFromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, out value);
        }

        value = default;
        return false;
    }

    private static bool TryReadDecimalFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        int pathIndex,
        out decimal value)
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
                if (reader.TokenType == JsonTokenType.Number && reader.TryGetDecimal(out value))
                    return true;

                break;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadDecimalFromObject(ref reader, jsonPathSegmentsUtf8, pathIndex + 1, out value);
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
