using System.Buffers;
using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

internal static class CollectionIndexedFieldReader
{
    private const int StackallocPropertyNameThreshold = 256;

    public static bool TryReadInt64(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out long value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadInt64(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadInt64(ReadOnlySpan<byte> payload, string jsonPropertyName, out long value)
    {
        ArgumentNullException.ThrowIfNull(jsonPropertyName);
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = default;
                return false;
            }
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                int byteCount = System.Text.Encoding.UTF8.GetByteCount(jsonPropertyName);
                byte[]? rented = null;
                Span<byte> propertyNameUtf8 = byteCount <= StackallocPropertyNameThreshold
                    ? stackalloc byte[StackallocPropertyNameThreshold]
                    : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

                try
                {
                    int written = System.Text.Encoding.UTF8.GetBytes(jsonPropertyName.AsSpan(), propertyNameUtf8);
                    return CollectionBinaryDocumentCodec.TryReadInt64(
                        documentPayload,
                        propertyNameUtf8[..written],
                        out value);
                }
                finally
                {
                    if (rented is not null)
                    {
                        propertyNameUtf8[..byteCount].Clear();
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
            }

            var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName || reader.CurrentDepth != 1)
                    continue;

                if (!reader.ValueTextEquals(jsonPropertyName.AsSpan()))
                    continue;

                if (!reader.Read())
                    break;

                if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out value))
                    return true;

                break;
            }

            value = default;
            return false;
        }
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    public static bool TryReadString(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out string? value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadString(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadStringUtf8(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, out ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryReadStringUtf8(payload, accessor.JsonPathSegmentsUtf8, out value);
    }

    public static bool TryReadString(ReadOnlySpan<byte> payload, string jsonPropertyName, out string? value)
    {
        ArgumentNullException.ThrowIfNull(jsonPropertyName);
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = null;
                return false;
            }
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                int byteCount = System.Text.Encoding.UTF8.GetByteCount(jsonPropertyName);
                byte[]? rented = null;
                Span<byte> propertyNameUtf8 = byteCount <= StackallocPropertyNameThreshold
                    ? stackalloc byte[StackallocPropertyNameThreshold]
                    : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

                try
                {
                    int written = System.Text.Encoding.UTF8.GetBytes(jsonPropertyName.AsSpan(), propertyNameUtf8);
                    return CollectionBinaryDocumentCodec.TryReadString(
                        documentPayload,
                        propertyNameUtf8[..written],
                        out value);
                }
                finally
                {
                    if (rented is not null)
                    {
                        propertyNameUtf8[..byteCount].Clear();
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
            }

            var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName || reader.CurrentDepth != 1)
                    continue;

                if (!reader.ValueTextEquals(jsonPropertyName.AsSpan()))
                    continue;

                if (!reader.Read())
                    break;

                if (reader.TokenType == JsonTokenType.String)
                {
                    value = reader.GetString();
                    return true;
                }

                break;
            }

            value = null;
            return false;
        }
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = null;
            return false;
        }
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

    public static bool TryReadIndexValues(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, List<DbValue> values)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        ArgumentNullException.ThrowIfNull(values);
        return TryReadIndexValues(payload, accessor.JsonPathSegmentsUtf8, accessor.JsonPathArraySegments, values);
    }

    public static bool TryArrayContainsValue(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, DbValue expectedValue)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        return TryArrayContainsValue(payload, accessor.JsonPathSegmentsUtf8, accessor.JsonPathArraySegments, expectedValue);
    }

    public static bool TryReadValue(ReadOnlySpan<byte> payload, string jsonPropertyName, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                int byteCount = System.Text.Encoding.UTF8.GetByteCount(jsonPropertyName);
                byte[]? rented = null;
                Span<byte> propertyNameUtf8 = byteCount <= StackallocPropertyNameThreshold
                    ? stackalloc byte[StackallocPropertyNameThreshold]
                    : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

                try
                {
                    int written = System.Text.Encoding.UTF8.GetBytes(jsonPropertyName.AsSpan(), propertyNameUtf8);
                    return CollectionBinaryDocumentCodec.TryReadValue(
                        documentPayload,
                        propertyNameUtf8[..written],
                        out value);
                }
                finally
                {
                    if (rented is not null)
                    {
                        propertyNameUtf8[..byteCount].Clear();
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    public static bool TryTextEquals(ReadOnlySpan<byte> payload, CollectionFieldAccessor accessor, string expectedValue)
    {
        ArgumentNullException.ThrowIfNull(accessor);
        ArgumentNullException.ThrowIfNull(expectedValue);
        return TryTextEquals(payload, accessor.JsonPathSegmentsUtf8, expectedValue);
    }

    public static bool TryTextEquals(ReadOnlySpan<byte> payload, string jsonPropertyName, string expectedValue)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                int byteCount = System.Text.Encoding.UTF8.GetByteCount(jsonPropertyName);
                byte[]? rented = null;
                Span<byte> propertyNameUtf8 = byteCount <= StackallocPropertyNameThreshold
                    ? stackalloc byte[StackallocPropertyNameThreshold]
                    : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

                try
                {
                    int written = System.Text.Encoding.UTF8.GetBytes(jsonPropertyName.AsSpan(), propertyNameUtf8);
                    return CollectionBinaryDocumentCodec.TryTextEquals(
                        documentPayload,
                        propertyNameUtf8[..written],
                        expectedValue);
                }
                finally
                {
                    if (rented is not null)
                    {
                        propertyNameUtf8[..byteCount].Clear();
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            return false;
        }
    }

    private static bool TryReadInt64(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out long value)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = default;
                return false;
            }
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    private static bool TryReadString(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out string? value)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = null;
                return false;
            }
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = null;
            return false;
        }
    }

    private static bool TryReadStringUtf8(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out ReadOnlySpan<byte> value)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = default;
                return false;
            }
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
                return CollectionBinaryDocumentCodec.TryReadStringUtf8(documentPayload, jsonPathSegmentsUtf8, out value);

            value = default;
            return false;
        }
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    private static bool TryReadBoolean(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out bool value)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = default;
                return false;
            }
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    private static bool TryReadDecimal(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out decimal value)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                value = default;
                return false;
            }
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    private static bool TryReadValue(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, out DbValue value)
    {
        value = default;
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            value = default;
            return false;
        }
    }

    private static bool TryTextEquals(ReadOnlySpan<byte> payload, byte[][] jsonPathSegmentsUtf8, string expectedValue)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
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
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            return false;
        }
    }

    private static bool TryReadIndexValues(
        ReadOnlySpan<byte> payload,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        List<DbValue> values)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                return CollectionBinaryDocumentCodec.TryReadArrayValues(
                    documentPayload,
                    jsonPathSegmentsUtf8,
                    jsonPathArraySegments,
                    values);
            }

            var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return false;

            return TryReadIndexValuesFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, 0, values);
        }
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            return false;
        }
    }

    private static bool TryArrayContainsValue(
        ReadOnlySpan<byte> payload,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        DbValue expectedValue)
    {
        if (!CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            if (!CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
                return false;
        }

        try
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
            {
                return CollectionBinaryDocumentCodec.TryArrayContainsValue(
                    documentPayload,
                    jsonPathSegmentsUtf8,
                    jsonPathArraySegments,
                    expectedValue);
            }

            var reader = new Utf8JsonReader(documentPayload, isFinalBlock: true, state: default);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return false;

            return TryArrayContainsValueFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, 0, expectedValue);
        }
        catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
        {
            return false;
        }
    }

    private static bool IsFastHeaderFallbackCandidate(Exception ex)
        => ex is CSharpDbException or JsonException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException;

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

    private static bool TryReadIndexValuesFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        int pathIndex,
        List<DbValue> values)
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

            if (jsonPathArraySegments[pathIndex])
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    break;

                if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
                    return TryCollectScalarArrayValues(ref reader, values);

                return TryReadIndexValuesFromArray(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex + 1, values);
            }

            if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
            {
                if (TryConvertCurrentTokenToDbValue(ref reader, out DbValue value) &&
                    value.Type is DbType.Integer or DbType.Text)
                {
                    values.Add(value);
                    return true;
                }

                return false;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryReadIndexValuesFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex + 1, values);
        }

        return false;
    }

    private static bool TryReadIndexValuesFromArray(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        int pathIndex,
        List<DbValue> values)
    {
        bool foundAny = false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return foundAny;

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                if (!SkipValue(ref reader))
                    return false;

                continue;
            }

            if (TryReadIndexValuesFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex, values))
                foundAny = true;
        }

        return false;
    }

    private static bool TryArrayContainsValueFromObject(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        int pathIndex,
        DbValue expectedValue)
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

            if (jsonPathArraySegments[pathIndex])
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    break;

                if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
                    return TryScalarArrayContainsValue(ref reader, expectedValue);

                return TryArrayContainsValueFromArray(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex + 1, expectedValue);
            }

            if (pathIndex == jsonPathSegmentsUtf8.Length - 1)
            {
                return TryConvertCurrentTokenToDbValue(ref reader, out DbValue value) &&
                       value.Type == expectedValue.Type &&
                       DbValue.Compare(value, expectedValue) == 0;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
                break;

            return TryArrayContainsValueFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex + 1, expectedValue);
        }

        return false;
    }

    private static bool TryArrayContainsValueFromArray(
        ref Utf8JsonReader reader,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        int pathIndex,
        DbValue expectedValue)
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return false;

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                if (!SkipValue(ref reader))
                    return false;

                continue;
            }

            if (TryArrayContainsValueFromObject(ref reader, jsonPathSegmentsUtf8, jsonPathArraySegments, pathIndex, expectedValue))
                return true;
        }

        return false;
    }

    private static bool TryCollectScalarArrayValues(ref Utf8JsonReader reader, List<DbValue> values)
    {
        bool foundAny = false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return foundAny;

            if (TryConvertCurrentTokenToDbValue(ref reader, out DbValue value) &&
                value.Type is DbType.Integer or DbType.Text)
            {
                values.Add(value);
                foundAny = true;
                continue;
            }

            if (!SkipValue(ref reader))
                return false;
        }

        return false;
    }

    private static bool TryScalarArrayContainsValue(ref Utf8JsonReader reader, DbValue expectedValue)
    {
        if (expectedValue.Type is not (DbType.Integer or DbType.Text))
            return false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return false;

            if (TryConvertCurrentTokenToDbValue(ref reader, out DbValue value) &&
                value.Type == expectedValue.Type &&
                DbValue.Compare(value, expectedValue) == 0)
            {
                return true;
            }

            if (!SkipValue(ref reader))
                return false;
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
