using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Engine;

internal sealed class CollectionDocumentCodec<T>
{
    private const int StackallocKeyThreshold = 256;
    private readonly IRecordSerializer _recordSerializer;
    private readonly ICollectionDocumentCodec<T>? _generatedCodec;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    internal CollectionDocumentCodec(IRecordSerializer recordSerializer)
    {
        _recordSerializer = recordSerializer ?? throw new ArgumentNullException(nameof(recordSerializer));
        UsesDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
        if (CollectionModelRegistry.TryGet<T>(out var model))
            _generatedCodec = model.CreateCodec(recordSerializer);
    }

    internal bool UsesDirectPayloadFormat { get; }

    [RequiresUnreferencedCode("Collection<T> JSON serialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON serialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    public byte[] Encode(string key, T document)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(document);

        if (_generatedCodec is not null)
            return _generatedCodec.Encode(key, document);

        if (!UsesDirectPayloadFormat)
        {
            string json = JsonSerializer.Serialize(document, s_jsonOptions);
            return _recordSerializer.Encode(
            [
                DbValue.FromText(key),
                DbValue.FromText(json),
            ]);
        }

        if (typeof(T) == typeof(JsonElement))
        {
            byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes((JsonElement)(object)document!, s_jsonOptions);
            return CollectionPayloadCodec.Encode(key, jsonUtf8);
        }

        try
        {
            byte[] binaryDocument = CollectionBinaryDocumentCodec.Encode(document);
            return CollectionPayloadCodec.EncodeBinary(key, binaryDocument);
        }
        catch (NotSupportedException)
        {
            byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions);
            return CollectionPayloadCodec.Encode(key, jsonUtf8);
        }
        catch (TypeInitializationException ex) when (ex.InnerException is NotSupportedException)
        {
            byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions);
            return CollectionPayloadCodec.Encode(key, jsonUtf8);
        }
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    public (string Key, T Document) Decode(ReadOnlySpan<byte> payload)
    {
        if (_generatedCodec is not null)
            return _generatedCodec.Decode(payload);

        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            string key = Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(payload, header));
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);

            try
            {
                T document = DecodeDirectDocumentPayload(documentPayload, header.Format);
                return (key, document);
            }
            catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
            {
                // Fall through to the existing slower path for marker collisions or corrupt direct payloads.
            }
        }

        return (DecodeKey(payload), DecodeDocument(payload));
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    public T DecodeDocument(ReadOnlySpan<byte> payload)
    {
        if (_generatedCodec is not null)
            return _generatedCodec.DecodeDocument(payload);

        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            try
            {
                return DecodeDirectDocumentPayload(documentPayload, header.Format);
            }
            catch (Exception ex) when (IsFastHeaderFallbackCandidate(ex))
            {
                // Fall through to the slower validated / legacy path if the fast probe
                // was triggered by a non-direct payload that happens to share the marker bytes.
            }
        }

        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            return DecodeDirectDocumentPayload(documentPayload, header.Format);
        }

        return DecodeLegacy(payload).Document;
    }

    public string DecodeKey(ReadOnlySpan<byte> payload)
    {
        if (_generatedCodec is not null)
            return _generatedCodec.DecodeKey(payload);

        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
        {
            return Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(payload, header));
        }

        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
        {
            return Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(payload, header));
        }

        return DecodeLegacyKey(payload);
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    public bool TryDecodeDocumentForKey(
        ReadOnlySpan<byte> payload,
        string expectedKey,
        out T? document)
    {
        if (_generatedCodec is not null)
            return _generatedCodec.TryDecodeDocumentForKey(payload, expectedKey, out document);

        if (!PayloadMatchesKey(payload, expectedKey))
        {
            document = default;
            return false;
        }

        document = DecodeDocument(payload);
        return true;
    }

    public bool PayloadMatchesKey(
        ReadOnlySpan<byte> payload,
        string expectedKey)
    {
        ArgumentNullException.ThrowIfNull(expectedKey);

        if (_generatedCodec is not null)
            return _generatedCodec.PayloadMatchesKey(payload, expectedKey);

        int byteCount = Encoding.UTF8.GetByteCount(expectedKey);
        byte[]? rented = null;
        Span<byte> utf8 = byteCount <= StackallocKeyThreshold
            ? stackalloc byte[StackallocKeyThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

        try
        {
            int written = Encoding.UTF8.GetBytes(expectedKey.AsSpan(), utf8);
            ReadOnlySpan<byte> expectedKeyUtf8 = utf8[..written];

            if (UsesDirectPayloadFormat &&
                CollectionPayloadCodec.TryReadFastHeader(payload, out var header))
            {
                return CollectionPayloadCodec.GetKeyUtf8(payload, header).SequenceEqual(expectedKeyUtf8);
            }

            if (UsesDirectPayloadFormat &&
                CollectionPayloadCodec.TryReadValidatedHeader(payload, out header))
            {
                return CollectionPayloadCodec.GetKeyUtf8(payload, header).SequenceEqual(expectedKeyUtf8);
            }

            if (_recordSerializer.TryColumnTextEquals(payload, 0, expectedKeyUtf8, out bool equals))
                return equals;

            return DecodeLegacyKey(payload) == expectedKey;
        }
        finally
        {
            if (rented != null)
            {
                utf8[..byteCount].Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    private (string Key, T Document) DecodeLegacy(ReadOnlySpan<byte> payload)
    {
        var values = _recordSerializer.Decode(payload);
        string storedKey = values[0].AsText;
        string json = values[1].AsText;
        T document = JsonSerializer.Deserialize<T>(json, s_jsonOptions)!;
        return (storedKey, document);
    }

    private string DecodeLegacyKey(ReadOnlySpan<byte> payload)
    {
        var values = _recordSerializer.DecodeUpTo(payload, 0);
        return values[0].AsText;
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    private static T DecodeDirectDocumentPayload(
        ReadOnlySpan<byte> documentPayload,
        CollectionPayloadCodec.CollectionPayloadFormat format)
    {
        if (format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
        {
            if (typeof(T) == typeof(JsonElement))
                return (T)(object)DecodeBinaryDocumentAsJsonElement(documentPayload);

            return CollectionBinaryDocumentCodec.Decode<T>(documentPayload);
        }

        return JsonSerializer.Deserialize<T>(documentPayload, s_jsonOptions)!;
    }

    private static JsonElement DecodeBinaryDocumentAsJsonElement(ReadOnlySpan<byte> documentPayload)
    {
        byte[] jsonUtf8 = CollectionBinaryDocumentCodec.EncodeJsonUtf8(documentPayload);
        using JsonDocument document = JsonDocument.Parse(jsonUtf8);
        return document.RootElement.Clone();
    }

    private static bool IsFastHeaderFallbackCandidate(Exception ex)
        => ex is CSharpDbException or JsonException or DecoderFallbackException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException;
}
