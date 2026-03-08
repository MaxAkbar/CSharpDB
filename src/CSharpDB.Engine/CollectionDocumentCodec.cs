using System.Text;
using System.Text.Json;
using CSharpDB.Core;

namespace CSharpDB.Engine;

internal sealed class CollectionDocumentCodec<T>
{
    private readonly IRecordSerializer _recordSerializer;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    internal CollectionDocumentCodec(IRecordSerializer recordSerializer)
    {
        _recordSerializer = recordSerializer ?? throw new ArgumentNullException(nameof(recordSerializer));
        UsesDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
    }

    internal bool UsesDirectPayloadFormat { get; }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Collection<T> JSON serialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Collection<T> JSON serialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal byte[] Encode(string key, T document)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(document);

        if (!UsesDirectPayloadFormat)
        {
            string json = JsonSerializer.Serialize(document, s_jsonOptions);
            return _recordSerializer.Encode(
            [
                DbValue.FromText(key),
                DbValue.FromText(json),
            ]);
        }

        byte[] keyUtf8 = Encoding.UTF8.GetBytes(key);
        byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions);
        return CollectionPayloadCodec.Encode(keyUtf8, jsonUtf8);
    }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal (string Key, T Document) Decode(ReadOnlySpan<byte> payload)
    {
        if (!UsesDirectPayloadFormat || !CollectionPayloadCodec.IsDirectPayload(payload))
            return DecodeLegacy(payload);

        string storedKey = CollectionPayloadCodec.DecodeKey(payload);
        T document = JsonSerializer.Deserialize<T>(CollectionPayloadCodec.GetJsonUtf8(payload), s_jsonOptions)!;
        return (storedKey, document);
    }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal bool TryDecodeDocumentForKey(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<byte> expectedKeyUtf8,
        string expectedKey,
        out T? document)
    {
        if (!PayloadMatchesKey(payload, expectedKeyUtf8, expectedKey))
        {
            document = default;
            return false;
        }

        if (UsesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
        {
            document = JsonSerializer.Deserialize<T>(CollectionPayloadCodec.GetJsonUtf8(payload), s_jsonOptions);
            return true;
        }

        document = DecodeLegacy(payload).Document;
        return true;
    }

    internal bool PayloadMatchesKey(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<byte> expectedKeyUtf8,
        string expectedKey)
    {
        if (UsesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
            return CollectionPayloadCodec.KeyEquals(payload, expectedKeyUtf8);

        if (_recordSerializer.TryColumnTextEquals(payload, 0, expectedKeyUtf8, out bool equals))
            return equals;

        return DecodeLegacyKey(payload) == expectedKey;
    }

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
}
