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

    [RequiresUnreferencedCode("Collection<T> JSON serialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON serialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
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

        byte[] binaryDocument = CollectionBinaryDocumentCodec.Encode(document);
        return CollectionPayloadCodec.EncodeBinary(key, binaryDocument);
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal (string Key, T Document) Decode(ReadOnlySpan<byte> payload)
    {
        return (DecodeKey(payload), DecodeDocument(payload));
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal T DecodeDocument(ReadOnlySpan<byte> payload)
    {
        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(payload, header);
            if (header.Format == CollectionPayloadCodec.CollectionPayloadFormat.Binary)
                return CollectionBinaryDocumentCodec.Decode<T>(documentPayload);

            return JsonSerializer.Deserialize<T>(documentPayload, s_jsonOptions)!;
        }

        return DecodeLegacy(payload).Document;
    }

    internal string DecodeKey(ReadOnlySpan<byte> payload)
    {
        if (UsesDirectPayloadFormat &&
            CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
        {
            return Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(payload, header));
        }

        return DecodeLegacyKey(payload);
    }

    [RequiresUnreferencedCode("Collection<T> JSON deserialization requires reflection. Use SQL API for NativeAOT scenarios.")]
    [RequiresDynamicCode("Collection<T> JSON deserialization requires runtime code generation. Use SQL API for NativeAOT scenarios.")]
    internal bool TryDecodeDocumentForKey(
        ReadOnlySpan<byte> payload,
        string expectedKey,
        out T? document)
    {
        if (!PayloadMatchesKey(payload, expectedKey))
        {
            document = default;
            return false;
        }

        document = DecodeDocument(payload);
        return true;
    }

    internal bool PayloadMatchesKey(
        ReadOnlySpan<byte> payload,
        string expectedKey)
    {
        ArgumentNullException.ThrowIfNull(expectedKey);

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
                CollectionPayloadCodec.TryReadValidatedHeader(payload, out var header))
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
}
