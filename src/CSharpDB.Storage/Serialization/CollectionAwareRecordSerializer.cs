using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Record serializer that understands both standard row records and direct collection payloads.
/// </summary>
public sealed class CollectionAwareRecordSerializer : IRecordSerializer
{
    private readonly IRecordSerializer _inner;

    public CollectionAwareRecordSerializer()
        : this(new DefaultRecordSerializer())
    {
    }

    public CollectionAwareRecordSerializer(IRecordSerializer inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public byte[] Encode(ReadOnlySpan<DbValue> values) => _inner.Encode(values);

    public DbValue[] Decode(ReadOnlySpan<byte> buffer)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
            return _inner.Decode(buffer);

        return
        [
            DbValue.FromText(Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header))),
            DbValue.FromText(
                header.Format == CollectionPayloadCodec.CollectionPayloadFormat.LegacyJson
                    ? Encoding.UTF8.GetString(CollectionPayloadCodec.GetDocumentPayload(buffer, header))
                    : CollectionBinaryDocumentCodec.DecodeJson(CollectionPayloadCodec.GetDocumentPayload(buffer, header)))
        ];
    }

    public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
            return _inner.DecodeInto(buffer, destination);

        int decodeCount = Math.Min(2, destination.Length);
        if (decodeCount <= 0)
            return 0;

        destination[0] = DbValue.FromText(Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header)));
        if (decodeCount > 1)
        {
            ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(buffer, header);
            destination[1] = DbValue.FromText(
                header.Format == CollectionPayloadCodec.CollectionPayloadFormat.LegacyJson
                    ? Encoding.UTF8.GetString(documentPayload)
                    : CollectionBinaryDocumentCodec.DecodeJson(documentPayload));
        }
        return decodeCount;
    }

    public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
        {
            _inner.DecodeSelectedInto(buffer, destination, selectedColumnIndices);
            return;
        }

        string? decodedKey = null;
        string? decodedJson = null;
        int decodeCount = Math.Min(destination.Length, selectedColumnIndices.Length);
        for (int i = 0; i < decodeCount; i++)
        {
            int columnIndex = selectedColumnIndices[i];
            if ((uint)columnIndex >= (uint)destination.Length)
                continue;

            switch (columnIndex)
            {
                case 0:
                    decodedKey ??= Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header));
                    destination[columnIndex] = DbValue.FromText(decodedKey);
                    break;
                case 1:
                    decodedJson ??= DecodeJson(buffer, header);
                    destination[columnIndex] = DbValue.FromText(decodedJson);
                    break;
                default:
                    destination[columnIndex] = DbValue.Null;
                    break;
            }
        }
    }

    public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
        {
            _inner.DecodeSelectedCompactInto(buffer, destination, selectedColumnIndices);
            return;
        }

        string? decodedKey = null;
        string? decodedJson = null;
        int decodeCount = Math.Min(destination.Length, selectedColumnIndices.Length);
        for (int i = 0; i < decodeCount; i++)
        {
            destination[i] = selectedColumnIndices[i] switch
            {
                0 => DbValue.FromText(decodedKey ??= Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header))),
                1 => DbValue.FromText(decodedJson ??= DecodeJson(buffer, header)),
                _ => DbValue.Null,
            };
        }
    }

    public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
            return _inner.DecodeUpTo(buffer, maxColumnIndexInclusive);

        if (maxColumnIndexInclusive < 0)
            return Array.Empty<DbValue>();
        if (maxColumnIndexInclusive == 0)
            return [DbValue.FromText(Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header)))];

        return
        [
            DbValue.FromText(Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header))),
            DbValue.FromText(DecodeJson(buffer, header))
        ];
    }

    public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
            return _inner.DecodeColumn(buffer, columnIndex);

        return columnIndex switch
        {
            0 => DbValue.FromText(Encoding.UTF8.GetString(CollectionPayloadCodec.GetKeyUtf8(buffer, header))),
            1 => DbValue.FromText(DecodeJson(buffer, header)),
            _ => DbValue.Null,
        };
    }

    public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out var header))
            return _inner.TryColumnTextEquals(buffer, columnIndex, expectedUtf8, out equals);

        equals = columnIndex switch
        {
            0 => CollectionPayloadCodec.GetKeyUtf8(buffer, header).SequenceEqual(expectedUtf8),
            1 => CollectionPayloadCodec.JsonEquals(buffer, expectedUtf8),
            _ => false,
        };

        return columnIndex is 0 or 1;
    }

    public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out _))
            return _inner.IsColumnNull(buffer, columnIndex);

        return columnIndex is not 0 and not 1;
    }

    public bool TryDecodeNumericColumn(
        ReadOnlySpan<byte> buffer,
        int columnIndex,
        out long intValue,
        out double realValue,
        out bool isReal)
    {
        if (!CollectionPayloadCodec.TryReadValidatedHeader(buffer, out _))
            return _inner.TryDecodeNumericColumn(buffer, columnIndex, out intValue, out realValue, out isReal);

        intValue = 0;
        realValue = 0;
        isReal = false;

        return false;
    }

    private static string DecodeJson(ReadOnlySpan<byte> buffer, CollectionPayloadCodec.Header header)
    {
        ReadOnlySpan<byte> documentPayload = CollectionPayloadCodec.GetDocumentPayload(buffer, header);
        return header.Format == CollectionPayloadCodec.CollectionPayloadFormat.LegacyJson
            ? Encoding.UTF8.GetString(documentPayload)
            : CollectionBinaryDocumentCodec.DecodeJson(documentPayload);
    }
}
