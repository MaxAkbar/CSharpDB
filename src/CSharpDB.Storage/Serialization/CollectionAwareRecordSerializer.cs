using CSharpDB.Core;

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
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.Decode(buffer);

        return
        [
            DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer)),
            DbValue.FromText(CollectionPayloadCodec.DecodeJson(buffer))
        ];
    }

    public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.DecodeInto(buffer, destination);

        int decodeCount = Math.Min(2, destination.Length);
        if (decodeCount <= 0)
            return 0;

        destination[0] = DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer));
        if (decodeCount > 1)
            destination[1] = DbValue.FromText(CollectionPayloadCodec.DecodeJson(buffer));
        return decodeCount;
    }

    public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
        {
            _inner.DecodeSelectedInto(buffer, destination, selectedColumnIndices);
            return;
        }

        int decodeCount = Math.Min(destination.Length, selectedColumnIndices.Length);
        for (int i = 0; i < decodeCount; i++)
        {
            switch (selectedColumnIndices[i])
            {
                case 0:
                    destination[i] = DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer));
                    break;
                case 1:
                    destination[i] = DbValue.FromText(CollectionPayloadCodec.DecodeJson(buffer));
                    break;
                default:
                    destination[i] = DbValue.Null;
                    break;
            }
        }
    }

    public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.DecodeUpTo(buffer, maxColumnIndexInclusive);

        if (maxColumnIndexInclusive < 0)
            return Array.Empty<DbValue>();
        if (maxColumnIndexInclusive == 0)
            return [DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer))];

        return
        [
            DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer)),
            DbValue.FromText(CollectionPayloadCodec.DecodeJson(buffer))
        ];
    }

    public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.DecodeColumn(buffer, columnIndex);

        return columnIndex switch
        {
            0 => DbValue.FromText(CollectionPayloadCodec.DecodeKey(buffer)),
            1 => DbValue.FromText(CollectionPayloadCodec.DecodeJson(buffer)),
            _ => DbValue.Null,
        };
    }

    public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.TryColumnTextEquals(buffer, columnIndex, expectedUtf8, out equals);

        equals = columnIndex switch
        {
            0 => CollectionPayloadCodec.KeyEquals(buffer, expectedUtf8),
            1 => CollectionPayloadCodec.GetJsonUtf8(buffer).SequenceEqual(expectedUtf8),
            _ => false,
        };

        return columnIndex is 0 or 1;
    }

    public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
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
        if (!CollectionPayloadCodec.IsDirectPayload(buffer))
            return _inner.TryDecodeNumericColumn(buffer, columnIndex, out intValue, out realValue, out isReal);

        intValue = 0;
        realValue = 0;
        isReal = false;

        return false;
    }
}
