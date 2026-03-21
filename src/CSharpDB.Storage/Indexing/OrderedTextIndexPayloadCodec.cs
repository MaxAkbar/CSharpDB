using System.Buffers.Binary;
using System.Text;

namespace CSharpDB.Storage.Indexing;

internal static class OrderedTextIndexPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'O', (byte)'T', (byte)'X', (byte)'1'];
    private const int HeaderSize = 8;

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= HeaderSize &&
           payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] CreateSingle(string text, long rowId)
    {
        ArgumentNullException.ThrowIfNull(text);
        return EncodeEntries([new OrderedTextBucket(text, RowIdPayloadCodec.CreateSingle(rowId))]);
    }

    public static byte[] CreateFromSorted(IReadOnlyDictionary<string, List<long>> rowIdsByText)
    {
        ArgumentNullException.ThrowIfNull(rowIdsByText);

        var entries = new List<OrderedTextBucket>(rowIdsByText.Count);
        foreach ((string text, List<long> rowIds) in rowIdsByText)
        {
            entries.Add(new OrderedTextBucket(
                text,
                RowIdPayloadCodec.CreateFromSorted(rowIds)));
        }

        return EncodeEntries(entries);
    }

    public static bool TryGetMatchingRowIdPayloadSlice(
        byte[] payload,
        string text,
        out ReadOnlyMemory<byte> rowIdPayload)
    {
        rowIdPayload = ReadOnlyMemory<byte>.Empty;
        if (!IsEncoded(payload))
            return false;

        byte[] expectedUtf8 = Encoding.UTF8.GetBytes(text);
        ReadOnlySpan<byte> span = payload;
        int offset = MagicBytes.Length;
        int entryCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (entryCount < 0)
            return false;

        for (int i = 0; i < entryCount; i++)
        {
            if (offset + sizeof(int) > span.Length)
                return false;

            int textLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (textLength < 0 || offset + textLength > span.Length)
                return false;

            ReadOnlySpan<byte> actualUtf8 = span.Slice(offset, textLength);
            offset += textLength;

            if (offset + sizeof(int) > span.Length)
                return false;

            int rowIdCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (rowIdCount < 0)
                return false;

            int rowIdPayloadLength = checked(rowIdCount * RowIdPayloadCodec.RowIdSize);
            if (offset + rowIdPayloadLength > span.Length)
                return false;

            if (actualUtf8.SequenceEqual(expectedUtf8))
            {
                rowIdPayload = payload.AsMemory(offset, rowIdPayloadLength);
                return true;
            }

            offset += rowIdPayloadLength;
        }

        return offset == span.Length;
    }

    public static bool TryCollectMatchingRowIdsInRange(
        ReadOnlySpan<byte> payload,
        string lowerBound,
        bool lowerInclusive,
        string upperBound,
        bool upperInclusive,
        List<long> rowIds)
    {
        if (!TryDecodeEntries(payload, out var entries))
            return false;

        for (int i = 0; i < entries.Count; i++)
        {
            if (!IsWithinRange(entries[i].Text, lowerBound, lowerInclusive, upperBound, upperInclusive))
                continue;

            int rowIdCount = RowIdPayloadCodec.GetCount(entries[i].RowIdPayload);
            for (int rowIdIndex = 0; rowIdIndex < rowIdCount; rowIdIndex++)
                rowIds.Add(RowIdPayloadCodec.ReadAt(entries[i].RowIdPayload, rowIdIndex));
        }

        return true;
    }

    public static byte[] Insert(
        ReadOnlySpan<byte> payload,
        string text,
        long rowId,
        out bool changed)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in ordered text-index bucket format.");

        int insertIndex = FindInsertIndex(entries, text, out bool exists);
        if (exists)
        {
            if (!RowIdPayloadCodec.TryInsertSorted(entries[insertIndex].RowIdPayload, rowId, out var updatedRowIds))
            {
                changed = false;
                return Array.Empty<byte>();
            }

            entries[insertIndex] = entries[insertIndex] with { RowIdPayload = updatedRowIds };
            changed = true;
            return EncodeEntries(entries);
        }

        entries.Insert(insertIndex, new OrderedTextBucket(text, RowIdPayloadCodec.CreateSingle(rowId)));
        changed = true;
        return EncodeEntries(entries);
    }

    public static byte[]? Remove(
        ReadOnlySpan<byte> payload,
        string text,
        long rowId,
        out bool changed)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in ordered text-index bucket format.");

        int index = FindEntryIndex(entries, text);
        if (index < 0)
        {
            changed = false;
            return null;
        }

        if (!RowIdPayloadCodec.TryRemoveSorted(entries[index].RowIdPayload, rowId, out byte[]? updatedRowIds))
        {
            changed = false;
            return null;
        }

        if (updatedRowIds == null || updatedRowIds.Length == 0)
            entries.RemoveAt(index);
        else
            entries[index] = entries[index] with { RowIdPayload = updatedRowIds };

        changed = true;
        return entries.Count == 0 ? null : EncodeEntries(entries);
    }

    private static bool TryDecodeEntries(ReadOnlySpan<byte> payload, out List<OrderedTextBucket> entries)
    {
        entries = [];
        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        int entryCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (entryCount < 0)
            return false;

        entries = new List<OrderedTextBucket>(entryCount);
        for (int i = 0; i < entryCount; i++)
        {
            if (offset + sizeof(int) > payload.Length)
                return false;

            int textLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (textLength < 0 || offset + textLength > payload.Length)
                return false;

            string text = Encoding.UTF8.GetString(payload.Slice(offset, textLength));
            offset += textLength;

            if (offset + sizeof(int) > payload.Length)
                return false;

            int rowIdCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (rowIdCount < 0)
                return false;

            int rowIdPayloadLength = checked(rowIdCount * RowIdPayloadCodec.RowIdSize);
            if (offset + rowIdPayloadLength > payload.Length)
                return false;

            byte[] rowIdPayload = payload.Slice(offset, rowIdPayloadLength).ToArray();
            offset += rowIdPayloadLength;

            entries.Add(new OrderedTextBucket(text, rowIdPayload));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeEntries(IReadOnlyList<OrderedTextBucket> entries)
    {
        int size = HeaderSize;
        for (int i = 0; i < entries.Count; i++)
        {
            int textByteLength = Encoding.UTF8.GetByteCount(entries[i].Text);
            int rowIdCount = RowIdPayloadCodec.GetCount(entries[i].RowIdPayload);
            size += sizeof(int) + textByteLength + sizeof(int) + (rowIdCount * RowIdPayloadCodec.RowIdSize);
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), entries.Count);
        offset += sizeof(int);

        for (int i = 0; i < entries.Count; i++)
        {
            OrderedTextBucket entry = entries[i];
            int textByteLength = Encoding.UTF8.GetByteCount(entry.Text);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), textByteLength);
            offset += sizeof(int);
            offset += Encoding.UTF8.GetBytes(entry.Text, payload.AsSpan(offset, textByteLength));

            int rowIdCount = RowIdPayloadCodec.GetCount(entry.RowIdPayload);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), rowIdCount);
            offset += sizeof(int);
            entry.RowIdPayload.CopyTo(payload.AsSpan(offset));
            offset += entry.RowIdPayload.Length;
        }

        return payload;
    }

    private static int FindInsertIndex(IReadOnlyList<OrderedTextBucket> entries, string text, out bool exists)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = string.Compare(entries[mid].Text, text, StringComparison.Ordinal);
            if (comparison == 0)
            {
                exists = true;
                return mid;
            }

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        exists = false;
        return low;
    }

    private static int FindEntryIndex(IReadOnlyList<OrderedTextBucket> entries, string text)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = string.Compare(entries[mid].Text, text, StringComparison.Ordinal);
            if (comparison == 0)
                return mid;

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return -1;
    }

    private static bool IsWithinRange(
        string value,
        string lowerBound,
        bool lowerInclusive,
        string upperBound,
        bool upperInclusive)
    {
        int lowerComparison = string.Compare(value, lowerBound, StringComparison.Ordinal);
        if (lowerComparison < 0 || (!lowerInclusive && lowerComparison == 0))
            return false;

        int upperComparison = string.Compare(value, upperBound, StringComparison.Ordinal);
        if (upperComparison > 0 || (!upperInclusive && upperComparison == 0))
            return false;

        return true;
    }

    private sealed record OrderedTextBucket(string Text, byte[] RowIdPayload);
}
