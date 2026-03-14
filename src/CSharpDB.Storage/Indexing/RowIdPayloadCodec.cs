using System.Buffers.Binary;

namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Helpers for sorted row-id payloads stored inside index values.
/// </summary>
internal static class RowIdPayloadCodec
{
    internal const int RowIdSize = sizeof(long);

    public static byte[] CreateSingle(long rowId)
    {
        byte[] payload = GC.AllocateUninitializedArray<byte>(RowIdSize);
        BinaryPrimitives.WriteInt64LittleEndian(payload, rowId);
        return payload;
    }

    public static byte[] CreateFromSorted(IReadOnlyList<long> rowIds)
    {
        ArgumentNullException.ThrowIfNull(rowIds);
        if (rowIds.Count == 0)
            return Array.Empty<byte>();
        if (rowIds.Count == 1)
            return CreateSingle(rowIds[0]);

        byte[] payload = GC.AllocateUninitializedArray<byte>(rowIds.Count * RowIdSize);
        for (int i = 0; i < rowIds.Count; i++)
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(i * RowIdSize, RowIdSize), rowIds[i]);

        return payload;
    }

    public static int GetCount(ReadOnlySpan<byte> payload) => payload.Length / RowIdSize;

    public static long ReadAt(ReadOnlySpan<byte> payload, int index)
    {
        int offset = checked(index * RowIdSize);
        return BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, RowIdSize));
    }

    public static bool TryInsertSorted(ReadOnlySpan<byte> payload, long rowId, out byte[] newPayload)
    {
        int count = GetCount(payload);
        int insertIndex = FindInsertIndex(payload, count, rowId, out bool exists);
        if (exists)
        {
            newPayload = Array.Empty<byte>();
            return false;
        }

        newPayload = GC.AllocateUninitializedArray<byte>((count + 1) * RowIdSize);
        int insertOffset = insertIndex * RowIdSize;
        payload[..insertOffset].CopyTo(newPayload);
        BinaryPrimitives.WriteInt64LittleEndian(newPayload.AsSpan(insertOffset, RowIdSize), rowId);
        payload[insertOffset..].CopyTo(newPayload.AsSpan(insertOffset + RowIdSize));
        return true;
    }

    public static bool TryInsert(ReadOnlySpan<byte> payload, long rowId, out byte[] newPayload)
    {
        if (IsSortedAscending(payload))
            return TryInsertSorted(payload, rowId, out newPayload);

        int count = GetCount(payload);
        for (int i = 0; i < count; i++)
        {
            if (ReadAt(payload, i) == rowId)
            {
                newPayload = Array.Empty<byte>();
                return false;
            }
        }

        newPayload = GC.AllocateUninitializedArray<byte>((count + 1) * RowIdSize);
        payload.CopyTo(newPayload);
        BinaryPrimitives.WriteInt64LittleEndian(newPayload.AsSpan(count * RowIdSize, RowIdSize), rowId);
        return true;
    }

    public static bool TryRemoveSorted(ReadOnlySpan<byte> payload, long rowId, out byte[]? newPayload)
    {
        int count = GetCount(payload);
        int index = FindIndex(payload, count, rowId);
        if (index < 0)
        {
            newPayload = null;
            return false;
        }

        if (count == 1)
        {
            newPayload = null;
            return true;
        }

        newPayload = GC.AllocateUninitializedArray<byte>((count - 1) * RowIdSize);
        int removeOffset = index * RowIdSize;
        payload[..removeOffset].CopyTo(newPayload);
        payload[(removeOffset + RowIdSize)..].CopyTo(newPayload.AsSpan(removeOffset));
        return true;
    }

    public static bool TryRemove(ReadOnlySpan<byte> payload, long rowId, out byte[]? newPayload)
    {
        if (IsSortedAscending(payload))
            return TryRemoveSorted(payload, rowId, out newPayload);

        int count = GetCount(payload);
        int removeIndex = -1;
        for (int i = 0; i < count; i++)
        {
            if (ReadAt(payload, i) == rowId)
            {
                removeIndex = i;
                break;
            }
        }

        if (removeIndex < 0)
        {
            newPayload = null;
            return false;
        }

        if (count == 1)
        {
            newPayload = null;
            return true;
        }

        newPayload = GC.AllocateUninitializedArray<byte>((count - 1) * RowIdSize);
        int removeOffset = removeIndex * RowIdSize;
        payload[..removeOffset].CopyTo(newPayload);
        payload[(removeOffset + RowIdSize)..].CopyTo(newPayload.AsSpan(removeOffset));
        return true;
    }

    private static int FindIndex(ReadOnlySpan<byte> payload, int count, long rowId)
    {
        int low = 0;
        int high = count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            long current = ReadAt(payload, mid);
            if (current == rowId)
                return mid;

            if (current < rowId)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return -1;
    }

    private static int FindInsertIndex(ReadOnlySpan<byte> payload, int count, long rowId, out bool exists)
    {
        int low = 0;
        int high = count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            long current = ReadAt(payload, mid);
            if (current == rowId)
            {
                exists = true;
                return mid;
            }

            if (current < rowId)
                low = mid + 1;
            else
                high = mid - 1;
        }

        exists = false;
        return low;
    }

    private static bool IsSortedAscending(ReadOnlySpan<byte> payload)
    {
        int count = GetCount(payload);
        for (int i = 1; i < count; i++)
        {
            if (ReadAt(payload, i - 1) > ReadAt(payload, i))
                return false;
        }

        return true;
    }
}
