using System.Buffers.Binary;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Encodes hashed index buckets with explicit key components so equality lookups
/// can disambiguate hash collisions without always fetching base table rows.
/// Legacy rowid-only payloads remain readable; this codec is only used when the
/// bucket was written in the new format.
/// </summary>
internal static class HashedIndexPayloadCodec
{
    private const int HeaderSize = 16;
    private const byte IntegerComponentTag = 1;
    private const byte TextComponentTag = 2;

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= HeaderSize &&
           payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] CreateSingle(
        ReadOnlySpan<DbValue> keyComponents,
        long rowId,
        bool omitTrailingInteger = false)
    {
        ReadOnlySpan<DbValue> storedKeyComponents = GetStoredKeyComponents(keyComponents, omitTrailingInteger);
        int totalLength = HeaderSize + GetEncodedKeySize(storedKeyComponents) + sizeof(int) + RowIdPayloadCodec.RowIdSize;
        byte[] payload = GC.AllocateUninitializedArray<byte>(totalLength);
        MagicBytes.CopyTo(payload);

        int offset = MagicBytes.Length;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), storedKeyComponents.Length);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), 1);
        offset += sizeof(int);
        offset = WriteKeyComponents(payload, offset, storedKeyComponents);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), 1);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, RowIdPayloadCodec.RowIdSize), rowId);
        return payload;
    }

    public static byte[] CreateSingleGroup(ReadOnlySpan<DbValue> keyComponents, ReadOnlySpan<byte> rowIdPayload)
        => EncodeGroups(keyComponents.Length, [new BucketGroup(keyComponents.ToArray(), rowIdPayload.ToArray())]);

    public static bool TryGetMatchingRowIds(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<DbValue> keyComponents,
        out byte[]? rowIdPayload)
    {
        rowIdPayload = null;
        if (!TryDecodeGroups(payload, out int componentCount, out var groups))
            return false;

        if (!TryGetComparableRequestedKeyComponents(componentCount, keyComponents, out ReadOnlySpan<DbValue> comparableKeyComponents))
            return true;

        for (int i = 0; i < groups.Count; i++)
        {
            if (!ComponentsEqual(groups[i].KeyComponents, comparableKeyComponents))
                continue;

            rowIdPayload = groups[i].RowIdPayload;
            return true;
        }

        return true;
    }

    public static bool TryGetMatchingRowIdPayloadSlice(
        byte[] payload,
        ReadOnlySpan<DbValue> keyComponents,
        byte[][]? expectedTextBytes,
        out ReadOnlyMemory<byte> rowIdPayload)
    {
        rowIdPayload = ReadOnlyMemory<byte>.Empty;
        ReadOnlySpan<byte> span = payload;
        if (!IsEncoded(span))
            return false;

        int offset = MagicBytes.Length;
        if (offset + sizeof(int) * 2 > span.Length)
            return false;

        int componentCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int groupCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (componentCount <= 0 || groupCount < 0)
            return false;

        if (!TryGetComparableRequestedKeyComponents(componentCount, keyComponents, out ReadOnlySpan<DbValue> comparableKeyComponents))
            return true;

        for (int groupIndex = 0; groupIndex < groupCount; groupIndex++)
        {
            bool matches = true;
            for (int componentIndex = 0; componentIndex < componentCount; componentIndex++)
            {
                if (offset >= span.Length)
                    return false;

                byte tag = span[offset++];
                var expectedComponent = comparableKeyComponents[componentIndex];
                switch (tag)
                {
                    case IntegerComponentTag:
                    {
                        if (offset + sizeof(long) > span.Length)
                            return false;

                        long value = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offset, sizeof(long)));
                        offset += sizeof(long);
                        if (expectedComponent.Type != DbType.Integer || expectedComponent.AsInteger != value)
                            matches = false;
                        break;
                    }

                    case TextComponentTag:
                    {
                        if (offset + sizeof(int) > span.Length)
                            return false;

                        int textByteLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
                        offset += sizeof(int);
                        if (textByteLength < 0 || offset + textByteLength > span.Length)
                            return false;

                        ReadOnlySpan<byte> actualTextBytes = span.Slice(offset, textByteLength);
                        offset += textByteLength;

                        ReadOnlySpan<byte> expectedBytes = GetExpectedTextBytes(expectedComponent, expectedTextBytes, componentIndex);
                        if (expectedComponent.Type != DbType.Text || !actualTextBytes.SequenceEqual(expectedBytes))
                            matches = false;
                        break;
                    }

                    default:
                        return false;
                }
            }

            if (offset + sizeof(int) > span.Length)
                return false;

            int rowIdCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (rowIdCount < 0)
                return false;

            int rowIdPayloadLength = checked(rowIdCount * RowIdPayloadCodec.RowIdSize);
            if (offset + rowIdPayloadLength > span.Length)
                return false;

            if (matches)
            {
                rowIdPayload = payload.AsMemory(offset, rowIdPayloadLength);
                return true;
            }

            offset += rowIdPayloadLength;
        }

        return offset == span.Length;
    }

    public static bool TryGetSingleMatchingRowId(
        byte[] payload,
        ReadOnlySpan<DbValue> keyComponents,
        byte[][]? expectedTextBytes,
        out bool foundRow,
        out long rowId)
    {
        foundRow = false;
        rowId = 0;

        ReadOnlySpan<byte> span = payload;
        if (!IsEncoded(span))
            return false;

        int offset = MagicBytes.Length;
        if (offset + sizeof(int) * 2 > span.Length)
            return false;

        int componentCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int groupCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (componentCount <= 0 || groupCount < 0)
            return false;

        if (!TryGetComparableRequestedKeyComponents(componentCount, keyComponents, out ReadOnlySpan<DbValue> comparableKeyComponents))
            return true;

        for (int groupIndex = 0; groupIndex < groupCount; groupIndex++)
        {
            bool matches = true;
            for (int componentIndex = 0; componentIndex < componentCount; componentIndex++)
            {
                if (offset >= span.Length)
                    return false;

                byte tag = span[offset++];
                        var expectedComponent = comparableKeyComponents[componentIndex];
                switch (tag)
                {
                    case IntegerComponentTag:
                    {
                        if (offset + sizeof(long) > span.Length)
                            return false;

                        long value = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offset, sizeof(long)));
                        offset += sizeof(long);
                        if (expectedComponent.Type != DbType.Integer || expectedComponent.AsInteger != value)
                            matches = false;
                        break;
                    }

                    case TextComponentTag:
                    {
                        if (offset + sizeof(int) > span.Length)
                            return false;

                        int textByteLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
                        offset += sizeof(int);
                        if (textByteLength < 0 || offset + textByteLength > span.Length)
                            return false;

                        ReadOnlySpan<byte> actualTextBytes = span.Slice(offset, textByteLength);
                        offset += textByteLength;

                        ReadOnlySpan<byte> expectedBytes = GetExpectedTextBytes(expectedComponent, expectedTextBytes, componentIndex);
                        if (expectedComponent.Type != DbType.Text || !actualTextBytes.SequenceEqual(expectedBytes))
                            matches = false;
                        break;
                    }

                    default:
                        return false;
                }
            }

            if (offset + sizeof(int) > span.Length)
                return false;

            int rowIdCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (rowIdCount < 0)
                return false;

            int rowIdPayloadLength = checked(rowIdCount * RowIdPayloadCodec.RowIdSize);
            if (offset + rowIdPayloadLength > span.Length)
                return false;

            if (!matches)
            {
                offset += rowIdPayloadLength;
                continue;
            }

            if (rowIdCount == 0)
                return true;

            if (rowIdCount != 1)
                return false;

            rowId = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offset, RowIdPayloadCodec.RowIdSize));
            foundRow = true;
            return true;
        }

        return offset == span.Length;
    }

    public static byte[] Insert(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<DbValue> keyComponents,
        long rowId,
        out bool changed)
    {
        if (!TryDecodeGroups(payload, out int componentCount, out var groups))
            throw new InvalidOperationException("Payload is not in hashed-index bucket format.");

        if (!TryGetComparableRequestedKeyComponents(componentCount, keyComponents, out ReadOnlySpan<DbValue> comparableKeyComponents))
            throw new InvalidOperationException("Hashed-index payload component count mismatch.");

        for (int i = 0; i < groups.Count; i++)
        {
            if (!ComponentsEqual(groups[i].KeyComponents, comparableKeyComponents))
                continue;

            if (!RowIdPayloadCodec.TryInsert(groups[i].RowIdPayload, rowId, out var newRowIdPayload))
            {
                changed = false;
                return Array.Empty<byte>();
            }

            groups[i] = groups[i] with { RowIdPayload = newRowIdPayload };
            changed = true;
            return EncodeGroups(componentCount, groups);
        }

        groups.Add(new BucketGroup(comparableKeyComponents.ToArray(), RowIdPayloadCodec.CreateSingle(rowId)));
        changed = true;
        return EncodeGroups(componentCount, groups);
    }

    public static byte[]? Remove(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<DbValue> keyComponents,
        long rowId,
        out bool changed)
    {
        if (!TryDecodeGroups(payload, out int componentCount, out var groups))
            throw new InvalidOperationException("Payload is not in hashed-index bucket format.");

        if (!TryGetComparableRequestedKeyComponents(componentCount, keyComponents, out ReadOnlySpan<DbValue> comparableKeyComponents))
            throw new InvalidOperationException("Hashed-index payload component count mismatch.");

        for (int i = 0; i < groups.Count; i++)
        {
            if (!ComponentsEqual(groups[i].KeyComponents, comparableKeyComponents))
                continue;

            if (!RowIdPayloadCodec.TryRemove(groups[i].RowIdPayload, rowId, out var newRowIdPayload))
            {
                changed = false;
                return null;
            }

            if (newRowIdPayload == null || newRowIdPayload.Length == 0)
                groups.RemoveAt(i);
            else
                groups[i] = groups[i] with { RowIdPayload = newRowIdPayload };

            changed = true;
            return groups.Count == 0 ? null : EncodeGroups(componentCount, groups);
        }

        changed = false;
        return null;
    }

    public static bool TryDecodeSingleGroup(
        ReadOnlySpan<byte> payload,
        out DbValue[]? keyComponents,
        out byte[]? rowIdPayload)
    {
        keyComponents = null;
        rowIdPayload = null;
        if (!TryDecodeGroups(payload, out _, out List<BucketGroup>? groups) || groups.Count != 1)
            return false;

        keyComponents = groups[0].KeyComponents;
        rowIdPayload = groups[0].RowIdPayload;
        return true;
    }

    internal static bool KeyComponentsEqualStored(
        ReadOnlySpan<DbValue> storedKeyComponents,
        ReadOnlySpan<DbValue> requestedKeyComponents)
    {
        return TryGetComparableRequestedKeyComponents(
                   storedKeyComponents.Length,
                   requestedKeyComponents,
                   out ReadOnlySpan<DbValue> comparableKeyComponents) &&
               ComponentsEqual(storedKeyComponents, comparableKeyComponents);
    }

    internal static bool TryDecodeGroups(
        ReadOnlySpan<byte> payload,
        out int componentCount,
        out List<BucketGroup> groups)
    {
        componentCount = 0;
        groups = new List<BucketGroup>();

        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        componentCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int groupCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (componentCount <= 0 || groupCount < 0)
            return false;

        groups = new List<BucketGroup>(groupCount);
        for (int groupIndex = 0; groupIndex < groupCount; groupIndex++)
        {
            var components = new DbValue[componentCount];
            for (int componentIndex = 0; componentIndex < componentCount; componentIndex++)
            {
                if (offset >= payload.Length)
                    return false;

                byte tag = payload[offset++];
                switch (tag)
                {
                    case IntegerComponentTag:
                        if (offset + sizeof(long) > payload.Length)
                            return false;
                        components[componentIndex] = DbValue.FromInteger(
                            BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, sizeof(long))));
                        offset += sizeof(long);
                        break;

                    case TextComponentTag:
                        if (offset + sizeof(int) > payload.Length)
                            return false;

                        int textByteLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
                        offset += sizeof(int);
                        if (textByteLength < 0 || offset + textByteLength > payload.Length)
                            return false;

                        components[componentIndex] = DbValue.FromText(
                            Encoding.UTF8.GetString(payload.Slice(offset, textByteLength)));
                        offset += textByteLength;
                        break;

                    default:
                        return false;
                }
            }

            if (offset + sizeof(int) > payload.Length)
                return false;

            int rowIdCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            if (rowIdCount < 0)
                return false;

            int rowIdPayloadLength = checked(rowIdCount * RowIdPayloadCodec.RowIdSize);
            if (offset + rowIdPayloadLength > payload.Length)
                return false;

            var rowIdPayload = payload.Slice(offset, rowIdPayloadLength).ToArray();
            offset += rowIdPayloadLength;
            groups.Add(new BucketGroup(components, rowIdPayload));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeGroups(int componentCount, IReadOnlyList<BucketGroup> groups)
    {
        int totalLength = HeaderSize;
        for (int i = 0; i < groups.Count; i++)
        {
            totalLength += GetEncodedKeySize(groups[i].KeyComponents);
            totalLength += sizeof(int);
            totalLength += groups[i].RowIdPayload.Length;
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(totalLength);
        MagicBytes.CopyTo(payload);

        int offset = MagicBytes.Length;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), componentCount);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), groups.Count);
        offset += sizeof(int);

        for (int i = 0; i < groups.Count; i++)
        {
            var group = groups[i];
            for (int componentIndex = 0; componentIndex < group.KeyComponents.Length; componentIndex++)
            {
                var component = group.KeyComponents[componentIndex];
                if (component.Type == DbType.Integer)
                {
                    payload[offset++] = IntegerComponentTag;
                    BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, sizeof(long)), component.AsInteger);
                    offset += sizeof(long);
                    continue;
                }

                if (component.Type != DbType.Text)
                    throw new InvalidOperationException($"Unsupported hashed index component type: {component.Type}.");

                payload[offset++] = TextComponentTag;
                int byteCount = Encoding.UTF8.GetByteCount(component.AsText);
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), byteCount);
                offset += sizeof(int);
                int written = Encoding.UTF8.GetBytes(component.AsText, payload.AsSpan(offset, byteCount));
                offset += written;
            }

            int rowIdCount = RowIdPayloadCodec.GetCount(group.RowIdPayload);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), rowIdCount);
            offset += sizeof(int);
            group.RowIdPayload.CopyTo(payload.AsSpan(offset));
            offset += group.RowIdPayload.Length;
        }

        return payload;
    }

    private static int WriteKeyComponents(byte[] payload, int offset, ReadOnlySpan<DbValue> keyComponents)
    {
        for (int i = 0; i < keyComponents.Length; i++)
        {
            var component = keyComponents[i];
            if (component.Type == DbType.Integer)
            {
                payload[offset++] = IntegerComponentTag;
                BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, sizeof(long)), component.AsInteger);
                offset += sizeof(long);
                continue;
            }

            if (component.Type != DbType.Text)
                throw new InvalidOperationException($"Unsupported hashed index component type: {component.Type}.");

            payload[offset++] = TextComponentTag;
            int byteCount = Encoding.UTF8.GetByteCount(component.AsText);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), byteCount);
            offset += sizeof(int);
            int written = Encoding.UTF8.GetBytes(component.AsText, payload.AsSpan(offset, byteCount));
            offset += written;
        }

        return offset;
    }

    private static ReadOnlySpan<DbValue> GetStoredKeyComponents(
        ReadOnlySpan<DbValue> keyComponents,
        bool omitTrailingInteger)
    {
        if (!omitTrailingInteger ||
            keyComponents.Length <= 1 ||
            keyComponents[^1].Type != DbType.Integer)
        {
            return keyComponents;
        }

        return keyComponents[..^1];
    }

    private static bool TryGetComparableRequestedKeyComponents(
        int storedComponentCount,
        ReadOnlySpan<DbValue> requestedKeyComponents,
        out ReadOnlySpan<DbValue> comparableKeyComponents)
    {
        if (storedComponentCount == requestedKeyComponents.Length)
        {
            comparableKeyComponents = requestedKeyComponents;
            return true;
        }

        if (requestedKeyComponents.Length > 1 &&
            storedComponentCount == requestedKeyComponents.Length - 1 &&
            requestedKeyComponents[^1].Type == DbType.Integer)
        {
            comparableKeyComponents = requestedKeyComponents[..^1];
            return true;
        }

        comparableKeyComponents = default;
        return false;
    }

    private static int GetEncodedKeySize(ReadOnlySpan<DbValue> keyComponents)
    {
        int size = 0;
        for (int i = 0; i < keyComponents.Length; i++)
        {
            size += 1;
            size += keyComponents[i].Type switch
            {
                DbType.Integer => sizeof(long),
                DbType.Text => sizeof(int) + Encoding.UTF8.GetByteCount(keyComponents[i].AsText),
                _ => throw new InvalidOperationException($"Unsupported hashed index component type: {keyComponents[i].Type}."),
            };
        }

        return size;
    }

    private static ReadOnlySpan<byte> GetExpectedTextBytes(
        DbValue expectedComponent,
        byte[][]? expectedTextBytes,
        int componentIndex)
    {
        if (expectedTextBytes is not null &&
            componentIndex < expectedTextBytes.Length &&
            expectedTextBytes[componentIndex] is { } cachedBytes)
        {
            return cachedBytes;
        }

        return expectedComponent.Type == DbType.Text
            ? Encoding.UTF8.GetBytes(expectedComponent.AsText)
            : ReadOnlySpan<byte>.Empty;
    }

    private static bool ComponentsEqual(ReadOnlySpan<DbValue> left, ReadOnlySpan<DbValue> right)
    {
        if (left.Length != right.Length)
            return false;

        for (int i = 0; i < left.Length; i++)
        {
            if (DbValue.Compare(left[i], right[i]) != 0)
                return false;
        }

        return true;
    }

    private static ReadOnlySpan<byte> MagicBytes => "CSDBHIX1"u8;

    internal readonly record struct BucketGroup(DbValue[] KeyComponents, byte[] RowIdPayload);
}
