using System.Buffers.Binary;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

internal readonly record struct AppendableHashedIndexPayload(
    DbValue[] KeyComponents,
    uint FirstPageId,
    uint LastPageId,
    int RowCount,
    long LastRowId,
    bool IsSortedAscending);

internal static class AppendableHashedIndexPayloadCodec
{
    private const byte IntegerComponentTag = 1;
    private const byte TextComponentTag = 2;
    private const byte SortedAscendingFlag = 1;

    private static ReadOnlySpan<byte> MagicBytes => "CSDBHAP1"u8;

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= MagicBytes.Length + 1 + sizeof(int) + (sizeof(uint) * 2) + sizeof(long) &&
           payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] Encode(
        ReadOnlySpan<DbValue> keyComponents,
        uint firstPageId,
        uint lastPageId,
        int rowCount,
        long lastRowId,
        bool isSortedAscending)
    {
        ArgumentOutOfRangeException.ThrowIfZero(firstPageId);
        ArgumentOutOfRangeException.ThrowIfZero(lastPageId);
        if (rowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        if (keyComponents.Length == 0)
            throw new ArgumentOutOfRangeException(nameof(keyComponents));

        int keySize = GetEncodedKeySize(keyComponents);
        int totalLength = MagicBytes.Length + 1 + sizeof(int) + (sizeof(uint) * 2) + sizeof(long) + keySize;
        byte[] payload = GC.AllocateUninitializedArray<byte>(totalLength);
        MagicBytes.CopyTo(payload);

        int offset = MagicBytes.Length;
        payload[offset++] = isSortedAscending ? SortedAscendingFlag : (byte)0;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), rowCount);
        offset += sizeof(int);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, sizeof(uint)), firstPageId);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, sizeof(uint)), lastPageId);
        offset += sizeof(uint);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, sizeof(long)), lastRowId);
        offset += sizeof(long);
        WriteKeyComponents(payload.AsSpan(offset), keyComponents);
        return payload;
    }

    public static bool TryDecode(ReadOnlySpan<byte> payload, out AppendableHashedIndexPayload decoded)
    {
        decoded = default;
        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        bool isSortedAscending = (payload[offset++] & SortedAscendingFlag) != 0;
        int rowCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        uint firstPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, sizeof(uint)));
        offset += sizeof(uint);
        uint lastPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, sizeof(uint)));
        offset += sizeof(uint);
        long lastRowId = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, sizeof(long)));
        offset += sizeof(long);

        if (rowCount <= 0 || firstPageId == 0 || lastPageId == 0)
            return false;

        if (!TryReadKeyComponents(payload[offset..], out DbValue[]? keyComponents, out int bytesRead) ||
            keyComponents == null)
            return false;

        if (bytesRead != payload.Length - offset)
            return false;

        decoded = new AppendableHashedIndexPayload(
            keyComponents,
            firstPageId,
            lastPageId,
            rowCount,
            lastRowId,
            isSortedAscending);
        return true;
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
                _ => throw new InvalidOperationException($"Unsupported appendable hashed key component type: {keyComponents[i].Type}."),
            };
        }

        return size;
    }

    private static void WriteKeyComponents(Span<byte> destination, ReadOnlySpan<DbValue> keyComponents)
    {
        int offset = 0;
        for (int i = 0; i < keyComponents.Length; i++)
        {
            DbValue component = keyComponents[i];
            if (component.Type == DbType.Integer)
            {
                destination[offset++] = IntegerComponentTag;
                BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, sizeof(long)), component.AsInteger);
                offset += sizeof(long);
                continue;
            }

            if (component.Type != DbType.Text)
                throw new InvalidOperationException($"Unsupported appendable hashed key component type: {component.Type}.");

            destination[offset++] = TextComponentTag;
            int byteCount = Encoding.UTF8.GetByteCount(component.AsText);
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), byteCount);
            offset += sizeof(int);
            offset += Encoding.UTF8.GetBytes(component.AsText, destination.Slice(offset, byteCount));
        }
    }

    private static bool TryReadKeyComponents(
        ReadOnlySpan<byte> payload,
        out DbValue[]? keyComponents,
        out int bytesRead)
    {
        bytesRead = 0;
        var components = new List<DbValue>();
        while (bytesRead < payload.Length)
        {
            byte tag = payload[bytesRead++];
            switch (tag)
            {
                case IntegerComponentTag:
                    if (bytesRead + sizeof(long) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }

                    components.Add(DbValue.FromInteger(
                        BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(bytesRead, sizeof(long)))));
                    bytesRead += sizeof(long);
                    break;

                case TextComponentTag:
                    if (bytesRead + sizeof(int) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }

                    int textByteLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(bytesRead, sizeof(int)));
                    bytesRead += sizeof(int);
                    if (textByteLength < 0 || bytesRead + textByteLength > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }

                    components.Add(DbValue.FromText(
                        Encoding.UTF8.GetString(payload.Slice(bytesRead, textByteLength))));
                    bytesRead += textByteLength;
                    break;

                default:
                    keyComponents = null;
                    return false;
            }
        }

        keyComponents = components.Count == 0 ? null : components.ToArray();
        return keyComponents != null;
    }
}
