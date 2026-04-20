using System.Buffers;
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

internal enum AppendableHashedIndexPayloadFormat
{
    InlineMutableState = 1,
    ExternalChainState = 2,
}

internal readonly record struct AppendableHashedIndexPayloadMetadata(
    AppendableHashedIndexPayloadFormat Format,
    int KeyComponentsOffset,
    uint FirstPageId,
    uint LastPageId,
    int RowCount,
    long LastRowId,
    bool IsSortedAscending,
    AppendableChainEncoding ChainEncoding);

internal readonly record struct AppendableHashedIndexPayloadReference(
    DbValue[] KeyComponents,
    AppendableHashedIndexPayloadMetadata Metadata);

internal static class AppendableHashedIndexPayloadCodec
{
    private const byte IntegerComponentTag = 1;
    private const byte TextComponentTag = 2;
    private const byte SortedAscendingFlag = 1;

    private static ReadOnlySpan<byte> InlineMagicBytes => "CSDBHAP1"u8;
    private static ReadOnlySpan<byte> ExternalMagicBytes => "CSDBHAP2"u8;

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => IsInlineEncoded(payload) || IsExternalEncoded(payload);

    public static byte[] Encode(
        ReadOnlySpan<DbValue> keyComponents,
        uint firstPageId,
        uint lastPageId,
        int rowCount,
        long lastRowId,
        bool isSortedAscending)
    {
        int keySize = GetEncodedKeySize(keyComponents);
        byte[] payload = AllocatePayload(
            InlineMagicBytes,
            keySize,
            firstPageId,
            lastPageId,
            rowCount,
            lastRowId,
            isSortedAscending,
            out int keyOffset);
        WriteKeyComponents(payload.AsSpan(keyOffset), keyComponents);
        return payload;
    }

    public static byte[] Encode(
        ReadOnlySpan<byte> encodedKeyComponents,
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
        if (encodedKeyComponents.Length == 0)
            throw new ArgumentOutOfRangeException(nameof(encodedKeyComponents));

        byte[] payload = AllocatePayload(
            InlineMagicBytes,
            encodedKeyComponents.Length,
            firstPageId,
            lastPageId,
            rowCount,
            lastRowId,
            isSortedAscending,
            out int keyOffset);
        encodedKeyComponents.CopyTo(payload.AsSpan(keyOffset));
        return payload;
    }

    public static byte[] EncodeExternal(
        ReadOnlySpan<DbValue> keyComponents,
        uint firstPageId)
    {
        ArgumentOutOfRangeException.ThrowIfZero(firstPageId);
        if (keyComponents.Length == 0)
            throw new ArgumentOutOfRangeException(nameof(keyComponents));

        int keySize = GetEncodedKeySize(keyComponents);
        int totalLength = ExternalMagicBytes.Length + sizeof(uint) + keySize;
        byte[] payload = GC.AllocateUninitializedArray<byte>(totalLength);
        ExternalMagicBytes.CopyTo(payload);

        int offset = ExternalMagicBytes.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, sizeof(uint)), firstPageId);
        offset += sizeof(uint);
        WriteKeyComponents(payload.AsSpan(offset), keyComponents);
        return payload;
    }

    public static bool TryDecode(ReadOnlySpan<byte> payload, out AppendableHashedIndexPayload decoded)
    {
        decoded = default;
        if (!IsInlineEncoded(payload))
            return false;

        int offset = InlineMagicBytes.Length;
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

    public static bool TryDecodeReference(
        ReadOnlySpan<byte> payload,
        out AppendableHashedIndexPayloadReference decoded)
    {
        decoded = default;
        if (!TryDecodeMetadata(payload, out AppendableHashedIndexPayloadMetadata metadata))
            return false;

        if (!TryReadKeyComponents(payload[metadata.KeyComponentsOffset..], out DbValue[]? keyComponents, out int bytesRead) ||
            keyComponents == null ||
            bytesRead != payload.Length - metadata.KeyComponentsOffset)
        {
            return false;
        }

        decoded = new AppendableHashedIndexPayloadReference(keyComponents, metadata);
        return true;
    }

    public static bool TryDecodeMetadata(
        ReadOnlySpan<byte> payload,
        out AppendableHashedIndexPayloadMetadata metadata)
    {
        metadata = default;
        if (IsInlineEncoded(payload))
        {
            int offset = InlineMagicBytes.Length;
            bool isSortedAscending = (payload[offset++] & SortedAscendingFlag) != 0;
            int rowCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            uint firstPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, sizeof(uint)));
            offset += sizeof(uint);
            uint lastPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, sizeof(uint)));
            offset += sizeof(uint);
            long lastRowId = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, sizeof(long)));
            offset += sizeof(long);

            if (rowCount <= 0 || firstPageId == 0 || lastPageId == 0 || offset >= payload.Length)
                return false;

            metadata = new AppendableHashedIndexPayloadMetadata(
                AppendableHashedIndexPayloadFormat.InlineMutableState,
                offset,
                firstPageId,
                lastPageId,
                rowCount,
                lastRowId,
                isSortedAscending,
                AppendableChainEncoding.Fixed64);
            return true;
        }

        if (!IsExternalEncoded(payload))
            return false;

        int externalOffset = ExternalMagicBytes.Length;
        uint externalFirstPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(externalOffset, sizeof(uint)));
        externalOffset += sizeof(uint);
        if (externalFirstPageId == 0 || externalOffset >= payload.Length)
            return false;

        metadata = new AppendableHashedIndexPayloadMetadata(
            AppendableHashedIndexPayloadFormat.ExternalChainState,
            externalOffset,
            externalFirstPageId,
            LastPageId: 0,
            RowCount: 0,
            LastRowId: 0,
            IsSortedAscending: false,
            ChainEncoding: AppendableChainEncoding.Fixed64);
        return true;
    }

    public static bool EncodedKeyComponentsEqual(
        ReadOnlySpan<byte> encodedKeyComponents,
        ReadOnlySpan<DbValue> keyComponents)
    {
        if (keyComponents.Length == 0)
            return false;

        int offset = 0;
        int componentIndex = 0;
        while (offset < encodedKeyComponents.Length)
        {
            if (componentIndex >= keyComponents.Length)
                return false;

            DbValue component = keyComponents[componentIndex];
            byte tag = encodedKeyComponents[offset++];
            switch (component.Type)
            {
                case DbType.Integer when tag == IntegerComponentTag:
                    if (offset + sizeof(long) > encodedKeyComponents.Length)
                        return false;

                    if (BinaryPrimitives.ReadInt64LittleEndian(
                            encodedKeyComponents.Slice(offset, sizeof(long))) != component.AsInteger)
                    {
                        return false;
                    }

                    offset += sizeof(long);
                    break;

                case DbType.Text when tag == TextComponentTag:
                    if (offset + sizeof(int) > encodedKeyComponents.Length)
                        return false;

                    int textByteLength = BinaryPrimitives.ReadInt32LittleEndian(
                        encodedKeyComponents.Slice(offset, sizeof(int)));
                    offset += sizeof(int);
                    if (textByteLength < 0 || offset + textByteLength > encodedKeyComponents.Length)
                        return false;

                    if (!Utf8BytesEqual(component.AsText, encodedKeyComponents.Slice(offset, textByteLength)))
                        return false;

                    offset += textByteLength;
                    break;

                default:
                    return false;
            }

            componentIndex++;
        }

        return componentIndex == keyComponents.Length ||
               (componentIndex == keyComponents.Length - 1 &&
                keyComponents[^1].Type == DbType.Integer);
    }

    private static byte[] AllocatePayload(
        ReadOnlySpan<byte> magicBytes,
        int keySize,
        uint firstPageId,
        uint lastPageId,
        int rowCount,
        long lastRowId,
        bool isSortedAscending,
        out int keyOffset)
    {
        int totalLength = magicBytes.Length + 1 + sizeof(int) + (sizeof(uint) * 2) + sizeof(long) + keySize;
        byte[] payload = GC.AllocateUninitializedArray<byte>(totalLength);
        magicBytes.CopyTo(payload);

        int offset = magicBytes.Length;
        payload[offset++] = isSortedAscending ? SortedAscendingFlag : (byte)0;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, sizeof(int)), rowCount);
        offset += sizeof(int);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, sizeof(uint)), firstPageId);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, sizeof(uint)), lastPageId);
        offset += sizeof(uint);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, sizeof(long)), lastRowId);
        offset += sizeof(long);
        keyOffset = offset;
        return payload;
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

    private static bool IsInlineEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= InlineMagicBytes.Length + 1 + sizeof(int) + (sizeof(uint) * 2) + sizeof(long) + 1 &&
           payload[..InlineMagicBytes.Length].SequenceEqual(InlineMagicBytes);

    private static bool IsExternalEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= ExternalMagicBytes.Length + sizeof(uint) + 1 &&
           payload[..ExternalMagicBytes.Length].SequenceEqual(ExternalMagicBytes);

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

    private static bool Utf8BytesEqual(string text, ReadOnlySpan<byte> encodedBytes)
    {
        int byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount != encodedBytes.Length)
            return false;

        if (byteCount == 0)
            return true;

        if (byteCount <= 256)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            int written = Encoding.UTF8.GetBytes(text.AsSpan(), buffer);
            return encodedBytes.SequenceEqual(buffer[..written]);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            int written = Encoding.UTF8.GetBytes(text.AsSpan(), rented.AsSpan(0, byteCount));
            return encodedBytes.SequenceEqual(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
