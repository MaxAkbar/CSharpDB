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
    private const byte RealComponentTag = 3;
    private const byte DecimalComponentTag = 4;
    private const byte BlobComponentTag = 5;
    private const byte BitStringComponentTag = 6;
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

                case DbType.Real when tag == RealComponentTag:
                    if (offset + sizeof(long) > encodedKeyComponents.Length)
                        return false;

                    if (BinaryPrimitives.ReadInt64LittleEndian(
                            encodedKeyComponents.Slice(offset, sizeof(long))) !=
                        RealIndexKeyCodec.GetCanonicalBits(component.AsReal))
                    {
                        return false;
                    }

                    offset += sizeof(long);
                    break;

                case DbType.Decimal when tag == DecimalComponentTag:
                    if (offset + sizeof(long) + sizeof(byte) > encodedKeyComponents.Length)
                        return false;

                    if (BinaryPrimitives.ReadInt64LittleEndian(
                            encodedKeyComponents.Slice(offset, sizeof(long))) != component.DecimalCoefficient)
                    {
                        return false;
                    }

                    offset += sizeof(long);
                    if (encodedKeyComponents[offset++] != component.DecimalScale)
                        return false;
                    break;

                case DbType.Blob when tag == BlobComponentTag && !component.IsBitString:
                    if (offset + sizeof(int) > encodedKeyComponents.Length)
                        return false;
                    int blobLength = BinaryPrimitives.ReadInt32LittleEndian(
                        encodedKeyComponents.Slice(offset, sizeof(int)));
                    offset += sizeof(int);
                    if (blobLength < 0 || offset + blobLength > encodedKeyComponents.Length ||
                        !encodedKeyComponents.Slice(offset, blobLength).SequenceEqual(component.AsBlob))
                    {
                        return false;
                    }
                    offset += blobLength;
                    break;

                case DbType.Blob when tag == BitStringComponentTag && component.IsBitString:
                    if (offset + (sizeof(int) * 2) > encodedKeyComponents.Length)
                        return false;
                    int bitLength = BinaryPrimitives.ReadInt32LittleEndian(
                        encodedKeyComponents.Slice(offset, sizeof(int)));
                    offset += sizeof(int);
                    int packedLength = BinaryPrimitives.ReadInt32LittleEndian(
                        encodedKeyComponents.Slice(offset, sizeof(int)));
                    offset += sizeof(int);
                    if (bitLength != component.BitLength ||
                        packedLength < 0 ||
                        offset + packedLength > encodedKeyComponents.Length ||
                        !encodedKeyComponents.Slice(offset, packedLength).SequenceEqual(component.AsBlob))
                    {
                        return false;
                    }
                    offset += packedLength;
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
                DbType.Real => sizeof(long),
                DbType.Decimal => sizeof(long) + sizeof(byte),
                DbType.Text => sizeof(int) + Encoding.UTF8.GetByteCount(keyComponents[i].AsText),
                DbType.Blob =>
                    sizeof(int) +
                    keyComponents[i].AsBlob.Length +
                    (keyComponents[i].IsBitString ? sizeof(int) : 0),
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

            if (component.Type == DbType.Real)
            {
                destination[offset++] = RealComponentTag;
                BinaryPrimitives.WriteInt64LittleEndian(
                    destination.Slice(offset, sizeof(long)),
                    RealIndexKeyCodec.GetCanonicalBits(component.AsReal));
                offset += sizeof(long);
                continue;
            }

            if (component.Type == DbType.Decimal)
            {
                destination[offset++] = DecimalComponentTag;
                BinaryPrimitives.WriteInt64LittleEndian(
                    destination.Slice(offset, sizeof(long)),
                    component.DecimalCoefficient);
                offset += sizeof(long);
                destination[offset++] = checked((byte)component.DecimalScale);
                continue;
            }

            if (component.Type == DbType.Blob)
            {
                destination[offset++] = component.IsBitString
                    ? BitStringComponentTag
                    : BlobComponentTag;
                if (component.IsBitString)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(
                        destination.Slice(offset, sizeof(int)),
                        component.BitLength);
                    offset += sizeof(int);
                }
                byte[] blob = component.AsBlob;
                BinaryPrimitives.WriteInt32LittleEndian(
                    destination.Slice(offset, sizeof(int)),
                    blob.Length);
                offset += sizeof(int);
                blob.CopyTo(destination.Slice(offset));
                offset += blob.Length;
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

                case RealComponentTag:
                    if (bytesRead + sizeof(long) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }

                    components.Add(DbValue.FromReal(
                        BitConverter.Int64BitsToDouble(
                            BinaryPrimitives.ReadInt64LittleEndian(
                                payload.Slice(bytesRead, sizeof(long))))));
                    bytesRead += sizeof(long);
                    break;

                case DecimalComponentTag:
                    if (bytesRead + sizeof(long) + sizeof(byte) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }

                    long coefficient = BinaryPrimitives.ReadInt64LittleEndian(
                        payload.Slice(bytesRead, sizeof(long)));
                    bytesRead += sizeof(long);
                    int scale = payload[bytesRead++];
                    try
                    {
                        components.Add(DbValue.FromDecimalParts(coefficient, scale));
                    }
                    catch (ArgumentException)
                    {
                        keyComponents = null;
                        return false;
                    }
                    catch (OverflowException)
                    {
                        keyComponents = null;
                        return false;
                    }
                    break;

                case BlobComponentTag:
                    if (bytesRead + sizeof(int) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }
                    int blobLength = BinaryPrimitives.ReadInt32LittleEndian(
                        payload.Slice(bytesRead, sizeof(int)));
                    bytesRead += sizeof(int);
                    if (blobLength < 0 || bytesRead + blobLength > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }
                    components.Add(DbValue.FromBlob(payload.Slice(bytesRead, blobLength).ToArray()));
                    bytesRead += blobLength;
                    break;

                case BitStringComponentTag:
                    if (bytesRead + (sizeof(int) * 2) > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }
                    int bitLength = BinaryPrimitives.ReadInt32LittleEndian(
                        payload.Slice(bytesRead, sizeof(int)));
                    bytesRead += sizeof(int);
                    int packedLength = BinaryPrimitives.ReadInt32LittleEndian(
                        payload.Slice(bytesRead, sizeof(int)));
                    bytesRead += sizeof(int);
                    if (bitLength <= 0 ||
                        packedLength != (bitLength / 8) + (bitLength % 8 == 0 ? 0 : 1) ||
                        bytesRead + packedLength > payload.Length)
                    {
                        keyComponents = null;
                        return false;
                    }
                    try
                    {
                        components.Add(DbValue.FromBitString(
                            payload.Slice(bytesRead, packedLength).ToArray(),
                            bitLength));
                    }
                    catch (ArgumentException)
                    {
                        keyComponents = null;
                        return false;
                    }
                    bytesRead += packedLength;
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
