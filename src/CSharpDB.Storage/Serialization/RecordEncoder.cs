using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Encodes/decodes a row of DbValues to/from a compact binary format.
/// Format: [columnCount:varint] then for each column: [typeTag:1 byte] [value data]
///   - Null: no data
///   - Integer: 8 bytes (long, little-endian)
///   - Real: 8 bytes (double, little-endian)
///   - Text: [length:varint] [utf8 bytes]
///   - Blob: [length:varint] [raw bytes]
/// </summary>
public static class RecordEncoder
{
    private static readonly Encoding Utf8 = Encoding.UTF8;
    private const int TextCacheCapacity = 64;
    private const int MaxCachedTextByteLength = 16;
    private const int EncodedTextCacheCapacity = 128;
    private const int MaxEncodedTextByteLength = 128;
    [ThreadStatic] private static TextCacheEntry[]? s_textCache;
    [ThreadStatic] private static EncodedTextCacheEntry[]? s_encodedTextCache;

    public static byte[] Encode(ReadOnlySpan<DbValue> values)
    {
        int size = GetEncodedLength(values);
        byte[] buffer = GC.AllocateUninitializedArray<byte>(size);
        EncodeInto(values, buffer);
        return buffer;
    }

    public static int GetEncodedLength(ReadOnlySpan<DbValue> values)
    {
        int size = Varint.SizeOf((ulong)values.Length);
        foreach (DbValue value in values)
            size += 1 + ValueDataSize(value);

        return size;
    }

    public static int EncodeInto(ReadOnlySpan<DbValue> values, Span<byte> destination)
    {
        return EncodeInto(values, destination, GetEncodedLength(values));
    }

    public static int EncodeInto(ReadOnlySpan<DbValue> values, Span<byte> destination, int encodedLength)
    {
        if (destination.Length < encodedLength)
        {
            throw new ArgumentException(
                "Destination buffer is too small for the encoded record.",
                nameof(destination));
        }

        int pos = Varint.Write(destination, (ulong)values.Length);

        foreach (DbValue v in values)
        {
            destination[pos++] = (byte)v.Type;
            switch (v.Type)
            {
                case DbType.Null:
                    break;
                case DbType.Integer:
                    BinaryPrimitives.WriteInt64LittleEndian(destination[pos..], v.AsInteger);
                    pos += 8;
                    break;
                case DbType.Real:
                    BinaryPrimitives.WriteInt64LittleEndian(
                        destination[pos..],
                        BitConverter.DoubleToInt64Bits(v.AsReal));
                    pos += 8;
                    break;
                case DbType.Text:
                {
                    string text = v.AsText;
                    if (TryGetEncodedTextBytes(text, out byte[]? encodedTextBytes) &&
                        encodedTextBytes is { } cachedTextBytes)
                    {
                        pos += Varint.Write(destination[pos..], (ulong)cachedTextBytes.Length);
                        cachedTextBytes.CopyTo(destination[pos..]);
                        pos += cachedTextBytes.Length;
                        break;
                    }

                    int byteCount = Utf8.GetByteCount(text);
                    pos += Varint.Write(destination[pos..], (ulong)byteCount);
                    pos += Utf8.GetBytes(text.AsSpan(), destination.Slice(pos, byteCount));
                    break;
                }
                case DbType.Blob:
                {
                    var blob = v.AsBlob;
                    pos += Varint.Write(destination[pos..], (ulong)blob.Length);
                    blob.CopyTo(destination[pos..]);
                    pos += blob.Length;
                    break;
                }
            }
        }

        return pos;
    }

    public static DbValue[] Decode(ReadOnlySpan<byte> buffer)
    {
        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        var values = new DbValue[count];
        for (int i = 0; i < count; i++)
        {
            var type = (DbType)buffer[pos++];
            switch (type)
            {
                case DbType.Null:
                    values[i] = DbValue.Null;
                    break;
                case DbType.Integer:
                    values[i] = DbValue.FromInteger(BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8)));
                    pos += 8;
                    break;
                case DbType.Real:
                    values[i] = DbValue.FromReal(BitConverter.Int64BitsToDouble(
                        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8))));
                    pos += 8;
                    break;
                case DbType.Text:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    string text = DecodeText(buffer.Slice(pos, len));
                    values[i] = DbValue.FromText(text);
                    pos += len;
                    break;
                }
                case DbType.Blob:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    byte[] blob = len == 0
                        ? Array.Empty<byte>()
                        : buffer.Slice(pos, len).ToArray();
                    values[i] = DbValue.FromBlob(blob);
                    pos += len;
                    break;
                }
            }
        }

        return values;
    }

    /// <summary>
    /// Decode values into a caller-provided destination buffer.
    /// Returns the number of decoded columns (up to destination length).
    /// </summary>
    public static int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
    {
        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        int decodeCount = Math.Min(count, destination.Length);
        if (decodeCount <= 0)
            return 0;

        for (int i = 0; i < decodeCount; i++)
        {
            var type = (DbType)buffer[pos++];
            switch (type)
            {
                case DbType.Null:
                    destination[i] = DbValue.Null;
                    break;
                case DbType.Integer:
                    destination[i] = DbValue.FromInteger(
                        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8)));
                    pos += 8;
                    break;
                case DbType.Real:
                    destination[i] = DbValue.FromReal(BitConverter.Int64BitsToDouble(
                        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8))));
                    pos += 8;
                    break;
                case DbType.Text:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    destination[i] = DbValue.FromText(DecodeText(buffer.Slice(pos, len)));
                    pos += len;
                    break;
                }
                case DbType.Blob:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    destination[i] = DbValue.FromBlob(len == 0
                        ? Array.Empty<byte>()
                        : buffer.Slice(pos, len).ToArray());
                    pos += len;
                    break;
                }
            }
        }

        return decodeCount;
    }

    /// <summary>
    /// Decode only the requested (sorted, unique) column indices into destination slots by original ordinal.
    /// Unrequested columns are skipped without materializing values.
    /// </summary>
    public static void DecodeSelectedInto(
        ReadOnlySpan<byte> buffer,
        Span<DbValue> destination,
        ReadOnlySpan<int> selectedColumnIndices)
    {
        if (selectedColumnIndices.IsEmpty)
            return;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        int selectedCursor = 0;
        int targetColumn = selectedColumnIndices[selectedCursor];
        for (int columnIndex = 0; columnIndex < count && selectedCursor < selectedColumnIndices.Length; columnIndex++)
        {
            var type = (DbType)buffer[pos++];
            if (columnIndex != targetColumn)
            {
                SkipValue(type, buffer, ref pos);
                continue;
            }

            bool canWrite = (uint)columnIndex < (uint)destination.Length;
            switch (type)
            {
                case DbType.Null:
                    if (canWrite)
                        destination[columnIndex] = DbValue.Null;
                    break;
                case DbType.Integer:
                {
                    long value = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                    if (canWrite)
                        destination[columnIndex] = DbValue.FromInteger(value);
                    pos += 8;
                    break;
                }
                case DbType.Real:
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                    if (canWrite)
                        destination[columnIndex] = DbValue.FromReal(BitConverter.Int64BitsToDouble(bits));
                    pos += 8;
                    break;
                }
                case DbType.Text:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    if (canWrite)
                        destination[columnIndex] = DbValue.FromText(DecodeText(buffer.Slice(pos, len)));
                    pos += len;
                    break;
                }
                case DbType.Blob:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    if (canWrite)
                    {
                        destination[columnIndex] = DbValue.FromBlob(len == 0
                            ? Array.Empty<byte>()
                            : buffer.Slice(pos, len).ToArray());
                    }
                    pos += len;
                    break;
                }
                default:
                    SkipValue(type, buffer, ref pos);
                    break;
            }

            selectedCursor++;
            if (selectedCursor < selectedColumnIndices.Length)
                targetColumn = selectedColumnIndices[selectedCursor];
        }
    }

    /// <summary>
    /// Decode only the requested (sorted, unique) column indices into a compact destination
    /// where destination slot i corresponds to selectedColumnIndices[i].
    /// </summary>
    public static void DecodeSelectedCompactInto(
        ReadOnlySpan<byte> buffer,
        Span<DbValue> destination,
        ReadOnlySpan<int> selectedColumnIndices)
    {
        if (selectedColumnIndices.IsEmpty || destination.IsEmpty)
            return;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        int selectedCursor = 0;
        int targetColumn = selectedColumnIndices[selectedCursor];
        for (int columnIndex = 0; columnIndex < count && selectedCursor < selectedColumnIndices.Length; columnIndex++)
        {
            var type = (DbType)buffer[pos++];
            if (columnIndex != targetColumn)
            {
                SkipValue(type, buffer, ref pos);
                continue;
            }

            bool canWrite = (uint)selectedCursor < (uint)destination.Length;
            switch (type)
            {
                case DbType.Null:
                    if (canWrite)
                        destination[selectedCursor] = DbValue.Null;
                    break;
                case DbType.Integer:
                {
                    long value = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                    if (canWrite)
                        destination[selectedCursor] = DbValue.FromInteger(value);
                    pos += 8;
                    break;
                }
                case DbType.Real:
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                    if (canWrite)
                        destination[selectedCursor] = DbValue.FromReal(BitConverter.Int64BitsToDouble(bits));
                    pos += 8;
                    break;
                }
                case DbType.Text:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    if (canWrite)
                        destination[selectedCursor] = DbValue.FromText(DecodeText(buffer.Slice(pos, len)));
                    pos += len;
                    break;
                }
                case DbType.Blob:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    if (canWrite)
                    {
                        destination[selectedCursor] = DbValue.FromBlob(len == 0
                            ? Array.Empty<byte>()
                            : buffer.Slice(pos, len).ToArray());
                    }
                    pos += len;
                    break;
                }
                default:
                    SkipValue(type, buffer, ref pos);
                    if (canWrite)
                        destination[selectedCursor] = DbValue.Null;
                    break;
            }

            selectedCursor++;
            if (selectedCursor < selectedColumnIndices.Length)
                targetColumn = selectedColumnIndices[selectedCursor];
        }
    }

    /// <summary>
    /// Decode only the prefix of columns up to maxColumnIndexInclusive.
    /// Columns after that index are skipped without materializing values.
    /// </summary>
    public static DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
    {
        if (maxColumnIndexInclusive < 0)
            return Array.Empty<DbValue>();

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        int decodeCount = Math.Min(count, maxColumnIndexInclusive + 1);
        if (decodeCount <= 0)
            return Array.Empty<DbValue>();

        var values = new DbValue[decodeCount];
        for (int i = 0; i < decodeCount; i++)
        {
            var type = (DbType)buffer[pos++];
            switch (type)
            {
                case DbType.Null:
                    values[i] = DbValue.Null;
                    break;
                case DbType.Integer:
                    values[i] = DbValue.FromInteger(BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8)));
                    pos += 8;
                    break;
                case DbType.Real:
                    values[i] = DbValue.FromReal(BitConverter.Int64BitsToDouble(
                        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8))));
                    pos += 8;
                    break;
                case DbType.Text:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    values[i] = DbValue.FromText(DecodeText(buffer.Slice(pos, len)));
                    pos += len;
                    break;
                }
                case DbType.Blob:
                {
                    int len = (int)Varint.Read(buffer[pos..], out int lb);
                    pos += lb;
                    values[i] = DbValue.FromBlob(len == 0
                        ? Array.Empty<byte>()
                        : buffer.Slice(pos, len).ToArray());
                    pos += len;
                    break;
                }
            }
        }

        return values;
    }

    /// <summary>
    /// Decode a single column by index. If the column does not exist, returns NULL.
    /// </summary>
    public static DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (columnIndex < 0)
            return DbValue.Null;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        if (columnIndex >= count)
            return DbValue.Null;

        for (int i = 0; i < columnIndex; i++)
        {
            var type = (DbType)buffer[pos++];
            SkipValue(type, buffer, ref pos);
        }

        var targetType = (DbType)buffer[pos++];
        switch (targetType)
        {
            case DbType.Null:
                return DbValue.Null;
            case DbType.Integer:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                return DbValue.FromInteger(v);
            }
            case DbType.Real:
            {
                long bits = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                return DbValue.FromReal(BitConverter.Int64BitsToDouble(bits));
            }
            case DbType.Text:
            {
                int len = (int)Varint.Read(buffer[pos..], out int lb);
                pos += lb;
                string text = DecodeText(buffer.Slice(pos, len));
                return DbValue.FromText(text);
            }
            case DbType.Blob:
            {
                int len = (int)Varint.Read(buffer[pos..], out int lb);
                pos += lb;
                byte[] blob = len == 0
                    ? Array.Empty<byte>()
                    : buffer.Slice(pos, len).ToArray();
                return DbValue.FromBlob(blob);
            }
            default:
                return DbValue.Null;
        }
    }

    /// <summary>
    /// Fast path for TEXT equality checks without materializing a managed string.
    /// Returns true only when the target column exists and is TEXT.
    /// </summary>
    public static bool TryColumnTextEquals(
        ReadOnlySpan<byte> buffer,
        int columnIndex,
        ReadOnlySpan<byte> expectedUtf8,
        out bool equals)
    {
        equals = false;

        if (columnIndex < 0)
            return false;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        if (columnIndex >= count)
            return false;

        for (int i = 0; i < columnIndex; i++)
        {
            var type = (DbType)buffer[pos++];
            SkipValue(type, buffer, ref pos);
        }

        var targetType = (DbType)buffer[pos++];
        if (targetType != DbType.Text)
            return false;

        int len = (int)Varint.Read(buffer[pos..], out int lb);
        pos += lb;

        equals = len == expectedUtf8.Length &&
            buffer.Slice(pos, len).SequenceEqual(expectedUtf8);

        return true;
    }

    /// <summary>
    /// Returns true when the requested column is missing or NULL, without materializing the value.
    /// </summary>
    public static bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
    {
        if (columnIndex < 0)
            return true;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        if (columnIndex >= count)
            return true;

        for (int i = 0; i < columnIndex; i++)
        {
            var type = (DbType)buffer[pos++];
            SkipValue(type, buffer, ref pos);
        }

        return (DbType)buffer[pos] == DbType.Null;
    }

    /// <summary>
    /// Decode a numeric column without constructing DbValue.
    /// Returns false when the column is missing or NULL.
    /// Throws when the target column contains a non-numeric value.
    /// </summary>
    public static bool TryDecodeNumericColumn(
        ReadOnlySpan<byte> buffer,
        int columnIndex,
        out long intValue,
        out double realValue,
        out bool isReal)
    {
        intValue = 0;
        realValue = 0;
        isReal = false;

        if (columnIndex < 0)
            return false;

        int pos = 0;
        int count = (int)Varint.Read(buffer, out int bytesRead);
        pos += bytesRead;

        if (columnIndex >= count)
            return false;

        for (int i = 0; i < columnIndex; i++)
        {
            var type = (DbType)buffer[pos++];
            SkipValue(type, buffer, ref pos);
        }

        var targetType = (DbType)buffer[pos++];
        switch (targetType)
        {
            case DbType.Null:
                return false;
            case DbType.Integer:
                intValue = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8));
                return true;
            case DbType.Real:
                isReal = true;
                realValue = BitConverter.Int64BitsToDouble(
                    BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pos, 8)));
                return true;
            default:
                throw new InvalidOperationException($"Cannot read {targetType} as Integer.");
        }
    }

    private static void SkipValue(DbType type, ReadOnlySpan<byte> buffer, ref int pos)
    {
        switch (type)
        {
            case DbType.Null:
                return;
            case DbType.Integer:
            case DbType.Real:
                pos += 8;
                return;
            case DbType.Text:
            case DbType.Blob:
            {
                int len = (int)Varint.Read(buffer[pos..], out int lb);
                pos += lb + len;
                return;
            }
        }
    }

    private static string DecodeText(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return string.Empty;

        if (CanUseTextCache(utf8))
            return DecodeTextCached(utf8);

        return Utf8.GetString(utf8);
    }

    private static string DecodeTextCached(ReadOnlySpan<byte> utf8)
    {
        var cache = s_textCache ??= new TextCacheEntry[TextCacheCapacity];
        int hash = ComputeHash(utf8);
        int slot = hash & (TextCacheCapacity - 1);
        ref var entry = ref cache[slot];
        PackSmallTextKey(utf8, out ulong keyLo, out ulong keyHi);

        if (entry.Hash == hash &&
            entry.Length == utf8.Length &&
            entry.KeyLo == keyLo &&
            entry.KeyHi == keyHi &&
            entry.Text != null)
        {
            return entry.Text;
        }

        string text = Utf8.GetString(utf8);
        entry.Hash = hash;
        entry.Length = (byte)utf8.Length;
        entry.KeyLo = keyLo;
        entry.KeyHi = keyHi;
        entry.Text = text;
        return text;
    }

    private static bool CanUseTextCache(ReadOnlySpan<byte> utf8)
    {
        if ((uint)utf8.Length > MaxCachedTextByteLength)
            return false;

        // Constrain caching to very small ASCII TitleCase tokens (for repeated enum-like values).
        // This avoids miss-heavy cache churn on high-cardinality text columns.
        byte first = utf8[0];
        if (first < (byte)'A' || first > (byte)'Z')
            return false;

        for (int i = 1; i < utf8.Length; i++)
        {
            byte b = utf8[i];
            if (b < (byte)'a' || b > (byte)'z')
                return false;
        }

        return true;
    }

    private static int ComputeHash(ReadOnlySpan<byte> utf8)
    {
        unchecked
        {
            uint hash = 2166136261;
            for (int i = 0; i < utf8.Length; i++)
                hash = (hash ^ utf8[i]) * 16777619;
            return (int)hash;
        }
    }

    private static void PackSmallTextKey(ReadOnlySpan<byte> utf8, out ulong keyLo, out ulong keyHi)
    {
        keyLo = 0;
        keyHi = 0;

        int loLen = Math.Min(utf8.Length, 8);
        for (int i = 0; i < loLen; i++)
            keyLo |= (ulong)utf8[i] << (i * 8);

        for (int i = 8; i < utf8.Length; i++)
            keyHi |= (ulong)utf8[i] << ((i - 8) * 8);
    }

    private struct TextCacheEntry
    {
        public int Hash;
        public int Length;
        public ulong KeyLo;
        public ulong KeyHi;
        public string? Text;
    }

    private struct EncodedTextCacheEntry
    {
        public int Hash;
        public string? Text;
        public byte[]? Utf8Bytes;
    }

    private static int ValueDataSize(DbValue v)
    {
        switch (v.Type)
        {
            case DbType.Null:
                return 0;
            case DbType.Integer:
            case DbType.Real:
                return 8;
            case DbType.Text:
            {
                int byteCount = TryGetEncodedTextBytes(v.AsText, out byte[]? encodedTextBytes)
                    ? encodedTextBytes!.Length
                    : Utf8.GetByteCount(v.AsText);
                return Varint.SizeOf((ulong)byteCount) + byteCount;
            }
            case DbType.Blob:
            {
                int len = v.AsBlob.Length;
                return Varint.SizeOf((ulong)len) + len;
            }
            default:
                return 0;
        }
    }

    private static bool TryGetEncodedTextBytes(string text, out byte[]? encodedTextBytes)
    {
        encodedTextBytes = null;
        if (string.IsNullOrEmpty(text))
        {
            encodedTextBytes = Array.Empty<byte>();
            return true;
        }

        if (text.Length > MaxEncodedTextByteLength)
            return false;

        EncodedTextCacheEntry[] cache = s_encodedTextCache ??= new EncodedTextCacheEntry[EncodedTextCacheCapacity];
        int hash = RuntimeHelpers.GetHashCode(text);
        int slot = hash & (EncodedTextCacheCapacity - 1);
        ref EncodedTextCacheEntry entry = ref cache[slot];
        if (entry.Hash == hash &&
            ReferenceEquals(entry.Text, text) &&
            entry.Utf8Bytes is { } cachedBytes)
        {
            encodedTextBytes = cachedBytes;
            return true;
        }

        int byteCount = Utf8.GetByteCount(text);
        if (byteCount > MaxEncodedTextByteLength)
            return false;

        byte[] utf8Bytes = GC.AllocateUninitializedArray<byte>(byteCount);
        if (byteCount > 0)
            Utf8.GetBytes(text.AsSpan(), utf8Bytes);

        entry.Hash = hash;
        entry.Text = text;
        entry.Utf8Bytes = utf8Bytes;
        encodedTextBytes = utf8Bytes;
        return true;
    }
}
