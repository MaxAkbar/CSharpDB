namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Variable-length integer encoding (LEB128-style, unsigned).
/// Small values encode in 1 byte; up to 64-bit values supported.
/// </summary>
public static class Varint
{
    public static int Write(Span<byte> buffer, ulong value)
    {
        int i = 0;
        do
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0) b |= 0x80;
            buffer[i++] = b;
        } while (value != 0);
        return i;
    }

    public static int Write(Span<byte> buffer, long value) =>
        Write(buffer, (ulong)value);

    public static ulong Read(ReadOnlySpan<byte> buffer, out int bytesRead)
    {
        ulong result = 0;
        int shift = 0;
        bytesRead = 0;
        byte b;
        do
        {
            b = buffer[bytesRead++];
            result |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && bytesRead < 10);
        return result;
    }

    public static long ReadSigned(ReadOnlySpan<byte> buffer, out int bytesRead) =>
        (long)Read(buffer, out bytesRead);

    /// <summary>
    /// Returns how many bytes are needed to encode this value.
    /// </summary>
    public static int SizeOf(ulong value)
    {
        int size = 1;
        while (value > 0x7F)
        {
            value >>= 7;
            size++;
        }
        return size;
    }

    public static int SizeOf(long value) => SizeOf((ulong)value);
}
