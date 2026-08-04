namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Canonicalizes REAL index components so values that compare equal always
/// produce the same persisted key representation.
/// </summary>
internal static class RealIndexKeyCodec
{
    private const long CanonicalNaNBits = 0x7ff8_0000_0000_0000L;

    public static long GetCanonicalBits(double value)
    {
        if (value == 0d)
            return 0L;

        if (double.IsNaN(value))
            return CanonicalNaNBits;

        return BitConverter.DoubleToInt64Bits(value);
    }

    public static double Normalize(double value) =>
        BitConverter.Int64BitsToDouble(GetCanonicalBits(value));
}
