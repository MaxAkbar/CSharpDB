using System.Text;

namespace CSharpDB.Storage.Indexing;

internal static class OrderedTextIndexKeyCodec
{
    private const int MaxPrefixBytes = 6;
    private const int BitsPerSymbol = 9;

    public static long ComputeKey(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        byte[] utf8 = Encoding.UTF8.GetBytes(text);
        int symbolCount = Math.Min(utf8.Length, MaxPrefixBytes);
        ulong packed = 0;

        for (int i = 0; i < symbolCount; i++)
            packed = (packed << BitsPerSymbol) | (uint)(utf8[i] + 1);

        if (utf8.Length < MaxPrefixBytes)
            packed <<= BitsPerSymbol;

        int remainingSymbols = MaxPrefixBytes - symbolCount - (utf8.Length < MaxPrefixBytes ? 1 : 0);
        if (remainingSymbols > 0)
            packed <<= remainingSymbols * BitsPerSymbol;

        return unchecked((long)packed);
    }
}
