using System.Text;

namespace CSharpDB.Storage.Indexing;

internal static class OrderedTextIndexKeyCodec
{
    private const int MaxPrefixBytes = 6;
    private const int BitsPerSymbol = 9;

    public static long ComputeKey(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        Span<byte> utf8Prefix = stackalloc byte[MaxPrefixBytes];
        Span<byte> runeBytes = stackalloc byte[4];
        int totalByteCount = 0;

        foreach (Rune rune in text.EnumerateRunes())
        {
            if (!rune.TryEncodeToUtf8(runeBytes, out int bytesWritten))
                throw new InvalidOperationException("Could not encode rune to UTF-8.");

            for (int i = 0; i < bytesWritten; i++)
            {
                if (totalByteCount < MaxPrefixBytes)
                    utf8Prefix[totalByteCount] = runeBytes[i];

                totalByteCount++;
                if (totalByteCount > MaxPrefixBytes)
                    goto Pack;
            }
        }

Pack:
        int symbolCount = Math.Min(totalByteCount, MaxPrefixBytes);
        ulong packed = 0;

        for (int i = 0; i < symbolCount; i++)
            packed = (packed << BitsPerSymbol) | (uint)(utf8Prefix[i] + 1);

        if (totalByteCount < MaxPrefixBytes)
            packed <<= BitsPerSymbol;

        int remainingSymbols = MaxPrefixBytes - symbolCount - (totalByteCount < MaxPrefixBytes ? 1 : 0);
        if (remainingSymbols > 0)
            packed <<= remainingSymbols * BitsPerSymbol;

        return unchecked((long)packed);
    }
}
