using System.Text;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class OrderedTextIndexKeyCodecTests
{
    [Theory]
    [InlineData("")]
    [InlineData("A")]
    [InlineData("Alpha")]
    [InlineData("Alphabet")]
    [InlineData("abcdef")]
    [InlineData("abcdefg")]
    [InlineData("hello world")]
    [InlineData("cafe")]
    [InlineData("café")]
    [InlineData("mañana")]
    [InlineData("東京")]
    [InlineData("😀")]
    [InlineData("😀😀")]
    [InlineData("a😀b")]
    public void ComputeKey_MatchesReferenceUtf8Packing(string text)
    {
        long expected = ComputeReferenceKey(text);

        Assert.Equal(expected, OrderedTextIndexKeyCodec.ComputeKey(text));
        Assert.Equal(expected, OrderedTextIndexKeyCodec.ComputeKey(Encoding.UTF8.GetBytes(text)));
    }

    private static long ComputeReferenceKey(string text)
    {
        const int maxPrefixBytes = 6;
        const int bitsPerSymbol = 9;

        byte[] utf8 = Encoding.UTF8.GetBytes(text);
        int symbolCount = Math.Min(utf8.Length, maxPrefixBytes);
        ulong packed = 0;

        for (int i = 0; i < symbolCount; i++)
            packed = (packed << bitsPerSymbol) | (uint)(utf8[i] + 1);

        if (utf8.Length < maxPrefixBytes)
            packed <<= bitsPerSymbol;

        int remainingSymbols = maxPrefixBytes - symbolCount - (utf8.Length < maxPrefixBytes ? 1 : 0);
        if (remainingSymbols > 0)
            packed <<= remainingSymbols * bitsPerSymbol;

        return unchecked((long)packed);
    }
}
