namespace CSharpDB.Tests;

public class VarintTests
{
    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(255UL)]
    [InlineData(16384UL)]
    [InlineData(ulong.MaxValue)]
    public void RoundTrip_Unsigned(ulong value)
    {
        Span<byte> buf = stackalloc byte[10];
        int written = Varint.Write(buf, value);
        ulong result = Varint.Read(buf, out int read);
        Assert.Equal(value, result);
        Assert.Equal(written, read);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    public void RoundTrip_Signed(long value)
    {
        Span<byte> buf = stackalloc byte[10];
        int written = Varint.Write(buf, value);
        long result = Varint.ReadSigned(buf, out int read);
        Assert.Equal(value, result);
        Assert.Equal(written, read);
    }

    [Fact]
    public void SizeOf_SmallValues()
    {
        Assert.Equal(1, Varint.SizeOf(0UL));
        Assert.Equal(1, Varint.SizeOf(127UL));
        Assert.Equal(2, Varint.SizeOf(128UL));
    }
}
