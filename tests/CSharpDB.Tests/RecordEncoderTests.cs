using CSharpDB.Core;

namespace CSharpDB.Tests;

public class RecordEncoderTests
{
    [Fact]
    public void RoundTrip_AllTypes()
    {
        var values = new DbValue[]
        {
            DbValue.Null,
            DbValue.FromInteger(42),
            DbValue.FromReal(3.14),
            DbValue.FromText("hello"),
            DbValue.FromBlob(new byte[] { 1, 2, 3 }),
        };

        var encoded = RecordEncoder.Encode(values);
        var decoded = RecordEncoder.Decode(encoded);

        Assert.Equal(values.Length, decoded.Length);
        Assert.True(decoded[0].IsNull);
        Assert.Equal(42, decoded[1].AsInteger);
        Assert.Equal(3.14, decoded[2].AsReal);
        Assert.Equal("hello", decoded[3].AsText);
        Assert.Equal(new byte[] { 1, 2, 3 }, decoded[4].AsBlob);
    }

    [Fact]
    public void RoundTrip_EmptyRow()
    {
        var values = Array.Empty<DbValue>();
        var encoded = RecordEncoder.Encode(values);
        var decoded = RecordEncoder.Decode(encoded);
        Assert.Empty(decoded);
    }

    [Fact]
    public void RoundTrip_LargeText()
    {
        var text = new string('x', 10000);
        var values = new[] { DbValue.FromText(text) };
        var encoded = RecordEncoder.Encode(values);
        var decoded = RecordEncoder.Decode(encoded);
        Assert.Equal(text, decoded[0].AsText);
    }

    [Fact]
    public void DecodeUpTo_Prefix()
    {
        var values = new DbValue[]
        {
            DbValue.FromInteger(7),
            DbValue.FromText("abc"),
            DbValue.FromBlob(new byte[] { 9, 8, 7 }),
        };

        var encoded = RecordEncoder.Encode(values);
        var decoded = RecordEncoder.DecodeUpTo(encoded, 1);

        Assert.Equal(2, decoded.Length);
        Assert.Equal(7, decoded[0].AsInteger);
        Assert.Equal("abc", decoded[1].AsText);
    }

    [Fact]
    public void DecodeUpTo_NegativeIndex_ReturnsEmpty()
    {
        var encoded = RecordEncoder.Encode(new[] { DbValue.FromInteger(1) });
        var decoded = RecordEncoder.DecodeUpTo(encoded, -1);
        Assert.Empty(decoded);
    }

    [Fact]
    public void DecodeUpTo_BeyondRowLength_ReturnsAllColumns()
    {
        var values = new DbValue[]
        {
            DbValue.FromInteger(1),
            DbValue.FromReal(2.5),
            DbValue.FromText("x"),
        };

        var encoded = RecordEncoder.Encode(values);
        var decoded = RecordEncoder.DecodeUpTo(encoded, 99);

        Assert.Equal(values.Length, decoded.Length);
        Assert.Equal(1, decoded[0].AsInteger);
        Assert.Equal(2.5, decoded[1].AsReal);
        Assert.Equal("x", decoded[2].AsText);
    }

    [Fact]
    public void DecodeColumn_ReturnsRequestedColumn()
    {
        var values = new DbValue[]
        {
            DbValue.FromInteger(11),
            DbValue.FromText("abc"),
            DbValue.FromReal(3.5),
        };

        var encoded = RecordEncoder.Encode(values);

        var col0 = RecordEncoder.DecodeColumn(encoded, 0);
        var col1 = RecordEncoder.DecodeColumn(encoded, 1);
        var col2 = RecordEncoder.DecodeColumn(encoded, 2);

        Assert.Equal(11, col0.AsInteger);
        Assert.Equal("abc", col1.AsText);
        Assert.Equal(3.5, col2.AsReal);
    }

    [Fact]
    public void DecodeColumn_OutOfRange_ReturnsNull()
    {
        var encoded = RecordEncoder.Encode(new[] { DbValue.FromInteger(1) });
        var decoded = RecordEncoder.DecodeColumn(encoded, 5);
        Assert.True(decoded.IsNull);
    }

    [Fact]
    public void DecodeColumn_NegativeIndex_ReturnsNull()
    {
        var encoded = RecordEncoder.Encode(new[] { DbValue.FromInteger(1) });
        var decoded = RecordEncoder.DecodeColumn(encoded, -1);
        Assert.True(decoded.IsNull);
    }

    [Fact]
    public void IsColumnNull_ReturnsExpectedValue()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.FromInteger(1),
            DbValue.Null,
            DbValue.FromText("x"),
        });

        Assert.False(RecordEncoder.IsColumnNull(encoded, 0));
        Assert.True(RecordEncoder.IsColumnNull(encoded, 1));
        Assert.False(RecordEncoder.IsColumnNull(encoded, 2));
        Assert.True(RecordEncoder.IsColumnNull(encoded, 3));
        Assert.True(RecordEncoder.IsColumnNull(encoded, -1));
    }

    [Fact]
    public void TryDecodeNumericColumn_DecodesIntegerAndReal()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.FromInteger(42),
            DbValue.FromReal(3.5),
        });

        Assert.True(RecordEncoder.TryDecodeNumericColumn(encoded, 0, out long i0, out double r0, out bool isReal0));
        Assert.False(isReal0);
        Assert.Equal(42, i0);
        Assert.Equal(0, r0);

        Assert.True(RecordEncoder.TryDecodeNumericColumn(encoded, 1, out long i1, out double r1, out bool isReal1));
        Assert.True(isReal1);
        Assert.Equal(0, i1);
        Assert.Equal(3.5, r1);
    }

    [Fact]
    public void TryDecodeNumericColumn_NullOrMissing_ReturnsFalse()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.Null,
            DbValue.FromInteger(1),
        });

        Assert.False(RecordEncoder.TryDecodeNumericColumn(encoded, 0, out _, out _, out _));
        Assert.False(RecordEncoder.TryDecodeNumericColumn(encoded, 2, out _, out _, out _));
        Assert.False(RecordEncoder.TryDecodeNumericColumn(encoded, -1, out _, out _, out _));
    }

    [Fact]
    public void TryDecodeNumericColumn_NonNumeric_Throws()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.FromText("abc"),
        });

        Assert.Throws<InvalidOperationException>(() =>
            RecordEncoder.TryDecodeNumericColumn(encoded, 0, out _, out _, out _));
    }

    [Fact]
    public void TryColumnTextEquals_MatchesUtf8BytesWithoutDecode()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.FromInteger(1),
            DbValue.FromText("Alpha"),
            DbValue.FromText("Beta"),
        });

        Assert.True(RecordEncoder.TryColumnTextEquals(encoded, 1, "Alpha"u8, out bool equals1));
        Assert.True(equals1);

        Assert.True(RecordEncoder.TryColumnTextEquals(encoded, 2, "Alpha"u8, out bool equals2));
        Assert.False(equals2);
    }

    [Fact]
    public void TryColumnTextEquals_MissingOrNonText_ReturnsFalse()
    {
        var encoded = RecordEncoder.Encode(new DbValue[]
        {
            DbValue.FromInteger(7),
            DbValue.Null,
        });

        Assert.False(RecordEncoder.TryColumnTextEquals(encoded, 0, "7"u8, out _));
        Assert.False(RecordEncoder.TryColumnTextEquals(encoded, 1, "x"u8, out _));
        Assert.False(RecordEncoder.TryColumnTextEquals(encoded, 2, "x"u8, out _));
        Assert.False(RecordEncoder.TryColumnTextEquals(encoded, -1, "x"u8, out _));
    }
}
