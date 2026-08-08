using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class HashedIndexPayloadCodecTests
{
    [Fact]
    public void CreateSingle_WithTrailingIntegerOmitted_MatchesFullLogicalKey()
    {
        DbValue[] fullKey =
        [
            DbValue.FromText("alpha"),
            DbValue.FromInteger(42),
        ];

        byte[] payload = HashedIndexPayloadCodec.CreateSingle(
            fullKey,
            rowId: 123,
            omitTrailingInteger: true);

        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(payload, fullKey, out byte[]? rowIds));
        Assert.NotNull(rowIds);
        Assert.Equal(1, RowIdPayloadCodec.GetCount(rowIds));
        Assert.Equal(123, RowIdPayloadCodec.ReadAt(rowIds, 0));
    }

    [Fact]
    public void Insert_IntoTrailingIntegerOmittedPayload_AddsSecondLogicalGroup()
    {
        DbValue[] alphaKey =
        [
            DbValue.FromText("alpha"),
            DbValue.FromInteger(42),
        ];
        DbValue[] betaKey =
        [
            DbValue.FromText("beta"),
            DbValue.FromInteger(42),
        ];

        byte[] payload = HashedIndexPayloadCodec.CreateSingle(
            alphaKey,
            rowId: 100,
            omitTrailingInteger: true);

        byte[] updated = HashedIndexPayloadCodec.Insert(
            payload,
            betaKey,
            rowId: 200,
            out bool changed);

        Assert.True(changed);
        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(updated, alphaKey, out byte[]? alphaRowIds));
        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(updated, betaKey, out byte[]? betaRowIds));
        Assert.NotNull(alphaRowIds);
        Assert.NotNull(betaRowIds);
        Assert.Equal(100, RowIdPayloadCodec.ReadAt(alphaRowIds, 0));
        Assert.Equal(200, RowIdPayloadCodec.ReadAt(betaRowIds, 0));
    }

    [Fact]
    public void RealComponents_RoundTripWithCanonicalZeroAndNaNKeys()
    {
        byte[] zeroPayload = HashedIndexPayloadCodec.CreateSingle(
            [DbValue.FromReal(-0d)],
            rowId: 101);

        Assert.True(
            HashedIndexPayloadCodec.TryGetMatchingRowIds(
                zeroPayload,
                [DbValue.FromReal(0d)],
                out byte[]? zeroRowIds));
        Assert.Equal(101, RowIdPayloadCodec.ReadAt(zeroRowIds!, 0));

        double alternateNaN = BitConverter.Int64BitsToDouble(
            unchecked((long)0xfff8_0000_0000_0042UL));
        byte[] nanPayload = HashedIndexPayloadCodec.CreateSingle(
            [DbValue.FromReal(double.NaN)],
            rowId: 202);

        Assert.True(
            HashedIndexPayloadCodec.TryGetMatchingRowIds(
                nanPayload,
                [DbValue.FromReal(alternateNaN)],
                out byte[]? nanRowIds));
        Assert.Equal(202, RowIdPayloadCodec.ReadAt(nanRowIds!, 0));
    }

    [Fact]
    public void DecimalAndBlobComponents_RoundTripAndRejectDifferentKeys()
    {
        DbValue[] key =
        [
            DbValue.FromDecimalParts(12345, 2),
            DbValue.FromBlob([0x00, 0x7f, 0xff]),
        ];
        byte[] payload = HashedIndexPayloadCodec.CreateSingle(key, rowId: 303);

        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(payload, key, out byte[]? rowIds));
        Assert.Equal(303, RowIdPayloadCodec.ReadAt(rowIds!, 0));

        Assert.True(
            HashedIndexPayloadCodec.TryGetMatchingRowIds(
                payload,
                [DbValue.FromDecimalParts(12346, 2), DbValue.FromBlob([0x00, 0x7f, 0xff])],
                out byte[]? differentDecimalRows));
        Assert.Null(differentDecimalRows);

        Assert.True(
            HashedIndexPayloadCodec.TryGetMatchingRowIds(
                payload,
                [DbValue.FromDecimalParts(12345, 2), DbValue.FromBlob([0x00, 0x7f, 0xfe])],
                out byte[]? differentBlobRows));
        Assert.Null(differentBlobRows);
    }
}
