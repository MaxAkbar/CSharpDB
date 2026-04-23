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
}
