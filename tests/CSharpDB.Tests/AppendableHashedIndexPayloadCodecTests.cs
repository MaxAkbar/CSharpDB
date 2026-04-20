using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class AppendableHashedIndexPayloadCodecTests
{
    [Fact]
    public void EncodeFromStoredKeyBytes_PreservesKeyComponents()
    {
        DbValue[] keyComponents =
        [
            DbValue.FromText("shared-category"),
            DbValue.FromInteger(42),
        ];

        byte[] originalPayload = AppendableHashedIndexPayloadCodec.Encode(
            keyComponents,
            firstPageId: 11,
            lastPageId: 19,
            rowCount: 7,
            lastRowId: 700,
            isSortedAscending: true);

        Assert.True(
            AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                originalPayload,
                out AppendableHashedIndexPayloadMetadata metadata));
        Assert.True(
            AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
                originalPayload.AsSpan(metadata.KeyComponentsOffset),
                keyComponents));

        byte[] rewrittenPayload = AppendableHashedIndexPayloadCodec.Encode(
            originalPayload.AsSpan(metadata.KeyComponentsOffset),
            firstPageId: metadata.FirstPageId,
            lastPageId: 23,
            rowCount: metadata.RowCount + 1,
            lastRowId: 701,
            isSortedAscending: metadata.IsSortedAscending);

        Assert.True(AppendableHashedIndexPayloadCodec.TryDecode(rewrittenPayload, out AppendableHashedIndexPayload decoded));
        Assert.Equal(metadata.FirstPageId, decoded.FirstPageId);
        Assert.Equal((uint)23, decoded.LastPageId);
        Assert.Equal(metadata.RowCount + 1, decoded.RowCount);
        Assert.Equal(701, decoded.LastRowId);
        Assert.True(decoded.IsSortedAscending);
        Assert.Equal(keyComponents, decoded.KeyComponents);
    }

    [Fact]
    public void EncodedKeyComponentsEqual_DetectsMismatchedText()
    {
        DbValue[] encodedKeyComponents = [DbValue.FromText("alpha"), DbValue.FromInteger(9)];
        byte[] payload = AppendableHashedIndexPayloadCodec.Encode(
            encodedKeyComponents,
            firstPageId: 3,
            lastPageId: 3,
            rowCount: 1,
            lastRowId: 9,
            isSortedAscending: true);

        Assert.True(
            AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                payload,
                out AppendableHashedIndexPayloadMetadata metadata));

        Assert.False(
            AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
                payload.AsSpan(metadata.KeyComponentsOffset),
                [DbValue.FromText("beta"), DbValue.FromInteger(9)]));
    }

    [Fact]
    public void EncodeExternal_DecodesReferenceAndMarksExternalChainState()
    {
        DbValue[] keyComponents = [DbValue.FromText("shared-category"), DbValue.FromInteger(17)];

        byte[] payload = AppendableHashedIndexPayloadCodec.EncodeExternal(
            keyComponents,
            firstPageId: 123);

        Assert.True(
            AppendableHashedIndexPayloadCodec.TryDecodeReference(
                payload,
                out AppendableHashedIndexPayloadReference decoded));
        Assert.Equal(AppendableHashedIndexPayloadFormat.ExternalChainState, decoded.Metadata.Format);
        Assert.Equal((uint)123, decoded.Metadata.FirstPageId);
        Assert.Equal(keyComponents, decoded.KeyComponents);
    }

    [Fact]
    public void EncodedKeyComponentsEqual_AllowsTrailingIntegerToBeOmitted()
    {
        byte[] payload = AppendableHashedIndexPayloadCodec.EncodeExternal(
            [DbValue.FromText("shared-category")],
            firstPageId: 123);

        Assert.True(
            AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                payload,
                out AppendableHashedIndexPayloadMetadata metadata));
        Assert.True(
            AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
                payload.AsSpan(metadata.KeyComponentsOffset),
                [DbValue.FromText("shared-category"), DbValue.FromInteger(17)]));
    }

    [Fact]
    public void AppendOptimizedIndexMutationContext_MatchesEncodedStoredKeyBytes_AndRejectsDifferentComponents()
    {
        byte[] payload = AppendableHashedIndexPayloadCodec.EncodeExternal(
            [DbValue.FromText("shared-category"), DbValue.FromInteger(17)],
            firstPageId: 123);

        Assert.True(
            AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                payload,
                out AppendableHashedIndexPayloadMetadata metadata));

        var context = new AppendOptimizedIndexMutationContext();
        context.Capture(
            key: 42,
            [DbValue.FromText("shared-category"), DbValue.FromInteger(17)],
            payload,
            metadata);

        Assert.True(
            context.Matches(
                key: 42,
                [DbValue.FromText("shared-category"), DbValue.FromInteger(17)],
                payload));

        Assert.False(
            context.Matches(
                key: 42,
                [DbValue.FromText("other-category"), DbValue.FromInteger(17)],
                payload));
    }
}
