using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Tests;

public sealed class InsertOnlyRebaseHelperTests
{
    [Fact]
    public void TryRebaseInsertOnlyLeafPage_SecondInsertOverfillsCommittedLeaf_ReturnsCapacityReject()
    {
        const uint pageId = 1;

        byte[] basePage = CreateLeafPage(pageId);
        var baseLeaf = new SlottedPage(basePage, pageId);
        InsertLeafCell(ref baseLeaf, key: 100, payloadLength: 2052, fillByte: 0x11);
        Assert.Equal(2023, baseLeaf.FreeSpace);

        byte[] committedPage = (byte[])basePage.Clone();
        var committedLeaf = new SlottedPage(committedPage, pageId);
        InsertLeafCell(ref committedLeaf, key: 200, payloadLength: 1000, fillByte: 0x22);
        Assert.Equal(1011, committedLeaf.FreeSpace);

        byte[] transactionPage = (byte[])basePage.Clone();
        var transactionLeaf = new SlottedPage(transactionPage, pageId);
        InsertLeafCell(ref transactionLeaf, key: 300, payloadLength: 1000, fillByte: 0x33);
        Assert.Equal(1011, transactionLeaf.FreeSpace);

        InsertOnlyRebaseResult result = LeafInsertRebaseHelper.TryRebaseInsertOnlyLeafPage(
            pageId,
            basePage,
            committedPage,
            transactionPage,
            out byte[]? rebasedPage,
            out LeafInsertRebaseRejectReason rejectReason);

        Assert.Equal(InsertOnlyRebaseResult.CapacityReject, result);
        Assert.Null(rebasedPage);
        Assert.Equal(LeafInsertRebaseRejectReason.None, rejectReason);
    }

    [Fact]
    public void TryRebaseCommittedSplitLeafPages_RetargetsTransactionInsertIntoCommittedRightSibling()
    {
        const uint leftPageId = 1;
        const uint rightPageId = 2;

        byte[] basePage = CreateLeafPage(leftPageId);
        var baseLeaf = new SlottedPage(basePage, leftPageId);
        InsertLeafCell(ref baseLeaf, key: 100, payloadLength: 2052, fillByte: 0x11);
        InsertLeafCell(ref baseLeaf, key: 150, payloadLength: 1000, fillByte: 0x22);

        byte[] committedLeftPage = CreateLeafPage(leftPageId);
        var committedLeftLeaf = new SlottedPage(committedLeftPage, leftPageId);
        committedLeftLeaf.RightChildOrNextLeaf = rightPageId;
        InsertLeafCell(ref committedLeftLeaf, key: 100, payloadLength: 2052, fillByte: 0x11);
        InsertLeafCell(ref committedLeftLeaf, key: 150, payloadLength: 1000, fillByte: 0x22);

        byte[] committedRightPage = CreateLeafPage(rightPageId);
        var committedRightLeaf = new SlottedPage(committedRightPage, rightPageId);
        InsertLeafCell(ref committedRightLeaf, key: 200, payloadLength: 1000, fillByte: 0x33);

        byte[] transactionPage = (byte[])basePage.Clone();
        var transactionLeaf = new SlottedPage(transactionPage, leftPageId);
        InsertLeafCell(ref transactionLeaf, key: 300, payloadLength: 500, fillByte: 0x44);

        InsertOnlyRebaseResult result = LeafInsertRebaseHelper.TryRebaseCommittedSplitLeafPages(
            leftPageId,
            rightPageId,
            basePage,
            committedLeftPage,
            committedRightPage,
            transactionPage,
            out byte[]? rebasedLeftPage,
            out byte[]? rebasedRightPage,
            out LeafInsertRebaseRejectReason rejectReason);

        Assert.Equal(InsertOnlyRebaseResult.Success, result);
        Assert.NotNull(rebasedLeftPage);
        Assert.NotNull(rebasedRightPage);
        Assert.Equal(LeafInsertRebaseRejectReason.None, rejectReason);

        var rebasedLeftLeaf = new SlottedPage(rebasedLeftPage!, leftPageId);
        var rebasedRightLeaf = new SlottedPage(rebasedRightPage!, rightPageId);
        Assert.Equal(rightPageId, rebasedLeftLeaf.RightChildOrNextLeaf);
        Assert.Equal(PageConstants.NullPageId, rebasedRightLeaf.RightChildOrNextLeaf);
        Assert.Equal((ushort)2, rebasedLeftLeaf.CellCount);
        Assert.Equal((ushort)2, rebasedRightLeaf.CellCount);
        Assert.Equal(100, ReadLeafKey(rebasedLeftLeaf, 0));
        Assert.Equal(150, ReadLeafKey(rebasedLeftLeaf, 1));
        Assert.Equal(200, ReadLeafKey(rebasedRightLeaf, 0));
        Assert.Equal(300, ReadLeafKey(rebasedRightLeaf, 1));
    }

    [Fact]
    public void TryRebaseInsertOnlyLeafPage_ChangedNextLeaf_ReturnsStructuralRejectReason()
    {
        const uint leftPageId = 1;
        const uint originalNextLeafPageId = 2;
        const uint committedNextLeafPageId = 3;

        byte[] basePage = CreateLeafPage(leftPageId);
        var baseLeaf = new SlottedPage(basePage, leftPageId);
        baseLeaf.RightChildOrNextLeaf = originalNextLeafPageId;
        InsertLeafCell(ref baseLeaf, key: 100, payloadLength: 256, fillByte: 0x11);

        byte[] committedPage = (byte[])basePage.Clone();
        var committedLeaf = new SlottedPage(committedPage, leftPageId);
        committedLeaf.RightChildOrNextLeaf = committedNextLeafPageId;
        InsertLeafCell(ref committedLeaf, key: 200, payloadLength: 256, fillByte: 0x22);

        byte[] transactionPage = (byte[])basePage.Clone();
        var transactionLeaf = new SlottedPage(transactionPage, leftPageId);
        InsertLeafCell(ref transactionLeaf, key: 300, payloadLength: 256, fillByte: 0x33);

        InsertOnlyRebaseResult result = LeafInsertRebaseHelper.TryRebaseInsertOnlyLeafPage(
            leftPageId,
            basePage,
            committedPage,
            transactionPage,
            out byte[]? rebasedPage,
            out LeafInsertRebaseRejectReason rejectReason);

        Assert.Equal(InsertOnlyRebaseResult.StructuralReject, result);
        Assert.Null(rebasedPage);
        Assert.Equal(LeafInsertRebaseRejectReason.NextLeafChanged, rejectReason);
    }

    private static byte[] CreateLeafPage(uint pageId)
    {
        byte[] page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        var leaf = new SlottedPage(page, pageId);
        leaf.Initialize(PageConstants.PageTypeLeaf);
        return page;
    }

    private static void InsertLeafCell(ref SlottedPage leaf, long key, int payloadLength, byte fillByte)
    {
        byte[] payload = GC.AllocateUninitializedArray<byte>(payloadLength);
        payload.AsSpan().Fill(fillByte);

        byte[] cell = BTree.BuildLeafCell(key, payload);
        bool inserted = leaf.InsertCell(leaf.CellCount, cell);
        Assert.True(inserted, $"Expected key {key} with payloadLength={payloadLength} to fit the test page.");
    }

    private static long ReadLeafKey(SlottedPage leaf, int index)
    {
        ReadOnlySpan<byte> cell = leaf.GetCellMemory(index).Span;
        Varint.Read(cell, out int headerBytes);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(headerBytes, 8));
    }
}
