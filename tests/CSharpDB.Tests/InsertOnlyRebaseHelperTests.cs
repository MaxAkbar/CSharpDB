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
            out byte[]? rebasedPage);

        Assert.Equal(InsertOnlyRebaseResult.CapacityReject, result);
        Assert.Null(rebasedPage);
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
}
