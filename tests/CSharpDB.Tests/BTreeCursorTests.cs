using System.Buffers.Binary;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class BTreeCursorTests
{
    [Fact]
    public async Task SeekAndMoveNext_AcrossMultipleLeafPages_YieldsOrderedKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);

        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        byte[] payload = new byte[1024];

        for (int key = 1; key <= 24; key++)
        {
            payload[0] = (byte)key;
            await tree.InsertAsync(key, payload, ct);
        }

        await pager.CommitAsync(ct);

        var cursor = tree.CreateCursor();
        Assert.True(await cursor.SeekAsync(7, ct));

        var keys = new List<long> { cursor.CurrentKey };
        while (keys.Count < 8 && await cursor.MoveNextAsync(ct))
            keys.Add(cursor.CurrentKey);

        Assert.Equal(new long[] { 7, 8, 9, 10, 11, 12, 13, 14 }, keys);
    }

    [Fact]
    public async Task VariablePayloadLeafSplits_PreserveAllKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);

        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);

        for (int key = 1; key <= 20; key++)
        {
            int payloadLength = key % 7 == 0
                ? 3000
                : key % 5 == 0
                    ? 1800
                    : 120;
            byte[] payload = new byte[payloadLength];
            payload[0] = (byte)key;
            await tree.InsertAsync(key, payload, ct);
        }

        await pager.CommitAsync(ct);

        Assert.Equal(20L, await tree.CountEntriesAsync(ct));

        for (int key = 1; key <= 20; key++)
        {
            byte[]? payload = await tree.FindAsync(key, ct);
            Assert.NotNull(payload);
            Assert.Equal((byte)key, payload![0]);
        }

        var cursor = tree.CreateCursor();
        var keys = new List<long>();
        while (await cursor.MoveNextAsync(ct))
            keys.Add(cursor.CurrentKey);

        Assert.Equal(Enumerable.Range(1, 20).Select(static value => (long)value).ToArray(), keys);
    }

    [Fact]
    public async Task AppendDrivenRootSplit_BiasesRightmostLeafToStaySparse()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);

        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await using var tx = await pager.BeginWriteTransactionAsync(ct);
        uint rootPageId;
        int nextKey = 1;
        using (tx.Bind())
        {
            rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            byte[] payload = new byte[160];

            while (tree.RootPageId == rootPageId)
            {
                payload[0] = (byte)nextKey;
                await tree.InsertAsync(nextKey, payload, ct);
                nextKey++;
            }

            rootPageId = tree.RootPageId;
            await tx.CommitAsync(ct);
        }

        var committedTree = new BTree(pager, rootPageId);
        var rootPage = await pager.GetPageAsync(rootPageId, ct);
        var root = new SlottedPage(rootPage, rootPageId);
        Assert.Equal(PageConstants.PageTypeInterior, root.PageType);
        Assert.Equal((ushort)1, root.CellCount);

        uint leftLeafPageId = BinaryPrimitives.ReadUInt32LittleEndian(root.GetCell(0).Slice(1, 4));
        uint rightLeafPageId = root.RightChildOrNextLeaf;

        var leftLeafPage = await pager.GetPageAsync(leftLeafPageId, ct);
        var leftLeaf = new SlottedPage(leftLeafPage, leftLeafPageId);
        var rightLeafPage = await pager.GetPageAsync(rightLeafPageId, ct);
        var rightLeaf = new SlottedPage(rightLeafPage, rightLeafPageId);

        Assert.Equal(PageConstants.PageTypeLeaf, leftLeaf.PageType);
        Assert.Equal(PageConstants.PageTypeLeaf, rightLeaf.PageType);
        Assert.True(leftLeaf.CellCount > rightLeaf.CellCount);

        for (int key = 1; key < nextKey; key++)
            Assert.NotNull(await committedTree.FindAsync(key, ct));
    }

    [Fact]
    public async Task ConcurrentExplicitTransactions_IndependentLeafSplitsCanRebaseSharedRootPage()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);

        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint initialRootPageId = await BTree.CreateNewAsync(pager, ct);
        var seedTree = new BTree(pager, initialRootPageId);
        int seededCount = await SeedTreeUntilRootHasAtLeastInteriorCellsAsync(seedTree, pager, minimumRootCells: 2, ct);
        uint rootPageId = seedTree.RootPageId;
        await pager.CommitAsync(ct);

        Assert.Equal(PageConstants.PageTypeInterior, await GetPageTypeAsync(pager, rootPageId, ct));
        ushort baselineRootCellCount = await GetRootCellCountAsync(pager, rootPageId, ct);
        Assert.True(baselineRootCellCount >= 2);

        await using var tx1 = await pager.BeginWriteTransactionAsync(ct);
        await using var tx2 = await pager.BeginWriteTransactionAsync(ct);

        List<long> leftInsertedKeys;
        using (tx1.Bind())
        {
            leftInsertedKeys = await ForceLeafSplitThatUpdatesRootAsync(
                pager,
                rootPageId,
                baselineRootCellCount,
                startKey: -1,
                step: -1,
                payloadSize: 768,
                ct);
        }

        List<long> rightInsertedKeys;
        using (tx2.Bind())
        {
            rightInsertedKeys = await ForceLeafSplitThatUpdatesRootAsync(
                pager,
                rootPageId,
                baselineRootCellCount,
                startKey: 1_000_000,
                step: 1,
                payloadSize: 768,
                ct);
        }

        await tx1.CommitAsync(ct);
        await tx2.CommitAsync(ct);

        var committedTree = new BTree(pager, rootPageId);
        Assert.Equal((ushort)(baselineRootCellCount + 2), await GetRootCellCountAsync(pager, rootPageId, ct));
        Assert.Equal(seededCount + leftInsertedKeys.Count + rightInsertedKeys.Count, await committedTree.CountEntriesAsync(ct));

        foreach (long key in leftInsertedKeys)
            Assert.NotNull(await committedTree.FindAsync(key, ct));

        foreach (long key in rightInsertedKeys)
            Assert.NotNull(await committedTree.FindAsync(key, ct));
    }

    private static async ValueTask<Pager> CreatePagerAsync(CancellationToken ct)
    {
        var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        var wal = new MemoryWriteAheadLog(walIndex);
        return await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
                MaxCachedPages = 32,
            },
            ct);
    }

    private static async ValueTask<int> SeedTreeUntilRootHasAtLeastInteriorCellsAsync(
        BTree tree,
        Pager pager,
        ushort minimumRootCells,
        CancellationToken ct)
    {
        byte[] payload = new byte[256];
        for (int key = 1; key <= 2048; key++)
        {
            payload[0] = (byte)key;
            await tree.InsertAsync(key, payload, ct);

            if (await GetPageTypeAsync(pager, tree.RootPageId, ct) == PageConstants.PageTypeInterior &&
                await GetRootCellCountAsync(pager, tree.RootPageId, ct) >= minimumRootCells)
            {
                return key;
            }
        }

        Assert.Fail("Failed to seed a tree with an interior root and multiple leaf children.");
        return 0;
    }

    private static async ValueTask<List<long>> ForceLeafSplitThatUpdatesRootAsync(
        Pager pager,
        uint rootPageId,
        ushort baselineRootCellCount,
        long startKey,
        long step,
        int payloadSize,
        CancellationToken ct)
    {
        var tree = new BTree(pager, rootPageId);
        byte[] payload = new byte[payloadSize];
        var insertedKeys = new List<long>();
        long key = startKey;

        for (int attempt = 0; attempt < 64; attempt++)
        {
            payload[0] = (byte)(attempt + 1);
            await tree.InsertAsync(key, payload, ct);
            insertedKeys.Add(key);

            Assert.Equal(rootPageId, tree.RootPageId);
            if (await GetRootCellCountAsync(pager, rootPageId, ct) == baselineRootCellCount + 1)
                return insertedKeys;

            key += step;
        }

        Assert.Fail("Failed to force a leaf split that propagated a separator into the shared root page.");
        return insertedKeys;
    }

    private static async ValueTask<byte> GetPageTypeAsync(Pager pager, uint pageId, CancellationToken ct)
    {
        var page = await pager.GetPageAsync(pageId, ct);
        var sp = new SlottedPage(page, pageId);
        return sp.PageType;
    }

    private static async ValueTask<ushort> GetRootCellCountAsync(Pager pager, uint rootPageId, CancellationToken ct)
    {
        var page = await pager.GetPageAsync(rootPageId, ct);
        var sp = new SlottedPage(page, rootPageId);
        return sp.CellCount;
    }
}
