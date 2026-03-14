using System.Buffers.Binary;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class BTreeDeleteRebalancingTests
{
    [Fact]
    public async Task DeleteAndReinsert_HotKeysWithGrowingPayload_RemainSearchable()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_btree_hot_reinsert_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new PagerOptions
        {
            CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
        };

        try
        {
            await using var pager = await OpenPagerAsync(dbPath, options, createNew: true, ct);
            await pager.BeginTransactionAsync(ct);

            uint rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);

            const int hotKeyCount = 256;
            const int rounds = 400;

            for (int round = 0; round < rounds; round++)
            {
                int payloadLength = (round + 1) * sizeof(long);
                var payload = new byte[payloadLength];
                for (int key = 0; key < hotKeyCount; key++)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(payloadLength - sizeof(long)), round);

                    var existing = await tree.FindAsync(key, ct);
                    if (existing == null)
                    {
                        await tree.InsertAsync(key, payload, ct);
                        continue;
                    }

                    Assert.True(await tree.DeleteAsync(key, ct));
                    await tree.InsertAsync(key, payload, ct);
                }
            }

            await pager.CommitAsync(ct);

            Assert.Equal(hotKeyCount, await tree.CountEntriesAsync(ct));
            for (int key = 0; key < hotKeyCount; key++)
            {
                var payload = await tree.FindAsync(key, ct);
                Assert.NotNull(payload);
                Assert.Equal(rounds * sizeof(long), payload!.Length);
            }
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
        }
    }

    [Fact]
    public async Task Delete_MassPrune_ReclaimsPagesAndCollapsesRoot()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_btree_delete_rebalance_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new PagerOptions
        {
            CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
        };

        try
        {
            await using var pager = await OpenPagerAsync(dbPath, options, createNew: true, ct);
            byte[] payload = new byte[160];
            await pager.BeginTransactionAsync(ct);
            uint rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            for (int i = 1; i <= 1500; i++)
            {
                payload[0] = (byte)(i & 0xFF);
                await tree.InsertAsync(i, payload, ct);
            }
            await pager.CommitAsync(ct);

            Assert.Equal(0u, pager.FreelistHead);

            await pager.BeginTransactionAsync(ct);
            for (int i = 1; i <= 1499; i++)
                Assert.True(await tree.DeleteAsync(i, ct));
            await pager.CommitAsync(ct);

            Assert.NotEqual(0u, pager.FreelistHead);
            Assert.Equal(1L, await tree.CountEntriesAsync(ct));
            Assert.Null(await tree.FindAsync(1, ct));
            Assert.NotNull(await tree.FindAsync(1500, ct));

            var rootPage = await pager.GetPageAsync(tree.RootPageId, ct);
            var rootSp = new SlottedPage(rootPage, tree.RootPageId);
            Assert.Equal(PageConstants.PageTypeLeaf, rootSp.PageType);
            Assert.Equal((ushort)1, rootSp.CellCount);

            var cursor = tree.CreateCursor();
            Assert.True(await cursor.MoveNextAsync(ct));
            Assert.Equal(1500L, cursor.CurrentKey);
            Assert.False(await cursor.MoveNextAsync(ct));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
        }
    }

    [Fact]
    public async Task Delete_TailRange_KeepsRightmostLeafNonEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_btree_delete_rightmost_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new PagerOptions
        {
            CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
        };

        try
        {
            await using var pager = await OpenPagerAsync(dbPath, options, createNew: true, ct);
            byte[] payload = new byte[180];
            await pager.BeginTransactionAsync(ct);
            uint rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            for (int i = 1; i <= 300; i++)
                await tree.InsertAsync(i, payload, ct);
            await pager.CommitAsync(ct);

            await pager.BeginTransactionAsync(ct);
            for (int i = 201; i <= 300; i++)
                Assert.True(await tree.DeleteAsync(i, ct));
            await pager.CommitAsync(ct);

            Assert.Equal(200L, await tree.CountEntriesAsync(ct));

            for (int i = 1; i <= 200; i++)
                Assert.NotNull(await tree.FindAsync(i, ct));

            for (int i = 201; i <= 300; i++)
                Assert.Null(await tree.FindAsync(i, ct));

            SlottedPage rightmostLeaf = await GetRightmostLeafAsync(tree, pager, ct);
            Assert.Equal(PageConstants.PageTypeLeaf, rightmostLeaf.PageType);
            Assert.True(rightmostLeaf.CellCount > 0);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
        }
    }

    private static async ValueTask<SlottedPage> GetRightmostLeafAsync(BTree tree, Pager pager, CancellationToken ct)
    {
        uint pageId = tree.RootPageId;
        while (true)
        {
            var page = await pager.GetPageAsync(pageId, ct);
            var sp = new SlottedPage(page, pageId);
            if (sp.PageType == PageConstants.PageTypeLeaf)
                return sp;

            pageId = sp.RightChildOrNextLeaf;
        }
    }

    private static async ValueTask<Pager> OpenPagerAsync(
        string dbPath,
        PagerOptions options,
        bool createNew,
        CancellationToken ct)
    {
        var device = new FileStorageDevice(dbPath, createNew);
        var walIndex = new WalIndex();
        var wal = new WriteAheadLog(dbPath, walIndex);
        var pager = await Pager.CreateAsync(device, wal, walIndex, options, ct);

        if (createNew)
            await pager.InitializeNewDatabaseAsync(ct);

        await pager.RecoverAsync(ct);
        return pager;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
