using System.Buffers.Binary;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class BTreeBoundedCacheMutationTests
{
    [Theory]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    public async Task InsertIntoCleanTree_LeafAndInteriorSplitsRemainOrderedAndSearchable(
        int cachePages, bool mapped)
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        var expected = new SortedDictionary<long, byte[]>();
        for (int key = 0; key < 320; key += 2)
            expected.Add(key, Payload(key, 3000, revision: 0));

        try
        {
            await SeedAsync(path, expected, expectedHeight: 2, ct);
            await using (var pager = await OpenPagerAsync(path, cachePages, mapped, createNew: false, ct))
            {
                // Reopening makes the parent and existing leaves clean. Recursive
                // descent and allocation can evict a held parent with these limits.
                await pager.BeginTransactionAsync(ct);
                var tree = new BTree(pager, pager.SchemaRootPage);
                for (int key = 1; key < 320; key += 2)
                {
                    byte[] value = Payload(key, 3000, revision: 1);
                    await tree.InsertAsync(key, value, ct);
                    expected.Add(key, value);
                }
                pager.SchemaRootPage = tree.RootPageId;
                Assert.Equal(3, await HeightAsync(pager, tree.RootPageId, ct));
                await AssertContentsAsync(new BTree(pager, tree.RootPageId), expected, allKeyCount: 320, ct);
                await pager.CommitAsync(ct);
                await pager.CheckpointAsync(ct);
                await AssertContentsAsync(new BTree(pager, pager.SchemaRootPage), expected, 320, ct);
            }
            await AssertReopenedAsync(path, cachePages, mapped, expected, allKeyCount: 320, ct);
        }
        finally
        {
            DeleteFiles(path);
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    public async Task ReplaceWithLargerPayloadInCleanTree_SplitsPreserveEveryKeyAndPayload(
        int cachePages, bool mapped)
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        var expected = new SortedDictionary<long, byte[]>();
        for (int key = 0; key < 320; key++)
            expected.Add(key, Payload(key, 1000, revision: 0));

        try
        {
            await SeedAsync(path, expected, expectedHeight: 2, ct);
            await using (var pager = await OpenPagerAsync(path, cachePages, mapped, createNew: false, ct))
            {
                await pager.BeginTransactionAsync(ct);
                var tree = new BTree(pager, pager.SchemaRootPage);
                // Grow alternating entries before their neighbors so replacements
                // split populated leaves rather than only following an append hint.
                for (int parity = 0; parity < 2; parity++)
                {
                    for (int key = parity; key < 320; key += 2)
                    {
                        byte[] value = Payload(key, 3000, revision: 1);
                        Assert.True(await tree.ReplaceAsync(key, value, ct));
                        expected[key] = value;
                    }
                }
                pager.SchemaRootPage = tree.RootPageId;
                Assert.Equal(3, await HeightAsync(pager, tree.RootPageId, ct));
                await AssertContentsAsync(new BTree(pager, tree.RootPageId), expected, allKeyCount: 320, ct);
                await pager.CommitAsync(ct);
                await pager.CheckpointAsync(ct);
                await AssertContentsAsync(new BTree(pager, pager.SchemaRootPage), expected, 320, ct);
            }
            await AssertReopenedAsync(path, cachePages, mapped, expected, allKeyCount: 320, ct);
        }
        finally
        {
            DeleteFiles(path);
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    public async Task DeleteFromCleanTree_RebalanceAndRootCollapsePreserveRemainingRows(
        int cachePages, bool mapped)
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        const int rowCount = 200;
        var expected = new SortedDictionary<long, byte[]>();
        for (int key = 0; key < rowCount; key++)
            expected.Add(key, Payload(key, 2000, revision: 0));

        try
        {
            // Two rows fit per leaf. A two-level tree exercises borrowing and
            // merging beneath a clean root, then the supported root-to-leaf
            // collapse. Deeper trees may retain underfull interior pages.
            await SeedAsync(path, expected, expectedHeight: 2, ct);
            await using (var pager = await OpenPagerAsync(path, cachePages, mapped, createNew: false, ct))
            {
                await pager.BeginTransactionAsync(ct);
                var tree = new BTree(pager, pager.SchemaRootPage);
                for (int low = 0; low < rowCount / 2 - 1; low++)
                {
                    int high = rowCount - 1 - low;
                    Assert.True(await tree.DeleteAsync(low, ct));
                    expected.Remove(low);
                    Assert.True(await tree.DeleteAsync(high, ct));
                    expected.Remove(high);
                }
                // Empty the final sibling as well; this implementation permits
                // one row per non-root leaf, so two survivors need not merge.
                Assert.True(await tree.DeleteAsync(rowCount / 2, ct));
                expected.Remove(rowCount / 2);
                pager.SchemaRootPage = tree.RootPageId;
                Assert.Single(expected);
                Assert.Equal(1, await HeightAsync(pager, tree.RootPageId, ct));
                Assert.NotEqual(0u, pager.FreelistHead);
                await AssertContentsAsync(new BTree(pager, tree.RootPageId), expected, allKeyCount: rowCount, ct);
                await pager.CommitAsync(ct);
                await pager.CheckpointAsync(ct);
                await AssertContentsAsync(new BTree(pager, pager.SchemaRootPage), expected, rowCount, ct);
            }
            await AssertReopenedAsync(path, cachePages, mapped, expected, allKeyCount: rowCount, ct);
        }
        finally
        {
            DeleteFiles(path);
        }
    }

    private static async Task SeedAsync(string path, SortedDictionary<long, byte[]> rows,
        int expectedHeight, CancellationToken ct)
    {
        // The fixture does not depend on the bounded-cache mutation behavior under
        // test. Its root is persisted in the pager header for independent reopening.
        await using var pager = await OpenPagerAsync(path, cachePages: null, mapped: false, createNew: true, ct);
        await pager.BeginTransactionAsync(ct);
        var tree = new BTree(pager, await BTree.CreateNewAsync(pager, ct));
        foreach (var row in rows)
            await tree.InsertAsync(row.Key, row.Value, ct);
        pager.SchemaRootPage = tree.RootPageId;
        Assert.Equal(expectedHeight, await HeightAsync(pager, tree.RootPageId, ct));
        await pager.CommitAsync(ct);
        await pager.CheckpointAsync(ct);
    }

    private static async Task AssertReopenedAsync(string path, int cachePages, bool mapped,
        SortedDictionary<long, byte[]> expected, int allKeyCount, CancellationToken ct)
    {
        await using var pager = await OpenPagerAsync(path, cachePages, mapped, createNew: false, ct);
        await AssertContentsAsync(new BTree(pager, pager.SchemaRootPage), expected, allKeyCount, ct);
    }

    private static async Task AssertContentsAsync(BTree tree, SortedDictionary<long, byte[]> expected,
        int allKeyCount, CancellationToken ct)
    {
        await using (var cursor = tree.CreateCursor())
        {
            foreach (var row in expected)
            {
                Assert.True(await cursor.MoveNextAsync(ct), $"Missing scan row for key {row.Key}.");
                Assert.Equal(row.Key, cursor.CurrentKey);
                Assert.Equal(row.Value, cursor.CurrentValue.ToArray());
            }
            Assert.False(await cursor.MoveNextAsync(ct));
        }

        // Every inserted key is checked independently: a leaf chain can contain
        // all rows while a corrupted parent makes individual keys unreachable.
        for (int key = 0; key < allKeyCount; key++)
        {
            byte[]? actual = await tree.FindAsync(key, ct);
            if (expected.TryGetValue(key, out byte[]? value))
            {
                Assert.NotNull(actual);
                Assert.Equal(value, actual);
            }
            else
                Assert.Null(actual);
        }
    }

    private static async ValueTask<int> HeightAsync(Pager pager, uint pageId, CancellationToken ct)
    {
        for (int height = 1; height <= 8; height++)
        {
            byte[] page = await pager.GetPageAsync(pageId, ct);
            var slotted = new SlottedPage(page, pageId);
            if (slotted.PageType == PageConstants.PageTypeLeaf)
                return height;
            Assert.Equal(PageConstants.PageTypeInterior, slotted.PageType);
            pageId = slotted.RightChildOrNextLeaf;
            Assert.NotEqual(PageConstants.NullPageId, pageId);
        }
        throw new InvalidOperationException("Unexpected B-tree height or cyclic interior chain.");
    }

    private static async ValueTask<Pager> OpenPagerAsync(string path, int? cachePages, bool mapped,
        bool createNew, CancellationToken ct)
    {
        var device = new FileStorageDevice(path, createNew);
        var index = new WalIndex();
        var wal = new WriteAheadLog(path, index);
        Pager? pager = null;
        try
        {
            pager = await Pager.CreateAsync(device, wal, index, new PagerOptions
            {
                MaxCachedPages = cachePages,
                UseMemoryMappedReads = mapped,
                EnableSequentialLeafReadAhead = false,
                PreserveOwnedPagesOnCheckpoint = false,
                CheckpointPolicy = new FrameCountCheckpointPolicy(100_000),
            }, ct);
            if (createNew)
                await pager.InitializeNewDatabaseAsync(ct);
            await pager.RecoverAsync(ct);
            return pager;
        }
        catch
        {
            if (pager is not null)
                await pager.DisposeAsync();
            else
            {
                await wal.DisposeAsync();
                await device.DisposeAsync();
            }
            throw;
        }
    }

    private static byte[] Payload(int key, int length, int revision)
    {
        byte[] result = new byte[length];
        for (int i = 0; i < result.Length; i++)
            result[i] = (byte)((key * 17 + revision * 73 + i * 131) & 0xff);
        BinaryPrimitives.WriteInt32LittleEndian(result, key);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(sizeof(int)), revision);
        return result;
    }

    private static string NewTempPath() =>
        Path.Combine(Path.GetTempPath(), $"csharpdb_btree_bounded_mutation_{Guid.NewGuid():N}.db");

    private static void DeleteFiles(string path)
    {
        File.Delete(path);
        File.Delete(path + ".wal");
    }
}
