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
}
