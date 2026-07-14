using System.Buffers.Binary;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class BTreeOverflowTests
{
    [Fact]
    public async Task OversizedPayload_PointReadCursorReplaceDeleteAndReuse_RoundTrips()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        byte[] oversized = CreatePayload(20_000, seed: 17);

        await tree.InsertAsync(1, oversized, ct);
        await tree.InsertAsync(2, "inline"u8.ToArray(), ct);
        uint pageCountWithOverflow = pager.PageCount;

        Assert.False(tree.TryFindCachedMemory(1, out _));
        Assert.Equal(oversized, await tree.FindAsync(1, ct));

        await using (var cursor = tree.CreateCursor())
        {
            Assert.True(await cursor.SeekAsync(1, ct));
            Assert.Equal(oversized, cursor.CurrentValue.ToArray());
            Assert.True(await cursor.MoveNextAsync(ct));
            Assert.Equal(2, cursor.CurrentKey);
            Assert.Equal("inline"u8.ToArray(), cursor.CurrentValue.ToArray());
        }

        Assert.True(await tree.ReplaceAsync(1, "now inline"u8.ToArray(), ct));
        Assert.Equal("now inline"u8.ToArray(), await tree.FindAsync(1, ct));

        // Reclaimed overflow pages are reused instead of extending the database.
        byte[] secondOversized = CreatePayload(20_000, seed: 91);
        await tree.InsertAsync(3, secondOversized, ct);
        Assert.Equal(pageCountWithOverflow, pager.PageCount);
        Assert.Equal(secondOversized, await tree.FindAsync(3, ct));

        Assert.True(await tree.DeleteAsync(3, ct));
        Assert.Null(await tree.FindAsync(3, ct));

        await pager.CommitAsync(ct);
    }

    [Fact]
    public async Task InlinePayloadMatchingOverflowMarker_IsNotMisclassifiedAndRoundTripsVerbatim()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        byte[] markerShapedPayload = new byte[16];
        Encoding.ASCII.GetBytes("CSDBBOV1").CopyTo(markerShapedPayload, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(markerShapedPayload.AsSpan(8), rootPageId);
        BinaryPrimitives.WriteInt32LittleEndian(markerShapedPayload.AsSpan(12), 1);
        uint pageCountBeforeInsert = pager.PageCount;

        await tree.InsertAsync(1, markerShapedPayload, ct);

        // The marker is ordinary inline data. Only the leaf-cell flag identifies an
        // overflow reference, so no overflow page should be allocated for this value.
        Assert.Equal(pageCountBeforeInsert, pager.PageCount);
        Assert.Equal(markerShapedPayload, await tree.FindAsync(1, ct));
        await using var cursor = tree.CreateCursor();
        Assert.True(await cursor.MoveNextAsync(ct));
        Assert.Equal(markerShapedPayload, cursor.CurrentValue.ToArray());

        await pager.CommitAsync(ct);
    }

    [Fact]
    public async Task FormatVersion1InlineMarkerPayload_ReopensWithoutMisclassification()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var device = new MemoryStorageDevice();
        uint rootPageId;
        byte[] markerShapedPayload = new byte[16];
        Encoding.ASCII.GetBytes("CSDBBOV1").CopyTo(markerShapedPayload, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(markerShapedPayload.AsSpan(8), 1);
        BinaryPrimitives.WriteInt32LittleEndian(markerShapedPayload.AsSpan(12), 1);

        await using (var pager = await CreatePagerAsync(device, ct))
        {
            await pager.InitializeNewDatabaseAsync(ct);
            await pager.RecoverAsync(ct);
            await pager.BeginTransactionAsync(ct);
            rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            await tree.InsertAsync(1, markerShapedPayload, ct);
            await pager.CommitAsync(ct);
            await pager.CheckpointAsync(ct);
        }

        var legacyImage = new byte[checked((int)device.Length)];
        Assert.Equal(legacyImage.Length, await device.ReadAsync(0, legacyImage, ct));
        BinaryPrimitives.WriteInt32LittleEndian(
            legacyImage.AsSpan(PageConstants.VersionOffset),
            PageConstants.MinimumSupportedFormatVersion);

        var reopenedDevice = new MemoryStorageDevice(legacyImage);
        await using var reopenedPager = await CreatePagerAsync(reopenedDevice, ct);
        await reopenedPager.RecoverAsync(ct);
        var reopenedTree = new BTree(reopenedPager, rootPageId);

        Assert.Equal(markerShapedPayload, await reopenedTree.FindAsync(1, ct));
        await using var cursor = reopenedTree.CreateCursor();
        Assert.True(await cursor.MoveNextAsync(ct));
        Assert.Equal(markerShapedPayload, cursor.CurrentValue.ToArray());

        byte[] baseHeader = new byte[PageConstants.FileHeaderSize];
        Assert.Equal(baseHeader.Length, await reopenedDevice.ReadAsync(0, baseHeader, ct));
        Assert.Equal(
            PageConstants.MinimumSupportedFormatVersion,
            BinaryPrimitives.ReadInt32LittleEndian(baseHeader.AsSpan(PageConstants.VersionOffset)));

        await reopenedPager.BeginTransactionAsync(ct);
        using (var canceledCommit = new CancellationTokenSource())
        {
            canceledCommit.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await reopenedPager.CommitAsync(canceledCommit.Token));
        }

        // A failed format-gate flush must roll back and release the legacy writer lock.
        await reopenedPager.BeginTransactionAsync(ct);
        byte[] oversized = CreatePayload(8_000, seed: 73);
        await reopenedTree.InsertAsync(2, oversized, ct);
        await reopenedPager.CommitAsync(ct);

        Assert.Equal(baseHeader.Length, await reopenedDevice.ReadAsync(0, baseHeader, ct));
        Assert.Equal(
            PageConstants.FormatVersion,
            BinaryPrimitives.ReadInt32LittleEndian(baseHeader.AsSpan(PageConstants.VersionOffset)));
        Assert.Equal(oversized, await reopenedTree.FindAsync(2, ct));
    }

    [Fact]
    public async Task MaximumInlinePayloadBoundary_UsesOverflowOnlyAboveTheLimit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        int inlineLimit = OverflowPageStore.MaxInlineBTreePayloadLength;
        byte[] atLimit = CreatePayload(inlineLimit, seed: 41);
        byte[] aboveLimit = CreatePayload(inlineLimit + 1, seed: 42);
        uint pageCountBeforeInsert = pager.PageCount;

        await tree.InsertAsync(1, atLimit, ct);

        Assert.Equal(pageCountBeforeInsert, pager.PageCount);
        Assert.Equal(atLimit, await tree.FindAsync(1, ct));

        Assert.True(await tree.DeleteAsync(1, ct));
        await tree.InsertAsync(2, aboveLimit, ct);

        Assert.True(pager.PageCount > pageCountBeforeInsert);
        Assert.Equal(aboveLimit, await tree.FindAsync(2, ct));

        await using var cursor = tree.CreateCursor();
        Assert.True(await cursor.MoveNextAsync(ct));
        Assert.Equal(2, cursor.CurrentKey);
        Assert.Equal(aboveLimit, cursor.CurrentValue.ToArray());
        Assert.False(await cursor.MoveNextAsync(ct));

        await pager.CommitAsync(ct);
    }

    [Fact]
    public async Task RejectedOversizedWrites_DoNotAllocateOverflowPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        byte[] oversized = CreatePayload(8_000, seed: 19);
        uint initialPageCount = pager.PageCount;

        Assert.False(await tree.ReplaceAsync(404, oversized, ct));
        Assert.Equal(initialPageCount, pager.PageCount);

        await tree.InsertAsync(1, oversized, ct);
        uint insertedPageCount = pager.PageCount;
        CSharpDbException duplicate = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await tree.InsertAsync(1, oversized, ct));
        Assert.Equal(ErrorCode.DuplicateKey, duplicate.Code);
        Assert.Equal(insertedPageCount, pager.PageCount);

        await pager.CommitAsync(ct);
    }

    [Fact]
    public async Task ManyOversizedPayloads_SplitLeavesAndCursorReturnsEveryRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var device = new MemoryStorageDevice();
        await using var pager = await CreatePagerAsync(device, ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await pager.BeginTransactionAsync(ct);

        uint rootPageId = await BTree.CreateNewAsync(pager, ct);
        var tree = new BTree(pager, rootPageId);
        const int rowCount = 180;

        for (int key = 0; key < rowCount; key++)
            await tree.InsertAsync(key, CreatePayload(5_000 + (key % 17), key), ct);

        byte[] rootPage = await pager.GetPageAsync(tree.RootPageId, ct);
        Assert.Equal(PageConstants.PageTypeInterior, rootPage[PageConstants.PageTypeOffset]);

        foreach (int key in new[] { 0, rowCount / 2, rowCount - 1 })
        {
            Assert.Equal(
                CreatePayload(5_000 + (key % 17), key),
                await tree.FindAsync(key, ct));
        }

        await using var cursor = tree.CreateCursor();
        int expectedKey = 0;
        while (await cursor.MoveNextAsync(ct))
        {
            Assert.Equal(expectedKey, cursor.CurrentKey);
            Assert.Equal(
                CreatePayload(5_000 + (expectedKey % 17), expectedKey),
                cursor.CurrentValue.ToArray());
            expectedKey++;
        }

        Assert.Equal(rowCount, expectedKey);
        await pager.CommitAsync(ct);
        await pager.CheckpointAsync(ct);

        byte[] databaseImage = new byte[checked((int)device.Length)];
        Assert.Equal(databaseImage.Length, await device.ReadAsync(0, databaseImage, ct));
        await using var reopenedPager = await CreatePagerAsync(new MemoryStorageDevice(databaseImage), ct);
        await reopenedPager.RecoverAsync(ct);
        var reopenedTree = new BTree(reopenedPager, tree.RootPageId);

        foreach (int key in new[] { 0, rowCount / 2, rowCount - 1 })
        {
            Assert.Equal(
                CreatePayload(5_000 + (key % 17), key),
                await reopenedTree.FindAsync(key, ct));
        }

        await using var reopenedCursor = reopenedTree.CreateCursor();
        expectedKey = 0;
        while (await reopenedCursor.MoveNextAsync(ct))
        {
            Assert.Equal(expectedKey, reopenedCursor.CurrentKey);
            Assert.Equal(
                CreatePayload(5_000 + (expectedKey % 17), expectedKey),
                reopenedCursor.CurrentValue.ToArray());
            expectedKey++;
        }

        Assert.Equal(rowCount, expectedKey);
    }

    private static byte[] CreatePayload(int length, int seed)
    {
        var payload = new byte[length];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)((i * 31 + seed) & 0xFF);
        return payload;
    }

    private static async ValueTask<Pager> CreatePagerAsync(CancellationToken ct)
    {
        return await CreatePagerAsync(new MemoryStorageDevice(), ct);
    }

    private static async ValueTask<Pager> CreatePagerAsync(
        MemoryStorageDevice device,
        CancellationToken ct)
    {
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
