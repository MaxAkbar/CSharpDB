using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class BTreeCursorReadAheadTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task UpperBoundOnCurrentLeaf_DoesNotReadNextLeaf(bool seek, bool inclusive)
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var cursor = fixture.Tree.CreateCursor(new IndexScanRange(1, true, fixture.FirstLastKey, inclusive));

        Assert.True(seek ? await cursor.SeekAsync(1, Ct) : await cursor.MoveNextAsync(Ct));
        Assert.Equal(1L, cursor.CurrentKey);
        Assert.Equal(1, fixture.Device.ReadCount(fixture.FirstLeaf));
        Assert.Equal(0, fixture.Device.ReadCount(fixture.SecondLeaf));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UpperBoundOnNextLeaf_PrefetchesOnlyThroughThatLeaf(bool inclusive)
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var cursor = fixture.Tree.CreateCursor(new IndexScanRange(1, true, fixture.SecondLastKey, inclusive));

        Assert.True(await cursor.SeekAsync(1, Ct));
        Assert.Equal(1, fixture.Device.ReadCount(fixture.SecondLeaf));
        while (cursor.CurrentKey <= fixture.FirstLastKey)
            Assert.True(await cursor.MoveNextAsync(Ct));

        Assert.Equal(fixture.FirstLastKey + 1, cursor.CurrentKey);
        Assert.Equal(1, fixture.Device.ReadCount(fixture.SecondLeaf));
        Assert.Equal(0, fixture.Device.ReadCount(fixture.ThirdLeaf));
    }

    [Fact]
    public async Task UnboundedCursor_StillPrefetchesNextLeaf()
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var cursor = fixture.Tree.CreateCursor();
        Assert.True(await cursor.SeekAsync(1, Ct));
        Assert.Equal(1, fixture.Device.ReadCount(fixture.SecondLeaf));
    }

    [Fact]
    public async Task ReadAheadHint_DoesNotRestrictCursorTraversalOrReseek()
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var cursor = fixture.Tree.CreateCursor(IndexScanRange.At(1));
        var keys = new List<long>();
        while (await cursor.MoveNextAsync(Ct)) keys.Add(cursor.CurrentKey);
        Assert.Equal(Enumerable.Range(1, 128).Select(i => (long)i), keys);
        Assert.True(await cursor.SeekAsync(100, Ct));
        Assert.Equal(100L, cursor.CurrentKey);
        Assert.True(await cursor.MoveNextAsync(Ct));
        Assert.Equal(101L, cursor.CurrentKey);
    }

    private sealed class Fixture(Pager writer, Pager reader, WalSnapshot snapshot,
        CountingDevice device, BTree tree, uint firstLeaf, uint secondLeaf, uint thirdLeaf,
        long firstLastKey, long secondLastKey) : IAsyncDisposable
    {
        public CountingDevice Device => device;
        public BTree Tree => tree;
        public uint FirstLeaf => firstLeaf;
        public uint SecondLeaf => secondLeaf;
        public uint ThirdLeaf => thirdLeaf;
        public long FirstLastKey => firstLastKey;
        public long SecondLastKey => secondLastKey;

        public static async Task<Fixture> CreateAsync()
        {
            var device = new CountingDevice();
            var index = new WalIndex();
            var writer = await Pager.CreateAsync(device, new MemoryWriteAheadLog(index), index,
                new PagerOptions { EnableSequentialLeafReadAhead = true, CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000) }, Ct);
            await writer.InitializeNewDatabaseAsync(Ct);
            await writer.RecoverAsync(Ct);
            await writer.BeginTransactionAsync(Ct);
            var tree = new BTree(writer, await BTree.CreateNewAsync(writer, Ct));
            for (int key = 1; key <= 128; key++) await tree.InsertAsync(key, new byte[512], Ct);
            await writer.CommitAsync(Ct);
            uint firstId = await tree.FindLeftmostLeafAsync(Ct);
            var first = new ReadOnlySlottedPage((await writer.GetPageReadAsync(firstId, Ct)).Memory, firstId);
            uint secondId = first.RightChildOrNextLeaf;
            var second = new ReadOnlySlottedPage((await writer.GetPageReadAsync(secondId, Ct)).Memory, secondId);
            uint thirdId = second.RightChildOrNextLeaf;
            Assert.NotEqual(PageConstants.NullPageId, thirdId);
            long firstLast = BTree.ReadLeafKey(first, first.CellCount - 1);
            long secondLast = BTree.ReadLeafKey(second, second.CellCount - 1);
            await writer.CheckpointAsync(Ct);
            var snapshot = writer.AcquireReaderSnapshot();
            var reader = writer.CreateSnapshotReader(snapshot);
            device.Reads.Clear();
            return new(writer, reader, snapshot, device, new BTree(reader, tree.RootPageId), firstId, secondId, thirdId, firstLast, secondLast);
        }

        public async ValueTask DisposeAsync()
        {
            reader.Dispose();
            writer.ReleaseReaderSnapshot(snapshot);
            await writer.DisposeAsync();
        }
    }

    private sealed class CountingDevice : IStorageDevice
    {
        private readonly MemoryStorageDevice _inner = new();
        public List<uint> Reads { get; } = [];
        public int ReadCount(uint page) => Reads.Count(p => p == page);
        public long Length => _inner.Length;
        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
        {
            Reads.Add((uint)(offset / PageConstants.PageSize));
            return _inner.ReadAsync(offset, buffer, ct);
        }
        public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(offset, buffer, ct);
        public ValueTask FlushAsync(CancellationToken ct = default) => _inner.FlushAsync(ct);
        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => _inner.SetLengthAsync(length, ct);
        public void Dispose() => _inner.Dispose();
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
