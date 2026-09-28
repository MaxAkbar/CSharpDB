using System.Buffers.Binary;
using System.Reflection;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class PagerWalHeaderBufferTests
{
    private static readonly FieldInfo WalHeaderBufferField = typeof(Pager).GetField(
        "_walHeaderPageBuffer", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo WalStorageField = typeof(MemoryWriteAheadLog).GetField(
        "_storage", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SnapshotReads_KeepScratchUnallocatedAndPageBuffersIndependent()
    {
        await using var fixture = await MemoryFixture.CreateAsync();
        uint pageId = await CommitNewPageAsync(fixture.Pager, 10);
        Assert.Null(Scratch(fixture.Pager));
        WalSnapshot old = fixture.Pager.AcquireReaderSnapshot();

        try
        {
            using var first = fixture.Pager.CreateSnapshotReader(old);
            using var sameSnapshot = fixture.Pager.CreateSnapshotReader(old);
            Assert.Null(Scratch(first));
            Assert.Null(Scratch(sameSnapshot));
            byte[] oldPage = await first.GetPageAsync(pageId, Ct);
            byte[] independentlyOwned = await sameSnapshot.GetPageAsync(pageId, Ct);
            Assert.NotSame(oldPage, independentlyOwned);
            Assert.Same(oldPage, await first.GetPageAsync(pageId, Ct));

            await fixture.Pager.BeginTransactionAsync(Ct);
            byte[] writerPage = await fixture.Pager.GetPageAsync(pageId, Ct);
            writerPage[128] = 20;
            await fixture.Pager.MarkDirtyAsync(pageId, Ct);
            await fixture.Pager.CommitAsync(Ct);

            WalSnapshot current = fixture.Pager.AcquireReaderSnapshot();
            try
            {
                using var latest = fixture.Pager.CreateSnapshotReader(current);
                byte[] latestPage = await latest.GetPageAsync(pageId, Ct);
                Assert.Equal((byte)10, oldPage[128]);
                Assert.Equal((byte)10, independentlyOwned[128]);
                Assert.Equal((byte)20, latestPage[128]);
                Assert.NotSame(oldPage, latestPage);
                Assert.NotSame(writerPage, latestPage);

                // Returned arrays remain independently owned; scratch allocation
                // must not replace them with a shared or recycled page buffer.
                first.Dispose();
                oldPage[128] = 99;
                Assert.Equal((byte)10, independentlyOwned[128]);
                Assert.Equal((byte)20, latestPage[128]);
                Assert.Null(Scratch(first));
                Assert.Null(Scratch(sameSnapshot));
                Assert.Null(Scratch(latest));
            }
            finally { fixture.Pager.ReleaseReaderSnapshot(current); }
        }
        finally { fixture.Pager.ReleaseReaderSnapshot(old); }

        Assert.Null(Scratch(fixture.Pager));
    }

    [Fact]
    public async Task Rollback_RestoresCommittedWalHeaderAndReusesScratch()
    {
        await using var fixture = await MemoryFixture.CreateAsync();
        uint firstPage = await CommitNewPageAsync(fixture.Pager, 10);
        Assert.True(fixture.Index.TryGetLatest(0, out _));
        Assert.Equal(1u, await ReadMainPageCountAsync(fixture.Device));
        Assert.Null(Scratch(fixture.Pager));

        var committedHeader = Header(fixture.Pager);
        await fixture.Pager.BeginTransactionAsync(Ct);
        uint discardedPage = await fixture.Pager.AllocatePageAsync(Ct);
        fixture.Pager.SchemaRootPage = discardedPage;
        fixture.Pager.FreelistHead = discardedPage;
        Assert.NotEqual(committedHeader, Header(fixture.Pager));
        await fixture.Pager.RollbackAsync(Ct);

        Assert.Equal(committedHeader, Header(fixture.Pager));
        Assert.Equal((byte)10, (await fixture.Pager.GetPageAsync(firstPage, Ct))[128]);
        byte[] scratch = Assert.IsType<byte[]>(Scratch(fixture.Pager));
        Assert.Equal(PageConstants.PageSize, scratch.Length);

        // Change the committed WAL header before repeating rollback, so reuse
        // also has to refresh the buffer rather than parse its previous bytes.
        uint secondPage = await CommitNewPageAsync(fixture.Pager, 20);
        var newerHeader = Header(fixture.Pager);
        Assert.NotEqual(committedHeader, newerHeader);
        await fixture.Pager.BeginTransactionAsync(Ct);
        uint secondDiscardedPage = await fixture.Pager.AllocatePageAsync(Ct);
        fixture.Pager.SchemaRootPage = secondDiscardedPage;
        fixture.Pager.FreelistHead = secondDiscardedPage;
        await fixture.Pager.RollbackAsync(Ct);

        Assert.Equal(newerHeader, Header(fixture.Pager));
        Assert.Equal((byte)20, (await fixture.Pager.GetPageAsync(secondPage, Ct))[128]);
        Assert.Same(scratch, Scratch(fixture.Pager));
        Assert.Equal(1u, await ReadMainPageCountAsync(fixture.Device));
    }

    [Fact]
    public async Task Recovery_UsesCommittedWalPageZeroAndReusesScratch()
    {
        byte[] mainBytes;
        byte[] walBytes;
        uint firstPage;
        (uint PageCount, uint SchemaRoot, uint FreelistHead, uint ChangeCounter) expectedHeader;
        await using (var original = await MemoryFixture.CreateAsync())
        {
            firstPage = await CommitNewPageAsync(original.Pager, 37);
            expectedHeader = Header(original.Pager);
            Assert.Equal(1u, await ReadMainPageCountAsync(original.Device));
            Assert.True(original.Index.TryGetLatest(0, out _));
            Assert.Null(Scratch(original.Pager));
            mainBytes = await CopyDeviceAsync(original.Device);
            walBytes = await CopyDeviceAsync((MemoryStorageDevice)WalStorageField.GetValue(original.Wal)!);
        }

        // Copies are independent of the original pager's clean-close checkpoint.
        await using var recovered = await MemoryFixture.CreateAsync(mainBytes, walBytes);
        Assert.Equal(1u, recovered.Pager.PageCount);
        Assert.Null(Scratch(recovered.Pager));
        await recovered.Pager.RecoverAsync(Ct);

        Assert.Equal(expectedHeader, Header(recovered.Pager));
        Assert.Equal((byte)37, (await recovered.Pager.GetPageAsync(firstPage, Ct))[128]);
        Assert.Equal(expectedHeader.PageCount, await ReadMainPageCountAsync(recovered.Device));
        Assert.Equal(0, recovered.Index.FrameCount);
        byte[] scratch = Assert.IsType<byte[]>(Scratch(recovered.Pager));
        Assert.Equal(PageConstants.PageSize, scratch.Length);

        uint secondPage = await CommitNewPageAsync(recovered.Pager, 73);
        var newerHeader = Header(recovered.Pager);
        Assert.True(recovered.Index.TryGetLatest(0, out _));
        Assert.NotEqual(newerHeader.PageCount, await ReadMainPageCountAsync(recovered.Device));
        await recovered.Pager.RecoverAsync(Ct);

        Assert.Equal(newerHeader, Header(recovered.Pager));
        Assert.Equal((byte)73, (await recovered.Pager.GetPageAsync(secondPage, Ct))[128]);
        Assert.Equal(newerHeader.PageCount, await ReadMainPageCountAsync(recovered.Device));
        Assert.Equal(0, recovered.Index.FrameCount);
        Assert.Same(scratch, Scratch(recovered.Pager));
    }

    [Fact]
    public async Task EmptyWalRecoveryAndMainFileSnapshotRead_DoNotAllocateScratch()
    {
        await using var fixture = await MemoryFixture.CreateAsync();
        await fixture.Pager.RecoverAsync(Ct);
        Assert.Equal(0, fixture.Index.FrameCount);
        Assert.Null(Scratch(fixture.Pager));
        WalSnapshot snapshot = fixture.Pager.AcquireReaderSnapshot();
        try
        {
            using var reader = fixture.Pager.CreateSnapshotReader(snapshot);
            byte[] page = await reader.GetPageAsync(0, Ct);
            Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(PageConstants.PageCountOffset)));
            Assert.Null(Scratch(reader));
        }
        finally { fixture.Pager.ReleaseReaderSnapshot(snapshot); }
        Assert.Null(Scratch(fixture.Pager));
    }

    private static byte[]? Scratch(Pager pager) => (byte[]?)WalHeaderBufferField.GetValue(pager);

    private static (uint PageCount, uint SchemaRoot, uint FreelistHead, uint ChangeCounter) Header(Pager pager)
        => (pager.PageCount, pager.SchemaRootPage, pager.FreelistHead, pager.ChangeCounter);

    private static async ValueTask<uint> CommitNewPageAsync(Pager pager, byte marker)
    {
        await pager.BeginTransactionAsync(Ct);
        uint pageId = await pager.AllocatePageAsync(Ct);
        byte[] page = await pager.GetPageAsync(pageId, Ct);
        page[128] = marker;
        pager.SchemaRootPage = pageId;
        await pager.MarkDirtyAsync(pageId, Ct);
        await pager.CommitAsync(Ct);
        return pageId;
    }

    private static async ValueTask<uint> ReadMainPageCountAsync(MemoryStorageDevice device)
    {
        byte[] header = new byte[PageConstants.FileHeaderSize];
        Assert.Equal(header.Length, await device.ReadAsync(0, header, Ct));
        return BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(PageConstants.PageCountOffset));
    }

    private static async ValueTask<byte[]> CopyDeviceAsync(MemoryStorageDevice device)
    {
        byte[] bytes = new byte[checked((int)device.Length)];
        Assert.Equal(bytes.Length, await device.ReadAsync(0, bytes, Ct));
        return bytes;
    }

    private sealed class MemoryFixture(
        Pager pager, MemoryStorageDevice device, MemoryWriteAheadLog wal, WalIndex index) : IAsyncDisposable
    {
        public Pager Pager { get; } = pager;
        public MemoryStorageDevice Device { get; } = device;
        public MemoryWriteAheadLog Wal { get; } = wal;
        public WalIndex Index { get; } = index;

        public static async ValueTask<MemoryFixture> CreateAsync(
            ReadOnlyMemory<byte> mainBytes = default, ReadOnlyMemory<byte> walBytes = default)
        {
            var device = new MemoryStorageDevice(mainBytes);
            var index = new WalIndex();
            var wal = new MemoryWriteAheadLog(index, initialBytes: walBytes);
            var pager = await CSharpDB.Storage.Paging.Pager.CreateAsync(device, wal, index, new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(int.MaxValue),
                MaxCachedWalReadPages = 0,
            }, Ct);
            if (mainBytes.IsEmpty)
                await pager.InitializeNewDatabaseAsync(Ct);
            return new MemoryFixture(pager, device, wal, index);
        }

        public ValueTask DisposeAsync() => Pager.DisposeAsync();
    }
}
