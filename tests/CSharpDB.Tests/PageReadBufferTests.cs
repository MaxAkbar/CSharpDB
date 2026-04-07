using CSharpDB.Storage.Device;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class PageReadBufferTests
{
    [Fact]
    public void OwnedBuffer_MaterializeOwnedBuffer_ReturnsSameArray()
    {
        byte[] page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        page[0] = 42;

        PageReadBuffer buffer = PageReadBuffer.FromOwnedBuffer(page);

        Assert.True(buffer.TryGetOwnedBuffer(out byte[]? owned));
        Assert.Same(page, owned);
        Assert.Same(page, buffer.MaterializeOwnedBuffer());
    }

    [Fact]
    public void ReadOnlyMemory_MaterializeOwnedBuffer_CopiesData()
    {
        byte[] source = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        source[0] = 7;
        source[^1] = 9;

        PageReadBuffer buffer = PageReadBuffer.FromReadOnlyMemory(source);
        byte[] materialized = buffer.MaterializeOwnedBuffer();

        Assert.False(buffer.TryGetOwnedBuffer(out _));
        Assert.NotSame(source, materialized);
        Assert.Equal(source[0], materialized[0]);
        Assert.Equal(source[^1], materialized[^1]);
    }

    [Fact]
    public async Task StorageDevicePageReadProvider_ReadPageAsync_ReadsAndZeroFillsPastEof()
    {
        byte[] initialBytes = new byte[] { 1, 2, 3, 4 };
        await using var device = new MemoryStorageDevice(initialBytes);
        var provider = new StorageDevicePageReadProvider(device);

        PageReadBuffer read = await provider.ReadPageAsync(0, TestContext.Current.CancellationToken);

        Assert.True(read.TryGetOwnedBuffer(out byte[]? page));
        Assert.NotNull(page);
        Assert.Equal(PageConstants.PageSize, page.Length);
        Assert.Equal((byte)1, page[0]);
        Assert.Equal((byte)4, page[3]);
        Assert.All(page[4..], value => Assert.Equal((byte)0, value));
    }

    [Fact]
    public async Task Pager_WithMemoryMappedReads_UsesReadOnlyPageBuffer_AndMaterializesOnMutableAccess()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_mmap_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using (var createPager = await OpenPagerAsync(dbPath, useMemoryMappedReads: false, createNew: true, ct))
            {
                await createPager.InitializeNewDatabaseAsync(ct);
            }

            await using var pager = await OpenPagerAsync(dbPath, useMemoryMappedReads: true, createNew: false, ct);

            PageReadBuffer readOnlyPage = await pager.GetPageReadAsync(0, ct);
            Assert.False(readOnlyPage.TryGetOwnedBuffer(out _));

            byte[] mutablePage = await pager.GetPageAsync(0, ct);
            Assert.Equal(PageConstants.PageSize, mutablePage.Length);

            PageReadBuffer cachedPage = await pager.GetPageReadAsync(0, ct);
            Assert.True(cachedPage.TryGetOwnedBuffer(out var ownedPage));
            Assert.Same(mutablePage, ownedPage);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public async Task Pager_WithMemoryMappedReads_UsesBoundedCache_ForReadOnlyPages()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_mmap_cache_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            uint extraPageId;
            await using (var createPager = await OpenPagerAsync(dbPath, useMemoryMappedReads: false, createNew: true, ct))
            {
                await createPager.InitializeNewDatabaseAsync(ct);
                await createPager.BeginTransactionAsync(ct);
                extraPageId = await createPager.AllocatePageAsync(ct);
                var extraPage = await createPager.GetPageAsync(extraPageId, ct);
                extraPage[123] = 45;
                await createPager.MarkDirtyAsync(extraPageId, ct);
                await createPager.CommitAsync(ct);
            }

            await using var pager = await OpenPagerAsync(
                dbPath,
                useMemoryMappedReads: true,
                createNew: false,
                ct,
                maxCachedPages: 1);

            PageReadBuffer firstPage = await pager.GetPageReadAsync(0, ct);
            Assert.False(firstPage.TryGetOwnedBuffer(out _));
            Assert.True(pager.TryGetCachedPageReadBuffer(0, out _));

            PageReadBuffer secondPage = await pager.GetPageReadAsync(extraPageId, ct);
            Assert.False(secondPage.TryGetOwnedBuffer(out _));

            Assert.False(pager.TryGetCachedPageReadBuffer(0, out _));
            Assert.True(pager.TryGetCachedPageReadBuffer(extraPageId, out _));
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public async Task Pager_WithMemoryMappedReads_RefreshesMappingAfterCheckpointGrowth_WhenOwnedPagePreservationIsDisabled()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_mmap_growth_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var pager = await OpenPagerAsync(
                dbPath,
                useMemoryMappedReads: true,
                createNew: true,
                ct,
                preserveOwnedPagesOnCheckpoint: false);
            await pager.InitializeNewDatabaseAsync(ct);

            await pager.BeginTransactionAsync(ct);
            uint pageId = await pager.AllocatePageAsync(ct);
            byte[] page = await pager.GetPageAsync(pageId, ct);
            page[321] = 99;
            await pager.MarkDirtyAsync(pageId, ct);
            await pager.CommitAsync(ct);
            await pager.CheckpointAsync(ct);

            PageReadBuffer mappedPage = await pager.GetPageReadAsync(pageId, ct);
            Assert.False(mappedPage.TryGetOwnedBuffer(out _));
            Assert.Equal((byte)99, mappedPage.Memory.Span[321]);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public async Task Pager_WithWalReadCache_RetainsReadOnlyWalPages_AfterMainCachePressure()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_cache_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var pager = await OpenPagerAsync(
                dbPath,
                useMemoryMappedReads: false,
                createNew: true,
                ct,
                maxCachedPages: 1,
                maxCachedWalReadPages: 2,
                checkpointPolicy: new FrameCountCheckpointPolicy(1_000_000));

            await pager.InitializeNewDatabaseAsync(ct);

            await pager.BeginTransactionAsync(ct);
            uint pageId = await pager.AllocatePageAsync(ct);
            byte[] newPage = await pager.GetPageAsync(pageId, ct);
            newPage[111] = 54;
            await pager.MarkDirtyAsync(pageId, ct);
            await pager.CommitAsync(ct);

            // Force the main page cache to prefer page 0 so the data page is found via the WAL cache.
            _ = await pager.GetPageReadAsync(0, ct);

            PageReadBuffer walPage = await pager.GetPageReadAsync(pageId, ct);
            Assert.False(walPage.TryGetOwnedBuffer(out _));
            Assert.Equal((byte)54, walPage.Memory.Span[111]);

            _ = await pager.GetPageReadAsync(0, ct);

            Assert.True(pager.TryGetCachedPageReadBuffer(pageId, out PageReadBuffer cachedWalPage));
            Assert.False(cachedWalPage.TryGetOwnedBuffer(out _));
            Assert.Equal((byte)54, cachedWalPage.Memory.Span[111]);

            byte[] mutablePage = await pager.GetPageAsync(pageId, ct);
            Assert.Equal((byte)54, mutablePage[111]);

            Assert.True(pager.TryGetCachedPageReadBuffer(pageId, out PageReadBuffer cachedMutablePage));
            Assert.True(cachedMutablePage.TryGetOwnedBuffer(out var ownedPage));
            Assert.Same(mutablePage, ownedPage);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    private static async ValueTask<Pager> OpenPagerAsync(
        string dbPath,
        bool useMemoryMappedReads,
        bool createNew,
        CancellationToken ct,
        int? maxCachedPages = null,
        int maxCachedWalReadPages = 0,
        ICheckpointPolicy? checkpointPolicy = null,
        bool preserveOwnedPagesOnCheckpoint = true)
    {
        var device = new FileStorageDevice(dbPath, createNew);
        var walIndex = new WalIndex();
        var wal = new WriteAheadLog(dbPath, walIndex);
        var pager = await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions
            {
                UseMemoryMappedReads = useMemoryMappedReads,
                MaxCachedPages = maxCachedPages,
                MaxCachedWalReadPages = maxCachedWalReadPages,
                CheckpointPolicy = checkpointPolicy ?? new FrameCountCheckpointPolicy(PageConstants.DefaultCheckpointThreshold),
                PreserveOwnedPagesOnCheckpoint = preserveOwnedPagesOnCheckpoint,
            },
            ct);

        if (createNew)
            return pager;

        await pager.RecoverAsync(ct);
        return pager;
    }
}
