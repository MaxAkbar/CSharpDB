using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class PagerDirtyBufferTests
{
    [Fact]
    public async Task DirtyPage_EvictedAndRevisitedWithinTransaction_PreservesAllMutations()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_dirty_buffer_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new PagerOptions
        {
            MaxCachedPages = 1,
            CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
        };

        try
        {
            uint pageId;
            await using (var pager = await OpenPagerAsync(dbPath, options, createNew: true, ct))
            {
                await pager.BeginTransactionAsync(ct);
                pageId = await pager.AllocatePageAsync(ct);
                var baselinePage = await pager.GetPageAsync(pageId, ct);
                baselinePage[100] = 10;
                await pager.MarkDirtyAsync(pageId, ct);
                await pager.CommitAsync(ct);

                await pager.BeginTransactionAsync(ct);
                var firstBuffer = await pager.GetPageAsync(pageId, ct);
                firstBuffer[100] = 20;
                await pager.MarkDirtyAsync(pageId, ct);

                uint evictingPageId = await pager.AllocatePageAsync(ct);
                var evictingPage = await pager.GetPageAsync(evictingPageId, ct);
                evictingPage[200] = 77;
                await pager.MarkDirtyAsync(evictingPageId, ct);

                var revisitedBuffer = await pager.GetPageAsync(pageId, ct);
                Assert.Same(firstBuffer, revisitedBuffer);

                revisitedBuffer[101] = 30;
                await pager.MarkDirtyAsync(pageId, ct);
                await pager.CommitAsync(ct);
            }

            await using (var verifyPager = await OpenPagerAsync(dbPath, options, createNew: false, ct))
            {
                var persistedPage = await verifyPager.GetPageAsync(pageId, ct);
                Assert.Equal((byte)20, persistedPage[100]);
                Assert.Equal((byte)30, persistedPage[101]);
            }
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
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
