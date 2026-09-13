using CSharpDB.Storage;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public class WalCheckpointBatchTests
{
    [Theory]
    [InlineData(1, false, false)]
    [InlineData(16, false, false)]
    [InlineData(17, false, false)]
    [InlineData(35, false, false)]
    [InlineData(35, true, false)]
    [InlineData(35, false, true)]
    public async Task Checkpoint_MixedWalRuns_PreservesLatestPageImages(
        int pageCount, bool reverseOrder, bool sparsePageIds)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_runs_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        try
        {
            await using var device = new MemoryStorageDevice();
            await using var wal = new WriteAheadLog(dbPath, new WalIndex());
            uint dbPageCount = (uint)(sparsePageIds ? pageCount * 2 : pageCount);
            await wal.OpenAsync(dbPageCount, ct);

            uint[] pageIds = Enumerable.Range(0, pageCount)
                .Select(i => (uint)(sparsePageIds ? i * 2 : i)).ToArray();
            var expected = pageIds.ToDictionary(id => id, id => CreatePage(id, 1));
            var appendOrder = reverseOrder ? pageIds.Reverse() : pageIds.AsEnumerable();
            wal.BeginTransaction();
            await wal.AppendFramesAsync(appendOrder.Select(id => new WalFrameWrite(id, expected[id])).ToArray(), ct);
            await (await wal.CommitAsync(dbPageCount, ct)).WaitAsync(ct);

            // Replacing an interior page breaks a batch into two runs and a singleton.
            // Include both sides of the 16-page checkpoint buffer boundary.
            uint[] replacements = pageIds.Where((_, i) => i % 7 == 3 || i == 15 || i == 16).ToArray();
            if (replacements.Length > 0)
            {
                wal.BeginTransaction();
                foreach (uint pageId in replacements)
                    expected[pageId] = CreatePage(pageId, 2);
                await wal.AppendFramesAsync(replacements.Select(id => new WalFrameWrite(id, expected[id])).ToArray(), ct);
                await (await wal.CommitAsync(dbPageCount, ct)).WaitAsync(ct);
            }

            await wal.CheckpointAsync(device, dbPageCount, ct, allowFinalize: false);
            Assert.True(wal.IsCheckpointCopyComplete);
            await AssertPagesAsync(device, expected, ct);

            // A later commit must survive finalizing the already copied checkpoint.
            uint lastPageId = pageIds[^1];
            byte[] latestPage = CreatePage(lastPageId, 3);
            wal.BeginTransaction();
            await wal.AppendFrameAsync(lastPageId, latestPage, ct);
            await (await wal.CommitAsync(dbPageCount, ct)).WaitAsync(ct);
            await wal.CheckpointAsync(device, dbPageCount, ct);
            await AssertPagesAsync(device, expected, ct);
            Assert.Equal(1, wal.Index.FrameCount);

            expected[lastPageId] = latestPage;
            await wal.CheckpointAsync(device, dbPageCount, ct);
            await AssertPagesAsync(device, expected, ct);
            Assert.Equal(0, wal.Index.FrameCount);
        }
        finally
        {
            if (File.Exists(walPath)) File.Delete(walPath);
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    private static byte[] CreatePage(uint pageId, int generation)
    {
        byte[] page = new byte[PageConstants.PageSize];
        for (int i = 0; i < page.Length; i++)
            page[i] = (byte)((pageId * 31 + i * 17 + generation * 71) & 255);
        return page;
    }

    private static async Task AssertPagesAsync(
        MemoryStorageDevice device, Dictionary<uint, byte[]> expected, CancellationToken ct)
    {
        byte[] actual = new byte[PageConstants.PageSize];
        foreach (var (pageId, page) in expected)
        {
            Assert.Equal(PageConstants.PageSize, await device.ReadAsync((long)pageId * PageConstants.PageSize, actual, ct));
            Assert.Equal(page, actual);
        }
    }
}
