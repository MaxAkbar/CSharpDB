using CSharpDB.Storage;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public class WalCheckpointBatchTests
{
    [Fact]
    public async Task IncrementalCheckpoint_GrowingCommits_ResizeOnlyForTheirOwnCopy()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_growth_length_{Guid.NewGuid():N}.db");
        try
        {
            await using var device = new MemoryStorageDevice();
            await using var wal = new WriteAheadLog(dbPath, new WalIndex());
            const uint originalPageCount = 35;
            await wal.OpenAsync(originalPageCount, ct);
            var expected = Enumerable.Range(0, (int)originalPageCount)
                .ToDictionary(i => (uint)i, i => CreatePage((uint)i, 1));
            await (await wal.AppendFramesAndCommitAsync(
                expected.Select(p => new WalFrameWrite(p.Key, p.Value)).ToArray(), originalPageCount, ct)).WaitAsync(ct);
            await wal.CheckpointStepAsync(device, originalPageCount, 10, ct, allowFinalize: false);

            for (uint pageId = originalPageCount; pageId < originalPageCount + 2; pageId++)
            {
                byte[] page = CreatePage(pageId, 2);
                expected[pageId] = page;
                expected[0] = CreatePage(0, (int)pageId);
                await (await wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(pageId, page), new WalFrameWrite(0, expected[0]) }, pageId + 1, ct)).WaitAsync(ct);
                await wal.CheckpointStepAsync(device, pageId + 1, 10, ct, allowFinalize: false);
                Assert.Equal((long)originalPageCount * PageConstants.PageSize, device.Length);
            }

            uint finalPageCount = originalPageCount + 2;
            await wal.CheckpointAsync(device, finalPageCount, ct, allowFinalize: false);
            await wal.CheckpointAsync(device, finalPageCount, ct);
            Assert.Equal((long)originalPageCount * PageConstants.PageSize, device.Length);
            Assert.True(wal.Index.FrameCount > 0);
            await wal.CheckpointAsync(device, finalPageCount, ct);
            Assert.Equal((long)finalPageCount * PageConstants.PageSize, device.Length);
            await AssertPagesAsync(device, expected, ct);
            Assert.Equal(0, wal.Index.FrameCount);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
        }
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(15, 0)]
    [InlineData(16, 0)]
    [InlineData(34, 0)]
    [InlineData(16, 1)]
    [InlineData(16, 2)]
    public async Task Checkpoint_StaleIndexAtValidationBatchBoundary_RebuildsCommittedPages(
        int stalePage, int invalidOffsetKind)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_validation_{Guid.NewGuid():N}.db");
        try
        {
            await using var device = new MemoryStorageDevice();
            var index = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, index);
            const int pageCount = 35;
            await wal.OpenAsync(pageCount, ct);
            var expected = Enumerable.Range(0, pageCount).ToDictionary(i => (uint)i, i => CreatePage((uint)i, 1));
            await (await wal.AppendFramesAndCommitAsync(
                expected.Select(entry => new WalFrameWrite(entry.Key, entry.Value)).ToArray(), pageCount, ct)).WaitAsync(ct);

            long staleOffset = invalidOffsetKind switch
            {
                1 => PageConstants.WalHeaderSize + (long)stalePage * PageConstants.WalFrameSize + 1,
                2 => PageConstants.WalHeaderSize + (long)pageCount * PageConstants.WalFrameSize,
                _ => PageConstants.WalHeaderSize + (long)((stalePage + 1) % pageCount) * PageConstants.WalFrameSize,
            };
            index.AddCommittedFrame((uint)stalePage, staleOffset);
            await wal.CheckpointAsync(device, pageCount, ct, allowFinalize: false);
            Assert.True(wal.IsCheckpointCopyComplete);
            Assert.Equal(pageCount, index.FrameCount);
            await AssertPagesAsync(device, expected, ct);
        }
        finally
        {
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Theory]
    [InlineData(15, 0)]
    [InlineData(16, 8)]
    [InlineData(34, 16)]
    public async Task Checkpoint_InvalidHeaderInValidationBatch_RetainsOnlyCompleteValidCommit(
        int invalidPage, int headerByte)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_corrupt_validation_{Guid.NewGuid():N}.db");
        try
        {
            await using var device = new MemoryStorageDevice();
            var index = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, index);
            const int pageCount = 35;
            await wal.OpenAsync(pageCount, ct);
            var expected = Enumerable.Range(0, pageCount).ToDictionary(i => (uint)i, i => CreatePage((uint)i, 1));
            await (await wal.AppendFramesAndCommitAsync(
                expected.Select(entry => new WalFrameWrite(entry.Key, entry.Value)).ToArray(), pageCount, ct)).WaitAsync(ct);
            await (await wal.AppendFramesAndCommitAsync(
                expected.Keys.Select(page => new WalFrameWrite(page, CreatePage(page, 2))).ToArray(), pageCount, ct)).WaitAsync(ct);

            using (var handle = File.OpenHandle(dbPath + ".wal", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                long offset = PageConstants.WalHeaderSize + (long)(pageCount + invalidPage) * PageConstants.WalFrameSize + headerByte;
                byte[] value = new byte[1];
                Assert.Equal(1, RandomAccess.Read(handle, value, offset));
                value[0] ^= 0x80;
                RandomAccess.Write(handle, value, offset);
                RandomAccess.FlushToDisk(handle);
            }

            await wal.CheckpointAsync(device, pageCount, ct, allowFinalize: false);
            Assert.True(wal.IsCheckpointCopyComplete);
            Assert.Equal(pageCount, index.FrameCount);
            await AssertPagesAsync(device, expected, ct);
        }
        finally
        {
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

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
