using CSharpDB.Storage;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public class WalCheckpointBatchTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Checkpoint_InvalidCopiedPrefix_PreservesLaterCommitAfterReopen(bool corruptSupersededFrame)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_prefix_reopen_{Guid.NewGuid():N}.db");
        byte[] page0 = CreatePage(0, 1);
        byte[] page1 = CreatePage(1, 1);
        byte[] replacedPage1 = CreatePage(1, 2);
        byte[] retainedPage2 = CreatePage(2, 3);
        try
        {
            await using (var device = new FileStorageDevice(dbPath, createNew: true))
            await using (var wal = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await wal.OpenAsync(3, ct);
                await (await wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(0, page0), new WalFrameWrite(1, page1) }, 3, ct)).WaitAsync(ct);
                await (await wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(1, replacedPage1) }, 3, ct)).WaitAsync(ct);

                // An indexed invalid frame triggers repair. An invalid older
                // version is absent from the latest-page map, but recovery
                // would still encounter it if finalization left it on disk.
                int corruptFrame = corruptSupersededFrame ? 1 : 2;
                using (var handle = File.OpenHandle(dbPath + ".wal", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                    long offset = PageConstants.WalHeaderSize + (long)corruptFrame * PageConstants.WalFrameSize;
                    byte[] value = new byte[1];
                    Assert.Equal(1, RandomAccess.Read(handle, value, offset));
                    value[0] ^= 0x80;
                    RandomAccess.Write(handle, value, offset);
                    RandomAccess.FlushToDisk(handle);
                }

                await wal.CheckpointAsync(device, 3, ct, allowFinalize: false);
                Assert.True(wal.IsCheckpointCopyComplete);
                Assert.True(wal.TryGetCheckpointRetainedWalStartOffset(out long retainedStart));
                Assert.Equal(PageConstants.WalHeaderSize + 3L * PageConstants.WalFrameSize, retainedStart);

                await (await wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(2, retainedPage2) }, 3, ct)).WaitAsync(ct);
                await wal.CheckpointAsync(device, 3, ct);
                Assert.False(wal.HasPendingCheckpoint);
                Assert.Equal(1, wal.Index.FrameCount);
            }

            // Reopen before copying the retained commit into the database.
            // It must remain recoverable despite the discarded invalid prefix.
            await using (var device = new FileStorageDevice(dbPath))
            await using (var reopened = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await reopened.OpenAsync(3, ct);
                Assert.Equal(1, reopened.Index.FrameCount);
                await AssertWalPagesAsync(reopened, new() { [2] = retainedPage2 }, ct);
                byte[] actual = new byte[PageConstants.PageSize];
                Assert.Equal(PageConstants.PageSize, await device.ReadAsync(0, actual, ct));
                Assert.Equal(page0, actual);
                Assert.Equal(PageConstants.PageSize, await device.ReadAsync(PageConstants.PageSize, actual, ct));
                Assert.Equal(corruptSupersededFrame ? replacedPage1 : page1, actual);
            }
        }
        finally
        {
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Theory]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(257)]
    public async Task Checkpoint_RetainedSuffixAcrossMoveChunks_PreservesCommitsAfterReopen(int retainedFrameCount)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_move_{Guid.NewGuid():N}.db");
        try
        {
            await using var device = new MemoryStorageDevice();
            uint pageCount = (uint)retainedFrameCount - 1;
            var original = new Dictionary<uint, byte[]> { [0] = CreatePage(0, 1), [1] = CreatePage(1, 1) };
            var expected = Enumerable.Range(0, (int)pageCount)
                .ToDictionary(i => (uint)i, i => CreatePage((uint)i, 2));

            await using (var wal = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await wal.OpenAsync(2, ct);
                await (await wal.AppendFramesAndCommitAsync(
                    original.Select(p => new WalFrameWrite(p.Key, p.Value)).ToArray(), 2, ct)).WaitAsync(ct);
                await wal.CheckpointAsync(device, 2, ct, allowFinalize: false);

                // The small copied prefix makes source and destination overlap
                // during larger moves. The last commit replaces a prior page;
                // its marker may sit in a partial final chunk.
                await (await wal.AppendFramesAndCommitAsync(
                    expected.Select(p => new WalFrameWrite(p.Key, p.Value)).ToArray(), pageCount, ct)).WaitAsync(ct);
                expected[0] = CreatePage(0, 3);
                await (await wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(0, expected[0]) }, pageCount, ct)).WaitAsync(ct);

                long generationBeforeCompaction = wal.Index.CommitCounter;
                await wal.CheckpointAsync(device, pageCount, ct);
                Assert.False(wal.HasPendingCheckpoint);
                Assert.Equal(retainedFrameCount, wal.Index.FrameCount);
                Assert.Equal(generationBeforeCompaction + 2, wal.Index.CommitCounter);
                Assert.Equal(PageConstants.WalHeaderSize + (long)retainedFrameCount * PageConstants.WalFrameSize,
                    new FileInfo(dbPath + ".wal").Length);
                await AssertPagesAsync(device, original, ct);
                await AssertWalPagesAsync(wal, expected, ct);
            }

            await using (var reopened = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await reopened.OpenAsync(2, ct);
                Assert.Equal(retainedFrameCount, reopened.Index.FrameCount);
                Assert.Equal(2L, reopened.Index.CommitCounter);
                await AssertWalPagesAsync(reopened, expected, ct);

                // A new append must start after the relocated suffix.
                expected[pageCount] = CreatePage(pageCount, 4);
                await (await reopened.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(pageCount, expected[pageCount]) }, pageCount + 1, ct)).WaitAsync(ct);
                await reopened.CheckpointAsync(device, pageCount + 1, ct);
                Assert.Equal(0, reopened.Index.FrameCount);
                await AssertPagesAsync(device, expected, ct);
            }
        }
        finally
        {
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    private static async Task AssertWalPagesAsync(
        WriteAheadLog wal, Dictionary<uint, byte[]> expected, CancellationToken ct)
    {
        foreach (var (pageId, page) in expected)
        {
            Assert.True(wal.Index.TryGetLatest(pageId, out long offset));
            Assert.Equal(page, await wal.ReadPageAsync(offset, ct));
        }
    }

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
