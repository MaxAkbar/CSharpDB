using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class SnapshotPagerWriteStateTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SnapshotDiagnosticsAndReset_DoNotChangeNamedWriterCounters()
    {
        await using var writer = await CreateWriterAsync();
        await writer.BeginTransactionAsync(Ct);
        var tree = new BTree(writer, await BTree.CreateNewAsync(writer, Ct), logicalTableName: "bench");
        for (int key = 1; key <= 128; key++)
        {
            byte[] payload = new byte[512];
            payload[0] = (byte)key;
            await tree.InsertAsync(key, payload, Ct);
        }
        await writer.CommitAsync(Ct);

        CommitPathDiagnosticsSnapshot before = writer.GetCommitPathDiagnosticsSnapshot();
        CommitPathBTreeResourceDiagnosticsSnapshot resource = Assert.Single(before.BTreeResourceDiagnostics!);
        Assert.Equal("table:bench:rowid", resource.ResourceName);
        Assert.True(resource.LeafSplitCount > 0);
        Assert.True(resource.RootSplitCount > 0);

        WalSnapshot snapshot = writer.AcquireReaderSnapshot();
        try
        {
            using var first = writer.CreateSnapshotReader(snapshot);
            using var second = writer.CreateSnapshotReader(snapshot);
            foreach (Pager reader in new[] { first, second })
            {
                AssertNoStructuralDiagnostics(reader);
                var readerTree = new BTree(reader, tree.RootPageId, logicalTableName: "bench");
                byte[] row = Assert.IsType<byte[]>(await readerTree.FindAsync(64, Ct));
                Assert.Equal((byte)64, row[0]);
                reader.ResetCommitPathDiagnostics();
                AssertNoStructuralDiagnostics(reader);
            }

            CommitPathDiagnosticsSnapshot after = writer.GetCommitPathDiagnosticsSnapshot();
            Assert.Equal(StructuralCounters(before), StructuralCounters(after));
            Assert.Equal(resource, Assert.Single(after.BTreeResourceDiagnostics!));

            writer.ResetCommitPathDiagnostics();
            AssertNoStructuralDiagnostics(writer);
            AssertNoStructuralDiagnostics(first);
            AssertNoStructuralDiagnostics(second);

            await writer.BeginTransactionAsync(Ct);
            for (int key = 129; key <= 256; key++)
                await tree.InsertAsync(key, new byte[512], Ct);
            await writer.CommitAsync(Ct);
            CommitPathBTreeResourceDiagnosticsSnapshot resumed =
                Assert.Single(writer.GetCommitPathDiagnosticsSnapshot().BTreeResourceDiagnostics!);
            Assert.Equal(resource.ResourceName, resumed.ResourceName);
            Assert.True(resumed.LeafSplitCount > 0);
            AssertNoStructuralDiagnostics(first);
            AssertNoStructuralDiagnostics(second);
        }
        finally
        {
            writer.ReleaseReaderSnapshot(snapshot);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotAllocationAndFree_ReturnFaultedOperationsWithoutAffectingWriter(bool canceled)
    {
        await using var writer = await CreateWriterAsync();
        await writer.BeginTransactionAsync(Ct);
        uint pageId = await writer.AllocatePageAsync(Ct);
        byte[] page = await writer.GetPageAsync(pageId, Ct);
        page[128] = 10;
        await writer.MarkDirtyAsync(pageId, Ct);
        await writer.CommitAsync(Ct);

        WalSnapshot snapshot = writer.AcquireReaderSnapshot();
        try
        {
            using var first = writer.CreateSnapshotReader(snapshot);
            using var second = writer.CreateSnapshotReader(snapshot);
            uint snapshotPageCount = first.PageCount;
            uint snapshotFreelistHead = first.FreelistHead;
            CancellationToken rejectionToken = canceled ? new CancellationToken(true) : Ct;
            foreach (Pager reader in new[] { first, second })
            {
                // Capture each operation outside the assertion delegate: the API
                // must return a faulted ValueTask rather than throw synchronously.
                ValueTask<uint> allocation = reader.AllocatePageAsync(rejectionToken);
                Assert.True(allocation.IsFaulted);
                var allocationError = await Assert.ThrowsAsync<InvalidOperationException>(() => allocation.AsTask());
                Assert.Equal("Cannot allocate pages on a read-only snapshot pager.", allocationError.Message);

                foreach (uint target in new[] { PageConstants.NullPageId, pageId })
                {
                    ValueTask free = reader.FreePageAsync(target, rejectionToken);
                    Assert.True(free.IsFaulted);
                    var freeError = await Assert.ThrowsAsync<InvalidOperationException>(() => free.AsTask());
                    Assert.Equal("Cannot free pages on a read-only snapshot pager.", freeError.Message);
                }
                Assert.Equal(snapshotPageCount, reader.PageCount);
                Assert.Equal(snapshotFreelistHead, reader.FreelistHead);
            }

            await writer.BeginTransactionAsync(Ct);
            byte[] changed = await writer.GetPageAsync(pageId, Ct);
            changed[128] = 20;
            await writer.MarkDirtyAsync(pageId, Ct);
            uint freedPage = await writer.AllocatePageAsync(Ct);
            await writer.FreePageAsync(freedPage, Ct);
            await writer.CommitAsync(Ct);

            await writer.BeginTransactionAsync(Ct);
            Assert.Equal(freedPage, await writer.AllocatePageAsync(Ct));
            await writer.CommitAsync(Ct);

            // These reads occur after the commits, so the old value must come
            // from the retained snapshot rather than a previously cached page.
            Assert.Equal((byte)10, (await first.GetPageAsync(pageId, Ct))[128]);
            Assert.Equal((byte)10, (await second.GetPageAsync(pageId, Ct))[128]);
            Assert.Equal(snapshotPageCount, first.PageCount);
            Assert.Equal(snapshotPageCount, second.PageCount);

            WalSnapshot latest = writer.AcquireReaderSnapshot();
            try
            {
                using var latestReader = writer.CreateSnapshotReader(latest);
                Assert.Equal((byte)20, (await latestReader.GetPageAsync(pageId, Ct))[128]);
                Assert.Equal(writer.PageCount, latestReader.PageCount);
            }
            finally
            {
                writer.ReleaseReaderSnapshot(latest);
            }
        }
        finally
        {
            writer.ReleaseReaderSnapshot(snapshot);
        }
    }

    private static void AssertNoStructuralDiagnostics(Pager pager)
    {
        CommitPathDiagnosticsSnapshot diagnostics = pager.GetCommitPathDiagnosticsSnapshot();
        Assert.Equal((0L, 0L, 0L, 0L, 0L, 0L, 0L), StructuralCounters(diagnostics));
        Assert.Empty(diagnostics.BTreeResourceDiagnostics ?? []);
    }

    private static (long, long, long, long, long, long, long) StructuralCounters(CommitPathDiagnosticsSnapshot value)
        => (value.BTreeLeafSplitCount, value.BTreeRightEdgeLeafSplitCount,
            value.BTreeInteriorInsertCount, value.BTreeRightEdgeInteriorInsertCount,
            value.BTreeInteriorSplitCount, value.BTreeRightEdgeInteriorSplitCount,
            value.BTreeRootSplitCount);

    private static async ValueTask<Pager> CreateWriterAsync()
    {
        var index = new WalIndex();
        var pager = await Pager.CreateAsync(new MemoryStorageDevice(), new MemoryWriteAheadLog(index), index,
            new PagerOptions { CheckpointPolicy = new FrameCountCheckpointPolicy(int.MaxValue) }, Ct);
        await pager.InitializeNewDatabaseAsync(Ct);
        await pager.RecoverAsync(Ct);
        return pager;
    }
}
