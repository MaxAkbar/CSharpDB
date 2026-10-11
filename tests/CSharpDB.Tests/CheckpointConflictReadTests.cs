using System.Buffers.Binary;
using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Transactions;
using CSharpDB.Storage.Wal;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class CheckpointConflictReadTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetainedTailCompaction_WaitsForConflictRead_AndPreservesPageContents(bool staleOffsetInBounds)
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync(keepReader: true);
        await using var tx = await f.CreateConflictingTransactionAsync();
        Assert.False(tx.State.Snapshot.HasWalFrames); // The completed prefix was filtered out.
        long offset = f.LatestOffset(f.PageId);
        byte[] expected = await f.Wal.ReadPageAsync(offset, f.Ct);
        Assert.True(f.Wal.TryGetCheckpointRetainedWalStartOffset(out long retainedStart));
        long removedBytes = retainedStart - PageConstants.WalHeaderSize;

        if (staleOffsetInBounds)
        {
            // Leave enough later frames that the stale offset reads a different page,
            // rather than EOF. Checking only file length would miss this corruption.
            for (byte marker = 91; marker <= 93; marker++)
                await f.CommitMarkerAsync(f.OtherPageId, marker);
            Assert.True(offset + PageConstants.WalFrameSize <= f.WalLength - removedBytes);
            byte[] displaced = await f.Wal.ReadPageAsync(offset + removedBytes, f.Ct);
            Assert.Equal((byte)f.OtherPageId, displaced[129]);
            Assert.NotEqual(expected[128], displaced[128]);
        }
        else
        {
            Assert.True(offset + PageConstants.WalFrameSize > f.WalLength - removedBytes);
        }

        f.ReleasePrefixReader();
        f.Wal.PauseOffset = offset;
        Task commit = tx.CommitAsync(f.Ct).AsTask();
        await f.Wal.ReadEntered.Task.WaitAsync(f.Ct);
        int finalizeCalls = f.Wal.FinalizeCalls;
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        bool finalizedDuringRead = f.Wal.FinalizeCalls != finalizeCalls;
        try
        {
            // With the old code finalization has already been entered. Finish it
            // before releasing the read to deterministically expose EOF/wrong bytes.
            if (finalizedDuringRead)
                await checkpoint.WaitAsync(f.Ct);
            else
                Assert.False(checkpoint.IsCompleted);
        }
        finally
        {
            f.Wal.ResumeRead.TrySetResult();
        }

        Exception? commitError = await Record.ExceptionAsync(() => commit.WaitAsync(f.Ct));
        await checkpoint.WaitAsync(f.Ct);
        if (staleOffsetInBounds)
            Assert.Equal(expected, f.Wal.PausedReadContents);
        Assert.Null(commitError);
        Assert.Equal(expected, f.Wal.PausedReadContents);
        Assert.False(finalizedDuringRead);
        await f.AssertProgressAndResetAsync();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RetainedTailCompaction_ProtectsParentAndSplitRightReads(bool pauseSplitRight, bool dirtyParent)
    {
        await using var f = await Fixture.CreateAsync();
        (uint rootPageId, uint rightLeafPageId) = await f.SeedSplitTreeAsync();
        await f.PrepareCopiedPrefixAsync(keepReader: true);
        await using var winner = await f.Pager.BeginWriteTransactionAsync(f.Ct);
        await using var loser = await f.Pager.BeginWriteTransactionAsync(f.Ct);
        Assert.False(loser.State.Snapshot.HasWalFrames);

        byte[] winnerPayload = Enumerable.Repeat((byte)0x44, 1000).ToArray();
        byte[] loserPayload = Enumerable.Repeat((byte)0x55, dirtyParent ? 1000 : 500).ToArray();
        using (winner.Bind())
            await new BTree(f.Pager, rootPageId).InsertAsync(200, winnerPayload, f.Ct);
        using (loser.Bind())
            await new BTree(f.Pager, rootPageId).InsertAsync(300, loserPayload, f.Ct);
        Assert.Equal(dirtyParent, loser.State.DirtyPages.Contains(rootPageId));
        await winner.CommitAsync(f.Ct);

        PageReadBuffer committedLeft = await f.Pager.ReadPageUncachedAsync(rightLeafPageId, f.Ct);
        uint splitRightPageId = new ReadOnlySlottedPage(committedLeft.Memory, rightLeafPageId).RightChildOrNextLeaf;
        Assert.NotEqual(PageConstants.NullPageId, splitRightPageId);
        uint pausedPageId = pauseSplitRight ? splitRightPageId : rootPageId;
        long offset = f.LatestOffset(pausedPageId);
        byte[] expected = await f.Wal.ReadPageAsync(offset, f.Ct);
        Assert.True(f.Wal.TryGetCheckpointRetainedWalStartOffset(out long retainedStart));
        long removedBytes = retainedStart - PageConstants.WalHeaderSize;
        for (byte marker = 91; marker <= 93; marker++)
            await f.CommitMarkerAsync(f.PageId, marker);
        Assert.True(offset + PageConstants.WalFrameSize <= f.WalLength - removedBytes);
        byte[] displaced = await f.Wal.ReadPageAsync(offset + removedBytes, f.Ct);
        Assert.False(expected.AsSpan().SequenceEqual(displaced));
        f.ReleasePrefixReader();

        f.Wal.PauseOffset = offset;
        Task commit = loser.CommitAsync(f.Ct).AsTask();
        await f.Wal.ReadEntered.Task.WaitAsync(f.Ct);
        int finalizeCalls = f.Wal.FinalizeCalls;
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        bool finalizedDuringRead = f.Wal.FinalizeCalls != finalizeCalls;
        try
        {
            if (finalizedDuringRead)
                await checkpoint.WaitAsync(f.Ct);
            else
                Assert.False(checkpoint.IsCompleted);
        }
        finally
        {
            f.Wal.ResumeRead.TrySetResult();
        }
        Exception? commitError = await Record.ExceptionAsync(() => commit.WaitAsync(f.Ct));
        await checkpoint.WaitAsync(f.Ct);
        Assert.Equal(expected, f.Wal.PausedReadContents);
        Assert.Null(commitError);
        Assert.False(finalizedDuringRead);

        await f.Pager.CheckpointAsync(f.Ct);
        await f.Pager.CheckpointAsync(f.Ct);
        Assert.Equal(PageConstants.WalHeaderSize, f.WalLength);
        var committedTree = new BTree(f.Pager, rootPageId);
        Assert.Equal(5L, await committedTree.CountEntriesAsync(f.Ct));
        Assert.Equal(Enumerable.Repeat((byte)0x11, 32), await committedTree.FindAsync(10, f.Ct));
        Assert.Equal(Enumerable.Repeat((byte)0x22, 2052), await committedTree.FindAsync(100, f.Ct));
        Assert.Equal(Enumerable.Repeat((byte)0x33, 1000), await committedTree.FindAsync(150, f.Ct));
        Assert.Equal(winnerPayload, await committedTree.FindAsync(200, f.Ct));
        Assert.Equal(loserPayload, await committedTree.FindAsync(300, f.Ct));
        Assert.Equal(0, f.Transactions.ActiveWriterCount);
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public async Task ConflictRead_ResetAndRetentionControls_PreserveContents(bool retainSnapshot, bool checkpointDuringRead)
    {
        await using var f = await Fixture.CreateAsync();
        if (retainSnapshot)
            await f.CommitMarkerAsync(f.PageId, 11);
        await using var tx = await f.CreateConflictingTransactionAsync();
        Assert.Equal(retainSnapshot, tx.State.Snapshot.HasWalFrames);
        long offset = f.LatestOffset(f.PageId);
        byte[] expected = await f.Wal.ReadPageAsync(offset, f.Ct);
        f.Wal.PauseOffset = offset;
        Task commit = tx.CommitAsync(f.Ct).AsTask();
        await f.Wal.ReadEntered.Task.WaitAsync(f.Ct);
        long length = f.WalLength;
        int finalizeCalls = f.Wal.FinalizeCalls;
        try
        {
            if (checkpointDuringRead)
            {
                await f.Pager.CheckpointAsync(f.Ct);
                Assert.True(f.Wal.HasPendingCheckpoint);
                Assert.Equal(length, f.WalLength);
                Assert.Equal(finalizeCalls, f.Wal.FinalizeCalls);
            }
        }
        finally
        {
            f.Wal.ResumeRead.TrySetResult();
        }
        await commit.WaitAsync(f.Ct);
        Assert.Equal(expected, f.Wal.PausedReadContents);
        await f.AssertProgressAndResetAsync();
    }

    [Fact]
    public async Task CopyCompleteReset_TakesCommitLockBeforeBeginBarrier()
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync();
        IDisposable commitLock = await f.Transactions.AcquireCommitLockAsync(f.Ct);
        int finalizeCalls = f.Wal.FinalizeCalls;
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        try
        {
            Assert.False(checkpoint.IsCompleted);
            Assert.Equal(finalizeCalls, f.Wal.FinalizeCalls);
            // A finalizer that takes begin first deadlocks an explicit commit that
            // already owns commit and needs to enter its pending-commit window.
            using var begin = await f.Transactions.AcquireBeginBarrierAsync(f.Ct);
        }
        finally
        {
            commitLock.Dispose();
        }
        await checkpoint.WaitAsync(f.Ct);
        Assert.Equal(PageConstants.WalHeaderSize, f.WalLength);
        await f.AssertProgressAndResetAsync();
    }

    [Theory]
    [InlineData("commit")]
    [InlineData("begin")]
    [InlineData("writer")]
    [InlineData("finalize")]
    public async Task CancelledFinalization_ReleasesAcquiredLocks_AndAllowsProgress(string blockedAt)
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(f.Ct);
        IDisposable? held = null;
        if (blockedAt == "commit")
            held = await f.Transactions.AcquireCommitLockAsync(f.Ct);
        else if (blockedAt == "begin")
            held = await f.Transactions.AcquireBeginBarrierAsync(f.Ct);
        else if (blockedAt == "writer")
            await f.Pager.BeginTransactionAsync(f.Ct);
        else
            f.Wal.BeforeFinalize = ct => new ValueTask(Task.Delay(Timeout.Infinite, ct));

        Task checkpoint = f.Pager.CheckpointAsync(cancellation.Token).AsTask();
        try
        {
            Assert.False(checkpoint.IsCompleted);
            if (blockedAt == "finalize")
                await f.Wal.FinalizeEntered.Task.WaitAsync(f.Ct);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => checkpoint.WaitAsync(f.Ct));
        }
        finally
        {
            held?.Dispose();
            f.Wal.BeforeFinalize = null;
            if (blockedAt == "writer")
                await f.Pager.RollbackAsync(f.Ct);
        }
        await f.AssertProgressAndResetAsync();
    }

    [Fact]
    public async Task FinalizationFailure_ReleasesAllLocks_AndAllowsRetry()
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync();
        f.Wal.BeforeFinalize = _ => throw new IOException("Injected checkpoint finalization failure.");
        await Assert.ThrowsAsync<IOException>(() => f.Pager.CheckpointAsync(f.Ct).AsTask());
        f.Wal.BeforeFinalize = null;
        await f.AssertProgressAndResetAsync();
    }

    [Fact]
    public async Task CancelledConflictRead_ReleasesCommitLock_AndUnblocksCompaction()
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync(keepReader: true);
        await using var tx = await f.CreateConflictingTransactionAsync();
        f.ReleasePrefixReader();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(f.Ct);
        f.Wal.PauseOffset = f.LatestOffset(f.PageId);
        Task commit = tx.CommitAsync(cancellation.Token).AsTask();
        await f.Wal.ReadEntered.Task.WaitAsync(f.Ct);
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => commit.WaitAsync(f.Ct));
        await checkpoint.WaitAsync(f.Ct);
        Assert.True(tx.State.Completed);
        await f.AssertProgressAndResetAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Finalization_WithPendingDurableCommit_AllowsCompletion(bool legacyWriter)
    {
        var flush = new PausingFlushPolicy();
        await using var f = await Fixture.CreateAsync(flush);
        await f.PrepareCopiedPrefixAsync();
        await using var tx = legacyWriter ? null : await f.Pager.BeginWriteTransactionAsync(f.Ct);
        if (legacyWriter)
            await f.Pager.BeginTransactionAsync(f.Ct);
        using (tx?.Bind())
            await f.SetMarkerAsync(f.PageId, 25);

        flush.Armed = true;
        Task<PagerCommitResult> append = legacyWriter
            ? f.Pager.BeginCommitAsync(f.Ct).AsTask()
            : tx!.BeginCommitAsync(f.Ct).AsTask();
        await flush.Entered.Task.WaitAsync(f.Ct);
        int finalizeCalls = f.Wal.FinalizeCalls;
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        try
        {
            if (legacyWriter)
            {
                await checkpoint.WaitAsync(f.Ct);
                Assert.True(f.Wal.HasPendingCheckpoint);
                Assert.Equal(finalizeCalls, f.Wal.FinalizeCalls);
            }
            else
            {
                Assert.False(checkpoint.IsCompleted);
            }
        }
        finally
        {
            flush.Resume.TrySetResult();
        }
        await (await append.WaitAsync(f.Ct)).WaitAsync(f.Ct);
        await checkpoint.WaitAsync(f.Ct);
        await f.AssertProgressAndResetAsync();
    }

    [Fact]
    public async Task Finalization_WaitingForLegacyWriter_DoesNotBlockItsCommit()
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync();
        await f.Pager.BeginTransactionAsync(f.Ct);
        await f.SetMarkerAsync(f.PageId, 25);
        Task checkpoint = f.Pager.CheckpointAsync(f.Ct).AsTask();
        Assert.False(checkpoint.IsCompleted);
        await f.Pager.CommitAsync(f.Ct);
        await checkpoint.WaitAsync(f.Ct);
        await f.AssertProgressAndResetAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Finalization_HoldsCommitLockThroughStateRefresh_AndReleasesIt(bool cancel)
    {
        await using var f = await Fixture.CreateAsync();
        await f.PrepareCopiedPrefixAsync(keepReader: true);
        await f.CommitMarkerAsync(f.PageId, 25);
        f.ReleasePrefixReader();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(f.Ct);
        f.Wal.AfterFinalize = () => f.Wal.PauseOffset = f.LatestOffset(0);
        Task checkpoint = f.Pager.CheckpointAsync(cancellation.Token).AsTask();
        await f.Wal.ReadEntered.Task.WaitAsync(f.Ct);
        Task<IDisposable> commitLock = f.Transactions.AcquireCommitLockAsync(f.Ct).AsTask();
        try
        {
            Assert.False(commitLock.IsCompleted);
        }
        finally
        {
            f.Wal.AfterFinalize = null;
            if (cancel)
                cancellation.Cancel();
            else
                f.Wal.ResumeRead.TrySetResult();
        }
        if (cancel)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => checkpoint.WaitAsync(f.Ct));
        else
            await checkpoint.WaitAsync(f.Ct);
        (await commitLock.WaitAsync(f.Ct)).Dispose();
        await f.AssertProgressAndResetAsync();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(250)]
    public async Task BackgroundCheckpoint_EightSqlWriters_PreserveAllRowsAfterReopen(int batchMicroseconds)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(60));
        CancellationToken ct = deadline.Token;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_writers_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UseDurableCommitBatchWindow(TimeSpan.FromMicroseconds(batchMicroseconds));
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(16),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = 4,
                MaxCachedPages = 16,
                MaxCachedWalReadPages = 0,
            });
        });
        try
        {
            await using (var db = await Database.OpenAsync(path, options, ct))
            {
                await db.ExecuteAsync("CREATE TABLE writes (id INTEGER PRIMARY KEY, writer INTEGER, sequence INTEGER)", ct);
                var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Task[] writers = Enumerable.Range(0, 8).Select(writer => Task.Run(async () =>
                {
                    await start.Task.WaitAsync(ct);
                    for (int sequence = 0; sequence < 24; sequence++)
                    {
                        await db.RunWriteTransactionAsync(async (tx, innerCt) =>
                        {
                            await tx.ExecuteAsync($"INSERT INTO writes (writer, sequence) VALUES ({writer}, {sequence})", innerCt);
                        }, new WriteTransactionOptions { MaxRetries = 40 }, ct);
                    }
                }, ct)).ToArray();
                start.SetResult();
                await Task.WhenAll(writers).WaitAsync(ct);
                await db.CheckpointAsync(ct);
            }

            await using var reopened = await Database.OpenAsync(path, options, ct);
            await using var result = await reopened.ExecuteAsync("SELECT id, writer, sequence FROM writes", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(192, rows.Count);
            Assert.Equal(192, rows.Select(row => row[0].AsInteger).Distinct().Count());
            Assert.Equal(
                Enumerable.Range(0, 8).SelectMany(writer => Enumerable.Range(0, 24).Select(sequence => ((long)writer, (long)sequence))),
                rows.Select(row => (row[1].AsInteger, row[2].AsInteger)).OrderBy(row => row.Item1).ThenBy(row => row.Item2));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly CancellationTokenSource _deadline =
            CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_conflict_{Guid.NewGuid():N}.db");
        public CancellationToken Ct => _deadline.Token;
        public WalIndex Index { get; } = new();
        public PausingWal Wal { get; private set; } = null!;
        public Pager Pager { get; private set; } = null!;
        public uint PageId { get; private set; }
        public uint OtherPageId { get; private set; }
        private WalSnapshot? _prefixReader;
        public long WalLength => new FileInfo(_path + ".wal").Length;
        public TransactionCoordinator Transactions => (TransactionCoordinator)typeof(Pager)
            .GetField("_transactions", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(Pager)!;

        public static async Task<Fixture> CreateAsync(IWalFlushPolicy? flush = null)
        {
            var f = new Fixture();
            f._deadline.CancelAfter(TimeSpan.FromSeconds(30));
            f.Wal = new PausingWal(new WriteAheadLog(f._path, f.Index, checksumProvider: null,
                flushPolicy: flush ?? DurableWalFlushPolicy.Instance,
                durableCommitBatchWindow: TimeSpan.FromMilliseconds(0.25)));
            f.Pager = await Pager.CreateAsync(new FileStorageDevice(f._path, createNew: true), f.Wal, f.Index,
                new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(int.MaxValue),
                    MaxCachedWalReadPages = 0,
                }, f.Ct);
            await f.Pager.InitializeNewDatabaseAsync(f.Ct);
            await f.Pager.BeginTransactionAsync(f.Ct);
            f.PageId = await f.Pager.AllocatePageAsync(f.Ct);
            f.OtherPageId = await f.Pager.AllocatePageAsync(f.Ct);
            f.Pager.SchemaRootPage = f.PageId;
            await f.SetMarkerAsync(f.PageId, 10);
            await f.SetMarkerAsync(f.OtherPageId, 90);
            await f.Pager.CommitAsync(f.Ct);
            await f.Pager.CheckpointAsync(f.Ct);
            return f;
        }

        public async Task PrepareCopiedPrefixAsync(bool keepReader = false)
        {
            await CommitMarkerAsync(PageId, 11);
            WalSnapshot pin = Pager.AcquireReaderSnapshot();
            try
            {
                await Pager.CheckpointAsync(Ct);
                Assert.True(Wal.HasPendingCheckpoint);
                Assert.True(Wal.IsCheckpointCopyComplete);
            }
            finally
            {
                if (keepReader)
                    _prefixReader = pin;
                else
                    Pager.ReleaseReaderSnapshot(pin);
            }
        }

        public async Task<(uint RootPageId, uint RightLeafPageId)> SeedSplitTreeAsync()
        {
            await Pager.BeginTransactionAsync(Ct);
            uint leftLeafPageId = await Pager.AllocatePageAsync(Ct);
            uint rightLeafPageId = await Pager.AllocatePageAsync(Ct);
            uint rootPageId = await Pager.AllocatePageAsync(Ct);
            Pager.SchemaRootPage = rootPageId;

            var left = new SlottedPage(await Pager.GetPageAsync(leftLeafPageId, Ct), leftLeafPageId);
            left.Initialize(PageConstants.PageTypeLeaf);
            left.RightChildOrNextLeaf = rightLeafPageId;
            Assert.True(left.InsertCell(0, BTree.BuildLeafCell(10, Enumerable.Repeat((byte)0x11, 32).ToArray())));
            await Pager.MarkDirtyAsync(leftLeafPageId, Ct);

            var right = new SlottedPage(await Pager.GetPageAsync(rightLeafPageId, Ct), rightLeafPageId);
            right.Initialize(PageConstants.PageTypeLeaf);
            Assert.True(right.InsertCell(0, BTree.BuildLeafCell(100, Enumerable.Repeat((byte)0x22, 2052).ToArray())));
            Assert.True(right.InsertCell(1, BTree.BuildLeafCell(150, Enumerable.Repeat((byte)0x33, 1000).ToArray())));
            await Pager.MarkDirtyAsync(rightLeafPageId, Ct);

            var root = new SlottedPage(await Pager.GetPageAsync(rootPageId, Ct), rootPageId);
            root.Initialize(PageConstants.PageTypeInterior);
            root.RightChildOrNextLeaf = rightLeafPageId;
            byte[] interiorCell = new byte[13];
            interiorCell[0] = 12;
            BinaryPrimitives.WriteUInt32LittleEndian(interiorCell.AsSpan(1, sizeof(uint)), leftLeafPageId);
            BinaryPrimitives.WriteInt64LittleEndian(interiorCell.AsSpan(5, sizeof(long)), 100);
            Assert.True(root.InsertCell(0, interiorCell));
            await Pager.MarkDirtyAsync(rootPageId, Ct);

            await Pager.CommitAsync(Ct);
            await Pager.CheckpointAsync(Ct);
            return (rootPageId, rightLeafPageId);
        }

        public void ReleasePrefixReader()
        {
            if (_prefixReader is { } reader)
            {
                _prefixReader = null;
                Pager.ReleaseReaderSnapshot(reader);
            }
        }

        public async Task<PagerWriteTransaction> CreateConflictingTransactionAsync()
        {
            var tx = await Pager.BeginWriteTransactionAsync(Ct);
            using (tx.Bind())
                await SetMarkerAsync(PageId, 25);
            for (byte marker = 21; marker <= 25; marker++)
                await CommitMarkerAsync(PageId, marker);
            // Its base version is still read from the original snapshot.
            PageReadBuffer original = await Pager.GetSnapshotPageReadAsync(PageId, tx.State.Snapshot, Ct);
            Assert.Contains(original.Memory.Span[128], new byte[] { 10, 11 });
            return tx;
        }

        public long LatestOffset(uint pageId)
        {
            Assert.True(Index.TryGetLatest(pageId, out long offset));
            return offset;
        }

        public async Task SetMarkerAsync(uint pageId, byte marker)
        {
            byte[] page = await Pager.GetPageAsync(pageId, Ct);
            page[128] = marker;
            page[129] = (byte)pageId;
            await Pager.MarkDirtyAsync(pageId, Ct);
        }

        public async Task CommitMarkerAsync(uint pageId, byte marker)
        {
            await using var tx = await Pager.BeginWriteTransactionAsync(Ct);
            using (tx.Bind())
                await SetMarkerAsync(pageId, marker);
            await tx.CommitAsync(Ct);
        }

        public async Task AssertProgressAndResetAsync()
        {
            // Check the conflict's exact contents before another write can hide it.
            PageReadBuffer page = await Pager.ReadPageUncachedAsync(PageId, Ct);
            Assert.Equal((byte)PageId, page.Memory.Span[129]);
            await CommitMarkerAsync(PageId, 31);
            await Pager.CheckpointAsync(Ct); // Finish the old prefix, if still pending.
            await Pager.CheckpointAsync(Ct); // Copy/reset any retained tail.
            Assert.False(Wal.HasPendingCheckpoint);
            Assert.Equal(PageConstants.WalHeaderSize, WalLength);
            Assert.Equal(0, Index.FrameCount);
            page = await Pager.ReadPageUncachedAsync(PageId, Ct);
            Assert.Equal((byte)31, page.Memory.Span[128]);
            Assert.Equal((byte)PageId, page.Memory.Span[129]);
            Assert.Equal(0, Transactions.ActiveWriterCount);
        }

        public async ValueTask DisposeAsync()
        {
            Wal.ResumeRead.TrySetResult();
            Wal.BeforeFinalize = null;
            ReleasePrefixReader();
            await Pager.DisposeAsync();
            _deadline.Dispose();
            File.Delete(_path);
            File.Delete(_path + ".wal");
        }
    }

    private sealed class PausingFlushPolicy : IWalFlushPolicy
    {
        public bool Armed;
        public bool AllowsWriteConcurrencyDuringCommitFlush => true;
        public bool PerformsDurableFlush => true;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            if (Armed)
            {
                Armed = false;
                Entered.TrySetResult();
                await Resume.Task.WaitAsync(cancellationToken);
            }
            RandomAccess.FlushToDisk(handle);
        }
    }

    // Forward the copy-plan capability too, so the test uses production snapshot
    // copy-safety decisions. Only the I/O scheduling is controlled by the test.
    private sealed class PausingWal(WriteAheadLog inner) : IWriteAheadLog, IWalCheckpointCopyPlan
    {
        public long PauseOffset = -1;
        public int FinalizeCalls;
        public byte[]? PausedReadContents;
        public Func<CancellationToken, ValueTask>? BeforeFinalize;
        public Action? AfterFinalize;
        public TaskCompletionSource ReadEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ResumeRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FinalizeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool HasPendingCheckpoint => inner.HasPendingCheckpoint;
        public bool IsCheckpointCopyComplete => inner.IsCheckpointCopyComplete;
        public bool HasPendingCommitWork => inner.HasPendingCommitWork;
        public bool IsOpen => inner.IsOpen;
        public bool TryGetCheckpointRetainedWalStartOffset(out long offset) => inner.TryGetCheckpointRetainedWalStartOffset(out offset);
        public ValueTask<ReadOnlyMemory<KeyValuePair<uint, long>>> PrepareCheckpointCopyAsync(uint count, CancellationToken ct) => ((IWalCheckpointCopyPlan)inner).PrepareCheckpointCopyAsync(count, ct);
        public ValueTask OpenAsync(uint count, CancellationToken ct = default) => inner.OpenAsync(count, ct);
        public void BeginTransaction() => inner.BeginTransaction();
        public ValueTask AppendFrameAsync(uint id, ReadOnlyMemory<byte> page, CancellationToken ct = default) => inner.AppendFrameAsync(id, page, ct);
        public ValueTask AppendFramesAsync(ReadOnlyMemory<WalFrameWrite> frames, CancellationToken ct = default) => inner.AppendFramesAsync(frames, ct);
        public ValueTask<WalCommitResult> AppendFramesAndCommitAsync(ReadOnlyMemory<WalFrameWrite> frames, uint count, CancellationToken ct = default) => inner.AppendFramesAndCommitAsync(frames, count, ct);
        public ValueTask<WalCommitResult> CommitAsync(uint count, CancellationToken ct = default) => inner.CommitAsync(count, ct);
        public ValueTask RollbackAsync(CancellationToken ct = default) => inner.RollbackAsync(ct);
        public ValueTask<byte[]> ReadPageAsync(long offset, CancellationToken ct = default) => inner.ReadPageAsync(offset, ct);
        public async ValueTask ReadPageIntoAsync(long offset, Memory<byte> destination, CancellationToken ct = default)
        {
            bool paused = Interlocked.CompareExchange(ref PauseOffset, -1, offset) == offset;
            if (paused)
            {
                ReadEntered.TrySetResult();
                await ResumeRead.Task.WaitAsync(ct);
            }
            await inner.ReadPageIntoAsync(offset, destination, ct);
            if (paused)
                PausedReadContents = destination.ToArray();
        }
        public ValueTask<bool> CheckpointStepAsync(IStorageDevice device, uint count, int max, CancellationToken ct = default, bool allowFinalize = true) => inner.CheckpointStepAsync(device, count, max, ct, allowFinalize);
        public async ValueTask CheckpointAsync(IStorageDevice device, uint count, CancellationToken ct = default, bool allowFinalize = true)
        {
            if (allowFinalize)
            {
                Interlocked.Increment(ref FinalizeCalls);
                FinalizeEntered.TrySetResult();
                if (BeforeFinalize is { } before)
                    await before(ct);
            }
            await inner.CheckpointAsync(device, count, ct, allowFinalize);
            if (allowFinalize)
                AfterFinalize?.Invoke();
        }
        public ValueTask CloseAndDeleteAsync() => inner.CloseAndDeleteAsync();
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
