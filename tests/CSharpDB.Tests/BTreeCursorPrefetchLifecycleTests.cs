using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class BTreeCursorPrefetchLifecycleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConsumedPrefetches_ContinueWithoutCancellation_AndRetainValues(bool asynchronous)
    {
        await using var fixture = await Fixture.CreateAsync();
        ReadControl? second = asynchronous ? fixture.Device.Hold(fixture.Leaves[1].PageId) : null;
        ReadControl? third = asynchronous ? fixture.Device.Hold(fixture.Leaves[2].PageId) : null;
        await using var cursor = fixture.Tree.CreateCursor();

        Assert.True(await cursor.SeekAsync(1, Ct));
        ReadOnlyMemory<byte> firstValue = cursor.CurrentValue;
        CancellationToken secondToken = Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[1].PageId));
        Assert.True(secondToken.CanBeCanceled);
        await MoveThroughAsync(cursor, fixture.Leaves[0].LastKey);
        await CrossLeafAsync(cursor, second);
        ReadOnlyMemory<byte> secondValue = cursor.CurrentValue;
        Assert.Equal(fixture.Leaves[1].FirstKey, cursor.CurrentKey);
        Assert.False(secondToken.IsCancellationRequested);

        CancellationToken thirdToken = Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[2].PageId));
        await MoveThroughAsync(cursor, fixture.Leaves[1].LastKey);
        await CrossLeafAsync(cursor, third);
        ReadOnlyMemory<byte> thirdValue = cursor.CurrentValue;
        Assert.Equal(fixture.Leaves[2].FirstKey, cursor.CurrentKey);
        Assert.False(secondToken.IsCancellationRequested);
        Assert.False(thirdToken.IsCancellationRequested);

        long expected = cursor.CurrentKey + 1;
        while (await cursor.MoveNextAsync(Ct))
        {
            Assert.Equal(expected++, cursor.CurrentKey);
            AssertPayload(cursor.CurrentValue, cursor.CurrentKey);
        }
        Assert.Equal(129, expected);
        Assert.False(await cursor.MoveNextAsync(Ct));
        await cursor.DisposeAsync();

        AssertPayload(firstValue, 1);
        AssertPayload(secondValue, fixture.Leaves[1].FirstKey);
        AssertPayload(thirdValue, fixture.Leaves[2].FirstKey);
    }

    [Fact]
    public async Task CancelledWait_RetriesDemandRead_WithoutReusingPendingReadCancellation()
    {
        await using var fixture = await Fixture.CreateAsync();
        ReadControl abandoned = fixture.Device.Hold(fixture.Leaves[1].PageId, ignoreCancellation: true);
        await using var cursor = fixture.Tree.CreateCursor();
        try
        {
            Assert.True(await cursor.SeekAsync(1, Ct));
            await MoveThroughAsync(cursor, fixture.Leaves[0].LastKey);
            ReadOnlyMemory<byte> retained = cursor.CurrentValue;
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            Task<bool> pending = cursor.MoveNextAsync(cancellation.Token).AsTask();
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);

            Assert.True(abandoned.Token.IsCancellationRequested);
            Assert.False(abandoned.Finished.IsCompleted);
            Assert.Equal(fixture.Leaves[0].LastKey, cursor.CurrentKey);
            AssertPayload(retained, fixture.Leaves[0].LastKey);
            Assert.True(await cursor.MoveNextAsync(Ct));
            Assert.Equal(fixture.Leaves[1].FirstKey, cursor.CurrentKey);
            AssertPayload(cursor.CurrentValue, cursor.CurrentKey);
            Assert.Equal(2, fixture.Device.ReadTokens(fixture.Leaves[1].PageId).Count);
            Assert.False(Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[2].PageId)).IsCancellationRequested);

            abandoned.Release();
            await abandoned.Finished.WaitAsync(Ct);
            Assert.True(await cursor.MoveNextAsync(Ct));
            Assert.Equal(fixture.Leaves[1].FirstKey + 1, cursor.CurrentKey);
            AssertPayload(retained, fixture.Leaves[0].LastKey);
        }
        finally
        {
            abandoned.Release();
            if (abandoned.Started) await abandoned.Finished.WaitAsync(Ct);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedSpeculativeRead_FallsBackToDemandRead_AndContinues(bool asynchronous)
    {
        await using var fixture = await Fixture.CreateAsync();
        ReadControl failed = fixture.Device.Hold(fixture.Leaves[1].PageId);
        if (!asynchronous) failed.Fail();
        await using var cursor = fixture.Tree.CreateCursor();
        Assert.True(await cursor.SeekAsync(1, Ct));
        ReadOnlyMemory<byte> retained = cursor.CurrentValue;
        await MoveThroughAsync(cursor, fixture.Leaves[0].LastKey);
        Task<bool> next = cursor.MoveNextAsync(Ct).AsTask();
        if (asynchronous)
        {
            Assert.False(next.IsCompleted);
            failed.Fail();
        }
        Assert.True(await next);
        Assert.Equal(fixture.Leaves[1].FirstKey, cursor.CurrentKey);
        AssertPayload(cursor.CurrentValue, cursor.CurrentKey);
        Assert.True(failed.Token.IsCancellationRequested);
        Assert.Equal(2, fixture.Device.ReadTokens(fixture.Leaves[1].PageId).Count);
        Assert.False(Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[2].PageId)).IsCancellationRequested);
        await MoveThroughAsync(cursor, fixture.Leaves[2].FirstKey);
        AssertPayload(retained, 1);
    }

    [Fact]
    public async Task Reseek_AbandonsPendingRead_WithoutCancellingNewRead()
    {
        await using var fixture = await Fixture.CreateAsync();
        ReadControl abandoned = fixture.Device.Hold(fixture.Leaves[1].PageId, ignoreCancellation: true);
        await using var cursor = fixture.Tree.CreateCursor();
        try
        {
            Assert.True(await cursor.SeekAsync(1, Ct));
            ReadOnlyMemory<byte> retained = cursor.CurrentValue;
            Assert.True(await cursor.SeekAsync(fixture.Leaves[2].FirstKey, Ct));
            Assert.True(abandoned.Token.IsCancellationRequested);
            Assert.False(abandoned.Finished.IsCompleted);
            Assert.Equal(fixture.Leaves[2].FirstKey, cursor.CurrentKey);
            Assert.False(Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[3].PageId)).IsCancellationRequested);

            abandoned.Release();
            await abandoned.Finished.WaitAsync(Ct);
            await MoveThroughAsync(cursor, fixture.Leaves[3].FirstKey);
            AssertPayload(retained, 1);
        }
        finally
        {
            abandoned.Release();
            if (abandoned.Started) await abandoned.Finished.WaitAsync(Ct);
        }
    }

    [Fact]
    public async Task Dispose_CancelsPendingRead_AndWaitsForItsCompletion()
    {
        await using var fixture = await Fixture.CreateAsync();
        ReadControl pendingRead = fixture.Device.Hold(fixture.Leaves[1].PageId, ignoreCancellation: true);
        var cursor = fixture.Tree.CreateCursor();
        Task? disposal = null;
        try
        {
            Assert.True(await cursor.SeekAsync(1, Ct));
            ReadOnlyMemory<byte> retained = cursor.CurrentValue;
            disposal = cursor.DisposeAsync().AsTask();
            Assert.True(pendingRead.Token.IsCancellationRequested);
            Assert.False(disposal.IsCompleted);
            pendingRead.Release();
            await disposal.WaitAsync(Ct);
            Assert.True(pendingRead.Finished.IsCompleted);
            AssertPayload(retained, 1);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => cursor.MoveNextAsync(Ct).AsTask());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => cursor.SeekAsync(1, Ct).AsTask());
            await cursor.DisposeAsync();
        }
        finally
        {
            pendingRead.Release();
            if (disposal is not null) await disposal.WaitAsync(Ct);
            else await cursor.DisposeAsync();
        }
    }

    [Fact]
    public async Task ReachingReadAheadBound_ReleasesCompletedRead_AndAllowsDemandTraversal()
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var cursor = fixture.Tree.CreateCursor(new IndexScanRange(1, true, fixture.Leaves[1].LastKey, true));
        Assert.True(await cursor.SeekAsync(1, Ct));
        CancellationToken secondToken = Assert.Single(fixture.Device.ReadTokens(fixture.Leaves[1].PageId));
        await MoveThroughAsync(cursor, fixture.Leaves[1].FirstKey);
        Assert.True(secondToken.IsCancellationRequested);
        Assert.Empty(fixture.Device.ReadTokens(fixture.Leaves[2].PageId));
        await MoveThroughAsync(cursor, fixture.Leaves[2].FirstKey);
        AssertPayload(cursor.CurrentValue, fixture.Leaves[2].FirstKey);
    }

    private static async Task CrossLeafAsync(BTreeCursor cursor, ReadControl? control)
    {
        Task<bool> next = cursor.MoveNextAsync(Ct).AsTask();
        if (control is not null)
        {
            Assert.False(next.IsCompleted);
            control.Release();
        }
        Assert.True(await next);
        AssertPayload(cursor.CurrentValue, cursor.CurrentKey);
    }

    private static async Task MoveThroughAsync(BTreeCursor cursor, long lastKey)
    {
        while (cursor.CurrentKey < lastKey)
        {
            long expected = cursor.CurrentKey + 1;
            Assert.True(await cursor.MoveNextAsync(Ct));
            Assert.Equal(expected, cursor.CurrentKey);
            AssertPayload(cursor.CurrentValue, expected);
        }
        Assert.Equal(lastKey, cursor.CurrentKey);
    }

    private static byte[] Payload(long key)
    {
        var value = new byte[512];
        Array.Fill(value, checked((byte)key));
        return value;
    }

    private static void AssertPayload(ReadOnlyMemory<byte> value, long key)
        => Assert.Equal(Payload(key), value.ToArray());

    private sealed record Leaf(uint PageId, long FirstKey, long LastKey);

    private sealed class Fixture(Pager writer, Pager reader, WalSnapshot snapshot,
        ControlledDevice device, BTree tree, IReadOnlyList<Leaf> leaves) : IAsyncDisposable
    {
        public ControlledDevice Device => device;
        public BTree Tree => tree;
        public IReadOnlyList<Leaf> Leaves => leaves;

        public static async Task<Fixture> CreateAsync()
        {
            var device = new ControlledDevice();
            var index = new WalIndex();
            var writer = await Pager.CreateAsync(device, new MemoryWriteAheadLog(index), index,
                new PagerOptions { EnableSequentialLeafReadAhead = true, CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000) }, Ct);
            await writer.InitializeNewDatabaseAsync(Ct);
            await writer.RecoverAsync(Ct);
            await writer.BeginTransactionAsync(Ct);
            var tree = new BTree(writer, await BTree.CreateNewAsync(writer, Ct));
            for (int key = 1; key <= 128; key++) await tree.InsertAsync(key, Payload(key), Ct);
            await writer.CommitAsync(Ct);
            var leaves = new List<Leaf>();
            uint pageId = await tree.FindLeftmostLeafAsync(Ct);
            while (pageId != PageConstants.NullPageId)
            {
                var page = new ReadOnlySlottedPage((await writer.GetPageReadAsync(pageId, Ct)).Memory, pageId);
                leaves.Add(new(pageId, BTree.ReadLeafKey(page, 0), BTree.ReadLeafKey(page, page.CellCount - 1)));
                pageId = page.RightChildOrNextLeaf;
            }
            Assert.True(leaves.Count >= 4);
            await writer.CheckpointAsync(Ct);
            var snapshot = writer.AcquireReaderSnapshot();
            var reader = writer.CreateSnapshotReader(snapshot);
            device.ResetReads();
            return new(writer, reader, snapshot, device, new BTree(reader, tree.RootPageId), leaves);
        }

        public async ValueTask DisposeAsync()
        {
            reader.Dispose();
            writer.ReleaseReaderSnapshot(snapshot);
            await writer.DisposeAsync();
        }
    }

    private sealed class ControlledDevice : IStorageDevice
    {
        private readonly MemoryStorageDevice _inner = new();
        private readonly object _gate = new();
        private readonly Dictionary<uint, ReadControl> _planned = [];
        private readonly List<(uint PageId, CancellationToken Token)> _reads = [];
        public long Length => _inner.Length;

        public ReadControl Hold(uint pageId, bool ignoreCancellation = false)
        {
            var control = new ReadControl(ignoreCancellation);
            lock (_gate) _planned.Add(pageId, control);
            return control;
        }

        public IReadOnlyList<CancellationToken> ReadTokens(uint pageId)
        {
            lock (_gate) return _reads.Where(r => r.PageId == pageId).Select(r => r.Token).ToArray();
        }

        public void ResetReads() { lock (_gate) _reads.Clear(); }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
        {
            ReadControl? control;
            lock (_gate)
            {
                uint pageId = checked((uint)(offset / PageConstants.PageSize));
                _reads.Add((pageId, ct));
                _planned.Remove(pageId, out control);
            }
            return control is null ? _inner.ReadAsync(offset, buffer, ct) : control.ReadAsync(_inner, offset, buffer, ct);
        }

        public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(offset, buffer, ct);
        public ValueTask FlushAsync(CancellationToken ct = default) => _inner.FlushAsync(ct);
        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => _inner.SetLengthAsync(length, ct);
        public void Dispose() => _inner.Dispose();
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class ReadControl(bool ignoreCancellation)
    {
        private readonly TaskCompletionSource<bool> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Started { get; private set; }
        public CancellationToken Token { get; private set; }
        public Task Finished => _finished.Task;
        public void Release() => _release.TrySetResult(true);
        public void Fail() => _release.TrySetException(new IOException("Controlled speculative read failure."));

        public async ValueTask<int> ReadAsync(MemoryStorageDevice inner, long offset, Memory<byte> buffer, CancellationToken ct)
        {
            Started = true;
            Token = ct;
            try
            {
                await _release.Task.WaitAsync(ignoreCancellation ? CancellationToken.None : ct);
                return await inner.ReadAsync(offset, buffer, ignoreCancellation ? CancellationToken.None : ct);
            }
            finally
            {
                _finished.TrySetResult(true);
            }
        }
    }
}
