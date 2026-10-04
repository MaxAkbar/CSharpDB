using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Transactions;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public class PagerLegacyCatalogCaptureTests
{
    [Theory(Timeout = 30_000)]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    public async Task CaptureWaitsForWholeLegacyCompletion_AndFailsClosedAfterInterceptorFailure(
        bool reservePendingCommit, int completionFailure)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var interceptor = new CompletionInterceptor(completionFailure);
        await using var device = new MemoryStorageDevice();
        var index = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(index);
        await using var pager = await Pager.CreateAsync(device, wal, index, new PagerOptions
        {
            CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            Interceptors = [interceptor],
        }, ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);
        await using PagerWriteTransaction? otherWriter = reservePendingCommit
            ? await pager.BeginWriteTransactionAsync(ct)
            : null;
        WalSnapshot beforeCommit = pager.AcquireReaderSnapshot();
        WalSnapshot? published = null;
        try
        {
            Assert.True(pager.CanCaptureCommittedCatalog(beforeCommit));
            await pager.BeginTransactionAsync(ct);
            Assert.False(pager.CanCaptureCommittedCatalog(beforeCommit));
            PagerCommitResult commit = await pager.BeginCommitAsync(ct);
            await interceptor.Started.Task.WaitAsync(ct);
            published = pager.AcquireReaderSnapshot();

            // WAL publication and writer release have already completed; the
            // interceptor is the remaining part of legacy commit completion.
            Assert.False(pager.IsWriteTransactionActive);
            Assert.False(wal.HasPendingCommitWork);
            Assert.False(pager.CanCaptureCommittedCatalog(published));
            interceptor.Release.TrySetResult();
            if (completionFailure == 1)
                await Assert.ThrowsAsync<InvalidOperationException>(() => commit.WaitAsync(ct).AsTask());
            else if (completionFailure == 2)
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => commit.WaitAsync(ct).AsTask());
            else
                await commit.WaitAsync(ct);

            Assert.False(pager.CanCaptureCommittedCatalog(beforeCommit));
            Assert.Equal(completionFailure == 0, pager.CanCaptureCommittedCatalog(published));

            // A later successful commit must not clear the conservative failure
            // state, and a completed guard must not leak a pending count.
            await pager.BeginTransactionAsync(ct);
            await pager.CommitAsync(ct);
            WalSnapshot next = pager.AcquireReaderSnapshot();
            try
            {
                Assert.Equal(completionFailure == 0, pager.CanCaptureCommittedCatalog(next));
                Assert.False(pager.CanCaptureCommittedCatalog(published));
            }
            finally { pager.ReleaseReaderSnapshot(next); }
        }
        finally
        {
            interceptor.Release.TrySetResult();
            if (published is not null)
                pager.ReleaseReaderSnapshot(published);
            pager.ReleaseReaderSnapshot(beforeCommit);
        }
    }

    private sealed class CompletionInterceptor(int completionFailure) : IPageOperationInterceptor
    {
        private bool _armed = true;
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
        {
            if (!_armed)
                return;
            _armed = false;
            Started.TrySetResult();
            await Release.Task.WaitAsync(ct);
            if (completionFailure == 1)
                throw new InvalidOperationException("Injected commit completion failure.");
            if (completionFailure == 2)
                throw new OperationCanceledException("Injected commit completion cancellation.");
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
