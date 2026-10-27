using CSharpDB.Engine;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class WriteTransactionDisposalTests
{
    [Fact]
    public async Task CommitPreparationFailure_DisposeRollsBackAndReleasesSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var indexes = new OneShotRootFailureIndexProvider();
        await using var database = await Database.OpenInMemoryAsync(new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions { IndexProvider = indexes },
        }, ct);
        await database.ExecuteAsync(
            "CREATE TABLE preparation_items (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await database.ExecuteAsync(
            "CREATE INDEX ix_preparation_value ON preparation_items (value)", ct);
        await database.ExecuteAsync("INSERT INTO preparation_items VALUES (1, 10)", ct);

        WriteTransaction transaction = await database.BeginWriteTransactionAsync(ct);
        try
        {
            await using var insert = await transaction.ExecuteAsync(
                "INSERT INTO preparation_items VALUES (2, 20)", ct);
            Assert.Equal(1, database.ActiveReaderCount);

            // An index provider can fail while the catalog prepares its root
            // metadata, before any WAL commit publication begins.
            indexes.Arm();
            IOException failure = await Assert.ThrowsAsync<IOException>(
                () => transaction.CommitAsync(ct).AsTask());
            Assert.Same(indexes.Failure, failure);
            Assert.Equal(1, indexes.InjectedFailures);
        }
        finally
        {
            await transaction.DisposeAsync();
        }

        Assert.Equal(0, database.ActiveReaderCount);
        await using (var result = await database.ExecuteAsync(
            "SELECT id FROM preparation_items ORDER BY id", ct))
        {
            Assert.Equal(1L, Assert.Single(await result.ToListAsync(ct))[0].AsInteger);
        }

        await using (WriteTransaction next = await database.BeginWriteTransactionAsync(ct))
        {
            await using var insert = await next.ExecuteAsync(
                "INSERT INTO preparation_items VALUES (3, 30)", ct);
            await next.CommitAsync(ct);
        }
        Assert.Equal(0, database.ActiveReaderCount);
        await using var verified = await database.ExecuteAsync(
            "SELECT id FROM preparation_items ORDER BY id", ct);
        Assert.Equal(new long[] { 1, 3 },
            (await verified.ToListAsync(ct)).Select(row => row[0].AsInteger));
    }

    private sealed class OneShotRootFailureIndexProvider : IIndexProvider
    {
        private readonly BTreeIndexProvider _inner = new();
        private int _armed;
        private int _injectedFailures;

        public IOException Failure { get; } = new("Injected index root metadata failure.");
        public int InjectedFailures => Volatile.Read(ref _injectedFailures);
        public void Arm() => Volatile.Write(ref _armed, 1);

        public IIndexStore CreateIndexStore(Pager pager, uint rootPageId, string logicalName)
            => new FaultingIndexStore(_inner.CreateIndexStore(pager, rootPageId, logicalName), this);

        private void ReadRoot()
        {
            if (Interlocked.Exchange(ref _armed, 0) == 0) return;
            Interlocked.Increment(ref _injectedFailures);
            throw Failure;
        }

        private sealed class FaultingIndexStore(IIndexStore inner, OneShotRootFailureIndexProvider owner)
            : IIndexStore
        {
            public string LogicalName => inner.LogicalName;
            public uint RootPageId { get { owner.ReadRoot(); return inner.RootPageId; } }
            public void RecordPointRead(long key) => inner.RecordPointRead(key);
            public void RecordRangeRead(IndexScanRange range) => inner.RecordRangeRead(range);
            public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
                => inner.FindAsync(key, ct);
            public ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default)
                => inner.FindMaxKeyAsync(range, ct);
            public ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
                => inner.InsertAsync(key, payload, ct);
            public ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
                => inner.ReplaceAsync(key, payload, ct);
            public ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
                => inner.DeleteAsync(key, ct);
            public IIndexCursor CreateCursor(IndexScanRange range) => inner.CreateCursor(range);
        }
    }
}
