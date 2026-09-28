using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class SimpleInsertTransactionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExplicitSimpleInserts_CompleteSynchronouslyAndCommitSingleAndMultipleRows()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);
        await CreateTableAsync(db);
        await db.BeginTransactionAsync(Ct);

        ValueTask<QueryResult> single = db.ExecuteAsync("INSERT INTO items VALUES (1, 'one')", Ct);
        Assert.True(single.IsCompletedSuccessfully);
        await using (QueryResult result = await single)
            Assert.Equal(1, result.RowsAffected);

        ValueTask<QueryResult> multiple = db.ExecuteAsync(
            "INSERT INTO items VALUES (2, 'two'), (3, 'three')", Ct);
        Assert.True(multiple.IsCompletedSuccessfully);
        await using (QueryResult result = await multiple)
            Assert.Equal(2, result.RowsAffected);

        await db.CommitAsync(Ct);
        Assert.Equal(new long[] { 1, 2, 3 }, await ReadIdsAsync(db, "items"));
    }

    [Fact]
    public async Task SynchronousSimpleInsertFailure_AbortsRollsBackAndAllowsRecovery()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);
        await CreateTableAsync(db);
        await InsertAsync(db, "INSERT INTO items VALUES (1, 'committed')");
        await db.BeginTransactionAsync(Ct);
        await InsertAsync(db, "INSERT INTO items VALUES (2, 'uncommitted')");

        ValueTask<QueryResult> duplicate = db.ExecuteAsync(
            "INSERT INTO items VALUES (1, 'duplicate')", Ct);
        Assert.True(duplicate.IsCompleted);
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await duplicate);
        Assert.Equal(ErrorCode.DuplicateKey, failure.Code);

        await AssertAbortedAndCommitRollbackAsync(db);
        Assert.Equal(new long[] { 1 }, await ReadIdsAsync(db, "items"));
        await AssertRecoveryAsync(db);
    }

    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("cancellation")]
    public async Task SuspendedSimpleInsert_PreservesCompletionAndTransactionState(string outcome)
    {
        var interceptor = new SuspendedReadInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { Interceptors = [interceptor] },
            },
        };
        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await CreateTableAsync(db);
        await InsertAsync(db, "INSERT INTO items VALUES (1, 'committed')");
        await db.BeginTransactionAsync(Ct);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        interceptor.Arm(db.GetTableRootPage("items"));
        Task<QueryResult> pending = db.ExecuteAsync(
            "INSERT INTO items VALUES (2, 'suspended')", cancellation.Token).AsTask();
        bool completionObserved = false;
        try
        {
            await interceptor.Entered.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            Assert.False(pending.IsCompleted);
            Assert.Equal(cancellation.Token, interceptor.ObservedToken);

            if (outcome == "success")
            {
                interceptor.Release();
                await using (QueryResult result = await pending.WaitAsync(TimeSpan.FromSeconds(10), Ct))
                {
                    completionObserved = true;
                    Assert.Equal(1, result.RowsAffected);
                }
                await db.CommitAsync(Ct);
                Assert.Equal(new long[] { 1, 2 }, await ReadIdsAsync(db, "items"));
            }
            else
            {
                if (outcome == "failure")
                {
                    var injected = new IOException("Injected suspended simple INSERT read failure.");
                    interceptor.Fail(injected);
                    IOException failure = await Assert.ThrowsAsync<IOException>(
                        async () => await pending.WaitAsync(TimeSpan.FromSeconds(10), Ct));
                    completionObserved = true;
                    Assert.Same(injected, failure);
                }
                else
                {
                    cancellation.Cancel();
                    OperationCanceledException failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                        async () => await pending.WaitAsync(TimeSpan.FromSeconds(10), Ct));
                    completionObserved = true;
                    Assert.Equal(cancellation.Token, failure.CancellationToken);
                }

                await AssertAbortedAndCommitRollbackAsync(db);
                Assert.Equal(new long[] { 1 }, await ReadIdsAsync(db, "items"));
                await AssertRecoveryAsync(db);
            }
        }
        finally
        {
            // Never leave the injected read blocked if an assertion fails.
            interceptor.Release();
            if (!completionObserved)
            {
                try
                {
                    await using QueryResult result = await pending.WaitAsync(TimeSpan.FromSeconds(10), Ct);
                }
                catch
                {
                    // Preserve the original assertion failure during cleanup.
                }
            }
        }
    }

    [Fact]
    public async Task TemporarySimpleInserts_BypassPersistentFailureAndDoNotPoisonHealthyTransaction()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);
        await CreateTableAsync(db);
        await db.ExecuteAsync("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, value TEXT)", Ct);
        await InsertAsync(db, "INSERT INTO items VALUES (1, 'committed')");
        await InsertAsync(db, "INSERT INTO scratch VALUES (1, 'temp')");
        await db.BeginTransactionAsync(Ct);

        CSharpDbException temporaryFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("INSERT INTO scratch VALUES (1, 'duplicate')", Ct));
        Assert.Equal(ErrorCode.DuplicateKey, temporaryFailure.Code);
        await InsertAsync(db, "INSERT INTO items VALUES (2, 'still usable')");
        await db.CommitAsync(Ct);

        await db.BeginTransactionAsync(Ct);
        CSharpDbException persistentFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("INSERT INTO items VALUES (1, 'duplicate')", Ct));
        Assert.Equal(ErrorCode.DuplicateKey, persistentFailure.Code);
        await InsertAsync(db, "INSERT INTO scratch VALUES (2, 'allowed'), (3, 'also allowed')");
        CSharpDbException failedTemporaryWrite = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("INSERT INTO scratch VALUES (1, 'duplicate again')", Ct));
        Assert.Equal(ErrorCode.DuplicateKey, failedTemporaryWrite.Code);

        await AssertAbortedAndCommitRollbackAsync(db);
        Assert.Equal(new long[] { 1, 2 }, await ReadIdsAsync(db, "items"));
        Assert.Equal(new long[] { 1, 2, 3 }, await ReadIdsAsync(db, "scratch"));
    }

    [Theory]
    [InlineData(ImplicitInsertExecutionMode.Serialized)]
    [InlineData(ImplicitInsertExecutionMode.ConcurrentWriteTransactions)]
    public async Task ImplicitSimpleInserts_CommitSingleAndMultipleRowsAndRecoverFromFailure(
        ImplicitInsertExecutionMode mode)
    {
        await using Database db = await Database.OpenInMemoryAsync(
            new DatabaseOptions { ImplicitInsertExecutionMode = mode }, Ct);
        await CreateTableAsync(db);
        await InsertAsync(db, "INSERT INTO items VALUES (1, 'one')");
        await InsertAsync(db, "INSERT INTO items VALUES (2, 'two'), (3, 'three')");

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("INSERT INTO items VALUES (1, 'duplicate')", Ct));
        Assert.Equal(ErrorCode.DuplicateKey, failure.Code);
        await InsertAsync(db, "INSERT INTO items VALUES (4, 'four')");
        Assert.Equal(new long[] { 1, 2, 3, 4 }, await ReadIdsAsync(db, "items"));
    }

    private static async Task CreateTableAsync(Database db)
    {
        await using QueryResult result = await db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", Ct);
    }

    private static async Task InsertAsync(Database db, string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
    }

    private static async Task<long[]> ReadIdsAsync(Database db, string table)
    {
        await using QueryResult result = await db.ExecuteAsync($"SELECT id FROM {table} ORDER BY id", Ct);
        return (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger).ToArray();
    }

    private static async Task AssertAbortedAndCommitRollbackAsync(Database db)
    {
        CSharpDbException rejected = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("INSERT INTO items VALUES (99, 'rejected')", Ct));
        Assert.Equal(ErrorCode.Unknown, rejected.Code);
        Assert.Equal(
            "The transaction is aborted because an earlier write failed; roll it back before issuing another write.",
            rejected.Message);

        CSharpDbException commit = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.CommitAsync(Ct));
        Assert.Equal(ErrorCode.Unknown, commit.Code);
        Assert.Contains("rolled back", commit.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task AssertRecoveryAsync(Database db)
    {
        await db.BeginTransactionAsync(Ct);
        await InsertAsync(db, "INSERT INTO items VALUES (3, 'recovered')");
        await db.CommitAsync(Ct);
        Assert.Equal(new long[] { 1, 3 }, await ReadIdsAsync(db, "items"));
    }

    private sealed class SuspendedReadInterceptor : IPageOperationInterceptor
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private uint _pageId;
        private int _armed;

        public Task Entered => _entered.Task;
        public CancellationToken ObservedToken { get; private set; }

        public void Arm(uint pageId)
        {
            _pageId = pageId;
            Volatile.Write(ref _armed, 1);
        }

        public void Release() => _release.TrySetResult();
        public void Fail(Exception exception) => _release.TrySetException(exception);

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
        {
            if (pageId != _pageId || Interlocked.Exchange(ref _armed, 0) == 0)
                return ValueTask.CompletedTask;

            ObservedToken = ct;
            _entered.TrySetResult();
            return new ValueTask(_release.Task.WaitAsync(ct));
        }

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
